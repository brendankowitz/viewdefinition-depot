"""Independent, offline checks of one resource table at a time."""

import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


CLOCKS = ("msftCreatedDatetime", "msftModifiedDatetime")
KEY = ("msftSourceSystem", "idOrig")


def _logical_type(dtype):
    if pa.types.is_struct(dtype):
        return ["struct", [_field(f) for f in dtype]]
    if pa.types.is_list(dtype):
        # Spark's containsNull is significant; Parquet's item/element label isn't.
        return ["array", dtype.value_field.nullable, _logical_type(dtype.value_type)]
    return str(dtype)


def _field(field):
    return [field.name, field.nullable, _logical_type(field.type)]


def schema_digest(schema: pa.Schema) -> str:
    """SHA-256 of the ordered logical fields, independent of writer metadata."""
    payload = json.dumps([_field(f) for f in schema], separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _delta_type(spec):
    if isinstance(spec, str):
        if decimal := re.fullmatch(r"decimal\((\d+),(\d+)\)", spec):
            return pa.decimal128(*map(int, decimal.groups()))
        return {"string": pa.string(), "integer": pa.int32(), "boolean": pa.bool_(),
                "timestamp": pa.timestamp("us", tz="UTC"), "date": pa.date32()}[spec]
    if spec["type"] == "struct":
        return pa.struct([pa.field(f["name"], _delta_type(f["type"]), nullable=f["nullable"])
                          for f in spec["fields"]])
    if spec["type"] == "array":
        return pa.list_(pa.field("element", _delta_type(spec["elementType"]), nullable=spec["containsNull"]))
    raise ValueError


def deployed_schema(log: Path) -> pa.Schema:
    """Read the pilot logical types from the v1.4.0 deployment's creation log."""
    try:
        with log.open(encoding="utf-8") as lines:
            (metadata,) = [a["metaData"] for a in map(json.loads, lines) if "metaData" in a]
        struct = _delta_type(json.loads(metadata["schemaString"]))
        if not pa.types.is_struct(struct):
            raise ValueError
        return pa.schema(list(struct))
    except (OSError, ValueError, KeyError, TypeError):
        raise ValueError("Delta metadata: missing or unsupported deployment schema") from None


def _encoded(value):
    if isinstance(value, dict):
        return {k: _encoded(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_encoded(v) for v in value]
    if isinstance(value, datetime):
        # The reference export runs in UTC, including any timezone-naive timestamps.
        utc = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
        return utc.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def _instant(text, field):
    # datetime.fromisoformat alone silently truncates excess fractional digits.
    if isinstance(text, str) and re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)", text):
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass
    raise ValueError(f"{field}: invalid UTC instant")


def _read_snapshot(path, digest):
    try:
        with pq.ParquetFile(path) as parquet:
            schema = parquet.schema_arrow
            if schema_digest(schema) != digest:
                raise ValueError(f"{path.stem}: schema digest differs")
            table = parquet.read()
    except (OSError, pa.ArrowException):
        raise ValueError(f"{path.stem}: cannot read Parquet snapshot") from None
    try:
        return schema, [_encoded(row) for row in table.to_pylist()]
    except (ValueError, OverflowError, KeyError, pa.ArrowException):
        raise ValueError(f"{path.stem}: cannot decode typed Parquet values") from None


def snapshot_values(path: Path, schema_sha256: str) -> list[dict]:
    """Encode a schema-checked table without stringifying structs or rounding decimals."""
    _, rows = _read_snapshot(path, schema_sha256)
    return rows


def _unique_fields(pairs):
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise ValueError("duplicate JSON field")
    return fields


def _read_expected(path, schema, table):
    records = []
    try:
        with path.open(encoding="utf-8") as lines:
            for line in lines:
                record = json.loads(line, object_pairs_hook=_unique_fields)
                if not isinstance(record, dict) or record.keys() - {"values", "timeBounds"}:
                    raise ValueError
                values, bounds = record["values"], record.get("timeBounds", {})
                if not isinstance(values, dict) or not isinstance(bounds, dict):
                    raise ValueError
                records.append((values, bounds))
    except (OSError, ValueError, KeyError):
        raise ValueError(f"{table}: invalid or missing expectation records") from None

    for values, bounds in records:
        if values.keys() != set(schema.names):
            raise ValueError(f"{table}: values fields differ from schema")
        for name, window in bounds.items():
            # Never print an untrusted extra key from an expectation.
            field = f"{table}.{name}" if name in schema.names else f"{table}.timeBounds"
            if name not in CLOCKS or name not in schema.names or not pa.types.is_timestamp(schema.field(name).type):
                raise ValueError(f"{field}: not an ingestion timestamp")
            if not isinstance(window, dict) or window.keys() != {"min", "max"}:
                raise ValueError(f"{field}: invalid time bounds")
            lo, hi = (_instant(window[k], field) for k in ("min", "max"))
            if not lo <= _instant(values[name], field) <= hi:
                raise ValueError(f"{field}: captured instant outside bounds")
            bounds[name] = (lo, hi)
    return records


def _difference(actual, expected, path):
    """Return a field path, never a row value or an unknown expectation key."""
    if type(actual) is not type(expected):
        return path
    if isinstance(actual, dict):
        if actual.keys() != expected.keys():
            return path
        for key in actual:
            if diff := _difference(actual[key], expected[key], f"{path}.{key}"):
                return diff
    elif isinstance(actual, list):
        if len(actual) != len(expected):
            return path
        for i, (a, e) in enumerate(zip(actual, expected)):
            if diff := _difference(a, e, f"{path}[{i}]"):
                return diff
    elif actual != expected:
        return path
    return None


def _row_difference(table, row, values, bounds, ranks):
    for name, actual in row.items():
        path = f"{table}.{name}"
        if name in bounds:
            lo, hi = bounds[name]
            if actual is None or not lo <= _instant(actual, path) <= hi:
                return path
        elif diff := _difference(actual, values[name], path):
            return diff
        if ranks and name in CLOCKS and actual is not None:
            if ranks[0][_instant(actual, path)] != ranks[1][_instant(values[name], path)]:
                return path
    return None


def _key(table, row):
    for name in KEY:
        if not isinstance(row.get(name), str):
            raise ValueError(f"{table}.{name}: missing or invalid row key")
    return tuple(row[k] for k in KEY)


def _match_group(table, actual, expected, ranks=None):
    # Windows of different widths can overlap. Rematch a broad expectation
    # rather than greedily consuming the only row a narrower one can use.
    candidates = []
    mismatch = table
    for values, bounds in expected:
        matches = []
        for i, row in enumerate(actual):
            if diff := _row_difference(table, row, values, bounds, ranks):
                mismatch = diff
            else:
                matches.append(i)
        if not matches:
            raise ValueError(f"{mismatch}: no matching row")
        candidates.append(matches)

    assigned = {}

    def assign(index, visited):
        for i in candidates[index]:
            if i not in visited:
                visited.add(i)
                if i not in assigned or assign(assigned[i], visited):
                    assigned[i] = index
                    return True
        return False

    if not all(assign(index, set()) for index in range(len(expected))):
        raise ValueError(f"{mismatch}: row multiplicity differs")


def _checked_snapshot(actual, expected, schema_sha256):
    schema, rows = _read_snapshot(actual, schema_sha256)
    label = actual.stem
    records = _read_expected(expected, schema, label)
    if not records:
        raise ValueError(f"{label}: reference row count is zero")
    if len(rows) != len(records):
        raise ValueError(f"{label}: row count differs")
    seen, wanted = defaultdict(list), defaultdict(list)
    for row in rows:
        seen[_key(label, row)].append(row)
    for values, bounds in records:
        wanted[_key(label, values)].append((values, bounds))
    for key in seen.keys() | wanted.keys():
        if len(seen[key]) != len(wanted[key]):
            raise ValueError(f"{label}: row count per (msftSourceSystem, idOrig) differs")
        _match_group(label, seen[key], wanted[key])
    return seen, wanted


def compare_snapshot(actual: Path, expected: Path, schema_sha256: str) -> None:
    """Check one table's schema and keyed row multiset, not Parquet bytes/order."""
    _checked_snapshot(actual, expected, schema_sha256)


def _clock_ranks(keyed_rows, table):
    # Rank within one source/id across all three events. Absolute run times
    # can vary; ties, increases and decreases must reproduce the observation.
    times = defaultdict(set)
    for key, row in keyed_rows:
        instants = times[key]
        for field in CLOCKS:
            if field not in row:
                raise ValueError(f"{table}.{field}: missing ingestion clock")
            if row[field] is not None:
                instants.add(_instant(row[field], f"{table}.{field}"))
    return {key: {t: i for i, t in enumerate(sorted(group))} for key, group in times.items()}


def compare_events(pairs: list[tuple[Path, Path]], schema_sha256: str) -> None:
    """Compare initial, replay and update for ONE table, including clock relationships."""
    if len(pairs) != 3 or len({actual.stem for actual, _ in pairs}) != 1:
        raise ValueError("table sequence: need initial, replay and update for the same table")
    if any(len({path.resolve() for path in paths}) != 3 for paths in zip(*pairs)):
        raise ValueError("table sequence: need three distinct exports and three distinct expectations")
    actual_events, expected_events = zip(*(_checked_snapshot(a, e, schema_sha256) for a, e in pairs))
    table = pairs[0][0].stem
    actual_ranks = _clock_ranks(((k, row) for event in actual_events for k, rows in event.items()
                                 for row in rows), table)
    expected_ranks = _clock_ranks(((k, v) for event in expected_events for k, records in event.items()
                                   for v, _ in records), table)
    for name, actual, expected in zip(("initial", "replay", "update"), actual_events, expected_events):
        for key, rows in actual.items():
            _match_group(f"{table} ({name})", rows, expected[key], (actual_ranks[key], expected_ranks[key]))
