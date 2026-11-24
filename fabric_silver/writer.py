"""Publish complete, typed Silver snapshots for a single local writer."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from fabric_silver.normalize import ShapeError, shape_row, target_schema
from fabric_silver.reference import schema_digest
from fabric_silver.views import RESOURCES, load_contract


class BatchError(ValueError):
    """Expected batch failure. Its message can be shown without source values."""


_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
_SNAPSHOT = re.compile(r"([0-9]{8,})-[A-Za-z0-9][A-Za-z0-9_-]*")
_AGE = "meta_lastUpdated"


def _token(value) -> bool:
    return isinstance(value, str) and _TOKEN.fullmatch(value) is not None


def _manifest_inputs(manifest: Path, event: str) -> tuple[dict, list[str]]:
    try:
        document = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        document = None
    if not isinstance(document, dict):
        raise BatchError("manifest: expected a readable JSON object")
    inputs = document.get("inputs")
    events = document.get("events")
    if not isinstance(inputs, dict) or not isinstance(events, list):
        raise BatchError("manifest: expected inputs and events")
    if not _token(event):
        raise BatchError("manifest: invalid event name")
    for input_id, item in inputs.items():
        if not _token(input_id):
            raise BatchError("manifest: invalid input id")
        if (not isinstance(item, dict) or item.get("resource") not in RESOURCES
                or not all(isinstance(item.get(key), str) and item[key]
                           for key in ("sourceSystem", "logicalPath"))):
            raise BatchError(f"{input_id}: invalid ingress context")
    matches = [entry for entry in events if isinstance(entry, dict) and entry.get("name") == event]
    if len(matches) != 1:
        raise BatchError(f"{event}: expected exactly one manifest event")
    selected = matches[0].get("inputs")
    if not isinstance(selected, list) or any(not _token(i) or i not in inputs for i in selected):
        raise BatchError(f"{event}: invalid manifest input list")
    return inputs, selected


def _check_extracted(extracted: Path, inputs: dict) -> None:
    try:
        files = sorted(extracted.iterdir())
    except OSError:
        files = None
    if files is None:
        raise BatchError("extracted: could not list input directory")
    for file in files:
        if file.suffix != ".parquet" or file.stem not in inputs:
            label = file.stem if _token(file.stem) else "<invalid input id>"
            raise BatchError(f"{label}: extra extracted file")


def _extracted_rows(extracted: Path, input_id: str, resource: str, columns: set[str]) -> list[dict]:
    try:
        table = pq.ParquetFile(extracted / f"{input_id}.parquet")
        with table:
            data = table.read()
        rows = data.to_pylist()
    except FileNotFoundError:
        reason = "missing extracted Parquet"
    except (OSError, UnicodeError, pa.ArrowException):
        reason = "unreadable extracted Parquet"
    else:
        # Shaping validates dict keys; also reject duplicate columns and check zero-row files.
        names = data.column_names
        if len(names) != len(set(names)) or (not rows and set(names) != columns):
            raise ShapeError(resource, "(row)", "extracted columns differ from the extraction view")
        return rows
    raise BatchError(f"{input_id}: {reason}")


def _current(state: Path) -> str | None:
    try:
        name = (state / "CURRENT").read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError):
        name = ""
    if not _SNAPSHOT.fullmatch(name):
        raise BatchError("state: invalid CURRENT pointer")
    return name


def _prior_rows(state: Path, name: str | None, resource: str, digest: str) -> dict[str, dict]:
    if name is None:
        return {}
    path = state / "snapshots" / name / f"{resource}.parquet"
    try:
        with pq.ParquetFile(path) as file:
            data = file.read()
        rows = data.to_pylist() if schema_digest(data.schema) == digest else None
    except (OSError, UnicodeError, pa.ArrowException):
        rows = None
    if rows is None:
        raise BatchError(f"{resource}: invalid prior snapshot Parquet")
    by_id = {row["id"]: row for row in rows}
    if None in by_id or len(by_id) != len(rows):
        raise BatchError(f"{resource}: invalid prior snapshot keys")
    return by_id


def _stage(resource: str, rows: list[dict], schema: pa.Schema, digest: str, path: Path) -> None:
    try:
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, path, coerce_timestamps="us", allow_truncated_timestamps=False)
        with pq.ParquetFile(path) as file:
            stored_digest = schema_digest(file.schema_arrow)
            stored = file.read()
        # Explicit typing first, then compare logical rows (including full decimals, ordered
        # nested values and UTC datetimes), independently of physical Parquet encoding and chunks.
        if stored_digest != digest:
            raise BatchError(f"{resource}: staged Parquet schema differs from HDS")
        if stored.to_pylist() != rows:
            raise BatchError(f"{resource}: staged Parquet rows differ from shaped rows")
    except (OSError, UnicodeError, pa.ArrowException):
        pass
    else:
        return
    raise BatchError(f"{resource}: could not write or read staged Parquet")


def _publish(state: Path, event: str, tables: dict, schemas: dict, digests: dict) -> Path:
    snapshots = state / "snapshots"
    temp_pointer = None
    renamed = False
    try:
        snapshots.mkdir(parents=True, exist_ok=True)
        # Include any complete but unreachable directory left by an interrupted publisher.
        last = max((int(match[1]) for path in snapshots.iterdir()
                    if (match := _SNAPSHOT.fullmatch(path.name))), default=0)
        destination = snapshots / f"{last + 1:08d}-{event}"
        with tempfile.TemporaryDirectory(prefix=".stage-", dir=state) as tmp:
            staged = Path(tmp) / "snapshot"
            staged.mkdir()
            for resource in RESOURCES:
                _stage(resource, tables[resource], schemas[resource], digests[resource],
                       staged / f"{resource}.parquet")

            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n",
                                             prefix=".current-", dir=state, delete=False) as pointer:
                temp_pointer = Path(pointer.name)
                pointer.write(destination.name + "\n")
            staged.rename(destination)
            renamed = True

        # Commit point: both tables are readable, and no fallible cleanup follows success.
        os.replace(temp_pointer, state / "CURRENT")
    except OSError:
        if renamed:
            shutil.rmtree(destination, ignore_errors=True)
        if temp_pointer is not None:
            try:
                temp_pointer.unlink(missing_ok=True)
            except OSError:
                pass  # This scratch pointer is never consulted by readers.
    else:
        return destination
    raise BatchError("state: could not publish snapshot")


def apply_batch(manifest: Path, event: str, extracted: Path, state: Path, *, ingest_time: datetime) -> Path:
    """Apply one ordered batch; atomically select a new two-table snapshot.

    The state belongs to one local writer. Readers resolve CURRENT once and use
    that immutable snapshot for both tables. This is not a Delta transaction.
    """
    if not isinstance(ingest_time, datetime) or ingest_time.utcoffset() is None:
        raise BatchError("ingest_time must be a timezone-aware datetime")
    try:
        moment = ingest_time.astimezone(UTC)
    except OverflowError:
        moment = None
    if moment is None:
        raise BatchError("ingest_time is out of timestamp range")

    inputs, selected = _manifest_inputs(manifest, event)
    _check_extracted(extracted, inputs)
    contracts = {resource: load_contract(resource) for resource in RESOURCES}
    digests = {resource: contract["schemaSha256"] for resource, contract in contracts.items()}
    columns = {resource: {col["column"] for col in contract["extraction"]}
               for resource, contract in contracts.items()}
    schemas = {resource: target_schema(resource) for resource in RESOURCES}

    # source-derived, unverified against a captured run: utils/dataframe_utils.py,
    # prepare_source_data and upsert_unique_to_delta_managed.
    latest = {resource: {} for resource in RESOURCES}
    seen = set()
    for input_id in selected:
        item = inputs[input_id]
        resource = item["resource"]
        for extracted_row in _extracted_rows(extracted, input_id, resource, columns[resource]):
            row = shape_row(resource, extracted_row, source_system=item["sourceSystem"],
                            logical_path=item["logicalPath"], ingest_time=moment)
            row_id, age = row["id"], row[_AGE]
            # Unlike HDS, fail on a row that could be silently skipped or arbitrarily selected.
            if age is None:
                raise ShapeError(resource, "meta.lastUpdated", "is required for a batch merge")
            key = resource, row_id, age
            if key in seen:
                raise ShapeError(resource, "meta.lastUpdated", "duplicate version for a normalized id in batch")
            seen.add(key)
            previous_candidate = latest[resource].get(row_id)
            if previous_candidate is None or previous_candidate[_AGE] < age:
                latest[resource][row_id] = row

    current = _current(state)
    tables = {}
    for resource in RESOURCES:
        rows = _prior_rows(state, current, resource, digests[resource])
        for row_id, candidate in latest[resource].items():
            previous = rows.get(row_id)
            if previous is None:
                rows[row_id] = candidate
            elif previous[_AGE] is not None and previous[_AGE] < candidate[_AGE]:
                rows[row_id] = {**candidate, "msftCreatedDatetime": previous["msftCreatedDatetime"]}
        tables[resource] = [rows[row_id] for row_id in sorted(rows)]
    return _publish(state, event, tables, schemas, digests)
