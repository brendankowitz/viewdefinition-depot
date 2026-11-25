"""Compare an operator's capture.json with <candidate-dir>/<event>/<table>.parquet.

Input path and expected rowsPath files are relative to capture.json.
logicalPath, callerNamespace and sourceToken describe the synthetic input;
sourceSystem and observedFilePath are the exact emitted msftSourceSystem
and msftFilePath. Observed lineage is never opened or rewritten. Only the
declared snapshots are compared; this command cannot authenticate origin.
"""

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from functools import cache
from pathlib import Path

from jsonschema import Draft202012Validator

from .reference import Snapshot, compare_table, parse_json, read_records, utc_instant


CONTRACT_DIR = Path(__file__).resolve().parents[1] / "contracts/fabric-hds/1.4.0/r4"
RESOURCE_DIR = CONTRACT_DIR / "resources"


def _json_file(path, label):
    try:
        return parse_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError):
        raise ValueError(f"{label}: invalid or missing JSON file") from None


@cache
def _validators():
    schema = _json_file(CONTRACT_DIR / "capture.schema.json", "capture format")
    manifest = Draft202012Validator(schema)
    return manifest, manifest.evolve(schema={"$ref": "#/$defs/rowRecord"})


def _validate(validator, value, label):
    if error := next(validator.iter_errors(value), None):
        # Schema paths are public; an instance path or validation message can contain row data.
        rule = "/".join(str(k) for k in error.absolute_schema_path if k not in ("properties", "items"))
        raise ValueError(f"{label}: violates capture schema ({rule})")


def _local_file(root, relative, label):
    # All manifest paths have already passed the schema's portable relative-path rule.
    try:
        path = (root / relative).resolve()
    except (OSError, RuntimeError):
        raise ValueError(f"{label}: cannot resolve local path") from None
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError(f"{label}: missing file or path outside its directory")
    return path


def _read_manifest(path: Path) -> dict:
    doc = _json_file(path, "capture manifest")
    _validate(_validators()[0], doc, "capture manifest")
    root = path.parent.resolve()
    for item in doc["inputs"].values():
        source = _local_file(root, item["path"], "capture input")
        with source.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != item["sha256"]:
                raise ValueError("capture input: SHA-256 differs")
    if len({e["name"].casefold() for e in doc["events"]}) != len(doc["events"]):
        raise ValueError("capture manifest: duplicate event name")
    for event in doc["events"]:
        if set(event["inputIds"]) - doc["inputs"].keys():
            raise ValueError("capture manifest: event refers to unknown input ID")
    return doc


def _run_window(event):
    window = event["runWindowUtc"]
    start, end = (utc_instant(window[k], "runWindowUtc") for k in ("start", "end"))
    if start > end:
        raise ValueError("capture manifest: reversed UTC run window")
    return start, end


def _validated_records(path, windows, bindings):
    validator = _validators()[1]
    for number, record in enumerate(read_records(path), 1):
        _validate(validator, record, f"expected row {number}")
        row = record["values"]
        source, lineage = row.get("msftSourceSystem"), row.get("msftFilePath")
        if not (isinstance(source, str) and isinstance(lineage, str) and (source, lineage) in bindings):
            raise ValueError(f"expected row {number}: no observed source/file binding for this or an earlier event")
        for field, bounds in record.get("timeBounds", {}).items():
            lo, hi = (utc_instant(bounds[k], field) for k in ("min", "max"))
            # A retained value may have been assigned by an earlier event.
            if not any(start <= lo <= hi <= end for start, end in windows):
                raise ValueError(f"{field}: bounds outside recorded run windows")
        yield record


def _check_contract(item):
    resource = item["resource"]
    label = f"{resource} contract"
    contract = _json_file(_local_file(RESOURCE_DIR.resolve(), f"{resource}.json", label), label)
    try:
        if contract["contractFormatVersion"] != "1":
            raise ValueError(f"{label}: unsupported contract format version")
        identity = (contract["resource"], contract["table"], contract["deployment"]["schemaSha256"])
    except (KeyError, TypeError):
        raise ValueError(f"{label}: missing deployed schema identity") from None
    if identity != (resource, item["table"], item["schemaSha256"]):
        raise ValueError(f"{label}: resource, table or schema digest differs")


def compare_capture(path: Path, candidates: Path) -> dict:
    """Compare exactly the named event/table snapshots; never derive expected rows from them."""
    doc = _read_manifest(path)
    root, candidates = path.parent.resolve(), candidates.resolve()
    tables = defaultdict(list)
    windows, results = [], []
    bindings = set()
    seen_rows, seen_candidates = set(), set()
    for event in doc["events"]:
        name = event["name"]
        windows.append(_run_window(event))
        for input_id in event["inputIds"]:
            source = doc["inputs"][input_id]
            bindings.add((source["sourceSystem"], source["observedFilePath"]))
        resources, table_names = set(), set()
        for item in event["resources"]:
            resource, table = item["resource"], item["table"]
            if resource in resources or table in table_names:
                raise ValueError(f"{name}: duplicate resource or table")
            resources.add(resource)
            table_names.add(table)
            _check_contract(item)
            expected = _local_file(root, item["rowsPath"], f"{table} rowsPath")
            actual = _local_file(candidates, f"{name}/{table}.parquet", f"{name}/{table} candidate")
            if expected in seen_rows or actual in seen_candidates:
                raise ValueError(f"{name}/{table}: reused snapshot file")
            seen_rows.add(expected)
            seen_candidates.add(actual)
            records = _validated_records(expected, tuple(windows), frozenset(bindings))
            tables[table, item["schemaSha256"]].append(
                Snapshot(actual, records, row_count=item["rowCount"], delta_version=item["deltaVersion"]))
            results.append({"event": name, **item})
    for (_, digest), snapshots in tables.items():
        compare_table(snapshots, digest)
    return {
        "result": "matched", "captureId": doc["captureId"], "snapshots": results,
        "candidateLayout": "<candidate-dir>/<event>/<table>.parquet",
        "captureLayout": "inputs[*].path and events[*].resources[*].rowsPath are relative to capture.json",
        "scope": "Declared snapshots only. Release/configuration and original Parquet hashes are "
                 "operator provenance, not authenticated by this comparison.",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="operator's capture.json")
    parser.add_argument("candidates", type=Path, help="independently supplied candidate directory")
    parser.add_argument("--report", type=Path, help="write the match report to a new JSON file (default: stdout)")
    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)
    try:
        report = compare_capture(args.manifest, args.candidates)
        report["command"] = ["python", "-m", "fabric_silver.capture", *argv]
        text = json.dumps(report, indent=2) + "\n"
        if args.report is None:
            sys.stdout.write(text)
        else:
            # Never overwrite a capture, candidate, or a previous comparison.
            with args.report.open("x", encoding="utf-8") as out:
                out.write(text)
    except ValueError as error:
        print(f"capture: {error}", file=sys.stderr)
        return 1
    except OSError:
        print("capture: cannot read local files or create a new report", file=sys.stderr)
        return 1
    except RecursionError:
        print("capture: input exceeds comparison nesting limits", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
