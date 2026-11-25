#!/usr/bin/env python3
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import re
import subprocess
import sys
import tempfile

try:
    import duckdb
except ImportError:
    print(
        "DuckDB is required; run python -m pip install -r requirements-test.txt",
        file=sys.stderr,
    )
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
VIEW_DIR = ROOT / "views" / "r4" / "fabric-silver"
INPUT_DIR = ROOT / "examples" / "r4" / "fabric-silver" / "input"
CONTRACT_DIR = ROOT / "contracts" / "fabric-hds" / "1.4.0"
CLASSIFICATIONS = {"direct", "serialized", "flattened", "loader-owned"}
DIRECT_TYPES = {
    "VARCHAR": "string",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
}
CLI = ["dotnet", "tool", "run", "ignixa-sqlonfhir", "--"]


def fail(message):
    print(f"sample check failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def run_cli(args):
    """Run the CLI and fail on its own failure markers: it exits 0 after per-view errors."""
    try:
        done = subprocess.run(
            CLI + args, cwd=ROOT, capture_output=True, text=True, encoding="utf-8"
        )
    except FileNotFoundError:
        fail("dotnet was not found on PATH")
    output = done.stdout + done.stderr
    sys.stdout.buffer.write(output.encode("utf-8"))
    sys.stdout.flush()
    counts = re.findall(r"(\d+) (?:failed|skipped)", output)
    if done.returncode or "\u2717" in output or "ERROR:" in output or any(
        int(count) for count in counts
    ):
        fail(f"ignixa-sqlonfhir {args[1]} reported failures (exit {done.returncode})")


def matches(actual, expected):
    if isinstance(actual, datetime) and isinstance(expected, str):
        expected_time = datetime.fromisoformat(expected.replace("Z", "+00:00"))
        actual_time = (
            actual.replace(tzinfo=timezone.utc)
            if actual.tzinfo is None
            else actual.astimezone(timezone.utc)
        )
        expected_time = (
            expected_time.replace(tzinfo=timezone.utc)
            if expected_time.tzinfo is None
            else expected_time.astimezone(timezone.utc)
        )
        return actual_time == expected_time
    if isinstance(actual, date) and not isinstance(actual, datetime):
        return isinstance(expected, str) and actual.isoformat() == expected
    if isinstance(actual, Decimal):
        return expected is not None and actual == Decimal(str(expected))
    if isinstance(actual, str) and isinstance(expected, (list, dict)):
        try:
            return json.loads(actual) == expected
        except json.JSONDecodeError:
            return False
    return actual == expected


def check_contract(contract, parquet_path):
    resource = contract["resource"]
    output_schema = contract["outputSchema"]
    output_names = [field["name"] for field in output_schema]
    target_types = {field["path"]: field["type"] for field in contract["targetFields"]}
    projection_columns = {item["column"] for item in contract["projections"]}
    loader_fields = {field["path"]: field for field in contract["loaderOwnedFields"]}

    if projection_columns != set(output_names):
        fail(f"{resource}: projection columns do not match outputSchema")

    for mapping in contract["projections"]:
        target = mapping["target"]
        if target not in target_types:
            fail(f"{resource}: unknown Fabric target path {target}")
        if mapping["targetType"] != target_types[target]:
            fail(
                f"{resource}: target type mismatch at {target}; "
                f"expected {target_types[target]!r}, got {mapping['targetType']!r}"
            )
        if mapping["classification"] not in CLASSIFICATIONS:
            fail(f"{resource}: unknown classification at {target}")
        if mapping["classification"] == "direct" and (
            mapping["column"] != target
            or DIRECT_TYPES.get(mapping["parquetType"]) != mapping["targetType"]
        ):
            fail(
                f"{resource}: direct projection does not match {target}; "
                f"expected column {target!r} with Fabric type {mapping['targetType']!r}, "
                f"got column {mapping['column']!r} with Parquet type {mapping['parquetType']!r}"
            )
        if mapping["classification"] == "loader-owned" and target not in loader_fields:
            fail(f"{resource}: loader-owned projection lacks a loader-owned field: {target}")
        expected_parquet_type = next(
            field["duckdbType"]
            for field in output_schema
            if field["name"] == mapping["column"]
        )
        if mapping["parquetType"] != expected_parquet_type:
            fail(
                f"{resource}: Parquet type mismatch for {mapping['column']}; "
                f"expected {expected_parquet_type!r}, got {mapping['parquetType']!r}"
            )

    for field in contract["loaderOwnedFields"]:
        if field["path"] not in target_types or not field["reason"]:
            fail(f"{resource}: invalid loader-owned field {field['path']}")
        if field["type"] != target_types[field["path"]]:
            fail(
                f"{resource}: loader-owned type mismatch at {field['path']}; "
                f"expected {target_types[field['path']]!r}, got {field['type']!r}"
            )
        projected = [m for m in contract["projections"] if m["target"] == field["path"]]
        if field["path"] in projection_columns and not any(
            mapping["column"] == field["path"] for mapping in projected
        ):
            fail(f"{resource}: loader-owned column targets another field: {field['path']}")
        if projected:
            if len(projected) != 1 or projected[0]["classification"] != "loader-owned":
                fail(f"{resource}: invalid projected loader-owned field: {field['path']}")
            observed = projected[0]["parquetType"]
            expected = field.get("sourceParquetType")
            if expected != observed:
                fail(
                    f"{resource}: loader-owned source type mismatch at {field['path']}; "
                    f"expected {expected!r}, got {observed!r}"
                )
        elif "sourceParquetType" in field:
            fail(f"{resource}: loader-owned conversion is not projected: {field['path']}")

    with duckdb.connect() as connection:
        actual_schema = [
            (row[0], row[1])
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet_path)]
            ).fetchall()
        ]
        expected_schema = [
            (field["name"], field["duckdbType"]) for field in output_schema
        ]
        if actual_schema != expected_schema:
            fail(
                f"{resource}: Parquet schema mismatch; expected "
                f"{expected_schema}, got {actual_schema}"
            )

        rows = connection.execute(
            "SELECT * FROM read_parquet(?)", [str(parquet_path)]
        ).fetchall()

    if len(rows) != contract["expectedRowCount"]:
        fail(
            f"{resource}: expected {contract['expectedRowCount']} rows, "
            f"got {len(rows)}"
        )

    positions = {name: index for index, name in enumerate(output_names)}
    by_key = {
        row[positions[contract["keyColumn"]]]: dict(zip(output_names, row))
        for row in rows
    }
    for expected_row in contract["expectedRows"]:
        key = expected_row[contract["keyColumn"]]
        if key not in by_key:
            fail(f"{resource}: expected row {key!r} is missing")
        for name, expected in expected_row.items():
            actual = by_key[key][name]
            if not matches(actual, expected):
                fail(
                    f"{resource} row {key!r}, column {name}: "
                    f"expected {expected!r}, got {actual!r}"
                )


def main():
    if not VIEW_DIR.is_dir():
        fail(f"missing view directory: {VIEW_DIR}")
    for resource in ("Patient", "Observation"):
        contract_path = CONTRACT_DIR / f"{resource}.json"
        if not contract_path.is_file():
            fail(f"missing contract: {contract_path}")
    contract_paths = sorted(CONTRACT_DIR.glob("*.json"))

    contracts = []
    for path in contract_paths:
        contract = json.loads(path.read_text(encoding="utf-8"))
        resource = contract.get("resource")
        if resource != path.stem:
            fail(
                f"{path.name}: contract resource {resource!r} "
                f"does not match filename stem {path.stem!r}"
            )
        fixture = INPUT_DIR / f"{resource}.ndjson"
        if not fixture.is_file():
            fail(f"missing fixture: {fixture}")
        view_path = VIEW_DIR / path.name
        if not view_path.is_file():
            fail(f"missing ViewDefinition: {view_path}")
        view = json.loads(view_path.read_text(encoding="utf-8"))
        if view.get("resource") != resource:
            fail(
                f"{view_path.name}: ViewDefinition resource {view.get('resource')!r} "
                f"does not match contract resource {resource!r}"
            )
        view_name = view["name"]
        if view_name != contract["view"]:
            fail(
                f"{resource}: expected view name {contract['view']!r}, "
                f"got {view_name!r}"
            )
        contracts.append(contract)

    views = {path.name for path in VIEW_DIR.glob("*.json")}
    contract_names = {path.name for path in contract_paths}
    if views != contract_names:
        fail(f"views and contracts differ: {sorted(views ^ contract_names)}")

    run_cli(["r4", "validate", "--views", str(VIEW_DIR)])
    with tempfile.TemporaryDirectory(prefix="fabric-silver-r4-") as directory:
        run_cli(
            [
                "r4",
                "run",
                "--views",
                str(VIEW_DIR),
                "--input",
                str(INPUT_DIR),
                "--out",
                directory,
                "--format",
                "parquet",
                "--var",
                "sourceSystem=example-east",
            ]
        )
        for contract in contracts:
            parquet_path = Path(directory) / f"{contract['resource']}.parquet"
            if not parquet_path.is_file():
                fail(f"missing Parquet output: {parquet_path.name}")
            check_contract(contract, parquet_path)

    print(f"Checked {len(contracts)} Fabric Silver R4 sample views.")


if __name__ == "__main__":
    main()
