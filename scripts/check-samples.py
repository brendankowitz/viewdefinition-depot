#!/usr/bin/env python3
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
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
        return actual == Decimal(str(expected))
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

    if projection_columns != set(output_names):
        fail(f"{resource}: projection columns do not match outputSchema")

    for mapping in contract["projections"]:
        target = mapping["target"]
        if target not in target_types:
            fail(f"{resource}: unknown Fabric target path {target}")
        if mapping["targetType"] != target_types[target]:
            fail(f"{resource}: target type mismatch at {target}")
        if mapping["classification"] not in CLASSIFICATIONS:
            fail(f"{resource}: unknown classification at {target}")
        if mapping["classification"] == "direct" and (
            mapping["column"] != target
            or DIRECT_TYPES.get(mapping["parquetType"]) != mapping["targetType"]
        ):
            fail(f"{resource}: direct projection does not match {target}")
        if mapping["parquetType"] != next(
            field["duckdbType"]
            for field in output_schema
            if field["name"] == mapping["column"]
        ):
            fail(f"{resource}: Parquet type mismatch for {mapping['column']}")

    for field in contract["loaderOwnedFields"]:
        if field["path"] not in target_types or not field["reason"]:
            fail(f"{resource}: invalid loader-owned field {field['path']}")
        if field["path"] in projection_columns:
            fail(f"{resource}: loader-owned field is also projected: {field['path']}")

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
        fixture = INPUT_DIR / f"{contract['resource']}.ndjson"
        if not fixture.is_file():
            fail(f"missing fixture: {fixture}")
        view = VIEW_DIR / f"{contract['resource']}.json"
        if not view.is_file():
            fail(f"missing ViewDefinition: {view}")
        view_name = json.loads(view.read_text(encoding="utf-8"))["name"]
        if view_name != contract["view"]:
            fail(
                f"{contract['resource']}: expected view name {contract['view']!r}, "
                f"got {view_name!r}"
            )
        contracts.append(contract)

    subprocess.run(
        CLI + ["r4", "validate", "--views", str(VIEW_DIR)],
        cwd=ROOT,
        check=True,
    )
    with tempfile.TemporaryDirectory(prefix="fabric-silver-r4-") as directory:
        subprocess.run(
            CLI
            + [
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
            ],
            cwd=ROOT,
            check=True,
        )
        for contract in contracts:
            parquet_path = Path(directory) / f"{contract['resource']}.parquet"
            if not parquet_path.is_file():
                fail(f"missing Parquet output: {parquet_path.name}")
            check_contract(contract, parquet_path)

    print(f"Checked {len(contracts)} Fabric Silver R4 sample views.")


if __name__ == "__main__":
    main()
