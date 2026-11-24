# Fabric Silver R4 Samples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a runnable, schema-checked Ignixa R4 Patient and Observation sample pack that is explicitly mapped to Microsoft Fabric healthcare data solutions Silver v1.4.0.

**Architecture:** Keep R4 Fabric Silver ViewDefinitions and synthetic input NDJSON separate. Store a small versioned target-field/mapping contract sourced from the HDS v1.4.0 Patient and Observation Delta metadata. A Python end-to-end checker invokes the pinned Ignixa CLI and uses DuckDB to verify exported Parquet schemas and values.

**Tech Stack:** Ignixa SQL on FHIR CLI 0.6.82, Python 3.13, DuckDB 2.0.1, .NET 10, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-24-fabric-silver-r4-samples-design.md`

**Evidence-based amendment:** CLI 0.6.82 names batch outputs by ViewDefinition file stem. The Patient date exports as `TIMESTAMP` and requires loader conversion to Fabric `date`. Repeating complex Patient fields are not losslessly serialized and remain loader-owned; the Observation fixture has one LOINC coding, projected as three flattened fields. The original intent to serialize the full collections is retained below as historical context, not a supported claim. The historical checker listing in Step 3 is superseded by `scripts/check-samples.py`, including its explicit conversion invariants and diagnostics.

## Global Constraints

- The first Fabric target is v1.4.0.
- A complete 146-resource catalog is a non-goal.
- A Fabric-compatible table writer, Spark transformation, or Delta Lake loader is a non-goal.
- Do not claim that flat ViewDefinition Parquet files are drop-in Fabric Silver Delta tables.
- Do not commit generated Parquet files or copy the 6.8 GB Fabric release or complete schemas into the repository.
- Pin `Ignixa.SqlOnFhir.Cli` at `0.6.82` in the .NET local tool manifest and DuckDB at `2.0.1` in the test requirements.

---

### Task 1: Create Fabric Silver contracts and runnable R4 samples

**Files:**
- Create: `.config/dotnet-tools.json` — pin `Ignixa.SqlOnFhir.Cli` to `0.6.82`.
- Create: `requirements-test.txt` — pin `duckdb==2.0.1`.
- Create: `contracts/fabric-hds/1.4.0/Patient.json` — selected HDS Patient target fields and mapping classifications.
- Create: `contracts/fabric-hds/1.4.0/Observation.json` — selected HDS Observation target fields and mapping classifications.
- Create: `views/r4/fabric-silver/Patient.json`.
- Create: `views/r4/fabric-silver/Observation.json`.
- Create: `examples/r4/fabric-silver/input/Patient.ndjson`.
- Create: `examples/r4/fabric-silver/input/Observation.ndjson`.
- Create: `scripts/check-samples.py` — validate and run the views, then check their Parquet output.

**Interfaces:**
- The batch output basename is the resource-named ViewDefinition file stem: `Patient.json` writes `Patient.parquet`; `Observation.json` writes `Observation.parquet`. Contract `view` is the ViewDefinition name.
- The definitions declare the string constant `sourceSystem`; the runner uses `--var sourceSystem=example-east`.
- Each resource contract declares its `resource`, `view`, `targetVersion`, `schemaSource`, `keyColumn`, `targetFields`, `loaderOwnedFields`, `outputSchema`, `expectedRowCount`, `projections`, and `expectedRows`. Each projection records `column`, FHIRPath `source`, Fabric `target`, Fabric `targetType`, `classification`, and expected Ignixa `parquetType`.
- The checker entry point is `python scripts/check-samples.py`. It runs the CLI using `subprocess.run(..., check=True)`, writes outputs into a `tempfile.TemporaryDirectory`, reads schema and rows with DuckDB, and fails with a nonzero exit code for command errors or contract mismatches.
- Supported compatibility classifications are `direct`, `serialized`, `flattened`, and `loader-owned`.

- [ ] **Step 1: Pin the local tools used by the checker**

Create `.config/dotnet-tools.json`:

```json
{
  "version": 1,
  "isRoot": true,
  "tools": {
    "ignixa.sqlonfhir.cli": {
      "version": "0.6.82",
      "commands": ["ignixa-sqlonfhir"]
    }
  }
}
```

Create `requirements-test.txt` containing:

```text
duckdb==2.0.1
```

Run:

```powershell
dotnet tool restore
python -m pip install -r requirements-test.txt
```

Expected: both dependencies restore successfully, `dotnet tool list --local` shows `Ignixa.SqlOnFhir.Cli` at `0.6.82`, and importing DuckDB reports `2.0.1`.

- [ ] **Step 2: Write the selected-field contracts and expected output schemas**

Create `contracts/fabric-hds/1.4.0/Patient.json` and `Observation.json`. Each file must record:

```json
{
  "resource": "Patient",
  "view": "patient_silver_projection",
  "targetVersion": "1.4.0",
  "schemaSource": "HDS.SourceCode/hds-build-artifacts/healthcare-tables/1.4.0/Silver/Patient/_delta_log/00000000000000000000.json",
  "keyColumn": "idOrig",
  "targetFields": [
    { "path": "idOrig", "type": "string" },
    { "path": "id", "type": "string" },
    { "path": "msftSourceSystem", "type": "string" },
    { "path": "meta_lastUpdatedOrig", "type": "timestamp" }
  ],
  "loaderOwnedFields": [
    { "path": "id", "type": "string", "reason": "Fabric-derived deterministic resource ID" }
  ],
  "expectedRowCount": 1,
  "outputSchema": [
    { "name": "idOrig", "duckdbType": "VARCHAR" },
    { "name": "msftSourceSystem", "duckdbType": "VARCHAR" },
    { "name": "meta_lastUpdatedOrig", "duckdbType": "TIMESTAMP" }
  ],
  "projections": [
    {
      "column": "idOrig",
      "source": "id",
      "target": "idOrig",
      "targetType": "string",
      "classification": "direct",
      "parquetType": "VARCHAR"
    }
  ],
  "expectedRows": [
    { "idOrig": "p1", "msftSourceSystem": "example-east" }
  ]
}
```

For Patient, include these target paths and Fabric types: `idOrig` string; loader-owned `id` string; `msftSourceSystem` string; `msftCreatedDatetime` and `msftModifiedDatetime` timestamps; `msftFilePath` string; `meta_lastUpdatedOrig` timestamp; `active` boolean; `gender` string; `birthDateOrig` date; and `identifierOrig_string` and `name_string` strings. Mark `id`, both Fabric timestamps, and `msftFilePath` as loader-owned. The original plan mapped `identifier` and `name` as serialized collections; because CLI 0.6.82 produces nulls for complex values, those fields are loader-owned instead. Record the observed Ignixa Parquet types separately: source update time and birth date are `TIMESTAMP`, booleans are `BOOLEAN`, and strings are `VARCHAR`. Birth date's conversion from Parquet `TIMESTAMP` to Fabric `date` is loader-owned.

For Observation, include these target paths and Fabric types: `idOrig` string; loader-owned `id` string; `msftSourceSystem` string; `msftCreatedDatetime` and `msftModifiedDatetime` timestamps; `msftFilePath` string; `meta_lastUpdatedOrig` timestamp; `status` string; `code.text` string; `code.coding` an array of coding structs; `subject.msftSourceReference` string; `valueQuantity.value` decimal(36,18); `valueQuantity.system`, `valueQuantity.code`, and `valueQuantity.unit` strings; and `valueString` string. Mark `id`, both Fabric timestamps, and `msftFilePath` as loader-owned. Map the source `id` to silver `idOrig` and the supplied namespace to `msftSourceSystem` as `direct`; map the original subject reference to `subject.msftSourceReference` and the flattened Quantity components to their respective nested paths as `flattened`. The original full-collection serialization intent is replaced by three flattened fields for the first LOINC coding; this is complete for the one-coding fixture, not arbitrary resources. Record observed Ignixa Parquet types separately, including `DECIMAL(38,18)` for the Quantity value. Store only selected target fields, not complete upstream schema files.

Run a standard-library JSON parse before adding views:

```powershell
python -c "import json,pathlib; [json.loads(p.read_text(encoding='utf-8')) for p in pathlib.Path('contracts/fabric-hds/1.4.0').glob('*.json')]; print('contracts parse')"
```

Expected: `contracts parse`.

- [ ] **Step 3: Write the checker before the ViewDefinitions**

Create `scripts/check-samples.py`. Resolve the repository root from `Path(__file__).resolve().parents[1]`; derive all view, fixture, and contract paths from that root. In `main()`, fail clearly if the view directory, an input contract, or a fixture is missing. For every contract, call these commands with `subprocess.run(..., cwd=ROOT, check=True)`:

```text
dotnet tool run ignixa-sqlonfhir -- r4 validate --views <ROOT/views/r4/fabric-silver>
dotnet tool run ignixa-sqlonfhir -- r4 run --views <ROOT/views/r4/fabric-silver> --input <ROOT/examples/r4/fabric-silver/input> --out <temporary output directory> --format parquet --var sourceSystem=example-east
```

Inspect each resource-named Parquet file with DuckDB. Use `DESCRIBE SELECT * FROM read_parquet(?)` with the Parquet path as a bound parameter and a `SELECT * FROM read_parquet(?)` query to read the rows. Assert that column names and DuckDB types equal the contract's `outputSchema`, that the row count equals `expectedRowCount`, and that keyed representative values equal `expectedRows`. Compare only generated output fields present in each expected row. Check every projection's `target` and every `loaderOwnedFields` path exists in the contract's selected `targetFields`; require each projection to have a recognized `classification` and each loader-owned path to have a reason. Fail rather than skip an unknown mapping. Print the failing contract, output column or target path, expected value/type, and observed value/type to stderr.

Do not catch and suppress subprocess errors, invalid Parquet, or failed assertions. Let `subprocess.run(check=True)` surface the command and exit status; when DuckDB cannot be imported, print the `python -m pip install -r requirements-test.txt` hint and exit nonzero.

The following is the historical checker implementation, retained for context. It is superseded by the checked-in checker, which handles resource-named files, projected loader-owned conversion, and detailed diagnostics:

```python
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
    contract_paths = sorted(CONTRACT_DIR.glob("*.json"))
    if not contract_paths:
        fail(f"no contracts found under {CONTRACT_DIR}")

    contracts = []
    for path in contract_paths:
        contract = json.loads(path.read_text(encoding="utf-8"))
        fixture = INPUT_DIR / f"{contract['resource']}.ndjson"
        if not fixture.is_file():
            fail(f"missing fixture: {fixture}")
        view = VIEW_DIR / f"{contract['resource']}.json"
        if not view.is_file():
            fail(f"missing ViewDefinition: {view}")
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
```

Run:

```powershell
python scripts/check-samples.py
```

Expected at this point: a nonzero exit with a clear missing sample or ViewDefinition message. The checker must not report success when its inputs are absent.

- [ ] **Step 4: Add one-row-per-resource synthetic FHIR fixtures**

Create `Patient.ndjson` with a single R4 Patient (`id` `p1`), `meta.lastUpdated`, two `identifier` entries, `active`, `gender`, `birthDate`, and a representative `name`. Create `Observation.ndjson` with exactly two R4 Observations whose `subject.reference` is `Patient/p1`: one final weight with LOINC `29463-7` and a Quantity (`72.5 kg`, UCUM system and code), and one final string-valued observation (`Reading captured`). Use ISO date/dateTime values and no real or copied patient data.

- [ ] **Step 5: Add the Patient ViewDefinition**

Create `views/r4/fabric-silver/Patient.json` as an R4 ViewDefinition named `patient_silver_projection`. Declare `fhirVersion: ["4.0.1"]`, the resource `Patient`, and the `sourceSystem` string constant. Emit these columns (the original complex collection columns are loader-owned, as amended above):

```json
[
  { "name": "msftSourceSystem", "path": "%sourceSystem", "type": "string" },
  { "name": "idOrig", "path": "id", "type": "string" },
  { "name": "meta_lastUpdatedOrig", "path": "meta.lastUpdated", "type": "datetime" },
  { "name": "active", "path": "active", "type": "boolean" },
  { "name": "gender", "path": "gender", "type": "string" },
  { "name": "birthDateOrig", "path": "birthDate", "type": "date" }
]
```

- [ ] **Step 6: Add the Observation ViewDefinition**

Create `views/r4/fabric-silver/Observation.json` as an R4 ViewDefinition named `observation_silver_projection`; declare `fhirVersion: ["4.0.1"]`, resource `Observation`, and the `sourceSystem` string constant. Emit namespace, original ID and update time, status, code text and a LOINC coding column, subject reference, Quantity value/system/code/unit, and the string choice branch:

```json
[
  { "name": "msftSourceSystem", "path": "%sourceSystem", "type": "string" },
  { "name": "idOrig", "path": "id", "type": "string" },
  { "name": "meta_lastUpdatedOrig", "path": "meta.lastUpdated", "type": "datetime" },
  { "name": "status", "path": "status", "type": "string" },
  { "name": "code_text", "path": "code.text", "type": "string" },
  { "name": "code_coding_loinc_system", "path": "code.coding.where(system = 'http://loinc.org').first().system", "type": "string" },
  { "name": "code_coding_loinc_code", "path": "code.coding.where(system = 'http://loinc.org').first().code", "type": "string" },
  { "name": "code_coding_loinc_display", "path": "code.coding.where(system = 'http://loinc.org').first().display", "type": "string" },
  { "name": "subject_reference", "path": "subject.reference", "type": "string" },
  { "name": "valueQuantity_value", "path": "value.ofType(Quantity).value", "type": "decimal" },
  { "name": "valueQuantity_system", "path": "value.ofType(Quantity).system", "type": "string" },
  { "name": "valueQuantity_code", "path": "value.ofType(Quantity).code", "type": "string" },
  { "name": "valueQuantity_unit", "path": "value.ofType(Quantity).unit", "type": "string" },
  { "name": "valueString", "path": "value.ofType(string)", "type": "string" }
]
```

- [ ] **Step 7: Check and correct the measured contract against the actual exports**

Run:

```powershell
python scripts/check-samples.py
```

Expected: Ignixa validates both views and emits both Parquet files. Update the contracts only to document observed exporter types and the selected HDS target-field paths/types; do not weaken checks for missing rows, bad values, or unknown compatibility classifications. Verify `meta_lastUpdatedOrig` and `birthDateOrig` are timestamps, the latter requires conversion to Fabric `date`, and Ignixa's decimal field is explicitly compared with the Fabric nested `decimal(36,18)` target. Confirm the Observation rows contain the expected coding components, reference, Quantity, and `valueString` values.

- [ ] **Step 8: Commit the runnable sample pack**

Run `git diff --check`, then review the contracts beside the actual v1.4.0 Delta-log types. Stage only the tool files, contracts, fixtures, views, and checker.

```powershell
git add .config/dotnet-tools.json requirements-test.txt contracts/fabric-hds/1.4.0 views/r4/fabric-silver examples/r4/fabric-silver/input scripts/check-samples.py
git commit -m "feat: add Fabric Silver R4 sample views" -m "Co-authored-by: Copilot App <223556219+Copilot@users.noreply.github.com>"
```

Expected: the checker passed before the commit, `git diff --check` passes, and the commit contains no generated Parquet or Fabric release files.

### Task 2: Document the pack and run the same check in CI

**Files:**
- Create: `README.md` — repository purpose, release/target index, prerequisites, and quick start.
- Create: `docs/packs/fabric-silver.md` — v1.4.0 provenance, mapping rules, and loader boundary.
- Create: `.github/workflows/check-samples.yml` — restore the pinned tools and run the end-to-end checker.

**Interfaces:**
- Contributor validation entry point: `python scripts/check-samples.py`.
- Manual CLI entry point after `dotnet tool restore`: `dotnet tool run ignixa-sqlonfhir -- r4 run --views views/r4/fabric-silver --input examples/r4/fabric-silver/input --out <output-directory> --format parquet --var sourceSystem=example-east`.
- CI runs the same Python checker from the repository root with .NET 10, Python 3.13, and the pinned DuckDB requirement.

- [ ] **Step 1: Write the root quick start and Fabric pack guidance**

Create `README.md` with the R4 Fabric Silver pack path, a link to `docs/packs/fabric-silver.md`, prerequisites, and these exact quick-start commands:

```powershell
dotnet tool restore
python -m pip install -r requirements-test.txt
python scripts/check-samples.py
```

Include a separate manual export command using `--out <output-directory>` and tell the reader that the generated files are Parquet projections rather than Fabric Delta tables.

Create `docs/packs/fabric-silver.md` with links to the Microsoft data architecture, clinical ingestion documentation, and v1.4.0 download. Record the official HDS archive source paths and the user-provided Delta-log locations. Explain medallion layers, the selected Patient and Observation fields, the four contract classifications, and loader-owned ID/reference normalization, timestamps, file lineage, and Delta upserts. State that repeating columns serialized as JSON strings and flattened Quantity/reference fields are not equivalent to their nested silver fields. Do not copy upstream HDS source code or full schemas.

- [ ] **Step 2: Add a least-privilege GitHub Actions check**

Create `.github/workflows/check-samples.yml`:

```yaml
name: Check sample views

on:
  pull_request:
  push:
  workflow_dispatch:

permissions:
  contents: read

jobs:
  sample-views:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: 10.0.x
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
      - name: Restore Ignixa CLI
        run: dotnet tool restore
      - name: Install Parquet checker
        run: python -m pip install -r requirements-test.txt
      - name: Validate and inspect sample exports
        run: python scripts/check-samples.py
```

- [ ] **Step 3: Run the documented validation path and whitespace check**

Run exactly the README quick-start commands, then run:

```powershell
git diff --check
git status --short
```

Expected: the end-to-end checker passes using the pinned tools, no Parquet or temporary files appear in `git status`, and `git diff --check` reports no whitespace errors.

- [ ] **Step 4: Commit the README, pack docs, and CI**

```powershell
git add README.md docs/packs/fabric-silver.md .github/workflows/check-samples.yml
git commit -m "docs: explain Fabric Silver sample contract" -m "Co-authored-by: Copilot App <223556219+Copilot@users.noreply.github.com>"
```

Expected: documentation and CI are committed separately from the runnable examples, and the worktree is clean.
