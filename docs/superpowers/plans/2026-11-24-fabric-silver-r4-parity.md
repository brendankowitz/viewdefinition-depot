# Fabric Silver R4 Patient and Observation Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce nested Silver Parquet for Patient and Observation and demonstrate that the results match HDS v1.4.0 after first ingest, replay, and update.

**Architecture:** Run independently usable Ignixa views once per source file. A Python companion writer reads their lossless intermediate rows, applies HDS behavior, and publishes versioned snapshots. A separate comparison against synthetic rows captured from HDS decides coverage; the writer never generates its own expected results.

**Tech stack:** FHIR R4 4.0.1, .NET 10/Ignixa CLI, Python, `unittest`, PyArrow for typed nested Parquet; keep the existing DuckDB starter checks.

**Spec:** [Fabric Silver R4 parity: Patient and Observation first](../specs/2026-11-24-fabric-silver-r4-parity-design.md)

## Global constraints

- Target HDS v1.4.0. The catalog denominator is 146 concrete R4 types. The pilot numerator starts at zero, not two.
- Leave `views/r4/fabric-silver/` usable and do not change its `0.6.82` CLI pin to consume an unreleased PR. Development can use an explicitly selected build; the supported command must eventually use a released pin.
- Output matches logical Silver rows and recursive schema, not Delta log files or identical Parquet bytes. Native arrays/structs and JSON-string counterparts are distinct columns.
- No raw-FHIR fallback in the writer, precomputed IDs from callers, real patient data, vendor source in the repo, or HDS download/credentials required in ordinary CI.
- Work on a follow-up rather than pushing pilot changes to the open starter PR. Standalone tooling and proven extraction can proceed while waiting for reference access; do not declare parity on that basis.

---

## Files and boundaries

| Location | Responsibility |
| --- | --- |
| `examples/r4/fabric-silver-parity/` | Synthetic NDJSON by batch/source, `batches.json` describing ingest events, `reference/` containing only small synthetic expectations and provenance. |
| `contracts/fabric-hds/1.4.0/parity/` | Resource-to-table catalog, independently authored mappings and shared logical types. Keep parity contracts below a subdirectory: the starter checker discovers `*.json` at the version root. |
| `views/r4/fabric-silver-parity/{Patient,Observation}.json` | Generated, reviewable extraction views. Directory contains nothing other than definitions. |
| `fabric_silver/{reference,views,normalize,writer}.py` | Compare independent reference data, generate extraction views, apply field rules, and write/reconcile snapshots, respectively. `fabric_silver/__main__.py` exposes the local writer. |
| `tests/test_fabric_{reference,views,normalize,writer,parity}.py` | Focused unit tests, actual Ignixa/Parquet read-back and the end-to-end reference comparison. |
| `docs/packs/fabric-silver-parity.md` | Reference-capture recipe, executable example, supported versions and coverage. Link from `README.md` without changing the starter pack's claims. |

### Task 1: Build the independent reference and comparator

**Files:** `examples/r4/fabric-silver-parity/{batches.json,input/,reference/}`, `fabric_silver/reference.py`, `tests/test_fabric_reference.py`, `docs/packs/fabric-silver-parity.md`, `requirements-parity.txt`.

**Interfaces:** `schema_digest(schema: pyarrow.Schema) -> str`; `compare_snapshot(actual: Path, expected: Path, schema_sha256: str) -> None`. `actual` is one table's Parquet snapshot and `expected` is a UTF-8 NDJSON file of typed-value expectations. Each record has `values` and, for uncontrolled clock fields only, optional `timeBounds` mapping the field name to inclusive UTC `min` and `max` instants. Check event-to-event time relationships separately. Raise `ValueError` identifying table/field or row count, never a source value.

- [ ] Start with a comparator test: construct a struct containing a list of two codings, change the second item, and require failure. Also reject a changed decimal scale, changed nested nullability, a missing column, and an extra identical row. A different physical order of rows must still pass. Use PyArrow 21.0.0 (supports the CI Python 3.13) for this new tool; leave `requirements-test.txt` unchanged.

  ```python
  expected = pa.schema([pa.field("amount", pa.decimal128(36, 18), nullable=True)])
  wrong = pa.schema([pa.field("amount", pa.decimal128(38, 18), nullable=True)])
  assert schema_digest(expected) != schema_digest(wrong)
  ```

- [ ] Run `python -m unittest discover -s tests -p test_fabric_reference.py -v` before and after implementation. Normalize schema to ordered names, logical types, nullability and array element types before hashing; omit writer-specific metadata. Group by `(msftSourceSystem, idOrig)` **within each resource**, comparing multisets, not dictionaries that discard duplicates. Preserve array order and exact JSON strings. Encode dates, instants and decimals canonically without passing decimals through float. Compare uncontrolled ingestion times with captured bounds and their relationships across batches, not a blanket timestamp exclusion.
- [ ] Create three events: first ingest east and west sources with the same local Patient ID; replay the same east input; then update the east Patient from a new source file with a later source version. Use `https://east.example.test/fhir` and `https://west.example.test/fhir` as logical source systems. Include a relative Patient reference and an absolute one, two distinct identifiers, a name with two given values, two distinct Observation codings, and separate Quantity and string Observations. Include absent optional fields. Use a `logicalPath` per input, distinct from its local path. The reference run and writer consume the same manifest.
- [ ] Capture actual tables from the **unmodified HDS clinical path**, never the depot writer. In a disposable synthetic lakehouse using the v1.4.0 package, use `msft_fhir_ndjson_bronze_ingestion.ipynb` and `msft_bronze_silver_flatten.ipynb` with the pinned configuration. Export Patient and Observation immediately after each event, before applying the next. Record release, notebook/config identities, normalization options, source namespaces, input hashes, time zone, run bounds and schema digests. Keep the raw capture outside Git; check in only the recipe, observations and synthetic expectations. In a local reference run use the same shipped transformations and configuration and demonstrate that the required runtime services are present.
- [ ] Compare the capture schema against the deployed Delta metadata, not an inferred schema from populated rows. Add valid synthetic cases for unexercised reachable fields, including mutually exclusive FHIR choices in separate records. Keep explanations for fields always null under the reference configuration. If HDS cannot be run, deliver the comparator and inputs but record **no verified resources**; do not invent a reference or ship a passing placeholder.
- [ ] Rerun the comparator against the real capture and a deliberately corrupted copy; require success only for the untouched capture. Commit the reference harness and, when available, its independently captured expectations.

### Task 2: Prove lossless extraction with real Ignixa output

**Files:** `fabric_silver/views.py`, `contracts/fabric-hds/1.4.0/parity/{Patient,Observation}.json`, `views/r4/fabric-silver-parity/{Patient,Observation}.json`, `tests/test_fabric_views.py`.

**Interfaces:** `make_view(resource: str, columns: list[dict]) -> dict`; `python -m fabric_silver.views --check` regenerates in memory and fails on stale or extra definitions. The extractor runs one file from the ingestion manifest at a time so its rows retain their source namespace and logical file, even when two sources share an ID.

- [ ] Start the read-back test with the existing CLI pin. Require two complete coding objects, in order, and both identifiers and nested given values. The flat starter's first-LOINC selection is not suitable for this test.

  ```python
  coding_column = {"name": "code_codings", "path": "code.coding",
                   "type": "string", "collection": True}
  actual = json.loads(pq.read_table(output)["code_codings"][0].as_py())
  self.assertEqual(actual, source_observation["code"]["coding"])
  ```

- [ ] Run `python -m unittest discover -s tests -p test_fabric_views.py -v`. Confirm failure is lost data rather than missing input files. Re-run against an explicitly selected development CLI built from [brendankowitz/ignixa-fhir#549](https://github.com/brendankowitz/ignixa-fhir/pull/549), recording the revision used. Do not modify the repo's tool manifest for this experiment.
- [ ] Author the per-resource field mappings and generate `status: "draft"` views with deterministic column order. Use `collection: true` for source-backed objects and read the JSON string once. For a single complex value, test it as a collection of zero or one plus a source-presence column where needed; verify an actual populated struct, extensions, and missing data before using that mapping. For primitive choices, use explicit paths such as `value.ofType(string)`. Test timestamps, partial-date behavior and decimals against the fixture rather than assuming a declared FHIR type yields the right Parquet value.
- [ ] Run `r4 validate` and `r4 run --format parquet` for each resource. Check the expected file exists, the row count and resource IDs agree, and the extracted columns match the original synthetic values. Reject lossy and unsupported mappings explicitly. An empty result, skipped resource, or CLI success exit with failed evaluations is not success.
- [ ] Regenerate and compare both checked-in views with `python -m fabric_silver.views --check`; rerun the focused test. Commit passing extraction cases. Record any upstream blocker by specific path and input rather than marking the resource covered.

### Task 3: Implement Silver shaping and normalization against reference cases

**Files:** `fabric_silver/normalize.py`, `contracts/fabric-hds/1.4.0/parity/{types,Patient,Observation}.json`, `tests/test_fabric_normalize.py`.

**Interfaces:** `target_schema(resource: str) -> pyarrow.Schema`; `shape_row(resource: str, extracted: dict, *, source_system: str, logical_path: str, ingest_time: datetime) -> dict`. Return one candidate, preserving originals separately from normalized fields. Task 4 owns decisions depending on prior state.

- [ ] Write tests using Task 1's expected records, independently of the transformation implementation. For each linked Patient/Observation pair compare normalized IDs/references, original IDs/references and identifier arrays. Test the same local ID under both source systems. Test a source-ID identifier that is already present. Compare complete reference structures, not just the ID leaf.
- [ ] Run `python -m unittest discover -s tests -p test_fabric_normalize.py -v`. Implement the observed v1.4.0 ID, identifier, date, source-reference and provenance rules. Consult the reference capture and the package's `flatten/normalization/` behavior; do not vendor its Python implementation. A value observed only in one capture is not a universal constant. Add identifying/error cases for external and unresolved references rather than substituting a successful lookup.
- [ ] Construct the full target schema from independently maintained shared types and ordered fields. Use the correct `decimal(36,18)`, date, timestamp, native struct/list types and nullability; test exact HDS string companions separately. Apply the same shaping path for each mapping; reject missing source columns, unexpected values, or invalid representations rather than silently dropping them. Validate before Arrow conversion: `from_pylist` with a declared schema alone can silently ignore extra keys or supply nulls for missing ones.

  ```python
  fields = [field.name for field in target_schema("Patient")]
  self.assertEqual(set(shaped), set(fields))
  self.assertEqual(shaped["identifierOrig"], expected["identifierOrig"])
  self.assertEqual(shaped["identifierOrig_string"], expected["identifierOrig_string"])
  ```

- [ ] Run the focused tests and compare both recursive schema digests. Maintain field-path evidence: each reachable mapping must have a populated reference example, including nested members; each always-null exception must name its reference condition. Commit independently passing rule groups; stop on an unexplained mismatch rather than adding a field exemption.

### Task 4: Publish typed snapshots with correct replay/update behavior

**Files:** `fabric_silver/writer.py`, `fabric_silver/__main__.py`, `tests/test_fabric_writer.py`.

**Interfaces:** `apply_batch(manifest: Path, event: str, extracted: Path, state: Path, *, ingest_time: datetime) -> Path` takes **extracted** Parquet tagged by input file, not raw FHIR. It returns the newly published snapshot directory. CLI: `python -m fabric_silver --manifest FILE --event NAME --extracted DIR --state DIR --ingest-time UTC_INSTANT`.

- [ ] Write an event test: apply `initial`, `replay`, and `update` in order and compare both table snapshots after each invocation, not just the last. Check created time, modified time and source file as well as all resource columns. Use the reference to determine whether a replay leaves a row unchanged or changes it. Keep both source systems distinct.
- [ ] In a second test, introduce a malformed complex value in the second resource of an update. Assert the batch fails with a field path but without the fixture value, and the prior **two-table** snapshot is still current. Test calling the next event from a new process to catch in-memory-only state.
- [ ] Run `python -m unittest discover -s tests -p test_fabric_writer.py -v`. Read the previous published snapshot and reproduce the reference's merge behavior; do not pick the last input row by accident. Map each extracted file to its recorded ingress context. Write both resources under a new private snapshot directory, validate read-back, and then atomically replace a small current-snapshot pointer on the same filesystem. The pilot supports a single local writer; cloud transactions are not promised.

  ```python
  table = pa.Table.from_pylist(rows, schema=target_schema(resource))
  pq.write_table(table, staged_file)
  if schema_digest(pq.read_schema(staged_file)) != expected_schema_digest:
      raise ValueError(f"{resource}: staged Parquet schema differs from HDS")
  ```

- [ ] Re-run the event and failure tests. Invoke the CLI once per event using the same state directory. Commit the writer only when its Parquet, rather than just its Python rows, passes the comparisons.

### Task 5: Make the parity claim reproducible and gate catalog growth

**Files:** `tests/test_fabric_parity.py`, `contracts/fabric-hds/1.4.0/parity/catalog.json`, `.github/workflows/check-samples.yml`, `.config/dotnet-tools.json`, `docs/packs/fabric-silver-parity.md`, `README.md`.

**Interfaces:** `python -m unittest discover -s tests -p 'test_fabric_*.py' -v` uses local fixtures and recorded expectations. `python scripts/check-samples.py` continues to check the original two flat views.

- [ ] Establish a released CLI containing the needed behavior by running Task 2's regression against that exact package. Only then update the tool manifest. If the upstream PR is open or no suitable release exists, keep the published pin and the documented development route. Do not turn this gate green by skipping a failed export.
- [ ] Build the catalog from the official R4 list (exclude `Resource`, `DomainResource`), map the two noncanonical HDS directory names explicitly, and check canonical names are unique. Entries without proof remain `not-implemented` or `blocked` with a reason; derive the verified count from passing view, fixture, schema and row checks. A missing reference file or contract must fail a claimed verification.
- [ ] Add the end-to-end test running the real pinned CLI, writer and reference comparison. Remove one coding, alter a reference target, change a nested type and drop an expected snapshot in separate negative tests: each must be caught at the correct boundary. Never contact Fabric from an ordinary CI run.
- [ ] Run `dotnet tool restore`, `python -m pip install -r requirements-test.txt -r requirements-parity.txt`, `python scripts/test_check_samples.py`, `python scripts/check-samples.py`, `python -m fabric_silver.views --check`, and the parity test command above. Document the per-file extraction and three-event writer invocation, what a verified entry means, and how to refresh the independent reference.
- [ ] Publish the pilot as **2/146** only after both resources pass; otherwise name the concrete blocker and actual verified count. Do not create 144 stubs. Commit the release pin, CI and evidence-backed documentation together.
