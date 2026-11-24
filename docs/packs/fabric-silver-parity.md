# Capturing an independent Fabric Silver reference

**FHIR R4 4.0.1 · HDS v1.4.0 · 0/146 resources verified.** Patient and
Observation have deployment schemas, not verified Silver rows. No designated
disposable synthetic Fabric target or independent row capture is available;
the local runtime lacks PySpark, Delta and Fabric `notebookutils`. This pack
contains inputs and an offline comparator, **not a Silver writer**. The
[flat starter](fabric-silver.md) and its Ignixa pin are unchanged.

## Run the offline checks

From the repository root, in a venv:

```sh
python -m pip install -r requirements-parity.txt
python -m unittest discover -s tests -p test_fabric_reference.py -v
```

These tests create temporary, deliberately small Parquet tables and corrupt
them. They do not use HDS and do not establish parity. The checked-in
[deployment fingerprints](../../examples/r4/fabric-silver-parity/reference/deployment.json)
were computed from the release artifacts; none are hashes inferred from
populated rows. There are deliberately no expectation NDJSON files yet.

## Check the lossless extraction views

The views in `views/r4/fabric-silver-parity/` are generated from
`contracts/fabric-hds/1.4.0/parity/` (which also maps every deployed Silver
column to extracted, derived or Fabric-assigned). They need a CLI that writes
complex `collection: true` columns as JSON text (Ignixa PR #549); the pinned
0.6.82 emits `null` elements instead. Extraction tests are skipped, saying so,
unless a development build exists or `IGNIXA_PARITY_CLI` names one.

```sh
python -m fabric_silver.views --check
python -m unittest discover -s tests -p test_fabric_views.py -v
```

This is extraction evidence only. It does not verify either resource.

## Inputs shared by the two runs

[`batches.json`](../../examples/r4/fabric-silver-parity/batches.json) is the
ingress contract. Resolve `inputs[key].path` relative to it. Verify SHA-256
over the file bytes **before staging and after copying**; don't reserialize
NDJSON or change line endings. Use the input key, not the basename or FHIR
ID, to associate per-file extraction with `sourceSystem` and `logicalPath`.
The logical path names the staged file, not the developer's checkout. The
clinical notebooks ingest the raw synthetic files; the future writer must
consume only extracted data plus ingress context, never reread raw FHIR.

| Event | Stage before running the notebooks | What this distinguishes |
| --- | --- | --- |
| `initial` | Both sources' Patient and Observation files | Same local Patient ID; one relative and one absolute Patient reference. East has two identifiers, two given names, two ordered codings and a decimal Quantity. West lacks identifiers and names and has a separate string Observation. |
| `replay` | Identical east files, **same** logical paths, hashes and namespace | File/checkpoint recognition vs. an insert or merge. Do not reset checkpoints or rename the files to force an outcome. |
| `update` | Only the new east Patient file | Later source version/time, changed name and active flag; west and both Observations remain in scope. |

## Reference run (operator only; never CI)

1. **Establish the target.** Use an explicitly designated, disposable,
   synthetic-only deployment of the [v1.4.0 release](https://www.microsoft.com/en-us/download/details.aspx?id=108754).
   Start from empty Bronze/Silver tables and fresh checkpoints **once**.
   Record the release/archive digest and runtime versions. Verify the
   packaged notebook, clinical pipeline, config and Avro byte hashes against
   `reference/deployment.json` (config paths are relative to `HDS.SourceCode`).
   Use the stock clinical parameters for Bronze `ClinicalFhir` and the
   shipped `fhir4/transformation/flatten/flatten_config.json` for Silver.
   Record safe fingerprints of the **effective** notebook parameters,
   resolved config and per-resource ID/identifier, reference and date
   normalization settings/defaults. No custom transforms or post-ingest
   fixes. Use UTC for Spark and the capture clock. A local substitute must
   demonstrate the *same* package, working Spark/Delta reads and writes,
   Fabric notebook utilities, mounted configuration, ingress and persistent
   checkpoint services; an import, schema download or hand-built DataFrame
   is not a reference run.
2. **Establish ingress identity before claiming any evidence.** Stage each
   input at its declared `Files/Process/Clinical/FHIR-NDJSON/...` path in the
   synthetic Bronze store. Record the association of the staged file and
   namespace with `https://east.example.test/fhir` or
   `https://west.example.test/fhir` exactly as in the manifest. The `east`
   and `west` directory tokens are not those URIs; `meta.source` is source
   data, not proof of HDS namespace assignment. Verify the deployment's
   supported namespace binding in Bronze and the actual `msftSourceSystem`
   and `msftFilePath` in Silver. Preserve exact emitted values and record
   the mapping. If the stock configuration cannot realize the required
   source identities, stop and report that fact. Do not patch Bronze, alias
   fields in the export, strip paths, or treat the two sources as one.
3. **Run one event at a time.** For the event's staged files, record an
   inclusive UTC lower bound immediately before invoking the packaged
   `msft_fhir_ndjson_bronze_ingestion.ipynb`, wait for successful completion,
   then run `msft_bronze_silver_flatten.ipynb` and wait for its streams and
   Delta commits to finish. Record the upper bound immediately after.
   Keep identical normalization settings, namespaces, tables and checkpoints
   throughout. Inspect failures, rejected files and stream/checkpoint
   progress; a successful notebook exit alone is insufficient. On replay,
   distinguish "no newly processed file" from "file ingested but row unchanged."
4. **Freeze both tables before staging the next event.** Record each table's
   current Delta version, full deployed schema, row count, and a checksum of
   its exported Parquet file. Read the committed **table**, not its loose
   partition files. For example, in the authorized export session, with
   `table_path` pointing to the Silver table and `private_out` a new,
   off-repository directory unique to this event and table:

   ```python
   from delta.tables import DeltaTable

   spark.conf.set("spark.sql.session.timeZone", "UTC")
   spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
   version = DeltaTable.forPath(spark, table_path).history(1).first()["version"]
   snapshot = spark.read.format("delta").option("versionAsOf", version).load(table_path)
   snapshot.coalesce(1).write.mode("errorifexists").parquet(private_out)
   ```

   Keep the single data part as `<private>/<event>/<Patient|Observation>.parquet`.
   Preserve every column, null, duplicate and array element. No casts,
   filtering, deduplication or JSON conversion on export. Keep raw captures,
   any identifying run details and all credentials outside Git.

## Make and check the six expectations

1. For each resource, read `metaData.schemaString` from **its** deployment
   log `<deltaLogRoot>/<table>/_delta_log/00000000000000000000.json`.
   `schema_digest(deployed_schema(Path(log)))` must equal the corresponding
   `tables[table].schemaSha256`. Check the live Delta metadata at each
   recorded version against that deployment schema as well. The comparator
   checks the Parquet schema against this same independent digest, including
   ordered struct fields, nullability, array element types and `containsNull`,
   decimal precision/scale and microsecond UTC instants. Writer metadata
   and Parquet's `item` vs. `element` list label do not participate. If
   export changed a type or nullability, resolve the discrepancy, don't
   fingerprint the export as the target.
2. **Only from those validated HDS captures**, obtain
   `snapshot_values(Path(parquet), digest)` from `fabric_silver.reference`.
   Write one UTF-8 JSON object per returned row to
   `reference/<event>/<table>.ndjson`, retaining duplicates. `values` is
   the entire row, including bounded clock values: JSON null/booleans/
   integers, arrays in order, objects, full-scale decimal strings, ISO dates,
   and UTC instants with `Z` (six fractional digits when nonzero). A Silver
   `_string` field stays the **exact string**, including whitespace; don't
   parse and re-emit it. For an uncontrolled non-null ingestion clock only,
   add `timeBounds[field] = {"min": lower, "max": upper}`. Use the run
   bounds of the event that **assigned** that value, not the current event
   for retained values. Only `msftCreatedDatetime` and `msftModifiedDatetime`
   accept bounds. Controlled clocks and source timestamps compare exactly.
3. Save the safe run observations with the expectations: input hashes,
   namespace/file lineage, release/notebook/config identities and options,
   UTC/run bounds, per-event versions/counts/schema digests, and observed
   creation/modification ties or changes. Audit coverage down to nested
   field paths. For each reachable but unpopulated field, add a valid
   synthetic witness (mutually exclusive FHIR choices in separate records)
   and repeat **all three** events on a fresh target. For an always-null
   field record the pinned rule, condition and corroborating observation;
   an unexplained null isn't coverage. Never check in private paths, vendor
   source or unsanitized logs. If an exact row would expose private data,
   withhold the artifact; don't silently redact an expected value.
4. From the repository root, with `private` the local directory of exported
   files, run the following for the untouched captures. `compare_events`
   calls the same per-snapshot checks as `compare_snapshot` and separately
   compares the observed equality/order of both clocks across events for
   each `(msftSourceSystem, idOrig)` within the resource.

   ```python
   import json
   from pathlib import Path
   from fabric_silver.reference import compare_events

   ref = Path("examples/r4/fabric-silver-parity/reference")
   tables = json.loads((ref / "deployment.json").read_text(encoding="utf-8"))["tables"]
   for table, identity in tables.items():
       pairs = [(private / event / f"{table}.parquet", ref / event / f"{table}.ndjson")
                for event in ("initial", "replay", "update")]
       compare_events(pairs, identity["schemaSha256"])
   ```

   Then on a **private copy**, duplicate one Observation row without
   changing its schema (e.g. `pa.concat_tables([t, t.slice(0, 1)])` and
   `pq.write_table`). Rerun with only that actual file replaced. Require
   `ValueError` for `Observation` row count and success again for the
   untouched file. Do not regenerate expectations from either copy or from
   the depot writer. These real-capture checks and any claim about HDS
   replay/update behavior are **pending**, not skipped tests counted as passing.
