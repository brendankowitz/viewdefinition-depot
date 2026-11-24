# Fabric Silver R4 parity pilot

**FHIR R4 4.0.1 · HDS v1.4.0 · 0/146 resources verified.** Patient and
Observation are `implemented-unverified`: the recursive schema digests of
the writer's published Parquet match the deployed Delta schemas (51 Patient
fields and 89 Observation fields). The shaping and merge rules are
source-derived and unverified against a capture. The other 144 resources
are `not-implemented`. The [catalog](../../contracts/fabric-hds/1.4.0/parity/catalog.json)
maps all concrete R4 resources to deployed Silver tables. The
[flat starter](fabric-silver.md) and its Ignixa pin are unchanged.

A resource counts as `verified` only when its contract and matching
generated view exist, the released CLI exports the pinned synthetic inputs
losslessly, and the writer's complete recursive schema and keyed rows match
the independent HDS reference after **every** manifest event, including
clock relationships. The test derives the count from those gates; a status
label, a schema match alone, or a development CLI run is not verification.

### Blockers and pending decisions

- **Independent HDS capture:** No disposable synthetic Fabric/HDS runtime is
  available. The local environment lacks PySpark, Delta and Fabric
  `notebookutils`; the release artifacts provide schemas, not observed rows.
- **Released extraction:** The latest released Ignixa CLI, `0.6.82`, drops
  complex collection values. [Ignixa PR #549](https://github.com/brendankowitz/ignixa-fhir/pull/549)
  was merged on 2026-11-24 (`400845482a83f35c77fa7c644bced4f68125f080`),
  but is unreleased. The pin stays unchanged.
- **Licensing:** Redistribution terms for HDS-derived contracts are
  unresolved; resolve them before expanding or distributing derived material.
- **Runtime assumptions:** Confirm exact Spark/Jackson JSON text and UTC
  session behavior against the stock runtime, including `_string` columns
  and timestamp handling.
- **File lineage:** The writer records the manifest's logical path as
  `msftFilePath`. Confirm whether HDS emits that path or a OneLake URI before
  selecting the contract; don't normalize the discrepancy away.

Track evidence and rollout in [viewdefinition-depot#2](https://github.com/brendankowitz/viewdefinition-depot/issues/2).
[ignixa-fhir#550](https://github.com/brendankowitz/ignixa-fhir/issues/550) is
the native Ignixa follow-on.

## Run the offline checks

From the repository root, in a venv:

```sh
python -m pip install -r requirements-test.txt -r requirements-parity.txt
python -m fabric_silver.views --check
python -m unittest discover -s tests -p "test_fabric_*.py" -v
```

The hermetic checks validate catalog invariants, publish snapshots from
hand-built extraction Parquet, and exercise the comparator using temporary
writer-derived expectations and four deliberate corruptions. These are
**harness mechanics only, not HDS evidence**. The checked-in
[deployment fingerprints](../../examples/r4/fabric-silver-parity/reference/deployment.json)
were computed from the release artifacts; none are hashes inferred from
populated rows. There are deliberately no expectation NDJSON files yet.

## Check the lossless extraction views

The views in `views/r4/fabric-silver-parity/` are generated from
`contracts/fabric-hds/1.4.0/parity/` (which also maps every deployed Silver
column to extracted, derived, constant or Fabric-assigned, with its logical
type from `parity/types.json`). Date, dateTime and instant fields are
extracted as source strings (lossless) and parsed by the shaping step below.

They need a CLI that writes complex `collection: true` columns as JSON text
(Ignixa PR #549). The extraction, writer, and CLI-to-comparator tests use
the development build when it exists at the default path. They skip only
when no explicit selection was made and that DLL is absent, with a reason
visible in verbose CI output. This skip is permitted only because no
released CLI can export these views yet. The CLI-to-comparator test checks
both tables across all three events and rejects a corrupted expected
reference target. Its expectations come from the local writer:
**harness mechanics only, not HDS evidence**. Set `IGNIXA_PARITY_CLI` to
the DLL path to run all three checks. An invalid explicit path or failed
export fails the run rather than skipping. The extraction suite alone also
accepts `pinned` to demonstrate the loss with `0.6.82`.

**Upstream blocker, pinned 0.6.82.** Complex `collection: true` columns come
back as one `null` per element, so content is lost (count is kept). Observed
by reading Parquet back against the source:

| Input | Lost columns |
| --- | --- |
| `input/east/initial/Observation.ndjson` (`o-quantity`) | `code` (incl. `Observation.code.coding` = `[null, null]` for its two codings), `meta`, `subject`, `valueQuantity` |
| `input/west/initial/Observation.ndjson` | `code`, `effectivePeriod`, `meta`, `subject` |
| `input/east/initial/Patient.ndjson`, `input/east/update/Patient.ndjson` | `extension`, `identifier`, `meta`, `name` |
| `input/west/initial/Patient.ndjson` | `meta` |

The supported command therefore needs a released CLI containing PR #549; the
extraction was verified only with a development build at revision
`40851de69a4f4cac84703050e87baa50221c4efc`. Neither Patient nor Observation is
covered or verified.

For development, build `tools/Ignixa.SqlOnFhir.Cli/Ignixa.SqlOnFhir.Cli.csproj`
from that revision in a separate Ignixa checkout with the .NET 10 SDK.
Use the resulting `bin/Debug/net10.0/ignixa-sqlonfhir.dll`. The tests also
discover it automatically if the checkout is at
`.superpowers/sdd/2026-11-24-fabric-silver-r4-parity/ignixa-549`.

```sh
python -m fabric_silver.views --check
python -m unittest discover -s tests -p test_fabric_views.py -v
```

This is extraction evidence only. It does not verify either resource.

## Shape extracted rows (source-derived, unverified)

`fabric_silver.normalize.target_schema(resource)` builds the Silver schema
from the contracts; a test requires it to equal the deployed Delta schema
digest. `shape_row(resource, extracted, source_system=..., logical_path=...,
ingest_time=...)` turns one extraction row into a Silver row: it parses the
JSON columns and date strings (partial dates become midnight UTC, offsets
become UTC), hashes the resource ID, adds the `fhirId` identifier, normalizes
references without dereferencing them, adds the `*Orig` columns and builds the
`_string` companions. The `limitations` text in each contract lists which
columns keep pre-normalization values and the known divergences. For example,
unknown JSON fields are rejected rather than dropped.

A field-path gate walks every leaf of both deployed schemas. Each leaf needs
a populated, hand-derived example, or a named HDS condition that keeps it
null. Only two such conditions exist. First, a normalized Reference's
`.reference` is always moved to `msftSourceReference`. Second, its
`.identifier.period` members stay null because HDS stores that period as a
string. Shaping errors name the resource, the mapping path and a fixed
reason. They never include source values. A mutation test replaces each
witness leaf and container with wrong-typed, non-finite, deeply nested and
unencodable values. Each result must be a valid row or such an error.
Inputs that would fail an HDS job are rejected rather than guessed. Examples
are NaN, lone surrogates and out-of-range identifier periods.

These rules were written from reading the HDS normalization source. No HDS
run has been captured, so every expected value in
`tests/test_fabric_normalize.py` is a hand-derived example, not observed
output. **Neither resource is verified.** This step needs only PyArrow:

```sh
python -m unittest discover -s tests -p test_fabric_normalize.py -v
```

## Publish a local pilot snapshot

Use a lossless Ignixa development build (see above) to run the matching
parity view separately on each manifest input. Put each output in a dedicated
directory as `<inputId>.parquet`, where `inputId` is the key under `inputs`
in `batches.json` (for example, `east-patient-1.parquet`). An event reads only
its listed files, in order; files listed elsewhere in the manifest may be
present or absent. Unknown files are rejected. The writer never reads the raw
NDJSON: the manifest provides the resource, source system and logical path.

From the repository root, using a new extraction directory and new state
directory (PowerShell example; set the DLL path first):

```powershell
$env:IGNIXA_PARITY_CLI = '<absolute path to ignixa-sqlonfhir.dll>'
$pack = 'examples/r4/fabric-silver-parity'
$manifest = "$pack/batches.json"
$inputs = (Get-Content $manifest -Raw | ConvertFrom-Json).inputs
New-Item -ItemType Directory extracted -ErrorAction Stop | Out-Null
foreach ($inputFile in $inputs.PSObject.Properties) {
    $item = $inputFile.Value
    $source = "$pack/$($item.path)"
    if ((Get-FileHash $source -Algorithm SHA256).Hash -ne $item.sha256) {
        throw "Fixture hash differs: $($inputFile.Name)"
    }
    $view = "views/r4/fabric-silver-parity/$($item.resource).json"
    $out = "extracted/$($inputFile.Name).parquet"
    dotnet $env:IGNIXA_PARITY_CLI r4 run --views $view --input $source --out $out --format parquet
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $out -PathType Leaf)) {
        throw "Extraction failed: $($inputFile.Name)"
    }
}

python -m fabric_silver --manifest $manifest --event initial --extracted extracted --state silver-state --ingest-time 2026-02-01T10:00:00Z
if ($LASTEXITCODE -ne 0) { throw 'Initial ingest failed' }
python -m fabric_silver --manifest $manifest --event replay --extracted extracted --state silver-state --ingest-time 2026-02-01T11:00:00Z
if ($LASTEXITCODE -ne 0) { throw 'Replay failed' }
python -m fabric_silver --manifest $manifest --event update --extracted extracted --state silver-state --ingest-time 2026-02-01T12:00:00Z
if ($LASTEXITCODE -ne 0) { throw 'Update failed' }
```

File existence does not prove lossless extraction; run the read-back tests
with that DLL first. Replay reuses the initial east extraction files. The
three writer invocations share one state directory and take explicit UTC
instants (`Z` or `+00:00`, up to six fractional digits). Each prints the
published directory. Resolve `silver-state/CURRENT` once per
read: it names a directory under `silver-state/snapshots/`, such as
`00000001-initial`, containing both `Patient.parquet` and
`Observation.parquet`, even when one table is empty. The writer sorts rows
by normalized ID, validates both staged files by reading them back, then
atomically replaces `CURRENT`. It retains old snapshots. This is for one
writer on a local filesystem, not a Delta/cloud transaction, multiwriter
protocol or power-loss durability guarantee.

The merge is **source-derived, unverified against a captured run**
(`utils/dataframe_utils.py`, `prepare_source_data` and
`upsert_unique_to_delta_managed`). The newest version per normalized ID is
eligible; an equal or older version leaves the stored row untouched. A newer
version preserves the created time and replaces the other values, including
modified time and file lineage. The pilot deliberately fails the whole batch
on a null `meta.lastUpdated` instead of silently skipping it, or on a duplicate
ID/timestamp pair in the same batch instead of choosing an unspecified winner.

`python -m unittest discover -s tests -p test_fabric_writer.py -v` checks both
deployed schema digests after publication, exercises the source-derived event
rules across process restarts, and tests lossless Ignixa input if the
development DLL is available (`IGNIXA_PARITY_CLI` may name that DLL). These
checks do not compare row values against a Fabric capture.

## Inputs shared by the two runs

[`batches.json`](../../examples/r4/fabric-silver-parity/batches.json) is the
ingress contract. Resolve `inputs[key].path` relative to it. Verify SHA-256
over the file bytes **before staging and after copying**; don't reserialize
NDJSON or change line endings. Use the input key, not the basename or FHIR
ID, to associate per-file extraction with `sourceSystem` and `logicalPath`.
The logical path names the staged file, not the developer's checkout. The
clinical notebooks ingest the raw synthetic files; the local writer
consumes only extracted data plus ingress context, never raw FHIR.

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

To refresh the reference, rerun the preceding operator recipe on a fresh
target with the pinned release and all three events; replace the six
expectations and their safe run provenance together. Never refresh
expectations from local writer output.

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

       Finally compare the **local writer's** three published directories against
       those same NDJSON files. Use the released CLI containing #549, not the
       development route. For each writer event use the recorded controllable
       clock or a deterministic UTC instant inside the capture bounds. Update
       the parity gate's event clocks with the chosen instants when checking in
       the capture; the example clocks above are for the local pilot only.

   Then on a **private copy**, duplicate one Observation row without
   changing its schema (e.g. `pa.concat_tables([t, t.slice(0, 1)])` and
   `pq.write_table`). Rerun with only that actual file replaced. Require
   `ValueError` for `Observation` row count and success again for the
   untouched file. Do not regenerate expectations from either copy or from
   the depot writer. These real-capture checks and any claim about HDS
   replay/update behavior are **pending**, not skipped tests counted as passing.
