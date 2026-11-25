# Fabric HDS v1.4.0: R4 projection catalog

The [catalog](../../contracts/fabric-hds/1.4.0/r4/catalog.json) pairs every concrete
FHIR R4 resource with a [ViewDefinition](../../views/r4/fabric-silver-hds/) and
a [contract](../../contracts/fabric-hds/1.4.0/r4/resources/) describing its
deployed Silver target. The view reads the **FHIR resource**, not the Silver
table. Coverage is **146/146 contracts and views**, not 146 tested HDS
loaders. There is no independent HDS row capture: **0/146 independently
verified**. Non-FHIR enrichment tables and abstract resource types are excluded.
Use the catalog's `table` rather than guessing a table name from `resource`;
the HDS release spells two of the MedicinalProduct table names differently.

## One projection, different destinations

| Destination | How to get there |
| --- | --- |
| Parquet file | Run the ViewDefinition with Ignixa. The CLI also supports CSV and NDJSON in batch mode. |
| **Custom** Fabric Lakehouse table | Stage the projection Parquet under Lakehouse **Files**, then load/register its rows as a **Delta table**. There is no direct Fabric-table sink in the pinned Ignixa SQL-on-FHIR CLI. |
| **Predefined HDS Silver** table | Not a sink for these results. The [native Ignixa writer project](https://github.com/brendankowitz/ignixa-fhir/issues/550) targets original FHIR, source namespace/file lineage, and deployed table metadata. Its initial acceptance pair is Patient and Observation, not all 146 types. |

For example, from the repository root in PowerShell:

```powershell
dotnet tool restore
dotnet tool run ignixa-sqlonfhir -- r4 validate --views views/r4/fabric-silver-hds
dotnet tool run ignixa-sqlonfhir -- r4 run --views views/r4/fabric-silver-hds/Patient.json --input examples/r4/fabric-silver-hds/input/east/initial/Patient.ndjson --out $env:TEMP\PatientProjection.parquet --format parquet
```

**Collection limitation:** the pinned CLI (`0.6.82`) writes `null` for every
complex-typed column even when the export succeeds. The generated views
export every complex element, including single-valued ones such as
`Observation.code`, as a JSON array (`[{…}]`), so all of them are affected
(the pinned CLI yields `[null]`). Don't use those
files when populated complex values matter. Use an Ignixa build containing
[the collection fix](https://github.com/brendankowitz/ignixa-fhir/pull/549);
replace `dotnet tool run ignixa-sqlonfhir --` above with
`dotnet $env:IGNIXA_PARITY_CLI` pointing to that DLL. A string column carrying
an ordered JSON array is still a string column, not a nested Delta array.
The [starter pack](fabric-silver.md) remains an alternative for selected
fields tested with the pinned release.

These are analytical projections, not a lossless copy of each FHIR
resource. For example, extensions and ids on primitive values (such as
`_birthDate`) are not exported at all. The pinned CLI can also round
wide scalar decimals in `ChargeItem.factorOverride`, `Media.duration`, and
`TestReport.score` before writing Parquet. In a `ChargeItem` export,
`123456789012345678.123456789012345678` became
`123456789012345678.123456789010000000`. Retain original FHIR when you
need that precision or need to reconstruct a resource.

After uploading the resulting file to `Files/projections/` in an attached
Fabric Lakehouse, this PySpark notebook statement creates a **new custom**
table with the Parquet columns. It errors rather than replacing an existing
table:

```python
(spark.read.parquet("Files/projections/PatientProjection.parquet")
      .write.format("delta").mode("errorifexists").saveAsTable("PatientProjection"))
```

This reads the same projection; it does **not** perform HDS identity
normalization, source-lineage assignment, or timestamp-based merges.
Dropping Parquet files directly into an existing Delta table's directory is
not a substitute for a Delta transaction.

## What is checked

```powershell
dotnet tool restore
python -m pip install -r requirements-test.txt -r requirements-parity.txt
python -m fabric_silver.build_contracts --check
python -m fabric_silver.views --check
python -m unittest discover -s tests -v
```

Neither `--check` needs the licensed download. The first re-expands the
logical types and checks the release fingerprints and generated contracts;
the second rejects missing, extra or stale views and mismatched catalog
identities. The tests run the released CLI over all 146 views using a
synthetic resource of each type and read the Parquet columns, ID and language back.
That checks execution and row shape, not populated-field fidelity for every
type. With `IGNIXA_PARITY_CLI` set to a collection-capable development DLL,
additional tests compare populated Patient, Observation, Condition, Encounter
and MedicationRequest fields with the input FHIR. A missing DLL explicitly
selected by the caller is an error, not a skip.

## Reading and refreshing contracts

Each [resource contract](../../contracts/fabric-hds/1.4.0/r4/resources/Patient.json)
has two ordered lists. `deployment.fields` describes the **target table**
as `[name, nullable, type]`. A type starting with `@` refers to a
[shared type](../../contracts/fabric-hds/1.4.0/r4/types.json): expand `struct`
fields and `array` element types recursively, retaining order and nullability.
`projection.columns` describes the **source view**, including FHIRPath,
cardinality, choice type and related target fields. Read `projection.targetMappings`
for the relationship in the opposite direction. For example, `Patient.id`
is an input to both `id` and `idOrig`; projecting a local `id` does not create
the normalized Silver identity. `unsupported` is explicit, not an empty
value to write to Silver. Across the catalog, 3,704 source projection
columns relate to 7,410 top-level target fields; **131 target fields have
no supported mapping**. For example, a `ConceptMap.sourceCanonical` URI
is not a Silver timestamp just because its deployed target has that type.
The interchange format is [version 1](../../contracts/fabric-hds/1.4.0/r4/format.schema.json).

To regenerate from upstream, download the [official HL7 R4 core package](https://hl7.org/fhir/R4/hl7.fhir.r4.core.tgz)
and unpack a licensed HDS v1.4.0 release **outside the repository**. Set
`HDS_SOURCE_DIR` to its `HDS.SourceCode` root and `FHIR_R4_PACKAGE` to the
HL7 `.tgz` file. From the repo root:

```powershell
python -m fabric_silver.build_contracts --check --hds-source "$env:HDS_SOURCE_DIR" --fhir-package "$env:FHIR_R4_PACKAGE"
python -m fabric_silver.build_contracts --write --hds-source "$env:HDS_SOURCE_DIR" --fhir-package "$env:FHIR_R4_PACKAGE"
python -m fabric_silver.views --write
```

For the exact deployed `schemaString`, `protocol` and `configuration`,
run `python -m fabric_silver.build_contracts --materialize-local --hds-source "$env:HDS_SOURCE_DIR"`.
It checks the source against the catalog and writes **only local** release
metadata under `.hds-local/1.4.0/r4/resources/` (ignored by Git). Neither
the ViewDefinitions nor the public logical schemas are a replacement for
that metadata.

## Comparing independent Silver output

The [synthetic batch manifest](../../examples/r4/fabric-silver-hds/batches.json)
is available for independent comparisons. Its initial event contains east
and west Patients with the same local ID; replay reuses the east files, and
the update supplies a new east Patient file. The committed deployment hashes
are schema measurements, **not captured rows**. An operator comparing
Silver behavior must run those events on a disposable synthetic HDS
deployment with the original configuration, keep the checkpoints between
events, and freeze both committed Delta table versions after each event.
For each input, record the source path segment you supplied to stock HDS
(`sourceToken`) separately
from the emitted `msftSourceSystem` (`sourceSystem`) and `msftFilePath`
(`observedFilePath`). Preserve a credential-free synthetic OneLake URI
verbatim if that's what HDS emits. In `batches.json`, `logicalPath` and
`callerNamespace` describe caller inputs and map to the capture fields of the
same names; never copy them into the capture's `sourceSystem` or
`observedFilePath`. Include partial dates, source identifiers
and assigners, relative and external references, tied versions, and exact
`_string` text in capture cases rather than inferring them from schemas.

Use [capture format 1](../../contracts/fabric-hds/1.4.0/r4/capture.schema.json)
for the manifest and its `#/$defs/rowRecord` definition for each expected
NDJSON row. Put **every** deployed column in `values`, including nulls.
Keep structs as objects and arrays in their observed order. Encode decimals
as fixed-point strings with their scale (e.g. `"1.500000000000000000"`),
dates as `YYYY-MM-DD`, and timestamps as UTC ISO text with `Z` and six
fractional digits when microseconds are nonzero (and no fractional part when
they are zero, e.g. `2025-01-01T00:00:00Z`). Preserve `_string` values
verbatim rather than parsing and reserializing their JSON. Only the two
ingestion clocks may have inclusive `timeBounds`; their observed values
still belong in `values`.

Paths to the synthetic inputs and expected rows are relative
to the manifest; supply candidate Parquet separately as
`<candidate-dir>/<event>/<deployed-table>.parquet`. For example:

```powershell
python -m fabric_silver.capture .\capture\capture.json .\candidate --report .\comparison.json
```

This compares the **declared snapshots only**, including order within arrays
and exact `_string` values. A table's Delta version cannot decrease; a replay
at the same version must have the same rows, regardless of row order or
Parquet encoding. A match does not authenticate the capture's origin,
certify Delta merge/CDF, or establish parity for any other data or table.

## Licensing and verification boundary

The public contracts describe schema and mapping facts derived from the
[HDS v1.4.0 release](https://www.microsoft.com/en-us/download/details.aspx?id=108754).
The repository's MIT license does not relicense Microsoft's artifacts.
Redistribution of HDS-derived material remains under review; see the
[Microsoft terms](https://go.microsoft.com/fwlink/?LinkId=2369925). This
catalog does not ship the HDS implementation or raw deployment logs. Use
your own licensed release for exact vendor metadata; do not embed these
contracts into Ignixa as a substitute for licensing clearance.
