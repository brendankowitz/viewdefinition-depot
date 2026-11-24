# Fabric Silver R4 parity: Patient and Observation first

**Target:** FHIR R4 4.0.1, Fabric healthcare data solutions (HDS) v1.4.0.
**Decision:** Keep the SQL-on-FHIR ViewDefinitions independently runnable with
Ignixa. Build a companion Silver writer in this repository. The output to
compare with HDS is the writer's Parquet, not Ignixa's intermediate Parquet.

## What counts as coverage

The [R4 resource type list](https://hl7.org/fhir/R4/valueset-resource-types.html)
has 146 concrete types after excluding `Resource` and `DomainResource`. The
v1.4.0 download has a deployed Silver target for each. Use canonical FHIR
names in definitions and explicit target names in the catalog: the shipped
tables for `MedicinalProductContraindication` and `MedicinalProductIndication`
are `MedicinalProductContraindiction` and `MedicinalProductIndictation`.
Other Silver tables, such as enrichment tables, are not R4 resources.

A resource counts as covered only when its complete nested output schema and
its behavior on populated synthetic inputs match the pinned HDS reference.
An ID-only view, a schema filled with nulls, or JSON strings in place of
native structs do not qualify. The existing [starter pack](../../packs/fabric-silver.md)
remains useful for flat exports; neither starter is a parity implementation.

The first deliverable covers only Patient (51 top-level target fields) and
Observation (89). Prove the shared extraction, normalization, and writing
rules with these linked resources before expanding to the other 144. The
target is the logical Silver table contents after each ingestion, exported as
Parquet. Reproducing Delta transaction files, Spark, Fabric orchestration, or
the separate FhirToDataLake format is not part of this design.

## How a batch becomes Silver rows

1. **Extract.** Ship one parity ViewDefinition per resource in
   `views/r4/fabric-silver-parity/`, separate from the starter pack. Each
   extracts source values, one row per source resource. Verify extraction on
   its own so that adding a writer cannot hide a lossy view. For repeated
   objects, use ordered JSON arrays as transport, not as the destination
   type. An input field that Ignixa cannot transport faithfully blocks that
   mapping; the writer must not silently reread the FHIR payload or accept
   a truncated projection.
2. **Apply HDS rules.** The companion writer takes the extracted rows, their
   input provenance (source namespace and file), a run clock, and the prior
   Silver state. It computes Fabric-assigned values, including normalized
   IDs, reference structures, original-value fields, and ingestion metadata.
   The caller does not supply precomputed IDs or a lookup answer. Keep input
   provenance associated with the correct row even if local IDs collide
   across sources. Use the pinned HDS configuration and observed results for
   supported reference forms and replay/update behavior; do not invent an
   identifier algorithm, perform network dereferencing, or assume each replay
   is an append.
3. **Write.** Assemble typed arrays and structs and the separate `_string`
   fields where the contract calls for both. For example, Patient has an
   `identifierOrig` array **and** `identifierOrig_string`; Observation has a
   `code` struct **and** `code_string`. Preserve array order and distinguish
   null from empty according to HDS, not the intermediate encoding. Publish
   the new table snapshots only after successful conversion and validation;
   a failed batch must leave the prior snapshot usable. Identify errors by
   resource, mapping path, and failure reason without logging source values.

Generate common mappings from the versioned field contracts and use explicit,
reviewed exceptions for transformations that are not inferable from a schema.
Check the runnable definitions into the catalog so users do not need the HDS
download or the generator to try them. Pure hand authoring would duplicate
the same mappings across 146 types; adding a Fabric-specific writer inside
Ignixa would couple a general FHIR export tool to one vendor release.

## What the Ignixa collection change unlocks

[brendankowitz/ignixa-fhir#549](https://github.com/brendankowitz/ignixa-fhir/pull/549)
proposes exporting source-backed complex collections as JSON text in one Parquet string
cell. Its Patient regression reads the identifiers and names back from the
actual file. It does not promise native nested columns or lossless scalar
complex projections. Test `Observation.code.coding` with more than one coding
in this repo; do not infer coverage from the Patient test. Capture additional
gaps separately, including single complex values or absent-versus-empty
values that a proposed extraction cannot distinguish.

Do not change the starter's `0.6.82` pin to test an unreleased PR. A build
of the upstream change can be used for development. Before advertising a
working parity pack, pin a released CLI and run both the starter and the
parity export checks against it.

## Independent reference and acceptance

Use the [official HDS v1.4.0 release](https://www.microsoft.com/en-us/download/details.aspx?id=108754).
Read the deployed schema from the `metaData.schemaString` entry in each
`Silver/<table>/_delta_log/00000000000000000000.json`; use the R4 source
Avro schemas as a cross-check. Schema metadata describes shape, not values.
Run the same synthetic input sequence through the v1.4.0 clinical ingestion
path and capture the actual Silver rows after each batch. Record release,
enabled normalization options, source identities, file paths, time zone,
and the observation time of each run. Supply the same clock inputs where
controllable; otherwise check timestamp bounds and create/modify
relationships against the reference, rather than ignoring those columns.
Keep a reproducible capture recipe and small, independently captured
synthetic expectations. Do not redistribute the HDS source or depend on a
developer's download path in CI. Without reference results the pilot is
unverified, even if it matches the schema.

| Case | Evidence required from the two outputs |
| --- | --- |
| Patient with two identifiers, multiple given names and a birth date | Original and normalized identifiers, nested names and string counterparts, array order, `birthDateOrig` as a date and `birthDate` as a timestamp. Include absent collections in a second record. |
| Observation referencing that Patient, with two codings and alternative `value[x]` inputs | All coding items survive, source and normalized references match HDS, the active choice has the right nested type and value, inactive branches match the reference, and Quantity precision/scale match. |
| Same local resource ID in two source systems; relative and absolute references | Original IDs, normalized IDs and reference targets match the appropriate HDS rows, not an accidental join on local ID alone. |
| First ingest, identical replay, then an update from a new file | Compare keyed row counts and values after **each** batch, including file lineage and created/modified times. Exercise both preservation and change, as observed in HDS. |

Compare the complete recursive logical schema, including field names, order,
types, decimal precision/scale, nullability and array element types. Compare
keyed rows, not file bytes or file order. Check JSON-text columns as strings
against HDS as well as decoding them to diagnose lost values. Record a source
or derivation for every target field and exercise each reachable field with
a populated value. If a target field is always null in the pinned configuration,
document the HDS rule and reference case that establish that behavior. An
unexplained null, type, value, or row-count difference keeps the resource out
of the verified count.

## Rollout

The first milestone is a reference-backed Patient/Observation comparison, not
146 generated files. Once both pass, expand in groups that share mapping
rules, retaining resource-specific fixtures and checks. For every counted
resource require a canonical name and deployed table mapping, a runnable
definition, and the same schema and value comparison. Report the numerator
against 146 and list blocked resources with concrete reasons.
