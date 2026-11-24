---
title: Fabric Silver R4 sample pack
sidebar_position: 1
description: Selected Patient and Observation Parquet projections for Fabric healthcare data solutions v1.4.0.
---

# Fabric Silver R4 sample pack

This pack targets selected fields in the R4 Silver healthcare data model in Fabric healthcare data solutions (HDS) v1.4.0. It is not a complete FHIR export or a Delta loader. For context, see Microsoft's [data architecture](https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/data-architecture-and-management), [clinical ingestion documentation](https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/healthcare-data-foundations), and [v1.4.0 download](https://www.microsoft.com/en-us/download/details.aspx?id=108754).

Bronze stores ingested clinical data in the `ClinicalFhir` staging table; Silver organizes transformed data into resource-specific healthcare tables. This pack produces selected Parquet projections for that Silver target, not drop-in Silver Delta tables.

## Provenance

The official HDS archive contains the R4 source schemas:

- `HDS.SourceCode/src/config/fhir4/schema/silver/patient.avsc`
- `HDS.SourceCode/src/config/fhir4/schema/silver/observation.avsc`

The selected target field snapshots in [`contracts/fabric-hds/1.4.0`](../../contracts/fabric-hds/1.4.0/) were sourced from the archive's deployed Delta schema logs:

- `HDS.SourceCode/hds-build-artifacts/healthcare-tables/1.4.0/Silver/Patient/_delta_log/00000000000000000000.json`
- `HDS.SourceCode/hds-build-artifacts/healthcare-tables/1.4.0/Silver/Observation/_delta_log/00000000000000000000.json`

The user-provided local copies were:

- `E:\Downloads\HDS v1.4.0\HDS.SourceCode\hds-build-artifacts\healthcare-tables\1.4.0\Silver\Patient\_delta_log\00000000000000000000.json`
- `E:\Downloads\HDS v1.4.0\HDS.SourceCode\hds-build-artifacts\healthcare-tables\1.4.0\Silver\Observation\_delta_log\00000000000000000000.json`

These local files are provenance only, not runtime or CI dependencies. To update the target version, compare the selected paths and types against the new official release, update the versioned contracts and projections deliberately, and rerun the checker.

## Projections and mapping

Run the [quick start](../../README.md#quick-start) to validate and inspect the synthetic fixtures. Ignixa 0.6.82 names the outputs from the ViewDefinition file stems: `Patient.parquet` and `Observation.parquet`. Each view is resource-grained. `msftSourceSystem` comes from the `sourceSystem` constant, which the manual export can override with `--var sourceSystem=...`.

- **Patient:** `idOrig`, `meta_lastUpdatedOrig`, `msftSourceSystem`, `active`, `gender`, and `birthDateOrig` are projected. `birthDateOrig` is output as `TIMESTAMP`, but the HDS target is `date`; conversion is loader-owned, not a direct type match. `identifierOrig_string` and `name_string` remain target contract fields but are not projected: the pinned evaluator emitted nulls for complex collection values. A loader needs the original FHIR data to supply them.
- **Observation:** `idOrig`, `msftSourceSystem`, `meta_lastUpdatedOrig`, `status`, `code.text`, the original `subject.reference`, Quantity components, and `valueString` are selected. Each fixture has one LOINC coding; its system, code, and display are flattened into columns. A loader must reconstruct them into nested `code.coding`. The view selects the first LOINC coding and does not generally serialize multiple codings or optional coding fields. Quantity components require nested reconstruction; the projected value is `DECIMAL(38,18)`, while the target is `decimal(36,18)`. The original subject reference is not normalized and must be placed into the nested target as appropriate.

The contracts classify mappings as:

- **direct:** output name and Parquet type match the selected target field.
- **serialized:** a repeating value is represented as a JSON string for a compatible `_string` field, not a nested Parquet array. The current pack does not project the complex Patient collections this way.
- **flattened:** scalar columns need reconstruction into nested target fields.
- **loader-owned:** the loader must derive or obtain a value, or convert a projected value, before loading.

Repeating values serialized as JSON strings and flattened Quantity or reference fields are not equivalent to their nested Silver fields. Fabric resource IDs and normalized references, ingestion creation and modification timestamps, source file lineage, and Delta upserts are outside the ViewDefinitions. Omitted source fields are not preserved in these Parquet projections; loaders needing them must obtain them from the original FHIR data.
