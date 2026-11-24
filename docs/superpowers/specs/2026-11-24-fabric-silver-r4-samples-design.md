# Fabric Silver R4 sample views

**Status:** Proposed; awaiting specification review
**Scope:** Initial Patient and Observation examples for Fabric healthcare data solutions v1.4.0

## Context

This repository currently contains only its license and Git ignore configuration. The initial contribution must establish the layout and conventions for a growing catalog of SQL on FHIR ViewDefinitions that Ignixa can run.

The initial target is the **silver healthcare data model** in Microsoft Fabric healthcare data solutions, not the bronze `ClinicalFhir` staging table. [The official Microsoft download](https://www.microsoft.com/en-us/download/details.aspx?id=108754) contains the v1.4.0 R4 resource schemas in `HDS.SourceCode/src/config/fhir4/schema/silver/` and the deployed Delta schemas in `HDS.SourceCode/hds-build-artifacts/healthcare-tables/1.4.0/Silver/{Patient,Observation}/_delta_log/00000000000000000000.json`. The broader architecture and the Fabric-owned transformation behavior are described in [Data foundations in healthcare data solutions](https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/data-architecture-and-management) and [Ingest clinical data using healthcare data foundations](https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/healthcare-data-foundations).

Ignixa's ViewDefinition Parquet export uses scalar columns. Collection results can be serialized as JSON strings, but a scalar projection is not itself a nested Delta table. Fabric adds behavior outside a ViewDefinition, including resource and reference ID normalization, nested schema construction, ingestion timestamps and file lineage, and Delta update handling. Samples must distinguish projections that can load directly from mappings that need further transformation or a loader.

## Goals

- Establish a compact, target-specific repository layout that can later accommodate other FHIR releases and Parquet contracts without mixing their semantics with Fabric Silver.
- Provide runnable, synthetic R4 Patient and Observation examples using Ignixa's SQL on FHIR CLI.
- Pin the first Fabric target to v1.4.0 and base selected-field contracts on the shipped Patient and Observation Delta schemas.
- Verify real Parquet schemas and representative rows, and label where an output is not an exact Fabric field or type match.
- Keep examples safe to copy and run: use synthetic FHIR data and no environment-specific source identities.

## Non-goals

- A complete 146-resource catalog.
- A Fabric-compatible table writer, Spark transformation, or Delta Lake loader.
- A claim that flat ViewDefinition Parquet files are drop-in Fabric Silver Delta tables.
- Committing generated Parquet files or copying the 6.8 GB Fabric release or complete schemas into the repository.
- Implementing the separate `FHIR-Analytics-Pipelines/FhirToDataLake` output contract.

## Repository layout

```text
README.md
views/
  r4/
    fabric-silver/
      Patient.json
      Observation.json
examples/
  r4/
    fabric-silver/
      input/
        Patient.ndjson
        Observation.ndjson
contracts/
  fabric-hds/
    1.4.0/
      Patient.json
      Observation.json
scripts/
  check-samples.py
requirements-test.txt
.config/
  dotnet-tools.json
.github/
  workflows/
    check-samples.yml
docs/
  packs/
    fabric-silver.md
```

The view directory contains only definitions, so it can be passed directly to Ignixa's batch runner. Fixtures are separate from views and contain synthetic NDJSON. The versioned contracts record the source HDS release and official artifact locations, field-level snapshots of only the relevant Fabric target paths and types, the expected Ignixa output types, and a compatibility classification. Contracts contain only the fields needed by these samples, not vendor schema files or copied source code.

## Sample behavior and mapping

Each ViewDefinition is resource-grained and emits no more than one row per Patient or Observation. `msftSourceSystem` is supplied by a typed ViewDefinition constant that can be overridden using Ignixa's `--var sourceSystem=<namespace>` argument.

### Patient

The sample projects `id` to `idOrig`; `meta.lastUpdated` to `meta_lastUpdatedOrig`; and includes `msftSourceSystem`, `active`, `gender`, and `birthDate` as `birthDateOrig`. It also demonstrates preserving the repeating `identifier` and `name` values in the Fabric-style `_string` columns where supported by Ignixa's collection serialization.

The fixture has one Patient with multiple identifiers. The view retains one Patient-grained row; it does not unroll identifiers into duplicate Patient rows.

### Observation

The sample includes `idOrig`, `msftSourceSystem`, `meta_lastUpdatedOrig`, `status`, a LOINC coding example and `code.text`, the original literal `subject.reference`, typed Quantity components, and `valueString`.

The two Observations exercise different R4 `value[x]` branches: a Quantity and a string. Quantity components map from flat columns to fields of a nested Fabric struct and require assembly. The CLI's decimal schema may have a different precision from the HDS `decimal(36,18)` field and must be explicitly classified and checked. A literal subject reference is source data, not a harmonized Fabric reference ID; the mapping identifies the source-reference destination and marks the normalization boundary.

### Compatibility classifications

Each contract entry uses one of these categories:

- **direct**: projected output name and Parquet type match the selected Fabric target field.
- **serialized**: a repeating FHIR value is emitted as a JSON string for a compatible Fabric `_string` target; it is not a Parquet array.
- **flattened**: scalar columns represent components of a nested Fabric target and require reconstruction.
- **loader-owned**: Fabric must derive the value or apply ingestion semantics that are not available in a ViewDefinition.

The documentation identifies the limits of each sample: selected fields are not a complete FHIR resource export, and omitted fields are not implicitly claimed to be preserved. In particular, generating Fabric resource IDs and normalized references, setting Fabric-managed created/modified timestamps and source file paths, and applying upserts remain outside the ViewDefinitions.

## Run and validation

Pin `Ignixa.SqlOnFhir.Cli` at `0.6.82` in the .NET local tool manifest and DuckDB at `2.0.1` in the test requirements. A contributor restores the local .NET tool and DuckDB test dependency, then runs `python scripts/check-samples.py`.

The check performs these steps and exits nonzero on any failure:

1. Run Ignixa R4 ViewDefinition validation.
2. Run the ViewDefinitions against both NDJSON fixtures to a temporary output directory.
3. Use DuckDB to inspect the generated Parquet fields, types, and representative values.
4. Compare each actual output field and Parquet type with the expected Ignixa output recorded in the contract. For each mapped Fabric path, verify its name, type, and nested structure against the checked-in field-level snapshot sourced from the HDS Delta schema. Require every projection mapping to be classified; verify sample values, including preserved repeating JSON strings and the two Observation choice branches.

GitHub Actions runs the same check. Generated Parquet and temporary files are not tracked. The README documents both the single validation command and the equivalent Ignixa commands for users who wish to export the samples themselves.

The HDS contracts are pinned snapshots, not a runtime dependency on the user's `E:\Downloads` path or a download of the Fabric package in CI. The version, release URL, source artifact locations, and contract update procedure are documented, so maintainers can review intentional contract changes when moving to another Fabric release.

## Documentation and scope of subsequent work

The root README explains the repository layout, R4-first target packs, and how to run a sample. The Fabric pack documentation links to the Microsoft architecture and v1.4.0 release and explains bronze versus silver, field compatibility classifications, and the loader boundary.

After review, implementation is limited to this two-resource R4 pack, its selected-field contracts, synthetic examples, test script, and CI. New resource coverage, broader FHIR releases, a FhirToDataLake pack, and Delta-loading capabilities are separate follow-on work.
