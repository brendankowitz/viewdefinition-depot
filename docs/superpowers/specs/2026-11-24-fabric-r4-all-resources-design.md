# Fabric HDS R4 contracts and ViewDefinitions

**Goal:** Publish a versioned HDS v1.4.0 contract and runnable SQL-on-FHIR
ViewDefinition for each of the 146 concrete FHIR R4 resource types with an
HDS Silver table.

**Architecture:** This repository is a catalog of contracts and custom
projections, not a Fabric Silver writer. Each contract records the deployed
Silver schema and its provenance, alongside the source-to-projection mapping
and generated ViewDefinition. Ignixa's native Delta writer is a separate
implementation in `ignixa-fhir`; it projects from the FHIR element tree and
does not depend on these ViewDefinitions or copy these contracts while their
licensing is unresolved.

The versioned contract and capture formats are a stable boundary for
independent tooling, including a future Ignixa implementation. This defines
a format, not a data dependency or authorization to embed HDS-derived
artifacts in Ignixa. While licensing is unresolved, the native writer uses
caller-supplied deployed HDS metadata. Its approved scope is narrower:
Phase 1 spike/ADR, followed by Phase 2 local Delta create/merge/CDF for
Patient and Observation only. Depot coverage for 146 resources does not
imply writer coverage beyond this acceptance pair.

**Tech Stack:** FHIR R4 4.0.1, HDS 1.4.0 deployment metadata and R4
configuration, SQL-on-FHIR ViewDefinitions, Ignixa CLI, JSON contracts,
Python generation and validation tooling.

**Design inputs:** `docs/superpowers/specs/2026-11-24-fabric-silver-r4-parity-design.md`,
the starter pack in `docs/packs/fabric-silver.md`, the parity pilot in
`docs/packs/fabric-silver-parity.md`, and the user's decision to keep the
contracts while removing the depot's shaping and writer implementation.

## Scope and boundary

Include all 146 concrete FHIR R4 resource types that map to deployed HDS
v1.4.0 Silver tables. Exclude abstract `Resource` and `DomainResource`,
non-FHIR enrichment tables, and the extra non-resource Avro schema. Preserve
HDS table names exactly, including the two known misspelled table names.

Ship one contract and one ViewDefinition per resource. The contract contains
the full ordered logical Silver schema, deployed Delta creation metadata
(`schemaString`, protocol, and table properties), version and source hashes,
and the mapping from resource fields to projection columns or fields that
require transformations outside a ViewDefinition. The view selects
source-backed values as a custom projection, using SQL-on-FHIR-supported
types and collection handling. It is not a loader contract for the nested
Silver schema.

The view role is a design assumption based on the approved option 2 and the
native writer's element-tree architecture: views are for custom SQL-on-FHIR
exports, not the native Silver output path. `fabric-silver.md` already states
that its starter projections are not drop-in Silver tables. The new pack
must preserve that distinction. If SQL-on-FHIR cannot express or transport a
field faithfully, record the limitation in its contract; do not silently
drop it or label the projection lossless.

Out of scope:

- Implementing or retaining a Silver shaper, local writer, or writer CLI in
  this repository.
- Reproducing Delta transaction logs, Spark, or Fabric orchestration.
- Claiming value parity without independent HDS captures.
- Embedding depot contracts into Ignixa before the licensing decision.

## Repository layout

Keep the starter PR #1 files and behavior unchanged. Add a separate
versioned catalog:

```text
contracts/fabric-hds/1.4.0/r4/
  catalog.json
  format.schema.json
  capture.schema.json
  types.json
  resources/
    Account.json
    ...
    Observation.json
    ...
views/r4/fabric-silver-hds/
  Account.json
  ...
  Observation.json
  ...
```

`catalog.json` is the authoritative 146-resource inventory and table-name
mapping. Each resource contract is the source of truth for its generated
view. `format.schema.json` defines the stable contract JSON shape. Shared
logical type definitions are factored into `types.json`; the per-resource
contract still carries the complete ordered top-level schema and its digest.

Contract format version 1 has this shape:

```json
{
  "contractFormatVersion": "1",
  "resource": "Patient",
  "table": "Patient",
  "fhirVersion": "4.0.1",
  "hdsVersion": "1.4.0",
  "deployment": {
    "schemaString": "{\"type\":\"struct\", ...}",
    "schemaSha256": "<logical-schema-digest>",
    "protocol": {},
    "configuration": {}
  },
  "provenance": {
    "release": "<release-identity>",
    "creationLogPath": "<release-relative-path>",
    "creationLogSha256": "<sha256>",
    "sourceArtifactSha256": {}
  },
  "projection": {
    "view": "<repo-relative-path>",
    "columns": [
      {
        "name": "id",
        "path": "id",
        "type": "string",
        "collection": false,
        "targetFields": ["idOrig"],
        "mappingKind": "direct",
        "evidence": "runtime-observed"
      }
    ]
  }
}
```

`schemaString` is the exact string from the deployed Delta log; `protocol`
and `configuration` preserve the raw Delta objects and their key/value
contents. `schemaSha256` is computed as SHA-256 over UTF-8 bytes of compact JSON
(`separators=(",", ":")`) encoding the ordered logical field list. Each
field encodes its name, nullability, and logical type recursively; structs
preserve field order, arrays include element nullability, and decimals
include precision and scale. The raw creation-log hash binds the stored
metadata to its release artifact. `mappingKind` is one of `direct`, `serialized`,
`derived`, or `unsupported`; `evidence` distinguishes `contract-sourced`,
`source-derived`, and `runtime-observed`. The schema document must reject
unknown format major versions rather than guess.

The catalog lists each canonical resource and exact HDS table name, and
references its contract and view by repository-relative path. Shared nested
logical types remain in `types.json`, while each resource contract resolves
to a complete schema without requiring another HDS artifact at runtime.

Retire the two-resource pilot's `fabric_silver/normalize.py`,
`fabric_silver/writer.py`, and `fabric_silver/__main__.py`, plus writer-only
fixtures and tests. Generalize and retain only tooling that serves catalog
generation, ViewDefinition generation/checking, and generic comparison of
future independent captures. Keep extraction tests and synthetic fixtures
when they exercise the ViewDefinitions rather than the removed writer.
Update README and pack docs so “view coverage” is distinct from “HDS
behavioral parity”; until independent references exist, verified parity
remains 0/146.

## Contract and view generation

Generate contracts from the pinned release's deployed Delta creation logs
and R4 schema/configuration inventory. Capture, per table, the exact ordered
logical schema, schema digest, protocol, table properties, and creation-log
hash. Cross-check table names against the R4 resource-type inventory and
Avro schema stems; do not infer an HDS table name solely from a FHIR resource
name.

For each resource, record projection columns in deterministic order with
their FHIRPath expression, SQL-on-FHIR type, collection behavior, and
relationship to the HDS target field(s). Separate direct source projections
from derived/loader-owned target fields. Mark non-expressible or
unsupported mappings explicitly. Contract metadata must distinguish facts
read from release artifacts from source-derived assumptions and observed
runtime behavior.

Generate each ViewDefinition from its contract. A deterministic checker
fails for a missing, extra, or stale view; duplicate resource or target
mapping; mismatched resource/table identity; malformed schema digest; or a
view column that differs from its declared projection. Generation must not
need a developer's HDS download path in CI.

The generator is not permission to redistribute upstream source. The public
repository will retain schema-derived contracts by the user's decision, with
an explicit unresolved licensing blocker. Do not copy upstream source files
or notices into generated contracts, and do not treat this publication
decision as license clearance.

## Independent capture format

Independent capture data, when available, uses
`examples/r4/fabric-silver-hds/reference/<capture-id>/`. Its `capture.json`
has `captureFormatVersion: "1"` and records the HDS release and artifact
hashes, effective configuration and hashes, runtime and UTC session
timezone, input paths and hashes, source namespace-to-file associations,
and per-event UTC run bounds. For each event/resource it records the
committed Delta version, row count, schema digest, exported Parquet hash,
and relative path to an NDJSON expected-row file. Each NDJSON line is one
complete row in `{"values": {...}}`; optional `timeBounds` contains
inclusive bounds only for uncontrolled ingestion-clock fields. Nested
values, array order, decimals, nulls, and `_string` text are preserved as
emitted. The capture gate requires this provenance and compares against the
contract schema; writer-generated expectations are never accepted as a
capture. The capture-format version is independent from the
contract-format version. Consumers reject unknown major versions.

The capture manifest uses these stable top-level fields:

```json
{
  "captureFormatVersion": "1",
  "captureId": "<stable-id>",
  "deployment": {
    "fhirVersion": "4.0.1",
    "hdsVersion": "1.4.0",
    "release": "<release-identity>",
    "artifactSha256": {},
    "configurationSha256": {},
    "runtime": {
      "platform": "<platform>",
      "sparkVersion": "<version>",
      "deltaVersion": "<version>",
      "javaVersion": "<version>"
    },
    "sessionTimeZone": "UTC"
  },
  "inputs": {
    "<input-id>": {
      "path": "<synthetic-input-relative-path>",
      "sha256": "<sha256>",
      "sourceSystem": "<observed-namespace>",
      "logicalPath": "<observed-source-file-lineage>"
    }
  },
  "events": [
    {
      "name": "<event>",
      "inputIds": ["<input-id>"],
      "runWindowUtc": {"start": "<inclusive-UTC>", "end": "<inclusive-UTC>"},
      "resources": [
        {
          "resource": "Patient",
          "table": "Patient",
          "deltaVersion": 0,
          "rowCount": 0,
          "schemaSha256": "<logical-schema-digest>",
          "parquetSha256": "<sha256>",
          "rowsPath": "rows/<event>/Patient.ndjson"
        }
      ]
    }
  ]
}
```

`capture.schema.json` validates the manifest and the expected-row record
shape. `runtime` has `platform`, `sparkVersion`, `deltaVersion`, and
`javaVersion`; configuration hashes are keyed by release-relative artifact
path. Each event also names its `inputIds`. A row record is
`{"values": <complete Silver row>}` with optional
`"timeBounds": {"<clock-field>": {"min": "<UTC>", "max": "<UTC>"}}`.
Only `msftCreatedDatetime` and `msftModifiedDatetime` may use inclusive UTC
bounds, and only when that clock is uncontrolled. The schema files define
required fields, types, path rules, and allowed clock fields; the format
must not depend on prose-only interpretation.

## Validation and evidence

The CI contract checks must establish that:

1. The inventory contains exactly the 146 concrete R4 resource types with
   deployed Silver tables, and every resource maps to its actual HDS table.
2. Every resource has one complete contract and one generated
   ViewDefinition; all 146 definitions pass structural validation.
3. Each recorded schema digest is computed from its ordered logical fields,
   including nested order, types, nullability, array element types, and
   decimal precision/scale. The contract's deployed metadata and provenance
   are internally consistent.
4. The contracts and views regenerate deterministically, and no existing
   starter ViewDefinition or contract changes as a side effect.
5. CLI extraction tests run when a suitable Ignixa CLI is selected. If no
   released CLI supports the required collection behavior, CI reports the
   relevant skip explicitly rather than claiming extraction verification.

These checks prove catalog completeness, schema-contract consistency, and
view-generation correctness; they do not prove that the views match HDS
runtime behavior. The parity count stays 0/146 until independent HDS rows
are captured and compared. The first capture change must record provenance
and gate on it; writer-generated expectations are not an HDS reference.

## Licensing and provenance

The contracts and field mappings are derived from HDS v1.4.0 release
artifacts. The related HDS license terms and the design spec's
“Do not redistribute the HDS source” constraint remain unresolved. The
depot's own license is MIT; do not infer compatibility or permission from
the user's decision to keep these artifacts. Keep the pack and PR draft
disclosures explicit. Do not copy the contracts into `ignixa-fhir` unless
the licensing decision is resolved.

## Rollout

First trim the draft parity PR to remove the depot's Python shaping and
writer while preserving the starter PR #1. Then expand the versioned
contracts and generated views from Patient/Observation to all 146 mapped R4
resources, updating generation checks and documentation. Keep the PR draft
while licensing is unresolved. The native Ignixa Delta writer remains a
separate workstream and consumes no depot code or contract artifacts until
that boundary is cleared.

The Ignixa workstream itself is Phase 1 spike/ADR followed by Phase 2 local
Delta create/merge/CDF for Patient and Observation only. Its initial
acceptance set must not be widened by this depot's 146-resource catalog.
