# Fabric HDS R4 contracts and ViewDefinitions

## Decision

The depot publishes one custom SQL-on-FHIR projection and one HDS v1.4.0
logical schema contract for **each of the 146 concrete FHIR R4 resources**
with a Silver table. The two selected-field starter examples remain
independently runnable. The depot does not implement a shaper, writer, or
Delta transaction protocol.

A ViewDefinition describes rows, not a destination. The same logical
projection can be materialized as Parquet or loaded into a custom Fabric
Lakehouse table with a separate Delta load. Microsoft's prescribed HDS
Silver table is a different contract: it includes normalized identities,
references, native nested values, lineage, ingestion clocks and merge
semantics which the projection does not produce. See the [pack guide](../../packs/fabric-silver-hds.md)
for the actual export commands and the pinned CLI's collection limitation.

The [native Ignixa writer](https://github.com/brendankowitz/ignixa-fhir/issues/550)
works from original FHIR and caller-supplied source identity, lineage, and
deployed table metadata. Its first acceptance pair is Patient and
Observation, not 146 resources. It does not consume these views or need a
copy of any depot contract.

## Contract boundary

Under `contracts/fabric-hds/1.4.0/r4/`, `catalog.json` maps the canonical
FHIR resource name to the actual deployed table name and links the contract
and generated view. This matters for two HDS table names which do not match
their resource spelling. Abstract resources and non-FHIR enrichment tables
are excluded.

Contract format version `1` records the complete ordered logical Silver
shape, nested types and nullability, deployment schema digest, creation-log
identity and SHA-256, and source projection mappings. The checked-in format
is defined by `format.schema.json`. Mapping data must distinguish FHIR
source fields from destination-only fields; a `*_string` HDS field or a
normalized ID is not evidence that the source projection implements it.
The `projection.columns` values drive the JSON ViewDefinitions under
`views/r4/fabric-silver-hds/`. Committed inputs suffice for normal use and
checking; operators need the pinned upstream artifacts only to refresh the
contracts or check them against the deployment.

The logical schema digest is SHA-256 of compact UTF-8 JSON of the ordered
logical field descriptors. Struct field names/order, types and nullability,
array element nullability and decimal precision/scale participate. It is
not the hash of a Delta log, a serialized `schemaString`, or a Parquet file.
Use the creation-log hash for artifact identity instead.

**Exact vendor metadata is local.** The creation log in a licensed release
is authoritative for the verbatim `metaData.schemaString`, Delta `protocol`,
and `metaData.configuration`. Do not copy vendor source, deployed metadata
blobs or derived normalization implementations into the public catalog.
Record the location and digest, and support local materialization of exact
metadata for callers that need it. The repository's MIT license does not
resolve redistribution rights for HDS-derived material.

## Capture boundary and acceptance

A versioned capture manifest plus expected-row NDJSON can describe an
independent, operator-supplied HDS run. `capture.schema.json` defines the
manifest/record format independently of the contract version. It identifies
the HDS release, artifact/configuration hashes, runtime/time zone, hashed
inputs, source namespaces, and event order and UTC time windows. For each
event/table it records a Delta version, row count, schema digest, exported
Parquet hash and an expected-row path.

Each NDJSON record has `values` for the whole row and, where needed,
inclusive `timeBounds` for uncontrolled `msftCreatedDatetime` or
`msftModifiedDatetime`. Preserve array order, decimals, nulls and `_string`
contents when comparing. Synthetic local test rows exercise the comparator;
they cannot establish HDS parity. No independent capture is checked in.

CI checks the complete inventory and pairs, deterministic view generation,
contract shape and digests, and actual Parquet export of a synthetic
resource of each type. Populated-field extraction requires a CLI that
preserves complex collection values. Keep catalog coverage, extraction
evidence, Delta protocol support and independently observed Silver row
parity as separate claims.
