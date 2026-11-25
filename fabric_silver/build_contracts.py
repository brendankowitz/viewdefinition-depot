"""Build or check the public R4 projection contracts. No network or Spark required.

Download https://hl7.org/fhir/R4/hl7.fhir.r4.core.tgz outside the checkout.
Run ``python -m fabric_silver.build_contracts --write --hds-source HDS.SourceCode
--fhir-package hl7.fhir.r4.core.tgz`` to rebuild from scratch. ``--check`` replays
the renderer from pinned public facts; supply either or both inputs to check them
against the original releases. ``--materialize-local --hds-source HDS.SourceCode``
writes exact Delta metadata only under .hds-local/1.4.0/r4/resources/.
"""

import argparse
from dataclasses import dataclass
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import tarfile
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PACK = Path("contracts/fabric-hds/1.4.0/r4")
VIEW_DIR = Path("views/r4/fabric-silver-hds")
LOCAL = Path(".hds-local/1.4.0/r4/resources")
HDS = "https://www.microsoft.com/en-us/download/details.aspx?id=108754"
HL7 = "https://hl7.org/fhir/R4/hl7.fhir.r4.core.tgz"
HL7_SHA256 = "b090bf929e1f665cf2c91583720849695bc38d2892a7c5037c56cb00817fb091"
VERSIONS = {"contractFormatVersion": "1", "fhirVersion": "4.0.1", "hdsVersion": "1.4.0"}
TABLES = "hds-build-artifacts/healthcare-tables/1.4.0/Silver"
CONFIG = "hds-build-artifacts/healthcare-configuration/1.4.0/_internal/fhir4"
ZERO = "_delta_log/00000000000000000000.json"

# Calculated independently from the legacy inventory, raw v1.4.0 release files
# and reference.schema_digest(reference.deployed_schema(log)) with PyArrow 21.
# Hash sorted [resource, table, log SHA, Avro SHA, logical schema SHA, schemaString SHA].
HDS_FACTS = "f6d7706a1b2527f1f2c4855b93cee01020c7e7400719b91c38ad5e68d53bcd10"
# Hash sorted [resource, raw StructureDefinition SHA, ordered top-level facts].
# Facts are [snapshot element path, resolved type code, minimum, maximum].
FHIR_FACTS = "1fbd80fc358bfe4c8bcfe3648a6050c5d06fdf7032219e3259e7c160e08f1924"
TABLE_ALIASES = {
    "MedicinalProductContraindication": "MedicinalProductContraindiction",
    "MedicinalProductIndication": "MedicinalProductIndictation",
}
FLATTEN_CONFIGS = {
    "MedicationRequest": "0a82b9fd75c5d5669bfaa87fa8dd597b6a814fc19189719d5156d4e589ab40ed",
    "Practitioner": "e1d27fe75b254e801aac608020c3e63f2dba1eb4880b6b9911eb6948b97a6e99",
}
FLATTENED = {
    "MedicationRequest": {"dispenseRequest_validityPeriod_start": "dispenseRequest",
                          "dispenseRequest_validityPeriod_end": "dispenseRequest"},
    "Practitioner": {"qualification_period_start": "qualification"},
}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def fingerprint(value):
    return sha(json.dumps(value, separators=(",", ":")).encode("utf-8"))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def avro_path(resource):
    return f"{CONFIG}/schema/silver/{resource.lower()}.avsc"


def flatten_path(resource):
    return f"{CONFIG}/transformation/flatten/{resource.lower()}.columnsconfig.json"


def log_path(table):
    return f"{TABLES}/{table}/{ZERO}"


def sd_path(resource):
    return f"package/StructureDefinition-{resource}.json"


def canonical(resource):
    return f"http://hl7.org/fhir/StructureDefinition/{resource}"


def snapshot_facts(definition):
    """Expand choices in snapshot order, using the referring element's cardinality."""
    elements = {e["id"]: e for e in definition["snapshot"]["element"]}
    facts = []
    for e in elements.values():
        if e["path"].count(".") != 1:
            continue
        types = e.get("type")
        if types is None:
            ref = e["contentReference"]
            require(ref.startswith("#"), f"external contentReference at {e['id']}")
            types = elements[ref[1:]]["type"]
        for t in types:
            code = t["code"]
            if code.startswith("http://hl7.org/fhirpath/System."):
                # R4 Resource.id has a FHIRPath system type annotated with the FHIR type.
                (code,) = [ext["valueUrl"] for ext in t["extension"]
                           if ext["url"] == canonical("structuredefinition-fhir-type")]
            facts.append([e["path"], code, e["min"], e["max"]])
    return facts


def read_hl7(path):
    data = path.read_bytes()
    require(sha(data) == HL7_SHA256, f"HL7 package SHA-256 mismatch; expected {HL7_SHA256}")
    resources = {}
    # Stream members; do not extract the tar archive into the workspace.
    with tarfile.open(fileobj=io.BytesIO(data), mode="r|gz") as archive:
        for member in archive:
            if not member.isfile() or not re.fullmatch(r"package/StructureDefinition-[^/]+\.json", member.name):
                continue
            raw = archive.extractfile(member).read()
            sd = json.loads(raw)
            if sd["kind"] != "resource" or sd["abstract"] or sd["derivation"] != "specialization":
                continue
            r = sd["type"]
            require(sd["fhirVersion"] == "4.0.1" and sd["url"] == canonical(r) and
                    member.name == sd_path(r) and r not in resources, f"{r}: wrong HL7 identity")
            resources[r] = (sha(raw), snapshot_facts(sd))
    return resources


def logical_type(t):
    """Delta -> the ordered logical descriptors hashed by fabric_silver.reference."""
    if isinstance(t, str):
        if match := re.fullmatch(r"decimal\((\d+),(\d+)\)", t):
            return f"decimal128({int(match[1])}, {int(match[2])})"
        return {"string": "string", "integer": "int32", "boolean": "bool",
                "date": "date32[day]", "timestamp": "timestamp[us, tz=UTC]"}[t]
    if t["type"] == "array":
        return ["array", t["containsNull"], logical_type(t["elementType"])]
    if t["type"] == "struct":
        return ["struct", [[f["name"], f["nullable"], logical_type(f["type"])] for f in t["fields"]]]
    raise ValueError(f"unrecognized Delta type: {t['type']}")


def pack_type(t, types):
    if isinstance(t, str):
        return t
    if t[0] == "array":
        entry = ["array", t[1], pack_type(t[2], types)]
    else:
        entry = ["struct", [[n, b, pack_type(v, types)] for n, b, v in t[1]]]
    key = "@" + fingerprint(entry)
    types[key] = entry
    return key


def unpack_type(key, types, cache):
    if key in cache:
        return cache[key]
    if not key.startswith("@"):
        return key
    entry = types[key]
    require("@" + fingerprint(entry) == key, f"shared logical type hash mismatch: {key}")
    if entry[0] == "array":
        t = ["array", entry[1], unpack_type(entry[2], types, cache)]
    elif entry[0] == "struct":
        t = ["struct", [[n, b, unpack_type(v, types, cache)] for n, b, v in entry[1]]]
    else:
        raise ValueError(f"invalid shared logical type: {key}")
    cache[key] = t
    return t


@dataclass
class Deployment:
    fields: list
    log_sha: str
    schema_string_sha: str
    artifacts: dict
    raw: dict | None = None  # Never passed to the public renderer.


def read_deployment(hds, resource, table):
    raw = (hds / log_path(table)).read_bytes()
    log_sha = sha(raw)
    actions = [json.loads(line) for line in raw.splitlines() if line.strip()]
    (md,) = [a["metaData"] for a in actions if "metaData" in a]
    (protocol,) = [a["protocol"] for a in actions if "protocol" in a]
    struct = logical_type(json.loads(md["schemaString"]))
    require(struct[0] == "struct", f"{resource}: expected Delta root struct")
    fields = struct[1]
    av = avro_path(resource)
    av_bytes = (hds / av).read_bytes()
    avro = json.loads(av_bytes)
    require(avro["name"] == resource and avro["type"] == "record", f"{resource}: wrong Avro identity")
    require({f["name"] for f in avro["fields"]} <= {f[0] for f in fields},
            f"{resource}: Avro fields missing from deployed schema")
    artifacts = {av: sha(av_bytes)}
    if resource in FLATTEN_CONFIGS:
        p = flatten_path(resource)
        cfg = (hds / p).read_bytes()
        require(sha(cfg) == FLATTEN_CONFIGS[resource], f"{resource}: flatten configuration hash differs")
        require({f["name"] for f in json.loads(cfg)["columns"]} == FLATTENED[resource].keys(),
                f"{resource}: unexpected flatten targets")
        artifacts[p] = sha(cfg)
    return Deployment(fields, log_sha, sha(md["schemaString"].encode("utf-8")), artifacts,
                      {"schemaString": md["schemaString"], "protocol": protocol,
                       "configuration": md["configuration"]})


def source_column(fact):
    element, code, minimum, maximum = fact
    field = element.split(".")[1]
    if field.endswith("[x]"):
        base = field[:-3]
        name, path = base + code[:1].upper() + code[1:], f"{base}.ofType({code})"
    else:
        name = path = field
    dtype = {"boolean": "boolean", "decimal": "decimal", "integer": "integer",
             "unsignedInt": "integer", "positiveInt": "integer"}.get(code, "string")
    column = {"name": name, "path": path, "type": dtype}
    # FHIR complex types start with an uppercase letter. Even a single complex
    # value needs the CLI's collection-of-JSON transport; its R4 maximum stays 1.
    if maximum != "1" or code[0].isupper():
        column["collection"] = True
    return {**column, "fhirElement": element, "fhirType": code, "min": minimum, "max": maximum}


def mapping(resource, field, dtype, sources):
    """Classify a target relationship, not the equivalence of any observed HDS rows."""
    if field in {"msftModifiedDatetime", "msftCreatedDatetime", "msftSourceSystem", "msftFilePath"}:
        return "derived", "ingestion-metadata", None
    if field == "resourceType":
        return "derived", "resource-discriminator", None
    if field in {"meta_lastUpdated", "meta_lastUpdatedOrig"}:
        return "derived", "nested-temporal-value", "meta"
    base = field.removesuffix("_string")
    name = base.removesuffix("Orig")
    if name in FLATTENED.get(resource, {}):
        reason = "silver-json-serialization" if field != base else "configured-flattening"
        return "derived", reason, FLATTENED[resource][name]
    if name not in sources:
        return "unsupported", "no-r4-source-element", None
    code = sources[name]["fhirType"]
    if field != base:
        return "derived", "silver-json-serialization", name
    if field == "id":
        return "derived", "normalized-identity", name
    if code[0].isupper():
        if dtype == "string":
            return "serialized", "complex-to-text", name
        return "derived", "structured-target", name
    if sources[name].get("collection"):
        # The export transports a JSON collection, not the deployed nested array.
        return "derived", "collection-to-array", name
    if dtype in ("timestamp[us, tz=UTC]", "date32[day]"):
        if code not in ("instant", "date", "dateTime"):
            return "unsupported", "incompatible-temporal-target", name
        return "derived", "temporal-conversion", name
    if code == "decimal":
        return "derived", "fixed-precision-decimal", name
    return "direct", "source-value", name


def projection(resource, facts, fields):
    columns = [source_column(f) for f in facts]
    by_name = {c["name"]: c for c in columns}
    require(len(by_name) == len(columns), f"{resource}: duplicate source column")
    targets = []
    by_source = {name: [] for name in by_name}
    for field, _, dtype in fields:
        kind, reason, source = mapping(resource, field, dtype, by_name)
        target = {"field": field, "mappingKind": kind, "reason": reason,
                  "evidence": "contract-sourced" if kind == "unsupported" else "source-derived"}
        if source is not None:
            target["sourceColumn"] = source
            by_source[source].append(target)
        targets.append(target)
    for c in columns:
        matches = by_source[c["name"]]
        c["targetFields"] = [m["field"] for m in matches]
        kinds = {m["mappingKind"] for m in matches}
        if not kinds - {"unsupported"}:
            c["mappingKind"] = "unsupported"
        elif c.get("collection"):
            c["mappingKind"] = "serialized"
        else:
            c["mappingKind"] = "direct" if "direct" in kinds else "derived"
        c["evidence"] = "source-derived"
    return {"view": (VIEW_DIR / f"{resource}.json").as_posix(), "columns": columns,
            "targetMappings": targets}


def public_inputs(pack, *, need_schema, need_fhir):
    """Read only irreducible facts. Paths, mappings, digests and catalog are rebuilt."""
    types = json.loads((pack / "types.json").read_bytes())["types"] if need_schema else {}
    deployments, sources, cache = {}, {}, {}
    paths = sorted((pack / "resources").glob("*.json"))
    require(len(paths) == 146, f"expected 146 public resource files, found {len(paths)}")
    for p in paths:
        doc = json.loads(p.read_bytes())
        r = p.stem
        require(re.fullmatch("[A-Z][A-Za-z0-9]+", r) and doc["resource"] == r,
                f"{r}: resource identity mismatch")
        prov = doc["provenance"]
        if need_schema:
            deployment = doc["deployment"]
            fields = [[n, b, unpack_type(t, types, cache)] for n, b, t in deployment["fields"]]
            artifacts = {avro_path(r): prov["sourceArtifactSha256"][avro_path(r)]}
            if r in FLATTEN_CONFIGS:
                artifacts[flatten_path(r)] = FLATTEN_CONFIGS[r]
            deployments[r] = Deployment(fields, prov["creationLogSha256"],
                                        deployment["schemaStringSha256"], artifacts)
        if need_fhir:
            facts = [[c[k] for k in ("fhirElement", "fhirType", "min", "max")]
                     for c in doc["projection"]["columns"]]
            sources[r] = (prov["fhirStructureDefinition"]["sha256"], facts)
    return deployments, sources


def render(value):
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def generate(pack, hds, fhir, materialize=False):
    """Validate all release identities before allowing any publication."""
    deployments, sources = {}, {}
    if not (hds and fhir):
        deployments, sources = public_inputs(pack, need_schema=not hds, need_fhir=not fhir)
    if fhir:
        sources = read_hl7(fhir)
    require(len(sources) == 146 and
            fingerprint([[r, *sources[r]] for r in sorted(sources)]) == FHIR_FACTS,
            "R4 source-facts digest differs from official HL7 4.0.1")
    public, private, types, entries, release = {}, {}, {}, [], []
    for resource in sorted(sources):
        table = TABLE_ALIASES.get(resource, resource)
        d = read_deployment(hds, resource, table) if hds else deployments[resource]
        sd_sha, facts = sources[resource]
        digest = fingerprint(d.fields)
        release.append([resource, table, d.log_sha, d.artifacts[avro_path(resource)], digest, d.schema_string_sha])
        fields = [[n, b, pack_type(v, types)] for n, b, v in d.fields]
        contract_path = PACK / "resources" / f"{resource}.json"
        proj = projection(resource, facts, d.fields)
        public[contract_path] = render({
            **VERSIONS, "resource": resource, "table": table,
            "deployment": {"schemaSha256": digest, "schemaStringSha256": d.schema_string_sha,
                           "fields": fields},
            "provenance": {"release": HDS, "creationLogPath": log_path(table), "creationLogSha256": d.log_sha,
                           "sourceArtifactSha256": d.artifacts, "fhirStructureDefinition": {
                               "canonical": canonical(resource), "packagePath": sd_path(resource), "sha256": sd_sha}},
            "projection": proj,
        })
        entries.append({"resource": resource, "table": table, "contract": contract_path.as_posix(),
                        "view": proj["view"], "schemaSha256": digest, "creationLogSha256": d.log_sha})
        if materialize:
            private[LOCAL / f"{resource}.json"] = render({
                **VERSIONS, "resource": resource, "table": table, "creationLogSha256": d.log_sha,
                "deployment": {"schemaSha256": digest, **d.raw},
            })
    require(fingerprint(release) == HDS_FACTS,
            "HDS release-facts digest differs (resource/table, log, Avro, logical schema or raw schema)")
    public[PACK / "types.json"] = render({"contractFormatVersion": "1", "types": dict(sorted(types.items()))})
    public[PACK / "catalog.json"] = render({
        **VERSIONS, "types": (PACK / "types.json").as_posix(),
        "provenance": {"release": HDS, "hdsFactsSha256": HDS_FACTS, "fhirPackage": HL7,
                       "fhirPackageSha256": HL7_SHA256, "fhirFactsSha256": FHIR_FACTS},
        "resources": entries,
    })
    return public, private


def check_files(root, files, resource_dir, *, writing=False):
    extra = sorted(p.name for p in (root / resource_dir).glob("*.json") if p.relative_to(root) not in files)
    require(not extra, f"unexpected files in {resource_dir}: {', '.join(extra)}")
    if not writing:
        stale = [p.as_posix() for p, text in files.items()
                 if not (root / p).is_file() or (root / p).read_text("utf-8") != text]
        require(not stale, f"stale or missing ({len(stale)}): {', '.join(stale[:5])}")


def write_files(root, files):
    # Fail closed on redirected directories, especially for local-only licensed output.
    for p in files:
        require((root / p).resolve() == root / p, f"refusing redirected output path {p}")
    for p, text in files.items():
        dest = root / p
        data = text.encode("utf-8")
        if dest.is_file() and dest.read_bytes() == data:
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(dir=dest.parent, delete=False)
        temp = Path(tmp.name)
        try:
            with tmp:
                tmp.write(data)
            temp.replace(dest)
        finally:
            temp.unlink(missing_ok=True)


def main(argv=None, *, root=ROOT):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="rebuild from both release inputs")
    mode.add_argument("--check", action="store_true", help="check published files; optional release inputs")
    mode.add_argument("--materialize-local", action="store_true", help="emit private Delta metadata after validation")
    parser.add_argument("--hds-source", type=Path, help="root of the local HDS.SourceCode v1.4.0 release")
    parser.add_argument("--fhir-package", type=Path, help="official hl7.fhir.r4.core.tgz (FHIR 4.0.1)")
    args = parser.parse_args(argv)
    if args.write and not (args.hds_source and args.fhir_package):
        parser.error("--write requires --hds-source and --fhir-package")
    if args.materialize_local and not args.hds_source:
        parser.error("--materialize-local requires --hds-source")
    root = root.resolve()
    try:
        public, private = generate(root / PACK, args.hds_source, args.fhir_package, args.materialize_local)
        check_files(root, public, PACK / "resources", writing=args.write)
        if args.write:
            write_files(root, public)
        if args.materialize_local:
            check_files(root, private, LOCAL, writing=True)
            write_files(root, private)
    except (OSError, ValueError, KeyError, IndexError, TypeError, tarfile.TarError) as exc:
        print(f"contracts: {exc}", file=sys.stderr)
        return 1
    print("146 R4 contracts: " + ("generated" if args.write else "checked")
          + (f"; local metadata: {LOCAL.as_posix()}" if args.materialize_local else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
