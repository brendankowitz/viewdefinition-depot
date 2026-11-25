"""The public contract is a source projection and a schema, not a Silver row."""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
PACK = Path("contracts/fabric-hds/1.4.0/r4")


def read(path):
    return json.loads(path.read_bytes())


def contract(name):
    return read(ROOT / PACK / "resources" / f"{name}.json")


def columns(name):
    return {c["name"]: c for c in contract(name)["projection"]["columns"]}


def targets(name):
    return {m["field"]: m for m in contract(name)["projection"]["targetMappings"]}


class PublicContracts(unittest.TestCase):
    def test_catalog_covers_every_deployment_including_misspelled_tables(self):
        self.assertTrue((ROOT / PACK / "catalog.json").is_file(), "missing ALL-146 catalog")
        catalog = read(ROOT / PACK / "catalog.json")
        entries = {r["resource"]: r for r in catalog["resources"]}
        self.assertEqual(len(catalog["resources"]), 146)
        self.assertEqual(len(entries), 146)
        self.assertEqual({r: v["table"] for r, v in entries.items() if r != v["table"]}, {
            "MedicinalProductContraindication": "MedicinalProductContraindiction",
            "MedicinalProductIndication": "MedicinalProductIndictation",
        })
        for name, entry in entries.items():
            with self.subTest(resource=name):
                c = read(ROOT / entry["contract"])
                self.assertEqual(entry["view"], c["projection"]["view"])
                self.assertEqual(entry["schemaSha256"], c["deployment"]["schemaSha256"])
                self.assertEqual(entry["creationLogSha256"], c["provenance"]["creationLogSha256"])
                fields = [f[0] for f in c["deployment"]["fields"]]
                mappings = c["projection"]["targetMappings"]
                self.assertEqual(fields, [m["field"] for m in mappings])
                by_column = {p["name"]: p for p in c["projection"]["columns"]}
                for p in by_column.values():
                    self.assertEqual(p["targetFields"], [
                        m["field"] for m in mappings if m.get("sourceColumn") == p["name"]
                    ])
                self.assertTrue(all(m["sourceColumn"] in by_column
                                    for m in mappings if "sourceColumn" in m))

    def test_source_shapes_are_fhir_not_inferred_from_silver(self):
        obs = columns("Observation")
        self.assertEqual({n for n in obs if n.startswith("value")}, {
            "valueQuantity", "valueCodeableConcept", "valueString", "valueBoolean", "valueInteger",
            "valueRange", "valueRatio", "valueSampledData", "valueTime", "valueDateTime", "valuePeriod",
        })
        for n, (path, dtype, array) in {
            "valueInteger": ("value.ofType(integer)", "integer", False),
            "valueBoolean": ("value.ofType(boolean)", "boolean", False),
            "valueQuantity": ("value.ofType(Quantity)", "string", True),
            "effectiveDateTime": ("effective.ofType(dateTime)", "string", False),
            "code": ("code", "string", True),
            "category": ("category", "string", True),
        }.items():
            self.assertEqual((obs[n]["path"], obs[n]["type"], obs[n].get("collection", False)),
                             (path, dtype, array))
        self.assertEqual((obs["code"]["min"], obs["code"]["max"]), (1, "1"))
        self.assertEqual((obs["category"]["min"], obs["category"]["max"]), (0, "*"))
        self.assertEqual(obs["modifierExtension"]["targetFields"], [])
        workflow = columns("ExampleScenario")["workflow"]
        self.assertEqual((workflow["type"], workflow["collection"], workflow["fhirType"]),
                         ("string", True, "canonical"))
        for r, n in [("Invoice", "totalPriceComponent"), ("ClaimResponse", "adjudication"),
                     ("ExplanationOfBenefit", "adjudication"), ("SubstanceSpecification", "molecularWeight")]:
            col = columns(r)[n]
            self.assertEqual((col["fhirType"], col["collection"]), ("BackboneElement", True))
        binary = columns("Binary")
        self.assertNotIn("identifier", binary)
        self.assertNotIn("extension", binary)
        self.assertIn("data", binary)

    def test_mapping_does_not_claim_loader_or_incompatible_values_are_direct(self):
        pat = targets("Patient")
        self.assertEqual(pat["idOrig"]["mappingKind"], "direct")
        self.assertEqual((pat["id"]["mappingKind"], pat["id"]["sourceColumn"]), ("derived", "id"))
        self.assertEqual(pat["birthDate"]["mappingKind"], "derived")
        self.assertEqual(pat["name_string"]["mappingKind"], "derived")
        self.assertNotIn("sourceColumn", pat["msftCreatedDatetime"])
        self.assertEqual(targets("Binary")["identifier"]["mappingKind"], "unsupported")
        # A deployed timestamp is not a conversion rule for arbitrary canonical URLs.
        cm = targets("ConceptMap")
        self.assertEqual(cm["sourceCanonical"]["mappingKind"], "unsupported")
        self.assertEqual(cm["sourceCanonicalOrig"]["mappingKind"], "unsupported")

    def test_repeating_primitive_transport_is_not_a_deployed_array(self):
        for resource, name in [("ActivityDefinition", "library"), ("ExampleScenario", "workflow"),
                               ("CapabilityStatement", "format")]:
            with self.subTest(resource=resource):
                col = columns(resource)[name]
                self.assertEqual((col["type"], col["collection"], col["mappingKind"]),
                                 ("string", True, "serialized"))
                mapping = targets(resource)
                self.assertEqual((mapping[name]["mappingKind"], mapping[name]["reason"]),
                                 ("derived", "collection-to-array"))
                self.assertEqual(mapping[name + "_string"]["reason"], "silver-json-serialization")

    def test_configured_flattening_preserves_lineage_for_companion_targets(self):
        for resource, name, source in [
            ("MedicationRequest", "dispenseRequest_validityPeriod_startOrig", "dispenseRequest"),
            ("MedicationRequest", "dispenseRequest_validityPeriod_endOrig", "dispenseRequest"),
            ("Practitioner", "qualification_period_startOrig", "qualification"),
            ("Practitioner", "qualification_period_startOrig_string", "qualification"),
        ]:
            with self.subTest(resource=resource, field=name):
                m = targets(resource)[name]
                self.assertEqual((m["mappingKind"], m.get("sourceColumn")), ("derived", source))
                self.assertIn(name, columns(resource)[source]["targetFields"])

    def test_offline_check_detects_edited_projection_schema_and_stray_contract(self):
        from fabric_silver import build_contracts as build

        self.assertEqual(build.main(["--check"]), 0)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            dest = root / PACK
            shutil.copytree(ROOT / PACK, dest)
            f = dest / "resources/Observation.json"
            original = f.read_bytes()
            self.assertEqual(build.main(["--check"], root=root), 0, "pristine copy must pass")
            for change in ("path", "cardinality", "nested fields", "log hash", "extra"):
                with self.subTest(change=change), contextlib.redirect_stderr(io.StringIO()):
                    c = json.loads(original)
                    if change == "path":
                        c["projection"]["columns"][0]["path"] = "missing"
                    elif change == "cardinality":
                        c["projection"]["columns"][0]["max"] = "*"
                    elif change == "nested fields":
                        c["deployment"]["fields"][0][1] = False
                    elif change == "log hash":
                        c["provenance"]["creationLogSha256"] = "0" * 64
                    else:
                        (dest / "resources/NotAResource.json").write_bytes(original)
                    f.write_text(json.dumps(c, indent=2) + "\n", encoding="utf-8")
                    self.assertNotEqual(build.main(["--check"], root=root), 0)
                    self.assertTrue(f.is_file(), "check must not remove files")

    def test_local_metadata_without_the_hds_release_is_rejected(self):
        from fabric_silver import build_contracts as build

        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(ValueError):
                build.generate(ROOT / PACK, None, None, materialize=True)

    def test_wrong_hl7_package_is_rejected_before_publication(self):
        from fabric_silver import build_contracts as build

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            package = root / "hl7.fhir.r4.core.tgz"
            package.write_bytes(b"not the pinned R4 package")
            with contextlib.redirect_stderr(io.StringIO()) as err:
                self.assertNotEqual(build.main(["--write", "--hds-source", str(root),
                                                "--fhir-package", str(package)], root=root), 0)
            self.assertIn("HL7 package SHA-256", err.getvalue())
            self.assertFalse((root / PACK).exists())

    @unittest.skipUnless(os.getenv("HDS_SOURCE_DIR") and importlib.util.find_spec("pyarrow"),
                         "set HDS_SOURCE_DIR and install PyArrow 21 for the independent schema cross-check")
    def test_all_schema_digests_match_arrow_reference_on_release_bytes(self):
        from fabric_silver.reference import deployed_schema, schema_digest

        root = Path(os.environ["HDS_SOURCE_DIR"])
        for entry in read(ROOT / PACK / "catalog.json")["resources"]:
            with self.subTest(resource=entry["resource"]):
                prov = read(ROOT / entry["contract"])["provenance"]
                log = root / prov["creationLogPath"]
                self.assertEqual(entry["schemaSha256"], schema_digest(deployed_schema(log)))
                self.assertEqual(entry["creationLogSha256"], hashlib.sha256(log.read_bytes()).hexdigest())


@unittest.skipUnless(os.getenv("HDS_SOURCE_DIR") and os.getenv("FHIR_R4_PACKAGE"),
                     "set HDS_SOURCE_DIR and FHIR_R4_PACKAGE to verify the licensed release")
class ReleaseContracts(unittest.TestCase):
    def test_clean_rebuild_and_verified_local_materialization(self):
        from fabric_silver import build_contracts as build

        hds, fhir = Path(os.environ["HDS_SOURCE_DIR"]), Path(os.environ["FHIR_R4_PACKAGE"])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args = ["--hds-source", str(hds), "--fhir-package", str(fhir)]
            self.assertEqual(build.main(["--write", *args], root=root), 0)
            dest = root / PACK
            generated = sorted([dest / "catalog.json", dest / "types.json", *(dest / "resources").glob("*.json")])
            self.assertEqual(len(generated), 148)
            for p in generated:
                self.assertEqual(p.read_text("utf-8"), (ROOT / p.relative_to(root)).read_text("utf-8"))
            # Every file exists and parses. A byte-only log mutation must still
            # reject the entire release, without writing even the earlier tables.
            bad = root / "changed-release"
            for entry in read(dest / "catalog.json")["resources"]:
                prov = read(root / entry["contract"])["provenance"]
                for path in [prov["creationLogPath"], *prov["sourceArtifactSha256"]]:
                    copy = bad / path
                    copy.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(hds / path, copy)
            bad_log = bad / contract("Patient")["provenance"]["creationLogPath"]
            bad_log.write_bytes(bad_log.read_bytes() + b"\n")
            with contextlib.redirect_stderr(io.StringIO()) as err:
                self.assertNotEqual(build.main(["--materialize-local", "--hds-source", str(bad)], root=root), 0)
            self.assertIn("digest", err.getvalue())
            self.assertFalse((root / ".hds-local").exists())
            self.assertEqual(build.main(["--materialize-local", "--hds-source", str(hds)], root=root), 0)
            private = root / ".hds-local/1.4.0/r4/resources"
            self.assertEqual(len(list(private.glob("*.json"))), 146)
            for entry in read(dest / "catalog.json")["resources"]:
                with self.subTest(resource=entry["resource"]):
                    resource = read(root / entry["contract"])
                    raw = hds / resource["provenance"]["creationLogPath"]
                    actions = list(map(json.loads, raw.read_bytes().splitlines()))
                    md = next(a["metaData"] for a in actions if "metaData" in a)
                    protocol = next(a["protocol"] for a in actions if "protocol" in a)
                    saved = read(private / f"{entry['resource']}.json")["deployment"]
                    self.assertEqual(saved["configuration"], md["configuration"])
                    self.assertEqual(saved["schemaString"], md["schemaString"])
                    self.assertEqual(saved["protocol"], protocol)


if __name__ == "__main__":
    unittest.main()
