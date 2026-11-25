"""Read real Ignixa Parquet output back and compare it with the synthetic source.

Evidence here is extraction-only: nothing in this file claims HDS parity.
Set IGNIXA_PARITY_CLI to the path of a built, collection-capable
``ignixa-sqlonfhir.dll``. ``pinned`` selects the repo-pinned 0.6.82 CLI, which
writes null for complex columns, so the extraction tests are expected to fail there.
The development extraction tests skip only when IGNIXA_PARITY_CLI is unset.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pyarrow.parquet as pq
from jsonschema import Draft202012Validator

from fabric_silver import views

ROOT = Path(__file__).resolve().parents[1]
PARITY = ROOT / "examples" / "r4" / "fabric-silver-hds"
PINNED = ["dotnet", "tool", "run", "ignixa-sqlonfhir", "--"]


def selected_cli():
    value = os.environ.get("IGNIXA_PARITY_CLI")
    if value == "pinned":
        return PINNED
    if value is None:
        raise unittest.SkipTest("IGNIXA_PARITY_CLI is unset; select a CLI supporting complex collections")
    dll = Path(value)
    if dll.suffix.lower() != ".dll" or not dll.is_file():
        raise AssertionError("IGNIXA_PARITY_CLI must be 'pinned' or an existing development CLI DLL")
    return ["dotnet", str(dll.resolve())]


def run_cli(cli, *args):
    done = subprocess.run(cli + list(args), cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    output = done.stdout + done.stderr
    # The CLI can exit 0 after a per-view failure; `validate` prints a failure mark,
    # `run` prints ERROR lines and a failed/skipped count.
    counts = re.findall(r"(\d+) (?:failed|skipped)", output)
    if done.returncode != 0 or "\u2717" in output or "ERROR:" in output or any(int(c) for c in counts):
        raise AssertionError(f"CLI failed ({done.returncode}):\n{done.stdout}\n{done.stderr}")
    return done


def extract(cli, view, ndjson, out):
    run_cli(cli, "r4", "run", "--views", str(view), "--input", str(ndjson), "--out", str(out), "--format", "parquet")
    if not out.is_file():
        raise AssertionError(f"no Parquet written for {ndjson.name}")
    return pq.read_table(out)


def write_ndjson(path, resources):
    path.write_text("".join(json.dumps(r) + "\n" for r in resources), encoding="utf-8", newline="\n")


def read_ndjson(path):
    return [json.loads(line, parse_float=Decimal) for line in path.read_text(encoding="utf-8").splitlines()]


# --- the independent oracle: what the source says, read without Ignixa ------

def source_key(path):
    path = path.replace("`", "")
    choice = re.fullmatch(r"(\w+)\.ofType\((\w+)\)", path)
    return choice[1] + choice[2][0].upper() + choice[2][1:] if choice else path


def expected_cell(column, resource):
    value = resource.get(source_key(column["path"]))
    if not column.get("collection"):
        return value
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def actual_cell(column, cell):
    return json.loads(cell, parse_float=Decimal) if column.get("collection") else cell


def column_mismatches(contract, table, resources):
    assert table.num_rows == len(resources), f"{table.num_rows} rows for {len(resources)} resources"
    assert table["id"].to_pylist() == [r["id"] for r in resources]
    bad = []
    for column in contract["projection"]["columns"]:
        cells = table[column["name"]].to_pylist()
        for resource, cell in zip(resources, cells):
            if actual_cell(column, cell) != expected_cell(column, resource):
                bad.append((column["name"], column["path"], resource["id"]))
    return bad


# --- synthetic coverage resources: every extracted column populated at least once ---

EXT = "https://example.test/fhir/StructureDefinition/pilot-label"


def extension(n):
    return [{"url": EXT, "valueString": n}]


def coding(n):
    return {"system": f"https://codes.example.test/{n}", "version": "1", "code": f"{n}-code",
            "display": f"Display {n}", "userSelected": True}


def concept(n):
    return {"id": f"cc-{n}", "extension": extension(n), "coding": [coding(n + "1"), coding(n + "2")], "text": n}


def reference(n):
    return {"reference": f"Thing/{n}", "type": "Thing", "display": n,
            "identifier": {"system": "https://ids.example.test", "value": n, "type": concept(n)}}


def quantity(n):
    return {"id": f"q-{n}", "extension": extension(n), "value": 2.5, "comparator": "<", "unit": "u",
            "system": "http://unitsofmeasure.org", "code": "1"}


def period(n):
    return {"id": f"p-{n}", "extension": extension(n), "start": "2025-01-01", "end": "2025-01-02T03:04:05Z"}


META = {"versionId": "1", "lastUpdated": "2025-01-10T12:00:00.123456Z", "source": "https://s.example.test",
        "profile": ["https://p.example.test/a", "https://p.example.test/b"],
        "security": [coding("sec1"), coding("sec2")], "tag": [coding("tag1"), coding("tag2")],
        "extension": extension("meta")}
COMMON = {
    "language": "en", "implicitRules": "https://rules.example.test", "meta": META,
    "text": {"status": "generated", "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">synthetic</div>"},
    "contained": [{"resourceType": "Organization", "id": "c1", "name": "Synthetic org"},
                  {"resourceType": "Practitioner", "id": "c2", "active": True}],
    "extension": extension("a") + extension("b"),
    "identifier": [{"use": "usual", "system": "https://ids.example.test", "value": "i1", "type": concept("idt"),
                    "period": period("i1"), "assigner": reference("assigner")},
                   {"use": "secondary", "system": "https://ids.example.test", "value": "i2"}],
}
PATIENTS = [
    {**COMMON, "resourceType": "Patient", "id": "cover-1", "active": True, "gender": "other", "birthDate": "1999",
     "deceasedBoolean": True, "multipleBirthInteger": 3,
     "name": [{"use": "official", "text": "Full", "family": "Family", "given": ["One", "Two"], "prefix": ["Dr"],
               "suffix": ["Jr"], "period": period("n1")}, {"family": "Second", "given": ["Only"]}],
     "telecom": [{"system": "phone", "value": "000", "use": "home", "rank": 1, "period": period("t")}],
     "address": [{"use": "home", "type": "both", "text": "T", "line": ["l1", "l2"], "city": "C", "district": "D",
                  "state": "S", "postalCode": "0", "country": "X", "period": period("a")}],
     "maritalStatus": concept("ms"),
     "photo": [{"contentType": "image/png", "language": "en", "data": "AAEC", "url": "https://x.example.test/p",
                "size": 3, "hash": "AAEC", "title": "t", "creation": "2025-01-01T00:00:00Z"}],
     "contact": [{"relationship": [concept("rel")], "name": {"family": "K"}, "telecom": [{"value": "1"}],
                  "address": {"city": "C"}, "gender": "male", "organization": reference("org"),
                  "period": period("c")}],
     "communication": [{"language": concept("lang"), "preferred": True}],
     "generalPractitioner": [reference("gp1"), reference("gp2")],
     "managingOrganization": reference("mo"),
     "link": [{"other": reference("other"), "type": "refer"}]},
    {"resourceType": "Patient", "id": "cover-2", "active": False, "deceasedDateTime": "2024-02-29T01:02:03+10:00",
     "multipleBirthBoolean": False, "birthDate": "2000-02-29", "gender": "female"},
]
OBSERVATIONS = [
    {**COMMON, "resourceType": "Observation", "id": "cover-1", "status": "final",
     "basedOn": [reference("b")], "partOf": [reference("po")], "category": [concept("c1"), concept("c2")],
     "code": concept("code"), "subject": reference("subj"), "focus": [reference("f")],
     "encounter": reference("enc"), "effectiveDateTime": "2025-01-10T12:30:00+01:00",
     "issued": "2025-01-10T12:31:00.123456Z", "performer": [reference("p1"), reference("p2")],
     "valueQuantity": quantity("vq"), "dataAbsentReason": concept("dar"), "interpretation": [concept("int")],
     "note": [{"authorReference": reference("author"), "time": "2025-01-01T00:00:00Z", "text": "note"}],
     "bodySite": concept("bs"), "method": concept("m"), "specimen": reference("sp"), "device": reference("dev"),
     "referenceRange": [{"low": quantity("lo"), "high": quantity("hi"), "type": concept("rt"),
                         "appliesTo": [concept("ap")], "age": {"low": quantity("al"), "high": quantity("ah")},
                         "text": "range"}],
     "hasMember": [reference("hm")], "derivedFrom": [reference("df")],
     "component": [{"code": concept("k1"), "valueQuantity": quantity("k1")},
                   {"code": concept("k2"), "valueString": "component text"}]},
    {"resourceType": "Observation", "id": "cover-2", "status": "final", "valueCodeableConcept": concept("vcc"),
     "effectivePeriod": period("eff")},
    {"resourceType": "Observation", "id": "cover-3", "status": "final", "valueString": "text",
     "effectiveInstant": "2025-01-10T12:30:00.123456Z"},
    {"resourceType": "Observation", "id": "cover-4", "status": "final", "valueBoolean": False,
     "effectiveTiming": {"event": ["2025-01-01T00:00:00Z", "2025-01-02"], "repeat": {"boundsPeriod": period("b"),
                                                                                  "count": 2}, "code": concept("t")}},
    {"resourceType": "Observation", "id": "cover-5", "status": "final", "valueInteger": 0,
     "effectiveDateTime": "2025"},
    {"resourceType": "Observation", "id": "cover-6", "status": "final",
     "valueRange": {"low": quantity("rl"), "high": quantity("rh")}},
    {"resourceType": "Observation", "id": "cover-7", "status": "final",
     "valueRatio": {"numerator": quantity("n"), "denominator": quantity("d")}},
    {"resourceType": "Observation", "id": "cover-8", "status": "final",
     "valueSampledData": {"origin": quantity("o"), "period": 1.5, "factor": 2.5, "lowerLimit": 0.5,
                          "upperLimit": 9.5, "dimensions": 1, "data": "1 2 3"}},
    {"resourceType": "Observation", "id": "cover-9", "status": "final", "valuePeriod": period("vp")},
    {"resourceType": "Observation", "id": "cover-10", "status": "final", "valueTime": "12:30:00"},
    {"resourceType": "Observation", "id": "cover-11", "status": "final", "valueDateTime": "2025-03"},
]
COVERAGE = {"Patient": PATIENTS, "Observation": OBSERVATIONS}


class ContractTests(unittest.TestCase):
    """Offline: no CLI, no .NET."""

    def test_make_view_emits_draft_in_given_order(self):
        view = views.make_view("Patient", [
            {"name": "z", "path": "z", "type": "string"},
            {"name": "a", "path": "a", "type": "string", "collection": True}])
        self.assertEqual(view["status"], "draft")
        self.assertEqual([c["name"] for c in view["select"][0]["column"]], ["z", "a"])
        self.assertEqual(view["select"][0]["column"][1]["collection"], True)
        self.assertNotIn("collection", view["select"][0]["column"][0])

    def test_checked_in_views_are_current(self):
        self.assertEqual(views.problems(), [])

    def test_all_resource_contracts_use_the_published_schema(self):
        schema = json.loads((views.PACK / "format.schema.json").read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        paths = sorted(views.CONTRACT_DIR.glob("*.json"))
        self.assertEqual(len(paths), 146)
        for path in paths:
            with self.subTest(resource=path.stem):
                validator.validate(views.load_contract(path.stem))

    def test_published_schema_rejects_unknown_keys_and_trailing_newlines(self):
        schema = json.loads((views.PACK / "format.schema.json").read_text(encoding="utf-8"))
        validator = Draft202012Validator(schema)
        contract = views.load_contract("Patient")
        for mutate in (
            lambda c: c.update(extra=1),
            lambda c: c["projection"]["columns"][0].update(extra=1),
            lambda c: c["deployment"].update(schemaSha256=c["deployment"]["schemaSha256"] + "\n"),
            lambda c: c["projection"]["columns"][0].update(name="id\n"),
        ):
            mutated = json.loads(json.dumps(contract))
            mutate(mutated)
            self.assertFalse(validator.is_valid(mutated))

    def test_check_rejects_stale_missing_and_extra_definitions(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            shutil.copy(views.VIEW_DIR / "Patient.json", directory)
            (directory / "Patient.json").write_text("{}\n", encoding="utf-8")
            (directory / "notes.txt").write_text("x", encoding="utf-8")
            problems = views.problems(directory)
            self.assertIn("extra definition: notes.txt", problems)
            self.assertIn("missing definition: Observation.json", problems)
            self.assertIn("stale definition: Patient.json", problems)

    def test_pilot_schema_digests_match_original_deployment_measurements(self):
        observed = json.loads((PARITY / "reference" / "deployment.json").read_text(encoding="utf-8"))
        for resource, measurement in observed["tables"].items():
            metadata = views.load_contract(resource)
            self.assertEqual(metadata["deployment"]["schemaSha256"], measurement["schemaSha256"])
            self.assertEqual(metadata["provenance"]["creationLogSha256"], measurement["creationLogSha256"])

    def test_reference_scenario_uses_the_expected_source_bytes(self):
        manifest = json.loads((PARITY / "batches.json").read_text(encoding="utf-8"))
        for item in manifest["inputs"].values():
            with self.subTest(input=item["path"]):
                self.assertEqual(hashlib.sha256((PARITY / item["path"]).read_bytes()).hexdigest(),
                                 item["sha256"])


class CliSelectionTests(unittest.TestCase):
    """Offline: no .NET needed."""

    def test_invalid_explicit_selection_fails_instead_of_skipping(self):
        with mock.patch.dict(os.environ, {"IGNIXA_PARITY_CLI": str(ROOT / "no-such-cli.dll")}):
            with self.assertRaises(AssertionError):
                selected_cli()

    def test_empty_or_blank_explicit_selection_fails(self):
        for value in ("", "   "):
            with self.subTest(value=value), mock.patch.dict(os.environ, {"IGNIXA_PARITY_CLI": value}):
                with self.assertRaises(AssertionError):
                    selected_cli()

    def test_pinned_selection_needs_no_file(self):
        with mock.patch.dict(os.environ, {"IGNIXA_PARITY_CLI": "pinned"}):
            self.assertEqual(selected_cli(), PINNED)

    def test_unset_selection_skips_with_a_reason(self):
        env = {k: v for k, v in os.environ.items() if k != "IGNIXA_PARITY_CLI"}
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(unittest.SkipTest, "IGNIXA_PARITY_CLI is unset"):
                selected_cli()


class PinnedCliBlockerTests(unittest.TestCase):
    """Characterizes the upstream blocker; fails loudly if the pinned CLI is missing."""

    def test_pinned_cli_rounds_wide_scalar_decimals_before_writing_parquet(self):
        wide = "123456789012345678.123456789012345678"
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            source = tmp / "ChargeItem.ndjson"
            source.write_text('{"resourceType":"ChargeItem","id":"wide","factorOverride":' + wide + '}\n',
                              encoding="utf-8")
            table = extract(PINNED, views.VIEW_DIR / "ChargeItem.json", source, tmp / "out.parquet")
        self.assertEqual(table["factorOverride"][0].as_py(), Decimal("123456789012345678.123456789010000000"))

    def test_pinned_cli_loses_complex_collection_content(self):
        source = read_ndjson(PARITY / "input" / "east" / "initial" / "Observation.ndjson")[0]
        view = views.make_view("Observation", [
            {"name": "code_codings", "path": "code.coding", "type": "string", "collection": True}])
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "Observation.json").write_text(views.render(view), encoding="utf-8")
            table = extract(PINNED, tmp / "Observation.json", PARITY / "input" / "east" / "initial" /
                            "Observation.ndjson", tmp / "out.parquet")
        actual = json.loads(table["code_codings"][0].as_py())
        self.assertEqual(len(actual), 2, "the pinned CLI still reports both codings")
        self.assertNotEqual(actual, source["code"]["coding"], "pinned CLI now round-trips; adopt it")
        self.assertEqual(actual, [None, None], "pinned CLI emits null for each complex element")

    def test_all_resource_views_run_and_write_the_declared_parquet_columns(self):
        # This is an engine smoke test, not evidence of populated-field or Silver parity.
        run_cli(PINNED, "r4", "validate", "--views", str(views.VIEW_DIR))
        entries = json.loads((views.PACK / "catalog.json").read_text(encoding="utf-8"))["resources"]
        with tempfile.TemporaryDirectory() as tmp:
            inputs, output = Path(tmp) / "in", Path(tmp) / "out"
            inputs.mkdir()
            output.mkdir()
            for entry in entries:
                r = entry["resource"]
                write_ndjson(inputs / f"{r}.ndjson", [{"resourceType": r, "id": "example", "language": "en"}])
            run_cli(PINNED, "r4", "run", "--views", str(views.VIEW_DIR), "--input", str(inputs),
                    "--input-pattern", "{resource}.ndjson", "--out", str(output), "--format", "parquet")
            self.assertEqual({p.stem for p in output.glob("*.parquet")}, {e["resource"] for e in entries})
            for entry in entries:
                r = entry["resource"]
                with self.subTest(resource=r):
                    table = pq.read_table(output / f"{r}.parquet")
                    columns = views.load_contract(r)["projection"]["columns"]
                    self.assertEqual(table.column_names, [c["name"] for c in columns])
                    self.assertEqual(table["id"].to_pylist(), ["example"])
                    self.assertEqual(table["language"].to_pylist(), ["en"])


class ExtractionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cli = selected_cli()

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)

    def run_view(self, resource, ndjson):
        out = self.tmp / f"{resource}-{abs(hash(str(ndjson)))}.parquet"
        return extract(self.cli, views.VIEW_DIR / f"{resource}.json", ndjson, out)

    def test_two_codings_read_back_complete_and_ordered(self):
        source = read_ndjson(PARITY / "input" / "east" / "initial" / "Observation.ndjson")[0]
        view = views.make_view("Observation", [
            {"name": "code_codings", "path": "code.coding", "type": "string", "collection": True}])
        (self.tmp / "Observation.json").write_text(views.render(view), encoding="utf-8")
        output = extract(self.cli, self.tmp / "Observation.json",
                         PARITY / "input" / "east" / "initial" / "Observation.ndjson", self.tmp / "o.parquet")
        actual = json.loads(output["code_codings"][0].as_py())
        self.assertEqual(len(actual), 2)
        self.assertEqual(actual, source["code"]["coding"])

    def test_generated_views_validate(self):
        run_cli(self.cli, "r4", "validate", "--views", str(views.VIEW_DIR))

    def test_non_pilot_projections_preserve_populated_source_fields(self):
        examples = {
            "Condition": {"clinicalStatus": {"coding": [coding("active"), coding("other")]},
                          "onsetDateTime": "2025-03"},
            "Encounter": {"status": "finished", "class": coding("AMB"),
                          "participant": [{"individual": {"reference": "Practitioner/p1"}}]},
            "MedicationRequest": {"status": "active", "intent": "order",
                                  "medicationReference": {"reference": "Medication/m1"}},
        }
        for resource, fields in examples.items():
            with self.subTest(resource=resource):
                source = {"resourceType": resource, "id": "example", **fields}
                path = self.tmp / f"{resource}.ndjson"
                write_ndjson(path, [source])
                table = self.run_view(resource, path)
                columns = views.load_contract(resource)["projection"]["columns"]
                by_key = {source_key(c["path"]): c for c in columns}
                for key in fields:
                    column = by_key[key]
                    self.assertEqual(actual_cell(column, table[column["name"]][0].as_py()),
                                     expected_cell(column, source), f"{resource}.{key}")

    def test_each_manifest_file_extracts_separately_without_loss(self):
        manifest = json.loads((PARITY / "batches.json").read_text(encoding="utf-8"))
        for name, entry in manifest["inputs"].items():
            with self.subTest(file=name):
                path = PARITY / entry["path"]
                resources = read_ndjson(path)
                table = self.run_view(entry["resource"], path)
                contract = views.load_contract(entry["resource"])
                self.assertEqual(column_mismatches(contract, table, resources), [])

    def test_shared_patient_id_stays_one_row_per_source_file(self):
        east = self.run_view("Patient", PARITY / "input" / "east" / "initial" / "Patient.ndjson")
        west = self.run_view("Patient", PARITY / "input" / "west" / "initial" / "Patient.ndjson")
        self.assertEqual((east["id"].to_pylist(), west["id"].to_pylist()), (["p-shared"], ["p-shared"]))
        self.assertEqual((east["birthDate"].to_pylist(), west["birthDate"].to_pylist()), (["2000-02-29"], ["1999-04"]))

    def test_fixture_values_keep_their_exact_source_form(self):
        patient = self.run_view("Patient", PARITY / "input" / "east" / "initial" / "Patient.ndjson").to_pylist()[0]
        names = json.loads(patient["name"])
        self.assertEqual(names[0]["given"], ["Synthetic", "Alpha"])
        identifiers = json.loads(patient["identifier"])
        self.assertEqual([i["value"] for i in identifiers], ["p-shared", "east-002"])
        self.assertEqual(json.loads(patient["extension"])[0]["valueString"], "synthetic-east")
        self.assertEqual(patient["multipleBirthInteger"], 2)
        self.assertEqual(json.loads(patient["meta"])[0]["lastUpdated"], "2025-01-10T10:00:00.123456Z")

        observation = self.run_view(
            "Observation", PARITY / "input" / "east" / "initial" / "Observation.ndjson").to_pylist()[0]
        quantity_json = json.loads(observation["valueQuantity"], parse_float=Decimal)
        self.assertEqual(quantity_json[0]["value"], Decimal("36.123456789012345678"))
        self.assertEqual(quantity_json[0]["extension"][0]["valueString"], "synthetic-quantity")
        self.assertEqual(observation["effectiveDateTime"], "2025-01-10T12:30:00+01:00")
        self.assertIsNone(observation["valueString"])

    def test_absent_values_are_null_or_empty_collections(self):
        patient = self.run_view("Patient", PARITY / "input" / "west" / "initial" / "Patient.ndjson").to_pylist()[0]
        self.assertEqual((patient["identifier"], patient["name"], patient["extension"]), ("[]", "[]", "[]"))
        self.assertIsNone(patient["active"])
        self.assertEqual(patient["multipleBirthBoolean"], False)
        observation = self.run_view(
            "Observation", PARITY / "input" / "west" / "initial" / "Observation.ndjson").to_pylist()[0]
        self.assertEqual(json.loads(observation["effectivePeriod"]), [{"start": "2025-01-10", "end": "2025-01-11"}])
        self.assertEqual(json.loads(observation["code"]), [{"text": "Synthetic label"}])
        self.assertEqual(observation["valueString"], "fixture label")
        self.assertEqual(observation["valueQuantity"], "[]")

    def test_decimal_text_is_preserved_including_trailing_zero_and_wide_values(self):
        path = self.tmp / "decimals.ndjson"
        path.write_text(
            '{"resourceType":"Observation","id":"d","valueQuantity":{"value":1.50}}\n'
            '{"resourceType":"Observation","id":"e","valueQuantity":'
            '{"value":100000000000000000000.123456789012345678}}\n', encoding="utf-8", newline="\n")
        cells = self.run_view("Observation", path)["valueQuantity"].to_pylist()
        self.assertEqual(cells, ['[{"value":1.50}]', '[{"value":100000000000000000000.123456789012345678}]'])

    def test_every_extracted_column_survives_populated_resources(self):
        for resource, resources in COVERAGE.items():
            with self.subTest(resource=resource):
                path = self.tmp / f"{resource}.ndjson"
                write_ndjson(path, resources)
                table = self.run_view(resource, path)
                contract = views.load_contract(resource)
                self.assertEqual(column_mismatches(contract, table, resources), [])


if __name__ == "__main__":
    unittest.main()
