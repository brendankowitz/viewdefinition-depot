"""Catalog evidence gate and harness mechanics only, not HDS evidence."""

import hashlib
import json
import re
import shutil
import tempfile
import unittest
from functools import cache
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from fabric_silver.reference import compare_events, snapshot_values
from fabric_silver.views import VIEW_DIR, contract_view, render
from fabric_silver.writer import apply_batch
from test_fabric_views import PINNED, column_mismatches, extract, read_ndjson as read_fhir
from test_fabric_writer import CONTRACTS, DEPLOYED, PARITY, RESOURCES, ROOT, TIMES, synthetic_rows

MANIFEST = PARITY / "batches.json"
REFERENCE = PARITY / "reference"
CATALOG = CONTRACTS / "catalog.json"
STATUSES = {"verified", "implemented-unverified", "not-implemented", "blocked"}


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_ndjson(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def write_ndjson(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")


def released_cli_snapshots(workspace, manifest, events):
    """Run only when a complete independent capture is available; never use a dev DLL."""
    extracted = workspace / "extracted"
    extracted.mkdir()
    for input_id, item in manifest["inputs"].items():
        resource = item["resource"]
        source = PARITY / item["path"]
        if hashlib.sha256(source.read_bytes()).hexdigest() != item["sha256"]:
            raise AssertionError(f"{input_id}: fixture digest differs")
        result = extract(PINNED, VIEW_DIR / f"{resource}.json", source, extracted / f"{input_id}.parquet")
        mismatches = column_mismatches(read_json(CONTRACTS / f"{resource}.json"), result, read_fhir(source))
        if mismatches:
            raise AssertionError(f"{input_id}: released CLI extraction differs from fixture: {mismatches}")
    return {event: apply_batch(MANIFEST, event, extracted, workspace / "state", ingest_time=TIMES[event])
            for event in events}


def evidence_status(entry, contracts, views, reference, events, snapshots):
    resource, table = entry["resource"], entry["table"]
    contract_path, view_path = contracts / f"{resource}.json", views / f"{resource}.json"
    if not contract_path.is_file() and not view_path.is_file():
        return "not-implemented", "missing contract and generated view"
    if not contract_path.is_file():
        return "blocked", "missing contract"
    if not view_path.is_file():
        return "blocked", "missing generated view"
    contract = read_json(contract_path)
    if contract["resource"] != resource or contract["unsupported"]:
        return "blocked", "contract identity differs or has unsupported mappings"
    if view_path.read_text(encoding="utf-8") != render(contract_view(contract)):
        return "blocked", "stale generated view"
    deployed = read_json(reference / "deployment.json")["tables"]
    digest = deployed.get(table, {}).get("schemaSha256")
    if not digest:
        return "implemented-unverified", "missing deployed schema digest"
    expectations = [reference / event / f"{table}.ndjson" for event in events]
    for event, expected in zip(events, expectations):
        if not expected.is_file():
            return "implemented-unverified", f"missing {event} reference snapshot"
    actual = snapshots()
    pairs = [(actual[event] / f"{resource}.parquet", expected)
             for event, expected in zip(events, expectations)]
    try:
        compare_events(pairs, digest)
    except ValueError as error:
        return "implemented-unverified", str(error)
    return "verified", "released extraction, deployed schema and captured rows agree"


def check_statuses(entries, contracts, views, reference, events, snapshots):
    statuses = []
    for entry in entries:
        status, detail = evidence_status(entry, contracts, views, reference, events, snapshots)
        claimed = entry["status"]
        if claimed != status:
            raise AssertionError(f"{entry['resource']}: claimed {claimed} but derived {status}: {detail}")
        statuses.append(status)
    return statuses


class CatalogTests(unittest.TestCase):
    def load_catalog(self):
        self.assertTrue(CATALOG.is_file(), "missing parity catalog.json")
        return read_json(CATALOG)

    def test_catalog_inventory(self):
        catalog = self.load_catalog()
        entries = catalog["resources"]
        canonical = {entry["resource"]: entry["table"] for entry in entries}
        self.assertEqual(len(entries), 146)
        self.assertEqual(len(canonical), len(entries), "duplicate canonical names")
        self.assertEqual(len(set(canonical.values())), len(entries), "duplicate deployed names")
        self.assertFalse({"Resource", "DomainResource"} & canonical.keys())
        self.assertEqual({r: t for r, t in canonical.items() if r != t}, {
            "MedicinalProductContraindication": "MedicinalProductContraindiction",
            "MedicinalProductIndication": "MedicinalProductIndictation",
        })
        for entry in entries:
            with self.subTest(resource=entry["resource"]):
                self.assertIn(entry["status"], STATUSES)
                if entry["status"] != "verified":
                    self.assertTrue(entry.get("reason") or catalog["statusReasons"].get(entry["status"]))

    def test_catalog_status_and_published_counts_match_evidence(self):
        entries = self.load_catalog()["resources"]
        manifest = read_json(MANIFEST)
        events = [event["name"] for event in manifest["events"]]
        with tempfile.TemporaryDirectory() as directory:
            snapshots = cache(lambda: released_cli_snapshots(Path(directory), manifest, events))
            statuses = check_statuses(entries, CONTRACTS, VIEW_DIR, REFERENCE, events, snapshots)
        count = f"{statuses.count('verified')}/{len(entries)}"
        for path in (ROOT / "README.md", ROOT / "docs/packs/fabric-silver-parity.md"):
            with self.subTest(path=path.name):
                text = path.read_text(encoding="utf-8")
                published = re.findall(r"\b(\d+/\d+) (?:resources )?verified\b", text)
                self.assertTrue(published, "missing published verified count")
                self.assertEqual(set(published), {count})


class HarnessMechanicsTests(unittest.TestCase):
    """Writer-derived expectations in temp only; never independent capture."""

    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)
        self.reference = self.root / "reference"
        self.events = [event["name"] for event in read_json(MANIFEST)["events"]]
        extracted = self.root / "extracted"
        extracted.mkdir()
        rows = synthetic_rows()
        rows["east-observation-1"]["code"] = json.dumps([{"coding": [
            {"system": "http://loinc.org", "code": "8310-5"},
            {"system": "https://codes.example.test/pilot", "code": "TEMP"},
        ]}])
        for input_id, row in rows.items():
            pq.write_table(pa.Table.from_pylist([row]), extracted / f"{input_id}.parquet")
        self.snapshots = {
            event: apply_batch(MANIFEST, event, extracted, self.root / "state", ingest_time=TIMES[event])
            for event in self.events
        }
        for resource in RESOURCES:
            for actual, expected in self.pairs(resource):
                values = snapshot_values(actual, DEPLOYED[resource]["schemaSha256"])
                write_ndjson(expected, ({"values": row} for row in values))

    def pairs(self, resource):
        return [(self.snapshots[event] / f"{resource}.parquet",
                 self.reference / event / f"{resource}.ndjson") for event in self.events]

    def compare(self, resource):
        compare_events(self.pairs(resource), DEPLOYED[resource]["schemaSha256"])

    def mutate_initial_observation(self, mutate):
        _, expected = self.pairs("Observation")[0]
        records = read_ndjson(expected)
        east, = [entry["values"] for entry in records
                 if entry["values"]["msftSourceSystem"] == "https://east.example.test/fhir"]
        mutate(east)
        write_ndjson(expected, records)

    def test_writer_snapshots_compare_across_all_three_events(self):
        for resource in RESOURCES:
            with self.subTest(resource=resource):
                self.compare(resource)

    def test_removed_coding_fails_nested_value_comparison(self):
        self.mutate_initial_observation(lambda row: row["code"]["coding"].pop())
        with self.assertRaisesRegex(ValueError, r"^Observation\.code\.coding: no matching row$"):
            self.compare("Observation")

    def test_altered_reference_target_fails_nested_value_comparison(self):
        self.mutate_initial_observation(lambda row: row["subject"].update(id="different-target-id"))
        with self.assertRaisesRegex(ValueError, r"^Observation\.subject\.id: no matching row$"):
            self.compare("Observation")

    def test_changed_nested_type_fails_schema_comparison(self):
        actual, _ = self.pairs("Patient")[0]
        table = pq.read_table(actual)
        # Keep the same top-level column name but alter a struct member.
        index = table.schema.get_field_index("name")
        names = table.schema.field(index).type
        members = [pa.field(f.name, pa.large_string()) if f.name == "family" else f
                   for f in names.value_type]
        changed = pa.list_(pa.field(names.value_field.name, pa.struct(members)))
        pq.write_table(table.set_column(index, "name", table.column(index).cast(changed)), actual)
        with self.assertRaisesRegex(ValueError, r"^Patient: schema digest differs$"):
            self.compare("Patient")

    def test_dropped_expected_snapshot_fails_at_missing_expectation(self):
        _, expected = self.pairs("Observation")[1]
        expected.unlink()
        with self.assertRaisesRegex(ValueError, r"^Observation: invalid or missing expectation records$"):
            self.compare("Observation")

    def test_catalog_rejects_claim_when_any_required_artifact_is_missing(self):
        resource = "Patient"
        contracts = self.root / "contracts"
        views = self.root / "views"
        contracts.mkdir()
        views.mkdir()
        contract = contracts / f"{resource}.json"
        view = views / f"{resource}.json"
        shutil.copyfile(CONTRACTS / contract.name, contract)
        shutil.copyfile(VIEW_DIR / view.name, view)
        catalog = contracts / "catalog.json"
        catalog.write_text(json.dumps({"resources": [
            {"resource": resource, "table": resource, "status": "verified"},
        ]}), encoding="utf-8")
        entries = read_json(catalog)["resources"]
        shutil.copyfile(REFERENCE / "deployment.json", self.reference / "deployment.json")
        # Positive control: the mechanical gate accepts these complete *test* artifacts.
        args = (entries, contracts, views, self.reference, self.events, lambda: self.snapshots)
        self.assertEqual(check_statuses(*args), ["verified"])
        for artifact, message in (
            (contract, r"missing contract"),
            (view, r"missing generated view"),
            (self.pairs(resource)[2][1], r"missing update reference snapshot"),
        ):
            with self.subTest(artifact=artifact.name):
                data = artifact.read_bytes()
                artifact.unlink()
                with self.assertRaisesRegex(AssertionError, rf"Patient: claimed verified .*{message}"):
                    check_statuses(*args)
                artifact.write_bytes(data)
