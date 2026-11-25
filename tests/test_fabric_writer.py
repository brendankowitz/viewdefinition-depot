"""Test publication and event semantics, not captured HDS row parity.

Merge expectations are source-derived, unverified against a captured run:
HDS utils/dataframe_utils.py, upsert_unique_to_delta_managed and prepare_source_data.
"""

import json
import os
import subprocess
import sys
import tempfile
import time
import traceback
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from fabric_silver.normalize import ShapeError, shape_row
from fabric_silver.reference import schema_digest
from fabric_silver.views import VIEW_DIR
from fabric_silver.writer import BatchError, apply_batch
from test_fabric_views import PARITY, ROOT, expected_cell, extract, read_ndjson

RESOURCES = ("Patient", "Observation")
TIMES = {event: datetime(2026, 2, 1, hour, tzinfo=UTC)
         for event, hour in (("initial", 10), ("replay", 11), ("update", 12))}
CONTRACTS = ROOT / "contracts" / "fabric-hds" / "1.4.0" / "parity"
EXTRACTION = {r: json.loads((CONTRACTS / f"{r}.json").read_text(encoding="utf-8"))["extraction"]
              for r in RESOURCES}
DEPLOYED = json.loads((PARITY / "reference" / "deployment.json").read_text(encoding="utf-8"))["tables"]


def row(resource, **values):
    return {**{col["column"]: "[]" if col.get("collection") else None
               for col in EXTRACTION[resource]}, **values}


def meta(timestamp, version):
    return json.dumps([{"lastUpdated": timestamp, "versionId": version}])


def synthetic_rows():
    return {
        "east-patient-1": row("Patient", id="shared", active=True, birthDate="2000-02-29",
                              name='[{"family":"First","given":["A","B"]}]',
                              meta=meta("2025-01-10T10:00:00Z", "1")),
        "west-patient-1": row("Patient", id="shared", meta=meta("2025-01-10T11:00:00Z", "1")),
        "east-observation-1": row("Observation", id="measurement", status="final",
                                  meta=meta("2025-01-10T12:00:00Z", "1"),
                                  subject='[{"reference":"Patient/shared"}]',
                                  valueQuantity='[{"value":36.123456789012345678}]'),
        "west-observation-1": row("Observation", id="measurement", status="final",
                                  meta=meta("2025-01-10T12:00:00Z", "1"), valueString="synthetic"),
        "east-patient-2": row("Patient", id="shared", active=False, birthDate="2000-02-29",
                              name='[{"family":"Second","given":["A","B"]}]',
                              meta=meta("2025-01-11T10:00:00Z", "2")),
    }


class Workspace(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.extracted = self.root / "extracted"
        self.extracted.mkdir()
        self.state = self.root / "state"
        self.manifest = self.root / "batches.json"
        self.spec = json.loads((PARITY / "batches.json").read_text(encoding="utf-8"))

    def save_manifest(self):
        self.manifest.write_text(json.dumps(self.spec), encoding="utf-8")

    def write_rows(self, input_id, *rows):
        pq.write_table(pa.Table.from_pylist(list(rows)), self.extracted / f"{input_id}.parquet")

    def apply(self, event, *, state=None, instant=None):
        return apply_batch(self.manifest, event, self.extracted, state or self.state,
                           ingest_time=TIMES[event] if instant is None else instant)

    def cli(self, event, instant=None):
        return subprocess.run(
            [sys.executable, "-m", "fabric_silver", "--manifest", str(self.manifest),
             "--event", event, "--extracted", str(self.extracted), "--state", str(self.state),
             "--ingest-time", TIMES[event].isoformat() if instant is None else instant],
            cwd=ROOT, text=True, encoding="utf-8", capture_output=True, timeout=20,
        )

    def current(self):
        return self.state / "snapshots" / (self.state / "CURRENT").read_text(encoding="utf-8").strip()

    def snapshot_rows(self, snapshot):
        self.assertEqual({p.name for p in snapshot.iterdir()}, {f"{r}.parquet" for r in RESOURCES})
        result = {}
        for resource in RESOURCES:
            file = snapshot / f"{resource}.parquet"
            schema = pq.read_schema(file)
            self.assertEqual(len(schema), DEPLOYED[resource]["fields"])
            self.assertEqual(schema_digest(schema), DEPLOYED[resource]["schemaSha256"])
            rows = pq.read_table(file).to_pylist()
            self.assertEqual([r["id"] for r in rows], sorted(r["id"] for r in rows))
            result[resource] = rows
        return result

    def assert_event_rows(self, snapshot, event, witness):
        # This is an explicit event matrix, not a second merge. Shape the independently authored
        # extraction witnesses only to specify all non-merge fields at the writer boundary.
        # source-derived, unverified against a captured run: utils/dataframe_utils.py,
        # upsert_unique_to_delta_managed.
        ids = {
            "initial": ["east-patient-1", "west-patient-1", "east-observation-1", "west-observation-1"],
            "replay": ["east-patient-1", "west-patient-1", "east-observation-1", "west-observation-1"],
            "update": ["east-patient-2", "west-patient-1", "east-observation-1", "west-observation-1"],
        }[event]
        actual = self.snapshot_rows(snapshot)
        for resource in RESOURCES:
            expected = {}
            for input_id in ids:
                item = self.spec["inputs"][input_id]
                if item["resource"] != resource:
                    continue
                modified = TIMES["update"] if input_id == "east-patient-2" else TIMES["initial"]
                shaped = shape_row(resource, witness[input_id], source_system=item["sourceSystem"],
                                   logical_path=item["logicalPath"], ingest_time=TIMES["initial"])
                shaped.update(msftCreatedDatetime=TIMES["initial"], msftModifiedDatetime=modified,
                              msftFilePath=item["logicalPath"], msftSourceSystem=item["sourceSystem"])
                expected[(shaped["msftSourceSystem"], shaped["idOrig"])] = shaped
            self.assertEqual(len(actual[resource]), 2)
            self.assertEqual(len({r["id"] for r in actual[resource]}), 2)
            self.assertEqual({(r["msftSourceSystem"], r["idOrig"]): r for r in actual[resource]}, expected)
        self.assertEqual(len({r["idOrig"] for r in actual["Patient"]}), 1)


class HermeticWriterTests(Workspace):
    def setUp(self):
        super().setUp()
        self.witness = synthetic_rows()
        self.save_manifest()
        for input_id, data in self.witness.items():
            self.write_rows(input_id, data)

    def test_each_event_in_a_new_process_checks_both_tables(self):
        for event in TIMES:
            with self.subTest(event=event):
                result = self.cli(event, TIMES[event].isoformat().replace("+00:00", "Z"))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stderr, "")
                self.assertEqual(Path(result.stdout.strip()), self.current())
                self.assert_event_rows(self.current(), event, self.witness)
        self.assertEqual(len(list((self.state / "snapshots").iterdir())), 3)

    def test_bad_second_resource_is_redacted_and_never_published(self):
        secret = "SYNTHETIC-PRIVATE-SENTINEL"
        bad = "bad-observation"
        self.spec["inputs"][bad] = self.spec["inputs"]["east-observation-1"].copy()
        self.spec["events"][2]["inputs"].append(bad)
        self.save_manifest()
        malformed = dict(self.witness["east-observation-1"],
                         performer='[{"display":"ok"},{"identifier":{"type":"' + secret + '"}}]')
        self.write_rows(bad, malformed)
        prior = self.apply("initial")
        current = (self.state / "CURRENT").read_bytes()
        tables = {file.name: file.read_bytes() for file in prior.iterdir()}
        try:
            self.apply("update")
        except ShapeError as exc:
            self.assertEqual((exc.resource, exc.path, exc.reason),
                             ("Observation", "performer[1].identifier.type", "is not JSON object text"))
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)
            for message in (str(exc), repr(exc.args), traceback.format_exc()):
                self.assertNotIn(secret, message)
        else:
            self.fail("malformed Observation did not fail the batch")
        result = self.cli("update")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertIn("Observation performer[1].identifier.type", result.stderr)
        self.assertIn("is not JSON object text", result.stderr)
        self.assertNotIn(secret, result.stdout + result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertEqual((self.state / "CURRENT").read_bytes(), current)
        self.assertEqual({file.name: file.read_bytes() for file in prior.iterdir()}, tables)
        self.assertEqual(list((self.state / "snapshots").iterdir()), [prior])
        self.assertEqual({p.name for p in self.state.iterdir()}, {"CURRENT", "snapshots"})
        later = self.cli("replay")
        self.assertEqual(later.returncode, 0, later.stderr)
        self.assert_event_rows(self.current(), "replay", self.witness)

    def test_latest_candidate_wins_and_older_event_cannot_replace_it(self):
        self.spec["events"][0]["inputs"] = ["east-patient-2", "east-patient-1"]
        self.save_manifest()
        original = self.snapshot_rows(self.apply("initial"))
        self.assertEqual(original["Observation"], [])
        self.assertEqual(len(original["Patient"]), 1)
        patient = original["Patient"][0]
        self.assertEqual(patient["meta"]["versionId"], "2")
        self.assertEqual(patient["msftFilePath"], self.spec["inputs"]["east-patient-2"]["logicalPath"])
        self.spec["events"][1]["inputs"] = ["east-patient-1"]
        self.save_manifest()
        self.assertEqual(self.snapshot_rows(self.apply("replay")), original)

    def test_null_last_updated_fails_even_with_another_valid_candidate(self):
        valid = self.witness["east-patient-1"]
        self.write_rows("east-patient-1", valid, dict(valid, meta="[]"))
        with self.assertRaisesRegex(ShapeError, r"Patient meta\.lastUpdated:.*required") as caught:
            self.apply("initial")
        self.assertEqual(caught.exception.path, "meta.lastUpdated")
        self.assertFalse((self.state / "CURRENT").exists())

    def test_tied_nonwinning_versions_fail_across_input_files(self):
        alias = "same-age"
        self.spec["inputs"][alias] = self.spec["inputs"]["east-patient-1"].copy()
        self.spec["events"][0]["inputs"] = ["east-patient-1", "east-patient-2", alias]
        self.save_manifest()
        self.write_rows(alias, dict(self.witness["east-patient-1"], active=False))
        with self.assertRaisesRegex(ShapeError, r"Patient meta\.lastUpdated:.*duplicate"):
            self.apply("initial")
        self.assertFalse((self.state / "CURRENT").exists())

    def test_file_errors_name_the_input_but_do_not_print_file_contents(self):
        path = self.extracted / "east-patient-1.parquet"
        path.unlink()
        missing = self.cli("initial")
        self.assertEqual(missing.returncode, 1, missing.stderr)
        self.assertIn("east-patient-1: missing extracted Parquet", missing.stderr)
        path.write_text("PRIVATE-FILE-BYTES", encoding="utf-8")
        unreadable = self.cli("initial")
        self.assertEqual(unreadable.returncode, 1, unreadable.stderr)
        self.assertIn("east-patient-1: unreadable extracted Parquet", unreadable.stderr)
        self.assertNotIn("PRIVATE-FILE-BYTES", unreadable.stderr + unreadable.stdout)
        (self.extracted / "unknown.parquet").touch()
        extra = self.cli("initial")
        self.assertEqual(extra.returncode, 1, extra.stderr)
        self.assertIn("unknown: extra extracted file", extra.stderr)
        self.assertFalse((self.state / "CURRENT").exists())

    def test_cli_rejects_invalid_clocks_without_echoing_input(self):
        for instant in ("2026-02-01T10:00:00", "2026-02-01T11:00:00+01:00", "PRIVATE-TIME"):
            with self.subTest(instant=instant):
                result = self.cli("initial", instant)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("expected an ISO-8601 UTC instant", result.stderr)
                self.assertNotIn(instant, result.stderr + result.stdout)
        # In-process validation is independent of whether this event has any rows.
        self.spec["events"][0]["inputs"] = []
        self.save_manifest()
        with self.assertRaisesRegex(ValueError, "ingest_time must be a timezone-aware datetime"):
            self.apply("initial", instant=datetime(2026, 2, 1))
        self.assertFalse((self.state / "CURRENT").exists())

    def test_same_clock_and_inputs_produce_identical_snapshot_bytes(self):
        first = self.apply("initial")
        before = time.time_ns()
        time.sleep(0.03)
        second = self.apply("initial", state=self.root / "another-state")
        self.assertGreater(time.time_ns(), before)
        for resource in RESOURCES:
            self.assertEqual((first / f"{resource}.parquet").read_bytes(),
                             (second / f"{resource}.parquet").read_bytes())

    def test_pointer_write_failure_preserves_current_and_cleans_staging(self):
        prior = self.apply("initial")
        pointer = (self.state / "CURRENT").read_bytes()
        with mock.patch("fabric_silver.writer.os.replace", side_effect=PermissionError("private detail")):
            with self.assertRaisesRegex(BatchError, "^state: could not publish snapshot$"):
                self.apply("update")
        self.assertEqual((self.state / "CURRENT").read_bytes(), pointer)
        self.assertEqual(list((self.state / "snapshots").iterdir()), [prior])
        self.assertEqual({p.name for p in self.state.iterdir()}, {"CURRENT", "snapshots"})
        self.assert_event_rows(prior, "initial", self.witness)

    def test_invalid_parquet_readback_never_changes_current(self):
        write_table = pq.write_table
        for defect in ("schema", "rows"):
            with self.subTest(defect=defect):
                self.state = self.root / defect
                prior = self.apply("initial")

                def corrupt(table, path, **kwargs):
                    if path.name == "Observation.parquet":
                        table = table.drop(["msftModifiedDatetime"]) if defect == "schema" else table.slice(0, 1)
                    return write_table(table, path, **kwargs)

                with mock.patch("fabric_silver.writer.pq.write_table", side_effect=corrupt):
                    with self.assertRaisesRegex(BatchError, f"Observation: staged Parquet {defect} differ"):
                        self.apply("update")
                self.assertEqual(self.current(), prior)
                self.assertEqual(list((self.state / "snapshots").iterdir()), [prior])
                self.assertEqual({p.name for p in self.state.iterdir()}, {"CURRENT", "snapshots"})

    def test_unexpected_shaping_error_is_not_reclassified(self):
        with mock.patch("fabric_silver.writer.shape_row", side_effect=RuntimeError("programming bug")):
            with self.assertRaisesRegex(RuntimeError, "programming bug"):
                self.apply("initial")
        self.assertFalse((self.state / "CURRENT").exists())


def development_cli():
    choice = os.environ.get("IGNIXA_PARITY_CLI")
    if choice is None:
        raise unittest.SkipTest("IGNIXA_PARITY_CLI is unset; set it to the development CLI DLL")
    dll = Path(choice)
    if not choice.strip() or dll.suffix.lower() != ".dll" or not dll.is_file():
        raise AssertionError("IGNIXA_PARITY_CLI must select an existing development DLL; "
                             "'pinned' is accepted only by the extraction suite")
    return ["dotnet", str(dll.resolve())]


class DevelopmentExtractionTests(Workspace):
    def test_ignixa_output_drives_all_three_writer_events(self):
        cli = development_cli()
        self.save_manifest()
        witness = {}
        for input_id, item in self.spec["inputs"].items():
            resource = item["resource"]
            source = PARITY / item["path"]
            extract(cli, VIEW_DIR / f"{resource}.json", source,
                    self.extracted / f"{input_id}.parquet")
            [raw] = read_ndjson(source)
            cells = {}
            for col in EXTRACTION[resource]:
                name = col["column"]
                cell = expected_cell(col, raw)
                if name == "valueQuantity" and cell:
                    # Hand-written decimal token: never convert the source decimal through float.
                    cells[name] = ('[{"value":36.123456789012345678,"unit":"degrees Celsius",'
                                   '"system":"http://unitsofmeasure.org","code":"Cel","extension":'
                                   '[{"url":"https://example.test/fhir/StructureDefinition/pilot-label",'
                                   '"valueString":"synthetic-quantity"}]}]')
                else:
                    cells[name] = json.dumps(cell) if col.get("collection") else cell
            witness[input_id] = cells
        for event in TIMES:
            with self.subTest(event=event):
                self.assert_event_rows(self.apply(event), event, witness)
