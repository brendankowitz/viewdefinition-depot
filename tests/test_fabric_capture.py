"""Offline format and comparison tests. All data are temporary, not HDS evidence."""

import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = ROOT / "contracts/fabric-hds/1.4.0/r4/capture.schema.json"
UTC = "2025-01-01T12:00:00Z"
SHA = "a" * 64
TABLE = "ExampleTable"  # Deliberately not the FHIR resource name.
ARROW_SCHEMA = pa.schema([(k, pa.string()) for k in ("msftSourceSystem", "idOrig", "msftFilePath")])
FIELDS = b'["msftSourceSystem",true,"string"],["idOrig",true,"string"],["msftFilePath",true,"string"]'
DIGEST = hashlib.sha256(b"[" + FIELDS + b"]").hexdigest()


def manifest():
    return {
        "captureFormatVersion": "1", "captureId": "unit-only",
        "deployment": {
            "fhirVersion": "4.0.1", "hdsVersion": "1.4.0", "release": "unit-release",
            "artifactSha256": {"release/artifact": SHA}, "configurationSha256": {"config/file": SHA},
            "runtime": {"platform": "unit", "sparkVersion": "1", "deltaVersion": "2", "javaVersion": "3"},
            "sessionTimeZone": "UTC",
        },
        "inputs": {"east": {
            "path": "input.ndjson", "sha256": hashlib.sha256(b"unit input\n").hexdigest(),
            "sourceToken": "east-drop", "callerNamespace": "https://east.example.test/fhir",
            "sourceSystem": "unit-east", "logicalPath": "fixture/east/input.ndjson",
            "observedFilePath": "Files/east-drop/input.ndjson",
        }},
        "events": [{
            "name": "first-load", "inputIds": ["east"], "runWindowUtc": {"start": UTC, "end": UTC},
            "resources": [{
                "resource": "Example", "table": TABLE, "deltaVersion": 4, "rowCount": 1,
                "schemaSha256": DIGEST, "parquetSha256": SHA, "rowsPath": "rows/example.ndjson",
            }],
        }],
    }


class FormatTests(unittest.TestCase):
    def test_published_schema_defines_manifest_and_row_records(self):
        from jsonschema import Draft202012Validator

        self.assertTrue(SCHEMA_FILE.is_file(), "missing versioned capture schema")
        schema = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        validator.validate(manifest())
        self.assertFalse(validator.is_valid({"values": {}}))
        row = validator.evolve(schema={"$ref": "#/$defs/rowRecord"})
        row.validate({"values": {"nullable": None, "nested": [2, 1]}, "timeBounds": {
            "msftCreatedDatetime": {"min": UTC, "max": UTC},
        }})
        for invalid in ({}, {"values": []}, {"values": {}, "deltaVersion": 1},
                        {"values": {}, "timeBounds": {"meta_lastUpdatedOrig": {"min": UTC, "max": UTC}}}):
            with self.subTest(invalid=invalid):
                self.assertFalse(row.is_valid(invalid))


class CaptureTests(unittest.TestCase):
    def setUp(self):
        from fabric_silver import capture

        self.capture = capture
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.archive = self.root / "capture"
        self.archive.mkdir()
        self.path = self.archive / "capture.json"
        self.candidates = self.root / "candidate"
        self.candidate = self.candidates / "first-load" / f"{TABLE}.parquet"
        self.candidate.parent.mkdir(parents=True)
        self.row = {"msftSourceSystem": "unit-east", "idOrig": "private-unit-id",
                    "msftFilePath": "Files/east-drop/input.ndjson"}
        pq.write_table(pa.Table.from_pylist([self.row], schema=ARROW_SCHEMA), self.candidate)
        self.records = self.archive / "rows/example.ndjson"
        self.records.parent.mkdir()
        self.records.write_text('{"values":{"msftSourceSystem":"unit-east","idOrig":"private-unit-id",'
                                '"msftFilePath":"Files/east-drop/input.ndjson"}}\n', encoding="utf-8")
        (self.archive / "input.ndjson").write_bytes(b"unit input\n")
        resources = self.root / "contracts/resources"
        resources.mkdir(parents=True)
        self.contract = resources / "Example.json"
        self.contract.write_text(json.dumps({
            "contractFormatVersion": "1", "resource": "Example", "table": TABLE,
            "deployment": {"schemaSha256": DIGEST},
        }), encoding="utf-8")
        self.enterContext(patch.object(capture, "RESOURCE_DIR", resources))

    def cli(self, doc, *args):
        self.path.write_text(json.dumps(doc), encoding="utf-8")
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = self.capture.main([str(self.path), str(self.candidates), *args])
        return code, stdout.getvalue(), stderr.getvalue()

    def reject(self, doc, reason=None):
        code, out, err = self.cli(doc)
        self.assertNotEqual(code, 0, out)
        self.assertTrue(err)
        if reason:
            self.assertIn(reason, err)
        self.assertNotIn("Traceback", err)
        for private in ("private-unit-id", "sig=secret", "user:password"):
            self.assertNotIn(private, err)

    def test_declared_table_matches_and_report_records_how_to_repeat_comparison(self):
        report_path = self.root / "comparison.json"
        code, _, err = self.cli(manifest(), "--report", str(report_path))
        self.assertEqual(code, 0, err)
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(report["captureId"], "unit-only")
        self.assertEqual(report["result"], "matched")
        self.assertIn("fabric_silver.capture", report["command"])
        self.assertIn(str(self.path), report["command"])
        self.assertIn(str(self.candidates), report["command"])
        self.assertEqual(report["command"][-2:], ["--report", str(report_path)])
        self.assertIn("<event>/<table>.parquet", report["candidateLayout"])
        self.assertEqual([(s["event"], s["table"], s["rowCount"]) for s in report["snapshots"]],
                         [("first-load", TABLE, 1)])
        self.assertNotIn("private-unit-id", str(report))

    def test_comparison_does_not_overwrite_a_snapshot_or_write_a_success_report_on_mismatch(self):
        before = self.candidate.read_bytes()
        code, _, _ = self.cli(manifest(), "--report", str(self.candidate))
        self.assertNotEqual(code, 0)
        self.assertEqual(self.candidate.read_bytes(), before)
        self.records.write_text('{"values":{}}', encoding="utf-8")
        report = self.root / "should-not-exist.json"
        code, _, _ = self.cli(manifest(), "--report", str(report))
        self.assertNotEqual(code, 0)
        self.assertFalse(report.exists())

    def test_only_well_formed_provenanced_events_are_compared(self):
        for path, invalid in [
            (["captureFormatVersion"], "2"), (["deployment", "release"], " "),
            (["deployment", "runtime"], {"platform": "Spark"}),
            (["deployment", "sessionTimeZone"], "Europe/Paris"),
            (["deployment", "configurationSha256"], {}), (["deployment", "artifactSha256"], {"x": "bad"}),
            (["deployment", "configurationSha256"], {"../config.json": SHA}),
            (["inputs", "east", "logicalPath"], "file://server/file"),
            (["inputs", "east", "sourceToken"], "Files/east"), (["inputs", "east", "sourceToken"], ".."),
            (["inputs", "east", "sourceSystem"], ""), (["inputs", "east", "sha256"], SHA),
            (["events", 0, "inputIds"], ["missing"]), (["events", 0, "inputIds"], []),
            (["events", 0, "runWindowUtc", "start"], "2025-01-01T12:00:00+01:00"),
            (["events", 0, "runWindowUtc", "start"], "2025-02-30T12:00:00Z"),
            (["events", 0, "runWindowUtc", "start"], "2025-01-01T12:00:01Z"),
            (["events", 0, "resources", 0, "parquetSha256"], None),
            (["events", 0, "resources", 0, "deltaVersion"], -1),
            (["events", 0, "resources", 0, "rowCount"], 0),
        ]:
            with self.subTest(path=path, invalid=invalid):
                doc = manifest()
                node = doc
                for key in path[:-1]:
                    node = node[key]
                node[path[-1]] = invalid
                self.reject(doc)

    def test_observed_stock_binding_not_caller_namespace_or_path_token_is_compared(self):
        doc = manifest()
        entry = doc["inputs"]["east"]
        for substitute in (entry["callerNamespace"], entry["sourceToken"]):
            with self.subTest(substitute=substitute):
                entry["sourceSystem"] = substitute  # Both files still agree; provenance does not.
                self.reject(doc)
        entry["sourceSystem"] = "unit-east"
        entry["observedFilePath"] = "Files/west/input.ndjson"
        self.reject(doc)
        entry["observedFilePath"] = "Files/east-drop/input.ndjson"
        del entry["callerNamespace"]  # Not every independent operator uses the depot's caller fixture.
        code, _, err = self.cli(doc)
        self.assertEqual(code, 0, err)
        del entry["sourceToken"]
        self.reject(doc)

    def test_onelake_lineage_is_opaque_and_never_rewritten_to_the_fixture_path(self):
        doc = manifest()
        entry = doc["inputs"]["east"]
        for uri in ("abfss://demo@onelake.dfs.fabric.microsoft.com/lake.Lakehouse/Files/east/a.ndjson",
                    "https://onelake.dfs.fabric.microsoft.com/demo/lake.Lakehouse/Files/east/a%20b.ndjson"):
            with self.subTest(uri=uri):
                entry["observedFilePath"] = uri
                row = {**self.row, "msftFilePath": uri}
                pq.write_table(pa.Table.from_pylist([row], schema=ARROW_SCHEMA), self.candidate)
                self.records.write_text(json.dumps({"values": row}), encoding="utf-8")
                code, _, err = self.cli(doc)
                self.assertEqual(code, 0, err)
                entry["observedFilePath"] = entry["logicalPath"]
                self.reject(doc)
                entry["observedFilePath"] = uri
                pq.write_table(pa.Table.from_pylist([{**row, "msftFilePath": entry["logicalPath"]}],
                                                     schema=ARROW_SCHEMA), self.candidate)
                self.reject(doc)

    def test_only_credential_free_onelake_uris_are_allowed_as_absolute_observations(self):
        for uri in ("https://user:password@onelake.dfs.fabric.microsoft.com/demo/lake/f",
                    "https://onelake.dfs.fabric.microsoft.com/demo/lake/f?sig=secret",
                    "abfss://demo:key@onelake.dfs.fabric.microsoft.com/lake/f",
                    "abfss://demo@onelake.dfs.fabric.microsoft.com/lake/f#fragment",
                    "https://onelake.dfs.fabric.microsoft.com.example.test/demo/lake/f",
                    "file:///C:/x", "abfss://demo@onelake.dfs.fabric.microsoft.com/lake/../other"):
            with self.subTest(uri=uri):
                doc = manifest()
                doc["inputs"]["east"]["observedFilePath"] = uri
                # Use the same value in rows/candidate. Failure must come from URI validation.
                row = {**self.row, "msftFilePath": uri}
                pq.write_table(pa.Table.from_pylist([row], schema=ARROW_SCHEMA), self.candidate)
                self.records.write_text(json.dumps({"values": row}), encoding="utf-8")
                self.reject(doc)

    def test_paths_must_be_local_relative_even_for_lineage_that_is_not_opened(self):
        for path in ["../outside", "/tmp/outside", "//server/share", "C:/absolute", "C:drive",
                     "rows\\..\\outside", "https://host/file", "file:stream", "rows/../input.ndjson",
                     "Files/NUL.json"]:
            with self.subTest(path=path):
                doc = manifest()
                doc["inputs"]["east"]["logicalPath"] = path  # Never opened; syntax alone must reject it.
                self.reject(doc)

    def test_resolved_rows_and_candidates_must_stay_inside_their_own_root(self):
        def link_directory(link, target):
            if os.name == "nt":
                # Junctions don't need the Windows symbolic-link privilege.
                subprocess.run(["cmd", "/d", "/c", "mklink", "/J", str(link), str(target)],
                               check=True, capture_output=True)
            else:
                link.symlink_to(target, target_is_directory=True)

        rows = self.records.parent
        outside = rows.rename(self.root / "outside-rows")
        link_directory(rows, outside)
        self.reject(manifest())
        if os.name == "nt":
            rows.rmdir()
        else:
            rows.unlink()
        outside.rename(rows)
        event = self.candidate.parent
        outside = event.rename(self.root / "outside-snapshots")
        link_directory(event, outside)
        self.reject(manifest())

    def test_duplicate_events_or_resources_are_not_counted_twice(self):
        doc = manifest()
        doc["events"] *= 2
        self.reject(doc)
        doc = manifest()
        doc["events"][0]["resources"] *= 2
        self.reject(doc)

    def test_contract_identity_and_deployed_digest_are_independent_of_both_inputs(self):
        original = json.loads(self.contract.read_text(encoding="utf-8"))
        for change in ({"contractFormatVersion": "2"}, {"resource": "Wrong"}, {"table": "Wrong"},
                       {"deployment": {"schemaSha256": SHA}}):
            with self.subTest(change=change):
                self.contract.write_text(json.dumps({**original, **change}), encoding="utf-8")
                self.reject(manifest())
        self.contract.unlink()
        self.reject(manifest())

    def test_missing_snapshot_or_malformed_or_incorrect_row_never_produces_a_match(self):
        for text in ("", "[]", '{"values":{},"unknown":true}', '{"values":NaN}',
                     '{"values":{"msftSourceSystem":"unit-east","idOrig":"wrong"}}'):
            with self.subTest(text=text):
                self.records.write_text(text, encoding="utf-8")
                self.reject(manifest())
        self.records.write_text(json.dumps({"values": self.row}), encoding="utf-8")
        self.candidate.unlink()
        self.reject(manifest())

    def test_explicit_zero_count_matches_only_an_empty_table_with_the_deployed_schema(self):
        doc = manifest()
        doc["events"][0]["resources"][0]["rowCount"] = 0
        self.records.write_bytes(b"")
        self.reject(doc)
        pq.write_table(pa.Table.from_pylist([], schema=ARROW_SCHEMA), self.candidate)
        code, _, err = self.cli(doc)
        self.assertEqual(code, 0, err)
        pq.write_table(pa.Table.from_pylist([], schema=ARROW_SCHEMA.remove(1)), self.candidate)
        self.reject(doc)

    def test_table_versions_are_monotonic_and_a_reused_version_has_immutable_rows(self):
        doc = manifest()
        first = doc["events"][0]
        replay = deepcopy(first)
        replay["name"] = "replay"
        first_table = first["resources"][0]
        replay_table = replay["resources"][0]
        replay_table["rowsPath"] = "rows/replay.ndjson"
        doc["events"].append(replay)
        replay_parquet = self.candidates / "replay" / self.candidate.name
        replay_parquet.parent.mkdir()
        rows = [self.row, {**self.row, "idOrig": "second"}]

        def write(snapshot, parquet, values):
            snapshot["rowCount"] = len(values)
            (self.archive / snapshot["rowsPath"]).write_text(
                "".join(json.dumps({"values": v}) + "\n" for v in values), encoding="utf-8")
            pq.write_table(pa.Table.from_pylist(values, schema=ARROW_SCHEMA), parquet)

        write(first_table, self.candidate, rows)
        write(replay_table, replay_parquet, rows[::-1])
        code, _, err = self.cli(doc)
        self.assertEqual(code, 0, err)
        for version, next_rows in [(3, rows), (4, rows[:1]), (4, [self.row, self.row])]:
            with self.subTest(version=version, size=len(next_rows)):
                replay_table["deltaVersion"] = version
                write(replay_table, replay_parquet, next_rows)
                self.reject(doc)
        replay_table["deltaVersion"] = 5
        code, _, err = self.cli(doc)
        self.assertEqual(code, 0, err)

    def test_retained_clock_may_use_earlier_run_window_but_must_remain_equal_on_replay(self):
        field = "msftCreatedDatetime"
        schema = ARROW_SCHEMA.append(pa.field(field, pa.timestamp("us", tz="UTC")))
        digest = hashlib.sha256(b"[" + FIELDS + b',["msftCreatedDatetime",true,"timestamp[us, tz=UTC]"]]').hexdigest()
        contract = json.loads(self.contract.read_text(encoding="utf-8"))
        contract["deployment"]["schemaSha256"] = digest
        self.contract.write_text(json.dumps(contract), encoding="utf-8")
        doc = manifest()
        initial = doc["events"][0]
        initial["resources"][0]["schemaSha256"] = digest
        end = "2025-01-01T12:00:05Z"
        initial["runWindowUtc"]["end"] = end
        record = {"values": {**self.row, field: UTC}, "timeBounds": {field: {"min": UTC, "max": end}}}
        self.records.write_text(json.dumps(record), encoding="utf-8")
        again = deepcopy(initial)
        doc["inputs"]["west"] = {**doc["inputs"]["east"], "sourceToken": "west-drop",
                                 "sourceSystem": "unit-west", "observedFilePath": "Files/west-drop/input.ndjson"}
        again.update(name="rerun", inputIds=["west"], runWindowUtc={"start": end, "end": end})
        again["resources"][0]["rowsPath"] = "rows/rerun.ndjson"
        doc["events"].append(again)
        replay_rows = self.records.with_name("rerun.ndjson")
        replay_rows.write_bytes(self.records.read_bytes())
        replay = self.candidates / "rerun" / self.candidate.name
        replay.parent.mkdir()

        def write_clock(path, stamp):
            pq.write_table(pa.Table.from_pylist([{**self.row, field: datetime.fromisoformat(stamp)}],
                                                schema=schema), path)
        write_clock(self.candidate, UTC)
        write_clock(replay, UTC)
        code, _, err = self.cli(doc)
        self.assertEqual(code, 0, err)
        write_clock(replay, end)  # Inside the same bounds, but no longer a retained clock.
        self.reject(doc)
        write_clock(replay, UTC)
        record["timeBounds"][field]["max"] = "2030-01-01T00:00:00Z"
        replay_rows.write_text(json.dumps(record), encoding="utf-8")
        self.reject(doc)

    def test_module_cli_fails_cleanly_when_no_capture_exists(self):
        proc = subprocess.run([sys.executable, "-m", "fabric_silver.capture", str(self.root / "absent"),
                               str(self.candidates)], cwd=ROOT, capture_output=True, text=True)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("capture", proc.stderr)
        self.assertNotIn("Traceback", proc.stderr)

    def test_module_cli_rejects_wrong_candidate_schema_at_the_actual_deployed_table_name(self):
        # Negative integration with the real contract directory; no HDS rows or original export.
        resource = "MedicinalProductIndication"
        contract = json.loads((SCHEMA_FILE.parent / "resources" / f"{resource}.json").read_text(encoding="utf-8"))
        table = "MedicinalProductIndictation"
        doc = manifest()
        doc["events"][0]["resources"][0].update(resource=resource, table=table,
                                                schemaSha256=contract["deployment"]["schemaSha256"])
        self.path.write_text(json.dumps(doc), encoding="utf-8")
        self.candidate.rename(self.candidate.with_name(f"{table}.parquet"))
        proc = subprocess.run([sys.executable, "-m", "fabric_silver.capture", str(self.path),
                               str(self.candidates)], cwd=ROOT, capture_output=True, text=True)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn(f"{table}: schema digest differs", proc.stderr)
