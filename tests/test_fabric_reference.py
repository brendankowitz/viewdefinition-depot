"""Comparator unit data only. These rows are not an HDS capture."""

import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from fabric_silver import reference
from fabric_silver.reference import compare_snapshot, schema_digest


CODING = pa.struct([("system", pa.string()), ("code", pa.string())])
SCHEMA = pa.schema([
    ("msftSourceSystem", pa.string()),
    ("idOrig", pa.string()),
    ("code", pa.struct([("coding", pa.list_(CODING))])),
    ("code_string", pa.string()),
    ("amount", pa.decimal128(36, 18)),
    ("day", pa.date32()),
    ("meta_lastUpdatedOrig", pa.timestamp("us", tz="UTC")),
    ("msftCreatedDatetime", pa.timestamp("us", tz="UTC")),
    ("msftModifiedDatetime", pa.timestamp("us", tz="UTC")),
])
TIME = datetime(2025, 1, 1, tzinfo=timezone.utc)
STAMP = "2025-01-01T00:00:00Z"


def test_row():
    return {
        "msftSourceSystem": "unit-east", "idOrig": "not-for-diagnostics",
        "code": {"coding": [{"system": "unit", "code": "A"}, {"system": "unit", "code": "B"}]},
        "code_string": '{"coding": []}', "amount": Decimal("123456789012345678.123456789012345678"),
        "day": date(2024, 2, 29), "meta_lastUpdatedOrig": TIME,
        "msftCreatedDatetime": TIME, "msftModifiedDatetime": TIME,
    }


def expectation():
    return {"values": {
        **test_row(), "amount": "123456789012345678.123456789012345678", "day": "2024-02-29",
        "meta_lastUpdatedOrig": STAMP, "msftCreatedDatetime": STAMP, "msftModifiedDatetime": STAMP,
    }}


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def files(self, actual, expected, schema=SCHEMA, event="initial"):
        directory = self.root / event
        directory.mkdir(exist_ok=True)
        parquet, ndjson = directory / "Observation.parquet", directory / "Observation.ndjson"
        pq.write_table(pa.Table.from_pylist(actual, schema=schema), parquet)
        ndjson.write_text("".join(json.dumps(row) + "\n" for row in expected), encoding="utf-8")
        return parquet, ndjson

    def compare(self, actual, expected, schema=SCHEMA):
        compare_snapshot(*self.files(actual, expected, schema), schema_digest(SCHEMA))

    def test_detects_changed_second_coding_without_disclosing_values(self):
        row = test_row()
        row["code"]["coding"][1]["code"] = "do-not-log-this"
        with self.assertRaisesRegex(ValueError, r"Observation.*code\.coding") as error:
            self.compare([row], [expectation()])
        for private in ("do-not-log-this", "not-for-diagnostics", "unit-east"):
            self.assertNotIn(private, str(error.exception))

    def test_array_order_is_significant(self):
        row = test_row()
        row["code"]["coding"].reverse()
        with self.assertRaisesRegex(ValueError, "Observation.*coding"):
            self.compare([row], [expectation()])

    def test_preserves_decimal_digits_instant_date_and_exact_json_text(self):
        self.compare([test_row()], [expectation()])
        for field, corrupted in [
            ("amount", "123456789012345678.123456789012345679"),
            ("meta_lastUpdatedOrig", "2025-01-01T00:00:00.000001Z"),
            ("day", "2024-02-28"), ("code_string", '{"coding":[]}'),
        ]:
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, f"Observation.*{field}"):
                ref = expectation()
                ref["values"][field] = corrupted
                self.compare([test_row()], [ref])

    def test_missing_expected_nested_member_is_not_null(self):
        row = test_row()
        row["code"]["coding"][1]["code"] = None
        ref = expectation()
        del ref["values"]["code"]["coding"][1]["code"]
        with self.assertRaisesRegex(ValueError, "Observation.*coding"):
            self.compare([row], [ref])

    def test_rejects_schema_drift_even_if_rows_do_not_exercise_field(self):
        for name, replacement in [
            ("amount", pa.decimal128(38, 18)), ("amount", pa.decimal128(36, 17)),
            ("code", pa.struct([pa.field("coding", pa.list_(CODING), nullable=False)])),
            ("code", pa.struct([("coding", pa.list_(pa.field("element", CODING, nullable=False)))])),
            ("code", None),
        ]:
            with self.subTest(replacement=replacement), self.assertRaisesRegex(ValueError, "Observation.*schema"):
                i = SCHEMA.get_field_index(name)
                changed = SCHEMA.remove(i) if replacement is None else SCHEMA.set(i, pa.field(name, replacement))
                self.compare([{**test_row(), "amount": None}], [expectation()], changed)

    def test_schema_order_matters_but_writer_metadata_and_list_label_do_not(self):
        f = pa.field("codes", pa.list_(pa.field("item", pa.string(), metadata={b"x": b"y"})))
        g = pa.field("value", pa.int32(), metadata={b"writer": b"v1"})
        clean = pa.schema([("codes", pa.list_(pa.field("element", pa.string()))), ("value", pa.int32())])
        self.assertEqual(schema_digest(pa.schema([f, g], metadata={b"w": b"v"})), schema_digest(clean))
        self.assertNotEqual(schema_digest(pa.schema([g, f])), schema_digest(clean))

    def test_compares_keyed_multisets_not_row_order_or_last_value(self):
        a, b = test_row(), {**test_row(), "amount": None}
        ea, eb = expectation(), expectation()
        eb["values"]["amount"] = None
        self.compare([a, b, a], [ea, ea, eb])
        with self.assertRaisesRegex(ValueError, "Observation.*amount"):
            self.compare([a, b, b], [ea, ea, eb])
        with self.assertRaisesRegex(ValueError, "Observation.*row count"):
            self.compare([a, b, a], [ea, eb])

    def test_does_not_merge_equal_local_ids_across_namespaces(self):
        east, west = test_row(), {**test_row(), "msftSourceSystem": "unit-west"}
        with self.assertRaisesRegex(ValueError, "Observation.*row count"):
            self.compare([east, west], [expectation(), expectation()])

    def test_empty_reference_is_not_pilot_evidence(self):
        with self.assertRaisesRegex(ValueError, "Observation.*row count"):
            self.compare([], [])

    def test_duplicate_expectation_field_is_not_silently_overwritten(self):
        path, ref = self.files([test_row()], [expectation()])
        text = ref.read_text(encoding="utf-8").replace('"idOrig":', '"idOrig":"do-not-log","idOrig":')
        ref.write_text(text, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Observation") as error:
            compare_snapshot(path, ref, schema_digest(SCHEMA))
        self.assertNotIn("do-not-log", str(error.exception))

    def test_unrepresentable_parquet_instant_is_a_table_error(self):
        path, ref = self.files([test_row()], [expectation()])
        table = pq.ParquetFile(path).read()
        field = SCHEMA.field("meta_lastUpdatedOrig")
        invalid = pa.array([2**63 - 1], type=field.type)
        pq.write_table(table.set_column(SCHEMA.get_field_index(field.name), field, invalid), path)
        with self.assertRaisesRegex(ValueError, "Observation"):
            compare_snapshot(path, ref, schema_digest(SCHEMA))

    def test_bounds_are_inclusive_and_do_not_mask_source_timestamps(self):
        ref = expectation()
        ref["timeBounds"] = {"msftModifiedDatetime": {"min": STAMP, "max": "2025-01-01T00:00:01Z"}}
        for us in (0, 1_000_000, 1_000_001):
            row = {**test_row(), "msftModifiedDatetime": TIME.replace(second=us // 1_000_000, microsecond=us % 1_000_000)}
            if us <= 1_000_000:
                self.compare([row], [ref])
            else:
                with self.assertRaisesRegex(ValueError, "Observation.*msftModifiedDatetime"):
                    self.compare([row], [ref])
        ref["timeBounds"]["meta_lastUpdatedOrig"] = {"min": STAMP, "max": STAMP}
        with self.assertRaisesRegex(ValueError, "Observation.*meta_lastUpdatedOrig"):
            self.compare([test_row()], [ref])

    def test_overlapping_clock_windows_require_distinct_rows_not_greedy_pairing(self):
        early, late = test_row(), {**test_row(), "msftModifiedDatetime": TIME.replace(second=1)}
        wide, narrow = expectation(), expectation()
        wide["timeBounds"] = {"msftModifiedDatetime": {"min": STAMP, "max": "2025-01-01T00:00:01Z"}}
        narrow["timeBounds"] = {"msftModifiedDatetime": {"min": STAMP, "max": STAMP}}
        self.compare([early, late], [wide, narrow])
        with self.assertRaisesRegex(ValueError, "Observation.*msftModifiedDatetime"):
            self.compare([late, late], [wide, narrow])

    def test_deployed_schema_uses_delta_metadata_even_for_never_populated_fields(self):
        # An invented unit-test Delta action, not HDS metadata.
        element = {"name": "amount", "type": "decimal(36,18)", "nullable": True, "metadata": {}}
        arr = {"type": "array", "elementType": {"type": "struct", "fields": [element]}, "containsNull": False}
        delta = {"type": "struct", "fields": [
            {"name": "a", "type": arr, "nullable": False, "metadata": {}},
            {"name": "t", "type": "timestamp", "nullable": True, "metadata": {}},
        ]}
        log = self.root / "00000000000000000000.json"
        log.write_text(json.dumps({"protocol": {}}) + "\n" + json.dumps({
            "metaData": {"schemaString": json.dumps(delta)}
        }), encoding="utf-8")
        item = pa.struct([("amount", pa.decimal128(36, 18))])
        expected = pa.schema([pa.field("a", pa.list_(pa.field("element", item, nullable=False)), nullable=False),
                              ("t", pa.timestamp("us", tz="UTC"))])
        self.assertEqual(reference.deployed_schema(log), expected)

    def test_can_encode_capture_values_only_with_the_independent_schema_digest(self):
        path, _ = self.files([test_row()], [expectation()])
        self.assertEqual(reference.snapshot_values(path, schema_digest(SCHEMA)), [expectation()["values"]])
        with self.assertRaisesRegex(ValueError, "Observation.*schema"):
            reference.snapshot_values(path, schema_digest(SCHEMA.remove(0)))

    def test_clock_equality_and_order_between_events_are_not_just_bounds(self):
        def event(name, expected_second, actual_second):
            ref = expectation()
            row = {**test_row(), "msftCreatedDatetime": TIME.replace(microsecond=7),
                   "msftModifiedDatetime": TIME.replace(second=actual_second, microsecond=7)}
            ref["values"]["msftModifiedDatetime"] = f"2025-01-01T00:00:0{expected_second}Z"
            ref["timeBounds"] = {field: {"min": STAMP, "max": "2025-01-01T00:00:05Z"}
                                 for field in ("msftCreatedDatetime", "msftModifiedDatetime")}
            return self.files([row], [ref], event=name)

        initial, update = event("initial", 0, 0), event("update", 2, 3)
        def check(replay):
            reference.compare_events([initial, replay, update], schema_digest(SCHEMA))

        check(event("replay", 0, 0))
        for expected_second, actual_second in [(0, 1), (1, 0), (1, 4)]:
            with self.subTest(expected_second=expected_second, actual_second=actual_second):
                replay = event("replay", expected_second, actual_second)
                # All snapshots are individually in bounds, even for this broken replay.
                compare_snapshot(*replay, schema_digest(SCHEMA))
                with self.assertRaisesRegex(ValueError, "Observation.*msftModifiedDatetime"):
                    check(replay)

    def test_ranked_duplicate_cannot_borrow_another_rows_clock_window(self):
        rows = [{**test_row(), "msftModifiedDatetime": TIME.replace(second=s)} for s in (2, 3)]
        refs = [expectation(), expectation()]
        for ref, second, last in zip(refs, (1, 2), (3, 2)):
            ref["values"]["msftModifiedDatetime"] = f"2025-01-01T00:00:0{second}Z"
            ref["timeBounds"] = {"msftModifiedDatetime": {"min": STAMP, "max": f"2025-01-01T00:00:0{last}Z"}}
        pairs = [self.files(rows, refs, event=e) for e in ("initial", "replay", "update")]
        for pair in pairs:
            compare_snapshot(*pair, schema_digest(SCHEMA))
        with self.assertRaisesRegex(ValueError, "Observation.*msftModifiedDatetime"):
            reference.compare_events(pairs, schema_digest(SCHEMA))

    def test_one_snapshot_cannot_stand_in_for_three_exports(self):
        pair = self.files([test_row()], [expectation()])
        with self.assertRaisesRegex(ValueError, "sequence"):
            reference.compare_events([pair, pair, pair], schema_digest(SCHEMA))


class IngressTests(unittest.TestCase):
    def test_both_runners_can_replay_the_same_bytes_and_namespace_then_update_east(self):
        pack = Path(__file__).resolve().parents[1] / "examples/r4/fabric-silver-hds"
        manifest = json.loads((pack / "batches.json").read_text(encoding="utf-8"))
        inputs = manifest["inputs"]
        self.assertEqual([e["name"] for e in manifest["events"]], ["initial", "replay", "update"])
        initial, replay, update = (e["inputs"] for e in manifest["events"])
        east, west = (f"https://{name}.example.test/fhir" for name in ("east", "west"))
        self.assertEqual(replay, [k for k in initial if inputs[k]["sourceSystem"] == east])
        self.assertEqual(len(update), 1)
        self.assertNotIn(update[0], initial)
        self.assertEqual(set(initial + update), set(inputs))
        self.assertEqual(len(inputs), len({f["logicalPath"] for f in inputs.values()}))

        resources = {}
        for key, f in inputs.items():
            self.assertNotEqual(f["path"], f["logicalPath"])
            data = (pack / f["path"]).read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(), f["sha256"], key)
            resources[key] = [json.loads(line, parse_float=Decimal) for line in data.decode("utf-8").splitlines()]
            self.assertTrue(resources[key])
            for row in resources[key]:
                self.assertEqual(row["resourceType"], f["resource"])
                self.assertEqual(row["meta"]["source"], f["sourceSystem"])

        patients = {inputs[k]["sourceSystem"]: resources[k][0] for k in initial if inputs[k]["resource"] == "Patient"}
        self.assertEqual(set(patients), {east, west})
        pid = patients[east]["id"]
        self.assertEqual(pid, patients[west]["id"])
        self.assertNotIn("identifier", patients[west])
        self.assertGreaterEqual(len({(i["system"], i["value"]) for i in patients[east]["identifier"]}), 2)
        self.assertEqual(len(set(patients[east]["name"][0]["given"])), 2)
        new = resources[update[0]][0]
        self.assertEqual((inputs[update[0]]["sourceSystem"], new["resourceType"], new["id"]), (east, "Patient", pid))
        self.assertGreater(int(new["meta"]["versionId"]), int(patients[east]["meta"]["versionId"]))
        self.assertGreater(datetime.fromisoformat(new["meta"]["lastUpdated"]),
                           datetime.fromisoformat(patients[east]["meta"]["lastUpdated"]))

        observations = [r for k in initial if inputs[k]["resource"] == "Observation" for r in resources[k]]
        self.assertEqual({r["subject"]["reference"] for r in observations}, {f"Patient/{pid}", f"{west}/Patient/{pid}"})
        self.assertEqual({tuple(k for k in r if k.startswith("value")) for r in observations},
                         {("valueQuantity",), ("valueString",)})
        self.assertTrue(any(len({(c["system"], c["code"]) for c in r["code"].get("coding", [])}) >= 2
                            for r in observations))
