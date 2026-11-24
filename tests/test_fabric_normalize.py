"""Silver shaping and normalization tests for the Fabric HDS v1.4.0 parity pilot.

There is no captured HDS run. Schema tests compare against the real deployed
Silver Delta schemas (recorded digests, plus the HDS build artifact when it is
present). Every value expectation below was derived by hand from reading the
HDS normalization source *before* the implementation was written, and is
labelled "source-derived, unverified against a captured run" with the HDS
file/function it follows. None of these tests establish verified parity.

These tests never need .NET or the Ignixa CLI: extracted rows are written by
hand in the shape of the extraction views' columns.
"""
from __future__ import annotations

import hashlib
import json
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from fabric_silver.normalize import ShapeError, shape_row, target_schema
from fabric_silver.reference import deployed_schema, schema_digest

ROOT = Path(__file__).resolve().parents[1]
PARITY = ROOT / "contracts" / "fabric-hds" / "1.4.0" / "parity"
FIXTURES = ROOT / "examples" / "r4" / "fabric-silver-parity"
HDS_SILVER = Path(r"E:\Downloads\HDS v1.4.0\HDS.SourceCode\hds-build-artifacts\healthcare-tables\1.4.0\Silver")

EAST = "https://east.example.test/fhir"
WEST = "https://west.example.test/fhir"
UTC = timezone.utc
INGEST = datetime(2025, 1, 12, 9, 0, 0, tzinfo=UTC)

DIGESTS = {
    "Patient": (51, "b9c4bc459f85b485bf2570526969125e823801700ab1d42d022f69cb69c0c582"),
    "Observation": (89, "cedf46756c6c3360cc93716222c8ff40fa218ab465011c7132943a5e8bd0e82f"),
}

UNVERIFIED = "source-derived, unverified against a captured run"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def utc(*parts) -> datetime:
    return datetime(*parts, tzinfo=UTC)


def extraction_columns(resource: str) -> list[dict]:
    return json.loads((PARITY / f"{resource}.json").read_text(encoding="utf-8"))["extraction"]


def _dumps(value) -> str:
    """Compact JSON that keeps Decimal source text (the views keep number text)."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return "{" + ",".join(json.dumps(k) + ":" + _dumps(v) for k, v in value.items()) + "}"
    if isinstance(value, list):
        return "[" + ",".join(_dumps(v) for v in value) + "]"
    return json.dumps(value, ensure_ascii=False)


def extracted_from_resource(resource: str, fhir: dict) -> dict:
    """Mimic the extraction views for a FHIR JSON resource (no CLI needed)."""
    row = {}
    for c in extraction_columns(resource):
        name = c["column"]
        value = fhir.get(name)
        if name in COLLECTION_COLUMNS[resource]:
            if value is None:
                row[name] = "[]"
            else:
                row[name] = _dumps(value if isinstance(value, list) else [value])
        else:
            row[name] = value
    return row


def ndjson(path: str) -> list[dict]:
    text = (FIXTURES / path).read_text(encoding="utf-8")
    return [json.loads(line, parse_float=Decimal) for line in text.splitlines() if line.strip()]


SCALARS = {
    "Patient": {"id", "language", "implicitRules", "active", "gender", "birthDate", "deceasedBoolean",
                "deceasedDateTime", "multipleBirthBoolean", "multipleBirthInteger"},
    "Observation": {"id", "language", "implicitRules", "status", "effectiveDateTime", "effectiveInstant", "issued",
                    "valueString", "valueBoolean", "valueInteger", "valueTime", "valueDateTime"},
}
COLLECTION_COLUMNS = {r: {c["column"] for c in extraction_columns(r)} - SCALARS[r] for r in SCALARS}


def blank_row(resource: str, **columns) -> dict:
    row = {c["column"]: ("[]" if c["column"] in COLLECTION_COLUMNS[resource] else None) for c in extraction_columns(resource)}
    row["id"] = "x1"
    row.update(columns)
    return row


def shape(resource: str, source: str = EAST, **columns) -> dict:
    return shape_row(resource, blank_row(resource, **columns), source_system=source,
                     logical_path=f"Files/test/{resource}.ndjson", ingest_time=INGEST)


# ---- hand-derived constant shapes -------------------------------------------------------------

FHIR_ID_TYPE = {
    "extension": None, "id": None,
    "coding": [{"extension": None, "id": None, "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                "version": None, "code": "fhirId", "display": "FHIR Id", "userSelected": None}],
    "text": "FHIR Id",
}
NULL_CC = {"extension": None, "id": None, "coding": None, "text": None}
NULL_PERIOD = {"extension": None, "id": None, "start": None, "end": None}
NULL_RAW_REF = {"extension": None, "id": None, "reference": None, "type": None, "identifier": None, "display": None}
# A non-null reference without identifier: conform_to_row_identifier_schema turns type/period/assigner into {}.
CONFORMED_EMPTY_ID = {"extension": None, "use": None, "value": None, "system": None,
                      "type": NULL_CC, "period": NULL_PERIOD, "assigner": NULL_RAW_REF}
# normalize_ref_struct(None): the early-return defaults.
NULL_INPUT_REF = {
    "reference": None, "type": None,
    "identifier": {"extension": None, "use": None, "value": None, "system": None, "type": None, "period": None, "assigner": None},
    "display": None, "extension": None, "id": None, "idOrig": None, "msftSourceReference": None,
}
# The fhirId element's all-null assigner after the nested identifier/assigner reference rule.
FHIR_ID_ASSIGNER = {"reference": None, "type": None, "identifier": CONFORMED_EMPTY_ID, "display": None,
                    "extension": None, "id": None, "idOrig": None, "msftSourceReference": None}


def fhir_id_element(value: str, system: str) -> dict:
    return {"extension": None, "use": None, "value": value, "system": system, "type": FHIR_ID_TYPE,
            "period": NULL_PERIOD, "assigner": FHIR_ID_ASSIGNER}


def local_ref(type_: str, rid: str, source: str, original: str, display=None) -> dict:
    return {
        "reference": None, "type": type_,
        "identifier": {"extension": None, "use": None, "value": rid, "system": source, "type": FHIR_ID_TYPE,
                       "period": NULL_PERIOD, "assigner": NULL_RAW_REF},
        "display": display, "extension": None,
        "id": sha(f"{source.split('://', 1)[1]}/{type_}/{rid}"), "idOrig": rid, "msftSourceReference": original,
    }


class SchemaTests(unittest.TestCase):
    def test_target_schema_matches_recorded_deployed_digest(self):
        for resource, (count, digest) in DIGESTS.items():
            with self.subTest(resource=resource):
                schema = target_schema(resource)
                self.assertEqual(len(schema), count)
                self.assertEqual(schema_digest(schema), digest)

    def test_target_schema_equals_deployed_delta_log(self):
        for resource in DIGESTS:
            log = HDS_SILVER / resource / "_delta_log" / "00000000000000000000.json"
            if not log.exists():
                self.skipTest("HDS build artifacts are not present on this machine")
            with self.subTest(resource=resource):
                self.assertTrue(target_schema(resource).equals(deployed_schema(log), check_metadata=False))

    def test_unknown_resource_is_rejected(self):
        with self.assertRaises(ShapeError):
            target_schema("Encounter")

    def test_shaped_rows_have_exactly_the_silver_columns_and_convert_to_arrow(self):
        for resource in DIGESTS:
            with self.subTest(resource=resource):
                row = shape(resource)
                schema = target_schema(resource)
                self.assertEqual(list(row), schema.names)
                table = pa.Table.from_pylist([row], schema=schema)
                self.assertEqual(table.to_pylist()[0]["id"], row["id"])


class IdentifierAndIdTests(unittest.TestCase):
    """identifier_normalization.normalize_resource_identifier + core.__process_normalization (resource id)."""

    def test_resource_id_is_sha256_of_scheme_less_source_type_and_id(self):
        """source-derived, unverified against a captured run: IdentifierNormalization.normalize_resource_id / NormalizationUtils.remove_url_scheme_regex."""
        east = shape("Patient", EAST, id="p-shared")
        west = shape("Patient", WEST, id="p-shared")
        self.assertEqual(east["id"], sha("east.example.test/fhir/Patient/p-shared"))
        self.assertEqual(west["id"], sha("west.example.test/fhir/Patient/p-shared"))
        self.assertNotEqual(east["id"], west["id"])
        self.assertEqual(east["idOrig"], "p-shared")
        trailing = shape("Patient", EAST + "/", id="p-shared")
        self.assertEqual(trailing["id"], east["id"])

    def test_fhir_id_identifier_is_appended_and_original_kept(self):
        """source-derived, unverified against a captured run: conform_to_col_identifier_schema, add_source_id_to_identifier_col, then the
        identifier/assigner nested reference rule (normalize_nested_ref_struct_in_array)."""
        row = shape("Patient", EAST, id="p-shared",
                    identifier='[{"use":"usual","system":"https://east.example.test/fhir","value":"p-shared"}]')
        self.assertEqual(row["identifierOrig"], [
            {"extension": None, "use": "usual", "value": "p-shared", "system": EAST, "type": None, "period": None, "assigner": None},
        ])
        self.assertEqual(row["identifier"], [
            {"extension": None, "use": "usual", "value": "p-shared", "system": EAST, "type": None, "period": None,
             "assigner": NULL_INPUT_REF},
            fhir_id_element("p-shared", EAST),
        ])

    def test_absent_identifier_becomes_only_the_fhir_id_element(self):
        """source-derived, unverified against a captured run: add_source_id_to_identifier_col replaces a null identifier with an empty array first."""
        row = shape("Patient", WEST, id="p9")
        self.assertIsNone(row["identifierOrig"])
        self.assertIsNone(row["identifierOrig_string"])
        self.assertEqual(row["identifier"], [fhir_id_element("p9", WEST)])

    def test_existing_fhir_id_coding_leaves_identifier_unchanged_without_dedupe(self):
        """source-derived, unverified against a captured run: source_id_identifier_exists_in_col short-circuits array_union."""
        element = '{"value":"a","type":{"coding":[{"code":"fhirId"}]}}'
        row = shape("Patient", EAST, id="p1", identifier=f"[{element},{element}]")
        expected = {"extension": None, "use": None, "value": "a", "system": None,
                    "type": {"extension": None, "id": None, "text": None,
                             "coding": [{"extension": None, "id": None, "system": None, "version": None,
                                         "code": "fhirId", "display": None, "userSelected": None}]},
                    "period": None, "assigner": NULL_INPUT_REF}
        self.assertEqual(row["identifier"], [expected, expected])

    def test_array_union_dedupes_existing_elements_when_appending(self):
        """source-derived, unverified against a captured run: F.array_union returns distinct elements."""
        element = '{"system":"https://identifiers.example.test/pilot","value":"east-002"}'
        row = shape("Patient", EAST, id="p1", identifier=f"[{element},{element}]")
        self.assertEqual(len(row["identifierOrig"]), 2)
        self.assertEqual([e["value"] for e in row["identifier"]], ["east-002", "p1"])

    def test_identifier_type_without_coding_gets_empty_coding_and_assigner_reference_normalized(self):
        """source-derived, unverified against a captured run: conform_to_col_identifier_schema coalesces coding to [] and always builds the
        assigner.identifier struct; ReferenceNormalization.normalize_ref_struct then normalizes the assigner."""
        row = shape("Patient", EAST, id="p1",
                    identifier='[{"value":"m1","type":{"text":"MRN"},"assigner":{"reference":"Organization/o1"}}]')
        self.assertEqual(row["identifierOrig"][0]["assigner"],
                         {"extension": None, "id": None, "reference": "Organization/o1", "type": None,
                          "identifier": None, "display": None})
        first = row["identifier"][0]
        self.assertEqual(first["type"], {"extension": None, "id": None, "coding": [], "text": "MRN"})
        self.assertEqual(first["assigner"], local_ref("Organization", "o1", EAST, "Organization/o1"))
        self.assertEqual(row["identifier"][1], fhir_id_element("p1", EAST))


class ReferenceTests(unittest.TestCase):
    """reference_normalization.ReferenceNormalization.normalize_ref_struct and its array variants."""

    def test_relative_literal_reference_is_resolved_against_the_source_system(self):
        """source-derived, unverified against a captured run: Literal pattern, base None -> normalize_resource_id + add_source_id_to_identifier_row."""
        row = shape("Observation", EAST, subject='[{"reference":"Patient/p-shared","type":"Patient","display":"East fixture"}]')
        self.assertEqual(row["subject"], local_ref("Patient", "p-shared", EAST, "Patient/p-shared", "East fixture"))

    def test_absolute_reference_with_same_base_ignores_scheme_and_infers_type(self):
        """source-derived, unverified against a captured run: remove_url_scheme_regex(base) == remove_url_scheme_regex(source_system)."""
        row = shape("Observation", WEST, subject='[{"reference":"http://west.example.test/fhir/Patient/p-shared"}]')
        self.assertEqual(row["subject"], local_ref("Patient", "p-shared", WEST, "http://west.example.test/fhir/Patient/p-shared"))

    def test_external_absolute_reference_is_not_resolved(self):
        """source-derived, unverified against a captured run: Literal with a foreign base: only type is inferred; reference moves to msftSourceReference."""
        original = "https://other.example.test/fhir/Patient/p-shared"
        row = shape("Observation", EAST, subject=json.dumps([{"reference": original}]))
        self.assertEqual(row["subject"], {"reference": None, "type": "Patient", "identifier": CONFORMED_EMPTY_ID,
                                          "display": None, "extension": None, "id": None, "idOrig": None,
                                          "msftSourceReference": original})

    def test_logical_reference_without_system_uses_source_system(self):
        """source-derived, unverified against a captured run: Logical pattern, no base and no system -> create_identifier(value, source_system)."""
        row = shape("Observation", EAST, subject='[{"reference":"Patient?identifier=123"}]')
        identifier = dict(CONFORMED_EMPTY_ID, value="123", system=EAST)
        self.assertEqual(row["subject"], {"reference": None, "type": "Patient", "identifier": identifier,
                                          "display": None, "extension": None, "id": None, "idOrig": None,
                                          "msftSourceReference": "Patient?identifier=123"})

    def test_logical_reference_with_system_uses_that_system(self):
        """source-derived, unverified against a captured run: Logical pattern, no base, system present -> create_identifier(value, system)."""
        original = "Patient?identifier=http://mrn.example.test|99"
        row = shape("Observation", EAST, subject=json.dumps([{"reference": original}]))
        self.assertEqual(row["subject"]["identifier"], dict(CONFORMED_EMPTY_ID, value="99", system="http://mrn.example.test"))
        self.assertEqual(row["subject"]["msftSourceReference"], original)

    def test_unrecognised_reference_is_moved_without_resolution(self):
        """source-derived, unverified against a captured run: neither pattern matches 'Thing/1'; the reference is still cleared."""
        row = shape("Observation", EAST, subject='[{"reference":"Thing/1","type":"Thing"}]')
        self.assertEqual(row["subject"], {"reference": None, "type": "Thing", "identifier": CONFORMED_EMPTY_ID,
                                          "display": None, "extension": None, "id": None, "idOrig": None,
                                          "msftSourceReference": "Thing/1"})

    def test_absent_struct_reference_becomes_the_udf_default_struct(self):
        """source-derived, unverified against a captured run: the Python UDF receives None and returns its default Row (non-null)."""
        row = shape("Observation", EAST)
        for column in ("subject", "encounter", "specimen", "device"):
            self.assertEqual(row[column], NULL_INPUT_REF, column)
        self.assertIsNone(row["basedOn"])  # normalize_ref_array returns a falsy column unchanged
        patient = shape("Patient", EAST)
        self.assertEqual(patient["managingOrganization"], NULL_INPUT_REF)
        self.assertIsNone(patient["generalPractitioner"])

    def test_explicit_identifier_value_suppresses_fhir_id_and_type_text_is_parsed(self):
        """source-derived, unverified against a captured run: identifier.type is a JSON string in the raw reference schema; conform_to_row_identifier_schema
        parses it with create_row_from_string; add_source_id_to_identifier_row returns early when value is set."""
        subject = '[{"reference":"Patient/p1","identifier":{"system":"http://mrn","value":"42","type":{"text":"MRN"}}}]'
        row = shape("Observation", EAST, subject=subject)
        self.assertEqual(row["subject"]["identifier"], {
            "extension": None, "use": None, "value": "42", "system": "http://mrn",
            "type": {"extension": None, "id": None, "coding": [], "text": "MRN"},
            "period": NULL_PERIOD, "assigner": NULL_RAW_REF})
        self.assertEqual(row["subject"]["id"], sha("east.example.test/fhir/Patient/p1"))

    def test_reference_identifier_text_fields_reproduce_python_str_casts(self):
        """source-derived, unverified against a captured run: get_row_field(..., cast_type=str) on parsed JSON yields Python repr text, and
        parse_assigner_json_safely parses the assigner string."""
        subject = json.dumps([{"display": "x", "identifier": {
            "value": "42",
            "type": {"coding": [{"code": "MR", "extension": [{"url": "u"}]}]},
            "assigner": {"display": "Org A", "identifier": {"value": "o-1"}}}}])
        row = shape("Observation", EAST, subject=subject)
        identifier = row["subject"]["identifier"]
        self.assertEqual(identifier["type"]["coding"], [{"extension": "[{'url': 'u'}]", "id": None, "system": None,
                                                         "version": None, "code": "MR", "display": None, "userSelected": None}])
        self.assertEqual(identifier["assigner"], {
            "extension": None, "id": None, "reference": None, "type": None,
            "identifier": {"extension": None, "use": None, "type": None, "value": "o-1", "system": None,
                           "period": None, "assigner": None},
            "display": "Org A"})
        self.assertIsNone(row["subject"]["msftSourceReference"])

    def test_reference_arrays_and_nested_references(self):
        """source-derived, unverified against a captured run: normalize_ref_array and normalize_nested_ref_struct_in_array (contact.organization,
        link.other, note.authorReference); a null nested reference becomes the default struct."""
        patient = shape("Patient", EAST, generalPractitioner='[{"reference":"Practitioner/d1"}]',
                        contact='[{"gender":"female","organization":{"reference":"Organization/o1"}},{"gender":"male"}]',
                        link='[{"type":"seealso","other":{"reference":"Patient/p2"}}]')
        self.assertEqual(patient["generalPractitioner"], [local_ref("Practitioner", "d1", EAST, "Practitioner/d1")])
        self.assertEqual(patient["contact"][0]["organization"], local_ref("Organization", "o1", EAST, "Organization/o1"))
        self.assertEqual(patient["contact"][1]["organization"], NULL_INPUT_REF)
        self.assertEqual(patient["contact"][1]["gender"], "male")
        self.assertEqual(patient["link"], [{"extension": None, "id": None, "other": local_ref("Patient", "p2", EAST, "Patient/p2"), "type": "seealso"}])
        obs = shape("Observation", WEST, note='[{"text":"n","authorReference":{"reference":"Practitioner/d2"}}]',
                    performer='[{"reference":"Practitioner/d2"},{"display":"anon"}]')
        self.assertEqual(obs["note"][0]["authorReference"], local_ref("Practitioner", "d2", WEST, "Practitioner/d2"))
        self.assertEqual(obs["performer"][1], {"reference": None, "type": None, "identifier": CONFORMED_EMPTY_ID,
                                               "display": "anon", "extension": None, "id": None, "idOrig": None,
                                               "msftSourceReference": None})


class DateTests(unittest.TestCase):
    """date_normalization.normalize_date + core.__process_normalization (date, dotted date) + Spark from_json."""

    def test_birth_date_partial_forms(self):
        """source-derived, unverified against a captured run: birthDate parses as a date; normalize_date returns midnight UTC; the Orig keeps the date."""
        for text, day in (("2000-02-29", date(2000, 2, 29)), ("1999-04", date(1999, 4, 1)), ("1999", date(1999, 1, 1))):
            with self.subTest(text=text):
                row = shape("Patient", birthDate=text)
                self.assertEqual(row["birthDateOrig"], day)
                self.assertEqual(row["birthDate"], datetime(day.year, day.month, day.day, tzinfo=UTC))

    def test_date_time_forms_convert_to_utc(self):
        """source-derived, unverified against a captured run: timestamps pass through normalize_date unchanged; the Orig equals the value."""
        cases = {
            "2025-01-10T12:30:00+01:00": utc(2025, 1, 10, 11, 30),
            "2025-01-10T12:30:00Z": utc(2025, 1, 10, 12, 30),
            "2025-01-10T12:30:00.1234567-05:00": utc(2025, 1, 10, 17, 30, 0, 123456),
            "2025-01-10": utc(2025, 1, 10),
            "2025-01": utc(2025, 1, 1),
            "2025": utc(2025, 1, 1),
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                row = shape("Observation", effectiveDateTime=text, issued=text, valueDateTime=text, effectiveInstant=text)
                for column in ("effectiveDateTime", "issued", "valueDateTime", "effectiveInstant"):
                    self.assertEqual(row[column], expected)
                    self.assertEqual(row[column + "Orig"], expected)
        patient = shape("Patient", deceasedDateTime="2024-12-31T23:00:00-02:00")
        self.assertEqual(patient["deceasedDateTime"], utc(2025, 1, 1, 1))
        self.assertEqual(patient["deceasedDateTimeOrig"], utc(2025, 1, 1, 1))

    def test_invalid_date_strings_are_rejected(self):
        for text in ("2025-01-10T12:30", "2025-01-10T12:30:00", "2025-02-30", "2025-13", "10/01/2025", "2025-01-10T25:00:00Z"):
            with self.subTest(text=text), self.assertRaises(ShapeError):
                shape("Observation", effectiveDateTime=text)
        with self.assertRaises(ShapeError):
            shape("Patient", birthDate="2000-02-30")

    def test_meta_last_updated_columns(self):
        """source-derived, unverified against a captured run: meta_lastUpdated is copied from meta.lastUpdated, date-normalized with an Orig copy, and the
        dotted 'meta.lastUpdated' rule appends meta.lastUpdatedOrig."""
        row = shape("Patient", meta='[{"versionId":"1","lastUpdated":"2025-01-10T10:00:00.123456Z"}]')
        moment = utc(2025, 1, 10, 10, 0, 0, 123456)
        self.assertEqual(row["meta_lastUpdated"], moment)
        self.assertEqual(row["meta_lastUpdatedOrig"], moment)
        self.assertEqual(row["meta"], {"extension": None, "id": None, "versionId": "1", "lastUpdated": moment,
                                       "source": None, "profile": None, "security": None, "tag": None,
                                       "lastUpdatedOrig": moment})
        empty = shape("Patient")
        self.assertIsNone(empty["meta"])
        self.assertIsNone(empty["meta_lastUpdated"])
        self.assertIsNone(empty["meta_lastUpdatedOrig"])

    def test_periods_get_orig_fields_only_where_configured(self):
        """source-derived, unverified against a captured run: effectivePeriod.start/end and valuePeriod.start/end are dotted date rules; component[].valuePeriod
        is not configured and keeps the plain Period shape."""
        row = shape("Observation", effectivePeriod='[{"start":"2025-01-10","end":"2025-01-11T08:00:00+08:00"}]',
                    component='[{"code":{"text":"c"},"valuePeriod":{"start":"2025-02"}}]')
        start, end = utc(2025, 1, 10), utc(2025, 1, 11)
        self.assertEqual(row["effectivePeriod"], {"extension": None, "id": None, "start": start, "end": end,
                                                  "startOrig": start, "endOrig": end})
        self.assertIsNone(row["valuePeriod"])
        self.assertEqual(row["component"][0]["valuePeriod"], {"extension": None, "id": None, "start": utc(2025, 2, 1), "end": None})


class ValueTypeTests(unittest.TestCase):
    """Spark from_json against the raw (Avro-derived) column types, mimicked without Spark."""

    def test_decimals_keep_scale_18(self):
        """source-derived, unverified against a captured run: decimal(36,18) columns; Spark rounds HALF_UP to the target scale."""
        row = shape("Observation", valueQuantity='[{"value":36.123456789012345678,"unit":"C"}]')
        self.assertEqual(row["valueQuantity"]["value"], Decimal("36.123456789012345678"))
        row = shape("Observation", valueQuantity='[{"value":0.0000000000000000005}]')
        self.assertEqual(row["valueQuantity"]["value"], Decimal("0.000000000000000001"))
        with self.assertRaises(ShapeError):
            shape("Observation", valueQuantity='[{"value":1234567890123456789.5}]')

    def test_string_fields_given_json_keep_compact_jackson_text(self):
        """source-derived, unverified against a captured run: Spark JacksonParser copies non-string tokens into StringType fields as compact JSON;
        doubles print as Java Double.toString."""
        extension = r'[{"url":"u","valueDecimal":0.5},{"url":"u","valueDecimal":12345678.9},{"url":"u","valueDecimal":0.0001},{"url":"u","valueDecimal":2.50},{"url":"u","valueInteger":7},{"url":"u","valueString":"a\"b\\c\n\u0001é/"}]'
        row = shape("Patient", extension=extension)
        self.assertEqual(row["extension"],
                         r'[{"url":"u","valueDecimal":0.5},{"url":"u","valueDecimal":1.23456789E7},{"url":"u","valueDecimal":1.0E-4},{"url":"u","valueDecimal":2.5},{"url":"u","valueInteger":7},{"url":"u","valueString":"a\"b\\c\n\u0001é/"}]')
        self.assertIsNone(shape("Patient")["extension"])
        row = shape("Patient", contained='[{"resourceType":"Organization","id":"o1"}]')
        self.assertEqual(row["contained"], ['{"resourceType":"Organization","id":"o1"}'])
        self.assertEqual(row["contained_string"], r'["{\"resourceType\":\"Organization\",\"id\":\"o1\"}"]')

    def test_malformed_input_is_rejected(self):
        bad = {
            "unknown column": dict(blank_row("Patient"), extra="x"),
            "missing column": {k: v for k, v in blank_row("Patient").items() if k != "gender"},
            "bool as text": blank_row("Patient", active="true"),
            "int32 overflow": blank_row("Patient", multipleBirthInteger=2 ** 31),
            "bool as int": blank_row("Patient", multipleBirthInteger=True),
            "two values for a single struct": blank_row("Patient", maritalStatus='[{"text":"a"},{"text":"b"}]'),
            "unknown nested field": blank_row("Patient", maritalStatus='[{"text":"a","foo":1}]'),
            "nested type mismatch": blank_row("Patient", maritalStatus='[{"coding":[{"userSelected":"yes"}]}]'),
            "not json": blank_row("Patient", name="[{"),
            "not an array": blank_row("Patient", name='{"family":"x"}'),
            "null element": blank_row("Patient", name="[null]"),
            "missing id": blank_row("Patient", id=None),
        }
        for label, extracted in bad.items():
            with self.subTest(label), self.assertRaises(ShapeError):
                shape_row("Patient", extracted, source_system=EAST, logical_path="p", ingest_time=INGEST)

    def test_provenance_arguments_are_validated(self):
        with self.assertRaises(ShapeError):
            shape_row("Patient", blank_row("Patient"), source_system=EAST, logical_path="p", ingest_time=datetime(2025, 1, 1))
        with self.assertRaises(ShapeError):
            shape_row("Patient", blank_row("Patient"), source_system="", logical_path="p", ingest_time=INGEST)


class ProvenanceTests(unittest.TestCase):
    def test_msft_columns_come_from_the_explicit_inputs(self):
        """source-derived, unverified against a captured run: dataframe_utils.add_normalized_current_timestamp_column sets both timestamps at write;
        msftFilePath is the pilot's logical path, not the OneLake input_file_name() URI."""
        moment = datetime(2025, 1, 12, 11, 0, tzinfo=timezone(timedelta(hours=2)))
        row = shape_row("Observation", blank_row("Observation"), source_system=WEST,
                        logical_path="Files/Process/Clinical/FHIR-NDJSON/west/initial/Observation.ndjson", ingest_time=moment)
        self.assertEqual(row["msftSourceSystem"], WEST)
        self.assertEqual(row["msftFilePath"], "Files/Process/Clinical/FHIR-NDJSON/west/initial/Observation.ndjson")
        self.assertEqual(row["msftModifiedDatetime"], utc(2025, 1, 12, 9))
        self.assertEqual(row["msftCreatedDatetime"], utc(2025, 1, 12, 9))
        self.assertEqual(row["msftModifiedDatetime"].utcoffset(), timedelta(0))
        self.assertEqual(row["resourceType"], "Observation")


class StringCompanionTests(unittest.TestCase):
    """dataframe_utils.stringify_complex_types: to_json(col, ignoreNullFields=False) on the normalized value."""

    def setUp(self):
        self.patient = shape_row("Patient", extracted_from_resource("Patient", ndjson("input/east/initial/Patient.ndjson")[0]),
                                 source_system=EAST, logical_path="Files/east/Patient.ndjson", ingest_time=INGEST)
        self.east_obs = shape_row("Observation", extracted_from_resource("Observation", ndjson("input/east/initial/Observation.ndjson")[0]),
                                  source_system=EAST, logical_path="Files/east/Observation.ndjson", ingest_time=INGEST)
        self.west_obs = shape_row("Observation", extracted_from_resource("Observation", ndjson("input/west/initial/Observation.ndjson")[0]),
                                  source_system=WEST, logical_path="Files/west/Observation.ndjson", ingest_time=INGEST)

    def test_meta_string_includes_nulls_and_millisecond_timestamps(self):
        """source-derived, unverified against a captured run."""
        self.assertEqual(self.patient["meta_string"],
                         '{"extension":null,"id":null,"versionId":"1","lastUpdated":"2025-01-10T10:00:00.123Z",'
                         '"source":"https://east.example.test/fhir","profile":null,"security":null,"tag":null,'
                         '"lastUpdatedOrig":"2025-01-10T10:00:00.123Z"}')

    def test_quantity_string_prints_decimal_at_scale_18(self):
        """source-derived, unverified against a captured run."""
        self.assertEqual(self.east_obs["valueQuantity_string"],
                         '{"extension":"[{\\"url\\":\\"https://example.test/fhir/StructureDefinition/pilot-label\\",'
                         '\\"valueString\\":\\"synthetic-quantity\\"}]","id":null,"value":36.123456789012345678,'
                         '"comparator":null,"unit":"degrees Celsius","system":"http://unitsofmeasure.org","code":"Cel"}')
        row = shape("Observation", valueQuantity='[{"value":2.5}]')
        self.assertIn('"value":2.500000000000000000,', row["valueQuantity_string"])
        row = shape("Observation", valueQuantity='[{"value":0}]')
        self.assertIn('"value":0E-18,', row["valueQuantity_string"])

    def test_reference_and_period_strings(self):
        """source-derived, unverified against a captured run."""
        h = sha("east.example.test/fhir/Patient/p-shared")
        self.assertEqual(self.east_obs["subject_string"],
                         '{"reference":null,"type":"Patient","identifier":{"extension":null,"use":null,"value":"p-shared",'
                         '"system":"https://east.example.test/fhir","type":{"extension":null,"id":null,"coding":[{"extension":null,'
                         '"id":null,"system":"http://terminology.hl7.org/CodeSystem/v2-0203","version":null,"code":"fhirId",'
                         '"display":"FHIR Id","userSelected":null}],"text":"FHIR Id"},"period":{"extension":null,"id":null,'
                         '"start":null,"end":null},"assigner":{"extension":null,"id":null,"reference":null,"type":null,'
                         '"identifier":null,"display":null}},"display":"East fixture","extension":null,'
                         f'"id":"{h}","idOrig":"p-shared","msftSourceReference":"Patient/p-shared"}}')
        self.assertEqual(self.west_obs["effectivePeriod_string"],
                         '{"extension":null,"id":null,"start":"2025-01-10T00:00:00.000Z","end":"2025-01-11T00:00:00.000Z",'
                         '"startOrig":"2025-01-10T00:00:00.000Z","endOrig":"2025-01-11T00:00:00.000Z"}')
        self.assertEqual(self.west_obs["code_string"], '{"extension":null,"id":null,"coding":null,"text":"Synthetic label"}')
        self.assertIsNone(self.west_obs["valueQuantity_string"])

    def test_every_string_companion_is_json_of_its_column(self):
        for row, resource in ((self.patient, "Patient"), (self.east_obs, "Observation"), (self.west_obs, "Observation")):
            for name in target_schema(resource).names:
                if name.endswith("_string"):
                    base = row[name[: -len("_string")]]
                    self.assertEqual(row[name] is None, base is None, name)


class FixtureTests(unittest.TestCase):
    """The pilot fixtures shape into Arrow without error; key values follow the rules above."""

    def test_fixtures_shape(self):
        """source-derived, unverified against a captured run."""
        cases = [
            ("Patient", "input/east/initial/Patient.ndjson", EAST),
            ("Patient", "input/west/initial/Patient.ndjson", WEST),
            ("Patient", "input/east/update/Patient.ndjson", EAST),
            ("Observation", "input/east/initial/Observation.ndjson", EAST),
            ("Observation", "input/west/initial/Observation.ndjson", WEST),
        ]
        rows = {}
        for resource, path, source in cases:
            rows[path] = [shape_row(resource, extracted_from_resource(resource, r), source_system=source,
                                    logical_path=path, ingest_time=INGEST) for r in ndjson(path)]
            pa.Table.from_pylist(rows[path], schema=target_schema(resource))
        east = rows["input/east/initial/Patient.ndjson"][0]
        self.assertEqual(east["birthDate"], utc(2000, 2, 29))
        self.assertEqual(east["multipleBirthInteger"], 2)
        self.assertEqual(east["extension"], '[{"url":"https://example.test/fhir/StructureDefinition/pilot-label","valueString":"synthetic-east"}]')
        self.assertEqual([i["value"] for i in east["identifier"]], ["p-shared", "east-002", "p-shared"])
        west = rows["input/west/initial/Patient.ndjson"][0]
        self.assertEqual(west["birthDateOrig"], date(1999, 4, 1))
        self.assertIs(west["multipleBirthBoolean"], False)
        self.assertEqual(rows["input/east/initial/Observation.ndjson"][0]["effectiveDateTime"], utc(2025, 1, 10, 11, 30))
        self.assertEqual(rows["input/west/initial/Observation.ndjson"][0]["subject"]["id"], west["id"])


if __name__ == "__main__":
    unittest.main()
