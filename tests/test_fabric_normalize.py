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
import re
import traceback
import unittest
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
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

    def test_decimal_precision_boundaries(self):
        """source-derived, unverified against a captured run: decimal(36,18) (Silver Delta schema / R4 Avro) holds 18 integer and
        18 fractional digits; Spark's Decimal.changePrecision rounds HALF_UP (away from zero) to scale 18, and a value whose
        rounded precision exceeds 36 does not fit."""
        fits = {
            "123456789012345678.123456789012345678": "123456789012345678.123456789012345678",
            "999999999999999999.999999999999999999": "999999999999999999.999999999999999999",
            "-999999999999999999.999999999999999999": "-999999999999999999.999999999999999999",
            "999999999999999999.9999999999999999994": "999999999999999999.999999999999999999",
            "999999999999999999": "999999999999999999.000000000000000000",
            "0.0000000000000000015": "0.000000000000000002",
            "-0.0000000000000000015": "-0.000000000000000002",
        }
        for text, expected in fits.items():
            with self.subTest(text=text):
                row = shape("Observation", valueQuantity=f'[{{"value":{text}}}]')
                self.assertEqual(row["valueQuantity"]["value"], Decimal(expected))
                self.assertEqual(row["valueQuantity"]["value"].as_tuple().exponent, -18)
                pa.Table.from_pylist([row], schema=target_schema("Observation"))
        overflow = ("999999999999999999.9999999999999999995", "-999999999999999999.9999999999999999995",
                    "1000000000000000000", "1E+400")
        for text in overflow:
            with self.subTest(text=text), self.assertRaises(ShapeError):
                shape("Observation", valueQuantity=f'[{{"value":{text}}}]')

    def test_exponents_beyond_bigdecimal_range(self):
        """source-derived, unverified against a captured run: Spark from_json reads a DecimalType field with Jackson's
        getDecimalValue (Java BigDecimal); an exponent beyond int range throws NumberFormatException, so the record is
        nulled in PERMISSIVE mode, a gap rejected here even when the mantissa is zero. A StringType field copies the token
        as a Java double: Double.parseDouble saturates to Infinity (rejected, as for 1e999) or to signed zero."""
        exponents = ("1e999999999999999999999999", "-1e999999999999999999999999", "1e-999999999999999999999999",
                     "-1e-999999999999999999999999", "0e+9999999999999999999")
        for text in exponents:
            with self.subTest(column="valueQuantity", text=text), self.assertRaises(ShapeError) as caught:
                shape("Observation", valueQuantity=f'[{{"value":{text}}}]')
            self.assertEqual((caught.exception.path, caught.exception.reason),
                             ("valueQuantity.value", "does not fit decimal(36,18)"))
        for text in exponents[:2]:
            with self.subTest(column="extension", text=text), self.assertRaises(ShapeError) as caught:
                shape("Observation", extension=f'[{{"url":"u","valueDecimal":{text}}}]')
            self.assertEqual((caught.exception.path, caught.exception.reason), ("extension", "is out of double range"))
        for text, double in zip(exponents[2:], ("0.0", "-0.0", "0.0")):
            with self.subTest(column="extension", text=text):
                row = shape("Observation", extension=f'[{{"url":"u","valueDecimal":{text}}}]')
                self.assertEqual(row["extension"], f'[{{"url":"u","valueDecimal":{double}}}]')

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


SENTINEL = "SENSITIVE_SENTINEL"


class ErrorRedactionTests(unittest.TestCase):
    """Design spec: identify errors by resource, mapping path and failure reason without logging source values."""

    def assert_redacted(self, resource, columns, path, fhir_path, reason, secret=SENTINEL):
        with self.assertRaises(ShapeError) as caught:
            shape_row(resource, columns, source_system=EAST, logical_path="Files/p.ndjson", ingest_time=INGEST)
        error = caught.exception
        self.assertEqual((error.resource, error.path, error.fhir_path, error.reason), (resource, path, fhir_path, reason))
        self.assertEqual(str(error), f"{resource} {path} (FHIRPath {fhir_path}): {reason}")
        self.assertIsNone(error.__cause__)
        self.assertIsNone(error.__context__)
        logged = "".join(traceback.format_exception(error)) + repr(error.args)
        self.assertNotIn(secret, logged)

    def test_errors_name_resource_and_path_without_source_values(self):
        cases = [
            ("Observation", {"effectiveDateTime": SENTINEL}, "effectiveDateTime", "effective.ofType(dateTime)",
             "is not a FHIR date, dateTime or instant"),
            ("Observation", {"effectiveDateTime": f"2025-02-30T00:00:00Z"}, "effectiveDateTime", "effective.ofType(dateTime)",
             "is not a valid date or time"),
            ("Patient", {"birthDate": SENTINEL}, "birthDate", "birthDate", "is not a FHIR date"),
            ("Patient", {"birthDate": "1999-02-30"}, "birthDate", "birthDate", "is not a valid date or time"),
            ("Patient", {"active": SENTINEL}, "active", "active", "expected a boolean"),
            ("Patient", {"multipleBirthInteger": SENTINEL}, "multipleBirthInteger", "multipleBirth.ofType(integer)",
             "expected a 32-bit integer"),
            ("Patient", {"maritalStatus": f'[{{"coding":[{{"code":"a"}},{{"userSelected":"{SENTINEL}"}}]}}]'},
             "maritalStatus.coding[1].userSelected", "maritalStatus.coding[1].userSelected", "expected a boolean"),
            ("Patient", {"name": f'[{{"family":"x"}},{{"{SENTINEL}":1}}]'}, "name[1]", "name[1]",
             "has a field that is not in the HDS Silver schema"),
            ("Patient", {"name": f'[{{"family":"{SENTINEL}"'}, "name", "name", "is not valid JSON"),
            ("Patient", {"name": f'{{"family":"{SENTINEL}"}}'}, "name", "name", "expected a JSON array"),
            ("Patient", {"name": f'[{{"given":["{SENTINEL}",null]}}]'}, "name[0].given[1]", "name[0].given[1]",
             "null array elements are not FHIR"),
            ("Patient", {"name": f'[{{"family":"{SENTINEL}"}},null]'}, "name[1]", "name[1]", "null array elements are not FHIR"),
            ("Patient", {"maritalStatus": f'[{{"text":"{SENTINEL}"}},{{"text":"b"}}]'}, "maritalStatus", "maritalStatus",
             "expected at most one value"),
            ("Patient", {"telecom": f'[{{"value":"{SENTINEL}","rank":99999999999}}]'}, "telecom[0].rank", "telecom[0].rank",
             "expected a 32-bit integer"),
            ("Observation", {"valueQuantity": '[{"value":9876543210987654321.5}]'}, "valueQuantity.value",
             "value.ofType(Quantity).value", "does not fit decimal(36,18)"),
            ("Patient", {"extension": f'[{{"url":"{SENTINEL}","valueDecimal":1e999}}]'},
             "extension", "extension", "is out of double range"),
            ("Observation", {"subject": f'[{{"reference":"Patient/p1","identifier":{{"type":"{SENTINEL}"}}}}]'},
             "subject.identifier.type", "subject.identifier.type", "is not JSON object text"),
            ("Observation", {"performer": f'[{{"display":"a"}},{{"identifier":{{"type":"[\\"{SENTINEL}\\"]"}}}}]'},
             "performer[1].identifier.type", "performer[1].identifier.type", "is not JSON object text"),
            ("Patient", {"contact": f'[{{"gender":"male"}},{{"organization":{{"identifier":{{"type":"{SENTINEL}"}}}}}}]'},
             "contact[1].organization.identifier.type", "contact[1].organization.identifier.type", "is not JSON object text"),
            ("Patient", {"identifier": f'[{{"value":"v","assigner":{{"identifier":{{"type":"{SENTINEL}"}}}}}}]'},
             "identifier[0].assigner.identifier.type", "identifier[0].assigner.identifier.type", "is not JSON object text"),
            ("Patient", {"id": None, "gender": SENTINEL}, "id", "id", "is required"),
            # A lone surrogate cannot be UTF-8 encoded (hashing, Parquet); previously a UnicodeEncodeError carried the id.
            ("Patient", {"id": SENTINEL + "\ud800"}, "id", "id", "is not encodable as UTF-8"),
            ("Patient", {"name": f'[{{"family":"{SENTINEL}\\udc00"}}]'}, "name[0].family", "name[0].family",
             "is not encodable as UTF-8"),
            # identifier_normalization.conform_to_row_identifier_schema iterates coding; HDS fails on a non-iterable.
            ("Observation", {"subject": f'[{{"reference":"Patient/p1","identifier":{{"type":{{"coding":42,'
                                        f'"text":"{SENTINEL}"}}}}}}]'},
             "subject.identifier.type.coding", "subject.identifier.type.coding", "expected a JSON array"),
            # Exponents beyond Decimal's range: previously decimal.InvalidOperation escaped json.loads.
            ("Observation", {"valueQuantity": f'[{{"unit":"{SENTINEL}","value":1e9999999999999999999999999}}]'},
             "valueQuantity.value", "value.ofType(Quantity).value", "does not fit decimal(36,18)"),
            ("Observation", {"extension": f'[{{"url":"{SENTINEL}","valueDecimal":1e9999999999999999999999999}}]'},
             "extension", "extension", "is out of double range"),
        ]
        for resource, columns, path, fhir_path, reason in cases:
            with self.subTest(path=path, reason=reason):
                self.assert_redacted(resource, blank_row(resource, **columns), path, fhir_path, reason,
                                     secret="9876543210987654321" if "valueQuantity" in columns else SENTINEL)

    def test_column_set_and_argument_errors_are_identified(self):
        extracted = dict(blank_row("Patient"), **{SENTINEL: SENTINEL})
        with self.assertRaises(ShapeError) as caught:
            shape_row("Patient", extracted, source_system=EAST, logical_path="p", ingest_time=INGEST)
        self.assertEqual((caught.exception.resource, caught.exception.path), ("Patient", "(row)"))
        self.assertNotIn(SENTINEL, str(caught.exception))
        for arguments, path in (({"source_system": ""}, "msftSourceSystem"), ({"logical_path": ""}, "msftFilePath"),
                                ({"ingest_time": datetime(2025, 1, 1)}, "msftModifiedDatetime")):
            call = {**dict(source_system=EAST, logical_path="p", ingest_time=INGEST), **arguments}
            with self.subTest(path=path), self.assertRaises(ShapeError) as caught:
                shape_row("Patient", blank_row("Patient"), **call)
            self.assertEqual((caught.exception.resource, caught.exception.path), ("Patient", path))


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
        """source-derived, unverified against a captured run: stringify_complex_types writes to_json(col, ignoreNullFields=False);
        timestamps print as yyyy-MM-dd'T'HH:mm:ss.SSSXXX (UTC session), decimals at scale 18, struct keys in schema order."""
        rows = [("Patient", self.patient), ("Observation", self.east_obs), ("Observation", self.west_obs)]
        rows += [(w.resource, shape_witness(w)) for w in witnesses()]
        for resource, row in rows:
            assert_companions_render_columns(self, resource, row)


# ---- field-path evidence ----------------------------------------------------------------------
# Every reachable Silver leaf path (from the deployed schema) must have a populated, independently
# expected example below or a named always-null condition. Pass-through columns follow from_json, so
# their expectation is `from_json_oracle` (the hand-written conversion rule) applied to the witness
# JSON; normalized columns carry hand-written literals.

EXT_URL = "https://example.test/fhir/StructureDefinition/witness"
FHIR_ID_CODING = FHIR_ID_TYPE["coding"][0]
CHOICE_GROUPS = (
    ("boundsDuration", "boundsRange", "boundsPeriod"),
    ("valueQuantity", "valueCodeableConcept", "valueString", "valueBoolean", "valueInteger", "valueRange", "valueRatio",
     "valueSampledData", "valueTime", "valueDateTime", "valuePeriod"),
)
WITNESS_INSTANT = "2024-05-06T07:08:09.123+02:00"


def ext(label: str) -> list:
    return [{"url": EXT_URL, "valueString": label}]


def ext_text(label: str) -> str:
    """Jackson text of ext(label) kept in a StringType field."""
    return f'[{{"url":"{EXT_URL}","valueString":"{label}"}}]'


def ext_repr(label: str) -> str:
    """str() of the json.loads result of ext(label): what get_row_field(..., cast=str) stores."""
    return f"[{{'url': '{EXT_URL}', 'valueString': '{label}'}}]"


def is_reference(t) -> bool:
    return pa.types.is_struct(t) and t.get_field_index("msftSourceReference") >= 0


def column_type(resource: str, column: str):
    return target_schema(resource).field(column).type


def fill(t, path: str, choice: str | None = None):
    """FHIR JSON populating every field of a pass-through Silver type (normalized References are left out).

    In a choice group only `choice` (default: the first alternative) is set; a list of such structs gets
    one element per alternative so that each one is populated.
    """
    if pa.types.is_list(t):
        element = t.value_type
        group = next((g for g in CHOICE_GROUPS if pa.types.is_struct(element)
                      and element.get_field_index(g[0]) >= 0), None)
        if group:
            return [fill(element, f"{path}[{i}]", alt) for i, alt in enumerate(group)]
        if path.endswith("contained"):
            return [{"resourceType": "Basic", "id": "contained-1"}]
        return [fill(element, f"{path}[0]", choice)]
    if pa.types.is_struct(t):
        out = {}
        for f in t:
            group = next((g for g in CHOICE_GROUPS if f.name in g), None)
            if is_reference(f.type) or (group and f.name != (choice if choice in group else group[0])):
                continue
            out[f.name] = ext(f"{path}.extension") if f.name == "extension" else fill(f.type, f"{path}.{f.name}", choice)
        return out
    if pa.types.is_string(t):
        return path
    if pa.types.is_boolean(t):
        return True
    if pa.types.is_int32(t):
        return 7
    if pa.types.is_decimal(t):
        return Decimal("12.5")
    if pa.types.is_timestamp(t):
        return WITNESS_INSTANT
    raise AssertionError(f"no witness value for {t}")


def from_json_oracle(value, t):
    """from_json of FHIR JSON into a Silver type with no normalization rule (hand-written conversion rule)."""
    if value is None:
        return None
    if pa.types.is_struct(t):
        return {f.name: from_json_oracle(value.get(f.name), f.type) for f in t}
    if pa.types.is_list(t):
        return [from_json_oracle(v, t.value_type) for v in value]
    if pa.types.is_string(t):
        return value if isinstance(value, str) else json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    if pa.types.is_decimal(t):
        return Decimal(value).quantize(Decimal("1E-18"))
    if pa.types.is_timestamp(t):
        return datetime.fromisoformat(value).astimezone(UTC)
    if pa.types.is_date(t):
        return date.fromisoformat(value)
    return value


def reference_witness(rtype: str, rid: str, source: str = EAST) -> tuple[dict, dict]:
    """A literal local reference carrying every Reference member, and its hand-derived Silver value.

    reference_normalization.normalize_ref_struct; identifier_normalization.conform_to_row_identifier_schema
    and add_source_id_to_identifier_row; utils.create_row_from_string / parse_assigner_json_safely /
    get_row_field. The raw ReferenceIdentifier members are strings (Jackson text), so: identifier.type is
    re-parsed with json.loads and its members cast with str() (Python repr for lists); identifier.period is
    a string, which get_row_field treats as "not a Row" -> all-null period; the assigner is re-parsed and its
    nested identifier members are str()-cast. The Reference's own id is not in the Silver Reference.
    """
    fhir = {
        "id": "ref-element-id", "extension": ext("re"), "reference": f"{rtype}/{rid}", "type": rtype,
        "identifier": {
            "extension": ext("ie"), "use": "official",
            "type": {"id": "t1", "extension": ext("te"),
                     "coding": [{"id": "c1", "extension": ext("ce"), "system": "urn:s", "version": "v1", "code": "k",
                                 "display": "K", "userSelected": True}],
                     "text": "tt"},
            "period": {"start": "2024-01-01T00:00:00Z"},
            "assigner": {"id": "a1", "extension": ext("ae"), "reference": "Organization/o1", "type": "Organization",
                         "display": "Assigner",
                         "identifier": {"extension": ext("aie"), "use": "secondary", "value": "av", "system": "urn:as",
                                        "type": {"text": "ORG"}, "period": {"start": "2024"},
                                        "assigner": {"display": "Parent"}}},
        },
        "display": "Display",
    }
    expected = {
        "reference": None, "type": rtype,
        "identifier": {
            "extension": ext_text("ie"), "use": "official", "value": rid, "system": source,
            "type": {"extension": ext_repr("te"), "id": "t1",
                     "coding": [{"extension": ext_repr("ce"), "id": "c1", "system": "urn:s", "version": "v1", "code": "k",
                                 "display": "K", "userSelected": True}, FHIR_ID_CODING],
                     "text": "tt"},
            "period": NULL_PERIOD,
            "assigner": {"extension": ext_repr("ae"), "id": "a1", "reference": "Organization/o1", "type": "Organization",
                         "identifier": {"extension": ext_repr("aie"), "use": "secondary", "type": "{'text': 'ORG'}",
                                        "value": "av", "system": "urn:as", "period": "{'start': '2024'}",
                                        "assigner": "{'display': 'Parent'}"},
                         "display": "Assigner"},
        },
        "display": "Display", "extension": ext_text("re"),
        "id": sha(f"{source.split('://', 1)[1]}/{rtype}/{rid}"), "idOrig": rid,
        "msftSourceReference": f"{rtype}/{rid}",
    }
    return fhir, expected


@dataclass(frozen=True)
class Witness:
    name: str
    resource: str
    fhir: dict
    expected: dict  # asserted Silver columns -> hand-derived value


def witness_meta() -> tuple[dict, dict]:
    """date_normalization on meta.lastUpdated: lastUpdatedOrig keeps the parsed timestamp; both are UTC."""
    fhir = {"id": "m1", "extension": ext("me"), "versionId": "3", "lastUpdated": "2024-06-01T12:00:00.5+02:00",
            "source": "urn:source", "profile": ["urn:profile"],
            "security": [fill(column_type("Patient", "maritalStatus").field("coding").type.value_type, "meta.security[0]")],
            "tag": [fill(column_type("Patient", "maritalStatus").field("coding").type.value_type, "meta.tag[0]")]}
    last = utc(2024, 6, 1, 10, 0, 0, 500000)
    meta_type = pa.struct([f for f in column_type("Patient", "meta") if f.name != "lastUpdatedOrig"])
    return fhir, {**from_json_oracle(fhir, meta_type), "lastUpdatedOrig": last}


def witness_identifier(resource: str, rid: str) -> tuple[list, list]:
    """identifier_normalization.add_source_id_to_identifier_col (array_union with the fhirId element), then the
    nested identifier.assigner reference rule; the fhirId element's all-null assigner Row is truthy."""
    assigner_in, assigner_out = reference_witness("Organization", "assigner-org")
    concept = column_type("Patient", "maritalStatus")  # CodeableConcept
    element = {"extension": ext("ide"), "use": "usual", "value": "mrn-1", "system": "urn:mrn",
               "type": fill(concept, "identifier[0].type"),
               "period": {"id": "ip", "extension": ext("ipe"), "start": "2020-01-01T00:00:00+02:00",
                          "end": "2021-01-01T00:00:00Z"},
               "assigner": assigner_in}
    # A display-only assigner with a business identifier: no reference, so no source id; the absent
    # type/period/assigner become HDS's `{}` placeholder (all-null structs).
    business = {"value": "mrn-2", "assigner": {"display": "Issuer", "identifier": {"system": "urn:issuer", "value": "iss-1"}}}
    business_out = {"extension": None, "use": None, "value": "mrn-2", "system": None, "type": None, "period": None,
                    "assigner": {"reference": None, "type": None,
                                 "identifier": {"extension": None, "use": None, "value": "iss-1", "system": "urn:issuer",
                                                "type": NULL_CC, "period": NULL_PERIOD, "assigner": NULL_RAW_REF},
                                 "display": "Issuer", "extension": None, "id": None, "idOrig": None,
                                 "msftSourceReference": None}}
    expected = [
        {"extension": ext_text("ide"), "use": "usual", "value": "mrn-1", "system": "urn:mrn",
         "type": from_json_oracle(element["type"], concept),
         "period": {"extension": ext_text("ipe"), "id": "ip", "start": utc(2019, 12, 31, 22), "end": utc(2021, 1, 1)},
         "assigner": assigner_out},
        business_out,
        {"extension": None, "use": None, "value": rid, "system": EAST, "type": FHIR_ID_TYPE, "period": NULL_PERIOD,
         "assigner": FHIR_ID_ASSIGNER},
    ]
    return [element, business], expected


def passthrough(resource: str, fhir: dict, columns) -> dict:
    return {c: from_json_oracle(fhir.get(c), column_type(resource, c)) for c in columns}


def provenance(resource: str, rid: str, path: str) -> dict:
    return {"id": sha(f"east.example.test/fhir/{resource}/{rid}"), "idOrig": rid, "resourceType": resource,
            "msftSourceSystem": EAST, "msftFilePath": path, "msftModifiedDatetime": INGEST, "msftCreatedDatetime": INGEST}


def witness_path(name: str) -> str:
    return f"Files/witness/{name}.ndjson"


@lru_cache(maxsize=None)
def witnesses() -> tuple:
    out = []

    # Patient: every pass-through column, every normalized column, deceased/multipleBirth boolean choices.
    t = lambda c: column_type("Patient", c)  # noqa: E731
    meta_in, meta_out = witness_meta()
    ids_in, ids_out = witness_identifier("Patient", "pw1")
    gp_in, gp_out = reference_witness("Practitioner", "gp1")
    mo_in, mo_out = reference_witness("Organization", "mo1")
    co_in, co_out = reference_witness("Organization", "contact-org")
    li_in, li_out = reference_witness("Patient", "linked")
    contact = fill(t("contact").value_type, "contact[0]")
    patient_passthrough = ("text", "contained", "name", "telecom", "address", "maritalStatus", "photo", "communication")
    fhir = {"resourceType": "Patient", "id": "pw1", "meta": meta_in, "implicitRules": "urn:rules", "language": "en",
            "extension": ext("pe"), "identifier": ids_in, "active": True, "gender": "female", "birthDate": "1990-07-15",
            "deceasedBoolean": False, "multipleBirthBoolean": True,
            **{c: fill(t(c), c) for c in patient_passthrough},
            "contact": [{**contact, "organization": co_in}], "generalPractitioner": [gp_in],
            "managingOrganization": mo_in, "link": [{"id": "l1", "extension": ext("le"), "other": li_in, "type": "seealso"}]}
    out.append(Witness("patient-all-members", "Patient", fhir, {
        **provenance("Patient", "pw1", witness_path("patient-all-members")),
        **passthrough("Patient", fhir, patient_passthrough + ("implicitRules", "language", "active", "gender",
                                                              "deceasedBoolean", "multipleBirthBoolean")),
        "extension": ext_text("pe"),
        "meta": meta_out, "meta_lastUpdated": meta_out["lastUpdated"], "meta_lastUpdatedOrig": meta_out["lastUpdated"],
        "identifier": ids_out, "identifierOrig": from_json_oracle(ids_in, t("identifierOrig")),
        "birthDate": utc(1990, 7, 15), "birthDateOrig": date(1990, 7, 15),
        "contact": [{**from_json_oracle(contact, t("contact").value_type), "organization": co_out}],
        "generalPractitioner": [gp_out], "managingOrganization": mo_out,
        "link": [{"extension": ext_text("le"), "id": "l1", "other": li_out, "type": "seealso"}],
    }))
    fhir = {"resourceType": "Patient", "id": "pw2", "deceasedDateTime": "2024-03-04T05:06:07+02:00",
            "multipleBirthInteger": 3}
    out.append(Witness("patient-datetime-integer-choices", "Patient", fhir, {
        **provenance("Patient", "pw2", witness_path("patient-datetime-integer-choices")),
        "deceasedDateTime": utc(2024, 3, 4, 3, 6, 7), "deceasedDateTimeOrig": utc(2024, 3, 4, 3, 6, 7),
        "multipleBirthInteger": 3,
    }))

    # Observation: every pass-through and normalized column; each effective[x] and value[x] choice.
    t = lambda c: column_type("Observation", c)  # noqa: E731
    meta_in, meta_out = witness_meta()
    ids_in, ids_out = witness_identifier("Observation", "ow1")
    single = {"subject": "Patient", "encounter": "Encounter", "specimen": "Specimen", "device": "Device"}
    multiple = {"basedOn": "ServiceRequest", "partOf": "Procedure", "focus": "Condition", "performer": "Practitioner",
                "hasMember": "Observation", "derivedFrom": "Observation"}
    refs = {c: reference_witness(rtype, f"{c}-1") for c, rtype in {**single, **multiple}.items()}
    author_in, author_out = reference_witness("Practitioner", "note-author")
    obs_passthrough = ("text", "contained", "category", "code", "valueQuantity", "dataAbsentReason", "interpretation",
                       "bodySite", "method", "referenceRange", "component")
    fhir = {"resourceType": "Observation", "id": "ow1", "meta": meta_in, "implicitRules": "urn:rules", "language": "en",
            "extension": ext("oe"), "identifier": ids_in, "status": "final",
            **{c: fill(t(c), c) for c in obs_passthrough},
            **{c: refs[c][0] for c in single}, **{c: [refs[c][0]] for c in multiple},
            "effectiveDateTime": "2024-01-01", "issued": "2024-01-01T10:00:00.25-03:00",
            "note": [{"id": "n1", "extension": ext("ne"), "authorReference": author_in, "time": "2024-02-03T04:05:06-05:00",
                      "text": "first"}, {"authorString": "Nurse", "text": "second"}]}
    out.append(Witness("observation-all-members", "Observation", fhir, {
        **provenance("Observation", "ow1", witness_path("observation-all-members")),
        **passthrough("Observation", fhir, obs_passthrough + ("implicitRules", "language", "status")),
        "extension": ext_text("oe"),
        "meta": meta_out, "meta_lastUpdated": meta_out["lastUpdated"], "meta_lastUpdatedOrig": meta_out["lastUpdated"],
        "identifier": ids_out, "identifierOrig": from_json_oracle(ids_in, t("identifierOrig")),
        **{c: refs[c][1] for c in single}, **{c: [refs[c][1]] for c in multiple},
        "effectiveDateTime": utc(2024, 1, 1), "effectiveDateTimeOrig": utc(2024, 1, 1),
        "issued": utc(2024, 1, 1, 13, 0, 0, 250000), "issuedOrig": utc(2024, 1, 1, 13, 0, 0, 250000),
        # The second note has no authorReference: normalize_ref_struct(None) returns its early defaults.
        "note": [{"extension": ext_text("ne"), "id": "n1", "authorReference": author_out, "authorString": None,
                  "time": utc(2024, 2, 3, 9, 5, 6), "text": "first"},
                 {"extension": None, "id": None, "authorReference": NULL_INPUT_REF, "authorString": "Nurse", "time": None,
                  "text": "second"}],
    }))
    choices = [
        ("effectivePeriod", {"id": "ep", "extension": ext("epe"), "start": "2024-01-02", "end": "2024-01-03T04:05:06+01:00"},
         {"extension": ext_text("epe"), "id": "ep", "start": utc(2024, 1, 2), "end": utc(2024, 1, 3, 3, 5, 6),
          "startOrig": utc(2024, 1, 2), "endOrig": utc(2024, 1, 3, 3, 5, 6)},
         "valuePeriod", {"id": "vp", "extension": ext("vpe"), "start": "2023-12", "end": "2024"},
         {"extension": ext_text("vpe"), "id": "vp", "start": utc(2023, 12, 1), "end": utc(2024, 1, 1),
          "startOrig": utc(2023, 12, 1), "endOrig": utc(2024, 1, 1)}),
        ("effectiveInstant", "2024-01-01T10:00:00Z", utc(2024, 1, 1, 10), "valueInteger", -4, -4),
        (None, None, None, "valueString", "text value", "text value"),
        (None, None, None, "valueBoolean", False, False),
        (None, None, None, "valueTime", "10:11:12", "10:11:12"),
        (None, None, None, "valueDateTime", "2024-02-29T23:30:00-01:00", utc(2024, 3, 1, 0, 30)),
    ]
    for i, bounds in enumerate(CHOICE_GROUPS[0]):  # effectiveTiming once per repeat.bounds[x]
        value_column = ("valueCodeableConcept", "valueRange", "valueRatio")[i]
        timing = fill(t("effectiveTiming"), "effectiveTiming", bounds)
        value = fill(t(value_column), value_column)
        choices.append(("effectiveTiming", timing, from_json_oracle(timing, t("effectiveTiming")),
                        value_column, value, from_json_oracle(value, t(value_column))))
    value = fill(t("valueSampledData"), "valueSampledData")
    choices.append((None, None, None, "valueSampledData", value, from_json_oracle(value, t("valueSampledData"))))
    for n, (effective, effective_in, effective_out, value_column, value_in, value_out) in enumerate(choices, start=2):
        rid, name = f"ow{n}", f"observation-{value_column}"
        fhir = {"resourceType": "Observation", "id": rid, value_column: value_in}
        expected = {**provenance("Observation", rid, witness_path(name)), value_column: value_out}
        if value_column == "valueDateTime":
            expected["valueDateTimeOrig"] = value_out
        if effective:
            fhir[effective] = effective_in
            expected[effective] = effective_out
            if effective == "effectiveInstant":
                expected["effectiveInstantOrig"] = effective_out
        out.append(Witness(name, "Observation", fhir, expected))
    return tuple(out)


def shape_witness(w: Witness) -> dict:
    return shape_row(w.resource, extracted_from_resource(w.resource, w.fhir), source_system=EAST,
                     logical_path=witness_path(w.name), ingest_time=INGEST)


def leaf_paths(t, prefix: str) -> set:
    if pa.types.is_struct(t):
        return set().union(*(leaf_paths(f.type, f"{prefix}.{f.name}") for f in t))
    if pa.types.is_list(t):
        return leaf_paths(t.value_type, f"{prefix}[]")
    return {prefix}


def populated_paths(value, t, prefix: str) -> set:
    if value is None:
        return set()
    if pa.types.is_struct(t):
        return set().union(*(populated_paths(value[f.name], f.type, f"{prefix}.{f.name}") for f in t))
    if pa.types.is_list(t):
        return set().union(set(), *(populated_paths(v, t.value_type, f"{prefix}[]") for v in value))
    return {prefix}


REFERENCE_SLOTS = {
    "Patient": ("generalPractitioner[]", "managingOrganization", "contact[].organization", "link[].other",
                "identifier[].assigner"),
    "Observation": ("basedOn[]", "partOf[]", "subject", "focus[]", "encounter", "performer[]", "specimen", "device",
                    "hasMember[]", "derivedFrom[]", "note[].authorReference", "identifier[].assigner"),
}


def always_null(resource: str) -> dict:
    """Silver leaf paths no FHIR input can populate under the HDS rules, each with its reference condition."""
    conditions = {}
    for slot in REFERENCE_SLOTS[resource]:
        conditions[f"{slot}.reference"] = (
            "reference_normalization.normalize_ref_struct moves every non-empty reference to msftSourceReference and "
            "sets reference to None; FHIR forbids empty strings, so no reference survives")
        for member in ("extension", "id", "start", "end"):
            conditions[f"{slot}.identifier.period.{member}"] = (
                "the raw Reference.identifier.period is a StringType field; identifier_normalization."
                "conform_to_row_identifier_schema reads its members with utils.get_row_field, which returns the default "
                "(None) for a value that is not a Row or dict")
    return conditions


def render_for_json(value, t):
    """The structure json.loads(companion, parse_float=Decimal, object_pairs_hook=list) must give for a column."""
    if value is None:
        return None
    if pa.types.is_struct(t):
        return [(f.name, render_for_json(value[f.name], f.type)) for f in t]
    if pa.types.is_list(t):
        return [render_for_json(v, t.value_type) for v in value]
    if pa.types.is_timestamp(t):
        return f"{value:%Y-%m-%dT%H:%M:%S}.{value.microsecond // 1000:03d}Z"
    if pa.types.is_date(t):
        return value.isoformat()
    return value


def assert_companions_render_columns(case: unittest.TestCase, resource: str, row: dict) -> None:
    schema = target_schema(resource)
    for name in schema.names:
        if not name.endswith("_string"):
            continue
        base = name[: -len("_string")]
        with case.subTest(resource=resource, column=name):
            if row[base] is None:
                case.assertIsNone(row[name])
                continue
            parsed = json.loads(row[name], parse_float=Decimal, object_pairs_hook=list)
            case.assertEqual(parsed, render_for_json(row[base], schema.field(base).type))


class FieldPathEvidenceTests(unittest.TestCase):
    """source-derived, unverified against a captured run: witness expectations follow from_json plus the HDS
    normalization files named on each helper; none of them come from a captured HDS run."""

    def test_witness_columns_equal_hand_derived_values(self):
        for w in witnesses():
            row = shape_witness(w)
            pa.Table.from_pylist([row], schema=target_schema(w.resource))
            for column, expected in w.expected.items():
                with self.subTest(witness=w.name, column=column):
                    self.assertEqual(row[column], expected)

    def test_every_silver_leaf_is_populated_or_has_a_named_always_null_condition(self):
        for resource in ("Patient", "Observation"):
            schema = target_schema(resource)
            self.assertEqual(schema_digest(schema), DIGESTS[resource][1])
            leaves = set().union(*(leaf_paths(f.type, f.name) for f in schema))
            conditions = always_null(resource)
            self.assertLessEqual(set(conditions), leaves, "always-null paths must exist in the deployed schema")
            evidenced, seen = set(), set()
            for w in (w for w in witnesses() if w.resource == resource):
                row = shape_witness(w)
                for column, expected in w.expected.items():
                    evidenced |= populated_paths(expected, schema.field(column).type, column)
                    if column + "_string" in schema.names and row[column + "_string"] is not None:
                        evidenced.add(column + "_string")  # content checked by the companion test
                seen |= set().union(*(populated_paths(row[f.name], f.type, f.name) for f in schema))
            with self.subTest(resource=resource, check="every leaf evidenced"):
                self.assertEqual(sorted(leaves - evidenced - set(conditions)), [])
            with self.subTest(resource=resource, check="always-null leaves stay null"):
                self.assertEqual(sorted(seen & set(conditions)), [])


# Every reason shape_row may report; each is fixed text that never carries source data.
SHAPE_REASONS = frozenset({
    "is required", "unsupported resource", "is not a FHIR date, dateTime or instant", "is not a FHIR date",
    "is not a valid date or time", "expected a JSON number", "does not fit decimal(36,18)", "expected a JSON object",
    "has a field that is not in the HDS Silver schema", "expected a JSON array", "null array elements are not FHIR",
    "expected a boolean", "expected a 32-bit integer", "expected a date/time string", "expected JSON array text",
    "is not valid JSON", "expected at most one value", "expected a string", "is out of double range",
    "is not JSON object text", "is not encodable as UTF-8", "is nested too deeply", "is out of timestamp range",
    "source_system must be a non-empty string", "logical_path must be a non-empty string",
    "ingest_time must be a timezone-aware datetime",
})
PLACEHOLDER = "__MUTATION_SITE__"
SCHEMAS = {resource: target_schema(resource) for resource in DIGESTS}
SURROGATE_SENTINEL = SENTINEL + "\ud800"
# JSON text substituted for one node of a populated witness. Wrong types, unencodable text, huge, non-finite and
# deeply nested values, each carrying the sentinel where it can.
JSON_MUTATIONS = (
    json.dumps(SENTINEL), json.dumps(SURROGATE_SENTINEL), json.dumps("\udc00" + SENTINEL), "42", "-0.0", "true", "false",
    "null", "1e400", "-1e400", "NaN", "Infinity", "9" * 5000, "1" + "0" * 40, "1e-400", "2147483648",
    f"[{json.dumps(SENTINEL)}]", "[null]", "[42]", "[[]]", f"{{{json.dumps(SENTINEL)}:1}}",
    f"{{{json.dumps(SURROGATE_SENTINEL)}:1}}", f'{{"text":{json.dumps(SURROGATE_SENTINEL)}}}',
    f'{{"coding":42,"text":{json.dumps(SENTINEL)}}}', f'{{"coding":[42,{json.dumps(SENTINEL)}]}}',
    f'{{"start":99999999999999999999,"end":{json.dumps(SENTINEL)}}}',
    "[" * 3000 + json.dumps(SENTINEL) + "]" * 3000,
    # Exponents beyond Decimal's (and Java BigDecimal's) range, huge and tiny, signed, zero mantissa; long digit strings.
    "1e999999999999999999999999", "-1e999999999999999999999999", "1e-999999999999999999999999",
    "-1e-999999999999999999999999", "0e+9999999999999999999", "0." + "0" * 5000 + "1", "9" * 5000 + ".5",
    f'{{"value":1e999999999999999999999999,"unit":{json.dumps(SENTINEL)}}}',
    # JSON text inside a string (HDS reparses Reference.identifier members held as strings).
    json.dumps(f'{{"coding":42,"text":{json.dumps(SENTINEL)}}}'), json.dumps(f'{{"value":"{SENTINEL}\\ud800"}}'),
    json.dumps("[" * 3000 + "]" * 3000), json.dumps(f'{{"identifier":{{"value":"{SENTINEL}\\ud800"}}}}'),
    json.dumps(f'{{"start":99999999999999999999,"text":{json.dumps(SENTINEL)}}}'),
)
# Python values substituted for a whole extraction column.
COLUMN_MUTATIONS = (SENTINEL, SURROGATE_SENTINEL, 42, True, 1.5, float("nan"), 10 ** 40, [SENTINEL], {SENTINEL: SENTINEL},
                    f"[{json.dumps(SENTINEL)}", f"{{{json.dumps(SENTINEL)}:1}}", f"[{json.dumps(SURROGATE_SENTINEL)}]",
                    "[" * 3000 + "]" * 3000)


def mutation_sites(value, path: str, keys: tuple = ()):
    """(site path, key chain) for every node below a JSON value, containers included."""
    items = value.items() if isinstance(value, dict) else enumerate(value) if isinstance(value, list) else ()
    for key, child in items:
        child_path = f"{path}.{key}" if isinstance(value, dict) else f"{path}[]"
        yield child_path, keys + (key,)
        yield from mutation_sites(child, child_path, keys + (key,))


def substitute(value, keys: tuple, replacement):
    """value with the node at keys replaced; each array on the way keeps only the element holding that node."""
    if not keys:
        return replacement
    if isinstance(value, list):
        return [substitute(value[keys[0]], keys[1:], replacement)]
    return {**value, keys[0]: substitute(value[keys[0]], keys[1:], replacement)}


def mutated_rows():
    """(resource, column, site, extracted row) for each mutation of each distinct site across the witnesses.

    Normalization is per column (only id feeds the identifier rules, and blank_row supplies it), so the mutated
    column is shaped in an otherwise blank row.
    """
    seen = set()
    for w in witnesses():
        for column in extracted_from_resource(w.resource, w.fhir):
            if (w.resource, column, "(column)") not in seen:
                seen.add((w.resource, column, "(column)"))
                for value in COLUMN_MUTATIONS:
                    yield w.resource, column, "(column)", blank_row(w.resource, **{column: value})
            if column not in COLLECTION_COLUMNS[w.resource] or w.fhir.get(column) is None:
                continue
            for site, keys in mutation_sites(w.fhir[column], column):
                if (w.resource, site) in seen:
                    continue
                seen.add((w.resource, site))
                fhir = {**w.fhir, column: substitute(w.fhir[column], keys, PLACEHOLDER)}
                template = extracted_from_resource(w.resource, fhir)[column]
                for text in JSON_MUTATIONS:
                    yield w.resource, column, site, blank_row(
                        w.resource, **{column: template.replace(json.dumps(PLACEHOLDER), text, 1)})


def mutation_problem(resource: str, column: str | None, call: dict):
    """None when shape_row gives a row or an identified, redacted ShapeError; else what is wrong.

    Returns the row (or None for an error) as the second item so callers can check rows are writable.
    """
    try:
        return None, shape_row(resource, **call)
    except ShapeError as error:
        failure = error
    except Exception as other:  # anything else may leak its message and lacks resource and path
        return f"{type(other).__name__} escaped shape_row", None
    logged = "".join(traceback.format_exception(failure)) + repr(failure.args) + str(failure)
    checks = (
        (failure.resource == resource, "wrong resource"),
        (failure.reason in SHAPE_REASONS, "reason is not a fixed reason"),
        (column is None or re.match(rf"{column}(?:[.\[]|$)", failure.path), "path is not rooted at the column"),
        (failure.__cause__ is None and failure.__context__ is None, "error is chained"),
        (SENTINEL not in logged, "sentinel is logged"),
    )
    return next((f"{problem}: {failure.path}: {failure.reason}" for ok, problem in checks if not ok), None), None


class MutationRedactionTests(unittest.TestCase):
    """Every input-triggered outcome is a valid Silver row or an identified, redacted ShapeError.

    Mutates each distinct node of the populated witnesses (leaves and containers) and each extraction column with
    wrong-typed, unencodable, huge, non-finite and deeply nested sentinel-bearing values.
    """

    def test_mutated_witnesses_give_rows_or_redacted_errors(self):
        problems, rows, errors = [], {resource: [] for resource in DIGESTS}, 0
        for resource, column, site, extracted in mutated_rows():
            problem, row = mutation_problem(resource, column, dict(
                extracted=extracted, source_system=EAST, logical_path="Files/m.ndjson", ingest_time=INGEST))
            if problem:
                problems.append(f"{resource} {site}: {problem}")
            elif row is None:
                errors += 1
            else:
                rows[resource].append((site, row))
        self.assertEqual(problems[:20], [], f"{len(problems)} mutations escaped redaction")
        for resource, shaped in rows.items():
            try:  # a valid row must be writable as Silver (UTF-8 strings, typed values)
                pa.Table.from_pylist([row for _, row in shaped], schema=SCHEMAS[resource])
            except (pa.ArrowException, UnicodeError):
                unwritable = []
                for site, row in shaped:
                    try:
                        pa.Table.from_pylist([row], schema=SCHEMAS[resource])
                    except (pa.ArrowException, UnicodeError) as error:
                        unwritable.append(f"{site}: {type(error).__name__}")
                self.fail(f"{resource}: {len(unwritable)} shaped rows are not writable: {unwritable[:20]}")
        self.assertGreater(sum(map(len, rows.values())), 0)
        self.assertGreater(errors, 0)

    def test_unencodable_or_out_of_range_arguments_are_identified(self):
        base = dict(extracted=blank_row("Patient"), source_system=EAST, logical_path="Files/m.ndjson", ingest_time=INGEST)
        for name, value in (("source_system", SURROGATE_SENTINEL), ("logical_path", SURROGATE_SENTINEL),
                            ("ingest_time", datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=5))))):
            with self.subTest(argument=name):
                problem, row = mutation_problem("Patient", None, {**base, name: value})
                self.assertIsNone(problem)
                self.assertIsNone(row, "expected a ShapeError")

    def test_programming_errors_are_not_disguised_as_shape_errors(self):
        """Only input-triggered failures at a mapping path become ShapeError; a bug in this module must surface as itself."""
        from unittest import mock
        import fabric_silver.normalize as normalize
        injected = (
            ("_resource_id", AttributeError, {}),
            ("_timestamp", TypeError, {"effectiveDateTime": "2025-01-01T00:00:00Z"}),
            ("_decimal", ArithmeticError, {"valueQuantity": '[{"value":1.5}]'}),
            ("_jackson", ValueError, {"extension": '[{"url":"u","valueInteger":1}]'}),
            ("_normalize_reference", KeyError, {"subject": '[{"reference":"Patient/p1"}]'}),
        )
        for helper, error, columns in injected:
            with self.subTest(helper=helper), mock.patch.object(normalize, helper, side_effect=error("injected")):
                with self.assertRaises(error) as caught:
                    shape("Observation", **columns)
                self.assertNotIsInstance(caught.exception, ShapeError)


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
