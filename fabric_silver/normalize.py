"""Shape lossless extraction rows into Fabric HDS v1.4.0 Silver rows.

The Silver schema comes from the parity contracts (column order and logical
type names) and ``parity/types.json`` (struct layouts); it must equal the
deployed HDS Delta schema exactly.

Value rules re-implement, without Spark, what HDS does between reading a FHIR
resource and writing Silver: ``from_json`` against the raw (Avro-derived)
column types, the configured normalizations in
``flatten/core.py::__process_normalization`` (resource id and identifier,
references, dates) and ``dataframe_utils.stringify_complex_types``. They were
derived from reading the HDS source; no captured HDS run exists, so none of
this is verified parity.

Input that HDS would silently drop or null (unknown JSON fields, type
mismatches, unparsable dates) is rejected with ShapeError instead, so that a
gap is never mistaken for a value.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from functools import lru_cache

import pyarrow as pa

from fabric_silver.views import CONTRACT_DIR, RESOURCES, load_contract


class ShapeError(ValueError):
    """The extracted row, or a shaping argument, cannot become a Silver row."""


UTC = timezone.utc
_SCALE = Decimal(1).scaleb(-18)
_MAX_DIGITS = 36
_INT32 = (-(2 ** 31), 2 ** 31 - 1)

# ---------------------------------------------------------------- logical types


@dataclass(frozen=True)
class _List:
    element: object


@dataclass(frozen=True)
class _Struct:
    name: str
    fields: tuple  # ((field name, type), ...) in schema order

    @property
    def names(self) -> tuple:
        return tuple(name for name, _ in self.fields)


_ARROW = {
    "string": pa.string(),
    "bool": pa.bool_(),
    "int32": pa.int32(),
    "decimal": pa.decimal128(36, 18),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
}


@lru_cache(maxsize=None)
def _type_spec() -> dict:
    return json.loads((CONTRACT_DIR / "types.json").read_text(encoding="utf-8"))["structs"]


@lru_cache(maxsize=None)
def _resolve(expr: str, raw: bool):
    """Resolve a type expression; raw=True gives the shape from_json produces before normalization."""
    if expr.startswith("list<") and expr.endswith(">"):
        return _List(_resolve(expr[5:-1], raw))
    if expr in _ARROW:
        return expr
    entry = _type_spec()[expr]
    if isinstance(entry, dict):
        name = entry["parsedAs"] if raw else expr
        fields = _type_spec()[name] if raw else entry["fields"]
    else:
        name, fields = expr, entry
    return _Struct(name, tuple((field, _resolve(t, raw)) for field, t in fields))


def _arrow(node) -> pa.DataType:
    if isinstance(node, _List):
        return pa.list_(pa.field("element", _arrow(node.element), nullable=True))
    if isinstance(node, _Struct):
        return pa.struct([pa.field(name, _arrow(t), nullable=True) for name, t in node.fields])
    return _ARROW[node]


@dataclass(frozen=True)
class _Resource:
    name: str
    columns: tuple  # ((silver column, logical type expression), ...)
    extraction: dict  # extraction column -> extraction type

    def silver(self, column: str):
        return _resolve(dict(self.columns)[column], False)

    def raw(self, column: str):
        """from_json type of an extraction column: its *Orig column keeps the pre-normalization type."""
        types = dict(self.columns)
        return _resolve(types.get(column + "Orig", types[column]), True)


@lru_cache(maxsize=None)
def _resource(resource: str) -> _Resource:
    if resource not in RESOURCES:
        raise ShapeError(f"unsupported resource {resource!r}; expected one of {RESOURCES}")
    contract = load_contract(resource)
    return _Resource(
        resource,
        tuple((c["name"], c["silverType"]) for c in contract["silverColumns"]),
        {c["column"]: c["type"] for c in contract["extraction"]},
    )


def target_schema(resource: str) -> pa.Schema:
    """The deployed HDS v1.4.0 Silver schema for the resource (every field nullable)."""
    spec = _resource(resource)
    return pa.schema([pa.field(name, _arrow(spec.silver(name)), nullable=True) for name, _ in spec.columns])


# ------------------------------------------------------------ JSON emulation


_SHORT_ESCAPES = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\t": "\\t", "\n": "\\n", "\f": "\\f", "\r": "\\r"}


def _quote(text: str) -> str:
    """Jackson's default string escaping: quote, backslash and C0 controls only."""
    out = []
    for ch in text:
        if ch in _SHORT_ESCAPES:
            out.append(_SHORT_ESCAPES[ch])
        elif ord(ch) < 0x20:
            out.append(f"\\u{ord(ch):04X}")
        else:
            out.append(ch)
    return '"' + "".join(out) + '"'


def _java_double(value: Decimal) -> str:
    """Java Double.toString of the double nearest the JSON number (what Jackson copies)."""
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        raise ShapeError(f"JSON number {value} is out of double range")
    if number == 0:
        return "-0.0" if str(number).startswith("-") else "0.0"
    shortest = Decimal(repr(abs(number))).normalize()
    sign = "-" if number < 0 else ""
    if Decimal("1e-3") <= shortest < Decimal("1e7"):
        text = format(shortest, "f")
        return sign + (text if "." in text else text + ".0")
    digits = "".join(map(str, shortest.as_tuple().digits))
    exponent = len(digits) - 1 + shortest.as_tuple().exponent
    return f"{sign}{digits[0]}.{digits[1:] or '0'}E{exponent}"


def _jackson(value) -> str:
    """Compact JSON text Spark keeps when a non-string JSON value lands in a StringType field."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _java_double(value)
    if isinstance(value, str):
        return _quote(value)
    if isinstance(value, list):
        return "[" + ",".join(_jackson(v) for v in value) + "]"
    return "{" + ",".join(_quote(k) + ":" + _jackson(v) for k, v in value.items()) + "}"


def _to_json(value, node) -> str:
    """to_json(col, ignoreNullFields=false) of a Silver value."""
    if value is None:
        return "null"
    if isinstance(node, _Struct):
        return "{" + ",".join(_quote(name) + ":" + _to_json(value[name], t) for name, t in node.fields) + "}"
    if isinstance(node, _List):
        return "[" + ",".join(_to_json(v, node.element) for v in value) + "]"
    if node == "string":
        return _quote(value)
    if node == "bool":
        return "true" if value else "false"
    if node in ("int32", "decimal"):
        return str(value)  # Decimal str follows Java BigDecimal.toString at scale 18
    if node == "timestamp":
        moment = value.astimezone(UTC)
        return (f'"{moment.year:04d}-{moment.month:02d}-{moment.day:02d}T{moment.hour:02d}:{moment.minute:02d}:'
                f'{moment.second:02d}.{moment.microsecond // 1000:03d}Z"')
    return f'"{value.year:04d}-{value.month:02d}-{value.day:02d}"'


# ---------------------------------------------------------- from_json parsing

_TIMESTAMP = re.compile(
    r"(\d{4})(?:-(\d{2})(?:-(\d{2})(?:T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2}))?)?)?")
_DATE = re.compile(r"(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?")


def _timestamp(text: str, path: str) -> datetime:
    """FHIR date/dateTime/instant text as a UTC instant (session time zone assumed UTC)."""
    match = _TIMESTAMP.fullmatch(text)
    if not match:
        raise ShapeError(f"{path}: {text!r} is not a FHIR date, dateTime or instant")
    year, month, day, hour, minute, second, fraction, offset = match.groups()
    try:
        if hour is None:
            return datetime(int(year), int(month or 1), int(day or 1), tzinfo=UTC)
        if offset == "Z":
            zone = UTC
        else:
            sign = -1 if offset[0] == "-" else 1
            zone = timezone(sign * timedelta(hours=int(offset[1:3]), minutes=int(offset[4:6])))
        micros = int((fraction or "")[:6].ljust(6, "0"))
        local = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), micros, tzinfo=zone)
    except ValueError as error:
        raise ShapeError(f"{path}: {text!r} is not a valid instant: {error}") from None
    return local.astimezone(UTC)


def _date(text: str, path: str) -> date:
    match = _DATE.fullmatch(text)
    if not match:
        raise ShapeError(f"{path}: {text!r} is not a FHIR date")
    year, month, day = match.groups()
    try:
        return date(int(year), int(month or 1), int(day or 1))
    except ValueError as error:
        raise ShapeError(f"{path}: {text!r} is not a valid date: {error}") from None


def _decimal(value, path: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise ShapeError(f"{path}: expected a JSON number, got {value!r}")
    try:
        scaled = Decimal(value).quantize(_SCALE, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        raise ShapeError(f"{path}: {value} does not fit decimal(36,18)") from None
    if len(scaled.as_tuple().digits) > _MAX_DIGITS:
        raise ShapeError(f"{path}: {value} does not fit decimal(36,18)")
    return scaled


def _parse(value, node, path: str):
    """from_json of one JSON value against a raw type, rejecting what Spark would drop or null."""
    if value is None:
        return None
    if isinstance(node, _Struct):
        if not isinstance(value, dict):
            raise ShapeError(f"{path}: expected a JSON object")
        unknown = set(value) - set(node.names)
        if unknown:
            raise ShapeError(f"{path}: fields {sorted(unknown)} are not in the HDS Silver schema")
        return {name: _parse(value.get(name), t, f"{path}.{name}") for name, t in node.fields}
    if isinstance(node, _List):
        if not isinstance(value, list):
            raise ShapeError(f"{path}: expected a JSON array")
        if any(v is None for v in value):
            raise ShapeError(f"{path}: null array elements are not FHIR")
        return [_parse(v, node.element, f"{path}[{i}]") for i, v in enumerate(value)]
    if node == "string":
        return value if isinstance(value, str) else _jackson(value)
    if node == "bool":
        if not isinstance(value, bool):
            raise ShapeError(f"{path}: expected a boolean, got {value!r}")
        return value
    if node == "int32":
        if isinstance(value, bool) or not isinstance(value, int) or not _INT32[0] <= value <= _INT32[1]:
            raise ShapeError(f"{path}: expected a 32-bit integer, got {value!r}")
        return value
    if node == "decimal":
        return _decimal(value, path)
    if not isinstance(value, str):
        raise ShapeError(f"{path}: expected a date/time string, got {value!r}")
    return _timestamp(value, path) if node == "timestamp" else _date(value, path)


_SCALAR_TYPES = {"string": str, "boolean": bool, "integer": int}


def _parse_column(spec: _Resource, column: str, value):
    node = spec.raw(column)
    kind = spec.extraction[column]
    if column in _collection_columns(spec.name):
        if not isinstance(value, str):
            raise ShapeError(f"{column}: expected JSON array text, got {value!r}")
        try:
            items = json.loads(value, parse_float=Decimal)
        except json.JSONDecodeError as error:
            raise ShapeError(f"{column}: invalid JSON: {error}") from None
        if not isinstance(items, list) or any(item is None for item in items):
            raise ShapeError(f"{column}: expected a JSON array without nulls")
        if not items:
            return None
        if isinstance(node, _Struct):
            if len(items) != 1:
                raise ShapeError(f"{column}: expected at most one value, got {len(items)}")
            return _parse(items[0], node, column)
        return _parse(items, node, column)
    if value is not None and (isinstance(value, bool) != (kind == "boolean")
                              or not isinstance(value, _SCALAR_TYPES[kind])):
        raise ShapeError(f"{column}: expected {kind}, got {value!r}")
    return _parse(value, node, column)


@lru_cache(maxsize=None)
def _collection_columns(resource: str) -> frozenset:
    """Columns the views emit as JSON array text (complex types and the string-typed extension)."""
    spec = _resource(resource)
    return frozenset(c for c in spec.extraction if c == "extension" or not isinstance(spec.raw(c), str))


# ------------------------------------------------- HDS reference normalization
# Mirrors flatten/normalization/{reference,identifier}_normalization.py and utils.py on plain dicts.
# A populated dict stands for a Spark Row (truthy even when every value is null); the empty dict
# stands for HDS's `{}` placeholder (falsy), which Spark writes as a non-null all-null struct.

_TYPES = ("Account|ActivityDefinition|AdministrableProductDefinition|AdverseEvent|AllergyIntolerance|Appointment|"
          "AppointmentResponse|AuditEvent|Basic|Binary|BiologicallyDerivedProduct|BodyStructure|Bundle|CapabilityStatement|"
          "CarePlan|CareTeam|CatalogEntry|ChargeItem|ChargeItemDefinition|Citation|Claim|ClaimResponse|ClinicalImpression|"
          "ClinicalUseDefinition|CodeSystem|Communication|CommunicationRequest|CompartmentDefinition|Composition|ConceptMap|"
          "Condition|Consent|Contract|Coverage|CoverageEligibilityRequest|CoverageEligibilityResponse|DetectedIssue|Device|"
          "DeviceDefinition|DeviceMetric|DeviceRequest|DeviceUseStatement|DiagnosticReport|DocumentManifest|DocumentReference|"
          "Encounter|Endpoint|EnrollmentRequest|EnrollmentResponse|EpisodeOfCare|EventDefinition|Evidence|EvidenceReport|"
          "EvidenceVariable|ExampleScenario|ExplanationOfBenefit|FamilyMemberHistory|Flag|Goal|GraphDefinition|Group|"
          "GuidanceResponse|HealthcareService|ImagingStudy|Immunization|ImmunizationEvaluation|ImmunizationRecommendation|"
          "ImplementationGuide|Ingredient|InsurancePlan|Invoice|Library|Linkage|List|Location|ManufacturedItemDefinition|"
          "Measure|MeasureReport|Media|Medication|MedicationAdministration|MedicationDispense|MedicationKnowledge|"
          "MedicationRequest|MedicationStatement|MedicinalProductDefinition|MessageDefinition|MessageHeader|MolecularSequence|"
          "NamingSystem|NutritionOrder|NutritionProduct|Observation|ObservationDefinition|OperationDefinition|"
          "OperationOutcome|Organization|OrganizationAffiliation|PackagedProductDefinition|Patient|PaymentNotice|"
          "PaymentReconciliation|Person|PlanDefinition|Practitioner|PractitionerRole|Procedure|Provenance|Questionnaire|"
          "QuestionnaireResponse|RegulatedAuthorization|RelatedPerson|RequestGroup|ResearchDefinition|"
          "ResearchElementDefinition|ResearchStudy|ResearchSubject|RiskAssessment|Schedule|SearchParameter|ServiceRequest|"
          "Slot|Specimen|SpecimenDefinition|StructureDefinition|StructureMap|Subscription|SubscriptionStatus|"
          "SubscriptionTopic|Substance|SubstanceDefinition|SupplyDelivery|SupplyRequest|Task|TerminologyCapabilities|"
          "TestReport|TestScript|ValueSet|VerificationResult|VisionPrescription")
_LITERAL = re.compile(r"((http|https):\/\/([A-Za-z0-9\-\\\.\:\%\$]*\/)+)?(" + _TYPES
                      + r")\/([A-Za-z0-9\-\.]{1,64})(\/_history\/[A-Za-z0-9\-\.]{1,64})?")
# HDS wraps the type alternation in a group of its own inside this group, hence groups 4-6 below.
_LOGICAL = re.compile(r"(?:((http|https):\/\/[A-Za-z0-9\-\\\.\:\%\$]*\/))?((" + _TYPES
                      + r"))\?identifier=(?:(.*)\|)?(.*)")

_FHIR_ID_SYSTEM = "http://terminology.hl7.org/CodeSystem/v2-0203"
_FHIR_ID_CODE = "fhirId"
_FHIR_ID_TEXT = "FHIR Id"
_CODEABLE_CONCEPT_FIELDS = ("extension", "id", "coding", "text")


def _fhir_id_coding() -> dict:
    return {"extension": None, "id": None, "system": _FHIR_ID_SYSTEM, "version": None, "code": _FHIR_ID_CODE,
            "display": _FHIR_ID_TEXT, "userSelected": None}


def _without_scheme(url):
    if not isinstance(url, str) or not url:
        return None
    url = re.sub(r"^\w+://", "", url)
    if url.endswith("/"):
        url = url[:-1]
    return url or None


def _resource_id(rid, type_, base):
    if rid is None or type_ is None:
        return None
    full = f"{type_}/{rid}"
    if base:
        full = f"{_without_scheme(base)}/{full}"
    return hashlib.sha256(full.encode()).hexdigest()


def _field(row, name, default=None, cast=None):
    """NormalizationUtils.get_row_field."""
    if not isinstance(row, dict):
        return default
    value = row[name] if name in row and row[name] is not None else default
    if cast and value is not None:
        try:
            value = cast(value)
        except (ValueError, TypeError):
            value = default
    return value


def _from_internal(value):
    """TimestampType().fromInternal: microseconds since the epoch; anything else raises TypeError."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("not epoch microseconds")
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value)


def _row_from_string(text: str, names) -> dict:
    """NormalizationUtils.create_row_from_string; HDS fails the job where this raises."""
    try:
        data = json.loads(text) if text else {}
    except json.JSONDecodeError as error:
        raise ShapeError(f"reference identifier text {text!r} is not JSON: {error}") from None
    if not isinstance(data, dict):
        raise ShapeError(f"reference identifier text {text!r} is not a JSON object")
    return {name: data.get(name) for name in names}


def _parse_assigner(value):
    """NormalizationUtils.parse_assigner_json_safely."""
    if not (value and isinstance(value, str)):
        return value
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError, ValueError):
        return value
    if not isinstance(parsed, dict):
        return value
    nested = parsed.get("identifier")
    if nested and isinstance(nested, dict):
        nested = {name: nested.get(name) for name in ("extension", "use", "value", "system", "type", "period", "assigner")}
    return {"extension": parsed.get("extension"), "id": parsed.get("id"), "reference": parsed.get("reference"),
            "type": parsed.get("type"), "identifier": nested, "display": parsed.get("display")}


def _conform_identifier_row(row) -> dict:
    """IdentifierNormalization.conform_to_row_identifier_schema."""
    type_ = _field(row, "type")
    if isinstance(type_, str):
        type_ = _row_from_string(type_, _CODEABLE_CONCEPT_FIELDS)
    if type_:
        coding = [{"extension": _field(c, "extension", cast=str), "id": _field(c, "id", cast=str),
                   "system": _field(c, "system", cast=str), "version": _field(c, "version", cast=str),
                   "code": _field(c, "code", cast=str), "display": _field(c, "display", cast=str),
                   "userSelected": _field(c, "userSelected", cast=bool)}
                  for c in _field(type_, "coding", [])]
        type_ = {"extension": _field(type_, "extension", cast=str), "id": _field(type_, "id", cast=str),
                 "coding": coding, "text": _field(type_, "text", cast=str)}
    else:
        type_ = {}
    period = _field(row, "period")
    if period:
        period = {"extension": _field(period, "extension", cast=str), "id": _field(period, "id", cast=str),
                  "start": _field(period, "start", cast=_from_internal), "end": _field(period, "end", cast=_from_internal)}
    else:
        period = {}
    assigner = _field(row, "assigner")
    if assigner:
        nested = _field(assigner, "identifier")
        nested = {name: _field(nested, name, cast=str)
                  for name in ("extension", "use", "type", "value", "system", "period", "assigner")} if nested else {}
        assigner = {"extension": _field(assigner, "extension", cast=str), "id": _field(assigner, "id", cast=str),
                    "reference": _field(assigner, "reference", cast=str), "type": _field(assigner, "type", cast=str),
                    "identifier": nested, "display": _field(assigner, "display", cast=str)}
    else:
        assigner = {}
    return {"extension": _field(row, "extension", cast=str), "use": _field(row, "use", cast=str),
            "value": _field(row, "value", cast=str), "system": _field(row, "system", cast=str),
            "type": type_, "period": period, "assigner": assigner}


def _has_fhir_id(identifier) -> bool:
    """IdentifierNormalization.source_id_identifier_exists_in_row."""
    type_ = _field(identifier, "type")
    if type_ is None:
        return False
    coding = _field(type_, "coding", cast=list)
    if coding is None:
        return False
    return any(_field(c, "code", cast=str) == _FHIR_ID_CODE for c in coding)


def _add_source_id(identifier, value, system) -> dict:
    """IdentifierNormalization.add_source_id_to_identifier_row."""
    identifier = _conform_identifier_row(identifier)
    if _has_fhir_id(identifier) or _field(identifier, "value") or _field(identifier, "system"):
        return identifier
    type_ = _field(identifier, "type")
    if type_:
        type_ = {"extension": _field(type_, "extension"), "id": _field(type_, "id"),
                 "coding": list(_field(type_, "coding", [])) + [_fhir_id_coding()], "text": _field(type_, "text")}
    else:
        type_ = {"extension": None, "id": None, "coding": [_fhir_id_coding()], "text": _FHIR_ID_TEXT}
    return {"extension": _field(identifier, "extension"), "use": _field(identifier, "use"), "value": value,
            "system": system, "type": type_, "period": _field(identifier, "period", {}),
            "assigner": _field(identifier, "assigner", {})}


def _create_identifier(identifier, value, system) -> dict:
    return {"extension": identifier["extension"], "use": identifier["use"], "value": value, "system": system,
            "type": identifier["type"], "period": identifier["period"], "assigner": identifier["assigner"]}


def _reference_details(reference: str):
    """NormalizationUtils.get_reference_details: (kind, base, type, id, system, value)."""
    literal = _LITERAL.search(reference)
    if literal:
        return "Literal", literal.group(1) or None, literal.group(4), literal.group(5), None, None
    logical = _LOGICAL.search(reference)
    if logical:
        return "Logical", logical.group(1) or None, logical.group(4), None, logical.group(5), logical.group(6)
    return None, None, None, None, None, None


def _normalize_reference(row, source: str) -> dict:
    """ReferenceNormalization.normalize_ref_struct (no dereferencing; only the reference text is parsed)."""
    reference = type_ = display = extension = id_ = id_orig = original = None
    identifier = {"extension": None, "use": None, "value": None, "system": None, "type": None, "period": None,
                  "assigner": None}
    if isinstance(row, dict) and row:
        reference, type_ = row.get("reference"), row.get("type")
        display, extension = row.get("display"), row.get("extension")
        original = reference
        previous = row.get("identifier", {})
        if previous:
            if isinstance(previous, dict):
                identifier = {name: previous.get(name) for name in ("extension", "use", "value", "system", "type", "period")}
                identifier["assigner"] = _parse_assigner(previous.get("assigner"))
            elif isinstance(previous, str):
                identifier = _row_from_string(previous, tuple(identifier))
        identifier = _conform_identifier_row(identifier)
        if reference:
            kind, base, parsed_type, rid, system, value = _reference_details(reference)
            type_ = type_ or parsed_type
            if kind == "Literal" and (not base or _without_scheme(base) == _without_scheme(source)):
                id_ = _resource_id(rid, type_, source)
                id_orig = rid
                identifier = _add_source_id(identifier, rid, source)
            if kind == "Logical":
                if not base and not system:
                    identifier = _create_identifier(identifier, value, source)
                elif _without_scheme(base) == _without_scheme(source) or (not base and system):
                    identifier = _create_identifier(identifier, value, system)
            original, reference = reference, None
    return {"reference": reference, "type": type_, "identifier": identifier, "display": display,
            "extension": extension, "id": id_, "idOrig": id_orig, "msftSourceReference": original}


# ---------------------------------------------------- HDS identifier and dates


def _conform_identifier_column(identifiers) -> list:
    """IdentifierNormalization.conform_to_col_identifier_schema (Spark column expressions)."""
    def coding(c):
        return {name: c[name] for name in ("extension", "id", "system", "version", "code", "display", "userSelected")}

    def element(x):
        type_, period, assigner = x["type"], x["period"], x["assigner"]
        if type_ is not None:
            type_ = {"extension": type_["extension"], "id": type_["id"],
                     "coding": [coding(c) for c in type_["coding"] or []], "text": type_["text"]}
        if period is not None:
            period = {name: period[name] for name in ("extension", "id", "start", "end")}
        if assigner is not None:
            nested = assigner["identifier"] or {}
            assigner = {"extension": assigner["extension"], "id": assigner["id"], "reference": assigner["reference"],
                        "type": assigner["type"],
                        "identifier": {name: nested.get(name)
                                       for name in ("extension", "use", "type", "value", "system", "period", "assigner")},
                        "display": assigner["display"]}
        return {"extension": x["extension"], "use": x["use"], "value": x["value"], "system": x["system"],
                "type": type_, "period": period, "assigner": assigner}

    return [element(x) for x in identifiers or []]


def _normalize_resource_identifier(identifiers, rid, source) -> list:
    """add_source_id_to_identifier_col: array_union with a fhirId element unless one exists."""
    conformed = _conform_identifier_column(identifiers)
    if any(x["type"] is not None and any(c["code"] == _FHIR_ID_CODE for c in x["type"]["coding"]) for x in conformed):
        return conformed
    fhir_id = {
        "extension": None, "use": None, "value": rid, "system": source,
        "type": {"extension": None, "id": None, "coding": [_fhir_id_coding()], "text": _FHIR_ID_TEXT},
        "period": {"extension": None, "id": None, "start": None, "end": None},
        "assigner": {"extension": None, "id": None, "reference": None, "type": None,
                     "identifier": dict.fromkeys(("extension", "use", "type", "value", "system", "period", "assigner")),
                     "display": None},
    }
    union = []
    for x in conformed + [fhir_id]:
        if x not in union:  # array_union keeps distinct elements, first occurrence first
            union.append(x)
    return union


def _normalize_date(value):
    """date_normalization.normalize_date: dates become midnight UTC; timestamps are unchanged."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    return value


# Configured normalizations per resource, in the HDS clinical flatten configuration order
# (flatten_config_generator output for R4), after the shared meta_lastUpdated and resource id rules.
_RULES = {
    "Patient": (
        ("references", "generalPractitioner"),
        ("reference", "managingOrganization"),
        ("date", "birthDate"),
        ("date", "deceasedDateTime"),
        ("date", "meta.lastUpdated"),
        ("nested", "identifier", "assigner"),
        ("nested", "contact", "organization"),
        ("nested", "link", "other"),
    ),
    "Observation": (
        ("references", "basedOn"),
        ("references", "partOf"),
        ("reference", "subject"),
        ("references", "focus"),
        ("reference", "encounter"),
        ("references", "performer"),
        ("reference", "specimen"),
        ("reference", "device"),
        ("references", "hasMember"),
        ("references", "derivedFrom"),
        ("date", "effectiveDateTime"),
        ("date", "effectiveInstant"),
        ("date", "issued"),
        ("date", "valueDateTime"),
        ("date", "meta.lastUpdated"),
        ("nested", "identifier", "assigner"),
        ("date", "effectivePeriod.start"),
        ("date", "effectivePeriod.end"),
        ("date", "valuePeriod.start"),
        ("date", "valuePeriod.end"),
        ("nested", "note", "authorReference"),
    ),
}


def _apply(rule: tuple, row: dict, source: str) -> None:
    kind, column = rule[0], rule[1]
    if kind == "reference":
        row[column] = _normalize_reference(row[column], source)
    elif kind == "references":
        if row[column]:
            row[column] = [_normalize_reference(r, source) for r in row[column]]
    elif kind == "nested":
        nested = rule[2]
        if row[column]:
            row[column] = [{**element, nested: _normalize_reference(element[nested], source)} for element in row[column]]
    elif "." in column:
        struct, field = column.split(".")
        if row[struct] is not None:
            value = row[struct][field]
            row[struct] = {**row[struct], field + "Orig": value, field: _normalize_date(value)}
    else:
        row[column + "Orig"] = row[column]
        row[column] = _normalize_date(row[column])


# ------------------------------------------------------------ Silver output


def _silver(value, node, path: str):
    """Check an internal value against its Silver type; the empty dict becomes an all-null struct."""
    if value is None:
        return None
    if isinstance(node, _Struct):
        if not isinstance(value, dict):
            raise AssertionError(f"{path}: expected a struct, got {value!r}")
        if value and set(value) != set(node.names):
            raise AssertionError(f"{path}: fields {sorted(value)} do not match {node.name} {list(node.names)}")
        return {name: _silver(value.get(name), t, f"{path}.{name}") for name, t in node.fields}
    if isinstance(node, _List):
        return [_silver(v, node.element, f"{path}[{i}]") for i, v in enumerate(value)]
    expected = {"string": str, "bool": bool, "int32": int, "decimal": Decimal, "timestamp": datetime, "date": date}[node]
    if not isinstance(value, expected) or (node == "date" and isinstance(value, datetime)) \
            or (node == "timestamp" and value.utcoffset() != timedelta(0)):
        raise AssertionError(f"{path}: {value!r} is not a Silver {node}")
    return value


def shape_row(resource: str, extracted: dict, *, source_system: str, logical_path: str,
              ingest_time: datetime) -> dict:
    """Shape one extraction-view row into a Silver row ordered like target_schema(resource).

    ingest_time stamps msftCreatedDatetime and msftModifiedDatetime; deciding what an update or a replay
    keeps is the writer's job. Raises ShapeError for input HDS would drop, null or fail on.
    """
    spec = _resource(resource)
    if not isinstance(source_system, str) or not source_system:
        raise ShapeError("source_system must be a non-empty string")
    if not isinstance(logical_path, str) or not logical_path:
        raise ShapeError("logical_path must be a non-empty string")
    if not isinstance(ingest_time, datetime) or ingest_time.utcoffset() is None:
        raise ShapeError("ingest_time must be a timezone-aware datetime")
    if not isinstance(extracted, dict) or set(extracted) != set(spec.extraction):
        given = set(extracted) if isinstance(extracted, dict) else set()
        raise ShapeError(f"extracted columns differ from the {resource} extraction view: "
                         f"missing {sorted(set(spec.extraction) - given)}, unexpected {sorted(given - set(spec.extraction))}")

    row = {column: _parse_column(spec, column, value) for column, value in extracted.items()}
    if row["id"] is None:
        raise ShapeError("id is required")

    # Shared rules: meta_lastUpdated (copied then date-normalized), then resource identifier and id.
    row["meta_lastUpdated"] = row["meta"]["lastUpdated"] if row["meta"] is not None else None
    _apply(("date", "meta_lastUpdated"), row, source_system)
    row["identifierOrig"] = row["identifier"]
    row["identifier"] = _normalize_resource_identifier(row["identifier"], row["id"], source_system)
    row["idOrig"] = row["id"]
    row["id"] = _resource_id(row["id"], resource, source_system)
    for rule in _RULES[resource]:
        _apply(rule, row, source_system)

    moment = ingest_time.astimezone(UTC)
    row.update(resourceType=resource, msftSourceSystem=source_system, msftFilePath=logical_path,
               msftModifiedDatetime=moment, msftCreatedDatetime=moment)

    shaped = {}
    for name, _ in spec.columns:
        if name.endswith("_string"):
            continue
        if name not in row:
            raise AssertionError(f"no rule produces Silver column {name}")
        shaped[name] = _silver(row[name], spec.silver(name), name)
    for name, _ in spec.columns:
        if name.endswith("_string"):
            base = name[: -len("_string")]
            shaped[name] = None if shaped[base] is None else _to_json(shaped[base], spec.silver(base))
    return {name: shaped[name] for name, _ in spec.columns}
