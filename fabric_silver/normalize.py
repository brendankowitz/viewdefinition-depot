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
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_EVEN, ROUND_HALF_UP, Context, Decimal, localcontext
from functools import lru_cache

import pyarrow as pa

from fabric_silver.views import CONTRACT_DIR, RESOURCES, load_contract


class ShapeError(ValueError):
    """The extracted row, or a shaping argument, cannot become a Silver row.

    Carries only the resource, the Silver mapping path (and its FHIRPath when the path starts at an
    extraction column) and a fixed reason, never source values, so logging it discloses no PHI. It is
    raised with no cause or context for the same reason.
    """

    def __init__(self, resource: str, path: str, reason: str, fhir_path: str | None = None):
        where = f"{path} (FHIRPath {fhir_path})" if fhir_path else path
        super().__init__(f"{resource} {where}: {reason}")
        self.resource, self.path, self.fhir_path, self.reason = resource, path, fhir_path, reason


class _Invalid(Exception):
    """Internal failure at a mapping path; shape_row turns it into a ShapeError naming the resource."""

    def __init__(self, path: str, reason: str):
        super().__init__(path, reason)
        self.path, self.reason = path, reason


UTC = timezone.utc
_SURROGATE = re.compile("[\ud800-\udfff]")
_MAX_DIGITS = 36
_INT32 = (-(2 ** 31), 2 ** 31 - 1)

# Copy these templates before use: decimal flags are sticky, even with traps disabled.
_DECIMAL_TEXT = Context(prec=28, rounding=ROUND_HALF_EVEN, Emin=-999999, Emax=999999,
                        capitals=1, clamp=0, traps=[], flags=[])
_DECIMAL_SCALE = Context(prec=_MAX_DIGITS + 1, rounding=ROUND_HALF_UP, Emin=-999999, Emax=999999,
                         capitals=1, clamp=0, traps=[], flags=[])
_SCALE = Decimal("1E-18", context=_DECIMAL_SCALE.copy())

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
    paths: dict  # extraction column -> FHIRPath it was extracted from

    def silver(self, column: str):
        return _resolve(dict(self.columns)[column], False)

    def raw(self, column: str):
        """from_json type of an extraction column: its *Orig column keeps the pre-normalization type."""
        types = dict(self.columns)
        return _resolve(types.get(column + "Orig", types[column]), True)

    def fhir_path(self, path: str) -> str | None:
        """The FHIRPath of a mapping path rooted at an extraction column (e.g. valueQuantity.value)."""
        root = re.match(r"[^.\[]+", path).group(0)
        return self.paths[root] + path[len(root):] if root in self.paths else None


@lru_cache(maxsize=None)
def _resource(resource: str) -> _Resource:
    if resource not in RESOURCES:
        raise ShapeError(str(resource), "resourceType", "unsupported resource")
    contract = load_contract(resource)
    return _Resource(
        resource,
        tuple((c["name"], c["silverType"]) for c in contract["silverColumns"]),
        {c["column"]: c["type"] for c in contract["extraction"]},
        {c["column"]: c["path"] for c in contract["extraction"]},
    )


def target_schema(resource: str) -> pa.Schema:
    """The deployed HDS v1.4.0 Silver schema for the resource (every field nullable)."""
    spec = _resource(resource)
    return pa.schema([pa.field(name, _arrow(spec.silver(name)), nullable=True) for name, _ in spec.columns])


# ------------------------------------------------------------ JSON emulation


_SHORT_ESCAPES = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\t": "\\t", "\n": "\\n", "\f": "\\f", "\r": "\\r"}
_NEEDS_ESCAPE = re.compile('["\\\\\x00-\x1f]')


def _quote(text: str) -> str:
    """Jackson's default string escaping: quote, backslash and C0 controls only."""
    return '"' + _NEEDS_ESCAPE.sub(lambda m: _SHORT_ESCAPES.get(m.group(), f"\\u{ord(m.group()):04X}"), text) + '"'


def _java_double(value: Decimal, path: str) -> str:
    """Java Double.toString of the double nearest the JSON number (what Jackson copies)."""
    with localcontext(_DECIMAL_TEXT) as ctx:
        if not value.is_finite():
            raise _Invalid(path, "is out of double range")
        number = float(value)
        if not math.isfinite(number):
            raise _Invalid(path, "is out of double range")
        if number == 0:
            return "-0.0" if str(number).startswith("-") else "0.0"
        shortest = ctx.normalize(Decimal(repr(abs(number)), context=ctx))
        sign = "-" if number < 0 else ""
        if -3 <= shortest.adjusted() < 7:
            text = format(shortest, "f")
            return sign + (text if "." in text else text + ".0")
        digits = "".join(map(str, shortest.as_tuple().digits))
        return f"{sign}{digits[0]}.{digits[1:] or '0'}E{shortest.adjusted()}"


def _jackson(value, path: str) -> str:
    """Compact JSON text Spark keeps when a non-string JSON value lands in a StringType field.

    path names the string field; errors stop there because keys below it are free-form source text.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _java_double(value, path)
    if isinstance(value, _HugeExponent):
        if value.double is None:
            raise _Invalid(path, "is out of double range")
        return value.double
    if isinstance(value, str):
        return _quote(_text(value, path))
    if isinstance(value, list):
        return "[" + ",".join(_jackson(v, path) for v in value) + "]"
    return "{" + ",".join(_quote(_text(k, path)) + ":" + _jackson(v, path) for k, v in value.items()) + "}"


def _text(value: str, path: str) -> str:
    """Silver strings are UTF-8 (hashed, written to Parquet); a lone surrogate (e.g. JSON "\\ud800") cannot be."""
    if _SURROGATE.search(value):
        raise _Invalid(path, "is not encodable as UTF-8")
    return value


def _check_texts(value, path: str) -> None:
    """_text for every key and string in a JSON value parsed by HDS's own json.loads (iterative: no depth limit)."""
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            _text(item, path)
        elif isinstance(item, list):
            pending.extend(item)
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())


def _reject_constant(name: str):
    raise ValueError("non-finite numbers are not JSON")  # never surfaces: callers map it to a fixed reason


@dataclass(frozen=True)
class _HugeExponent:
    """A JSON number whose exponent is beyond Decimal's range (and Java BigDecimal's int scale).

    Kept as a value so the failure is reported at its leaf: a decimal leaf rejects it, a string leaf gets the
    Java double Jackson would copy (signed zero), or None when that double is infinite.
    """
    double: str | None


def _json_number(token: str):
    """parse_float for extraction JSON: the exact Decimal, or _HugeExponent when Decimal cannot hold the exponent."""
    # Unlike Context.create_decimal, this constructor doesn't round to context precision.
    value = Decimal(token, context=_DECIMAL_TEXT.copy())
    if value.is_finite():
        return value
    # JSON's number grammar leaves an unrepresentable exponent as the only failure.
    mantissa, _, exponent = token.lower().partition("e")
    if exponent.startswith("-") or not mantissa.strip("-0."):
        return _HugeExponent("-0.0" if token.startswith("-") else "0.0")
    return _HugeExponent(None)


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
    if node == "int32":
        return str(value)
    if node == "decimal":
        return _DECIMAL_TEXT.copy().to_sci_string(value)  # BigDecimal.toString at scale 18
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
        raise _Invalid(path, "is not a FHIR date, dateTime or instant")
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
        return local.astimezone(UTC)
    except (ValueError, OverflowError):
        pass
    raise _Invalid(path, "is not a valid date or time")


def _date(text: str, path: str) -> date:
    match = _DATE.fullmatch(text)
    if not match:
        raise _Invalid(path, "is not a FHIR date")
    year, month, day = match.groups()
    try:
        return date(int(year), int(month or 1), int(day or 1))
    except ValueError:
        pass
    raise _Invalid(path, "is not a valid date or time")


def _decimal(value, path: str) -> Decimal:
    """Round HALF_UP to scale 18, then require at most 36 digits (Spark Decimal.changePrecision)."""
    if isinstance(value, _HugeExponent):  # Java BigDecimal cannot read it either: Spark nulls the record
        raise _Invalid(path, "does not fit decimal(36,18)")
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise _Invalid(path, "expected a JSON number")
    # One digit beyond the target lets a rounding carry (e.g. 18 nines .5e-18 -> 10^18) show as 37 digits.
    ctx = _DECIMAL_SCALE.copy()
    exact = Decimal(value, context=ctx) if isinstance(value, int) else value
    if not exact.is_finite():
        raise _Invalid(path, "does not fit decimal(36,18)")
    scaled = ctx.quantize(exact, _SCALE)
    if not scaled.is_finite() or len(scaled.as_tuple().digits) > _MAX_DIGITS:
        raise _Invalid(path, "does not fit decimal(36,18)")
    return scaled


def _parse(value, node, path: str):
    """from_json of one JSON value against a raw type, rejecting what Spark would drop or null."""
    if value is None:
        return None
    if isinstance(node, _Struct):
        if not isinstance(value, dict):
            raise _Invalid(path, "expected a JSON object")
        if not set(value) <= set(node.names):
            raise _Invalid(path, "has a field that is not in the HDS Silver schema")  # key names are source data
        return {name: _parse(value.get(name), t, f"{path}.{name}") for name, t in node.fields}
    if isinstance(node, _List):
        if not isinstance(value, list):
            raise _Invalid(path, "expected a JSON array")
        return [_parse_element(v, node.element, f"{path}[{i}]") for i, v in enumerate(value)]
    if node == "string":
        if isinstance(value, str):
            return _text(value, path)
        try:
            return _jackson(value, path)
        except RecursionError:
            pass
        raise _Invalid(path, "is nested too deeply")
    if node == "bool":
        if not isinstance(value, bool):
            raise _Invalid(path, "expected a boolean")
        return value
    if node == "int32":
        if isinstance(value, bool) or not isinstance(value, int) or not _INT32[0] <= value <= _INT32[1]:
            raise _Invalid(path, "expected a 32-bit integer")
        return value
    if node == "decimal":
        return _decimal(value, path)
    if not isinstance(value, str):
        raise _Invalid(path, "expected a date/time string")
    return _timestamp(value, path) if node == "timestamp" else _date(value, path)


def _parse_element(value, node, path: str):
    if value is None:
        raise _Invalid(path, "null array elements are not FHIR")
    return _parse(value, node, path)


_SCALAR_REASONS = {"string": "expected a string", "boolean": "expected a boolean", "integer": "expected a 32-bit integer"}
_SCALAR_TYPES = {"string": str, "boolean": bool, "integer": int}


def _parse_column(spec: _Resource, column: str, value):
    node = spec.raw(column)
    kind = spec.extraction[column]
    if column in _collection_columns(spec.name):
        if not isinstance(value, str):
            raise _Invalid(column, "expected JSON array text")
        try:  # NaN/Infinity are not JSON (FHIR forbids them); rejecting keeps floats out of the parsed tree
            items = json.loads(value, parse_float=_json_number, parse_constant=_reject_constant)
        except ValueError:  # JSONDecodeError, a non-finite constant, or an integer beyond Python's digit limit
            items = _NOT_JSON
        except RecursionError:
            items = _TOO_DEEP
        if items is _NOT_JSON:
            raise _Invalid(column, "is not valid JSON")
        if items is _TOO_DEEP:
            raise _Invalid(column, "is nested too deeply")
        if not isinstance(items, list):
            raise _Invalid(column, "expected a JSON array")
        if not items:
            return None
        if isinstance(node, _Struct):
            if len(items) != 1:
                raise _Invalid(column, "expected at most one value")
            return _parse_element(items[0], node, column)
        return _parse(items, node, column)
    if value is not None and (isinstance(value, bool) != (kind == "boolean")
                              or not isinstance(value, _SCALAR_TYPES[kind])):
        raise _Invalid(column, _SCALAR_REASONS[kind])
    return _parse(value, node, column)


_NOT_JSON = object()
_TOO_DEEP = object()


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


def _row_from_string(text: str, names, path: str) -> dict:
    """NormalizationUtils.create_row_from_string; HDS fails the job where this raises."""
    try:
        data = json.loads(text) if text else {}
    except ValueError:
        data = None
    except RecursionError:
        data = _TOO_DEEP
    if data is _TOO_DEEP:
        raise _Invalid(path, "is nested too deeply")
    if not isinstance(data, dict):
        raise _Invalid(path, "is not JSON object text")
    _check_texts(data, path)  # JSON escapes can decode to lone surrogates the source string did not contain
    return {name: data.get(name) for name in names}


def _parse_assigner(value, path: str):
    """NormalizationUtils.parse_assigner_json_safely (it catches decode errors only; deeper failures fail HDS)."""
    if not (value and isinstance(value, str)):
        return value
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError, ValueError):
        return value
    except RecursionError:
        parsed = _TOO_DEEP
    if parsed is _TOO_DEEP:
        raise _Invalid(path, "is nested too deeply")
    if not isinstance(parsed, dict):
        return value
    _check_texts(parsed, path)
    nested = parsed.get("identifier")
    if nested and isinstance(nested, dict):
        nested = {name: nested.get(name) for name in ("extension", "use", "value", "system", "type", "period", "assigner")}
    return {"extension": parsed.get("extension"), "id": parsed.get("id"), "reference": parsed.get("reference"),
            "type": parsed.get("type"), "identifier": nested, "display": parsed.get("display")}


def _conform_identifier_row(row, path: str) -> dict:
    """IdentifierNormalization.conform_to_row_identifier_schema.

    Its str() casts recurse through whatever JSON HDS reparsed; too deep a value fails HDS and is rejected here.
    """
    try:
        return _conform_identifier_members(row, path)
    except RecursionError:
        pass
    raise _Invalid(path, "is nested too deeply")


def _period_time(period, name: str, path: str):
    """A Period member cast with TimestampType().fromInternal; overflow is not caught by get_row_field."""
    try:
        return _field(period, name, cast=_from_internal)
    except OverflowError:
        pass
    raise _Invalid(f"{path}.period.{name}", "is out of timestamp range")


def _conform_identifier_members(row, path: str) -> dict:
    type_ = _field(row, "type")
    if isinstance(type_, str):
        type_ = _row_from_string(type_, _CODEABLE_CONCEPT_FIELDS, f"{path}.type")
    if type_:
        try:  # HDS iterates coding: text and objects iterate (characters, keys); a number or boolean fails the job
            codings = list(_field(type_, "coding", []))
        except TypeError:
            codings = None
        if codings is None:
            raise _Invalid(f"{path}.type.coding", "expected a JSON array")
        coding = [{"extension": _field(c, "extension", cast=str), "id": _field(c, "id", cast=str),
                   "system": _field(c, "system", cast=str), "version": _field(c, "version", cast=str),
                   "code": _field(c, "code", cast=str), "display": _field(c, "display", cast=str),
                   "userSelected": _field(c, "userSelected", cast=bool)}
                  for c in codings]
        type_ = {"extension": _field(type_, "extension", cast=str), "id": _field(type_, "id", cast=str),
                 "coding": coding, "text": _field(type_, "text", cast=str)}
    else:
        type_ = {}
    period = _field(row, "period")
    if period:
        period = {"extension": _field(period, "extension", cast=str), "id": _field(period, "id", cast=str),
                  "start": _period_time(period, "start", path), "end": _period_time(period, "end", path)}
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


def _add_source_id(identifier, value, system, path: str) -> dict:
    """IdentifierNormalization.add_source_id_to_identifier_row."""
    identifier = _conform_identifier_row(identifier, path)
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


def _normalize_reference(row, source: str, path: str) -> dict:
    """ReferenceNormalization.normalize_ref_struct (no dereferencing; only the reference text is parsed)."""
    reference = type_ = display = extension = id_ = id_orig = original = None
    identifier = {"extension": None, "use": None, "value": None, "system": None, "type": None, "period": None,
                  "assigner": None}
    identifier_path = f"{path}.identifier"
    if isinstance(row, dict) and row:
        reference, type_ = row.get("reference"), row.get("type")
        display, extension = row.get("display"), row.get("extension")
        original = reference
        previous = row.get("identifier", {})
        if previous:
            if isinstance(previous, dict):
                identifier = {name: previous.get(name) for name in ("extension", "use", "value", "system", "type", "period")}
                identifier["assigner"] = _parse_assigner(previous.get("assigner"), f"{identifier_path}.assigner")
            elif isinstance(previous, str):
                identifier = _row_from_string(previous, tuple(identifier), identifier_path)
        identifier = _conform_identifier_row(identifier, identifier_path)
        if reference:
            kind, base, parsed_type, rid, system, value = _reference_details(reference)
            type_ = type_ or parsed_type
            if kind == "Literal" and (not base or _without_scheme(base) == _without_scheme(source)):
                id_ = _resource_id(rid, type_, source)
                id_orig = rid
                identifier = _add_source_id(identifier, rid, source, identifier_path)
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
        row[column] = _normalize_reference(row[column], source, column)
    elif kind == "references":
        if row[column]:
            row[column] = [_normalize_reference(r, source, f"{column}[{i}]") for i, r in enumerate(row[column])]
    elif kind == "nested":
        nested = rule[2]
        if row[column]:
            row[column] = [{**element, nested: _normalize_reference(element[nested], source, f"{column}[{i}].{nested}")}
                           for i, element in enumerate(row[column])]
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
    """Check an internal value against its Silver type; the empty dict becomes an all-null struct.

    A mismatch here is a bug in this module, not bad input; messages name the path only.
    """
    if value is None:
        return None
    if isinstance(node, _Struct):
        if not isinstance(value, dict):
            raise AssertionError(f"{path}: expected a struct")
        if value and set(value) != set(node.names):
            raise AssertionError(f"{path}: fields do not match {node.name}")
        return {name: _silver(value.get(name), t, f"{path}.{name}") for name, t in node.fields}
    if isinstance(node, _List):
        return [_silver(v, node.element, f"{path}[{i}]") for i, v in enumerate(value)]
    expected = {"string": str, "bool": bool, "int32": int, "decimal": Decimal, "timestamp": datetime, "date": date}[node]
    if not isinstance(value, expected) or (node == "date" and isinstance(value, datetime)) \
            or (node == "timestamp" and value.utcoffset() != timedelta(0)):
        raise AssertionError(f"{path}: value is not a Silver {node}")
    return value


def shape_row(resource: str, extracted: dict, *, source_system: str, logical_path: str,
              ingest_time: datetime) -> dict:
    """Shape one extraction-view row into a Silver row ordered like target_schema(resource).

    ingest_time stamps msftCreatedDatetime and msftModifiedDatetime; deciding what an update or a replay
    keeps is the writer's job. Raises ShapeError for input HDS would drop, null or fail on.
    """
    spec = _resource(resource)
    if not isinstance(source_system, str) or not source_system:
        raise ShapeError(resource, "msftSourceSystem", "source_system must be a non-empty string")
    if not isinstance(logical_path, str) or not logical_path:
        raise ShapeError(resource, "msftFilePath", "logical_path must be a non-empty string")
    for path, text in (("msftSourceSystem", source_system), ("msftFilePath", logical_path)):
        if _SURROGATE.search(text):
            raise ShapeError(resource, path, "is not encodable as UTF-8")
    if not isinstance(ingest_time, datetime) or ingest_time.utcoffset() is None:
        raise ShapeError(resource, "msftModifiedDatetime", "ingest_time must be a timezone-aware datetime")
    try:
        ingest_time = ingest_time.astimezone(UTC)
    except OverflowError:  # e.g. 0001-01-01 at a positive offset
        ingest_time = None
    if ingest_time is None:
        raise ShapeError(resource, "msftModifiedDatetime", "is out of timestamp range")
    if not isinstance(extracted, dict) or set(extracted) != set(spec.extraction):
        given = set(extracted) if isinstance(extracted, dict) else set()
        # Unexpected names are counted, not listed: they come from the caller and may carry source text.
        raise ShapeError(resource, "(row)", f"extracted columns differ from the extraction view: missing "
                                            f"{sorted(set(spec.extraction) - given)}, {len(given - set(spec.extraction))} unexpected")
    try:
        return _shape(spec, extracted, source_system, logical_path, ingest_time)
    except _Invalid as invalid:
        failure = invalid
    # Raised outside the handler so neither the internal error nor its cause (e.g. a JSONDecodeError
    # holding the whole document) is chained onto the ShapeError.
    raise ShapeError(resource, failure.path, failure.reason, spec.fhir_path(failure.path)) from None


def _shape(spec: _Resource, extracted: dict, source_system: str, logical_path: str, ingest_time: datetime) -> dict:
    resource = spec.name
    row = {column: _parse_column(spec, column, value) for column, value in extracted.items()}
    if row["id"] is None:
        raise _Invalid("id", "is required")

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
