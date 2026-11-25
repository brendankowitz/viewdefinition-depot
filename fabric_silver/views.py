"""Generate the R4 SQL-on-FHIR projections from the HDS catalog's contracts."""

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACK = ROOT / "contracts" / "fabric-hds" / "1.4.0" / "r4"
CONTRACT_DIR = PACK / "resources"
VIEW_DIR = ROOT / "views" / "r4" / "fabric-silver-hds"


def make_view(resource: str, columns: list[dict]) -> dict:
    """Build a draft ViewDefinition with columns in the given order."""
    return {
        "resourceType": "ViewDefinition",
        "name": f"{resource.lower()}_hds_projection",
        "status": "draft",
        "resource": resource,
        "fhirVersion": ["4.0.1"],
        "select": [{"column": [_column(c) for c in columns]}],
    }


def _column(column: dict) -> dict:
    result = {"name": column["name"], "path": column["path"], "type": column["type"]}
    if column.get("collection"):
        result["collection"] = True
    return result


def load_contract(resource: str) -> dict:
    return json.loads((CONTRACT_DIR / f"{resource}.json").read_text(encoding="utf-8"))


def contract_view(contract: dict) -> dict:
    if contract["contractFormatVersion"] != "1":
        raise ValueError(f"{contract['resource']}: unsupported contract format")
    columns = contract["projection"]["columns"]
    names = [c["name"] for c in columns]
    if not names or len(names) != len(set(names)):
        raise ValueError(f"{contract['resource']}: empty projection or duplicate column name")
    return make_view(contract["resource"], columns)


def render(view: dict) -> str:
    return json.dumps(view, indent=2) + "\n"


def expected_files() -> dict[str, str]:
    catalog = json.loads((PACK / "catalog.json").read_text(encoding="utf-8"))
    entries = catalog["resources"]
    resources = {e["resource"] for e in entries}
    if (len(entries) != 146 or len(resources) != len(entries)
            or len({e["table"] for e in entries}) != len(entries)):
        raise ValueError("expected 146 unique concrete R4 resources and HDS tables")
    if not all(re.fullmatch("[A-Z][A-Za-z0-9]*", r) for r in resources):
        raise ValueError("invalid resource name in catalog")
    if {p.name for p in CONTRACT_DIR.glob("*.json")} != {f"{r}.json" for r in resources}:
        raise ValueError("resource contract files do not match the catalog")
    expected = {}
    for entry in entries:
        r = entry["resource"]
        contract = load_contract(r)
        filename = f"{r}.json"
        view = (VIEW_DIR / filename).relative_to(ROOT).as_posix()
        if (contract["resource"] != r or contract["table"] != entry["table"]
                or entry["contract"] != (CONTRACT_DIR / filename).relative_to(ROOT).as_posix()
                or contract["projection"]["view"] != view or entry["view"] != view):
            raise ValueError(f"{r}: catalog, contract and view identities differ")
        expected[filename] = render(contract_view(contract))
    return expected


def problems(directory: Path = VIEW_DIR) -> list[str]:
    expected = expected_files()
    found = {p.name for p in directory.iterdir()} if directory.is_dir() else set()
    result = [f"extra definition: {name}" for name in sorted(found - expected.keys())]
    for name, text in expected.items():
        path = directory / name
        if name not in found:
            result.append(f"missing definition: {name}")
        elif not path.is_file() or path.read_text(encoding="utf-8") != text:
            result.append(f"stale definition: {name}")
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="fail on stale, missing or extra definitions")
    mode.add_argument("--write", action="store_true", help="regenerate the definitions")
    args = parser.parse_args(argv)
    try:
        if args.write:
            expected = expected_files()
            VIEW_DIR.mkdir(parents=True, exist_ok=True)
            for name, text in expected.items():
                (VIEW_DIR / name).write_text(text, encoding="utf-8", newline="\n")
        found = problems()
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f"views check failed: {error}\n")
    for line in found:
        print(f"views check failed: {line}", file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main())
