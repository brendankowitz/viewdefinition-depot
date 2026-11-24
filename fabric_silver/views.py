"""Generate and check the parity extraction ViewDefinitions from their contracts.

The contract is the single source of truth: its ``extraction`` list drives the
view, and its ``silverColumns`` list says how each deployed Silver column is
sourced. This module only writes definitions; running them is the CLI's job.
"""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "contracts" / "fabric-hds" / "1.4.0" / "parity"
VIEW_DIR = ROOT / "views" / "r4" / "fabric-silver-parity"
RESOURCES = ("Patient", "Observation")


def make_view(resource: str, columns: list[dict]) -> dict:
    """Build a draft ViewDefinition with columns in the given order."""
    return {
        "resourceType": "ViewDefinition",
        "name": f"{resource.lower()}_silver_parity_extraction",
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
    columns = [{**c, "name": c["column"]} for c in contract["extraction"]]
    return make_view(contract["resource"], columns)


def render(view: dict) -> str:
    return json.dumps(view, indent=2) + "\n"


def expected_files() -> dict[str, str]:
    return {f"{r}.json": render(contract_view(load_contract(r))) for r in RESOURCES}


def problems(directory: Path = VIEW_DIR) -> list[str]:
    expected = expected_files()
    found = {p.name for p in directory.iterdir()} if directory.is_dir() else set()
    result = [f"extra definition: {name}" for name in sorted(found - expected.keys())]
    for name, text in expected.items():
        path = directory / name
        if name not in found:
            result.append(f"missing definition: {name}")
        elif path.read_text(encoding="utf-8") != text:
            result.append(f"stale definition: {name}")
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="fail on stale, missing or extra definitions")
    mode.add_argument("--write", action="store_true", help="regenerate the definitions")
    args = parser.parse_args(argv)
    if args.write:
        VIEW_DIR.mkdir(parents=True, exist_ok=True)
        for name, text in expected_files().items():
            (VIEW_DIR / name).write_text(text, encoding="utf-8", newline="\n")
        return 0
    found = problems()
    for line in found:
        print(f"views check failed: {line}", file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main())
