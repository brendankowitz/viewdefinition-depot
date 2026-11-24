"""Local CLI for applying one parity-pilot batch."""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

from fabric_silver.normalize import ShapeError
from fabric_silver.writer import BatchError, apply_batch


def _instant(text: str) -> datetime:
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
                    r"(?:\.[0-9]{1,6})?(?:Z|\+00:00)", text):
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass
    raise argparse.ArgumentTypeError("expected an ISO-8601 UTC instant ending in Z or +00:00")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--event", required=True)
    parser.add_argument("--extracted", required=True, type=Path)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--ingest-time", required=True, type=_instant)
    args = parser.parse_args(argv)
    try:
        destination = apply_batch(args.manifest, args.event, args.extracted, args.state,
                                  ingest_time=args.ingest_time)
    except (BatchError, ShapeError) as exc:
        print(exc, file=sys.stderr)
        return 1
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
