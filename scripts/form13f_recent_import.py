from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from market_data.filings import DEFAULT_13F_THRESHOLD, Form13FImporter  # type: ignore[import-not-found]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import recent Form 13F cover pages and holdings into MySQL."
    )
    parser.add_argument(
        "--mode",
        choices=("deadline", "month-end", "both"),
        default="both",
        help="deadline imports recent reporting periods; month-end imports a filedAt window.",
    )
    parser.add_argument(
        "--as-of",
        type=parse_date,
        default=dt.date.today(),
        help="Reference date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=DEFAULT_13F_THRESHOLD,
        help="Minimum tableValueTotal to import. Defaults to 1,000,000,000.",
    )
    parser.add_argument(
        "--quarters-back",
        type=int,
        default=2,
        help="Number of recent completed reporting periods to import for deadline mode.",
    )
    parser.add_argument(
        "--filed-lookback-days",
        type=int,
        default=60,
        help="Days to look back from --as-of for month-end filedAt imports.",
    )
    parser.add_argument(
        "--min-request-interval",
        type=float,
        default=30.0,
        help="Minimum seconds between sec-api requests.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Database insert batch size.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars.",
    )
    return parser.parse_args()


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def run(args: argparse.Namespace) -> dict[str, Any]:
    importer = Form13FImporter(
        batch_size=args.batch_size,
        min_request_interval=args.min_request_interval,
        show_progress=not args.no_progress,
    )

    results: dict[str, Any] = {
        "mode": args.mode,
        "as_of": args.as_of.isoformat(),
        "threshold": args.threshold,
    }

    if args.mode in {"deadline", "both"}:
        results["deadline"] = importer.import_recent_periods(
            as_of=args.as_of,
            quarters_back=args.quarters_back,
            threshold=args.threshold,
        )

    if args.mode in {"month-end", "both"}:
        filed_since = args.as_of - dt.timedelta(days=args.filed_lookback_days)
        results["month_end"] = importer.import_filed_since(
            filed_since=filed_since,
            filed_until=args.as_of,
            threshold=args.threshold,
        )
        results["month_end"]["filed_since"] = filed_since.isoformat()
        results["month_end"]["filed_until"] = args.as_of.isoformat()

    return results


def main() -> int:
    args = parse_args()
    results = run(args)
    print(json.dumps(results, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
