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

from market_data.filings import ExecutiveCompensationImporter  # type: ignore[import-not-found]


DEFAULT_YEARS_BACK = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import recent executive compensation records into MySQL."
    )
    parser.add_argument(
        "--watchlist-location",
        default=None,
        help="Path to a symbol watchlist. Defaults to watchlist_locations.hadv.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Start reporting year. Overrides --years-back when provided.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="End reporting year. Defaults to the current calendar year.",
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=DEFAULT_YEARS_BACK,
        help=(
            "Recent reporting years to refresh when --start-year is omitted. "
            f"Defaults to {DEFAULT_YEARS_BACK}, meaning current and prior year."
        ),
    )
    parser.add_argument(
        "--min-request-interval",
        type=float,
        default=2.0,
        help="Minimum seconds between sec-api requests.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Database insert batch size.",
    )
    parser.add_argument(
        "--symbol-batch-size",
        type=int,
        default=25,
        help="Number of symbols per sec-api query before recursive splitting.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars.",
    )
    return parser.parse_args()


def run(args: argparse.Namespace) -> dict[str, Any]:
    importer = ExecutiveCompensationImporter(
        batch_size=args.batch_size,
        symbol_batch_size=args.symbol_batch_size,
        min_request_interval=args.min_request_interval,
        show_progress=not args.no_progress,
    )

    end_year = args.end_year or dt.date.today().year
    if args.start_year is not None or args.end_year is not None:
        start_year = args.start_year if args.start_year is not None else end_year - args.years_back
        results = importer.import_watchlist(
            watchlist_location=args.watchlist_location,
            start_year=start_year,
            end_year=end_year,
        )
    else:
        results = importer.import_recent_years(
            watchlist_location=args.watchlist_location,
            years_back=args.years_back,
        )
        start_year = dt.date.today().year - args.years_back

    results.update(
        {
            "watchlist_location": args.watchlist_location or "watchlist_locations.hadv",
            "start_year": start_year,
            "end_year": end_year,
            "years_back": args.years_back,
            "min_request_interval": args.min_request_interval,
            "symbol_batch_size": args.symbol_batch_size,
        }
    )
    return results


def main() -> int:
    args = parse_args()
    results = run(args)
    print(json.dumps(results, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
