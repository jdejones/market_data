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

from market_data.filings import Form4Importer  # type: ignore[import-not-found]


DEFAULT_DAILY_LOOKBACK_DAYS = 7


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Form 4 insider purchase/sale filings into MySQL."
    )
    parser.add_argument(
        "--watchlist-location",
        default=None,
        help="Path to a symbol watchlist. Defaults to watchlist_locations.hadv.",
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=None,
        help="Start date in YYYY-MM-DD format. Overrides --lookback-days when provided.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=None,
        help="End date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_DAILY_LOOKBACK_DAYS,
        help=f"Trailing days to import when --start-date is omitted. Defaults to {DEFAULT_DAILY_LOOKBACK_DAYS}.",
    )
    parser.add_argument(
        "--date-field",
        choices=("filedAt", "periodOfReport", "nonDerivativeTable.transactions.transactionDate"),
        default="filedAt",
        help="sec-api date field to range-filter. Defaults to filedAt for daily maintenance.",
    )
    parser.add_argument(
        "--transaction-codes",
        default="P,S",
        help="Comma-separated transaction codes to import. Defaults to P,S.",
    )
    parser.add_argument(
        "--document-types",
        default="4,4/A",
        help="Comma-separated document types to import. Defaults to 4,4/A.",
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
        help="Number of symbols per sec-api query before date-window splitting.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars.",
    )
    return parser.parse_args()


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def run(args: argparse.Namespace) -> dict[str, Any]:
    importer = Form4Importer(
        batch_size=args.batch_size,
        symbol_batch_size=args.symbol_batch_size,
        min_request_interval=args.min_request_interval,
        show_progress=not args.no_progress,
    )
    end_date = args.end_date or dt.date.today()
    start_date = args.start_date

    results = importer.import_watchlist(
        watchlist_location=args.watchlist_location,
        start_date=start_date,
        end_date=end_date,
        lookback_days=args.lookback_days,
        date_field=args.date_field,
        transaction_codes=parse_csv(args.transaction_codes),
        document_types=parse_csv(args.document_types),
    )
    results.update(
        {
            "watchlist_location": args.watchlist_location or "watchlist_locations.hadv",
            "start_date": (start_date or (end_date - dt.timedelta(days=args.lookback_days))).isoformat(),
            "end_date": end_date.isoformat(),
            "date_field": args.date_field,
            "transaction_codes": parse_csv(args.transaction_codes),
            "document_types": parse_csv(args.document_types),
            "min_request_interval": args.min_request_interval,
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
