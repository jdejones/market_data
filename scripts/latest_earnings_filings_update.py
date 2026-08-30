#! This script will be scheduled to run Mon-Fri at 7:05p.m. It's possible there are weekend filings though theses would be 
#! extremely rare. If it appears as a problem check the run rechedule in task scheduler.
"""Update stocks.latest_earnings_filings from the SEC filing stream JSONL log."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sec_api import RenderApi
from sqlalchemy import URL, bindparam, create_engine, text
from sqlalchemy.engine import Engine


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from market_data.api_keys import database_password, sec_api_key  # type: ignore[import-not-found]


LOGGER = logging.getLogger(__name__)

DEFAULT_JSONL_PATH = Path(r"C:\Users\jdejo\News_Tracker\filings_stream_gui_log.jsonl")
DEFAULT_MAX_WORKERS = 8
READ_BATCH_SIZE = 500

FORM_COLUMNS = {
    "10-K": {"filing": "form10k", "link": "form10k_link"},
    "10-Q": {"filing": "form10q", "link": "form10q_link"},
}


@dataclass(frozen=True)
class FilingEvent:
    symbol: str
    form_type: str
    filed_at: datetime
    link: str


def parse_mysql_datetime(value: Any) -> datetime:
    """Convert a stream timestamp to a naive UTC datetime for MySQL."""
    if not value:
        raise ValueError("filed_at is missing")

    parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def load_latest_filing_events(jsonl_path: Path) -> tuple[list[FilingEvent], int]:
    """
    Read filing events and retain the newest event for each symbol/form pair.

    The live stream also writes status-log records. Those records and filing
    types other than exact 10-K and 10-Q forms are intentionally ignored.
    A malformed final line can occur while the live process is writing and is
    skipped so it can be read on the next updater run.
    """
    newest: dict[tuple[str, str], FilingEvent] = {}
    malformed_lines = 0

    with jsonl_path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                malformed_lines += 1
                LOGGER.warning("Skipping malformed JSONL line %d", line_number)
                continue

            if record.get("kind") != "filing":
                continue

            data = record.get("data")
            if not isinstance(data, dict):
                continue

            form_type = str(data.get("form_type") or "").strip().upper()
            if form_type not in FORM_COLUMNS:
                continue

            symbol = str(data.get("ticker") or data.get("symbol") or "").strip().upper()
            link = str(data.get("link") or "").strip()
            if not symbol or not link:
                LOGGER.warning(
                    "Skipping filing on line %d because symbol or link is missing",
                    line_number,
                )
                continue

            try:
                event = FilingEvent(
                    symbol=symbol,
                    form_type=form_type,
                    filed_at=parse_mysql_datetime(data.get("filed_at")),
                    link=link,
                )
            except (TypeError, ValueError) as exc:
                LOGGER.warning("Skipping filing on line %d: %s", line_number, exc)
                continue

            key = (event.symbol, event.form_type)
            previous = newest.get(key)
            if previous is None or event.filed_at >= previous.filed_at:
                newest[key] = event

    return list(newest.values()), malformed_lines


def make_stocks_engine() -> Engine:
    url = URL.create(
        drivername="mysql+pymysql",
        username="root",
        password=database_password,
        host="127.0.0.1",
        port=3306,
        database="stocks",
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10},
    )


def batched(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def load_existing_rows(
    engine: Engine,
    symbols: Iterable[str],
) -> dict[str, dict[str, Any]]:
    """Load current dates and links for candidate symbols."""
    normalized_symbols = sorted(set(symbols))
    if not normalized_symbols:
        return {}

    statement = text(
        """
        SELECT symbol, filed_at, form10k_link, form10q_link
        FROM latest_earnings_filings
        WHERE symbol IN :symbols
        """
    ).bindparams(bindparam("symbols", expanding=True))

    existing: dict[str, dict[str, Any]] = {}
    with engine.connect() as connection:
        for symbol_batch in batched(normalized_symbols, READ_BATCH_SIZE):
            rows = connection.execute(
                statement,
                {"symbols": symbol_batch},
            ).mappings()
            for row in rows:
                symbol = str(row["symbol"]).upper()
                current = existing.get(symbol)
                if current is None or (
                    row["filed_at"] is not None
                    and (
                        current["filed_at"] is None
                        or row["filed_at"] > current["filed_at"]
                    )
                ):
                    existing[symbol] = dict(row)
    return existing


def select_new_events(
    events: Iterable[FilingEvent],
    existing: dict[str, dict[str, Any]],
) -> tuple[list[FilingEvent], int]:
    """Exclude events whose link is stored already or whose filing is stale."""
    selected: list[FilingEvent] = []
    skipped = 0

    for event in events:
        current = existing.get(event.symbol)
        if current is None:
            selected.append(event)
            continue

        link_column = FORM_COLUMNS[event.form_type]["link"]
        current_link = current.get(link_column)
        current_filed_at = current.get("filed_at")

        if current_link == event.link:
            skipped += 1
            continue

        # filed_at represents the newest filing stored on the symbol row. If
        # this form already has content, do not replace it with an older event.
        if (
            current_link
            and current_filed_at is not None
            and event.filed_at <= current_filed_at
        ):
            skipped += 1
            continue

        selected.append(event)

    return selected, skipped


def download_filing(event: FilingEvent) -> dict[str, Any]:
    """Download the complete primary filing HTML from its SEC archive link."""
    filing_html = RenderApi(api_key=sec_api_key).get_filing(event.link)
    if not filing_html:
        raise RuntimeError("Downloaded filing was empty")

    return {
        "symbol": event.symbol,
        "form_type": event.form_type,
        "filed_at": event.filed_at,
        "link": event.link,
        "filing": filing_html,
    }


def download_filings(
    events: Iterable[FilingEvent],
    max_workers: int,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    downloads: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    event_list = list(events)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_filing, event): event for event in event_list
        }
        for future in as_completed(futures):
            event = futures[future]
            try:
                downloads.append(future.result())
            except Exception as exc:
                errors.append(
                    {
                        "symbol": event.symbol,
                        "form_type": event.form_type,
                        "error": str(exc).replace(sec_api_key, "[REDACTED]"),
                    }
                )

    return downloads, errors


def combine_symbol_downloads(
    downloads: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Combine 10-K and 10-Q downloads so a new symbol is inserted once."""
    combined: dict[str, dict[str, Any]] = {}

    for download in downloads:
        symbol = download["symbol"]
        row = combined.setdefault(
            symbol,
            {
                "symbol": symbol,
                "filed_at": download["filed_at"],
                "form10k": None,
                "form10k_link": None,
                "form10q": None,
                "form10q_link": None,
            },
        )
        row["filed_at"] = max(row["filed_at"], download["filed_at"])

        columns = FORM_COLUMNS[download["form_type"]]
        row[columns["filing"]] = download["filing"]
        row[columns["link"]] = download["link"]

    return list(combined.values())


def store_downloads(
    engine: Engine,
    downloads: Iterable[dict[str, Any]],
    existing_symbols: set[str],
) -> tuple[int, int]:
    """Update existing symbols and insert symbols not yet in the table."""
    rows = combine_symbol_downloads(downloads)
    updates = [row for row in rows if row["symbol"] in existing_symbols]
    inserts = [row for row in rows if row["symbol"] not in existing_symbols]

    update_statement = text(
        """
        UPDATE latest_earnings_filings
        SET filed_at = CASE
                WHEN filed_at IS NULL OR filed_at < :filed_at THEN :filed_at
                ELSE filed_at
            END,
            form10k = COALESCE(:form10k, form10k),
            form10k_link = COALESCE(:form10k_link, form10k_link),
            form10q = COALESCE(:form10q, form10q),
            form10q_link = COALESCE(:form10q_link, form10q_link)
        WHERE symbol = :symbol
        """
    )
    insert_statement = text(
        """
        INSERT INTO latest_earnings_filings (
            symbol,
            filed_at,
            form10k,
            form10k_link,
            form10q,
            form10q_link
        )
        VALUES (
            :symbol,
            :filed_at,
            :form10k,
            :form10k_link,
            :form10q,
            :form10q_link
        )
        """
    )

    with engine.begin() as connection:
        if updates:
            connection.execute(update_statement, updates)
        if inserts:
            connection.execute(insert_statement, inserts)

    return len(updates), len(inserts)


def update_latest_earnings_filings(
    jsonl_path: Path = DEFAULT_JSONL_PATH,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> dict[str, Any]:
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if not jsonl_path.is_file():
        raise FileNotFoundError(f"Filing stream JSONL does not exist: {jsonl_path}")

    events, malformed_lines = load_latest_filing_events(jsonl_path)
    engine = make_stocks_engine()
    try:
        existing = load_existing_rows(engine, (event.symbol for event in events))
        selected, skipped = select_new_events(events, existing)
        downloads, errors = download_filings(selected, max_workers=max_workers)
        updated, inserted = store_downloads(
            engine,
            downloads,
            existing_symbols=set(existing),
        )
    finally:
        engine.dispose()

    return {
        "eligible_events": len(events),
        "skipped_events": skipped,
        "downloaded_filings": len(downloads),
        "updated_symbols": updated,
        "inserted_symbols": inserted,
        "malformed_lines": malformed_lines,
        "errors": errors,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--jsonl",
        type=Path,
        default=DEFAULT_JSONL_PATH,
        help=f"Filing stream JSONL path (default: {DEFAULT_JSONL_PATH})",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Concurrent sec-api downloads (default: {DEFAULT_MAX_WORKERS})",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = parse_args()

    try:
        summary = update_latest_earnings_filings(
            jsonl_path=args.jsonl,
            max_workers=args.max_workers,
        )
    except Exception:
        LOGGER.exception("Latest earnings filing update failed")
        return 1

    LOGGER.info(
        "Eligible: %d; skipped: %d; downloaded: %d; updated: %d; "
        "inserted: %d; malformed JSONL lines: %d; errors: %d",
        summary["eligible_events"],
        summary["skipped_events"],
        summary["downloaded_filings"],
        summary["updated_symbols"],
        summary["inserted_symbols"],
        summary["malformed_lines"],
        len(summary["errors"]),
    )
    for error in summary["errors"]:
        LOGGER.error(
            "%s %s download failed: %s",
            error["symbol"],
            error["form_type"],
            error["error"],
        )

    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
