#! This script will be scheduled to run Mon-Fri at 7:05p.m. It's possible there are weekend filings though theses would be 
#! extremely rare. If it appears as a problem check the run rechedule in task scheduler.
"""Update stocks.latest_earnings_filings from the SEC filing stream JSONL log."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sec_api import MappingApi, QueryApi, RenderApi
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
    "10-K": {
        "filing": "form10k",
        "link": "form10k_link",
        "period": "period_of_10k",
    },
    "10-Q": {
        "filing": "form10q",
        "link": "form10q_link",
        "period": "period_of_10q",
    },
}

ACCESSION_WITH_DASHES_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")
ACCESSION_DIRECTORY_RE = re.compile(r"/(\d{18})(?:/|$)")


@dataclass(frozen=True)
class FilingEvent:
    symbol: str
    form_type: str
    filed_at: datetime
    link: str
    period_of_report: date | None = None
    cik: str | None = None


def parse_mysql_datetime(value: Any) -> datetime:
    """Convert a stream timestamp to a naive UTC datetime for MySQL."""
    if not value:
        raise ValueError("filed_at is missing")

    parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def parse_report_period(value: Any) -> date:
    """Convert SEC periodOfReport metadata to a date."""
    if not value:
        raise ValueError("periodOfReport is missing")
    return date.fromisoformat(str(value).strip()[:10])


def extract_accession_number(link: str) -> str:
    """Extract a dashed SEC accession number from an archive URL."""
    dashed_match = ACCESSION_WITH_DASHES_RE.search(link)
    if dashed_match:
        return dashed_match.group(1)

    directory_match = ACCESSION_DIRECTORY_RE.search(link)
    if directory_match:
        compact = directory_match.group(1)
        return f"{compact[:10]}-{compact[10:12]}-{compact[12:]}"

    raise ValueError(f"Could not determine SEC accession number from {link}")


def normalize_cik(value: Any) -> str:
    normalized = str(value or "").strip().lstrip("0")
    if not normalized.isdigit():
        raise ValueError("SEC metadata did not include a valid issuer CIK")
    return normalized


def resolve_filing_event(
    event: FilingEvent,
    query_api: QueryApi,
    mapping_api: MappingApi,
    issuer_cik_cache: dict[str, set[str]],
) -> FilingEvent:
    """Resolve canonical filing metadata and verify the symbol's current issuer."""
    accession_number = extract_accession_number(event.link)
    response = query_api.get_filings(
        {
            "query": {
                "query_string": {
                    "query": f'accessionNo:"{accession_number}"',
                }
            },
            "from": "0",
            "size": "10",
        }
    )
    filings = response.get("filings", [])
    metadata = next(
        (
            filing
            for filing in filings
            if filing.get("accessionNo") == accession_number
            and str(filing.get("formType") or "").upper() == event.form_type
        ),
        None,
    )
    if metadata is None:
        raise LookupError(
            f"SEC-API returned no {event.form_type} metadata for {accession_number}"
        )

    filing_cik = normalize_cik(metadata.get("cik"))
    allowed_ciks = issuer_cik_cache.get(event.symbol)
    if allowed_ciks is None:
        mappings = mapping_api.resolve("ticker", event.symbol) or []
        exact_mappings = [
            mapping
            for mapping in mappings
            if str(mapping.get("ticker") or "").strip().upper() == event.symbol
        ]
        active_mappings = [
            mapping for mapping in exact_mappings if not mapping.get("isDelisted")
        ]
        issuer_mappings = active_mappings or exact_mappings
        allowed_ciks = {
            normalize_cik(mapping.get("cik")) for mapping in issuer_mappings
        }
        issuer_cik_cache[event.symbol] = allowed_ciks

    if not allowed_ciks:
        raise LookupError(f"SEC-API returned no issuer mapping for {event.symbol}")
    if filing_cik not in allowed_ciks:
        raise ValueError(
            f"issuer CIK {filing_cik} does not match {event.symbol} "
            f"CIK(s) {sorted(allowed_ciks)}"
        )

    primary_link = str(metadata.get("linkToFilingDetails") or "").strip()
    if not primary_link:
        primary_link = next(
            (
                str(document.get("documentUrl") or "").strip()
                for document in metadata.get("documentFormatFiles", []) or []
                if str(document.get("type") or "").strip().upper()
                == event.form_type
                and document.get("documentUrl")
            ),
            "",
        )
    if not primary_link:
        raise LookupError("SEC-API metadata did not include a primary filing document")

    return replace(
        event,
        filed_at=parse_mysql_datetime(metadata.get("filedAt")),
        link=primary_link,
        period_of_report=parse_report_period(metadata.get("periodOfReport")),
        cik=filing_cik,
    )


def resolve_filing_events(
    events: Iterable[FilingEvent],
) -> tuple[list[FilingEvent], list[dict[str, str]]]:
    """Resolve and validate candidate events, retaining per-event failures."""
    resolved: list[FilingEvent] = []
    errors: list[dict[str, str]] = []
    query_api = QueryApi(api_key=sec_api_key)
    mapping_api = MappingApi(api_key=sec_api_key)
    issuer_cik_cache: dict[str, set[str]] = {}

    for event in events:
        try:
            resolved.append(
                resolve_filing_event(
                    event,
                    query_api=query_api,
                    mapping_api=mapping_api,
                    issuer_cik_cache=issuer_cik_cache,
                )
            )
        except Exception as exc:
            errors.append(
                {
                    "symbol": event.symbol,
                    "form_type": event.form_type,
                    "error": str(exc).replace(sec_api_key, "[REDACTED]"),
                }
            )

    return resolved, errors


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
        SELECT symbol, filed_at, form10k_link, form10q_link,
               period_of_10k, period_of_10q
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


def exclude_unchanged_events(
    events: Iterable[FilingEvent],
    existing: dict[str, dict[str, Any]],
) -> tuple[list[FilingEvent], int]:
    """Avoid SEC-API requests for links that are already stored."""
    candidates: list[FilingEvent] = []
    skipped = 0
    for event in events:
        current = existing.get(event.symbol)
        link_column = FORM_COLUMNS[event.form_type]["link"]
        if current is not None and current.get(link_column) == event.link:
            skipped += 1
        else:
            candidates.append(event)
    return candidates, skipped


def select_new_events(
    events: Iterable[FilingEvent],
    existing: dict[str, dict[str, Any]],
) -> tuple[list[FilingEvent], int]:
    """Exclude filings that do not cover a newer period for their form."""
    selected: list[FilingEvent] = []
    skipped = 0

    for event in events:
        current = existing.get(event.symbol)
        if current is None:
            selected.append(event)
            continue

        link_column = FORM_COLUMNS[event.form_type]["link"]
        current_link = current.get(link_column)
        period_column = FORM_COLUMNS[event.form_type]["period"]
        current_period = current.get(period_column)

        if current_link == event.link:
            skipped += 1
            continue

        # Annual and quarterly freshness are independent. periodOfReport also
        # prevents a recently accepted filing for an old period from winning.
        if (
            current_link
            and current_period is not None
            and event.period_of_report is not None
            and event.period_of_report <= current_period
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
        "period_of_report": event.period_of_report,
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
                "period_of_10k": None,
                "form10q": None,
                "form10q_link": None,
                "period_of_10q": None,
            },
        )
        row["filed_at"] = max(row["filed_at"], download["filed_at"])

        columns = FORM_COLUMNS[download["form_type"]]
        row[columns["filing"]] = download["filing"]
        row[columns["link"]] = download["link"]
        row[columns["period"]] = download["period_of_report"]

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
                WHEN (
                    :form10k IS NOT NULL
                    AND (period_of_10k IS NULL OR :period_of_10k > period_of_10k)
                ) OR (
                    :form10q IS NOT NULL
                    AND (period_of_10q IS NULL OR :period_of_10q > period_of_10q)
                )
                THEN CASE
                    WHEN filed_at IS NULL OR filed_at < :filed_at THEN :filed_at
                    ELSE filed_at
                END
                ELSE filed_at
            END,
            form10k = CASE
                WHEN :form10k IS NOT NULL
                     AND (period_of_10k IS NULL OR :period_of_10k > period_of_10k)
                THEN :form10k ELSE form10k
            END,
            form10k_link = CASE
                WHEN :form10k IS NOT NULL
                     AND (period_of_10k IS NULL OR :period_of_10k > period_of_10k)
                THEN :form10k_link ELSE form10k_link
            END,
            period_of_10k = CASE
                WHEN :form10k IS NOT NULL
                     AND (period_of_10k IS NULL OR :period_of_10k > period_of_10k)
                THEN :period_of_10k ELSE period_of_10k
            END,
            form10q = CASE
                WHEN :form10q IS NOT NULL
                     AND (period_of_10q IS NULL OR :period_of_10q > period_of_10q)
                THEN :form10q ELSE form10q
            END,
            form10q_link = CASE
                WHEN :form10q IS NOT NULL
                     AND (period_of_10q IS NULL OR :period_of_10q > period_of_10q)
                THEN :form10q_link ELSE form10q_link
            END,
            period_of_10q = CASE
                WHEN :form10q IS NOT NULL
                     AND (period_of_10q IS NULL OR :period_of_10q > period_of_10q)
                THEN :period_of_10q ELSE period_of_10q
            END
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
            period_of_10k,
            form10q,
            form10q_link,
            period_of_10q
        )
        VALUES (
            :symbol,
            :filed_at,
            :form10k,
            :form10k_link,
            :period_of_10k,
            :form10q,
            :form10q_link,
            :period_of_10q
        )
        ON DUPLICATE KEY UPDATE
            filed_at = CASE
                WHEN (
                    :form10k IS NOT NULL
                    AND (period_of_10k IS NULL OR :period_of_10k > period_of_10k)
                ) OR (
                    :form10q IS NOT NULL
                    AND (period_of_10q IS NULL OR :period_of_10q > period_of_10q)
                )
                THEN CASE
                    WHEN filed_at IS NULL OR filed_at < :filed_at THEN :filed_at
                    ELSE filed_at
                END
                ELSE filed_at
            END,
            form10k = CASE
                WHEN :form10k IS NOT NULL
                     AND (period_of_10k IS NULL OR :period_of_10k > period_of_10k)
                THEN :form10k ELSE form10k
            END,
            form10k_link = CASE
                WHEN :form10k IS NOT NULL
                     AND (period_of_10k IS NULL OR :period_of_10k > period_of_10k)
                THEN :form10k_link ELSE form10k_link
            END,
            period_of_10k = CASE
                WHEN :form10k IS NOT NULL
                     AND (period_of_10k IS NULL OR :period_of_10k > period_of_10k)
                THEN :period_of_10k ELSE period_of_10k
            END,
            form10q = CASE
                WHEN :form10q IS NOT NULL
                     AND (period_of_10q IS NULL OR :period_of_10q > period_of_10q)
                THEN :form10q ELSE form10q
            END,
            form10q_link = CASE
                WHEN :form10q IS NOT NULL
                     AND (period_of_10q IS NULL OR :period_of_10q > period_of_10q)
                THEN :form10q_link ELSE form10q_link
            END,
            period_of_10q = CASE
                WHEN :form10q IS NOT NULL
                     AND (period_of_10q IS NULL OR :period_of_10q > period_of_10q)
                THEN :period_of_10q ELSE period_of_10q
            END
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
        candidates, unchanged = exclude_unchanged_events(events, existing)
        resolved, metadata_errors = resolve_filing_events(candidates)
        selected, stale = select_new_events(resolved, existing)
        downloads, download_errors = download_filings(
            selected,
            max_workers=max_workers,
        )
        updated, inserted = store_downloads(
            engine,
            downloads,
            existing_symbols=set(existing),
        )
    finally:
        engine.dispose()

    return {
        "eligible_events": len(events),
        "skipped_events": unchanged + stale,
        "downloaded_filings": len(downloads),
        "updated_symbols": updated,
        "inserted_symbols": inserted,
        "malformed_lines": malformed_lines,
        "errors": metadata_errors + download_errors,
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
            "%s %s processing failed: %s",
            error["symbol"],
            error["form_type"],
            error["error"],
        )

    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
