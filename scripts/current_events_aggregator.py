from __future__ import annotations

import argparse
import datetime as dt
import os
import time
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "gptdb"
STOCKS_DB = "stocks"
NEWS_DB = "news"
ELEVATED_RVOL_TABLE = "elevated_rvol"
CURRENT_EVENTS_TABLE = "current_events"
SYMBOL_COLUMN = "symbol"
DATE_COLUMN = "date"
RVOL_COLUMN = "rvol"
HEADLINES_COLUMN = "Title"
NEWS_AGGREGATE_COLUMN = "news_aggregate"
EASTERN = ZoneInfo("US/Eastern")


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def make_engine(database: str) -> Engine:
    password = quote_plus(require_env("gptdb"))
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def clear_current_events(engine: Engine) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {mysql_identifier(CURRENT_EVENTS_TABLE)}")
        )
    return result.rowcount or 0


def load_candidate_symbols(engine: Engine, rvol_threshold: float) -> dict[str, float]:
    query = text(
        f"""
        SELECT
            {mysql_identifier(SYMBOL_COLUMN)} AS symbol,
            {mysql_identifier(RVOL_COLUMN)} AS rvol
        FROM {mysql_identifier(ELEVATED_RVOL_TABLE)}
        WHERE {mysql_identifier(RVOL_COLUMN)} >= :rvol_threshold
        ORDER BY {mysql_identifier(SYMBOL_COLUMN)}
        """
    )
    with engine.connect() as conn:
        return {
            str(row.symbol).upper(): float(row.rvol)
            for row in conn.execute(query, {"rvol_threshold": rvol_threshold})
            if row.symbol is not None and row.rvol is not None
        }


def fetch_event_headlines(
    news_engine: Engine,
    symbol: str,
    event_date: dt.date,
) -> list[str]:
    query = text(
        f"""
        SELECT {mysql_identifier(HEADLINES_COLUMN)}
        FROM {mysql_identifier(symbol.lower())}
        WHERE DATE({mysql_identifier(DATE_COLUMN)}) = :event_date
        """
    )
    with news_engine.connect() as conn:
        return [
            str(row[0]).strip()
            for row in conn.execute(query, {"event_date": event_date})
            if row[0] is not None and str(row[0]).strip()
        ]


def upsert_current_event(
    stocks_engine: Engine,
    symbol: str,
    event_date: dt.date,
    headlines: list[str],
) -> None:
    news_aggregate = "\n".join(headlines) if headlines else None
    query = text(
        f"""
        INSERT INTO {mysql_identifier(CURRENT_EVENTS_TABLE)}
            (
                {mysql_identifier(SYMBOL_COLUMN)},
                {mysql_identifier(DATE_COLUMN)},
                {mysql_identifier(NEWS_AGGREGATE_COLUMN)}
            )
        VALUES (:symbol, :event_date, :news_aggregate)
        ON DUPLICATE KEY UPDATE
            {mysql_identifier(NEWS_AGGREGATE_COLUMN)} = VALUES(
                {mysql_identifier(NEWS_AGGREGATE_COLUMN)}
            )
        """
    )
    with stocks_engine.begin() as conn:
        conn.execute(
            query,
            {
                "symbol": symbol,
                "event_date": event_date,
                "news_aggregate": news_aggregate,
            },
        )


def process_candidates(
    stocks_engine: Engine,
    news_engine: Engine,
    processed_symbols: set[str],
    rvol_threshold: float,
    event_date: dt.date,
) -> int:
    candidates = load_candidate_symbols(stocks_engine, rvol_threshold)
    new_symbols = sorted(set(candidates) - processed_symbols)
    processed_count = 0

    for symbol in new_symbols:
        try:
            headlines = fetch_event_headlines(news_engine, symbol, event_date)
            upsert_current_event(stocks_engine, symbol, event_date, headlines)
        except Exception as exc:
            # A failed symbol is deliberately not marked processed so a transient
            # database failure can be retried on the next polling cycle.
            print(f"{symbol}: failed to aggregate current headlines: {exc}", flush=True)
            continue

        processed_symbols.add(symbol)
        processed_count += 1
        print(
            f"{symbol}: stored {len(headlines)} headline(s) at "
            f"RVol {candidates[symbol]:.2f}.",
            flush=True,
        )

    return processed_count


def sleep_with_status(seconds: float, message: str) -> None:
    if seconds <= 0:
        return
    print(f"{message} Sleeping {seconds:g} seconds.", flush=True)
    time.sleep(seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate today's news once for each symbol reaching an intraday "
            "RVol threshold."
        )
    )
    parser.add_argument("--rvol-threshold", type=float, default=2.0)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument(
        "--startup-delay",
        type=float,
        default=60.0,
        help=(
            "Seconds to wait after clearing current_events before reading "
            "elevated_rvol, allowing Current RVol to clear stale startup rows."
        ),
    )
    parser.add_argument(
        "--run-duration-hours",
        type=float,
        default=None,
        help=(
            "Stop automatically after this many hours. By default, run until "
            "interrupted."
        ),
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.poll_interval <= 0:
        raise ValueError("--poll-interval must be greater than zero")
    if args.startup_delay < 0:
        raise ValueError("--startup-delay cannot be negative")
    if args.run_duration_hours is not None and args.run_duration_hours <= 0:
        raise ValueError("--run-duration-hours must be greater than zero")


def main() -> None:
    args = parse_args()
    validate_args(args)
    deadline = (
        time.monotonic() + (args.run_duration_hours * 60 * 60)
        if args.run_duration_hours is not None
        else None
    )
    stocks_engine = make_engine(STOCKS_DB)
    news_engine = make_engine(NEWS_DB)

    deleted_count = clear_current_events(stocks_engine)
    print(
        f"Cleared {deleted_count} row(s) from {STOCKS_DB}.{CURRENT_EVENTS_TABLE}.",
        flush=True,
    )
    sleep_with_status(
        min(
            args.startup_delay,
            max(0.0, deadline - time.monotonic()),
        )
        if deadline is not None
        else args.startup_delay,
        "Waiting for the Current RVol startup reset to finish.",
    )

    event_date = dt.datetime.now(EASTERN).date()
    processed_symbols: set[str] = set()
    print(
        f"Monitoring {STOCKS_DB}.{ELEVATED_RVOL_TABLE} for "
        f"RVol >= {args.rvol_threshold:g} on {event_date:%Y-%m-%d}.",
        flush=True,
    )

    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                print(
                    f"Run duration of {args.run_duration_hours:g} hour(s) completed.",
                    flush=True,
                )
                break

            current_date = dt.datetime.now(EASTERN).date()
            if current_date != event_date:
                clear_current_events(stocks_engine)
                processed_symbols.clear()
                event_date = current_date
                print(
                    f"Started a new event session for {event_date:%Y-%m-%d}.",
                    flush=True,
                )

            try:
                process_candidates(
                    stocks_engine=stocks_engine,
                    news_engine=news_engine,
                    processed_symbols=processed_symbols,
                    rvol_threshold=args.rvol_threshold,
                    event_date=event_date,
                )
            except Exception as exc:
                print(f"Candidate polling failed: {exc}", flush=True)

            sleep_seconds = args.poll_interval
            if deadline is not None:
                sleep_seconds = min(
                    sleep_seconds,
                    max(0.0, deadline - time.monotonic()),
                )
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        print("Current-events aggregator stopped.", flush=True)


if __name__ == "__main__":
    main()
