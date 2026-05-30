from __future__ import annotations

import argparse
import datetime as dt
import gzip
import importlib
import pickle
import sys
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))


DEFAULT_SYMBOLS_PICKLE = Path(
    r"E:\Market Research\Dataset\daily_after_close_study\symbols.pkl.gz"
)
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "price_data_streamer"
STOCKS_DB = "stocks"
RECENT_EVENTS_TABLE = "recent_events"
DATE_COLUMN = "date"
SYMBOL_COLUMN = "symbol"
RVOL_COLUMN = "RVol"
ATRS_TRADED_COLUMN = "ATRs_Traded"


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def make_engine() -> Engine:
    from market_data.api_keys import intraday_stream_database  # type: ignore[import-not-found]

    password = quote_plus(intraday_stream_database)
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{STOCKS_DB}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def prune_old_events(engine: Engine, max_age_days: int) -> int:
    cutoff_date = dt.date.today() - dt.timedelta(days=max_age_days)
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"""
                DELETE FROM {mysql_identifier(RECENT_EVENTS_TABLE)}
                WHERE {mysql_identifier(DATE_COLUMN)} < :cutoff_date
                """
            ),
            {"cutoff_date": cutoff_date},
        )
    return result.rowcount or 0


def load_symbol_objects(symbols_pickle: Path) -> Any:
    if not symbols_pickle.exists():
        raise FileNotFoundError(f"Symbols pickle not found: {symbols_pickle}")

    symbol_data_module = importlib.import_module("market_data.Symbol_Data")
    sys.modules.setdefault("Symbol_Data", symbol_data_module)

    with gzip.open(symbols_pickle, "rb") as f:
        return pickle.load(f)


def iter_symbol_data(symbols_obj: Any) -> Iterable[tuple[str, pd.DataFrame]]:
    if isinstance(symbols_obj, dict):
        iterable = symbols_obj.items()
    elif isinstance(symbols_obj, (list, tuple, set)):
        iterable = ((getattr(item, "symbol", None), item) for item in symbols_obj)
    else:
        iterable = ((getattr(symbols_obj, "symbol", None), symbols_obj),)

    for symbol_hint, value in iterable:
        symbol = getattr(value, "symbol", symbol_hint)
        df = getattr(value, "df", value)

        if symbol is None or not isinstance(df, pd.DataFrame):
            continue

        yield str(symbol).upper(), df


def event_date_from_row(index_value: Any, row: pd.Series) -> dt.date | None:
    timestamp = pd.to_datetime(index_value, errors="coerce")
    if pd.notna(timestamp):
        return timestamp.date()

    for column in ("Date", "date", "Datetime", "datetime", "Timestamp", "timestamp"):
        if column not in row:
            continue
        timestamp = pd.to_datetime(row[column], errors="coerce")
        if pd.notna(timestamp):
            return timestamp.date()

    return None


def find_recent_events(
    symbols_obj: Any,
    lookback_rows: int,
    rvol_threshold: float,
    atrs_traded_threshold: float,
) -> pd.DataFrame:
    events: list[dict[str, Any]] = []

    for symbol, df in iter_symbol_data(symbols_obj):
        missing_columns = {RVOL_COLUMN, ATRS_TRADED_COLUMN} - set(df.columns)
        if missing_columns:
            print(f"{symbol}: missing columns {sorted(missing_columns)}; skipping")
            continue

        recent_rows = df.tail(lookback_rows)
        rvol = pd.to_numeric(recent_rows[RVOL_COLUMN], errors="coerce")
        atrs_traded = pd.to_numeric(recent_rows[ATRS_TRADED_COLUMN], errors="coerce")
        mask = (
            rvol.replace([np.inf, -np.inf], np.nan).gt(rvol_threshold)
            & atrs_traded.replace([np.inf, -np.inf], np.nan).gt(atrs_traded_threshold)
        )

        for index_value, row in recent_rows.loc[mask].iterrows():
            event_date = event_date_from_row(index_value, row)
            if event_date is None:
                print(f"{symbol}: could not determine event date for row {index_value!r}")
                continue

            events.append(
                {
                    SYMBOL_COLUMN: symbol,
                    DATE_COLUMN: event_date,
                }
            )

    if not events:
        return pd.DataFrame(columns=[SYMBOL_COLUMN, DATE_COLUMN])

    return (
        pd.DataFrame(events)
        .drop_duplicates(subset=[SYMBOL_COLUMN, DATE_COLUMN])
        .sort_values([DATE_COLUMN, SYMBOL_COLUMN])
        .reset_index(drop=True)
    )


def insert_events(engine: Engine, events: pd.DataFrame) -> int:
    if events.empty:
        return 0

    records = events.to_dict(orient="records")
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"""
                INSERT INTO {mysql_identifier(RECENT_EVENTS_TABLE)}
                    ({mysql_identifier(SYMBOL_COLUMN)}, {mysql_identifier(DATE_COLUMN)})
                SELECT :symbol, :date
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {mysql_identifier(RECENT_EVENTS_TABLE)}
                    WHERE {mysql_identifier(SYMBOL_COLUMN)} = :symbol
                      AND {mysql_identifier(DATE_COLUMN)} = :date
                )
                """
            ),
            records,
        )
    return result.rowcount or 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Store recent high RVol/range-expansion events in stocks.recent_events."
    )
    parser.add_argument(
        "--symbols-pickle",
        type=Path,
        default=DEFAULT_SYMBOLS_PICKLE,
        help="Path to the gzip pickle containing SymbolData objects.",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=14,
        help="Remove recent_events rows older than this many calendar days.",
    )
    parser.add_argument(
        "--lookback-rows",
        type=int,
        default=1,
        help="Number of most recent daily rows to inspect per symbol.",
    )
    parser.add_argument("--rvol-threshold", type=float, default=2.0)
    parser.add_argument("--atrs-traded-threshold", type=float, default=1.75)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = make_engine()

    deleted_count = prune_old_events(engine, args.max_age_days)
    symbols_obj = load_symbol_objects(args.symbols_pickle)
    events = find_recent_events(
        symbols_obj=symbols_obj,
        lookback_rows=args.lookback_rows,
        rvol_threshold=args.rvol_threshold,
        atrs_traded_threshold=args.atrs_traded_threshold,
    )
    inserted_count = insert_events(engine, events)

    print(
        f"Deleted {deleted_count} old rows. "
        f"Found {len(events)} recent events; inserted {inserted_count} new rows."
    )


if __name__ == "__main__":
    main()
