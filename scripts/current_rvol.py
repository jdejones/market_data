from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine
from tqdm import tqdm


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from market_data.api_keys import intraday_stream_database  # type: ignore[import-not-found]
from market_data.price_data_import import intraday_import  # type: ignore[import-not-found]


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "price_data_streamer"
STOCKS_DB = "stocks"
STREAM_DB = "intraday_price_stream"
TEMP_RVOL_TABLE = "temp_cum_rvol"
STREAM_TABLE = "ohlcv_1m"
ELEVATED_RVOL_TABLE = "elevated_rvol"
TIMESTAMP_COLUMN = "timestamp"
EASTERN = ZoneInfo("US/Eastern")


@dataclass(slots=True)
class MonitorState:
    cumulative_volume: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    processed_rows: set[tuple[str, pd.Timestamp]] = field(default_factory=set)
    latest_rvol: dict[str, float] = field(default_factory=dict)
    elevated_symbols: set[str] = field(default_factory=set)
    last_seen_timestamp: pd.Timestamp | None = None


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def chunked(items: list[str], chunk_size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def load_symbols(symbols_file: Path) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()

    with symbols_file.open("r", encoding="utf-8") as f:
        for line in f:
            symbol = line.strip().upper()
            if not symbol or symbol.startswith("#") or symbol in seen:
                continue
            symbols.append(symbol)
            seen.add(symbol)

    if not symbols:
        raise ValueError(f"No symbols found in {symbols_file}")

    return symbols


def make_engine(database: str) -> Engine:
    password = quote_plus(intraday_stream_database)
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def compute_symbol_avg_cumulative_volume(
    item: tuple[str, pd.DataFrame | None, dt.date, int],
) -> tuple[str, pd.Series | None]:
    symbol, df, target_date, lookback_days = item

    if df is None or df.empty:
        return symbol, None

    try:
        data = df.copy()
        if not isinstance(data.index, pd.DatetimeIndex):
            data.index = pd.to_datetime(data.index)

        data = data.sort_index()
        data = data.loc[data.index.date < target_date]
        if data.empty or "Volume" not in data.columns:
            return symbol, None

        dates = pd.Series(data.index.date, index=data.index)
        historical_dates = sorted(dates.unique())[-lookback_days:]
        if len(historical_dates) < lookback_days:
            return symbol, None

        data = data.loc[dates.isin(historical_dates)].copy()
        dates = pd.Series(data.index.date, index=data.index)
        times = pd.Series(data.index.time, index=data.index)
        data["cumulative_volume"] = data["Volume"].groupby(dates).cumsum()
        data["time"] = times

        profile = data.groupby("time")["cumulative_volume"].mean()
        profile.name = symbol
        return symbol, profile
    except Exception as exc:
        print(f"{symbol}: failed to calculate average cumulative volume: {exc}")
        return symbol, None


def build_historical_profiles(
    symbols: list[str],
    target_date: dt.date,
    lookback_days: int,
    calendar_buffer_days: int,
    market_open_only: bool,
    process_workers: int,
) -> pd.DataFrame:
    start_date = target_date - dt.timedelta(days=lookback_days + calendar_buffer_days)
    print(
        "Importing historical 1-minute data "
        f"from {start_date:%Y-%m-%d} through {target_date:%Y-%m-%d} "
        f"for {len(symbols)} symbols..."
    )

    intraday_data = intraday_import(
        wl=symbols,
        from_date=start_date.strftime("%Y-%m-%d"),
        to_date=target_date.strftime("%Y-%m-%d"),
        timespan="minute",
        multiplier=1,
        market_open_only=market_open_only,
    )

    tasks = [
        (symbol, intraday_data.get(symbol), target_date, lookback_days)
        for symbol in symbols
    ]
    max_workers = max(1, min(process_workers, len(tasks)))
    profiles: dict[str, pd.Series] = {}

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(compute_symbol_avg_cumulative_volume, task): task[0]
            for task in tasks
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Calculating historical RVol profiles",
        ):
            symbol, profile = future.result()
            if profile is not None and not profile.empty:
                profiles[symbol] = profile

    if not profiles:
        raise ValueError("No historical RVol profiles were calculated")

    profile_df = pd.concat(profiles.values(), axis=1)
    profile_df = profile_df.reindex(columns=[symbol for symbol in symbols if symbol in profile_df.columns])
    profile_df = profile_df.sort_index()

    full_minutes = pd.date_range(
        start=dt.datetime.combine(dt.date(2000, 1, 1), min(profile_df.index)),
        end=dt.datetime.combine(dt.date(2000, 1, 1), max(profile_df.index)),
        freq="1min",
    ).time
    profile_df = profile_df.reindex(full_minutes).ffill()
    profile_df.index = [minute.strftime("%H:%M:%S") for minute in profile_df.index]
    profile_df.index.name = TIMESTAMP_COLUMN
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan)

    print(f"Calculated historical profiles for {len(profile_df.columns)} symbols")
    return profile_df


def existing_columns(engine: Engine, table_name: str) -> list[str]:
    query = text(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
        """
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(query, {"table_name": table_name})]


def reset_temp_profile_table(
    engine: Engine,
    symbols: list[str],
    alter_chunk_size: int,
) -> None:
    del symbols, alter_chunk_size
    table = mysql_identifier(TEMP_RVOL_TABLE)

    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table}"))


def write_temp_profiles(engine: Engine, profile_df: pd.DataFrame, chunksize: int) -> None:
    output = (
        profile_df.reset_index()
        .melt(
            id_vars=TIMESTAMP_COLUMN,
            var_name="symbol",
            value_name="avg_cum_volume",
        )
        .dropna(subset=["avg_cum_volume"])
        .loc[:, ["symbol", TIMESTAMP_COLUMN, "avg_cum_volume"]]
    )
    output.to_sql(
        TEMP_RVOL_TABLE,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=chunksize,
    )


def prepare_temp_profile_table(
    engine: Engine,
    profile_df: pd.DataFrame,
    alter_chunk_size: int,
    insert_chunksize: int,
) -> None:
    symbols = list(profile_df.columns)

    print("Resetting stocks.temp_cum_rvol...")
    reset_temp_profile_table(engine, symbols, alter_chunk_size)
    print("Writing historical average cumulative volume profiles...")
    write_temp_profiles(engine, profile_df, insert_chunksize)


def reset_elevated_rvol_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {mysql_identifier(ELEVATED_RVOL_TABLE)} (
                    symbol VARCHAR(32) NOT NULL
                )
                """
            )
        )
        conn.execute(text(f"DELETE FROM {mysql_identifier(ELEVATED_RVOL_TABLE)}"))


def fetch_stream_rows(
    engine: Engine,
    symbols: list[str],
    start_ts: dt.datetime,
    end_ts: dt.datetime,
    symbol_chunk_size: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    stmt = text(
        f"""
        SELECT Symbol, Timestamp, Volume
        FROM {mysql_identifier(STREAM_TABLE)}
        WHERE Timestamp >= :start_ts
          AND Timestamp < :end_ts
          AND Symbol IN :symbols
        ORDER BY Timestamp, Symbol
        """
    ).bindparams(bindparam("symbols", expanding=True))

    for symbol_group in chunked(symbols, symbol_chunk_size):
        frame = pd.read_sql(
            stmt,
            con=engine,
            params={
                "start_ts": start_ts,
                "end_ts": end_ts,
                "symbols": symbol_group,
            },
        )
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=["Symbol", "Timestamp", "Volume"])

    rows = pd.concat(frames, ignore_index=True)
    rows["Timestamp"] = pd.to_datetime(rows["Timestamp"])
    rows = rows.sort_values(["Timestamp", "Symbol"])
    return rows


def latest_denominator(profile_df: pd.DataFrame, symbol: str, timestamp: pd.Timestamp) -> float | None:
    if symbol not in profile_df.columns:
        return None

    time_key = timestamp.floor("min").strftime("%H:%M:%S")
    if time_key in profile_df.index:
        denom = profile_df.at[time_key, symbol]
    else:
        available = profile_df.index[profile_df.index <= time_key]
        if len(available) == 0:
            return None
        denom = profile_df.at[available[-1], symbol]

    if pd.isna(denom) or denom == 0:
        return None
    return float(denom)


def process_stream_rows(
    rows: pd.DataFrame,
    profile_df: pd.DataFrame,
    state: MonitorState,
) -> dict[str, float]:
    latest_rvol: dict[str, float] = {}

    for row in rows.itertuples(index=False):
        symbol = str(row.Symbol).upper()
        timestamp = pd.Timestamp(row.Timestamp).floor("min")
        key = (symbol, timestamp)
        if key in state.processed_rows:
            continue

        state.processed_rows.add(key)
        state.last_seen_timestamp = (
            timestamp
            if state.last_seen_timestamp is None
            else max(state.last_seen_timestamp, timestamp)
        )

        volume = row.Volume
        if pd.isna(volume):
            continue

        state.cumulative_volume[symbol] += float(volume)
        denominator = latest_denominator(profile_df, symbol, timestamp)
        if denominator is None:
            continue

        rvol = state.cumulative_volume[symbol] / denominator
        if not pd.isna(rvol) and np.isfinite(rvol):
            latest_rvol[symbol] = state.latest_rvol[symbol] = float(rvol)

    return latest_rvol


def update_elevated_rvol_table(
    engine: Engine,
    state: MonitorState,
    latest_rvol: dict[str, float],
    threshold: float,
) -> None:
    above_threshold = {
        symbol for symbol, rvol in latest_rvol.items() if rvol > threshold
    }
    below_threshold = {
        symbol for symbol, rvol in latest_rvol.items() if rvol < threshold
    }
    to_insert = sorted(above_threshold - state.elevated_symbols)
    to_delete = sorted(below_threshold & state.elevated_symbols)
    to_refresh = set(latest_rvol) & state.elevated_symbols

    if not to_insert and not to_delete and not to_refresh:
        return

    if to_insert:
        state.elevated_symbols.update(to_insert)
    if to_delete:
        state.elevated_symbols.difference_update(to_delete)

    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {mysql_identifier(ELEVATED_RVOL_TABLE)}"))
        if state.elevated_symbols:
            conn.execute(
                text(
                    f"INSERT INTO {mysql_identifier(ELEVATED_RVOL_TABLE)} "
                    "(symbol, rvol) VALUES (:symbol, :rvol)"
                ),
                [
                    {"symbol": symbol, "rvol": state.latest_rvol[symbol]}
                    for symbol in sorted(state.elevated_symbols)
                ],
            )


def monitor_current_rvol(
    stream_engine: Engine,
    stocks_engine: Engine,
    symbols: list[str],
    profile_df: pd.DataFrame,
    threshold: float,
    poll_interval: float,
    query_overlap_minutes: int,
    symbol_chunk_size: int,
    verbose: bool,
) -> None:
    state = MonitorState()
    current_day = dt.datetime.now(EASTERN).date()
    day_start = dt.datetime.combine(current_day, dt.time.min)
    day_end = day_start + dt.timedelta(days=1)

    print(f"Monitoring current RVol for {current_day:%Y-%m-%d}...")
    while True:
        if state.last_seen_timestamp is None:
            start_ts = day_start
        else:
            start_ts = max(
                day_start,
                state.last_seen_timestamp.to_pydatetime()
                - dt.timedelta(minutes=query_overlap_minutes),
            )

        rows = fetch_stream_rows(
            engine=stream_engine,
            symbols=symbols,
            start_ts=start_ts,
            end_ts=day_end,
            symbol_chunk_size=symbol_chunk_size,
        )
        latest_rvol = process_stream_rows(rows, profile_df, state)
        update_elevated_rvol_table(stocks_engine, state, latest_rvol, threshold)

        if verbose:
            print(
                f"{dt.datetime.now(EASTERN):%H:%M:%S}: "
                f"processed_rows={len(state.processed_rows)} "
                f"latest_updates={len(latest_rvol)} "
                f"elevated={len(state.elevated_symbols)}"
            )

        time.sleep(poll_interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate current intraday RVol and maintain stocks.elevated_rvol."
    )
    parser.add_argument(
        "--symbols-file",
        required=True,
        type=Path,
        help="Path to a newline-delimited text file of symbols.",
    )
    parser.add_argument("--lookback-days", type=int, default=20)
    parser.add_argument("--calendar-buffer-days", type=int, default=15)
    parser.add_argument("--rvol-threshold", type=float, default=1.5)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--query-overlap-minutes", type=int, default=2)
    parser.add_argument("--symbol-query-chunk-size", type=int, default=500)
    parser.add_argument("--alter-chunk-size", type=int, default=100)
    parser.add_argument("--insert-chunksize", type=int, default=200)
    parser.add_argument(
        "--process-workers",
        type=int,
        default=max(1, (os.cpu_count() or 2) - 1),
    )
    parser.add_argument(
        "--market-open-only",
        action="store_true",
        help="Restrict historical profile imports to regular market hours.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbols = load_symbols(args.symbols_file)
    target_date = dt.datetime.now(EASTERN).date()
    stocks_engine = make_engine(STOCKS_DB)
    stream_engine = make_engine(STREAM_DB)

    reset_temp_profile_table(stocks_engine, [], args.alter_chunk_size)
    reset_elevated_rvol_table(stocks_engine)

    profile_df = build_historical_profiles(
        symbols=symbols,
        target_date=target_date,
        lookback_days=args.lookback_days,
        calendar_buffer_days=args.calendar_buffer_days,
        market_open_only=args.market_open_only,
        process_workers=args.process_workers,
    )
    prepare_temp_profile_table(
        engine=stocks_engine,
        profile_df=profile_df,
        alter_chunk_size=args.alter_chunk_size,
        insert_chunksize=args.insert_chunksize,
    )
    monitor_current_rvol(
        stream_engine=stream_engine,
        stocks_engine=stocks_engine,
        symbols=list(profile_df.columns),
        profile_df=profile_df,
        threshold=args.rvol_threshold,
        poll_interval=args.poll_interval,
        query_overlap_minutes=args.query_overlap_minutes,
        symbol_chunk_size=args.symbol_query_chunk_size,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()