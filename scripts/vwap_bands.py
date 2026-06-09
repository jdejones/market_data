from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
GPTDB_MYSQL_USER = "gptdb"
STREAM_MYSQL_USER = "price_data_streamer"
STOCKS_DB = "stocks"
STREAM_DB = "intraday_price_stream"
ELEVATED_RVOL_TABLE = "elevated_rvol"
OUTPUT_TABLE = "vwap_bands"
STREAM_TABLE = "ohlcv_1m"
EASTERN = ZoneInfo("America/New_York")
DEFAULT_BAND_MULTIPLIERS = (1.0, 2.0, 3.0)
OUTPUT_COLUMNS = [
    "symbol",
    "vwap",
    "vb1_pos",
    "vb2_pos",
    "vb3_pos",
    "vb1_neg",
    "vb2_neg",
    "vb3_neg",
]
VALUE_COLUMNS = [column for column in OUTPUT_COLUMNS if column != "symbol"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Calculate current session VWAP bands for a watchlist and write "
            "the latest values to stocks.vwap_bands."
        )
    )
    parser.add_argument(
        "--symbols-file",
        type=Path,
        help=(
            "Optional newline-delimited symbol file. Defaults to symbols in "
            "stocks.elevated_rvol."
        ),
    )
    parser.add_argument(
        "--date",
        type=lambda value: dt.datetime.strptime(value, "%Y-%m-%d").date(),
        help="Session date to calculate, in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--symbol-query-chunk-size", type=int, default=250)
    parser.add_argument(
        "--calc-mode",
        choices=("standard-deviation", "percentage"),
        default="standard-deviation",
        help=(
            "Band distance mode matching TradingView. Standard deviation is "
            "the PineScript default; percentage treats multiplier 1 as 1%%."
        ),
    )
    parser.add_argument("--band-mult-1", type=float, default=DEFAULT_BAND_MULTIPLIERS[0])
    parser.add_argument("--band-mult-2", type=float, default=DEFAULT_BAND_MULTIPLIERS[1])
    parser.add_argument("--band-mult-3", type=float, default=DEFAULT_BAND_MULTIPLIERS[2])
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single update instead of continuously polling.",
    )
    return parser.parse_args()


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def intraday_stream_password() -> str:
    env_password = os.getenv("intraday_stream_password")
    if env_password:
        return env_password

    from market_data.api_keys import intraday_stream_database  # type: ignore[import-not-found]

    return intraday_stream_database


def make_gptdb_engine(database: str) -> Engine:
    password = quote_plus(require_env("gptdb"))
    url = (
        f"mysql+pymysql://{GPTDB_MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def make_stream_engine(database: str) -> Engine:
    password = quote_plus(intraday_stream_password())
    url = (
        f"mysql+pymysql://{STREAM_MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def chunked(items: list[str], chunk_size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def resolve_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    column_list = list(columns)
    by_lower = {column.lower(): column for column in column_list}
    for candidate in candidates:
        if candidate in column_list:
            return candidate
        match = by_lower.get(candidate.lower())
        if match is not None:
            return match
    return None


def normalize_symbols(values: Iterable[Any]) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value is None or pd.isna(value):
            continue
        symbol = str(value).strip().upper()
        if not symbol or symbol.startswith("#") or symbol in seen:
            continue
        symbols.append(symbol)
        seen.add(symbol)
    return symbols


def load_symbols_file(symbols_file: Path) -> list[str]:
    with symbols_file.open("r", encoding="utf-8") as f:
        symbols = normalize_symbols(line.strip() for line in f)

    if not symbols:
        raise ValueError(f"No symbols found in {symbols_file}")
    return symbols


def fetch_elevated_symbols(engine: Engine) -> list[str]:
    query = text(
        f"""
        SELECT *
        FROM {mysql_identifier(ELEVATED_RVOL_TABLE)}
        ORDER BY symbol
        """
    )
    frame = pd.read_sql(query, con=engine)
    if frame.empty:
        return []

    symbol_col = resolve_column(frame.columns, ("symbol", "Symbol", "ticker", "Ticker"))
    if symbol_col is None:
        symbol_col = frame.columns[0]

    return normalize_symbols(frame[symbol_col])


def empty_ohlcv_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["Symbol", "Timestamp", "High", "Low", "Close", "Volume"]
    )


def fetch_ohlcv_rows(
    engine: Engine,
    symbols: list[str],
    start_ts: dt.datetime,
    end_ts: dt.datetime,
    symbol_chunk_size: int,
) -> pd.DataFrame:
    if not symbols:
        return empty_ohlcv_frame()

    frames: list[pd.DataFrame] = []
    stmt = text(
        f"""
        SELECT Symbol, Timestamp, High, Low, Close, Volume
        FROM {mysql_identifier(STREAM_TABLE)}
        WHERE Timestamp >= :start_ts
          AND Timestamp < :end_ts
          AND Symbol IN :symbols
        ORDER BY Symbol, Timestamp
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
        return empty_ohlcv_frame()

    rows = pd.concat(frames, ignore_index=True)
    rows["Symbol"] = rows["Symbol"].astype(str).str.upper()
    rows["Timestamp"] = pd.to_datetime(rows["Timestamp"])
    return rows.sort_values(["Symbol", "Timestamp"])


def calculate_symbol_vwap_bands(
    symbol_rows: pd.DataFrame,
    band_multipliers: tuple[float, float, float],
    calc_mode: str,
) -> dict[str, Any] | None:
    data = symbol_rows.sort_values("Timestamp").copy()
    for column in ("High", "Low", "Close", "Volume"):
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["High", "Low", "Close", "Volume"])
    data = data[data["Volume"] > 0]
    if data.empty:
        return None

    src = data[["High", "Low", "Close"]].mean(axis=1)
    volume = data["Volume"]
    cumulative_volume = volume.cumsum()
    cumulative_price_volume = (src * volume).cumsum()
    vwap = cumulative_price_volume / cumulative_volume

    if calc_mode == "percentage":
        band_basis = vwap * 0.01
    else:
        cumulative_squared_price_volume = ((src ** 2) * volume).cumsum()
        variance = cumulative_squared_price_volume / cumulative_volume - (vwap ** 2)
        band_basis = np.sqrt(variance.clip(lower=0))

    latest_vwap = float(vwap.iloc[-1])
    latest_basis = float(band_basis.iloc[-1])
    if not math.isfinite(latest_vwap) or not math.isfinite(latest_basis):
        return None

    return {
        "symbol": str(data["Symbol"].iloc[-1]).upper(),
        "vwap": latest_vwap,
        "vb1_pos": latest_vwap + latest_basis * band_multipliers[0],
        "vb2_pos": latest_vwap + latest_basis * band_multipliers[1],
        "vb3_pos": latest_vwap + latest_basis * band_multipliers[2],
        "vb1_neg": latest_vwap - latest_basis * band_multipliers[0],
        "vb2_neg": latest_vwap - latest_basis * band_multipliers[1],
        "vb3_neg": latest_vwap - latest_basis * band_multipliers[2],
    }


def calculate_vwap_bands(
    rows: pd.DataFrame,
    symbols: list[str],
    band_multipliers: tuple[float, float, float],
    calc_mode: str,
) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    grouped_rows = {
        symbol: symbol_rows for symbol, symbol_rows in rows.groupby("Symbol", sort=False)
    }
    output_rows: list[dict[str, Any]] = []
    for symbol in symbols:
        symbol_rows = grouped_rows.get(symbol)
        if symbol_rows is None or symbol_rows.empty:
            continue
        result = calculate_symbol_vwap_bands(symbol_rows, band_multipliers, calc_mode)
        if result is not None:
            output_rows.append(result)

    return pd.DataFrame(output_rows, columns=OUTPUT_COLUMNS)


def reset_output_table(engine: Engine) -> None:
    assignments = ", ".join(
        f"{mysql_identifier(column)} = NULL" for column in VALUE_COLUMNS
    )
    with engine.begin() as conn:
        conn.execute(text(f"UPDATE {mysql_identifier(OUTPUT_TABLE)} SET {assignments}"))


def write_vwap_bands(engine: Engine, bands: pd.DataFrame) -> None:
    if bands.empty:
        return

    records = bands.loc[:, OUTPUT_COLUMNS].to_dict("records")
    symbols = [str(record["symbol"]).upper() for record in records]
    select_existing = text(
        f"""
        SELECT symbol
        FROM {mysql_identifier(OUTPUT_TABLE)}
        WHERE symbol IN :symbols
        """
    ).bindparams(bindparam("symbols", expanding=True))
    update_stmt = text(
        f"""
        UPDATE {mysql_identifier(OUTPUT_TABLE)}
        SET {", ".join(f"{mysql_identifier(column)} = :{column}" for column in VALUE_COLUMNS)}
        WHERE symbol = :symbol
        """
    )
    insert_stmt = text(
        f"""
        INSERT INTO {mysql_identifier(OUTPUT_TABLE)}
            ({", ".join(mysql_identifier(column) for column in OUTPUT_COLUMNS)})
        VALUES
            ({", ".join(f":{column}" for column in OUTPUT_COLUMNS)})
        """
    )

    with engine.begin() as conn:
        existing_symbols = {
            str(row[0]).upper()
            for row in conn.execute(select_existing, {"symbols": symbols})
        }
        rows_to_update = [
            record for record in records if str(record["symbol"]).upper() in existing_symbols
        ]
        rows_to_insert = [
            record for record in records if str(record["symbol"]).upper() not in existing_symbols
        ]

        if rows_to_update:
            conn.execute(update_stmt, rows_to_update)
        if rows_to_insert:
            conn.execute(insert_stmt, rows_to_insert)


def resolve_symbols(stocks_engine: Engine, symbols_file: Path | None) -> list[str]:
    if symbols_file is not None:
        return load_symbols_file(symbols_file)
    return fetch_elevated_symbols(stocks_engine)


def update_vwap_bands(
    stocks_engine: Engine,
    stream_engine: Engine,
    symbols: list[str],
    session_date: dt.date,
    symbol_query_chunk_size: int,
    band_multipliers: tuple[float, float, float],
    calc_mode: str,
) -> int:
    reset_output_table(stocks_engine)

    if not symbols:
        return 0

    session_start = dt.datetime.combine(session_date, dt.time.min)
    session_end = session_start + dt.timedelta(days=1)
    rows = fetch_ohlcv_rows(
        engine=stream_engine,
        symbols=symbols,
        start_ts=session_start,
        end_ts=session_end,
        symbol_chunk_size=symbol_query_chunk_size,
    )
    bands = calculate_vwap_bands(
        rows=rows,
        symbols=symbols,
        band_multipliers=band_multipliers,
        calc_mode=calc_mode,
    )
    write_vwap_bands(stocks_engine, bands)
    return len(bands)


def main() -> None:
    args = parse_args()
    stocks_engine = make_gptdb_engine(STOCKS_DB)
    stream_engine = make_stream_engine(STREAM_DB)
    band_multipliers = (args.band_mult_1, args.band_mult_2, args.band_mult_3)

    print(f"Continuously updating stocks.{OUTPUT_TABLE}...")
    while True:
        session_date = args.date or dt.datetime.now(EASTERN).date()
        symbols = resolve_symbols(stocks_engine, args.symbols_file)
        rows_written = update_vwap_bands(
            stocks_engine=stocks_engine,
            stream_engine=stream_engine,
            symbols=symbols,
            session_date=session_date,
            symbol_query_chunk_size=args.symbol_query_chunk_size,
            band_multipliers=band_multipliers,
            calc_mode=args.calc_mode,
        )
        print(
            f"{dt.datetime.now(EASTERN):%H:%M:%S}: "
            f"symbols={len(symbols)} wrote={rows_written}"
        )

        if args.once:
            break
        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
