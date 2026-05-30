from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

# Allow running this file directly by absolute path from outside the repo.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from market_data.api_keys import intraday_stream_database, polygon_api_key  # type: ignore[import-not-found]

from polygon.websocket import WebSocketClient
from polygon.websocket.models import Feed, Market, WebSocketMessage
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "price_data_streamer"
MYSQL_SCHEMA = "intraday_price_stream"
MYSQL_TABLE = "ohlcv_1m"

DEFAULT_FEED = "delayed"
PRICE_COLUMNS = ("Open", "High", "Low", "Close", "Volume", "VWAP")
EASTERN_TIME = ZoneInfo("America/New_York")
PROGRESS_EVERY_ROWS = 1_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream Polygon/Massive 1-minute OHLCV aggregates into MySQL."
    )
    parser.add_argument(
        "--symbols-file",
        type=Path,
        help=(
            "Optional text file containing one ticker per line. "
            "Defaults to all stock symbols via AM.*."
        ),
    )
    parser.add_argument(
        "--feed",
        choices=("realtime", "delayed"),
        default=DEFAULT_FEED,
        help="Polygon websocket feed to use. Defaults to delayed.",
    )
    return parser.parse_args()


def build_engine() -> Engine:
    password = quote_plus(intraday_stream_database)
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{password}@{MYSQL_HOST}:{MYSQL_PORT}/"
        f"{MYSQL_SCHEMA}"
    )
    return create_engine(url, pool_pre_ping=True, future=True)


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def ensure_table_structure(engine: Engine) -> None:
    table = mysql_identifier(MYSQL_TABLE)

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    {mysql_identifier("Symbol")} VARCHAR(32) NOT NULL,
                    {mysql_identifier("Timestamp")} DATETIME NOT NULL,
                    {mysql_identifier("Open")} FLOAT,
                    {mysql_identifier("High")} FLOAT,
                    {mysql_identifier("Low")} FLOAT,
                    {mysql_identifier("Close")} FLOAT,
                    {mysql_identifier("Volume")} FLOAT,
                    {mysql_identifier("VWAP")} FLOAT,
                    PRIMARY KEY ({mysql_identifier("Symbol")}, {mysql_identifier("Timestamp")})
                )
                """
            )
        )
        conn.execute(text(f"DELETE FROM {table}"))

        columns = {
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = :schema
                    AND TABLE_NAME = :table
                    """
                ),
                {"schema": MYSQL_SCHEMA, "table": MYSQL_TABLE},
            )
        }

        if "Symbol" not in columns:
            conn.execute(
                text(
                    f"ALTER TABLE {table} "
                    f"ADD COLUMN {mysql_identifier('Symbol')} VARCHAR(32) NOT NULL FIRST"
                )
            )
            columns.add("Symbol")

        if "Timestamp" not in columns:
            conn.execute(
                text(
                    f"ALTER TABLE {table} "
                    f"ADD COLUMN {mysql_identifier('Timestamp')} DATETIME NOT NULL "
                    f"AFTER {mysql_identifier('Symbol')}"
                )
            )
            columns.add("Timestamp")

        for column in PRICE_COLUMNS:
            if column not in columns:
                conn.execute(
                    text(
                        f"ALTER TABLE {table} "
                        f"ADD COLUMN {mysql_identifier(column)} FLOAT"
                    )
                )
                columns.add(column)

        primary_key_columns = [
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = :schema
                    AND TABLE_NAME = :table
                    AND CONSTRAINT_NAME = 'PRIMARY'
                    ORDER BY ORDINAL_POSITION
                    """
                ),
                {"schema": MYSQL_SCHEMA, "table": MYSQL_TABLE},
            )
        ]

        if not primary_key_columns:
            conn.execute(
                text(
                    f"ALTER TABLE {table} "
                    f"ADD PRIMARY KEY ({mysql_identifier('Symbol')}, "
                    f"{mysql_identifier('Timestamp')})"
                )
            )


def reset_table(engine: Engine) -> None:
    ensure_table_structure(engine)


def read_symbols(symbols_file: Path | None) -> list[str]:
    if symbols_file is None:
        return []

    symbols = []
    with symbols_file.open("r", encoding="utf-8") as f:
        for line in f:
            symbol = line.strip().upper()
            if symbol and not symbol.startswith("#"):
                symbols.append(symbol)

    if not symbols:
        raise ValueError(f"No symbols found in {symbols_file}")

    return symbols


def build_subscriptions(symbols: Sequence[str]) -> str:
    if not symbols:
        return "AM.*"

    return ",".join(f"AM.{symbol}" for symbol in symbols)


def select_feed(feed_name: str) -> Feed:
    if feed_name == "delayed":
        return Feed.Delayed

    return Feed.RealTime


def raw_value(message: WebSocketMessage, *attrs: str) -> object:
    for attr in attrs:
        value = getattr(message, attr, None)
        if value is not None:
            return value
    return None


def normalize_timestamp(raw_timestamp: object) -> dt.datetime | None:
    if raw_timestamp is None:
        return None

    if isinstance(raw_timestamp, dt.datetime):
        timestamp = raw_timestamp
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
        return timestamp.astimezone(EASTERN_TIME).replace(tzinfo=None)

    try:
        timestamp_int = int(raw_timestamp)
    except (TypeError, ValueError):
        return None

    # Polygon aggregate timestamps are normally Unix milliseconds.
    if timestamp_int > 1_000_000_000_000_000:
        seconds = timestamp_int / 1_000_000_000
    elif timestamp_int > 1_000_000_000_000:
        seconds = timestamp_int / 1_000
    else:
        seconds = timestamp_int

    return (
        dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc)
        .astimezone(EASTERN_TIME)
        .replace(tzinfo=None)
    )


def aggregate_rows(
    messages: Iterable[WebSocketMessage],
) -> list[dict[str, object]]:
    rows = []
    for message in messages:
        symbol = raw_value(message, "symbol", "ticker", "sym")
        timestamp = normalize_timestamp(
            raw_value(message, "start_timestamp", "s", "timestamp", "t")
        )

        open_price = raw_value(message, "open", "o")
        high = raw_value(message, "high", "h")
        low = raw_value(message, "low", "l")
        close = raw_value(message, "close", "c")
        volume = raw_value(message, "volume", "v", "accumulated_volume")
        vwap = raw_value(message, "vwap", "vw", "average_price")

        if (
            symbol is None
            or timestamp is None
            or open_price is None
            or high is None
            or low is None
            or close is None
            or volume is None
            or vwap is None
        ):
            continue

        rows.append(
            {
                "Symbol": str(symbol).upper(),
                "Timestamp": timestamp,
                "Open": float(open_price),
                "High": float(high),
                "Low": float(low),
                "Close": float(close),
                "Volume": float(volume),
                "VWAP": float(vwap),
            }
        )

    return rows


def insert_rows(engine: Engine, rows: list[dict[str, object]]) -> None:
    if not rows:
        return

    statement = text(
        f"""
        INSERT INTO {mysql_identifier(MYSQL_TABLE)}
            ({mysql_identifier("Symbol")},
             {mysql_identifier("Timestamp")},
             {mysql_identifier("Open")},
             {mysql_identifier("High")},
             {mysql_identifier("Low")},
             {mysql_identifier("Close")},
             {mysql_identifier("Volume")},
             {mysql_identifier("VWAP")})
        VALUES
            (:Symbol, :Timestamp, :Open, :High, :Low, :Close, :Volume, :VWAP)
        ON DUPLICATE KEY UPDATE
            {mysql_identifier("Open")} = VALUES({mysql_identifier("Open")}),
            {mysql_identifier("High")} = VALUES({mysql_identifier("High")}),
            {mysql_identifier("Low")} = VALUES({mysql_identifier("Low")}),
            {mysql_identifier("Close")} = VALUES({mysql_identifier("Close")}),
            {mysql_identifier("Volume")} = VALUES({mysql_identifier("Volume")}),
            {mysql_identifier("VWAP")} = VALUES({mysql_identifier("VWAP")})
        """
    )

    with engine.begin() as conn:
        conn.execute(statement, rows)


def run_stream(engine: Engine, symbols: Sequence[str], feed_name: str) -> None:
    client = WebSocketClient(
        api_key=polygon_api_key,
        feed=select_feed(feed_name),
        market=Market.Stocks,
    )
    subscriptions = build_subscriptions(symbols)
    client.subscribe(subscriptions)
    inserted_rows = 0

    def handle_msg(messages: list[WebSocketMessage]) -> None:
        nonlocal inserted_rows

        rows = aggregate_rows(messages)
        insert_rows(engine, rows)
        if rows:
            inserted_rows += len(rows)
            if inserted_rows % PROGRESS_EVERY_ROWS < len(rows):
                print(f"Inserted {inserted_rows:,} aggregate row(s).", flush=True)

    subscription_label = "AM.*" if not symbols else f"{len(symbols)} symbol(s)"
    print(f"Subscribed to {subscription_label} on the {feed_name} feed.")
    client.run(handle_msg=handle_msg)


def main() -> None:
    args = parse_args()
    symbols = read_symbols(args.symbols_file)
    engine = build_engine()

    reset_table(engine)
    print(f"Cleared {MYSQL_SCHEMA}.{MYSQL_TABLE}")

    run_stream(engine, symbols=symbols, feed_name=args.feed)


if __name__ == "__main__":
    main()
