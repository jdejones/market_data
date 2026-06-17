from __future__ import annotations

import argparse
import datetime as dt
import queue
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import ttk
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine, URL


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from market_data.api_keys import gptdb, intraday_stream_database  # type: ignore[import-not-found]


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
GPTDB_MYSQL_USER = "gptdb"
STREAM_MYSQL_USER = "price_data_streamer"
STOCKS_DB = "stocks"
NEWS_DB = "news"
STREAM_DB = "intraday_price_stream"
ELEVATED_RVOL_TABLE = "elevated_rvol"
SUMMARY_TABLE = "summary"
NEW_EP_TABLE = "new_ep"
DAILY_QUANT_RATING_TABLE = "daily_quant_rating"
STREAM_TABLE = "ohlcv_1m"
DATE_COLUMN = "date"
SYMBOL_COLUMN = "symbol"
NEWS_AGGREGATE_COLUMN = "news_aggregate"
HEADLINES_COLUMN = "Title"
EASTERN = ZoneInfo("US/Eastern")
CENTRAL = ZoneInfo("America/Chicago")
MARKET_OPEN_CENTRAL = dt.time(8, 30)
OPEN_BAR_EASTERN = dt.time(9, 31)
DISPLAY_COLUMNS = (
    "symbol",
    "rvol",
    "quant_rating",
    "event_summary",
    "news_aggregate",
)


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def chunked(items: list[str], chunk_size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def make_gptdb_engine(database: str) -> Engine:
    url = URL.create(
        "mysql+pymysql",
        username=GPTDB_MYSQL_USER,
        password=gptdb,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=database,
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def make_stream_engine() -> Engine:
    url = URL.create(
        "mysql+pymysql",
        username=STREAM_MYSQL_USER,
        password=intraday_stream_database,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=STREAM_DB,
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": 5,
            "host": MYSQL_HOST,
            "port": MYSQL_PORT,
        },
    )


def table_columns(engine: Engine, table_name: str) -> list[str]:
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
        return [str(row[0]) for row in conn.execute(query, {"table_name": table_name})]


def clear_new_ep_table(stocks_engine: Engine) -> int:
    with stocks_engine.begin() as conn:
        result = conn.execute(text(f"DELETE FROM {mysql_identifier(NEW_EP_TABLE)}"))
    return result.rowcount or 0


def wait_for_market_open(
    poll_interval: float,
    status_callback: Callable[[str], None] | None = None,
) -> None:
    while True:
        now = dt.datetime.now(CENTRAL)
        market_open = dt.datetime.combine(now.date(), MARKET_OPEN_CENTRAL, tzinfo=CENTRAL)
        if now >= market_open:
            return

        seconds = min(poll_interval, max(1.0, (market_open - now).total_seconds()))
        message = (
            f"Waiting for market open at {market_open:%H:%M:%S %Z}; "
            f"sleeping {seconds:.0f}s."
        )
        print(message)
        if status_callback is not None:
            status_callback(message)
        time.sleep(seconds)


def load_elevated_rvol_symbols(stocks_engine: Engine) -> dict[str, float | None]:
    columns = table_columns(stocks_engine, ELEVATED_RVOL_TABLE)
    if SYMBOL_COLUMN not in columns:
        raise ValueError(f"{STOCKS_DB}.{ELEVATED_RVOL_TABLE} is missing {SYMBOL_COLUMN!r}")

    rvol_select = ", rvol" if "rvol" in columns else ""
    query = text(
        f"""
        SELECT {mysql_identifier(SYMBOL_COLUMN)}{rvol_select}
        FROM {mysql_identifier(ELEVATED_RVOL_TABLE)}
        ORDER BY {mysql_identifier(SYMBOL_COLUMN)}
        """
    )
    symbols: dict[str, float | None] = {}
    with stocks_engine.connect() as conn:
        for row in conn.execute(query):
            symbol = str(row[0]).upper()
            rvol = row[1] if len(row) > 1 else None
            symbols[symbol] = float(rvol) if rvol is not None else None
    return symbols


def load_summary_rows(stocks_engine: Engine, symbols: list[str]) -> dict[str, dict[str, float]]:
    if not symbols:
        return {}

    query = text(
        f"""
        SELECT
            {mysql_identifier(SYMBOL_COLUMN)} AS symbol,
            {mysql_identifier("close")} AS prior_close,
            {mysql_identifier("20dma")} AS twenty_dma
        FROM {mysql_identifier(SUMMARY_TABLE)}
        WHERE {mysql_identifier(SYMBOL_COLUMN)} IN :symbols
        """
    ).bindparams(bindparam("symbols", expanding=True))

    rows: dict[str, dict[str, float]] = {}
    with stocks_engine.connect() as conn:
        for row in conn.execute(query, {"symbols": symbols}):
            prior_close = pd.to_numeric(row.prior_close, errors="coerce")
            twenty_dma = pd.to_numeric(row.twenty_dma, errors="coerce")
            if pd.isna(prior_close) or pd.isna(twenty_dma):
                continue
            rows[str(row.symbol).upper()] = {
                "prior_close": float(prior_close),
                "twenty_dma": float(twenty_dma),
            }
    return rows


def latest_quant_rating_column(columns: list[str]) -> str | None:
    symbol_columns = {"index", "symbol", "Symbol"}
    rating_columns = [column for column in columns if column not in symbol_columns]
    return rating_columns[-1] if rating_columns else None


def load_quant_ratings(stocks_engine: Engine, symbols: list[str]) -> dict[str, float | None]:
    if not symbols:
        return {}

    columns = table_columns(stocks_engine, DAILY_QUANT_RATING_TABLE)
    symbol_column = next(
        (column for column in ("index", "symbol", "Symbol") if column in columns),
        None,
    )
    rating_column = latest_quant_rating_column(columns)
    if symbol_column is None or rating_column is None:
        return {}

    query = text(
        f"""
        SELECT
            {mysql_identifier(symbol_column)} AS symbol,
            {mysql_identifier(rating_column)} AS quant_rating
        FROM {mysql_identifier(DAILY_QUANT_RATING_TABLE)}
        WHERE {mysql_identifier(symbol_column)} IN :symbols
        """
    ).bindparams(bindparam("symbols", expanding=True))

    ratings: dict[str, float | None] = {}
    with stocks_engine.connect() as conn:
        for row in conn.execute(query, {"symbols": symbols}):
            rating = pd.to_numeric(row.quant_rating, errors="coerce")
            ratings[str(row.symbol).upper()] = None if pd.isna(rating) else float(rating)
    return ratings


def fetch_open_prices(
    stream_engine: Engine,
    symbols: list[str],
    trading_day: dt.date,
    symbol_chunk_size: int,
) -> dict[str, float]:
    open_ts = dt.datetime.combine(trading_day, OPEN_BAR_EASTERN)
    end_ts = open_ts + dt.timedelta(minutes=1)
    query = text(
        f"""
        SELECT Symbol, Open
        FROM {mysql_identifier(STREAM_TABLE)}
        WHERE Timestamp >= :open_ts
          AND Timestamp < :end_ts
          AND Symbol IN :symbols
        """
    ).bindparams(bindparam("symbols", expanding=True))

    prices: dict[str, float] = {}
    with stream_engine.connect() as conn:
        for symbol_group in chunked(symbols, symbol_chunk_size):
            for row in conn.execute(
                query,
                {"open_ts": open_ts, "end_ts": end_ts, "symbols": symbol_group},
            ):
                open_price = pd.to_numeric(row.Open, errors="coerce")
                if not pd.isna(open_price):
                    prices[str(row.Symbol).upper()] = float(open_price)
    return prices


def fetch_latest_closes(
    stream_engine: Engine,
    symbols: list[str],
    trading_day: dt.date,
    symbol_chunk_size: int,
) -> dict[str, dict[str, Any]]:
    day_start = dt.datetime.combine(trading_day, dt.time.min)
    day_end = day_start + dt.timedelta(days=1)
    query = text(
        f"""
        SELECT o.Symbol, o.Timestamp, o.Close
        FROM {mysql_identifier(STREAM_TABLE)} AS o
        INNER JOIN (
            SELECT Symbol, MAX(Timestamp) AS latest_ts
            FROM {mysql_identifier(STREAM_TABLE)}
            WHERE Timestamp >= :day_start
              AND Timestamp < :day_end
              AND Symbol IN :symbols
            GROUP BY Symbol
        ) AS latest
          ON o.Symbol = latest.Symbol
         AND o.Timestamp = latest.latest_ts
        """
    ).bindparams(bindparam("symbols", expanding=True))

    closes: dict[str, dict[str, Any]] = {}
    with stream_engine.connect() as conn:
        for symbol_group in chunked(symbols, symbol_chunk_size):
            for row in conn.execute(
                query,
                {"day_start": day_start, "day_end": day_end, "symbols": symbol_group},
            ):
                close_price = pd.to_numeric(row.Close, errors="coerce")
                if not pd.isna(close_price):
                    closes[str(row.Symbol).upper()] = {
                        "current_close": float(close_price),
                        "latest_timestamp": row.Timestamp,
                    }
    return closes


def find_new_eps(
    stocks_engine: Engine,
    stream_engine: Engine,
    gap_threshold: float,
    symbol_chunk_size: int,
) -> list[dict[str, Any]]:
    elevated = load_elevated_rvol_symbols(stocks_engine)
    symbols = sorted(elevated)
    if not symbols:
        return []

    trading_day = dt.datetime.now(EASTERN).date()
    summary_rows = load_summary_rows(stocks_engine, symbols)
    quant_ratings = load_quant_ratings(stocks_engine, symbols)
    open_prices = fetch_open_prices(stream_engine, symbols, trading_day, symbol_chunk_size)
    latest_closes = fetch_latest_closes(stream_engine, symbols, trading_day, symbol_chunk_size)

    events: list[dict[str, Any]] = []
    for symbol in symbols:
        summary = summary_rows.get(symbol)
        current_open = open_prices.get(symbol)
        latest = latest_closes.get(symbol)
        if summary is None or current_open is None or latest is None:
            continue

        prior_close = summary["prior_close"]
        if prior_close == 0:
            continue

        gap_percent = ((current_open - prior_close) / prior_close) * 100
        current_close = latest["current_close"]
        twenty_dma = summary["twenty_dma"]
        if gap_percent >= gap_threshold and current_close > twenty_dma:
            events.append(
                {
                    "symbol": symbol,
                    "date": trading_day,
                    "quant_rating": quant_ratings.get(symbol),
                    "prior_close": prior_close,
                    "current_open": current_open,
                    "current_close": current_close,
                    "twenty_dma": twenty_dma,
                    "gap_percent": gap_percent,
                    "rvol": elevated.get(symbol),
                    "latest_timestamp": latest["latest_timestamp"],
                }
            )

    return sorted(events, key=lambda row: row["symbol"])


def values_for_table_columns(event: dict[str, Any], columns: list[str]) -> dict[str, Any]:
    value_by_column = {
        "symbol": event["symbol"],
        "date": event["date"],
        "quant_rating": event["quant_rating"],
        "prior_close": event["prior_close"],
        "current_open": event["current_open"],
        "open": event["current_open"],
        "current_close": event["current_close"],
        "latest_close": event["current_close"],
        "close": event["current_close"],
        "20dma": event["twenty_dma"],
        "twenty_dma": event["twenty_dma"],
        "gap_percent": event["gap_percent"],
        "gap_pct": event["gap_percent"],
        "rvol": event["rvol"],
    }
    return {
        column: value_by_column[column]
        for column in columns
        if column in value_by_column
    }


def upsert_new_ep_rows(stocks_engine: Engine, events: list[dict[str, Any]]) -> int:
    if not events:
        return 0

    columns = table_columns(stocks_engine, NEW_EP_TABLE)
    if SYMBOL_COLUMN not in columns:
        raise ValueError(f"{STOCKS_DB}.{NEW_EP_TABLE} is missing {SYMBOL_COLUMN!r}")

    match_columns = [SYMBOL_COLUMN]
    if DATE_COLUMN in columns:
        match_columns.append(DATE_COLUMN)

    inserted_count = 0
    with stocks_engine.begin() as conn:
        for event in events:
            values = values_for_table_columns(event, columns)
            insert_columns = list(values)
            column_sql = ", ".join(mysql_identifier(column) for column in insert_columns)
            value_sql = ", ".join(f":{column}" for column in insert_columns)
            where_sql = " AND ".join(
                f"{mysql_identifier(column)} = :{column}" for column in match_columns
            )

            insert_stmt = text(
                f"""
                INSERT INTO {mysql_identifier(NEW_EP_TABLE)} ({column_sql})
                SELECT {value_sql}
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {mysql_identifier(NEW_EP_TABLE)}
                    WHERE {where_sql}
                )
                """
            )
            result = conn.execute(insert_stmt, values)
            inserted_count += result.rowcount or 0

            update_columns = [
                column for column in insert_columns if column not in match_columns
            ]
            if update_columns:
                update_sql = ", ".join(
                    f"{mysql_identifier(column)} = :{column}" for column in update_columns
                )
                update_stmt = text(
                    f"""
                    UPDATE {mysql_identifier(NEW_EP_TABLE)}
                    SET {update_sql}
                    WHERE {where_sql}
                    """
                )
                conn.execute(update_stmt, values)

    return inserted_count


def fetch_event_headlines(news_engine: Engine, symbol: str, event_date: dt.date) -> list[str]:
    query = text(
        f"""
        SELECT {mysql_identifier(HEADLINES_COLUMN)}
        FROM {mysql_identifier(symbol.lower())}
        WHERE DATE({mysql_identifier(DATE_COLUMN)}) = :event_date
        """
    )
    with news_engine.connect() as conn:
        result = conn.execute(query, {"event_date": event_date})
        return [
            str(row[0]).strip()
            for row in result
            if row[0] is not None and str(row[0]).strip()
        ]


def update_news_aggregates(
    stocks_engine: Engine,
    news_engine: Engine,
    events: list[dict[str, Any]],
) -> int:
    if not events:
        return 0

    columns = table_columns(stocks_engine, NEW_EP_TABLE)
    if NEWS_AGGREGATE_COLUMN not in columns:
        return 0

    match_date = DATE_COLUMN in columns
    where_sql = f"{mysql_identifier(SYMBOL_COLUMN)} = :symbol"
    if match_date:
        where_sql += f" AND {mysql_identifier(DATE_COLUMN)} = :date"

    update_stmt = text(
        f"""
        UPDATE {mysql_identifier(NEW_EP_TABLE)}
        SET {mysql_identifier(NEWS_AGGREGATE_COLUMN)} = :news_aggregate
        WHERE {where_sql}
        """
    )

    updated_count = 0
    with stocks_engine.begin() as conn:
        for event in events:
            symbol = str(event["symbol"])
            event_date = event["date"]
            try:
                headlines = fetch_event_headlines(news_engine, symbol, event_date)
            except Exception as exc:
                print(f"{symbol} {event_date}: no same-day news loaded: {exc}")
                continue

            if not headlines:
                continue

            result = conn.execute(
                update_stmt,
                {
                    "symbol": symbol,
                    "date": event_date,
                    "news_aggregate": "\n".join(headlines),
                },
            )
            updated_count += result.rowcount or 0

    return updated_count


def display_events(events: list[dict[str, Any]], inserted_count: int, updated_news_count: int) -> None:
    now = dt.datetime.now(EASTERN)
    if not events:
        print(f"{now:%H:%M:%S %Z}: no elevated-RVol episodic pivots found.")
        return

    display = pd.DataFrame(events)
    display = display[
        [
            "symbol",
            "gap_percent",
            "current_open",
            "current_close",
            "prior_close",
            "twenty_dma",
            "rvol",
            "quant_rating",
            "latest_timestamp",
        ]
    ].copy()
    display["gap_percent"] = display["gap_percent"].map(lambda value: f"{value:.2f}%")
    for column in ("current_open", "current_close", "prior_close", "twenty_dma"):
        display[column] = display[column].map(lambda value: f"{value:.2f}")
    print(f"\n{now:%H:%M:%S %Z}: episodic pivots")
    print(display.to_string(index=False))
    print(f"Inserted {inserted_count} new rows; updated news for {updated_news_count} rows.")


def run_scan_once(
    stocks_engine: Engine,
    news_engine: Engine,
    stream_engine: Engine,
    gap_threshold: float,
    symbol_chunk_size: int,
) -> tuple[list[dict[str, Any]], int, int]:
    events = find_new_eps(
        stocks_engine=stocks_engine,
        stream_engine=stream_engine,
        gap_threshold=gap_threshold,
        symbol_chunk_size=symbol_chunk_size,
    )
    inserted_count = upsert_new_ep_rows(stocks_engine, events)
    updated_news_count = update_news_aggregates(stocks_engine, news_engine, events)
    return events, inserted_count, updated_news_count


def load_new_ep_display_rows(stocks_engine: Engine) -> list[dict[str, Any]]:
    columns = table_columns(stocks_engine, NEW_EP_TABLE)
    select_columns = [column for column in DISPLAY_COLUMNS if column in columns]
    if SYMBOL_COLUMN not in select_columns:
        raise ValueError(f"{STOCKS_DB}.{NEW_EP_TABLE} is missing {SYMBOL_COLUMN!r}")

    query = text(
        f"""
        SELECT {", ".join(mysql_identifier(column) for column in select_columns)}
        FROM {mysql_identifier(NEW_EP_TABLE)}
        ORDER BY {mysql_identifier(SYMBOL_COLUMN)}
        """
    )
    with stocks_engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(query)]


def format_display_value(column: str, value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if column in {"rvol", "quant_rating"}:
        return f"{float(value):.2f}"
    return str(value)


class NewEPGUI:
    def __init__(
        self,
        root: tk.Tk,
        stocks_engine: Engine,
        news_engine: Engine,
        stream_engine: Engine,
        args: argparse.Namespace,
    ) -> None:
        self.root = root
        self.stocks_engine = stocks_engine
        self.news_engine = news_engine
        self.stream_engine = stream_engine
        self.args = args
        self.messages: queue.Queue[dict[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.refresh_now_event = threading.Event()
        self.worker: threading.Thread | None = None

        self.status_var = tk.StringVar(value="Ready")
        self.root.title("New Episodic Pivots")
        self.root.geometry("1250x560")
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        self._build_widgets()
        self.start_worker()
        self.root.after(250, self.process_messages)

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        self.tree = ttk.Treeview(
            container,
            columns=DISPLAY_COLUMNS,
            show="headings",
            selectmode="browse",
        )
        widths = {
            "symbol": 90,
            "rvol": 90,
            "quant_rating": 110,
            "event_summary": 360,
            "news_aggregate": 620,
        }
        for column in DISPLAY_COLUMNS:
            self.tree.heading(column, text=column)
            self.tree.column(column, width=widths[column], anchor=tk.W)

        y_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=self.tree.yview)
        x_scroll = ttk.Scrollbar(container, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")

        controls = ttk.Frame(container)
        controls.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        ttk.Button(controls, text="Refresh Now", command=self.refresh_now).pack(
            side=tk.LEFT
        )

        ttk.Label(container, textvariable=self.status_var).grid(
            row=3,
            column=0,
            columnspan=2,
            sticky=tk.W,
            pady=(10, 0),
        )

        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

    def start_worker(self) -> None:
        self.worker = threading.Thread(target=self.run_worker, daemon=True)
        self.worker.start()

    def run_worker(self) -> None:
        try:
            if not self.args.no_wait:
                wait_for_market_open(self.args.poll_interval, self.status_queue)

            while not self.stop_event.is_set():
                scan_started = dt.datetime.now(EASTERN)
                events, inserted_count, updated_news_count = run_scan_once(
                    stocks_engine=self.stocks_engine,
                    news_engine=self.news_engine,
                    stream_engine=self.stream_engine,
                    gap_threshold=self.args.gap_threshold,
                    symbol_chunk_size=self.args.symbol_query_chunk_size,
                )
                display_rows = load_new_ep_display_rows(self.stocks_engine)
                self.messages.put(
                    {
                        "type": "result",
                        "events": display_rows,
                        "inserted_count": inserted_count,
                        "updated_news_count": updated_news_count,
                        "event_count": len(events),
                        "scan_started": scan_started,
                    }
                )

                if self.args.once:
                    break

                self.refresh_now_event.wait(self.args.poll_interval)
                self.refresh_now_event.clear()
        except Exception as exc:
            stream_url = self.stream_engine.url.render_as_string(hide_password=True)
            self.messages.put(
                {
                    "type": "error",
                    "error": f"{exc} | stream_url={stream_url}",
                }
            )

    def status_queue(self, message: str) -> None:
        self.messages.put({"type": "status", "message": message})

    def refresh_now(self) -> None:
        self.status_var.set("Refresh requested...")
        self.refresh_now_event.set()

    def process_messages(self) -> None:
        while True:
            try:
                message = self.messages.get_nowait()
            except queue.Empty:
                break

            if message["type"] == "status":
                self.status_var.set(message["message"])
            elif message["type"] == "result":
                self.render_events(
                    events=message["events"],
                    inserted_count=message["inserted_count"],
                    updated_news_count=message["updated_news_count"],
                    event_count=message["event_count"],
                    scan_started=message["scan_started"],
                )
            elif message["type"] == "error":
                self.status_var.set(f"Error: {message['error']}")

        if not self.stop_event.is_set():
            self.root.after(250, self.process_messages)

    def render_events(
        self,
        events: list[dict[str, Any]],
        inserted_count: int,
        updated_news_count: int,
        event_count: int,
        scan_started: dt.datetime,
    ) -> None:
        self.tree.delete(*self.tree.get_children())
        for event in events:
            values = [
                format_display_value(column, event.get(column))
                for column in DISPLAY_COLUMNS
            ]
            self.tree.insert("", tk.END, values=values)

        self.status_var.set(
            f"{scan_started:%H:%M:%S %Z}: {event_count} EPs found; "
            f"inserted {inserted_count}; updated news for {updated_news_count}."
        )

    def close(self) -> None:
        self.stop_event.set()
        self.refresh_now_event.set()
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find elevated-RVol stocks that qualify as intraday episodic pivots "
            "and store them in stocks.new_ep."
        )
    )
    parser.add_argument("--gap-threshold", type=float, default=8.0)
    parser.add_argument("--poll-interval", type=float, default=30.0)
    parser.add_argument("--symbol-query-chunk-size", type=int, default=500)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one scan after the market-open gate, then exit.",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Skip waiting for the 8:30 Central market-open gate.",
    )
    parser.add_argument(
        "--no-gui",
        action="store_true",
        help="Run in console mode instead of opening the Tkinter GUI.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stocks_engine = make_gptdb_engine(STOCKS_DB)
    deleted_count = clear_new_ep_table(stocks_engine)
    print(f"Cleared {deleted_count} rows from {STOCKS_DB}.{NEW_EP_TABLE}.")

    news_engine = make_gptdb_engine(NEWS_DB)
    stream_engine = make_stream_engine()

    if not args.no_gui:
        root = tk.Tk()
        NewEPGUI(
            root=root,
            stocks_engine=stocks_engine,
            news_engine=news_engine,
            stream_engine=stream_engine,
            args=args,
        )
        root.mainloop()
        return

    if not args.no_wait:
        wait_for_market_open(args.poll_interval)

    while True:
        events, inserted_count, updated_news_count = run_scan_once(
            stocks_engine=stocks_engine,
            news_engine=news_engine,
            stream_engine=stream_engine,
            gap_threshold=args.gap_threshold,
            symbol_chunk_size=args.symbol_query_chunk_size,
        )
        display_events(events, inserted_count, updated_news_count)
        if args.once:
            break
        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
