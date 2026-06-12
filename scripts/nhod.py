from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import queue
import sys
import threading
import traceback
import tkinter as tk
from dataclasses import dataclass, replace
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Iterable
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

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
DAILY_QUANT_RATING_TABLE = "daily_quant_rating"
OUTPUT_TABLE = "nhod"
STREAM_TABLE = "ohlcv_1m"
EASTERN = ZoneInfo("America/New_York")

DISPLAY_COLUMNS = ("symbol", "rvol", "quant_rating", "hod_time", "hod_price")
OUTPUT_COLUMNS = ("symbol", "rvol", "quant_rating")
SORTABLE_COLUMNS = set(DISPLAY_COLUMNS)
RVOL_COLUMN_CANDIDATES = (
    "rvol",
    "RVol",
    "RVOL",
    "relative_volume",
    "relative volume",
    "current_rvol",
)


@dataclass(frozen=True)
class MonitorConfig:
    lookback_minutes: int
    poll_interval: float
    symbol_chunk_size: int
    include_extended_hours: bool
    once: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor stocks.elevated_rvol for symbols making a new high of day "
            "within a configurable lookback window, update stocks.nhod, and "
            "display the table in a live GUI."
        )
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=10,
        help="Minutes to look back for new high-of-day events. Defaults to 10.",
    )
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--symbol-query-chunk-size", type=int, default=250)
    parser.add_argument(
        "--include-extended-hours",
        action="store_true",
        help="Use all rows from the current date instead of regular-session rows only.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one refresh and exit instead of continuously updating.",
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


def numeric_or_none(value: Any) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return None
    number_float = float(number)
    return number_float if math.isfinite(number_float) else None


def format_number(value: Any, decimals: int = 2) -> str:
    number = numeric_or_none(value)
    if number is None:
        return ""
    return f"{number:.{decimals}f}"


def format_time(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).strftime("%H:%M")


def fetch_elevated_rvol(engine: Engine) -> pd.DataFrame:
    query = text(
        f"""
        SELECT *
        FROM {mysql_identifier(ELEVATED_RVOL_TABLE)}
        ORDER BY symbol
        """
    )
    frame = pd.read_sql(query, con=engine)
    if frame.empty:
        return pd.DataFrame(columns=["symbol", "rvol"])

    symbol_col = resolve_column(frame.columns, ("symbol", "Symbol", "ticker", "Ticker"))
    if symbol_col is None:
        symbol_col = frame.columns[0]
    rvol_col = resolve_column(frame.columns, RVOL_COLUMN_CANDIDATES)

    result = pd.DataFrame()
    result["symbol"] = frame[symbol_col].astype(str).str.strip().str.upper()
    result["rvol"] = frame[rvol_col].map(numeric_or_none) if rvol_col else None
    result = result[result["symbol"] != ""]
    return result.drop_duplicates("symbol")


def load_latest_quant_ratings(engine: Engine) -> dict[str, float | None]:
    frame = pd.read_sql(
        f"SELECT * FROM {mysql_identifier(DAILY_QUANT_RATING_TABLE)}",
        con=engine,
    )
    if frame.empty or "index" not in frame.columns:
        return {}

    dated_columns: list[tuple[pd.Timestamp, str]] = []
    for column in frame.columns:
        if column == "index":
            continue
        timestamp = pd.to_datetime(str(column), errors="coerce")
        if pd.notna(timestamp):
            dated_columns.append((pd.Timestamp(timestamp), column))
    if not dated_columns:
        return {}

    _, latest_column = max(dated_columns, key=lambda item: item[0])
    latest = frame.set_index("index")[latest_column]
    return {
        str(symbol).strip().upper(): numeric_or_none(value)
        for symbol, value in latest.items()
        if str(symbol).strip()
    }


def empty_ohlcv_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["Symbol", "Timestamp", "High"])


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
        SELECT Symbol, Timestamp, High
        FROM {mysql_identifier(STREAM_TABLE)}
        WHERE Timestamp >= :start_ts
          AND Timestamp <= :end_ts
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
    rows["High"] = pd.to_numeric(rows["High"], errors="coerce")
    return rows.dropna(subset=["High"]).sort_values(["Symbol", "Timestamp"])


def session_bounds(include_extended_hours: bool) -> tuple[dt.datetime, dt.datetime]:
    today = dt.datetime.now(EASTERN).date()
    if include_extended_hours:
        start_time = dt.time.min
        end_time = dt.time.max
    else:
        start_time = dt.time(9, 30)
        end_time = dt.time(16, 0)
    return dt.datetime.combine(today, start_time), dt.datetime.combine(today, end_time)


def detect_nhod_rows(
    elevated_rvol: pd.DataFrame,
    ohlcv_rows: pd.DataFrame,
    quant_ratings: dict[str, float | None],
    lookback_minutes: int,
    as_of: dt.datetime,
) -> pd.DataFrame:
    if elevated_rvol.empty or ohlcv_rows.empty:
        return pd.DataFrame(columns=DISPLAY_COLUMNS)

    rvol_by_symbol = elevated_rvol.set_index("symbol")["rvol"].to_dict()
    cutoff = pd.Timestamp(as_of - dt.timedelta(minutes=lookback_minutes))
    output_rows: list[dict[str, Any]] = []

    for symbol, symbol_rows in ohlcv_rows.groupby("Symbol", sort=False):
        data = symbol_rows.sort_values("Timestamp").copy()
        previous_hod = data["High"].cummax().shift(1)
        new_hod = previous_hod.isna() | (data["High"] > previous_hod)
        recent_new_hods = data.loc[new_hod & (data["Timestamp"] >= cutoff)]
        if recent_new_hods.empty:
            continue

        latest_hod = recent_new_hods.iloc[-1]
        symbol_upper = str(symbol).upper()
        output_rows.append(
            {
                "symbol": symbol_upper,
                "rvol": numeric_or_none(rvol_by_symbol.get(symbol_upper)),
                "quant_rating": quant_ratings.get(symbol_upper),
                "hod_time": pd.Timestamp(latest_hod["Timestamp"]),
                "hod_price": numeric_or_none(latest_hod["High"]),
            }
        )

    return pd.DataFrame(output_rows, columns=DISPLAY_COLUMNS).sort_values(
        ["rvol", "symbol"],
        ascending=[False, True],
        na_position="last",
    )


def reset_output_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {mysql_identifier(OUTPUT_TABLE)}"))


def write_nhod_rows(engine: Engine, rows: pd.DataFrame) -> None:
    if rows.empty:
        records: list[dict[str, Any]] = []
    else:
        clean_rows = rows.loc[:, OUTPUT_COLUMNS].astype(object)
        clean_rows = clean_rows.where(pd.notna(clean_rows), None)
        records = clean_rows.to_dict("records")

    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {mysql_identifier(OUTPUT_TABLE)}"))
        if records:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {mysql_identifier(OUTPUT_TABLE)}
                        ({", ".join(mysql_identifier(column) for column in OUTPUT_COLUMNS)})
                    VALUES
                        ({", ".join(f":{column}" for column in OUTPUT_COLUMNS)})
                    """
                ),
                records,
            )


def refresh_nhod(
    stocks_engine: Engine,
    stream_engine: Engine,
    config: MonitorConfig,
) -> pd.DataFrame:
    elevated = fetch_elevated_rvol(stocks_engine)
    symbols = elevated["symbol"].tolist() if not elevated.empty else []
    quant_ratings = load_latest_quant_ratings(stocks_engine)
    session_start, session_end = session_bounds(config.include_extended_hours)
    as_of = dt.datetime.now(EASTERN).replace(tzinfo=None)
    rows = fetch_ohlcv_rows(
        engine=stream_engine,
        symbols=symbols,
        start_ts=session_start,
        end_ts=min(as_of, session_end),
        symbol_chunk_size=config.symbol_chunk_size,
    )
    nhod_rows = detect_nhod_rows(
        elevated_rvol=elevated,
        ohlcv_rows=rows,
        quant_ratings=quant_ratings,
        lookback_minutes=config.lookback_minutes,
        as_of=as_of,
    )
    write_nhod_rows(stocks_engine, nhod_rows)
    return nhod_rows


class NhodGUI:
    def __init__(self, root: tk.Tk, config: MonitorConfig) -> None:
        self.root = root
        self.config = config
        self.output_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.refresh_event = threading.Event()
        self.lookback_lock = threading.Lock()
        self.lookback_minutes = config.lookback_minutes
        self.last_rows: list[dict[str, Any]] = []
        self.sort_column = "rvol"
        self.sort_descending = True

        self.root.title("New High of Day")
        self.root.geometry("760x500")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.status_var = tk.StringVar(value="Starting...")
        self.lookback_var = tk.StringVar(value=str(config.lookback_minutes))
        self.pause_button_text = tk.StringVar(value="Pause Updates")
        self._build_widgets()
        self.start_worker()
        self.root.after(250, self.process_queue)

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
            "symbol": 100,
            "rvol": 100,
            "quant_rating": 120,
            "hod_time": 120,
            "hod_price": 120,
        }
        anchors = {
            "symbol": tk.W,
            "rvol": tk.E,
            "quant_rating": tk.E,
            "hod_time": tk.CENTER,
            "hod_price": tk.E,
        }
        for column in DISPLAY_COLUMNS:
            self.tree.heading(
                column,
                text=column,
                command=lambda col=column: self.sort_by_column(col),
            )
            self.tree.column(column, width=widths[column], anchor=anchors[column])

        y_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=y_scroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")

        controls = ttk.Frame(container)
        controls.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        controls.columnconfigure(4, weight=1)

        ttk.Label(controls, text="Lookback min").grid(
            row=0,
            column=0,
            sticky=tk.W,
            padx=(0, 6),
        )
        lookback_entry = ttk.Entry(controls, textvariable=self.lookback_var, width=8)
        lookback_entry.grid(row=0, column=1, sticky=tk.W, padx=(0, 8))
        lookback_entry.bind("<Return>", lambda _event: self.apply_lookback())
        ttk.Button(controls, text="Apply Lookback", command=self.apply_lookback).grid(
            row=0,
            column=2,
            sticky=tk.W,
            padx=(0, 8),
        )
        ttk.Button(
            controls,
            textvariable=self.pause_button_text,
            command=self.toggle_pause,
        ).grid(row=0, column=3, sticky=tk.W, padx=(0, 8))
        ttk.Button(container, text="Refresh Now", command=self.render_last_rows).grid(
            row=2,
            column=0,
            sticky=tk.W,
            pady=(10, 0),
        )
        ttk.Label(container, textvariable=self.status_var).grid(
            row=2,
            column=0,
            sticky=tk.E,
            pady=(10, 0),
        )
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

    def start_worker(self) -> None:
        worker = threading.Thread(
            target=self.worker_loop,
            name="nhod-gui-worker",
            daemon=True,
        )
        worker.start()

    def current_lookback_minutes(self) -> int:
        with self.lookback_lock:
            return self.lookback_minutes

    def apply_lookback(self) -> None:
        try:
            lookback_minutes = int(self.lookback_var.get().strip())
        except ValueError:
            messagebox.showwarning("Invalid Lookback", "Enter a whole number of minutes.")
            return
        if lookback_minutes <= 0:
            messagebox.showwarning("Invalid Lookback", "Lookback minutes must be greater than 0.")
            return

        with self.lookback_lock:
            self.lookback_minutes = lookback_minutes
        self.refresh_event.set()
        self.status_var.set(f"Lookback set to {lookback_minutes}m")

    def toggle_pause(self) -> None:
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.pause_button_text.set("Pause Updates")
            self.status_var.set("Updates resumed")
        else:
            self.pause_event.set()
            self.pause_button_text.set("Resume Updates")
            self.status_var.set("Updates paused")
        self.refresh_event.set()

    def sort_by_column(self, column: str) -> None:
        if column not in SORTABLE_COLUMNS:
            return
        if self.sort_column == column:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_column = column
            self.sort_descending = column != "symbol"
        self.render_rows(self.last_rows)

    def render_last_rows(self) -> None:
        self.refresh_event.set()
        self.render_rows(self.last_rows)

    def render_rows(self, rows: list[dict[str, Any]]) -> None:
        sorted_rows = list(rows)
        if self.sort_column == "symbol":
            sorted_rows.sort(
                key=lambda row: str(row.get("symbol", "")),
                reverse=self.sort_descending,
            )
        elif self.sort_column == "hod_time":
            sorted_rows.sort(
                key=lambda row: pd.Timestamp(row.get("hod_time", pd.Timestamp.min)),
                reverse=self.sort_descending,
            )
        else:
            sorted_rows.sort(
                key=lambda row: numeric_or_none(row.get(self.sort_column)) or float("-inf"),
                reverse=self.sort_descending,
            )

        self.tree.delete(*self.tree.get_children())
        for row in sorted_rows:
            self.tree.insert(
                "",
                tk.END,
                values=(
                    row.get("symbol", ""),
                    format_number(row.get("rvol")),
                    format_number(row.get("quant_rating")),
                    format_time(row.get("hod_time")),
                    format_number(row.get("hod_price")),
                ),
            )

    def process_queue(self) -> None:
        try:
            while True:
                message_type, payload = self.output_queue.get_nowait()
                if message_type == "status":
                    self.status_var.set(str(payload))
                elif message_type == "rows":
                    self.last_rows = payload
                    self.render_rows(self.last_rows)
                elif message_type == "error":
                    self.status_var.set("Worker failed")
                    messagebox.showerror("NHOD Failed", str(payload))
        except queue.Empty:
            pass

        if not self.stop_event.is_set():
            self.root.after(250, self.process_queue)

    def worker_loop(self) -> None:
        try:
            stocks_engine = make_gptdb_engine(STOCKS_DB)
            stream_engine = make_stream_engine(STREAM_DB)
            reset_output_table(stocks_engine)
            self.output_queue.put(("status", "Monitoring new highs of day..."))

            while not self.stop_event.is_set():
                if self.pause_event.is_set():
                    self.stop_event.wait(0.25)
                    continue

                active_config = replace(
                    self.config,
                    lookback_minutes=self.current_lookback_minutes(),
                )
                rows = refresh_nhod(stocks_engine, stream_engine, active_config)
                records = rows.to_dict("records")
                self.output_queue.put(("rows", records))
                self.output_queue.put(
                    (
                        "status",
                        f"{dt.datetime.now(EASTERN):%H:%M:%S}: "
                        f"nhod={len(records)} lookback={active_config.lookback_minutes}m",
                    )
                )

                if self.config.once:
                    self.stop_event.set()
                    self.root.after(0, self.root.destroy)
                    break
                self.refresh_event.wait(self.config.poll_interval)
                self.refresh_event.clear()
        except Exception:
            self.output_queue.put(("error", traceback.format_exc()))

    def on_close(self) -> None:
        self.stop_event.set()
        self.root.destroy()


def main() -> None:
    args = parse_args()
    if args.lookback_minutes <= 0:
        raise ValueError("--lookback-minutes must be greater than 0")
    if args.poll_interval <= 0:
        raise ValueError("--poll-interval must be greater than 0")
    if args.symbol_query_chunk_size <= 0:
        raise ValueError("--symbol-query-chunk-size must be greater than 0")

    config = MonitorConfig(
        lookback_minutes=args.lookback_minutes,
        poll_interval=args.poll_interval,
        symbol_chunk_size=args.symbol_query_chunk_size,
        include_extended_hours=args.include_extended_hours,
        once=args.once,
    )
    root = tk.Tk()
    app = NhodGUI(root, config)
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
