from __future__ import annotations

import argparse
import datetime as dt
import os
import queue
import sys
import threading
import time
import traceback
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "price_data_streamer"
METADATA_MYSQL_USER = "gptdb"
STOCKS_DB = "stocks"
STREAM_DB = "intraday_price_stream"
EASTERN = ZoneInfo("US/Eastern")
SYMBOL_COLUMN = "symbol"
RVOL_COLUMN = "rvol"
QUANT_RATING_COLUMN = "quant rating"
RECENT_EVENT_COLUMN = "recent event"
TABLE_COLUMNS = (
    SYMBOL_COLUMN,
    RVOL_COLUMN,
    QUANT_RATING_COLUMN,
    RECENT_EVENT_COLUMN,
)
SORTABLE_COLUMNS = {SYMBOL_COLUMN, RVOL_COLUMN, QUANT_RATING_COLUMN}
DAILY_QUANT_RATING_TABLE = "daily_quant_rating"
DEFAULT_EVENTS_TABLE = "current_events"
FALLBACK_EVENTS_TABLE = "recent_events"
DATE_COLUMN_CANDIDATES = ("date", "Date", "event_date", "Event_Date")
EVENT_SUMMARY_COLUMN_CANDIDATES = (
    "event_summary",
    "event summary",
    "summary",
    "concurrent_event",
    "prior_event",
)
SYMBOL_COLUMN_CANDIDATES = ("symbol", "Symbol", "ticker", "Ticker", "index")


@dataclass(frozen=True)
class GuiConfig:
    symbols_file: Path
    lookback_days: int
    calendar_buffer_days: int
    initial_threshold: float
    poll_interval: float
    query_overlap_minutes: int
    symbol_query_chunk_size: int
    alter_chunk_size: int
    insert_chunksize: int
    process_workers: int
    market_open_only: bool
    events_table: str
    update_elevated_table: bool
    verbose: bool


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def make_engine(database: str) -> Engine:
    password = quote_plus(require_env("intraday_stream_password"))
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def make_metadata_engine(database: str) -> Engine:
    password = quote_plus(require_env("gptdb"))
    url = (
        f"mysql+pymysql://{METADATA_MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


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


def load_current_rvol_module() -> Any:
    # current_rvol imports the package-level api_keys module, which requires
    # several unrelated credentials at import time. These placeholders keep
    # this GUI focused on the Polygon and intraday stream credentials it uses.
    for name in (
        "DATABASE_PASSWORD",
        "SEC_API_KEY",
        "SEEKING_ALPHA_API_KEY",
        "SA_ACCESS_TOKEN",
        "gptdb",
    ):
        os.environ.setdefault(name, "unused-by-current-rvol-gui")

    import current_rvol  # type: ignore[import-not-found]

    return current_rvol


def resolve_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    by_lower = {column.lower(): column for column in columns}
    for candidate in candidates:
        if candidate in columns:
            return candidate
        match = by_lower.get(candidate.lower())
        if match is not None:
            return match
    return None


def load_latest_quant_ratings(engine: Engine) -> tuple[dict[str, Any], str | None]:
    frame = pd.read_sql(
        f"SELECT * FROM {mysql_identifier(DAILY_QUANT_RATING_TABLE)}",
        con=engine,
    )
    if frame.empty or "index" not in frame.columns:
        return {}, None

    date_columns: list[tuple[pd.Timestamp, str]] = []
    for column in frame.columns:
        if column == "index":
            continue
        timestamp = pd.to_datetime(str(column), errors="coerce")
        if pd.notna(timestamp):
            date_columns.append((pd.Timestamp(timestamp), column))

    if not date_columns:
        return {}, None

    _, latest_column = max(date_columns, key=lambda item: item[0])
    values = frame.set_index("index")[latest_column].dropna()
    ratings = {str(symbol).upper(): value for symbol, value in values.items()}
    return ratings, str(latest_column)


def load_latest_events(
    engine: Engine,
    preferred_table: str,
) -> tuple[dict[str, str], str | None]:
    table_candidates = [preferred_table]
    if preferred_table != FALLBACK_EVENTS_TABLE:
        table_candidates.append(FALLBACK_EVENTS_TABLE)

    for table_name in table_candidates:
        try:
            columns = existing_columns(engine, table_name)
        except Exception:
            continue

        symbol_column = resolve_column(columns, SYMBOL_COLUMN_CANDIDATES)
        date_column = resolve_column(columns, DATE_COLUMN_CANDIDATES)
        event_column = resolve_column(columns, EVENT_SUMMARY_COLUMN_CANDIDATES)
        if not symbol_column or not date_column or not event_column:
            continue

        query = text(
            f"""
            SELECT
                {mysql_identifier(symbol_column)} AS symbol,
                {mysql_identifier(date_column)} AS event_date,
                {mysql_identifier(event_column)} AS event_summary
            FROM {mysql_identifier(table_name)}
            WHERE {mysql_identifier(symbol_column)} IS NOT NULL
              AND {mysql_identifier(date_column)} IS NOT NULL
              AND {mysql_identifier(event_column)} IS NOT NULL
            """
        )
        frame = pd.read_sql(query, con=engine)
        if frame.empty:
            return {}, table_name

        frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
        frame = frame.dropna(subset=["event_date"])
        if frame.empty:
            return {}, table_name

        frame["symbol"] = frame["symbol"].astype(str).str.upper()
        latest = (
            frame.sort_values(["symbol", "event_date"])
            .groupby("symbol", as_index=False)
            .tail(1)
        )
        events = {
            str(row.symbol).upper(): str(row.event_summary)
            for row in latest.itertuples(index=False)
            if row.event_summary is not None and str(row.event_summary).strip()
        }
        return events, table_name

    return {}, None


def format_number(value: Any) -> str:
    number = pd.to_numeric(value, errors="coerce")
    if pd.notna(number):
        return f"{float(number):.2f}"
    return "" if value is None or pd.isna(value) else str(value)


def optional_float(value: Any) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return None
    return float(number)


def quant_rating_sort_key(value: Any, descending: bool) -> tuple[int, float]:
    number = optional_float(value)
    if number is None:
        return (1, 0.0)
    return (0, -number if descending else number)


class CurrentRvolGUI:
    def __init__(self, root: tk.Tk, config: GuiConfig) -> None:
        self.root = root
        self.config = config
        self.output_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.threshold_lock = threading.Lock()
        self.threshold = config.initial_threshold
        self.last_rows: list[dict[str, Any]] = []
        self.sort_column = RVOL_COLUMN
        self.sort_descending = True
        self.worker_thread: threading.Thread | None = None

        self.root.title("Current RVol")
        self.root.geometry("1200x650")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.status_var = tk.StringVar(value="Starting...")
        self.threshold_var = tk.StringVar(value=str(config.initial_threshold))
        self.pause_button_text = tk.StringVar(value="Pause Updates")

        self._build_widgets()
        self.start_worker()
        self.root.after(250, self.process_queue)

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        self.tree = ttk.Treeview(
            container,
            columns=TABLE_COLUMNS,
            show="headings",
            selectmode="browse",
        )
        widths = {
            SYMBOL_COLUMN: 100,
            RVOL_COLUMN: 100,
            QUANT_RATING_COLUMN: 120,
            RECENT_EVENT_COLUMN: 850,
        }
        anchors = {
            SYMBOL_COLUMN: tk.W,
            RVOL_COLUMN: tk.E,
            QUANT_RATING_COLUMN: tk.E,
            RECENT_EVENT_COLUMN: tk.W,
        }
        for column in TABLE_COLUMNS:
            command = (
                (lambda col=column: self.sort_by_column(col))
                if column in SORTABLE_COLUMNS
                else ""
            )
            self.tree.heading(column, text=column, command=command)
            self.tree.column(column, width=widths[column], anchor=anchors[column])

        y_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=self.tree.yview)
        x_scroll = ttk.Scrollbar(container, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

        self.tree.grid(row=0, column=0, columnspan=5, sticky="nsew")
        y_scroll.grid(row=0, column=5, sticky="ns")
        x_scroll.grid(row=1, column=0, columnspan=5, sticky="ew")

        ttk.Label(container, text="RVol Threshold").grid(
            row=2,
            column=0,
            sticky=tk.W,
            pady=(10, 0),
        )
        threshold_entry = ttk.Entry(container, textvariable=self.threshold_var, width=12)
        threshold_entry.grid(row=3, column=0, sticky="ew", padx=(0, 8))
        threshold_entry.bind("<Return>", lambda _event: self.apply_threshold())

        ttk.Button(container, text="Apply Threshold", command=self.apply_threshold).grid(
            row=3,
            column=1,
            sticky="ew",
            padx=(0, 8),
        )
        ttk.Button(
            container,
            textvariable=self.pause_button_text,
            command=self.toggle_pause,
        ).grid(row=3, column=2, sticky="ew", padx=(0, 8))
        ttk.Button(container, text="Refresh Now", command=self.refresh_last_rows).grid(
            row=3,
            column=3,
            sticky="ew",
            padx=(0, 8),
        )
        ttk.Label(container, textvariable=self.status_var).grid(
            row=3,
            column=4,
            sticky=tk.W,
        )

        for column_index in range(5):
            container.columnconfigure(column_index, weight=1)
        container.rowconfigure(0, weight=1)

    def start_worker(self) -> None:
        self.worker_thread = threading.Thread(
            target=self.worker_loop,
            name="current-rvol-gui-worker",
            daemon=True,
        )
        self.worker_thread.start()

    def current_threshold(self) -> float:
        with self.threshold_lock:
            return self.threshold

    def apply_threshold(self) -> None:
        try:
            threshold = float(self.threshold_var.get().strip())
        except ValueError:
            messagebox.showwarning("Invalid Threshold", "Enter a numeric RVol threshold.")
            return

        with self.threshold_lock:
            self.threshold = threshold
        self.status_var.set(f"Threshold set to {threshold:g}")
        self.refresh_last_rows()

    def toggle_pause(self) -> None:
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.pause_button_text.set("Pause Updates")
            self.status_var.set("Updates resumed")
        else:
            self.pause_event.set()
            self.pause_button_text.set("Resume Updates")
            self.status_var.set("Updates paused")

    def refresh_last_rows(self) -> None:
        self.render_rows(self.last_rows)

    def sort_by_column(self, column: str) -> None:
        if column not in SORTABLE_COLUMNS:
            return
        if self.sort_column == column:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_column = column
            self.sort_descending = column != SYMBOL_COLUMN
        self.render_rows(self.last_rows)

    def render_rows(self, rows: list[dict[str, Any]]) -> None:
        threshold = self.current_threshold()
        sorted_rows = [
            row
            for row in rows
            if float(row.get(RVOL_COLUMN, float("-inf"))) >= threshold
        ]
        if self.sort_column == SYMBOL_COLUMN:
            sorted_rows.sort(
                key=lambda row: str(row.get(SYMBOL_COLUMN, "")).upper(),
                reverse=self.sort_descending,
            )
        elif self.sort_column == RVOL_COLUMN:
            sorted_rows.sort(
                key=lambda row: row.get(RVOL_COLUMN, float("-inf")),
                reverse=self.sort_descending,
            )
        elif self.sort_column == QUANT_RATING_COLUMN:
            sorted_rows.sort(
                key=lambda row: quant_rating_sort_key(
                    row.get(QUANT_RATING_COLUMN),
                    self.sort_descending,
                ),
            )

        self.tree.delete(*self.tree.get_children())
        for row in sorted_rows:
            self.tree.insert(
                "",
                tk.END,
                values=(
                    row.get(SYMBOL_COLUMN, ""),
                    format_number(row.get(RVOL_COLUMN)),
                    format_number(row.get(QUANT_RATING_COLUMN)),
                    row.get(RECENT_EVENT_COLUMN, ""),
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
                    messagebox.showerror("Current RVol Failed", str(payload))
        except queue.Empty:
            pass

        if not self.stop_event.is_set():
            self.root.after(250, self.process_queue)

    def worker_loop(self) -> None:
        try:
            self._worker_loop()
        except Exception:
            self.output_queue.put(("error", traceback.format_exc()))

    def _worker_loop(self) -> None:
        self.output_queue.put(("status", "Loading symbols..."))
        symbols = load_symbols(self.config.symbols_file)
        stocks_engine = make_engine(STOCKS_DB)
        metadata_engine = make_metadata_engine(STOCKS_DB)
        stream_engine = make_engine(STREAM_DB)
        current_rvol = load_current_rvol_module()

        self.output_queue.put(("status", "Loading quant ratings and recent events..."))
        quant_ratings, latest_quant_column = load_latest_quant_ratings(metadata_engine)
        recent_events, events_table = load_latest_events(
            metadata_engine,
            self.config.events_table,
        )

        target_date = dt.datetime.now(EASTERN).date()
        self.output_queue.put(("status", "Building historical RVol profiles..."))
        profile_df = current_rvol.build_historical_profiles(
            symbols=symbols,
            target_date=target_date,
            lookback_days=self.config.lookback_days,
            calendar_buffer_days=self.config.calendar_buffer_days,
            market_open_only=self.config.market_open_only,
            process_workers=self.config.process_workers,
        )

        current_rvol.reset_temp_profile_table(
            stocks_engine,
            [],
            self.config.alter_chunk_size,
        )
        current_rvol.prepare_temp_profile_table(
            engine=stocks_engine,
            profile_df=profile_df,
            alter_chunk_size=self.config.alter_chunk_size,
            insert_chunksize=self.config.insert_chunksize,
        )
        if self.config.update_elevated_table:
            current_rvol.reset_elevated_rvol_table(stocks_engine)

        state = current_rvol.MonitorState()
        current_day = dt.datetime.now(EASTERN).date()
        day_start = dt.datetime.combine(current_day, dt.time.min)
        day_end = day_start + dt.timedelta(days=1)
        symbols = list(profile_df.columns)
        metadata = []
        if latest_quant_column:
            metadata.append(f"quant={latest_quant_column}")
        if events_table:
            metadata.append(f"events={events_table}")
        metadata_text = f" ({', '.join(metadata)})" if metadata else ""
        self.output_queue.put(
            (
                "status",
                f"Monitoring {len(symbols)} symbols for {current_day:%Y-%m-%d}{metadata_text}",
            )
        )

        while not self.stop_event.is_set():
            if self.pause_event.is_set():
                time.sleep(0.25)
                continue

            if state.last_seen_timestamp is None:
                start_ts = day_start
            else:
                start_ts = max(
                    day_start,
                    state.last_seen_timestamp.to_pydatetime()
                    - dt.timedelta(minutes=self.config.query_overlap_minutes),
                )

            rows = current_rvol.fetch_stream_rows(
                engine=stream_engine,
                symbols=symbols,
                start_ts=start_ts,
                end_ts=day_end,
                symbol_chunk_size=self.config.symbol_query_chunk_size,
            )
            latest_rvol = current_rvol.process_stream_rows(rows, profile_df, state)
            threshold = self.current_threshold()
            if self.config.update_elevated_table:
                current_rvol.update_elevated_rvol_table(
                    stocks_engine,
                    state,
                    latest_rvol,
                    threshold,
                )

            display_rows = self.display_rows(
                state.latest_rvol,
                quant_ratings,
                recent_events,
            )
            self.output_queue.put(("rows", display_rows))

            if self.config.verbose:
                self.output_queue.put(
                    (
                        "status",
                        f"{dt.datetime.now(EASTERN):%H:%M:%S}: "
                        f"updates={len(latest_rvol)} displayed={len(display_rows)}",
                    )
                )

            time.sleep(self.config.poll_interval)

    @staticmethod
    def display_rows(
        latest_rvol: dict[str, float],
        quant_ratings: dict[str, Any],
        recent_events: dict[str, str],
    ) -> list[dict[str, Any]]:
        return [
            {
                SYMBOL_COLUMN: symbol,
                RVOL_COLUMN: rvol,
                QUANT_RATING_COLUMN: quant_ratings.get(symbol, ""),
                RECENT_EVENT_COLUMN: recent_events.get(symbol, ""),
            }
            for symbol, rvol in latest_rvol.items()
        ]

    def on_close(self) -> None:
        self.stop_event.set()
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GUI wrapper for current intraday RVol output."
    )
    parser.add_argument(
        "--symbols-file",
        required=True,
        type=Path,
        help="Path to a newline-delimited text file of symbols.",
    )
    parser.add_argument("--lookback-days", type=int, default=20)
    parser.add_argument("--calendar-buffer-days", type=int, default=10)
    parser.add_argument("--rvol-threshold", type=float, default=1.5)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--query-overlap-minutes", type=int, default=2)
    parser.add_argument("--symbol-query-chunk-size", type=int, default=500)
    parser.add_argument("--alter-chunk-size", type=int, default=100)
    parser.add_argument("--insert-chunksize", type=int, default=200)
    parser.add_argument("--process-workers", type=int, default=4)
    parser.add_argument(
        "--market-open-only",
        action="store_true",
        help="Restrict historical profile imports to regular market hours.",
    )
    parser.add_argument(
        "--events-table",
        default=DEFAULT_EVENTS_TABLE,
        help=(
            "Stocks database table containing event summaries. "
            f"Falls back to {FALLBACK_EVENTS_TABLE!r} if available."
        ),
    )
    parser.add_argument(
        "--update-elevated-table",
        action="store_true",
        help="Also maintain stocks.elevated_rvol while the GUI runs.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = GuiConfig(
        symbols_file=args.symbols_file,
        lookback_days=args.lookback_days,
        calendar_buffer_days=args.calendar_buffer_days,
        initial_threshold=args.rvol_threshold,
        poll_interval=args.poll_interval,
        query_overlap_minutes=args.query_overlap_minutes,
        symbol_query_chunk_size=args.symbol_query_chunk_size,
        alter_chunk_size=args.alter_chunk_size,
        insert_chunksize=args.insert_chunksize,
        process_workers=args.process_workers,
        market_open_only=args.market_open_only,
        events_table=args.events_table,
        update_elevated_table=args.update_elevated_table,
        verbose=args.verbose,
    )
    root = tk.Tk()
    app = CurrentRvolGUI(root, config)
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
