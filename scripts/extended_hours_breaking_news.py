from __future__ import annotations

import argparse
import datetime as dt
import os
import queue
import sys
import threading
import traceback
import tkinter as tk
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Iterable
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
STOCKS_MYSQL_USER = "price_data_streamer"
NEWS_MYSQL_USER = "gptdb"
STOCKS_DB = "stocks"
NEWS_DB = "news"
STREAM_DB = "intraday_price_stream"
STREAM_TABLE = "ohlcv_1m"
RESULTS_TABLE = "extended_hours_breaking_news"
NEWS_TABLE = "stock_news"
NEWS_SYMBOL_COLUMN = "Ticker"
NEWS_DATE_COLUMN = "Date"
NEWS_HEADLINE_COLUMN = "Title"
EASTERN = ZoneInfo("US/Eastern")

SYMBOL_COLUMN = "symbol"
RVOL_COLUMN = "rvol"
HEADLINES_COLUMN = "headlines"
TABLE_COLUMNS = (SYMBOL_COLUMN, RVOL_COLUMN, HEADLINES_COLUMN)


@dataclass(frozen=True)
class GuiConfig:
    symbols_file: Path
    lookback_days: int
    calendar_buffer_days: int
    initial_threshold: float
    refresh_seconds: float
    query_overlap_minutes: int
    symbol_query_chunk_size: int
    news_query_chunk_size: int
    timespan: str
    multiplier: int
    verbose: bool


@dataclass
class MonitorState:
    cumulative_volume: dict[str, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    processed_rows: set[tuple[str, pd.Timestamp]] = field(default_factory=set)
    latest_rvol: dict[str, float] = field(default_factory=dict)
    last_seen_timestamp: pd.Timestamp | None = None


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def make_stocks_engine() -> Engine:
    password = quote_plus(require_env("intraday_stream_password"))
    url = (
        f"mysql+pymysql://{STOCKS_MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{STOCKS_DB}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def make_stream_engine() -> Engine:
    password = quote_plus(require_env("intraday_stream_password"))
    url = (
        f"mysql+pymysql://{STOCKS_MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{STREAM_DB}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def make_news_engine() -> Engine:
    password = quote_plus(require_env("gptdb"))
    url = (
        f"mysql+pymysql://{NEWS_MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{NEWS_DB}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def load_symbols(symbols_file: Path) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    with symbols_file.open("r", encoding="utf-8") as file:
        for line in file:
            symbol = line.strip().upper()
            if not symbol or symbol.startswith("#") or symbol in seen:
                continue
            symbols.append(symbol)
            seen.add(symbol)

    if not symbols:
        raise ValueError(f"No symbols found in {symbols_file}")
    return symbols


def chunked(items: list[str], chunk_size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), chunk_size):
        yield items[index:index + chunk_size]


def clear_results_table(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(f"DELETE FROM {mysql_identifier(RESULTS_TABLE)}")
        )


def _calculate_symbol_rvol_orth_profile(
    symbol_data_tuple: tuple[str, pd.DataFrame | None],
    target_date: dt.date,
    lookback_days: int,
) -> tuple[str, pd.Series | None]:
    """Build one symbol's average cumulative outside-RTH volume profile."""
    symbol, df = symbol_data_tuple
    if df is None or df.empty or "Volume" not in df.columns:
        return symbol, None

    try:
        data = df.copy()
        if not isinstance(data.index, pd.DatetimeIndex):
            data.index = pd.to_datetime(data.index)
        if data.index.tz is not None:
            data.index = data.index.tz_convert(EASTERN)
        data = data.sort_index()

        all_dates = np.asarray(data.index.date)
        historical_dates = sorted(
            day for day in np.unique(all_dates) if day < target_date
        )[-lookback_days:]
        if len(historical_dates) < lookback_days:
            return symbol, None

        historical_data = data[np.isin(all_dates, historical_dates)].copy()
        times = historical_data.index.time
        regular_hours = (
            (times >= dt.time(9, 30))
            & (times < dt.time(16, 0))
        )
        historical_data = historical_data.loc[~regular_hours]

        minute_times = pd.date_range(
            "2000-01-01 00:00:00",
            "2000-01-01 23:59:00",
            freq="1min",
        ).time
        outside_rth_times = minute_times[
            (minute_times < dt.time(9, 30))
            | (minute_times >= dt.time(16, 0))
        ]

        daily_profiles = []
        for historical_date in historical_dates:
            day_data = historical_data[
                historical_data.index.date == historical_date
            ]
            minute_volume = (
                day_data["Volume"]
                .groupby(day_data.index.time)
                .sum()
                .reindex(outside_rth_times, fill_value=0)
            )
            daily_profiles.append(minute_volume.cumsum())

        profile = pd.concat(daily_profiles, axis=1).mean(axis=1)
        profile.name = symbol
        return symbol, profile
    except Exception as exc:
        print(f"{symbol}: failed to calculate outside-RTH profile: {exc}")
        return symbol, None


def _build_historical_profiles(
    symbols: list[str],
    target_date: dt.date,
    lookback_days: int,
    calendar_buffer_days: int,
    timespan: str,
    multiplier: int,
) -> pd.DataFrame:
    # Delay this package import until the GUI is visible and the stale results
    # table has been cleared.
    from market_data.price_data_import import (  # type: ignore[import-not-found]
        intraday_import,
    )

    start_date = target_date - dt.timedelta(
        days=lookback_days + calendar_buffer_days
    )
    intraday_data = intraday_import(
        wl=symbols,
        from_date=start_date.strftime("%Y-%m-%d"),
        to_date=target_date.strftime("%Y-%m-%d"),
        timespan=timespan,
        multiplier=multiplier,
        market_open_only=False,
    )

    tasks = [(symbol, intraday_data.get(symbol)) for symbol in symbols]
    calculate_profile = partial(
        _calculate_symbol_rvol_orth_profile,
        target_date=target_date,
        lookback_days=lookback_days,
    )
    profiles: dict[str, pd.Series] = {}
    with ProcessPoolExecutor() as executor:
        for symbol, profile in executor.map(calculate_profile, tasks):
            if profile is not None and not profile.empty:
                profiles[symbol] = profile

    if not profiles:
        raise ValueError("No outside-RTH historical profiles were calculated")

    profile_df = pd.concat(profiles.values(), axis=1)
    profile_df = profile_df.reindex(
        columns=[symbol for symbol in symbols if symbol in profile_df]
    )
    profile_df.index = [
        minute.strftime("%H:%M:%S") for minute in profile_df.index
    ]
    profile_df.index.name = "timestamp"
    return profile_df.replace([np.inf, -np.inf], np.nan)


def fetch_stream_rows(
    engine: Engine,
    symbols: list[str],
    start_ts: dt.datetime,
    end_ts: dt.datetime,
    symbol_chunk_size: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    query = text(
        f"""
        SELECT Symbol, Timestamp, Volume
        FROM {mysql_identifier(STREAM_TABLE)}
        WHERE Timestamp >= :start_ts
          AND Timestamp < :end_ts
          AND (TIME(Timestamp) < '09:30:00' OR TIME(Timestamp) >= '16:00:00')
          AND Symbol IN :symbols
        ORDER BY Timestamp, Symbol
        """
    ).bindparams(bindparam("symbols", expanding=True))

    for symbol_group in chunked(symbols, symbol_chunk_size):
        frame = pd.read_sql(
            query,
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
    return rows.sort_values(["Timestamp", "Symbol"])


def latest_denominator(
    profile_df: pd.DataFrame,
    symbol: str,
    timestamp: pd.Timestamp,
) -> float | None:
    if symbol not in profile_df.columns:
        return None

    time_key = timestamp.floor("min").strftime("%H:%M:%S")
    if time_key not in profile_df.index:
        return None

    denominator = profile_df.at[time_key, symbol]
    if pd.isna(denominator) or denominator <= 0:
        return None
    return float(denominator)


def process_stream_rows(
    rows: pd.DataFrame,
    profile_df: pd.DataFrame,
    state: MonitorState,
) -> None:
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

        if pd.isna(row.Volume):
            continue

        state.cumulative_volume[symbol] += float(row.Volume)
        denominator = latest_denominator(profile_df, symbol, timestamp)
        if denominator is None:
            continue

        rvol = state.cumulative_volume[symbol] / denominator
        if np.isfinite(rvol):
            state.latest_rvol[symbol] = float(rvol)


def refresh_current_rvol(
    profile_df: pd.DataFrame,
    state: MonitorState,
    current_timestamp: dt.datetime,
) -> None:
    timestamp = pd.Timestamp(current_timestamp).floor("min")
    for symbol, cumulative_volume in state.cumulative_volume.items():
        denominator = latest_denominator(profile_df, symbol, timestamp)
        if denominator is None:
            continue
        rvol = cumulative_volume / denominator
        if np.isfinite(rvol):
            state.latest_rvol[symbol] = float(rvol)


def prune_processed_rows(
    state: MonitorState,
    query_overlap_minutes: int,
) -> None:
    if state.last_seen_timestamp is None:
        return
    cutoff = state.last_seen_timestamp - pd.Timedelta(
        minutes=query_overlap_minutes + 1
    )
    state.processed_rows = {
        key for key in state.processed_rows if key[1] >= cutoff
    }


def fetch_headlines(
    news_engine: Engine,
    symbols: list[str],
    news_date: dt.date,
    chunk_size: int,
) -> dict[str, list[str]]:
    headlines: dict[str, list[str]] = {symbol: [] for symbol in symbols}
    if not symbols:
        return headlines

    query = text(
        f"""
        SELECT
            {mysql_identifier(NEWS_SYMBOL_COLUMN)} AS symbol,
            {mysql_identifier(NEWS_HEADLINE_COLUMN)} AS headline
        FROM {mysql_identifier(NEWS_TABLE)}
        WHERE {mysql_identifier(NEWS_SYMBOL_COLUMN)} IN :symbols
          AND DATE({mysql_identifier(NEWS_DATE_COLUMN)}) = :news_date
        ORDER BY {mysql_identifier(NEWS_DATE_COLUMN)} DESC
        """
    ).bindparams(bindparam("symbols", expanding=True))

    with news_engine.connect() as connection:
        for symbol_group in chunked(symbols, chunk_size):
            rows = connection.execute(
                query,
                {"symbols": symbol_group, "news_date": news_date},
            )
            for row in rows:
                symbol = str(row.symbol).upper()
                headline = (
                    str(row.headline).strip()
                    if row.headline is not None
                    else ""
                )
                if headline and headline not in headlines.setdefault(symbol, []):
                    headlines[symbol].append(headline)
    return headlines


def build_display_rows(
    latest_rvol: dict[str, float],
    threshold: float,
    headlines: dict[str, list[str]],
) -> list[dict[str, Any]]:
    return [
        {
            SYMBOL_COLUMN: symbol,
            RVOL_COLUMN: rvol,
            HEADLINES_COLUMN: "\n".join(headlines.get(symbol, [])),
        }
        for symbol, rvol in latest_rvol.items()
        if rvol >= threshold
    ]


def replace_results_table(
    engine: Engine,
    rows: list[dict[str, Any]],
) -> None:
    insert_query = text(
        f"""
        INSERT INTO {mysql_identifier(RESULTS_TABLE)}
            (
                {mysql_identifier(SYMBOL_COLUMN)},
                {mysql_identifier(RVOL_COLUMN)},
                {mysql_identifier(HEADLINES_COLUMN)}
            )
        VALUES (:symbol, :rvol, :headlines)
        """
    )
    with engine.begin() as connection:
        connection.execute(
            text(f"DELETE FROM {mysql_identifier(RESULTS_TABLE)}")
        )
        if rows:
            connection.execute(insert_query, rows)


class ExtendedHoursBreakingNewsGUI:
    def __init__(self, root: tk.Tk, config: GuiConfig) -> None:
        self.root = root
        self.config = config
        self.output_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.refresh_event = threading.Event()
        self.pause_event = threading.Event()
        self.threshold_lock = threading.Lock()
        self.threshold = config.initial_threshold
        self.last_rows: list[dict[str, Any]] = []
        self.sort_column = RVOL_COLUMN
        self.sort_descending = True

        self.root.title("Extended-Hours Breaking News")
        self.root.geometry("1400x760")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.status_var = tk.StringVar(value="Starting...")
        self.threshold_var = tk.StringVar(value=str(config.initial_threshold))
        self.count_var = tk.StringVar(value="Symbols: 0")
        self.pause_button_text = tk.StringVar(value="Pause Updates")

        self._build_widgets()
        threading.Thread(
            target=self.worker_loop,
            name="extended-hours-breaking-news-worker",
            daemon=True,
        ).start()
        self.root.after(250, self.process_queue)

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        style = ttk.Style(self.root)
        style.configure("BreakingNews.Treeview", rowheight=68)
        self.tree = ttk.Treeview(
            container,
            columns=TABLE_COLUMNS,
            show="headings",
            selectmode="browse",
            style="BreakingNews.Treeview",
        )
        widths = {
            SYMBOL_COLUMN: 110,
            RVOL_COLUMN: 100,
            HEADLINES_COLUMN: 1120,
        }
        anchors = {
            SYMBOL_COLUMN: tk.W,
            RVOL_COLUMN: tk.E,
            HEADLINES_COLUMN: tk.W,
        }
        for column in TABLE_COLUMNS:
            command = (
                (lambda selected=column: self.sort_by_column(selected))
                if column in {SYMBOL_COLUMN, RVOL_COLUMN}
                else ""
            )
            self.tree.heading(
                column,
                text=column.title(),
                command=command,
            )
            self.tree.column(
                column,
                width=widths[column],
                minwidth=widths[column],
                anchor=anchors[column],
                stretch=(column == HEADLINES_COLUMN),
            )

        vertical_scroll = ttk.Scrollbar(
            container,
            orient=tk.VERTICAL,
            command=self.tree.yview,
        )
        horizontal_scroll = ttk.Scrollbar(
            container,
            orient=tk.HORIZONTAL,
            command=self.tree.xview,
        )
        self.tree.configure(
            yscrollcommand=vertical_scroll.set,
            xscrollcommand=horizontal_scroll.set,
        )
        self.tree.grid(row=0, column=0, sticky="nsew")
        vertical_scroll.grid(row=0, column=1, sticky="ns")
        horizontal_scroll.grid(row=1, column=0, sticky="ew")

        controls = ttk.Frame(container)
        controls.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        controls.columnconfigure(5, weight=1)

        ttk.Label(controls, text="RVol Threshold").grid(row=0, column=0)
        threshold_entry = ttk.Entry(
            controls,
            textvariable=self.threshold_var,
            width=10,
        )
        threshold_entry.grid(row=0, column=1, padx=(8, 8))
        threshold_entry.bind("<Return>", lambda _event: self.apply_threshold())
        ttk.Button(
            controls,
            text="Apply Threshold",
            command=self.apply_threshold,
        ).grid(row=0, column=2, padx=(0, 8))
        ttk.Button(
            controls,
            text="Refresh Now",
            command=self.request_refresh,
        ).grid(row=0, column=3, padx=(0, 8))
        ttk.Button(
            controls,
            textvariable=self.pause_button_text,
            command=self.toggle_pause,
        ).grid(row=0, column=4)
        ttk.Label(
            controls,
            textvariable=self.count_var,
        ).grid(row=0, column=5, sticky="e")

        status_label = ttk.Label(
            container,
            textvariable=self.status_var,
            justify=tk.LEFT,
        )
        status_label.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        container.bind(
            "<Configure>",
            lambda event: status_label.configure(wraplength=max(event.width, 1)),
        )

        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

    def current_threshold(self) -> float:
        with self.threshold_lock:
            return self.threshold

    def apply_threshold(self) -> None:
        try:
            threshold = float(self.threshold_var.get().strip())
        except ValueError:
            messagebox.showwarning(
                "Invalid Threshold",
                "Enter a numeric RVol threshold.",
            )
            return
        if threshold < 0:
            messagebox.showwarning(
                "Invalid Threshold",
                "RVol threshold cannot be negative.",
            )
            return

        with self.threshold_lock:
            self.threshold = threshold
        if self.pause_event.is_set():
            self.status_var.set(
                f"Threshold set to {threshold:g}; updates remain paused."
            )
        else:
            self.status_var.set(f"Threshold set to {threshold:g}; refreshing...")
            self.refresh_event.set()

    def request_refresh(self) -> None:
        if self.pause_event.is_set():
            self.status_var.set("Updates are paused. Resume to refresh.")
            return
        self.status_var.set("Refresh requested...")
        self.refresh_event.set()

    def toggle_pause(self) -> None:
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.pause_button_text.set("Pause Updates")
            self.status_var.set("Updates resumed; refreshing...")
        else:
            self.pause_event.set()
            self.pause_button_text.set("Resume Updates")
            self.status_var.set(
                "Updates paused. An update already in progress will finish."
            )
        self.refresh_event.set()

    def sort_by_column(self, column: str) -> None:
        if self.sort_column == column:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_column = column
            self.sort_descending = column == RVOL_COLUMN
        self.render_rows(self.last_rows)

    def render_rows(self, rows: list[dict[str, Any]]) -> None:
        sorted_rows = sorted(
            rows,
            key=lambda row: row.get(self.sort_column, ""),
            reverse=self.sort_descending,
        )
        self.tree.delete(*self.tree.get_children())
        for row in sorted_rows:
            self.tree.insert(
                "",
                tk.END,
                values=(
                    row[SYMBOL_COLUMN],
                    f"{float(row[RVOL_COLUMN]):.2f}",
                    row.get(HEADLINES_COLUMN, ""),
                ),
            )
        self.count_var.set(f"Symbols: {len(rows)}")

    def process_queue(self) -> None:
        try:
            while True:
                message_type, payload = self.output_queue.get_nowait()
                if message_type == "status":
                    self.status_var.set(str(payload))
                elif message_type == "rows":
                    self.last_rows = payload
                    self.render_rows(payload)
                elif message_type == "error":
                    self.status_var.set("Worker failed")
                    messagebox.showerror(
                        "Extended-Hours RVol Failed",
                        str(payload),
                    )
        except queue.Empty:
            pass

        if not self.stop_event.is_set():
            self.root.after(250, self.process_queue)

    def worker_loop(self) -> None:
        stocks_engine: Engine | None = None
        news_engine: Engine | None = None
        stream_engine: Engine | None = None
        try:
            self.output_queue.put(("status", "Loading symbols..."))
            symbols = load_symbols(self.config.symbols_file)
            stocks_engine = make_stocks_engine()
            news_engine = make_news_engine()
            stream_engine = make_stream_engine()

            self.output_queue.put(
                ("status", f"Clearing stocks.{RESULTS_TABLE}...")
            )
            clear_results_table(stocks_engine)

            while not self.stop_event.is_set():
                target_date = dt.datetime.now(EASTERN).date()
                self.output_queue.put(
                    (
                        "status",
                        f"Building {self.config.lookback_days}-day outside-RTH "
                        f"profiles for {len(symbols)} symbols...",
                    )
                )
                profile_df = _build_historical_profiles(
                    symbols=symbols,
                    target_date=target_date,
                    lookback_days=self.config.lookback_days,
                    calendar_buffer_days=self.config.calendar_buffer_days,
                    timespan=self.config.timespan,
                    multiplier=self.config.multiplier,
                )
                monitored_symbols = list(profile_df.columns)
                state = MonitorState()
                day_start = dt.datetime.combine(target_date, dt.time.min)
                day_end = day_start + dt.timedelta(days=1)
                query_cursor = day_start
                bootstrapped = False

                # The historical raw bars are local to _build_historical_profiles
                # and are released before this incremental monitoring loop.
                while (
                    not self.stop_event.is_set()
                    and dt.datetime.now(EASTERN).date() == target_date
                ):
                    if self.pause_event.is_set():
                        self.output_queue.put(
                            (
                                "status",
                                "Updates paused. Click Resume Updates to continue.",
                            )
                        )
                        while (
                            self.pause_event.is_set()
                            and not self.stop_event.is_set()
                        ):
                            self.refresh_event.wait(0.5)
                            self.refresh_event.clear()
                        continue

                    now_eastern = dt.datetime.now(EASTERN)
                    now_time = now_eastern.time().replace(tzinfo=None)
                    if (
                        bootstrapped
                        and dt.time(9, 30) <= now_time < dt.time(16, 0)
                    ):
                        postmarket_start = dt.datetime.combine(
                            target_date,
                            dt.time(16, 0),
                            tzinfo=EASTERN,
                        )
                        wait_seconds = max(
                            1.0,
                            (postmarket_start - now_eastern).total_seconds(),
                        )
                        self.output_queue.put(
                            (
                                "status",
                                "Regular trading hours: stream retrieval paused "
                                "until 16:00 ET.",
                            )
                        )
                        self.refresh_event.clear()
                        self.refresh_event.wait(wait_seconds)
                        continue

                    self.refresh_event.clear()
                    query_end = min(
                        now_eastern.replace(tzinfo=None),
                        day_end,
                    )
                    query_start = max(
                        day_start,
                        query_cursor - dt.timedelta(
                            minutes=self.config.query_overlap_minutes
                        ),
                    )
                    rows = fetch_stream_rows(
                        engine=stream_engine,
                        symbols=monitored_symbols,
                        start_ts=query_start,
                        end_ts=query_end,
                        symbol_chunk_size=self.config.symbol_query_chunk_size,
                    )
                    process_stream_rows(rows, profile_df, state)
                    refresh_current_rvol(profile_df, state, query_end)
                    prune_processed_rows(
                        state,
                        self.config.query_overlap_minutes,
                    )
                    query_cursor = query_end
                    bootstrapped = True

                    threshold = self.current_threshold()
                    qualifying_symbols = sorted(
                        symbol
                        for symbol, value in state.latest_rvol.items()
                        if value >= threshold
                    )
                    headlines = fetch_headlines(
                        news_engine=news_engine,
                        symbols=qualifying_symbols,
                        news_date=target_date,
                        chunk_size=self.config.news_query_chunk_size,
                    )
                    display_rows = build_display_rows(
                        latest_rvol=state.latest_rvol,
                        threshold=threshold,
                        headlines=headlines,
                    )
                    replace_results_table(stocks_engine, display_rows)
                    self.output_queue.put(("rows", display_rows))

                    status = (
                        f"{now_eastern:%H:%M:%S} ET: {len(display_rows)} symbols "
                        f"at or above RVol {threshold:g}. Next stream check in "
                        f"{self.config.refresh_seconds:g} seconds."
                    )
                    if self.config.verbose:
                        status += (
                            f" Stream rows={len(rows)}; "
                            f"tracked={len(state.latest_rvol)}."
                        )
                    self.output_queue.put(("status", status))
                    self.refresh_event.wait(self.config.refresh_seconds)

                if not self.stop_event.is_set():
                    self.output_queue.put(
                        ("status", "New trading day detected; rebuilding profiles...")
                    )
                    clear_results_table(stocks_engine)
        except Exception:
            self.output_queue.put(("error", traceback.format_exc()))
        finally:
            if stocks_engine is not None:
                stocks_engine.dispose()
            if news_engine is not None:
                news_engine.dispose()
            if stream_engine is not None:
                stream_engine.dispose()

    def on_close(self) -> None:
        self.stop_event.set()
        self.refresh_event.set()
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Display elevated outside-regular-hours RVol and today's "
            "news headlines."
        )
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
    parser.add_argument("--refresh-seconds", type=float, default=60.0)
    parser.add_argument("--query-overlap-minutes", type=int, default=2)
    parser.add_argument("--symbol-query-chunk-size", type=int, default=500)
    parser.add_argument("--news-query-chunk-size", type=int, default=500)
    parser.add_argument("--timespan", default="minute")
    parser.add_argument("--multiplier", type=int, default=1)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.lookback_days <= 0:
        raise ValueError("--lookback-days must be greater than zero")
    if args.calendar_buffer_days <= 0:
        raise ValueError("--calendar-buffer-days must be greater than zero")
    if args.rvol_threshold < 0:
        raise ValueError("--rvol-threshold cannot be negative")
    if args.refresh_seconds <= 0:
        raise ValueError("--refresh-seconds must be greater than zero")
    if args.query_overlap_minutes < 0:
        raise ValueError("--query-overlap-minutes cannot be negative")
    if args.symbol_query_chunk_size <= 0:
        raise ValueError("--symbol-query-chunk-size must be greater than zero")
    if args.news_query_chunk_size <= 0:
        raise ValueError("--news-query-chunk-size must be greater than zero")
    if args.multiplier <= 0:
        raise ValueError("--multiplier must be greater than zero")

    config = GuiConfig(
        symbols_file=args.symbols_file,
        lookback_days=args.lookback_days,
        calendar_buffer_days=args.calendar_buffer_days,
        initial_threshold=args.rvol_threshold,
        refresh_seconds=args.refresh_seconds,
        query_overlap_minutes=args.query_overlap_minutes,
        symbol_query_chunk_size=args.symbol_query_chunk_size,
        news_query_chunk_size=args.news_query_chunk_size,
        timespan=args.timespan,
        multiplier=args.multiplier,
        verbose=args.verbose,
    )
    root = tk.Tk()
    _app = ExtendedHoursBreakingNewsGUI(root, config)
    root.mainloop()


if __name__ == "__main__":
    main()
