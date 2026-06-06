from __future__ import annotations

"""
Intraday Hi/Lo swing monitor.

Settings:
    Timeframe min:
        Number of 1-minute OHLCV bars to combine before calculating swings.
        The default is 3, so bars are resampled into 3-minute OHLCV candles.
    Poll sec:
        Seconds to wait between refresh cycles. Each cycle reloads the current
        elevated-RVol symbol list, fetches today's OHLCV rows, recalculates
        swings, and updates the GUI.
    Min candidate bars:
        Minimum number of completed resampled bars that must print after the
        latest Hi1/Lo1 before it can be treated as a potential Hi2/Lo2.
    Min reversal pct:
        Minimum percentage move away from the latest Hi1/Lo1 before it can be
        treated as a potential structural swing. Use 0.005 for 0.5%.

States:
    No data yet:
        The symbol is in stocks.elevated_rvol, but no OHLCV rows have been
        found for the current session.
    No completed bars:
        OHLCV rows exist, but there is not yet a completed resampled bar.
    No swing yet:
        There are completed bars, but no Hi1/Lo1 has been detected.
    Candidate Hi1 / Candidate Lo1:
        A local high/low has printed at the first swing level, but it has not
        met the candidate reversal rules for a possible structural swing.
    Potential Hi2 forming / Potential Lo2 forming:
        The latest Hi1/Lo1 is newer than the latest Hi2/Lo2, price has moved
        away from it, and the candidate bar/reversal settings are satisfied.
    Hi2 provisional / Lo2 provisional:
        A structural swing exists, but because this is real-time data the most
        recent structural swing can still be invalidated by later price action.
    Confirmed Hi2 provisional / Confirmed Lo2 provisional:
        A new Hi2/Lo2 appeared during the latest refresh. It is an alert event,
        but it remains provisional until later market structure confirms it.
    Confirmed Hi2 confirmed / Confirmed Lo2 confirmed:
        A later structural swing has appeared after the prior structural swing,
        making the prior swing less likely to repaint.
    Invalidated structural high / Invalidated structural low:
        A previously tracked Hi2/Lo2 disappeared after recalculation, usually
        because later Hi1/Lo1 price action invalidated the latest structural
        swing.
"""

import argparse
import datetime as dt
import math
import numbers
import os
import queue
import sys
import threading
import time
import traceback
import tkinter as tk
from dataclasses import dataclass, field
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Iterable
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from scipy.signal import find_peaks
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
STREAM_TABLE = "ohlcv_1m"
EASTERN = ZoneInfo("America/New_York")

DEFAULT_TIMEFRAME_MINUTES = 3
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_SYMBOL_CHUNK_SIZE = 250
DEFAULT_MIN_CANDIDATE_BARS = 1
DEFAULT_MIN_REVERSAL_PCT = 0.0
DEFAULT_MAX_SWING_LEVEL = 2
REGULAR_SESSION_START = dt.time(9, 30)
REGULAR_SESSION_END = dt.time(16, 0)

DISPLAY_COLUMNS = (
    "symbol",
    "rvol",
    "state",
    "event",
    "last_close",
    "last_bar",
    "hi1",
    "hi1_time",
    "lo1",
    "lo1_time",
    "hi2",
    "hi2_time",
    "lo2",
    "lo2_time",
)
SORTABLE_COLUMNS = {"symbol", "rvol", "state", "event", "last_close", "last_bar"}
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
    timeframe_minutes: int
    poll_interval: float
    symbol_chunk_size: int
    min_candidate_bars: int
    min_reversal_pct: float
    regular_session_only: bool
    max_swing_level: int


@dataclass
class SymbolSwingState:
    hi2_points: dict[pd.Timestamp, float] = field(default_factory=dict)
    lo2_points: dict[pd.Timestamp, float] = field(default_factory=dict)
    latest_confirmed_type: str | None = None
    latest_confirmed_time: pd.Timestamp | None = None
    latest_confirmed_value: float | None = None
    last_state: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor intraday Hi/Lo swing states for symbols currently listed "
            "in stocks.elevated_rvol."
        )
    )
    parser.add_argument("--timeframe-minutes", type=int, default=DEFAULT_TIMEFRAME_MINUTES)
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL_SECONDS)
    parser.add_argument("--symbol-query-chunk-size", type=int, default=DEFAULT_SYMBOL_CHUNK_SIZE)
    parser.add_argument("--min-candidate-bars", type=int, default=DEFAULT_MIN_CANDIDATE_BARS)
    parser.add_argument("--min-reversal-pct", type=float, default=DEFAULT_MIN_REVERSAL_PCT)
    parser.add_argument("--max-swing-level", type=int, default=DEFAULT_MAX_SWING_LEVEL)
    parser.add_argument(
        "--include-extended-hours",
        action="store_true",
        help="Include premarket/after-hours rows from the current date.",
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


def normalize_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(EASTERN).tz_localize(None)
    return timestamp


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
    if value is None or pd.isna(value):
        return None
    if isinstance(value, numbers.Number):
        number = float(value)
        return number if math.isfinite(number) else None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def fetch_elevated_symbols(engine: Engine) -> pd.DataFrame:
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
    frame["symbol"] = frame[symbol_col].astype(str).str.upper()

    rvol_col = resolve_column(frame.columns, RVOL_COLUMN_CANDIDATES)
    if rvol_col is None:
        frame["rvol"] = np.nan
    else:
        frame["rvol"] = frame[rvol_col].map(numeric_or_none)

    return frame[["symbol", "rvol"]].drop_duplicates("symbol")


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
        SELECT Symbol, Timestamp, Open, High, Low, Close, Volume, VWAP
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
    return rows.sort_values(["Symbol", "Timestamp"])


def empty_ohlcv_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["Symbol", "Timestamp", "Open", "High", "Low", "Close", "Volume", "VWAP"]
    )


def resample_symbol_ohlcv(df: pd.DataFrame, timeframe_minutes: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume", "VWAP"])

    data = df.copy()
    data["Timestamp"] = pd.to_datetime(data["Timestamp"])
    data = data.sort_values("Timestamp").set_index("Timestamp")
    rule = f"{timeframe_minutes}min"

    resampled = data.resample(
        rule,
        origin="start_day",
        offset=f"{REGULAR_SESSION_START.hour}h{REGULAR_SESSION_START.minute}min",
        label="right",
        closed="left",
    ).agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
            "VWAP": "last",
        }
    )
    return resampled.dropna(subset=["Open", "High", "Low", "Close"])


def enforce_hilo_alternation(points: pd.DataFrame) -> pd.DataFrame:
    if points.empty:
        return points

    cleaned = points.sort_index().copy()
    for _ in range(10):
        previous = cleaned.copy()
        rows: list[pd.Series] = []

        for _, row in cleaned.iterrows():
            current = row.copy()
            if not rows:
                rows.append(current)
                continue

            prior = rows[-1]
            if current["kind"] == prior["kind"]:
                if current["kind"] == "high":
                    if current["price"] > prior["price"]:
                        rows[-1] = current
                elif current["price"] < prior["price"]:
                    rows[-1] = current
                continue

            if prior["kind"] == "high" and current["kind"] == "low":
                if current["price"] >= prior["price"]:
                    continue
            elif prior["kind"] == "low" and current["kind"] == "high":
                if current["price"] <= prior["price"]:
                    continue

            rows.append(current)

        cleaned = pd.DataFrame(rows)
        if not cleaned.empty:
            cleaned.index = [row.name for row in rows]
            cleaned = cleaned.sort_index()

        if cleaned.equals(previous):
            break

    return cleaned


def calculate_swings(
    df: pd.DataFrame,
    max_level: int = DEFAULT_MAX_SWING_LEVEL,
) -> pd.DataFrame:
    result = df.copy()
    for level in range(1, max_level + 1):
        result[f"Hi{level}"] = np.nan
        result[f"Lo{level}"] = np.nan

    if len(result) < 3:
        return result

    reduction = result[["Open", "High", "Low", "Close"]].copy()
    reduction["avg_px"] = reduction[["High", "Low", "Close"]].mean(axis=1).round(2)

    highs = reduction["avg_px"].to_numpy()
    lows = -reduction["avg_px"].to_numpy()

    for level in range(1, max_level + 1):
        if len(reduction) < 3:
            break

        high_ix = find_peaks(highs, distance=1, width=0)[0]
        low_ix = find_peaks(lows, distance=1, width=0)[0]
        if len(high_ix) == 0 and len(low_ix) == 0:
            break

        high_points = pd.DataFrame(
            {
                "kind": "high",
                "price": reduction.iloc[high_ix]["High"],
            },
            index=reduction.iloc[high_ix].index,
        )
        low_points = pd.DataFrame(
            {
                "kind": "low",
                "price": reduction.iloc[low_ix]["Low"],
            },
            index=reduction.iloc[low_ix].index,
        )
        points = pd.concat([high_points, low_points]).sort_index()
        points = enforce_hilo_alternation(points)
        if points.empty:
            break

        hi_col = f"Hi{level}"
        lo_col = f"Lo{level}"
        high_mask = points["kind"] == "high"
        low_mask = points["kind"] == "low"
        result.loc[points.index[high_mask], hi_col] = points.loc[high_mask, "price"]
        result.loc[points.index[low_mask], lo_col] = points.loc[low_mask, "price"]

        reduction = reduction.loc[points.index].copy()
        if len(reduction) < 3:
            break

        reduction[hi_col] = result.loc[reduction.index, hi_col]
        reduction[lo_col] = result.loc[reduction.index, lo_col]
        reduction[hi_col] = reduction[hi_col].ffill().bfill()
        reduction[lo_col] = reduction[lo_col].ffill().bfill()
        highs = reduction[hi_col].to_numpy()
        lows = -reduction[lo_col].to_numpy()

    cleanup_latest_structural_swing(result)
    return result


def cleanup_latest_structural_swing(df: pd.DataFrame) -> None:
    if not {"Hi1", "Lo1", "Hi2", "Lo2"}.issubset(df.columns):
        return

    hi2 = df["Hi2"].dropna()
    lo2 = df["Lo2"].dropna()
    if hi2.empty or lo2.empty:
        return

    latest_hi_time = hi2.index[-1]
    latest_lo_time = lo2.index[-1]
    latest_hi = float(hi2.iloc[-1])
    latest_lo = float(lo2.iloc[-1])

    if latest_hi_time > latest_lo_time:
        later_hi1 = df.loc[latest_hi_time:, "Hi1"].dropna()
        if (not later_hi1.empty and later_hi1.max() > latest_hi) or latest_hi < latest_lo:
            df.loc[latest_hi_time, "Hi2"] = np.nan
    elif latest_lo_time > latest_hi_time:
        later_lo1 = df.loc[latest_lo_time:, "Lo1"].dropna()
        if (not later_lo1.empty and later_lo1.min() < latest_lo) or latest_hi < latest_lo:
            df.loc[latest_lo_time, "Lo2"] = np.nan


def latest_point(df: pd.DataFrame, column: str) -> tuple[pd.Timestamp | None, float | None]:
    if column not in df.columns:
        return None, None
    series = df[column].dropna()
    if series.empty:
        return None, None
    return normalize_timestamp(series.index[-1]), float(series.iloc[-1])


def format_time(value: pd.Timestamp | None) -> str:
    if value is None:
        return ""
    return value.strftime("%H:%M")


def format_float(value: Any, decimals: int = 2) -> str:
    number = numeric_or_none(value)
    if number is None:
        return ""
    return f"{number:.{decimals}f}"


def classify_symbol_state(
    symbol: str,
    swings: pd.DataFrame,
    prior_state: SymbolSwingState,
    min_candidate_bars: int,
    min_reversal_pct: float,
) -> tuple[dict[str, Any], SymbolSwingState, str | None]:
    hi1_time, hi1_value = latest_point(swings, "Hi1")
    lo1_time, lo1_value = latest_point(swings, "Lo1")
    hi2_time, hi2_value = latest_point(swings, "Hi2")
    lo2_time, lo2_value = latest_point(swings, "Lo2")

    current_hi2 = {normalize_timestamp(ix): float(value) for ix, value in swings["Hi2"].dropna().items()}
    current_lo2 = {normalize_timestamp(ix): float(value) for ix, value in swings["Lo2"].dropna().items()}
    last_close = float(swings["Close"].iloc[-1]) if not swings.empty else None
    last_bar = normalize_timestamp(swings.index[-1]) if not swings.empty else None

    new_hi2 = sorted(set(current_hi2) - set(prior_state.hi2_points))
    new_lo2 = sorted(set(current_lo2) - set(prior_state.lo2_points))
    removed_hi2 = sorted(set(prior_state.hi2_points) - set(current_hi2))
    removed_lo2 = sorted(set(prior_state.lo2_points) - set(current_lo2))

    event: str | None = None
    state = "No swing yet"
    latest_confirmed_type = prior_state.latest_confirmed_type
    latest_confirmed_time = prior_state.latest_confirmed_time
    latest_confirmed_value = prior_state.latest_confirmed_value

    if new_hi2:
        latest_confirmed_time = new_hi2[-1]
        latest_confirmed_value = current_hi2[latest_confirmed_time]
        latest_confirmed_type = "Hi2"
        event = f"Confirmed Hi2 {format_float(latest_confirmed_value)} at {format_time(latest_confirmed_time)}"
        state = "Confirmed Hi2 provisional"
    elif new_lo2:
        latest_confirmed_time = new_lo2[-1]
        latest_confirmed_value = current_lo2[latest_confirmed_time]
        latest_confirmed_type = "Lo2"
        event = f"Confirmed Lo2 {format_float(latest_confirmed_value)} at {format_time(latest_confirmed_time)}"
        state = "Confirmed Lo2 provisional"
    elif removed_hi2:
        event = f"Invalidated Hi2 at {format_time(removed_hi2[-1])}"
        state = "Invalidated structural high"
    elif removed_lo2:
        event = f"Invalidated Lo2 at {format_time(removed_lo2[-1])}"
        state = "Invalidated structural low"

    latest_rt_kind: str | None = None
    latest_rt_time: pd.Timestamp | None = None
    latest_rt_value: float | None = None
    if hi1_time is not None and (lo1_time is None or hi1_time > lo1_time):
        latest_rt_kind, latest_rt_time, latest_rt_value = "Hi1", hi1_time, hi1_value
    elif lo1_time is not None:
        latest_rt_kind, latest_rt_time, latest_rt_value = "Lo1", lo1_time, lo1_value

    if event is None and latest_rt_kind and latest_rt_time and latest_rt_value is not None:
        latest_structural_time = max(
            [time for time in (hi2_time, lo2_time) if time is not None],
            default=None,
        )
        is_newer_than_structural = (
            latest_structural_time is None or latest_rt_time > latest_structural_time
        )
        bars_since = len(swings.loc[latest_rt_time:]) - 1
        reversal_pct = 0.0
        if last_close is not None and latest_rt_value:
            reversal_pct = abs(last_close / latest_rt_value - 1.0)

        moved_away = (
            bars_since >= min_candidate_bars
            and reversal_pct >= min_reversal_pct
            and (
                (latest_rt_kind == "Hi1" and last_close is not None and last_close < latest_rt_value)
                or (latest_rt_kind == "Lo1" and last_close is not None and last_close > latest_rt_value)
            )
        )
        if is_newer_than_structural and moved_away:
            state = (
                "Potential Hi2 forming"
                if latest_rt_kind == "Hi1"
                else "Potential Lo2 forming"
            )
            event = f"{latest_rt_kind} candidate at {format_time(latest_rt_time)}"
        elif latest_rt_kind == "Hi1":
            state = "Candidate Hi1"
        elif latest_rt_kind == "Lo1":
            state = "Candidate Lo1"

    if event is None and hi2_time is not None and lo2_time is not None:
        if hi2_time > lo2_time:
            state = "Hi2 provisional"
            latest_confirmed_type, latest_confirmed_time, latest_confirmed_value = "Hi2", hi2_time, hi2_value
        else:
            state = "Lo2 provisional"
            latest_confirmed_type, latest_confirmed_time, latest_confirmed_value = "Lo2", lo2_time, lo2_value
    elif event is None and hi2_time is not None:
        state = "Hi2 provisional"
        latest_confirmed_type, latest_confirmed_time, latest_confirmed_value = "Hi2", hi2_time, hi2_value
    elif event is None and lo2_time is not None:
        state = "Lo2 provisional"
        latest_confirmed_type, latest_confirmed_time, latest_confirmed_value = "Lo2", lo2_time, lo2_value

    if (
        latest_confirmed_time is not None
        and prior_state.latest_confirmed_time is not None
        and latest_confirmed_time > prior_state.latest_confirmed_time
    ):
        state = state.replace("provisional", "confirmed")

    new_state = SymbolSwingState(
        hi2_points=current_hi2,
        lo2_points=current_lo2,
        latest_confirmed_type=latest_confirmed_type,
        latest_confirmed_time=latest_confirmed_time,
        latest_confirmed_value=latest_confirmed_value,
        last_state=state,
    )

    row = {
        "symbol": symbol,
        "state": state,
        "event": event or "",
        "last_close": last_close,
        "last_bar": last_bar,
        "hi1": hi1_value,
        "hi1_time": hi1_time,
        "lo1": lo1_value,
        "lo1_time": lo1_time,
        "hi2": hi2_value,
        "hi2_time": hi2_time,
        "lo2": lo2_value,
        "lo2_time": lo2_time,
    }
    return row, new_state, event


class IntradayHiloGUI:
    def __init__(self, root: tk.Tk, config: MonitorConfig):
        self.root = root
        self.config = config
        self.output_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.config_lock = threading.Lock()
        self.symbol_states: dict[str, SymbolSwingState] = {}
        self.last_rows: list[dict[str, Any]] = []
        self.sort_column = "symbol"
        self.sort_descending = False
        self.worker_thread: threading.Thread | None = None

        self.root.title("Intraday Hi/Lo Monitor")
        self.root.geometry("1450x750")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.status_var = tk.StringVar(value="Starting...")
        self.pause_button_text = tk.StringVar(value="Pause Updates")
        self.timeframe_var = tk.StringVar(value=str(config.timeframe_minutes))
        self.poll_interval_var = tk.StringVar(value=str(config.poll_interval))
        self.min_candidate_bars_var = tk.StringVar(value=str(config.min_candidate_bars))
        self.min_reversal_pct_var = tk.StringVar(value=str(config.min_reversal_pct))
        self.state_filter_var = tk.StringVar(value="All states")
        self.event_filter_var = tk.StringVar(value="All events")

        self._build_widgets()
        self.start_worker()
        self.root.after(250, self.process_queue)

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            container,
            columns=DISPLAY_COLUMNS,
            show="headings",
            selectmode="browse",
        )
        widths = {
            "symbol": 85,
            "rvol": 80,
            "state": 180,
            "event": 230,
            "last_close": 90,
            "last_bar": 80,
            "hi1": 85,
            "hi1_time": 80,
            "lo1": 85,
            "lo1_time": 80,
            "hi2": 85,
            "hi2_time": 80,
            "lo2": 85,
            "lo2_time": 80,
        }
        for column in DISPLAY_COLUMNS:
            self.tree.heading(
                column,
                text=column,
                command=lambda c=column: self.sort_by_column(c),
            )
            self.tree.column(column, width=widths[column], anchor=tk.W)

        y_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=self.tree.yview)
        x_scroll = ttk.Scrollbar(container, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")

        controls = ttk.Frame(container)
        controls.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(10, 0))

        ttk.Label(controls, text="Timeframe min").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(controls, textvariable=self.timeframe_var, width=8).grid(
            row=1, column=0, sticky="ew", padx=(0, 8)
        )
        ttk.Label(controls, text="Poll sec").grid(row=0, column=1, sticky=tk.W)
        ttk.Entry(controls, textvariable=self.poll_interval_var, width=8).grid(
            row=1, column=1, sticky="ew", padx=(0, 8)
        )
        ttk.Label(controls, text="Min candidate bars").grid(row=0, column=2, sticky=tk.W)
        ttk.Entry(controls, textvariable=self.min_candidate_bars_var, width=8).grid(
            row=1, column=2, sticky="ew", padx=(0, 8)
        )
        ttk.Label(controls, text="Min reversal pct").grid(row=0, column=3, sticky=tk.W)
        ttk.Entry(controls, textvariable=self.min_reversal_pct_var, width=10).grid(
            row=1, column=3, sticky="ew", padx=(0, 8)
        )
        ttk.Button(controls, text="Apply Settings", command=self.apply_settings).grid(
            row=1, column=4, sticky="ew", padx=(0, 8)
        )
        ttk.Button(
            controls,
            textvariable=self.pause_button_text,
            command=self.toggle_pause,
        ).grid(row=1, column=5, sticky="ew", padx=(0, 8))
        ttk.Button(controls, text="Refresh Now", command=self.refresh_last_rows).grid(
            row=1, column=6, sticky="ew", padx=(0, 8)
        )
        ttk.Label(controls, textvariable=self.status_var).grid(
            row=1, column=7, sticky=tk.W
        )

        filters = ttk.Frame(container)
        filters.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        ttk.Label(filters, text="State filter").grid(row=0, column=0, sticky=tk.W)
        self.state_filter_combo = ttk.Combobox(
            filters,
            textvariable=self.state_filter_var,
            values=("All states",),
            state="readonly",
            width=28,
        )
        self.state_filter_combo.grid(row=1, column=0, sticky=tk.W, padx=(0, 8))
        self.state_filter_combo.bind("<<ComboboxSelected>>", lambda _event: self.refresh_last_rows())

        ttk.Label(filters, text="Event filter").grid(row=0, column=1, sticky=tk.W)
        self.event_filter_combo = ttk.Combobox(
            filters,
            textvariable=self.event_filter_var,
            values=("All events",),
            state="readonly",
            width=36,
        )
        self.event_filter_combo.grid(row=1, column=1, sticky=tk.W, padx=(0, 8))
        self.event_filter_combo.bind("<<ComboboxSelected>>", lambda _event: self.refresh_last_rows())

        ttk.Button(filters, text="Clear Filters", command=self.clear_filters).grid(
            row=1, column=2, sticky=tk.W
        )

        ttk.Label(container, text="Alerts").grid(row=4, column=0, sticky=tk.W, pady=(10, 0))
        self.alerts = tk.Listbox(container, height=7)
        self.alerts.grid(row=5, column=0, columnspan=2, sticky="ew")

    def start_worker(self) -> None:
        self.worker_thread = threading.Thread(target=self.worker_loop, daemon=True)
        self.worker_thread.start()

    def current_config(self) -> MonitorConfig:
        with self.config_lock:
            return self.config

    def apply_settings(self) -> None:
        try:
            timeframe = int(self.timeframe_var.get().strip())
            poll_interval = float(self.poll_interval_var.get().strip())
            min_candidate_bars = int(self.min_candidate_bars_var.get().strip())
            min_reversal_pct = float(self.min_reversal_pct_var.get().strip())
        except ValueError:
            messagebox.showwarning("Invalid Settings", "Enter numeric settings.")
            return

        if timeframe < 1 or poll_interval <= 0 or min_candidate_bars < 0 or min_reversal_pct < 0:
            messagebox.showwarning("Invalid Settings", "Settings must be positive values.")
            return

        with self.config_lock:
            self.config = MonitorConfig(
                timeframe_minutes=timeframe,
                poll_interval=poll_interval,
                symbol_chunk_size=self.config.symbol_chunk_size,
                min_candidate_bars=min_candidate_bars,
                min_reversal_pct=min_reversal_pct,
                regular_session_only=self.config.regular_session_only,
                max_swing_level=self.config.max_swing_level,
            )
            self.symbol_states.clear()

        self.status_var.set("Settings applied; swing state reset")

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

    def clear_filters(self) -> None:
        self.state_filter_var.set("All states")
        self.event_filter_var.set("All events")
        self.render_rows(self.last_rows)

    def sort_by_column(self, column: str) -> None:
        if column not in SORTABLE_COLUMNS:
            return
        if self.sort_column == column:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_column = column
            self.sort_descending = column != "symbol"
        self.render_rows(self.last_rows)

    def render_rows(self, rows: list[dict[str, Any]]) -> None:
        self.update_filter_options(rows)
        state_filter = self.state_filter_var.get()
        event_filter = self.event_filter_var.get()
        sorted_rows = [
            row
            for row in rows
            if (
                state_filter == "All states"
                or str(row.get("state", "")) == state_filter
            )
            and (
                event_filter == "All events"
                or str(row.get("event", "")) == event_filter
            )
        ]
        if self.sort_column == "symbol":
            sorted_rows.sort(
                key=lambda row: str(row.get("symbol", "")),
                reverse=self.sort_descending,
            )
        elif self.sort_column == "rvol":
            sorted_rows.sort(
                key=lambda row: numeric_or_none(row.get("rvol")) or float("-inf"),
                reverse=self.sort_descending,
            )
        elif self.sort_column == "last_close":
            sorted_rows.sort(
                key=lambda row: numeric_or_none(row.get("last_close")) or float("-inf"),
                reverse=self.sort_descending,
            )
        elif self.sort_column == "last_bar":
            sorted_rows.sort(
                key=lambda row: row.get("last_bar") or pd.Timestamp.min,
                reverse=self.sort_descending,
            )
        elif self.sort_column in {"state", "event"}:
            sorted_rows.sort(
                key=lambda row: str(row.get(self.sort_column, "")),
                reverse=self.sort_descending,
            )

        self.tree.delete(*self.tree.get_children())
        for row in sorted_rows:
            values = []
            for column in DISPLAY_COLUMNS:
                value = row.get(column)
                if isinstance(value, pd.Timestamp):
                    value = format_time(value)
                elif numeric_or_none(value) is not None:
                    value = format_float(value, 2 if column != "rvol" else 3)
                elif value is None:
                    value = ""
                values.append(value)
            self.tree.insert("", tk.END, values=values)

    def update_filter_options(self, rows: list[dict[str, Any]]) -> None:
        state_values = ["All states"] + sorted(
            {
                str(row.get("state", ""))
                for row in rows
                if str(row.get("state", ""))
            }
        )
        event_values = ["All events"] + sorted(
            {
                str(row.get("event", ""))
                for row in rows
                if str(row.get("event", ""))
            }
        )
        self.state_filter_combo.configure(values=state_values)
        self.event_filter_combo.configure(values=event_values)

        if self.state_filter_var.get() not in state_values:
            self.state_filter_var.set("All states")
        if self.event_filter_var.get() not in event_values:
            self.event_filter_var.set("All events")

    def process_queue(self) -> None:
        try:
            while True:
                kind, payload = self.output_queue.get_nowait()
                if kind == "rows":
                    self.last_rows = payload
                    self.render_rows(payload)
                elif kind == "status":
                    self.status_var.set(payload)
                elif kind == "alert":
                    self.alerts.insert(0, payload)
                    if self.alerts.size() > 250:
                        self.alerts.delete(250, tk.END)
                elif kind == "error":
                    self.status_var.set("Worker error")
                    messagebox.showerror("Intraday Hi/Lo Error", payload)
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
        stocks_engine = make_gptdb_engine(STOCKS_DB)
        stream_engine = make_stream_engine(STREAM_DB)
        self.output_queue.put(("status", "Connected; waiting for symbols..."))

        while not self.stop_event.is_set():
            if self.pause_event.is_set():
                time.sleep(0.25)
                continue

            config = self.current_config()
            now = dt.datetime.now(EASTERN).replace(tzinfo=None)
            day = now.date()
            start_time = REGULAR_SESSION_START if config.regular_session_only else dt.time.min
            end_time = REGULAR_SESSION_END if config.regular_session_only else dt.time.max
            start_ts = dt.datetime.combine(day, start_time)
            end_ts = min(now, dt.datetime.combine(day, end_time))

            elevated = fetch_elevated_symbols(stocks_engine)
            symbols = elevated["symbol"].tolist()
            rvol_by_symbol = {
                symbol: numeric_or_none(rvol)
                for symbol, rvol in zip(elevated["symbol"], elevated["rvol"], strict=False)
            }
            if not symbols:
                self.output_queue.put(("rows", []))
                self.output_queue.put(("status", "No symbols in stocks.elevated_rvol"))
                time.sleep(config.poll_interval)
                continue

            raw = fetch_ohlcv_rows(
                stream_engine,
                symbols=symbols,
                start_ts=start_ts,
                end_ts=end_ts,
                symbol_chunk_size=config.symbol_chunk_size,
            )

            rows: list[dict[str, Any]] = []
            grouped_rows = {
                symbol: symbol_rows
                for symbol, symbol_rows in raw.groupby("Symbol", sort=True)
            }
            for symbol in symbols:
                symbol_rows = grouped_rows.get(symbol)
                if symbol_rows is None or symbol_rows.empty:
                    rows.append(
                        {
                            "symbol": symbol,
                            "rvol": rvol_by_symbol.get(symbol),
                            "state": "No data yet",
                            "event": "",
                        }
                    )
                    continue

                bars = resample_symbol_ohlcv(symbol_rows, config.timeframe_minutes)
                if bars.empty:
                    rows.append(
                        {
                            "symbol": symbol,
                            "rvol": rvol_by_symbol.get(symbol),
                            "state": "No completed bars",
                            "event": "",
                        }
                    )
                    continue
                swings = calculate_swings(bars, max_level=config.max_swing_level)
                prior = self.symbol_states.get(symbol, SymbolSwingState())
                row, new_state, event = classify_symbol_state(
                    symbol=symbol,
                    swings=swings,
                    prior_state=prior,
                    min_candidate_bars=config.min_candidate_bars,
                    min_reversal_pct=config.min_reversal_pct,
                )
                row["rvol"] = rvol_by_symbol.get(symbol)
                self.symbol_states[symbol] = new_state
                rows.append(row)
                if event:
                    self.output_queue.put(
                        (
                            "alert",
                            f"{dt.datetime.now(EASTERN):%H:%M:%S} {symbol}: {event}",
                        )
                    )

            active_symbols = set(symbols)
            for stale_symbol in list(self.symbol_states):
                if stale_symbol not in active_symbols:
                    del self.symbol_states[stale_symbol]

            self.output_queue.put(("rows", rows))
            self.output_queue.put(
                (
                    "status",
                    f"{dt.datetime.now(EASTERN):%H:%M:%S}: "
                    f"{len(rows)}/{len(symbols)} symbols, "
                    f"{config.timeframe_minutes}-minute bars",
                )
            )
            time.sleep(config.poll_interval)

    def on_close(self) -> None:
        self.stop_event.set()
        self.root.destroy()


def main() -> None:
    args = parse_args()
    config = MonitorConfig(
        timeframe_minutes=args.timeframe_minutes,
        poll_interval=args.poll_interval,
        symbol_chunk_size=args.symbol_query_chunk_size,
        min_candidate_bars=args.min_candidate_bars,
        min_reversal_pct=args.min_reversal_pct,
        regular_session_only=not args.include_extended_hours,
        max_swing_level=max(2, args.max_swing_level),
    )
    root = tk.Tk()
    IntradayHiloGUI(root, config)
    root.mainloop()


if __name__ == "__main__":
    main()
