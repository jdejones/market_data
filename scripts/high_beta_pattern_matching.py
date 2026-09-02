from __future__ import annotations

import argparse
import datetime as dt
import pickle
import queue
import sys
import threading
import time
import traceback
import tkinter as tk
from dataclasses import dataclass
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

from market_data.api_keys import intraday_stream_database  # type: ignore[import-not-found]


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "price_data_streamer"
STREAM_DATABASE = "intraday_price_stream"
STOCKS_DATABASE = "stocks"
STREAM_TABLE = "ohlcv_1m"
RVOL_PROFILE_TABLE = "temp_cum_rvol"
EASTERN = ZoneInfo("America/New_York")
MARKET_OPEN = dt.time(9, 30)
REGULAR_MARKET_CLOSE = dt.time(16, 0)
DEFAULT_ARTIFACT_PATH = Path(
    r"E:\Market Research\Dataset\market_data_pickled\high_beta_medoid_library.pkl"
)

SYMBOL_COLUMN = "Symbol"
BRANCH_COLUMN = "Branch"
SIMILARITY_COLUMN = "Similarity"
CORRELATION_COLUMN = "Correlation"
RMSE_COLUMN = "RMSE (%)"
BARS_COLUMN = "Bars"
RVOL_COLUMN = "RVol"
UPDATED_COLUMN = "Updated"
TABLE_COLUMNS = (
    SYMBOL_COLUMN,
    BRANCH_COLUMN,
    SIMILARITY_COLUMN,
    CORRELATION_COLUMN,
    RMSE_COLUMN,
    BARS_COLUMN,
    RVOL_COLUMN,
    UPDATED_COLUMN,
)
SORTABLE_COLUMNS = set(TABLE_COLUMNS)


@dataclass(frozen=True)
class GuiConfig:
    artifact_path: Path
    poll_interval: float
    query_overlap_minutes: int
    symbol_query_chunk_size: int
    start_delay_minutes: int
    minimum_correlation_bars: int
    shape_weight: float


@dataclass(frozen=True)
class PatternMatch:
    symbol: str
    branch: int
    similarity: float
    correlation: float | None
    rmse_pct: float
    bars: int
    rvol: float | None
    updated: pd.Timestamp


def mysql_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def chunked(items: list[str], chunk_size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), chunk_size):
        yield items[index:index + chunk_size]


def make_engine(database: str) -> Engine:
    password = quote_plus(intraday_stream_database)
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        future=True,
        connect_args={"connect_timeout": 5},
    )


def load_medoid_artifact(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(
            f"Medoid artifact not found: {path}\n"
            "Run the medoid cell in workbooks/High_Beta.ipynb first."
        )

    with path.open("rb") as artifact_file:
        artifact = pickle.load(artifact_file)

    required_keys = {
        "artifact_version",
        "timeframe_minutes",
        "bars_per_session",
        "high_beta_symbols",
        "medoids",
    }
    missing = required_keys.difference(artifact)
    if missing:
        raise ValueError(
            f"Medoid artifact is missing: {', '.join(sorted(missing))}"
        )

    timeframe = int(artifact["timeframe_minutes"])
    bars_per_session = int(artifact["bars_per_session"])
    if timeframe < 1 or bars_per_session < 1:
        raise ValueError("Artifact timeframe and bars per session must be positive.")

    symbols = [
        str(symbol).strip().upper()
        for symbol in artifact["high_beta_symbols"]
        if str(symbol).strip()
    ]
    if not symbols:
        raise ValueError("The medoid artifact contains no high-beta symbols.")

    normalized_medoids: dict[int, dict[str, Any]] = {}
    for raw_branch, raw_medoid in artifact["medoids"].items():
        branch = int(raw_branch)
        medoid = dict(raw_medoid)
        path_values = np.asarray(
            medoid["cumulative_return_path"],
            dtype=float,
        ).reshape(-1)
        if len(path_values) != bars_per_session:
            raise ValueError(
                f"Branch {branch} has {len(path_values)} path values; "
                f"expected {bars_per_session}."
            )
        if not np.isfinite(path_values).all():
            raise ValueError(f"Branch {branch} contains non-finite path values.")
        medoid["cumulative_return_path"] = path_values
        normalized_medoids[branch] = medoid

    if not normalized_medoids:
        raise ValueError("The medoid artifact contains no branch medoids.")

    artifact["high_beta_symbols"] = list(dict.fromkeys(symbols))
    artifact["medoids"] = normalized_medoids
    return artifact


def fetch_stream_rows(
    engine: Engine,
    symbols: list[str],
    start_timestamp: dt.datetime,
    end_timestamp: dt.datetime,
    symbol_chunk_size: int,
) -> pd.DataFrame:
    statement = text(
        f"""
        SELECT Symbol, Timestamp, Open, High, Low, Close, Volume
        FROM {mysql_identifier(STREAM_TABLE)}
        WHERE Timestamp >= :start_timestamp
          AND Timestamp < :end_timestamp
          AND Symbol IN :symbols
        ORDER BY Timestamp, Symbol
        """
    ).bindparams(bindparam("symbols", expanding=True))

    frames = []
    for symbol_group in chunked(symbols, symbol_chunk_size):
        frame = pd.read_sql(
            statement,
            con=engine,
            params={
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "symbols": symbol_group,
            },
        )
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame(
            columns=[
                "Symbol",
                "Timestamp",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
            ]
        )

    rows = pd.concat(frames, ignore_index=True)
    rows["Symbol"] = rows["Symbol"].astype(str).str.upper()
    rows["Timestamp"] = pd.to_datetime(rows["Timestamp"], errors="coerce")
    rows = rows.dropna(subset=["Symbol", "Timestamp"])
    return rows.sort_values(["Timestamp", "Symbol"])


def merge_stream_rows(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        merged = incoming.copy()
    elif incoming.empty:
        return existing
    else:
        merged = pd.concat([existing, incoming], ignore_index=True)

    return (
        merged.sort_values(["Timestamp", "Symbol"])
        .drop_duplicates(["Symbol", "Timestamp"], keep="last")
        .reset_index(drop=True)
    )


def load_rvol_profiles(
    engine: Engine,
    symbols: list[str],
    symbol_chunk_size: int,
) -> dict[str, pd.Series]:
    statement = text(
        f"""
        SELECT symbol, timestamp, avg_cum_volume
        FROM {mysql_identifier(RVOL_PROFILE_TABLE)}
        WHERE symbol IN :symbols
        ORDER BY symbol, timestamp
        """
    ).bindparams(bindparam("symbols", expanding=True))

    frames = []
    try:
        for symbol_group in chunked(symbols, symbol_chunk_size):
            frame = pd.read_sql(
                statement,
                con=engine,
                params={"symbols": symbol_group},
            )
            if not frame.empty:
                frames.append(frame)
    except Exception:
        return {}

    if not frames:
        return {}

    profiles = pd.concat(frames, ignore_index=True)
    profiles["symbol"] = profiles["symbol"].astype(str).str.upper()
    profiles["timestamp"] = profiles["timestamp"].astype(str)
    profiles["avg_cum_volume"] = pd.to_numeric(
        profiles["avg_cum_volume"],
        errors="coerce",
    )
    profiles = profiles.dropna(subset=["avg_cum_volume"])

    return {
        symbol: group.set_index("timestamp")["avg_cum_volume"].sort_index()
        for symbol, group in profiles.groupby("symbol")
    }


def calculate_current_rvol(
    symbol_rows: pd.DataFrame,
    profile: pd.Series | None,
) -> float | None:
    if profile is None or profile.empty or symbol_rows.empty:
        return None

    latest_timestamp = pd.Timestamp(symbol_rows["Timestamp"].max()).floor("min")
    time_key = latest_timestamp.strftime("%H:%M:%S")
    available_times = profile.index[profile.index <= time_key]
    if len(available_times) == 0:
        return None

    denominator = pd.to_numeric(profile.loc[available_times[-1]], errors="coerce")
    cumulative_volume = pd.to_numeric(
        symbol_rows["Volume"],
        errors="coerce",
    ).sum()
    if (
        pd.isna(denominator)
        or float(denominator) <= 0
        or pd.isna(cumulative_volume)
    ):
        return None

    rvol = float(cumulative_volume) / float(denominator)
    return rvol if np.isfinite(rvol) else None


def build_completed_path(
    symbol_rows: pd.DataFrame,
    timeframe_minutes: int,
    bars_per_session: int,
    as_of: dt.datetime,
) -> np.ndarray | None:
    if symbol_rows.empty:
        return None

    data = symbol_rows.copy()
    data["Timestamp"] = pd.to_datetime(data["Timestamp"], errors="coerce")
    data = data.dropna(
        subset=["Timestamp", "Open", "High", "Low", "Close"]
    ).sort_values("Timestamp")
    if data.empty:
        return None

    minutes_after_midnight = (
        data["Timestamp"].dt.hour * 60 + data["Timestamp"].dt.minute
    )
    open_minute = MARKET_OPEN.hour * 60 + MARKET_OPEN.minute
    data["bar_number"] = (
        (minutes_after_midnight - open_minute) // timeframe_minutes
    )
    data = data.loc[data["bar_number"].between(0, bars_per_session - 1)]
    if data.empty:
        return None

    elapsed_minutes = max(
        0,
        int(
            (
                as_of
                - dt.datetime.combine(as_of.date(), MARKET_OPEN)
            ).total_seconds()
            // 60
        ),
    )
    completed_bars = min(
        elapsed_minutes // timeframe_minutes,
        bars_per_session,
    )
    if completed_bars < 1:
        return None

    bars = (
        data.groupby("bar_number")
        .agg(
            Open=("Open", "first"),
            High=("High", "max"),
            Low=("Low", "min"),
            Close=("Close", "last"),
            Volume=("Volume", "sum"),
        )
        .reindex(range(completed_bars))
    )
    bars[["Open", "High", "Low", "Close"]] = bars[
        ["Open", "High", "Low", "Close"]
    ].interpolate(
        method="linear",
        limit=2,
        limit_area="inside",
    )
    if bars[["Open", "High", "Low", "Close"]].isna().any().any():
        return None

    first_open = float(bars.iloc[0]["Open"])
    if not np.isfinite(first_open) or first_open <= 0:
        return None

    path = bars["Close"].to_numpy(dtype=float) / first_open - 1
    return path if np.isfinite(path).all() else None


def score_against_medoids(
    current_path: np.ndarray,
    medoids: dict[int, dict[str, Any]],
    minimum_correlation_bars: int,
    shape_weight: float,
) -> tuple[int, float, float | None, float]:
    path_length = len(current_path)
    medoid_prefixes = {
        branch: np.asarray(medoid["cumulative_return_path"], dtype=float)[
            :path_length
        ]
        for branch, medoid in medoids.items()
    }
    reference_values = np.concatenate(list(medoid_prefixes.values()))
    reference_scale = max(float(np.std(reference_values, ddof=0)), 0.0025)

    scores = []
    for branch, medoid_prefix in medoid_prefixes.items():
        rmse = float(np.sqrt(np.mean(np.square(current_path - medoid_prefix))))
        correlation: float | None = None
        if (
            path_length >= minimum_correlation_bars
            and np.std(current_path, ddof=0) > 0
            and np.std(medoid_prefix, ddof=0) > 0
        ):
            candidate = float(np.corrcoef(current_path, medoid_prefix)[0, 1])
            if np.isfinite(candidate):
                correlation = float(np.clip(candidate, -1, 1))

        magnitude_distance = rmse / reference_scale
        if correlation is None:
            combined_distance = magnitude_distance
        else:
            correlation_distance = (1 - correlation) / 2
            combined_distance = (
                shape_weight * correlation_distance
                + (1 - shape_weight) * magnitude_distance
            )

        similarity = 100 / (1 + combined_distance)
        scores.append((branch, similarity, correlation, rmse * 100))

    return max(scores, key=lambda result: result[1])


def calculate_matches(
    stream_rows: pd.DataFrame,
    artifact: dict[str, Any],
    rvol_profiles: dict[str, pd.Series],
    as_of: dt.datetime,
    minimum_correlation_bars: int,
    shape_weight: float,
) -> dict[str, PatternMatch]:
    if stream_rows.empty:
        return {}

    timeframe = int(artifact["timeframe_minutes"])
    bars_per_session = int(artifact["bars_per_session"])
    medoids = artifact["medoids"]
    matches = {}

    for symbol, symbol_rows in stream_rows.groupby("Symbol"):
        path = build_completed_path(
            symbol_rows,
            timeframe_minutes=timeframe,
            bars_per_session=bars_per_session,
            as_of=as_of,
        )
        if path is None:
            continue

        branch, similarity, correlation, rmse_pct = score_against_medoids(
            path,
            medoids=medoids,
            minimum_correlation_bars=minimum_correlation_bars,
            shape_weight=shape_weight,
        )
        matches[str(symbol)] = PatternMatch(
            symbol=str(symbol),
            branch=int(branch),
            similarity=float(similarity),
            correlation=correlation,
            rmse_pct=float(rmse_pct),
            bars=len(path),
            rvol=calculate_current_rvol(
                symbol_rows,
                rvol_profiles.get(str(symbol)),
            ),
            updated=pd.Timestamp(symbol_rows["Timestamp"].max()),
        )

    return matches


def optional_float(value: Any) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


def format_float(value: float | None, digits: int = 2) -> str:
    return "" if value is None else f"{value:.{digits}f}"


class HighBetaPatternMatchingGUI:
    def __init__(self, root: tk.Tk, config: GuiConfig) -> None:
        self.root = root
        self.config = config
        self.output_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.reload_event = threading.Event()
        self.matches: dict[str, PatternMatch] = {}
        self.sort_column = SIMILARITY_COLUMN
        self.sort_descending = True

        self.root.title("High Beta Pattern Matching")
        self.root.geometry("1100x650")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.status_var = tk.StringVar(value="Starting...")
        self.count_var = tk.StringVar(value="Matched: 0")
        self.branch_filter_var = tk.StringVar(value="All branches")

        self._build_widgets()
        self.worker = threading.Thread(
            target=self.worker_loop,
            name="high-beta-pattern-matching-worker",
            daemon=True,
        )
        self.worker.start()
        self.root.after(250, self.process_queue)

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        filter_frame = ttk.Frame(container)
        filter_frame.grid(
            row=0,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(0, 8),
        )
        ttk.Label(filter_frame, text="Branch:").pack(side=tk.LEFT)
        self.branch_filter = ttk.Combobox(
            filter_frame,
            textvariable=self.branch_filter_var,
            values=("All branches",),
            state="readonly",
            width=16,
        )
        self.branch_filter.pack(side=tk.LEFT, padx=(6, 0))
        self.branch_filter.bind(
            "<<ComboboxSelected>>",
            lambda _event: self.render_matches(),
        )

        self.tree = ttk.Treeview(
            container,
            columns=TABLE_COLUMNS,
            show="headings",
            selectmode="browse",
        )
        widths = {
            SYMBOL_COLUMN: 100,
            BRANCH_COLUMN: 80,
            SIMILARITY_COLUMN: 110,
            CORRELATION_COLUMN: 110,
            RMSE_COLUMN: 100,
            BARS_COLUMN: 70,
            RVOL_COLUMN: 90,
            UPDATED_COLUMN: 160,
        }
        for column in TABLE_COLUMNS:
            self.tree.heading(
                column,
                text=column,
                command=lambda selected=column: self.sort_by_column(selected),
            )
            anchor = tk.W if column in {SYMBOL_COLUMN, UPDATED_COLUMN} else tk.E
            self.tree.column(column, width=widths[column], anchor=anchor)

        y_scroll = ttk.Scrollbar(
            container,
            orient=tk.VERTICAL,
            command=self.tree.yview,
        )
        x_scroll = ttk.Scrollbar(
            container,
            orient=tk.HORIZONTAL,
            command=self.tree.xview,
        )
        self.tree.configure(
            yscrollcommand=y_scroll.set,
            xscrollcommand=x_scroll.set,
        )
        self.tree.grid(row=1, column=0, sticky="nsew")
        y_scroll.grid(row=1, column=1, sticky="ns")
        x_scroll.grid(row=2, column=0, sticky="ew")

        footer = ttk.Frame(container)
        footer.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        footer.columnconfigure(0, weight=1)
        ttk.Label(
            footer,
            textvariable=self.status_var,
            justify=tk.LEFT,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(footer, textvariable=self.count_var).grid(
            row=1,
            column=0,
            sticky="w",
        )
        ttk.Button(
            footer,
            text="Reload Medoids",
            command=self.request_reload,
        ).grid(row=0, column=1, rowspan=2, padx=(8, 0))

    def request_reload(self) -> None:
        self.reload_event.set()
        self.status_var.set("Reloading medoid artifact...")

    def sort_by_column(self, column: str) -> None:
        if column not in SORTABLE_COLUMNS:
            return
        if self.sort_column == column:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_column = column
            self.sort_descending = column != SYMBOL_COLUMN
        self.render_matches()

    def render_matches(self) -> None:
        all_matches = list(self.matches.values())
        branches = sorted({match.branch for match in all_matches})
        filter_values = ("All branches", *(f"Branch {branch}" for branch in branches))
        self.branch_filter.configure(values=filter_values)

        selected_filter = self.branch_filter_var.get()
        if selected_filter not in filter_values:
            selected_filter = "All branches"
            self.branch_filter_var.set(selected_filter)

        if selected_filter == "All branches":
            matches = all_matches
        else:
            selected_branch = int(selected_filter.removeprefix("Branch "))
            matches = [
                match for match in all_matches if match.branch == selected_branch
            ]

        def sort_key(match: PatternMatch) -> Any:
            values = {
                SYMBOL_COLUMN: match.symbol,
                BRANCH_COLUMN: match.branch,
                SIMILARITY_COLUMN: match.similarity,
                CORRELATION_COLUMN: match.correlation,
                RMSE_COLUMN: match.rmse_pct,
                BARS_COLUMN: match.bars,
                RVOL_COLUMN: match.rvol,
                UPDATED_COLUMN: match.updated,
            }
            value = values[self.sort_column]
            if value is None:
                return float("-inf") if self.sort_descending else float("inf")
            return value

        matches.sort(key=sort_key, reverse=self.sort_descending)
        self.tree.delete(*self.tree.get_children())
        for match in matches:
            self.tree.insert(
                "",
                tk.END,
                values=(
                    match.symbol,
                    match.branch,
                    format_float(match.similarity),
                    format_float(match.correlation, 3),
                    format_float(match.rmse_pct, 3),
                    match.bars,
                    format_float(match.rvol),
                    match.updated.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        branch_counts = {
            branch: sum(match.branch == branch for match in all_matches)
            for branch in branches
        }
        count_parts = [f"Matched: {len(all_matches)}"]
        count_parts.extend(
            f"Branch {branch}: {branch_counts[branch]}" for branch in branches
        )
        if selected_filter != "All branches":
            count_parts.append(f"Shown: {len(matches)}")
        self.count_var.set(" | ".join(count_parts))

    def process_queue(self) -> None:
        try:
            while True:
                message_type, payload = self.output_queue.get_nowait()
                if message_type == "status":
                    self.status_var.set(str(payload))
                elif message_type == "matches":
                    self.matches.update(payload)
                    self.render_matches()
                elif message_type == "error":
                    self.status_var.set("Worker failed")
                    messagebox.showerror("Pattern Matching Failed", str(payload))
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
        artifact = load_medoid_artifact(self.config.artifact_path)
        symbols = list(artifact["high_beta_symbols"])
        stream_engine = make_engine(STREAM_DATABASE)
        stocks_engine = make_engine(STOCKS_DATABASE)
        stream_rows = pd.DataFrame()
        last_seen_timestamp: pd.Timestamp | None = None
        rvol_profiles: dict[str, pd.Series] = {}
        last_rvol_profile_attempt = float("-inf")

        current_date = dt.datetime.now(EASTERN).date()
        market_open = dt.datetime.combine(current_date, MARKET_OPEN)
        processing_start = market_open + dt.timedelta(
            minutes=self.config.start_delay_minutes
        )
        market_close = dt.datetime.combine(current_date, REGULAR_MARKET_CLOSE)

        while not self.stop_event.is_set():
            now = dt.datetime.now(EASTERN).replace(tzinfo=None)
            if now.date() != current_date:
                current_date = now.date()
                market_open = dt.datetime.combine(current_date, MARKET_OPEN)
                processing_start = market_open + dt.timedelta(
                    minutes=self.config.start_delay_minutes
                )
                market_close = dt.datetime.combine(
                    current_date,
                    REGULAR_MARKET_CLOSE,
                )
                stream_rows = pd.DataFrame()
                last_seen_timestamp = None
                self.matches.clear()

            if self.reload_event.is_set():
                artifact = load_medoid_artifact(self.config.artifact_path)
                symbols = list(artifact["high_beta_symbols"])
                self.reload_event.clear()
                self.output_queue.put(
                    (
                        "status",
                        f"Reloaded {len(artifact['medoids'])} medoids.",
                    )
                )

            if now < processing_start:
                seconds_remaining = max(
                    0,
                    int((processing_start - now).total_seconds()),
                )
                self.output_queue.put(
                    (
                        "status",
                        f"Waiting until {processing_start:%H:%M} ET "
                        f"({seconds_remaining // 60}m remaining)...",
                    )
                )
                self.stop_event.wait(min(self.config.poll_interval, 5))
                continue

            if time.monotonic() - last_rvol_profile_attempt >= 60:
                refreshed_profiles = load_rvol_profiles(
                    stocks_engine,
                    symbols,
                    self.config.symbol_query_chunk_size,
                )
                if refreshed_profiles:
                    rvol_profiles = refreshed_profiles
                last_rvol_profile_attempt = time.monotonic()

            query_start = market_open
            if last_seen_timestamp is not None:
                query_start = max(
                    market_open,
                    last_seen_timestamp.to_pydatetime()
                    - dt.timedelta(minutes=self.config.query_overlap_minutes),
                )
            query_end = min(now + dt.timedelta(seconds=1), market_close)
            incoming = fetch_stream_rows(
                stream_engine,
                symbols=symbols,
                start_timestamp=query_start,
                end_timestamp=query_end,
                symbol_chunk_size=self.config.symbol_query_chunk_size,
            )
            stream_rows = merge_stream_rows(stream_rows, incoming)
            if not incoming.empty:
                last_seen_timestamp = pd.Timestamp(incoming["Timestamp"].max())

            matches = calculate_matches(
                stream_rows,
                artifact=artifact,
                rvol_profiles=rvol_profiles,
                as_of=min(now, market_close),
                minimum_correlation_bars=self.config.minimum_correlation_bars,
                shape_weight=self.config.shape_weight,
            )
            if matches:
                self.output_queue.put(("matches", matches))

            rvol_status = (
                f"RVol profiles: {len(rvol_profiles)}"
                if rvol_profiles
                else "RVol unavailable"
            )
            session_status = (
                "Market closed; displaying final matches"
                if now >= market_close
                else "Monitoring"
            )
            self.output_queue.put(
                (
                    "status",
                    f"{session_status} {len(symbols)} high-beta symbols | "
                    f"{len(matches)} matched | {rvol_status} | "
                    f"as of {now:%H:%M:%S} ET",
                )
            )
            self.stop_event.wait(self.config.poll_interval)

    def on_close(self) -> None:
        self.stop_event.set()
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Continuously match current high-beta intraday price paths "
            "to persisted dendrogram medoids."
        )
    )
    parser.add_argument(
        "--artifact-path",
        type=Path,
        default=DEFAULT_ARTIFACT_PATH,
    )
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--query-overlap-minutes", type=int, default=2)
    parser.add_argument("--symbol-query-chunk-size", type=int, default=500)
    parser.add_argument(
        "--start-delay-minutes",
        type=int,
        default=5,
        help="Begin matching this many minutes after the 09:30 ET open.",
    )
    parser.add_argument("--minimum-correlation-bars", type=int, default=3)
    parser.add_argument(
        "--shape-weight",
        type=float,
        default=0.7,
        help="Correlation-distance weight; the remainder weights return RMSE.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.poll_interval <= 0:
        raise ValueError("--poll-interval must be greater than zero")
    if args.query_overlap_minutes < 0:
        raise ValueError("--query-overlap-minutes cannot be negative")
    if args.symbol_query_chunk_size < 1:
        raise ValueError("--symbol-query-chunk-size must be at least one")
    if args.start_delay_minutes < 0:
        raise ValueError("--start-delay-minutes cannot be negative")
    if args.minimum_correlation_bars < 2:
        raise ValueError("--minimum-correlation-bars must be at least two")
    if not 0 <= args.shape_weight <= 1:
        raise ValueError("--shape-weight must be between zero and one")

    config = GuiConfig(
        artifact_path=args.artifact_path,
        poll_interval=args.poll_interval,
        query_overlap_minutes=args.query_overlap_minutes,
        symbol_query_chunk_size=args.symbol_query_chunk_size,
        start_delay_minutes=args.start_delay_minutes,
        minimum_correlation_bars=args.minimum_correlation_bars,
        shape_weight=args.shape_weight,
    )
    root = tk.Tk()
    app = HighBetaPatternMatchingGUI(root, config)
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
