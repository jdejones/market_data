from __future__ import annotations

import argparse
import datetime as dt
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
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle
from sklearn.cluster import AgglomerativeClustering
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from market_data.api_keys import (  # type: ignore[import-not-found]
    gptdb,
    intraday_stream_database,
)


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "price_data_streamer"
STOCKS_MYSQL_USER = "gptdb"
STREAM_DATABASE = "intraday_price_stream"
STOCKS_DATABASE = "stocks"
STREAM_TABLE = "ohlcv_1m"
RVOL_PROFILE_TABLE = "temp_cum_rvol"

EASTERN = ZoneInfo("America/New_York")
MARKET_OPEN = dt.time(9, 30)
REGULAR_MARKET_CLOSE = dt.time(16, 0)
REGULAR_SESSION_MINUTES = 390
DEFAULT_SYMBOLS_PATH = Path(
    r"E:\Market Research\Studies\Sector Studies\Watchlists\High_Beta.txt"
)

STRUCTURE_FEATURE_WEIGHTS = {
    "standardized_path": 2.0,
    "consecutive_return": 2.0,
    "candle_body": 1.0,
    "intraday_range": 1.0,
    "drawdown": 1.0,
    "rolling_volatility": 1.0,
    "relative_volume": 0.75,
}

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
class AppConfig:
    symbols_path: Path
    poll_interval_seconds: float
    query_overlap_minutes: int
    symbol_query_chunk_size: int
    start_delay_minutes: int
    minimum_feature_bars: int
    minimum_correlation_bars: int
    max_missing_bars: int
    pca_components: int
    shape_weight: float
    initial_update_interval_minutes: int
    initial_n_clusters: int
    initial_timeframe_minutes: int


@dataclass(frozen=True)
class RuntimeSettings:
    update_interval_minutes: int
    n_clusters: int
    timeframe_minutes: int


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


@dataclass(frozen=True)
class SessionObservation:
    symbol: str
    bars: pd.DataFrame
    features: dict[str, np.ndarray]
    cumulative_path: np.ndarray
    updated: pd.Timestamp


@dataclass(frozen=True)
class BranchMedoid:
    branch: int
    symbol: str
    bars: pd.DataFrame
    cumulative_path: np.ndarray
    session_return: float
    members: int


@dataclass(frozen=True)
class ClusterResult:
    matches: dict[str, PatternMatch]
    medoids: dict[int, BranchMedoid]
    timeframe_minutes: int
    explained_variance: float
    calculated_at: dt.datetime
    eligible_symbols: int


class InsufficientDataError(ValueError):
    pass


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


def make_stocks_engine() -> Engine:
    password = quote_plus(gptdb)
    url = (
        f"mysql+pymysql://{STOCKS_MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{STOCKS_DATABASE}"
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        future=True,
        connect_args={"connect_timeout": 5},
    )


def load_symbols(path: Path) -> list[str]:
    if not path.is_file():
        raise FileNotFoundError(
            f"High-beta symbols file not found: {path}\n"
            "Run the High_Beta.ipynb watchlist export first or pass --symbols-path."
        )

    symbols: list[str] = []
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        symbol = raw_line.strip().upper()
        if symbol and symbol != "SPY" and not symbol.startswith("#"):
            symbols.append(symbol)
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        raise ValueError(f"No symbols were found in {path}.")
    return symbols


def empty_stream_frame() -> pd.DataFrame:
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

    frames: list[pd.DataFrame] = []
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
        return empty_stream_frame()

    rows = pd.concat(frames, ignore_index=True)
    rows["Symbol"] = rows["Symbol"].astype(str).str.upper()
    rows["Timestamp"] = pd.to_datetime(rows["Timestamp"], errors="coerce")
    numeric_columns = ["Open", "High", "Low", "Close", "Volume"]
    rows[numeric_columns] = rows[numeric_columns].apply(
        pd.to_numeric,
        errors="coerce",
    )
    rows = rows.dropna(subset=["Symbol", "Timestamp"])
    return rows.sort_values(["Timestamp", "Symbol"])


def merge_stream_rows(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
) -> pd.DataFrame:
    if incoming.empty:
        return existing
    if existing.empty:
        merged = incoming.copy()
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

    frames: list[pd.DataFrame] = []
    for symbol_group in chunked(symbols, symbol_chunk_size):
        frame = pd.read_sql(
            statement,
            con=engine,
            params={"symbols": symbol_group},
        )
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return {}

    profiles = pd.concat(frames, ignore_index=True)
    profiles["symbol"] = profiles["symbol"].astype(str).str.upper()
    profile_times = pd.to_timedelta(profiles["timestamp"], errors="coerce")
    profiles["timestamp"] = profile_times.map(
        lambda value: (
            None
            if pd.isna(value)
            else (
                dt.datetime.min
                + dt.timedelta(
                    seconds=int(value.total_seconds()) % (24 * 60 * 60)
                )
            ).strftime("%H:%M:%S")
        )
    )
    profiles["avg_cum_volume"] = pd.to_numeric(
        profiles["avg_cum_volume"],
        errors="coerce",
    )
    profiles = profiles.dropna(subset=["timestamp", "avg_cum_volume"])
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


def completed_bar_count(as_of: dt.datetime, timeframe_minutes: int) -> int:
    market_open = dt.datetime.combine(as_of.date(), MARKET_OPEN)
    elapsed_minutes = max(
        0,
        int((as_of - market_open).total_seconds() // 60),
    )
    return min(
        elapsed_minutes // timeframe_minutes,
        REGULAR_SESSION_MINUTES // timeframe_minutes,
    )


def aggregate_completed_bars(
    symbol_rows: pd.DataFrame,
    timeframe_minutes: int,
    as_of: dt.datetime,
    max_missing_bars: int,
) -> pd.DataFrame | None:
    if symbol_rows.empty:
        return None

    completed_bars = completed_bar_count(as_of, timeframe_minutes)
    if completed_bars < 1:
        return None

    data = symbol_rows.copy()
    data["Timestamp"] = pd.to_datetime(data["Timestamp"], errors="coerce")
    numeric_columns = ["Open", "High", "Low", "Close", "Volume"]
    data[numeric_columns] = data[numeric_columns].apply(
        pd.to_numeric,
        errors="coerce",
    )
    data = data.dropna(
        subset=["Timestamp", "Open", "High", "Low", "Close", "Volume"]
    ).sort_values("Timestamp")
    if data.empty:
        return None

    open_minute = MARKET_OPEN.hour * 60 + MARKET_OPEN.minute
    minutes_after_midnight = (
        data["Timestamp"].dt.hour * 60 + data["Timestamp"].dt.minute
    )
    data["bar_number"] = (
        (minutes_after_midnight - open_minute) // timeframe_minutes
    )
    data = data.loc[data["bar_number"].between(0, completed_bars - 1)]
    if data.empty:
        return None

    bars = (
        data.groupby("bar_number")
        .agg(
            Open=("Open", "first"),
            High=("High", "max"),
            Low=("Low", "min"),
            Close=("Close", "last"),
            Volume=("Volume", "sum"),
            Timestamp=("Timestamp", "max"),
        )
        .reindex(range(completed_bars))
    )
    price_columns = ["Open", "High", "Low", "Close"]
    bars[price_columns] = bars[price_columns].interpolate(
        method="linear",
        limit=max_missing_bars,
        limit_area="inside",
    )
    bars["Volume"] = bars["Volume"].interpolate(
        method="linear",
        limit=max_missing_bars,
        limit_area="inside",
    )
    if bars[price_columns + ["Volume"]].isna().any().any():
        return None
    if (bars[price_columns] <= 0).any().any() or bars["Volume"].sum() <= 0:
        return None

    fallback_timestamps = [
        dt.datetime.combine(as_of.date(), MARKET_OPEN)
        + dt.timedelta(minutes=index * timeframe_minutes)
        for index in range(completed_bars)
    ]
    bars["Timestamp"] = pd.to_datetime(bars["Timestamp"], errors="coerce")
    bars["Timestamp"] = bars["Timestamp"].fillna(
        pd.Series(fallback_timestamps, index=bars.index)
    )
    return bars


def extract_session_features(
    bars: pd.DataFrame,
) -> tuple[dict[str, np.ndarray], np.ndarray] | None:
    first_open = float(bars.iloc[0]["Open"])
    if not np.isfinite(first_open) or first_open <= 0:
        return None

    close = bars["Close"].astype(float)
    open_ = bars["Open"].astype(float)
    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)
    volume = bars["Volume"].clip(lower=0).astype(float)
    if volume.sum() <= 0:
        return None

    cumulative_path = close / first_open - 1
    path_scale = float(cumulative_path.std(ddof=0))
    if not np.isfinite(path_scale) or path_scale == 0:
        return None
    standardized_path = (
        cumulative_path - cumulative_path.mean()
    ) / path_scale

    previous_close = close.shift(1)
    previous_close.iloc[0] = open_.iloc[0]
    consecutive_return = np.log(close / previous_close)
    candle_body = close / open_ - 1
    intraday_range = (high - low) / previous_close
    drawdown = close / close.cummax() - 1
    rolling_volatility = (
        consecutive_return.rolling(window=10, min_periods=2)
        .std(ddof=0)
        .fillna(0)
    )
    relative_volume = volume / volume.sum()

    features = {
        "standardized_path": standardized_path.to_numpy(dtype=float),
        "consecutive_return": consecutive_return.to_numpy(dtype=float),
        "candle_body": candle_body.to_numpy(dtype=float),
        "intraday_range": intraday_range.to_numpy(dtype=float),
        "drawdown": drawdown.to_numpy(dtype=float),
        "rolling_volatility": rolling_volatility.to_numpy(dtype=float),
        "relative_volume": relative_volume.to_numpy(dtype=float),
    }
    if not all(np.isfinite(values).all() for values in features.values()):
        return None
    return features, cumulative_path.to_numpy(dtype=float)


def build_observations(
    stream_rows: pd.DataFrame,
    timeframe_minutes: int,
    as_of: dt.datetime,
    max_missing_bars: int,
    minimum_feature_bars: int,
) -> list[SessionObservation]:
    observations: list[SessionObservation] = []
    if stream_rows.empty:
        return observations

    for symbol, symbol_rows in stream_rows.groupby("Symbol"):
        bars = aggregate_completed_bars(
            symbol_rows,
            timeframe_minutes=timeframe_minutes,
            as_of=as_of,
            max_missing_bars=max_missing_bars,
        )
        if bars is None or len(bars) < minimum_feature_bars:
            continue
        extracted = extract_session_features(bars)
        if extracted is None:
            continue
        features, cumulative_path = extracted
        observations.append(
            SessionObservation(
                symbol=str(symbol),
                bars=bars,
                features=features,
                cumulative_path=cumulative_path,
                updated=pd.Timestamp(symbol_rows["Timestamp"].max()),
            )
        )
    return observations


def path_similarity(
    current_path: np.ndarray,
    medoid_path: np.ndarray,
    reference_scale: float,
    minimum_correlation_bars: int,
    shape_weight: float,
) -> tuple[float, float | None, float]:
    rmse = float(np.sqrt(np.mean(np.square(current_path - medoid_path))))
    correlation: float | None = None
    if (
        len(current_path) >= minimum_correlation_bars
        and np.std(current_path, ddof=0) > 0
        and np.std(medoid_path, ddof=0) > 0
    ):
        candidate = float(np.corrcoef(current_path, medoid_path)[0, 1])
        if np.isfinite(candidate):
            correlation = float(np.clip(candidate, -1, 1))

    magnitude_distance = rmse / max(reference_scale, 0.0025)
    if correlation is None:
        combined_distance = magnitude_distance
    else:
        correlation_distance = (1 - correlation) / 2
        combined_distance = (
            shape_weight * correlation_distance
            + (1 - shape_weight) * magnitude_distance
        )
    similarity = 100 / (1 + combined_distance)
    return similarity, correlation, rmse * 100


def cluster_current_sessions(
    stream_rows: pd.DataFrame,
    rvol_profiles: dict[str, pd.Series],
    as_of: dt.datetime,
    settings: RuntimeSettings,
    config: AppConfig,
) -> ClusterResult:
    observations = build_observations(
        stream_rows,
        timeframe_minutes=settings.timeframe_minutes,
        as_of=as_of,
        max_missing_bars=config.max_missing_bars,
        minimum_feature_bars=config.minimum_feature_bars,
    )
    if len(observations) < settings.n_clusters:
        raise InsufficientDataError(
            f"Only {len(observations)} eligible symbols are available for "
            f"{settings.n_clusters} clusters."
        )

    scaled_blocks: list[np.ndarray] = []
    for feature_name, weight in STRUCTURE_FEATURE_WEIGHTS.items():
        feature_matrix = np.vstack(
            [observation.features[feature_name] for observation in observations]
        )
        scaled_blocks.append(
            StandardScaler().fit_transform(feature_matrix) * weight
        )
    feature_matrix = np.hstack(scaled_blocks)
    pca_components = min(
        config.pca_components,
        feature_matrix.shape[0] - 1,
        feature_matrix.shape[1],
    )
    if pca_components < 1:
        raise InsufficientDataError("At least two eligible symbols are required.")

    pca = PCA(n_components=pca_components)
    embedding = pca.fit_transform(feature_matrix)
    raw_labels = AgglomerativeClustering(
        n_clusters=settings.n_clusters,
        linkage="ward",
    ).fit_predict(embedding)

    ending_returns = np.asarray(
        [observation.cumulative_path[-1] for observation in observations]
    )
    labels_by_return = sorted(
        np.unique(raw_labels),
        key=lambda label: float(ending_returns[raw_labels == label].mean()),
    )
    branch_map = {
        int(raw_label): branch
        for branch, raw_label in enumerate(labels_by_return, start=1)
    }
    branches = np.asarray([branch_map[int(label)] for label in raw_labels])

    medoids: dict[int, BranchMedoid] = {}
    medoid_observation_indices: dict[int, int] = {}
    for branch in range(1, settings.n_clusters + 1):
        member_indices = np.flatnonzero(branches == branch)
        branch_embedding = embedding[member_indices]
        pairwise_distances = np.linalg.norm(
            branch_embedding[:, np.newaxis, :]
            - branch_embedding[np.newaxis, :, :],
            axis=2,
        )
        medoid_index = int(
            member_indices[int(np.argmin(pairwise_distances.mean(axis=1)))]
        )
        medoid_observation_indices[branch] = medoid_index
        observation = observations[medoid_index]
        medoids[branch] = BranchMedoid(
            branch=branch,
            symbol=observation.symbol,
            bars=observation.bars.copy(),
            cumulative_path=observation.cumulative_path.copy(),
            session_return=float(observation.cumulative_path[-1]),
            members=len(member_indices),
        )

    reference_values = np.concatenate(
        [medoid.cumulative_path for medoid in medoids.values()]
    )
    reference_scale = max(float(np.std(reference_values, ddof=0)), 0.0025)
    matches: dict[str, PatternMatch] = {}
    grouped_rows = {
        str(symbol): rows
        for symbol, rows in stream_rows.groupby("Symbol")
    }
    for observation_index, observation in enumerate(observations):
        branch = int(branches[observation_index])
        medoid_path = observations[
            medoid_observation_indices[branch]
        ].cumulative_path
        similarity, correlation, rmse_pct = path_similarity(
            observation.cumulative_path,
            medoid_path,
            reference_scale=reference_scale,
            minimum_correlation_bars=config.minimum_correlation_bars,
            shape_weight=config.shape_weight,
        )
        symbol_rows = grouped_rows[observation.symbol]
        matches[observation.symbol] = PatternMatch(
            symbol=observation.symbol,
            branch=branch,
            similarity=float(similarity),
            correlation=correlation,
            rmse_pct=float(rmse_pct),
            bars=len(observation.cumulative_path),
            rvol=calculate_current_rvol(
                symbol_rows,
                rvol_profiles.get(observation.symbol),
            ),
            updated=observation.updated,
        )

    explained_variance = float(pca.explained_variance_ratio_.sum())
    return ClusterResult(
        matches=matches,
        medoids=medoids,
        timeframe_minutes=settings.timeframe_minutes,
        explained_variance=explained_variance,
        calculated_at=as_of,
        eligible_symbols=len(observations),
    )


def optional_float(value: Any) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


def format_float(value: float | None, digits: int = 2) -> str:
    return "" if value is None else f"{value:.{digits}f}"


class RealtimeHighBetaClusteringGUI:
    def __init__(self, root: tk.Tk, config: AppConfig) -> None:
        self.root = root
        self.config = config
        self.output_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.recalculate_event = threading.Event()
        self.settings_lock = threading.Lock()
        self.settings = RuntimeSettings(
            update_interval_minutes=config.initial_update_interval_minutes,
            n_clusters=config.initial_n_clusters,
            timeframe_minutes=config.initial_timeframe_minutes,
        )

        self.matches: dict[str, PatternMatch] = {}
        self.medoids: dict[int, BranchMedoid] = {}
        self.universe_size: int | None = None
        self.current_timeframe_minutes = config.initial_timeframe_minutes
        self.sort_column = SIMILARITY_COLUMN
        self.sort_descending = True

        self.status_var = tk.StringVar(value="Starting...")
        self.count_var = tk.StringVar(value="Matched: 0")
        self.branch_filter_var = tk.StringVar(value="All branches")
        self.chart_branch_var = tk.StringVar(value="")
        self.update_interval_var = tk.StringVar(
            value=str(config.initial_update_interval_minutes)
        )
        self.n_clusters_var = tk.StringVar(
            value=str(config.initial_n_clusters)
        )
        self.timeframe_var = tk.StringVar(
            value=str(config.initial_timeframe_minutes)
        )
        self.pause_button_var = tk.StringVar(value="Pause updates")

        self.root.title("Real-Time High-Beta Clustering")
        self.root.geometry("1500x800")
        self.root.minsize(1050, 600)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self._build_widgets()

        self.worker = threading.Thread(
            target=self.worker_loop,
            name="realtime-high-beta-clustering-worker",
            daemon=True,
        )
        self.worker.start()
        self.root.after(250, self.process_queue)

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        controls = ttk.Frame(container)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(controls, text="Update interval (min):").pack(side=tk.LEFT)
        ttk.Entry(
            controls,
            textvariable=self.update_interval_var,
            width=6,
        ).pack(side=tk.LEFT, padx=(5, 12))
        ttk.Label(controls, text="STRUCTURE_N_CLUSTERS:").pack(side=tk.LEFT)
        ttk.Entry(
            controls,
            textvariable=self.n_clusters_var,
            width=6,
        ).pack(side=tk.LEFT, padx=(5, 12))
        ttk.Label(
            controls,
            text="STRUCTURE_TIMEFRAME_MINUTES:",
        ).pack(side=tk.LEFT)
        ttk.Entry(
            controls,
            textvariable=self.timeframe_var,
            width=6,
        ).pack(side=tk.LEFT, padx=(5, 12))
        ttk.Button(
            controls,
            text="Apply and refresh",
            command=self.apply_settings,
        ).pack(side=tk.LEFT)
        ttk.Button(
            controls,
            textvariable=self.pause_button_var,
            command=self.toggle_pause,
        ).pack(side=tk.LEFT, padx=(8, 0))

        panes = ttk.Panedwindow(container, orient=tk.HORIZONTAL)
        panes.grid(row=1, column=0, sticky="nsew")
        left_panel = ttk.Frame(panes, padding=(0, 0, 5, 0))
        right_panel = ttk.Frame(panes, padding=(5, 0, 0, 0))
        panes.add(left_panel, weight=3)
        panes.add(right_panel, weight=2)

        left_panel.columnconfigure(0, weight=1)
        left_panel.rowconfigure(1, weight=1)
        filter_frame = ttk.Frame(left_panel)
        filter_frame.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 6))
        ttk.Label(filter_frame, text="Table branch:").pack(side=tk.LEFT)
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
            left_panel,
            columns=TABLE_COLUMNS,
            show="headings",
            selectmode="browse",
        )
        widths = {
            SYMBOL_COLUMN: 80,
            BRANCH_COLUMN: 65,
            SIMILARITY_COLUMN: 90,
            CORRELATION_COLUMN: 90,
            RMSE_COLUMN: 85,
            BARS_COLUMN: 55,
            RVOL_COLUMN: 65,
            UPDATED_COLUMN: 145,
        }
        for column in TABLE_COLUMNS:
            self.tree.heading(
                column,
                text=column,
                command=lambda selected=column: self.sort_by_column(selected),
            )
            anchor = tk.W if column in {SYMBOL_COLUMN, UPDATED_COLUMN} else tk.E
            self.tree.column(
                column,
                width=widths[column],
                minwidth=50,
                anchor=anchor,
            )

        y_scroll = ttk.Scrollbar(
            left_panel,
            orient=tk.VERTICAL,
            command=self.tree.yview,
        )
        x_scroll = ttk.Scrollbar(
            left_panel,
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

        right_panel.columnconfigure(0, weight=1)
        right_panel.rowconfigure(1, weight=1)
        chart_controls = ttk.Frame(right_panel)
        chart_controls.grid(row=0, column=0, sticky="w", pady=(0, 6))
        ttk.Label(chart_controls, text="Medoid branch:").pack(side=tk.LEFT)
        self.chart_branch = ttk.Combobox(
            chart_controls,
            textvariable=self.chart_branch_var,
            values=(),
            state="readonly",
            width=16,
        )
        self.chart_branch.pack(side=tk.LEFT, padx=(6, 0))
        self.chart_branch.bind(
            "<<ComboboxSelected>>",
            lambda _event: self.render_medoid_chart(),
        )

        self.figure = Figure(figsize=(6.4, 6.4), dpi=100)
        self.price_axis = self.figure.add_subplot(211)
        self.volume_axis = self.figure.add_subplot(212, sharex=self.price_axis)
        self.canvas = FigureCanvasTkAgg(self.figure, master=right_panel)
        self.canvas.get_tk_widget().grid(row=1, column=0, sticky="nsew")
        self._draw_empty_chart("Waiting for the first clustering update")

        footer = ttk.Frame(container)
        footer.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        footer.columnconfigure(0, weight=1)
        ttk.Label(
            footer,
            textvariable=self.status_var,
            justify=tk.LEFT,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            footer,
            textvariable=self.count_var,
        ).grid(row=1, column=0, sticky="w")

    def get_settings(self) -> RuntimeSettings:
        with self.settings_lock:
            return self.settings

    def apply_settings(self) -> None:
        try:
            update_interval = int(self.update_interval_var.get())
            n_clusters = int(self.n_clusters_var.get())
            timeframe = int(self.timeframe_var.get())
            if update_interval < 1:
                raise ValueError("Update interval must be at least 1 minute.")
            if n_clusters < 2:
                raise ValueError("STRUCTURE_N_CLUSTERS must be at least 2.")
            if (
                self.universe_size is not None
                and n_clusters > self.universe_size
            ):
                raise ValueError(
                    "STRUCTURE_N_CLUSTERS cannot exceed the "
                    f"{self.universe_size} loaded high-beta symbols."
                )
            if timeframe < 1:
                raise ValueError(
                    "STRUCTURE_TIMEFRAME_MINUTES must be at least 1."
                )
            if REGULAR_SESSION_MINUTES % timeframe:
                raise ValueError(
                    "STRUCTURE_TIMEFRAME_MINUTES must divide evenly into "
                    f"{REGULAR_SESSION_MINUTES}."
                )
        except ValueError as error:
            messagebox.showerror("Invalid clustering settings", str(error))
            return

        with self.settings_lock:
            self.settings = RuntimeSettings(
                update_interval_minutes=update_interval,
                n_clusters=n_clusters,
                timeframe_minutes=timeframe,
            )
        self.recalculate_event.set()
        self.status_var.set("Settings applied; refreshing when data is eligible...")

    def toggle_pause(self) -> None:
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.recalculate_event.set()
            self.pause_button_var.set("Pause updates")
            self.status_var.set("Updates resumed; refreshing...")
        else:
            self.pause_event.set()
            self.pause_button_var.set("Resume updates")
            self.status_var.set("Updates paused; live data will continue buffering.")

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
        filter_values = (
            "All branches",
            *(f"Branch {branch}" for branch in branches),
        )
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
                match
                for match in all_matches
                if match.branch == selected_branch
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
                return (
                    float("-inf")
                    if self.sort_descending
                    else float("inf")
                )
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
            f"Branch {branch}: {branch_counts[branch]}"
            for branch in branches
        )
        if selected_filter != "All branches":
            count_parts.append(f"Shown: {len(matches)}")
        self.count_var.set(" | ".join(count_parts))

    def update_chart_branches(self) -> None:
        values = tuple(f"Branch {branch}" for branch in sorted(self.medoids))
        self.chart_branch.configure(values=values)
        selected = self.chart_branch_var.get()
        if selected not in values:
            self.chart_branch_var.set(values[0] if values else "")
        self.render_medoid_chart()

    def _draw_empty_chart(self, message: str) -> None:
        self.price_axis.clear()
        self.volume_axis.clear()
        self.price_axis.text(
            0.5,
            0.5,
            message,
            ha="center",
            va="center",
            transform=self.price_axis.transAxes,
        )
        self.price_axis.set_axis_off()
        self.volume_axis.set_axis_off()
        self.figure.tight_layout()
        self.canvas.draw_idle()

    def render_medoid_chart(self) -> None:
        selected = self.chart_branch_var.get()
        if not selected:
            self._draw_empty_chart("No medoid branch is available")
            return
        try:
            branch = int(selected.removeprefix("Branch "))
        except ValueError:
            self._draw_empty_chart("Select a valid medoid branch")
            return
        medoid = self.medoids.get(branch)
        if medoid is None or medoid.bars.empty:
            self._draw_empty_chart(f"No medoid is available for branch {branch}")
            return

        self.price_axis.clear()
        self.volume_axis.clear()
        self.price_axis.set_axis_on()
        self.volume_axis.set_axis_on()

        bars = medoid.bars.reset_index(drop=True)
        first_open = float(bars.iloc[0]["Open"])
        x_values = np.arange(len(bars))
        price_columns = ["Open", "High", "Low", "Close"]
        returns = bars[price_columns].astype(float) / first_open * 100 - 100
        colors = np.where(
            bars["Close"].to_numpy() >= bars["Open"].to_numpy(),
            "#22a447",
            "#df3030",
        )
        for index, color in enumerate(colors):
            low_value = float(returns.iloc[index]["Low"])
            high_value = float(returns.iloc[index]["High"])
            open_value = float(returns.iloc[index]["Open"])
            close_value = float(returns.iloc[index]["Close"])
            self.price_axis.vlines(
                index,
                low_value,
                high_value,
                color=color,
                linewidth=0.8,
            )
            body_bottom = min(open_value, close_value)
            body_height = max(abs(close_value - open_value), 0.002)
            self.price_axis.add_patch(
                Rectangle(
                    (index - 0.32, body_bottom),
                    0.64,
                    body_height,
                    facecolor=color,
                    edgecolor=color,
                    linewidth=0.6,
                )
            )

        self.price_axis.axhline(
            0,
            color="black",
            linewidth=0.8,
            linestyle="--",
            alpha=0.7,
        )
        self.price_axis.grid(alpha=0.2)
        self.price_axis.set_ylabel("Return from open (%)")
        self.price_axis.set_title(
            f"Branch {branch} medoid | {medoid.symbol} | "
            f"n={medoid.members} | return={medoid.session_return * 100:.2f}%"
        )

        volumes = bars["Volume"].to_numpy(dtype=float)
        self.volume_axis.bar(
            x_values,
            volumes,
            color=colors,
            width=0.64,
            alpha=0.35,
        )
        self.volume_axis.set_ylabel("Volume")
        self.volume_axis.grid(alpha=0.15)
        tick_count = min(8, len(bars))
        tick_indices = np.unique(
            np.linspace(0, len(bars) - 1, tick_count, dtype=int)
        )
        open_datetime = dt.datetime.combine(dt.date.today(), MARKET_OPEN)
        tick_labels = [
            (
                open_datetime
                + dt.timedelta(
                    minutes=int(index) * self.current_timeframe_minutes
                )
            ).strftime("%H:%M")
            for index in tick_indices
        ]
        self.volume_axis.set_xticks(tick_indices)
        self.volume_axis.set_xticklabels(tick_labels)
        self.volume_axis.set_xlabel("Time of day (ET)")
        self.price_axis.tick_params(labelbottom=False)
        self.figure.tight_layout()
        self.canvas.draw_idle()

    def process_queue(self) -> None:
        try:
            while True:
                message_type, payload = self.output_queue.get_nowait()
                if message_type == "status":
                    self.status_var.set(str(payload))
                elif message_type == "cluster":
                    result = payload
                    if not isinstance(result, ClusterResult):
                        continue
                    self.matches = result.matches
                    self.medoids = result.medoids
                    self.current_timeframe_minutes = result.timeframe_minutes
                    self.render_matches()
                    self.update_chart_branches()
                elif message_type == "clear":
                    self.matches = {}
                    self.medoids = {}
                    self.render_matches()
                    self.update_chart_branches()
                elif message_type == "universe":
                    self.universe_size = int(payload)
                elif message_type == "error":
                    self.status_var.set("Worker failed")
                    messagebox.showerror(
                        "Real-Time Clustering Failed",
                        str(payload),
                    )
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
        symbols = load_symbols(self.config.symbols_path)
        self.output_queue.put(("universe", len(symbols)))
        stream_engine = make_engine(STREAM_DATABASE)
        stocks_engine = make_stocks_engine()
        stream_rows = empty_stream_frame()
        last_seen_timestamp: pd.Timestamp | None = None
        rvol_profiles: dict[str, pd.Series] = {}
        rvol_profile_error: str | None = None
        last_rvol_profile_attempt = float("-inf")
        last_calculation_at: float | None = None
        current_date = dt.datetime.now(EASTERN).date()

        try:
            while not self.stop_event.is_set():
                now = dt.datetime.now(EASTERN).replace(tzinfo=None)
                if now.date() != current_date:
                    current_date = now.date()
                    stream_rows = empty_stream_frame()
                    last_seen_timestamp = None
                    rvol_profiles = {}
                    rvol_profile_error = None
                    last_rvol_profile_attempt = float("-inf")
                    last_calculation_at = None
                    self.recalculate_event.clear()
                    self.output_queue.put(("clear", current_date))

                market_open = dt.datetime.combine(current_date, MARKET_OPEN)
                market_close = dt.datetime.combine(
                    current_date,
                    REGULAR_MARKET_CLOSE,
                )
                settings = self.get_settings()
                processing_start = market_open + dt.timedelta(
                    minutes=self.config.start_delay_minutes
                )

                if now < market_open:
                    minutes_remaining = max(
                        0,
                        int((market_open - now).total_seconds() // 60),
                    )
                    self.output_queue.put(
                        (
                            "status",
                            f"Waiting for the 09:30 ET market open "
                            f"({minutes_remaining}m remaining)...",
                        )
                    )
                    self.stop_event.wait(
                        min(self.config.poll_interval_seconds, 5)
                    )
                    continue

                if now >= market_close:
                    self.output_queue.put(
                        (
                            "status",
                            f"Market closed; displaying final results for "
                            f"{current_date:%Y-%m-%d}.",
                        )
                    )
                    self.stop_event.wait(
                        min(self.config.poll_interval_seconds, 30)
                    )
                    continue

                if time.monotonic() - last_rvol_profile_attempt >= 60:
                    try:
                        refreshed_profiles = load_rvol_profiles(
                            stocks_engine,
                            symbols,
                            self.config.symbol_query_chunk_size,
                        )
                    except Exception as error:
                        rvol_profile_error = (
                            f"{type(error).__name__}: "
                            f"{str(error).splitlines()[0]}"
                        )
                    else:
                        if refreshed_profiles:
                            rvol_profiles = refreshed_profiles
                            rvol_profile_error = None
                    last_rvol_profile_attempt = time.monotonic()

                day_start = dt.datetime.combine(current_date, dt.time.min)
                query_start = day_start
                if last_seen_timestamp is not None:
                    query_start = max(
                        day_start,
                        last_seen_timestamp.to_pydatetime()
                        - dt.timedelta(
                            minutes=self.config.query_overlap_minutes
                        ),
                    )
                incoming = fetch_stream_rows(
                    stream_engine,
                    symbols=symbols,
                    start_timestamp=query_start,
                    end_timestamp=now + dt.timedelta(seconds=1),
                    symbol_chunk_size=self.config.symbol_query_chunk_size,
                )
                stream_rows = merge_stream_rows(stream_rows, incoming)
                if not incoming.empty:
                    last_seen_timestamp = pd.Timestamp(
                        incoming["Timestamp"].max()
                    )

                completed_bars = completed_bar_count(
                    now,
                    settings.timeframe_minutes,
                )
                if now < processing_start:
                    self.output_queue.put(
                        (
                            "status",
                            f"Buffering live data until "
                            f"{processing_start:%H:%M} ET...",
                        )
                    )
                elif completed_bars < self.config.minimum_feature_bars:
                    self.output_queue.put(
                        (
                            "status",
                            f"Buffering {settings.timeframe_minutes}-minute "
                            f"bars ({completed_bars}/"
                            f"{self.config.minimum_feature_bars} required)...",
                        )
                    )
                elif self.pause_event.is_set():
                    self.output_queue.put(
                        (
                            "status",
                            f"Updates paused | buffered rows: "
                            f"{len(stream_rows):,} | as of {now:%H:%M:%S} ET",
                        )
                    )
                else:
                    interval_seconds = (
                        settings.update_interval_minutes * 60
                    )
                    forced_refresh = self.recalculate_event.is_set()
                    update_due = (
                        forced_refresh
                        or last_calculation_at is None
                        or time.monotonic() - last_calculation_at
                        >= interval_seconds
                    )
                    if update_due:
                        settings = self.get_settings()
                        last_calculation_at = time.monotonic()
                        self.recalculate_event.clear()
                        try:
                            result = cluster_current_sessions(
                                stream_rows=stream_rows,
                                rvol_profiles=rvol_profiles,
                                as_of=now,
                                settings=settings,
                                config=self.config,
                            )
                        except InsufficientDataError as error:
                            self.output_queue.put(
                                (
                                    "status",
                                    f"{error} Next attempt in "
                                    f"{settings.update_interval_minutes}m.",
                                )
                            )
                        else:
                            self.output_queue.put(("cluster", result))
                            rvol_status = (
                                f"RVol profiles: {len(rvol_profiles)}"
                                if rvol_profiles
                                else (
                                    f"RVol unavailable ({rvol_profile_error})"
                                    if rvol_profile_error
                                    else "RVol unavailable"
                                )
                            )
                            self.output_queue.put(
                                (
                                    "status",
                                    f"Clustered {result.eligible_symbols}/"
                                    f"{len(symbols)} symbols into "
                                    f"{len(result.medoids)} branches | "
                                    f"PCA variance: "
                                    f"{result.explained_variance:.1%} | "
                                    f"{rvol_status} | "
                                    f"as of {now:%H:%M:%S} ET",
                                )
                            )

                self.stop_event.wait(self.config.poll_interval_seconds)
        finally:
            stream_engine.dispose()
            stocks_engine.dispose()

    def on_close(self) -> None:
        self.stop_event.set()
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Repeatedly cluster current-day high-beta intraday paths with "
            "PCA and Ward agglomerative clustering."
        )
    )
    parser.add_argument(
        "--symbols-path",
        type=Path,
        default=DEFAULT_SYMBOLS_PATH,
    )
    parser.add_argument(
        "--update-interval-minutes",
        type=int,
        default=5,
    )
    parser.add_argument("--n-clusters", type=int, default=6)
    parser.add_argument("--timeframe-minutes", type=int, default=3)
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    parser.add_argument("--query-overlap-minutes", type=int, default=2)
    parser.add_argument("--symbol-query-chunk-size", type=int, default=500)
    parser.add_argument("--start-delay-minutes", type=int, default=5)
    parser.add_argument("--minimum-feature-bars", type=int, default=3)
    parser.add_argument("--minimum-correlation-bars", type=int, default=3)
    parser.add_argument("--max-missing-bars", type=int, default=2)
    parser.add_argument("--pca-components", type=int, default=40)
    parser.add_argument(
        "--shape-weight",
        type=float,
        default=0.7,
        help="Correlation-distance weight; the remainder weights return RMSE.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.update_interval_minutes < 1:
        raise ValueError("--update-interval-minutes must be at least one")
    if args.n_clusters < 2:
        raise ValueError("--n-clusters must be at least two")
    if args.timeframe_minutes < 1:
        raise ValueError("--timeframe-minutes must be at least one")
    if REGULAR_SESSION_MINUTES % args.timeframe_minutes:
        raise ValueError(
            "--timeframe-minutes must divide evenly into "
            f"{REGULAR_SESSION_MINUTES}"
        )
    if args.poll_interval_seconds <= 0:
        raise ValueError("--poll-interval-seconds must be greater than zero")
    if args.query_overlap_minutes < 0:
        raise ValueError("--query-overlap-minutes cannot be negative")
    if args.symbol_query_chunk_size < 1:
        raise ValueError("--symbol-query-chunk-size must be at least one")
    if args.start_delay_minutes < 0:
        raise ValueError("--start-delay-minutes cannot be negative")
    if args.minimum_feature_bars < 2:
        raise ValueError("--minimum-feature-bars must be at least two")
    if args.minimum_correlation_bars < 2:
        raise ValueError("--minimum-correlation-bars must be at least two")
    if args.max_missing_bars < 0:
        raise ValueError("--max-missing-bars cannot be negative")
    if args.pca_components < 1:
        raise ValueError("--pca-components must be at least one")
    if not 0 <= args.shape_weight <= 1:
        raise ValueError("--shape-weight must be between zero and one")


def main() -> None:
    args = parse_args()
    validate_args(args)
    config = AppConfig(
        symbols_path=args.symbols_path,
        poll_interval_seconds=args.poll_interval_seconds,
        query_overlap_minutes=args.query_overlap_minutes,
        symbol_query_chunk_size=args.symbol_query_chunk_size,
        start_delay_minutes=args.start_delay_minutes,
        minimum_feature_bars=args.minimum_feature_bars,
        minimum_correlation_bars=args.minimum_correlation_bars,
        max_missing_bars=args.max_missing_bars,
        pca_components=args.pca_components,
        shape_weight=args.shape_weight,
        initial_update_interval_minutes=args.update_interval_minutes,
        initial_n_clusters=args.n_clusters,
        initial_timeframe_minutes=args.timeframe_minutes,
    )
    root = tk.Tk()
    app = RealtimeHighBetaClusteringGUI(root, config)
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
