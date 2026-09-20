"""Interactive stock neighborhoods using the pairs_trading notebook's returns.

Run from the market_data repository: python scripts/network_analysis_gui.py
Only the existing daily price sources are read; no database writes or disk cache.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import queue
import sys
import threading
import time
import tkinter as tk
from concurrent.futures import CancelledError, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Callable, Iterable

import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from joblib import Parallel, delayed, parallel_config
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))


PAIR_PROCESS_THRESHOLD = 2048
PAIR_BATCH_SIZE = 64
MAX_PAIR_PROCESSES = 8


@dataclass(frozen=True)
class ProgressUpdate:
    message: str
    phase: str
    completed: int | None = None
    total: int | None = None
    timestamp: float = field(default_factory=time.perf_counter)

    def __str__(self) -> str:
        return self.message


@dataclass
class ProgressState:
    started_at: float = field(default_factory=time.perf_counter)
    update: ProgressUpdate | None = None
    phase_started: float | None = None

    def accept(self, update: ProgressUpdate) -> None:
        if self.update is None or update.phase != self.update.phase:
            self.phase_started = update.timestamp
        self.update = update

    @property
    def percent(self) -> float | None:
        update = self.update
        if update is None or update.completed is None or update.total is None:
            return None
        if update.total == 0:
            return 100.0
        return min(100.0, max(0.0, update.completed * 100.0 / update.total))

    def eta_seconds(self, now: float) -> float | None:
        update = self.update
        if (update is None or not update.total or not update.completed
                or self.phase_started is None):
            return None
        if update.completed >= update.total:
            return 0.0
        measured = update.timestamp - self.phase_started
        # Avoid presenting an estimate from a fraction of a second of work.
        if measured < 1.0:
            return None
        remaining = measured * (update.total - update.completed) / update.completed
        return max(1.0, remaining - max(0.0, now - update.timestamp))


@dataclass
class SymbolHistory:
    limit: int = 10
    symbols: list[str] = field(default_factory=list)
    position: int = -1

    def visit(self, symbol: str) -> None:
        symbol = symbol.strip().upper()
        if self.position >= 0 and self.symbols[self.position] == symbol:
            return
        self.symbols = self.symbols[:self.position + 1] + [symbol]
        self.symbols = self.symbols[-self.limit:]
        self.position = len(self.symbols) - 1

    def target(self, direction: int) -> tuple[int, str] | None:
        position = self.position + direction
        if 0 <= position < len(self.symbols):
            return position, self.symbols[position]
        return None


@dataclass(frozen=True)
class PriceData:
    returns: pd.DataFrame
    source: str
    elapsed: float
    detail: str = ""


def _check_load_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise CancelledError()


def _close_series_returns(close: pd.Series, lookback: int) -> pd.Series:
    """Calculate each symbol's returns before aligning symbols by trading date."""
    close = pd.to_numeric(close, errors="coerce").copy()
    close.index = pd.to_datetime(close.index, errors="coerce")
    close = close.loc[close.index.notna()]
    close = close.loc[~close.index.duplicated(keep="last")].sort_index()
    close = close.tail(lookback + 1)
    # A missing close must not become an invented zero return through forward fill.
    return close.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan).tail(lookback)


def _assemble_returns(series: dict[str, pd.Series]) -> pd.DataFrame:
    if not series:
        raise ValueError("No daily closing prices were found in the selected source.")
    returns = pd.DataFrame(series).sort_index()
    returns = returns.dropna(axis=0, how="all").dropna(axis=1, how="all")
    returns.index.name = "Date"
    if returns.empty:
        raise ValueError("Daily prices contain no usable returns; at least two closes are needed.")
    return returns


def _load_mysql_returns(
    lookback: int,
    cancel_event: threading.Event | None,
    progress: Callable[[ProgressUpdate], None],
) -> pd.DataFrame:
    # api_keys.py uses this same environment variable. Reading it directly keeps
    # loading three price columns independent of market_data's expensive imports.
    password = os.environ.get("DATABASE_PASSWORD")
    if not password:
        raise RuntimeError("The DATABASE_PASSWORD environment variable is not set.")
    url = URL.create(
        "mysql+pymysql", username="root", password=password,
        host="127.0.0.1", port=3306, database="daily_ohlcv",
    )
    engine = create_engine(
        url, pool_pre_ping=True,
        connect_args={"connect_timeout": 5, "read_timeout": 180},
    )
    # Rank within each symbol instead of imposing one calendar cutoff: suspended
    # and newly listed symbols retain the notebook's individual history windows.
    # Only these three columns cross the connection, not the wide technical table.
    query = text(
        """
        SELECT symbol, date, close
        FROM (
            SELECT symbol, date, close,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_num
            FROM daily_symbol_bars
        ) AS recent
        WHERE row_num <= :observations
        """
    )
    frames: list[pd.DataFrame] = []
    count = 0
    try:
        progress(ProgressUpdate(
            "Reading recent daily closes from MySQL; the first query may take a minute...",
            "mysql-read",
        ))
        _check_load_cancelled(cancel_event)
        with engine.connect().execution_options(stream_results=True) as connection:
            for chunk in pd.read_sql_query(
                query, con=connection, params={"observations": lookback + 1},
                chunksize=100_000,
            ):
                _check_load_cancelled(cancel_event)
                frames.append(chunk)
                count += len(chunk)
                progress(ProgressUpdate(f"Read {count:,} daily closes from MySQL...", "mysql-read"))
    except CancelledError:
        raise
    except Exception as exc:
        # Never expose a connection URL/password or a driver traceback in the UI.
        original = getattr(exc, "orig", None)
        code = original.args[0] if original is not None and original.args else None
        suffix = f"; database error {code}" if isinstance(code, int) else ""
        raise RuntimeError(
            f"MySQL daily prices could not be loaded ({type(exc).__name__}{suffix}). "
            "Check the local MySQL server and DATABASE_PASSWORD, or choose Saved symbols."
        ) from None
    finally:
        engine.dispose()
    _check_load_cancelled(cancel_event)
    if not frames:
        raise ValueError("daily_ohlcv.daily_symbol_bars contains no daily prices.")
    progress(ProgressUpdate("Grouping MySQL daily closes by symbol...", "mysql-grouping"))
    _check_load_cancelled(cancel_event)
    bars = pd.concat(frames, ignore_index=True)
    del frames
    series: dict[str, pd.Series] = {}
    grouped = bars.groupby("symbol", sort=False)
    total = grouped.ngroups
    progress(ProgressUpdate("Calculating daily returns for each symbol...", "mysql-returns", 0, total))
    for number, (symbol, group) in enumerate(grouped, start=1):
        _check_load_cancelled(cancel_event)
        name = str(symbol).strip().upper()
        if name:
            series[name] = _close_series_returns(group.set_index("date")["close"], lookback)
        if number % 50 == 0 or number == total:
            progress(ProgressUpdate(
                f"Calculated returns for {number:,}/{total:,} MySQL symbols...",
                "mysql-returns", number, total,
            ))
    progress(ProgressUpdate("Aligning daily returns by trading date...", "mysql-alignment"))
    _check_load_cancelled(cancel_event)
    return _assemble_returns(series)


def _load_saved_returns(
    lookback: int,
    cancel_event: threading.Event | None,
    progress: Callable[[ProgressUpdate], None],
) -> pd.DataFrame:
    # Import the scripts loader by path so merely opening this GUI does not import
    # the whole market_data package. Unpickling still uses the actual SymbolData.
    loader_path = Path(__file__).with_name("load_daily_variables.py")
    spec = importlib.util.spec_from_file_location("_network_daily_loader", loader_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("The daily variables loader could not be opened.")
    loader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loader)
    saved_path = Path(loader.BASE) / "symbols.pkl.gz"
    if not saved_path.is_file():
        raise FileNotFoundError(f"Saved symbols were not found at {saved_path}.")
    size_gb = saved_path.stat().st_size / (1024 ** 3)
    progress(ProgressUpdate(
        f"Loading saved symbols ({size_gb:.1f} GB compressed); this can take several minutes...",
        "saved-read",
    ))
    _check_load_cancelled(cancel_event)
    loaded = loader.load_all(["symbols"])
    _check_load_cancelled(cancel_event)
    symbols = loaded.get("symbols")
    if not isinstance(symbols, dict) or not symbols:
        raise RuntimeError("The daily variables loader did not return a symbols dictionary.")
    series: dict[str, pd.Series] = {}
    total = len(symbols)
    progress(ProgressUpdate("Calculating daily returns for each symbol...", "saved-returns", 0, total))
    for number, (symbol, data) in enumerate(symbols.items(), start=1):
        _check_load_cancelled(cancel_event)
        frame = data if isinstance(data, pd.DataFrame) else getattr(data, "df", None)
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            column = next((column for column in frame.columns if str(column).lower() == "close"), None)
            name = str(symbol).strip().upper()
            if column is not None and name:
                series[name] = _close_series_returns(frame[column], lookback)
        if number % 50 == 0 or number == total:
            progress(ProgressUpdate(
                f"Processed returns for {number:,}/{total:,} saved symbols...",
                "saved-returns", number, total,
            ))
    progress(ProgressUpdate("Aligning daily returns by trading date...", "saved-alignment"))
    _check_load_cancelled(cancel_event)
    return _assemble_returns(series)


def load_price_data(
    source: str = "Auto",
    lookback: int = 252,
    cancel_event: threading.Event | None = None,
    progress: Callable[[ProgressUpdate], None] | None = None,
) -> PriceData:
    """Load only closing prices, then retain up to lookback returns per symbol.

    Auto prefers MySQL's narrow result over decompressing the multi-gigabyte saved
    SymbolData dictionary; the saved symbols-only loader remains a fallback.
    """
    if source not in {"Auto", "MySQL", "Saved symbols"}:
        raise ValueError("Choose Auto, MySQL, or Saved symbols as the data source.")
    if isinstance(lookback, bool) or not isinstance(lookback, int) or lookback < 2:
        raise ValueError("History must contain at least 2 daily returns.")
    report = progress or (lambda message: None)
    started = time.perf_counter()
    _check_load_cancelled(cancel_event)
    sources = ["MySQL", "Saved symbols"] if source == "Auto" else [source]
    errors: list[str] = []
    for candidate in sources:
        try:
            loader = _load_mysql_returns if candidate == "MySQL" else _load_saved_returns
            returns = loader(lookback, cancel_event, report)
            _check_load_cancelled(cancel_event)
            detail = ""
            if errors:
                detail = "MySQL was unavailable; loaded saved symbols instead."
            return PriceData(returns, candidate, time.perf_counter() - started, detail)
        except CancelledError:
            raise
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")
            if source == "Auto" and candidate == "MySQL":
                report(ProgressUpdate(
                    "MySQL was unavailable; trying the saved symbols-only loader...", "price-source",
                ))
    raise RuntimeError("\n".join(errors))


@dataclass(frozen=True)
class AnalysisSettings:
    base_corr: float = 0.65
    high_corr: float = 0.8
    min_dur: int = 5
    rolling_window: int = 20
    lookback: int = 252
    max_neighbors: int = 40

    def validate(self) -> None:
        for name, value in (("Base correlation", self.base_corr), ("High correlation", self.high_corr)):
            if not np.isfinite(value) or not -1 <= value <= 1:
                raise ValueError(f"{name} must be a finite number between -1 and 1.")
        if self.high_corr < self.base_corr:
            raise ValueError("High correlation must be at least the base correlation.")
        for name, value, minimum in (
            ("Minimum duration", self.min_dur, 1),
            ("Rolling window", self.rolling_window, 2),
            ("Return lookback", self.lookback, 2),
            ("Graph neighbors", self.max_neighbors, 1),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, np.integer)) or value < minimum:
                raise ValueError(f"{name} must be an integer of at least {minimum}.")
        if self.lookback < self.rolling_window + self.min_dur - 1:
            raise ValueError("Return lookback must cover the rolling window plus minimum duration minus one.")


@dataclass(frozen=True)
class CorrelationEpisode:
    start: pd.Timestamp
    end: pd.Timestamp
    duration: int
    peak: float


@dataclass(frozen=True)
class PairCorrelation:
    symbol_a: str
    symbol_b: str
    baseline: float
    rolling: pd.Series
    episodes: tuple[CorrelationEpisode, ...]

    @property
    def longest(self) -> int:
        return max(episode.duration for episode in self.episodes)

    @property
    def latest(self) -> float:
        # Do not silently substitute an older value for an undefined latest window.
        return float(self.rolling.iloc[-1])

    def other(self, symbol: str) -> str:
        return self.symbol_b if symbol == self.symbol_a else self.symbol_a


@dataclass(frozen=True)
class NeighborhoodResult:
    symbol: str
    settings: AnalysisSettings
    pairs: tuple[PairCorrelation, ...]
    edges: tuple[PairCorrelation, ...]
    neighbors: tuple[str, ...]
    candidate_count: int


def find_episodes(
    rolling: pd.Series, high_corr: float, min_dur: int,
) -> tuple[CorrelationEpisode, ...]:
    """Keep complete runs, including one ending on the final observation."""
    mask = (np.isfinite(rolling) & rolling.ge(high_corr)).to_numpy(dtype=bool)
    boundaries = np.diff(np.r_[False, mask, False].astype(np.int8))
    starts = np.flatnonzero(boundaries == 1)
    stops = np.flatnonzero(boundaries == -1)
    return tuple(
        CorrelationEpisode(
            rolling.index[start], rolling.index[stop - 1], int(stop - start),
            float(rolling.iloc[start:stop].max()),
        )
        for start, stop in zip(starts, stops)
        if stop - start >= min_dur
    )


def evaluate_pair(
    returns: pd.DataFrame, symbol_a: str, symbol_b: str, settings: AnalysisSettings,
) -> PairCorrelation | None:
    """Notebook Pearson/rolling calculation on shared, finite daily returns.

    Missing observations are dropped per pair, as in the notebook. Durations
    therefore count consecutive *shared observations*, not calendar days.
    The two thresholds are inclusive: an observation meeting a threshold counts.
    """
    if symbol_a == symbol_b:
        return None
    pair = returns.loc[:, [symbol_a, symbol_b]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(pair) < settings.rolling_window + settings.min_dur - 1:
        return None
    baseline = float(pair.iloc[:, 0].corr(pair.iloc[:, 1]))
    if not np.isfinite(baseline) or baseline < settings.base_corr:
        return None
    rolling = pair.iloc[:, 0].rolling(
        settings.rolling_window, min_periods=settings.rolling_window,
    ).corr(pair.iloc[:, 1]).replace([np.inf, -np.inf], np.nan).clip(-1, 1)
    episodes = find_episodes(rolling, settings.high_corr, settings.min_dur)
    if not episodes:
        return None
    return PairCorrelation(symbol_a, symbol_b, baseline, rolling, episodes)


def _evaluate_pair_batch(values, dates, symbols, pairs, settings):
    """Process entry point: shared numeric data, one frame per bounded batch."""
    returns = pd.DataFrame(values, index=dates, columns=symbols, copy=False)
    kept = []
    for left, right in pairs:
        pair = evaluate_pair(returns, left, right, settings)
        if pair is not None:
            kept.append(pair)
    return len(pairs), kept


def _evaluate_pairs(
    returns, pairs, settings, cancel_event=None,
    progress: Callable[[ProgressUpdate], None] | None = None,
    description="Checking rolling episodes",
):
    """Use processes only when there is enough work to amortize startup.

    Keep the same evaluator and input order on both paths. Joblib shares large
    numeric arrays through read-only temporary memory maps, instead of sending
    a copy of the entire price universe with each pair. Its loky backend works
    from the GUI's background thread on Windows and reuses workers across calls.
    """
    def checkpoint(completed):
        _check_load_cancelled(cancel_event)
        if progress:
            progress(ProgressUpdate(
                f"{description}: {completed:,}/{len(pairs):,} candidates",
                description, completed, len(pairs),
            ))
        _check_load_cancelled(cancel_event)

    checkpoint(0)
    workers = min(MAX_PAIR_PROCESSES, os.cpu_count() or 1)
    if len(pairs) < PAIR_PROCESS_THRESHOLD or workers < 2:
        kept = []
        for number, (left, right) in enumerate(pairs, start=1):
            _check_load_cancelled(cancel_event)
            pair = evaluate_pair(returns, left, right, settings)
            if pair is not None:
                kept.append(pair)
            checkpoint(number)
        _check_load_cancelled(cancel_event)
        return kept

    values = returns.to_numpy(dtype=np.float64, copy=False)
    batches = (pairs[start:start + PAIR_BATCH_SIZE] for start in range(0, len(pairs), PAIR_BATCH_SIZE))
    kept = []
    completed = 0
    # Limit native-library threads inside each process to avoid oversubscribing
    # the machine. Pre-dispatch and batches bound outstanding work on Cancel.
    with parallel_config(backend="loky", inner_max_num_threads=1):
        with Parallel(
            n_jobs=workers, return_as="generator", batch_size=1,
            pre_dispatch="2*n_jobs", max_nbytes="1M", mmap_mode="r",
        ) as pool:
            results = pool(
                delayed(_evaluate_pair_batch)(
                    values, returns.index, returns.columns, batch, settings,
                )
                for batch in batches
            )
            try:
                for count, batch_kept in results:
                    checkpoint(completed + count)
                    kept.extend(batch_kept)
                    completed += count
            finally:
                # Cancelling or closing the GUI stops dispatch and disposes of
                # this call's memory maps; partial graphs are never published.
                results.close()
    _check_load_cancelled(cancel_event)
    return kept


def analyze_neighborhood(
    returns: pd.DataFrame, symbol: str, settings: AnalysisSettings,
    cancel_event: threading.Event | None = None,
    progress: Callable[[ProgressUpdate], None] | None = None,
) -> NeighborhoodResult:
    """Compute the selected row of the correlation matrix, then its neighborhood.

    This is equivalent to selecting that row from returns.corr(), without
    allocating a universe-squared matrix. Every candidate of the center is
    screened. The graph cap affects display and neighbor-to-neighbor work only.
    """
    settings.validate()
    symbol = symbol.strip().upper()
    if symbol not in returns.columns:
        raise ValueError(f"{symbol or 'The selected symbol'} is not in the loaded price universe.")

    def checkpoint(update: ProgressUpdate | None = None):
        _check_load_cancelled(cancel_event)
        if update is not None and progress:
            progress(update)
        _check_load_cancelled(cancel_event)

    checkpoint(ProgressUpdate(
        f"Calculating {symbol} baseline correlations against {len(returns.columns):,} symbols...",
        "center-baselines",
    ))
    clean = returns.replace([np.inf, -np.inf], np.nan)
    baselines = clean.corrwith(clean[symbol]).drop(labels=[symbol]).dropna()
    candidates = baselines[baselines.ge(settings.base_corr)].sort_values(ascending=False)
    pairs = _evaluate_pairs(
        clean, [(symbol, neighbor) for neighbor in candidates.index], settings,
        cancel_event, progress, f"Checking {symbol} rolling episodes",
    )
    pairs.sort(key=lambda pair: (-pair.baseline, pair.other(symbol)))
    neighbors = tuple(pair.other(symbol) for pair in pairs[:settings.max_neighbors])
    edges = list(pairs[:settings.max_neighbors])
    # A true induced neighborhood: neighbors connect only if they pass the same
    # baseline AND episode rules. Connections are never inferred transitively.
    if len(neighbors) > 1:
        checkpoint(ProgressUpdate(
            "Calculating baseline correlations between displayed neighbors...", "neighbor-baselines",
        ))
        local = clean.loc[:, list(neighbors)]
        correlations = local.corr()
        neighbor_pairs = []
        total = len(neighbors) * (len(neighbors) - 1) // 2
        completed = 0
        checkpoint(ProgressUpdate(
            "Selecting baseline candidates between displayed neighbors...", "neighbor-candidates", 0, total,
        ))
        for row, left in enumerate(neighbors[:-1]):
            checkpoint()
            for right in neighbors[row + 1:]:
                checkpoint()
                value = correlations.at[left, right]
                if np.isfinite(value) and value >= settings.base_corr:
                    neighbor_pairs.append((left, right))
                completed += 1
                if completed % 250 == 0 or completed == total:
                    checkpoint(ProgressUpdate(
                        f"Selected baseline candidates from {completed:,}/{total:,} neighbor pairs...",
                        "neighbor-candidates", completed, total,
                    ))
        edges.extend(_evaluate_pairs(
            local, neighbor_pairs, settings, cancel_event, progress,
            "Checking connections between displayed neighbors",
        ))
    checkpoint()
    return NeighborhoodResult(symbol, settings, tuple(pairs), tuple(edges), neighbors, len(candidates))


@dataclass(frozen=True)
class SpreadData:
    symbol: str
    focal_symbol: str
    spread: pd.Series
    current: float
    current_date: pd.Timestamp | None
    cumulative: float
    mean: float
    std: float
    count: int


def parse_spread_dates(
    start_date: str | None = None, end_date: str | None = None,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """Validate optional, inclusive calendar dates without expanding loaded data."""
    from datetime import date

    bounds = []
    for label, value in (("Start", start_date), ("End", end_date)):
        value = value.strip() if value is not None else ""
        if not value:
            bounds.append(None)
            continue
        try:
            parsed = date.fromisoformat(value)
            if parsed.isoformat() != value:
                raise ValueError
            bounds.append(pd.Timestamp(parsed))
        except ValueError as exc:
            raise ValueError(f"{label} date must be a valid date in YYYY-MM-DD format.") from exc
    start, end = bounds
    if start is not None and end is not None and start > end:
        raise ValueError("Start date must be on or before end date.")
    return start, end


def calculate_spreads(
    returns: pd.DataFrame, focal_symbol: str, neighbors: Iterable[str],
    start_date: str | None = None, end_date: str | None = None,
) -> tuple[SpreadData, ...]:
    """Subtract each neighbor's daily return from the focal stock's daily return.

    The input already reflects Return lookback. Calendar bounds only select
    from those observations, and missing returns remain gaps in the chart.
    Cumulative spread is the arithmetic sum, not a compounded return. Range
    statistics use shared finite observations; current uses the latest shared
    finite observation inside the selected date range.
    """
    start, end = parse_spread_dates(start_date, end_date)
    focal_symbol = focal_symbol.strip().upper()
    if focal_symbol not in returns.columns:
        raise ValueError(f"{focal_symbol or 'The selected symbol'} is not in the loaded price universe.")
    symbols = tuple(dict.fromkeys(
        symbol.strip().upper() for symbol in neighbors
        if symbol.strip().upper() != focal_symbol
    ))
    for symbol in symbols:
        if symbol not in returns.columns:
            raise ValueError(f"{symbol or 'A correlated symbol'} is not in the loaded price universe.")
    clean = returns.loc[:, [focal_symbol, *symbols]].sort_index().replace([np.inf, -np.inf], np.nan)
    dates = pd.DatetimeIndex(clean.index).normalize()
    selected = np.ones(len(clean), dtype=bool)
    if start is not None:
        selected &= dates >= (start.tz_localize(dates.tz) if dates.tz is not None else start)
    if end is not None:
        selected &= dates <= (end.tz_localize(dates.tz) if dates.tz is not None else end)
    results = []
    for symbol in symbols:
        full = (clean[focal_symbol] - clean[symbol]).replace([np.inf, -np.inf], np.nan).rename(symbol)
        spread = full.loc[selected]
        finite = spread.dropna()
        count = len(finite)
        results.append(SpreadData(
            symbol=symbol, focal_symbol=focal_symbol, spread=spread,
            current=float(finite.iloc[-1]) if count else float("nan"),
            current_date=pd.Timestamp(finite.index[-1]) if count else None,
            cumulative=float(finite.sum()) if count else float("nan"),
            mean=float(finite.mean()) if count else float("nan"),
            std=float(finite.std(ddof=1)) if count > 1 else float("nan"),
            count=count,
        ))
    return tuple(results)


class NetworkAnalysisApp:
    POLL_MS = 100
    BLUE = "#247b9e"
    ORANGE = "#d78b24"

    def __init__(self, root: tk.Tk, initial_symbol: str = "PANW") -> None:
        self.root = root
        self.root.title("Intermittent Correlation Network")
        self.root.geometry(f"{min(1840, max(1300, root.winfo_screenwidth() - 80))}x940")
        self.root.minsize(1300, 850)
        self.root.protocol("WM_DELETE_WINDOW", self.close)
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="correlation")
        self.future = None
        self.cancel_event = threading.Event()
        self.messages = queue.SimpleQueue()
        self.after_id = None
        self.busy = False
        self.history = SymbolHistory()
        self.pending_history_index = None
        self.progress_state = ProgressState()
        self.data = None
        self.data_key = None
        self.displayed_data = None
        self.result = None
        self.selected_pair = None
        self.positions = {}
        self.node_artists = {}
        self.edge_artists = []
        self.drag_node = None
        self.hover_text = ""
        self.row_pairs = {}
        self.sort_descending = {}
        self.spread_rows = {}
        self.spread_selected_symbol = None
        self.spread_range = (None, None)
        self.spread_sort_descending = {}
        self.source_var = tk.StringVar(value="Auto")
        self.symbol_var = tk.StringVar(value=initial_symbol.upper())
        self.base_var = tk.StringVar(value="0.65")
        self.high_var = tk.StringVar(value="0.8")
        self.duration_var = tk.StringVar(value="5")
        self.window_var = tk.StringVar(value="20")
        self.lookback_var = tk.StringVar(value="252")
        self.limit_var = tk.StringVar(value="40")
        self.status_var = tk.StringVar(value="Enter a stock symbol, then select Analyze.")
        self.progress_var = tk.StringVar(value="Ready")
        self.data_var = tk.StringVar(value="Daily percentage returns • Pearson correlation • no self-pairs")
        self.graph_var = tk.StringVar(value="Stock neighborhood")
        self.pair_var = tk.StringVar(value="Select a node, edge, or table row to inspect a pair.")
        self.neighbor_var = tk.StringVar(value="Qualifying neighbors")
        self.spread_heading_var = tk.StringVar(value="Return spreads")
        self.spread_direction_var = tk.StringVar(value="Focal return − neighbor return • percentage points (pp)")
        self.spread_custom_var = tk.BooleanVar(value=False)
        self.spread_start_var = tk.StringVar()
        self.spread_end_var = tk.StringVar()
        self.spread_period_var = tk.StringVar(value="Full return lookback • dates use YYYY-MM-DD")
        self.spread_summary_var = tk.StringVar(value="Select a spread row to inspect its daily history.")
        self._build_widgets()
        self.root.bind("<Alt-Left>", lambda event: self.navigate_history(-1) or "break")
        self.root.bind("<Alt-Right>", lambda event: self.navigate_history(1) or "break")

    def _build_widgets(self) -> None:
        frame = ttk.Frame(self.root, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(3, weight=1)
        header = ttk.Frame(frame)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="Intermittent correlation", font=("Segoe UI", 17, "bold")).grid(row=0, column=0, sticky="w")
        self.back_button = ttk.Button(
            header, text="← Back", command=lambda: self.navigate_history(-1), state=tk.DISABLED,
        )
        self.back_button.grid(row=0, column=1, padx=(8, 4))
        self.forward_button = ttk.Button(
            header, text="Forward →", command=lambda: self.navigate_history(1), state=tk.DISABLED,
        )
        self.forward_button.grid(row=0, column=2)
        ttk.Label(frame, textvariable=self.data_var).grid(row=1, column=0, sticky="w", pady=(2, 8))
        controls = ttk.LabelFrame(frame, text="Daily returns and thresholds", padding=8)
        controls.grid(row=2, column=0, sticky="ew")
        self.inputs = []
        fields = (
            ("Stock", self.symbol_var, 12),
            ("Base correlation ≥", self.base_var, 13),
            ("High correlation ≥", self.high_var, 13),
            ("Min. duration", self.duration_var, 12),
            ("Rolling window", self.window_var, 12),
            ("Return lookback", self.lookback_var, 12),
            ("Graph neighbors", self.limit_var, 12),
        )
        for column, (label, variable, width) in enumerate(fields):
            ttk.Label(controls, text=label).grid(row=0, column=column, sticky="w")
            entry = ttk.Entry(controls, textvariable=variable, width=width)
            entry.grid(row=1, column=column, sticky="ew", padx=(0, 8))
            entry.bind("<Return>", lambda event: self.analyze())
            controls.columnconfigure(column, weight=1)
            self.inputs.append(entry)
        ttk.Label(controls, text="Data source").grid(row=0, column=7, sticky="w")
        self.source_box = ttk.Combobox(
            controls, textvariable=self.source_var, values=("Auto", "MySQL", "Saved symbols"),
            state="readonly", width=14,
        )
        self.source_box.grid(row=1, column=7, sticky="ew")
        actions = ttk.Frame(controls)
        actions.grid(row=2, column=0, columnspan=8, sticky="ew", pady=(8, 0))
        self.analyze_button = ttk.Button(actions, text="Analyze", command=self.analyze)
        self.analyze_button.pack(side=tk.LEFT)
        self.reload_button = ttk.Button(actions, text="Reload prices", command=lambda: self.analyze(force_reload=True))
        self.reload_button.pack(side=tk.LEFT, padx=6)
        self.cancel_button = ttk.Button(actions, text="Cancel", command=self.cancel, state=tk.DISABLED)
        self.cancel_button.pack(side=tk.LEFT)
        ttk.Label(actions, text="Duration and window count shared trading observations.").pack(side=tk.LEFT, padx=12)
        ttk.Button(actions, text="Method", command=self.show_method).pack(side=tk.RIGHT)

        split = ttk.Panedwindow(frame, orient=tk.HORIZONTAL)
        split.grid(row=3, column=0, sticky="nsew", pady=8)
        graph = ttk.Frame(split, padding=5)
        inspector = ttk.Frame(split, padding=5)
        spread_panel = ttk.Frame(split, padding=5)
        graph.columnconfigure(0, weight=1)
        graph.rowconfigure(2, weight=1)
        inspector.columnconfigure(0, weight=1)
        inspector.rowconfigure(4, weight=1)
        split.add(graph, weight=3)
        split.add(inspector, weight=2)
        split.add(spread_panel, weight=2)
        ttk.Label(graph, textvariable=self.graph_var, font=("Segoe UI", 11, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(graph, text="Drag nodes • scroll to zoom • click to inspect\nDouble-click a node to recenter").grid(row=1, column=0, sticky="w", pady=4)
        self.graph_figure = Figure(figsize=(7.4, 6.5), dpi=100, facecolor="#f7f9fc")
        self.graph_axes = self.graph_figure.add_subplot(111)
        self.graph_figure.subplots_adjust(left=0.02, right=0.98, bottom=0.02, top=0.98)
        self.graph_canvas = FigureCanvasTkAgg(self.graph_figure, master=graph)
        self.graph_canvas.get_tk_widget().grid(row=2, column=0, sticky="nsew")
        toolbar_frame = ttk.Frame(graph)
        toolbar_frame.grid(row=3, column=0, sticky="ew")
        self.graph_toolbar = NavigationToolbar2Tk(self.graph_canvas, toolbar_frame, pack_toolbar=False)
        self.graph_toolbar.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(toolbar_frame, text="Reset layout", command=self.reset_layout).pack(side=tk.RIGHT)
        ttk.Label(graph, text="Orange: selected stock   •   Thick edges: stronger baseline\nSolid: links to selected stock   •   Dashed: links between neighbors").grid(row=4, column=0, sticky="w")
        self.hover_var = tk.StringVar(value=" ")
        ttk.Label(graph, textvariable=self.hover_var, wraplength=560).grid(row=5, column=0, sticky="w", pady=(4, 0))
        self._empty_graph("Load daily prices to explore a stock's correlation neighborhood.")
        for event, callback in (
            ("button_press_event", self._on_press),
            ("button_release_event", self._on_release),
            ("motion_notify_event", self._on_motion),
            ("scroll_event", self._on_scroll),
        ):
            self.graph_canvas.mpl_connect(event, callback)

        ttk.Label(inspector, textvariable=self.neighbor_var, font=("Segoe UI", 11, "bold")).grid(row=0, column=0, sticky="w")
        self.neighbor_tree = self._make_tree(
            inspector,
            (("symbol", "Symbol", 75), ("base", "Base", 62), ("latest", "Latest", 62),
             ("longest", "Longest", 65), ("episodes", "Episodes", 65)), height=5, row=1,
        )
        for column in self.neighbor_tree["columns"]:
            self.neighbor_tree.heading(column, command=lambda col=column: self._sort_neighbors(col))
        self.neighbor_tree.bind("<<TreeviewSelect>>", self._on_row)
        self.neighbor_tree.bind("<Double-1>", self._on_row_recenter)
        ttk.Label(inspector, text="All matches are listed; graph uses the neighbor limit.", wraplength=420).grid(row=2, column=0, sticky="w", pady=(3, 7))
        ttk.Label(inspector, textvariable=self.pair_var, wraplength=420).grid(row=3, column=0, sticky="w")
        self.detail_figure = Figure(figsize=(5.1, 3.0), dpi=100, constrained_layout=True)
        self.detail_axes = self.detail_figure.add_subplot(111)
        self.detail_canvas = FigureCanvasTkAgg(self.detail_figure, master=inspector)
        self.detail_canvas.get_tk_widget().grid(row=4, column=0, sticky="nsew", pady=(4, 0))
        detail_toolbar = ttk.Frame(inspector)
        detail_toolbar.grid(row=5, column=0, sticky="ew")
        NavigationToolbar2Tk(self.detail_canvas, detail_toolbar).update()
        self.detail_axes.set_title("Rolling correlation", fontsize=11)
        self.detail_axes.set_ylim(-1.05, 1.05)
        self.detail_canvas.draw_idle()
        ttk.Label(inspector, text="Qualifying episodes (shared observations)").grid(row=6, column=0, sticky="w", pady=(6, 3))
        self.episode_tree = self._make_tree(
            inspector,
            (("start", "Start", 100), ("end", "End", 100), ("duration", "Duration", 65), ("peak", "Peak", 65)),
            height=3, row=7,
        )
        self.episode_tree.bind("<<TreeviewSelect>>", self._on_episode)
        ttk.Label(inspector, text="Select an episode to zoom; use Home to restore.").grid(row=8, column=0, sticky="w", pady=3)
        self._build_spread_panel(spread_panel)
        # The three panes can be resized independently; wrap explanatory labels
        # to each pane instead of letting fixed-width text clip adjacent content.
        for pane in (graph, inspector, spread_panel):
            pane.bind("<Configure>", self._wrap_panel_labels)
        progress_frame = ttk.Frame(frame)
        progress_frame.grid(row=4, column=0, sticky="ew")
        self.progressbar = ttk.Progressbar(progress_frame, mode="determinate", maximum=100, length=200)
        self.progressbar.grid(row=0, column=0, padx=(0, 10))
        self.progressbar.grid_remove()
        ttk.Label(progress_frame, textvariable=self.progress_var).grid(row=0, column=1, sticky="w")
        ttk.Label(frame, textvariable=self.status_var, wraplength=1060).grid(row=5, column=0, sticky="w", pady=(4, 0))

    @staticmethod
    def _wrap_panel_labels(event):
        for child in event.widget.winfo_children():
            if isinstance(child, ttk.Label):
                child.configure(wraplength=max(180, event.width - 16))

    def _build_spread_panel(self, panel):
        panel.columnconfigure(0, weight=1)
        panel.rowconfigure(6, weight=1)
        ttk.Label(panel, textvariable=self.spread_heading_var, font=("Segoe UI", 11, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(panel, textvariable=self.spread_direction_var, wraplength=390).grid(row=1, column=0, sticky="w", pady=(3, 6))
        controls = ttk.Frame(panel)
        controls.grid(row=2, column=0, sticky="ew")
        controls.columnconfigure(1, weight=1)
        controls.columnconfigure(3, weight=1)
        self.spread_custom_check = ttk.Checkbutton(
            controls, text="Use custom dates", variable=self.spread_custom_var,
            command=self._change_spread_date_mode,
        )
        self.spread_custom_check.grid(row=0, column=0, columnspan=3, sticky="w")
        self.spread_apply_button = ttk.Button(controls, text="Apply", command=self.apply_spread_range, state=tk.DISABLED)
        self.spread_apply_button.grid(row=0, column=3, sticky="e", pady=(0, 4))
        ttk.Label(controls, text="From").grid(row=1, column=0, sticky="w", padx=(0, 4))
        self.spread_start_entry = ttk.Entry(controls, textvariable=self.spread_start_var, width=12, state=tk.DISABLED)
        self.spread_start_entry.grid(row=1, column=1, sticky="ew", padx=(0, 8))
        ttk.Label(controls, text="To").grid(row=1, column=2, sticky="w", padx=(0, 4))
        self.spread_end_entry = ttk.Entry(controls, textvariable=self.spread_end_var, width=12, state=tk.DISABLED)
        self.spread_end_entry.grid(row=1, column=3, sticky="ew")
        for entry in (self.spread_start_entry, self.spread_end_entry):
            entry.bind("<Return>", lambda event: self.apply_spread_range())
        ttk.Label(panel, textvariable=self.spread_period_var, wraplength=390).grid(row=3, column=0, sticky="w", pady=6)
        self.spread_tree = self._make_tree(
            panel,
            (("symbol", "Neighbor", 70), ("current", "Current (pp)", 90),
             ("cumulative", "Sum (pp)", 90), ("date", "As of", 100)),
            height=5, row=4,
        )
        for column in self.spread_tree["columns"]:
            self.spread_tree.heading(column, command=lambda col=column: self._sort_spreads(col))
        self.spread_tree.bind("<<TreeviewSelect>>", self._on_spread_row)
        ttk.Label(panel, textvariable=self.spread_summary_var, wraplength=390).grid(row=5, column=0, sticky="w", pady=6)
        self.spread_figure = Figure(figsize=(4.6, 3.2), dpi=100, constrained_layout=True)
        self.spread_axes = self.spread_figure.add_subplot(111)
        self.spread_canvas = FigureCanvasTkAgg(self.spread_figure, master=panel)
        self.spread_canvas.get_tk_widget().grid(row=6, column=0, sticky="nsew")
        toolbar = ttk.Frame(panel)
        toolbar.grid(row=7, column=0, sticky="ew")
        self.spread_toolbar = NavigationToolbar2Tk(self.spread_canvas, toolbar)
        self.spread_toolbar.update()
        self._empty_spread_chart("Analyze a stock to inspect its return spreads.")

    def _set_spread_controls(self):
        self.spread_custom_check.configure(state=tk.DISABLED if self.busy else tk.NORMAL)
        self.spread_apply_button.configure(state=tk.NORMAL if not self.busy and self.result is not None else tk.DISABLED)
        entry_state = tk.NORMAL if not self.busy and self.spread_custom_var.get() else tk.DISABLED
        self.spread_start_entry.configure(state=entry_state)
        self.spread_end_entry.configure(state=entry_state)

    def _change_spread_date_mode(self):
        self._set_spread_controls()
        self.apply_spread_range()

    def apply_spread_range(self):
        if self.busy:
            return
        start = self.spread_start_var.get().strip() if self.spread_custom_var.get() else None
        end = self.spread_end_var.get().strip() if self.spread_custom_var.get() else None
        try:
            parse_spread_dates(start, end)
        except ValueError as exc:
            messagebox.showerror("Invalid spread date range", str(exc), parent=self.root)
            return
        self.spread_range = (start or None, end or None)
        self._refresh_spreads()

    @staticmethod
    def _format_spread(value):
        return f"{value * 100:+.3f}" if np.isfinite(value) else "—"

    def _refresh_spreads(self):
        self.spread_tree.delete(*self.spread_tree.get_children())
        self.spread_rows.clear()
        if self.result is None or self.displayed_data is None:
            self._empty_spread_chart("Analyze a stock to inspect its return spreads.")
            return
        focal = self.result.symbol
        returns = self.displayed_data.returns
        rows = calculate_spreads(
            returns, focal, (pair.other(focal) for pair in self.result.pairs), *self.spread_range,
        )
        self.spread_heading_var.set(f"{focal} return spreads ({len(rows):,})")
        self.spread_direction_var.set(f"{focal} return − neighbor return • percentage points (pp)")
        start, end = self.spread_range
        period = f"{start or 'loaded start'} to {end or 'loaded end'}" if start or end else "Full return lookback"
        self.spread_period_var.set(f"{period} • within {self.result.settings.lookback} loaded returns/symbol\nDates: YYYY-MM-DD; blank bounds use loaded limits.")
        for row in rows:
            self.spread_rows[row.symbol] = row
            self.spread_tree.insert("", tk.END, iid=row.symbol, values=(
                row.symbol, self._format_spread(row.current), self._format_spread(row.cumulative),
                f"{row.current_date:%Y-%m-%d}" if row.current_date is not None else "—",
            ))
        if rows:
            selected = self.spread_selected_symbol if self.spread_selected_symbol in self.spread_rows else rows[0].symbol
            self._select_spread_symbol(selected)
        else:
            self.spread_selected_symbol = None
            self.spread_summary_var.set("No qualifying neighbors for this stock.")
            self._empty_spread_chart("No qualifying neighbors.")

    def _select_spread_symbol(self, symbol):
        if symbol not in self.spread_rows:
            return
        self.spread_selected_symbol = symbol
        if self.spread_tree.selection() != (symbol,):
            self.spread_tree.selection_set(symbol)
            self.spread_tree.see(symbol)
        self._draw_spread(self.spread_rows[symbol])

    def _on_spread_row(self, event):
        selected = self.spread_tree.selection()
        if not selected or selected[0] not in self.spread_rows:
            return
        symbol = selected[0]
        self.spread_selected_symbol = symbol
        self._draw_spread(self.spread_rows[symbol])
        pair = next((pair for pair in self.result.pairs if pair.other(self.result.symbol) == symbol), None)
        if pair is not None and self.selected_pair is not pair:
            self.select_pair(pair)

    def _sort_spreads(self, column):
        reverse = self.spread_sort_descending.get(column, column != "symbol")
        def key(symbol):
            row = self.spread_rows[symbol]
            return {"symbol": symbol, "current": row.current, "cumulative": row.cumulative, "date": row.current_date}[column]
        available = [symbol for symbol in self.spread_rows if pd.notna(key(symbol))]
        unavailable = [symbol for symbol in self.spread_rows if pd.isna(key(symbol))]
        for position, symbol in enumerate(sorted(available, key=key, reverse=reverse) + unavailable):
            self.spread_tree.move(symbol, "", position)
        self.spread_sort_descending[column] = not reverse

    def _empty_spread_chart(self, message):
        self.spread_axes.clear()
        self.spread_axes.text(0.5, 0.5, message, ha="center", va="center", wrap=True, transform=self.spread_axes.transAxes)
        self.spread_axes.set_axis_off()
        self.spread_canvas.draw_idle()

    def _draw_spread(self, row):
        if not row.count:
            self.spread_summary_var.set(f"{row.focal_symbol} − {row.symbol}: no shared returns in this period.")
            self._empty_spread_chart("No shared returns in the selected period.\nWiden the dates or increase Return lookback.")
            return
        self.spread_summary_var.set(
            f"{row.count:,} shared observations • arithmetic sum {self._format_spread(row.cumulative)} pp\n"
            f"Mean {self._format_spread(row.mean)} pp • sample SD {self._format_spread(row.std)} pp"
        )
        axes = self.spread_axes
        axes.clear()
        series = row.spread.loc[row.spread.first_valid_index():row.spread.last_valid_index()]
        axes.plot(series.index, series * 100, color=self.BLUE, linewidth=1.2, marker="." if row.count == 1 else None, label="Daily spread")
        axes.axhline(row.mean * 100, color="#677486", linestyle=":", linewidth=1.2, label="Mean")
        if np.isfinite(row.std):
            axes.axhline((row.mean + row.std) * 100, color=self.ORANGE, linestyle="--", linewidth=1, label="Mean ± 1 SD")
            axes.axhline((row.mean - row.std) * 100, color=self.ORANGE, linestyle="--", linewidth=1)
            axes.axhline((row.mean + 2 * row.std) * 100, color="#8b5fbf", linestyle="-.", linewidth=1, label="Mean ± 2 SD")
            axes.axhline((row.mean - 2 * row.std) * 100, color="#8b5fbf", linestyle="-.", linewidth=1)
        axes.set_title(f"{row.focal_symbol} − {row.symbol}", fontsize=10)
        axes.set_ylabel("Daily spread (pp)", fontsize=9)
        locator = mdates.AutoDateLocator(minticks=3, maxticks=5)
        axes.xaxis.set_major_locator(locator)
        axes.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
        axes.tick_params(labelsize=8)
        axes.grid(axis="y", alpha=0.15)
        axes.legend(loc="best", fontsize=8)
        self.spread_toolbar.update()
        self.spread_canvas.draw_idle()

    @staticmethod
    def _make_tree(parent, columns, height, row):
        frame = ttk.Frame(parent)
        frame.grid(row=row, column=0, sticky="ew")
        frame.columnconfigure(0, weight=1)
        tree = ttk.Treeview(frame, columns=tuple(col[0] for col in columns), show="headings", height=height, selectmode="browse")
        scroll = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        tree.grid(row=0, column=0, sticky="ew")
        scroll.grid(row=0, column=1, sticky="ns")
        for name, label, width in columns:
            tree.heading(name, text=label)
            tree.column(name, width=width, minwidth=50, anchor=tk.W)
        return tree

    def show_method(self):
        messagebox.showinfo(
            "Correlation method",
            "Daily returns = Close.pct_change(fill_method=None), using the latest lookback observations "
            "per symbol (252 by default), as in pairs_trading.ipynb. No prices are forward-filled.\n\n"
            "An edge requires Pearson baseline correlation ≥ base AND at least one run of rolling "
            "correlation ≥ high lasting the minimum duration. Both thresholds are inclusive. "
            "Only distinct symbols are compared; different symbols with correlation 1 are retained.\n\n"
            "Rolling correlations use shared, non-missing return observations, matching the notebook. "
            "Duration counts shared observations, so dates may span gaps or holidays. "
            "The latest value is dated in the pair details and may be older than the universe's latest date.\n\n"
            "Every stock is compared with the selected stock. The graph displays its strongest qualifying "
            "neighbors up to the limit, plus qualifying edges between those displayed neighbors. "
            "The table includes all qualifying neighbors. An edge can reflect a historical episode.\n\n"
            "Auto tries the narrow MySQL close-price query first, then the saved symbols loader. "
            "Prices are cached in memory until Reload prices or the source/lookback changes.",
            parent=self.root,
        )

    def _settings(self):
        try:
            settings = AnalysisSettings(
                base_corr=float(self.base_var.get()), high_corr=float(self.high_var.get()),
                min_dur=int(self.duration_var.get()), rolling_window=int(self.window_var.get()),
                lookback=int(self.lookback_var.get()), max_neighbors=int(self.limit_var.get()),
            )
        except ValueError as exc:
            raise ValueError("Enter numeric correlations and whole numbers for duration, window, lookback, and graph limit.") from exc
        settings.validate()
        if not self.symbol_var.get().strip():
            raise ValueError("Enter a stock symbol.")
        return settings

    def analyze(self, force_reload=False, *, history_index=None):
        if self.future is not None:
            return False
        try:
            settings = self._settings()
        except ValueError as exc:
            messagebox.showerror("Invalid analysis settings", str(exc), parent=self.root)
            return False
        symbol = self.symbol_var.get().strip().upper()
        self.symbol_var.set(symbol)
        source = self.source_var.get()
        key = (source, settings.lookback)
        cached = self.data if self.data_key == key and not force_reload else None
        self.cancel_event = threading.Event()
        self.messages = queue.SimpleQueue()
        self.pending_history_index = history_index
        self.progress_state = ProgressState()
        self._set_busy(True)
        self.status_var.set("Loading daily close prices..." if cached is None else f"Analyzing {symbol} with cached prices...")
        self._render_progress()
        self.future = self.executor.submit(self._compute, cached, key, symbol, settings, self.cancel_event)
        self.after_id = self.root.after(self.POLL_MS, self._poll)
        return True

    def _compute(self, cached, key, symbol, settings, cancel_event):
        data = cached or load_price_data(key[0], key[1], cancel_event, self.messages.put)
        # Return the data even if analysis fails so a typo doesn't force a reload.
        try:
            result = analyze_neighborhood(data.returns, symbol, settings, cancel_event, self.messages.put)
        except Exception as exc:
            return data, key, None, exc
        return data, key, result, None

    def _set_busy(self, busy):
        self.busy = busy
        for widget in self.inputs + [self.analyze_button, self.reload_button]:
            widget.configure(state=tk.DISABLED if busy else tk.NORMAL)
        self.source_box.configure(state=tk.DISABLED if busy else "readonly")
        self.cancel_button.configure(state=tk.NORMAL if busy else tk.DISABLED)
        self._update_navigation_buttons()
        self._set_spread_controls()

    @staticmethod
    def _duration_text(seconds):
        seconds = max(0, int(seconds))
        minutes, seconds = divmod(seconds, 60)
        return f"{minutes}:{seconds:02d}"

    def _render_progress(self):
        now = time.perf_counter()
        elapsed = self._duration_text(now - self.progress_state.started_at)
        percent = self.progress_state.percent
        if self.cancel_event.is_set():
            self.progressbar.grid_remove()
            self.progress_var.set(f"Stopping • elapsed {elapsed}")
        elif percent is None:
            self.progressbar.grid_remove()
            self.progress_var.set(f"Working • elapsed {elapsed} • percentage / ETA unavailable for this phase")
        else:
            self.progressbar.grid()
            self.progressbar.configure(value=percent)
            eta = self.progress_state.eta_seconds(now)
            remaining = f" • about {self._duration_text(eta)} remaining in this phase" if eta else ""
            self.progress_var.set(f"This phase: {int(percent)}%{remaining} • elapsed {elapsed}")

    def _finish_progress(self, outcome):
        elapsed = self._duration_text(time.perf_counter() - self.progress_state.started_at)
        self.progress_var.set(f"{outcome} • elapsed {elapsed}")
        if outcome == "Complete":
            self.progressbar.grid()
            self.progressbar.configure(value=100)
        else:
            self.progressbar.grid_remove()

    def _update_navigation_buttons(self):
        for direction, button in ((-1, self.back_button), (1, self.forward_button)):
            enabled = not self.busy and self.history.target(direction) is not None
            button.configure(state=tk.NORMAL if enabled else tk.DISABLED)

    def navigate_history(self, direction):
        if self.busy:
            return
        target = self.history.target(direction)
        if target is None:
            return
        position, symbol = target
        previous = self.symbol_var.get()
        self.symbol_var.set(symbol)
        if not self.analyze(history_index=position):
            self.symbol_var.set(previous)

    def _finish_navigation(self, symbol=None):
        # Commit only a successfully displayed graph. A failed/cancelled Back
        # request leaves the browser position and currently displayed stock intact.
        if symbol is not None:
            if self.pending_history_index is None:
                self.history.visit(symbol)
            else:
                self.history.position = self.pending_history_index
        elif self.pending_history_index is not None and self.result is not None:
            self.symbol_var.set(self.result.symbol)
        self.pending_history_index = None
        self._update_navigation_buttons()

    def _poll(self):
        self.after_id = None
        while not self.messages.empty():
            update = self.messages.get()
            if not isinstance(update, ProgressUpdate):
                update = ProgressUpdate(str(update), str(update))
            self.progress_state.accept(update)
            if not self.cancel_event.is_set():
                self.status_var.set(update.message)
        self._render_progress()
        if not self.future.done():
            self.after_id = self.root.after(self.POLL_MS, self._poll)
            return
        future, self.future = self.future, None
        try:
            data, key, result, error = future.result()
            self.data, self.data_key = data, key
            if error:
                raise error
            if self.cancel_event.is_set():
                raise CancelledError()
        except CancelledError:
            self._finish_navigation()
            self._set_busy(False)
            self._finish_progress("Cancelled")
            self.status_var.set("Cancelled. The previous completed graph remains available.")
            return
        except Exception as exc:
            self._finish_navigation()
            self._set_busy(False)
            self._finish_progress("Failed")
            self.status_var.set("Analysis failed. The previous completed graph remains available.")
            messagebox.showerror("Network analysis failed", str(exc), parent=self.root)
            return
        self.result = result
        self.displayed_data = data
        valid_dates = data.returns.dropna(how="all").index
        self.data_var.set(
            f"{data.source} • {len(data.returns.columns):,} symbols • "
            f"{valid_dates.min():%Y-%m-%d} to {valid_dates.max():%Y-%m-%d} • "
            f"up to {key[1]} returns/symbol • loaded in {data.elapsed:.1f}s"
        )
        self._display_result()
        self._finish_navigation(result.symbol)
        self._set_busy(False)
        self._finish_progress("Complete")
        self.status_var.set(
            f"{result.symbol}: {len(result.pairs):,} qualifying neighbors from {result.candidate_count:,} baseline candidates. "
            f"Graph: {len(result.neighbors):,} neighbors / {len(result.edges):,} edges. {data.detail}"
        )

    def cancel(self):
        self.cancel_event.set()
        self.cancel_button.configure(state=tk.DISABLED)
        self.status_var.set("Cancelling after the current data read or calculation...")
        self._render_progress()

    def _display_result(self):
        result = self.result
        self.selected_pair = None
        self.row_pairs.clear()
        self.neighbor_tree.delete(*self.neighbor_tree.get_children())
        self.episode_tree.delete(*self.episode_tree.get_children())
        for index, pair in enumerate(result.pairs):
            key = str(index)
            self.row_pairs[key] = pair
            self.neighbor_tree.insert("", tk.END, iid=key, values=(
                pair.other(result.symbol), f"{pair.baseline:.3f}", self._format_corr(pair.latest),
                pair.longest, len(pair.episodes),
            ))
        self.graph_var.set(
            f"{result.symbol} · {len(result.neighbors)} of {len(result.pairs)} qualifying neighbors"
        )
        self.neighbor_var.set(f"{result.symbol} qualifying neighbors ({len(result.pairs):,})")
        self._refresh_spreads()
        self.reset_layout()
        if result.pairs:
            self.neighbor_tree.selection_set("0")
            self.select_pair(result.pairs[0])
        else:
            self.pair_var.set("No pairs meet both thresholds and the minimum duration.")
            self.detail_axes.clear()
            self.detail_axes.set_title("No qualifying pair", fontsize=11)
            self.detail_axes.set_ylim(-1.05, 1.05)
            self.detail_canvas.draw_idle()

    @staticmethod
    def _format_corr(value):
        return f"{value:.3f}" if np.isfinite(value) else "—"

    def _empty_graph(self, text):
        self.graph_axes.clear()
        self.graph_axes.set_facecolor("#f7f9fc")
        self.graph_axes.set_axis_off()
        self.graph_axes.text(0.5, 0.5, text, ha="center", va="center", wrap=True,
                             transform=self.graph_axes.transAxes, color="#526176")
        self.graph_canvas.draw_idle()

    def reset_layout(self):
        if self.result is None:
            return
        self.drag_node = None
        self.hover_text = ""
        self.hover_var.set(" ")
        self.positions = {self.result.symbol: np.array([0.0, 0.0])}
        count = len(self.result.neighbors)
        for index, symbol in enumerate(self.result.neighbors):
            angle = np.pi / 2 - 2 * np.pi * index / max(1, count)
            self.positions[symbol] = np.array([np.cos(angle), np.sin(angle)])
        self._draw_graph()

    def _draw_graph(self):
        axes = self.graph_axes
        axes.clear()
        axes.set_facecolor("#f7f9fc")
        axes.set_axis_off()
        axes.set_aspect("equal", adjustable="box")
        axes.set_xlim(-1.3, 1.3)
        axes.set_ylim(-1.3, 1.3)
        self.node_artists.clear()
        self.edge_artists.clear()
        for pair in self.result.edges:
            left, right = self.positions[pair.symbol_a], self.positions[pair.symbol_b]
            central = self.result.symbol in (pair.symbol_a, pair.symbol_b)
            line, = axes.plot(
                [left[0], right[0]], [left[1], right[1]],
                color=self.BLUE if central else "#9aaeb9", alpha=0.7 if central else 0.5,
                linewidth=0.7 + 2.8 * max(0, pair.baseline), linestyle="-" if central else "--", zorder=1,
            )
            self.edge_artists.append((pair, line))
        for symbol, position in self.positions.items():
            center = symbol == self.result.symbol
            node, = axes.plot(
                *position, marker="o", markersize=25 if center else 18, linestyle="",
                markerfacecolor=self.ORANGE if center else self.BLUE,
                markeredgecolor="white", markeredgewidth=1.8, zorder=3,
            )
            label = axes.annotate(symbol, position, xytext=(0, 19 if center else 14),
                                  textcoords="offset points", ha="center", fontsize=10 if center else 9,
                                  fontweight="bold", color="#26354a", zorder=4)
            self.node_artists[symbol] = (node, label)
        if not self.result.neighbors:
            axes.text(0, -0.4, "No neighbors pass these settings.", ha="center", color="#526176")
        self.graph_toolbar.update()
        self._highlight_pair()
        self.graph_canvas.draw_idle()

    def _highlight_pair(self):
        for pair, line in self.edge_artists:
            selected = pair is self.selected_pair
            central = self.result.symbol in (pair.symbol_a, pair.symbol_b)
            line.set_color(self.ORANGE if selected else (self.BLUE if central else "#9aaeb9"))
            line.set_alpha(1 if selected else (0.7 if central else 0.5))
            line.set_zorder(2 if selected else 1)
        self.graph_canvas.draw_idle()

    def _node_at(self, event):
        for symbol, (node, label) in self.node_artists.items():
            if node.contains(event)[0] or label.contains(event)[0]:
                return symbol
        return None

    def _edge_at(self, event):
        # Screen-space distance chooses the closest line where edges overlap.
        point = np.array([event.x, event.y])
        nearest, distance = None, 7.0
        for pair, line in self.edge_artists:
            left, right = self.graph_axes.transData.transform(
                [self.positions[pair.symbol_a], self.positions[pair.symbol_b]]
            )
            delta = right - left
            denominator = float(delta @ delta)
            t = np.clip(float((point - left) @ delta) / denominator, 0, 1) if denominator else 0
            candidate = float(np.linalg.norm(point - (left + t * delta)))
            if candidate < distance:
                nearest, distance = pair, candidate
        return nearest

    def _on_press(self, event):
        if event.inaxes != self.graph_axes or event.button != 1 or self.graph_toolbar.mode:
            return
        node = self._node_at(event)
        if node:
            if event.dblclick:
                self.recenter(node)
                return
            self.drag_node = node
            if node != self.result.symbol:
                pair = next(pair for pair in self.result.pairs if pair.other(self.result.symbol) == node)
                self.select_pair(pair)
        else:
            pair = self._edge_at(event)
            if pair:
                self.select_pair(pair)

    def _on_release(self, event):
        self.drag_node = None

    def _on_motion(self, event):
        if event.inaxes != self.graph_axes or self.graph_toolbar.mode or self.result is None:
            return
        if self.drag_node and event.xdata is not None and event.ydata is not None:
            point = np.array([event.xdata, event.ydata])
            self.positions[self.drag_node] = point
            node, label = self.node_artists[self.drag_node]
            node.set_data([point[0]], [point[1]])
            label.xy = tuple(point)
            for pair, line in self.edge_artists:
                left, right = self.positions[pair.symbol_a], self.positions[pair.symbol_b]
                line.set_data([left[0], right[0]], [left[1], right[1]])
            self.graph_canvas.draw_idle()
            return
        node = self._node_at(event)
        if node:
            text = f"{node} · double-click to center the network here"
        else:
            pair = self._edge_at(event)
            text = (f"{pair.symbol_a} / {pair.symbol_b} · base {pair.baseline:.3f} · "
                    f"latest {self._format_corr(pair.latest)} · longest episode {pair.longest} observations") if pair else " "
        if text != self.hover_text:
            self.hover_text = text
            self.hover_var.set(text)

    def _on_scroll(self, event):
        if event.inaxes != self.graph_axes or self.graph_toolbar.mode or event.xdata is None:
            return
        self.graph_toolbar.push_current()
        factor = 0.8 if event.button == "up" else 1.25
        for center, getter, setter in (
            (event.xdata, self.graph_axes.get_xlim, self.graph_axes.set_xlim),
            (event.ydata, self.graph_axes.get_ylim, self.graph_axes.set_ylim),
        ):
            low, high = getter()
            setter(center + (low - center) * factor, center + (high - center) * factor)
        self.graph_toolbar.push_current()
        self.graph_canvas.draw_idle()

    def _on_row(self, event):
        selection = self.neighbor_tree.selection()
        if selection and selection[0] in self.row_pairs:
            self.select_pair(self.row_pairs[selection[0]])

    def _on_row_recenter(self, event):
        row = self.neighbor_tree.identify_row(event.y)
        if row in self.row_pairs:
            self.recenter(self.row_pairs[row].other(self.result.symbol))

    def recenter(self, symbol):
        if self.future is None:
            self.symbol_var.set(symbol)
            self.analyze()

    def _sort_neighbors(self, column):
        reverse = self.sort_descending.get(column, column != "symbol")
        def sort_key(key):
            pair = self.row_pairs[key]
            return {"symbol": pair.other(self.result.symbol), "base": pair.baseline,
                    "latest": pair.latest if np.isfinite(pair.latest) else -2,
                    "longest": pair.longest, "episodes": len(pair.episodes)}[column]
        for index, key in enumerate(sorted(self.row_pairs, key=sort_key, reverse=reverse)):
            self.neighbor_tree.move(key, "", index)
        self.sort_descending[column] = not reverse

    def select_pair(self, pair):
        self.selected_pair = pair
        settings = self.result.settings
        latest_date = pair.rolling.index[-1]
        self.pair_var.set(
            f"{pair.symbol_a} / {pair.symbol_b}  |  baseline {pair.baseline:.3f}\n"
            f"Latest {self._format_corr(pair.latest)} on {latest_date:%Y-%m-%d}  |  "
            f"{len(pair.episodes)} episodes  |  longest {pair.longest} observations"
        )
        axes = self.detail_axes
        axes.clear()
        axes.plot(pair.rolling.index, pair.rolling, color=self.BLUE, linewidth=1.5, label="Rolling")
        axes.axhline(settings.high_corr, color=self.ORANGE, linestyle="--", linewidth=1, label="High threshold")
        axes.axhline(pair.baseline, color="#798391", linestyle=":", linewidth=1, label="Pair baseline")
        for episode in pair.episodes:
            axes.axvspan(episode.start - pd.Timedelta(hours=12), episode.end + pd.Timedelta(hours=12),
                         color=self.ORANGE, alpha=0.16)
        axes.set_ylim(-1.05, 1.05)
        axes.set_ylabel("Correlation", fontsize=9)
        axes.set_title(f"{settings.rolling_window}-observation rolling correlation", fontsize=10)
        locator = mdates.AutoDateLocator(minticks=3, maxticks=5)
        axes.xaxis.set_major_locator(locator)
        axes.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
        axes.tick_params(labelsize=8)
        axes.grid(axis="y", alpha=0.15)
        axes.legend(loc="lower left", fontsize=8, ncol=3)
        self.detail_canvas.toolbar.update()
        self.detail_canvas.draw_idle()
        self.episode_tree.delete(*self.episode_tree.get_children())
        for index, episode in enumerate(pair.episodes):
            self.episode_tree.insert("", tk.END, iid=str(index), values=(
                f"{episode.start:%Y-%m-%d}", f"{episode.end:%Y-%m-%d}", episode.duration, f"{episode.peak:.3f}",
            ))
        self._highlight_pair()
        if self.result.symbol in (pair.symbol_a, pair.symbol_b):
            self._select_spread_symbol(pair.other(self.result.symbol))

    def _on_episode(self, event):
        selection = self.episode_tree.selection()
        if not selection or self.selected_pair is None:
            return
        episode = self.selected_pair.episodes[int(selection[0])]
        self.detail_canvas.toolbar.push_current()
        padding = max(pd.Timedelta(days=3), (episode.end - episode.start) / 4)
        self.detail_axes.set_xlim(episode.start - padding, episode.end + padding)
        self.detail_canvas.toolbar.push_current()
        self.detail_canvas.draw_idle()

    def close(self):
        self.cancel_event.set()
        if self.after_id is not None:
            self.root.after_cancel(self.after_id)
        self.executor.shutdown(wait=False, cancel_futures=True)
        self.root.destroy()


def main():
    parser = argparse.ArgumentParser(description="Interactive intermittent stock-correlation network.")
    parser.add_argument("--symbol", default="PANW", help="initial center symbol (default: PANW)")
    args = parser.parse_args()
    root = tk.Tk()
    NetworkAnalysisApp(root, args.symbol)
    root.mainloop()


if __name__ == "__main__":
    main()
