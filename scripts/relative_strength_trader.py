"""Tkinter tool for comparing a stock basket with a benchmark ETF."""

from __future__ import annotations

import gc
import gzip
import os
import pickle
import re
import sys
import threading
from io import StringIO
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import pandas as pd
import requests
from sqlalchemy import bindparam, create_engine, text


# Allow this file to be launched directly from the scripts directory.
PACKAGE_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = PACKAGE_DIR.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from market_data.api_keys import database_password, finviz_api_key
from market_data import fundamentals as fu


DEFAULT_DATA_DIR = Path(
    os.environ.get(
        "MARKET_DATA_DAILY_DIR",
        r"E:\Market Research\Dataset\daily_after_close_study",
    )
)


def _load_pickle(data_dir: Path, name: str) -> dict:
    path = data_dir / f"{name}.pkl.gz"
    with gzip.open(path, "rb") as file:
        return pickle.load(file)


def _daily_ohlcv_engine():
    url = f"mysql+pymysql://root:{database_password}@127.0.0.1:3306/daily_ohlcv"
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )


def load_available_symbols() -> set[str]:
    """Load only the stock tickers available in the daily OHLCV database."""
    engine = _daily_ohlcv_engine()
    try:
        frame = pd.read_sql(
            text("SELECT DISTINCT symbol FROM daily_symbol_bars"),
            con=engine,
        )
    finally:
        engine.dispose()

    if "symbol" not in frame:
        raise ValueError("daily_symbol_bars does not contain a symbol column")
    return {
        str(symbol).strip().upper()
        for symbol in frame["symbol"].dropna()
        if str(symbol).strip()
    }


def load_symbol_close_prices(symbols: list[str], start: str) -> pd.DataFrame:
    """Load Close history for only the requested symbols and date range."""
    if not symbols:
        return pd.DataFrame(columns=["symbol", "date", "close"])

    query = text(
        """
        SELECT symbol, date, close
        FROM daily_symbol_bars
        WHERE symbol IN :symbols
          AND date >= :start
        ORDER BY symbol, date
        """
    ).bindparams(bindparam("symbols", expanding=True))
    engine = _daily_ohlcv_engine()
    try:
        frame = pd.read_sql_query(
            query,
            con=engine,
            params={"symbols": symbols, "start": pd.Timestamp(start).date()},
        )
    finally:
        engine.dispose()

    if frame.empty:
        return frame
    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame.dropna(subset=["symbol", "date", "close"]).drop_duplicates(
        subset=["symbol", "date"],
        keep="last",
    )


def relative_close(
    stock_close: pd.Series,
    benchmark_close: pd.Series,
) -> pd.Series:
    """Return stock Close relative to a benchmark, normalized to 100."""
    stock = stock_close.rename("Stock")
    stock.index = pd.to_datetime(stock.index)
    benchmark = benchmark_close.rename("Benchmark").copy()
    benchmark.index = pd.to_datetime(benchmark.index)
    aligned = stock.to_frame().join(benchmark, how="inner")
    aligned["Benchmark"] = aligned["Benchmark"].ffill()
    aligned = aligned.dropna(subset=["Stock", "Benchmark"])
    if aligned.empty:
        raise ValueError("stock and benchmark have no usable overlapping prices")
    if aligned["Benchmark"].eq(0).any():
        raise ValueError("benchmark contains zero values")

    values = aligned["Stock"].div(aligned["Benchmark"])
    initial_value = values.iloc[0]
    if initial_value == 0:
        raise ValueError("initial relative Close is zero")
    return values.div(initial_value).mul(100).round(3)


def _compact_etfs(objects: dict) -> dict[str, pd.Series]:
    """Retain only the benchmark Close series for each ETF."""
    for symbol in list(objects):
        value = objects[symbol]
        frame = getattr(value, "df", None)
        if frame is None or "Close" not in frame.columns:
            del objects[symbol]
            continue
        objects[symbol] = frame["Close"].copy()
    return objects


def load_saved_objects(
    data_dir: Path = DEFAULT_DATA_DIR,
) -> tuple[set[str], dict[str, pd.Series]]:
    """Load available stock tickers from MySQL and ETF prices from disk."""
    try:
        symbols = load_available_symbols()
        etfs = _compact_etfs(_load_pickle(data_dir, "etfs"))
        gc.collect()
    except Exception as exc:
        raise RuntimeError(
            f"Could not load symbols from MySQL or ETFs from {data_dir}: {exc}"
        ) from exc
    return symbols, etfs


def load_latest_quant_ratings() -> dict[str, object]:
    """Load only each symbol and the newest daily quant-rating column."""
    url = f"mysql+pymysql://root:{database_password}@127.0.0.1:3306/stocks"
    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )
    try:
        columns = pd.read_sql("SHOW COLUMNS FROM daily_quant_rating", con=engine)
        if columns.empty or "Field" not in columns:
            raise ValueError("daily_quant_rating does not contain any columns")

        latest_column = str(columns.iloc[-1]["Field"])
        if latest_column == "index":
            raise ValueError("daily_quant_rating does not contain a rating column")
        quoted_latest_column = latest_column.replace("`", "``")
        ratings = pd.read_sql(
            "SELECT `index` AS Symbol, "
            f"`{quoted_latest_column}` AS Quant_Rating "
            "FROM daily_quant_rating",
            con=engine,
        )
    finally:
        engine.dispose()

    result = {}
    for symbol, rating in ratings.itertuples(index=False, name=None):
        if pd.isna(symbol) or pd.isna(rating):
            continue
        result[str(symbol).strip().upper()] = rating
    return result


def etf_membership(etf: str) -> pd.DataFrame:
    """Return the Finviz holdings search results for an ETF ticker."""
    ticker = etf.strip().upper()
    if not ticker:
        raise ValueError("Enter an ETF ticker for the Finviz membership search.")
    url = (
        "https://elite.finviz.com/export/screener"
        f"?v=111&f=etf_heldby_{ticker}&ft=5&auth={finviz_api_key}"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    result = pd.read_csv(StringIO(response.text))
    if "Ticker" not in result.columns:
        raise ValueError("The Finviz response does not contain a Ticker column.")
    return result


def read_symbol_file(path: str) -> list[str]:
    """Read newline-, comma-, or whitespace-delimited tickers from a text file."""
    file_path = Path(path).expanduser()
    if not file_path.is_file():
        raise FileNotFoundError(f"Symbol file does not exist: {file_path}")
    text = file_path.read_text(encoding="utf-8-sig")
    return [value.upper() for value in re.split(r"[\s,;]+", text) if value]


class ETFTraderApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("ETF Relative Strength")
        self.root.geometry("1250x760")
        self.root.minsize(1000, 650)

        self.symbols: set[str] = set()
        self.etfs: dict[str, pd.Series] = {}
        self.quant_ratings: dict[str, object] = {}
        self.relative_df = pd.DataFrame()
        self.current_row = pd.Series(dtype=float)
        self.calculation_running = False
        self.range_refresh_job: str | None = None
        self.active_sort_column = "relative"
        self.relative_sort_ascending = False
        self.quant_sort_ascending = False

        self.benchmark_var = tk.StringVar()
        self.start_date_var = tk.StringVar()
        self.source_var = tk.StringVar(value="file")
        self.file_path_var = tk.StringVar()
        self.membership_etf_var = tk.StringVar()
        self.level_var = tk.StringVar(value="industry")
        self.member_name_var = tk.StringVar()
        self.describe_row_var = tk.StringVar(value="-1")
        self.filter_mode_var = tk.StringVar(value="Outside range")
        self.low_var = tk.DoubleVar(value=0)
        self.high_var = tk.DoubleVar(value=100)
        self.low_label_var = tk.StringVar(value="0.000")
        self.high_label_var = tk.StringVar(value="100.000")
        self.status_var = tk.StringVar(value="Loading symbols and ETFs...")

        self._build_ui()
        self._set_controls_enabled(False)
        threading.Thread(target=self._load_data, daemon=True).start()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(3, weight=1)

        settings = ttk.LabelFrame(container, text="Comparison", padding=8)
        settings.grid(row=0, column=0, sticky="ew")
        settings.columnconfigure(1, weight=1)

        ttk.Label(settings, text="Benchmark ETF").grid(
            row=0, column=0, sticky="w", padx=(0, 6)
        )
        self.benchmark_box = ttk.Combobox(
            settings,
            textvariable=self.benchmark_var,
            state="readonly",
            width=18,
        )
        self.benchmark_box.grid(row=0, column=1, sticky="w")

        ttk.Label(settings, text="Start date (YYYY-MM-DD)").grid(
            row=0, column=2, sticky="w", padx=(24, 6)
        )
        self.start_date_entry = ttk.Entry(
            settings, textvariable=self.start_date_var, width=16
        )
        self.start_date_entry.grid(row=0, column=3, sticky="w")

        sources = ttk.LabelFrame(container, text="Symbol list source", padding=8)
        sources.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        sources.columnconfigure(1, weight=1)

        ttk.Radiobutton(
            sources,
            text="Local text file",
            variable=self.source_var,
            value="file",
        ).grid(row=0, column=0, sticky="w")
        self.file_entry = ttk.Entry(sources, textvariable=self.file_path_var)
        self.file_entry.grid(row=0, column=1, sticky="ew", padx=6)
        self.browse_button = ttk.Button(
            sources, text="Browse...", command=self._browse_file
        )
        self.browse_button.grid(row=0, column=2)

        ttk.Radiobutton(
            sources,
            text="Finviz ETF membership",
            variable=self.source_var,
            value="finviz",
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.membership_entry = ttk.Entry(
            sources, textvariable=self.membership_etf_var, width=16
        )
        self.membership_entry.grid(row=1, column=1, sticky="w", padx=6, pady=(6, 0))
        ttk.Label(sources, text="ETF ticker").grid(
            row=1, column=2, sticky="w", pady=(6, 0)
        )

        ttk.Radiobutton(
            sources,
            text="Sector / industry (comma-separated)",
            variable=self.source_var,
            value="sector_industry",
        ).grid(row=2, column=0, sticky="w", pady=(6, 0))
        sector_inputs = ttk.Frame(sources)
        sector_inputs.grid(
            row=2, column=1, columnspan=2, sticky="ew", padx=6, pady=(6, 0)
        )
        sector_inputs.columnconfigure(1, weight=1)
        self.level_box = ttk.Combobox(
            sector_inputs,
            textvariable=self.level_var,
            values=("industry", "sector"),
            state="readonly",
            width=10,
        )
        self.level_box.grid(row=0, column=0, sticky="w")
        self.member_name_entry = ttk.Entry(
            sector_inputs, textvariable=self.member_name_var
        )
        self.member_name_entry.grid(row=0, column=1, sticky="ew", padx=(6, 0))

        actions = ttk.Frame(container)
        actions.grid(row=2, column=0, sticky="ew", pady=8)
        actions.columnconfigure(5, weight=1)
        self.calculate_button = ttk.Button(
            actions, text="Calculate", command=self._start_calculation
        )
        self.calculate_button.grid(row=0, column=0)
        ttk.Label(actions, text="Describe row").grid(
            row=0, column=1, padx=(16, 6)
        )
        self.describe_entry = ttk.Entry(
            actions, textvariable=self.describe_row_var, width=14
        )
        self.describe_entry.grid(row=0, column=2)
        self.describe_button = ttk.Button(
            actions, text="Describe", command=self._describe_selected_row
        )
        self.describe_button.grid(row=0, column=3, padx=(6, 0))
        ttk.Label(actions, textvariable=self.status_var).grid(
            row=0, column=5, sticky="e"
        )

        output = ttk.Panedwindow(container, orient="horizontal")
        output.grid(row=3, column=0, sticky="nsew")

        tables_frame = ttk.Frame(output)
        tables_frame.columnconfigure(0, weight=1)
        tables_frame.rowconfigure(0, weight=1)

        results_frame = ttk.LabelFrame(tables_frame, text="Relative values", padding=6)
        results_frame.grid(row=0, column=0, sticky="nsew")
        describe_frame = ttk.LabelFrame(
            tables_frame, text="DataFrame describe()", padding=6
        )
        describe_frame.grid(row=1, column=0, sticky="ew", pady=(6, 0))

        plot_frame = ttk.LabelFrame(output, text="Relative strength history", padding=6)
        output.add(tables_frame, weight=2)
        output.add(plot_frame, weight=4)

        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        self.results_tree = ttk.Treeview(
            results_frame,
            columns=("symbol", "relative", "quant_rating"),
            show="headings",
            selectmode="browse",
        )
        self.results_tree.heading("symbol", text="Symbol")
        self.results_tree.heading(
            "relative",
            text="Relative Close ▼",
            command=self._toggle_relative_sort,
        )
        self.results_tree.heading(
            "quant_rating",
            text="Quant Rating",
            command=self._toggle_quant_sort,
        )
        self.results_tree.column("symbol", width=130, anchor="center")
        self.results_tree.column("relative", width=150, anchor="e")
        self.results_tree.column("quant_rating", width=120, anchor="center")
        results_scroll = ttk.Scrollbar(
            results_frame, orient="vertical", command=self.results_tree.yview
        )
        self.results_tree.configure(yscrollcommand=results_scroll.set)
        self.results_tree.grid(row=0, column=0, sticky="nsew")
        results_scroll.grid(row=0, column=1, sticky="ns")
        self.results_tree.bind("<<TreeviewSelect>>", self._plot_selected_symbol)

        plot_frame.columnconfigure(0, weight=1)
        plot_frame.rowconfigure(0, weight=1)
        self.plot_figure = Figure(figsize=(5, 4), dpi=100)
        self.plot_axes = self.plot_figure.add_subplot(111)
        self.plot_canvas = FigureCanvasTkAgg(self.plot_figure, master=plot_frame)
        self.plot_canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        self._show_plot_prompt()

        describe_frame.columnconfigure(0, weight=1)
        describe_frame.rowconfigure(0, weight=1)
        self.describe_tree = ttk.Treeview(
            describe_frame,
            columns=("statistic", "value"),
            show="headings",
            selectmode="none",
            height=8,
        )
        self.describe_tree.heading("statistic", text="Statistic")
        self.describe_tree.heading("value", text="Value")
        self.describe_tree.column("statistic", width=110)
        self.describe_tree.column("value", width=120, anchor="e")
        self.describe_tree.grid(row=0, column=0, sticky="nsew")

        range_frame = ttk.LabelFrame(container, text="Relative strength range", padding=8)
        range_frame.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        range_frame.columnconfigure(1, weight=1)

        ttk.Label(range_frame, text="Lower").grid(row=0, column=0, sticky="w")
        self.low_scale = ttk.Scale(
            range_frame,
            variable=self.low_var,
            command=lambda _value: self._range_changed("low"),
        )
        self.low_scale.grid(row=0, column=1, sticky="ew", padx=6)
        ttk.Label(range_frame, textvariable=self.low_label_var, width=10).grid(
            row=0, column=2
        )

        ttk.Label(range_frame, text="Upper").grid(row=1, column=0, sticky="w")
        self.high_scale = ttk.Scale(
            range_frame,
            variable=self.high_var,
            command=lambda _value: self._range_changed("high"),
        )
        self.high_scale.grid(row=1, column=1, sticky="ew", padx=6)
        ttk.Label(range_frame, textvariable=self.high_label_var, width=10).grid(
            row=1, column=2
        )

        ttk.Label(range_frame, text="Show").grid(
            row=0, column=3, padx=(18, 6)
        )
        self.filter_box = ttk.Combobox(
            range_frame,
            textvariable=self.filter_mode_var,
            values=("Outside range", "Inside range"),
            state="readonly",
            width=15,
        )
        self.filter_box.grid(row=0, column=4)
        self.filter_box.bind(
            "<<ComboboxSelected>>", lambda _event: self._refresh_results()
        )

        self.controls = (
            self.benchmark_box,
            self.start_date_entry,
            self.file_entry,
            self.browse_button,
            self.membership_entry,
            self.level_box,
            self.member_name_entry,
            self.calculate_button,
            self.describe_entry,
            self.describe_button,
            self.low_scale,
            self.high_scale,
            self.filter_box,
        )

    def _set_controls_enabled(self, enabled: bool) -> None:
        state = "!disabled" if enabled else "disabled"
        for widget in self.controls:
            widget.state([state])
        if enabled:
            self.benchmark_box.state(["readonly"])
            self.level_box.state(["readonly"])
            self.filter_box.state(["readonly"])

    def _load_data(self) -> None:
        try:
            symbols, etfs = load_saved_objects()
            quant_ratings = load_latest_quant_ratings()
        except Exception as exc:
            self.root.after(0, self._load_failed, str(exc))
            return
        self.root.after(0, self._load_complete, symbols, etfs, quant_ratings)

    def _load_complete(
        self,
        symbols: set[str],
        etfs: dict[str, pd.Series],
        quant_ratings: dict[str, object],
    ) -> None:
        self.symbols = symbols
        self.etfs = etfs
        self.quant_ratings = quant_ratings
        tickers = sorted(etfs)
        self.benchmark_box.configure(values=tickers)
        if tickers:
            self.benchmark_var.set("SPY" if "SPY" in etfs else tickers[0])
        self.status_var.set(
            f"Ready — {len(symbols):,} symbols, {len(etfs):,} ETFs, "
            f"{len(quant_ratings):,} quant ratings loaded"
        )
        self._set_controls_enabled(True)

    def _load_failed(self, error: str) -> None:
        self.status_var.set("Loading failed")
        messagebox.showerror("Unable to load data", error)

    def _browse_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Choose a symbol list",
            filetypes=(("Text files", "*.txt"), ("All files", "*.*")),
        )
        if path:
            self.file_path_var.set(path)
            self.source_var.set("file")

    def _start_calculation(self) -> None:
        if self.calculation_running:
            return
        benchmark = self.benchmark_var.get().strip().upper()
        start = self.start_date_var.get().strip()
        if benchmark not in self.etfs:
            messagebox.showerror("Invalid benchmark", "Select a benchmark ETF.")
            return
        if not start:
            messagebox.showerror("Missing start date", "Enter a start date.")
            return
        try:
            pd.Timestamp(start)
        except ValueError:
            messagebox.showerror(
                "Invalid start date", "Use a date such as 2026-06-22."
            )
            return

        source = self.source_var.get()
        source_options = {
            "file": self.file_path_var.get(),
            "finviz": self.membership_etf_var.get(),
            "level": self.level_var.get(),
            "member_name": self.member_name_var.get(),
        }
        self.calculation_running = True
        self.calculate_button.state(["disabled"])
        self.status_var.set("Building symbol list and calculating...")
        threading.Thread(
            target=self._calculate,
            args=(benchmark, start, source, source_options),
            daemon=True,
        ).start()

    def _get_source_symbols(self, source: str, options: dict) -> list[str]:
        if source == "file":
            values = read_symbol_file(options["file"])
        elif source == "finviz":
            values = etf_membership(options["finviz"])["Ticker"].tolist()
        elif source == "sector_industry":
            names = [
                name.strip()
                for name in options["member_name"].split(",")
                if name.strip()
            ]
            if not names:
                raise ValueError("Enter one or more sector or industry names.")
            requested_names = set(names)
            level = options["level"]
            values = [
                symbol
                for symbol, classifications in fu.sectors_industries.items()
                if classifications.get(level) in requested_names
            ]
        else:
            raise ValueError(f"Unknown symbol source: {source}")

        # Preserve input order while removing duplicates and unavailable symbols.
        return list(
            dict.fromkeys(
                str(value).strip().upper()
                for value in values
                if str(value).strip().upper() in self.symbols
            )
        )

    def _calculate(
        self, benchmark: str, start: str, source: str, options: dict
    ) -> None:
        try:
            members = self._get_source_symbols(source, options)
            if not members:
                raise ValueError(
                    "The selected source did not contain symbols available "
                    "in the daily OHLCV database."
                )

            benchmark_close = self.etfs[benchmark]
            close_prices = load_symbol_close_prices(members, start)
            if close_prices.empty:
                raise ValueError(
                    "No Close data was found for the selected symbols and start date."
                )

            series = {}
            failures = {
                symbol: "No Close data was found in the requested date range."
                for symbol in members
            }
            for symbol, frame in close_prices.groupby("symbol", sort=False):
                try:
                    stock_close = frame.set_index("date")["close"].sort_index()
                    values = relative_close(stock_close, benchmark_close)
                    if not values.empty:
                        series[symbol] = values.astype("float32").rename(symbol)
                        failures.pop(symbol, None)
                except Exception as exc:
                    failures[symbol] = str(exc)

            if not series:
                raise ValueError(
                    "No relative series could be calculated. Check the start "
                    "date and price-data overlap."
                )
            result = pd.concat(series.values(), axis=1, copy=False)
        except Exception as exc:
            self.root.after(0, self._calculation_failed, str(exc))
            return
        self.root.after(
            0,
            self._calculation_complete,
            result,
            len(members),
            len(failures),
        )

    def _calculation_complete(
        self, result: pd.DataFrame, member_count: int, failure_count: int
    ) -> None:
        self.relative_df = result
        self.calculation_running = False
        self.calculate_button.state(["!disabled"])
        self.status_var.set(
            f"Calculated {result.shape[1]:,} of {member_count:,} symbols"
            + (f" ({failure_count:,} skipped)" if failure_count else "")
        )
        self._show_plot_prompt()
        self._describe_selected_row()

    def _calculation_failed(self, error: str) -> None:
        self.calculation_running = False
        self.calculate_button.state(["!disabled"])
        self.status_var.set("Calculation failed")
        messagebox.showerror("Unable to calculate relative strength", error)

    def _selected_row(self) -> pd.Series:
        if self.relative_df.empty:
            raise ValueError("Calculate relative strength first.")
        selector = self.describe_row_var.get().strip()
        if not selector:
            raise ValueError("Enter a row number or index date.")

        try:
            position = int(selector)
        except ValueError:
            try:
                selected = self.relative_df.loc[selector]
            except KeyError:
                try:
                    selected = self.relative_df.loc[pd.Timestamp(selector)]
                except (KeyError, ValueError) as exc:
                    raise ValueError(
                        f"Row {selector!r} is not in the result index."
                    ) from exc
        else:
            try:
                selected = self.relative_df.iloc[position]
            except IndexError as exc:
                raise ValueError(
                    f"Row position {position} is outside the result."
                ) from exc

        if isinstance(selected, pd.DataFrame):
            selected = selected.iloc[-1]
        return selected.dropna()

    def _describe_selected_row(self) -> None:
        try:
            row = self._selected_row()
            if row.empty:
                raise ValueError("The selected row contains no relative values.")
        except ValueError as exc:
            messagebox.showerror("Unable to describe row", str(exc))
            return

        self.current_row = row
        description = row.describe()
        for item in self.describe_tree.get_children():
            self.describe_tree.delete(item)
        for statistic, value in description.items():
            self.describe_tree.insert(
                "", "end", values=(statistic, f"{float(value):,.3f}")
            )

        minimum = float(row.min())
        maximum = float(row.max())
        if minimum == maximum:
            minimum -= 1
            maximum += 1
        self.low_scale.configure(from_=minimum, to=maximum)
        self.high_scale.configure(from_=minimum, to=maximum)

        # Start with the middle half selected, matching the notebook workflow.
        self.low_var.set(float(description["25%"]))
        self.high_var.set(float(description["75%"]))
        self._update_range_labels()
        self._refresh_results()

    def _range_changed(self, changed: str) -> None:
        low = self.low_var.get()
        high = self.high_var.get()
        if low > high:
            if changed == "low":
                self.high_var.set(low)
            else:
                self.low_var.set(high)
        self._update_range_labels()
        if self.range_refresh_job is not None:
            self.root.after_cancel(self.range_refresh_job)
        self.range_refresh_job = self.root.after(75, self._refresh_results)

    def _update_range_labels(self) -> None:
        self.low_label_var.set(f"{self.low_var.get():.3f}")
        self.high_label_var.set(f"{self.high_var.get():.3f}")

    def _toggle_relative_sort(self) -> None:
        if self.active_sort_column == "relative":
            self.relative_sort_ascending = not self.relative_sort_ascending
        self.active_sort_column = "relative"
        self._update_sort_headings()
        self._refresh_results()

    def _toggle_quant_sort(self) -> None:
        if self.active_sort_column == "quant_rating":
            self.quant_sort_ascending = not self.quant_sort_ascending
        self.active_sort_column = "quant_rating"
        self._update_sort_headings()
        self._refresh_results()

    def _update_sort_headings(self) -> None:
        relative_direction = "▲" if self.relative_sort_ascending else "▼"
        quant_direction = "▲" if self.quant_sort_ascending else "▼"
        self.results_tree.heading(
            "relative",
            text=(
                f"Relative Close {relative_direction}"
                if self.active_sort_column == "relative"
                else "Relative Close"
            ),
        )
        self.results_tree.heading(
            "quant_rating",
            text=(
                f"Quant Rating {quant_direction}"
                if self.active_sort_column == "quant_rating"
                else "Quant Rating"
            ),
        )

    def _show_plot_prompt(self) -> None:
        self.plot_axes.clear()
        self.plot_axes.text(
            0.5,
            0.5,
            "Select a symbol from the Relative values table",
            ha="center",
            va="center",
            transform=self.plot_axes.transAxes,
        )
        self.plot_axes.set_axis_off()
        self.plot_canvas.draw_idle()

    def _plot_selected_symbol(self, _event: tk.Event[tk.Misc]) -> None:
        selection = self.results_tree.selection()
        if not selection:
            return
        values = self.results_tree.item(selection[0], "values")
        if not values:
            return
        symbol = str(values[0])
        if symbol not in self.relative_df:
            return

        relative_values = self.relative_df[symbol].dropna()
        if relative_values.empty:
            return

        self.plot_axes.clear()
        self.plot_axes.set_axis_on()
        relative_values.plot(
            ax=self.plot_axes,
            color="#1f77b4",
            linewidth=1.5,
        )
        self.plot_axes.axhline(100, color="gray", linewidth=0.8, alpha=0.6)
        self.plot_axes.set_title(
            f"{symbol} relative to {self.benchmark_var.get()}"
        )
        self.plot_axes.set_xlabel("")
        self.plot_axes.set_ylabel("Relative Close")
        self.plot_axes.grid(True, alpha=0.25)
        self.plot_figure.tight_layout()
        self.plot_canvas.draw_idle()

    def _refresh_results(self) -> None:
        self.range_refresh_job = None
        if self.current_row.empty:
            return
        low = self.low_var.get()
        high = self.high_var.get()
        if self.filter_mode_var.get() == "Outside range":
            filtered = self.current_row[
                (self.current_row < low) | (self.current_row > high)
            ]
        else:
            filtered = self.current_row[
                (self.current_row >= low) & (self.current_row <= high)
            ]
        if self.active_sort_column == "quant_rating":
            quant_values = pd.Series(
                {
                    symbol: pd.to_numeric(
                        self.quant_ratings.get(str(symbol).upper()),
                        errors="coerce",
                    )
                    for symbol in filtered.index
                },
                dtype="float64",
            )
            ordered_symbols = quant_values.sort_values(
                ascending=self.quant_sort_ascending,
                na_position="last",
            ).index
            filtered = filtered.reindex(ordered_symbols)
        else:
            filtered = filtered.sort_values(
                ascending=self.relative_sort_ascending
            )

        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        for symbol, value in filtered.items():
            quant_rating = self.quant_ratings.get(str(symbol).upper(), "N/A")
            if quant_rating != "N/A":
                quant_rating = f"{float(quant_rating):.2f}"
            self.results_tree.insert(
                "",
                "end",
                values=(symbol, f"{float(value):,.3f}", quant_rating),
            )


def main() -> None:
    root = tk.Tk()
    ETFTraderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
