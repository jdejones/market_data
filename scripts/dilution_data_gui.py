"""Desktop dashboard for SEC filing-derived company dilution data.

The GUI deliberately keeps live retrieval and stored analytics separate:

* Live Data calls sec-api.io through :class:`DilutionTracker`.
* Stored Analytics reads rows previously written to the local MySQL database.
* Import & Database fetches fresh data and upserts it into that database.
* Source Explorer exposes individual filing sources, the text parser, and the
  dilution stack's offline validation helpers.

Launch from the repository root with:

    python -m market_data.scripts.dilution_data_gui
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import queue
import re
import sys
import tkinter as tk
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Any, Callable, Iterable

import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from market_data.filings import (  # type: ignore[import-not-found]
    AtmCapacityTracker,
    DilutionTextParser,
    DilutionTracker,
    DilutionTrackerAnalytics,
    DilutionTrackerDatabase,
    DilutionTrackerImporter,
    validate_atm_capacity_tracker_sample,
    validate_dilution_storage_normalizer_sample,
    validate_dilution_storage_query_sample,
    validate_dilution_text_parser_sample,
    validate_dilution_tracker_sample,
)
from market_data.price_data_import import db_import  # type: ignore[import-not-found]


DEFAULT_LOOKBACK_DAYS = 365 * 5
POLL_INTERVAL_MS = 100
EVENT_COLORS = {
    "authorized": "#9467bd",
    "atm_amendment": "#1f77b4",
    "atm_capacity": "#17becf",
    "atm_remaining": "#2ca02c",
    "atm_sold": "#ff7f0e",
    "atm_terminated": "#111111",
    "cancelled": "#7f7f7f",
    "issuable": "#e377c2",
    "newly_issued": "#d62728",
    "outstanding": "#8c564b",
    "registered": "#bcbd22",
    "repurchased": "#9467bd",
    "reserved": "#7f7f7f",
    "selling_stockholder": "#c49c94",
    "sold": "#ff9896",
    "underlying_convertibles": "#ff7f0e",
    "underlying_warrants": "#2ca02c",
}


def normalize_symbol(value: str) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise ValueError("A symbol is required.")
    return symbol


def parse_date(value: str, label: str) -> dt.date:
    try:
        return dt.date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"{label} must use YYYY-MM-DD format.") from exc


def parse_symbols(value: str) -> list[str]:
    symbols = [
        item.strip().upper()
        for item in re.split(r"[\s,;]+", str(value or ""))
        if item.strip()
    ]
    return list(dict.fromkeys(symbols))


def json_default(value: Any) -> str:
    if isinstance(value, (dt.date, dt.datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return str(value.item())
    return str(value)


def display_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(value, (pd.Timestamp, dt.datetime)):
        return value.strftime("%Y-%m-%d %H:%M")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        magnitude = abs(value)
        if magnitude >= 1_000_000:
            return f"{value:,.0f}"
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    return str(value)


def compact_number(value: Any, prefix: str = "") -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if not math.isfinite(number):
        return "—"
    for threshold, suffix in (
        (1_000_000_000_000, "T"),
        (1_000_000_000, "B"),
        (1_000_000, "M"),
        (1_000, "K"),
    ):
        if abs(number) >= threshold:
            return f"{prefix}{number / threshold:,.2f}{suffix}"
    return f"{prefix}{number:,.0f}"


def _close_series(price_frame: pd.DataFrame) -> pd.Series:
    if price_frame is None or price_frame.empty:
        return pd.Series(dtype=float, name="close")
    close_column = next(
        (column for column in price_frame.columns if str(column).casefold() == "close"),
        None,
    )
    if close_column is None:
        raise ValueError("Price data do not contain a Close column.")
    close = pd.to_numeric(price_frame[close_column], errors="coerce").copy()
    close.index = pd.to_datetime(close.index, errors="coerce").normalize()
    close = close[~close.index.isna()]
    close = close[~close.index.duplicated(keep="last")].sort_index()
    close.name = "close"
    return close


def _share_series(share_history: pd.DataFrame) -> pd.Series:
    required = {"period", "shares_outstanding"}
    if share_history is None or share_history.empty or not required.issubset(share_history):
        return pd.Series(dtype=float, name="shares_outstanding")

    shares = share_history.copy()
    shares["period"] = pd.to_datetime(shares["period"], errors="coerce").dt.normalize()
    shares["shares_outstanding"] = pd.to_numeric(
        shares["shares_outstanding"], errors="coerce"
    )
    if "share_class" not in shares:
        shares["share_class"] = "common"
    if "reported_at" in shares:
        shares["reported_at"] = pd.to_datetime(shares["reported_at"], errors="coerce")
        shares = shares.sort_values(["period", "share_class", "reported_at"])
    shares = shares.dropna(subset=["period"])
    shares = shares.drop_duplicates(["period", "share_class"], keep="last")
    result = shares.groupby("period")["shares_outstanding"].sum(min_count=1).sort_index()
    result.name = "shares_outstanding"
    return result


def build_market_cap_frame(
    price_frame: pd.DataFrame,
    share_history: pd.DataFrame,
) -> pd.DataFrame:
    """Align daily closes with reported share counts and calculate market cap.

    Missing historical closes and share counts are forward-filled. If the raw
    most-recent Close is NaN, that final Close is set to zero to represent the
    requested delisting assumption instead of carrying an old price forward.
    """

    close = _close_series(price_frame)
    shares = _share_series(share_history)
    if close.empty:
        return pd.DataFrame(
            columns=["raw_close", "close", "shares_outstanding", "market_cap"]
        )

    frame = close.rename("raw_close").to_frame()
    latest_raw_close_missing = bool(pd.isna(frame["raw_close"].iloc[-1]))
    frame["close"] = frame["raw_close"].ffill()
    if latest_raw_close_missing:
        frame.iloc[-1, frame.columns.get_loc("close")] = 0.0

    frame = frame.join(shares, how="left")
    if not shares.empty:
        combined_index = frame.index.union(shares.index).sort_values()
        aligned_shares = shares.reindex(combined_index).ffill().reindex(frame.index)
        frame["shares_outstanding"] = aligned_shares
    frame["market_cap"] = frame["close"] * frame["shares_outstanding"]
    frame.attrs["delisted_assumption"] = latest_raw_close_missing
    return frame


class DataFrameTable(ttk.Frame):
    """A sortable Treeview that can display arbitrary DataFrame columns."""

    def __init__(self, master: tk.Misc) -> None:
        super().__init__(master)
        self.frame = pd.DataFrame()
        self.tree = ttk.Treeview(self, show="headings")
        y_scroll = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.tree.yview)
        x_scroll = ttk.Scrollbar(self, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

    def set_frame(self, frame: pd.DataFrame | None) -> None:
        self.frame = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        self.tree.delete(*self.tree.get_children())
        columns = [str(column) for column in self.frame.columns]
        self.tree.configure(columns=columns)
        for column in columns:
            width = min(320, max(90, len(column) * 9 + 24))
            self.tree.heading(
                column,
                text=column.replace("_", " ").title(),
                command=lambda key=column: self._sort(key),
            )
            self.tree.column(column, width=width, minwidth=70, stretch=True)
        for position, (_, row) in enumerate(self.frame.iterrows()):
            self.tree.insert(
                "",
                tk.END,
                iid=f"row-{position}",
                values=[display_value(row.get(column)) for column in self.frame.columns],
            )

    def selected_record(self) -> dict[str, Any] | None:
        selected = self.tree.selection()
        if not selected or self.frame.empty:
            return None
        try:
            position = self.tree.index(selected[0])
            return self.frame.iloc[position].to_dict()
        except (IndexError, ValueError):
            return None

    def _sort(self, column: str) -> None:
        if self.frame.empty or column not in self.frame:
            return
        ascending = bool(self.frame.attrs.get(f"sort_{column}", True))
        self.frame = self.frame.sort_values(
            column, ascending=ascending, na_position="last", kind="stable"
        ).reset_index(drop=True)
        self.frame.attrs[f"sort_{column}"] = not ascending
        self.set_frame(self.frame)


class DilutionDataGui:
    def __init__(self, root: tk.Tk, initial_symbol: str = "") -> None:
        self.root = root
        self.root.title("Company Dilution Data")
        self.root.geometry("1500x930")
        self.root.minsize(1050, 700)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        self.executor = ThreadPoolExecutor(
            max_workers=3, thread_name_prefix="dilution-gui"
        )
        self.results: queue.Queue[
            tuple[Callable[[Any], None], Any, BaseException | None, str]
        ] = queue.Queue()
        self.closing = False
        self.live_summary: dict[str, pd.DataFrame] = {}
        self.live_symbol = ""
        self.source_rows: tuple[list[dict[str, Any]], list[dict[str, Any]]] | None = None
        self.tables: list[DataFrameTable] = []

        today = dt.date.today()
        start = today - dt.timedelta(days=DEFAULT_LOOKBACK_DAYS)
        self.symbol_var = tk.StringVar(value=initial_symbol.upper())
        self.start_var = tk.StringVar(value=start.isoformat())
        self.end_var = tk.StringVar(value=today.isoformat())
        self.status_var = tk.StringVar(value="Ready.")

        self._build_widgets()
        self.root.after(POLL_INTERVAL_MS, self._poll_results)

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=8)
        container.pack(fill=tk.BOTH, expand=True)
        container.rowconfigure(1, weight=1)
        container.columnconfigure(0, weight=1)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(
            header, text="Company Dilution Data", font=("Segoe UI", 16, "bold")
        ).pack(side=tk.LEFT)
        ttk.Button(header, text="Export Current Table…", command=self.export_current).pack(
            side=tk.RIGHT
        )

        self.notebook = ttk.Notebook(container)
        self.notebook.grid(row=1, column=0, sticky="nsew")
        self._build_live_tab()
        self._build_stored_tab()
        self._build_import_tab()
        self._build_source_tab()

        footer = ttk.Frame(container)
        footer.grid(row=2, column=0, sticky="ew", pady=(6, 0))
        ttk.Label(footer, textvariable=self.status_var).pack(side=tk.LEFT)
        ttk.Label(
            footer,
            text=(
                "Filing-derived dilution and ATM availability are estimates; "
                "dollar ATM capacity is converted at daily Close."
            ),
            foreground="#666666",
        ).pack(side=tk.RIGHT)

    def _build_live_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Live Data & Chart")
        tab.rowconfigure(3, weight=1)
        tab.columnconfigure(0, weight=1)

        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self._labeled_entry(controls, "Symbol", self.symbol_var, 0, 11)
        self._labeled_entry(controls, "Start date", self.start_var, 1, 13)
        self._labeled_entry(controls, "End date", self.end_var, 2, 13)
        ttk.Button(controls, text="Fetch Live Summary", command=self.fetch_live).grid(
            row=1, column=3, padx=(8, 0)
        )
        ttk.Button(controls, text="Refresh Chart", command=self.refresh_chart).grid(
            row=1, column=4, padx=(6, 0)
        )

        metrics = ttk.Frame(tab)
        metrics.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        self.metric_vars: dict[str, tk.StringVar] = {}
        for index, (label, key) in enumerate(
            (
                ("Latest shares", "latest_shares"),
                ("Latest actual change", "actual_change"),
                ("Raw parsed potential shares", "potential_shares"),
                ("Raw potential / latest", "potential_pct"),
                ("Estimated ATM available", "atm_available"),
            )
        ):
            metrics.columnconfigure(index, weight=1)
            box = ttk.LabelFrame(metrics, text=label, padding=6)
            box.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 5, 0))
            self.metric_vars[key] = tk.StringVar(value="—")
            ttk.Label(
                box, textvariable=self.metric_vars[key], font=("Segoe UI", 12, "bold")
            ).pack()

        view_controls = ttk.Frame(tab)
        view_controls.grid(row=2, column=0, sticky="ew", pady=(0, 4))
        ttk.Label(view_controls, text="Table").pack(side=tk.LEFT)
        self.summary_view_var = tk.StringVar(value="potential_dilution_events")
        self.summary_view_box = ttk.Combobox(
            view_controls,
            textvariable=self.summary_view_var,
            state="readonly",
            width=34,
            values=(
                "share_count_history",
                "historical_dilution",
                "potential_dilution_events",
                "potential_shares_by_category",
                "registered_reserved_capacity",
                "low_confidence_review",
                "candidate_filings",
                "latest_shares_outstanding",
                "atm_capacity_history",
                "atm_capacity_daily",
            ),
        )
        self.summary_view_box.pack(side=tk.LEFT, padx=(6, 0))
        self.summary_view_box.bind("<<ComboboxSelected>>", self._show_summary_view)
        ttk.Label(
            view_controls,
            text="Double-click a row containing source_url to open the filing.",
            foreground="#666666",
        ).pack(side=tk.RIGHT)

        panes = ttk.Panedwindow(tab, orient=tk.VERTICAL)
        panes.grid(row=3, column=0, sticky="nsew")

        chart_frame = ttk.LabelFrame(panes, text="Market Cap, Share Count, and Filing Events")
        chart_frame.rowconfigure(0, weight=1)
        chart_frame.columnconfigure(0, weight=1)
        panes.add(chart_frame, weight=3)
        self.figure = Figure(figsize=(12, 5), dpi=100, constrained_layout=True)
        self.market_cap_axes = self.figure.add_subplot(211)
        self.shares_axes = self.figure.add_subplot(212, sharex=self.market_cap_axes)
        self.chart_canvas = FigureCanvasTkAgg(self.figure, master=chart_frame)
        self.chart_canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        toolbar_frame = ttk.Frame(chart_frame)
        toolbar_frame.grid(row=1, column=0, sticky="ew")
        NavigationToolbar2Tk(self.chart_canvas, toolbar_frame).update()

        table_frame = ttk.LabelFrame(panes, text="Live Result")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)
        panes.add(table_frame, weight=2)
        self.live_table = self._new_table(table_frame)
        self.live_table.grid(row=0, column=0, sticky="nsew")
        self.live_table.tree.bind("<Double-1>", lambda _event: self._open_table_url(self.live_table))
        self._draw_empty_chart("Fetch a live summary to populate the chart.")

    def _build_stored_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Stored Analytics")
        tab.rowconfigure(2, weight=1)
        tab.columnconfigure(0, weight=1)

        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.analytics_mode_var = tk.StringVar(value="Compare symbols")
        self._labeled_combo(
            controls,
            "Query",
            self.analytics_mode_var,
            0,
            ("Compare symbols", "Snapshots", "Events", "Candidate filings"),
            22,
        )
        self.analytics_symbols_var = tk.StringVar()
        self._labeled_entry(controls, "Symbols (optional)", self.analytics_symbols_var, 1, 25)
        self.analytics_start_var = tk.StringVar()
        self._labeled_entry(controls, "Start (optional)", self.analytics_start_var, 2, 13)
        self.analytics_end_var = tk.StringVar()
        self._labeled_entry(controls, "End (optional)", self.analytics_end_var, 3, 13)
        self.analytics_category_var = tk.StringVar()
        self._labeled_entry(
            controls,
            "Category / status",
            self.analytics_category_var,
            4,
            18,
        )
        self.analytics_confidence_var = tk.StringVar()
        self._labeled_entry(
            controls, "Confidence", self.analytics_confidence_var, 5, 12
        )
        ttk.Button(controls, text="Run Stored Query", command=self.run_analytics).grid(
            row=1, column=6, padx=(8, 0)
        )

        self.analytics_note_var = tk.StringVar(
            value="Queries the local dilution_tracker MySQL database; no SEC request is made."
        )
        ttk.Label(
            tab, textvariable=self.analytics_note_var, foreground="#666666"
        ).grid(row=1, column=0, sticky="w", pady=(0, 5))

        self.analytics_table = self._new_table(tab)
        self.analytics_table.grid(row=2, column=0, sticky="nsew")
        self.analytics_table.tree.bind(
            "<Double-1>", lambda _event: self._open_table_url(self.analytics_table)
        )

    def _build_import_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Import & Database")
        tab.rowconfigure(2, weight=1)
        tab.columnconfigure(0, weight=1)

        import_box = ttk.LabelFrame(tab, text="Fetch Fresh Data and Persist", padding=8)
        import_box.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        self.import_symbols_var = tk.StringVar(value=self.symbol_var.get())
        self._labeled_entry(
            import_box, "Symbols (comma or space separated)", self.import_symbols_var, 0, 38
        )
        self.import_start_var = tk.StringVar(value=self.start_var.get())
        self._labeled_entry(import_box, "Start date", self.import_start_var, 1, 13)
        self.import_end_var = tk.StringVar(value=self.end_var.get())
        self._labeled_entry(import_box, "End date", self.import_end_var, 2, 13)
        ttk.Button(import_box, text="Import Symbols", command=self.import_symbols).grid(
            row=1, column=3, padx=(8, 0)
        )
        ttk.Button(import_box, text="Import Watchlist…", command=self.import_watchlist).grid(
            row=1, column=4, padx=(6, 0)
        )

        db_box = ttk.LabelFrame(tab, text="Local Database", padding=8)
        db_box.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(db_box, text="Create / Verify Tables", command=self.setup_database).pack(
            side=tk.LEFT
        )
        ttk.Button(db_box, text="Refresh Row Counts", command=self.load_row_counts).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        ttk.Label(
            db_box,
            text="Imports use upserts; rerunning the same filing window does not append duplicates.",
            foreground="#666666",
        ).pack(side=tk.RIGHT)

        output_frame = ttk.Panedwindow(tab, orient=tk.HORIZONTAL)
        output_frame.grid(row=2, column=0, sticky="nsew")
        counts_box = ttk.LabelFrame(output_frame, text="Database Row Counts")
        counts_box.rowconfigure(0, weight=1)
        counts_box.columnconfigure(0, weight=1)
        output_frame.add(counts_box, weight=1)
        self.counts_table = self._new_table(counts_box)
        self.counts_table.grid(row=0, column=0, sticky="nsew")

        results_box = ttk.LabelFrame(output_frame, text="Last Import Result")
        results_box.rowconfigure(0, weight=1)
        results_box.columnconfigure(0, weight=1)
        output_frame.add(results_box, weight=2)
        self.import_output = scrolledtext.ScrolledText(results_box, wrap=tk.WORD)
        self.import_output.grid(row=0, column=0, sticky="nsew")

    def _build_source_tab(self) -> None:
        tab = ttk.Frame(self.notebook, padding=8)
        self.notebook.add(tab, text="Source Explorer & Validation")
        tab.rowconfigure(2, weight=1)
        tab.columnconfigure(0, weight=1)

        controls = ttk.Frame(tab)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.source_name_var = tk.StringVar(value="periodic")
        self._labeled_combo(
            controls,
            "Filing source",
            self.source_name_var,
            0,
            (
                "periodic",
                "current_event",
                "registered_offering",
                "private_offering",
                "equity_plan",
                "merger_share_issuance",
            ),
            24,
        )
        self.source_symbol_var = tk.StringVar(value=self.symbol_var.get())
        self._labeled_entry(controls, "Symbol", self.source_symbol_var, 1, 11)
        self.source_start_var = tk.StringVar(value=self.start_var.get())
        self._labeled_entry(controls, "Start date", self.source_start_var, 2, 13)
        self.source_end_var = tk.StringVar(value=self.end_var.get())
        self._labeled_entry(controls, "End date", self.source_end_var, 3, 13)
        ttk.Button(controls, text="Collect Source", command=self.collect_source).grid(
            row=1, column=4, padx=(8, 0)
        )
        ttk.Button(controls, text="Get Float JSON", command=self.get_float_json).grid(
            row=1, column=5, padx=(6, 0)
        )

        tool_controls = ttk.Frame(tab)
        tool_controls.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        self.source_result_var = tk.StringVar(value="events")
        ttk.Label(tool_controls, text="Source result").pack(side=tk.LEFT)
        source_result_box = ttk.Combobox(
            tool_controls,
            textvariable=self.source_result_var,
            values=("events", "candidates"),
            state="readonly",
            width=14,
        )
        source_result_box.pack(side=tk.LEFT, padx=(5, 10))
        source_result_box.bind("<<ComboboxSelected>>", self._show_source_result)
        ttk.Button(tool_controls, text="Run Parser on Text", command=self.run_parser).pack(
            side=tk.LEFT
        )
        ttk.Button(
            tool_controls, text="Validate Parser", command=lambda: self.run_validator("parser")
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            tool_controls, text="Validate ATM", command=lambda: self.run_validator("atm")
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            tool_controls, text="Validate Tracker", command=lambda: self.run_validator("tracker")
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            tool_controls,
            text="Validate Normalizer",
            command=lambda: self.run_validator("normalizer"),
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            tool_controls,
            text="Validate Stored Queries",
            command=lambda: self.run_validator("queries"),
        ).pack(side=tk.LEFT, padx=(6, 0))

        panes = ttk.Panedwindow(tab, orient=tk.VERTICAL)
        panes.grid(row=2, column=0, sticky="nsew")
        source_table_box = ttk.LabelFrame(panes, text="Source / Parser Rows")
        source_table_box.rowconfigure(0, weight=1)
        source_table_box.columnconfigure(0, weight=1)
        panes.add(source_table_box, weight=2)
        self.source_table = self._new_table(source_table_box)
        self.source_table.grid(row=0, column=0, sticky="nsew")
        self.source_table.tree.bind(
            "<Double-1>", lambda _event: self._open_table_url(self.source_table)
        )

        text_box = ttk.LabelFrame(
            panes, text="Disclosure Text Input / JSON and Validation Output"
        )
        text_box.rowconfigure(0, weight=1)
        text_box.columnconfigure(0, weight=1)
        panes.add(text_box, weight=1)
        self.source_text = scrolledtext.ScrolledText(text_box, wrap=tk.WORD)
        self.source_text.grid(row=0, column=0, sticky="nsew")
        self.source_text.insert(
            "1.0",
            "Paste filing disclosure text here, then choose Run Parser on Text.\n",
        )

    def fetch_live(self) -> None:
        try:
            symbol = normalize_symbol(self.symbol_var.get())
            start = parse_date(self.start_var.get(), "Start date")
            end = parse_date(self.end_var.get(), "End date")
            if end < start:
                raise ValueError("End date must be on or after start date.")
        except ValueError as exc:
            messagebox.showerror("Invalid live query", str(exc), parent=self.root)
            return

        def work() -> tuple[
            str, dict[str, pd.DataFrame], pd.DataFrame, str | None
        ]:
            tracker = DilutionTracker()
            summary = tracker.get_dilution_summary(symbol, start, end)
            price_error = None
            try:
                price_map = db_import([symbol])
                prices = price_map.get(symbol, pd.DataFrame())
            except Exception as exc:
                prices = pd.DataFrame()
                price_error = str(exc)
            atm_tracker = AtmCapacityTracker()
            atm_history = atm_tracker.build_history(
                summary.get("potential_dilution_events"),
                prices=prices,
            )
            summary["atm_capacity_history"] = atm_history
            summary["atm_capacity_daily"] = atm_tracker.build_daily_history(
                atm_history,
                prices,
            )
            return symbol, summary, prices, price_error

        self._submit(work, self._live_loaded, f"Fetch live dilution data for {symbol}")

    def _live_loaded(
        self,
        result: tuple[str, dict[str, pd.DataFrame], pd.DataFrame, str | None],
    ) -> None:
        symbol, summary, prices, price_error = result
        self.live_symbol = symbol
        self.live_summary = summary
        self.live_prices = prices
        self._show_summary_view()
        self._update_metrics()
        self._render_chart()
        events = summary.get("potential_dilution_events", pd.DataFrame())
        if price_error:
            self.status_var.set(
                f"Loaded {symbol} dilution data, but local prices were unavailable: "
                f"{price_error}"
            )
        else:
            self.status_var.set(
                f"Loaded {symbol}: {len(events):,} potential event rows and "
                f"{len(prices):,} daily price rows."
            )

    def refresh_chart(self) -> None:
        if not self.live_summary:
            self.fetch_live()
            return
        self._render_chart()

    def _show_summary_view(self, _event: tk.Event[Any] | None = None) -> None:
        frame = self.live_summary.get(self.summary_view_var.get(), pd.DataFrame())
        self.live_table.set_frame(frame)

    def _update_metrics(self) -> None:
        latest = self.live_summary.get("latest_shares_outstanding", pd.DataFrame())
        history = self.live_summary.get("historical_dilution", pd.DataFrame())
        events = self.live_summary.get("potential_shares_by_category", pd.DataFrame())
        atm_history = self.live_summary.get("atm_capacity_history", pd.DataFrame())

        latest_shares = (
            pd.to_numeric(latest.get("shares_outstanding"), errors="coerce").sum(min_count=1)
            if not latest.empty
            else np.nan
        )
        actual = (
            pd.to_numeric(history.get("actual_dilution_pct"), errors="coerce").dropna()
            if not history.empty
            else pd.Series(dtype=float)
        )
        potential_categories = {
            "underlying_warrants",
            "underlying_convertibles",
            "registered",
            "reserved",
            "issuable",
            "newly_issued",
            "atm_capacity",
            "atm_remaining",
        }
        potential_rows = (
            events.loc[events["category"].isin(potential_categories)]
            if not events.empty and "category" in events
            else pd.DataFrame()
        )
        potential_shares = (
            pd.to_numeric(potential_rows.get("quantity"), errors="coerce").sum(min_count=1)
            if not potential_rows.empty
            else np.nan
        )
        atm_available_values = (
            pd.to_numeric(
                atm_history.get("available_shares_estimate"), errors="coerce"
            ).dropna()
            if not atm_history.empty
            else pd.Series(dtype=float)
        )
        potential_pct = (
            potential_shares / latest_shares
            if pd.notna(potential_shares)
            and pd.notna(latest_shares)
            and float(latest_shares) != 0
            else np.nan
        )
        self.metric_vars["latest_shares"].set(compact_number(latest_shares))
        self.metric_vars["actual_change"].set(
            f"{actual.iloc[-1]:.2%}" if not actual.empty else "—"
        )
        self.metric_vars["potential_shares"].set(compact_number(potential_shares))
        self.metric_vars["potential_pct"].set(
            f"{potential_pct:.2%}" if pd.notna(potential_pct) else "—"
        )
        self.metric_vars["atm_available"].set(
            compact_number(atm_available_values.iloc[-1])
            if not atm_available_values.empty
            else "—"
        )

    def _render_chart(self) -> None:
        self.market_cap_axes.clear()
        self.shares_axes.clear()
        share_history = self.live_summary.get("share_count_history", pd.DataFrame())
        events = self.live_summary.get("potential_dilution_events", pd.DataFrame())
        prices = getattr(self, "live_prices", pd.DataFrame())

        market_frame = build_market_cap_frame(prices, share_history)
        if not market_frame.empty and market_frame["market_cap"].notna().any():
            self.market_cap_axes.plot(
                market_frame.index,
                market_frame["market_cap"],
                color="#1f77b4",
                linewidth=1.5,
                label="Market cap",
            )
            self.market_cap_axes.set_ylabel("Market cap")
            self.market_cap_axes.ticklabel_format(style="plain", axis="y")
            self.market_cap_axes.yaxis.set_major_formatter(
                lambda value, _position: compact_number(value, "$")
            )
            if market_frame.attrs.get("delisted_assumption"):
                self.market_cap_axes.scatter(
                    market_frame.index[-1],
                    market_frame["market_cap"].iloc[-1],
                    color="#d62728",
                    zorder=5,
                    label="Latest missing Close → 0",
                )
        else:
            self.market_cap_axes.text(
                0.5,
                0.5,
                "No overlapping local Close and share-count data",
                transform=self.market_cap_axes.transAxes,
                ha="center",
                va="center",
                color="#666666",
            )

        shares = _share_series(share_history)
        atm_daily = self.live_summary.get("atm_capacity_daily", pd.DataFrame())
        if not shares.empty:
            self.shares_axes.step(
                shares.index,
                shares.values,
                where="post",
                color="#2ca02c",
                linewidth=1.6,
                marker="o",
                markersize=3,
                label="Shares outstanding",
            )
            self.shares_axes.yaxis.set_major_formatter(
                lambda value, _position: compact_number(value)
            )
        else:
            self.shares_axes.text(
                0.5,
                0.5,
                "No share-count history returned",
                transform=self.shares_axes.transAxes,
                ha="center",
                va="center",
                color="#666666",
            )
        if (
            not atm_daily.empty
            and {"date", "available_shares_estimate"}.issubset(atm_daily.columns)
        ):
            atm_dates = pd.to_datetime(atm_daily["date"], errors="coerce")
            atm_values = pd.to_numeric(
                atm_daily["available_shares_estimate"], errors="coerce"
            )
            valid = atm_dates.notna() & atm_values.notna()
            if valid.any():
                self.shares_axes.plot(
                    atm_dates.loc[valid],
                    atm_values.loc[valid],
                    color="#17becf",
                    linewidth=1.8,
                    linestyle="--",
                    label="Estimated ATM shares available",
                )
        self.shares_axes.set_ylabel("Shares")
        self.shares_axes.set_xlabel("Date")

        self._add_event_markers(events)
        for axes in (self.market_cap_axes, self.shares_axes):
            axes.grid(alpha=0.22)
            handles, labels = axes.get_legend_handles_labels()
            if handles:
                axes.legend(loc="best", fontsize=8, ncols=min(4, len(handles)))
        self.market_cap_axes.set_title(
            f"{self.live_symbol} Market Capitalization, ATM Capacity, and Dilution Events"
        )
        self.shares_axes.xaxis.set_major_locator(mdates.AutoDateLocator())
        self.shares_axes.xaxis.set_major_formatter(
            mdates.ConciseDateFormatter(self.shares_axes.xaxis.get_major_locator())
        )
        self.chart_canvas.draw_idle()

    def _add_event_markers(self, events: pd.DataFrame) -> None:
        if events is None or events.empty or "filed_at" not in events:
            return
        event_frame = events.copy()
        event_frame["filed_at"] = pd.to_datetime(
            event_frame["filed_at"], errors="coerce"
        ).dt.normalize()
        event_frame = event_frame.dropna(subset=["filed_at"])
        event_frame = event_frame.loc[
            ~event_frame.get("category", pd.Series(index=event_frame.index)).isin(
                ["outstanding", "repurchased", "cancelled"]
            )
        ].drop_duplicates(["filed_at", "category"])
        seen_categories: set[str] = set()
        for _, row in event_frame.iterrows():
            category = str(row.get("category") or "event")
            color = EVENT_COLORS.get(category, "#d62728")
            label = category.replace("_", " ").title() if category not in seen_categories else None
            seen_categories.add(category)
            for axes in (self.market_cap_axes, self.shares_axes):
                axes.axvline(
                    row["filed_at"], color=color, alpha=0.32, linewidth=1, label=label
                )

    def _draw_empty_chart(self, message: str) -> None:
        for axes in (self.market_cap_axes, self.shares_axes):
            axes.clear()
            axes.text(
                0.5,
                0.5,
                message,
                transform=axes.transAxes,
                ha="center",
                va="center",
                color="#666666",
            )
            axes.set_axis_off()
        self.chart_canvas.draw_idle()

    def run_analytics(self) -> None:
        mode = self.analytics_mode_var.get()
        symbols = parse_symbols(self.analytics_symbols_var.get()) or None
        try:
            start = (
                parse_date(self.analytics_start_var.get(), "Start date")
                if self.analytics_start_var.get().strip()
                else None
            )
            end = (
                parse_date(self.analytics_end_var.get(), "End date")
                if self.analytics_end_var.get().strip()
                else None
            )
        except ValueError as exc:
            messagebox.showerror("Invalid stored query", str(exc), parent=self.root)
            return
        category_or_status = [
            item.lower()
            for item in parse_symbols(self.analytics_category_var.get())
        ] or None
        confidence = [
            item.lower()
            for item in parse_symbols(self.analytics_confidence_var.get())
        ] or None

        def work() -> pd.DataFrame:
            analytics = DilutionTrackerAnalytics()
            if mode == "Compare symbols":
                return analytics.compare_symbols(symbols, start, end)
            if mode == "Snapshots":
                return analytics.symbol_snapshot(symbols, start, end)
            if mode == "Events":
                return analytics.dilution_events(
                    symbols,
                    start,
                    end,
                    categories=category_or_status,
                    confidence=confidence,
                )
            return analytics.candidate_filings(
                symbols, start, end, parse_status=category_or_status
            )

        self._submit(work, self._analytics_loaded, f"Run stored {mode.lower()} query")

    def _analytics_loaded(self, frame: pd.DataFrame) -> None:
        self.analytics_table.set_frame(frame)
        self.analytics_note_var.set(f"Returned {len(frame):,} stored rows.")

    def import_symbols(self) -> None:
        symbols = parse_symbols(self.import_symbols_var.get())
        if not symbols:
            messagebox.showerror("Import", "Enter at least one symbol.", parent=self.root)
            return
        try:
            start = parse_date(self.import_start_var.get(), "Start date")
            end = parse_date(self.import_end_var.get(), "End date")
        except ValueError as exc:
            messagebox.showerror("Import", str(exc), parent=self.root)
            return

        def work() -> dict[str, Any]:
            importer = DilutionTrackerImporter(show_progress=False)
            return importer.import_symbols(symbols, start, end)

        self._submit(work, self._import_finished, f"Import {len(symbols)} symbol(s)")

    def import_watchlist(self) -> None:
        path = filedialog.askopenfilename(
            parent=self.root,
            title="Choose watchlist",
            filetypes=(("Text and CSV", "*.txt *.csv"), ("All files", "*.*")),
        )
        if not path:
            return
        try:
            start = parse_date(self.import_start_var.get(), "Start date")
            end = parse_date(self.import_end_var.get(), "End date")
        except ValueError as exc:
            messagebox.showerror("Import", str(exc), parent=self.root)
            return

        def work() -> dict[str, Any]:
            importer = DilutionTrackerImporter(show_progress=False)
            return importer.import_watchlist(path, start, end)

        self._submit(work, self._import_finished, f"Import watchlist {Path(path).name}")

    def _import_finished(self, result: dict[str, Any]) -> None:
        self.import_output.delete("1.0", tk.END)
        self.import_output.insert(
            "1.0", json.dumps(result, indent=2, default=json_default)
        )
        self.status_var.set(
            f"Import completed: {result.get('symbols', 0)} symbols, "
            f"{result.get('event_rows', 0)} event rows, "
            f"{len(result.get('errors', []))} errors."
        )
        self.load_row_counts()

    def setup_database(self) -> None:
        def work() -> dict[str, int]:
            database = DilutionTrackerDatabase()
            database.setup()
            return database.row_counts()

        self._submit(work, self._counts_loaded, "Create or verify dilution tables")

    def load_row_counts(self) -> None:
        self._submit(
            lambda: DilutionTrackerDatabase().row_counts(),
            self._counts_loaded,
            "Load dilution database row counts",
        )

    def _counts_loaded(self, counts: dict[str, int]) -> None:
        frame = pd.DataFrame(
            [{"table": table, "rows": count} for table, count in counts.items()]
        )
        self.counts_table.set_frame(frame)
        self.status_var.set("Loaded dilution_tracker database row counts.")

    def collect_source(self) -> None:
        try:
            symbol = normalize_symbol(self.source_symbol_var.get())
            start = parse_date(self.source_start_var.get(), "Start date")
            end = parse_date(self.source_end_var.get(), "End date")
        except ValueError as exc:
            messagebox.showerror("Source explorer", str(exc), parent=self.root)
            return
        source_name = self.source_name_var.get()

        def work() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            tracker = DilutionTracker()
            source = next(
                item for item in tracker.sources if item.source_name == source_name
            )
            return source.collect(symbol, start, end)

        self._submit(work, self._source_loaded, f"Collect {source_name} filings for {symbol}")

    def _source_loaded(
        self, rows: tuple[list[dict[str, Any]], list[dict[str, Any]]]
    ) -> None:
        self.source_rows = rows
        self._show_source_result()
        self.status_var.set(
            f"Source returned {len(rows[0]):,} events and {len(rows[1]):,} candidates."
        )

    def _show_source_result(self, _event: tk.Event[Any] | None = None) -> None:
        if self.source_rows is None:
            return
        selected = self.source_rows[0] if self.source_result_var.get() == "events" else self.source_rows[1]
        self.source_table.set_frame(pd.DataFrame(selected))

    def get_float_json(self) -> None:
        try:
            symbol = normalize_symbol(self.source_symbol_var.get())
        except ValueError as exc:
            messagebox.showerror("Float API", str(exc), parent=self.root)
            return

        def work() -> dict[str, Any]:
            return DilutionTracker().client.get_float(ticker=symbol)

        self._submit(work, self._show_json_output, f"Get float data for {symbol}")

    def run_parser(self) -> None:
        text_value = self.source_text.get("1.0", tk.END).strip()
        if not text_value:
            messagebox.showerror(
                "Dilution parser", "Paste disclosure text first.", parent=self.root
            )
            return
        symbol = self.source_symbol_var.get().strip().upper() or "SAMPLE"
        metadata = {
            "symbol": symbol,
            "cik": None,
            "accession_no": "manual-gui-input",
            "form_type": "MANUAL",
            "filed_at": dt.datetime.now(),
            "period_of_report": dt.date.today(),
            "company_name": symbol,
            "source_url": None,
        }
        rows = DilutionTextParser().parse(
            text_value,
            metadata,
            source_endpoint="manual_gui",
            source_section="pasted_text",
        )
        self.source_table.set_frame(pd.DataFrame(rows))
        self.status_var.set(f"Parser produced {len(rows):,} dilution event rows.")

    def run_validator(self, name: str) -> None:
        validators: dict[str, Callable[[], dict[str, Any]]] = {
            "parser": validate_dilution_text_parser_sample,
            "atm": validate_atm_capacity_tracker_sample,
            "tracker": validate_dilution_tracker_sample,
            "normalizer": validate_dilution_storage_normalizer_sample,
            "queries": validate_dilution_storage_query_sample,
        }
        self._submit(validators[name], self._show_json_output, f"Run {name} validation")

    def _show_json_output(self, result: Any) -> None:
        self.source_text.delete("1.0", tk.END)
        self.source_text.insert(
            "1.0", json.dumps(result, indent=2, default=json_default)
        )

    def export_current(self) -> None:
        table = self._current_table()
        if table is None or table.frame.empty:
            messagebox.showinfo(
                "Export", "The active tab has no table rows to export.", parent=self.root
            )
            return
        path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Export table",
            defaultextension=".csv",
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
        )
        if path:
            table.frame.to_csv(path, index=False)
            self.status_var.set(f"Exported {len(table.frame):,} rows to {path}.")

    def _current_table(self) -> DataFrameTable | None:
        selected = self.notebook.index(self.notebook.select())
        if selected == 0:
            return self.live_table
        if selected == 1:
            return self.analytics_table
        if selected == 2:
            return self.counts_table
        if selected == 3:
            return self.source_table
        return None

    def _open_table_url(self, table: DataFrameTable) -> None:
        row = table.selected_record()
        if not row:
            return
        url = row.get("source_url") or row.get("link_to_filing_details")
        if url and str(url).startswith(("http://", "https://")):
            webbrowser.open(str(url))

    def _submit(
        self,
        work: Callable[[], Any],
        callback: Callable[[Any], None],
        description: str,
    ) -> None:
        if self.closing:
            return
        self.status_var.set(f"{description}…")
        future = self.executor.submit(work)

        def done(completed: Any) -> None:
            try:
                self.results.put((callback, completed.result(), None, description))
            except BaseException as exc:
                self.results.put((callback, None, exc, description))

        future.add_done_callback(done)

    def _poll_results(self) -> None:
        if self.closing:
            return
        while True:
            try:
                callback, result, error, description = self.results.get_nowait()
            except queue.Empty:
                break
            if error is not None:
                self.status_var.set(f"{description} failed.")
                messagebox.showerror(
                    "Dilution Data",
                    f"{description} failed:\n\n{error}",
                    parent=self.root,
                )
            else:
                try:
                    callback(result)
                except Exception as exc:
                    self.status_var.set(f"Could not display result for {description}.")
                    messagebox.showerror(
                        "Dilution Data",
                        f"Could not display result:\n\n{exc}",
                        parent=self.root,
                    )
        self.root.after(POLL_INTERVAL_MS, self._poll_results)

    def _new_table(self, master: tk.Misc) -> DataFrameTable:
        table = DataFrameTable(master)
        self.tables.append(table)
        return table

    @staticmethod
    def _labeled_entry(
        master: tk.Misc,
        label: str,
        variable: tk.StringVar,
        column: int,
        width: int,
    ) -> ttk.Entry:
        ttk.Label(master, text=label).grid(
            row=0, column=column, sticky="w", padx=(8 if column else 0, 0)
        )
        entry = ttk.Entry(master, textvariable=variable, width=width)
        entry.grid(
            row=1, column=column, sticky="ew", padx=(8 if column else 0, 0)
        )
        return entry

    @staticmethod
    def _labeled_combo(
        master: tk.Misc,
        label: str,
        variable: tk.StringVar,
        column: int,
        values: Iterable[str],
        width: int,
    ) -> ttk.Combobox:
        ttk.Label(master, text=label).grid(
            row=0, column=column, sticky="w", padx=(8 if column else 0, 0)
        )
        combo = ttk.Combobox(
            master,
            textvariable=variable,
            values=tuple(values),
            width=width,
            state="readonly",
        )
        combo.grid(
            row=1, column=column, sticky="ew", padx=(8 if column else 0, 0)
        )
        return combo

    def close(self) -> None:
        self.closing = True
        self.executor.shutdown(wait=False, cancel_futures=True)
        self.root.destroy()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="", help="Initial ticker symbol")
    args = parser.parse_args()

    root = tk.Tk()
    DilutionDataGui(root, initial_symbol=args.symbol)
    root.mainloop()


if __name__ == "__main__":
    main()
