from __future__ import annotations

import datetime as dt
import math
import re
import sys
import tkinter as tk
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Literal
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import bindparam, create_engine, inspect, text
from sqlalchemy.engine import Engine


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from market_data.api_keys import (  # type: ignore[import-not-found]
    database_password,
    intraday_stream_database,
)


STOCKS_SCHEMA = "stocks"
RESULTS_SCHEMA = "results_finvizsearch"
THEMES_TABLE = "finviz_themes"
MEMBERSHIP_TABLE = "finviz_symbol_themes"
STREAM_SCHEMA = "intraday_price_stream"
STREAM_TABLE = "ohlcv_1m"
STREAM_USER = "price_data_streamer"
INTRADAY_INTERVAL = "Intraday (%)"
EASTERN = ZoneInfo("America/New_York")
SYMBOL_CHUNK_SIZE = 750
NONE_INTERVAL = "None"
DATE_TABLE_PATTERN = re.compile(r"^\d{4}_\d{2}_\d{2}$")
CategoryMode = Literal["Theme", "Sub-Theme"]

INTERVAL_COLUMNS: dict[str, str] = {
    "Daily (%)": "Change",
    "Week (%)": "Performance (Week)(%)",
    "Month (%)": "Performance (Month)(%)",
    "Quarter (%)": "Performance (Quarter)(%)",
    "Half Year (%)": "Performance (Half Year)(%)",
    "Year (%)": "Performance (Year)(%)",
    "Year To Date (%)": "Performance (YearToDate)",
}
INTERVAL_SCALES: dict[str, float] = {
    display_name: (
        1.0 if database_column == "Performance (YearToDate)" else 100.0
    )
    for display_name, database_column in INTERVAL_COLUMNS.items()
}
INTERVAL_SCALES[INTRADAY_INTERVAL] = 1.0


def mysql_identifier(name: str) -> str:
    return f"`{str(name).replace('`', '``')}`"


def make_engine() -> Engine:
    password = quote_plus(database_password)
    url = f"mysql+pymysql://root:{password}@127.0.0.1:3306"
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )


def make_stream_engine() -> Engine:
    password = quote_plus(intraday_stream_database)
    url = (
        f"mysql+pymysql://{STREAM_USER}:{password}@127.0.0.1:3306/"
        f"{STREAM_SCHEMA}"
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )


def latest_snapshot_table(table_names: list[str]) -> tuple[str, dt.date]:
    dated_tables: list[tuple[dt.date, str]] = []
    for table_name in table_names:
        if not DATE_TABLE_PATTERN.fullmatch(table_name):
            continue
        try:
            table_date = dt.datetime.strptime(table_name, "%Y_%m_%d").date()
        except ValueError:
            continue
        dated_tables.append((table_date, table_name))

    if not dated_tables:
        raise RuntimeError(
            f"No YYYY_MM_DD tables were found in {RESULTS_SCHEMA}."
        )
    table_date, table_name = max(dated_tables)
    return table_name, table_date


@dataclass(frozen=True)
class DashboardData:
    snapshot_date: dt.date
    category_rows: list[dict[str, Any]]
    symbol_rows: dict[str, list[dict[str, Any]]]
    latest_timestamp: dt.datetime | None = None
    eligible_symbols: int = 0
    priced_symbols: int = 0


class ThemeDataRepository:
    def __init__(self, engine: Engine, stream_engine: Engine | None = None) -> None:
        self.engine = engine
        self.stream_engine = stream_engine

    def load(
        self,
        mode: CategoryMode,
        intervals: tuple[str, ...],
    ) -> DashboardData:
        table_names = inspect(self.engine).get_table_names(schema=RESULTS_SCHEMA)
        snapshot_table, snapshot_date = latest_snapshot_table(table_names)
        self._validate_result_columns(snapshot_table, intervals)
        frame = self._fetch_joined_data(snapshot_table, mode, intervals)
        category_rows, symbol_rows = self._transform(frame, intervals)
        return DashboardData(snapshot_date, category_rows, symbol_rows)

    def load_intraday(
        self,
        mode: CategoryMode,
        trading_day: dt.date | None = None,
    ) -> DashboardData:
        if self.stream_engine is None:
            raise RuntimeError("The intraday stream database is not configured.")

        table_names = inspect(self.engine).get_table_names(schema=RESULTS_SCHEMA)
        snapshot_table, snapshot_date = latest_snapshot_table(table_names)
        self._validate_named_result_columns(snapshot_table, {"Ticker", "Price"})
        frame = self._fetch_intraday_baselines(snapshot_table, mode)
        frame["symbol"] = frame["symbol"].astype("string").str.strip().str.upper()
        frame["baseline_price"] = pd.to_numeric(
            frame["baseline_price"], errors="coerce"
        )

        eligible_mask = frame["symbol"].notna() & frame["baseline_price"].gt(0)
        eligible_symbols = sorted(frame.loc[eligible_mask, "symbol"].unique())
        trading_day = trading_day or dt.datetime.now(EASTERN).date()
        latest_prices = self._fetch_latest_intraday_prices(
            eligible_symbols, trading_day
        )

        if latest_prices:
            latest_frame = pd.DataFrame(
                (
                    {
                        "symbol": symbol,
                        "latest_price": values["latest_price"],
                        "latest_timestamp": values["latest_timestamp"],
                    }
                    for symbol, values in latest_prices.items()
                )
            )
            frame = frame.merge(latest_frame, on="symbol", how="left")
        else:
            frame["latest_price"] = pd.NA
            frame["latest_timestamp"] = pd.NaT

        frame["latest_price"] = pd.to_numeric(
            frame["latest_price"], errors="coerce"
        )
        frame[INTRADAY_INTERVAL] = (
            frame["latest_price"].div(frame["baseline_price"]).sub(1.0).mul(100.0)
        )
        category_rows, symbol_rows = self._transform(
            frame, (INTRADAY_INTERVAL,)
        )
        valid_timestamps = pd.to_datetime(
            frame["latest_timestamp"], errors="coerce"
        ).dropna()
        latest_timestamp = (
            valid_timestamps.max().to_pydatetime()
            if not valid_timestamps.empty
            else None
        )
        priced_symbols = int(
            frame.loc[frame[INTRADAY_INTERVAL].notna(), "symbol"].nunique()
        )
        return DashboardData(
            snapshot_date=snapshot_date,
            category_rows=category_rows,
            symbol_rows=symbol_rows,
            latest_timestamp=latest_timestamp,
            eligible_symbols=len(eligible_symbols),
            priced_symbols=priced_symbols,
        )

    def _validate_result_columns(
        self,
        snapshot_table: str,
        intervals: tuple[str, ...],
    ) -> None:
        self._validate_named_result_columns(
            snapshot_table,
            {"Ticker", *(INTERVAL_COLUMNS[name] for name in intervals)},
        )

    def _validate_named_result_columns(
        self,
        snapshot_table: str,
        required: set[str],
    ) -> None:
        available = {
            column["name"]
            for column in inspect(self.engine).get_columns(
                snapshot_table, schema=RESULTS_SCHEMA
            )
        }
        missing = required - available
        if missing:
            missing_text = ", ".join(sorted(missing))
            raise RuntimeError(
                f"{RESULTS_SCHEMA}.{snapshot_table} is missing: {missing_text}"
            )

    def _fetch_intraday_baselines(
        self,
        snapshot_table: str,
        mode: CategoryMode,
    ) -> pd.DataFrame:
        if mode == "Theme":
            label_column = "theme"
            filter_column = "finviz_theme_filter"
            filter_prefix = "theme_"
        else:
            label_column = "subtheme_full_label"
            filter_column = "finviz_subtheme_filter"
            filter_prefix = "subtheme_"

        query = text(
            f"""
            SELECT
                taxonomy.category_label,
                taxonomy.filter_token,
                members.symbol,
                results.{mysql_identifier("Price")} AS baseline_price
            FROM (
                SELECT DISTINCT
                    {mysql_identifier(label_column)} AS category_label,
                    {mysql_identifier(filter_column)} AS filter_token
                FROM {mysql_identifier(STOCKS_SCHEMA)}.{mysql_identifier(THEMES_TABLE)}
                WHERE {mysql_identifier(label_column)} IS NOT NULL
                  AND {mysql_identifier(label_column)} <> ''
                  AND {mysql_identifier(filter_column)} LIKE :filter_pattern
                  AND {mysql_identifier(filter_column)} <> :empty_filter
            ) AS taxonomy
            LEFT JOIN (
                SELECT DISTINCT symbol, theme_subtheme
                FROM {mysql_identifier(STOCKS_SCHEMA)}.{mysql_identifier(MEMBERSHIP_TABLE)}
                WHERE symbol IS NOT NULL
                  AND symbol <> ''
            ) AS members
              ON members.theme_subtheme COLLATE utf8mb4_unicode_ci
               = taxonomy.filter_token COLLATE utf8mb4_unicode_ci
            LEFT JOIN (
                SELECT
                    {mysql_identifier("Ticker")},
                    {mysql_identifier("Price")}
                FROM {mysql_identifier(RESULTS_SCHEMA)}.{mysql_identifier(snapshot_table)}
            ) AS results
              ON UPPER(TRIM(results.{mysql_identifier("Ticker")}))
                   COLLATE utf8mb4_unicode_ci
               = UPPER(TRIM(members.symbol)) COLLATE utf8mb4_unicode_ci
            ORDER BY taxonomy.category_label, members.symbol
            """
        )
        with self.engine.connect() as connection:
            return pd.read_sql_query(
                query,
                connection,
                params={
                    "filter_pattern": f"{filter_prefix}%",
                    "empty_filter": filter_prefix,
                },
            )

    def _fetch_latest_intraday_prices(
        self,
        symbols: list[str],
        trading_day: dt.date,
    ) -> dict[str, dict[str, Any]]:
        if not symbols or self.stream_engine is None:
            return {}

        day_start = dt.datetime.combine(trading_day, dt.time.min)
        day_end = day_start + dt.timedelta(days=1)
        query = text(
            f"""
            SELECT stream.Symbol, stream.Timestamp, stream.Close
            FROM {mysql_identifier(STREAM_TABLE)} AS stream
            INNER JOIN (
                SELECT Symbol, MAX(Timestamp) AS latest_timestamp
                FROM {mysql_identifier(STREAM_TABLE)}
                WHERE Timestamp >= :day_start
                  AND Timestamp < :day_end
                  AND Symbol IN :symbols
                GROUP BY Symbol
            ) AS latest
              ON stream.Symbol = latest.Symbol
             AND stream.Timestamp = latest.latest_timestamp
            """
        ).bindparams(bindparam("symbols", expanding=True))

        prices: dict[str, dict[str, Any]] = {}
        with self.stream_engine.connect() as connection:
            for start in range(0, len(symbols), SYMBOL_CHUNK_SIZE):
                symbol_group = symbols[start : start + SYMBOL_CHUNK_SIZE]
                rows = connection.execute(
                    query,
                    {
                        "day_start": day_start,
                        "day_end": day_end,
                        "symbols": symbol_group,
                    },
                )
                for row in rows:
                    close_price = pd.to_numeric(row.Close, errors="coerce")
                    if pd.isna(close_price):
                        continue
                    prices[str(row.Symbol).strip().upper()] = {
                        "latest_price": float(close_price),
                        "latest_timestamp": row.Timestamp,
                    }
        return prices

    def _fetch_joined_data(
        self,
        snapshot_table: str,
        mode: CategoryMode,
        intervals: tuple[str, ...],
    ) -> pd.DataFrame:
        if mode == "Theme":
            label_column = "theme"
            filter_column = "finviz_theme_filter"
            filter_prefix = "theme_"
        else:
            label_column = "subtheme_full_label"
            filter_column = "finviz_subtheme_filter"
            filter_prefix = "subtheme_"

        interval_selects = ",\n".join(
            (
                f"results.{mysql_identifier(INTERVAL_COLUMNS[display_name])} "
                f"AS {mysql_identifier(display_name)}"
            )
            for display_name in intervals
        )
        query = text(
            f"""
            SELECT
                taxonomy.category_label,
                taxonomy.filter_token,
                members.symbol,
                {interval_selects}
            FROM (
                SELECT DISTINCT
                    {mysql_identifier(label_column)} AS category_label,
                    {mysql_identifier(filter_column)} AS filter_token
                FROM {mysql_identifier(STOCKS_SCHEMA)}.{mysql_identifier(THEMES_TABLE)}
                WHERE {mysql_identifier(label_column)} IS NOT NULL
                  AND {mysql_identifier(label_column)} <> ''
                  AND {mysql_identifier(filter_column)} LIKE :filter_pattern
                  AND {mysql_identifier(filter_column)} <> :empty_filter
            ) AS taxonomy
            LEFT JOIN (
                SELECT DISTINCT symbol, theme_subtheme
                FROM {mysql_identifier(STOCKS_SCHEMA)}.{mysql_identifier(MEMBERSHIP_TABLE)}
                WHERE symbol IS NOT NULL
                  AND symbol <> ''
            ) AS members
              ON members.theme_subtheme COLLATE utf8mb4_unicode_ci
               = taxonomy.filter_token COLLATE utf8mb4_unicode_ci
            LEFT JOIN (
                SELECT
                    {mysql_identifier("Ticker")},
                    {", ".join(mysql_identifier(INTERVAL_COLUMNS[name]) for name in intervals)}
                FROM {mysql_identifier(RESULTS_SCHEMA)}.{mysql_identifier(snapshot_table)}
            ) AS results
              ON UPPER(TRIM(results.{mysql_identifier("Ticker")}))
                   COLLATE utf8mb4_unicode_ci
               = UPPER(TRIM(members.symbol)) COLLATE utf8mb4_unicode_ci
            ORDER BY taxonomy.category_label, members.symbol
            """
        )
        with self.engine.connect() as connection:
            return pd.read_sql_query(
                query,
                connection,
                params={
                    "filter_pattern": f"{filter_prefix}%",
                    "empty_filter": filter_prefix,
                },
            )

    @staticmethod
    def _transform(
        frame: pd.DataFrame,
        intervals: tuple[str, ...],
    ) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        for interval in intervals:
            frame[interval] = (
                pd.to_numeric(frame[interval], errors="coerce")
                * INTERVAL_SCALES[interval]
            )

        frame["symbol"] = frame["symbol"].astype("string").str.strip().str.upper()
        symbol_level = (
            frame.groupby(
                ["filter_token", "category_label", "symbol"],
                dropna=False,
                as_index=False,
            )[list(intervals)]
            .mean()
        )

        categories = (
            symbol_level.groupby(
                ["filter_token", "category_label"],
                dropna=False,
                as_index=False,
            )[list(intervals)]
            .mean()
            .sort_values("category_label", key=lambda values: values.str.casefold())
        )

        category_rows = [
            ThemeDataRepository._clean_record(record)
            for record in categories.to_dict(orient="records")
        ]
        symbol_rows: dict[str, list[dict[str, Any]]] = {}
        for filter_token, group in symbol_level.groupby("filter_token", sort=False):
            valid_symbols = group[group["symbol"].notna()].sort_values("symbol")
            symbol_rows[str(filter_token)] = [
                ThemeDataRepository._clean_record(record)
                for record in valid_symbols.to_dict(orient="records")
            ]
        return category_rows, symbol_rows

    @staticmethod
    def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
        return {
            key: (None if pd.isna(value) else value)
            for key, value in record.items()
        }


class ThemePerformanceDashboard:
    POLL_INTERVAL_MS = 100

    def __init__(self, root: tk.Tk, repository: ThemeDataRepository) -> None:
        self.root = root
        self.repository = repository
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="theme-data")
        self.load_future: Future[DashboardData] | None = None
        self.intraday_future: Future[DashboardData] | None = None
        self.intraday_after_id: str | None = None
        self.intraday_running = False
        self.closing = False
        self.current_data: DashboardData | None = None
        self.intraday_data: DashboardData | None = None
        self.category_rows: list[dict[str, Any]] = []
        self.intraday_category_rows: list[dict[str, Any]] = []
        self.current_symbol_rows: list[dict[str, Any]] = []
        self.category_sort: tuple[str, bool] | None = None
        self.intraday_category_sort: tuple[str, bool] | None = None
        self.symbol_sort: tuple[str, bool] | None = None
        self.active_intervals: tuple[str, ...] = ()
        self.symbol_intervals: tuple[str, ...] = ()
        self.active_symbol_source: Literal["snapshot", "intraday"] | None = None

        self.root.title("Theme Performance Dashboard")
        self.root.geometry("1180x920")
        self.root.minsize(850, 700)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        self.mode_var = tk.StringVar(value="Theme")
        self.intraday_mode_var = tk.StringVar(value="Theme")
        self.intraday_minutes_var = tk.StringVar(value="15")
        self.interval_vars = (
            tk.StringVar(value="Week (%)"),
            tk.StringVar(value="Month (%)"),
            tk.StringVar(value="Quarter (%)"),
        )
        self.status_var = tk.StringVar(value="Ready")
        self.category_means_var = tk.StringVar(value="Column means: —")
        self.intraday_status_var = tk.StringVar(
            value="Real-time capture is stopped."
        )
        self.symbol_means_var = tk.StringVar(value="Column means: —")
        self.symbol_status_var = tk.StringVar(
            value="Select a row in either category table."
        )

        self._build_widgets()
        self.refresh()

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=3)
        container.rowconfigure(4, weight=3)
        container.rowconfigure(6, weight=2)

        controls = ttk.LabelFrame(container, text="Analysis", padding=8)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 8))

        ttk.Label(controls, text="Category").grid(row=0, column=0, sticky="w")
        self.theme_radio = ttk.Radiobutton(
            controls,
            text="Theme",
            value="Theme",
            variable=self.mode_var,
        )
        self.theme_radio.grid(row=1, column=0, sticky="w", padx=(0, 12))
        self.subtheme_radio = ttk.Radiobutton(
            controls,
            text="Sub-Theme",
            value="Sub-Theme",
            variable=self.mode_var,
        )
        self.subtheme_radio.grid(row=1, column=1, sticky="w", padx=(0, 20))

        interval_values = (NONE_INTERVAL, *INTERVAL_COLUMNS.keys())
        self.interval_boxes: list[ttk.Combobox] = []
        for index, variable in enumerate(self.interval_vars, start=1):
            column = index + 1
            ttk.Label(controls, text=f"Interval {index}").grid(
                row=0, column=column, sticky="w"
            )
            box = ttk.Combobox(
                controls,
                textvariable=variable,
                values=interval_values,
                state="readonly",
                width=19,
            )
            box.grid(row=1, column=column, sticky="ew", padx=(0, 8))
            controls.columnconfigure(column, weight=1)
            self.interval_boxes.append(box)

        self.refresh_button = ttk.Button(
            controls, text="Refresh", command=self.refresh
        )
        self.refresh_button.grid(row=1, column=5, sticky="ew")

        category_frame = ttk.LabelFrame(
            container, text="Theme Performance", padding=5
        )
        category_frame.grid(row=1, column=0, sticky="nsew")
        category_frame.columnconfigure(0, weight=1)
        category_frame.rowconfigure(0, weight=1)
        self.category_tree = self._make_tree(category_frame)
        self.category_tree.bind("<<TreeviewSelect>>", self._on_category_selected)

        category_footer = ttk.Frame(container)
        category_footer.grid(row=2, column=0, sticky="ew", pady=(4, 8))
        ttk.Label(category_footer, textvariable=self.category_means_var).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(category_footer, textvariable=self.status_var).grid(
            row=1, column=0, sticky="w"
        )

        intraday_controls = ttk.LabelFrame(
            container, text="Real-Time Capture", padding=8
        )
        intraday_controls.grid(row=3, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(intraday_controls, text="Category").grid(
            row=0, column=0, sticky="w"
        )
        self.intraday_theme_radio = ttk.Radiobutton(
            intraday_controls,
            text="Theme",
            value="Theme",
            variable=self.intraday_mode_var,
        )
        self.intraday_theme_radio.grid(row=1, column=0, sticky="w", padx=(0, 12))
        self.intraday_subtheme_radio = ttk.Radiobutton(
            intraday_controls,
            text="Sub-Theme",
            value="Sub-Theme",
            variable=self.intraday_mode_var,
        )
        self.intraday_subtheme_radio.grid(
            row=1, column=1, sticky="w", padx=(0, 20)
        )
        ttk.Label(intraday_controls, text="Update every (minutes)").grid(
            row=0, column=2, sticky="w"
        )
        self.intraday_minutes_entry = ttk.Entry(
            intraday_controls,
            textvariable=self.intraday_minutes_var,
            width=10,
        )
        self.intraday_minutes_entry.grid(
            row=1, column=2, sticky="w", padx=(0, 8)
        )
        self.intraday_start_button = ttk.Button(
            intraday_controls,
            text="Start",
            command=self.start_intraday_capture,
        )
        self.intraday_start_button.grid(row=1, column=3, sticky="ew", padx=(0, 8))
        self.intraday_stop_button = ttk.Button(
            intraday_controls,
            text="Stop",
            command=self.stop_intraday_capture,
            state=tk.DISABLED,
        )
        self.intraday_stop_button.grid(row=1, column=4, sticky="ew")
        intraday_controls.columnconfigure(5, weight=1)

        intraday_frame = ttk.LabelFrame(
            container, text="Intraday, Real-Time", padding=5
        )
        intraday_frame.grid(row=4, column=0, sticky="nsew")
        intraday_frame.columnconfigure(0, weight=1)
        intraday_frame.rowconfigure(0, weight=1)
        self.intraday_category_tree = self._make_tree(intraday_frame)
        self.intraday_category_tree.bind(
            "<<TreeviewSelect>>", self._on_intraday_category_selected
        )
        self._configure_tree(
            self.intraday_category_tree,
            ("category_label", INTRADAY_INTERVAL),
            "intraday_category",
        )

        ttk.Label(container, textvariable=self.intraday_status_var).grid(
            row=5, column=0, sticky="w", pady=(4, 8)
        )

        self.symbol_frame = ttk.LabelFrame(
            container, text="Constituent Performance", padding=5
        )
        self.symbol_frame.grid(row=6, column=0, sticky="nsew")
        self.symbol_frame.columnconfigure(0, weight=1)
        self.symbol_frame.rowconfigure(0, weight=1)
        self.symbol_tree = self._make_tree(self.symbol_frame)

        symbol_footer = ttk.Frame(container)
        symbol_footer.grid(row=7, column=0, sticky="ew", pady=(4, 0))
        ttk.Label(symbol_footer, textvariable=self.symbol_means_var).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(symbol_footer, textvariable=self.symbol_status_var).grid(
            row=1, column=0, sticky="w"
        )

    @staticmethod
    def _make_tree(parent: ttk.LabelFrame) -> ttk.Treeview:
        tree = ttk.Treeview(parent, show="headings", selectmode="browse")
        y_scroll = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=tree.yview)
        x_scroll = ttk.Scrollbar(parent, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        return tree

    def selected_intervals(self) -> tuple[str, ...] | None:
        intervals = tuple(
            value
            for value in (variable.get() for variable in self.interval_vars)
            if value != NONE_INTERVAL
        )
        if not intervals:
            messagebox.showwarning(
                "Missing Interval", "Select at least one performance interval."
            )
            return None
        if len(set(intervals)) != len(intervals):
            messagebox.showwarning(
                "Duplicate Interval", "Select each performance interval only once."
            )
            return None
        return intervals

    def refresh(self) -> None:
        if self.load_future is not None:
            return
        intervals = self.selected_intervals()
        if intervals is None:
            return

        mode = self.mode_var.get()
        if mode not in ("Theme", "Sub-Theme"):
            return

        self._set_loading(True)
        self.status_var.set("Loading the latest Finviz snapshot...")
        self.load_future = self.executor.submit(
            self.repository.load,
            mode,
            intervals,
        )
        self.root.after(self.POLL_INTERVAL_MS, self._poll_load)

    def _poll_load(self) -> None:
        future = self.load_future
        if future is None:
            return
        if not future.done():
            self.root.after(self.POLL_INTERVAL_MS, self._poll_load)
            return

        self.load_future = None
        self._set_loading(False)
        try:
            data = future.result()
        except Exception as exc:
            self.status_var.set("Load failed")
            messagebox.showerror("Dashboard Load Failed", str(exc))
            return

        self.current_data = data
        self.active_intervals = tuple(
            value
            for value in (variable.get() for variable in self.interval_vars)
            if value != NONE_INTERVAL
        )
        self.category_rows = list(data.category_rows)
        self.category_sort = None
        self._configure_tables()
        self._populate_category_table()
        if self.active_symbol_source != "intraday":
            self._clear_symbol_table()
        self.status_var.set(
            f"Snapshot {data.snapshot_date:%Y-%m-%d} | "
            f"{len(data.category_rows)} categories loaded | Select a row for symbols"
        )

    def _set_loading(self, loading: bool) -> None:
        control_state = tk.DISABLED if loading else tk.NORMAL
        combo_state = tk.DISABLED if loading else "readonly"
        self.refresh_button.configure(state=control_state)
        self.theme_radio.configure(state=control_state)
        self.subtheme_radio.configure(state=control_state)
        for box in self.interval_boxes:
            box.configure(state=combo_state)

    def _intraday_minutes(self, show_error: bool = True) -> float | None:
        try:
            minutes = float(self.intraday_minutes_var.get().strip())
        except ValueError:
            minutes = 0.0
        if minutes <= 0:
            if show_error:
                messagebox.showwarning(
                    "Invalid Update Interval",
                    "Enter an update interval greater than zero minutes.",
                )
            return None
        return minutes

    def start_intraday_capture(self) -> None:
        if self._intraday_minutes() is None:
            return
        self.intraday_running = True
        self.intraday_start_button.configure(state=tk.DISABLED)
        self.intraday_stop_button.configure(state=tk.NORMAL)
        if self.intraday_after_id is not None:
            self.root.after_cancel(self.intraday_after_id)
            self.intraday_after_id = None
        self.refresh_intraday()

    def stop_intraday_capture(self) -> None:
        self.intraday_running = False
        if self.intraday_after_id is not None:
            self.root.after_cancel(self.intraday_after_id)
            self.intraday_after_id = None
        self.intraday_start_button.configure(state=tk.NORMAL)
        self.intraday_stop_button.configure(state=tk.DISABLED)
        if self.intraday_future is None:
            self.intraday_status_var.set("Real-time capture is stopped.")
        else:
            self.intraday_status_var.set(
                "Stopping after the current real-time capture completes..."
            )

    def refresh_intraday(self) -> None:
        self.intraday_after_id = None
        if self.intraday_future is not None or self.closing:
            return
        mode = self.intraday_mode_var.get()
        if mode not in ("Theme", "Sub-Theme"):
            return
        self.intraday_status_var.set("Capturing current intraday prices...")
        self.intraday_future = self.executor.submit(
            self.repository.load_intraday,
            mode,
        )
        self.root.after(self.POLL_INTERVAL_MS, self._poll_intraday_load)

    def _poll_intraday_load(self) -> None:
        future = self.intraday_future
        if future is None or self.closing:
            return
        if not future.done():
            self.root.after(self.POLL_INTERVAL_MS, self._poll_intraday_load)
            return

        self.intraday_future = None
        try:
            data = future.result()
        except Exception as exc:
            self.intraday_status_var.set(f"Real-time capture failed: {exc}")
            if self.intraday_running:
                self._schedule_intraday_capture()
            return

        selected = self.intraday_category_tree.selection()
        selected_token = selected[0] if selected else None
        self.intraday_data = data
        self.intraday_category_rows = list(data.category_rows)
        self.intraday_category_sort = None
        self._populate_intraday_category_table()

        if (
            selected_token is not None
            and self.intraday_category_tree.exists(selected_token)
        ):
            self.intraday_category_tree.selection_set(selected_token)
            if self.active_symbol_source == "intraday":
                self._show_intraday_symbols(selected_token)
        elif self.active_symbol_source == "intraday":
            self._clear_symbol_table()

        as_of = (
            data.latest_timestamp.strftime("%Y-%m-%d %H:%M")
            if data.latest_timestamp is not None
            else "no current-day bars"
        )
        self.intraday_status_var.set(
            f"As of {as_of} ET | Snapshot baseline {data.snapshot_date:%Y-%m-%d} | "
            f"{data.priced_symbols}/{data.eligible_symbols} symbols priced"
        )
        if self.intraday_running:
            self._schedule_intraday_capture()
        else:
            self.intraday_start_button.configure(state=tk.NORMAL)
            self.intraday_stop_button.configure(state=tk.DISABLED)

    def _schedule_intraday_capture(self) -> None:
        minutes = self._intraday_minutes()
        if minutes is None:
            self.stop_intraday_capture()
            return
        delay_ms = max(1, round(minutes * 60_000))
        self.intraday_after_id = self.root.after(
            delay_ms, self.refresh_intraday
        )

    def _configure_tables(self) -> None:
        category_columns = ("category_label", *self.active_intervals)
        self._configure_tree(
            self.category_tree,
            category_columns,
            "snapshot_category",
        )

    def _configure_tree(
        self,
        tree: ttk.Treeview,
        columns: tuple[str, ...],
        table_name: Literal[
            "snapshot_category", "intraday_category", "symbol"
        ],
    ) -> None:
        tree.configure(columns=columns)
        for column in columns:
            heading = (
                "Theme / Sub-Theme"
                if column == "category_label"
                else "Symbol" if column == "symbol" else column
            )
            tree.heading(
                column,
                text=heading,
                command=lambda col=column, table=table_name: self.sort_table(
                    table, col
                ),
            )
            if column in ("category_label", "symbol"):
                width = 330 if column == "category_label" else 140
                anchor = tk.W
                stretch = True
            else:
                width = 145
                anchor = tk.E
                stretch = False
            tree.column(
                column,
                width=width,
                minwidth=90,
                anchor=anchor,
                stretch=stretch,
            )

    def _populate_category_table(self) -> None:
        self.category_tree.delete(*self.category_tree.get_children())
        for row in self.category_rows:
            filter_token = str(row["filter_token"])
            values = (
                row["category_label"],
                *(self.format_percent(row[name]) for name in self.active_intervals),
            )
            self.category_tree.insert("", tk.END, iid=filter_token, values=values)
        self.category_means_var.set(
            self.mean_summary(self.category_rows, self.active_intervals)
        )

    def _populate_intraday_category_table(self) -> None:
        self.intraday_category_tree.delete(
            *self.intraday_category_tree.get_children()
        )
        for row in self.intraday_category_rows:
            filter_token = str(row["filter_token"])
            values = (
                row["category_label"],
                self.format_percent(row[INTRADAY_INTERVAL]),
            )
            self.intraday_category_tree.insert(
                "", tk.END, iid=filter_token, values=values
            )

    def _on_category_selected(self, _event: tk.Event[Any]) -> None:
        selection = self.category_tree.selection()
        if not selection or self.current_data is None:
            return
        self.intraday_category_tree.selection_remove(
            *self.intraday_category_tree.selection()
        )
        self._show_snapshot_symbols(selection[0])

    def _show_snapshot_symbols(self, filter_token: str) -> None:
        if self.current_data is None:
            return
        self.active_symbol_source = "snapshot"
        self.symbol_intervals = self.active_intervals
        self.current_symbol_rows = list(
            self.current_data.symbol_rows.get(filter_token, [])
        )
        self.symbol_sort = None
        self._configure_tree(
            self.symbol_tree,
            ("symbol", *self.symbol_intervals),
            "symbol",
        )
        self._populate_symbol_table()
        category_name = self.category_tree.set(filter_token, "category_label")
        self.symbol_frame.configure(text="Constituent Performance — Snapshot")
        self.symbol_status_var.set(
            f"Snapshot {self.current_data.snapshot_date:%Y-%m-%d} | "
            f"{category_name}: {len(self.current_symbol_rows)} symbols"
        )

    def _on_intraday_category_selected(self, _event: tk.Event[Any]) -> None:
        selection = self.intraday_category_tree.selection()
        if not selection or self.intraday_data is None:
            return
        self.category_tree.selection_remove(*self.category_tree.selection())
        self._show_intraday_symbols(selection[0])

    def _show_intraday_symbols(self, filter_token: str) -> None:
        if self.intraday_data is None:
            return
        self.active_symbol_source = "intraday"
        self.symbol_intervals = (INTRADAY_INTERVAL,)
        self.current_symbol_rows = list(
            self.intraday_data.symbol_rows.get(filter_token, [])
        )
        self.symbol_sort = None
        self._configure_tree(
            self.symbol_tree,
            ("symbol", *self.symbol_intervals),
            "symbol",
        )
        self._populate_symbol_table()
        category_name = self.intraday_category_tree.set(
            filter_token, "category_label"
        )
        as_of = (
            self.intraday_data.latest_timestamp.strftime("%Y-%m-%d %H:%M")
            if self.intraday_data.latest_timestamp is not None
            else "no current-day bars"
        )
        self.symbol_frame.configure(text="Constituent Performance — Intraday")
        self.symbol_status_var.set(
            f"As of {as_of} ET | {category_name}: "
            f"{len(self.current_symbol_rows)} symbols"
        )

    def _clear_symbol_table(self) -> None:
        self.active_symbol_source = None
        self.current_symbol_rows = []
        self.symbol_intervals = ()
        self.symbol_sort = None
        self.symbol_tree.delete(*self.symbol_tree.get_children())
        self.symbol_frame.configure(text="Constituent Performance")
        self.symbol_means_var.set("Column means: —")
        self.symbol_status_var.set("Select a row in either category table.")

    def _populate_symbol_table(self) -> None:
        self.symbol_tree.delete(*self.symbol_tree.get_children())
        for row in self.current_symbol_rows:
            values = (
                row["symbol"],
                *(self.format_percent(row[name]) for name in self.symbol_intervals),
            )
            self.symbol_tree.insert("", tk.END, values=values)
        self.symbol_means_var.set(
            self.mean_summary(self.current_symbol_rows, self.symbol_intervals)
        )

    def sort_table(
        self,
        table_name: Literal[
            "snapshot_category", "intraday_category", "symbol"
        ],
        column: str,
    ) -> None:
        if table_name == "snapshot_category":
            rows = self.category_rows
            previous = self.category_sort
            tree = self.category_tree
            columns = ("category_label", *self.active_intervals)
        elif table_name == "intraday_category":
            rows = self.intraday_category_rows
            previous = self.intraday_category_sort
            tree = self.intraday_category_tree
            columns = ("category_label", INTRADAY_INTERVAL)
        else:
            rows = self.current_symbol_rows
            previous = self.symbol_sort
            tree = self.symbol_tree
            columns = ("symbol", *self.symbol_intervals)

        descending = not previous[1] if previous and previous[0] == column else False
        non_missing = [row for row in rows if row.get(column) is not None]
        missing = [row for row in rows if row.get(column) is None]
        if column in ("category_label", "symbol"):
            key = lambda row: str(row[column]).casefold()
        else:
            key = lambda row: float(row[column])
        non_missing.sort(key=key, reverse=descending)
        rows[:] = non_missing + missing

        if table_name == "snapshot_category":
            self.category_sort = (column, descending)
            self._populate_category_table()
        elif table_name == "intraday_category":
            self.intraday_category_sort = (column, descending)
            self._populate_intraday_category_table()
        else:
            self.symbol_sort = (column, descending)
            self._populate_symbol_table()

        self._reset_headings(tree, columns)
        heading = (
            "Theme / Sub-Theme"
            if column == "category_label"
            else "Symbol" if column == "symbol" else column
        )
        arrow = " ▼" if descending else " ▲"
        tree.heading(column, text=heading + arrow)

    @staticmethod
    def _reset_headings(
        tree: ttk.Treeview,
        columns: tuple[str, ...],
    ) -> None:
        for column in columns:
            heading = (
                "Theme / Sub-Theme"
                if column == "category_label"
                else "Symbol" if column == "symbol" else column
            )
            tree.heading(column, text=heading)

    @staticmethod
    def format_percent(value: Any) -> str:
        return "" if value is None else f"{float(value):.2f}%"

    @staticmethod
    def mean_summary(
        rows: list[dict[str, Any]],
        columns: tuple[str, ...],
    ) -> str:
        means: list[str] = []
        for column in columns:
            values: list[float] = []
            for row in rows:
                value = row.get(column)
                if value is None:
                    continue
                try:
                    numeric_value = float(value)
                except (TypeError, ValueError):
                    continue
                if math.isfinite(numeric_value):
                    values.append(numeric_value)
            if values:
                means.append(f"{column}: {sum(values) / len(values):.2f}%")
        return "Column means: " + (" | ".join(means) if means else "—")

    def close(self) -> None:
        self.closing = True
        self.intraday_running = False
        if self.intraday_after_id is not None:
            self.root.after_cancel(self.intraday_after_id)
            self.intraday_after_id = None
        self.executor.shutdown(wait=False, cancel_futures=True)
        self.repository.engine.dispose()
        if self.repository.stream_engine is not None:
            self.repository.stream_engine.dispose()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    repository = ThemeDataRepository(make_engine(), make_stream_engine())
    app = ThemePerformanceDashboard(root, repository)
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
