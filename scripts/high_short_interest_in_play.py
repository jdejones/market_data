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
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Iterable
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "gptdb"
STOCKS_DB = "stocks"
ELEVATED_RVOL_TABLE = "elevated_rvol"
DAILY_QUANT_RATING_TABLE = "daily_quant_rating"
OUTPUT_TABLE = "high_short_interest_in_play"
EASTERN = ZoneInfo("America/New_York")

DEFAULT_SHORT_INTEREST_FILE = Path(
    r"E:\Market Research\Dataset\Fundamental Data\historic_short_interest.txt"
)
DISPLAY_COLUMNS = ("symbol", "rvol", "quant_rating", "short_interest")
OUTPUT_COLUMNS = DISPLAY_COLUMNS
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
    short_interest_file: Path
    short_interest_threshold: float
    poll_interval: float
    once: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor high short-interest symbols that are also present in "
            "stocks.elevated_rvol, update stocks.high_short_interest_in_play, "
            "and display the table in a live GUI."
        )
    )
    parser.add_argument(
        "--short-interest-file",
        type=Path,
        default=DEFAULT_SHORT_INTEREST_FILE,
        help=f"Path to historic_short_interest.txt. Defaults to {DEFAULT_SHORT_INTEREST_FILE}",
    )
    parser.add_argument(
        "--short-interest-threshold",
        type=float,
        default=0.20,
        help="Minimum short interest as a decimal. Defaults to 0.20.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Seconds between refreshes. Defaults to 5.",
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


def make_engine(database: str) -> Engine:
    password = quote_plus(require_env("gptdb"))
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    )
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


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


def format_percent(value: Any, decimals: int = 1) -> str:
    number = numeric_or_none(value)
    if number is None:
        return ""
    return f"{number * 100:.{decimals}f}%"


def read_short_interest_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Short interest file not found: {path}")

    return pd.read_csv(path, sep=None, engine="python")


def parse_short_interest_values(series: pd.Series) -> pd.Series:
    text_values = series.astype(str).str.strip()
    has_percent_sign = text_values.str.endswith("%")
    numeric = pd.to_numeric(
        text_values.str.rstrip("%").str.replace(",", "", regex=False),
        errors="coerce",
    )
    numeric.loc[has_percent_sign] = numeric.loc[has_percent_sign] / 100

    non_null = numeric.dropna()
    if not non_null.empty and non_null.quantile(0.90) > 1:
        numeric = numeric / 100

    return numeric


def load_high_short_interest(
    path: Path,
    threshold: float,
) -> tuple[pd.DataFrame, str]:
    frame = read_short_interest_file(path)
    ticker_column = resolve_column(frame.columns, ("Ticker", "ticker", "Symbol", "symbol"))
    if ticker_column is None:
        raise ValueError(f"No Ticker column found in {path}")

    dated_columns: list[tuple[pd.Timestamp, str]] = []
    for column in frame.columns:
        if column == ticker_column:
            continue
        timestamp = pd.to_datetime(str(column), errors="coerce")
        if pd.notna(timestamp):
            dated_columns.append((pd.Timestamp(timestamp), column))
    if not dated_columns:
        raise ValueError(f"No date columns found in {path}")

    latest_release, latest_column = max(dated_columns, key=lambda item: item[0])
    short_interest = parse_short_interest_values(frame[latest_column])

    output = pd.DataFrame(
        {
            "symbol": frame[ticker_column].astype(str).str.strip().str.upper(),
            "short_interest": short_interest,
        }
    )
    output = output[(output["symbol"] != "") & (output["short_interest"] > threshold)]
    output = output.dropna(subset=["short_interest"]).drop_duplicates("symbol")
    output = output.sort_values(["short_interest", "symbol"], ascending=[False, True])
    return output.reset_index(drop=True), latest_release.strftime("%Y-%m-%d")


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

    output = pd.DataFrame()
    output["symbol"] = frame[symbol_col].astype(str).str.strip().str.upper()
    output["rvol"] = frame[rvol_col].map(numeric_or_none) if rvol_col else None
    output = output[output["symbol"] != ""]
    return output.drop_duplicates("symbol")


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


def build_rows(
    high_short_interest: pd.DataFrame,
    elevated_rvol: pd.DataFrame,
    quant_ratings: dict[str, float | None],
) -> pd.DataFrame:
    if high_short_interest.empty or elevated_rvol.empty:
        return pd.DataFrame(columns=DISPLAY_COLUMNS)

    rows = elevated_rvol.merge(high_short_interest, on="symbol", how="inner")
    if rows.empty:
        return pd.DataFrame(columns=DISPLAY_COLUMNS)

    rows["quant_rating"] = rows["symbol"].map(quant_ratings)
    rows = rows.loc[:, DISPLAY_COLUMNS]
    return rows.sort_values(
        ["short_interest", "rvol", "symbol"],
        ascending=[False, False, True],
        na_position="last",
    ).reset_index(drop=True)


def reset_output_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {mysql_identifier(OUTPUT_TABLE)}"))


def write_output_rows(engine: Engine, rows: pd.DataFrame) -> None:
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


def refresh_high_short_interest_in_play(
    stocks_engine: Engine,
    config: MonitorConfig,
) -> tuple[pd.DataFrame, str, int]:
    high_short_interest, release_date = load_high_short_interest(
        config.short_interest_file,
        config.short_interest_threshold,
    )
    elevated = fetch_elevated_rvol(stocks_engine)
    quant_ratings = load_latest_quant_ratings(stocks_engine)
    rows = build_rows(high_short_interest, elevated, quant_ratings)
    write_output_rows(stocks_engine, rows)
    return rows, release_date, len(high_short_interest)


class HighShortInterestInPlayGUI:
    def __init__(self, root: tk.Tk, config: MonitorConfig) -> None:
        self.root = root
        self.config = config
        self.output_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.refresh_event = threading.Event()
        self.last_rows: list[dict[str, Any]] = []
        self.sort_column = "short_interest"
        self.sort_descending = True

        self.root.title("High Short Interest In Play")
        self.root.geometry("720x500")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.status_var = tk.StringVar(value="Starting...")
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
            "short_interest": 140,
        }
        anchors = {
            "symbol": tk.W,
            "rvol": tk.E,
            "quant_rating": tk.E,
            "short_interest": tk.E,
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

        ttk.Button(container, text="Refresh Now", command=self.refresh_now).grid(
            row=1,
            column=0,
            sticky=tk.W,
            pady=(10, 0),
        )
        ttk.Button(
            container,
            textvariable=self.pause_button_text,
            command=self.toggle_pause,
        ).grid(
            row=1,
            column=0,
            sticky=tk.W,
            padx=(100, 0),
            pady=(10, 0),
        )
        ttk.Label(container, textvariable=self.status_var).grid(
            row=1,
            column=0,
            sticky=tk.E,
            pady=(10, 0),
        )
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

    def start_worker(self) -> None:
        worker = threading.Thread(
            target=self.worker_loop,
            name="high-short-interest-in-play-worker",
            daemon=True,
        )
        worker.start()

    def sort_by_column(self, column: str) -> None:
        if column not in SORTABLE_COLUMNS:
            return
        if self.sort_column == column:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_column = column
            self.sort_descending = column != "symbol"
        self.render_rows(self.last_rows)

    def refresh_now(self) -> None:
        self.refresh_event.set()
        self.render_rows(self.last_rows)

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

    def render_rows(self, rows: list[dict[str, Any]]) -> None:
        sorted_rows = list(rows)
        if self.sort_column == "symbol":
            sorted_rows.sort(
                key=lambda row: str(row.get("symbol", "")),
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
                    format_percent(row.get("short_interest")),
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
                    messagebox.showerror("High Short Interest Failed", str(payload))
        except queue.Empty:
            pass

        if not self.stop_event.is_set():
            self.root.after(250, self.process_queue)

    def worker_loop(self) -> None:
        try:
            stocks_engine = make_engine(STOCKS_DB)
            reset_output_table(stocks_engine)
            self.output_queue.put(("status", "Monitoring high short interest in play..."))

            while not self.stop_event.is_set():
                if self.pause_event.is_set():
                    self.stop_event.wait(0.25)
                    continue

                rows, release_date, high_short_count = refresh_high_short_interest_in_play(
                    stocks_engine,
                    self.config,
                )
                records = rows.to_dict("records")
                self.output_queue.put(("rows", records))
                self.output_queue.put(
                    (
                        "status",
                        f"{dt.datetime.now(EASTERN):%H:%M:%S}: "
                        f"in_play={len(records)} high_short={high_short_count} "
                        f"release={release_date}",
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
    if args.short_interest_threshold < 0:
        raise ValueError("--short-interest-threshold must be greater than or equal to 0")
    if args.poll_interval <= 0:
        raise ValueError("--poll-interval must be greater than 0")

    config = MonitorConfig(
        short_interest_file=args.short_interest_file,
        short_interest_threshold=args.short_interest_threshold,
        poll_interval=args.poll_interval,
        once=args.once,
    )
    root = tk.Tk()
    app = HighShortInterestInPlayGUI(root, config)
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
