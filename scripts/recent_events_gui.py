from __future__ import annotations

import datetime as dt
import sys
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from recent_events import (  # type: ignore[import-not-found]
    DATE_COLUMN,
    RECENT_EVENTS_TABLE,
    SYMBOL_COLUMN,
    make_engine,
    mysql_identifier,
)


CONCURRENT_EVENT_COLUMN = "concurrent_event"
PRIOR_EVENT_COLUMN = "prior_event"
COLUMNS = (SYMBOL_COLUMN, DATE_COLUMN, CONCURRENT_EVENT_COLUMN, PRIOR_EVENT_COLUMN)


class RecentEventsGUI:
    def __init__(self, root: tk.Tk, engine: Engine) -> None:
        self.root = root
        self.engine = engine
        self.sort_state: dict[str, bool] = {column: False for column in COLUMNS}

        self.root.title("Recent Events")
        self.root.geometry("900x480")

        self.status_var = tk.StringVar(value="Ready")
        self.symbol_var = tk.StringVar()
        self.date_var = tk.StringVar(value=dt.date.today().isoformat())
        self.concurrent_event_var = tk.StringVar()
        self.prior_event_var = tk.StringVar()

        self._build_widgets()
        self.refresh_rows()

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        self.tree = ttk.Treeview(
            container,
            columns=COLUMNS,
            show="headings",
            selectmode="extended",
        )
        for column in COLUMNS:
            self.tree.heading(
                column,
                text=column,
                command=lambda col=column: self.sort_by_column(col),
            )
            width = 130 if column in {SYMBOL_COLUMN, DATE_COLUMN} else 260
            self.tree.column(column, width=width, anchor=tk.W)

        y_scroll = ttk.Scrollbar(container, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=y_scroll.set)
        self.tree.grid(row=0, column=0, columnspan=6, sticky="nsew")
        y_scroll.grid(row=0, column=6, sticky="ns")

        ttk.Label(container, text="Symbol").grid(row=1, column=0, sticky=tk.W, pady=(10, 0))
        ttk.Entry(container, textvariable=self.symbol_var, width=16).grid(
            row=2, column=0, sticky="ew", padx=(0, 8)
        )

        ttk.Label(container, text="Date (YYYY-MM-DD)").grid(
            row=1, column=1, sticky=tk.W, pady=(10, 0)
        )
        ttk.Entry(container, textvariable=self.date_var, width=16).grid(
            row=2, column=1, sticky="ew", padx=(0, 8)
        )

        ttk.Label(container, text="Concurrent Event").grid(
            row=1, column=2, sticky=tk.W, pady=(10, 0)
        )
        ttk.Entry(container, textvariable=self.concurrent_event_var, width=28).grid(
            row=2, column=2, sticky="ew", padx=(0, 8)
        )

        ttk.Label(container, text="Prior Event").grid(
            row=1, column=3, sticky=tk.W, pady=(10, 0)
        )
        ttk.Entry(container, textvariable=self.prior_event_var, width=28).grid(
            row=2, column=3, sticky="ew", padx=(0, 8)
        )

        ttk.Button(container, text="Add Row", command=self.add_row).grid(
            row=2, column=4, sticky="ew", padx=(0, 8)
        )
        ttk.Button(container, text="Remove Selected", command=self.remove_selected).grid(
            row=2, column=5, sticky="ew"
        )
        ttk.Button(container, text="Refresh", command=self.refresh_rows).grid(
            row=3, column=0, sticky="ew", pady=(10, 0)
        )

        ttk.Label(container, textvariable=self.status_var).grid(
            row=3, column=1, columnspan=5, sticky=tk.W, pady=(10, 0)
        )

        for column_index in range(6):
            container.columnconfigure(column_index, weight=1)
        container.rowconfigure(0, weight=1)

    def fetch_rows(self) -> list[dict[str, Any]]:
        query = text(
            f"""
            SELECT
                {mysql_identifier(SYMBOL_COLUMN)},
                {mysql_identifier(DATE_COLUMN)},
                {mysql_identifier(CONCURRENT_EVENT_COLUMN)},
                {mysql_identifier(PRIOR_EVENT_COLUMN)}
            FROM {mysql_identifier(RECENT_EVENTS_TABLE)}
            ORDER BY {mysql_identifier(DATE_COLUMN)} DESC, {mysql_identifier(SYMBOL_COLUMN)}
            """
        )
        with self.engine.connect() as conn:
            return [dict(row._mapping) for row in conn.execute(query)]

    def refresh_rows(self) -> None:
        try:
            rows = self.fetch_rows()
        except Exception as exc:
            messagebox.showerror("Refresh Failed", str(exc))
            self.status_var.set("Refresh failed")
            return

        self.tree.delete(*self.tree.get_children())
        for row in rows:
            values = tuple(self.format_value(row[column]) for column in COLUMNS)
            self.tree.insert("", tk.END, values=values)

        self.status_var.set(f"Loaded {len(rows)} rows")

    def sort_by_column(self, column: str) -> None:
        descending = not self.sort_state[column]
        self.sort_state[column] = descending

        rows = [
            (self.tree.set(item_id, column), item_id)
            for item_id in self.tree.get_children("")
        ]
        rows.sort(key=lambda row: self.sort_key(row[0]), reverse=descending)

        for position, (_, item_id) in enumerate(rows):
            self.tree.move(item_id, "", position)

    def add_row(self) -> None:
        symbol = self.symbol_var.get().strip().upper()
        date_text = self.date_var.get().strip()
        concurrent_event = self.blank_to_none(self.concurrent_event_var.get())
        prior_event = self.blank_to_none(self.prior_event_var.get())

        if not symbol:
            messagebox.showwarning("Missing Symbol", "Enter a symbol before adding a row.")
            return

        try:
            event_date = dt.date.fromisoformat(date_text)
        except ValueError:
            messagebox.showwarning("Invalid Date", "Enter the date as YYYY-MM-DD.")
            return

        query = text(
            f"""
            INSERT INTO {mysql_identifier(RECENT_EVENTS_TABLE)}
                (
                    {mysql_identifier(SYMBOL_COLUMN)},
                    {mysql_identifier(DATE_COLUMN)},
                    {mysql_identifier(CONCURRENT_EVENT_COLUMN)},
                    {mysql_identifier(PRIOR_EVENT_COLUMN)}
                )
            SELECT :symbol, :date, :concurrent_event, :prior_event
            WHERE NOT EXISTS (
                SELECT 1
                FROM {mysql_identifier(RECENT_EVENTS_TABLE)}
                WHERE {mysql_identifier(SYMBOL_COLUMN)} = :symbol
                  AND {mysql_identifier(DATE_COLUMN)} = :date
            )
            """
        )

        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    query,
                    {
                        SYMBOL_COLUMN: symbol,
                        DATE_COLUMN: event_date,
                        CONCURRENT_EVENT_COLUMN: concurrent_event,
                        PRIOR_EVENT_COLUMN: prior_event,
                    },
                )
        except Exception as exc:
            messagebox.showerror("Add Failed", str(exc))
            self.status_var.set("Add failed")
            return

        self.symbol_var.set("")
        self.concurrent_event_var.set("")
        self.prior_event_var.set("")
        self.refresh_rows()
        self.status_var.set(f"Inserted {result.rowcount or 0} row")

    def remove_selected(self) -> None:
        item_ids = self.tree.selection()
        if not item_ids:
            messagebox.showwarning("No Selection", "Select one or more rows to remove.")
            return

        if not messagebox.askyesno(
            "Confirm Remove",
            f"Remove {len(item_ids)} selected row(s)?",
        ):
            return

        records = []
        for item_id in item_ids:
            values = self.tree.item(item_id, "values")
            records.append(
                {
                    SYMBOL_COLUMN: values[0],
                    DATE_COLUMN: dt.date.fromisoformat(values[1]),
                }
            )

        query = text(
            f"""
            DELETE FROM {mysql_identifier(RECENT_EVENTS_TABLE)}
            WHERE {mysql_identifier(SYMBOL_COLUMN)} = :symbol
              AND {mysql_identifier(DATE_COLUMN)} = :date
            """
        )

        try:
            with self.engine.begin() as conn:
                result = conn.execute(query, records)
        except Exception as exc:
            messagebox.showerror("Remove Failed", str(exc))
            self.status_var.set("Remove failed")
            return

        self.refresh_rows()
        self.status_var.set(f"Removed {result.rowcount or 0} row(s)")

    @staticmethod
    def format_value(value: Any) -> str:
        if isinstance(value, (dt.date, dt.datetime)):
            return value.isoformat()[:10]
        return "" if value is None else str(value)

    @staticmethod
    def blank_to_none(value: str) -> str | None:
        value = value.strip()
        return value if value else None

    @staticmethod
    def sort_key(value: str) -> tuple[int, Any]:
        try:
            return (0, dt.date.fromisoformat(value))
        except ValueError:
            return (1, value.upper())


def main() -> None:
    root = tk.Tk()
    app = RecentEventsGUI(root, make_engine())
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
