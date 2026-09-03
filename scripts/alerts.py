from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import queue
import re
import sys
import threading
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Protocol
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection, Engine


PACKAGE_PARENT = Path(__file__).resolve().parents[2]
if str(PACKAGE_PARENT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_PARENT))

from market_data.api_keys import database_password, gptdb  # type: ignore[import-not-found]


MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
ALERTS_MYSQL_USER = "root"
STOCKS_MYSQL_USER = "gptdb"
ALERTS_DB = "alerts"
STOCKS_DB = "stocks"
DEFINITION_TABLE = "definition"
PASSED_TABLE = "passed"
ELEVATED_RVOL_TABLE = "elevated_rvol"
EASTERN = ZoneInfo("US/Eastern")
PASSED_RETENTION = dt.timedelta(days=30)
DEFAULT_POLL_INTERVAL = 5.0
SCHEMA_VERSION = 1


def make_engine(database: str, user: str, password_value: str) -> Engine:
    password = quote_plus(password_value)
    url = (
        f"mysql+pymysql://{user}:{password}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{database}"
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )


def eastern_now() -> dt.datetime:
    return dt.datetime.now(EASTERN)


def database_datetime(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is not None:
        value = value.astimezone(EASTERN).replace(tzinfo=None)
    return value


def parse_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("Alert data must be a JSON object")


def json_text(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def normalize_symbols(value: str) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in re.split(r"[\s,;]+", value):
        symbol = raw_symbol.strip().upper()
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    return symbols


def cooldown_seconds(mode: str, amount: str = "", unit: str = "Hours") -> float:
    fixed = {
        "Immediate": 0.0,
        "Day": 24.0 * 60.0 * 60.0,
        "Week": 7.0 * 24.0 * 60.0 * 60.0,
    }
    if mode in fixed:
        return fixed[mode]
    if mode != "Custom":
        raise ValueError(f"Unsupported cooldown option: {mode}")
    try:
        numeric_amount = float(amount)
    except ValueError as exc:
        raise ValueError("Custom cooldown must be a number.") from exc
    if numeric_amount <= 0:
        raise ValueError("Custom cooldown must be greater than zero.")
    multipliers = {
        "Minutes": 60.0,
        "Hours": 60.0 * 60.0,
        "Days": 24.0 * 60.0 * 60.0,
    }
    if unit not in multipliers:
        raise ValueError(f"Unsupported cooldown unit: {unit}")
    return numeric_amount * multipliers[unit]


def expiration_from_choice(
    choice: str,
    custom_date: str,
    created_at: dt.datetime,
) -> dt.datetime | None:
    fixed = {
        "Day": dt.timedelta(days=1),
        "Week": dt.timedelta(days=7),
        "Month": dt.timedelta(days=30),
        "Year": dt.timedelta(days=365),
    }
    if choice in fixed:
        return created_at + fixed[choice]
    if choice == "No expiration":
        return None
    if choice != "Custom date":
        raise ValueError(f"Unsupported expiration option: {choice}")
    try:
        date_value = dt.date.fromisoformat(custom_date.strip())
    except ValueError as exc:
        raise ValueError("Custom expiration must use YYYY-MM-DD.") from exc
    expiration = dt.datetime.combine(
        date_value,
        dt.time(23, 59, 59, 999999),
        tzinfo=EASTERN,
    )
    if expiration <= created_at:
        raise ValueError("Custom expiration must be in the future.")
    return expiration


def parse_iso_datetime(value: Any) -> dt.datetime | None:
    if not value:
        return None
    parsed = dt.datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=EASTERN)
    return parsed.astimezone(EASTERN)


def format_datetime(value: Any) -> str:
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
    if isinstance(value, dt.date):
        return value.isoformat()
    return "" if value is None else str(value)


@dataclass(frozen=True)
class AlertDefinition:
    name: str
    created_at: dt.datetime
    data: dict[str, Any]


@dataclass(frozen=True)
class PassedAlert:
    name: str
    passed_at: dt.datetime
    data: dict[str, Any]


@dataclass
class EvaluationResult:
    passed: list[PassedAlert]
    updated_data: dict[str, Any] | None
    delete_definition: bool = False


class AlertSpecificEditor(Protocol):
    def values(self) -> dict[str, Any]:
        ...


class AlertType(Protocol):
    type_id: str
    display_name: str

    def build_editor(self, parent: ttk.Frame) -> AlertSpecificEditor:
        ...

    def build_definition_data(
        self,
        specific_values: dict[str, Any],
        *,
        comment: str,
        repeat: bool,
        cooldown_mode: str,
        cooldown_value_seconds: float,
        expiration: dt.datetime | None,
    ) -> dict[str, Any]:
        ...

    def symbols(self, definition: AlertDefinition) -> set[str]:
        ...

    def evaluate(
        self,
        definition: AlertDefinition,
        observations: dict[str, float],
        now: dt.datetime,
    ) -> EvaluationResult:
        ...


class RelativeVolumeEditor:
    def __init__(self, parent: ttk.Frame) -> None:
        self.symbols_var = tk.StringVar()
        self.threshold_var = tk.StringVar(value="2.0")

        ttk.Label(parent, text="Symbols (comma, space, or line separated)").grid(
            row=0, column=0, sticky="w"
        )
        self.symbols_text = tk.Text(parent, width=48, height=5, wrap=tk.WORD)
        self.symbols_text.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(2, 8))
        ttk.Label(parent, text="RVol threshold").grid(row=2, column=0, sticky="w")
        ttk.Entry(parent, textvariable=self.threshold_var, width=16).grid(
            row=3, column=0, sticky="w"
        )
        parent.columnconfigure(1, weight=1)

    def values(self) -> dict[str, Any]:
        symbols = normalize_symbols(self.symbols_text.get("1.0", tk.END))
        if not symbols:
            raise ValueError("Enter at least one symbol.")
        try:
            threshold = float(self.threshold_var.get().strip())
        except ValueError as exc:
            raise ValueError("RVol threshold must be a number.") from exc
        if threshold <= 0:
            raise ValueError("RVol threshold must be greater than zero.")
        return {"symbols": symbols, "threshold": threshold}


class RelativeVolumeAlertType:
    type_id = "relative_volume"
    display_name = "Relative Volume"

    def build_editor(self, parent: ttk.Frame) -> AlertSpecificEditor:
        return RelativeVolumeEditor(parent)

    def build_definition_data(
        self,
        specific_values: dict[str, Any],
        *,
        comment: str,
        repeat: bool,
        cooldown_mode: str,
        cooldown_value_seconds: float,
        expiration: dt.datetime | None,
    ) -> dict[str, Any]:
        symbols = [str(symbol) for symbol in specific_values["symbols"]]
        return {
            "schema_version": SCHEMA_VERSION,
            "alert_type": self.type_id,
            "comment": comment,
            "config": {
                "symbols": symbols,
                "threshold": float(specific_values["threshold"]),
                "repeat": repeat,
                "cooldown": {
                    "mode": cooldown_mode,
                    "seconds": cooldown_value_seconds,
                },
                "expiration": expiration.isoformat() if expiration else None,
            },
            "state": {
                "symbols": {
                    symbol: {"next_eligible_at": None}
                    for symbol in symbols
                }
            },
        }

    def symbols(self, definition: AlertDefinition) -> set[str]:
        state = definition.data.get("state", {}).get("symbols", {})
        return {str(symbol).upper() for symbol in state}

    def evaluate(
        self,
        definition: AlertDefinition,
        observations: dict[str, float],
        now: dt.datetime,
    ) -> EvaluationResult:
        data = copy.deepcopy(definition.data)
        config = data.get("config", {})
        state = data.setdefault("state", {}).setdefault("symbols", {})
        expiration = parse_iso_datetime(config.get("expiration"))
        if expiration is not None and now >= expiration:
            return EvaluationResult([], None, delete_definition=True)

        threshold = float(config["threshold"])
        repeat = bool(config.get("repeat", False))
        cooldown = float(config.get("cooldown", {}).get("seconds", 0.0))
        passed: list[PassedAlert] = []
        changed = False

        for symbol in list(state):
            observed = observations.get(symbol.upper())
            if observed is None or observed < threshold:
                continue
            next_eligible = parse_iso_datetime(state[symbol].get("next_eligible_at"))
            if next_eligible is not None and now < next_eligible:
                continue

            # Use the scan time plus a deterministic microsecond offset so
            # simultaneous symbol passes remain unique under (name, date).
            pass_time = now + dt.timedelta(microseconds=len(passed))
            passed.append(
                PassedAlert(
                    name=definition.name,
                    passed_at=pass_time,
                    data={
                        "schema_version": SCHEMA_VERSION,
                        "alert_type": self.type_id,
                        "alert_name": definition.name,
                        "symbol": symbol,
                        "threshold": threshold,
                        "observed_rvol": observed,
                        "comment": data.get("comment", ""),
                        "definition_created_at": definition.created_at.isoformat(),
                    },
                )
            )
            changed = True
            if repeat:
                state[symbol]["next_eligible_at"] = (
                    pass_time + dt.timedelta(seconds=cooldown)
                ).isoformat()
            else:
                del state[symbol]
                configured_symbols = config.get("symbols", [])
                config["symbols"] = [
                    item for item in configured_symbols if str(item) != symbol
                ]

        if not state:
            return EvaluationResult(passed, None, delete_definition=True)
        return EvaluationResult(passed, data if changed else None)


class AlertRegistry:
    def __init__(self, alert_types: list[AlertType]) -> None:
        self._by_id = {alert_type.type_id: alert_type for alert_type in alert_types}
        self._by_name = {
            alert_type.display_name: alert_type for alert_type in alert_types
        }

    @property
    def display_names(self) -> list[str]:
        return sorted(self._by_name)

    def by_id(self, type_id: str) -> AlertType | None:
        return self._by_id.get(type_id)

    def by_display_name(self, display_name: str) -> AlertType:
        return self._by_name[display_name]


class AlertRepository:
    def __init__(self, alerts_engine: Engine, stocks_engine: Engine) -> None:
        self.alerts_engine = alerts_engine
        self.stocks_engine = stocks_engine

    def load_definitions(self) -> list[AlertDefinition]:
        query = text(
            f"SELECT name, date, data FROM `{DEFINITION_TABLE}` "
            "ORDER BY date DESC, name"
        )
        with self.alerts_engine.connect() as connection:
            return [
                AlertDefinition(
                    name=str(row.name),
                    created_at=row.date,
                    data=parse_json(row.data),
                )
                for row in connection.execute(query)
            ]

    def load_passed(self, since: dt.datetime) -> list[PassedAlert]:
        query = text(
            f"SELECT name, date, data FROM `{PASSED_TABLE}` "
            "WHERE date >= :since ORDER BY date DESC, name"
        )
        with self.alerts_engine.connect() as connection:
            return [
                PassedAlert(
                    name=str(row.name),
                    passed_at=row.date,
                    data=parse_json(row.data),
                )
                for row in connection.execute(
                    query,
                    {"since": database_datetime(since)},
                )
            ]

    def insert_definition(self, definition: AlertDefinition) -> None:
        query = text(
            f"INSERT INTO `{DEFINITION_TABLE}` (name, date, data) "
            "VALUES (:name, :date, :data)"
        )
        with self.alerts_engine.begin() as connection:
            connection.execute(
                query,
                {
                    "name": definition.name,
                    "date": database_datetime(definition.created_at),
                    "data": json_text(definition.data),
                },
            )

    def delete_definition(self, name: str, created_at: dt.datetime) -> int:
        query = text(
            f"DELETE FROM `{DEFINITION_TABLE}` "
            "WHERE name = :name AND date = :date"
        )
        with self.alerts_engine.begin() as connection:
            result = connection.execute(
                query,
                {"name": name, "date": database_datetime(created_at)},
            )
            return result.rowcount or 0

    def purge_old_passed(self, before: dt.datetime) -> int:
        query = text(f"DELETE FROM `{PASSED_TABLE}` WHERE date < :before")
        with self.alerts_engine.begin() as connection:
            result = connection.execute(
                query,
                {"before": database_datetime(before)},
            )
            return result.rowcount or 0

    def load_rvol(self, symbols: set[str]) -> dict[str, float]:
        if not symbols:
            return {}
        query = text(
            f"SELECT symbol, rvol FROM `{ELEVATED_RVOL_TABLE}` "
            "WHERE symbol IN :symbols"
        ).bindparams(bindparam("symbols", expanding=True))
        result: dict[str, float] = {}
        ordered = sorted(symbols)
        with self.stocks_engine.connect() as connection:
            for start in range(0, len(ordered), 500):
                rows = connection.execute(
                    query,
                    {"symbols": ordered[start:start + 500]},
                )
                for row in rows:
                    if row.rvol is not None:
                        result[str(row.symbol).upper()] = float(row.rvol)
        return result

    def apply_evaluation(
        self,
        definition: AlertDefinition,
        result: EvaluationResult,
    ) -> bool:
        exists_query = text(
            f"SELECT 1 FROM `{DEFINITION_TABLE}` "
            "WHERE name = :name AND date = :date"
        )
        params = {
            "name": definition.name,
            "date": database_datetime(definition.created_at),
        }
        with self.alerts_engine.begin() as connection:
            if connection.execute(exists_query, params).first() is None:
                return False
            self._insert_passed(connection, result.passed)
            if result.delete_definition:
                connection.execute(
                    text(
                        f"DELETE FROM `{DEFINITION_TABLE}` "
                        "WHERE name = :name AND date = :date"
                    ),
                    params,
                )
            elif result.updated_data is not None:
                connection.execute(
                    text(
                        f"UPDATE `{DEFINITION_TABLE}` "
                        "SET data = :data "
                        "WHERE name = :name AND date = :date"
                    ),
                    {**params, "data": json_text(result.updated_data)},
                )
        return True

    @staticmethod
    def _insert_passed(
        connection: Connection,
        passed: list[PassedAlert],
    ) -> None:
        if not passed:
            return
        query = text(
            f"INSERT INTO `{PASSED_TABLE}` (name, date, data) "
            "VALUES (:name, :date, :data)"
        )
        connection.execute(
            query,
            [
                {
                    "name": item.name,
                    "date": database_datetime(item.passed_at),
                    "data": json_text(item.data),
                }
                for item in passed
            ],
        )

    def dispose(self) -> None:
        self.alerts_engine.dispose()
        self.stocks_engine.dispose()


class AlertScanner:
    def __init__(
        self,
        repository: AlertRepository,
        registry: AlertRegistry,
        messages: queue.Queue[tuple[str, Any]],
        stop_event: threading.Event,
        poll_interval: float,
    ) -> None:
        self.repository = repository
        self.registry = registry
        self.messages = messages
        self.stop_event = stop_event
        self.poll_interval = poll_interval

    def run(self) -> None:
        next_cleanup = eastern_now()
        while not self.stop_event.is_set():
            try:
                now = eastern_now()
                if now >= next_cleanup:
                    removed = self.repository.purge_old_passed(
                        now - PASSED_RETENTION
                    )
                    if removed:
                        self.messages.put(("changed", None))
                    next_cleanup = now + dt.timedelta(hours=1)
                pass_count = self.scan_once(now)
                self.messages.put(
                    (
                        "status",
                        f"Scanning every {self.poll_interval:g}s; "
                        f"last scan {now:%H:%M:%S}; {pass_count} new pass(es)",
                    )
                )
            except Exception as exc:
                self.messages.put(("error", f"Scanner error: {exc}"))
            self.stop_event.wait(self.poll_interval)

    def scan_once(self, now: dt.datetime | None = None) -> int:
        now = now or eastern_now()
        definitions = self.repository.load_definitions()
        targets: set[str] = set()
        for definition in definitions:
            handler = self.registry.by_id(str(definition.data.get("alert_type", "")))
            if handler is not None:
                targets.update(handler.symbols(definition))
        observations = self.repository.load_rvol(targets)

        total_passes = 0
        changed = False
        for definition in definitions:
            handler = self.registry.by_id(str(definition.data.get("alert_type", "")))
            if handler is None:
                continue
            evaluation_time = now + dt.timedelta(microseconds=total_passes)
            result = handler.evaluate(definition, observations, evaluation_time)
            if (
                result.passed
                or result.updated_data is not None
                or result.delete_definition
            ):
                applied = self.repository.apply_evaluation(definition, result)
                if applied:
                    total_passes += len(result.passed)
                    changed = True
                    for passed in result.passed:
                        self.messages.put(("passed", passed))
        if changed:
            self.messages.put(("changed", None))
        return total_passes


class CreateAlertDialog:
    def __init__(
        self,
        parent: tk.Tk,
        registry: AlertRegistry,
        on_create: Any,
    ) -> None:
        self.registry = registry
        self.on_create = on_create
        self.editor: AlertSpecificEditor | None = None

        self.window = tk.Toplevel(parent)
        self.window.title("Create Alert")
        self.window.geometry("620x720")
        self.window.minsize(560, 620)
        self.window.transient(parent)
        self.window.grab_set()

        self.name_var = tk.StringVar()
        self.type_var = tk.StringVar(value=registry.display_names[0])
        self.repeat_var = tk.BooleanVar(value=False)
        self.cooldown_var = tk.StringVar(value="Day")
        self.cooldown_amount_var = tk.StringVar(value="1")
        self.cooldown_unit_var = tk.StringVar(value="Hours")
        self.expiration_var = tk.StringVar(value="Week")
        self.custom_expiration_var = tk.StringVar(
            value=(dt.date.today() + dt.timedelta(days=7)).isoformat()
        )

        self._build()
        self._show_type_editor()
        self._sync_repeat_controls()
        self._sync_expiration_controls()

    def _build(self) -> None:
        outer = ttk.Frame(self.window, padding=12)
        outer.pack(fill=tk.BOTH, expand=True)
        outer.columnconfigure(0, weight=1)

        ttk.Label(outer, text="Name").grid(row=0, column=0, sticky="w")
        ttk.Entry(outer, textvariable=self.name_var).grid(
            row=1, column=0, sticky="ew", pady=(2, 8)
        )
        ttk.Label(outer, text="Alert type").grid(row=2, column=0, sticky="w")
        type_box = ttk.Combobox(
            outer,
            textvariable=self.type_var,
            values=self.registry.display_names,
            state="readonly",
        )
        type_box.grid(row=3, column=0, sticky="ew", pady=(2, 8))
        type_box.bind("<<ComboboxSelected>>", lambda _event: self._show_type_editor())

        self.type_frame = ttk.LabelFrame(outer, text="Alert settings", padding=10)
        self.type_frame.grid(row=4, column=0, sticky="nsew", pady=(0, 8))
        outer.rowconfigure(4, weight=1)

        lifecycle = ttk.LabelFrame(outer, text="Lifecycle", padding=10)
        lifecycle.grid(row=5, column=0, sticky="ew", pady=(0, 8))
        lifecycle.columnconfigure(1, weight=1)
        ttk.Checkbutton(
            lifecycle,
            text="Repeat",
            variable=self.repeat_var,
            command=self._sync_repeat_controls,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(lifecycle, text="Cooldown").grid(row=1, column=0, sticky="w")
        self.cooldown_box = ttk.Combobox(
            lifecycle,
            textvariable=self.cooldown_var,
            values=("Immediate", "Day", "Week", "Custom"),
            state="readonly",
            width=16,
        )
        self.cooldown_box.grid(row=1, column=1, sticky="w", padx=(8, 0))
        self.cooldown_box.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._sync_repeat_controls(),
        )
        self.cooldown_amount_entry = ttk.Entry(
            lifecycle,
            textvariable=self.cooldown_amount_var,
            width=10,
        )
        self.cooldown_amount_entry.grid(row=2, column=1, sticky="w", padx=(8, 0))
        self.cooldown_unit_box = ttk.Combobox(
            lifecycle,
            textvariable=self.cooldown_unit_var,
            values=("Minutes", "Hours", "Days"),
            state="readonly",
            width=12,
        )
        self.cooldown_unit_box.grid(row=2, column=1, sticky="w", padx=(95, 0))

        ttk.Label(lifecycle, text="Expiration").grid(
            row=3, column=0, sticky="w", pady=(8, 0)
        )
        expiration_box = ttk.Combobox(
            lifecycle,
            textvariable=self.expiration_var,
            values=("Day", "Week", "Month", "Year", "Custom date", "No expiration"),
            state="readonly",
            width=16,
        )
        expiration_box.grid(row=3, column=1, sticky="w", padx=(8, 0), pady=(8, 0))
        expiration_box.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._sync_expiration_controls(),
        )
        self.custom_expiration_entry = ttk.Entry(
            lifecycle,
            textvariable=self.custom_expiration_var,
            width=16,
        )
        self.custom_expiration_entry.grid(
            row=4, column=1, sticky="w", padx=(8, 0), pady=(4, 0)
        )

        ttk.Label(outer, text="Comment").grid(row=6, column=0, sticky="w")
        self.comment_text = tk.Text(outer, height=5, wrap=tk.WORD)
        self.comment_text.grid(row=7, column=0, sticky="ew", pady=(2, 8))

        buttons = ttk.Frame(outer)
        buttons.grid(row=8, column=0, sticky="e")
        ttk.Button(buttons, text="Cancel", command=self.window.destroy).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        ttk.Button(buttons, text="Create", command=self._create).pack(side=tk.LEFT)

    def _show_type_editor(self) -> None:
        for child in self.type_frame.winfo_children():
            child.destroy()
        handler = self.registry.by_display_name(self.type_var.get())
        self.editor = handler.build_editor(self.type_frame)

    def _sync_repeat_controls(self) -> None:
        repeat_state = "readonly" if self.repeat_var.get() else "disabled"
        self.cooldown_box.configure(state=repeat_state)
        custom_enabled = self.repeat_var.get() and self.cooldown_var.get() == "Custom"
        self.cooldown_amount_entry.configure(
            state="normal" if custom_enabled else "disabled"
        )
        self.cooldown_unit_box.configure(
            state="readonly" if custom_enabled else "disabled"
        )

    def _sync_expiration_controls(self) -> None:
        self.custom_expiration_entry.configure(
            state="normal"
            if self.expiration_var.get() == "Custom date"
            else "disabled"
        )

    def _create(self) -> None:
        name = self.name_var.get().strip()
        if not name:
            messagebox.showwarning("Missing Name", "Enter an alert name.", parent=self.window)
            return
        if len(name) > 255:
            messagebox.showwarning(
                "Name Too Long",
                "Alert names cannot exceed 255 characters.",
                parent=self.window,
            )
            return
        if self.editor is None:
            return
        try:
            specific_values = self.editor.values()
            repeat = self.repeat_var.get()
            cooldown_value = (
                cooldown_seconds(
                    self.cooldown_var.get(),
                    self.cooldown_amount_var.get(),
                    self.cooldown_unit_var.get(),
                )
                if repeat
                else 0.0
            )
            created_at = eastern_now()
            expiration = expiration_from_choice(
                self.expiration_var.get(),
                self.custom_expiration_var.get(),
                created_at,
            )
            handler = self.registry.by_display_name(self.type_var.get())
            data = handler.build_definition_data(
                specific_values,
                comment=self.comment_text.get("1.0", tk.END).strip(),
                repeat=repeat,
                cooldown_mode=self.cooldown_var.get() if repeat else "None",
                cooldown_value_seconds=cooldown_value,
                expiration=expiration,
            )
            self.on_create(AlertDefinition(name, created_at, data))
        except Exception as exc:
            messagebox.showerror("Cannot Create Alert", str(exc), parent=self.window)
            return
        self.window.destroy()


class AlertsApp:
    PASSED_PERIODS = {
        "Day": dt.timedelta(days=1),
        "Week": dt.timedelta(days=7),
        "Month": PASSED_RETENTION,
    }

    def __init__(
        self,
        root: tk.Tk,
        repository: AlertRepository,
        registry: AlertRegistry,
        poll_interval: float,
    ) -> None:
        self.root = root
        self.repository = repository
        self.registry = registry
        self.poll_interval = poll_interval
        self.messages: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.closing = False
        self.after_id: str | None = None
        self.definition_rows: dict[str, AlertDefinition] = {}
        self.passed_rows: dict[str, PassedAlert] = {}
        self.sort_descending: dict[tuple[str, str], bool] = {}

        self.status_var = tk.StringVar(value="Starting scanner...")
        self.period_var = tk.StringVar(value="Day")
        self.root.title("Alerts")
        self.root.geometry("1280x760")
        self.root.minsize(950, 620)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        self._build_widgets()
        self.refresh_all()
        self.scanner = AlertScanner(
            repository,
            registry,
            self.messages,
            self.stop_event,
            poll_interval,
        )
        self.worker = threading.Thread(
            target=self.scanner.run,
            name="alert-scanner",
            daemon=True,
        )
        self.worker.start()
        self._process_messages()

    def _build_widgets(self) -> None:
        outer = ttk.Frame(self.root, padding=10)
        outer.pack(fill=tk.BOTH, expand=True)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(1, weight=3)
        outer.rowconfigure(2, weight=2)

        toolbar = ttk.Frame(outer)
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(toolbar, text="Create Alert", command=self.open_create_dialog).pack(
            side=tk.LEFT
        )
        ttk.Button(toolbar, text="Delete Active", command=self.delete_selected).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        ttk.Button(toolbar, text="Refresh", command=self.refresh_all).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        ttk.Label(toolbar, text="Passed period:").pack(side=tk.LEFT, padx=(20, 4))
        period_box = ttk.Combobox(
            toolbar,
            textvariable=self.period_var,
            values=tuple(self.PASSED_PERIODS),
            state="readonly",
            width=9,
        )
        period_box.pack(side=tk.LEFT)
        period_box.bind("<<ComboboxSelected>>", lambda _event: self.refresh_passed())
        ttk.Label(toolbar, textvariable=self.status_var).pack(side=tk.RIGHT)

        panes = ttk.Panedwindow(outer, orient=tk.HORIZONTAL)
        panes.grid(row=1, column=0, sticky="nsew")
        passed_frame = ttk.LabelFrame(panes, text="Passed Alerts", padding=6)
        active_frame = ttk.LabelFrame(panes, text="Created / Active Alerts", padding=6)
        panes.add(passed_frame, weight=1)
        panes.add(active_frame, weight=1)

        self.passed_tree = self._make_tree(
            passed_frame,
            ("passed_at", "name"),
            {
                "passed_at": ("Passed", 165),
                "name": ("Name", 180),
            },
            "passed",
        )
        self.passed_tree.tag_configure("passed", foreground="#b00020")
        self.passed_tree.bind(
            "<<TreeviewSelect>>",
            lambda _event: self._show_selected_passed(),
        )

        self.definition_tree = self._make_tree(
            active_frame,
            ("created_at", "name", "type", "expiration"),
            {
                "created_at": ("Created", 165),
                "name": ("Name", 180),
                "type": ("Type", 120),
                "expiration": ("Expiration", 165),
            },
            "active",
        )
        self.definition_tree.bind(
            "<<TreeviewSelect>>",
            lambda _event: self._show_selected_definition(),
        )

        detail_frame = ttk.LabelFrame(outer, text="Alert Details", padding=6)
        detail_frame.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        detail_frame.rowconfigure(0, weight=1)
        detail_frame.columnconfigure(0, weight=1)
        self.detail_text = tk.Text(
            detail_frame,
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=("TkFixedFont", 10),
        )
        detail_scroll = ttk.Scrollbar(
            detail_frame,
            orient=tk.VERTICAL,
            command=self.detail_text.yview,
        )
        self.detail_text.configure(yscrollcommand=detail_scroll.set)
        self.detail_text.grid(row=0, column=0, sticky="nsew")
        detail_scroll.grid(row=0, column=1, sticky="ns")

    def _make_tree(
        self,
        parent: ttk.Frame,
        columns: tuple[str, ...],
        headings: dict[str, tuple[str, int]],
        tree_name: str,
    ) -> ttk.Treeview:
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)
        tree = ttk.Treeview(parent, columns=columns, show="headings", selectmode="browse")
        for column in columns:
            label, width = headings[column]
            tree.heading(
                column,
                text=label,
                command=lambda c=column, t=tree, n=tree_name: self._sort_tree(t, n, c),
            )
            tree.column(column, width=width, minwidth=60, anchor=tk.W)
        y_scroll = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=tree.yview)
        x_scroll = ttk.Scrollbar(parent, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        return tree

    def _sort_tree(self, tree: ttk.Treeview, tree_name: str, column: str) -> None:
        key = (tree_name, column)
        descending = not self.sort_descending.get(key, False)
        self.sort_descending[key] = descending
        rows = [(tree.set(item, column), item) for item in tree.get_children("")]

        def value_key(item: tuple[str, str]) -> tuple[int, Any]:
            value = item[0]
            try:
                return (0, float(value))
            except ValueError:
                return (1, value.casefold())

        rows.sort(key=value_key, reverse=descending)
        for index, (_value, item) in enumerate(rows):
            tree.move(item, "", index)

    def open_create_dialog(self) -> None:
        CreateAlertDialog(self.root, self.registry, self.create_definition)

    def create_definition(self, definition: AlertDefinition) -> None:
        try:
            self.repository.insert_definition(definition)
        except Exception as exc:
            messagebox.showerror("Create Failed", str(exc), parent=self.root)
            return
        self.status_var.set(f"Created alert: {definition.name}")
        self.refresh_definitions()

    def delete_selected(self) -> None:
        selected = self.definition_tree.selection()
        if not selected:
            messagebox.showwarning(
                "No Selection",
                "Select an active alert to delete.",
                parent=self.root,
            )
            return
        definition = self.definition_rows[selected[0]]
        if not messagebox.askyesno(
            "Delete Alert",
            f'Delete "{definition.name}"?',
            parent=self.root,
        ):
            return
        try:
            self.repository.delete_definition(definition.name, definition.created_at)
        except Exception as exc:
            messagebox.showerror("Delete Failed", str(exc), parent=self.root)
            return
        self.refresh_definitions()
        self._set_details("")

    def refresh_all(self) -> None:
        self.refresh_definitions()
        self.refresh_passed()

    def refresh_definitions(self) -> None:
        try:
            definitions = self.repository.load_definitions()
        except Exception as exc:
            self.status_var.set(f"Definition refresh failed: {exc}")
            return
        self.definition_tree.delete(*self.definition_tree.get_children())
        self.definition_rows.clear()
        for definition in definitions:
            config = definition.data.get("config", {})
            alert_type = str(definition.data.get("alert_type", "Unknown"))
            handler = self.registry.by_id(alert_type)
            item_id = self.definition_tree.insert(
                "",
                tk.END,
                values=(
                    format_datetime(definition.created_at),
                    definition.name,
                    handler.display_name if handler else alert_type,
                    self._display_iso(config.get("expiration")),
                ),
            )
            self.definition_rows[item_id] = definition

    def refresh_passed(self) -> None:
        since = eastern_now() - self.PASSED_PERIODS[self.period_var.get()]
        try:
            passed_rows = self.repository.load_passed(since)
        except Exception as exc:
            self.status_var.set(f"Passed refresh failed: {exc}")
            return
        self.passed_tree.delete(*self.passed_tree.get_children())
        self.passed_rows.clear()
        for passed in passed_rows:
            item_id = self.passed_tree.insert(
                "",
                tk.END,
                values=(
                    format_datetime(passed.passed_at),
                    passed.name,
                ),
                tags=("passed",),
            )
            self.passed_rows[item_id] = passed

    def _show_selected_definition(self) -> None:
        selected = self.definition_tree.selection()
        if not selected:
            return
        definition = self.definition_rows.get(selected[0])
        if definition is None:
            return
        self._set_details(
            self._details_text(
                "Active Alert",
                definition.name,
                definition.created_at,
                definition.data,
            )
        )

    def _show_selected_passed(self) -> None:
        selected = self.passed_tree.selection()
        if not selected:
            return
        passed = self.passed_rows.get(selected[0])
        if passed is None:
            return
        self._set_details(
            self._details_text(
                "Passed Alert",
                passed.name,
                passed.passed_at,
                passed.data,
            )
        )

    @staticmethod
    def _details_text(
        heading: str,
        name: str,
        event_time: dt.datetime,
        data: dict[str, Any],
    ) -> str:
        return (
            f"{heading}\n"
            f"Name: {name}\n"
            f"Date: {format_datetime(event_time)}\n\n"
            f"{json.dumps(data, indent=2, sort_keys=True, default=str)}"
        )

    @staticmethod
    def _display_iso(value: Any) -> str:
        parsed = parse_iso_datetime(value)
        return format_datetime(parsed) if parsed else "No expiration"

    def _set_details(self, content: str) -> None:
        self.detail_text.configure(state=tk.NORMAL)
        self.detail_text.delete("1.0", tk.END)
        self.detail_text.insert("1.0", content)
        self.detail_text.configure(state=tk.DISABLED)

    def _process_messages(self) -> None:
        if self.closing:
            return
        should_refresh = False
        passed_count = 0
        while True:
            try:
                kind, payload = self.messages.get_nowait()
            except queue.Empty:
                break
            if kind == "status":
                self.status_var.set(str(payload))
            elif kind == "error":
                self.status_var.set(str(payload))
            elif kind == "changed":
                should_refresh = True
            elif kind == "passed":
                passed_count += 1
        if should_refresh:
            self.refresh_all()
        if passed_count:
            self.root.bell()
            self.root.deiconify()
            self.root.lift()
        self.after_id = self.root.after(250, self._process_messages)

    def close(self) -> None:
        if self.closing:
            return
        self.closing = True
        self.stop_event.set()
        if self.after_id is not None:
            try:
                self.root.after_cancel(self.after_id)
            except tk.TclError:
                pass
        self.repository.dispose()
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor and display data alerts.")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL,
        help="Seconds between alert scans (default: 5).",
    )
    args = parser.parse_args()
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be greater than zero")
    return args


def main() -> None:
    args = parse_args()
    registry = AlertRegistry([RelativeVolumeAlertType()])
    repository = AlertRepository(
        alerts_engine=make_engine(ALERTS_DB, ALERTS_MYSQL_USER, database_password),
        stocks_engine=make_engine(STOCKS_DB, STOCKS_MYSQL_USER, gptdb),
    )
    root = tk.Tk()
    AlertsApp(root, repository, registry, args.poll_interval)
    root.mainloop()


if __name__ == "__main__":
    main()
