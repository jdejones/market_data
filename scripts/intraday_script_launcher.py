from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import subprocess
import sys
import tempfile
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
from typing import BinaryIO, Callable


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

CREATE_NO_WINDOW = 0x08000000
CREATE_NEW_PROCESS_GROUP = 0x00000200
WM_CLOSE = 0x0010


@dataclass(frozen=True)
class WindowSlot:
    x: float
    y: float
    width: float
    height: float


@dataclass(frozen=True)
class ScriptSpec:
    key: str
    label: str
    filename: str
    arguments: Callable[["DashboardConfig"], list[str]]
    autostart: bool = False
    selectable: bool = True
    window_title: str | None = None
    window_slot: WindowSlot | None = None


@dataclass(frozen=True)
class DashboardConfig:
    python_exe: Path
    symbols_files: tuple[Path, ...]
    startup_delay_seconds: int


@dataclass
class ManagedProcess:
    process: subprocess.Popen[bytes]
    log_file: BinaryIO
    state: str


def no_arguments(_config: DashboardConfig) -> list[str]:
    return []


def stream_arguments(config: DashboardConfig) -> list[str]:
    return ["--symbols-file", *(str(path) for path in config.symbols_files)]


def current_rvol_arguments(config: DashboardConfig) -> list[str]:
    return [
        "--symbols-file",
        str(config.symbols_files[0]),
        "--update-elevated-table",
        "--update-ep-rvol-table",
    ]


SCRIPT_SPECS = (
    ScriptSpec(
        key="intraday_price_stream",
        label="Intraday Price Stream",
        filename="intraday_price_stream.py",
        arguments=stream_arguments,
        autostart=True,
        selectable=False,
    ),
    ScriptSpec(
        key="current_rvol_gui",
        label="Current RVol",
        filename="current_rvol_gui.py",
        arguments=current_rvol_arguments,
        autostart=True,
        selectable=False,
        window_title="Current RVol",
        window_slot=WindowSlot(0.00, 0.00, 0.43, 0.28),
    ),
    ScriptSpec(
        key="high_short_interest_in_play",
        label="High Short Interest In Play",
        filename="high_short_interest_in_play.py",
        arguments=no_arguments,
        window_title="High Short Interest In Play",
        window_slot=WindowSlot(0.63, 0.00, 0.37, 0.28),
    ),
    ScriptSpec(
        key="etf_trader",
        label="ETF Relative Strength",
        filename="etf_trader.py",
        arguments=no_arguments,
        window_title="ETF Relative Strength",
    ),
    ScriptSpec(
        key="vwap_bands",
        label="VWAP Bands",
        filename="vwap_bands.py",
        arguments=no_arguments,
    ),
    ScriptSpec(
        key="nhod",
        label="New High of Day",
        filename="nhod.py",
        arguments=no_arguments,
        window_title="New High of Day",
        window_slot=WindowSlot(0.00, 0.29, 0.20, 0.36),
    ),
    ScriptSpec(
        key="nlod",
        label="New Low of Day",
        filename="nlod.py",
        arguments=no_arguments,
        window_title="New Low of Day",
        window_slot=WindowSlot(0.21, 0.29, 0.20, 0.36),
    ),
    ScriptSpec(
        key="ep_continuation",
        label="EP Continuation",
        filename="ep_continuation.py",
        arguments=no_arguments,
        window_title="EP Continuation",
        window_slot=WindowSlot(0.57, 0.29, 0.20, 0.36),
    ),
    ScriptSpec(
        key="ep_59ema_cross_below_vwap",
        label="EP 5/9 EMA Cross Below VWAP",
        filename="ep_59ema_cross_below_vwap.py",
        arguments=no_arguments,
        window_title="EP 5/9 EMA Cross Below VWAP",
        window_slot=WindowSlot(0.78, 0.29, 0.22, 0.36),
    ),
)
SPECS_BY_KEY = {spec.key: spec for spec in SCRIPT_SPECS}


class MarketDataDashboard:
    def __init__(self, root: tk.Tk, config: DashboardConfig) -> None:
        self.root = root
        self.config = config
        self.processes: dict[str, ManagedProcess] = {}
        self.statuses = {spec.key: "Not running" for spec in SCRIPT_SPECS}
        self.closing = False

        self.root.title("Market Data Dashboard")
        self._place_dashboard()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        optional_specs = [spec for spec in SCRIPT_SPECS if spec.selectable]
        self.optional_by_label = {spec.label: spec for spec in optional_specs}
        self.selected_script = tk.StringVar(value=optional_specs[0].label)
        self.summary_var = tk.StringVar(value="Starting required market-data services...")

        self._build_widgets(optional_specs)
        self.root.after(100, self._autostart_stream)
        self.root.after(
            self.config.startup_delay_seconds * 1_000,
            self._autostart_current_rvol,
        )
        self.root.after(500, self.poll_processes)

    def _place_dashboard(self) -> None:
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        width = max(360, int(screen_width * 0.19))
        height = max(300, int(screen_height * 0.28))
        x = int(screen_width * 0.435)
        self.root.geometry(f"{width}x{height}+{x}+0")
        self.root.minsize(360, 300)

    def _build_widgets(self, optional_specs: list[ScriptSpec]) -> None:
        container = ttk.Frame(self.root, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        selection = ttk.Frame(container)
        selection.grid(row=0, column=0, sticky="ew")
        selection.columnconfigure(0, weight=1)

        ttk.Label(selection, text="Optional daily script").grid(
            row=0,
            column=0,
            sticky="w",
        )
        dropdown = ttk.Combobox(
            selection,
            textvariable=self.selected_script,
            values=[spec.label for spec in optional_specs],
            state="readonly",
        )
        dropdown.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 6))
        dropdown.bind("<<ComboboxSelected>>", lambda _event: self.select_tree_row())

        ttk.Button(selection, text="Start", command=self.start_selected).grid(
            row=2,
            column=0,
            sticky="ew",
            padx=(0, 4),
        )
        ttk.Button(selection, text="Stop", command=self.stop_selected).grid(
            row=2,
            column=1,
            sticky="ew",
            padx=(4, 0),
        )

        self.process_tree = ttk.Treeview(
            container,
            columns=("state", "pid"),
            show="tree headings",
            height=8,
            selectmode="extended",
        )
        self.process_tree.heading("#0", text="Script")
        self.process_tree.heading("state", text="State")
        self.process_tree.heading("pid", text="PID")
        self.process_tree.column("#0", width=165, stretch=True)
        self.process_tree.column("state", width=90, stretch=False)
        self.process_tree.column("pid", width=55, stretch=False, anchor=tk.E)
        self.process_tree.grid(row=1, column=0, sticky="nsew", pady=(10, 6))
        self.process_tree.bind("<<TreeviewSelect>>", self.on_tree_selection)

        for spec in SCRIPT_SPECS:
            self.process_tree.insert(
                "",
                tk.END,
                iid=spec.key,
                text=spec.label,
                values=("Not running", ""),
            )

        controls = ttk.Frame(container)
        controls.grid(row=2, column=0, sticky="ew")
        controls.columnconfigure(0, weight=1)
        controls.columnconfigure(1, weight=1)
        ttk.Button(
            controls,
            text="Arrange Windows",
            command=self.arrange_all_windows,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(
            controls,
            text="Stop Highlighted",
            command=self.stop_highlighted,
        ).grid(row=0, column=1, sticky="ew", padx=(4, 0))

        ttk.Label(
            container,
            textvariable=self.summary_var,
            wraplength=340,
            justify=tk.LEFT,
        ).grid(row=3, column=0, sticky="ew", pady=(6, 0))

        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

    def _autostart_stream(self) -> None:
        self.start_script(SPECS_BY_KEY["intraday_price_stream"])
        self.summary_var.set(
            f"Current RVol will start in {self.config.startup_delay_seconds} seconds."
        )

    def _autostart_current_rvol(self) -> None:
        self.start_script(SPECS_BY_KEY["current_rvol_gui"])
        self.summary_var.set("Required startup scripts have been launched.")

    def start_selected(self) -> None:
        spec = self.optional_by_label[self.selected_script.get()]
        self.start_script(spec)

    def stop_selected(self) -> None:
        spec = self.optional_by_label[self.selected_script.get()]
        self.stop_script(spec.key)

    def stop_highlighted(self) -> None:
        selected = self.process_tree.selection()
        running = [
            key
            for key in selected
            if (
                (managed := self.processes.get(key)) is not None
                and managed.process.poll() is None
            )
        ]
        if not running:
            self.summary_var.set("No highlighted scripts are running.")
            return

        for key in running:
            self.stop_script(key)
        self.summary_var.set(f"Stopping {len(running)} highlighted script(s)...")

    def select_tree_row(self) -> None:
        spec = self.optional_by_label[self.selected_script.get()]
        self.process_tree.selection_set(spec.key)
        self.process_tree.see(spec.key)

    def on_tree_selection(self, _event: tk.Event[tk.Misc]) -> None:
        selected = self.process_tree.selection()
        if len(selected) != 1:
            return
        spec = SPECS_BY_KEY[selected[0]]
        if spec.selectable:
            self.selected_script.set(spec.label)

    def start_script(self, spec: ScriptSpec) -> None:
        managed = self.processes.get(spec.key)
        if managed is not None and managed.process.poll() is None:
            self.summary_var.set(f"{spec.label} is already running.")
            return

        log_dir = Path(tempfile.gettempdir()) / "market_data_dashboard"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{spec.key}.log"
        log_file = log_path.open("ab")
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"\n[{timestamp}] Starting {spec.label}\n".encode())
        log_file.flush()

        command = [
            str(self.config.python_exe),
            str(SCRIPT_DIR / spec.filename),
            *spec.arguments(self.config),
        ]
        try:
            process = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                stdin=subprocess.DEVNULL,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                creationflags=CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP,
            )
        except Exception as exc:
            log_file.close()
            self.statuses[spec.key] = "Launch failed"
            self.refresh_tree()
            messagebox.showerror(
                "Script Launch Failed",
                f"Could not start {spec.label}:\n\n{exc}",
            )
            return

        self.processes[spec.key] = ManagedProcess(
            process=process,
            log_file=log_file,
            state="Running",
        )
        self.statuses[spec.key] = "Running"
        self.summary_var.set(f"Started {spec.label}. Log: {log_path}")
        self.refresh_tree()
        if spec.window_slot is not None:
            self._schedule_window_arrangement(spec.key, attempts_remaining=20)

    def stop_script(self, key: str) -> None:
        spec = SPECS_BY_KEY[key]
        managed = self.processes.get(key)
        if managed is None or managed.process.poll() is not None:
            self.summary_var.set(f"{spec.label} is not running.")
            return

        self.statuses[key] = "Stopping"
        self.refresh_tree()
        if spec.window_title and self._post_close_to_windows(
            managed.process.pid,
            spec.window_title,
        ):
            self.root.after(2_000, lambda: self._terminate_if_running(key))
        else:
            managed.process.terminate()
        self.summary_var.set(f"Stopping {spec.label}...")

    def _terminate_if_running(self, key: str) -> None:
        managed = self.processes.get(key)
        if managed is not None and managed.process.poll() is None:
            managed.process.terminate()

    def poll_processes(self) -> None:
        for key, managed in list(self.processes.items()):
            exit_code = managed.process.poll()
            if exit_code is None:
                continue
            if not managed.log_file.closed:
                managed.log_file.close()
            if self.statuses[key] == "Stopping":
                self.statuses[key] = "Stopped"
            else:
                self.statuses[key] = f"Exited ({exit_code})"

        self.refresh_tree()
        if not self.closing:
            self.root.after(500, self.poll_processes)

    def refresh_tree(self) -> None:
        for spec in SCRIPT_SPECS:
            managed = self.processes.get(spec.key)
            pid = (
                str(managed.process.pid)
                if managed is not None and managed.process.poll() is None
                else ""
            )
            self.process_tree.item(
                spec.key,
                values=(self.statuses[spec.key], pid),
            )

    def arrange_all_windows(self) -> None:
        arranged = 0
        for spec in SCRIPT_SPECS:
            managed = self.processes.get(spec.key)
            if (
                managed is not None
                and managed.process.poll() is None
                and spec.window_slot is not None
                and self._arrange_window(spec, managed.process.pid)
            ):
                arranged += 1
        self.summary_var.set(f"Arranged {arranged} managed GUI window(s).")

    def _schedule_window_arrangement(self, key: str, attempts_remaining: int) -> None:
        if self.closing:
            return
        spec = SPECS_BY_KEY[key]
        managed = self.processes.get(key)
        if managed is None or managed.process.poll() is not None:
            return
        if self._arrange_window(spec, managed.process.pid):
            return
        if attempts_remaining > 0:
            self.root.after(
                500,
                lambda: self._schedule_window_arrangement(
                    key,
                    attempts_remaining - 1,
                ),
            )

    def _arrange_window(self, spec: ScriptSpec, process_id: int) -> bool:
        if spec.window_slot is None or spec.window_title is None:
            return False
        handles = self._find_windows(process_id, spec.window_title)
        if not handles:
            return False

        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        slot = spec.window_slot
        x = int(screen_width * slot.x)
        y = int(screen_height * slot.y)
        width = max(300, int(screen_width * slot.width))
        height = max(240, int(screen_height * slot.height))
        ctypes.windll.user32.MoveWindow(
            ctypes.c_void_p(handles[0]),
            x,
            y,
            width,
            height,
            True,
        )
        return True

    @staticmethod
    def _find_windows(process_id: int, title_text: str) -> list[int]:
        user32 = ctypes.windll.user32
        handles: list[int] = []
        callback_type = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)

        def callback(hwnd: int, _lparam: int) -> bool:
            window_pid = ctypes.c_ulong()
            window_handle = ctypes.c_void_p(hwnd)
            user32.GetWindowThreadProcessId(window_handle, ctypes.byref(window_pid))
            if (
                window_pid.value != process_id
                or not user32.IsWindowVisible(window_handle)
            ):
                return True
            title_length = user32.GetWindowTextLengthW(window_handle)
            title_buffer = ctypes.create_unicode_buffer(title_length + 1)
            user32.GetWindowTextW(window_handle, title_buffer, title_length + 1)
            if title_text.casefold() in title_buffer.value.casefold():
                handles.append(hwnd)
            return True

        user32.EnumWindows(callback_type(callback), 0)
        return handles

    def _post_close_to_windows(self, process_id: int, title_text: str) -> bool:
        handles = self._find_windows(process_id, title_text)
        for hwnd in handles:
            ctypes.windll.user32.PostMessageW(ctypes.c_void_p(hwnd), WM_CLOSE, 0, 0)
        return bool(handles)

    def on_close(self) -> None:
        running = [
            key
            for key, managed in self.processes.items()
            if managed.process.poll() is None
        ]
        if running and messagebox.askyesno(
            "Close Market Data Dashboard",
            "Stop all scripts launched by this dashboard before closing?",
        ):
            self.closing = True
            for key in running:
                self.stop_script(key)
            self.root.after(2_500, self._finish_close)
            return
        self._finish_close()

    def _finish_close(self) -> None:
        for managed in self.processes.values():
            if self.closing and managed.process.poll() is None:
                managed.process.terminate()
            if not managed.log_file.closed:
                managed.log_file.close()
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch and manage the daily market-data scripts."
    )
    parser.add_argument("--python-exe", type=Path, default=Path(sys.executable))
    parser.add_argument("--symbols-file", required=True, type=Path, nargs="+")
    parser.add_argument("--startup-delay-seconds", type=int, default=30)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.startup_delay_seconds < 0:
        raise ValueError("--startup-delay-seconds must be greater than or equal to zero")
    if not args.python_exe.is_file():
        raise FileNotFoundError(f"Python executable not found: {args.python_exe}")
    missing_symbols_files = [path for path in args.symbols_file if not path.is_file()]
    if missing_symbols_files:
        paths = ", ".join(str(path) for path in missing_symbols_files)
        raise FileNotFoundError(f"Symbols file(s) not found: {paths}")

    config = DashboardConfig(
        python_exe=args.python_exe,
        symbols_files=tuple(args.symbols_file),
        startup_delay_seconds=args.startup_delay_seconds,
    )
    root = tk.Tk()
    app = MarketDataDashboard(root, config)
    _ = app
    root.mainloop()


if __name__ == "__main__":
    main()
