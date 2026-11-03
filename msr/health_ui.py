"""Health in the main window: a one-line status in the bottom bar (green / amber / red, click
for details) and the scheduler that runs msr/health.py on a background thread.

Results reach Tk through a queue drained by after(); the worker never touches Tk. Each new
problem is logged once, and once more when it clears.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import tkinter as tk
from tkinter import ttk
from typing import Callable

from msr import dialogs
from msr import health

STARTUP_DELAY_MS = 20_000     # after startup work; recording never waits for health checks
POLL_MS = 500
STYLE = {health.OK: "Ok.TLabel", health.WARN: "Warn.TLabel", health.ERROR: "Error.TLabel"}


def interval_minutes(config) -> float:
    """[Health] check_minutes; 0 (or less) = periodic checks off; at least 1 minute otherwise."""
    try:
        value = float(config.get("Health", "check_minutes", fallback="10") or 0)
    except (TypeError, ValueError):
        value = 10.0
    return 0.0 if value <= 0 else max(1.0, value)


class HealthController:
    """Owns the label, the details window and the schedule. All app access goes through the
    callables, so tests drive it with fakes."""

    def __init__(self, parent, *, config, get_status: Callable[[], dict], get_folder: Callable[[], str],
                 free_gb: Callable[[str], float | None], get_versions: Callable[[], dict],
                 get_state: Callable[[], dict], twitch_check: Callable[[], health.Issue | None] = lambda: None,
                 push_failed: Callable[[], bool] = lambda: False, open_updates: Callable[[], None] | None = None,
                 logger=None, tooltip=None, autostart: bool = True):
        self.parent, self.config = parent, config
        self.get_status, self.get_folder, self.free_gb = get_status, get_folder, free_gb
        self.get_versions, self.get_state = get_versions, get_state
        self.twitch_check, self.push_failed, self.open_updates = twitch_check, push_failed, open_updates
        self.log = logger or logging.getLogger("msr.health")
        self.monitor = health.Monitor()
        self.startup: list[health.Issue] = []
        self.report: health.Report | None = None
        self.q: queue.Queue = queue.Queue()
        self._busy = False
        self._next = None
        self._dialog = None
        self._stopped = False
        self.label = ttk.Label(parent, text="\u25cf Health: checking soon", style="Muted.TLabel", cursor="hand2")
        self.label.bind("<Button-1>", lambda _e: self.show_details())
        if tooltip is not None:
            tooltip(self.label, "Health checks: recording tools, recordings folder and free space, Twitch sign-in;\n"
                                "every few minutes: free space, stalled recordings, sites whose checks keep failing.\n"
                                "Click for details. Interval: Options > Other > Health check every (min).")
        self._ids: dict[str, str] = {}
        if autostart:
            self._ids["start"] = self.parent.after(STARTUP_DELAY_MS, lambda: self.run(startup=True))
        self._ids["drain"] = self.parent.after(POLL_MS, self._drain)
        self.label.bind("<Destroy>", lambda _e: self.stop(), add="+")

    def stop(self) -> None:
        """Cancel the timers (the window is closing)."""
        for after_id in list(self._ids.values()) + [self._next]:
            if after_id:
                try:
                    self.parent.after_cancel(after_id)
                except tk.TclError:
                    pass
        self._ids.clear()
        self._next = None
        self._stopped = True

    # -- work (background thread)
    def _work(self, startup: bool) -> None:
        try:
            min_gb = self.config.getfloat("Recording", "min_disk_space_gb", fallback=5.0)
            folder = self.get_folder()
            free = self.free_gb(folder)
            first = None
            if startup:
                versions = self.get_versions()
                first = (health.tool_issues(versions) + health.folder_issues(folder, free, min_gb)
                         + [i for i in [self.twitch_check()] if i]
                         + health.stale_issues(versions, self.get_state()))
            periodic = self.monitor.periodic(dict(self.get_status() or {}), time.time(), folder, free, min_gb)
            if self.push_failed():
                periodic.append(health.Issue(health.WARN, "twitch", "Twitch sign-in is no longer valid: "
                                                                   "reconnect it in Options (timed checks still run)"))
            self.q.put((first, periodic))
        except Exception as e:  # a health check must never take the app down
            self.q.put((None, [health.Issue(health.WARN, "health", f"Health check failed: {e}")]))

    def run(self, startup: bool = False) -> None:
        if self._busy:
            return
        self._busy = True
        threading.Thread(target=self._work, args=(startup,), daemon=True, name="msr-health").start()

    def check_now(self) -> None:
        self.run(startup=True)

    # -- Tk side
    def _drain(self) -> None:
        try:
            while True:
                first, periodic = self.q.get_nowait()
                self._busy = False
                if first is not None:
                    self.startup = first
                    # tool / Twitch / version problems are re-evaluated only by a full check
                startup = [i for i in self.startup if not i.key.startswith("disk")]
                self._show(health.merge(startup, periodic))
                self._schedule()
        except queue.Empty:
            pass
        if self._stopped:
            return
        try:
            self._ids["drain"] = self.parent.after(POLL_MS, self._drain)
        except tk.TclError:
            pass

    def _schedule(self) -> None:
        if self._next is not None:
            try:
                self.parent.after_cancel(self._next)
            except tk.TclError:
                pass
            self._next = None
        minutes = interval_minutes(self.config)
        if minutes and not self._stopped:
            self._next = self.parent.after(int(minutes * 60_000), self._periodic)

    def _periodic(self) -> None:
        self._next = None
        self.run(startup=False)

    def _show(self, report: health.Report) -> None:
        fresh, cleared = health.changes(self.report, report)
        for issue in fresh:
            (self.log.error if issue.level == health.ERROR else self.log.warning)(f"Health: {issue.text}")
        for key in cleared:
            self.log.info(f"Health: resolved ({key})")
        if self.report is None and not report.issues:
            self.log.info("Health: all checks OK")
        self.report = report
        self.label.configure(text=health.summary(report), style=STYLE[report.level])
        if self._dialog is not None and self._dialog.winfo_exists():
            self._fill_dialog()

    def show_details(self) -> None:
        if self._dialog is not None and self._dialog.winfo_exists():
            self._dialog.lift()
            return
        win, frame = dialogs.new_dialog(self.parent, "Health")
        self._dialog, self._body = win, ttk.Frame(frame)
        self._body.pack(fill="x")
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=(10, 0))
        ttk.Button(row, text="Close", command=win.destroy).pack(side="right")
        ttk.Button(row, text="Check now", command=self.check_now).pack(side="right", padx=(0, 6))
        if self.open_updates is not None:
            ttk.Button(row, text="Updates\u2026", command=self.open_updates).pack(side="right", padx=(0, 6))
        win.bind("<Escape>", lambda _e: win.destroy())
        self._fill_dialog()
        dialogs.present(win, self.parent)

    def _fill_dialog(self) -> None:
        for w in self._body.winfo_children():
            w.destroy()
        report = self.report
        if report is None:
            ttk.Label(self._body, text="Not checked yet (the first check runs shortly after start).").pack(anchor="w")
            return
        if not report.issues:
            ttk.Label(self._body, text="\u2714 Everything checked is fine.", style="Ok.TLabel").pack(anchor="w")
        for issue in report.issues:
            mark = "\u2716" if issue.level == health.ERROR else "\u26a0"
            ttk.Label(self._body, text=f"{mark} {issue.text}", style=STYLE[issue.level], wraplength=520,
                      justify="left").pack(anchor="w", pady=1)
        minutes = interval_minutes(self.config)
        every = f"every {minutes:g} min" if minutes else "periodic checks off"
        ttk.Label(self._body, style="Muted.TLabel",
                  text=f"Checked {time.strftime('%H:%M', time.localtime(report.checked_at))}; {every}. "
                       "Updates never install on their own.").pack(anchor="w", pady=(6, 0))
