"""Read-only data for the terminal status viewer (msr_tui.py).

No Textual and no tkinter here, so it is easy to test. Nothing writes a file: the viewer reads
``msr_status.json`` and, when a copy is running, asks it for log lines with the existing
read-only ``logs`` command over agent control.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field

from msr import control, themes

STALE_AFTER = 15.0     # seconds without a status rewrite before MSR counts as not running
LOG_LINES = 8

# Viewer states, in the order the GUI cares about them.
RECORDING = "recording"
CATCHING_UP = "catching up"
LIVE = "live, not recording"
CHECKING = "checking"
PROCESSING = "processing"
ERROR = "error"
OFFLINE = "offline"

# State -> theme token (same tokens the GUI channel list uses).
STATE_TOKEN = {RECORDING: "rec_fg", CATCHING_UP: "remux_fg", LIVE: "badge", CHECKING: "check_fg",
               PROCESSING: "completed_fg", ERROR: "error_fg", OFFLINE: "offline_fg"}


def resolve_data_dir(cli_value=None, environ=None) -> str:
    """Same order as msr_ctl.py: --data-dir, MSR_HOME, MSR_DATA, then the current folder."""
    environ = os.environ if environ is None else environ
    for value in (cli_value, environ.get("MSR_HOME"), environ.get("MSR_DATA")):
        value = (value or "").strip().strip('"')
        if value:
            return os.path.abspath(os.path.expandvars(os.path.expanduser(value)))
    return os.getcwd()


def classify(row: dict) -> str:
    """One channel row of msr_status.json -> a viewer state."""
    status = str(row.get("status") or "").lower()
    detail = str(row.get("detail") or "").lower()
    if row.get("error") or status.startswith("error") or "failed" in status:
        return ERROR
    if row.get("recording") or status.startswith("recording"):
        return CATCHING_UP if "catching up" in detail else RECORDING
    if status.startswith(("checking", "initializing")):
        return CHECKING
    if status.startswith(("processing", "remuxing")):
        return PROCESSING
    if row.get("live"):
        return LIVE
    return OFFLINE


def short_time(iso_text) -> str:
    """'2026-11-04T13:05:09' -> '13:05:09' today, else 'Nov 03 13:05'."""
    if not iso_text:
        return "-"
    try:
        t = time.strptime(str(iso_text)[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return str(iso_text)
    if time.strftime("%Y-%m-%d", t) == time.strftime("%Y-%m-%d"):
        return time.strftime("%H:%M:%S", t)
    return time.strftime("%b %d %H:%M", t)


def format_age(seconds) -> str:
    if seconds is None:
        return "?"
    s = max(0, int(seconds))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {s % 3600 // 60}m"


def format_gb(value) -> str:
    try:
        gb = float(value)
    except (TypeError, ValueError):
        return "?"
    return f"{gb / 1024:.2f} TB" if gb >= 1024 else f"{gb:.1f} GB"


@dataclass
class ChannelView:
    name: str
    platform: str
    state: str
    status: str        # MSR's own text, e.g. "Offline (reconnecting)"
    duration: str
    size: str
    last_live: str
    color: str


@dataclass
class Snapshot:
    found: bool = False          # status file exists and parsed
    running: bool = False        # fresh and says running
    reason: str = ""             # why not running / problem, for the header
    version: str = "?"
    mode: str = ""
    age: float | None = None     # seconds since the status file was rewritten
    recording: int = 0
    disk: str = "?"
    disk_low: bool = False
    control: bool = False        # msr_control.json present (agent control on)
    channels: list = field(default_factory=list)


def _status_text(row) -> str:
    status, detail = str(row.get("status") or ""), str(row.get("detail") or "")
    return f"{status} ({detail})" if detail else (status or "-")


def parse_status(data: dict, now: float | None = None, theme=None, stale_after=STALE_AFTER) -> Snapshot:
    """msr_status.json contents -> Snapshot. Tolerates missing keys (older/newer builds)."""
    theme = theme or themes.THEMES[themes.DEFAULT_THEME]
    now = time.time() if now is None else now
    snap = Snapshot(found=True)
    app = data.get("app") or {}
    snap.version = str(app.get("version") or "?")
    snap.mode = str(app.get("mode") or "")
    updated = data.get("updated_at")
    snap.age = (now - float(updated)) if isinstance(updated, (int, float)) else None
    if app.get("running") is False:
        snap.reason = "MSR closed (status file says not running)"
    elif snap.age is None or snap.age > stale_after:
        snap.reason = f"status file is stale ({format_age(snap.age)} old)"
    else:
        snap.running = True
    session = data.get("session") or {}
    snap.recording = int(session.get("recording") or 0)
    main = ((data.get("health") or {}).get("disk") or {}).get("main") or {}
    snap.disk = format_gb(main.get("free_gb"))
    snap.disk_low = bool(main.get("low"))
    for row in data.get("channels") or []:
        if not isinstance(row, dict):
            continue
        state = classify(row)
        snap.channels.append(ChannelView(
            name=str(row.get("name") or "?"), platform=str(row.get("platform") or ""), state=state,
            status="Live, not recording" if state == LIVE else _status_text(row), duration=str(row.get("duration") or "") or "-",
            size=str(row.get("size") or "") or "-", last_live=short_time(row.get("last_live")),
            color=getattr(theme, STATE_TOKEN[state])))
    return snap


def read_snapshot(data_dir: str, now: float | None = None, theme=None) -> Snapshot:
    """Read msr_status.json (read-only). Missing or broken file -> a Snapshot that says why."""
    path = os.path.join(data_dir, control.STATUS_FILE)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            raise ValueError("not an object")
    except FileNotFoundError:
        snap = Snapshot(reason=f"no {control.STATUS_FILE} in {data_dir} (MSR not started here, or control off)")
    except (OSError, ValueError) as e:
        # MSR replaces the file atomically, so this is rare; the next refresh retries
        snap = Snapshot(reason=f"could not read {control.STATUS_FILE}: {e.__class__.__name__}")
    else:
        snap = parse_status(data, now=now, theme=theme)
    snap.control = os.path.isfile(os.path.join(data_dir, control.CONTROL_FILE))
    return snap


def fetch_logs(data_dir: str, lines: int = LOG_LINES, call=control.call) -> list[str]:
    """Last log lines from the running copy via the read-only ``logs`` command ([] if none)."""
    try:
        reply = call(data_dir, "logs", {"lines": lines}, timeout=3.0)
    except Exception:
        return []
    if not isinstance(reply, dict) or not reply.get("ok", True):
        return []
    result = reply.get("result", reply)
    return [str(x) for x in (result.get("lines") or [])][-lines:]
