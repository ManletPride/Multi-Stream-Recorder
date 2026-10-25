"""Remember window size and place: the main window (clamped so a changed monitor never
leaves it off-screen) and resizable dialogs. Everything lives in window_state.json, which
the main window already uses; this module only ever *merges* into it."""

from __future__ import annotations

import json
import os
import re
import tkinter as tk
from pathlib import Path

_GEOMETRY = re.compile(r"^(\d+)x(\d+)(?:([+-])(-?\d+)([+-])(-?\d+))?$")
DIALOGS_KEY = "dialogs"
KEEP_VISIBLE = 80  # px of the window that must stay on the monitor


def parse_geometry(text) -> tuple[int, int, int | None, int | None] | None:
    """(w, h, x, y) from Tk's "WxH+X+Y" (x and y None when only a size). None if malformed."""
    match = _GEOMETRY.match(str(text or "").strip())
    if not match:
        return None
    w, h = int(match.group(1)), int(match.group(2))
    if match.group(3) is None:
        return w, h, None, None
    x = int(match.group(4)) if match.group(3) == "+" else -int(match.group(4))
    y = int(match.group(6)) if match.group(5) == "+" else -int(match.group(6))
    return w, h, x, y


def format_geometry(w: int, h: int, x: int | None = None, y: int | None = None) -> str:
    """Tk geometry text. A negative place is written +-N, which Tk reads as 'left of 0'
    (a bare -N would mean 'from the right edge')."""
    size = f"{int(w)}x{int(h)}"
    return size if x is None or y is None else f"{size}+{int(x)}+{int(y)}"


def clamp_geometry(text, min_size: tuple[int, int], area: tuple[int, int, int, int]) -> str | None:
    """`text` fitted to the monitor work area (left, top, right, bottom): not smaller than
    min_size, not bigger than the area, and at least KEEP_VISIBLE px on screen (the title
    bar stays reachable). A size-only value stays size-only. None for junk."""
    parsed = parse_geometry(text)
    if parsed is None:
        return None
    w, h, x, y = parsed
    left, top, right, bottom = area
    w = min(max(w, min_size[0]), max(min_size[0], right - left))
    h = min(max(h, min_size[1]), max(min_size[1], bottom - top))
    if x is None or y is None:
        return format_geometry(w, h)
    x = min(max(x, left - w + KEEP_VISIBLE), right - KEEP_VISIBLE)
    y = min(max(y, top), bottom - KEEP_VISIBLE)
    return format_geometry(w, h, x, y)


def load(path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def merge(path, values: dict, *, dialog: tuple[str, str] | None = None) -> None:
    """Write `values` into the state file, keeping every other key. dialog=(key, geometry)
    updates one entry of the "dialogs" map. Never raises (a lost position is harmless)."""
    try:
        state = load(path)
        state.update(values)
        if dialog is not None:
            dialogs = state.get(DIALOGS_KEY)
            dialogs = dict(dialogs) if isinstance(dialogs, dict) else {}
            dialogs[dialog[0]] = dialog[1]
            state[DIALOGS_KEY] = dialogs
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2)
        os.replace(tmp, path)
    except OSError:
        pass


def work_area_for(win: tk.Misc, geometry: str):
    from msr import dialogs

    parsed = parse_geometry(geometry)
    x = (parsed[2] or 0) + (parsed[0] // 2) if parsed else 0
    y = (parsed[3] or 0) + (parsed[1] // 2) if parsed else 0
    return dialogs.work_area(win, x, y)


def fit_main_geometry(win: tk.Misc, saved, default: str, min_size: tuple[int, int]) -> str:
    """The geometry to give the main window: the saved one made safe for today's monitors."""
    for candidate in (saved, default):
        fitted = clamp_geometry(candidate, min_size, work_area_for(win, str(candidate)))
        if fitted:
            return fitted
    return default


def attach(win: tk.Toplevel, state_file, key: str, min_size: tuple[int, int] | None = None) -> bool:
    """Give a resizable dialog its last size and place, and save them when it closes.
    Call after the dialog is laid out and before dialogs.present(). Returns True when a
    saved geometry was applied."""
    latest = {"geometry": None}
    applied = False
    saved = load(state_file).get(DIALOGS_KEY, {})
    saved = saved.get(key) if isinstance(saved, dict) else None
    if saved:
        win.update_idletasks()
        floor = min_size or (win.winfo_reqwidth() // 2, win.winfo_reqheight() // 2)
        fitted = clamp_geometry(saved, floor, work_area_for(win, saved))
        if fitted:
            win._msr_geometry = fitted  # dialogs.center_over uses it instead of centring
            applied = True
    if min_size:
        win.minsize(*min_size)

    def moved(event) -> None:
        if event.widget is win:
            try:
                if str(win.state()) == "normal":
                    latest["geometry"] = win.winfo_geometry()
            except tk.TclError:
                pass

    def closing(event) -> None:
        if event.widget is win and latest["geometry"]:
            geometry = latest["geometry"]
            if parse_geometry(geometry):
                merge(state_file, {}, dialog=(key, geometry))

    win.bind("<Configure>", moved, add="+")
    win.bind("<Destroy>", closing, add="+")
    return applied