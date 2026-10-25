"""Themed, always-parented dialogs for the Tk GUI.

Native ``messagebox`` / ``simpledialog`` windows ignore the app theme and, called
without ``parent=``, open wherever Windows likes. These helpers replace them:

* every dialog belongs to a window (``parent``) and is centred on it, on the same
  monitor, clamped to that monitor's work area;
* the dark title bar is applied *before* the window is positioned and shown, so it
  never flashes in the monitor's top-left corner (creating the native window makes
  Windows move it to its cascade spot);
* if the parent is hidden (minimised to the tray) the dialog centres on the screen
  and is not made transient to it (a transient of a hidden window is invisible).

Colours come from the ttk style that ``apply_theme`` already set, so nothing here
duplicates the palette. The GUI registers its dark/light switch once with
``set_dark_getter``; without it the mode is guessed from the frame background.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Callable

from msr import themes, wintheme
from msr.winstate import format_geometry, parse_geometry

_dark_getter: Callable[[], bool] | None = None

# glyph and the theme token its colour comes from
_ICONS = {"info": ("\u2139", "info"), "warning": ("\u26a0", "warn"), "error": ("\u2716", "error"),
          "question": ("?", "info")}
_RETITLE_MS = (100, 500)


def icon_color(kind: str) -> str:
    return themes.current()[_ICONS[kind][1]]


def set_dark_getter(getter: Callable[[], bool] | None) -> None:
    """Override how dialogs learn whether the theme is dark (default: the current theme's flag)."""
    global _dark_getter
    _dark_getter = getter


def frame_background(win: tk.Misc) -> str:
    try:
        return str(ttk.Style(win).lookup("TFrame", "background")) or themes.current()["bg"]
    except tk.TclError:
        return themes.current()["bg"]


def _luminance(color: str) -> float:
    color = color.lstrip("#")
    if len(color) == 3:
        color = "".join(c * 2 for c in color)
    try:
        r, g, b = (int(color[i:i + 2], 16) for i in (0, 2, 4))
    except ValueError:
        return 1.0
    return (0.299 * r + 0.587 * g + 0.114 * b) / 255


def is_dark(win: tk.Misc) -> bool:
    if _dark_getter is not None:
        try:
            return bool(_dark_getter())
        except Exception:
            pass
    return themes.current().dark


def apply_title_bar(win: tk.Toplevel, dark: bool | None = None) -> None:
    """Themed caption now, again shortly after the window is shown, and on every <Map>.
    Marks the window as themed (the guard tests look for this)."""
    def paint() -> None:
        try:
            if win.winfo_exists():
                wintheme.set_title_bar_dark(win, is_dark(win) if dark is None else dark,
                                            themes.caption_colors(themes.current()))
        except tk.TclError:
            pass

    paint()
    wintheme.enable_background_drag(win)  # every dialog can be moved by its background
    win._msr_title_bar = True  # type: ignore[attr-defined]
    pending: list[str] = []

    def cancel(event=None) -> None:
        if event is not None and event.widget is not win:
            return
        for token in pending:
            try:
                win.after_cancel(token)
            except tk.TclError:
                pass
        pending.clear()

    try:
        win.bind("<Map>", lambda e: paint() if e.widget is win else None, add="+")
        win.bind("<Destroy>", cancel, add="+")  # a dialog closed in under 0.5 s leaves no stray callbacks
        for delay in _RETITLE_MS:
            pending.append(win.after(delay, paint))
    except tk.TclError:
        pass


# ---------------------------------------------------------------- placement
def work_area(win: tk.Misc, x: int = 0, y: int = 0) -> tuple[int, int, int, int]:
    """(left, top, right, bottom) of the usable area of the monitor nearest (x, y)."""
    try:
        import ctypes
        from ctypes import wintypes

        class MONITORINFO(ctypes.Structure):
            _fields_ = [("cbSize", wintypes.DWORD), ("rcMonitor", wintypes.RECT),
                        ("rcWork", wintypes.RECT), ("dwFlags", wintypes.DWORD)]

        user32 = ctypes.windll.user32
        user32.MonitorFromPoint.restype = wintypes.HMONITOR
        user32.MonitorFromPoint.argtypes = [wintypes.POINT, wintypes.DWORD]
        monitor = user32.MonitorFromPoint(wintypes.POINT(int(x), int(y)), 2)  # nearest
        info = MONITORINFO()
        info.cbSize = ctypes.sizeof(MONITORINFO)
        if monitor and user32.GetMonitorInfoW(monitor, ctypes.byref(info)):
            r = info.rcWork
            return r.left, r.top, r.right, r.bottom
    except Exception:
        pass
    return 0, 0, win.winfo_screenwidth(), max(200, win.winfo_screenheight() - 40)


def centered_position(owner: tuple[int, int, int, int], size: tuple[int, int],
                      area: tuple[int, int, int, int]) -> tuple[int, int]:
    """Top-left for a window of `size` centred in `owner` (x, y, w, h), kept inside `area`
    (left, top, right, bottom)."""
    px, py, pw, ph = owner
    ww, wh = size
    left, top, right, bottom = area
    x = min(max(left, px + (pw - ww) // 2), max(left, right - ww))
    y = min(max(top, py + (ph - wh) // 2), max(top, bottom - wh))
    return x, y


def visible_owner(parent: tk.Misc | None) -> tk.Toplevel | tk.Tk | None:
    """parent's toplevel when it is on screen, else None (hidden to the tray, closing)."""
    if parent is None:
        return None
    try:
        top = parent.winfo_toplevel()
        if top.winfo_exists() and top.winfo_viewable() and str(top.state()) not in ("withdrawn", "iconic"):
            return top
    except (tk.TclError, AttributeError):
        pass
    return None


def center_over(win: tk.Toplevel, parent: tk.Misc | None) -> None:
    """Centre `win` over `parent` (or the screen when parent is hidden). The caption is
    themed first, the place chosen after, and set again when the window maps."""
    win.update_idletasks()
    if not getattr(win, "_msr_title_bar", False):
        apply_title_bar(win)
    owner = visible_owner(parent)
    size = (win.winfo_reqwidth(), win.winfo_reqheight())
    stored = parse_geometry(getattr(win, "_msr_geometry", None))
    if stored:  # a remembered size (and place) from winstate.attach()
        size = (stored[0], stored[1])
    if owner is not None:
        try:
            owner.update_idletasks()
            rect = (owner.winfo_rootx(), owner.winfo_rooty(), owner.winfo_width(), owner.winfo_height())
        except tk.TclError:
            owner, rect = None, (0, 0, 0, 0)
    if owner is None:
        rect = (0, 0, win.winfo_screenwidth(), win.winfo_screenheight())
    area = work_area(win, rect[0] + rect[2] // 2, rect[1] + rect[3] // 2)
    x, y = centered_position(rect, size, area)
    wanted = f"+{x}+{y}"
    if stored:
        wanted = format_geometry(stored[0], stored[1], *( (stored[2], stored[3]) if stored[2] is not None else (x, y) ))
    win.geometry(wanted)
    win._msr_place = wanted  # type: ignore[attr-defined]
    done = [False]

    def mapped(event=None) -> None:
        if done[0] or (event is not None and event.widget is not win):
            return
        done[0] = True

        def fix() -> None:
            try:
                win.geometry(wanted)  # a no-op when it is already there
                win.lift()
            except tk.TclError:
                pass

        try:
            win.after_idle(fix)
        except tk.TclError:
            pass

    win.bind("<Map>", mapped, add="+")


def _reveal(win: tk.Toplevel, on_top: bool) -> None:
    """Show a window that was built withdrawn. (Tests replace this: no window appears.)"""
    win.deiconify()
    win.lift()
    if on_top:  # parent hidden to the tray: make sure the user sees the question
        win.attributes("-topmost", True)
        win.after(300, lambda: win.winfo_exists() and win.attributes("-topmost", False))


def present(win: tk.Toplevel, parent: tk.Misc | None) -> None:
    """Finish a custom dialog that was built withdrawn: transient to a visible parent,
    caption themed, centred, then shown."""
    owner = visible_owner(parent)
    if owner is not None:
        try:
            win.transient(owner)
        except tk.TclError:
            pass
    center_over(win, parent)
    _reveal(win, owner is None)


def new_dialog(parent: tk.Misc, title: str, *, padding: int = 14) -> tuple[tk.Toplevel, ttk.Frame]:
    """A withdrawn, themed, non-resizable Toplevel with a padded ttk frame, owned by
    parent's window. Finish with present() or a modal helper."""
    owner = visible_owner(parent)
    win = tk.Toplevel(owner or parent)
    win.withdraw()  # shown once it is in place, so it never flashes in a corner
    win.title(title)
    win.resizable(False, False)
    win.configure(bg=frame_background(win))
    apply_title_bar(win)
    frame = ttk.Frame(win, padding=padding)
    frame.pack(fill="both", expand=True)
    return win, frame


def run_modal(win: tk.Toplevel, parent: tk.Misc, focus: tk.Misc | None = None) -> None:
    """Show a dialog built with new_dialog() modally: centred, grabbed, until it is destroyed."""
    _show_modal(win, parent, focus)


def _show_modal(win: tk.Toplevel, parent: tk.Misc, focus: tk.Misc | None = None) -> None:
    present(win, parent)
    try:
        win.grab_set()
    except tk.TclError:
        pass
    (focus or win).focus_set()
    win.wait_window(win)


def _buttons(frame: ttk.Frame, win: tk.Toplevel, specs: list[tuple[str, str, Callable[[], None]]]) -> dict:
    """Right-aligned buttons; specs are (key, label, command) in left-to-right order."""
    row = ttk.Frame(frame)
    row.pack(fill="x", pady=(14, 0))
    made = {}
    for key, label, command in reversed(specs):
        button = ttk.Button(row, text=label, command=command, width=max(8, len(label) + 2))
        button.pack(side="right", padx=(6, 0))
        made[key] = button
    win._msr_buttons = made  # type: ignore[attr-defined]
    return made


def _bind_keys(win: tk.Toplevel, enter: Callable[[], object], escape: Callable[[], object]) -> None:
    """Enter runs `enter`, Escape and the window's X run `escape`. The handlers are kept on
    the window (_msr_enter / _msr_escape) so tests can run them without key events."""
    def run(handler):
        def inner(_event=None):
            handler()
            return "break"
        return inner

    win.bind("<Return>", run(enter))
    win.bind("<KP_Enter>", run(enter))
    win.bind("<Escape>", run(escape))
    win.protocol("WM_DELETE_WINDOW", escape)
    win._msr_enter, win._msr_escape = enter, escape  # type: ignore[attr-defined]


def _body(frame: ttk.Frame, message: str, kind: str) -> None:
    wrap = min(560, max(320, frame.winfo_screenwidth() // 3))
    top = ttk.Frame(frame)
    top.pack(fill="x")
    glyph = _ICONS[kind][0]
    tk.Label(top, text=glyph, fg=icon_color(kind), bg=frame_background(frame), font=("Segoe UI Symbol", 20)).pack(
        side="left", anchor="n", padx=(0, 12))
    ttk.Label(top, text=message, wraplength=wrap, justify="left").pack(side="left", fill="x", expand=True)


# ---------------------------------------------------------------- messages
def _message(parent: tk.Misc, title: str, message: str, kind: str) -> None:
    win, frame = new_dialog(parent, title)
    _body(frame, message, kind)
    buttons = _buttons(frame, win, [("ok", "OK", win.destroy)])
    _bind_keys(win, win.destroy, win.destroy)
    _show_modal(win, parent, buttons["ok"])


def show_info(parent: tk.Misc, title: str, message: str) -> None:
    _message(parent, title, message, "info")


def show_warning(parent: tk.Misc, title: str, message: str) -> None:
    _message(parent, title, message, "warning")


def show_error(parent: tk.Misc, title: str, message: str) -> None:
    _message(parent, title, message, "error")


def ask_yes_no(parent: tk.Misc, title: str, message: str, default_no: bool = False) -> bool:
    """Yes/No question. Closing the window or Escape answers No. default_no puts the focus
    on No and makes Enter mean No."""
    win, frame = new_dialog(parent, title)
    _body(frame, message, "question")
    result = {"yes": False}

    def yes() -> None:
        result["yes"] = True
        win.destroy()

    buttons = _buttons(frame, win, [("yes", "Yes", yes), ("no", "No", win.destroy)])
    _bind_keys(win, win.destroy if default_no else yes, win.destroy)
    win._msr_result = result  # type: ignore[attr-defined]
    _show_modal(win, parent, buttons["no" if default_no else "yes"])
    return result["yes"]


# ---------------------------------------------------------------- numbers
def parse_number(text: str, *, integer: bool, minvalue: float | None, maxvalue: float | None):
    """(value, error). Accepts a decimal comma. The error text is shown under the box."""
    raw = (text or "").strip().replace(",", ".")
    try:
        value = int(raw) if integer else float(raw)
    except ValueError:
        value = None
    if value is not None and value != value:  # NaN
        value = None
    low = "" if minvalue is None else f"{minvalue:g}"
    high = "" if maxvalue is None else f"{maxvalue:g}"
    if value is None:
        kind = "a whole number" if integer else "a number"
        if minvalue is not None and maxvalue is not None:
            return None, f"Enter {kind} from {low} to {high}."
        return None, f"Enter {kind}."
    if (minvalue is not None and value < minvalue) or (maxvalue is not None and value > maxvalue):
        return None, f"The value must be from {low} to {high}."
    return value, ""


def _ask_number(parent, title, prompt, *, integer, minvalue, maxvalue, initial):
    win, frame = new_dialog(parent, title)
    ttk.Label(frame, text=prompt, wraplength=420, justify="left").pack(anchor="w")
    var = tk.StringVar(win, value="" if initial is None else f"{initial:g}")
    entry = ttk.Entry(frame, textvariable=var, width=14)
    entry.pack(anchor="w", pady=(8, 0))
    error = tk.Label(frame, text="", fg=icon_color("error"), bg=frame_background(frame), anchor="w",
                     justify="left", wraplength=420)
    error.pack(fill="x", pady=(4, 0))
    result: dict = {}

    def ok():
        value, message = parse_number(var.get(), integer=integer, minvalue=minvalue, maxvalue=maxvalue)
        if message:
            error.configure(text=message)
            entry.focus_set()
            entry.select_range(0, "end")
            return
        result["value"] = value
        win.destroy()

    _buttons(frame, win, [("ok", "OK", ok), ("cancel", "Cancel", win.destroy)])
    _bind_keys(win, ok, win.destroy)
    win._msr_entry, win._msr_var, win._msr_result = entry, var, result  # type: ignore[attr-defined]
    entry.select_range(0, "end")
    _show_modal(win, parent, entry)
    return result.get("value")


def ask_float(parent, title, prompt, *, minvalue=None, maxvalue=None, initial=None) -> float | None:
    """A number (decimal comma allowed) in range, or None when cancelled."""
    return _ask_number(parent, title, prompt, integer=False, minvalue=minvalue, maxvalue=maxvalue, initial=initial)


def ask_int(parent, title, prompt, *, minvalue=None, maxvalue=None, initial=None) -> int | None:
    """A whole number in range, or None when cancelled."""
    return _ask_number(parent, title, prompt, integer=True, minvalue=minvalue, maxvalue=maxvalue, initial=initial)
