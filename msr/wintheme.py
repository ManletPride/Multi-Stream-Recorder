"""Windows title-bar theming shared by the main window and dialogs.

Tk can't colour the native caption, so on Windows 10 20H1+/11 we ask DWM for
the immersive dark title bar (DWMWA_USE_IMMERSIVE_DARK_MODE = 20; 19 on early
Windows 11 builds). Native ``messagebox`` dialogs are OS-owned and follow the
system theme only; they cannot be themed from here.
"""

import os
import sys

DWMWA_USE_IMMERSIVE_DARK_MODE = 20
DWMWA_USE_IMMERSIVE_DARK_MODE_OLD = 19
_SWP_FLAGS = 0x0001 | 0x0002 | 0x0004 | 0x0010 | 0x0020  # NOSIZE|NOMOVE|NOZORDER|NOACTIVATE|FRAMECHANGED


def frame_hwnd(win):
    """HWND of the window's outer frame (the part with the title bar)."""
    try:
        frame = win.wm_frame()
        hwnd = int(str(frame), 16) if frame else 0
    except Exception:
        hwnd = 0
    if hwnd:
        return hwnd
    try:
        import ctypes
        return ctypes.windll.user32.GetParent(win.winfo_id())
    except Exception:
        return 0


DWMWA_BORDER_COLOR = 34
DWMWA_CAPTION_COLOR = 35
DWMWA_TEXT_COLOR = 36
_WINDOWS_11_BUILD = 22000


def colorref(color: str) -> int:
    """'#rrggbb' as the Windows COLORREF (0x00BBGGRR) the DWM caption attributes take."""
    return int(color[5:7], 16) << 16 | int(color[3:5], 16) << 8 | int(color[1:3], 16)


def windows_11() -> bool:
    try:
        return os.name == "nt" and sys.getwindowsversion().build >= _WINDOWS_11_BUILD
    except Exception:
        return False


def set_caption_colors(win, colors) -> bool:
    """Windows 11 only: paint the caption/border/text from the theme's (bg, fg, border) hex colours.
    Silently does nothing on older Windows or other systems (the dark/light mode still applies)."""
    if not colors or not windows_11():
        return False
    try:
        import ctypes
        hwnd = frame_hwnd(win)
        if not hwnd:
            return False
        dwm = ctypes.windll.dwmapi
        ok = True
        for attribute, color in zip((DWMWA_CAPTION_COLOR, DWMWA_TEXT_COLOR, DWMWA_BORDER_COLOR), colors):
            value = ctypes.c_int(colorref(color))
            ok = dwm.DwmSetWindowAttribute(hwnd, attribute, ctypes.byref(value), ctypes.sizeof(value)) == 0 and ok
        return ok
    except Exception:
        return False


def set_title_bar_dark(win, dark=True, colors=None):
    """Dark (or light) caption for a Tk/Toplevel window, plus the theme's caption colours on
    Windows 11 when ``colors`` = (background, text, border) is given. Returns True if applied."""
    if os.name != "nt" or win is None:
        return False
    try:
        import ctypes
        win.update_idletasks()  # make sure the frame exists / is mapped
        hwnd = frame_hwnd(win)
        if not hwnd:
            return False
        value = ctypes.c_int(1 if dark else 0)
        dwm = ctypes.windll.dwmapi
        hr = dwm.DwmSetWindowAttribute(hwnd, DWMWA_USE_IMMERSIVE_DARK_MODE,
                                       ctypes.byref(value), ctypes.sizeof(value))
        if hr != 0:
            hr = dwm.DwmSetWindowAttribute(hwnd, DWMWA_USE_IMMERSIVE_DARK_MODE_OLD,
                                           ctypes.byref(value), ctypes.sizeof(value))
        set_caption_colors(win, colors)
        # Repaint the caption now instead of on the next activation/resize.
        ctypes.windll.user32.SetWindowPos(hwnd, 0, 0, 0, 0, 0, _SWP_FLAGS)
        return hr == 0
    except Exception:
        return False  # older Windows — silently skip


# ---------------------------------------------------------------- drag by background
WM_NCLBUTTONDOWN = 0x00A1
HTCAPTION = 2
# The rule (one whitelist, no per-widget patches). A press may start a window drag when it lands on
#  - a PASSIVE class: frames, label frames, labels, separators, progress bars (labels only while
#    they are not clickable: image, <Button-1> binding or a hand cursor mean "I am a control");
#  - the EMPTY STRIP of a notebook's tab bar (right of the last tab), never a tab itself;
#  - a Canvas that says it is passive: msr_drag_passive = True (a canvas with no click behaviour),
#    or passive_at(x, y) returning True (the clickable charts: only where nothing is clickable).
# Everything else (buttons, entries, combos, lists, trees, text, scrollbars, sashes, menus,
# check/radio buttons, ...) keeps its normal behaviour. A new widget class is NOT draggable until
# it is added here on purpose; tests/test_gui_smoke.py walks every tab and fails on a class that
# is in neither list.
_PLAIN_CLASSES = {"Tk", "Toplevel", "Frame", "TFrame", "Labelframe", "TLabelframe", "Label", "TLabel",
                  "TSeparator", "Progressbar", "TProgressbar"}
INTERACTIVE_CLASSES = {"Button", "TButton", "Checkbutton", "TCheckbutton", "Radiobutton", "TRadiobutton",
                       "Entry", "TEntry", "Combobox", "TCombobox", "Spinbox", "TSpinbox", "Listbox", "Text",
                       "Treeview", "Scrollbar", "TScrollbar", "Scale", "TScale", "Menubutton", "TMenubutton",
                       "Menu", "Panedwindow", "TPanedwindow", "TSizegrip"}


def in_empty_tab_strip(notebook, x: int, y: int) -> bool:
    """True when (x, y) in the notebook's own coordinates is in the tab bar but not on a tab
    (the empty stretch right of the last tab, or the margin left of the first). Tabs on top only."""
    try:
        if not notebook.tabs():
            return False
        selected = notebook.nametowidget(notebook.select())
        if selected.winfo_y() <= 2 or selected.winfo_x() > 4:  # tab bar not on top
            return False
        if not 0 <= y < selected.winfo_y() or not 0 <= x < notebook.winfo_width():
            return False
        try:
            notebook.index(f"@{int(x)},{int(y)}")  # raises when no tab is there
            return False
        except Exception:
            return True
    except Exception:
        return False


def is_drag_background(widget, x: int | None = None, y: int | None = None) -> bool:
    """True for plain background (see the rule above) and false for anything the user interacts
    with. x, y are the press position in the widget's own coordinates (needed for notebooks and
    charts, which are draggable only in their empty parts)."""
    try:
        cls = widget.winfo_class()
        if cls in ("TNotebook", "Notebook"):
            return x is not None and in_empty_tab_strip(widget, x, y)
        if cls == "Canvas":
            probe = getattr(widget, "passive_at", None)
            if callable(probe):
                return x is not None and bool(probe(x, y))
            return bool(getattr(widget, "msr_drag_passive", False)) and not widget.bind("<Button-1>")
        if cls not in _PLAIN_CLASSES:
            return False
        if cls in ("Label", "TLabel"):
            if str(widget.cget("image")):
                return False
            if (widget.bind("<Button-1>") or widget.bind("<ButtonPress-1>") or widget.bind("<Double-Button-1>")
                    or widget.bind("<ButtonRelease-1>")):
                return False
            if str(widget.cget("cursor")) in ("hand1", "hand2", "pointinghand"):
                return False
        return True
    except Exception:
        return False


def caption_lparam(x_root: int, y_root: int) -> int:
    """MAKELONG(x, y) of the screen position, 16 bits each (negative = left/above the origin)."""
    return ((int(y_root) & 0xFFFF) << 16) | (int(x_root) & 0xFFFF)


def _begin_caption_drag(top, x_root: int, y_root: int) -> bool:
    """Ask Windows to move `top` as if its title bar was grabbed. PostMessage, never
    SendMessage: SendMessage runs the move loop inside the Tk callback and Tk timers that
    fire in it re-enter Tcl and crash the process."""
    if os.name != "nt":
        return False
    try:
        import ctypes
        user32 = ctypes.windll.user32
        hwnd = frame_hwnd(top)
        if not hwnd:
            return False
        lparam = caption_lparam(x_root, y_root)
        user32.ReleaseCapture()
        user32.PostMessageW(hwnd, WM_NCLBUTTONDOWN, HTCAPTION, lparam)
        return True
    except Exception:
        return False


def enable_background_drag(top) -> None:
    """Let the window be moved by dragging its plain background (once per window)."""
    if getattr(top, "_msr_drag", False):
        return
    top._msr_drag = True

    def press(event):
        widget = event.widget
        if not is_drag_background(widget, getattr(event, "x", None), getattr(event, "y", None)):
            return None
        try:
            if str(top.state()) == "zoomed" or widget.winfo_toplevel() is not top:
                return None
        except Exception:
            return None
        return "break" if _begin_caption_drag(top, event.x_root, event.y_root) else None

    top._msr_drag_press = press  # type: ignore[attr-defined]  (tests call it without a mouse)
    top.bind("<ButtonPress-1>", press, add="+")


def theme_toplevels(root, dark, seen=None, colors=None):
    """Apply the caption to every titled Toplevel under *root* (dialogs,
    including tkinter.simpledialog ones). Overrideredirect tooltips are skipped."""
    import tkinter as tk
    done = []
    stack = list(root.winfo_children())
    while stack:
        w = stack.pop()
        try:
            stack.extend(w.winfo_children())
            if not isinstance(w, tk.Toplevel):
                continue
            if w.wm_overrideredirect():
                continue
            if seen is not None:
                if str(w) in seen:
                    continue
                seen.add(str(w))
            set_title_bar_dark(w, dark, colors)
            done.append(w)
        except Exception:
            continue
    return done


def watch_new_dialogs(root, dark_getter, interval_ms=60, rounds=40):
    """Theme dialogs that appear in the next ~2.4 s (call right before a
    blocking simpledialog.ask*(); Tk's after() still runs in its wait loop)."""
    seen = set()

    def tick(n):
        try:
            theme_toplevels(root, bool(dark_getter()), seen)
        except Exception:
            pass
        if n > 0:
            root.after(interval_ms, tick, n - 1)

    root.after(interval_ms, tick, rounds)
