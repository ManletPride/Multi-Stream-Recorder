"""Windows title-bar theming shared by the main window and dialogs.

Tk can't colour the native caption, so on Windows 10 20H1+/11 we ask DWM for
the immersive dark title bar (DWMWA_USE_IMMERSIVE_DARK_MODE = 20; 19 on early
Windows 11 builds). Native ``messagebox`` dialogs are OS-owned and follow the
system theme only; they cannot be themed from here.
"""

import os

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


def set_title_bar_dark(win, dark=True):
    """Dark (or light) caption for a Tk/Toplevel window. Returns True if applied."""
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
        # Repaint the caption now instead of on the next activation/resize.
        ctypes.windll.user32.SetWindowPos(hwnd, 0, 0, 0, 0, 0, _SWP_FLAGS)
        return hr == 0
    except Exception:
        return False  # older Windows — silently skip


def theme_toplevels(root, dark, seen=None):
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
            set_title_bar_dark(w, dark)
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
