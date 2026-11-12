"""The one override-redirect tooltip.

Compositors can resize an override-redirect window after it maps. Omarchy's
floating-workspace mode (``workspace-modes.lua``) matches every new non-modal
window and forces ``float``, a size of about ``min(1100, 64% of the monitor)``
by ``min(760, 68% of the monitor)``, and a centered move. That rule hits Tk's
tooltip, so the themed label sits in the corner of a huge ``entry_bg`` box
(measured 1100x760 on Hyprland 0.56.2). Building the window withdrawn and
setting ``-type tooltip`` does not stop the rule. The fix is a ``<Configure>``
handler that puts the requested ``WxH+X+Y`` back. It only does that a few
times per tip, so a compositor that keeps resizing cannot spin the handler.

The window is still created withdrawn, with the theme background already set,
so it is not mapped first at Tk's default size and light-grey ``#d9d9d9``.

Every tip goes through :func:`popup`. Do not add another
``wm_overrideredirect(True)`` window. Do not call ``update()`` here — this
runs from enter/motion callbacks, and ``update()`` re-enters the Tk loop.
"""
from __future__ import annotations

import sys
import tkinter as tk

from msr import themes as _themes


def ui_font(size: int):
    """Tooltip face. Segoe UI on Windows; the Tk default face everywhere else.

    The rest of the window still names Segoe UI directly. There is no shared
    UI-font helper, and that face is not installed on Linux, so a tip that
    asks for it measures against a fallback and looks wrong.
    """
    if sys.platform == "win32":
        return ("Segoe UI", int(size))
    import tkinter.font as tkfont
    family = tkfont.nametofont("TkDefaultFont").cget("family")
    return (family, int(size))


def clamp_tooltip_origin(x, y, width, height, screen_w, screen_h):
    """Keep a ``width`` x ``height`` tip on the screen. Pure: no Tk."""
    x, y = int(x), int(y)
    width, height = max(1, int(width)), max(1, int(height))
    if screen_w and width < int(screen_w) and x + width > int(screen_w):
        x = int(screen_w) - width
    if screen_h and height < int(screen_h) and y + height > int(screen_h):
        y = int(screen_h) - height
    if x < 0:
        x = 0
    if y < 0:
        y = 0
    return x, y


# Omarchy applies its floating rule once on map. A few corrections cover a
# compositor that applies it again. After that we stop: setting geometry from
# <Configure> can itself generate <Configure>, and an endless fight would pin
# the UI thread.
RESIZE_CORRECTIONS = 4


def _hold_requested_size(tw: tk.Toplevel, width: int, height: int, x: int, y: int) -> None:
    """Snap the tip back if something else changes its size (a compositor rule)."""
    wanted = f"{width}x{height}+{x}+{y}"
    state = {"left": RESIZE_CORRECTIONS, "applying": False}

    def on_configure(event):
        if event.widget is not tw or state["applying"] or state["left"] <= 0:
            return
        try:
            got_w, got_h = int(event.width), int(event.height)
        except (TypeError, ValueError, tk.TclError):
            return
        if got_w == width and got_h == height:
            return
        state["left"] -= 1
        state["applying"] = True
        try:
            tw.wm_geometry(wanted)
        except tk.TclError:
            pass
        finally:
            state["applying"] = False

    tw.bind("<Configure>", on_configure, add="+")


def _mark_x11_tooltip(win: tk.Toplevel) -> None:
    """Tell the window manager this is a tooltip. X11 only; ignored if unsupported."""
    try:
        if win.tk.call("tk", "windowingsystem") != "x11":
            return
    except tk.TclError:
        return
    try:
        win.wm_attributes("-type", "tooltip")
    except tk.TclError:
        pass


def popup(parent, text, x, y, size=8):
    """Show one tip at root coordinates ``(x, y)``. Returns the Toplevel, or None.

    The window is still withdrawn, and already ``WxH+X+Y``, when it is mapped.
    """
    text = "" if text is None else str(text)
    if not text:
        return None
    t = _themes.current()
    bg, fg = t["entry_bg"], t["fg"]
    tw = tk.Toplevel(parent)
    tw.withdraw()
    tw.configure(background=bg)
    tw.wm_overrideredirect(True)
    _mark_x11_tooltip(tw)
    tk.Label(
        tw, text=text, justify=tk.LEFT, background=bg, foreground=fg,
        relief="solid", borderwidth=1, font=ui_font(size), padx=6, pady=4,
    ).pack()
    tw.update_idletasks()
    width, height = tw.winfo_reqwidth(), tw.winfo_reqheight()
    x, y = clamp_tooltip_origin(x, y, width, height, tw.winfo_screenwidth(), tw.winfo_screenheight())
    tw.wm_geometry(f"{width}x{height}+{x}+{y}")
    # Flush the size while withdrawn. Deiconify alone maps the window first and
    # applies geometry afterwards, which is the grey flash.
    tw.update_idletasks()
    # Bound before map, so the compositor's resize (which arrives as
    # <Configure> just after map) is caught. The initial size is already the
    # requested one, so that event does not spend a correction.
    _hold_requested_size(tw, width, height, x, y)
    tw.deiconify()
    return tw


class Tooltip:
    """Tip that follows a widget: below it on enter, gone on leave.

    ``text`` is a string or a zero-argument callable (read each time it shows).
    """

    def __init__(self, widget, text, size=8):
        self.widget = widget
        self.text = text
        self.size = size
        self.tip = None
        widget.bind("<Enter>", self._show, add="+")
        widget.bind("<Leave>", self._hide, add="+")

    def _show(self, _event=None):
        if self.tip is not None:
            return
        try:
            text = self.text() if callable(self.text) else self.text
        except tk.TclError:
            return
        if not text:
            return
        try:
            x = self.widget.winfo_rootx() + 20
            y = self.widget.winfo_rooty() + self.widget.winfo_height() + 4
        except tk.TclError:
            return
        self.tip = popup(self.widget, text, x, y, size=self.size)

    def _hide(self, _event=None):
        tip, self.tip = self.tip, None
        if tip is not None:
            try:
                tip.destroy()
            except tk.TclError:
                pass
