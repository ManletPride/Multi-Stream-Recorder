"""Which mouse event opens a right-click menu.

On X11, Tk's ``tk_popup`` from ``<Button-3>`` leaves the button release for the
menu. A quick click then invokes whatever item is under the pointer (the
channel list's Enable, for example). Opening on ``<ButtonRelease-3>`` means
that release already happened, so the menu stays up until an item is chosen,
the pointer clicks elsewhere, or Esc — Tk's own ``tk_popup`` behaviour.

Windows and macOS keep ``<Button-3>``. The handler is unchanged: it still
selects the row under ``event.y`` and calls ``tk_popup``.
"""
from __future__ import annotations


def _windowingsystem(widget) -> str:
    try:
        return str(widget.tk.call("tk", "windowingsystem"))
    except tk.TclError:
        return ""


def context_sequence(windowingsystem: str) -> str:
    """``<ButtonRelease-3>`` on X11, ``<Button-3>`` everywhere else."""
    if windowingsystem == "x11":
        return "<ButtonRelease-3>"
    return "<Button-3>"


def bind_context_menu(widget, handler) -> str:
    """Bind ``handler`` to the right-click sequence for this display.

    Returns the sequence that was bound.
    """
    sequence = context_sequence(_windowingsystem(widget))
    widget.bind(sequence, handler)
    return sequence
