"""Mouse-wheel notches for Windows, macOS and X11.

X11 Tk 8.6 sends ``<Button-4>`` (up) and ``<Button-5>`` (down) and leaves
``delta`` at 0. Windows and macOS send ``<MouseWheel>`` with ``delta`` in
multiples of 120. Button-4/5 carry the same ``event.state``, so Alt/Ctrl/Shift
checks keep working.
"""
from __future__ import annotations

SEQUENCES = ("<MouseWheel>", "<Button-4>", "<Button-5>")


def wheel_delta(event) -> int:
    """Windows-style delta. Button-4 is +120, Button-5 is -120, otherwise ``event.delta``.

    Passing this to code that divides by 120 keeps the Windows math, including a
    high-resolution wheel that is not a whole number of notches.
    """
    num = getattr(event, "num", None)
    if num == 4:
        return 120
    if num == 5:
        return -120
    try:
        return int(getattr(event, "delta", 0) or 0)
    except (TypeError, ValueError):
        return 0


def wheel_steps(event) -> int:
    """Signed notches, positive = up.

    Button-4 is +1 and Button-5 is -1. A MouseWheel event is ``int(delta / 120)``
    (120 -> 1, 240 -> 2, -240 -> -2), the same truncation the scroll sites used.
    """
    num = getattr(event, "num", None)
    if num == 4:
        return 1
    if num == 5:
        return -1
    delta = wheel_delta(event)
    if delta == 0:
        return 0
    return int(delta / 120)


def bind_wheel(widget, handler, all: bool = False) -> None:
    """Bind ``handler`` to ``<MouseWheel>``, ``<Button-4>`` and ``<Button-5>``.

    ``all=True`` uses ``bind_all`` and replaces any previous all-binding for
    those sequences (the Options tab turns this on while the pointer is inside
    it, so the wheel works over the entries). A widget bind is added, so it
    does not remove other bindings on that widget.
    """
    bind = widget.bind_all if all else widget.bind
    for seq in SEQUENCES:
        if all:
            bind(seq, handler)
        else:
            bind(seq, handler, add="+")


def unbind_wheel(widget, all: bool = False) -> None:
    """Remove the three wheel sequences. ``all=True`` is ``unbind_all``."""
    if all:
        for seq in SEQUENCES:
            widget.unbind_all(seq)
        return
    for seq in SEQUENCES:
        widget.unbind(seq)


def widget_is_inside(widget, ancestor) -> bool:
    """True when ``widget`` is ``ancestor`` or a descendant of it."""
    w = widget
    while w is not None:
        if w is ancestor:
            return True
        w = getattr(w, "master", None)
    return False
