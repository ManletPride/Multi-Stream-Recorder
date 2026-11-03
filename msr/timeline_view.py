"""Clip Editor timeline zoom / pan / wheel mapping, without Tk (mirrors Leapcut's marks bar)."""

from __future__ import annotations

import sys

MIN_SPAN = 2.0          # seconds: the most the timeline zooms in
ZOOM_STEP = 1.5         # + / - keys
WHEEL_ZOOM = 1.4        # one Alt + wheel notch
PAN_FRACTION = 0.15     # one plain / Shift wheel notch, of the visible span
FOLLOW_EDGE = 0.02      # page when the playhead gets this close to an edge
SEEK_STEPS = (0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0)

SHIFT_BIT = 0x1
CONTROL_BIT = 0x4
WINDOWS_ALT_BIT = 0x20000   # Tk on Windows (0x8 is NumLock there)
X11_ALT_BIT = 0x8


def is_alt(state, platform=None) -> bool:
    windows = (platform or sys.platform).startswith("win")
    return bool(int(state) & (WINDOWS_ALT_BIT if windows else X11_ALT_BIT))


def is_ctrl(state) -> bool:
    return bool(int(state) & CONTROL_BIT)


def is_shift(state) -> bool:
    return bool(int(state) & SHIFT_BIT)


def wheel_action(state, delta, platform=None):
    """Alt = zoom (up = in, at the pointer), Ctrl = seek (up = back), else pan (up = earlier).
    Returns (kind, notches); notches is signed, up positive."""
    steps = max(-8.0, min(8.0, delta / 120.0))
    if is_alt(state, platform):
        return "zoom", steps
    if is_ctrl(state):
        return "seek", steps
    return "pan", steps


def seek_step(span: float) -> float:
    """A round step about 1/50 of the visible span (whole 2 h file: 2 min; zoomed to 10 s: 0.2 s)."""
    want = max(span, 0.0) / 50.0
    for s in SEEK_STEPS:
        if s >= want:
            return s
    return SEEK_STEPS[-1]


class View:
    def __init__(self) -> None:
        self.start = 0.0
        self.end: float | None = None   # None = the whole file (it may still grow)

    def span(self, length: float) -> tuple[float, float]:
        if self.end is None or length <= 0:
            return 0.0, max(length, 1e-6)
        end = min(self.end, length)
        return min(self.start, max(0.0, end - MIN_SPAN)), end

    def zoomed(self) -> bool:
        return self.end is not None

    def factor(self, length: float) -> float:
        a, b = self.span(length)
        return max(1.0, length / max(1e-6, b - a)) if length > 0 else 1.0

    def x(self, t: float, width: float, length: float) -> float:
        a, b = self.span(length)
        return (t - a) / max(1e-6, b - a) * width

    def t(self, x: float, width: float, length: float) -> float:
        a, b = self.span(length)
        return min(max(0.0, a + x / max(1.0, width) * (b - a)), max(0.0, length))

    def zoom(self, factor: float, at: float, length: float) -> None:
        """factor > 1 zooms in; the time `at` keeps its place on screen. Zooming out to the whole file resets."""
        if length <= 0:
            return
        a, b = self.span(length)
        new = max(MIN_SPAN, (b - a) / max(factor, 1e-6))
        if new >= length:
            self.reset()
            return
        share = (at - a) / max(1e-6, b - a)
        self.start = min(max(0.0, at - share * new), length - new)
        self.end = self.start + new

    def pan(self, seconds: float, length: float) -> None:
        if self.end is None:
            return
        a, b = self.span(length)
        span = b - a
        self.start = min(max(0.0, a + seconds), max(0.0, length - span))
        self.end = self.start + span

    def follow(self, t: float, length: float) -> bool:
        """Page so t stays visible (a sixth in from the left, like Leapcut). True if the view moved."""
        if self.end is None:
            return False
        a, b = self.span(length)
        span = b - a
        if a + span * FOLLOW_EDGE <= t <= b - span * FOLLOW_EDGE or (b >= length and t >= a):
            return False
        self.start = min(max(0.0, t - span / 6), max(0.0, length - span))
        self.end = self.start + span
        return True

    def reset(self) -> None:
        self.start, self.end = 0.0, None
