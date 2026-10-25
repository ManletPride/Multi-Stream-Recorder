"""Small Canvas charts in the app theme: horizontal bars, a donut and monthly columns.

No dependencies. Each chart redraws itself when its canvas is resized or the theme changes,
and shows an exact-number tooltip drawn on the canvas itself (no extra windows) when the
pointer is over a bar, slice or column. The geometry helpers at the top are pure functions.
"""

from __future__ import annotations

import math
import tkinter as tk
from tkinter import ttk

from msr import dialogs

PLATFORM_COLORS = {
    "kick": "#53b518", "twitch": "#9146ff", "youtube": "#e8402f", "tiktok": "#19b8b0",
    "rumble": "#85c742", "fishtank": "#e8a33d", "custom": "#6c8ebf", "other": "#8c8c8c",
}
FALLBACK_COLORS = ("#c75d9b", "#4aa3a3", "#b08a3e", "#7a6fd0", "#5e9e6e", "#c9704a")


# ------------------------------------------------------------------ pure helpers
def color_for(platform: str, index: int = 0) -> str:
    return PLATFORM_COLORS.get(platform.lower()) or FALLBACK_COLORS[index % len(FALLBACK_COLORS)]


def slice_angles(values: list[float]) -> list[tuple[float, float]]:
    """(start, extent) in Tk arc degrees for each value, clockwise from 12 o'clock.
    A full circle is drawn as 359.99 degrees (Tk draws a 360 degree arc as nothing)."""
    total = float(sum(v for v in values if v > 0))
    if total <= 0:
        return []
    out, angle = [], 90.0
    for v in values:
        extent = -360.0 * max(v, 0) / total
        if extent <= -359.99:
            extent = -359.99
        out.append((angle, extent))
        angle += extent
    return out


def bar_widths(values: list[float], max_width: int, min_width: int = 2) -> list[int]:
    """Pixel widths scaled to the largest value; any positive value gets at least min_width."""
    top = max([v for v in values if v > 0] or [0])
    if top <= 0 or max_width <= 0:
        return [0] * len(values)
    return [max(min_width, round(max_width * v / top)) if v > 0 else 0 for v in values]


def ellipsize(text: str, max_chars: int) -> str:
    if max_chars <= 1:
        return text[:max(0, max_chars)]
    return text if len(text) <= max_chars else text[:max_chars - 1] + "\u2026"


def month_axis(months: dict, limit: int = 24) -> list[str]:
    """Consecutive 'YYYY-MM' keys from the first to the last month present (gaps filled
    with empty months), at most the newest `limit`. Unparseable keys are ignored."""
    parsed = []
    for key in months:
        try:
            y, m = key.split("-")
            parsed.append((int(y), int(m)))
        except ValueError:
            continue
    if not parsed:
        return []
    (y, m), (ly, lm) = min(parsed), max(parsed)
    keys = []
    while (y, m) <= (ly, lm):
        keys.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return keys[-limit:]


def label_step(count: int, width: int, label_px: int = 44) -> int:
    """Show every n-th axis label so they don't overlap."""
    if count <= 0:
        return 1
    return max(1, math.ceil(count * label_px / max(width, 1)))


def row_key(row, index: int):
    """The optional click key of a chart row (an extra last element), else None."""
    return row[index] if len(row) > index else None


# ------------------------------------------------------------------ theme
def palette(win: tk.Misc) -> dict:
    dark = dialogs.is_dark(win)
    try:
        fg = str(ttk.Style(win).lookup("TLabel", "foreground")) or ""
    except tk.TclError:
        fg = ""
    if not fg or fg.startswith("SystemWindowText"):
        fg = "#d4d4d4" if dark else "#1e1e1e"
    return {
        "bg": dialogs.frame_background(win), "fg": fg,
        "muted": "#8a97b3" if dark else "#666666",
        "track": "#16294a" if dark else "#e2e2e2",
        "accent": "#4a9eff" if dark else "#0078d4",
        "tip_bg": "#1d3557" if dark else "#fffbe6", "tip_fg": "#ffffff" if dark else "#1e1e1e",
        "tip_edge": "#4a9eff" if dark else "#c0a000",
    }


FONT = ("Segoe UI", 9)
FONT_BIG = ("Segoe UI", 12, "bold")


class ChartCanvas(tk.Canvas):
    """Base: holds data, redraws on resize/theme, and shows tooltips for registered items."""

    empty_text = "Nothing to show yet"

    def __init__(self, parent, height: int = 200, width: int = 400):
        super().__init__(parent, height=height, width=width, highlightthickness=0, bd=0)
        self.data = None
        self._hits: dict[int, str] = {}
        self._keys: dict[int, str] = {}
        self.on_click: Callable[[str], None] | None = None   # called with the clicked item's key
        self.on_double: Callable[[str], None] | None = None
        self._drawn_width = -1
        self._job = None
        self.bind("<Configure>", self._on_configure)
        self.bind("<Motion>", self._motion)
        self.bind("<Leave>", lambda _e: self.hide_tip())
        self.bind("<Button-1>", lambda e: self._clicked(e, self.on_click))
        self.bind("<Double-Button-1>", lambda e: self._clicked(e, self.on_double))
        self.bind("<Destroy>", self._cancel_job)
        self.redraw()

    # -- data / drawing
    def set_data(self, data) -> None:
        self.data = data
        self.redraw()

    def size(self) -> tuple[int, int]:
        w, h = self.winfo_width(), self.winfo_height()
        if w <= 1:
            w = int(self.cget("width"))
        if h <= 1:
            h = int(self.cget("height"))
        return w, h

    def redraw(self) -> None:
        self.delete("all")
        self._hits = {}
        self._keys = {}
        pal = palette(self)
        self.configure(bg=pal["bg"])
        w, h = self.size()
        self._drawn_width = w
        if not self.data:
            self.create_text(w // 2, h // 2, text=self.empty_text, fill=pal["muted"], font=FONT)
            return
        self.draw(w, h, pal)

    def draw(self, w: int, h: int, pal: dict) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    def hit(self, item: int, text: str, key: str | None = None) -> int:
        self._hits[item] = text
        if key is not None:
            self._keys[item] = key
        return item

    def key_at(self, x: int, y: int) -> str | None:
        """The key (a site, channel or month) of the clickable item under the pointer."""
        for item in reversed(self.find_overlapping(x, y, x, y)):
            if item in self._keys:
                return self._keys[item]
        return None

    def passive_at(self, x: int, y: int) -> bool:
        """True where a press does nothing (margins, titles, empty space): the window may be
        dragged from there. Clickable bars, slices and months keep selecting."""
        return self.key_at(x, y) is None

    def _clicked(self, event, handler) -> None:
        key = self.key_at(event.x, event.y)
        if key is not None and handler is not None:
            handler(key)

    def _on_configure(self, event) -> None:
        if event.width != self._drawn_width and self._job is None:
            self._job = self.after(60, self._resized)

    def _resized(self) -> None:
        self._job = None
        try:
            self.redraw()
        except tk.TclError:
            pass

    def _cancel_job(self, _event=None) -> None:
        if self._job is not None:
            try:
                self.after_cancel(self._job)
            except tk.TclError:
                pass
            self._job = None

    # -- tooltips
    def tooltip_at(self, x: int, y: int) -> str | None:
        for item in reversed(self.find_overlapping(x, y, x, y)):
            if item in self._hits:
                return self._hits[item]
        return None

    def _motion(self, event) -> None:
        try:
            self.configure(cursor="hand2" if (self._keys and self.key_at(event.x, event.y) is not None) else "")
        except tk.TclError:
            pass
        text = self.tooltip_at(event.x, event.y)
        if text:
            self.show_tip(event.x, event.y, text)
        else:
            self.hide_tip()

    def show_tip(self, x: int, y: int, text: str) -> None:
        self.hide_tip()
        pal = palette(self)
        w, h = self.size()
        label = self.create_text(0, 0, text=text, anchor="nw", fill=pal["tip_fg"], font=FONT, tags="tip",
                                 width=max(60, w - 24))  # long tips wrap instead of running off the canvas
        x0, y0, x1, y1 = self.bbox(label)
        tw, th = x1 - x0 + 12, y1 - y0 + 8
        tx = x + 14 if x + 14 + tw <= w else max(2, x - 14 - tw)
        ty = y + 14 if y + 14 + th <= h else max(2, y - 14 - th)
        self.coords(label, tx + 6, ty + 4)
        box = self.create_rectangle(tx, ty, tx + tw, ty + th, fill=pal["tip_bg"], outline=pal["tip_edge"], tags="tip")
        self.tag_lower(box, label)
        self.tag_raise("tip")

    def hide_tip(self) -> None:
        self.delete("tip")


class BarChart(ChartCanvas):
    """data: list of (label, value, value_text, tooltip, color)."""

    ROW = 24
    empty_text = "No channels yet"

    def set_data(self, data) -> None:
        rows = len(data or [])
        self.configure(height=max(60, rows * self.ROW + 10))
        super().set_data(data)

    def draw(self, w: int, h: int, pal: dict) -> None:
        label_w = min(190, max(90, w // 3))
        value_w = 78
        left, right = label_w + 10, max(label_w + 20, w - value_w - 8)
        widths = bar_widths([r[1] for r in self.data], right - left)
        for i, row in enumerate(self.data):
            label, _value, value_text, tip, color = row[:5]
            y = 6 + i * self.ROW
            mid = y + self.ROW // 2
            key = row_key(self.data[i], 5)
            self.hit(self.create_rectangle(0, y, w, y + self.ROW, fill="", outline=""), tip, key)  # whole row hovers
            self.create_text(label_w, mid, text=ellipsize(label, max(4, label_w // 7)), anchor="e",
                             fill=pal["fg"], font=FONT)
            self.create_rectangle(left, mid - 7, right, mid + 7, fill=pal["track"], outline="")
            if widths[i]:
                self.hit(self.create_rectangle(left, mid - 7, left + widths[i], mid + 7, fill=color, outline=""), tip, key)
            self.create_text(max(left + widths[i], left) + 6, mid, text=value_text, anchor="w",
                             fill=pal["muted"], font=FONT)


class DonutChart(ChartCanvas):
    """data: list of (label, value, legend_text, tooltip, color). center_text shows in the hole."""

    empty_text = "No recordings yet"

    def __init__(self, parent, height: int = 190, width: int = 380):
        self.center_text = ""
        super().__init__(parent, height=height, width=width)

    def draw(self, w: int, h: int, pal: dict) -> None:
        size = min(h - 12, 180)
        x0, y0 = 10, (h - size) // 2
        box = (x0, y0, x0 + size, y0 + size)
        angles = slice_angles([r[1] for r in self.data])
        for row, (start, extent) in zip(self.data, angles):
            item = self.create_arc(*box, start=start, extent=extent, style="pieslice", fill=row[4],
                                   outline=pal["bg"], width=2)
            self.hit(item, row[3], row_key(row, 5))
        hole = size * 0.30
        cx, cy = x0 + size / 2, y0 + size / 2
        self.create_oval(cx - hole, cy - hole, cx + hole, cy + hole, fill=pal["bg"], outline=pal["bg"])
        if self.center_text:
            self.create_text(cx, cy, text=self.center_text, fill=pal["fg"], font=FONT_BIG)
        lx, ly = x0 + size + 24, max(8, (h - len(self.data) * 22) // 2)
        for i, row in enumerate(self.data[:8]):
            y = ly + i * 22
            sw = self.create_rectangle(lx, y + 3, lx + 12, y + 15, fill=row[4], outline="")
            txt = self.create_text(lx + 20, y + 9, text=ellipsize(row[2], max(8, (w - lx - 24) // 7)),
                                   anchor="w", fill=pal["fg"], font=FONT)
            self.hit(sw, row[3], row_key(row, 5))
            self.hit(txt, row[3], row_key(row, 5))


class ColumnChart(ChartCanvas):
    """data: list of (label, value, tooltip) in display order, e.g. one per month."""

    empty_text = "No dated recordings yet"

    def draw(self, w: int, h: int, pal: dict) -> None:
        n = len(self.data)
        base, top, left = h - 22, 10, 8
        usable = max(10, w - left * 2)
        slot = usable / n
        bar_w = max(2, min(36, slot * 0.7))
        peak = max([r[1] for r in self.data] or [0]) or 1
        step = label_step(n, usable)
        self.create_line(left, base, w - left, base, fill=pal["track"])
        shown = -10 ** 6
        for i, row in enumerate(self.data):
            label, value, tip = row[0], row[1], row[2]
            key = row_key(row, 3)
            x = left + slot * i + (slot - bar_w) / 2
            height = 0 if value <= 0 else max(2, (base - top) * value / peak)
            if value > 0:
                self.hit(self.create_rectangle(x, base - height, x + bar_w, base, fill=pal["accent"], outline=""), tip, key)
            slot_left = left + slot * i  # the whole column slot hovers, not just the (maybe thin) bar
            self.hit(self.create_rectangle(slot_left, top, slot_left + slot, base, fill="", outline=""), tip, key)
            if i % step == 0 or (i == n - 1 and i - shown >= step * 0.6):
                shown = i
                self.create_text(x + bar_w / 2, base + 11, text=label, fill=pal["muted"], font=("Segoe UI", 8))
