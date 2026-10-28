"""MSR colour themes: one palette of semantic tokens per theme, no tkinter, no colours anywhere else.

Every colour the app draws comes from the current Theme (``themes.current()``): ttk styles,
Tk widgets, Text tags, charts, dialogs, menus and the Windows title bar. To add a theme,
add ONE entry to ``THEMES`` below (the tests then check that every token is present and that
text and chart colours are readable). The Omarchy-inspired themes are not written by hand: their
published palettes live in ``msr/theme_palettes.py`` and ``from_palette()`` below builds the tokens. A user can also drop ``themes/*.json`` files in the data
folder (see ``load_user_themes``); those are validated token by token and bad files are ignored.

Token groups (a "pair" in the contrast tests is a text colour on its background):
  surfaces   bg panel fg muted hint disabled_fg border accent accent_fg select_bg select_fg
  caption    title_bg title_fg title_border      (Windows 11 caption colours, optional effect)
  inputs     entry_bg entry_fg placeholder listbox_bg listbox_fg
  buttons    btn_bg btn_fg btn_hover btn_hover_fg start_bg start_fg stop_bg stop_fg
  tabs/tree  tab_bg tab_fg tree_bg tree_fg tree_field tree_heading_bg tree_heading_fg
  log        log_bg log_fg log_warn log_error find_bg find_fg
  status     ok warn error info link badge rec_fg completed_fg offline_fg check_fg remux_fg error_fg
  charts     chart_track chart_accent tip_bg tip_fg tip_edge series_1..series_6
             site_kick site_twitch site_youtube site_tiktok site_rumble site_fishtank site_custom site_other
"""
from __future__ import annotations

import json
import os
import re
from typing import Callable, Iterable

DEFAULT_THEME = "PowerShell Blue"
LIGHT_THEME = "Light"          # what the old dark_mode = false migrates to

_HEX = re.compile(r"^#[0-9a-fA-F]{6}$")

SERIES = tuple(f"series_{i}" for i in range(1, 7))
SITE_TOKENS = {"kick": "site_kick", "twitch": "site_twitch", "youtube": "site_youtube", "tiktok": "site_tiktok",
               "rumble": "site_rumble", "fishtank": "site_fishtank", "custom": "site_custom", "other": "site_other"}

COLOR_TOKENS = (
    "bg", "panel", "fg", "muted", "hint", "disabled_fg", "border", "accent", "accent_fg", "select_bg", "select_fg",
    "title_bg", "title_fg", "title_border",
    "entry_bg", "entry_fg", "placeholder", "listbox_bg", "listbox_fg",
    "btn_bg", "btn_fg", "btn_hover", "btn_hover_fg", "start_bg", "start_fg", "stop_bg", "stop_fg",
    "tab_bg", "tab_fg", "tree_bg", "tree_fg", "tree_field", "tree_heading_bg", "tree_heading_fg",
    "log_bg", "log_fg", "log_warn", "log_error", "find_bg", "find_fg",
    "ok", "warn", "error", "info", "link", "badge", "rec_fg", "completed_fg", "offline_fg", "check_fg",
    "remux_fg", "error_fg",
    "chart_track", "chart_accent", "tip_bg", "tip_fg", "tip_edge",
) + SERIES + tuple(SITE_TOKENS.values())

# (text token, background token): must reach WCAG 4.5:1 in every built-in theme.
TEXT_PAIRS = (
    ("fg", "bg"), ("muted", "bg"), ("hint", "bg"), ("fg", "panel"), ("muted", "panel"),
    ("accent_fg", "accent"), ("select_fg", "select_bg"),
    ("title_fg", "title_bg"),
    ("entry_fg", "entry_bg"), ("placeholder", "entry_bg"), ("listbox_fg", "listbox_bg"),
    ("btn_fg", "btn_bg"), ("btn_hover_fg", "btn_hover"), ("start_fg", "start_bg"), ("stop_fg", "stop_bg"),
    ("tab_fg", "tab_bg"), ("tree_fg", "tree_bg"), ("tree_fg", "tree_field"), ("tree_heading_fg", "tree_heading_bg"),
    ("log_fg", "log_bg"), ("log_warn", "log_bg"), ("log_error", "log_bg"), ("find_fg", "find_bg"),
    ("ok", "bg"), ("warn", "bg"), ("error", "bg"), ("info", "bg"), ("link", "bg"),
    ("rec_fg", "tree_field"), ("completed_fg", "tree_field"), ("offline_fg", "tree_field"),
    ("check_fg", "tree_field"), ("remux_fg", "tree_field"), ("error_fg", "tree_field"),
    ("badge", "tree_field"), ("offline_fg", "bg"),
    ("tip_fg", "tip_bg"), ("fg", "entry_bg"), ("ok", "listbox_bg"), ("error", "listbox_bg"),
)
# Chart colours must reach 3:1 (graphics) against the panel they are drawn on.
GRAPHIC_PAIRS = tuple((tok, "bg") for tok in SERIES + tuple(SITE_TOKENS.values()) + ("chart_accent",))


# ---------------------------------------------------------------- colour maths
def is_hex_color(text) -> bool:
    return isinstance(text, str) and bool(_HEX.match(text))


def _rgb(color: str) -> tuple[int, int, int]:
    return int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)


def luminance(color: str) -> float:
    def channel(v: int) -> float:
        v /= 255
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4
    r, g, b = _rgb(color)
    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)


def contrast_ratio(a: str, b: str) -> float:
    la, lb = luminance(a), luminance(b)
    hi, lo = max(la, lb), min(la, lb)
    return (hi + 0.05) / (lo + 0.05)


# ---------------------------------------------------------------- the theme object
class Theme:
    """A named palette. ``theme['bg']`` and ``theme.bg`` both work; ``theme.dark`` is a bool
    (drives the Windows title-bar mode and nothing else)."""

    def __init__(self, name: str, dark: bool, colors: dict[str, str], credit: dict | None = None):
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "dark", bool(dark))
        object.__setattr__(self, "colors", dict(colors))
        object.__setattr__(self, "credit", dict(credit) if credit else None)   # palette source, or None

    def __setattr__(self, key, value):
        raise AttributeError("themes are read-only; make a new Theme")

    def __getitem__(self, key: str) -> str:
        return self.colors[key]

    def __getattr__(self, key: str) -> str:
        try:
            return object.__getattribute__(self, "colors")[key]
        except KeyError:
            raise AttributeError(key) from None

    def get(self, key: str, default=None):
        return self.colors.get(key, default)

    def series(self) -> tuple[str, ...]:
        return tuple(self.colors[k] for k in SERIES)

    def site(self, platform: str, index: int = 0) -> str:
        token = SITE_TOKENS.get((platform or "").lower())
        return self.colors[token] if token else self.series()[index % len(SERIES)]

    def credit_line(self) -> str:
        """One line for the Theme window ("" for the themes MSR drew itself)."""
        c = self.credit
        if not c:
            return ""
        return (f"Palette: {c['upstream']} ({c['upstream_license']}), "
                f"via the Omarchy theme {c['port']} ({c['port_license']}).")

    def __repr__(self) -> str:
        return f"Theme({self.name!r}, dark={self.dark})"


def _make(name: str, dark: bool, credit: dict | None = None, **given: str) -> Theme:
    """Fill the tokens a theme does not spell out from the ones it does."""
    c = dict(given)
    black, white = "#000000", "#ffffff"

    def on(color: str) -> str:  # readable text colour for a background
        return white if contrast_ratio(white, color) >= contrast_ratio(black, color) else black

    c.setdefault("hint", c["muted"])
    c.setdefault("panel", c["entry_bg"])
    c.setdefault("accent_fg", on(c["accent"]))
    c.setdefault("select_bg", c["accent"])
    c.setdefault("select_fg", on(c["select_bg"]))
    c.setdefault("title_fg", on(c["title_bg"]))
    c.setdefault("title_border", c["accent"])
    c.setdefault("placeholder", c["muted"])
    c.setdefault("btn_hover", c["accent"])
    c.setdefault("btn_hover_fg", on(c["btn_hover"]))
    c.setdefault("start_fg", on(c["start_bg"]))
    c.setdefault("stop_fg", on(c["stop_bg"]))
    c.setdefault("tab_bg", c["entry_bg"])
    c.setdefault("tab_fg", c["fg"])
    c.setdefault("tree_bg", c["bg"])
    c.setdefault("tree_fg", c["fg"])
    c.setdefault("tree_field", c["listbox_bg"])
    c.setdefault("tree_heading_bg", c["btn_bg"])
    c.setdefault("tree_heading_fg", c["btn_fg"])
    c.setdefault("find_fg", on(c["find_bg"]))
    c.setdefault("info", c["link"])
    c.setdefault("badge", c["warn"])
    c.setdefault("error_fg", c["error"])
    c.setdefault("chart_track", c["entry_bg"])
    c.setdefault("chart_accent", c["link"])
    c.setdefault("tip_bg", c["entry_bg"])
    c.setdefault("tip_fg", c["fg"])
    c.setdefault("tip_edge", c["accent"])
    return Theme(name, dark, c, credit)



# ---------------------------------------------------------------- palette -> theme (Omarchy-inspired themes)
def _srgb_to_lab(color: str) -> tuple[float, float, float]:
    def lin(v: int) -> float:
        v /= 255
        return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4
    r, g, b = (lin(v) for v in _rgb(color))
    x = (0.4124564 * r + 0.3575761 * g + 0.1804375 * b) / 0.95047
    y = 0.2126729 * r + 0.7151522 * g + 0.0721750 * b
    z = (0.0193339 * r + 0.1191920 * g + 0.9503041 * b) / 1.08883

    def f(t: float) -> float:
        return t ** (1 / 3) if t > 0.008856 else 7.787 * t + 16 / 116
    fx, fy, fz = f(x), f(y), f(z)
    return 116 * fy - 16, 500 * (fx - fy), 200 * (fy - fz)


def color_distance(a: str, b: str) -> float:
    """CIE76 colour difference (about 2.3 is just noticeable; 20+ is clearly a different colour)."""
    la, lb = _srgb_to_lab(a), _srgb_to_lab(b)
    return sum((x - y) ** 2 for x, y in zip(la, lb)) ** 0.5


def hue_of(color: str) -> float:
    """Hue in degrees (0-360) of a #rrggbb colour."""
    import colorsys
    r, g, b = (v / 255 for v in _rgb(color))
    return colorsys.rgb_to_hls(r, g, b)[0] * 360


def _hex(rgb: Iterable[float]) -> str:
    return "#%02x%02x%02x" % tuple(max(0, min(255, round(v))) for v in rgb)


def _mix(a: str, b: str, t: float) -> str:
    """a moved a fraction t of the way to b."""
    return _hex(x + (y - x) * t for x, y in zip(_rgb(a), _rgb(b)))


def _nudge(color: str, backgrounds: Iterable[str], ratio: float, toward: str) -> str:
    """The colour itself if it reaches `ratio` on every background, else the first 4 percent step toward
    `toward` (white on dark themes, black on light ones) that does: hue kept, only readability changed."""
    backgrounds = list(backgrounds)
    color = color.lower()
    for step in range(26):
        candidate = _mix(color, toward, 0.04 * step)
        if min(contrast_ratio(candidate, b) for b in backgrounds) >= ratio:
            return candidate
    return toward


# Palette colours tried, in this order, for the six chart series; a colour too close to one already taken
# is skipped (several palettes reuse one colour for cyan/orange/accent or have no orange at all).
SERIES_ORDER = ("magenta", "cyan", "yellow", "blue", "green", "orange", "red", "accent")
SERIES_DISTANCES = (20.0, 16.0, 13.0)   # CIE76 between any two series colours: tried in turn until six fit
SERIES_MIN_DISTANCE = SERIES_DISTANCES[-1]   # the floor every palette theme must still meet
STATUS_MIN_DISTANCE = 40.0     # Start vs Stop and ok vs error must differ at least this much
GREEN_HUES = (60.0, 180.0)     # hue band for "green" (Start button, ok colour)


def from_palette(spec: dict) -> Theme:
    """Build a Theme from a published palette (see msr/theme_palettes.py).

    Readability fixes, the only liberties taken with the published colours: the text colours, status colours
    (red/green/yellow/blue/cyan/orange) and series colours are nudged toward white (dark theme) or black
    (light theme) until they reach the same 4.5:1 / 3:1 contrast every built-in theme has; muted text, the
    border and the chart track are derived from fg/bg (a palette's own 'comment' colour is deliberately
    faint). Surfaces come from the palette's lighter/darker tints, else from bg blended toward fg."""
    name, dark = spec["name"], bool(spec["dark"])
    raw = {k: spec[k].lower() for k in ("bg", "fg", "selection", "accent", "red", "green", "yellow", "blue",
                                        "magenta", "cyan")}
    raw.update({k: v.lower() for k, v in (spec.get("fix") or {}).items()})
    white, black = "#ffffff", "#000000"
    toward = white if dark else black
    bg, fg = raw["bg"], raw["fg"]
    entry_bg = (spec.get("lighter") or (_mix(bg, fg, 0.08) if dark else _mix(bg, white, 0.55))).lower()
    darker = (spec.get("darker") or (_mix(bg, black, 0.3) if dark else _mix(bg, black, 0.06))).lower()
    listbox_bg = _mix(bg, entry_bg, 0.5)
    surfaces = [bg, entry_bg, listbox_bg]
    readable = surfaces + [darker]

    def text(color: str, backgrounds=surfaces, ratio: float = 4.5) -> str:
        return _nudge(color, backgrounds, ratio, toward)

    fg_ = text(fg)
    muted = text(_mix(fg, bg, 0.38))
    offline = text(_mix(fg, bg, 0.45))
    red, green, yellow, blue = (text(raw[k], readable) for k in ("red", "green", "yellow", "blue"))
    cyan = text(raw["cyan"], readable)
    orange = text(spec.get("orange") or raw["yellow"], readable)
    accent = raw["accent"]
    selection = raw["selection"]
    if contrast_ratio(fg_, selection) >= 4.5:
        btn_bg = selection
    else:
        btn_bg = _mix(entry_bg, fg, 0.12) if dark else _mix(entry_bg, black, 0.07)
    log_bg = darker if dark else _mix(bg, white, 0.5)

    pool = {"magenta": raw["magenta"], "cyan": cyan, "yellow": yellow, "blue": blue, "green": green,
            "orange": orange if spec.get("orange") else None, "red": red, "accent": accent}
    candidates = [_nudge(pool[key], [bg], 3.0, toward) for key in SERIES_ORDER if pool.get(key)]
    series: list[str] = []
    for distance in SERIES_DISTANCES:       # a pastel palette (Vesper) needs the looser distances to find six
        series = []
        for color in candidates:
            if all(color_distance(color, other) >= distance for other in series):
                series.append(color)
            if len(series) == len(SERIES):
                break
        if len(series) == len(SERIES):
            break
    if len(series) != len(SERIES):
        raise ValueError(f"palette {name!r} has fewer than six distinguishable colours for the charts")
    sites = _SITES_DARK if dark else _SITES_LIGHT
    colors = dict(
        bg=bg, fg=fg_, muted=muted, hint=muted, disabled_fg=_mix(fg, bg, 0.62), border=_mix(bg, fg, 0.22),
        accent=accent, title_bg=btn_bg if dark else darker, title_border=accent,
        entry_bg=entry_bg, entry_fg=fg_, placeholder=muted, listbox_bg=listbox_bg, listbox_fg=fg_,
        btn_bg=btn_bg, btn_fg=fg_, btn_hover=accent, start_bg=green, stop_bg=red,
        tab_bg=entry_bg, tab_fg=fg_, tree_heading_bg=btn_bg,
        log_bg=log_bg, log_fg=text(_mix(green, fg, 0.3), [log_bg]), log_warn=yellow, log_error=red,
        find_bg=_mix(yellow, bg, 0.65),
        ok=green, warn=yellow, error=red, link=blue, badge=orange,
        rec_fg=green, completed_fg=cyan, offline_fg=offline, check_fg=muted, remux_fg=yellow,
        chart_track=_mix(bg, fg, 0.14), chart_accent=blue, tip_bg=btn_bg, tip_fg=fg_, tip_edge=accent,
        **sites)
    for i, color in enumerate(series):
        colors[f"series_{i + 1}"] = color
    return _make(name, dark, spec.get("credit"), **colors)


# Chart colours for dark and light surfaces (the sites keep their brand hue in every theme).
_SITES_DARK = dict(site_kick="#53b518", site_twitch="#9146ff", site_youtube="#e8402f", site_tiktok="#19b8b0",
                   site_rumble="#85c742", site_fishtank="#e8a33d", site_custom="#6c8ebf", site_other="#8c8c8c",
                   series_1="#c75d9b", series_2="#4aa3a3", series_3="#b08a3e", series_4="#9a8fe8",
                   series_5="#5e9e6e", series_6="#c9704a")
_SITES_LIGHT = dict(site_kick="#3d7f0e", site_twitch="#7a2cf0", site_youtube="#d03423", site_tiktok="#0f7f7a",
                    site_rumble="#52801f", site_fishtank="#a86a00", site_custom="#496c9f", site_other="#6f6f6f",
                    series_1="#b0447f", series_2="#2f7f7f", series_3="#8a6a1f", series_4="#5b4fc0",
                    series_5="#3f7f55", series_6="#b0552d")
_SITES_HC = dict(site_kick="#7dff3a", site_twitch="#b88cff", site_youtube="#ff6a5a", site_tiktok="#3df0e6",
                 site_rumble="#b5ff6a", site_fishtank="#ffc04d", site_custom="#9fc4ff", site_other="#d0d0d0",
                 series_1="#ff7ac8", series_2="#6ff0f0", series_3="#e8c860", series_4="#b0a8ff",
                 series_5="#8fe8a0", series_6="#ffa070")

THEMES: dict[str, Theme] = {}


def _add(theme: Theme) -> None:
    THEMES[theme.name] = theme


_add(_make(
    "PowerShell Blue", True, bg="#0a1628", fg="#d4d4d4", muted="#9fb4d4", hint="#8fa3bf", disabled_fg="#6a7a9a",
    border="#1a3a5c", accent="#1a5fb4", accent_fg="#ffffff", select_bg="#1a5fb4", select_fg="#ffffff",
    title_bg="#1d4a8a", title_fg="#ffffff", title_border="#2f6feb",
    entry_bg="#112240", entry_fg="#d4d4d4", placeholder="#8fa3bf", listbox_bg="#0d1f3c", listbox_fg="#e6edf3",
    btn_bg="#132d5e", btn_fg="#d4d4d4", btn_hover="#1a5fb4", btn_hover_fg="#ffffff",
    start_bg="#1b7a2b", stop_bg="#b71c1c",
    tab_bg="#112240", tab_fg="#d4d4d4", tree_heading_bg="#132d5e",
    log_bg="#081422", log_fg="#b5cea8", log_warn="#e5c07b", log_error="#ff7b7b", find_bg="#6b5900",
    ok="#8fd18f", warn="#e2c15a", error="#f44747", link="#4fc3f7", badge="#ffb300",
    rec_fg="#4ec94e", completed_fg="#4ec9b0", offline_fg="#8a9bbd", check_fg="#93a3c2", remux_fg="#dcdcaa",
    chart_track="#16294a", chart_accent="#4a9eff", tip_bg="#1d3557", tip_fg="#ffffff", tip_edge="#4a9eff",
    **_SITES_DARK))

_add(_make(
    "Neutral Dark", True, bg="#1f1f1f", fg="#d6d6d6", muted="#a9b4c2", hint="#9aa2ad", disabled_fg="#7c7c7c",
    border="#3c3c3c", accent="#2b76c4", accent_fg="#ffffff", select_bg="#3b78c2", select_fg="#ffffff",
    title_bg="#3a3d41", title_fg="#ffffff", title_border="#5a5a5a",
    entry_bg="#2a2a2a", entry_fg="#d6d6d6", placeholder="#9a9a9a", listbox_bg="#252526", listbox_fg="#e8e8e8",
    btn_bg="#333333", btn_fg="#d6d6d6", btn_hover="#2b76c4", btn_hover_fg="#ffffff",
    start_bg="#1b7a2b", stop_bg="#b71c1c",
    log_bg="#181818", log_fg="#c3d9b0", log_warn="#e5c07b", log_error="#ff7b7b", find_bg="#6b5900",
    ok="#8fd18f", warn="#e2c15a", error="#ff6060", link="#4fc3f7", badge="#ffb300",
    rec_fg="#4ec94e", completed_fg="#4ec9b0", offline_fg="#9a9a9a", check_fg="#a0a0a0", remux_fg="#dcdcaa",
    chart_track="#333333", chart_accent="#4a9eff", tip_bg="#333333", tip_fg="#ffffff", tip_edge="#4a9eff",
    **_SITES_DARK))

_add(_make(
    "Light", False, bg="#f3f3f3", fg="#1f1f1f", muted="#44546a", hint="#5d6875", disabled_fg="#8a8f96",
    border="#c2c7cf", accent="#0a64c8", accent_fg="#ffffff", select_bg="#0a64c8", select_fg="#ffffff",
    title_bg="#d3dae4", title_fg="#111111", title_border="#9aa4b2",
    entry_bg="#ffffff", entry_fg="#1f1f1f", placeholder="#5d6875", listbox_bg="#fafafa", listbox_fg="#202020",
    btn_bg="#e3e5e8", btn_fg="#1f1f1f", btn_hover="#0a64c8", btn_hover_fg="#ffffff",
    start_bg="#1b7a2b", stop_bg="#b71c1c",
    tab_bg="#e3e5e8", tab_fg="#1f1f1f", tree_bg="#f3f3f3", tree_heading_bg="#e3e5e8",
    log_bg="#fbfbfb", log_fg="#24572e", log_warn="#8a5a00", log_error="#b71c1c", find_bg="#ffe58a",
    ok="#1a7f37", warn="#8f5f00", error="#b71c1c", link="#0a64c8", badge="#8f5f00",
    rec_fg="#116329", completed_fg="#1f6f45", offline_fg="#5b6470", check_fg="#5d6875", remux_fg="#7a5600",
    chart_track="#e2e2e2", chart_accent="#0a64c8", tip_bg="#fffbe6", tip_fg="#1f1f1f", tip_edge="#a07c00",
    **_SITES_LIGHT))

_add(_make(
    "High Contrast", True, bg="#000000", fg="#ffffff", muted="#bfe0ff", hint="#d9d9d9", disabled_fg="#a6a6a6",
    border="#bfbfbf", accent="#ffd400", accent_fg="#000000", select_bg="#ffd400", select_fg="#000000",
    title_bg="#ffd400", title_fg="#000000", title_border="#ffd400",
    entry_bg="#0d0d0d", entry_fg="#ffffff", placeholder="#d0d0d0", listbox_bg="#080808", listbox_fg="#ffffff",
    btn_bg="#1c1c1c", btn_fg="#ffffff", btn_hover="#ffd400", btn_hover_fg="#000000",
    start_bg="#006400", stop_bg="#b00000",
    log_bg="#050505", log_fg="#7dff9a", log_warn="#ffd400", log_error="#ff8080", find_bg="#ffd400",
    ok="#7dff7d", warn="#ffd400", error="#ff6b6b", link="#6fd3ff", badge="#ffd400",
    rec_fg="#7dff7d", completed_fg="#6ff0e0", offline_fg="#d0d0d0", check_fg="#d9d9d9", remux_fg="#ffe680",
    chart_track="#333333", chart_accent="#ffd400", tip_bg="#000000", tip_fg="#ffffff", tip_edge="#ffd400",
    **_SITES_HC))

_add(_make(
    "Midnight Purple", True, bg="#120d1f", fg="#e0dcee", muted="#b4acd0", hint="#a79fc4", disabled_fg="#6f6890",
    border="#2f2550", accent="#7c4dff", accent_fg="#ffffff", select_bg="#7c4dff", select_fg="#ffffff",
    title_bg="#3b2a73", title_fg="#ffffff", title_border="#7c4dff",
    entry_bg="#1c1530", entry_fg="#e0dcee", placeholder="#a79fc4", listbox_bg="#171026", listbox_fg="#ece8f8",
    btn_bg="#2a1f4d", btn_fg="#e0dcee", btn_hover="#7c4dff", btn_hover_fg="#ffffff",
    start_bg="#1b7a2b", stop_bg="#b71c1c",
    log_bg="#0e0a19", log_fg="#c5e1a5", log_warn="#e5c07b", log_error="#ff8a8a", find_bg="#6b5900",
    ok="#8fd18f", warn="#e2c15a", error="#ff6b7a", link="#82b1ff", badge="#ffb300",
    rec_fg="#69db7c", completed_fg="#4ec9b0", offline_fg="#9a92bd", check_fg="#a79fc4", remux_fg="#dcdcaa",
    chart_track="#241a3f", chart_accent="#9f7bff", tip_bg="#2a1f4d", tip_fg="#ffffff", tip_edge="#9f7bff",
    **_SITES_DARK))

_add(_make(
    "Paper", False, bg="#f6f1e7", fg="#2b2620", muted="#5c5143", hint="#6b5f50", disabled_fg="#9a8f7d",
    border="#cfc4b0", accent="#8a4b08", accent_fg="#ffffff", select_bg="#8a4b08", select_fg="#ffffff",
    title_bg="#e2d6bf", title_fg="#2b2620", title_border="#b9a98a",
    entry_bg="#fffdf8", entry_fg="#2b2620", placeholder="#6b5f50", listbox_bg="#fffbf2", listbox_fg="#2b2620",
    btn_bg="#e8dfcf", btn_fg="#2b2620", btn_hover="#8a4b08", btn_hover_fg="#ffffff",
    start_bg="#1b7a2b", stop_bg="#b71c1c",
    tab_bg="#e8dfcf", tab_fg="#2b2620", tree_bg="#f6f1e7", tree_heading_bg="#e8dfcf",
    log_bg="#fffaf0", log_fg="#3c5a2c", log_warn="#7a4f00", log_error="#a61b1b", find_bg="#ffe08a",
    ok="#2d6a2d", warn="#8a5a00", error="#a61b1b", link="#0b5aa8", badge="#8a5a00",
    rec_fg="#1d5f2a", completed_fg="#1f6048", offline_fg="#6b5f50", check_fg="#6b5f50", remux_fg="#755000",
    chart_track="#e6dccb", chart_accent="#8a4b08", tip_bg="#fffbe6", tip_fg="#2b2620", tip_edge="#a07c00",
    **_SITES_LIGHT))

from msr import theme_palettes  # noqa: E402  (data only)

# Omarchy-inspired themes: dark ones first, then light, after the six hand-made themes. PowerShell Blue stays first.
for _spec in sorted(theme_palettes.PALETTES, key=lambda sp: not sp["dark"]):
    _add(from_palette(_spec))
del _spec

THEME_NAMES = tuple(THEMES)
GROUP_BUILTIN = tuple(n for n in THEME_NAMES if THEMES[n].credit is None)       # hand-made, MSR's own
GROUP_DARK = tuple(n for n in THEME_NAMES if THEMES[n].credit and THEMES[n].dark)
GROUP_LIGHT = tuple(n for n in THEME_NAMES if THEMES[n].credit and not THEMES[n].dark)

# ---------------------------------------------------------------- current theme
_current: Theme = THEMES[DEFAULT_THEME]
_listeners: list[Callable[[Theme], None]] = []


def current() -> Theme:
    return _current


def find(name: str | None) -> Theme | None:
    """Case-insensitive lookup among the built-in and the loaded user themes."""
    wanted = (name or "").strip().lower()
    for key, theme in THEMES.items():
        if key.lower() == wanted:
            return theme
    return None


def set_current(theme: Theme | str) -> Theme:
    """Make a theme the live one and tell the listeners (apply code registers via on_change)."""
    global _current
    found = theme if isinstance(theme, Theme) else (find(theme) or THEMES[DEFAULT_THEME])
    _current = found
    for listener in list(_listeners):
        try:
            listener(found)
        except Exception:
            pass
    return found


def on_change(listener: Callable[[Theme], None]) -> Callable[[Theme], None]:
    _listeners.append(listener)
    return listener


def resolve_saved(theme_name: str | None, legacy_dark: bool | None = None) -> Theme:
    """The theme to start with. A saved theme name wins; otherwise the old dark_mode switch
    migrates (true -> PowerShell Blue, false -> Light); nothing saved -> PowerShell Blue."""
    named = find(theme_name) if theme_name else None
    if named is not None:
        return named
    if legacy_dark is False:
        return THEMES[LIGHT_THEME]
    return THEMES[DEFAULT_THEME]


def audit(theme: Theme) -> tuple[float, float]:
    """(lowest text contrast, lowest chart-colour contrast) of a theme, for the Theme window and the tests."""
    text = min(contrast_ratio(theme[a], theme[b]) for a, b in TEXT_PAIRS)
    graphic = min(contrast_ratio(theme[a], theme[b]) for a, b in GRAPHIC_PAIRS)
    return text, graphic


def caption_colors(theme: Theme) -> tuple[str, str, str]:
    """(background, text, border) for the Windows 11 caption."""
    return theme["title_bg"], theme["title_fg"], theme["title_border"]


def parse_bool(value, default: bool | None = None) -> bool | None:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower() if value is not None else ""
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default


# ---------------------------------------------------------------- user themes (themes/*.json in the data folder)
MAX_USER_THEMES = 20
MAX_FILE_BYTES = 20_000


def validate_user_theme(data) -> tuple[Theme | None, str]:
    """(theme, "") for a valid definition, else (None, why). Format:
    {"name": "My theme", "base": "PowerShell Blue", "dark": true, "colors": {"bg": "#101010", ...}}
    Only known tokens with #rrggbb values are accepted; anything else rejects the file."""
    if not isinstance(data, dict):
        return None, "not an object"
    name = data.get("name")
    if not isinstance(name, str) or not name.strip() or len(name.strip()) > 40 or not name.strip().isprintable():
        return None, "missing or bad name"
    name = name.strip()
    if name.lower() in {k.lower() for k in THEME_NAMES}:
        return None, "the name is already used by a built-in theme"
    base = find(str(data.get("base") or DEFAULT_THEME))
    if base is None or base.name not in THEME_NAMES:
        return None, "unknown base theme"
    colors = data.get("colors", {})
    if not isinstance(colors, dict):
        return None, "colors must be an object"
    merged = dict(base.colors)
    for token, value in colors.items():
        if token not in COLOR_TOKENS:
            return None, f"unknown colour name {str(token)[:30]!r}"
        if not is_hex_color(value):
            return None, f"{token} must look like #rrggbb"
        merged[token] = value.lower()
    dark = parse_bool(data.get("dark"), base.dark)
    return Theme(name, bool(dark), merged), ""


def load_user_themes(folder: str) -> tuple[list[Theme], list[str]]:
    """Read ``*.json`` in a folder (missing folder = none). Returns (themes, problems); a bad file is
    skipped and reported, never raised. At most MAX_USER_THEMES, files over 20 KB are ignored."""
    themes: list[Theme] = []
    problems: list[str] = []
    try:
        names = sorted(n for n in os.listdir(folder) if n.lower().endswith(".json"))
    except OSError:
        return themes, problems
    seen: set[str] = set()
    for fname in names[:MAX_USER_THEMES]:
        path = os.path.join(folder, fname)
        try:
            if os.path.islink(path) or os.path.getsize(path) > MAX_FILE_BYTES:
                problems.append(f"{fname}: skipped (link or too large)")
                continue
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError) as exc:
            problems.append(f"{fname}: {exc.__class__.__name__}")
            continue
        theme, why = validate_user_theme(data)
        if theme is None:
            problems.append(f"{fname}: {why}")
        elif theme.name.lower() in seen:
            problems.append(f"{fname}: duplicate theme name")
        else:
            seen.add(theme.name.lower())
            themes.append(theme)
    return themes, problems


def register_user_themes(themes: Iterable[Theme]) -> None:
    """Add loaded user themes to the list (replacing earlier user themes, never a built-in)."""
    for key in [k for k in THEMES if k not in THEME_NAMES]:
        del THEMES[key]
    for theme in themes:
        if theme.name not in THEME_NAMES:
            THEMES[theme.name] = theme
