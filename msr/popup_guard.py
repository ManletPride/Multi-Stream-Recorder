"""Put override-redirect popups back after a compositor resizes them.

Omarchy's floating-workspace rule matches every new non-modal window, including
Tk menus and combobox popdowns, and forces a large floating size (about
1100x760) plus a centered move. ``<Configure>`` then reports that size.
``winfo_rootx`` is already the compositor's position, so the posted coordinates
have to be remembered.

The same cap as the tooltip: a few corrections, then stop, so a resize fight
cannot pin the UI thread. The geometry is put back after ``<Configure>``
returns; doing it inside the handler restores the size and leaves the
compositor's position. Windows is left alone (no extra bindings).
"""
from __future__ import annotations

import tkinter as tk

# A few corrections cover the rule being applied more than once.
RESIZE_CORRECTIONS = 4
# Tk's own frame can be a few pixels off the requested size (combobox popdown).
# The compositor jump is hundreds of pixels.
_SLOP = 40


def _is_x11(widget) -> bool:
    try:
        return widget.tk.call("tk", "windowingsystem") == "x11"
    except tk.TclError:
        return False


def _parse_xy(geom) -> tuple[int, int] | None:
    try:
        _, _, place = str(geom).partition("+")
        x, y = place.split("+", 1)
        return int(x), int(y)
    except (ValueError, AttributeError):
        return None


def _defer_geometry(win, spec: str, state: dict) -> None:
    """Apply ``spec`` once this ``<Configure>`` has returned."""
    if state.get("pending"):
        return
    state["pending"] = spec
    state["applying"] = True

    def fix(win=win, state=state):
        spec_now = state.pop("pending", None)
        try:
            if spec_now:
                _set_geometry(win, spec_now)
        except tk.TclError:
            pass
        finally:
            state["applying"] = False

    try:
        win.winfo_toplevel().after_idle(fix)
    except tk.TclError:
        state["applying"] = False
        state.pop("pending", None)


def _set_geometry(win, spec: str) -> None:
    setter = getattr(win, "wm_geometry", None)
    if setter is not None:
        setter(spec)
    else:
        win.tk.call("wm", "geometry", win, spec)


def _forced(got_w: int, got_h: int, req_w: int, req_h: int) -> bool:
    return abs(got_w - req_w) > _SLOP or abs(got_h - req_h) > _SLOP


def keep_popup_geometry(win, get_xy=None, *, reset: bool = False, x11_only: bool = True) -> None:
    """Re-apply this window's real size if a compositor inflates it.

    ``get_xy`` returns the ``(x, y)`` to put back (the point the popup was
    posted at). When it is omitted, the last size that still matched the
    window's own request is used, which is how a cascade menu keeps the
    position Tk chose. No-op off X11 when ``x11_only`` is set.
    """
    if x11_only and not _is_x11(win):
        return
    state = getattr(win, "_msr_popup_state", None)
    if state is None:
        state = {"left": RESIZE_CORRECTIONS, "applying": False, "good": None, "get_xy": get_xy}
        win._msr_popup_state = state

        def on_configure(event, win=win):
            _on_configure(win, event)

        def on_map(event, win=win):
            if getattr(event, "widget", None) is not win:
                return
            st = getattr(win, "_msr_popup_state", None)
            if st is None or st["applying"]:
                return
            # A new posting. The geometry here is still the one Tk chose;
            # the compositor's move arrives as a later Configure.
            st["left"] = RESIZE_CORRECTIONS
            st["accept_place"] = True
            _remember_place(win, st)

        win.bind("<Configure>", on_configure, add="+")
        win.bind("<Map>", on_map, add="+")
    if get_xy is not None:
        state["get_xy"] = get_xy
    if reset:
        state["left"] = RESIZE_CORRECTIONS
        state["good"] = None
    if get_xy is not None:
        try:
            xy = get_xy()
        except tk.TclError:
            xy = None
        if xy is not None:
            # Size is filled in from the first honest Configure (or from req).
            prev = state["good"]
            state["good"] = (None if prev is None else prev[0], None if prev is None else prev[1],
                             int(xy[0]), int(xy[1]))


def _remember_place(win, state) -> None:
    """Store the current geometry when it is still the size Tk asked for."""
    try:
        got_w, got_h = int(win.winfo_width()), int(win.winfo_height())
        req_w, req_h = int(win.winfo_reqwidth()), int(win.winfo_reqheight())
        xy = _parse_xy(win.winfo_geometry())
    except (TypeError, ValueError, tk.TclError):
        return
    if xy is None or req_w <= 1 or req_h <= 1 or _forced(got_w, got_h, req_w, req_h):
        return
    state["good"] = (got_w, got_h, xy[0], xy[1])


def _place_jumped(saved, xy) -> bool:
    if saved is None or xy is None or saved[2] is None:
        return False
    return abs(saved[2] - xy[0]) > _SLOP or abs(saved[3] - xy[1]) > _SLOP


def _on_configure(win, event) -> None:
    if getattr(event, "widget", None) is not win:
        return
    state = getattr(win, "_msr_popup_state", None)
    if state is None or state["applying"] or state["left"] <= 0:
        return
    try:
        got_w, got_h = int(event.width), int(event.height)
        req_w, req_h = int(win.winfo_reqwidth()), int(win.winfo_reqheight())
    except (TypeError, ValueError, tk.TclError):
        return
    if req_w <= 1 or req_h <= 1:
        return
    xy_now = _parse_xy(win.winfo_geometry())
    if not _forced(got_w, got_h, req_w, req_h):
        # Tk's own size. Remember it, unless this is only the compositor's
        # move of a place we already stored (a cascade has no posted point).
        if xy_now is not None and (
            state.get("accept_place") or not _place_jumped(state.get("good"), xy_now)
        ):
            state["good"] = (got_w, got_h, xy_now[0], xy_now[1])
            state["accept_place"] = False
            return
        if not _place_jumped(state.get("good"), xy_now):
            return
    good = state.get("good")
    posted = None
    getter = state.get("get_xy")
    if getter is not None:
        try:
            posted = getter()
        except tk.TclError:
            posted = None
    if posted is not None:
        x, y = int(posted[0]), int(posted[1])
    elif good is not None and good[2] is not None:
        x, y = good[2], good[3]
    elif xy_now is not None:
        x, y = xy_now
    else:
        return
    if good is not None and good[0]:
        width, height = good[0], good[1]
    else:
        width, height = req_w, req_h
    # A later Configure at the compositor's origin must not replace this place.
    state["accept_place"] = False
    state["left"] -= 1
    # Defer the correction. Setting geometry from inside <Configure> restores
    # the size but, on X11, leaves the compositor's position in place.
    _defer_geometry(win, f"{width}x{height}+{x}+{y}", state)


def popup_menu(menu, x, y) -> None:
    """``tk_popup`` that remembers where the menu was asked to open.

    On X11 a later ``<Configure>`` puts that origin and the menu's real size
    back. On Windows this is only ``tk_popup``.
    """
    x, y = int(x), int(y)
    keep_popup_geometry(menu, lambda: (x, y), reset=True, x11_only=True)
    menu.tk_popup(x, y)


def guard_menu(menu) -> None:
    """Watch a menu Tk posts itself (a cascade, or a Menubutton's menu)."""
    keep_popup_geometry(menu, x11_only=True)


def guard_popdown(combo) -> None:
    """Watch a ttk combobox drop-down. It is an override-redirect toplevel too."""
    if not _is_x11(combo):
        return
    try:
        pop = str(combo.tk.call("ttk::combobox::PopdownWindow", combo._w))
    except tk.TclError:
        return
    state = _pop_states.get(pop)
    if state is None:
        state = {"left": RESIZE_CORRECTIONS, "applying": False, "good": None,
                 "tk": combo.tk, "widget": combo}
        _pop_states[pop] = state
        cb = combo.register(_pop_configure)
        # Leading + appends; replacing ttk's own bindings would break the list.
        combo.tk.call("bind", pop, "<Configure>", f"+{cb} %W %w %h")
    else:
        state["left"] = RESIZE_CORRECTIONS


def _pop_configure(path, width, height) -> None:
    path = str(path)
    state = _pop_states.get(path)
    if state is None or state["applying"] or state["left"] <= 0:
        return
    tk_ = state["tk"]
    try:
        got_w, got_h = int(width), int(height)
        req_w = int(tk_.call("winfo", "reqwidth", path))
        req_h = int(tk_.call("winfo", "reqheight", path))
        geom = str(tk_.call("winfo", "geometry", path))
    except (TypeError, ValueError, tk.TclError):
        return
    if req_w <= 1 or req_h <= 1:
        return
    xy_now = _parse_xy(geom)
    if not _forced(got_w, got_h, req_w, req_h):
        if xy_now is not None and not _place_jumped(state.get("good"), xy_now):
            state["good"] = (got_w, got_h, xy_now[0], xy_now[1])
            return
        if not _place_jumped(state.get("good"), xy_now):
            return
        # Size is still Tk's; only the origin moved. Fall through and put it back.
    good = state.get("good")
    if good is not None and good[2] is not None:
        width_s, height_s, x, y = good
    elif xy_now is not None:
        width_s, height_s, x, y = req_w, req_h, xy_now[0], xy_now[1]
    else:
        return
    state["left"] -= 1
    widget = state.get("widget")
    if widget is None:
        return
    # Same deferral as menus: a geometry change inside <Configure> does not
    # keep the position.
    holder = widget

    def fix(path=path, spec=f"{width_s}x{height_s}+{x}+{y}", state=state):
        try:
            state["tk"].call("wm", "geometry", path, spec)
        except tk.TclError:
            pass
        finally:
            state["applying"] = False

    state["applying"] = True
    try:
        holder.winfo_toplevel().after_idle(fix)
    except tk.TclError:
        state["applying"] = False


_pop_states: dict = {}
_combobox_wrapped = False


def _press_combo(event) -> None:
    try:
        guard_popdown(event.widget)
    except tk.TclError:
        pass


def install_combobox_guard(widget) -> None:
    """Arm drop-downs created after the theme is applied. No-op off X11."""
    global _combobox_wrapped
    if _combobox_wrapped or not _is_x11(widget):
        _combobox_wrapped = True
        return
    from tkinter import ttk
    original = ttk.Combobox.__init__

    def init(self, *args, **kwargs):
        original(self, *args, **kwargs)
        self.bind("<ButtonPress-1>", _press_combo, add="+")

    ttk.Combobox.__init__ = init
    _combobox_wrapped = True
