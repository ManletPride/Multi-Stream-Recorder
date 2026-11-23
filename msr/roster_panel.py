"""Left-side Channel Roster panel for Multi-Stream Recorder.

Extracted from ``msr/gui.py`` so search, view filtering, grouped Treeview,
cookie badges, add/remove/reorder/sort, context menu, and cookie health footer
live in a dedicated, theme-aware panel class.
"""
from __future__ import annotations

import datetime
import logging
import threading
import time
import tkinter as tk
from tkinter import ttk

from msr import channel_panel as cp
from msr import dialogs as _dialogs
from msr import themes as _themes
from msr import themeui as _themeui
from msr.channel_panel import move_list_items
from msr.config import fishtank_password
from msr.context_menu import bind_context_menu
from msr.presence import combined_state
from msr.tooltip import Tooltip, popup
from msr.util import (
    find_cookies_file,
    get_cookie_status_for_channel,
    validate_channel_name,
    validate_cookies,
)
from msr.wheel import bind_wheel, wheel_steps


class RosterPanel:
    """Channel roster panel (search, All/Enabled/Live, grouped tree, badges, cookies)."""

    CHECK_ON = "\u2611"
    CHECK_OFF = "\u2610"
    BADGE_CANVAS_W = 22
    GROUP_TITLES = {
        cp.GROUP_RECORDING: "Recording",
        cp.GROUP_ENABLED: "Watching",
        cp.GROUP_DISABLED: "Off",
    }

    def __init__(
        self,
        root,
        style,
        *,
        config,
        channels: list[dict],
        win_state: dict,
        state_file: str,
        load_state,
        save_state,
        save_channels,
        get_recorder,
        presence_store,
        sounds,
        on_status_refresh,
        on_open_browser,
        on_open_clips_dir,
        on_select=None,
    ):
        self.root = root
        self.style = style
        self.config = config
        self.channels = channels
        self.state_file = state_file
        self._load_state = load_state
        self._save_state = save_state
        self._save_channels = save_channels
        self._get_recorder = get_recorder
        self.presence_store = presence_store
        self.sounds = sounds
        self._on_status_refresh = on_status_refresh
        self._on_open_browser = on_open_browser
        self._on_open_clips_dir = on_open_clips_dir
        self._on_select_cb = on_select

        self.frame = tk.Frame(root, padx=12, pady=12)
        self.frame.grid(row=0, column=0, sticky="ns", padx=(10, 5), pady=10)

        self.filter_var = tk.StringVar(value=win_state.get("roster_filter", ""))
        self.view_var = tk.StringVar(value=cp.coerce_view(
            win_state.get("roster_view"), bool(win_state.get("roster_enabled_only", False)),
        ))
        self.disabled_collapsed = bool(win_state.get("roster_disabled_collapsed", False))

        self._cookie_info_cache = validate_cookies(find_cookies_file(config))
        self._fishtank_login = self._compute_fishtank_login()
        self._row_group: dict[str, str] = {}
        self._badges: dict[str, str] = {}
        self._roster_sig = None
        self._badge_redraw_after_id = None
        self._filter_debounce_id = None
        self._hover = {"tip": None, "text": None}
        self._cookie_dot_state = ["", "muted"]

        self._build_search()
        self._build_view_toggle()
        self._build_tree()
        self._build_add_button()
        self._build_cookie_footer()
        self._build_menus_and_keys()

        self.populate()
        self._update_search_placeholder()
        self.update_cookie_status()
        self.root.after(300_000, self._periodic_cookie_check)

    # ── Widget construction ─────────────────────────────────────────────────

    def _compute_fishtank_login(self) -> bool:
        return bool(
            str(self.config.get("Fishtank", "email", fallback="") or "").strip()
            and fishtank_password(self.config).strip()
        )

    def refresh_fishtank_login(self) -> None:
        self._fishtank_login = self._compute_fishtank_login()

    def _build_search(self) -> None:
        self.search_frame = tk.Frame(self.frame)
        self.search_frame.pack(fill=tk.X, pady=(0, 6))
        self.filter_entry = tk.Entry(
            self.search_frame, textvariable=self.filter_var,
            font=("Segoe UI", 10), borderwidth=0, relief="flat",
        )
        self.filter_entry.pack(fill=tk.X, ipady=4)
        self.search_placeholder = tk.Label(
            self.search_frame, text="Search channels\u2026",
            font=("Segoe UI", 10), anchor="w", cursor="xterm",
        )
        self.search_placeholder.bind("<Button-1>", lambda _e: self.filter_entry.focus_set())
        self.filter_entry.bind("<FocusIn>", self._update_search_placeholder, add="+")
        self.filter_entry.bind("<FocusOut>", self._update_search_placeholder, add="+")
        self.filter_entry.bind("<Escape>", lambda _e: self.filter_var.set(""))
        self.filter_entry.bind("<Return>", self._search_enter)
        Tooltip(
            self.filter_entry,
            "Filter as you type (name or platform).\n"
            "Enter: add a channel with this name.  Esc: clear.",
        )

    def _build_view_toggle(self) -> None:
        self.view_frame = tk.Frame(self.frame)
        self.view_frame.pack(fill=tk.X, pady=(0, 6))
        self.view_buttons: list[tk.Radiobutton] = []
        for label, val in (("All", cp.VIEW_ALL), ("Enabled", cp.VIEW_ENABLED), ("Live", cp.VIEW_LIVE)):
            btn = tk.Radiobutton(
                self.view_frame, text=label, value=val, variable=self.view_var,
                indicatoron=0, font=("Segoe UI", 9), relief="flat", bd=0,
                padx=10, pady=3, cursor="hand2",
            )
            btn.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 1))
            self.view_buttons.append(btn)

    def _build_tree(self) -> None:
        self.tree_frame = tk.Frame(self.frame)
        self.tree_frame.pack(fill=tk.BOTH, expand=True)

        self.tree = ttk.Treeview(
            self.tree_frame,
            columns=("check", "name", "tag", "_key"),
            displaycolumns=("check", "name", "tag"),
            show="tree", height=10, selectmode="extended", style="Roster.Treeview",
        )
        self.tree.column("#0", width=0, minwidth=0, stretch=False)
        self.tree.column("check", width=26, anchor="center", stretch=False)
        self.tree.column("name", width=200, anchor="w")
        self.tree.column("tag", width=96, anchor="e", stretch=False)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.badge_canvas = tk.Canvas(
            self.tree_frame, width=self.BADGE_CANVAS_W,
            highlightthickness=0, borderwidth=0,
        )
        self.badge_canvas.pack(side=tk.LEFT, fill=tk.Y)
        self.badge_canvas.msr_drag_passive = True

        self.tree.bind("<Configure>", self._schedule_badge_redraw)
        self.tree.bind("<MouseWheel>", self._schedule_badge_redraw)
        self.tree.bind("<Button-4>", self._schedule_badge_redraw)
        self.tree.bind("<Button-5>", self._schedule_badge_redraw)
        bind_wheel(self.badge_canvas, self._badge_wheel)

        orig_yview = self.tree.yview

        def _yview(*args):
            result = orig_yview(*args)
            if args:
                self._schedule_badge_redraw()
            return result

        self.tree.yview = _yview

        self.tree.bind("<Motion>", self._hover_motion, add="+")
        self.tree.bind("<Leave>", self._hover_hide, add="+")
        self.badge_canvas.bind("<Motion>", lambda e: self._hover_motion(e, "badge"), add="+")
        self.badge_canvas.bind("<Leave>", self._hover_hide, add="+")

        self.tree.bind("<ButtonRelease-1>", self._toggle_channel_check)
        self.tree.bind("<Double-ButtonRelease-1>", self._double_click_toggle)

        self.filter_var.trace_add("write", self._on_filter_change)
        self.view_var.trace_add("write", self._on_view_change)

    def _build_add_button(self) -> None:
        self.add_frame = tk.Frame(self.frame)
        self.add_frame.pack(fill=tk.X, pady=(8, 0))
        self.add_btn = tk.Button(
            self.add_frame, text="+  Add channel", command=self.add_channel,
            font=("Segoe UI", 9), cursor="hand2",
        )
        self.add_btn.pack(fill=tk.X, ipady=2)
        Tooltip(
            self.add_btn,
            "Add a Kick / Twitch / YouTube / Rumble / TikTok /\n"
            "Fishtank channel or a custom URL  (Ctrl+N or Insert).\n"
            "Right-click a channel to enable, remove, or reorder.",
        )

    def _build_cookie_footer(self) -> None:
        self.cookie_frame = tk.Frame(self.frame)
        self.cookie_frame.pack(fill=tk.X, pady=(8, 0))
        self.cookie_indicator = tk.Label(self.cookie_frame, text="\u25cf", font=("Segoe UI", 10))
        self.cookie_indicator.pack(side=tk.LEFT, padx=(0, 4))
        self.cookie_label = tk.Label(self.cookie_frame, text="Cookies: checking...", font=("Segoe UI", 8))
        self.cookie_label.pack(side=tk.LEFT)
        Tooltip(self.cookie_frame, self._cookie_summary_tip)

    def _build_menus_and_keys(self) -> None:
        self.sort_menu = tk.Menu(self.root, tearoff=0)
        self.sort_menu.add_command(label="Enabled First", command=lambda: self.sort_channels("enabled"))
        self.sort_menu.add_command(label="By Platform", command=lambda: self.sort_channels("platform"))
        self.sort_menu.add_command(label="By Name (A\u2013Z)", command=lambda: self.sort_channels("name"))

        self.ctx_menu = tk.Menu(self.root, tearoff=0)
        bind_context_menu(self.tree, self.show_context_menu)

        self.tree.bind("<Delete>", lambda _e: self.remove_selected())
        self.tree.bind("<space>", lambda _e: (self._toggle_selected_channels(), "break")[1])
        self.tree.bind("<Return>", lambda _e: (self.add_channel(), "break")[1])
        self.tree.bind("<Insert>", lambda _e: self.add_channel())
        self.root.bind("<Control-n>", lambda _e: self.add_channel())
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

    # ── State persistence & lookup helpers ──────────────────────────────────

    def persist_state(self) -> None:
        try:
            st = self._load_state(self.state_file)
            st["roster_filter"] = self.filter_var.get()
            st["roster_view"] = self.view_var.get()
            st["roster_enabled_only"] = self.view_var.get() == cp.VIEW_ENABLED
            st["roster_disabled_collapsed"] = bool(self.disabled_collapsed)
            self._save_state(self.state_file, st)
        except Exception:
            pass

    def _update_search_placeholder(self, *_) -> None:
        if self.filter_var.get() or self.root.focus_get() is self.filter_entry:
            self.search_placeholder.place_forget()
        else:
            self.search_placeholder.place(in_=self.filter_entry, x=4, rely=0.5, anchor="w")

    def _status_lookup(self) -> dict:
        rec = self._get_recorder()
        if rec and rec.is_running:
            try:
                return rec.status_dict
            except Exception:
                return {}
        return {}

    def _roster_states(self) -> dict:
        sd = self._status_lookup()
        pres = self.presence_store.snapshot()
        now = time.time()
        out = {}
        for ch in self.channels:
            key = ch["name"]
            out[key] = combined_state(
                cp.channel_live_state(sd.get(key)), pres.get(key),
                ch.get("enabled", True), now,
            )
        return out

    def _compute_badges(self) -> dict[str, str]:
        sd = self._status_lookup()
        info = self._cookie_info_cache
        out = {}
        for ch in self.channels:
            key = ch["name"]
            if not ch.get("enabled", True):
                continue
            tip = cp.cookie_badge(
                key, info, sd.get(key),
                get_status=get_cookie_status_for_channel,
                fishtank_login=self._fishtank_login,
            )
            if tip:
                out[key] = tip
        return out

    @staticmethod
    def _channel_name_from_item(item: str | None) -> str | None:
        if item and item.startswith("c:"):
            return item[2:]
        return None

    def _index_for_name(self, name: str) -> int:
        for i, ch in enumerate(self.channels):
            if ch["name"] == name:
                return i
        return -1

    def item_for_name(self, name: str) -> str | None:
        iid = "c:" + name
        try:
            return iid if self.tree.exists(iid) else None
        except tk.TclError:
            return None

    def selected_names(self) -> list[str]:
        names = []
        for item in self.tree.selection():
            name = self._channel_name_from_item(item)
            if name:
                names.append(name)
        return names

    def select_names(self, names: list[str], see_first: bool = True) -> None:
        items = [i for i in (self.item_for_name(n) for n in names) if i]
        if not items:
            return
        self.tree.selection_set(items)
        if see_first:
            self.tree.see(items[0])

    def focus_search(self) -> None:
        self.filter_entry.focus_set()

    # ── Badges & hover tooltip ──────────────────────────────────────────────

    def _redraw_badge_canvas(self) -> None:
        t = _themes.current()
        self.badge_canvas.configure(bg=t["tree_field"])
        self.badge_canvas.delete("all")
        for item in self.tree.get_children():
            key = self._channel_name_from_item(item)
            if not key or key not in self._badges:
                continue
            bbox = self.tree.bbox(item)
            if not bbox:
                continue
            _x, y, _w, h = bbox
            self.badge_canvas.create_text(
                self.BADGE_CANVAS_W // 2, y + h // 2,
                text=cp.BADGE_GLYPH, fill=t["badge"], font=("Segoe UI Symbol", 10),
            )

    def schedule_badge_redraw(self, _event=None) -> None:
        if self._badge_redraw_after_id is not None:
            return

        def _run():
            self._badge_redraw_after_id = None
            self._redraw_badge_canvas()

        self._badge_redraw_after_id = self.badge_canvas.after(16, _run)

    _schedule_badge_redraw = schedule_badge_redraw

    def _badge_wheel(self, event) -> str:
        self.tree.yview_scroll(-wheel_steps(event), "units")
        return "break"

    def _hover_hide(self, _e=None) -> None:
        if self._hover["tip"] is not None:
            try:
                self._hover["tip"].destroy()
            except tk.TclError:
                pass
        self._hover["tip"] = None
        self._hover["text"] = None

    def _hover_text(self, y: int, region_col: str) -> str | None:
        item = self.tree.identify_row(y)
        key = self._channel_name_from_item(item) if item else None
        if not key:
            return None
        if region_col == "badge":
            return self._badges.get(key)
        if region_col == "#3":
            tip = f"{key}"
            if key in self._badges:
                tip += f"\n{self._badges[key]}"
            return tip
        return self._badges.get(key) if region_col == "#2" and key in self._badges else None

    def _hover_motion(self, event, col=None) -> None:
        if col is None:
            col = self.tree.identify_column(event.x)
        text = self._hover_text(event.y, col)
        if text == self._hover["text"]:
            return
        self._hover_hide()
        if not text:
            return
        self._hover["tip"] = popup(self.tree, text, event.x_root + 14, event.y_root + 16, size=9)
        self._hover["text"] = text

    # ── Tree population & live tick ─────────────────────────────────────────

    def _layout_signature(self, groups):
        return (
            tuple((g, tuple(keys)) for g, keys in groups),
            self.disabled_collapsed,
            bool(self.filter_var.get().strip()),
            tuple(ch.get("enabled", True) for ch in self.channels),
        )

    def populate(self, states=None) -> None:
        if states is None:
            states = self._roster_states()
        query = self.filter_var.get()
        view = self.view_var.get()
        groups = cp.group_channels(self.channels, states, query, view)
        self._roster_sig = self._layout_signature(groups)
        enabled_by_key = {ch["name"]: ch.get("enabled", True) for ch in self.channels}

        selected = self.selected_names()
        try:
            top = self.tree.yview()[0]
        except tk.TclError:
            top = 0.0
        self.tree.delete(*self.tree.get_children())
        self._row_group.clear()
        show_headers = sum(1 for _g, keys in groups if keys) > 1 or bool(groups[2][1])
        for gid, keys in groups:
            if not keys:
                continue
            collapsed = (
                gid == cp.GROUP_DISABLED and self.disabled_collapsed and not query.strip()
            )
            if show_headers:
                arrow = ("\u25b8" if collapsed else "\u25be") if gid == cp.GROUP_DISABLED else ""
                self.tree.insert(
                    "", tk.END, iid="h:" + gid,
                    values=(arrow, f"{self.GROUP_TITLES[gid]} ({len(keys)})", "", ""),
                    tags=("header",),
                )
            if collapsed:
                continue
            for key in keys:
                enabled = enabled_by_key.get(key, True)
                display, ptag = cp.channel_display(key)
                iid = "c:" + key
                self.tree.insert(
                    "", tk.END, iid=iid,
                    values=(self.CHECK_ON if enabled else self.CHECK_OFF, display, ptag, key),
                    tags=("enabled" if enabled else "disabled",),
                )
                self._row_group[iid] = gid
        if selected:
            self.select_names(selected, see_first=False)
        try:
            self.tree.yview_moveto(top)
        except tk.TclError:
            pass
        self._badges.clear()
        self._badges.update(self._compute_badges())
        self.schedule_badge_redraw()

    def live_tick(self) -> None:
        try:
            states = self._roster_states()
            groups = cp.group_channels(
                self.channels, states, self.filter_var.get(), self.view_var.get(),
            )
            if self._layout_signature(groups) != self._roster_sig:
                self.populate(states)
                return
            badges = self._compute_badges()
            if badges != self._badges:
                self._badges.clear()
                self._badges.update(badges)
                self.schedule_badge_redraw()
        except tk.TclError:
            pass

    # ── Selection, filtering & toggling ─────────────────────────────────────

    def _on_tree_select(self, _event=None) -> None:
        headers = [i for i in self.tree.selection() if i.startswith("h:")]
        if headers:
            self.tree.selection_remove(*headers)
            return
        self.schedule_badge_redraw()
        if self._on_select_cb is not None:
            names = self.selected_names()
            if names:
                self._on_select_cb(names[0])

    def _toggle_disabled_group(self) -> None:
        self.disabled_collapsed = not self.disabled_collapsed
        self.populate()
        self.persist_state()

    def _set_enabled(self, names: list[str], enabled: bool) -> None:
        changed = False
        for name in names:
            idx = self._index_for_name(name)
            if 0 <= idx < len(self.channels) and self.channels[idx].get("enabled", True) != enabled:
                self.channels[idx]["enabled"] = enabled
                changed = True
        if changed:
            self.populate()
            self._save_channels()

    def _toggle_channel_check(self, event) -> None:
        item = self.tree.identify_row(event.y)
        if not item:
            return
        if item.startswith("h:"):
            if item == "h:" + cp.GROUP_DISABLED:
                self._toggle_disabled_group()
            return
        if self.tree.identify_column(event.x) == "#1":
            name = self._channel_name_from_item(item)
            idx = self._index_for_name(name) if name else -1
            if 0 <= idx < len(self.channels):
                self._set_enabled([name], not self.channels[idx].get("enabled", True))

    def _double_click_toggle(self, event) -> None:
        item = self.tree.identify_row(event.y)
        name = self._channel_name_from_item(item)
        if not name or self.tree.identify_column(event.x) == "#1":
            return
        idx = self._index_for_name(name)
        if 0 <= idx < len(self.channels):
            self._set_enabled([name], not self.channels[idx].get("enabled", True))

    def _apply_roster_filter(self) -> None:
        self._filter_debounce_id = None
        self.populate()

    def _on_filter_change(self, *_) -> None:
        self._update_search_placeholder()
        prev = self._filter_debounce_id
        if prev is not None:
            try:
                self.root.after_cancel(prev)
            except tk.TclError:
                pass
        self._filter_debounce_id = self.root.after(150, self._apply_roster_filter)

    def _on_view_change(self, *_) -> None:
        self.populate()
        self.persist_state()
        self._paint_view_buttons()

    # ── Add / remove / reorder / sort ───────────────────────────────────────

    def _add_channel_from_dialog(self, name: str, platform: str):
        existing_names = [ch["name"] for ch in self.channels]
        is_valid, error_msg = validate_channel_name(name, platform, existing_names)
        if not is_valid:
            return False, error_msg
        ch_name = cp.build_channel_key(name, platform)
        if ch_name in existing_names:
            return False, f"{ch_name} is already in the list."
        self.channels.append({"name": ch_name, "enabled": True})
        if self.filter_var.get():
            self.filter_var.set("")
        if self.view_var.get() == cp.VIEW_LIVE:
            self.view_var.set(cp.VIEW_ALL)
        self.populate()
        self.select_names([ch_name])
        self._save_channels()
        rec = self._get_recorder()
        if rec and rec.is_running:
            rec.start_channel(ch_name)
        elif rec:
            rec.status_dict[ch_name] = {"status": "Initializing", "detail": "", "size": "", "time": ""}
        return True, ""

    def add_channel(self, initial_text: str = "") -> None:
        t = _themes.current()
        cp.open_add_channel_dialog(
            self.root, t, self._add_channel_from_dialog,
            initial_text=initial_text, dark=t.dark,
        )

    def _search_enter(self, _e=None) -> str:
        self.add_channel(self.filter_var.get().strip())
        return "break"

    def remove_selected(self) -> None:
        names = self.selected_names()
        if not names:
            return
        shown = "\n".join(names[:8]) + (f"\n\u2026 and {len(names) - 8} more" if len(names) > 8 else "")
        if not _dialogs.ask_yes_no(
            self.root,
            "Remove channel" + ("s" if len(names) > 1 else ""),
            f"Remove from the list (recordings on disk are kept)?\n\n{shown}\n\n"
            "You can bring the old list back with \u22ef \u2192 Restore previous channel list.",
        ):
            return
        drop = set(names)
        rec = self._get_recorder()
        for ch_name in names:
            if rec:
                rec.remove_channel(ch_name)
        self.channels[:] = [ch for ch in self.channels if ch["name"] not in drop]
        self.populate()
        self._save_channels()

    def _move_channel_step(self, direction: str) -> None:
        names = self.selected_names()
        if not names:
            return
        iid = "c:" + names[0]
        rows = [
            i for i in self.tree.get_children()
            if i.startswith("c:") and self._row_group.get(i) == self._row_group.get(iid)
        ]
        if iid not in rows:
            return
        pos = rows.index(iid) + (-1 if direction == "up" else 1)
        if pos < 0 or pos >= len(rows):
            return
        anchor = self._channel_name_from_item(rows[pos])
        if anchor and cp.move_relative(self.channels, names[0], anchor, after=(direction == "down")):
            self.populate()
            self._save_channels()
            self.select_names([names[0]])

    def _move_selected_channels(self, to: str) -> None:
        names = self.selected_names()
        if not names:
            return
        indices = [self._index_for_name(n) for n in names]
        new_idx = move_list_items(self.channels, indices, to=to)
        if not new_idx:
            return
        self.populate()
        self._save_channels()
        moved = [self.channels[i]["name"] for i in new_idx]
        self.select_names(moved, see_first=(to == "top"))

    @staticmethod
    def _channel_sort_parts(ch: dict) -> tuple[str, str]:
        platform, user = cp.split_channel_key(ch["name"])
        return platform.lower(), user.lower()

    def sort_channels(self, mode: str) -> None:
        if len(self.channels) < 2:
            return
        selected_names = set(self.selected_names())
        if mode == "enabled":
            self.channels.sort(key=lambda ch: not ch.get("enabled", True))
        elif mode == "platform":
            self.channels.sort(key=self._channel_sort_parts)
        elif mode == "name":
            self.channels.sort(key=lambda ch: self._channel_sort_parts(ch)[1])
        self.populate()
        self._save_channels()
        if selected_names:
            self.select_names(list(selected_names))
        logging.info(f"Channel list sorted ({mode})")

    # ── Cookie footer ───────────────────────────────────────────────────────

    def _cookie_summary_tip(self) -> str:
        summary = cp.platform_cookie_summary(
            self._cookie_info_cache,
            get_status=get_cookie_status_for_channel,
            fishtank_login=self._fishtank_login,
        )
        words = {
            "present": "ok", "missing": "no cookie", "expired": "EXPIRED",
            "unknown": "unknown", "login": "email/password login",
        }
        lines = [f"{cp.PLATFORM_LABELS[p]}: {words.get(s, s)}" for p, s in summary.items()]
        return (
            "Per-site cookies (cookies.txt):\n" + "\n".join(lines)
            + f"\n\n{cp.BADGE_GLYPH} on a channel = its site needs attention."
        )

    def _cookie_dot(self, glyph: str, tone: str) -> None:
        self._cookie_dot_state[:] = [glyph, tone]
        self.cookie_indicator.configure(text=glyph, fg=_themes.current()[tone])

    def update_cookie_status(self) -> None:
        cookies_path = find_cookies_file(self.config)
        info = validate_cookies(cookies_path)
        self._cookie_info_cache = info

        base_text = f"{info['total_cookies']} entries, {len(info['domains'])} domains"
        if not cookies_path:
            self._cookie_dot("\u25cb", "muted")
            self.cookie_label.configure(text="No cookies.txt found")
        elif not info["valid"]:
            self._cookie_dot("\u25cf", "error")
            warning = info["warnings"][0] if info["warnings"] else "Invalid format"
            self.cookie_label.configure(text=f"Cookies: {warning}")
        elif info["has_expired_auth"]:
            self._cookie_dot("\u25cf", "warn")
            domains = ", ".join(info["expired_domains"][:2])
            self.cookie_label.configure(text=f"Cookies: auth expired ({domains})")
            self.sounds.play("problem", f"cookies:{domains}")
        else:
            expiry_note = ""
            if info["auth_expiry"]:
                days_left = (info["auth_expiry"] - datetime.datetime.now()).days
                if days_left < 7:
                    expiry_note = f" (auth renew in {days_left}d)"
                    self._cookie_dot("\u25cf", "warn" if days_left < 2 else "ok")
                else:
                    self._cookie_dot("\u25cf", "ok")
            else:
                self._cookie_dot("\u25cf", "ok")
            self.cookie_label.configure(text=f"Cookies: {base_text}{expiry_note}")

        self._badges.clear()
        self._badges.update(self._compute_badges())
        self.schedule_badge_redraw()

    def _periodic_cookie_check(self) -> None:
        self.update_cookie_status()
        self.root.after(300_000, self._periodic_cookie_check)

    # ── Context menu & per-channel actions ──────────────────────────────────

    def _start_selected_channel(self) -> None:
        rec = self._get_recorder()
        if not self.tree.selection() or not rec or not rec.is_running:
            return
        for ch_name in self.selected_names():
            idx = self._index_for_name(ch_name)
            if idx < 0:
                continue
            if not self.channels[idx].get("enabled", True):
                self.channels[idx]["enabled"] = True
                self.populate()
                self._save_channels()
            rec.start_channel(ch_name)

    def _stop_selected_channel(self) -> None:
        rec = self._get_recorder()
        if not self.tree.selection() or not rec or not rec.is_running:
            return
        names = self.selected_names()
        if not names:
            return

        def _bg():
            for ch_name in names:
                rec.stop_channel(ch_name)
            self.root.after(0, self._on_status_refresh)

        threading.Thread(target=_bg, daemon=True, name="stop-roster").start()

    def _check_now_selected(self) -> None:
        rec = self._get_recorder()
        if not self.tree.selection() or not rec or not rec.is_running:
            return
        for ch_name in self.selected_names():
            rec.check_now(ch_name)

    def _open_channel_telegram(self) -> None:
        names = self.selected_names()
        if len(names) != 1:
            _dialogs.show_info(self.root, "Telegram", "Select one channel.")
            return
        name = names[0]
        idx = self._index_for_name(name)
        if idx < 0:
            return
        rec = self.channels[idx]
        shown, plat = cp.channel_display(name)

        def on_save(upload, topic_text):
            from msr.telegram_queue import SettingError, apply_channel_telegram
            try:
                apply_channel_telegram(rec, upload=upload, topic=topic_text)
            except SettingError as exc:
                return str(exc)
            self._save_channels()
            return ""

        cp.open_channel_telegram_dialog(
            self.root, f"{shown} \u00b7 {plat}",
            rec.get("telegram_upload"), rec.get("telegram_topic"), on_save,
        )

    def _copy_channel_name(self) -> None:
        names = self.selected_names()
        if names:
            self.root.clipboard_clear()
            self.root.clipboard_append("\n".join(names))

    def _open_channel_url(self) -> None:
        names = self.selected_names()
        if names:
            self._on_open_browser(names[0])

    def _open_roster_clips_dir(self) -> None:
        if not self.tree.selection():
            logging.info("Open Clips Folder: select a channel in the list first")
            return
        names = self.selected_names()
        if names:
            self._on_open_clips_dir(names[0])

    def _toggle_selected_channels(self) -> None:
        for ch_name in self.selected_names():
            idx = self._index_for_name(ch_name)
            if 0 <= idx < len(self.channels):
                self.channels[idx]["enabled"] = not self.channels[idx].get("enabled", True)
        self.populate()
        self._save_channels()

    def _enable_all_channels(self) -> None:
        for ch in self.channels:
            ch["enabled"] = True
        self.populate()
        self._save_channels()

    def _disable_all_channels(self) -> None:
        for ch in self.channels:
            ch["enabled"] = False
        self.populate()
        self._save_channels()

    def show_context_menu(self, event) -> None:
        item = self.tree.identify_row(event.y)
        if item and item.startswith("h:"):
            return
        if item and item not in self.tree.selection():
            self.tree.selection_set(item)
        names = self.selected_names()

        self.ctx_menu.delete(0, tk.END)
        if names:
            all_on = all(
                self.channels[self._index_for_name(n)].get("enabled", True)
                for n in names if self._index_for_name(n) >= 0
            )
            if all_on:
                self.ctx_menu.add_command(label="Disable", command=lambda: self._set_enabled(names, False))
            else:
                self.ctx_menu.add_command(label="Enable", command=lambda: self._set_enabled(names, True))

        rec = self._get_recorder()
        if rec and rec.is_running and names:
            ch_name = names[0]
            idx = self._index_for_name(ch_name)
            if 0 <= idx < len(self.channels):
                st = rec.status_dict.get(ch_name, {})
                status_lower = st.get("status", "").lower()
                if status_lower in ("stopped",) or ch_name not in rec.status_dict:
                    self.ctx_menu.add_command(label="Start Recording", command=self._start_selected_channel)
                elif any(w in status_lower for w in ("recording", "checking", "offline", "initializing")):
                    self.ctx_menu.add_command(label="Stop Recording", command=self._stop_selected_channel)
                else:
                    self.ctx_menu.add_command(label="Start Recording", command=self._start_selected_channel)
                if status_lower.startswith(("offline", "error")):
                    self.ctx_menu.add_command(label="Check Now", command=self._check_now_selected)

        self.ctx_menu.add_separator()
        self.ctx_menu.add_command(label="Open stream page", command=self._open_channel_url)
        self.ctx_menu.add_command(label="Open Clips Folder", command=self._open_roster_clips_dir)
        self.ctx_menu.add_command(label="Copy Name", command=self._copy_channel_name)
        self.ctx_menu.add_command(label="Telegram\u2026", command=self._open_channel_telegram)
        self.ctx_menu.add_separator()
        self.ctx_menu.add_command(label="Move to Top", command=lambda: self._move_selected_channels("top"))
        self.ctx_menu.add_command(label="Move Up", command=lambda: self._move_channel_step("up"))
        self.ctx_menu.add_command(label="Move Down", command=lambda: self._move_channel_step("down"))
        self.ctx_menu.add_command(label="Move to Bottom", command=lambda: self._move_selected_channels("bottom"))
        self.ctx_menu.add_cascade(label="Sort", menu=self.sort_menu)
        self.ctx_menu.add_separator()
        self.ctx_menu.add_command(label="Enable All", command=self._enable_all_channels)
        self.ctx_menu.add_command(label="Disable All", command=self._disable_all_channels)
        self.ctx_menu.add_command(label="Add channel\u2026", command=self.add_channel)
        self.ctx_menu.add_separator()
        self.ctx_menu.add_command(label="Remove\u2026", command=self.remove_selected)

        try:
            self.ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.ctx_menu.grab_release()

    # ── Theming ─────────────────────────────────────────────────────────────

    def _paint_view_buttons(self, *_) -> None:
        t = _themes.current()
        for btn in self.view_buttons:
            on = btn.cget("value") == self.view_var.get()
            btn.configure(fg=t["accent_fg"] if on else t["btn_fg"])

    def apply_theme(self, t) -> None:
        border_color = t["border"]
        self.style.configure(
            "Roster.Treeview",
            background=t["tree_field"], fieldbackground=t["tree_field"],
            foreground=t["tree_fg"], rowheight=28, font=("Segoe UI", 10),
            borderwidth=0, bordercolor=border_color,
            lightcolor=t["tree_field"], darkcolor=t["tree_field"],
        )
        self.style.map(
            "Roster.Treeview",
            background=[("selected", t["select_bg"])],
            foreground=[("selected", t["select_fg"])],
        )
        self.style.layout("Roster.Treeview", [("Treeview.treearea", {"sticky": "nswe"})])
        self.tree.tag_configure("enabled", foreground=t["tree_fg"])
        self.tree.tag_configure("disabled", foreground=t["check_fg"])
        self.tree.tag_configure("header", foreground=t["offline_fg"], font=("Segoe UI", 9, "bold"))

        for w in (self.frame, self.tree_frame, self.search_frame, self.view_frame,
                  self.add_frame, self.cookie_frame):
            w.configure(bg=t["bg"])
        self.tree_frame.configure(
            bg=t["tree_field"], highlightthickness=1, highlightbackground=border_color,
        )
        self.filter_entry.configure(
            bg=t["entry_bg"], fg=t["entry_fg"], insertbackground=t["fg"],
            disabledforeground=t["disabled_fg"],
            selectbackground=t["select_bg"], selectforeground=t["select_fg"],
            highlightbackground=border_color, highlightcolor=t["accent"],
            highlightthickness=1, relief="flat",
        )
        self.search_placeholder.configure(bg=t["entry_bg"], fg=t["placeholder"])
        for btn in self.view_buttons:
            btn.configure(
                bg=t["btn_bg"], fg=t["btn_fg"], selectcolor=t["accent"],
                activebackground=t["btn_hover"], activeforeground=t["btn_hover_fg"],
                disabledforeground=t["disabled_fg"], highlightthickness=0,
            )
        self._paint_view_buttons()
        self.add_btn.configure(
            bg=t["btn_bg"], fg=t["btn_fg"],
            activebackground=t["btn_hover"], activeforeground=t["btn_hover_fg"],
            disabledforeground=t["disabled_fg"],
            highlightbackground=border_color, relief="flat", bd=1,
        )
        self.cookie_label.configure(bg=t["bg"], fg=t["fg"])
        self.cookie_indicator.configure(bg=t["bg"], fg=t[self._cookie_dot_state[1]])
        self.badge_canvas.configure(bg=t["tree_field"])
        for menu in (self.ctx_menu, self.sort_menu):
            menu.configure(**_themeui.menu_colors(t))
        self.schedule_badge_redraw()
