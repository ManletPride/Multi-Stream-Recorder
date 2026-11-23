"""Bottom control bar, health badge, and status/update footer for Multi-Stream Recorder.

Extracted from ``msr/gui.py`` so Start/Stop buttons, Theme picker, Polling and
Clip Length selectors, Check Now, Updates badge, HealthController host, More
menu, status bar, and update banner live in a dedicated, theme-aware widget.
"""
from __future__ import annotations

import logging
import tkinter as tk
from tkinter import ttk

from msr import __version__
from msr import deps as _deps
from msr import depsupdate as _depsupdate
from msr import dialogs as _dialogs
from msr import health as _health
from msr import health_ui as _health_ui
from msr import theme_dialog as _theme_dialog
from msr import themes as _themes
from msr import themeui as _themeui
from msr.deps import HAS_STREAMLINK, HAS_YTDLP, STREAMLINK_VERSION, YTDLP_VERSION
from msr.status import _free_gb as _health_free_gb
from msr.tooltip import Tooltip
from msr.util import open_local_path


class BottomBar:
    """Bottom action bar + status bar + update banner."""

    POLL_PRESETS = {
        "Relaxed (5 min)": 5.0,
        "Normal (3 min)": 3.0,
        "Fast (1 min)": 1.0,
    }
    CUSTOM_POLL_LABEL = "Custom\u2026"
    POLL_MIN_MINUTES = 0.5
    POLL_MAX_MINUTES = 120.0

    CLIP_PRESETS = {
        "15 sec": 15,
        "30 sec": 30,
        "1 min": 60,
        "2 min": 120,
        "5 min": 300,
    }
    CUSTOM_CLIP_LABEL = "Custom\u2026"
    CLIP_MIN_SECONDS = 5
    CLIP_MAX_SECONDS = 1800

    def __init__(
        self,
        root,
        *,
        config,
        win_state: dict,
        state_file: str,
        library_data_dir: str,
        load_state,
        get_recorder,
        on_start,
        on_stop,
        on_apply_theme,
        reload_user_themes,
        user_themes_dir: str,
        get_user_theme_problems,
        on_reflect_option,
        on_about,
        on_open_updates,
        on_open_library,
        on_restore_channels,
        on_restore_settings,
    ):
        self.root = root
        self.config = config
        self.state_file = state_file
        self.library_data_dir = library_data_dir
        self._load_state = load_state
        self._get_recorder = get_recorder
        self._on_apply_theme = on_apply_theme
        self._reload_user_themes = reload_user_themes
        self._user_themes_dir = user_themes_dir
        self._get_user_theme_problems = get_user_theme_problems
        self._on_reflect_option = on_reflect_option
        self._on_open_updates = on_open_updates

        self.frame = tk.Frame(root)
        self.frame.grid(row=1, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))

        self.btn_frame = tk.Frame(self.frame)
        self.btn_frame.pack(side=tk.LEFT, padx=10)

        t = _themes.current()
        self.start_button = tk.Button(
            self.btn_frame, text=" Start Recording ", command=on_start,
            bg=t["start_bg"], fg=t["start_fg"],
            font=("Segoe UI", 11, "bold"), width=18, height=2,
        )
        self.start_button.pack(side=tk.LEFT, padx=(0, 12))

        self.stop_button = tk.Button(
            self.btn_frame, text=" Stop Recording ", command=on_stop,
            bg=t["stop_bg"], fg=t["stop_fg"],
            font=("Segoe UI", 11, "bold"), width=18, height=2,
            state=tk.DISABLED,
        )
        self.stop_button.pack(side=tk.LEFT)

        self.toggle_frame = tk.Frame(self.frame)
        self.toggle_frame.pack(side=tk.RIGHT, padx=10)

        self.theme_button = ttk.Button(
            self.toggle_frame, text="Theme...", command=self.open_theme_dialog,
        )
        self.theme_button.pack(side=tk.RIGHT, padx=8)

        self._build_poll_selector()
        self._build_clip_selector()

        self.check_now_btn = ttk.Button(
            self.toggle_frame, text="Check Now", command=self._check_all_now, width=10,
        )
        self.check_now_btn.pack(side=tk.RIGHT, padx=(0, 6))
        Tooltip(
            self.check_now_btn,
            "Skip the wait \u2014 immediately check all\nenabled channels for live streams.\n"
            "(Right-click a single channel for a\nper-channel Check Now.)",
        )

        self.about_btn = ttk.Button(self.toggle_frame, text="About", command=on_about, width=6)
        self.about_btn.pack(side=tk.RIGHT, padx=(0, 4))

        self.updates_btn = ttk.Button(
            self.toggle_frame, text="Updates\u2026", command=on_open_updates, width=10,
        )
        self.updates_btn.pack(side=tk.RIGHT, padx=(0, 4))
        Tooltip(
            self.updates_btn,
            "Versions of MSR, yt-dlp, streamlink, ffmpeg and Python packages;\n"
            "check and install updates. A dot means updates are available.",
        )
        self.set_updates_badge(_depsupdate.remembered_updates(win_state))

        self._build_health_controller()

        self.more_btn = ttk.Menubutton(self.toggle_frame, text="\u22ef", width=3)
        self.more_menu = tk.Menu(self.more_btn, tearoff=0)
        self.more_menu.add_command(label="Library summary\u2026", command=on_open_library)
        self.more_menu.add_separator()
        self.more_menu.add_command(label="Restore previous channel list\u2026", command=on_restore_channels)
        self.more_menu.add_command(label="Restore previous settings (config.ini)\u2026", command=on_restore_settings)
        self.more_btn.configure(menu=self.more_menu)
        self.more_btn.pack(side=tk.RIGHT, padx=(0, 4))

        self._build_status_bar()

    # ── Sub-sections ────────────────────────────────────────────────────────

    @staticmethod
    def _custom_poll_display(minutes: float) -> str:
        return f"Custom ({minutes:g} min)"

    @staticmethod
    def _custom_clip_display(seconds: int) -> str:
        return f"Custom ({seconds}s)"

    def _build_poll_selector(self) -> None:
        self.poll_label = tk.Label(self.toggle_frame, text="Polling:", font=("Segoe UI", 9))
        current_poll = self.config.getfloat("Timeouts", "poll_interval_minutes", fallback=3.0)
        poll_default = next(
            (n for n, v in self.POLL_PRESETS.items() if abs(v - current_poll) < 0.1),
            self._custom_poll_display(current_poll),
        )
        self.poll_var = tk.StringVar(value=poll_default)
        self._last_poll_display = poll_default
        self.poll_combo = ttk.Combobox(
            self.toggle_frame, textvariable=self.poll_var,
            values=list(self.POLL_PRESETS.keys()) + [self.CUSTOM_POLL_LABEL],
            state="readonly", width=14,
        )
        self.poll_combo.pack(side=tk.RIGHT, padx=(0, 6))
        self.poll_combo.bind("<<ComboboxSelected>>", self._on_poll_change)
        self.poll_label.pack(side=tk.RIGHT, padx=(0, 2))

    def _build_clip_selector(self) -> None:
        self.clip_label = tk.Label(self.toggle_frame, text="Clip Length:", font=("Segoe UI", 9))
        current_clip_len = self.config.getint("Clipping", "clip_length_seconds", fallback=30)
        clip_default = next(
            (n for n, v in self.CLIP_PRESETS.items() if v == current_clip_len),
            self._custom_clip_display(current_clip_len),
        )
        self.clip_var = tk.StringVar(value=clip_default)
        self._last_clip_display = clip_default
        self.clip_combo = ttk.Combobox(
            self.toggle_frame, textvariable=self.clip_var,
            values=list(self.CLIP_PRESETS.keys()) + [self.CUSTOM_CLIP_LABEL],
            state="readonly", width=12,
        )
        self.clip_combo.pack(side=tk.RIGHT, padx=(0, 6))
        self.clip_combo.bind("<<ComboboxSelected>>", self._on_clip_length_change)
        self.clip_label.pack(side=tk.RIGHT, padx=(0, 2))
        Tooltip(
            self.clip_combo,
            "Length of the clip cut when you use\n'Clip Now' on a recording channel\n"
            "(button above the status list, or\nright-click the channel).",
        )

    def _build_health_controller(self) -> None:
        def _health_twitch():
            from msr import twitch_eventsub as _te
            return _health.twitch_issue(
                _te.load_token(self.library_data_dir),
                self.config.getboolean("Twitch", "push_enabled", fallback=False),
                _te.validate, _te.TwitchAuthError,
                fresh=_te.TokenStore(self.library_data_dir).get_fresh,
            )

        self.health_ctl = _health_ui.HealthController(
            self.frame, config=self.config,
            get_status=lambda: dict(getattr(self._get_recorder(), "status_dict", None) or {}),
            get_folder=lambda: self.config.get("Paths", "streams_dir", fallback="") or "",
            free_gb=_health_free_gb,
            get_versions=lambda: _health.check_tools(
                self.config.get("Advanced", "ffmpeg_path", fallback="ffmpeg") or "ffmpeg",
            ),
            get_state=lambda: self._load_state(self.state_file),
            twitch_check=_health_twitch,
            push_failed=lambda: bool(
                getattr(getattr(self._get_recorder(), "twitch_push", None), "_auth_failed", False)
            ),
            open_updates=self._on_open_updates,
            logger=logging.getLogger("msr.health"),
            tooltip=Tooltip,
        )
        self.health_ctl.label.pack(side=tk.LEFT, padx=(6, 0))

    def _build_status_bar(self) -> None:
        dep_parts = []
        if HAS_YTDLP:
            dep_parts.append(f"yt-dlp {YTDLP_VERSION}")
        if HAS_STREAMLINK:
            dep_parts.append(f"streamlink {STREAMLINK_VERSION}")
        if _deps.HAS_FFMPEG:
            dep_parts.append("ffmpeg")
        dep_str = " | ".join(dep_parts) if dep_parts else "No recording tools found"

        self.status_bar = tk.Label(
            self.root,
            text=f"  v{__version__}  \u2014  {dep_str}  \u2014  Ctrl+Q quit, F1 about, Enter add, Del remove",
            font=("Segoe UI", 8), anchor="w", padx=6, pady=2,
        )
        self.status_bar.grid(row=2, column=0, columnspan=2, sticky="ew")
        self.status_bar.configure(cursor="hand2")
        self.status_bar.bind("<Button-1>", self._on_open_updates)

        self.update_label = tk.Label(
            self.root, text="", font=("Segoe UI", 8, "bold"),
            fg=_themes.current()["link"], cursor="hand2", anchor="e", padx=10, pady=2,
        )
        self.update_label.grid(row=2, column=1, sticky="e")
        self.update_label.grid_remove()

    # ── Actions & reflection ────────────────────────────────────────────────

    def _apply_chosen_theme(self, theme) -> None:
        _themes.set_current(theme)
        self._on_apply_theme()

    def _save_chosen_theme(self, theme) -> None:
        self.config.config.set("GUI", "theme", theme.name)
        self.config.config.set("GUI", "dark_mode", "true" if theme.dark else "false")
        try:
            self.config.save()
        except Exception as exc:
            logging.warning(f"Could not save the theme choice: {exc}")

    def open_theme_dialog(self) -> None:
        _theme_dialog.open_theme_dialog(
            self.root,
            apply=self._apply_chosen_theme,
            save=self._save_chosen_theme,
            reload=self._reload_user_themes,
            folder=self._user_themes_dir,
            opener=open_local_path,
            problems=self._get_user_theme_problems,
        )

    def _apply_poll_interval(self, minutes: float, display_name: str) -> None:
        self.config.config.set("Timeouts", "poll_interval_minutes", str(minutes))
        try:
            self.config.save()
        except Exception:
            pass
        rec = self._get_recorder()
        if rec and rec.is_running:
            rec.set_poll_interval(minutes)
        self._last_poll_display = display_name
        self._on_reflect_option("Timeouts", "poll_interval_minutes")
        logging.info(f"Polling interval changed to {minutes} minutes ({display_name})")

    def _on_poll_change(self, *_args) -> None:
        selected = self.poll_var.get()
        if selected == self.CUSTOM_POLL_LABEL:
            minutes = _dialogs.ask_float(
                self.root,
                "Custom Polling Interval",
                f"Minutes between offline checks "
                f"({self.POLL_MIN_MINUTES:g}\u2013{self.POLL_MAX_MINUTES:g}):\n\n"
                f"Values below {self.POLL_MIN_MINUTES:g} min are not allowed \u2014 "
                "checking too often risks rate limiting or IP bans.",
                minvalue=self.POLL_MIN_MINUTES, maxvalue=self.POLL_MAX_MINUTES,
            )
            if minutes is None:
                self.poll_var.set(self._last_poll_display)
                return
            display = self._custom_poll_display(minutes)
            self.poll_var.set(display)
            self._apply_poll_interval(minutes, display)
        else:
            minutes = self.POLL_PRESETS.get(selected, 3.0)
            self._apply_poll_interval(minutes, selected)

    def _apply_clip_length(self, seconds: int, display_name: str) -> None:
        self.config.config.set("Clipping", "clip_length_seconds", str(int(seconds)))
        try:
            self.config.save()
        except Exception:
            pass
        self._last_clip_display = display_name
        logging.info(f"Clip length changed to {int(seconds)}s ({display_name})")
        self._on_reflect_option("Clipping", "clip_length_seconds")

    def _on_clip_length_change(self, *_args) -> None:
        selected = self.clip_var.get()
        if selected == self.CUSTOM_CLIP_LABEL:
            seconds = _dialogs.ask_int(
                self.root,
                "Custom Clip Length",
                f"Seconds to grab when clipping a live recording "
                f"({self.CLIP_MIN_SECONDS}\u2013{self.CLIP_MAX_SECONDS}):",
                minvalue=self.CLIP_MIN_SECONDS, maxvalue=self.CLIP_MAX_SECONDS,
            )
            if seconds is None:
                self.clip_var.set(self._last_clip_display)
                return
            display = self._custom_clip_display(seconds)
            self.clip_var.set(display)
            self._apply_clip_length(seconds, display)
        else:
            seconds = self.CLIP_PRESETS.get(selected, 30)
            self._apply_clip_length(seconds, selected)

    def _check_all_now(self) -> None:
        rec = self._get_recorder()
        if rec and rec.is_running:
            rec.check_all_now()
        else:
            logging.info("Check Now clicked but no recording session is running")

    def set_updates_badge(self, names) -> None:
        self.updates_btn.configure(text="Updates \u2022" if names else "Updates\u2026")

    def reflect_poll(self, minutes: float) -> None:
        shown = next(
            (n for n, v in self.POLL_PRESETS.items() if abs(v - minutes) < 0.1),
            self._custom_poll_display(minutes),
        )
        self.poll_var.set(shown)
        self._last_poll_display = shown

    def reflect_clip(self, seconds: int) -> None:
        shown = next(
            (n for n, v in self.CLIP_PRESETS.items() if v == seconds),
            self._custom_clip_display(seconds),
        )
        self.clip_var.set(shown)
        self._last_clip_display = shown

    def set_recording_buttons(self, *, start_enabled: bool, stop_enabled: bool) -> None:
        self.start_button.config(state=tk.NORMAL if start_enabled else tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL if stop_enabled else tk.DISABLED)

    # ── Theming ─────────────────────────────────────────────────────────────

    def apply_theme(self, t) -> None:
        for w in (self.frame, self.btn_frame, self.toggle_frame):
            w.configure(bg=t["bg"])
        self.poll_label.configure(bg=t["bg"], fg=t["fg"])
        self.clip_label.configure(bg=t["bg"], fg=t["fg"])
        self.start_button.configure(
            bg=t["start_bg"], activebackground=t["start_bg"],
            fg=t["start_fg"], activeforeground=t["start_fg"],
            disabledforeground=t["disabled_fg"], relief="flat", bd=0,
        )
        self.stop_button.configure(
            bg=t["stop_bg"], activebackground=t["stop_bg"],
            fg=t["stop_fg"], activeforeground=t["stop_fg"],
            disabledforeground=t["disabled_fg"], relief="flat", bd=0,
        )
        self.status_bar.configure(bg=t["bg"], fg=t["offline_fg"])
        self.update_label.configure(bg=t["bg"], fg=t["link"])
        self.more_menu.configure(**_themeui.menu_colors(t))
