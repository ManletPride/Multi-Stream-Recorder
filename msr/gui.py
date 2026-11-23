"""Tkinter GUI: status table, channel roster, clips, tray, theme."""
import datetime
import json
import logging
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
from tkinter import ttk

from msr import __version__
from msr.deps import (
    DENO_VERSION, HAS_CURL_CFFI, HAS_DENO,
    HAS_NOTIFICATIONS, HAS_PSUTIL, HAS_STREAMLINK, HAS_TRAY, HAS_WEBSOCKET,
    HAS_YTDLP, STREAMLINK_VERSION, YTDLP_VERSION,
    Image, ImageDraw, plyer_notification, pystray,
)
from msr import channel_panel as cp
from msr import control as _control
from msr.channel_panel import move_list_items  # noqa: F401  (pure; also used by msr/control.py)
from msr import deps as _deps  # read _deps.HAS_FFMPEG at use time: configure_ffmpeg updates it
from msr.config import default_streams_dir, fishtank_password
from msr import dialogs as _dialogs
from msr import history as _history
from msr import library as _library
from msr import library_actions as _library_actions
from msr import library_manage as _library_manage
from msr import library_window as _library_window
from msr import options_panel as _options_panel
from msr import sounds as _sounds_mod
from msr import preview_state as _preview_state
from msr import startup_error as _startup_error
from msr import storage as _storage
from msr import winstate as _winstate
from msr import restore_dialog as _restore_dialog
from msr import logview
from msr import themes as _themes
from msr.bottom_bar import BottomBar
from msr.context_menu import bind_context_menu
from msr.roster_panel import RosterPanel
from msr.status_panel import (
    StatusPanel,
    display_name as _status_display_name,
    platform_label as _status_platform_label,
)
from msr.tooltip import Tooltip, popup
from msr.wheel import bind_wheel, wheel_steps
from msr import themeui as _themeui
from msr import theme_dialog as _theme_dialog
from msr import wintheme as _wintheme
from msr.presence import PresencePoller, PresenceStore, combined_state
from msr.recorder import StreamRecorder
from msr.util import (
    application_dir, channel_file_stem, channel_key_to_dirs, channel_watch_url,
    check_for_updates, coerce_channel_records, ffprobe_from_ffmpeg, find_cookies_file,
    get_cookie_status_for_channel, kill_orphan_recording_processes,
    open_existing_folder, open_local_path, parse_custom_url,
    RedactLogFilter, redact_for_log, reveal_in_file_browser, validate_channel_name, validate_cookies,
)
from msr.iometer import (
    IoSampler, format_header, format_tooltip, meter_severity, sum_stream_mbps,
)
from msr.worker import (
    CATCHUP_UNAVAILABLE, channel_status_allows_clip, clip_unavailable_reason, create_clip, create_preview_frame,
    create_screenshot, find_active_recording_file, screenshot_extension,
)

# ────────────────────────────────────────────────
#          Notifications
# ────────────────────────────────────────────────


class NotificationThrottle:
    """Rate-limits desktop notifications to prevent spam.

    Rules:
        - Global cooldown: minimum 30 seconds between any two notifications
        - Per-channel error dedup: same channel+error only notifies once
        - Burst limit: after 5 notifications in 2 minutes, suppresses until quiet
    """

    def __init__(self, cooldown=30, burst_limit=5, burst_window=120):
        self._cooldown = cooldown
        self._burst_limit = burst_limit
        self._burst_window = burst_window
        self._last_sent = 0
        self._recent_times = []        # timestamps of recent notifications
        self._sent_errors = set()       # (channel, error_snippet) dedup keys
        self._suppressed_count = 0

    def should_send(self, category="info", channel="", detail=""):
        """Check if a notification should be sent.  Returns True if allowed."""
        now = time.time()

        # Global cooldown
        if now - self._last_sent < self._cooldown:
            self._suppressed_count += 1
            return False

        # Burst detection: too many recent notifications
        self._recent_times = [t for t in self._recent_times if now - t < self._burst_window]
        if len(self._recent_times) >= self._burst_limit:
            self._suppressed_count += 1
            return False

        # Error dedup: don't re-notify same channel+error
        if category == "error" and channel:
            key = (channel, detail[:50] if detail else "")
            if key in self._sent_errors:
                return False
            self._sent_errors.add(key)

        self._last_sent = now
        self._recent_times.append(now)
        return True

    def reset(self):
        """Reset all state (call when starting/stopping recording)."""
        self._last_sent = 0
        self._recent_times.clear()
        self._sent_errors.clear()
        self._suppressed_count = 0

    @property
    def suppressed_count(self):
        return self._suppressed_count


# Global throttle instance
_notif_throttle = NotificationThrottle()


def _show_about(root, config):
    """About: version, sites, tools and packages, environment, folders, msr_ctl, disclaimer."""
    from msr.about import about_for
    _dialogs.show_info(root, f"About Multi-Stream Recorder v{__version__}", about_for(config, __version__))


def send_notification(title, message, timeout=5, category="info", channel="", detail=""):
    """Send a desktop toast notification with rate limiting.

    Args:
        title: Notification title
        message: Notification body
        timeout: Display duration in seconds
        category: One of 'info', 'error', 'recording', 'complete' — used for throttling
        channel: Channel name for per-channel dedup
        detail: Error detail string for dedup
    """
    if not HAS_NOTIFICATIONS:
        return
    if not _notif_throttle.should_send(category, channel, detail):
        return
    try:
        plyer_notification.notify(
            title=title,
            message=message,
            app_name=f"Multi-Stream Recorder v{__version__}",
            timeout=timeout,
        )
    except Exception:
        pass  # notifications are best-effort


def channel_matches_filter(name, enabled=True, query="", enabled_only=False):
    """True if a roster row should stay visible for *query* / Enabled only."""
    if enabled_only and not enabled:
        return False
    q = (query or "").strip().lower()
    if not q:
        return True
    return q in (name or "").lower()


def ordered_status_keys(keys, status_lookup, roster_names, recording_first=True):
    """Status-table order: roster sequence, optionally recording/remuxing first."""
    index = {name: i for i, name in enumerate(roster_names)}

    def sort_key(ch):
        rec = status_lookup.get(ch) if status_lookup else None
        st = rec.get("status", "") if isinstance(rec, dict) else (rec or "")
        live = 0 if channel_status_allows_clip(st) else 1
        group = live if recording_first else 0
        return (group, index.get(ch, 10 ** 9), ch or "")

    return sorted(keys, key=sort_key)


# ────────────────────────────────────────────────
#          Window State Persistence
# ────────────────────────────────────────────────

def load_window_state(state_file):
    """Load saved window geometry and preferences."""
    try:
        if os.path.isfile(state_file):
            with open(state_file, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_window_state(state_file, state):
    """Save window geometry and preferences."""
    try:
        _history.atomic_write(state_file, json.dumps(state, indent=2).encode("utf-8"))
        _storage.write_ok("the window state", state_file)
    except OSError as e:
        _storage.write_failed("the window state", state_file, e)
    except Exception:
        pass


# ────────────────────────────────────────────────
#          System Tray Icon
# ────────────────────────────────────────────────

def create_tray_icon_image(recording=False):
    """Create a simple tray icon image using PIL.

    The app icon (assets/icon_64.png): red record dot when recording, grey when idle. Falls
    back to a plain circle if the file is missing.
    """
    from msr import appicon
    img = appicon.tray_image(recording)
    if img is not None:
        return img
    size = 64
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    color = (76, 175, 80, 255) if recording else (128, 128, 128, 255)
    draw.ellipse([4, 4, size - 4, size - 4], fill=color)
    inner_color = (255, 255, 255, 255)
    draw.ellipse([20, 20, size - 20, size - 20], fill=inner_color)
    return img


# ────────────────────────────────────────────────
#          GUI Log Handler
# ────────────────────────────────────────────────

class QueueLogHandler(logging.Handler):
    """A logging handler that puts log records into a bounded queue for the GUI.

    If the GUI drain lags (verbose mode can emit far more than 50 lines per
    500ms tick), drop the oldest pending line so memory cannot grow without
    bound and the viewer stays on recent output.
    """

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
        self.addFilter(RedactLogFilter())  # child-logger records skip the root logger's filter

    def emit(self, record):
        try:
            msg = self.format(record)
            try:
                self.log_queue.put_nowait(msg)
            except queue.Full:
                try:
                    self.log_queue.get_nowait()
                except queue.Empty:
                    pass
                try:
                    self.log_queue.put_nowait(msg)
                except queue.Full:
                    pass
        except Exception:
            pass


# ────────────────────────────────────────────────
#          GUI
# ────────────────────────────────────────────────

# Status-row actions are mouse-only. ttk runs a focused button's command on
# <Key-space> (ttk::button::activate). Clicking DVR left that button focused,
# and the Clip Editor never took the keyboard, so the next Space opened another
# editor. This tag is not on the channel list: its <space> binding stays as it is.
_HEADER_ACTION = "MsrHeaderAction"


def _swallow_header_space(_event=None):
    return "break"


def _header_action_button(parent, **kwargs):
    """Clip Now / DVR / Clips / Screenshot / Streams. Click still runs the command."""
    kwargs["takefocus"] = 0
    btn = ttk.Button(parent, **kwargs)
    btn.bind_class(_HEADER_ACTION, "<Key-space>", _swallow_header_space)
    btn.bind_class(_HEADER_ACTION, "<KeyRelease-space>", _swallow_header_space)
    tags = btn.bindtags()
    if _HEADER_ACTION not in tags:
        btn.bindtags((tags[0], _HEADER_ACTION) + tuple(tags[1:]))
    return btn


def main_gui(config):
    """Main GUI window composing RosterPanel, StatusPanel, Logs, Library, Options, and BottomBar."""
    import tkinter as tk
    from tkinter import ttk
    import queue as stdlib_queue

    # ── Load window state & theme ──
    state_file = config.get('GUI', 'window_state_file', fallback='window_state.json')
    win_state = load_window_state(state_file)
    notifications_enabled = config.getboolean('GUI', 'notifications', fallback=True)
    want_tray = config.getboolean('GUI', 'minimize_to_tray', fallback=True)

    data_dir = os.path.dirname(os.path.abspath(config.config_file))
    user_themes_dir = os.path.join(data_dir, 'themes')
    user_theme_problems = []

    def _load_user_themes():
        found, user_theme_problems[:] = _themes.load_user_themes(user_themes_dir)
        _themes.register_user_themes(found)
        for line in user_theme_problems:
            logging.warning(f"Theme file ignored: {line}")

    _load_user_themes()
    _legacy_dark = _themes.parse_bool(win_state.get('dark_mode'), None)
    if _legacy_dark is None:
        _legacy_dark = _themes.parse_bool(config.get('GUI', 'dark_mode', fallback=''), None)
    _themes.set_current(_themes.resolve_saved(config.get('GUI', 'theme', fallback=''), _legacy_dark))

    # ── Create root window ──
    from msr import appicon as _appicon
    _appicon.set_app_id()
    root = tk.Tk(className="Multi-Stream Recorder")
    _appicon.apply(root)
    smoke_test = os.environ.get("MSR_SMOKE_TEST") == "1"
    if smoke_test:
        root.withdraw()
    root.title(f"Multi-Stream Recorder v{__version__}")

    def set_title_bar_dark(_dark=None):
        theme = _themes.current()
        colors = _themes.caption_colors(theme)
        _wintheme.set_title_bar_dark(root, theme.dark, colors)
        _wintheme.theme_toplevels(root, theme.dark, colors=colors)

    geom = _winstate.fit_main_geometry(root, win_state.get('geometry'), '1100x760', (1060, 620))
    root.geometry(geom)
    _wintheme.enable_background_drag(root)
    root.minsize(1060, 620)

    root.grid_rowconfigure(0, weight=1)
    root.grid_columnconfigure(0, weight=0)
    root.grid_columnconfigure(1, weight=1)

    # ── Shared runtime state (initialized before any panel or callback) ──
    recorder = None
    _stop_in_progress = [False]
    _nav_lock = [False]
    _preview_selected = [None]
    _notified_live = set()
    _notified_complete = set()
    _telegram_uploads = [None]
    style = ttk.Style()
    sounds = _sounds_mod.Sounds(config)

    # ── Log queue for GUI log viewer ──
    log_queue = stdlib_queue.Queue(maxsize=2000)
    gui_log_handler = QueueLogHandler(log_queue)
    gui_log_handler.setLevel(logging.INFO)
    gui_log_handler.setFormatter(logview.GuiFormatter(
        "%(asctime)s %(message)s", datefmt="%H:%M:%S"
    ))
    logging.root.addHandler(gui_log_handler)

    # ── Channel roster data & presence ──
    channels_file = config.get('Paths', 'channels_file')
    channels = []
    skipped_on_load = []
    try:
        with open(channels_file, "r") as f:
            loaded = json.load(f)
        channels, skipped_on_load = coerce_channel_records(loaded)
    except FileNotFoundError:
        logging.info("No existing channels file found, creating new one")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse channels file: {e}")
    except Exception as e:
        logging.error(f"Error loading channels: {e}")

    def _presence_watch_keys():
        return [ch["name"] for ch in channels if not ch.get("enabled", True)]

    def save_channels():
        _history.snapshot(channels_file, min_interval=45)
        try:
            _history.atomic_write(channels_file, json.dumps(channels, indent=2).encode("utf-8"))
            _storage.write_ok("the channel list", channels_file)
        except OSError as e:
            _storage.write_failed("the channel list", channels_file, e)
        except Exception as e:
            logging.error(f"Failed to save channels: {e}")
        try:
            if recorder and recorder.is_running:
                recorder.set_watch_channels(_presence_watch_keys())
        except Exception as e:
            logging.debug(f"Presence watch sync failed: {e}")

    if skipped_on_load:
        for raw in skipped_on_load:
            logging.warning(
                "Ignoring unsafe or unknown channel in roster: %s",
                redact_for_log(raw),
            )
        save_channels()

    def get_enabled_channels():
        return [ch["name"] for ch in channels if ch.get("enabled", True)]

    presence_store = PresenceStore()
    presence_poller = PresencePoller(presence_store, lambda: [dict(c) for c in list(channels)])
    presence_poller.start()

    # ── Cross-panel navigation sync ──
    def _set_nav_lock(flag):
        _nav_lock[0] = bool(flag)

    def _scroll_nav_into_view(ch_name):
        if not ch_name:
            return
        try:
            item = roster_panel.item_for_name(ch_name)
            if item:
                roster_panel.tree.see(item)
            status_panel.scroll_row_into_view(ch_name)
            status_panel.preview_strip.scroll_into_view(ch_name)
        except tk.TclError:
            pass

    def _sync_nav(ch_name, origin):
        if _nav_lock[0] or not ch_name:
            return
        if ch_name == _preview_selected[0] and origin != "preview":
            status_panel.preview_strip.paint_selection(ch_name)
            return
        _nav_lock[0] = True
        try:
            _preview_selected[0] = ch_name
            if origin != "status":
                status_panel.select_row(ch_name, see=False)
            if origin != "roster":
                roster_panel.select_names([ch_name], see_first=False)
            status_panel.preview_strip.paint_selection(ch_name)
        finally:
            _nav_lock[0] = False
        root.after_idle(lambda n=ch_name: _scroll_nav_into_view(n))

    # ── Left panel: Channel Roster ──
    roster_panel = RosterPanel(
        root, style,
        config=config,
        channels=channels,
        win_state=win_state,
        state_file=state_file,
        load_state=load_window_state,
        save_state=save_window_state,
        save_channels=save_channels,
        get_recorder=lambda: recorder,
        presence_store=presence_store,
        sounds=sounds,
        on_status_refresh=lambda: _schedule_status_refresh(0),
        on_open_browser=StatusPanel.open_channel_in_browser,
        on_open_clips_dir=lambda ch: status_panel.open_clips_dir_for_channel(ch),
        on_select=lambda ch: _sync_nav(ch, "roster") if ch != _preview_selected[0] else None,
    )

    def _reload_channels_from_disk(_path=None):
        try:
            with open(channels_file, "r") as f:
                fresh, _skipped = coerce_channel_records(json.load(f))
        except Exception as e:
            logging.error(f"Could not reload the restored channel list: {e}")
            return
        channels[:] = fresh
        roster_panel.populate()
        logging.info("Channel list restored from history (%d channels)", len(fresh))

    def _restore_channels():
        def blocked():
            if recorder and recorder.is_running:
                return "Stop recording first, then restore the channel list."
            return None

        _restore_dialog.open_restore(
            root, channels_file, "Restore previous channel list",
            can_restore=blocked, on_restored=_reload_channels_from_disk,
            state_file=state_file,
        )

    def _restore_settings():
        def restored(_path):
            config.reload()
            logging.info("config.ini restored from history")

        _restore_dialog.open_restore(
            root, config.config_file, "Restore previous settings (config.ini)",
            on_restored=restored, state_file=state_file,
            note="Restart Multi-Stream Recorder to use the restored settings.",
        )

    def _about_dialog():
        _show_about(root, config)

    root.bind("<Control-q>", lambda e: on_close())
    root.bind("<F1>", lambda e: _about_dialog())

    # ── Right panel: Notebook ──
    frame_right = tk.Frame(root)
    frame_right.grid(row=0, column=1, sticky="nsew", padx=(5, 10), pady=10)
    frame_right.grid_rowconfigure(0, weight=1)
    frame_right.grid_columnconfigure(0, weight=1)

    notebook = ttk.Notebook(frame_right)
    notebook.grid(row=0, column=0, sticky="nsew")

    # ── Tab 1: Status (with embedded PreviewStrip) ──
    status_panel = StatusPanel(
        notebook, root,
        config=config,
        win_state=win_state,
        state_file=state_file,
        load_state=load_window_state,
        save_state=save_window_state,
        get_recorder=lambda: recorder,
        get_channels=lambda: channels,
        get_telegram_uploader=lambda: _telegram_uploads[0],
        sounds=sounds,
        notifications_enabled=notifications_enabled,
        send_notification=send_notification,
        header_action_button=_header_action_button,
        grab_preview_frame=lambda *a, **k: create_preview_frame(*a, **k),
        on_status_refresh=lambda: _schedule_status_refresh(0),
        on_open_options=lambda: notebook.select(options_panel.frame),
        on_select_channel=lambda ch: _sync_nav(ch, "status") if ch != _preview_selected[0] else None,
        on_preview_select=lambda ch: _sync_nav(ch, "preview"),
        set_nav_lock=_set_nav_lock,
    )
    notebook.add(status_panel.frame, text="  Status  ")
    notebook.bind("<<NotebookTabChanged>>", status_panel.preview_strip.kick, add="+")
    root.bind("<Map>", lambda e: status_panel.preview_strip.kick() if e.widget is root else None, add="+")

    # ── Tab 2: Logs ──
    log_tab = ttk.Frame(notebook)
    notebook.add(log_tab, text="  Logs  ")

    log_toolbar = ttk.Frame(log_tab)
    log_toolbar.pack(fill=tk.X, padx=10, pady=(8, 4))

    auto_scroll_var = tk.BooleanVar(value=True)
    ttk.Checkbutton(
        log_toolbar, text="Auto-scroll", variable=auto_scroll_var,
        command=lambda: auto_scroll_var.get() and log_follow.jump(),
    ).pack(side=tk.LEFT)

    def clear_logs():
        log_follow.clear()

    ttk.Button(log_toolbar, text="Clear", command=clear_logs).pack(side=tk.RIGHT)

    def _log_file_path():
        for handler in logging.root.handlers:
            name = getattr(handler, "baseFilename", None)
            if name:
                return name
        return os.path.join(config.get('Paths', 'streams_dir'), "stream_recorder.log")

    def open_log_file():
        def _notepad(path):
            subprocess.Popen(["notepad.exe", path])  # .log has no program on some PCs
        ok, why = logview.open_log_file(
            _log_file_path(), open_local_path,
            _notepad if os.name == "nt" else None,
        )
        if not ok:
            _dialogs.show_info(root, "Open log file", why)

    ttk.Button(log_toolbar, text="Open log file", command=open_log_file).pack(side=tk.RIGHT, padx=(0, 6))

    log_filter_var = tk.StringVar()
    log_level_var = tk.StringVar(value=logview.level_label(logview.LEVEL_ALL))
    log_filter_info = ttk.Label(log_toolbar, text="")
    ttk.Label(log_toolbar, text="Filter:").pack(side=tk.LEFT, padx=(18, 4))
    log_filter_entry = ttk.Entry(log_toolbar, textvariable=log_filter_var, width=24)
    log_filter_entry.pack(side=tk.LEFT)
    log_filter_clear = ttk.Button(
        log_toolbar, text="\u2715", width=3,
        command=lambda: (log_filter_var.set(""), log_filter_entry.focus_set()),
    )
    log_filter_clear.pack(side=tk.LEFT, padx=(2, 6))
    log_level_box = ttk.Combobox(
        log_toolbar, textvariable=log_level_var, state="readonly", width=19,
        values=[label for _key, label in logview.LEVEL_CHOICES],
    )
    log_level_box.pack(side=tk.LEFT)
    log_filter_info.pack(side=tk.LEFT, padx=(8, 0))
    _log_filter_job = [None]

    def _apply_log_filter(*_args):
        _log_filter_job[0] = None
        log_follow.set_filter(log_filter_var.get(), logview.level_key(log_level_var.get()))

    def _log_filter_changed(*_args):
        if _log_filter_job[0] is not None:
            root.after_cancel(_log_filter_job[0])
        _log_filter_job[0] = root.after(250, _apply_log_filter)

    log_filter_var.trace_add("write", _log_filter_changed)
    log_level_box.bind("<<ComboboxSelected>>", lambda _e: (_apply_log_filter(), log_level_box.selection_clear()))
    log_filter_entry.bind("<Escape>", lambda _e: log_filter_var.set(""))

    log_text = tk.Text(
        log_tab, wrap=tk.WORD, font=("Consolas", 9), state=tk.DISABLED,
        borderwidth=0, relief="flat",
    )
    log_scroll = ttk.Scrollbar(log_tab, orient=tk.VERTICAL, command=log_text.yview)
    new_lines_btn = ttk.Button(log_tab, text="\u2193 New lines")

    def _show_new_lines(count):
        if count > 0:
            new_lines_btn.configure(text=f"\u2193 {count} new line{'s' if count != 1 else ''}")
            new_lines_btn.place(relx=1.0, rely=1.0, x=-34, y=-18, anchor="se")
            new_lines_btn.lift()
        else:
            new_lines_btn.place_forget()

    def _log_filter_stats(shown, total):
        log_filter_info.configure(text=f"{shown} of {total} lines" if log_follow.filtering else "")

    log_follow = logview.LogFollower(
        log_text, enabled=auto_scroll_var.get, on_behind=_show_new_lines, on_stats=_log_filter_stats,
    )
    new_lines_btn.configure(command=log_follow.jump)

    def _log_yscroll(first, last):
        log_scroll.set(first, last)
        log_follow.on_scroll(first, last)

    log_text.configure(yscrollcommand=_log_yscroll)

    def _focus_log_filter(_event=None):
        try:
            if notebook.select() == str(log_tab):
                log_filter_entry.focus_set()
                log_filter_entry.select_range(0, tk.END)
                return "break"
        except tk.TclError:
            pass
        return None

    def _ctrl_f(_event=None):
        if _focus_log_filter() == "break":
            return "break"
        roster_panel.focus_search()
        return "break"

    root.bind("<Control-f>", _ctrl_f)
    log_scroll.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 10), pady=(0, 10))
    log_text.pack(fill=tk.BOTH, expand=True, padx=(10, 0), pady=(0, 10))

    # ── Tab 3: Library ──
    def _library_purge_settings():
        try:
            days = config.getint('Cleanup', 'auto_purge_days', fallback=7)
        except ValueError:
            days = 7
        return days, config.getboolean('Cleanup', 'purge_on_startup', fallback=True)

    def _library_busy():
        return _library_actions.busy_from_status(dict(getattr(recorder, 'status_dict', None) or {}))

    library_data_dir = data_dir

    def _library_save_extra_dirs(text):
        running = bool(recorder and recorder.is_running)
        _control.apply_setting(config, "Library", "extra_dirs", text, recording=running)

    library_panel = _library_window.LibraryPanel(
        notebook,
        manage_ctx=_library_manage.ManageContext(
            data_dir=library_data_dir,
            log_path=os.path.join(library_data_dir, "library_actions.log"),
            manifest_path=os.path.join(library_data_dir, "library_moves.json"),
            get_busy=_library_busy, get_purge=_library_purge_settings,
            revealer=reveal_in_file_browser, opener=open_local_path, logger=logging.getLogger("msr.library"),
            clip_editor=lambda p, start=None, end=None: status_panel.open_clip_editor(
                p, start_at=start, end_at=end,
            ),
        ),
        get_roots=lambda: _library.roots_from_config(config),
        get_roster=lambda: [c["name"] for c in channels],
        cache_path=os.path.join(library_data_dir, "library_cache.json"),
        ffprobe=ffprobe_from_ffmpeg(config.get("Advanced", "ffmpeg_path", fallback="ffmpeg") or "ffmpeg"),
        opener=open_local_path,
        save_extra_dirs=_library_save_extra_dirs,
    )
    notebook.add(library_panel.frame, text="  Library  ")

    def _library_tab_opened(_event=None):
        try:
            if notebook.select() == str(library_panel.frame):
                library_panel.activate()
        except tk.TclError:
            pass

    notebook.bind("<<NotebookTabChanged>>", _library_tab_opened, add="+")

    # ── Tab 4: Options ──
    def _options_apply_setting(section, key, value):
        running = bool(recorder and recorder.is_running)
        try:
            value = _control.apply_setting(
                config, section, key, value, recording=running,
                live=(lambda v: recorder.set_poll_interval(float(v))) if running else None,
            )
        except _control.ControlError as e:
            raise ValueError(str(e)) from None
        _reflect_setting(section, key)
        if section == "Telegram":
            _telegram_sync()
        if section == "Fishtank":
            roster_panel.refresh_fishtank_login()
        if (section, key) == ("Advanced", "ffmpeg_path"):
            _deps.configure_ffmpeg(value)
            status_panel.refresh_ffmpeg_notice()
        logging.info(f"Setting {section}.{key} changed in Options")
        return value

    def _options_twitch_changed():
        tw = getattr(recorder, 'twitch_push', None) if recorder and recorder.is_running else None
        if tw is not None:
            threading.Thread(target=tw.restart, daemon=True, name="twitch-push-restart").start()

    def _telegram_sync():
        up = _telegram_uploads[0]
        if up is not None:
            try:
                up.sync()
            except Exception:
                logging.debug("telegram sync", exc_info=True)

    def _telegram_status():
        up = _telegram_uploads[0]
        if up is None:
            return {"enabled": False, "line": "Off", "state": "off"}
        return up.status()

    def _telegram_test():
        up = _telegram_uploads[0]
        if up is None:
            return {"ok": False, "error": "Uploader is not running."}
        return up.test()

    options_panel = _options_panel.OptionsPanel(
        notebook, config, library_data_dir, _options_apply_setting,
        on_twitch_changed=_options_twitch_changed, tooltip=Tooltip, sound_test=sounds.test,
        open_updates=lambda: _open_updates(),
        on_telegram_changed=_telegram_sync, telegram_status=_telegram_status,
        telegram_test=_telegram_test,
    )
    notebook.add(options_panel.frame, text="  Options  ")

    def _options_tab_opened(_event=None):
        try:
            if notebook.select() == str(options_panel.frame):
                options_panel.apply_theme()
                options_panel.activate()
        except tk.TclError:
            pass

    notebook.bind("<<NotebookTabChanged>>", _options_tab_opened, add="+")

    # ── Bottom bar, status bar, and update banner ──
    from msr import depsupdate as _depsupdate

    def _open_updates(event=None):
        from msr.deps_dialog import open_deps_dialog
        open_deps_dialog(
            root, config=config, state_file=state_file,
            ffmpeg_path=config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg') or 'ffmpeg',
            on_msr_update=lambda row, parent=None: _begin_in_app_update(release=row, parent=parent),
            on_badge=bottom_bar.set_updates_badge,
            on_checked=_apply_banner_from_rows,
        )

    bottom_bar = BottomBar(
        root,
        config=config,
        win_state=win_state,
        state_file=state_file,
        library_data_dir=library_data_dir,
        load_state=load_window_state,
        get_recorder=lambda: recorder,
        on_start=lambda: start_recording(),
        on_stop=lambda: stop_recording(),
        on_apply_theme=lambda: apply_theme(),
        reload_user_themes=_load_user_themes,
        user_themes_dir=user_themes_dir,
        get_user_theme_problems=lambda: list(user_theme_problems),
        on_reflect_option=lambda s, k: options_panel.reflect(s, k),
        on_about=_about_dialog,
        on_open_updates=_open_updates,
        on_open_library=lambda: notebook.select(library_panel.frame),
        on_restore_channels=_restore_channels,
        on_restore_settings=_restore_settings,
    )
    update_label = bottom_bar.update_label
    health_ctl = bottom_bar.health_ctl

    # ── Master apply_theme (all panels constructed above — zero NameError guards) ──
    def apply_theme(*_args):
        t = _themes.current()
        _themeui.apply_ttk(root, style, t)
        root.configure(bg=t['bg'])
        set_title_bar_dark()
        border_color = t['border']

        frame_right.configure(bg=t['bg'])
        roster_panel.apply_theme(t)
        status_panel.apply_theme(t)
        bottom_bar.apply_theme(t)

        log_text.configure(
            bg=t['log_bg'], fg=t['log_fg'], insertbackground=t['log_fg'],
            highlightbackground=border_color, highlightcolor=border_color,
            highlightthickness=1, relief='flat',
        )
        log_text.tag_configure("lvl_warning", foreground=t['log_warn'])
        log_text.tag_configure("lvl_error", foreground=t['log_error'])
        log_text.tag_configure("find", background=t['find_bg'], foreground=t['find_fg'])
        log_text.tag_raise("find")
        library_panel.refresh_theme()
        options_panel.apply_theme()
        _themeui.recolor_popdowns(root, t)

    # ── In-app updates & daily dependency checks ──
    _update_url = [None]
    _update_zipball = [None]
    _update_tag = [None]
    _update_busy = [False]
    _banner_check_at = [0.0]

    def _ui_parent(parent):
        try:
            if parent is not None and parent.winfo_exists():
                return parent
        except tk.TclError:
            pass
        return root

    def _remember_release(tag, url, zipball):
        _update_tag[0] = tag or None
        _update_url[0] = url or None
        _update_zipball[0] = zipball or None

    def _show_update_banner(tag=None):
        from msr.update import banner_text
        text = banner_text(__version__, application_dir(), tag if tag is not None else _update_tag[0])
        if not text:
            update_label.grid_remove()
            return
        update_label.configure(text=text, cursor="hand2")
        update_label.grid()

    def _clear_update_banner():
        if _update_busy[0]:
            return
        _update_tag[0] = None
        _update_url[0] = None
        _update_zipball[0] = None
        update_label.grid_remove()

    def _apply_banner_from_rows(rows):
        if _update_busy[0]:
            return
        row = next((r for r in rows or [] if r.get("name") == "msr"), None)
        from msr.update import disk_is_newer
        ahead = disk_is_newer(application_dir(), __version__)
        status = (row or {}).get("status")
        if row and row.get("latest") and status not in (None, "unchecked", "unknown"):
            _remember_release(str(row["latest"]).lstrip("vV"), row.get("url") or "", row.get("zipball") or "")
            _banner_check_at[0] = time.time()
        if ahead or (status == "update_available" and (row or {}).get("latest")):
            _show_update_banner(_update_tag[0])
        elif status == "up_to_date":
            _clear_update_banner()

    def _on_update_found(latest_tag, release_url, zipball_url=None):
        _remember_release(latest_tag, release_url, zipball_url or "")
        root.after(0, lambda: _show_update_banner(latest_tag))

    def _restore_update_banner():
        _update_busy[0] = False
        _show_update_banner(_update_tag[0])

    def _cannot_start(msg, title="Cannot update", parent=None, *, clear_busy=True):
        logging.warning("In-app update did not start: %s", " ".join((msg or "").split()))
        _dialogs.show_error(_ui_parent(parent), title, msg)
        if clear_busy:
            _update_busy[0] = False

    def _explain_update(title, msg, parent=None):
        logging.warning("In-app update did not start: %s", " ".join((msg or "").split()))
        _dialogs.show_info(_ui_parent(parent), title, msg)
        _update_busy[0] = False

    def _begin_in_app_update(event=None, release=None, parent=None):
        from msr.update import BUSY_MESSAGE, NO_RELEASE_MESSAGE, choose_release, disk_is_newer
        ask_parent = parent or root
        if _update_busy[0]:
            _cannot_start(BUSY_MESSAGE, parent=ask_parent, clear_busy=False)
            return False
        stored = None
        if _update_tag[0]:
            stored = {"tag": _update_tag[0], "url": _update_url[0] or "", "zipball": _update_zipball[0] or ""}
        chosen = choose_release(release, stored)
        if chosen is None and disk_is_newer(application_dir(), __version__):
            chosen = {"tag": "", "url": "", "zipball": ""}
        if chosen is None:
            _update_busy[0] = True

            def work():
                from msr.update import fetch_latest_release
                found, why = fetch_latest_release(__version__)

                def done():
                    _update_busy[0] = False
                    if not found:
                        _cannot_start(why or NO_RELEASE_MESSAGE, parent=ask_parent)
                        return
                    _remember_release(found["tag"], found.get("url"), found.get("zipball"))
                    _banner_check_at[0] = time.time()
                    _show_update_banner(found["tag"])
                    if _start_planned(found, ask_parent):
                        _dismiss_deps_dialog(ask_parent)

                root.after(0, done)

            threading.Thread(target=work, daemon=True, name="update-recheck").start()
            return False
        return _start_planned(chosen, ask_parent)

    def _dismiss_deps_dialog(parent):
        try:
            if parent is not None and parent is not root and parent.winfo_exists():
                parent.destroy()
                root._msr_deps_dialog = None
        except tk.TclError:
            pass

    def _start_planned(rel, parent):
        from msr.update import BUSY_MESSAGE, choose_release, plan_app_update
        if _update_busy[0]:
            _cannot_start(BUSY_MESSAGE, parent=parent, clear_busy=False)
            return False
        target = application_dir()
        recording = bool(recorder and recorder.is_running)
        plan = plan_app_update(target, __version__, rel, recording=recording)
        chosen = choose_release(rel, None)
        if chosen and plan["action"] != "blocked":
            _remember_release(chosen["tag"], chosen.get("url"), chosen.get("zipball"))
        if plan["action"] == "blocked":
            _cannot_start(plan["message"], title=plan.get("title") or "Cannot update", parent=parent)
            return False
        if plan["action"] == "git_manual":
            _explain_update(plan["title"], plan["message"], parent=parent)
            return False
        if plan["action"] == "zip" and not os.access(target, os.W_OK):
            _cannot_start(f"The install folder is not writable:\n{target}", parent=parent)
            return False
        if plan["confirm"] and not _dialogs.ask_yes_no(parent, plan["title"], plan["message"]):
            logging.info("In-app update cancelled")
            return False
        _update_busy[0] = True
        if plan["action"] == "zip":
            update_label.configure(text=f"  Downloading v{plan['tag']}\u2026", cursor="arrow")
            update_label.grid()
            _download_zip(plan, target)
            return True
        if plan["action"] == "git_pull":
            update_label.configure(text="  Updating with git\u2026", cursor="arrow")
            update_label.grid()
            _pull_then_restart(target, parent)
            return True
        update_label.configure(text="  Restarting\u2026", cursor="arrow")
        update_label.grid()
        _stop_then(_restart_then_quit, open_release=False)
        return True

    def _fail(msg, *, open_release=True):
        logging.warning("In-app update failed: %s", msg)
        detail = f"{msg}\n\nYou can still open the release page from GitHub." if open_release else msg
        _dialogs.show_error(root, "Update failed", detail)
        if open_release and _update_url[0]:
            import webbrowser
            webbrowser.open(_update_url[0])
        _restore_update_banner()

    def _restart_then_quit():
        def _after_children():
            from msr.update import relaunch_app
            relaunch_app(application_dir())
        _full_quit(after_children=_after_children)

    def _stop_then(after_stop, *, open_release):
        rec = recorder
        if rec and rec.is_running:
            update_label.configure(text="  Stopping recordings\u2026")

            def _bg_stop():
                try:
                    rec.stop()
                except Exception:
                    logging.exception("Stop before update failed")
                    root.after(0, lambda: _fail(
                        "Could not stop recordings. Try Stop Recording, then click the banner again.",
                        open_release=open_release,
                    ))
                    return
                root.after(0, after_stop)

            threading.Thread(target=_bg_stop, daemon=True, name="update-stop").start()
            return
        after_stop()

    def _download_zip(plan, target):
        from msr.update import prepare_update, spawn_updater
        zip_url = plan["zipball"]

        def _spawn_then_quit(staging):
            def _after_children():
                spawn_updater(staging, target_dir=target)
            _full_quit(after_children=_after_children)

        def _bg_download():
            try:
                staging = prepare_update(zip_url)
            except Exception as exc:
                err = str(exc) or "Download failed"
                root.after(0, lambda m=err: _fail(m))
                return
            root.after(0, lambda: _stop_then(lambda: _spawn_then_quit(staging), open_release=True))

        threading.Thread(target=_bg_download, daemon=True, name="update-download").start()

    def _pull_then_restart(target, parent):
        def work():
            from msr.update import git_pull_ff_only
            ok, text = git_pull_ff_only(target)

            def done():
                if not ok:
                    logging.warning("In-app update failed: git pull --ff-only: %s", text)
                    _dialogs.show_error(
                        _ui_parent(parent), "Update failed",
                        "git pull --ff-only failed.\n\n" + (text or "git pull --ff-only failed."),
                    )
                    _restore_update_banner()
                    return
                logging.info("git pull --ff-only finished; restarting")
                _stop_then(_restart_then_quit, open_release=False)

            root.after(0, done)

        threading.Thread(target=work, daemon=True, name="update-git-pull").start()

    update_label.bind("<Button-1>", _begin_in_app_update)

    def _banner_if_files_are_newer():
        from msr.update import disk_is_newer
        if disk_is_newer(application_dir(), __version__):
            _show_update_banner(None)

    root.after(800, _banner_if_files_are_newer)

    def _delayed_version_check():
        _banner_check_at[0] = time.time()
        check_for_updates(__version__, callback=_on_update_found)

    root.after(10_000, _delayed_version_check)

    def _repeat_banner_check():
        if time.time() - _banner_check_at[0] < 24 * 3600:
            return
        _banner_check_at[0] = time.time()
        check_for_updates(__version__, callback=_on_update_found)

    _deps_q = stdlib_queue.Queue()

    def _daily_deps_check():
        root.after(3_600_000, _daily_deps_check)
        if (not config.getboolean('Updates', 'daily_check', fallback=True)
                or not _depsupdate.due(load_window_state(state_file))):
            _repeat_banner_check()
            return
        _banner_check_at[0] = time.time()

        def work():
            try:
                rs = _depsupdate.rows(
                    True,
                    ffmpeg_path=config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg') or 'ffmpeg',
                )
                if any(r['status'] != 'unknown' for r in rs if r['name'] != 'ffmpeg'):
                    _depsupdate.remember(state_file, rs)
                _deps_q.put(rs)
            except Exception:
                logging.debug("Daily dependency check failed", exc_info=True)
                _deps_q.put(None)

        threading.Thread(target=work, daemon=True, name="deps-daily").start()
        _drain_deps_q()

    def _drain_deps_q():
        try:
            payload = _deps_q.get_nowait()
            if payload is not None:
                bottom_bar.set_updates_badge(_depsupdate.updates_available(payload))
                _apply_banner_from_rows(payload)
        except stdlib_queue.Empty:
            root.after(1000, _drain_deps_q)

    root.after(60_000, _daily_deps_check)

    def _warn_if_last_update_failed():
        from msr.update import update_error_path
        path = update_error_path()
        if not os.path.isfile(path):
            return
        try:
            with open(path, encoding="utf-8") as fh:
                msg = fh.read().strip() or "The previous in-app update failed."
        except Exception:
            msg = "The previous in-app update failed."
        try:
            os.remove(path)
        except OSError:
            pass
        _dialogs.show_error(root, "Update failed", msg)

    def _show_update_notice():
        from msr.apply_update import NOTICE_FILE
        path = os.path.join(application_dir(), NOTICE_FILE)
        if not os.path.isfile(path):
            return
        try:
            with open(path, encoding="utf-8") as fh:
                msg = fh.read().strip()
            os.remove(path)
        except OSError:
            return
        if msg:
            _dialogs.show_info(root, "Update installed", msg)

    def _show_venv_notice():
        from msr import venv as _venv
        if _venv.LAUNCH_NOTICE:
            _dialogs.show_warning(root, "Multi-Stream Recorder environment", _venv.LAUNCH_NOTICE)

    root.after(400, _warn_if_last_update_failed)
    root.after(500, _show_update_notice)
    root.after(2500, _show_venv_notice)

    # ── Periodic status & log refresh ──
    _status_after_id = [None]
    _last_tray_title = [None]

    def _schedule_status_refresh(delay_ms=2500):
        """Ensure only a single refresh_status timer chain is ever armed."""
        prev = _status_after_id[0]
        if prev is not None:
            try:
                root.after_cancel(prev)
            except tk.TclError:
                pass
        _status_after_id[0] = root.after(delay_ms, refresh_status)

    def refresh_status():
        _status_after_id[0] = None
        try:
            status_panel.show_storage_notice(
                recorder.storage_notice() if recorder and recorder.is_running else "",
            )
        except Exception as e:
            logging.debug(f"storage notice: {e}")

        if recorder and recorder.is_running:
            recorder.update_status_from_queue()
            tree = status_panel.tree

            existing = {}
            for iid in tree.get_children():
                try:
                    key = tree.set(iid, "_key")
                except tk.TclError:
                    key = ""
                if key:
                    existing[key] = iid

            seen = set()
            for ch_name in recorder.status_dict:
                seen.add(ch_name)
                st = recorder.status_dict.get(ch_name, {"status": "Unknown", "detail": "", "size": "", "time": ""})
                curr = st["status"].lower()

                if "recording" in curr:
                    detail = st.get("detail", "")
                    display = f"Recording  {detail}" if detail else "Recording"
                else:
                    display = st["status"]
                    if st.get("detail"):
                        display += f" ({st['detail']})"

                plat_label = _status_platform_label(ch_name)

                if "recording" in curr:
                    tag = "recording"
                    if "remuxing" in (st.get("detail") or "").lower():
                        tag = "remuxing"
                    _notified_complete.discard(ch_name)
                    if ch_name not in _notified_live and "starting" not in st.get("detail", ""):
                        _notified_live.add(ch_name)
                        sounds.play("live")
                        if notifications_enabled:
                            send_notification(
                                "Stream Recording", f"Now recording: {ch_name}",
                                category="recording", channel=ch_name,
                            )
                elif "remuxing" in curr or "pending" in curr:
                    tag = "remuxing"
                elif "checking" in curr or "initializing" in curr:
                    tag = "checking"
                elif "error" in curr or "failed" in curr:
                    tag = "error"
                    sounds.play("problem", f"{ch_name}:{display}")
                    if notifications_enabled:
                        send_notification(
                            "Recording Error", f"{ch_name}: {display}",
                            category="error", channel=ch_name, detail=display,
                        )
                elif "completed" in curr:
                    tag = "completed"
                    _notified_live.discard(ch_name)
                    if notifications_enabled and ch_name not in _notified_complete:
                        _notified_complete.add(ch_name)
                        send_notification(
                            "Recording Complete", f"{ch_name}: {st.get('detail', '')}",
                            category="complete", channel=ch_name,
                        )
                elif "offline" in curr:
                    tag = "offline"
                    _notified_live.discard(ch_name)
                elif "stopped" in curr:
                    tag = "stopped"
                    _notified_live.discard(ch_name)
                else:
                    tag = "unknown"

                disp_name = _status_display_name(ch_name)
                new_vals = (
                    disp_name, display,
                    st.get("size", ""), st.get("time", ""),
                    plat_label, ch_name,
                )
                iid = existing.get(ch_name)
                if iid is None:
                    tree.insert("", tk.END, values=new_vals, tags=(tag,))
                else:
                    old_vals = tuple(tree.item(iid, "values"))
                    old_tags = tree.item(iid, "tags")
                    if old_vals != tuple(str(v) for v in new_vals):
                        tree.item(iid, values=new_vals, tags=(tag,))
                    elif not old_tags or old_tags[0] != tag:
                        tree.item(iid, tags=(tag,))

            for key, iid in existing.items():
                if key not in seen:
                    tree.delete(iid)

            status_panel.reorder_rows()

            if tray_icon is not None:
                active = sum(
                    1 for ch_name in recorder.status_dict
                    if recorder.status_dict.get(ch_name, {}).get("status", "").lower().startswith("recording")
                )
                new_title = f"Multi-Stream Recorder \u2014 {active} recording" if active else "Multi-Stream Recorder \u2014 idle"
                if new_title != _last_tray_title[0]:
                    tray_icon.title = new_title
                    _last_tray_title[0] = new_title

        roster_panel.live_tick()
        _schedule_status_refresh(2500)

    def refresh_logs():
        batch = []
        while len(batch) < 50:
            try:
                batch.append(log_queue.get_nowait())
            except stdlib_queue.Empty:
                break
        if batch:
            log_follow.append(batch)
        root.after(500, refresh_logs)

    # ── Recording controls ──
    def start_recording(interactive=True):
        nonlocal recorder
        if _stop_in_progress[0]:
            logging.info("Start ignored — stop is still finishing")
            if not interactive:
                raise _control.ControlError("The last stop is still finishing; try again shortly.", "busy")
            return
        enabled = get_enabled_channels()
        if not enabled:
            if not interactive:
                raise _control.ControlError("Enable at least one channel first.", "conflict")
            _dialogs.show_warning(root, "No channels", "Enable at least one channel (click the checkbox).")
            return
        if recorder and recorder.is_running:
            if not interactive:
                raise _control.ControlError("Recording is already running.", "conflict")
            _dialogs.show_info(root, "Already running", "Recording is active.")
            return

        if len(enabled) >= 30 and interactive:
            if not _dialogs.ask_yes_no(
                root, "High Channel Count",
                f"You're about to monitor {len(enabled)} channels simultaneously.\n\n"
                "Idle channels share a lightweight poller, but recording many live streams "
                "at once can still strain your CPU, network, and disk I/O.\n\nContinue?",
            ):
                return

        issues = cp.preflight_dependency_issues(
            enabled,
            has_streamlink=HAS_STREAMLINK,
            has_ytdlp=HAS_YTDLP,
            has_ffmpeg=_deps.HAS_FFMPEG,
            has_deno=HAS_DENO,
        )

        if issues and not interactive:
            for issue in issues:
                logging.warning(f"Start (control): {issue}")
        elif issues:
            msg = "The following issues may prevent recording:\n\n" + "\n\n".join(issues) + "\n\nStart anyway?"
            if not _dialogs.ask_yes_no(root, "Dependency Warning", msg):
                return

        logging.info("=" * 60)
        logging.info("Starting recording session...")
        for ch in enabled:
            logging.info(f"  Channel: {ch}")
        disabled_count = len(channels) - len(enabled)
        if disabled_count > 0:
            logging.info(f"  ({disabled_count} channel(s) disabled — not monitoring)")
        logging.info("=" * 60)

        recorder = StreamRecorder(
            enabled, config, presence=presence_store,
            watch_channels=_presence_watch_keys(),
            log_sink=gui_log_handler,
        )
        threading.Thread(target=recorder.run, daemon=True).start()

        bottom_bar.set_recording_buttons(start_enabled=False, stop_enabled=True)
        _notified_live.clear()
        _notified_complete.clear()
        _notif_throttle.reset()

        if notifications_enabled:
            send_notification("Recording Started", f"Monitoring {len(enabled)} channel(s)")
        return "starting"

    def stop_recording():
        nonlocal recorder
        if _stop_in_progress[0]:
            return
        if not recorder or not recorder.is_running:
            return

        rec = recorder
        _stop_in_progress[0] = True
        bottom_bar.set_recording_buttons(start_enabled=False, stop_enabled=False)

        summary_parts = []
        active_count = 0
        try:
            for ch_name, st in rec.status_dict.items():
                if st.get("status", "").lower().startswith("recording"):
                    active_count += 1
                    size_str = st.get("size", "")
                    time_str = st.get("time", "")
                    if size_str and time_str:
                        summary_parts.append(f"  {ch_name}: {size_str}, {time_str}")
        except Exception:
            pass

        logging.info("Stop requested — terminating...")

        def _after_stop():
            nonlocal recorder
            _stop_in_progress[0] = False
            if summary_parts:
                logging.info(f"Recording summary — {active_count} stream(s) captured:")
                for part in summary_parts:
                    logging.info(part)
            else:
                logging.info("Recording summary — no active streams were being captured")

            tree = status_panel.tree
            for item in tree.get_children():
                tree.delete(item)
            try:
                keys = list(rec.status_dict)
            except Exception:
                keys = []
            for ch_name in keys:
                tree.insert(
                    "", tk.END,
                    values=(
                        _status_display_name(ch_name), "Stopped", "", "",
                        _status_platform_label(ch_name), ch_name,
                    ),
                    tags=("stopped",),
                )

            recorder = None
            bottom_bar.set_recording_buttons(start_enabled=True, stop_enabled=False)
            _notified_live.clear()
            _notified_complete.clear()
            _notif_throttle.reset()
            if notifications_enabled:
                send_notification("Recording Stopped", "All recordings have been stopped.")

        def _bg():
            try:
                rec.stop()
            except Exception:
                logging.exception("Stop failed")
            root.after(0, _after_stop)

        threading.Thread(target=_bg, daemon=True, name="stop-recording").start()

    # ── System tray ──
    tray_icon = None
    _tray_ready = [False]
    _tray_hidden = [False]
    _tray_notice = [False]
    _dock_filter = _deps.DockFailureFilter()

    def _tray_notice_once():
        if _tray_notice[0]:
            return
        _tray_notice[0] = True
        logging.info(_deps.TRAY_DISABLED_INFO)

    def _stop_tray_icon(icon):
        try:
            icon.stop()
        except Exception:
            pass

    def _abandon_tray():
        nonlocal tray_icon
        _tray_ready[0] = False
        icon = tray_icon
        tray_icon = None
        if icon is not None:
            threading.Thread(target=_stop_tray_icon, args=(icon,), daemon=True, name="tray-stop").start()
        if _tray_hidden[0]:
            _tray_hidden[0] = False
            try:
                root.deiconify()
                root.lift()
            except tk.TclError:
                pass
        _tray_notice_once()

    def _withdraw_for_tray():
        """Hide the main window only when the tray icon is up. Otherwise stay shown."""
        decision = _deps.hide_to_tray_decision(
            _tray_ready[0], _dock_filter.failed, tray_icon is not None,
        )
        if decision != "withdraw":
            _tray_hidden[0] = False
            try:
                if not smoke_test and str(root.state()) == "withdrawn":
                    root.deiconify()
            except tk.TclError:
                pass
            _tray_notice_once()
            return
        _tray_hidden[0] = True
        root.withdraw()

    def _watch_tray_dock():
        if _dock_filter.failed:
            _abandon_tray()
            return
        if tray_icon is not None:
            root.after(1000, _watch_tray_dock)

    def _poll_tray_dock(left):
        if _dock_filter.failed:
            _abandon_tray()
            return
        if tray_icon is None:
            return
        try:
            up = bool(tray_icon.visible)
        except Exception:
            up = False
        if up or left <= 0:
            _tray_ready[0] = True
            root.after(1000, _watch_tray_dock)
            return
        root.after(200, lambda: _poll_tray_dock(left - 1))

    def _run_tray_icon(icon):
        try:
            icon.run()
        except Exception:
            _dock_filter.failed = True

    def setup_tray():
        nonlocal tray_icon
        if not want_tray:
            return
        backend = ""
        xembed = True
        if HAS_TRAY and sys.platform.startswith("linux"):
            backend = _deps.pystray_backend_name(pystray)
            if backend == "xorg":
                xembed = _deps.xembed_tray_present()
        if not _deps.tray_can_start(bool(HAS_TRAY), backend, xembed):
            _tray_notice_once()
            return

        def show_window(icon=None, item=None):
            def _restore():
                _tray_hidden[0] = False
                root.deiconify()
                root.lift()
            root.after(0, _restore)

        def hide_to_tray(icon=None, item=None):
            root.after(0, _withdraw_for_tray)

        def quit_app(icon=None, item=None):
            root.after(0, on_close)

        def tray_start(icon=None, item=None):
            root.after(0, start_recording)

        def tray_stop(icon=None, item=None):
            root.after(0, stop_recording)

        menu = pystray.Menu(
            pystray.MenuItem("Show Window", show_window, default=True),
            pystray.MenuItem("Hide to Tray", hide_to_tray),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Start Recording", tray_start),
            pystray.MenuItem("Stop Recording", tray_stop),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit", quit_app),
        )

        _deps.install_dock_filter(_dock_filter)
        icon_img = create_tray_icon_image(recording=False)
        tray_icon = pystray.Icon("multi_stream_recorder", icon_img, "Multi-Stream Recorder", menu)
        threading.Thread(target=_run_tray_icon, args=(tray_icon,), daemon=True, name="tray").start()
        if sys.platform.startswith("linux"):
            root.after(200, lambda: _poll_tray_dock(15))
        else:
            _tray_ready[0] = True

    setup_tray()

    def _on_root_unmap(event):
        if event.widget is not root or smoke_test or _tray_hidden[0]:
            return
        if not want_tray:
            return
        try:
            if str(root.state()) == "iconic":
                _withdraw_for_tray()
        except tk.TclError:
            pass

    root.bind("<Unmap>", _on_root_unmap, add="+")

    # ── Window close behavior ──
    def _active_recording_count():
        if not (recorder and recorder.is_running):
            return 0
        try:
            return sum(
                1 for ch_name in list(recorder.status_dict)
                if str((recorder.status_dict.get(ch_name) or {}).get("status", "")).lower().startswith("recording")
            )
        except Exception:
            return 0

    def on_close(confirm=True):
        """Confirm if streams are actively recording, save window state, then fully quit."""
        active = _active_recording_count() if confirm else 0
        if active > 0 and not smoke_test and _startup_error.dialogs_allowed():
            verb = "stream is" if active == 1 else "streams are"
            if not _dialogs.ask_yes_no(
                root,
                "Quit while recording?",
                f"{active} {verb} currently recording.\n\n"
                "Stop active recordings and quit Multi-Stream Recorder?",
            ):
                return
        try:
            state = load_window_state(state_file)
            state.update({
                'geometry': root.geometry(),
                'column_widths': status_panel.column_widths(),
                'roster_filter': roster_panel.filter_var.get(),
                'roster_view': roster_panel.view_var.get(),
                'roster_enabled_only': roster_panel.view_var.get() == cp.VIEW_ENABLED,
                'roster_disabled_collapsed': bool(roster_panel.disabled_collapsed),
                'status_recording_first': bool(status_panel.recording_first_var.get()),
            })
            save_window_state(state_file, state)
        except Exception:
            pass

        _full_quit()

    def _full_quit(after_children=None):
        """Fully terminate the application — stop recorder, tray, and exit."""
        try:
            if _telegram_uploads[0] is not None:
                _telegram_uploads[0].stop()
        except Exception:
            pass
        try:
            if _control_service[0] is not None:
                _control_service[0].stop()
        except Exception:
            pass
        try:
            if recorder and recorder.is_running:
                recorder.stop()
        except Exception:
            pass
        try:
            if recorder:
                recorder.manager.shutdown()
        except Exception:
            pass
        try:
            import psutil
            parent = psutil.Process(os.getpid())
            children = parent.children(recursive=True)
            for child in children:
                try:
                    child.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            psutil.wait_procs(children, timeout=3)
        except Exception:
            pass
        try:
            kill_orphan_recording_processes(
                logging.getLogger(),
                streams_dir=config.get("Paths", "streams_dir"),
            )
        except Exception:
            pass
        if after_children is not None:
            try:
                after_children()
            except Exception:
                logging.exception("Failed to start update helper")
        # 3. Clean up cached preview frames, stop tray, and destroy window
        try:
            status_panel.preview_strip.cleanup_cache()
        except Exception:
            pass
        try:
            if tray_icon:
                tray_icon.stop()
        except Exception:
            pass
        try:
            root.destroy()
        except Exception:
            pass
        os._exit(0)

    root.protocol("WM_DELETE_WINDOW", on_close)

    # ── Agent / headless control (msr/control.py, msr_ctl.py) ──
    from msr import control_exec as _control_exec
    _control_service = [None]
    _control_jobs = _control_exec.SerialQueue("window")
    _control_pump = _control_exec.TkPump(root, _control_jobs)
    if _control.enabled(config):
        _control_pump.start()

    def _reflect_setting(section, key):
        try:
            options_panel.reflect(section, key)
        except Exception:
            pass
        if (section, key) == ("Timeouts", "poll_interval_minutes"):
            bottom_bar.reflect_poll(config.getfloat('Timeouts', 'poll_interval_minutes', fallback=3.0))
        elif (section, key) == ("Clipping", "clip_length_seconds"):
            bottom_bar.reflect_clip(config.getint('Clipping', 'clip_length_seconds', fallback=30))

    def _control_schedule_quit(after_children=None):
        if after_children is None:
            root.after(300, lambda: on_close(confirm=False))
        else:
            root.after(300, lambda: _full_quit(after_children=after_children))

    def _control_after_edit():
        roster_panel.populate()
        save_channels()

    def _control_apply_setting(section, key, value):
        value = _control.apply_setting(
            config, section, key, value,
            recording=bool(recorder and recorder.is_running),
        )
        _reflect_setting(section, key)
        if section == "Telegram":
            _telegram_sync()
        if section == "Fishtank":
            roster_panel.refresh_fishtank_login()
        return value

    def _control_twitch_changed():
        try:
            options_panel.refresh_status()
        except Exception:
            pass
        _options_twitch_changed()

    def _control_reload():
        _reload_channels_from_disk()
        return len(channels)

    from msr.telegram_upload import Uploader
    _telegram_uploads[0] = Uploader(config, library_data_dir, get_recorder=lambda: recorder)
    _telegram_uploads[0].sync()

    _control_bridge = _control_exec.GuiBridge(config, _control_jobs, {
        "get_recorder": lambda: recorder,
        "get_presence": lambda: presence_store,
        "channels": lambda: channels,
        "after_edit": _control_after_edit,
        "start": lambda: start_recording(interactive=False),
        "stop": stop_recording,
        "schedule_quit": _control_schedule_quit,
        "reload": _control_reload,
        "reflect": _reflect_setting,
        "twitch_changed": _control_twitch_changed,
        "apply_setting": _control_apply_setting,
        "health_report": lambda: health_ctl.report,
        "health_check_now": health_ctl.check_now,
        "sound_test": sounds.test,
        "update_tag": lambda: _update_tag[0],
    })
    _control_bridge.uploads = _telegram_uploads[0]
    _control_service[0] = _control.Service.start_for(_control_bridge)

    # ── Apply theme and start refresh loops ──
    apply_theme()
    root.after(100, lambda: set_title_bar_dark())
    root.after(500, lambda: set_title_bar_dark())
    _schedule_status_refresh(1500)
    status_panel.start_timers()
    root.after(500, refresh_logs)

    if smoke_test and os.environ.get("MSR_SMOKE_KEEP") != "1":
        root.after(2500, lambda: os._exit(0))

    try:
        root.mainloop()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt detected — shutting down...")
        _full_quit()
