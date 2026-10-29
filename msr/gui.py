"""Tkinter GUI: status table, channel roster, clips, tray, theme."""
import datetime
import json
import logging
import os
import queue
import subprocess
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
from msr import preview_state as _preview_state
from msr import storage as _storage
from msr import winstate as _winstate
from msr import restore_dialog as _restore_dialog
from msr import logview
from msr import themes as _themes
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
    channel_status_allows_clip, create_clip, create_preview_frame,
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
    # Outer circle
    color = (76, 175, 80, 255) if recording else (128, 128, 128, 255)
    draw.ellipse([4, 4, size - 4, size - 4], fill=color)
    # Inner dot
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

def main_gui(config):
    """Main GUI window with dark mode, system tray, log viewer, and notifications."""
    import tkinter as tk
    from tkinter import ttk
    import queue as stdlib_queue

    # ── Load window state ──
    state_file = config.get('GUI', 'window_state_file', fallback='window_state.json')
    win_state = load_window_state(state_file)
    notifications_enabled = config.getboolean('GUI', 'notifications', fallback=True)
    minimize_to_tray = config.getboolean('GUI', 'minimize_to_tray', fallback=True) and HAS_TRAY

    # ── Theme: every colour below comes from msr/themes.py. [GUI] theme = <name> wins; an older
    # install only has dark_mode (window_state.json first, then config.ini): true -> PowerShell Blue,
    # false -> Light. themes/*.json in the data folder add user themes.
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
    _appicon.set_app_id()  # own taskbar group and icon, not python's
    root = tk.Tk()
    _appicon.apply(root)   # every later Toplevel inherits it
    # MSR_SMOKE_TEST=1 (tests/test_gui_smoke.py): build the whole window, never show it, exit
    # shortly after the first refresh ticks. A crash while building the UI exits non-zero.
    smoke_test = os.environ.get("MSR_SMOKE_TEST") == "1"
    if smoke_test:
        root.withdraw()
    root.title(f"Multi-Stream Recorder v{__version__}")

    # tk variables must be created AFTER tk.Tk()

    # ── Windows: dark title bar via DWM API ──

    def set_title_bar_dark(_dark=None):
        """The theme's caption (dark/light mode, and its colours on Windows 11) for the main window
        and any open dialogs."""
        theme = _themes.current()
        colors = _themes.caption_colors(theme)
        _wintheme.set_title_bar_dark(root, theme.dark, colors)
        _wintheme.theme_toplevels(root, theme.dark, colors=colors)

    # Restore geometry (made safe for the monitors connected now: never off-screen)
    geom = _winstate.fit_main_geometry(root, win_state.get('geometry'), '1100x760', (1060, 720))
    root.geometry(geom)
    _wintheme.enable_background_drag(root)  # drag the window by its plain background
    # Width floor raised from 960 to 1060 in v1.8.0: the bottom toolbar packs
    # right-to-left, so when it runs out of horizontal room the *leftmost*
    # widget (the About button) is the one that gets clipped — it was
    # rendering as "At". Adding the Clip Length label + selector pushed the
    # bar's natural width past the old 960 floor, so the floor has to move
    # with it rather than the button being widened (which needs MORE room,
    # not less, and so makes the clipping worse).
    root.minsize(1060, 720)

    root.grid_rowconfigure(0, weight=1)
    root.grid_columnconfigure(0, weight=0)
    root.grid_columnconfigure(1, weight=1)

    # ── Log queue for GUI log viewer ──
    log_queue = stdlib_queue.Queue(maxsize=2000)
    gui_log_handler = QueueLogHandler(log_queue)
    gui_log_handler.setLevel(logging.INFO)
    gui_log_handler.setFormatter(logview.GuiFormatter(
        "%(asctime)s %(message)s", datefmt="%H:%M:%S"
    ))
    logging.root.addHandler(gui_log_handler)

    # ── Notification tracking (avoid spamming) ──
    _notified_live = set()       # channels we already sent a "live" notification for
    _notified_complete = set()   # channels we already sent a "complete" notification for
                                 # (edge-triggered: one toast per recording, not every refresh)

    # ── Apply theme ──
    style = ttk.Style()

    _library_panel = [None]  # set when the Library tab is built (apply_theme can run first)

    def apply_theme(*_args):
        t = _themes.current()
        _themeui.apply_ttk(root, style, t)
        root.configure(bg=t['bg'])
        set_title_bar_dark()  # Windows: dark/light caption (and theme colours on Windows 11)
        border_color = t['border']

        # Tag colors for treeview
        tree.tag_configure("recording", foreground=t['rec_fg'], font=("Segoe UI", 10, "bold"))
        tree.tag_configure("completed", foreground=t['completed_fg'])
        tree.tag_configure("offline", foreground=t['offline_fg'])
        tree.tag_configure("checking", foreground=t['check_fg'])
        tree.tag_configure("remuxing", foreground=t['remux_fg'])
        tree.tag_configure("error", foreground=t['error_fg'])
        tree.tag_configure("stopped", foreground=t['offline_fg'])
        tree.tag_configure("unknown", foreground=t['offline_fg'])

        # Tk widgets (non-ttk) — set all frame backgrounds
        for w in [frame_right, bottom_bar, btn_frame, toggle_frame, status_header, status_meter_row]:
            w.configure(bg=t['bg'])
        try:
            _theme_roster(t)
        except NameError:
            pass  # not yet defined on first apply_theme call during startup
        status_label.configure(bg=t['bg'], fg=t['fg'])
        try:
            storage_notice_label.configure(bg=t['bg'], fg=t['error_fg'])
        except NameError:
            pass  # not yet defined on first apply_theme call during startup
        poll_label.configure(bg=t['bg'], fg=t['fg'])
        clip_label.configure(bg=t['bg'], fg=t['fg'])
        try:
            io_meter_label.configure(bg=t['bg'])
            _paint_io_meter()
        except NameError:
            pass
        try:
            _theme_preview_strip(t)
        except NameError:
            pass

        start_button.configure(bg=t['start_bg'], activebackground=t['start_bg'],
                               fg=t['start_fg'], activeforeground=t['start_fg'],
                               disabledforeground=t['disabled_fg'],
                               relief='flat', bd=0)
        stop_button.configure(bg=t['stop_bg'], activebackground=t['stop_bg'],
                              fg=t['stop_fg'], activeforeground=t['stop_fg'],
                              disabledforeground=t['disabled_fg'],
                              relief='flat', bd=0)

        log_text.configure(bg=t['log_bg'], fg=t['log_fg'], insertbackground=t['log_fg'],
                           highlightbackground=border_color, highlightcolor=border_color,
                           highlightthickness=1, relief='flat')
        log_text.tag_configure("lvl_warning", foreground=t['log_warn'])
        log_text.tag_configure("lvl_error", foreground=t['log_error'])
        log_text.tag_configure("find", background=t['find_bg'], foreground=t['find_fg'])
        log_text.tag_raise("find")
        if _library_panel[0] is not None:
            _library_panel[0].refresh_theme()  # charts, site colours and tooltips follow the theme

        # Status bar at bottom
        status_bar.configure(bg=t['bg'], fg=t['offline_fg'])
        update_label.configure(bg=t['bg'], fg=t['link'])

        # Context menu theming - borderwidth=0 removes the bright system border
        for menu in (ctx_menu, status_ctx_menu, more_menu, sort_menu):
            menu.configure(**_themeui.menu_colors(t))
        _themeui.recolor_popdowns(root, t)  # drop-down lists that already exist keep old colours otherwise

    # ── Load channels ──
    # channels is a list of dicts: [{"name": "twitch:saruei", "enabled": True}, ...]
    # Backward-compatible with old format: ["twitch:saruei", "betty-fae"]
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

    def save_channels():
        # The list as it was goes to the history first (a burst of quick edits keeps one copy).
        _history.snapshot(channels_file, min_interval=45)
        try:
            # replaced in one step: a failing disk must not leave half a channel list
            _history.atomic_write(channels_file, json.dumps(channels, indent=2).encode("utf-8"))
            _storage.write_ok("the channel list", channels_file)
        except OSError as e:
            _storage.write_failed("the channel list", channels_file, e)
        except Exception as e:
            logging.error(f"Failed to save channels: {e}")
        # Keep the push listener's presence-only (disabled) set in step.
        try:
            if recorder and recorder.is_running:
                recorder.set_watch_channels(_presence_watch_keys())
        except NameError:
            pass  # called during load, before the recorder slot exists
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
        """Return list of channel name strings that are checked/enabled."""
        return [ch["name"] for ch in channels if ch.get("enabled", True)]

    def get_all_channel_names():
        """Return list of all channel name strings."""
        return [ch["name"] for ch in channels]

    # ── Live presence for ALL channels (msr/presence.py) ──
    # Read-only: Kick push (while a session runs) + a slow poll of DISABLED
    # channels. Never starts or wakes a recording.
    presence_store = PresenceStore()

    def _presence_watch_keys():
        return [ch["name"] for ch in channels if not ch.get("enabled", True)]

    presence_poller = PresencePoller(presence_store, lambda: [dict(c) for c in list(channels)])
    presence_poller.start()

    # ── Left panel — Channel roster (see msr/channel_panel.py) ──
    # No status dots (removed by request): live state only drives the
    # Recording group and the Live filter.
    # Layout: search + All/Enabled/Live toggle, a grouped Treeview
    # (Recording / Watching / Off), a narrow canvas strip for the
    # amber cookie badge, then "+ Add channel" and the cookies.txt line.
    frame_left = tk.Frame(root, padx=12, pady=12)
    frame_left.grid(row=0, column=0, sticky="ns", padx=(10, 5), pady=10)

    # ── Tooltip helper (also used by the right panel) ──
    class _Tooltip:
        def __init__(self, widget, text):
            self._widget = widget
            self._text = text
            self._tip = None
            widget.bind("<Enter>", self._show, add="+")
            widget.bind("<Leave>", self._hide, add="+")
        def _show(self, event=None):
            if self._tip:
                return
            text = self._text() if callable(self._text) else self._text
            if not text:
                return
            x = self._widget.winfo_rootx() + 20
            y = self._widget.winfo_rooty() + self._widget.winfo_height() + 4
            self._tip = tw = tk.Toplevel(self._widget)
            tw.wm_overrideredirect(True)
            tw.wm_geometry(f"+{x}+{y}")
            t = _themes.current()
            tk.Label(tw, text=text, justify=tk.LEFT,
                     background=t['entry_bg'], foreground=t['fg'],
                     relief='solid', borderwidth=1,
                     font=("Segoe UI", 8), padx=6, pady=4).pack()
        def _hide(self, event=None):
            if self._tip:
                self._tip.destroy()
                self._tip = None

    CHECK_ON = "☑"
    CHECK_OFF = "☐"

    roster_filter_var = tk.StringVar(value=win_state.get("roster_filter", ""))
    roster_view_var = tk.StringVar(value=cp.coerce_view(
        win_state.get("roster_view"), bool(win_state.get("roster_enabled_only", False))))
    disabled_collapsed = [bool(win_state.get("roster_disabled_collapsed", False))]
    recording_first_var = tk.BooleanVar(
        value=bool(win_state.get("status_recording_first", True))
    )
    _nav_lock = [False]
    _preview_selected = [None]

    def _persist_roster_state():
        try:
            st = load_window_state(state_file)
            st['roster_filter'] = roster_filter_var.get()
            st['roster_view'] = roster_view_var.get()
            st['roster_enabled_only'] = roster_view_var.get() == cp.VIEW_ENABLED
            st['roster_disabled_collapsed'] = bool(disabled_collapsed[0])
            save_window_state(state_file, st)
        except Exception:
            pass

    # Pre-fill the cookie cache before the first draw (data parse only).
    _cookie_info_cache = [validate_cookies(find_cookies_file(config))]
    _fishtank_login = bool(
        str(config.get('Fishtank', 'email', fallback='') or '').strip()
        and fishtank_password(config).strip()
    )

    # ── Search + view toggle ──
    search_frame = tk.Frame(frame_left)
    search_frame.pack(fill=tk.X, pady=(0, 6))
    filter_entry = tk.Entry(search_frame, textvariable=roster_filter_var,
                            font=("Segoe UI", 10), borderwidth=0, relief="flat")
    filter_entry.pack(fill=tk.X, ipady=4)
    search_placeholder = tk.Label(search_frame, text="Search channels…",
                                  font=("Segoe UI", 10), anchor="w", cursor="xterm")
    search_placeholder.bind("<Button-1>", lambda e: filter_entry.focus_set())

    def _update_search_placeholder(*_):
        if roster_filter_var.get() or root.focus_get() is filter_entry:
            search_placeholder.place_forget()
        else:
            search_placeholder.place(in_=filter_entry, x=4, rely=0.5, anchor="w")

    filter_entry.bind("<FocusIn>", _update_search_placeholder, add="+")
    filter_entry.bind("<FocusOut>", _update_search_placeholder, add="+")
    filter_entry.bind("<Escape>", lambda e: roster_filter_var.set(""))

    view_frame = tk.Frame(frame_left)
    view_frame.pack(fill=tk.X, pady=(0, 6))
    view_buttons = []
    for _label, _val in (("All", cp.VIEW_ALL), ("Enabled", cp.VIEW_ENABLED), ("Live", cp.VIEW_LIVE)):
        _b = tk.Radiobutton(view_frame, text=_label, value=_val, variable=roster_view_var,
                            indicatoron=0, font=("Segoe UI", 9), relief="flat", bd=0,
                            padx=10, pady=3, cursor="hand2")
        _b.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 1))
        view_buttons.append(_b)

    # ── Tree + badge strip ──
    ch_tree_frame = tk.Frame(frame_left)
    ch_tree_frame.pack(fill=tk.BOTH, expand=True)

    ch_tree = ttk.Treeview(ch_tree_frame, columns=("check", "name", "tag", "_key"),
                           displaycolumns=("check", "name", "tag"), show="tree",
                           height=10, selectmode="extended", style="Roster.Treeview")
    ch_tree.column("#0", width=0, minwidth=0, stretch=False)  # hidden tree column
    ch_tree.column("check", width=26, anchor="center", stretch=False)
    ch_tree.column("name", width=200, anchor="w")
    ch_tree.column("tag", width=96, anchor="e", stretch=False)
    ch_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    BADGE_CANVAS_W = 22
    badge_canvas = tk.Canvas(ch_tree_frame, width=BADGE_CANVAS_W, highlightthickness=0,
                           borderwidth=0)
    badge_canvas.pack(side=tk.LEFT, fill=tk.Y)
    badge_canvas.msr_drag_passive = True  # only hover tips: the window can be dragged from here

    _row_group = {}     # iid -> group id
    _badges = {}        # channel key -> tooltip text
    _roster_sig = [None]

    def _status_lookup():
        if recorder and recorder.is_running:
            try:
                return recorder.status_dict
            except Exception:
                return {}
        return {}

    def _roster_states():
        sd = _status_lookup()
        pres = presence_store.snapshot()
        now = time.time()
        out = {}
        for ch in channels:
            key = ch["name"]
            out[key] = combined_state(cp.channel_live_state(sd.get(key)), pres.get(key),
                                      ch.get("enabled", True), now)
        return out

    def _compute_badges():
        sd = _status_lookup()
        info = _cookie_info_cache[0]
        out = {}
        for ch in channels:
            key = ch["name"]
            if not ch.get("enabled", True):
                continue  # not monitored; no nagging on disabled rows
            tip = cp.cookie_badge(key, info, sd.get(key),
                                  get_status=get_cookie_status_for_channel,
                                  fishtank_login=_fishtank_login)
            if tip:
                out[key] = tip
        return out

    def _redraw_badge_canvas():
        """Paint amber cookie badges next to the rows that need one."""
        t = _themes.current()
        badge_canvas.configure(bg=t['tree_field'])
        badge_canvas.delete("all")
        for item in ch_tree.get_children():
            key = _channel_name_from_item(item)
            if not key or key not in _badges:
                continue
            bbox = ch_tree.bbox(item)
            if not bbox:
                continue
            _x, y, _w, h = bbox
            badge_canvas.create_text(BADGE_CANVAS_W // 2, y + h // 2, text=cp.BADGE_GLYPH,
                                   fill=t['badge'], font=("Segoe UI Symbol", 10))

    _badge_redraw_after_id = [None]

    def _schedule_badge_redraw(_event=None):
        if _badge_redraw_after_id[0] is not None:
            return
        def _run():
            _badge_redraw_after_id[0] = None
            _redraw_badge_canvas()
        _badge_redraw_after_id[0] = badge_canvas.after(16, _run)

    ch_tree.bind("<Configure>", _schedule_badge_redraw)
    ch_tree.bind("<MouseWheel>", _schedule_badge_redraw)
    ch_tree.bind("<Button-4>", _schedule_badge_redraw)
    ch_tree.bind("<Button-5>", _schedule_badge_redraw)
    badge_canvas.bind("<MouseWheel>", lambda e: (ch_tree.yview_scroll(int(-e.delta / 120), "units"), "break")[1])
    _orig_yview = ch_tree.yview

    def _yview(*args):
        result = _orig_yview(*args)
        if args:
            _schedule_badge_redraw()
        return result

    ch_tree.yview = _yview

    # ── Hover tooltip: badge reason, full channel key ──
    _hover = {"tip": None, "text": None}

    def _hover_hide(_e=None):
        if _hover["tip"] is not None:
            try:
                _hover["tip"].destroy()
            except tk.TclError:
                pass
        _hover["tip"] = None
        _hover["text"] = None

    def _hover_text(y, region_col):
        item = ch_tree.identify_row(y)
        key = _channel_name_from_item(item) if item else None
        if not key:
            return None
        if region_col == "badge":
            return _badges.get(key)
        if region_col == "#3":
            tip = f"{key}"
            if key in _badges:
                tip += f"\n{_badges[key]}"
            return tip
        return _badges.get(key) if region_col == "#2" and key in _badges else None

    def _hover_motion(event, col=None):
        if col is None:
            col = ch_tree.identify_column(event.x)
        text = _hover_text(event.y, col)
        if text == _hover["text"]:
            return
        _hover_hide()
        if not text:
            return
        t = _themes.current()
        tw = tk.Toplevel(ch_tree)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{event.x_root + 14}+{event.y_root + 16}")
        tk.Label(tw, text=text, justify=tk.LEFT, background=t['entry_bg'],
                 foreground=t['fg'], relief='solid', borderwidth=1,
                 font=("Segoe UI", 9), padx=6, pady=4).pack()
        _hover["tip"] = tw
        _hover["text"] = text

    ch_tree.bind("<Motion>", _hover_motion, add="+")
    ch_tree.bind("<Leave>", _hover_hide, add="+")
    badge_canvas.bind("<Motion>", lambda e: _hover_motion(e, "badge"), add="+")
    badge_canvas.bind("<Leave>", _hover_hide, add="+")

    def _channel_name_from_item(item):
        if item and item.startswith("c:"):
            return item[2:]
        return None

    def _index_for_name(name):
        for i, ch in enumerate(channels):
            if ch["name"] == name:
                return i
        return -1

    def _item_for_name(name):
        iid = "c:" + name
        try:
            return iid if ch_tree.exists(iid) else None
        except tk.TclError:
            return None

    def _selected_roster_names():
        names = []
        for item in ch_tree.selection():
            name = _channel_name_from_item(item)
            if name:
                names.append(name)
        return names

    def _select_roster_names(names, see_first=True):
        items = [i for i in (_item_for_name(n) for n in names) if i]
        if not items:
            return
        ch_tree.selection_set(items)
        if see_first:
            ch_tree.see(items[0])

    _GROUP_TITLES = {
        cp.GROUP_RECORDING: "Recording",
        cp.GROUP_ENABLED: "Watching",
        cp.GROUP_DISABLED: "Off",
    }

    def _layout_signature(groups):
        return (tuple((g, tuple(keys)) for g, keys in groups),
                disabled_collapsed[0], bool(roster_filter_var.get().strip()),
                tuple(ch.get("enabled", True) for ch in channels))

    def _populate_channel_tree(states=None):
        """Rebuild the roster from ``channels`` (filter, groups)."""
        if states is None:
            states = _roster_states()
        query = roster_filter_var.get()
        view = roster_view_var.get()
        groups = cp.group_channels(channels, states, query, view)
        _roster_sig[0] = _layout_signature(groups)
        enabled_by_key = {ch["name"]: ch.get("enabled", True) for ch in channels}

        selected = _selected_roster_names()
        try:
            top = ch_tree.yview()[0]
        except tk.TclError:
            top = 0.0
        ch_tree.delete(*ch_tree.get_children())
        _row_group.clear()
        show_headers = sum(1 for _g, keys in groups if keys) > 1 or bool(groups[2][1])
        for gid, keys in groups:
            if not keys:
                continue
            collapsed = (gid == cp.GROUP_DISABLED and disabled_collapsed[0]
                         and not query.strip())
            if show_headers:
                arrow = ("▸" if collapsed else "▾") if gid == cp.GROUP_DISABLED else ""
                ch_tree.insert("", tk.END, iid="h:" + gid,
                               values=(arrow, f"{_GROUP_TITLES[gid]} ({len(keys)})", "", ""),
                               tags=("header",))
            if collapsed:
                continue
            for key in keys:
                enabled = enabled_by_key.get(key, True)
                display, ptag = cp.channel_display(key)
                iid = "c:" + key
                ch_tree.insert("", tk.END, iid=iid,
                               values=(CHECK_ON if enabled else CHECK_OFF, display, ptag, key),
                               tags=("enabled" if enabled else "disabled",))
                _row_group[iid] = gid
        if selected:
            _select_roster_names(selected, see_first=False)
        try:
            ch_tree.yview_moveto(top)
        except tk.TclError:
            pass
        _badges.clear()
        _badges.update(_compute_badges())
        _schedule_badge_redraw()

    def _roster_live_tick():
        """Called from refresh_status: regroup / refilter rows, refresh badges."""
        try:
            states = _roster_states()
            groups = cp.group_channels(channels, states, roster_filter_var.get(),
                                       roster_view_var.get())
            if _layout_signature(groups) != _roster_sig[0]:
                _populate_channel_tree(states)
                return
            badges = _compute_badges()
            if badges != _badges:
                _badges.clear()
                _badges.update(badges)
                _schedule_badge_redraw()
        except tk.TclError:
            pass

    def _toggle_disabled_group():
        disabled_collapsed[0] = not disabled_collapsed[0]
        _populate_channel_tree()
        _persist_roster_state()

    def _set_enabled(names, enabled):
        changed = False
        for name in names:
            idx = _index_for_name(name)
            if 0 <= idx < len(channels) and channels[idx].get("enabled", True) != enabled:
                channels[idx]["enabled"] = enabled
                changed = True
        if changed:
            _populate_channel_tree()
            save_channels()

    def _toggle_channel_check(event):
        """Click the checkbox column to toggle; click a group header to fold it."""
        item = ch_tree.identify_row(event.y)
        if not item:
            return
        if item.startswith("h:"):
            if item == "h:" + cp.GROUP_DISABLED:
                _toggle_disabled_group()
            return
        if ch_tree.identify_column(event.x) == "#1":
            name = _channel_name_from_item(item)
            idx = _index_for_name(name)
            if 0 <= idx < len(channels):
                _set_enabled([name], not channels[idx].get("enabled", True))

    ch_tree.bind("<ButtonRelease-1>", _toggle_channel_check)

    def _double_click_toggle(event):
        """Double-clicking anywhere on a channel row toggles its enabled state."""
        item = ch_tree.identify_row(event.y)
        name = _channel_name_from_item(item)
        if not name or ch_tree.identify_column(event.x) == "#1":
            return  # the single-click handler already toggled the checkbox
        idx = _index_for_name(name)
        if 0 <= idx < len(channels):
            _set_enabled([name], not channels[idx].get("enabled", True))

    ch_tree.bind("<Double-ButtonRelease-1>", _double_click_toggle)

    def _on_filter_change(*_):
        _update_search_placeholder()
        _populate_channel_tree()

    roster_filter_var.trace_add("write", _on_filter_change)
    roster_view_var.trace_add("write", lambda *_: (_populate_channel_tree(), _persist_roster_state()))

    recorder = None
    _stop_in_progress = [False]

    # ── Add / remove ──
    def _add_channel_from_dialog(name, platform):
        existing_names = get_all_channel_names()
        is_valid, error_msg = validate_channel_name(name, platform, existing_names)
        if not is_valid:
            return False, error_msg
        ch_name = cp.build_channel_key(name, platform)
        if ch_name in existing_names:
            return False, f"{ch_name} is already in the list."
        ch = {"name": ch_name, "enabled": True}
        channels.append(ch)
        if roster_filter_var.get():
            roster_filter_var.set("")  # make sure the new row is visible
        if roster_view_var.get() == cp.VIEW_LIVE:
            roster_view_var.set(cp.VIEW_ALL)
        _populate_channel_tree()
        _select_roster_names([ch_name])
        save_channels()
        if recorder and recorder.is_running:
            recorder.start_channel(ch_name)
        elif recorder:
            recorder.status_dict[ch_name] = {"status": "Initializing", "detail": "", "size": "", "time": ""}
        return True, ""

    def add_channel(initial_text=""):
        t = _themes.current()
        cp.open_add_channel_dialog(root, t, _add_channel_from_dialog,
                                   initial_text=initial_text, dark=_themes.current().dark)

    def remove_selected():
        names = _selected_roster_names()
        if not names:
            return
        shown = "\n".join(names[:8]) + (f"\n… and {len(names) - 8} more" if len(names) > 8 else "")
        if not _dialogs.ask_yes_no(
                root,
                "Remove channel" + ("s" if len(names) > 1 else ""),
                f"Remove from the list (recordings on disk are kept)?\n\n{shown}\n\n"
                "You can bring the old list back with \u22ef \u2192 Restore previous channel list."):
            return
        drop = set(names)
        for ch_name in names:
            if recorder:
                recorder.remove_channel(ch_name)
        channels[:] = [ch for ch in channels if ch["name"] not in drop]
        _populate_channel_tree()
        save_channels()

    # ── Reorder (context menu; replaces the ▲ ▼ Top buttons) ──
    def _move_channel_step(direction):
        """Swap with the visible neighbour in the same group (up or down)."""
        names = _selected_roster_names()
        if not names:
            return
        iid = "c:" + names[0]
        rows = [i for i in ch_tree.get_children() if i.startswith("c:")
                and _row_group.get(i) == _row_group.get(iid)]
        if iid not in rows:
            return
        pos = rows.index(iid) + (-1 if direction == "up" else 1)
        if pos < 0 or pos >= len(rows):
            return
        anchor = _channel_name_from_item(rows[pos])
        if cp.move_relative(channels, names[0], anchor, after=(direction == "down")):
            _populate_channel_tree()
            save_channels()
            _select_roster_names([names[0]])

    def _move_channel_up():
        _move_channel_step("up")

    def _move_channel_down():
        _move_channel_step("down")

    def _move_selected_channels(to):
        """Jump selected roster rows to the top or bottom as a stable block."""
        names = _selected_roster_names()
        if not names:
            return
        indices = [_index_for_name(n) for n in names]
        new_idx = move_list_items(channels, indices, to=to)
        if not new_idx:
            return
        _populate_channel_tree()
        save_channels()
        moved = [channels[i]["name"] for i in new_idx]
        _select_roster_names(moved, see_first=(to == "top"))

    def _move_channels_to_top():
        _move_selected_channels("top")

    def _move_channels_to_bottom():
        _move_selected_channels("bottom")

    # ── Channel list sorting (one-shot, persisted; right-click → Sort) ──
    def _channel_sort_parts(ch):
        platform, user = cp.split_channel_key(ch["name"])
        return platform.lower(), user.lower()

    def _sort_channels(mode):
        if len(channels) < 2:
            return
        selected_names = set(_selected_roster_names())
        if mode == "enabled":
            channels.sort(key=lambda ch: not ch.get("enabled", True))
        elif mode == "platform":
            channels.sort(key=_channel_sort_parts)
        elif mode == "name":
            channels.sort(key=lambda ch: _channel_sort_parts(ch)[1])
        _populate_channel_tree()
        save_channels()
        if selected_names:
            _select_roster_names(list(selected_names))
        logging.info(f"Channel list sorted ({mode})")

    sort_menu = tk.Menu(root, tearoff=0)
    sort_menu.add_command(label="Enabled First", command=lambda: _sort_channels("enabled"))
    sort_menu.add_command(label="By Platform", command=lambda: _sort_channels("platform"))
    sort_menu.add_command(label="By Name (A–Z)", command=lambda: _sort_channels("name"))

    add_frame = tk.Frame(frame_left)
    add_frame.pack(fill=tk.X, pady=(8, 0))
    add_btn = tk.Button(add_frame, text="+  Add channel", command=add_channel,
                        font=("Segoe UI", 9), cursor="hand2")
    add_btn.pack(fill=tk.X, ipady=2)
    _Tooltip(add_btn, "Add a Kick / Twitch / YouTube / Rumble / TikTok /\n"
                      "Fishtank channel or a custom URL  (Ctrl+N or Insert).\n"
                      "Right-click a channel to enable, remove, or reorder.")

    def _search_enter(_e=None):
        text = roster_filter_var.get().strip()
        add_channel(text)
        return "break"

    filter_entry.bind("<Return>", _search_enter)
    _Tooltip(filter_entry, "Filter as you type (name or platform).\n"
                           "Enter: add a channel with this name.  Esc: clear.")

    # ── Cookie status line ──
    cookie_frame = tk.Frame(frame_left)
    cookie_frame.pack(fill=tk.X, pady=(8, 0))

    cookie_indicator = tk.Label(cookie_frame, text="●", font=("Segoe UI", 10))
    cookie_indicator.pack(side=tk.LEFT, padx=(0, 4))
    cookie_label = tk.Label(cookie_frame, text="Cookies: checking...", font=("Segoe UI", 8))
    cookie_label.pack(side=tk.LEFT)

    def _cookie_summary_tip():
        summary = cp.platform_cookie_summary(
            _cookie_info_cache[0], get_status=get_cookie_status_for_channel,
            fishtank_login=_fishtank_login)
        words = {"present": "ok", "missing": "no cookie", "expired": "EXPIRED",
                 "unknown": "unknown", "login": "email/password login"}
        lines = [f"{cp.PLATFORM_LABELS[p]}: {words.get(s, s)}" for p, s in summary.items()]
        return ("Per-site cookies (cookies.txt):\n" + "\n".join(lines) +
                f"\n\n{cp.BADGE_GLYPH} on a channel = its site needs attention.")

    _Tooltip(cookie_frame, _cookie_summary_tip)

    cookie_dot_state = ["", "muted"]  # glyph and theme token, so a theme change can repaint the dot

    def _cookie_dot(glyph, tone):
        cookie_dot_state[:] = [glyph, tone]
        cookie_indicator.configure(text=glyph, fg=_themes.current()[tone])

    def update_cookie_status():
        """Check cookies.txt, update the global indicator, and refresh badges."""
        cookies_path = find_cookies_file(config)
        info = validate_cookies(cookies_path)
        _cookie_info_cache[0] = info

        base_text = f"{info['total_cookies']} entries, {len(info['domains'])} domains"

        if not cookies_path:
            _cookie_dot("○", "muted")
            cookie_label.configure(text="No cookies.txt found")
        elif not info['valid']:
            _cookie_dot("●", "error")
            warning = info['warnings'][0] if info['warnings'] else "Invalid format"
            cookie_label.configure(text=f"Cookies: {warning}")
        elif info['has_expired_auth']:
            _cookie_dot("●", "warn")
            domains = ", ".join(info['expired_domains'][:2])
            cookie_label.configure(text=f"Cookies: auth expired ({domains})")
        else:
            expiry_note = ""
            if info['auth_expiry']:
                days_left = (info['auth_expiry'] - datetime.datetime.now()).days
                if days_left < 7:
                    expiry_note = f" (auth renew in {days_left}d)"
                    if days_left < 2:
                        _cookie_dot("●", "warn")
                    else:
                        _cookie_dot("●", "ok")
                else:
                    _cookie_dot("●", "ok")
            else:
                _cookie_dot("●", "ok")
            cookie_label.configure(text=f"Cookies: {base_text}{expiry_note}")

        _badges.clear()
        _badges.update(_compute_badges())
        _schedule_badge_redraw()

    _populate_channel_tree()
    _update_search_placeholder()
    update_cookie_status()

    def _periodic_cookie_check():
        update_cookie_status()
        root.after(300_000, _periodic_cookie_check)  # 5 minutes

    root.after(300_000, _periodic_cookie_check)

    def _theme_roster(t):
        """Left-panel colours; called from apply_theme."""
        border_color = t['border']
        style.configure('Roster.Treeview', background=t['tree_field'],
                        fieldbackground=t['tree_field'], foreground=t['tree_fg'],
                        rowheight=28, font=("Segoe UI", 10), borderwidth=0,
                        bordercolor=border_color, lightcolor=t['tree_field'],
                        darkcolor=t['tree_field'])
        style.map('Roster.Treeview', background=[('selected', t['select_bg'])],
                  foreground=[('selected', t['select_fg'])])
        style.layout('Roster.Treeview', [('Treeview.treearea', {'sticky': 'nswe'})])
        ch_tree.tag_configure("enabled", foreground=t['tree_fg'])
        ch_tree.tag_configure("disabled", foreground=t['check_fg'])
        ch_tree.tag_configure("header", foreground=t['offline_fg'],
                              font=("Segoe UI", 9, "bold"))
        for w in (frame_left, ch_tree_frame, search_frame, view_frame, add_frame,
                  cookie_frame):
            w.configure(bg=t['bg'])
        ch_tree_frame.configure(bg=t['tree_field'], highlightthickness=1,
                                highlightbackground=border_color)
        filter_entry.configure(bg=t['entry_bg'], fg=t['entry_fg'],
                               insertbackground=t['fg'], disabledforeground=t['disabled_fg'],
                               selectbackground=t['select_bg'], selectforeground=t['select_fg'],
                               highlightbackground=border_color,
                               highlightcolor=t['accent'],
                               highlightthickness=1, relief='flat')
        search_placeholder.configure(bg=t['entry_bg'], fg=t['placeholder'])
        for b in view_buttons:
            b.configure(bg=t['btn_bg'], fg=t['btn_fg'], selectcolor=t['accent'],
                        activebackground=t['btn_hover'], activeforeground=t['btn_hover_fg'],
                        disabledforeground=t['disabled_fg'], highlightthickness=0)
        _paint_view_buttons()
        add_btn.configure(bg=t['btn_bg'], fg=t['btn_fg'], activebackground=t['btn_hover'],
                          activeforeground=t['btn_hover_fg'], disabledforeground=t['disabled_fg'],
                          highlightbackground=border_color, relief='flat', bd=1)
        cookie_label.configure(bg=t['bg'], fg=t['fg'])
        cookie_indicator.configure(bg=t['bg'], fg=t[cookie_dot_state[1]])
        badge_canvas.configure(bg=t['tree_field'])
        _schedule_badge_redraw()

    def _paint_view_buttons(*_):
        t = _themes.current()
        for b in view_buttons:
            on = b.cget("value") == roster_view_var.get()
            b.configure(fg=t['accent_fg'] if on else t['btn_fg'])

    roster_view_var.trace_add("write", _paint_view_buttons)

    # ── Right-click context menu on channel list ──
    ctx_menu = tk.Menu(root, tearoff=0)

    def _start_selected_channel_from_list():
        """Start recording a channel from the channel list (mid-session)."""
        if not ch_tree.selection() or not recorder or not recorder.is_running:
            return
        for ch_name in _selected_roster_names():
            idx = _index_for_name(ch_name)
            if idx < 0:
                continue
            if not channels[idx].get("enabled", True):
                channels[idx]["enabled"] = True
                _populate_channel_tree()
                save_channels()
            recorder.start_channel(ch_name)

    def _stop_selected_channel_from_list():
        """Stop recording a channel from the channel list."""
        if not ch_tree.selection() or not recorder or not recorder.is_running:
            return
        names = _selected_roster_names()
        if not names:
            return
        rec = recorder

        def _bg():
            for ch_name in names:
                rec.stop_channel(ch_name)
            root.after(0, refresh_status)

        threading.Thread(target=_bg, daemon=True, name="stop-roster").start()

    def _check_now_selected_from_list():
        """Skip the poll timer and check the selected channel(s) immediately."""
        if not ch_tree.selection() or not recorder or not recorder.is_running:
            return
        for ch_name in _selected_roster_names():
            recorder.check_now(ch_name)

    def show_context_menu(event):
        item = ch_tree.identify_row(event.y)
        if item and item.startswith("h:"):
            return
        if item and item not in ch_tree.selection():
            ch_tree.selection_set(item)
        names = _selected_roster_names()

        ctx_menu.delete(0, tk.END)

        if names:
            all_on = all(channels[_index_for_name(n)].get("enabled", True)
                         for n in names if _index_for_name(n) >= 0)
            if all_on:
                ctx_menu.add_command(label="Disable", command=lambda: _set_enabled(names, False))
            else:
                ctx_menu.add_command(label="Enable", command=lambda: _set_enabled(names, True))

        if recorder and recorder.is_running and names:
            ch_name = names[0]
            idx = _index_for_name(ch_name)
            if 0 <= idx < len(channels):
                st = recorder.status_dict.get(ch_name, {})
                status_lower = st.get("status", "").lower()
                if status_lower in ("stopped",) or ch_name not in recorder.status_dict:
                    ctx_menu.add_command(label="Start Recording", command=_start_selected_channel_from_list)
                elif "recording" in status_lower or "checking" in status_lower or "offline" in status_lower or "initializing" in status_lower:
                    ctx_menu.add_command(label="Stop Recording", command=_stop_selected_channel_from_list)
                else:
                    ctx_menu.add_command(label="Start Recording", command=_start_selected_channel_from_list)
                if status_lower.startswith(("offline", "error")):
                    ctx_menu.add_command(label="Check Now", command=_check_now_selected_from_list)
        ctx_menu.add_separator()

        ctx_menu.add_command(label="Open stream page", command=open_channel_url)
        ctx_menu.add_command(label="Open Clips Folder", command=_open_roster_clips_dir)
        ctx_menu.add_command(label="Copy Name", command=copy_channel_name)
        ctx_menu.add_separator()
        ctx_menu.add_command(label="Move to Top", command=_move_channels_to_top)
        ctx_menu.add_command(label="Move Up", command=_move_channel_up)
        ctx_menu.add_command(label="Move Down", command=_move_channel_down)
        ctx_menu.add_command(label="Move to Bottom", command=_move_channels_to_bottom)
        ctx_menu.add_cascade(label="Sort", menu=sort_menu)
        ctx_menu.add_separator()
        ctx_menu.add_command(label="Enable All", command=_enable_all_channels)
        ctx_menu.add_command(label="Disable All", command=_disable_all_channels)
        ctx_menu.add_command(label="Add channel…", command=add_channel)
        ctx_menu.add_separator()
        ctx_menu.add_command(label="Remove…", command=remove_selected)

        try:
            ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            ctx_menu.grab_release()

    def copy_channel_name():
        names = _selected_roster_names()
        if names:
            root.clipboard_clear()
            root.clipboard_append("\n".join(names))

    def open_channel_url():
        names = _selected_roster_names()
        if names:
            _open_channel_in_browser(names[0])

    def _toggle_selected_channels():
        """Toggle enabled state for all selected channels."""
        for ch_name in _selected_roster_names():
            idx = _index_for_name(ch_name)
            if 0 <= idx < len(channels):
                channels[idx]["enabled"] = not channels[idx].get("enabled", True)
        _populate_channel_tree()
        save_channels()

    def _enable_all_channels():
        for ch in channels:
            ch["enabled"] = True
        _populate_channel_tree()
        save_channels()

    def _disable_all_channels():
        for ch in channels:
            ch["enabled"] = False
        _populate_channel_tree()
        save_channels()

    ch_tree.bind("<Button-3>", show_context_menu)

    # ── Keyboard shortcuts ──
    ch_tree.bind("<Delete>", lambda e: remove_selected())
    ch_tree.bind("<space>", lambda e: (_toggle_selected_channels(), "break")[1])
    ch_tree.bind("<Return>", lambda e: (add_channel(), "break")[1])
    ch_tree.bind("<Insert>", lambda e: add_channel())
    root.bind("<Control-n>", lambda e: add_channel())
    root.bind("<Control-f>", lambda e: (filter_entry.focus_set(), "break")[1])

    def _reload_channels_from_disk(_path=None):
        """Re-read channels.json after a restore (not saved again: the file is what was chosen)."""
        try:
            with open(channels_file, "r") as f:
                fresh, skipped = coerce_channel_records(json.load(f))
        except Exception as e:
            logging.error(f"Could not reload the restored channel list: {e}")
            return
        channels[:] = fresh
        _populate_channel_tree()
        logging.info("Channel list restored from history (%d channels)", len(fresh))

    def _restore_channels():
        def blocked():
            if recorder and recorder.is_running:
                return "Stop recording first, then restore the channel list."
            return None

        _restore_dialog.open_restore(root, channels_file, "Restore previous channel list",
                                     can_restore=blocked, on_restored=_reload_channels_from_disk,
                                     state_file=state_file)

    def _restore_settings():
        def restored(_path):
            config.reload()
            logging.info("config.ini restored from history")

        _restore_dialog.open_restore(
            root, config.config_file, "Restore previous settings (config.ini)", on_restored=restored,
            state_file=state_file, note="Restart Multi-Stream Recorder to use the restored settings.")

    def _about_dialog():
        _show_about(root, config)

    root.bind("<Control-q>", lambda e: _full_quit())
    root.bind("<F1>", lambda e: _about_dialog())

    # ── Right panel — Tabbed notebook ──
    frame_right = tk.Frame(root)
    frame_right.grid(row=0, column=1, sticky="nsew", padx=(5, 10), pady=10)
    frame_right.grid_rowconfigure(0, weight=1)
    frame_right.grid_columnconfigure(0, weight=1)

    notebook = ttk.Notebook(frame_right)
    notebook.grid(row=0, column=0, sticky="nsew")

    # ── Tab 1: Status ──
    status_tab = ttk.Frame(notebook)
    notebook.add(status_tab, text="  Status  ")

    status_header = tk.Frame(status_tab)
    status_header.pack(fill=tk.X, padx=10, pady=(10, 6))
    # The disk/network meter lives on its own thin row under the header: the header's buttons
    # (Clip Now, DVR, Clips, Screenshot, Streams) already fill the row at the default window width.
    status_meter_row = tk.Frame(status_tab)
    # Shown under the header only while the recordings folder is unreachable (an unplugged drive).
    storage_notice_label = tk.Label(status_tab, text="", anchor="w", justify="left",
                                    font=("Segoe UI", 9, "bold"), wraplength=820)
    _storage_notice_shown = [""]
    status_label = tk.Label(status_header, text="Live Recording Status",
                            font=("Segoe UI", 13, "bold"), anchor="w")
    status_label.pack(side=tk.LEFT)
    recording_first_cb = ttk.Checkbutton(
        status_header, text="Recording first",
        variable=recording_first_var,
        command=lambda: _reorder_status_rows() if recorder else None,
    )
    recording_first_cb.pack(side=tk.LEFT, padx=(12, 0))
    _Tooltip(recording_first_cb,
             "Keep Recording / Remuxing rows at the\n"
             "top of the status table, in roster order.")

    # Experimental live-frame strip. ffmpeg grabs a scaled still from each
    # recording's .ts on a worker thread every PREVIEW_MS. Sizes/rate are
    # starting points to tweak. PNG because tk.PhotoImage has no JPEG.
    PREVIEW_W, PREVIEW_H = 160, 90
    PREVIEW_BIG_W, PREVIEW_BIG_H = 480, 270  # 3× the strip tiles
    PREVIEW_MS = 4000
    _preview_photos = {}
    _preview_tiles = {}
    _preview_busy = [False]
    _preview_big = [None]
    _preview_big_photo = [None]
    _pstate = _preview_state.PreviewState()  # last_ok, working seek, back-off per channel (Tk-free)
    _preview_after = [None]   # the pending _tick_previews timer, so a kick can replace it
    _preview_kick = [False]   # an immediate tick is already queued

    preview_wrap = tk.Frame(status_tab)
    preview_wrap.pack(fill=tk.X, padx=10, pady=(0, 6))
    preview_big_frame = tk.Frame(preview_wrap, padx=6, pady=6, highlightthickness=1)
    preview_big_header = tk.Frame(preview_big_frame)
    preview_big_header.pack(fill=tk.X, pady=(0, 4))
    preview_big_name = tk.Label(
        preview_big_header, text="", font=("Segoe UI", 10, "bold"), anchor="w",
    )
    preview_big_name.pack(side=tk.LEFT)
    preview_big_close = ttk.Button(preview_big_header, text="Close", width=7)
    preview_big_close.pack(side=tk.RIGHT)
    preview_big_hint = tk.Label(
        preview_big_header, text="Double-click a tile to pin · Close to dismiss",
        font=("Segoe UI", 8), anchor="e",
    )
    preview_big_hint.pack(side=tk.RIGHT, padx=(8, 6))
    preview_big_img = tk.Label(preview_big_frame, bd=0)
    preview_big_img.pack()
    preview_canvas = tk.Canvas(
        preview_wrap, height=28, highlightthickness=1, bd=0,
    )
    preview_hsb = ttk.Scrollbar(preview_wrap, orient="horizontal",
                                command=preview_canvas.xview)
    preview_canvas.configure(xscrollcommand=preview_hsb.set)
    preview_canvas.pack(fill=tk.X, side=tk.TOP)
    preview_canvas.msr_drag_passive = True  # empty strip around the thumbnails (they are their own widgets)
    preview_hsb.pack(fill=tk.X, side=tk.TOP)
    preview_inner = tk.Frame(preview_canvas)
    _preview_canvas_win = preview_canvas.create_window((0, 0), window=preview_inner, anchor="nw")
    preview_empty = tk.Label(
        preview_inner,
        text="Live previews appear here while channels are recording",
        font=("Segoe UI", 9), anchor="w", padx=8, pady=4,
    )
    preview_empty.pack(side=tk.LEFT)

    def _preview_inner_configured(_event=None):
        preview_canvas.configure(scrollregion=preview_canvas.bbox("all"))

    preview_inner.bind("<Configure>", _preview_inner_configured)

    def _theme_preview_strip(t):
        preview_wrap.configure(bg=t["bg"])
        preview_canvas.configure(
            bg=t["tree_field"], highlightbackground=t["border"],
            highlightcolor=t["border"],
        )
        preview_inner.configure(bg=t["tree_field"])
        preview_empty.configure(bg=t["tree_field"], fg=t["offline_fg"])
        preview_big_frame.configure(
            bg=t["tree_field"], highlightbackground=t["accent"],
            highlightcolor=t["accent"],
        )
        preview_big_header.configure(bg=t["tree_field"])
        preview_big_name.configure(bg=t["tree_field"], fg=t["fg"])
        preview_big_hint.configure(bg=t["tree_field"], fg=t["offline_fg"])
        preview_big_img.configure(bg=t["tree_field"], fg=t["fg"])
        for tile in _preview_tiles.values():
            tile["frame"].configure(bg=t["tree_field"], highlightbackground=t["border"])
            tile["img"].configure(bg=t["tree_field"])
            tile["name"].configure(bg=t["tree_field"], fg=t["fg"])
        try:
            _paint_preview_selection(_preview_selected[0])
        except NameError:
            pass

    # _key is a hidden column holding the internal channel id (e.g. "twitch:foo"
    # vs Kick "foo") so Clip Now / Stop / Restart never pick the wrong row when
    # two platforms share a display name.
    VISIBLE_STATUS_COLUMNS = ("Channel", "Status", "Size", "Elapsed", "Platform")
    columns = VISIBLE_STATUS_COLUMNS + ("_key",)
    tree = ttk.Treeview(status_tab, columns=columns, displaycolumns=VISIBLE_STATUS_COLUMNS,
                        show="headings", height=20)
    tree.heading("Channel", text="Channel")
    tree.heading("Status", text="Status")
    tree.heading("Size", text="Size")
    tree.heading("Elapsed", text="Elapsed")
    tree.heading("Platform", text="Platform")
    tree.column("Channel", width=200, anchor="w")
    tree.column("Status", width=300, anchor="w")
    tree.column("Size", width=90, anchor="center")
    tree.column("Elapsed", width=80, anchor="center")
    tree.column("Platform", width=75, anchor="center")
    tree.column("_key", width=0, stretch=False)

    # Restore column widths saved from last session
    saved_widths = win_state.get('column_widths', {})
    for col in VISIBLE_STATUS_COLUMNS:
        if col in saved_widths:
            try:
                tree.column(col, width=int(saved_widths[col]))
            except (ValueError, tk.TclError):
                pass
    tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

    # ── Status tree right-click context menu ──
    status_ctx_menu = tk.Menu(root, tearoff=0)

    def _get_selected_status_channel():
        """Return the internal channel key of the selected status-tree row.

        Reads the hidden ``_key`` column (not the display name) so Kick ``foo``
        and ``twitch:foo`` stay distinct for Clip Now / Stop / Restart.
        """
        selected = tree.selection()
        if not selected:
            return None
        try:
            key = tree.set(selected[0], "_key")
        except tk.TclError:
            return None
        return key or None

    def _stop_selected_channel():
        ch_name = _get_selected_status_channel()
        if not ch_name or not recorder or not recorder.is_running:
            return
        st = recorder.status_dict.get(ch_name, {})
        if st.get("status", "").lower() in ("stopped",):
            return  # already stopped
        rec = recorder

        def _bg():
            rec.stop_channel(ch_name)
            root.after(0, refresh_status)

        threading.Thread(target=_bg, daemon=True, name=f"stop-{ch_name}").start()

    def _start_selected_channel():
        ch_name = _get_selected_status_channel()
        if not ch_name or not recorder or not recorder.is_running:
            return
        st = recorder.status_dict.get(ch_name, {})
        if st.get("status", "").lower() not in ("stopped",):
            return  # only restart channels that were individually stopped
        recorder.start_channel(ch_name)
        root.after(0, refresh_status)  # reflect restarting state immediately

    def _check_now_selected_channel():
        """Skip the poll timer and check the selected channel immediately."""
        ch_name = _get_selected_status_channel()
        if not ch_name or not recorder or not recorder.is_running:
            return
        recorder.check_now(ch_name)

    def _open_channel_in_browser(ch_name):
        url = channel_watch_url(ch_name)
        if not url:
            logging.info(f"Open in Browser: no URL for {ch_name}")
            return
        import webbrowser
        webbrowser.open(url)

    def _open_status_channel_url():
        ch_name = _get_selected_status_channel()
        if not ch_name:
            return
        _open_channel_in_browser(ch_name)

    # ── Instant clip / screenshot — cut from the live .ts without
    # touching the recording worker (see create_clip / create_screenshot) ──
    def _clips_output_dir(platform, username_dir):
        """Resolve the output directory for clips/screenshots of a channel,
        honoring the clips_dir config override (blank = streams_dir/Clips)."""
        base = config.get('Clipping', 'clips_dir', fallback='').strip()
        if not base:
            base = os.path.join(config.get('Paths', 'streams_dir'), 'Clips')
        return os.path.join(base, platform, username_dir)

    def _run_clip_job(ch_name, kind):
        """Off the GUI thread: resolve the live .ts, cut a clip or grab a
        screenshot, and report the result via log line + desktop notification."""
        display = _get_display_name(ch_name)
        label = "Clip" if kind == "clip" else "Screenshot"

        raw_file = find_active_recording_file(recorder, ch_name)
        if not raw_file:
            logging.warning(f"{label}: no active recording file found for {display}")
            send_notification(f"{label} failed", f"{display}: no active recording found",
                              category="error", channel=ch_name)
            return

        platform, username_dir = channel_key_to_dirs(ch_name)
        out_dir = _clips_output_dir(platform, username_dir)
        file_stem = channel_file_stem(username_dir)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        ffmpeg_path = config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg') or 'ffmpeg'

        if kind == "clip":
            clip_seconds = config.getint('Clipping', 'clip_length_seconds', fallback=30)
            out_file = os.path.join(out_dir, f"{file_stem}_{ts}_{clip_seconds}s.mp4")
            ok, msg = create_clip(raw_file, out_file, clip_seconds, ffmpeg_path, logging)
        else:
            shot_fmt = config.get('Clipping', 'screenshot_format', fallback='jpg')
            shot_quality = config.getint('Clipping', 'screenshot_quality', fallback=2)
            ext = screenshot_extension(shot_fmt)
            out_file = os.path.join(out_dir, f"{file_stem}_{ts}{ext}")
            ok, msg = create_screenshot(raw_file, out_file, ffmpeg_path, logging,
                                        fmt=shot_fmt, quality=shot_quality)

        if ok:
            logging.info(f"{label} saved for {display}: {out_file}")
            send_notification(f"{label} saved", f"{display} — {os.path.basename(out_file)}",
                              category="complete", channel=ch_name)
        else:
            logging.error(f"{label} failed for {display}: {msg}")
            send_notification(f"{label} failed", f"{display}: {msg}",
                              category="error", channel=ch_name)

    # (channel_key, kind) pairs with a clip/screenshot already running.
    # Clip Now used to spawn a new ffmpeg per click, stacking probes on the
    # same live .ts; ignore repeats until the in-flight job finishes.
    _media_jobs_in_flight = set()

    def _start_media_job(ch_name, kind, start_msg):
        """Run a clip or screenshot off the GUI thread, at most one per channel+kind."""
        job_key = (ch_name, kind)
        label = "Clip Now" if kind == "clip" else "Screenshot Now"
        if job_key in _media_jobs_in_flight:
            logging.info(f"{label}: already in progress for {_get_display_name(ch_name)} — ignored")
            return
        _media_jobs_in_flight.add(job_key)
        logging.info(start_msg)

        def _run():
            try:
                _run_clip_job(ch_name, kind)
            finally:
                _media_jobs_in_flight.discard(job_key)

        threading.Thread(target=_run, daemon=True,
                         name=f"{'clip' if kind == 'clip' else 'shot'}-{ch_name}").start()

    def _clip_selected_channel():
        ch_name = _get_selected_status_channel()
        if not ch_name:
            logging.info("Clip Now: select a channel in the status list first")
            return
        if not recorder or not recorder.is_running:
            logging.info("Clip Now: recording is not running")
            return
        st = recorder.status_dict.get(ch_name, {})
        if not channel_status_allows_clip(st.get("status", "")):
            logging.info(f"Clip Now: {_get_display_name(ch_name)} is not currently recording")
            return
        clip_seconds = config.getint('Clipping', 'clip_length_seconds', fallback=30)
        _start_media_job(
            ch_name, "clip",
            f"Clip Now: cutting last {clip_seconds}s for {_get_display_name(ch_name)}",
        )

    def _screenshot_selected_channel():
        ch_name = _get_selected_status_channel()
        if not ch_name:
            logging.info("Screenshot Now: select a channel in the status list first")
            return
        if not recorder or not recorder.is_running:
            logging.info("Screenshot Now: recording is not running")
            return
        st = recorder.status_dict.get(ch_name, {})
        if not channel_status_allows_clip(st.get("status", "")):
            logging.info(f"Screenshot Now: {_get_display_name(ch_name)} is not currently recording")
            return
        _start_media_job(
            ch_name, "screenshot",
            f"Screenshot Now: grabbing a frame for {_get_display_name(ch_name)}",
        )

    def _clips_base():
        return (config.get('Clipping', 'clips_dir', fallback='').strip()
                or os.path.join(config.get('Paths', 'streams_dir'), 'Clips'))

    def _open_clip_editor(path=None, ch_name=None, live=False):
        """The Clip Editor on a recording in progress (live) or a finished file."""
        from msr import clip_editor as _clip_editor
        rec = recorder

        def follow():
            return find_active_recording_file(rec, ch_name) if (rec is not None and ch_name) else None
        try:
            _clip_editor.open_editor(
                root, path, ffmpeg=config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg') or 'ffmpeg',
                clips_base=_clips_base(), title_name=_get_display_name(ch_name) if ch_name else "",
                live=live, follow=follow if live else None, state_file=state_file, logger=logging,
                opener=open_local_path)
        except Exception as e:
            logging.error(f"Clip Editor: could not open: {e}")
            _dialogs.show_error(root, "Clip Editor", f"Could not open the Clip Editor: {e}")

    def _newest_processed(ch_name):
        try:
            platform, username_dir = channel_key_to_dirs(ch_name)
            folder = os.path.join(config.get('Paths', 'streams_dir'), 'Processed', platform, username_dir)
            mp4s = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith('.mp4')]
            return max(mp4s, key=os.path.getmtime) if mp4s else None
        except (OSError, ValueError):
            return None

    def _clip_editor_selected_channel(_event=None):
        """Clip Editor for the selected channel: its recording in progress, else its newest
        finished recording."""
        ch_name = _get_selected_status_channel()
        if not ch_name:
            _dialogs.show_info(root, "DVR / Clip Editor",
                               "Select a channel in the status list first.\n\n"
                               "While it records you can scrub back and cut a clip; otherwise its "
                               "newest finished recording opens.")
            return "break"
        raw = None
        if recorder and recorder.is_running:
            st = recorder.status_dict.get(ch_name, {})
            if channel_status_allows_clip(st.get("status", "")):
                raw = find_active_recording_file(recorder, ch_name)
        if raw:
            _open_clip_editor(raw, ch_name, live=True)
        else:
            done = _newest_processed(ch_name)
            if done:
                _open_clip_editor(done, ch_name, live=False)
            else:
                _dialogs.show_info(root, "Clip Editor",
                                   f"{_get_display_name(ch_name)} is not recording and has no finished "
                                   "recording yet.")
        return "break"

    def _open_clips_dir_for_channel(ch_name):
        """Open Explorer/Finder on this channel's clips & screenshots folder."""
        if not ch_name:
            logging.info("Open Clips Folder: select a channel first")
            return
        try:
            platform, username_dir = channel_key_to_dirs(ch_name)
            path = _clips_output_dir(platform, username_dir)
            os.makedirs(path, exist_ok=True)
            open_local_path(path)
        except Exception as e:
            logging.warning(f"Open Clips Folder: {e}")

    def _open_selected_clips_dir():
        ch_name = _get_selected_status_channel()
        if not ch_name:
            logging.info("Open Clips Folder: select a channel in the status list first")
            return
        _open_clips_dir_for_channel(ch_name)

    def _open_roster_clips_dir():
        selected_items = ch_tree.selection()
        if not selected_items:
            logging.info("Open Clips Folder: select a channel in the list first")
            return
        names = _selected_roster_names()
        if names:
            _open_clips_dir_for_channel(names[0])

    def _open_recordings_folder():
        """The Streams button: open streams_dir (Recorded/Processed live below it). Creates
        nothing; a themed popup says so if the folder is not there yet."""
        ok, why = open_existing_folder(config.get("Paths", "streams_dir", fallback=""),
                                       default_streams_dir())
        if not ok:
            _dialogs.show_info(root, "Streams folder", why)

    # Left to right: Clip Now (primary, accent) | DVR | Clips | Screenshot | Streams.
    # pack(side=RIGHT) puts the first packed button rightmost, so they are packed in reverse.
    streams_folder_btn = ttk.Button(status_header, text="Streams",
                                    command=_open_recordings_folder, width=8)
    streams_folder_btn.pack(side=tk.RIGHT)
    _Tooltip(streams_folder_btn,
             "Open the recordings folder\n"
             "(streams_dir: Recorded, Processed, Clips...).")

    screenshot_now_btn = ttk.Button(status_header, text="Screenshot",
                                    command=_screenshot_selected_channel, width=11)
    screenshot_now_btn.pack(side=tk.RIGHT, padx=(0, 6))
    _Tooltip(screenshot_now_btn,
             "Grab a still frame from the channel\n"
             "selected in the status list.")

    clips_folder_btn = ttk.Button(status_header, text="Clips",
                                  command=_open_selected_clips_dir, width=6)
    clips_folder_btn.pack(side=tk.RIGHT, padx=(0, 6))
    _Tooltip(clips_folder_btn,
             "Open the clips & screenshots folder\n"
             "of the channel selected in the status list.")

    dvr_btn = ttk.Button(status_header, text="DVR",
                         command=_clip_editor_selected_channel, width=6)
    dvr_btn.pack(side=tk.RIGHT, padx=(0, 6))
    _Tooltip(dvr_btn,
             "Scrub back through a live recording and cut a clip\n"
             "(Clip Editor: In/Out marks, sound, live edge).\n"
             "Not recording? Opens the channel's newest finished recording.")

    clip_now_btn = ttk.Button(status_header, text="Clip Now", style="Accent.TButton",
                              command=_clip_selected_channel, width=9)
    clip_now_btn.pack(side=tk.RIGHT, padx=(0, 6))
    clip_now_btn.bind("<Shift-Button-1>", _clip_editor_selected_channel)
    _Tooltip(clip_now_btn,
             "Cut the last N seconds (Clip Length setting)\n"
             "of the channel selected in the status list.\n"
             "Shift+click or DVR: open the Clip Editor.")

    # Whole-disk write + NIC download, right-aligned on its own row under the header.
    # Hidden when psutil is missing — there is no fallback counter.
    _io_sampler = IoSampler(config.get("Paths", "streams_dir"))
    io_meter_label = tk.Label(
        status_meter_row, text=format_header(_io_sampler.last),
        font=("Segoe UI", 9), anchor="e", width=32,
    )

    def _paint_io_meter():
        if not HAS_PSUTIL:
            return
        t = _themes.current()
        snap = _io_sampler.last
        sev = meter_severity(snap.disk_busy_frac)
        if sev == "hot":
            fg = t["error_fg"]
        elif sev == "warn":
            fg = t["remux_fg"]
        else:
            fg = t["fg"]
        io_meter_label.configure(bg=t["bg"], fg=fg, text=format_header(snap))

    def _refresh_io_meter():
        sizes = {}
        details = []
        try:
            if recorder and recorder.is_running:
                for ch_name, st in recorder.status_dict.items():
                    if not str(st.get("status", "")).lower().startswith("recording"):
                        continue
                    details.append(st.get("detail", ""))
                    raw = st.get("size_bytes")
                    if isinstance(raw, (int, float)) and raw >= 0:
                        sizes[ch_name] = int(raw)
        except Exception:
            pass
        _io_sampler.sample(
            msr_sizes=sizes,
            msr_stream_mbps=sum_stream_mbps(details),
        )
        _paint_io_meter()
        root.after(1000, _refresh_io_meter)

    if HAS_PSUTIL:
        status_meter_row.pack(fill=tk.X, padx=10, pady=(0, 4), after=status_header)
        io_meter_label.pack(side=tk.RIGHT)
        _Tooltip(io_meter_label, lambda: format_tooltip(_io_sampler.last))

    def _show_status_context_menu(event):
        """Show context menu on status tree with options appropriate to channel state."""
        item = tree.identify_row(event.y)
        if not item:
            return
        tree.selection_set(item)

        # Rebuild menu based on channel state
        status_ctx_menu.delete(0, tk.END)

        ch_name = _get_selected_status_channel()
        if ch_name and recorder and recorder.is_running:
            st = recorder.status_dict.get(ch_name, {})
            status_lower = st.get("status", "").lower()

            if status_lower == "stopped":
                status_ctx_menu.add_command(label="Restart Channel", command=_start_selected_channel)
            else:
                status_ctx_menu.add_command(label="Stop Channel", command=_stop_selected_channel)
                if status_lower.startswith(("offline", "error")):
                    status_ctx_menu.add_command(label="Check Now", command=_check_now_selected_channel)
                if channel_status_allows_clip(status_lower):
                    clip_seconds = config.getint('Clipping', 'clip_length_seconds', fallback=30)
                    status_ctx_menu.add_command(
                        label=f"Clip Now ({clip_seconds}s)", command=_clip_selected_channel)
                    status_ctx_menu.add_command(
                        label="Screenshot Now", command=_screenshot_selected_channel)
                status_ctx_menu.add_command(label="Clip Editor\u2026", command=_clip_editor_selected_channel)

            status_ctx_menu.add_separator()

        status_ctx_menu.add_command(label="Open in Browser", command=_open_status_channel_url)
        status_ctx_menu.add_command(label="Open Clips Folder", command=_open_selected_clips_dir)

        try:
            status_ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            status_ctx_menu.grab_release()

    tree.bind("<Button-3>", _show_status_context_menu)

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
        """The file the app is logging to (the shared rotating handler), else the default."""
        for handler in logging.root.handlers:
            name = getattr(handler, "baseFilename", None)
            if name:
                return name
        return os.path.join(config.get('Paths', 'streams_dir'), "stream_recorder.log")

    def open_log_file():
        def _notepad(path):
            subprocess.Popen(["notepad.exe", path])  # .log has no program on some PCs
        ok, why = logview.open_log_file(_log_file_path(), open_local_path,
                                        _notepad if os.name == "nt" else None)
        if not ok:
            _dialogs.show_info(root, "Open log file", why)

    ttk.Button(log_toolbar, text="Open log file", command=open_log_file).pack(side=tk.RIGHT, padx=(0, 6))

    # Filter: words (all must match, any case) and a minimum level.
    log_filter_var = tk.StringVar()
    log_level_var = tk.StringVar(value=logview.level_label(logview.LEVEL_ALL))
    log_filter_info = ttk.Label(log_toolbar, text="")
    ttk.Label(log_toolbar, text="Filter:").pack(side=tk.LEFT, padx=(18, 4))
    log_filter_entry = ttk.Entry(log_toolbar, textvariable=log_filter_var, width=24)
    log_filter_entry.pack(side=tk.LEFT)
    log_filter_clear = ttk.Button(log_toolbar, text="\u2715", width=3,
                              command=lambda: (log_filter_var.set(""), log_filter_entry.focus_set()))
    log_filter_clear.pack(side=tk.LEFT, padx=(2, 6))
    log_level_box = ttk.Combobox(log_toolbar, textvariable=log_level_var, state="readonly", width=19,
                             values=[label for _key, label in logview.LEVEL_CHOICES])
    log_level_box.pack(side=tk.LEFT)
    log_filter_info.pack(side=tk.LEFT, padx=(8, 0))
    _log_filter_job = [None]

    def _apply_log_filter(*_args):
        _log_filter_job[0] = None
        log_follow.set_filter(log_filter_var.get(), logview.level_key(log_level_var.get()))

    def _log_filter_changed(*_args):
        # Wait for a pause in typing, then re-render (up to 5000 lines).
        if _log_filter_job[0] is not None:
            root.after_cancel(_log_filter_job[0])
        _log_filter_job[0] = root.after(250, _apply_log_filter)

    log_filter_var.trace_add("write", _log_filter_changed)
    log_level_box.bind("<<ComboboxSelected>>", lambda _e: (_apply_log_filter(), log_level_box.selection_clear()))
    log_filter_entry.bind("<Escape>", lambda _e: log_filter_var.set(""))

    log_text = tk.Text(log_tab, wrap=tk.WORD, font=("Consolas", 9), state=tk.DISABLED,
                       borderwidth=0, relief="flat")
    log_scroll = ttk.Scrollbar(log_tab, orient=tk.VERTICAL, command=log_text.yview)
    new_lines_btn = ttk.Button(log_tab, text="\u2193 New lines")

    def _show_new_lines(count):
        # Auto-scroll follows the log only while you are at the bottom; when you have
        # scrolled back, new lines wait and this button jumps to them.
        if count > 0:
            new_lines_btn.configure(text=f"\u2193 {count} new line{'s' if count != 1 else ''}")
            new_lines_btn.place(relx=1.0, rely=1.0, x=-34, y=-18, anchor="se")
            new_lines_btn.lift()
        else:
            new_lines_btn.place_forget()

    def _log_filter_stats(shown, total):
        log_filter_info.configure(text=f"{shown} of {total} lines" if log_follow.filtering else "")

    log_follow = logview.LogFollower(log_text, enabled=auto_scroll_var.get, on_behind=_show_new_lines,
                                     on_stats=_log_filter_stats)
    new_lines_btn.configure(command=log_follow.jump)

    def _log_yscroll(first, last):
        log_scroll.set(first, last)
        log_follow.on_scroll(first, last)

    log_text.configure(yscrollcommand=_log_yscroll)

    def _focus_log_filter(_event=None):
        # Ctrl+F on the Logs tab goes to the filter box
        try:
            if notebook.select() == str(log_tab):
                log_filter_entry.focus_set()
                log_filter_entry.select_range(0, tk.END)
                return "break"
        except tk.TclError:
            pass
        return None

    def _ctrl_f(_event=None):
        # One Ctrl+F handler (a second root binding would never run: the first returns "break"):
        # the Logs tab filters the log, any other tab filters the roster.
        if _focus_log_filter() == "break":
            return "break"
        filter_entry.focus_set()
        return "break"

    root.bind("<Control-f>", _ctrl_f)
    log_scroll.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 10), pady=(0, 10))
    log_text.pack(fill=tk.BOTH, expand=True, padx=(10, 0), pady=(0, 10))

    # Library tab: size per site/channel, charts, per-month totals. Scans (read-only, in a
    # background thread) the first time it is opened; remembered video lengths live in the
    # app's own data folder (library_cache.json, git-ignored).
    def _library_purge_settings():
        try:
            days = config.getint('Cleanup', 'auto_purge_days', fallback=7)
        except ValueError:
            days = 7
        return days, config.getboolean('Cleanup', 'purge_on_startup', fallback=True)

    def _library_busy():
        # which channels / files the recorder is working on right now (they are never touched)
        return _library_actions.busy_from_status(dict(getattr(recorder, 'status_dict', None) or {}))

    library_data_dir = os.path.dirname(os.path.abspath(config.config_file))
    library_panel = _library_window.LibraryPanel(
        notebook,
        manage_ctx=_library_manage.ManageContext(
            data_dir=library_data_dir,
            log_path=os.path.join(library_data_dir, "library_actions.log"),
            manifest_path=os.path.join(library_data_dir, "library_moves.json"),
            get_busy=_library_busy, get_purge=_library_purge_settings,
            revealer=reveal_in_file_browser, opener=open_local_path, logger=logging.getLogger("msr.library"),
            clip_editor=lambda p: _open_clip_editor(p)),
        get_roots=lambda: _library.roots_from_config(config),
        get_roster=lambda: [c["name"] for c in channels],
        cache_path=os.path.join(os.path.dirname(os.path.abspath(config.config_file)), "library_cache.json"),
        ffprobe=ffprobe_from_ffmpeg(config.get("Advanced", "ffmpeg_path", fallback="ffmpeg") or "ffmpeg"),
        opener=open_local_path)
    _library_panel[0] = library_panel
    notebook.add(library_panel.frame, text="  Library  ")

    def _library_tab_opened(_event=None):
        try:
            if notebook.select() == str(library_panel.frame):
                library_panel.activate()
        except tk.TclError:
            pass

    notebook.bind("<<NotebookTabChanged>>", _library_tab_opened, add="+")

    # ── Bottom bar — buttons and toggles ──
    bottom_bar = tk.Frame(root)
    bottom_bar.grid(row=1, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))

    btn_frame = tk.Frame(bottom_bar)
    btn_frame.pack(side=tk.LEFT, padx=10)

    start_button = tk.Button(
        btn_frame, text=" Start Recording ", command=lambda: start_recording(),
        bg=_themes.current()['start_bg'], fg=_themes.current()['start_fg'],
        font=("Segoe UI", 11, "bold"), width=18, height=2,
    )
    start_button.pack(side=tk.LEFT, padx=(0, 12))

    stop_button = tk.Button(
        btn_frame, text=" Stop Recording ", command=lambda: stop_recording(),
        bg=_themes.current()['stop_bg'], fg=_themes.current()['stop_fg'],
        font=("Segoe UI", 11, "bold"), width=18, height=2,
        state=tk.DISABLED,
    )
    stop_button.pack(side=tk.LEFT)

    toggle_frame = tk.Frame(bottom_bar)
    toggle_frame.pack(side=tk.RIGHT, padx=10)

    def _apply_chosen_theme(theme):
        """Live preview and final apply of a theme (no restart)."""
        _themes.set_current(theme)
        apply_theme()

    def _save_chosen_theme(theme):
        """Remember the choice in config.ini ([GUI] theme; dark_mode is kept in step for older versions)."""
        config.config.set('GUI', 'theme', theme.name)
        config.config.set('GUI', 'dark_mode', 'true' if theme.dark else 'false')
        try:
            config.save()
        except Exception as exc:
            logging.warning(f"Could not save the theme choice: {exc}")

    def open_theme_dialog():
        _theme_dialog.open_theme_dialog(
            root, apply=_apply_chosen_theme, save=_save_chosen_theme, reload=_load_user_themes,
            folder=user_themes_dir, opener=open_local_path, problems=lambda: list(user_theme_problems))

    theme_button = ttk.Button(toggle_frame, text="Theme...", command=open_theme_dialog)
    theme_button.pack(side=tk.RIGHT, padx=8)

    # ── Polling speed selector ──
    POLL_PRESETS = {
        "Relaxed (5 min)": 5.0,
        "Normal (3 min)": 3.0,
        "Fast (1 min)": 1.0,
    }
    CUSTOM_POLL_LABEL = "Custom…"
    POLL_MIN_MINUTES = 0.5   # floor — protects against rate limiting / IP bans
    POLL_MAX_MINUTES = 120.0

    def _custom_poll_display(minutes):
        return f"Custom ({minutes:g} min)"

    poll_label = tk.Label(toggle_frame, text="Polling:", font=("Segoe UI", 9))

    current_poll = config.getfloat('Timeouts', 'poll_interval_minutes', fallback=3.0)
    # Find the matching preset name, or show it as a custom value
    poll_default = _custom_poll_display(current_poll)
    for name, val in POLL_PRESETS.items():
        if abs(val - current_poll) < 0.1:
            poll_default = name
            break
    poll_var = tk.StringVar(value=poll_default)
    _last_poll_display = [poll_default]  # remembered so a cancelled dialog can revert

    def _apply_poll_interval(minutes, display_name):
        """Persist a new poll interval and push it to running workers."""
        config.config.set('Timeouts', 'poll_interval_minutes', str(minutes))
        # Write to config.ini so it survives restarts
        try:
            config.save()
        except Exception:
            pass
        # Live-apply: workers re-read the shared runtime dict each cycle,
        # and set_poll_interval wakes them so the change is immediate.
        if recorder and recorder.is_running:
            recorder.set_poll_interval(minutes)
        _last_poll_display[0] = display_name
        logging.info(f"Polling interval changed to {minutes} minutes ({display_name})")

    def on_poll_change(*_args):
        selected = poll_var.get()
        if selected == CUSTOM_POLL_LABEL:
            minutes = _dialogs.ask_float(
                root,
                "Custom Polling Interval",
                f"Minutes between offline checks "
                f"({POLL_MIN_MINUTES:g}–{POLL_MAX_MINUTES:g}):\n\n"
                f"Values below {POLL_MIN_MINUTES:g} min are not allowed — "
                f"checking too often risks rate limiting or IP bans.",
                minvalue=POLL_MIN_MINUTES, maxvalue=POLL_MAX_MINUTES)
            if minutes is None:
                # Cancelled — restore whatever was selected before
                poll_var.set(_last_poll_display[0])
                return
            display = _custom_poll_display(minutes)
            poll_var.set(display)
            _apply_poll_interval(minutes, display)
        else:
            minutes = POLL_PRESETS.get(selected, 3.0)
            _apply_poll_interval(minutes, selected)

    poll_combo = ttk.Combobox(toggle_frame, textvariable=poll_var,
                              values=list(POLL_PRESETS.keys()) + [CUSTOM_POLL_LABEL],
                              state="readonly", width=14)
    poll_combo.pack(side=tk.RIGHT, padx=(0, 6))
    poll_combo.bind("<<ComboboxSelected>>", on_poll_change)
    # side=RIGHT packs first=rightmost, so the combo must be packed before
    # its label or the caption sits on the *next* control to the right.
    poll_label.pack(side=tk.RIGHT, padx=(0, 2))

    # ── Clip length selector — length used by "Clip Now" in the status
    # tree's right-click menu (see _clip_selected_channel below) ──
    CLIP_PRESETS = {
        "15 sec": 15,
        "30 sec": 30,
        "1 min": 60,
        "2 min": 120,
        "5 min": 300,
    }
    CUSTOM_CLIP_LABEL = "Custom…"
    CLIP_MIN_SECONDS = 5
    CLIP_MAX_SECONDS = 1800  # 30 min — a stream-copy cut stays fast even at
                              # this length, no real reason to allow more

    def _custom_clip_display(seconds):
        return f"Custom ({seconds}s)"

    clip_label = tk.Label(toggle_frame, text="Clip Length:", font=("Segoe UI", 9))

    current_clip_len = config.getint('Clipping', 'clip_length_seconds', fallback=30)
    clip_default = _custom_clip_display(current_clip_len)
    for name, val in CLIP_PRESETS.items():
        if val == current_clip_len:
            clip_default = name
            break
    clip_var = tk.StringVar(value=clip_default)
    _last_clip_display = [clip_default]  # remembered so a cancelled dialog can revert

    def _apply_clip_length(seconds, display_name):
        """Persist the clip length so it survives restarts. Nothing needs to
        be pushed to workers — clips are cut on demand from the main process,
        not by the recording workers themselves."""
        config.config.set('Clipping', 'clip_length_seconds', str(int(seconds)))
        try:
            config.save()
        except Exception:
            pass
        _last_clip_display[0] = display_name
        logging.info(f"Clip length changed to {int(seconds)}s ({display_name})")

    def on_clip_length_change(*_args):
        selected = clip_var.get()
        if selected == CUSTOM_CLIP_LABEL:
            seconds = _dialogs.ask_int(
                root,
                "Custom Clip Length",
                f"Seconds to grab when clipping a live recording "
                f"({CLIP_MIN_SECONDS}–{CLIP_MAX_SECONDS}):",
                minvalue=CLIP_MIN_SECONDS, maxvalue=CLIP_MAX_SECONDS)
            if seconds is None:
                # Cancelled — restore whatever was selected before
                clip_var.set(_last_clip_display[0])
                return
            display = _custom_clip_display(seconds)
            clip_var.set(display)
            _apply_clip_length(seconds, display)
        else:
            seconds = CLIP_PRESETS.get(selected, 30)
            _apply_clip_length(seconds, selected)

    clip_combo = ttk.Combobox(toggle_frame, textvariable=clip_var,
                              values=list(CLIP_PRESETS.keys()) + [CUSTOM_CLIP_LABEL],
                              state="readonly", width=12)
    clip_combo.pack(side=tk.RIGHT, padx=(0, 6))
    clip_combo.bind("<<ComboboxSelected>>", on_clip_length_change)
    clip_label.pack(side=tk.RIGHT, padx=(0, 2))
    _Tooltip(clip_combo,
             "Length of the clip cut when you use\n'Clip Now' on a recording channel\n"
             "(button above the status list, or\nright-click the channel).")

    # ── Check Now button — skip the poll timer on all active channels ──
    def _check_all_now():
        if recorder and recorder.is_running:
            recorder.check_all_now()
        else:
            logging.info("Check Now clicked but no recording session is running")

    check_now_btn = ttk.Button(toggle_frame, text="Check Now",
                               command=_check_all_now, width=10)
    check_now_btn.pack(side=tk.RIGHT, padx=(0, 6))
    _Tooltip(check_now_btn,
             "Skip the wait — immediately check all\nenabled channels for live streams.\n(Right-click a single channel for a\nper-channel Check Now.)")

    about_btn = ttk.Button(toggle_frame, text="About", command=_about_dialog, width=6)
    about_btn.pack(side=tk.RIGHT, padx=(0, 4))

    # Updates: dependency versions and pip updates (msr/depsupdate.py, msr/deps_dialog.py)
    from msr import depsupdate as _depsupdate

    def _set_updates_badge(names):
        updates_btn.configure(text="Updates \u2022" if names else "Updates\u2026")

    def _open_updates(event=None):
        from msr.deps_dialog import open_deps_dialog
        open_deps_dialog(root, config=config, state_file=state_file,
                         ffmpeg_path=config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg') or 'ffmpeg',
                         on_msr_update=lambda: _begin_in_app_update(), on_badge=_set_updates_badge)

    updates_btn = ttk.Button(toggle_frame, text="Updates\u2026", command=_open_updates, width=10)
    updates_btn.pack(side=tk.RIGHT, padx=(0, 4))
    _Tooltip(updates_btn, "Versions of MSR, yt-dlp, streamlink, ffmpeg and Python packages;\n"
                          "check and install updates. A dot means updates are available.")
    _set_updates_badge(_depsupdate.remembered_updates(win_state))
    more_btn = ttk.Menubutton(toggle_frame, text="\u22ef", width=3)
    more_menu = tk.Menu(more_btn, tearoff=0)
    more_menu.add_command(label="Library summary\u2026", command=lambda: notebook.select(library_panel.frame))
    more_menu.add_separator()
    more_menu.add_command(label="Restore previous channel list\u2026", command=_restore_channels)
    more_menu.add_command(label="Restore previous settings (config.ini)\u2026", command=_restore_settings)
    more_btn.configure(menu=more_menu)
    more_btn.pack(side=tk.RIGHT, padx=(0, 4))

    # ── Status bar at very bottom ──
    dep_parts = []
    if HAS_YTDLP:
        dep_parts.append(f"yt-dlp {YTDLP_VERSION}")
    if HAS_STREAMLINK:
        dep_parts.append(f"streamlink {STREAMLINK_VERSION}")
    if _deps.HAS_FFMPEG:
        dep_parts.append(f"ffmpeg")
    dep_str = " | ".join(dep_parts) if dep_parts else "No recording tools found"

    status_bar = tk.Label(root, text=f"  v{__version__}  —  {dep_str}  —  Ctrl+Q quit, F1 about, Enter add, Del remove",
                          font=("Segoe UI", 8), anchor="w", padx=6, pady=2)
    status_bar.grid(row=2, column=0, columnspan=2, sticky="ew")
    status_bar.configure(cursor="hand2")
    status_bar.bind("<Button-1>", _open_updates)

    # ── Version update check ──
    update_label = tk.Label(root, text="", font=("Segoe UI", 8, "bold"),
                            fg=_themes.current()['link'], cursor="hand2", anchor="e", padx=10, pady=2)
    # Overlaid on the right side of the status bar row
    update_label.grid(row=2, column=1, sticky="e")
    update_label.grid_remove()  # hidden until an update is found

    _update_url = [None]
    _update_zipball = [None]
    _update_tag = [None]
    _update_busy = [False]

    def _on_update_found(latest_tag, release_url, zipball_url=None):
        """Called from version check thread — schedules GUI update on main thread."""
        _update_url[0] = release_url
        _update_zipball[0] = zipball_url or None
        _update_tag[0] = latest_tag
        def _show():
            update_label.configure(text=f"  v{latest_tag} available — click to install")
            update_label.grid()
        root.after(0, _show)

    def _restore_update_banner():
        tag = _update_tag[0]
        if tag:
            update_label.configure(
                text=f"  v{tag} available — click to install", cursor="hand2",
            )
            update_label.grid()
        _update_busy[0] = False

    def _begin_in_app_update(event=None):
        if _update_busy[0] or not _update_tag[0]:
            return
        from msr.update import (
            prepare_update, spawn_updater, zipball_url_for_tag, zipball_url_is_allowed,
        )
        tag = _update_tag[0]
        zip_url = _update_zipball[0]
        if not zipball_url_is_allowed(zip_url or ""):
            zip_url = zipball_url_for_tag(tag)
        target = application_dir()
        if not os.access(target, os.W_OK):
            _dialogs.show_error(
                root,
                "Cannot update",
                f"The install folder is not writable:\n{target}",
            )
            return
        recording = bool(recorder and recorder.is_running)
        extra = (
            "Active recordings will be stopped first.\n\n"
            if recording else ""
        )
        if not _dialogs.ask_yes_no(
            root,
            "Install update",
            f"Download v{tag} from GitHub and restart Multi-Stream Recorder?\n\n"
            f"{extra}"
            "config.ini, channels.json, cookies.txt, and recordings stay.\n"
            "The app will quit and reopen on the new version.",
        ):
            return
        _update_busy[0] = True
        update_label.configure(text=f"  Downloading v{tag}…", cursor="arrow")

        def _fail(msg):
            logging.warning("In-app update failed: %s", msg)
            _dialogs.show_error(
                root,
                "Update failed",
                f"{msg}\n\nYou can still open the release page from GitHub.",
            )
            if _update_url[0]:
                import webbrowser
                webbrowser.open(_update_url[0])
            _restore_update_banner()

        def _spawn_then_quit(staging):
            def _after_children():
                spawn_updater(staging, target_dir=target)
            _full_quit(after_children=_after_children)

        def _stop_then_quit(staging):
            rec = recorder
            if rec and rec.is_running:
                update_label.configure(text="  Stopping recordings…")

                def _bg_stop():
                    try:
                        rec.stop()
                    except Exception:
                        logging.exception("Stop before update failed")
                        root.after(0, lambda: _fail(
                            "Could not stop recordings. Try Stop Recording, then click the banner again."
                        ))
                        return
                    root.after(0, lambda: _spawn_then_quit(staging))

                threading.Thread(target=_bg_stop, daemon=True, name="update-stop").start()
                return
            _spawn_then_quit(staging)

        def _bg_download():
            try:
                staging = prepare_update(zip_url)
            except Exception as e:
                err = str(e) or "Download failed"
                root.after(0, lambda m=err: _fail(m))
                return
            root.after(0, lambda: _stop_then_quit(staging))

        threading.Thread(target=_bg_download, daemon=True, name="update-download").start()

    update_label.bind("<Button-1>", _begin_in_app_update)

    # Delay the check by 10 seconds so it doesn't slow startup
    def _delayed_version_check():
        check_for_updates(__version__, callback=_on_update_found)
    root.after(10_000, _delayed_version_check)

    # Daily dependency check: a background thread long after startup; never delays recording.
    _deps_q = stdlib_queue.Queue()

    def _daily_deps_check():
        if not config.getboolean('Updates', 'daily_check', fallback=True):
            return
        if not _depsupdate.due(load_window_state(state_file)):
            return

        def work():
            try:
                rs = _depsupdate.rows(True, ffmpeg_path=config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg') or 'ffmpeg')
                if any(r['status'] != 'unknown' for r in rs if r['name'] != 'ffmpeg'):
                    _depsupdate.remember(state_file, rs)
                _deps_q.put(_depsupdate.updates_available(rs))
            except Exception:
                logging.debug("Daily dependency check failed", exc_info=True)
                _deps_q.put(None)

        threading.Thread(target=work, daemon=True, name="deps-daily").start()
        _drain_deps_q()

    def _drain_deps_q():
        try:
            names = _deps_q.get_nowait()
            if names is not None:
                _set_updates_badge(names)
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

    root.after(400, _warn_if_last_update_failed)

    def _show_venv_notice():
        from msr import venv as _venv
        if _venv.LAUNCH_NOTICE:
            _dialogs.show_warning(root, "Multi-Stream Recorder environment", _venv.LAUNCH_NOTICE)
    root.after(2500, _show_venv_notice)   # after recording has started; never blocks it
    root.after(500, _show_update_notice)

    # ── Refresh functions ──
    def _get_platform_label(ch_name):
        """Extract a display platform from a channel name string."""
        if ch_name.startswith("twitch:"):
            return "Twitch"
        elif ch_name.startswith("youtube:"):
            return "YouTube"
        elif ch_name.startswith("fishtank:"):
            return "Fishtank"
        elif ch_name.startswith("rumble:"):
            return "Rumble"
        elif ch_name.startswith("tiktok:"):
            return "TikTok"
        elif ch_name.startswith("custom:"):
            platform, _ = parse_custom_url(ch_name.split(":", 1)[1])
            return platform.capitalize()
        else:
            return "Kick"

    def _get_display_name(ch_name):
        """Extract a clean display name for the status table."""
        if ch_name.startswith("custom:"):
            _, channel = parse_custom_url(ch_name.split(":", 1)[1])
            return channel
        elif ":" in ch_name:
            return ch_name.split(":", 1)[1]
        return ch_name

    def _preview_cache_file(ch_name, suffix=""):
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in ch_name)[:80]
        folder = os.path.join(tempfile.gettempdir(), "msr-previews")
        return os.path.join(folder, f"{safe}{suffix}.png")

    def _set_big_preview_image(png_path=None, from_small=None):
        """Show *png_path*, or a 3× zoom of an existing strip PhotoImage."""
        photo = None
        if png_path and os.path.isfile(png_path):
            try:
                photo = tk.PhotoImage(file=png_path)
            except tk.TclError:
                photo = None
        if photo is None and from_small is not None:
            try:
                photo = from_small.zoom(3, 3)
            except tk.TclError:
                photo = None
        if photo is None:
            return
        preview_big_img.configure(image=photo)
        _preview_big_photo[0] = photo

    def _hide_big_preview():
        _preview_big[0] = None
        _preview_big_photo[0] = None
        try:
            preview_big_frame.pack_forget()
            preview_big_img.configure(image="")
        except tk.TclError:
            pass

    def _show_big_preview(ch_name):
        if not ch_name:
            return
        if _preview_big[0] == ch_name:
            _hide_big_preview()
            return
        if _pstate.status(ch_name).kind == "ended":
            return  # only its last frame is left; nothing to pin
        _preview_big[0] = ch_name
        preview_big_name.configure(text=_get_display_name(ch_name))
        _style_big_preview(_themes.current())
        try:
            packed = bool(preview_big_frame.pack_info())
        except tk.TclError:
            packed = False
        if not packed:
            preview_big_frame.pack(
                fill=tk.X, side=tk.TOP, before=preview_canvas, pady=(0, 6),
            )
        # Start from the strip tile (current), not from the cached ..._big.png: that file may be
        # left over from an earlier stream of this channel. The next tick writes a fresh one.
        _set_big_preview_image(from_small=_preview_photos.get(ch_name))
        _sync_nav(ch_name, "preview")

    preview_big_close.configure(command=_hide_big_preview)
    preview_big_img.bind("<Button-1>", lambda _e: (
        _sync_nav(_preview_big[0], "preview") if _preview_big[0] else None
    ))
    preview_big_img.bind("<Double-Button-1>", lambda _e: _hide_big_preview())

    def _recording_preview_jobs():
        """(channel_key, raw_file) for channels that can currently clip.

        Order follows the roster so the strip matches the left list.
        """
        if not recorder or not recorder.is_running:
            return []
        jobs = []
        seen = set()
        roster = [ch["name"] for ch in channels]
        extra = [k for k in recorder.status_dict if k not in roster]
        for ch_name in roster + extra:
            if ch_name in seen:
                continue
            seen.add(ch_name)
            st = recorder.status_dict.get(ch_name) or {}
            if not channel_status_allows_clip(st.get("status", "")):
                continue
            path = st.get("raw_file") or ""
            if not path or not os.path.isfile(path):
                path = find_active_recording_file(recorder, ch_name) or ""
            if not path:
                continue
            size = st.get("size_bytes") or 0
            if size and size < 64 * 1024:
                continue
            jobs.append((ch_name, path))
        return jobs

    def _select_status_row(ch_name, see=False):
        for iid in tree.get_children():
            try:
                if tree.set(iid, "_key") == ch_name:
                    tree.selection_set(iid)
                    if see:
                        tree.see(iid)
                    return
            except tk.TclError:
                continue

    def _style_preview_tile(ch_name, t, selected):
        """Border, name and age text of one tile: selected, stale ("12 s"), or ended."""
        tile = _preview_tiles.get(ch_name)
        if not tile:
            return
        look = _preview_state.tile_look(_pstate.status(ch_name), t, selected)
        tile["frame"].configure(highlightbackground=look.border, highlightthickness=look.thickness)
        tile["name"].configure(text=_get_display_name(ch_name) + look.suffix, fg=look.name_fg)

    def _style_big_preview(t):
        ch_name = _preview_big[0]
        if not ch_name:
            return
        st = _pstate.status(ch_name)
        look = _preview_state.tile_look(st, t)
        border = _preview_state.pane_border(st, t)
        preview_big_frame.configure(highlightbackground=border, highlightcolor=border)
        preview_big_name.configure(text=_get_display_name(ch_name) + look.suffix, fg=look.name_fg)

    def _paint_preview_selection(ch_name):
        """Accent the active tile and show each tile's age. No update_idletasks: that nested the Tk loop."""
        t = _themes.current()
        _preview_selected[0] = ch_name
        for name in _preview_tiles:
            _style_preview_tile(name, t, name == ch_name)
        _style_big_preview(t)

    def _scroll_nav_into_view(ch_name):
        if not ch_name:
            return
        try:
            item = _item_for_name(ch_name)
            if item:
                ch_tree.see(item)
            for iid in tree.get_children():
                if tree.set(iid, "_key") == ch_name:
                    tree.see(iid)
                    break
            tile = _preview_tiles.get(ch_name)
            if tile:
                inner_w = preview_inner.winfo_reqwidth()
                canvas_w = preview_canvas.winfo_width()
                if inner_w > canvas_w > 1:
                    x = tile["frame"].winfo_x()
                    preview_canvas.xview_moveto(max(0.0, min(1.0, x / inner_w)))
        except tk.TclError:
            pass

    def _sync_nav(ch_name, origin):
        """Keep roster, status row, and preview tile on the same channel.

        Must not call update()/see() while handling <<TreeviewSelect>> —
        that nested the Tk event loop and froze the GUI.
        """
        if _nav_lock[0] or not ch_name:
            return
        if ch_name == _preview_selected[0] and origin != "preview":
            _paint_preview_selection(ch_name)
            return
        _nav_lock[0] = True
        try:
            _preview_selected[0] = ch_name
            if origin != "status":
                _select_status_row(ch_name, see=False)
            if origin != "roster":
                _select_roster_names([ch_name], see_first=False)
            _paint_preview_selection(ch_name)
        finally:
            _nav_lock[0] = False
        root.after_idle(lambda n=ch_name: _scroll_nav_into_view(n))

    def _on_roster_select(_event=None):
        headers = [i for i in ch_tree.selection() if i.startswith("h:")]
        if headers:
            ch_tree.selection_remove(*headers)  # group headers are not channels
            return
        _schedule_badge_redraw()
        if _nav_lock[0]:
            return
        names = _selected_roster_names()
        if names and names[0] != _preview_selected[0]:
            _sync_nav(names[0], "roster")

    def _on_status_select(_event=None):
        if _nav_lock[0]:
            return
        ch_name = _get_selected_status_channel()
        if ch_name and ch_name != _preview_selected[0]:
            _sync_nav(ch_name, "status")

    ch_tree.bind("<<TreeviewSelect>>", _on_roster_select)
    tree.bind("<<TreeviewSelect>>", _on_status_select)

    def _sync_preview_tiles(keys):
        t = _themes.current()
        for ch_name in keys:
            _pstate.mark_active(ch_name)
        shown = list(keys)
        for ch_name in list(_preview_tiles):
            if ch_name in keys:
                continue
            _pstate.mark_ended(ch_name)
            if _pstate.keep_ended(ch_name):
                shown.append(ch_name)  # its last frame stays for a while, labelled "ended"
                continue
            _preview_tiles[ch_name]["frame"].destroy()
            _preview_tiles.pop(ch_name, None)
            _preview_photos.pop(ch_name, None)
            _pstate.forget(ch_name)
        if _preview_big[0] and _preview_big[0] not in keys:
            _hide_big_preview()
        if not shown:
            preview_empty.pack(side=tk.LEFT)
            preview_canvas.configure(height=28)
            return
        preview_empty.pack_forget()
        preview_canvas.configure(height=PREVIEW_H + 42)  # room for a name that wraps to two lines
        for ch_name in shown:
            if ch_name in _preview_tiles:
                continue
            tile = tk.Frame(preview_inner, padx=4, pady=4,
                            highlightthickness=1, highlightbackground=t["border"],
                            bg=t["tree_field"])
            blank = tk.PhotoImage(width=PREVIEW_W, height=PREVIEW_H)
            img_lbl = tk.Label(tile, image=blank, bg=t["tree_field"], bd=0)
            _preview_photos[ch_name] = blank
            img_lbl.pack()
            name_lbl = tk.Label(
                tile, text=_get_display_name(ch_name),
                font=("Segoe UI", 8), bg=t["tree_field"], fg=t["fg"],
                wraplength=PREVIEW_W, justify=tk.CENTER,
            )
            name_lbl.pack()
            tile.bind("<Button-1>", lambda _e, k=ch_name: _sync_nav(k, "preview"))
            img_lbl.bind("<Button-1>", lambda _e, k=ch_name: _sync_nav(k, "preview"))
            name_lbl.bind("<Button-1>", lambda _e, k=ch_name: _sync_nav(k, "preview"))
            tile.bind("<Double-Button-1>", lambda _e, k=ch_name: _show_big_preview(k))
            img_lbl.bind("<Double-Button-1>", lambda _e, k=ch_name: _show_big_preview(k))
            name_lbl.bind("<Double-Button-1>", lambda _e, k=ch_name: _show_big_preview(k))
            _preview_tiles[ch_name] = {"frame": tile, "img": img_lbl, "name": name_lbl}
        packed = []
        for child in preview_inner.pack_slaves():
            for name, tile in _preview_tiles.items():
                if tile["frame"] is child:
                    packed.append(name)
                    break
        if packed != shown:
            for ch_name in shown:
                _preview_tiles[ch_name]["frame"].pack_forget()
            for ch_name in shown:
                _preview_tiles[ch_name]["frame"].pack(side=tk.LEFT, padx=(0, 6))
        _paint_preview_selection(_preview_selected[0])

    def _set_preview_image(ch_name, png_path):
        tile = _preview_tiles.get(ch_name)
        if not tile or not png_path or not os.path.isfile(png_path):
            return
        try:
            photo = tk.PhotoImage(file=png_path)
        except tk.TclError:
            return
        tile["img"].configure(image=photo, width=PREVIEW_W, height=PREVIEW_H)
        _preview_photos[ch_name] = photo

    def _finish_preview_batch(results, keys):
        _preview_busy[0] = False
        try:
            if not root.winfo_exists():
                return
        except tk.TclError:
            return
        _sync_preview_tiles(keys)
        for row in results:
            ch_name = row[0]
            png_path = row[1] if len(row) > 1 else None
            big_path = row[2] if len(row) > 2 else None
            if png_path:
                _set_preview_image(ch_name, png_path)
            if _preview_big[0] == ch_name:
                _set_big_preview_image(
                    png_path=big_path,
                    from_small=_preview_photos.get(ch_name),
                )
        _paint_preview_selection(_preview_selected[0])  # ages: a channel whose grabs fail goes stale

    def _tick_previews():
        _preview_kick[0] = False
        try:
            if not root.winfo_exists():
                return
        except tk.TclError:
            return
        if _preview_after[0] is not None:
            try:
                root.after_cancel(_preview_after[0])  # a kick must not leave two timer chains running
            except tk.TclError:
                pass
        _preview_after[0] = root.after(PREVIEW_MS, _tick_previews)
        try:
            if not preview_wrap.winfo_viewable():
                return  # Status tab hidden or window minimised: nobody can see it, so no ffmpeg
        except tk.TclError:
            return
        if _preview_busy[0]:
            return
        jobs = _recording_preview_jobs()
        keys = [ch for ch, _path in jobs]
        if not jobs:
            _sync_preview_tiles([])
            _paint_preview_selection(_preview_selected[0])
            return
        _preview_busy[0] = True
        ffmpeg_path = config.get("Advanced", "ffmpeg_path", fallback="ffmpeg")

        def _work():
            grabbed = []
            try:
                for ch_name, raw_file in jobs:
                    if (ch_name, "clip") in _media_jobs_in_flight or (
                        ch_name, "screenshot"
                    ) in _media_jobs_in_flight:
                        continue
                    _pstate.note_source(ch_name, raw_file)
                    if not _pstate.due(ch_name):
                        continue  # every seek failed lately; wait out the back-off
                    out = _preview_cache_file(ch_name)
                    started = _pstate.clock()
                    report = {}
                    ok, msg = create_preview_frame(
                        raw_file, out, ffmpeg_path, logging,
                        width=PREVIEW_W, height=PREVIEW_H,
                        attempts=_pstate.plan(ch_name), report=report,
                    )
                    if ok:
                        _pstate.record_success(ch_name, report.get("attempt"))
                    else:
                        wait = _pstate.record_failure(ch_name, started)
                        logging.debug("Preview grab failed for %s (%s); next try in %.0f s", ch_name, msg, wait)
                    big_out = None
                    if ok and _preview_big[0] == ch_name:
                        big_path = _preview_cache_file(ch_name, "_big")
                        ok_big, _msg = create_preview_frame(
                            raw_file, big_path, ffmpeg_path, logging,
                            width=PREVIEW_BIG_W, height=PREVIEW_BIG_H,
                            attempts=_pstate.plan(ch_name, allow_retry=False),
                        )
                        if ok_big:
                            big_out = big_path
                    grabbed.append((ch_name, out if ok else None, big_out))
            except Exception:
                logging.debug("Preview grab failed", exc_info=True)
            try:
                root.after(0, lambda: _finish_preview_batch(grabbed, keys))
            except Exception:
                _preview_busy[0] = False

        threading.Thread(target=_work, daemon=True, name="preview-grab").start()

    def _kick_previews(_event=None):
        """Grab now (the strip just became visible) instead of waiting for the next timer."""
        if _preview_kick[0]:
            return
        _preview_kick[0] = True
        try:
            root.after_idle(_tick_previews)
        except tk.TclError:
            _preview_kick[0] = False

    preview_wrap.bind("<Map>", _kick_previews, add="+")
    notebook.bind("<<NotebookTabChanged>>", _kick_previews, add="+")
    # Restoring a minimised window maps only the toplevel, not the strip inside it.
    root.bind("<Map>", lambda e: _kick_previews() if e.widget is root else None, add="+")

    def _reorder_status_rows():
        """Apply roster order, with Recording/Remuxing first when enabled."""
        if not recorder:
            return
        roster = [ch["name"] for ch in channels]
        keys = []
        iid_by_key = {}
        for iid in tree.get_children():
            try:
                key = tree.set(iid, "_key")
            except tk.TclError:
                continue
            if not key:
                continue
            keys.append(key)
            iid_by_key[key] = iid
        ordered = ordered_status_keys(
            keys, recorder.status_dict, roster,
            recording_first=bool(recording_first_var.get()),
        )
        # tree.move can fire <<TreeviewSelect>>; hold the nav lock so that
        # does not re-enter _sync_nav (which used to freeze the GUI).
        _nav_lock[0] = True
        try:
            for i, ch_name in enumerate(ordered):
                iid = iid_by_key.get(ch_name)
                if iid:
                    tree.move(iid, "", i)
        finally:
            _nav_lock[0] = False

    def _show_storage_notice(text):
        """Show/hide the recordings-folder warning; one toast when it appears."""
        t = _themes.current()
        storage_notice_label.configure(bg=t['bg'], fg=t['error_fg'], text=text)
        if text and not storage_notice_label.winfo_manager():
            storage_notice_label.pack(fill=tk.X, padx=10, pady=(0, 4), after=status_header)
        elif not text and storage_notice_label.winfo_manager():
            storage_notice_label.pack_forget()
        if text and text != _storage_notice_shown[0] and notifications_enabled:
            send_notification("Recordings folder", text, category="error", channel="storage",
                              detail=text)
        _storage_notice_shown[0] = text

    def refresh_status():
        try:
            _show_storage_notice(recorder.storage_notice() if recorder and recorder.is_running else "")
        except Exception as e:
            logging.debug(f"storage notice: {e}")
        if recorder and recorder.is_running:
            recorder.update_status_from_queue()

            # Update existing rows in place.  Rebuilding the whole tree every
            # 2.5s was discarding selection, flickering tags, and re-firing
            # notification logic against a brand-new iid each tick.
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

                # Build the Status cell text.
                # While recording: show stream info (resolution/bitrate) if available,
                # otherwise fall back to any detail text (e.g. "starting", "fallback (streamlink)").
                # For all other states: status + detail in parens as before.
                if "recording" in curr:
                    detail = st.get("detail", "")
                    if detail:
                        display = f"Recording  {detail}"
                    else:
                        display = "Recording"
                else:
                    display = st["status"]
                    if st.get("detail"):
                        display += f" ({st['detail']})"

                platform_label = _get_platform_label(ch_name)

                if "recording" in curr:
                    tag = "recording"
                    # Size-split remux of the previous file: keep status as
                    # Recording (clips still work) but color the row as remuxing.
                    if "remuxing" in (st.get("detail") or "").lower():
                        tag = "remuxing"
                    # A new recording may complete later — allow one complete toast again.
                    _notified_complete.discard(ch_name)
                    if notifications_enabled and ch_name not in _notified_live and "starting" not in st.get("detail", ""):
                        _notified_live.add(ch_name)
                        send_notification("Stream Recording", f"Now recording: {ch_name}",
                                          category="recording", channel=ch_name)
                elif "remuxing" in curr or "pending" in curr:
                    tag = "remuxing"
                elif "checking" in curr or "initializing" in curr:
                    tag = "checking"
                elif "error" in curr or "failed" in curr:
                    tag = "error"
                    if notifications_enabled:
                        send_notification("Recording Error", f"{ch_name}: {display}",
                                          category="error", channel=ch_name, detail=display)
                elif "completed" in curr:
                    tag = "completed"
                    _notified_live.discard(ch_name)
                    if notifications_enabled and ch_name not in _notified_complete:
                        _notified_complete.add(ch_name)
                        send_notification("Recording Complete", f"{ch_name}: {st.get('detail', '')}",
                                          category="complete", channel=ch_name)
                elif "offline" in curr:
                    tag = "offline"
                    _notified_live.discard(ch_name)
                elif "stopped" in curr:
                    tag = "stopped"
                    _notified_live.discard(ch_name)
                else:
                    tag = "unknown"

                display_name = _get_display_name(ch_name)
                new_vals = (
                    display_name, display,
                    st.get("size", ""), st.get("time", ""),
                    platform_label, ch_name,
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

            _reorder_status_rows()

            # Update tray icon tooltip if tray exists
            if tray_icon is not None:
                active = sum(1 for ch_name in recorder.status_dict
                             if recorder.status_dict.get(ch_name, {}).get("status", "").lower().startswith("recording"))
                tray_icon.title = f"Multi-Stream Recorder — {active} recording" if active else "Multi-Stream Recorder — idle"

        _roster_live_tick()
        root.after(2500, refresh_status)

    def refresh_logs():
        """Pull log messages from queue and append to log viewer."""
        batch = []
        while len(batch) < 50:  # up to 50 messages per tick to avoid blocking
            try:
                batch.append(log_queue.get_nowait())
            except stdlib_queue.Empty:
                break
        if batch:
            log_follow.append(batch)  # trims to ~4000 lines past 5000; follows only at the bottom
        root.after(500, refresh_logs)

    # ── Recording controls ──
    def start_recording(interactive=True):
        """Start Recording. interactive=False (agent control): no dialogs; a refusal raises
        ControlError and the dependency / channel-count warnings only go to the log."""
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

        # Warn about large number of concurrent recordings
        if len(enabled) >= 15 and interactive:
            if not _dialogs.ask_yes_no(root, "High Channel Count",
                    f"You're about to monitor {len(enabled)} channels simultaneously.\n\n"
                    "This may strain your CPU, RAM, and disk I/O. Each channel spawns "
                    "its own process.\n\nContinue?"):
                return

        # Pre-flight checks — warn about channels that can't be recorded
        twitch_channels = [ch for ch in enabled if ch.startswith("twitch:")]
        ytdlp_channels = [ch for ch in enabled if not ch.startswith("twitch:")]
        issues = []

        if twitch_channels and not HAS_STREAMLINK:
            names = ", ".join(twitch_channels[:3])
            issues.append(f"Twitch channels ({names}) require streamlink, which is not installed.")

        if ytdlp_channels and not HAS_YTDLP:
            names = ", ".join(ytdlp_channels[:3])
            issues.append(f"Kick/YouTube/custom channels ({names}) require yt-dlp, which is not installed.")

        if not _deps.HAS_FFMPEG:
            issues.append("ffmpeg is not installed — recordings cannot be remuxed to MP4.")

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

        recorder = StreamRecorder(enabled, config, presence=presence_store,
                                  watch_channels=_presence_watch_keys())
        threading.Thread(target=recorder.run, daemon=True).start()

        start_button.config(state=tk.DISABLED)
        stop_button.config(state=tk.NORMAL)
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
        start_button.config(state=tk.DISABLED)
        stop_button.config(state=tk.DISABLED)

        # Collect recording summary before stopping (include all tracked channels)
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

            for item in tree.get_children():
                tree.delete(item)
            try:
                keys = list(rec.status_dict)
            except Exception:
                keys = []
            for ch_name in keys:
                display_name = _get_display_name(ch_name)
                platform_label = _get_platform_label(ch_name)
                tree.insert("", tk.END, values=(display_name, "Stopped", "", "", platform_label, ch_name),
                            tags=("stopped",))

            recorder = None
            start_button.config(state=tk.NORMAL)
            stop_button.config(state=tk.DISABLED)
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

    def setup_tray():
        nonlocal tray_icon
        if not HAS_TRAY or not minimize_to_tray:
            return

        def show_window(icon=None, item=None):
            root.after(0, root.deiconify)
            root.after(50, root.lift)

        def hide_to_tray(icon=None, item=None):
            root.after(0, root.withdraw)

        def quit_app(icon=None, item=None):
            root.after(0, _full_quit)

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

        icon_img = create_tray_icon_image(recording=False)
        tray_icon = pystray.Icon("multi_stream_recorder", icon_img, "Multi-Stream Recorder", menu)
        threading.Thread(target=tray_icon.run, daemon=True).start()

    setup_tray()

    # ── Window close behavior ──
    def on_close():
        """Always fully quit when the window X is clicked."""
        # Save window state
        try:
            col_widths = {col: tree.column(col, "width") for col in VISIBLE_STATUS_COLUMNS}
            state = load_window_state(state_file)  # keep keys written elsewhere (dialog places)
            state.update({
                'geometry': root.geometry(),
                'column_widths': col_widths,
                'roster_filter': roster_filter_var.get(),
                'roster_view': roster_view_var.get(),
                'roster_enabled_only': roster_view_var.get() == cp.VIEW_ENABLED,
                'roster_disabled_collapsed': bool(disabled_collapsed[0]),
                'status_recording_first': bool(recording_first_var.get()),
            })
            save_window_state(state_file, state)
        except Exception:
            pass

        _full_quit()

    def _full_quit(after_children=None):
        """Fully terminate the application — stop recorder, tray, and exit.
        
        Kills ALL child processes by walking the process tree, then calls
        os._exit(0) as a final backstop.  This prevents orphaned workers.
        *after_children* runs after that kill (used to spawn the update helper
        so it is not torn down with the workers).
        """
        # 0. No more commands; msr_status.json says "not running"
        try:
            if _control_service[0] is not None:
                _control_service[0].stop()
        except Exception:
            pass
        # 1. Try graceful recorder shutdown
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

        # 2. Kill ALL child processes of this Python process (nuclear option)
        #    This catches any workers/manager processes that survived stop()
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

        # 2b. Workers from a crashed *other* instance are not our children.
        try:
            kill_orphan_recording_processes(
                logging.getLogger(),
                streams_dir=config.get("Paths", "streams_dir"),
            )
        except Exception:
            pass

        # 2c. Start the update helper only after workers are gone so
        # psutil.children() cannot kill it.
        if after_children is not None:
            try:
                after_children()
            except Exception:
                logging.exception("Failed to start update helper")

        # 3. Stop tray and destroy window
        try:
            if tray_icon:
                tray_icon.stop()
        except Exception:
            pass
        try:
            root.destroy()
        except Exception:
            pass
        # Force exit — this MUST be the last line and MUST execute
        os._exit(0)

    root.protocol("WM_DELETE_WINDOW", on_close)

    # ── Agent / headless control (msr/control.py, msr_ctl.py) ──
    # Commands arrive on a server thread. Roster edits run here on the Tk thread (_on_tk) so the
    # list, the tree and channels.json stay one thing; recorder calls run on the server thread,
    # as the GUI's own background stop/start helpers do. refresh_status shows the result.
    _tk_thread = threading.current_thread()
    _control_service = [None]

    def _on_tk(fn, timeout=30):
        if threading.current_thread() is _tk_thread:
            return fn()
        box = {}
        done = threading.Event()

        def run():
            try:
                box["value"] = fn()
            except BaseException as e:  # handed to the caller's thread
                box["error"] = e
            finally:
                done.set()

        root.after(0, run)
        if not done.wait(timeout):
            raise _control.ControlError("The window did not respond in time; try again.", "busy")
        if "error" in box:
            raise box["error"]
        return box.get("value")

    def _reflect_setting(section, key):
        """Show a setting changed by control in the selectors that display it."""
        if (section, key) == ("Timeouts", "poll_interval_minutes"):
            minutes = config.getfloat('Timeouts', 'poll_interval_minutes', fallback=3.0)
            shown = next((n for n, v in POLL_PRESETS.items() if abs(v - minutes) < 0.1),
                         _custom_poll_display(minutes))
            poll_var.set(shown)
            _last_poll_display[0] = shown
        elif (section, key) == ("Clipping", "clip_length_seconds"):
            seconds = config.getint('Clipping', 'clip_length_seconds', fallback=30)
            shown = next((n for n, v in CLIP_PRESETS.items() if v == seconds), _custom_clip_display(seconds))
            clip_var.set(shown)
            _last_clip_display[0] = shown

    class _GuiControlApp(_control.AppAdapter):
        mode = "gui"

        @property
        def recorder(self):
            return recorder

        @property
        def presence(self):
            return presence_store

        def records(self):
            return [dict(c) for c in list(channels)]

        def mutate(self, fn):
            def work():
                result = fn(channels)
                _populate_channel_tree()
                save_channels()
                return result
            return _on_tk(work)

        def start_session(self):
            return _on_tk(lambda: start_recording(interactive=False))

        def stop_session(self):
            if not (recorder and recorder.is_running):
                raise _control.ControlError("Recording is not running.", "conflict")
            _on_tk(stop_recording)
            return "stopping"

        def request_quit(self, after_children=None):
            if after_children is None:
                root.after(300, on_close)
            else:
                root.after(300, lambda: _full_quit(after_children=after_children))

        def reload_channels(self):
            def work():
                _reload_channels_from_disk()
                return len(channels)
            return _on_tk(work)

        def settings_changed(self, section, key):
            _on_tk(lambda: _reflect_setting(section, key))

        def update_info(self):
            return {"available": bool(_update_tag[0]), "latest": _update_tag[0]}

    _control_service[0] = _control.Service.start_for(_GuiControlApp(config))

    # ── Apply theme and start refresh loops ──
    apply_theme()
    # Re-apply dark title bar after window is fully mapped (100ms + 500ms delays)
    # The DWM attribute only takes effect once the HWND is realized
    root.after(100, lambda: set_title_bar_dark())
    root.after(500, lambda: set_title_bar_dark())
    root.after(1500, refresh_status)
    root.after(2000, _tick_previews)
    root.after(500, refresh_logs)
    if HAS_PSUTIL:
        root.after(1000, _refresh_io_meter)

    # MSR_SMOKE_KEEP=1 (tests/test_control.py): stay up hidden until a control "quit"
    if smoke_test and os.environ.get("MSR_SMOKE_KEEP") != "1":
        root.after(2500, lambda: os._exit(0))

    try:
        root.mainloop()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt detected — shutting down...")
        _full_quit()
