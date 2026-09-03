"""Tkinter GUI: status table, channel roster, clips, tray, theme."""
import datetime
import json
import logging
import os
import queue
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from msr import __version__
from msr.deps import (
    DENO_VERSION, FFMPEG_VERSION, HAS_CURL_CFFI, HAS_DENO, HAS_FFMPEG,
    HAS_NOTIFICATIONS, HAS_PSUTIL, HAS_STREAMLINK, HAS_TRAY, HAS_WEBSOCKET,
    HAS_YTDLP, STREAMLINK_VERSION, YTDLP_VERSION,
    Image, ImageDraw, plyer_notification, pystray,
)
from msr.recorder import StreamRecorder
from msr.util import (
    COOKIE_DOT_COLORS, COOKIE_STATUS_UNKNOWN,
    channel_file_stem, channel_key_to_dirs, channel_watch_url, check_for_updates,
    coerce_channel_records, find_cookies_file,
    get_cookie_status_for_channel, open_local_path, parse_custom_url,
    redact_for_log, validate_channel_name, validate_cookies,
)
from msr.iometer import (
    IoSampler, format_header, format_tooltip, meter_severity, sum_stream_mbps,
)
from msr.worker import (
    create_clip, create_screenshot, find_active_recording_file,
    screenshot_extension,
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


def move_list_items(seq, indices, to="top"):
    """Move entries at *indices* to the top or bottom of *seq* (in place).

    The moved block keeps its relative order, as does the rest of the list.
    Invalid indices are ignored. Returns the new indices of the moved
    entries (sorted, in list order).
    """
    if not seq:
        return []
    want = []
    seen = set()
    for raw in indices:
        try:
            i = int(raw)
        except (TypeError, ValueError):
            continue
        if i < 0 or i >= len(seq) or i in seen:
            continue
        seen.add(i)
        want.append(i)
    want.sort()
    if not want:
        return []
    moving = [seq[i] for i in want]
    rest = [item for i, item in enumerate(seq) if i not in seen]
    if to == "bottom":
        seq[:] = rest + moving
        start = len(rest)
    else:
        seq[:] = moving + rest
        start = 0
    return list(range(start, start + len(moving)))


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
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


# ────────────────────────────────────────────────
#          System Tray Icon
# ────────────────────────────────────────────────

def create_tray_icon_image(recording=False):
    """Create a simple tray icon image using PIL.

    Green circle when recording, grey when idle.
    """
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
    from tkinter import ttk, messagebox, simpledialog
    import queue as stdlib_queue

    # ── Theme colors ──
    DARK = {
        'bg': '#0a1628', 'fg': '#d4d4d4', 'accent': '#1a5fb4',
        'border': '#1a3a5c',
        'entry_bg': '#112240', 'entry_fg': '#d4d4d4',
        'listbox_bg': '#0d1f3c', 'listbox_fg': '#cccccc',
        'select_bg': '#1a5fb4', 'select_fg': '#ffffff',
        'tree_bg': '#0a1628', 'tree_fg': '#d4d4d4', 'tree_field': '#0d1f3c',
        'tree_heading_bg': '#132d5e', 'tree_heading_fg': '#d4d4d4',
        'btn_bg': '#132d5e', 'btn_fg': '#d4d4d4',
        'log_bg': '#081422', 'log_fg': '#b5cea8',
        'tab_bg': '#112240', 'tab_fg': '#d4d4d4',
        'start_bg': '#1b7a2b', 'stop_bg': '#b71c1c',
        'rec_fg': '#4ec94e', 'offline_fg': '#5a6a8a', 'error_fg': '#f44747',
        'completed_fg': '#4ec9b0', 'remux_fg': '#dcdcaa', 'check_fg': '#6a7a9a',
    }
    LIGHT = {
        'bg': '#f0f0f0', 'fg': '#1e1e1e', 'accent': '#0078d4',
        'border': '#c0c0c0',
        'entry_bg': '#ffffff', 'entry_fg': '#1e1e1e',
        'listbox_bg': '#ffffff', 'listbox_fg': '#1e1e1e',
        'select_bg': '#0078d4', 'select_fg': '#ffffff',
        'tree_bg': '#ffffff', 'tree_fg': '#1e1e1e', 'tree_field': '#ffffff',
        'tree_heading_bg': '#e0e0e0', 'tree_heading_fg': '#1e1e1e',
        'btn_bg': '#e0e0e0', 'btn_fg': '#1e1e1e',
        'log_bg': '#ffffff', 'log_fg': '#1e1e1e',
        'tab_bg': '#f0f0f0', 'tab_fg': '#1e1e1e',
        'start_bg': '#4CAF50', 'stop_bg': '#F44336',
        'rec_fg': '#006400', 'offline_fg': '#696969', 'error_fg': '#B22222',
        'completed_fg': '#2E8B57', 'remux_fg': '#DAA520', 'check_fg': '#808080',
    }

    # ── Load window state ──
    state_file = config.get('GUI', 'window_state_file', fallback='window_state.json')
    win_state = load_window_state(state_file)
    notifications_enabled = config.getboolean('GUI', 'notifications', fallback=True)
    minimize_to_tray = config.getboolean('GUI', 'minimize_to_tray', fallback=True) and HAS_TRAY

    # ── Create root window ──
    root = tk.Tk()
    root.title(f"Multi-Stream Recorder v{__version__}")

    # tk variables must be created AFTER tk.Tk()
    dark_mode = tk.BooleanVar(value=win_state.get('dark_mode',
                              config.getboolean('GUI', 'dark_mode', fallback=True)))

    # ── Windows: dark title bar via DWM API ──
    def set_title_bar_dark(dark=True):
        """Use Windows DWM API to set title bar color.

        Windows 11 and Windows 10 20H1+ support DWMWA_USE_IMMERSIVE_DARK_MODE.
        Attribute 20 works on Windows 11 build 22000+ and Win10 20H1+.
        Attribute 19 works on earlier Windows 11 insider/pre-release builds.
        We try both for maximum compatibility.
        """
        if os.name != 'nt':
            return
        try:
            import ctypes
            hwnd = ctypes.windll.user32.GetParent(root.winfo_id())
            value = ctypes.c_int(1 if dark else 0)
            # Try attribute 20 first (standard), then 19 (pre-release Win11)
            hr = ctypes.windll.dwmapi.DwmSetWindowAttribute(
                hwnd, 20, ctypes.byref(value), ctypes.sizeof(value)
            )
            if hr != 0:  # S_OK = 0
                ctypes.windll.dwmapi.DwmSetWindowAttribute(
                    hwnd, 19, ctypes.byref(value), ctypes.sizeof(value)
                )
            # Force title bar repaint by toggling size slightly
            root.update_idletasks()
        except Exception:
            pass  # older Windows or non-Windows — silently skip

    # Restore geometry
    geom = win_state.get('geometry', '1100x760')
    root.geometry(geom)
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
    gui_log_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(message)s", datefmt="%H:%M:%S"
    ))
    logging.root.addHandler(gui_log_handler)

    # ── Notification tracking (avoid spamming) ──
    _notified_live = set()       # channels we already sent a "live" notification for
    _notified_complete = set()   # channels we already sent a "complete" notification for
                                 # (edge-triggered: one toast per recording, not every refresh)

    # ── Apply theme ──
    style = ttk.Style()

    def apply_theme(*_args):
        t = DARK if dark_mode.get() else LIGHT
        style.theme_use('clam')

        root.configure(bg=t['bg'])

        # Windows: set dark/light title bar
        set_title_bar_dark(dark_mode.get())

        # ttk styles — set bordercolor to match theme (clam theme uses these)
        border_color = t['border']
        style.configure('.', background=t['bg'], foreground=t['fg'],
                        fieldbackground=t['tree_field'],
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color)
        style.configure('TFrame', background=t['bg'])
        style.configure('TLabel', background=t['bg'], foreground=t['fg'])
        style.configure('TNotebook', background=t['bg'], borderwidth=0,
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color, tabmargins=[0, 0, 0, 0])
        style.configure('TNotebook.Tab', background=t['tab_bg'], foreground=t['tab_fg'],
                        padding=[12, 4],
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color)
        style.map('TNotebook.Tab',
                  background=[('selected', t['accent']), ('!selected', t['tab_bg'])],
                  foreground=[('selected', '#ffffff'), ('!selected', t['tab_fg'])],
                  lightcolor=[('selected', border_color), ('!selected', border_color)],
                  darkcolor=[('selected', border_color), ('!selected', border_color)],
                  bordercolor=[('selected', border_color), ('!selected', border_color)])
        style.configure('Treeview', background=t['tree_field'], foreground=t['tree_fg'],
                        fieldbackground=t['tree_field'], rowheight=22,
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color)
        style.configure('Treeview.Heading', background=t['tree_heading_bg'],
                        foreground=t['tree_heading_fg'], font=("Segoe UI", 10, "bold"),
                        bordercolor=border_color, lightcolor=t['tree_heading_bg'],
                        darkcolor=border_color)
        style.map('Treeview', background=[('selected', t['select_bg'])],
                  foreground=[('selected', t['select_fg'])])
        style.configure('TCombobox', fieldbackground=t['entry_bg'], foreground=t['entry_fg'],
                        background=t['btn_bg'], arrowcolor=t['fg'],
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color)
        style.map('TCombobox', fieldbackground=[('readonly', t['entry_bg'])],
                  foreground=[('readonly', t['entry_fg'])],
                  bordercolor=[('focus', t['accent'])])
        # Style the dropdown list (Tk popdown)
        root.option_add('*TCombobox*Listbox.background', t['entry_bg'])
        root.option_add('*TCombobox*Listbox.foreground', t['entry_fg'])
        root.option_add('*TCombobox*Listbox.selectBackground', t['select_bg'])
        root.option_add('*TCombobox*Listbox.selectForeground', t['select_fg'])
        style.configure('TCheckbutton', background=t['bg'], foreground=t['fg'],
                        indicatorcolor=t['entry_bg'], indicatorrelief='flat')
        style.map('TCheckbutton',
                  background=[('active', t['bg']), ('pressed', t['bg'])],
                  indicatorcolor=[('selected', t['accent']), ('pressed', t['accent'])])
        style.configure('TButton', background=t['btn_bg'], foreground=t['btn_fg'],
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color, padding=[8, 4])
        style.map('TButton',
                  background=[('active', t['accent']), ('pressed', t['accent'])],
                  foreground=[('active', '#ffffff'), ('pressed', '#ffffff')])
        style.configure('Vertical.TScrollbar', background=t['btn_bg'],
                        bordercolor=border_color, arrowcolor=t['fg'],
                        troughcolor=t['tree_field'],
                        lightcolor=border_color, darkcolor=border_color)
        style.map('Vertical.TScrollbar',
                  background=[('active', t['accent']), ('pressed', t['accent'])])

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
        for w in [frame_left, ch_tree_frame, platform_frame, btn_small_frame, move_btn_frame,
                  frame_right, bottom_bar, btn_frame, toggle_frame, status_header]:
            w.configure(bg=t['bg'])
        ch_tree.tag_configure("enabled", foreground=t['fg'])
        ch_tree.tag_configure("disabled", foreground="#777777")
        try:
            dot_canvas.configure(bg=t['bg'])
            _redraw_dot_canvas()
        except NameError:
            pass  # not yet defined on first apply_theme call during startup
        entry.configure(bg=t['entry_bg'], fg=t['entry_fg'], insertbackground=t['fg'],
                        highlightbackground=border_color, highlightcolor=t['accent'],
                        highlightthickness=1, relief='flat')
        platform_label.configure(bg=t['bg'], fg=t['fg'])
        status_label.configure(bg=t['bg'], fg=t['fg'])
        poll_label.configure(bg=t['bg'], fg=t['fg'])
        clip_label.configure(bg=t['bg'], fg=t['fg'])
        cookie_frame.configure(bg=t['bg'])
        cookie_label.configure(bg=t['bg'], fg=t['fg'])
        cookie_indicator.configure(bg=t['bg'])
        try:
            io_meter_label.configure(bg=t['bg'])
            _paint_io_meter()
        except NameError:
            pass

        for btn, bg_key in [(add_btn, 'btn_bg'), (remove_btn, 'btn_bg'),
                            (up_btn, 'btn_bg'), (down_btn, 'btn_bg'),
                            (top_btn, 'btn_bg'), (sort_btn, 'btn_bg')]:
            btn.configure(bg=t[bg_key], fg=t['btn_fg'], activebackground=t['accent'],
                          activeforeground='#ffffff',
                          highlightbackground=border_color, relief='flat', bd=1)
        start_button.configure(bg=t['start_bg'], activebackground=t['start_bg'],
                               fg='#ffffff', activeforeground='#ffffff',
                               relief='flat', bd=0)
        stop_button.configure(bg=t['stop_bg'], activebackground=t['stop_bg'],
                              fg='#ffffff', activeforeground='#ffffff',
                              relief='flat', bd=0)

        log_text.configure(bg=t['log_bg'], fg=t['log_fg'], insertbackground=t['log_fg'],
                           highlightbackground=border_color, highlightcolor=border_color,
                           highlightthickness=1, relief='flat')

        # Status bar at bottom
        status_bar.configure(bg=t['bg'], fg=t['offline_fg'])
        update_label.configure(bg=t['bg'])

        # Context menu theming — borderwidth=0 removes the bright system border
        ctx_menu.configure(bg=t['entry_bg'], fg=t['fg'],
                          activebackground=t['accent'], activeforeground='#ffffff',
                          borderwidth=0, activeborderwidth=0, relief='flat')
        status_ctx_menu.configure(bg=t['entry_bg'], fg=t['fg'],
                                  activebackground=t['accent'], activeforeground='#ffffff',
                                  borderwidth=0, activeborderwidth=0, relief='flat')
        sort_menu.configure(bg=t['entry_bg'], fg=t['fg'],
                            activebackground=t['accent'], activeforeground='#ffffff',
                            borderwidth=0, activeborderwidth=0, relief='flat')

        # Save dark_mode preference
        try:
            state = load_window_state(state_file)
            state['dark_mode'] = dark_mode.get()
            save_window_state(state_file, state)
        except Exception:
            pass

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
        try:
            with open(channels_file, "w") as f:
                json.dump(channels, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save channels: {e}")

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

    # ── Left panel — Channel list with checkboxes ──
    frame_left = tk.Frame(root, padx=12, pady=12)
    frame_left.grid(row=0, column=0, sticky="ns", padx=(10, 5), pady=10)

    # Container so ch_tree and the dot canvas sit side by side
    ch_tree_frame = tk.Frame(frame_left)
    ch_tree_frame.pack(fill=tk.BOTH, expand=True)

    # Treeview — no cookie column; dots are drawn on a Canvas overlay instead
    ch_tree = ttk.Treeview(ch_tree_frame, columns=("check", "name"), show="tree",
                           height=22, selectmode="extended")
    ch_tree.column("#0", width=0, stretch=False)  # hidden tree column
    ch_tree.column("check", width=28, anchor="center", stretch=False)
    ch_tree.column("name", width=270, anchor="w")
    ch_tree.heading("check", text="")
    ch_tree.heading("name", text="")
    ch_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    # Narrow Canvas strip for cookie dots — drawn independently of row tags
    DOT_CANVAS_W = 18
    dot_canvas = tk.Canvas(ch_tree_frame, width=DOT_CANVAS_W, highlightthickness=0,
                           borderwidth=0)
    dot_canvas.pack(side=tk.LEFT, fill=tk.Y)

    def _redraw_dot_canvas():
        """Repaint the dot canvas to match current treeview row positions."""
        t = DARK if dark_mode.get() else LIGHT
        dot_canvas.configure(bg=t['bg'])
        dot_canvas.delete("all")
        for item in ch_tree.get_children():
            bbox = ch_tree.bbox(item)
            if not bbox:
                continue  # row is scrolled out of view
            x, y, w, h = bbox
            ch_name = ch_tree.item(item, "values")[1]  # col 0=check, 1=name
            _, color = _cookie_dot_for_channel(ch_name)
            cx = DOT_CANVAS_W // 2
            cy = y + h // 2
            r = 4
            dot_canvas.create_oval(cx - r, cy - r, cx + r, cy + r,
                                   fill=color, outline="")

    _dot_redraw_after_id = [None]

    def _schedule_dot_redraw(_event=None):
        """Coalesce scroll/select/theme redraws onto the next idle tick."""
        if _dot_redraw_after_id[0] is not None:
            return
        def _run():
            _dot_redraw_after_id[0] = None
            _redraw_dot_canvas()
        _dot_redraw_after_id[0] = dot_canvas.after(16, _run)

    # Redraw on content, selection, size, and wheel/keyboard scroll — not a
    # 500ms idle loop. yview is wrapped so arrow-key scrolling is caught too.
    ch_tree.bind("<<TreeviewSelect>>", _schedule_dot_redraw)
    ch_tree.bind("<Configure>", _schedule_dot_redraw)
    ch_tree.bind("<MouseWheel>", _schedule_dot_redraw)
    ch_tree.bind("<Button-4>", _schedule_dot_redraw)  # Linux scroll up
    ch_tree.bind("<Button-5>", _schedule_dot_redraw)  # Linux scroll down
    _orig_yview = ch_tree.yview

    def _yview(*args):
        result = _orig_yview(*args)
        if args:
            _schedule_dot_redraw()
        return result

    ch_tree.yview = _yview
    dot_canvas.after(100, _redraw_dot_canvas)

    # ── Tooltip explaining the cookie dot column ──
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
            t = DARK if dark_mode.get() else LIGHT
            tk.Label(tw, text=text, justify=tk.LEFT,
                     background=t['entry_bg'], foreground=t['fg'],
                     relief='solid', borderwidth=1,
                     font=("Segoe UI", 8), padx=6, pady=4).pack()
        def _hide(self, event=None):
            if self._tip:
                self._tip.destroy()
                self._tip = None

    _TOOLTIP_TEXT = "● Green  = cookie present\n● Orange = auth cookie expired\n● Red    = no cookie for this site\n○ Grey   = unknown / no cookies.txt"
    _Tooltip(ch_tree, _TOOLTIP_TEXT)
    _Tooltip(dot_canvas, _TOOLTIP_TEXT)

    CHECK_ON = "☑"
    CHECK_OFF = "☐"

    # Pre-fill the cookie cache immediately — before the first tree draw —
    # so dots have real colours from the start rather than going grey first.
    # This is just the data parse; widget updates happen later in update_cookie_status().
    _cookie_info_cache = [validate_cookies(find_cookies_file(config))]

    def _cookie_dot_for_channel(ch_name):
        """Return (dot_char, fg_color) for the cookie indicator column."""
        info = _cookie_info_cache[0]
        if info is None:
            return ("○", COOKIE_DOT_COLORS[COOKIE_STATUS_UNKNOWN])
        status = get_cookie_status_for_channel(ch_name, info)
        dot = "●" if status != COOKIE_STATUS_UNKNOWN else "○"
        return (dot, COOKIE_DOT_COLORS[status])

    # Simple row tags — just enabled/disabled for dimming.
    # Cookie dot colours are drawn by the Canvas, not by row tags.
    ch_tree.tag_configure("enabled", foreground="")   # themed by apply_theme
    ch_tree.tag_configure("disabled", foreground="#777777")

    def _populate_channel_tree():
        """Rebuild the channel treeview from the channels list."""
        ch_tree.delete(*ch_tree.get_children())
        for ch in channels:
            check = CHECK_ON if ch.get("enabled", True) else CHECK_OFF
            tag = "enabled" if ch.get("enabled", True) else "disabled"
            ch_tree.insert("", tk.END, values=(check, ch["name"]), tags=(tag,))
        dot_canvas.after(20, _redraw_dot_canvas)

    _populate_channel_tree()

    def _toggle_channel_check(event):
        """Toggle checkbox when user clicks in the check column."""
        region = ch_tree.identify_region(event.x, event.y)
        col = ch_tree.identify_column(event.x)
        item = ch_tree.identify_row(event.y)
        if not item:
            return
        # Only toggle if clicking the check column (#1)
        if col == "#1":
            idx = ch_tree.index(item)
            if 0 <= idx < len(channels):
                channels[idx]["enabled"] = not channels[idx].get("enabled", True)
                enabled = channels[idx]["enabled"]
                check = CHECK_ON if enabled else CHECK_OFF
                tag = "enabled" if enabled else "disabled"
                ch_tree.item(item, values=(check, channels[idx]["name"]), tags=(tag,))
                dot_canvas.after(20, _redraw_dot_canvas)
                save_channels()

    ch_tree.bind("<ButtonRelease-1>", _toggle_channel_check)

    def _double_click_toggle(event):
        """Double-clicking anywhere on a row toggles its enabled state."""
        item = ch_tree.identify_row(event.y)
        if not item:
            return
        idx = ch_tree.index(item)
        if 0 <= idx < len(channels):
            channels[idx]["enabled"] = not channels[idx].get("enabled", True)
            enabled = channels[idx]["enabled"]
            check = CHECK_ON if enabled else CHECK_OFF
            tag = "enabled" if enabled else "disabled"
            ch_tree.item(item, values=(check, channels[idx]["name"]), tags=(tag,))
            dot_canvas.after(20, _redraw_dot_canvas)
            save_channels()

    ch_tree.bind("<Double-ButtonRelease-1>", _double_click_toggle)

    platform_frame = tk.Frame(frame_left)
    platform_frame.pack(fill=tk.X, pady=(10, 5))
    platform_label = tk.Label(platform_frame, text="Platform:", font=("Segoe UI", 9))
    platform_label.pack(side=tk.LEFT)
    platform_var = tk.StringVar(value="kick")
    ttk.Combobox(platform_frame, textvariable=platform_var,
                 values=["kick", "twitch", "youtube", "rumble", "tiktok", "fishtank", "custom"], state="readonly", width=10).pack(side=tk.LEFT, padx=6)

    entry = tk.Entry(frame_left, width=34, font=("Segoe UI", 10), borderwidth=0, relief="flat")
    entry.pack(pady=6)

    btn_small_frame = tk.Frame(frame_left)
    btn_small_frame.pack(pady=4)

    recorder = None
    _stop_in_progress = [False]

    def add_channel():
        platform = platform_var.get()
        name = entry.get().strip()
        if not name:
            return

        existing_names = get_all_channel_names()
        is_valid, error_msg = validate_channel_name(name, platform, existing_names)
        if not is_valid:
            messagebox.showwarning("Invalid Channel", error_msg)
            return

        if platform == "custom":
            ch_name = f"custom:{name}"  # name is the full URL
        elif platform != "kick":
            ch_name = f"{platform}:{name}"
        else:
            ch_name = name

        ch = {"name": ch_name, "enabled": True}
        channels.append(ch)
        ch_tree.insert("", tk.END, values=(CHECK_ON, ch_name), tags=("enabled",))
        dot_canvas.after(20, _redraw_dot_canvas)
        save_channels()
        if recorder and recorder.is_running:
            recorder.start_channel(ch_name)
        elif recorder:
            recorder.status_dict[ch_name] = {"status": "Initializing", "detail": "", "size": "", "time": ""}
        entry.delete(0, tk.END)

    def remove_selected():
        selected_items = ch_tree.selection()
        # Get indices in reverse order to avoid shifting
        indices = sorted([ch_tree.index(item) for item in selected_items], reverse=True)
        for idx in indices:
            if 0 <= idx < len(channels):
                ch_name = channels[idx]["name"]
                if recorder:
                    recorder.remove_channel(ch_name)
                del channels[idx]
        _populate_channel_tree()
        save_channels()

    def _move_channel_up():
        """Move selected channel up in the list."""
        selected = ch_tree.selection()
        if not selected:
            return
        idx = ch_tree.index(selected[0])
        if idx <= 0 or idx >= len(channels):
            return
        channels[idx], channels[idx - 1] = channels[idx - 1], channels[idx]
        _populate_channel_tree()
        save_channels()
        # Re-select the moved item
        new_item = ch_tree.get_children()[idx - 1]
        ch_tree.selection_set(new_item)
        ch_tree.see(new_item)

    def _move_channel_down():
        """Move selected channel down in the list."""
        selected = ch_tree.selection()
        if not selected:
            return
        idx = ch_tree.index(selected[0])
        if idx < 0 or idx >= len(channels) - 1:
            return
        channels[idx], channels[idx + 1] = channels[idx + 1], channels[idx]
        _populate_channel_tree()
        save_channels()
        # Re-select the moved item
        new_item = ch_tree.get_children()[idx + 1]
        ch_tree.selection_set(new_item)
        ch_tree.see(new_item)

    def _move_selected_channels(to):
        """Jump selected roster rows to the top or bottom as a stable block."""
        selected = ch_tree.selection()
        if not selected:
            return
        indices = [ch_tree.index(item) for item in selected]
        new_idx = move_list_items(channels, indices, to=to)
        if not new_idx:
            return
        _populate_channel_tree()
        save_channels()
        items = ch_tree.get_children()
        to_select = [items[i] for i in new_idx if i < len(items)]
        if to_select:
            ch_tree.selection_set(to_select)
            ch_tree.see(to_select[0] if to == "top" else to_select[-1])

    def _move_channels_to_top():
        _move_selected_channels("top")

    def _move_channels_to_bottom():
        _move_selected_channels("bottom")

    add_btn = tk.Button(btn_small_frame, text="Add", command=add_channel, width=10,
                        font=("Segoe UI", 9))
    add_btn.pack(side=tk.LEFT, padx=4)
    remove_btn = tk.Button(btn_small_frame, text="Remove", command=remove_selected, width=10,
                           font=("Segoe UI", 9))
    remove_btn.pack(side=tk.LEFT, padx=4)

    # Move Up/Down buttons for channel reordering
    move_btn_frame = tk.Frame(frame_left)
    move_btn_frame.pack(pady=2)
    up_btn = tk.Button(move_btn_frame, text="▲", command=_move_channel_up, width=3,
                       font=("Segoe UI", 8))
    up_btn.pack(side=tk.LEFT, padx=2)
    down_btn = tk.Button(move_btn_frame, text="▼", command=_move_channel_down, width=3,
                         font=("Segoe UI", 8))
    down_btn.pack(side=tk.LEFT, padx=2)
    top_btn = tk.Button(move_btn_frame, text="Top", command=_move_channels_to_top, width=4,
                        font=("Segoe UI", 8))
    top_btn.pack(side=tk.LEFT, padx=2)
    _Tooltip(top_btn,
             "Move the selected channel(s) to the\n"
             "top of the roster. Right-click for\n"
             "Move to Top / Move to Bottom.")

    # ── Channel list sorting ──
    def _channel_sort_parts(ch):
        """Return (platform, username) lowercased for sorting.
        Bare names are Kick channels; custom entries sort by their URL."""
        name = ch["name"]
        if ":" in name:
            platform, user = name.split(":", 1)
        else:
            platform, user = "kick", name
        return platform.lower(), user.lower()

    def _sort_channels(mode):
        """One-shot re-order of the channel list, persisted like manual moves.

        All sorts are stable (Python's list.sort), so 'Enabled First' keeps
        the existing relative order within the enabled and disabled groups —
        and sorts can be layered: By Platform then Enabled First gives
        enabled-on-top, each group still ordered by platform.
        """
        if len(channels) < 2:
            return
        # Remember selection by name so it survives the reshuffle
        selected_names = set()
        for item in ch_tree.selection():
            idx = ch_tree.index(item)
            if 0 <= idx < len(channels):
                selected_names.add(channels[idx]["name"])

        if mode == "enabled":
            channels.sort(key=lambda ch: not ch.get("enabled", True))
        elif mode == "platform":
            channels.sort(key=_channel_sort_parts)
        elif mode == "name":
            channels.sort(key=lambda ch: _channel_sort_parts(ch)[1])

        _populate_channel_tree()
        save_channels()

        # Restore selection on the moved rows
        if selected_names:
            items = ch_tree.get_children()
            to_select = [items[i] for i, ch in enumerate(channels)
                         if ch["name"] in selected_names and i < len(items)]
            if to_select:
                ch_tree.selection_set(to_select)
                ch_tree.see(to_select[0])
        logging.info(f"Channel list sorted ({mode})")

    sort_menu = tk.Menu(root, tearoff=0)
    sort_menu.add_command(label="Enabled First", command=lambda: _sort_channels("enabled"))
    sort_menu.add_command(label="By Platform", command=lambda: _sort_channels("platform"))
    sort_menu.add_command(label="By Name (A–Z)", command=lambda: _sort_channels("name"))

    def _show_sort_menu():
        try:
            sort_menu.tk_popup(sort_btn.winfo_rootx(),
                               sort_btn.winfo_rooty() + sort_btn.winfo_height())
        finally:
            sort_menu.grab_release()

    sort_btn = tk.Button(move_btn_frame, text="Sort ▾", command=_show_sort_menu,
                         width=7, font=("Segoe UI", 8))
    sort_btn.pack(side=tk.LEFT, padx=2)

    # ── Cookie status indicator ──
    cookie_frame = tk.Frame(frame_left)
    cookie_frame.pack(fill=tk.X, pady=(8, 0))

    cookie_indicator = tk.Label(cookie_frame, text="●", font=("Segoe UI", 10))
    cookie_indicator.pack(side=tk.LEFT, padx=(0, 4))
    cookie_label = tk.Label(cookie_frame, text="Cookies: checking...", font=("Segoe UI", 8))
    cookie_label.pack(side=tk.LEFT)

    def update_cookie_status():
        """Check cookies.txt, update the global indicator, and refresh per-channel dots."""
        cookies_path = find_cookies_file(config)
        info = validate_cookies(cookies_path)

        # Refresh the cache so per-channel dots update on the next tree redraw
        _cookie_info_cache[0] = info

        base_text = f"{info['total_cookies']} entries, {len(info['domains'])} domains"

        if not cookies_path:
            cookie_indicator.configure(text="○", fg="#888888")
            cookie_label.configure(text="No cookies.txt found")
        elif not info['valid']:
            cookie_indicator.configure(text="●", fg="#F44336")
            warning = info['warnings'][0] if info['warnings'] else "Invalid format"
            cookie_label.configure(text=f"Cookies: {warning}")
        elif info['has_expired_auth']:
            cookie_indicator.configure(text="●", fg="#FF9800")
            domains = ", ".join(info['expired_domains'][:2])
            cookie_label.configure(text=f"Cookies: auth expired ({domains})")
        else:
            expiry_note = ""
            if info['auth_expiry']:
                days_left = (info['auth_expiry'] - datetime.datetime.now()).days
                if days_left < 7:
                    expiry_note = f" (auth renew in {days_left}d)"
                    if days_left < 2:
                        cookie_indicator.configure(text="●", fg="#FF9800")
                    else:
                        cookie_indicator.configure(text="●", fg="#4CAF50")
                else:
                    cookie_indicator.configure(text="●", fg="#4CAF50")
            else:
                cookie_indicator.configure(text="●", fg="#4CAF50")
            cookie_label.configure(text=f"Cookies: {base_text}{expiry_note}")

        # Refresh dots to reflect the updated cache
        dot_canvas.after(20, _redraw_dot_canvas)

    # Run initial check, then re-check every 5 minutes
    update_cookie_status()

    def _periodic_cookie_check():
        update_cookie_status()
        root.after(300_000, _periodic_cookie_check)  # 5 minutes

    root.after(300_000, _periodic_cookie_check)

    # ── Right-click context menu on channel list ──
    ctx_menu = tk.Menu(root, tearoff=0)

    def _start_selected_channel_from_list():
        """Start recording a channel from the channel list (mid-session)."""
        selected_items = ch_tree.selection()
        if not selected_items or not recorder or not recorder.is_running:
            return
        for item in selected_items:
            idx = ch_tree.index(item)
            if 0 <= idx < len(channels):
                ch_name = channels[idx]["name"]
                # Enable the channel if it's not already
                if not channels[idx].get("enabled", True):
                    channels[idx]["enabled"] = True
                    _populate_channel_tree()
                    save_channels()
                # Start it in the running session
                recorder.start_channel(ch_name)

    def _stop_selected_channel_from_list():
        """Stop recording a channel from the channel list."""
        selected_items = ch_tree.selection()
        if not selected_items or not recorder or not recorder.is_running:
            return
        names = []
        for item in selected_items:
            idx = ch_tree.index(item)
            if 0 <= idx < len(channels):
                names.append(channels[idx]["name"])
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
        selected_items = ch_tree.selection()
        if not selected_items or not recorder or not recorder.is_running:
            return
        for item in selected_items:
            idx = ch_tree.index(item)
            if 0 <= idx < len(channels):
                recorder.check_now(channels[idx]["name"])

    def show_context_menu(event):
        # Select the item under cursor if not already selected
        item = ch_tree.identify_row(event.y)
        if item:
            if item not in ch_tree.selection():
                ch_tree.selection_set(item)

        # Rebuild menu dynamically
        ctx_menu.delete(0, tk.END)

        # If a recording session is active, show Start/Stop Recording option
        if recorder and recorder.is_running and ch_tree.selection():
            idx = ch_tree.index(ch_tree.selection()[0])
            if 0 <= idx < len(channels):
                ch_name = channels[idx]["name"]
                st = recorder.status_dict.get(ch_name, {})
                status_lower = st.get("status", "").lower()
                if status_lower in ("stopped",) or ch_name not in recorder.status_dict:
                    ctx_menu.add_command(label="Start Recording", command=_start_selected_channel_from_list)
                elif "recording" in status_lower or "checking" in status_lower or "offline" in status_lower or "initializing" in status_lower:
                    ctx_menu.add_command(label="Stop Recording", command=_stop_selected_channel_from_list)
                else:
                    ctx_menu.add_command(label="Start Recording", command=_start_selected_channel_from_list)
                # "Check Now" — skip the poll timer for channels that are
                # waiting between checks (offline, error backoff, etc.)
                if status_lower.startswith(("offline", "error")):
                    ctx_menu.add_command(label="Check Now", command=_check_now_selected_from_list)
                ctx_menu.add_separator()

        ctx_menu.add_command(label="Open in Browser", command=open_channel_url)
        ctx_menu.add_command(label="Open Clips Folder", command=_open_roster_clips_dir)
        ctx_menu.add_command(label="Copy Name", command=copy_channel_name)
        ctx_menu.add_separator()
        ctx_menu.add_command(label="Toggle Selected", command=_toggle_selected_channels)
        ctx_menu.add_command(label="Enable All", command=_enable_all_channels)
        ctx_menu.add_command(label="Disable All", command=_disable_all_channels)
        ctx_menu.add_cascade(label="Sort", menu=sort_menu)
        ctx_menu.add_command(label="Move to Top", command=_move_channels_to_top)
        ctx_menu.add_command(label="Move to Bottom", command=_move_channels_to_bottom)
        ctx_menu.add_separator()
        ctx_menu.add_command(label="Remove", command=remove_selected)

        try:
            ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            ctx_menu.grab_release()

    def copy_channel_name():
        selected_items = ch_tree.selection()
        if selected_items:
            names = []
            for item in selected_items:
                idx = ch_tree.index(item)
                if 0 <= idx < len(channels):
                    names.append(channels[idx]["name"])
            root.clipboard_clear()
            root.clipboard_append("\n".join(names))

    def open_channel_url():
        selected_items = ch_tree.selection()
        if selected_items:
            idx = ch_tree.index(selected_items[0])
            if 0 <= idx < len(channels):
                _open_channel_in_browser(channels[idx]["name"])

    def _toggle_selected_channels():
        """Toggle enabled state for all selected channels."""
        for item in ch_tree.selection():
            idx = ch_tree.index(item)
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
    entry.bind("<Return>", lambda e: add_channel())
    ch_tree.bind("<Delete>", lambda e: remove_selected())

    def _about_dialog():
        deps = []
        deps.append(f"yt-dlp {YTDLP_VERSION}" if HAS_YTDLP else "yt-dlp: not found")
        deps.append(f"streamlink {STREAMLINK_VERSION}" if HAS_STREAMLINK else "streamlink: not found")
        deps.append(f"ffmpeg {FFMPEG_VERSION}" if HAS_FFMPEG else "ffmpeg: not found")
        deps.append(f"deno {DENO_VERSION}" if HAS_DENO else "deno: not found (YouTube nsig)")
        deps.append(f"psutil: {'yes' if HAS_PSUTIL else 'no'}")
        deps.append(f"pystray: {'yes' if HAS_TRAY else 'no'}")
        deps.append(f"curl_cffi: {'yes' if HAS_CURL_CFFI else 'no'} (browser impersonation)")
        deps.append(f"websocket-client: {'yes' if HAS_WEBSOCKET else 'no'} (Kick push notifications)")
        deps_str = "\n".join(deps)

        streams = config.get('Paths', 'streams_dir')
        clips = config.get('Clipping', 'clips_dir', fallback='').strip()
        if not clips:
            clips = os.path.join(streams, 'Clips')
        cfg_path = os.path.abspath(getattr(config, 'config_file', 'config.ini'))

        messagebox.showinfo(
            f"Multi-Stream Recorder v{__version__}",
            f"Multi-Stream Recorder v{__version__}\n\n"
            f"Kick, Twitch, YouTube, Rumble, TikTok,\n"
            f"Fishtank.live, and custom URLs (yt-dlp).\n\n"
            f"Dependencies:\n{deps_str}\n\n"
            f"Recordings:\n{streams}\n\n"
            f"Clips & screenshots:\n{clips}\n\n"
            f"To change those folders, edit streams_dir\n"
            f"(and optional clips_dir) in:\n{cfg_path}\n"
            f"then fully quit and relaunch.\n\n"
            f"Personal archival only — follow each site's terms.\n"
            f"cookies.txt is your login; do not share it.\n"
            f"Chaturbate and similar custom URLs are adult."
        )

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
    status_label = tk.Label(status_header, text="Live Recording Status",
                            font=("Segoe UI", 13, "bold"), anchor="w")
    status_label.pack(side=tk.LEFT)

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
        ffmpeg_path = config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg')

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
        if st.get("status", "").lower() != "recording":
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
        if st.get("status", "").lower() != "recording":
            logging.info(f"Screenshot Now: {_get_display_name(ch_name)} is not currently recording")
            return
        _start_media_job(
            ch_name, "screenshot",
            f"Screenshot Now: grabbing a frame for {_get_display_name(ch_name)}",
        )

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
        idx = ch_tree.index(selected_items[0])
        if 0 <= idx < len(channels):
            _open_clips_dir_for_channel(channels[idx]["name"])

    # pack(side=RIGHT) first = rightmost. Clip Now is the primary action.
    clip_now_btn = ttk.Button(status_header, text="Clip Now",
                              command=_clip_selected_channel, width=10)
    clip_now_btn.pack(side=tk.RIGHT)
    _Tooltip(clip_now_btn,
             "Cut a clip of the channel selected in the\n"
             "status list, using the Clip Length setting.\n"
             "Right-click a recording for the same action.")

    screenshot_now_btn = ttk.Button(status_header, text="Screenshot",
                                    command=_screenshot_selected_channel, width=12)
    screenshot_now_btn.pack(side=tk.RIGHT, padx=(0, 6))
    _Tooltip(screenshot_now_btn,
             "Grab a still frame from the channel\n"
             "selected in the status list.\n"
             "Right-click a recording for the same action.")

    clips_folder_btn = ttk.Button(status_header, text="Clips",
                                  command=_open_selected_clips_dir, width=7)
    clips_folder_btn.pack(side=tk.RIGHT, padx=(0, 6))
    _Tooltip(clips_folder_btn,
             "Open the clips & screenshots folder\n"
             "for the channel selected in the\n"
             "status list. Right-click a row for\n"
             "the same action.")

    # Whole-disk write + NIC download, immediately left of the Clips button.
    # Hidden when psutil is missing — there is no fallback counter.
    _io_sampler = IoSampler(config.get("Paths", "streams_dir"))
    io_meter_label = tk.Label(
        status_header, text=format_header(_io_sampler.last),
        font=("Segoe UI", 9), anchor="e", width=32,
    )

    def _paint_io_meter():
        if not HAS_PSUTIL:
            return
        t = DARK if dark_mode.get() else LIGHT
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
        io_meter_label.pack(side=tk.RIGHT, padx=(0, 12))
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
                if status_lower == "recording":
                    clip_seconds = config.getint('Clipping', 'clip_length_seconds', fallback=30)
                    status_ctx_menu.add_command(
                        label=f"Clip Now ({clip_seconds}s)", command=_clip_selected_channel)
                    status_ctx_menu.add_command(
                        label="Screenshot Now", command=_screenshot_selected_channel)

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
    ttk.Checkbutton(log_toolbar, text="Auto-scroll", variable=auto_scroll_var).pack(side=tk.LEFT)

    def clear_logs():
        log_text.configure(state=tk.NORMAL)
        log_text.delete("1.0", tk.END)
        log_text.configure(state=tk.DISABLED)

    ttk.Button(log_toolbar, text="Clear", command=clear_logs).pack(side=tk.RIGHT)

    log_text = tk.Text(log_tab, wrap=tk.WORD, font=("Consolas", 9), state=tk.DISABLED,
                       borderwidth=0, relief="flat")
    log_scroll = ttk.Scrollbar(log_tab, orient=tk.VERTICAL, command=log_text.yview)
    log_text.configure(yscrollcommand=log_scroll.set)
    log_scroll.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 10), pady=(0, 10))
    log_text.pack(fill=tk.BOTH, expand=True, padx=(10, 0), pady=(0, 10))

    # ── Bottom bar — buttons and toggles ──
    bottom_bar = tk.Frame(root)
    bottom_bar.grid(row=1, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))

    btn_frame = tk.Frame(bottom_bar)
    btn_frame.pack(side=tk.LEFT, padx=10)

    start_button = tk.Button(
        btn_frame, text=" Start Recording ", command=lambda: start_recording(),
        bg="#4CAF50", fg="white", font=("Segoe UI", 11, "bold"), width=18, height=2,
    )
    start_button.pack(side=tk.LEFT, padx=(0, 12))

    stop_button = tk.Button(
        btn_frame, text=" Stop Recording ", command=lambda: stop_recording(),
        bg="#F44336", fg="white", font=("Segoe UI", 11, "bold"), width=18, height=2,
        state=tk.DISABLED,
    )
    stop_button.pack(side=tk.LEFT)

    toggle_frame = tk.Frame(bottom_bar)
    toggle_frame.pack(side=tk.RIGHT, padx=10)

    dark_check = ttk.Checkbutton(toggle_frame, text="Dark Mode", variable=dark_mode,
                                 command=lambda: apply_theme())
    dark_check.pack(side=tk.RIGHT, padx=8)

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
            with open(config.config_file, 'w') as f:
                config.config.write(f)
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
            minutes = simpledialog.askfloat(
                "Custom Polling Interval",
                f"Minutes between offline checks "
                f"({POLL_MIN_MINUTES:g}–{POLL_MAX_MINUTES:g}):\n\n"
                f"Values below {POLL_MIN_MINUTES:g} min are not allowed — "
                f"checking too often risks rate limiting or IP bans.",
                parent=root,
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
            with open(config.config_file, 'w') as f:
                config.config.write(f)
        except Exception:
            pass
        _last_clip_display[0] = display_name
        logging.info(f"Clip length changed to {int(seconds)}s ({display_name})")

    def on_clip_length_change(*_args):
        selected = clip_var.get()
        if selected == CUSTOM_CLIP_LABEL:
            seconds = simpledialog.askinteger(
                "Custom Clip Length",
                f"Seconds to grab when clipping a live recording "
                f"({CLIP_MIN_SECONDS}–{CLIP_MAX_SECONDS}):",
                parent=root,
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

    # ── Status bar at very bottom ──
    dep_parts = []
    if HAS_YTDLP:
        dep_parts.append(f"yt-dlp {YTDLP_VERSION}")
    if HAS_STREAMLINK:
        dep_parts.append(f"streamlink {STREAMLINK_VERSION}")
    if HAS_FFMPEG:
        dep_parts.append(f"ffmpeg")
    dep_str = " | ".join(dep_parts) if dep_parts else "No recording tools found"

    status_bar = tk.Label(root, text=f"  v{__version__}  —  {dep_str}  —  Ctrl+Q quit, F1 about, Enter add, Del remove",
                          font=("Segoe UI", 8), anchor="w", padx=6, pady=2)
    status_bar.grid(row=2, column=0, columnspan=2, sticky="ew")

    # ── Version update check ──
    update_label = tk.Label(root, text="", font=("Segoe UI", 8, "bold"),
                            fg="#4FC3F7", cursor="hand2", anchor="e", padx=10, pady=2)
    # Overlaid on the right side of the status bar row
    update_label.grid(row=2, column=1, sticky="e")
    update_label.grid_remove()  # hidden until an update is found

    _update_url = [None]  # mutable container for the release URL

    def _on_update_found(latest_tag, release_url):
        """Called from version check thread — schedules GUI update on main thread."""
        _update_url[0] = release_url
        def _show():
            update_label.configure(text=f"  v{latest_tag} available ↗")
            update_label.grid()
        root.after(0, _show)

    def _open_release(event=None):
        if _update_url[0]:
            import webbrowser
            webbrowser.open(_update_url[0])

    update_label.bind("<Button-1>", _open_release)

    # Delay the check by 10 seconds so it doesn't slow startup
    def _delayed_version_check():
        check_for_updates(__version__, callback=_on_update_found)
    root.after(10_000, _delayed_version_check)

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

    def refresh_status():
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

            # Update tray icon tooltip if tray exists
            if tray_icon is not None:
                active = sum(1 for ch_name in recorder.status_dict
                             if recorder.status_dict.get(ch_name, {}).get("status", "").lower().startswith("recording"))
                tray_icon.title = f"Multi-Stream Recorder — {active} recording" if active else "Multi-Stream Recorder — idle"

        root.after(2500, refresh_status)

    def refresh_logs():
        """Pull log messages from queue and append to log viewer."""
        count = 0
        while count < 50:  # process up to 50 messages per tick to avoid blocking
            try:
                msg = log_queue.get_nowait()
                log_text.configure(state=tk.NORMAL)
                log_text.insert(tk.END, msg + "\n")
                # Keep log buffer reasonable (max ~5000 lines)
                line_count = int(log_text.index('end-1c').split('.')[0])
                if line_count > 5000:
                    log_text.delete("1.0", f"{line_count - 4000}.0")
                log_text.configure(state=tk.DISABLED)
                if auto_scroll_var.get():
                    log_text.see(tk.END)
                count += 1
            except stdlib_queue.Empty:
                break
        root.after(500, refresh_logs)

    # ── Recording controls ──
    def start_recording():
        nonlocal recorder
        if _stop_in_progress[0]:
            logging.info("Start ignored — stop is still finishing")
            return
        enabled = get_enabled_channels()
        if not enabled:
            messagebox.showwarning("No channels", "Enable at least one channel (click the checkbox).")
            return
        if recorder and recorder.is_running:
            messagebox.showinfo("Already running", "Recording is active.")
            return

        # Warn about large number of concurrent recordings
        if len(enabled) >= 15:
            if not messagebox.askyesno("High Channel Count",
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

        if not HAS_FFMPEG:
            issues.append("ffmpeg is not installed — recordings cannot be remuxed to MP4.")

        if issues:
            msg = "The following issues may prevent recording:\n\n" + "\n\n".join(issues) + "\n\nStart anyway?"
            if not messagebox.askyesno("Dependency Warning", msg):
                return

        logging.info("=" * 60)
        logging.info("Starting recording session...")
        for ch in enabled:
            logging.info(f"  Channel: {ch}")
        disabled_count = len(channels) - len(enabled)
        if disabled_count > 0:
            logging.info(f"  ({disabled_count} channel(s) disabled — not monitoring)")
        logging.info("=" * 60)

        recorder = StreamRecorder(enabled, config)
        threading.Thread(target=recorder.run, daemon=True).start()

        start_button.config(state=tk.DISABLED)
        stop_button.config(state=tk.NORMAL)
        _notified_live.clear()
        _notified_complete.clear()
        _notif_throttle.reset()

        if notifications_enabled:
            send_notification("Recording Started", f"Monitoring {len(enabled)} channel(s)")

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
            state = {
                'geometry': root.geometry(),
                'dark_mode': dark_mode.get(),
                'column_widths': col_widths,
            }
            save_window_state(state_file, state)
        except Exception:
            pass

        _full_quit()

    def _full_quit():
        """Fully terminate the application — stop recorder, tray, and exit.
        
        Kills ALL child processes by walking the process tree, then calls
        os._exit(0) as a final backstop.  This prevents orphaned workers.
        """
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

    # ── Apply theme and start refresh loops ──
    apply_theme()
    # Re-apply dark title bar after window is fully mapped (100ms + 500ms delays)
    # The DWM attribute only takes effect once the HWND is realized
    root.after(100, lambda: set_title_bar_dark(dark_mode.get()))
    root.after(500, lambda: set_title_bar_dark(dark_mode.get()))
    root.after(1500, refresh_status)
    root.after(500, refresh_logs)
    if HAS_PSUTIL:
        root.after(1000, _refresh_io_meter)

    try:
        root.mainloop()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt detected — shutting down...")
        _full_quit()

