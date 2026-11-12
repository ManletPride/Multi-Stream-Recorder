"""The Options tab: Twitch connect plus grouped settings, scrollable.

Rows come from ``GROUPS``. Writes go through ``apply_setting(section, key, value)``
(gui.py: ``control.apply_setting``, the same checks and save as ``config_set``, then live
apply and sync other selectors). Secrets are
never shown or logged.
"""

import logging
import os
import re
import threading
import tkinter as tk
import webbrowser
from tkinter import filedialog, ttk

from msr import themes as _themes
from msr.tooltip import Tooltip
from msr.wheel import bind_wheel, unbind_wheel, wheel_steps, widget_is_inside
from msr import twitch_connect as tc
from msr import twitch_eventsub as te

MANAGE_URL = "https://www.twitch.tv/settings/connections"
MANAGE_TIP = "See or remove apps that can access your Twitch account"
TWITCH_TIP = ("Sign in to Twitch once so MSR gets instant go-live alerts (about 10 channels).\n"
              "No permissions requested; timed checks keep running as backup.")

NOW, NEXT, RESTART = "now", "next recording", "restart"

COOKIE_SYNC_WARNING = "Cookies are your browser login; keep this file out of synced or shared folders."
_SYNCED = ("onedrive", "dropbox", "google drive", "googledrive", "my drive", "desktop", "icloud drive", "iclouddrive")


def in_synced_folder(path):
    """True if a folder in *path* looks like OneDrive/Dropbox/Google Drive/Desktop."""
    if not path:
        return False
    parts = re.split(r"[\\/]+", os.path.abspath(str(path)).lower())[:-1]
    return any(p == s or p.startswith(s + " ") or p.startswith(s + "-") for p in parts for s in _SYNCED)


def row(section, key, label, kind="num", hint="", tip="", applies=NEXT, choices=(), width=8):
    return dict(section=section, key=key, label=label, kind=kind, hint=hint, tip=tip,
                applies=applies, choices=tuple(choices), width=width)


# 0-meanings verified in worker.py / recorder.py / util.py / storage.py.
GROUPS = (
    ("Recording", False, (
        row("Recording", "quality", "Quality", "choice", "best = highest available",
            "streamlink / yt-dlp quality name. Unknown names fall back to best.",
            choices=("best", "1080p60", "1080p", "720p60", "720p", "480p", "worst"), width=10),
        row("Recording", "filename_pattern", "File name", "pattern", "",
            "Tokens: {username} {platform} {date} {time} {timestamp} {title}\n"
            "Keep {timestamp} (or {date}+{time}) so files never collide.", width=28),
        row("Recording", "split_on_resolution_change", "New file when the resolution changes", "bool",
            tip="Starts a fresh file if the stream changes resolution (e.g. TikTok battles);\n"
                "mixing two resolutions in one file breaks playback."),
        row("Recording", "max_record_hours", "Max part length (h)", hint="0 = no limit",
            tip="Long streams are cut into parts of at most this length (seamless hand-off)."),
        row("Recording", "max_file_size_gb", "Split files at (GB)", hint="0 = never split by size",
            tip="The next part starts before this one closes, so nothing is lost."),
        row("Recording", "min_disk_space_gb", "Min free space (GB)", hint="0 = no free-space check",
            tip="New recordings wait while the recordings drive has less free space than this."),
    )),
    ("YouTube", False, (
        row("YouTube", "live_from_start", "Record from the start of the broadcast", "bool",
            hint="keeps what aired before MSR noticed",
            tip="yt-dlp --live-from-start on the first part of a broadcast: it catches up\n"
                "faster than real time, then follows live. No DVR = normal live recording."),
        row("YouTube", "live_from_start_max_minutes", "Only if live for at most (min)",
            hint="minutes, at least check interval + 2; raise it to grab older footage (more like a VOD download)", width=6,
            tip="Covers a start missed between checks. Older broadcasts record from now, no backlog.\n"
                "Never less than the check interval + 2 min. 0 = no limit."),
    )),
    ("Checking", False, (
        row("Timeouts", "poll_interval_minutes", "Check every (min)", hint="0.5\u2013120",
            tip="How often offline channels are checked. Kick and Twitch instant alerts\n"
                "wake a channel sooner; these checks always keep running.", applies=NOW),
    )),
    ("Folders", False, (
        row("Paths", "streams_dir", "Recordings", "dir", "used from the next start",
            "Changing it while recording is refused. MSR's one-copy-per-folder lock\n"
            "moves with it at the next start.", applies=RESTART, width=34),
        row("Clipping", "clips_dir", "Clips + screenshots", "dir", "blank = Recordings\\Clips",
            applies=NOW, width=34),
        row("Paths", "fallback_dir", "Backup (drive gone)", "dir", "blank = temp folder, off = pause",
            "Where recording continues while the recordings drive is unplugged;\n"
            "files move back when it returns.", width=34),
        row("Paths", "cookies_file", "Cookies file", "file", "blank = find automatically",
            "Netscape cookies.txt for logged-in streams (subscriber-only, age-gated).", width=34),
    )),
    ("Clips", False, (
        row("Clipping", "clip_length_seconds", "Clip length (s)", hint="5\u20131800", applies=NOW,
            tip="Same setting as Clip Length in the bottom bar."),
        row("Clipping", "screenshot_format", "Screenshot format", "choice", "jpg is ~5x smaller than png",
            choices=("jpg", "png", "webp"), applies=NOW, width=6),
        row("Clipping", "screenshot_quality", "Screenshot quality", hint="", applies=NOW,
            tip="jpg: 2\u201331, lower is better.  webp: 50\u2013100, higher is better.  png: ignored."),
    )),
    ("Cleanup", False, (
        row("Cleanup", "purge_on_startup", "Empty old PendingDeletion files at start", "bool", applies=RESTART),
        row("Cleanup", "auto_purge_days", "Keep PendingDeletion (days)", hint="0 = never delete",
            applies=RESTART),
        row("Cleanup", "max_log_size_mb", "Rotate log at (MB)", hint="0 = never rotate", applies=RESTART),
        row("Cleanup", "log_backup_count", "Old logs kept", hint="0 = keep none", applies=RESTART),
    )),
    ("Sounds", False, (
        row("Sounds", "go_live", "Chime when a channel goes live", "sound", applies=NOW, choices=("live",),
            tip="Plays when a channel starts recording - not for split parts or reconnects."),
        row("Sounds", "problems", "Alert when something needs attention", "sound", applies=NOW, choices=("problem",),
            tip="Recording error, low disk space, recordings drive gone, cookies expired.\n"
                "The same problem sounds again only after 5 minutes."),
        row("Sounds", "clip_saved", "Tick when a clip or screenshot is saved", "sound", applies=NOW,
            choices=("clip",)),
        row("Sounds", "quiet_while_locked", "Quiet while Windows is locked", "bool", applies=NOW),
    )),
    ("Fishtank login", False, (
        row("Fishtank", "email", "Email", "text", "", width=28),
        row("Fishtank", "password", "Password", "password", "", width=20,
            tip="Encrypted in config.ini on Windows (DPAPI), plain elsewhere; never logged.\n"
                "MSR_FISHTANK_PASSWORD, if set, wins and keeps it out of the file."),
    )),
    ("Other", False, (
        row("Library", "extra_dirs", "Extra Library folders", "dirlist", "scan only, ; separated",
            "Shown in the Library tab as Other folder N. Never cleaned up.", applies=NOW, width=34),
        row("Health", "check_minutes", "Health check every (min)", hint="0 = off",
            tip="Free space, recordings that stopped growing, sites whose checks all keep failing.\n"
                "Reads the status table only: no extra network, check rates unchanged.", applies=NOW),
        row("Control", "enabled", "Agent control (msr_ctl)", "bool", applies=RESTART,
            tip="Local control server + status file used by msr_ctl and agents."),
        row("Advanced", "ffmpeg_path", "ffmpeg", "ffmpeg", "", "Program name on PATH or a full path.",
            applies=RESTART, width=28),
    )),
    ("Advanced timing", True, (
        row("Timeouts", "reconnect_grace_minutes", "Fast re-checks after a drop (min)",
            hint="15 s checks; 0 = off"),
        row("Timeouts", "reconnect_extended_minutes", "Then 30 s checks for (min)", hint="0 = off"),
        row("Timeouts", "error_backoff_max_minutes", "Longest wait after errors (min)",
            hint="0 = retry without waiting (not advised)"),
        row("Timeouts", "poll_jitter_percent", "Check-time jitter (%)", hint="0\u2013100, 0 = exact",
            tip="Random spread so channels are not all checked in the same second."),
    )),
)


def all_rows():
    for _title, _collapsed, rows in GROUPS:
        yield from rows


def default_for(section, key):
    from msr.config import Config
    from msr.control import EXTRA_SETTINGS
    d = dict(Config.DEFAULT_CONFIG.get(section, {}))
    d.update(EXTRA_SETTINGS.get(section, {}))
    return d.get(key, "")


def preview_name(pattern):
    from msr.util import build_filename
    pattern = (pattern or "").strip()
    if not pattern:
        return "(empty - {username}_{timestamp} is used)"
    return build_filename(pattern, "channel", "twitch", "Stream title") + ".mp4"


def _fishtank_test(email, password):
    from msr.platforms import FishtankAuth
    auth = FishtankAuth(None, logging.getLogger("msr.options"), email=email, password=password)
    return bool(auth._login_with_credentials())


def _ffmpeg_check(path):
    from msr.deps import check_ffmpeg
    return check_ffmpeg(path)


class OptionsPanel:
    def __init__(self, container, config, data_dir, apply_setting, on_twitch_changed=None,
                 tooltip=None, api=te, connect_job=tc.ConnectJob, opener=webbrowser.open,
                 ask_dir=None, ask_file=None, fishtank_test=_fishtank_test, ffmpeg_check=_ffmpeg_check,
                 environ=os.environ, sound_test=None, open_updates=None):
        self.config = config
        self.data_dir = data_dir
        self.apply_setting = apply_setting
        self.on_twitch_changed = on_twitch_changed or (lambda: None)
        self.api = api
        self.connect_job = connect_job
        self.opener = opener
        self.ask_dir = ask_dir or (lambda initial: filedialog.askdirectory(parent=self.frame, initialdir=initial or None))
        self.ask_file = ask_file or (lambda initial: filedialog.askopenfilename(parent=self.frame, initialdir=initial or None))
        self.fishtank_test = fishtank_test
        self.ffmpeg_check = ffmpeg_check
        self.environ = environ
        self.sound_test = sound_test
        self.open_updates = open_updates
        self.tip = tooltip or Tooltip  # same helper as the rest of the window (msr/tooltip.py)
        self.job = None
        self._foreign = None   # a sign-in started elsewhere (control); shown, not driven
        self._uri = ""
        self._code = ""
        self.vars = {}
        self.hints = {}
        self.groups = {}
        t = _themes.current()

        # scrollable body
        self.frame = ttk.Frame(container)
        self.canvas = tk.Canvas(self.frame, highlightthickness=0, borderwidth=0, bg=t["bg"])
        self.scroll = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scroll.set)
        self.scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.body = ttk.Frame(self.canvas, padding=10)
        self._win = self.canvas.create_window(0, 0, window=self.body, anchor="nw")
        self.body.bind("<Configure>", lambda _e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.bind("<Configure>", lambda e: self.canvas.itemconfigure(self._win, width=e.width))
        # bind_all so the wheel works over the entries, not only the canvas.
        # Leave also fires when the pointer moves onto a child (NotifyInferior);
        # that must not drop the binding. Hiding the tab (Unmap) does drop it.
        self.frame.bind("<Enter>", self._wheel_on, add="+")
        self.frame.bind("<Leave>", self._wheel_off, add="+")
        self.frame.bind("<Unmap>", self._wheel_unmap, add="+")

        self._build_twitch(t)
        if open_updates is not None:
            self._build_updates()
        for title, collapsed, rows in GROUPS:
            self._build_group(title, collapsed, rows, t)
        self.message = ttk.Label(self.body, text="", foreground=t["muted"])
        self.message.pack(fill=tk.X, pady=(6, 0))
        self.refresh_status()

    def _wheel_on(self, _event=None):
        bind_wheel(self.canvas, self._wheel, all=True)

    def _wheel_off(self, _event=None):
        try:
            x, y = self.frame.winfo_pointerxy()
            under = self.frame.winfo_containing(x, y)
        except tk.TclError:
            under = None
        if widget_is_inside(under, self.frame):
            return
        unbind_wheel(self.canvas, all=True)

    def _wheel_unmap(self, event):
        if getattr(event, "widget", None) is self.frame:
            unbind_wheel(self.canvas, all=True)

    def _wheel(self, e):
        steps = wheel_steps(e)
        if steps and self.canvas.yview() != (0.0, 1.0):
            self.canvas.yview_scroll(-steps, "units")

    # -- building --

    def _build_twitch(self, t):
        tw = ttk.LabelFrame(self.body, text=" Twitch instant alerts ", padding=(8, 4))
        tw.pack(fill=tk.X)
        r = ttk.Frame(tw)
        r.pack(fill=tk.X)
        self.push_var = tk.BooleanVar(value=bool(self.config.getboolean("Twitch", "push_enabled", False)))
        self.push_cb = ttk.Checkbutton(r, text="Use instant alerts", variable=self.push_var, command=self._toggle_push)
        self.push_cb.pack(side=tk.LEFT)
        self.connect_btn = ttk.Button(r, text="Connect Twitch", command=self.connect)
        self.connect_btn.pack(side=tk.LEFT, padx=(10, 4))
        self.disconnect_btn = ttk.Button(r, text="Disconnect", command=self.disconnect)
        self.disconnect_btn.pack(side=tk.LEFT)
        self.manage_btn = ttk.Button(r, text="Manage on Twitch", command=self.manage)
        self.manage_btn.pack(side=tk.LEFT, padx=(4, 0))
        self.tip(self.manage_btn, MANAGE_TIP)
        self.status = ttk.Label(r, text="")
        self.status.pack(side=tk.LEFT, padx=(10, 0))
        for w in (self.push_cb, self.connect_btn):
            self.tip(w, TWITCH_TIP)
        self.code_row = ttk.Frame(tw)
        self.code_lbl = ttk.Label(self.code_row, text="")
        self.code_lbl.pack(side=tk.LEFT)
        self.link = ttk.Label(self.code_row, text="", cursor="hand2", font=("Segoe UI", 9, "underline"),
                              foreground=t["link"])
        self.link.pack(side=tk.LEFT, padx=(4, 8))
        self.link.bind("<Button-1>", lambda _e: self.open_link())
        ttk.Button(self.code_row, text="Copy code", command=self.copy_code).pack(side=tk.LEFT)
        ttk.Button(self.code_row, text="Cancel", command=self.cancel).pack(side=tk.LEFT, padx=(4, 0))

    def _build_updates(self):
        """Shortcut to the Updates window (versions, one-click pip update with rollback)."""
        box = ttk.LabelFrame(self.body, text=" Updates ", padding=(8, 4))
        box.pack(fill=tk.X, pady=(8, 0))
        btn = ttk.Button(box, text="Updates\u2026", command=self.open_updates)
        btn.pack(side=tk.LEFT)
        ttk.Label(box, text="yt-dlp, streamlink, ffmpeg and packages; nothing updates on its own",
                  style="Muted.TLabel").pack(side=tk.LEFT, padx=(10, 0))
        self.tip(btn, "When a site stops recording, an old yt-dlp / streamlink is the usual cause.\n"
                      "Updates are one click and the previous version is put back if it fails.")

    def _build_group(self, title, collapsed, rows, t):
        holder = ttk.Frame(self.body)
        holder.pack(fill=tk.X, pady=(8, 0))
        box = ttk.LabelFrame(holder, text=f" {title} ", padding=(8, 4))
        box.columnconfigure(2, weight=1)
        if collapsed:
            btn = ttk.Button(holder, text=f"\u25b8 {title}")

            def toggle(b=btn, f=box, name=title):
                if f.winfo_manager():
                    f.pack_forget()
                    b.configure(text=f"\u25b8 {name}")
                else:
                    f.pack(fill=tk.X)
                    b.configure(text=f"\u25be {name}")
            btn.configure(command=toggle)
            btn.pack(anchor="w")
            self.groups[title] = (box, toggle)
        else:
            box.pack(fill=tk.X)
            self.groups[title] = (box, None)
        for i, spec in enumerate(rows):
            self._build_row(box, i, spec, t)
        if title == "Folders":
            self.cookie_warn = ttk.Label(box, text="", foreground=t["warn"])
            self.cookie_warn.grid(row=len(rows), column=0, columnspan=3, sticky="w")
            self._cookie_warning()

    def _build_row(self, box, i, spec, t):
        s, k, kind = spec["section"], spec["key"], spec["kind"]
        cur = self.config.get(s, k, default_for(s, k))
        if kind in ("bool", "sound"):
            v = tk.BooleanVar(value=str(cur).strip().lower() in ("1", "true", "yes", "on"))
            w = ttk.Checkbutton(box, text=spec["label"], variable=v,
                                command=lambda: self._commit(s, k, "true" if v.get() else "false"))
            w.grid(row=i, column=0, columnspan=1 if kind == "sound" else 2 if spec["hint"] else 3,
                   sticky="w", pady=1)
            if kind == "bool" and spec["hint"]:
                hint = ttk.Label(box, text=spec["hint"], foreground=t["muted"])
                hint.grid(row=i, column=2, sticky="w")
                self.hints[(s, k)] = hint
            if kind == "sound":
                ttk.Button(box, text="Test", width=6,
                           command=lambda ev=spec["choices"][0]: self.sound_test and self.sound_test(ev)
                           ).grid(row=i, column=1, sticky="w", padx=6, pady=1)
        else:
            ttk.Label(box, text=spec["label"]).grid(row=i, column=0, sticky="w", pady=1)
            cell = ttk.Frame(box)
            cell.grid(row=i, column=1, sticky="w", padx=6, pady=1)
            v = tk.StringVar(value="" if kind == "password" else cur)
            if kind == "choice":
                w = ttk.Combobox(cell, textvariable=v, values=spec["choices"], width=spec["width"])
                w.bind("<<ComboboxSelected>>", lambda _e: self._commit(s, k, v.get()), add="+")
            else:
                w = ttk.Entry(cell, textvariable=v, width=spec["width"], show="\u2022" if kind == "password" else "")
            w.pack(side=tk.LEFT)
            for ev in ("<Return>", "<FocusOut>"):
                w.bind(ev, lambda _e: self._commit_field(spec, v), add="+")
            if kind in ("dir", "file", "dirlist", "ffmpeg"):
                ttk.Button(cell, text="\u2026", width=3, command=lambda: self._browse(spec, v)).pack(side=tk.LEFT, padx=(2, 0))
            if kind == "ffmpeg":
                ttk.Button(cell, text="Check", command=lambda: self.check_ffmpeg(v.get())).pack(side=tk.LEFT, padx=(4, 0))
            if kind == "password":
                ttk.Button(cell, text="Clear", command=lambda: self._commit(s, k, "")).pack(side=tk.LEFT, padx=(4, 0))
                ttk.Button(cell, text="Test login", command=self.test_fishtank).pack(side=tk.LEFT, padx=(4, 0))
            hint = ttk.Label(box, text=spec["hint"], foreground=t["muted"])
            hint.grid(row=i, column=2, sticky="w")
            self.hints[(s, k)] = hint
            if kind == "pattern":
                v.trace_add("write", lambda *_: self._preview_pattern(v.get()))
                self._preview_pattern(cur)
            if kind == "password":
                self._password_hint()
            if (s, k) == ("Clipping", "screenshot_quality"):
                self._quality_hint()
        if spec["tip"]:
            self.tip(w, spec["tip"])
        self.vars[(s, k)] = v

    # -- row helpers --

    def _preview_pattern(self, pattern):
        h = self.hints.get(("Recording", "filename_pattern"))
        if h is not None:
            try:
                h.configure(text="\u2192 " + preview_name(pattern))
            except Exception:
                h.configure(text="")

    def _quality_hint(self):
        fmt = (self.config.get("Clipping", "screenshot_format", "jpg") or "jpg").lower()
        h = self.hints.get(("Clipping", "screenshot_quality"))
        if h is not None:
            h.configure(text={"png": "png ignores quality", "webp": "50\u2013100, higher is better"}
                        .get(fmt, "2\u201331, lower is better"))

    def _password_hint(self):
        h = self.hints.get(("Fishtank", "password"))
        if h is None:
            return
        from msr.config import FISHTANK_PASSWORD_ENV
        if (self.environ.get(FISHTANK_PASSWORD_ENV) or "").strip():
            text = f"set by {FISHTANK_PASSWORD_ENV} (wins)"
        else:
            from msr.config import secret_is_saved
            text = {"saved": "saved - type to replace",
                    "unreadable": "saved on another PC/user - type it again"}.get(
                secret_is_saved(self.config, "Fishtank", "password"), "not set")
        h.configure(text=text)

    def _commit_field(self, spec, v):
        if spec["kind"] == "password":
            if v.get():
                self._commit(spec["section"], spec["key"], v.get())
                v.set("")
            return
        self._commit(spec["section"], spec["key"], v.get())

    def _browse(self, spec, v):
        cur = v.get().strip()
        if spec["kind"] in ("file", "ffmpeg"):
            got = self.ask_file(os.path.dirname(cur) if cur else "")
        else:
            got = self.ask_dir(cur.split(";")[-1].strip() if cur and cur.lower() != "off" else "")
        if not got:
            return
        got = os.path.normpath(got)
        if spec["kind"] == "dirlist":
            parts = [p.strip() for p in cur.split(";") if p.strip()]
            if got not in parts:
                parts.append(got)
            got = ";".join(parts)
        v.set(got)
        self._commit(spec["section"], spec["key"], got)

    def cookies_path(self):
        """Configured cookies file, else the auto-found one ('' if none)."""
        cur = str(self.config.get("Paths", "cookies_file", "") or "").strip()
        if cur:
            return cur
        try:
            from msr.util import find_cookies_file
            return find_cookies_file(self.config) or ""
        except Exception:
            return ""

    def _cookie_warning(self):
        lbl = getattr(self, "cookie_warn", None)
        if lbl is None:
            return
        if in_synced_folder(self.cookies_path()):
            lbl.configure(text="\u26a0 " + COOKIE_SYNC_WARNING)
            lbl.grid()
        else:
            lbl.configure(text="")
            lbl.grid_remove()

    def _say(self, text, error=False):
        t = _themes.current()
        self.message.configure(text=text, foreground=t["error"] if error else t["muted"])

    def _commit(self, section, key, value):
        current = self.config.get(section, key, default_for(section, key))
        if str(value).strip() == str(current or "").strip():
            return
        try:
            saved = self.apply_setting(section, key, value)
        except Exception as e:
            self._say(str(e), error=True)
            self.reflect(section, key)
            return
        from msr.control import masked
        spec = next((r for r in all_rows() if (r["section"], r["key"]) == (section, key)), None)
        when = {NOW: "", RESTART: " (applies after restarting MSR)",
                NEXT: " (applies from the next recording)"}.get(spec["applies"] if spec else NOW, "")
        label = spec["label"] if spec else f"{section}.{key}"
        shown = masked(key, saved)
        self._say(f"Saved {label}" + (f" = {shown}" if shown not in ("", "<set>") else "") + when)
        self.reflect(section, key)

    def reflect(self, section, key):
        """Show the stored value (after a failed edit or a change from elsewhere)."""
        if (section, key) == ("Twitch", "push_enabled"):
            self.push_var.set(bool(self.config.getboolean("Twitch", "push_enabled", False)))
            return
        if (section, key) == ("Clipping", "screenshot_format"):
            self._quality_hint()
        if (section, key) in (("Paths", "cookies_file"), ("Paths", "streams_dir")):
            self._cookie_warning()
        if (section, key) == ("Fishtank", "password"):
            self._password_hint()
            return
        v = self.vars.get((section, key))
        if v is None:
            return
        cur = self.config.get(section, key, default_for(section, key))
        if isinstance(v, tk.BooleanVar):
            v.set(str(cur).strip().lower() in ("1", "true", "yes", "on"))
        else:
            v.set(cur)

    # -- background checks (thread + after(); never block Tk) --

    def _background(self, work, done):
        box = []
        threading.Thread(target=lambda: box.append(self._safe(work)), daemon=True, name="options-check").start()

        def pick(tries=0):
            if box:
                done(box[0])
            elif tries < 240:
                self.frame.after(250, lambda: pick(tries + 1))
        self.frame.after(100, pick)

    @staticmethod
    def _safe(work):
        try:
            return ("ok", work())
        except Exception as e:
            return ("error", type(e).__name__)

    def check_ffmpeg(self, path):
        path = (path or "ffmpeg").strip() or "ffmpeg"
        h = self.hints[("Advanced", "ffmpeg_path")]
        h.configure(text="checking\u2026", foreground=_themes.current()["muted"])

        def done(res):
            t = _themes.current()
            ver = res[1] if res[0] == "ok" else None
            h.configure(text=(f"ffmpeg {str(ver).split()[0]}" if ver else "does not run"),
                        foreground=t["ok"] if ver else t["error"])
        self._background(lambda: self.ffmpeg_check(path), done)

    def test_fishtank(self):
        from msr.config import fishtank_password
        email = str(self.vars[("Fishtank", "email")].get() or "").strip()
        typed = self.vars[("Fishtank", "password")].get()
        password = typed or fishtank_password(self.config)
        h = self.hints[("Fishtank", "password")]
        if not email or not password:
            h.configure(text="enter email and password first", foreground=_themes.current()["warn"])
            return
        h.configure(text="testing\u2026", foreground=_themes.current()["muted"])

        def done(res):
            t = _themes.current()
            ok = res == ("ok", True)
            h.configure(text="login OK" if ok else "login failed", foreground=t["ok"] if ok else t["error"])
        self._background(lambda: self.fishtank_test(email, password), done)

    # -- Twitch --

    def client_id(self):
        return tc.client_id_for(self.config)

    def _toggle_push(self):
        self._commit("Twitch", "push_enabled", "true" if self.push_var.get() else "false")
        self.on_twitch_changed()

    def refresh_status(self, state=None, login=None):
        if state is None:
            state, login = tc.status_of(self.data_dir, self.client_id(), self.api)
        t = _themes.current()
        busy = bool(self.job and self.job.running())
        foreign = bool(self._foreign and self._foreign.running())
        if foreign:
            text, color = "Sign-in already in progress (started by an agent)\u2026", t["info"]
            busy = True
        elif busy:
            text, color = "Waiting for approval on twitch.tv\u2026", t["info"]
        elif state == "connected":
            text, color = f"Connected as {login or '?'}", t["ok"]
        elif state == "expired":
            text, color = "Expired \u2013 connect again", t["warn"]
        else:
            text, color = "Not connected", t["muted"]
        self.state = state
        self.status.configure(text=text, foreground=color)
        self.connect_btn.configure(state=tk.DISABLED if busy else tk.NORMAL,
                                   text="Reconnect" if state == "connected" else "Connect Twitch")
        self.disconnect_btn.configure(state=tk.NORMAL if state != "none" and not busy else tk.DISABLED)
        if not busy:
            self.code_row.pack_forget()

    def connect(self):
        if self.job and self.job.running():
            return
        other = tc.active()
        if other is not None and other is not self.job:
            return self._show_foreign(other)
        self._say("")
        job = self.connect_job(self.client_id(), self.data_dir, api=self.api)
        job.owner = "options"
        try:
            self.job = job.start()
        except tc.SignInBusy as e:
            return self._show_foreign(e.job)
        self.refresh_status()
        self.frame.after(100, self._poll)

    def cancel(self):
        job = self.job or self._foreign or tc.active()
        if job:
            job.cancel()

    def _show_foreign(self, job):
        """Another sign-in runs: show its code and state; poll until it ends."""
        first = self._foreign is not job
        self._foreign = job
        info = getattr(job, "info", {}) or {}
        if info.get("user_code") and info.get("user_code") != self._code:
            self._code = info["user_code"]
            self._uri = te.safe_verification_uri(info.get("verification_url"), self._code)
            self.code_lbl.configure(text=f"Code {self._code} \u2013 open")
            self.link.configure(text=self._uri)
            self.code_row.pack(fill=tk.X, pady=(4, 0))
        if first:
            self._say("A Twitch sign-in is already in progress; approve it or Cancel.")
        if job.running():
            self.refresh_status()
            self.frame.after(250, lambda: self._foreign is job and self._show_foreign(job))
        else:
            self._foreign = None
            self._say("")
            self.refresh_status()

    def disconnect(self):
        """Revoke + delete on a thread so a slow network never freezes the GUI."""
        self.disconnect_btn.configure(state=tk.DISABLED)
        self._say("Disconnecting\u2026")

        def done(res):
            if res != ("ok", True):
                logging.getLogger("msr.options").warning(
                    "Twitch: could not revoke the token online; deleted it locally. "
                    "Use Manage on Twitch to remove MSR's access.")
                self._say("Disconnected locally (Twitch unreachable) \u2013 use Manage on Twitch to remove access.")
            else:
                self._say("Twitch disconnected; timed checks continue.")
            self.refresh_status()
            self.on_twitch_changed()
        self._background(lambda: tc.disconnect(self.data_dir, self.api), done)

    def manage(self):
        self.opener(MANAGE_URL)

    def open_link(self):
        if self._uri:
            self.opener(te.safe_verification_uri(self._uri, self._code))

    def copy_code(self):
        if self._code:
            self.frame.clipboard_clear()
            self.frame.clipboard_append(self._code)
            self._say("Code copied.")

    def _poll(self):
        job = self.job
        if job is None:
            return
        try:
            while True:
                self._handle(job.events.get_nowait())
        except Exception:
            pass
        if job.running() or not job.events.empty():
            self.frame.after(250, self._poll)
        else:
            self.refresh_status()

    def _handle(self, ev):
        kind = ev[0]
        if kind == "code":
            self._code = ev[2]
            self._uri = te.safe_verification_uri(ev[1], self._code)
            self.code_lbl.configure(text=f"Code {self._code} \u2013 open")
            self.link.configure(text=self._uri)
            self.code_row.pack(fill=tk.X, pady=(4, 0))
            self._say(f"Approve in the browser within {int(ev[3]) // 60} min.")
        elif kind == "connected":
            self.job = None
            if not self.push_var.get():
                self.push_var.set(True)
                self._commit("Twitch", "push_enabled", "true")
            self._say("Connected. Instant alerts start now for running sessions.")
            self.refresh_status("connected", ev[1])
            self.on_twitch_changed()
        else:
            self.job = None
            self._say({"expired": "Code expired \u2013 click Connect Twitch again.",
                       "cancelled": "Cancelled."}.get(kind, f"Twitch: {ev[-1]}"))
            self.refresh_status()

    def activate(self):
        """Tab opened: validate the saved token on a thread."""
        cid = self.client_id()

        def done(res):
            if res[0] == "ok" and not (self.job and self.job.running()) and not self._foreign:
                self.refresh_status(*res[1])
        other = tc.active()
        if other is not None and other is not self.job:
            self._show_foreign(other)
        self._background(lambda: tc.check_online(self.data_dir, cid, self.api), done)

    def apply_theme(self):
        t = _themes.current()
        self.canvas.configure(bg=t["bg"])
        self.link.configure(foreground=t["link"])
        for h in self.hints.values():
            h.configure(foreground=t["muted"])
        self._preview_pattern(self.vars[("Recording", "filename_pattern")].get())
        self.refresh_status()
