"""The Options tab: Twitch push connect + a few typed settings from config.py's schema.

Writes go through ``apply_setting(section, key, value)`` (gui.py: validate,
set, Config.save, live-apply, refresh other selectors). No secrets shown.
"""

import tkinter as tk
import webbrowser
from tkinter import ttk

from msr import themes as _themes
from msr import twitch_connect as tc
from msr import twitch_eventsub as te

TWITCH_TIP = ("Sign in to Twitch once so MSR gets instant go-live alerts (about 10 channels).\n"
              "No permissions requested; timed checks keep running as backup.")

# (section, key, label, tooltip). Type and range come from config._coerce_specs.
GENERAL_ROWS = (
    ("Timeouts", "poll_interval_minutes", "Check every (min)", "How often offline channels are checked."),
    ("Recording", "max_record_hours", "Max recording (h)", "Stop a recording after this long (0 = no limit)."),
    ("Recording", "max_file_size_gb", "Split files at (GB)", "Start a new file past this size (0 = off)."),
    ("Recording", "min_disk_space_gb", "Min free space (GB)", "Pause recording below this much free space."),
    ("Cleanup", "auto_purge_days", "Purge PendingDeletion (days)", "At app start, remove older files (0 = never)."),
    ("GUI", "notifications", "Desktop notifications", ""),
    ("GUI", "minimize_to_tray", "Minimize to tray", ""),
)


def spec_for(section, key):
    from msr.config import _coerce_specs
    for s, k, kind, default, low, high in _coerce_specs():
        if (s, k) == (section, key):
            return kind, default, low, high
    return "str", "", None, None


def range_text(kind, low, high):
    if kind == "bool":
        return ""
    if low is not None and high is not None:
        return f"{low:g}\u2013{high:g}"
    if low is not None:
        return f"\u2265 {low:g}"
    return ""


class _Tip:
    """Fallback tooltip (gui.py passes its own _Tooltip)."""
    def __init__(self, widget, text):
        self.w, self.text, self.tip = widget, text, None
        widget.bind("<Enter>", self.show, add="+")
        widget.bind("<Leave>", self.hide, add="+")

    def show(self, _e=None):
        if self.tip or not self.text:
            return
        t = _themes.current()
        self.tip = tk.Toplevel(self.w)
        self.tip.wm_overrideredirect(True)
        self.tip.wm_geometry(f"+{self.w.winfo_rootx() + 20}+{self.w.winfo_rooty() + self.w.winfo_height() + 4}")
        tk.Label(self.tip, text=self.text, justify=tk.LEFT, background=t["entry_bg"], foreground=t["fg"],
                 relief="solid", borderwidth=1, font=("Segoe UI", 8), padx=6, pady=4).pack()

    def hide(self, _e=None):
        if self.tip:
            self.tip.destroy()
            self.tip = None


class OptionsPanel:
    def __init__(self, container, config, data_dir, apply_setting, on_twitch_changed=None,
                 tooltip=None, api=te, connect_job=tc.ConnectJob, opener=webbrowser.open):
        self.config = config
        self.data_dir = data_dir
        self.apply_setting = apply_setting
        self.on_twitch_changed = on_twitch_changed or (lambda: None)
        self.api = api
        self.connect_job = connect_job
        self.opener = opener
        self.job = None
        self._uri = ""
        self._code = ""
        tip = tooltip or _Tip
        self.frame = ttk.Frame(container, padding=10)

        # -- Twitch --
        tw = ttk.LabelFrame(self.frame, text=" Twitch instant alerts ", padding=(8, 4))
        tw.pack(fill=tk.X)
        row = ttk.Frame(tw)
        row.pack(fill=tk.X)
        self.push_var = tk.BooleanVar(value=bool(config.getboolean("Twitch", "push_enabled", False)))
        self.push_cb = ttk.Checkbutton(row, text="Use instant alerts", variable=self.push_var,
                                       command=self._toggle_push)
        self.push_cb.pack(side=tk.LEFT)
        self.connect_btn = ttk.Button(row, text="Connect Twitch", command=self.connect)
        self.connect_btn.pack(side=tk.LEFT, padx=(10, 4))
        self.disconnect_btn = ttk.Button(row, text="Disconnect", command=self.disconnect)
        self.disconnect_btn.pack(side=tk.LEFT)
        self.status = ttk.Label(row, text="")
        self.status.pack(side=tk.LEFT, padx=(10, 0))
        for w in (self.push_cb, self.connect_btn):
            tip(w, TWITCH_TIP)
        self.code_row = ttk.Frame(tw)
        self.code_lbl = ttk.Label(self.code_row, text="")
        self.code_lbl.pack(side=tk.LEFT)
        t = _themes.current()
        self.link = ttk.Label(self.code_row, text="", cursor="hand2", font=("Segoe UI", 9, "underline"),
                              foreground=t["link"])
        self.link.pack(side=tk.LEFT, padx=(4, 8))
        self.link.bind("<Button-1>", lambda _e: self.open_link())
        self.copy_btn = ttk.Button(self.code_row, text="Copy code", command=self.copy_code)
        self.copy_btn.pack(side=tk.LEFT)
        self.cancel_btn = ttk.Button(self.code_row, text="Cancel", command=self.cancel)
        self.cancel_btn.pack(side=tk.LEFT, padx=(4, 0))

        # -- General (schema-driven) --
        gen = ttk.LabelFrame(self.frame, text=" General ", padding=(8, 4))
        gen.pack(fill=tk.X, pady=(8, 0))
        self.vars = {}
        for i, (section, key, label, hint) in enumerate(GENERAL_ROWS):
            kind, default, low, high = spec_for(section, key)
            if kind == "bool":
                v = tk.BooleanVar(value=bool(config.getboolean(section, key, default)))
                w = ttk.Checkbutton(gen, text=label, variable=v,
                                    command=lambda s=section, k=key, v=v: self._commit(s, k, "true" if v.get() else "false"))
                w.grid(row=i, column=0, columnspan=3, sticky="w", pady=1)
            else:
                ttk.Label(gen, text=label).grid(row=i, column=0, sticky="w", pady=1)
                v = tk.StringVar(value=config.get(section, key, str(default)))
                w = ttk.Entry(gen, textvariable=v, width=8)
                w.grid(row=i, column=1, sticky="w", padx=6, pady=1)
                for ev in ("<Return>", "<FocusOut>"):
                    w.bind(ev, lambda _e, s=section, k=key, v=v: self._commit(s, k, v.get()), add="+")
                ttk.Label(gen, text=range_text(kind, low, high), foreground=t["muted"]).grid(
                    row=i, column=2, sticky="w")
            if hint:
                tip(w, hint)
            self.vars[(section, key)] = v
        self.message = ttk.Label(self.frame, text="", foreground=t["muted"])
        self.message.pack(fill=tk.X, pady=(6, 0))
        self.refresh_status()

    # -- generic settings --

    def _commit(self, section, key, value):
        current = self.config.get(section, key, "")
        if str(value).strip() == str(current).strip():
            return
        try:
            saved = self.apply_setting(section, key, value)
        except Exception as e:
            self.message.configure(text=str(e), foreground=_themes.current()["error"])
            self.reflect(section, key)
            return
        self.message.configure(text=f"Saved {section}.{key} = {saved}", foreground=_themes.current()["muted"])

    def reflect(self, section, key):
        """Show the stored value (after a failed edit or a change from elsewhere)."""
        v = self.vars.get((section, key))
        if v is None:
            if (section, key) == ("Twitch", "push_enabled"):
                self.push_var.set(bool(self.config.getboolean("Twitch", "push_enabled", False)))
            return
        kind, default, _, _ = spec_for(section, key)
        if kind == "bool":
            v.set(bool(self.config.getboolean(section, key, default)))
        else:
            v.set(self.config.get(section, key, str(default)))

    # -- Twitch --

    def client_id(self):
        return (self.config.get("Twitch", "client_id", "") or "").strip() or te.DEFAULT_CLIENT_ID

    def _toggle_push(self):
        self._commit("Twitch", "push_enabled", "true" if self.push_var.get() else "false")
        self.on_twitch_changed()

    def refresh_status(self, state=None, login=None):
        if state is None:
            state, login = tc.status_of(self.data_dir, self.client_id(), self.api)
        t = _themes.current()
        busy = bool(self.job and self.job.running())
        if busy:
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
        self.message.configure(text="")
        self.job = self.connect_job(self.client_id(), self.data_dir, api=self.api).start()
        self.refresh_status()
        self.frame.after(100, self._poll)

    def cancel(self):
        if self.job:
            self.job.cancel()

    def disconnect(self):
        tc.disconnect(self.data_dir, self.api)
        self.message.configure(text="Twitch disconnected; timed checks continue.")
        self.refresh_status()
        self.on_twitch_changed()

    def open_link(self):
        if self._uri:
            self.opener(self._uri)

    def copy_code(self):
        if self._code:
            self.frame.clipboard_clear()
            self.frame.clipboard_append(self._code)
            self.message.configure(text="Code copied.")

    def _poll(self):
        job = self.job
        if job is None:
            return
        try:
            while True:
                ev = job.events.get_nowait()
                self._handle(ev)
        except Exception:
            pass
        if job.running() or not job.events.empty():
            self.frame.after(250, self._poll)
        else:
            self.refresh_status()

    def _handle(self, ev):
        kind = ev[0]
        if kind == "code":
            self._uri, self._code = ev[1], ev[2]
            self.code_lbl.configure(text=f"Code {self._code} \u2013 open")
            self.link.configure(text=self._uri)
            self.code_row.pack(fill=tk.X, pady=(4, 0))
            self.message.configure(text=f"Approve in the browser within {int(ev[3]) // 60} min.")
        elif kind == "connected":
            self.job = None
            if not self.push_var.get():
                self.push_var.set(True)
                self._commit("Twitch", "push_enabled", "true")
            self.message.configure(text="Connected. Instant alerts start now for running sessions.")
            self.refresh_status("connected", ev[1])
            self.on_twitch_changed()
        else:
            self.job = None
            self.message.configure(text={"expired": "Code expired \u2013 click Connect Twitch again.",
                                         "cancelled": "Cancelled."}.get(kind, f"Twitch: {ev[-1]}"))
            self.refresh_status()

    def activate(self):
        """Tab opened: validate the saved token on a thread; the result is picked up by after()."""
        import threading
        cid, box = self.client_id(), []
        threading.Thread(target=lambda: box.append(tc.check_online(self.data_dir, cid, self.api)),
                         daemon=True, name="twitch-validate").start()

        def pick(tries=0):
            if box:
                if not (self.job and self.job.running()):
                    self.refresh_status(*box[0])
            elif tries < 120:
                self.frame.after(250, lambda: pick(tries + 1))
        self.frame.after(250, pick)

    def apply_theme(self):
        t = _themes.current()
        self.link.configure(foreground=t["link"])
        self.refresh_status()
