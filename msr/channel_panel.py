"""Left roster panel: pure helpers plus the small Add Channel dialog.

Pure functions here are unit-tested headlessly (no Tk needed). The panel
widgets themselves are built in ``msr.gui.main_gui`` because they share a
lot of closure state with the rest of the window.

Channel *keys* are never changed here: Kick stays a bare name and every
other platform stays ``platform:rest`` exactly as stored in channels.json.
Only the on-screen text is different.
"""

import re

# ── Platform display ──

PLATFORM_LABELS = {
    "kick": "Kick",
    "twitch": "Twitch",
    "youtube": "YouTube",
    "rumble": "Rumble",
    "tiktok": "TikTok",
    "fishtank": "Fishtank",
    "custom": "Custom",
}

# Order shown in the Add Channel platform picker.
ADD_PLATFORMS = ["kick", "twitch", "youtube", "rumble", "tiktok", "fishtank", "custom"]


def split_channel_key(key):
    """Return ``(platform, rest)`` for a stored channel key.

    Bare names are Kick. Unknown prefixes (should not happen after
    ``coerce_channel_records``) are treated as part of a Kick-style name so
    nothing is ever silently re-labelled.
    """
    key = key or ""
    if ":" in key:
        platform, rest = key.split(":", 1)
        p = platform.lower()
        if p in PLATFORM_LABELS and p != "kick":
            return p, rest
    return "kick", key


def channel_display(key):
    """Return ``(display_name, platform_tag)`` for a roster row.

    ``twitch:clovernekoo`` -> ``("clovernekoo", "Twitch")``;
    ``vampireliquor`` -> ``("vampireliquor", "Kick")``;
    ``custom:https://www.chaturbate.com/alice/`` -> ``("chaturbate.com/alice", "Custom")``.
    """
    platform, rest = split_channel_key(key)
    if platform == "custom":
        shown = re.sub(r"^[a-z][a-z0-9+.-]*://", "", rest, flags=re.I)
        if shown.lower().startswith("www."):
            shown = shown[4:]
        shown = shown.rstrip("/") or rest
        return shown, PLATFORM_LABELS["custom"]
    return rest, PLATFORM_LABELS[platform]


def build_channel_key(name, platform):
    """Inverse of ``channel_display`` for the Add dialog (storage format)."""
    name = (name or "").strip()
    platform = (platform or "kick").strip().lower()
    if platform == "kick":
        return name
    return f"{platform}:{name}"


def guess_platform_from_text(text):
    """Split ``twitch:foo`` typed into the Add box into ``("twitch", "foo")``.

    Plain names return ``(None, text)``; full http(s) URLs return
    ``("custom", url)`` unless a known site matches a simple pattern.
    """
    t = (text or "").strip()
    low = t.lower()
    m = re.match(r"^https?://(?:www\.)?(kick\.com|twitch\.tv|rumble\.com/c)/([^/?#]+)/?$", t, re.I)
    if m:
        site = m.group(1).lower()
        plat = {"kick.com": "kick", "twitch.tv": "twitch", "rumble.com/c": "rumble"}[site]
        return plat, m.group(2)
    if low.startswith(("http://", "https://")):
        return "custom", t
    if ":" in t:
        p, rest = t.split(":", 1)
        if p.lower() in PLATFORM_LABELS:
            return p.lower(), rest.strip()
    return None, t


# ── Live state (drives the Recording group and the Live filter) ──

STATE_RECORDING = "recording"
STATE_LIVE = "live"
STATE_OFFLINE = "offline"


_LIVE_RE = re.compile(r"\blive\b", re.I)
_NOT_LIVE_RE = re.compile(r"\b(not|no longer|offline)\b[^,;)]*\blive\b", re.I)
# Worker details that mean "the stream is up but no file is being written":
# capture is (re)starting, or the reconnect grace after a drop.
_LIVE_NOT_CAPTURING_RE = re.compile(r"^(starting|retry\b)|reconnecting|bad stream data", re.I)


def channel_live_state(status_entry):
    """Map a worker status dict (the same one the Status table shows) to a state.

    * ``recording``: ``Recording`` with a file being written (any detail
      except the start-up ones below, including size-split remux).
    * ``live``: the stream is up but nothing is being written yet or
      right now: ``Recording`` with detail ``starting`` / ``retry ...``,
      ``Offline`` in the reconnect grace (``reconnecting (Ns left)``) or the
      ``bad stream data, retry`` back-off, or any status text that says
      "live" (and not "not live").
    * ``offline``: everything else (Offline, Checking, Completed,
      Error, Stopped, Processing/Remuxing after the stream ended, unknown,
      no recorder running).
    """
    if not isinstance(status_entry, dict):
        return STATE_OFFLINE
    status = str(status_entry.get("status") or "").strip()
    detail = str(status_entry.get("detail") or "").strip()
    low = status.lower()
    if _LIVE_NOT_CAPTURING_RE.search(detail) and low.startswith(("recording", "offline")):
        return STATE_LIVE
    if low.startswith("recording"):
        return STATE_RECORDING
    if low.startswith(("error", "stopped", "completed", "remux failed", "offline")):
        return STATE_OFFLINE
    text = f"{status} {detail}"
    if _LIVE_RE.search(text) and not _NOT_LIVE_RE.search(text):
        return STATE_LIVE
    return STATE_OFFLINE


# ── Grouping / filtering ──

GROUP_RECORDING = "recording"
GROUP_ENABLED = "enabled"
GROUP_DISABLED = "disabled"

VIEW_ALL = "all"
VIEW_ENABLED = "enabled"
VIEW_LIVE = "live"
VIEWS = (VIEW_ALL, VIEW_ENABLED, VIEW_LIVE)


def coerce_view(value, legacy_enabled_only=False):
    v = str(value or "").strip().lower()
    if v in VIEWS:
        return v
    return VIEW_ENABLED if legacy_enabled_only else VIEW_ALL


def roster_matches(key, enabled, state, query="", view=VIEW_ALL):
    """True if a roster row stays visible for the search text and view."""
    if view == VIEW_ENABLED and not enabled:
        return False
    if view == VIEW_LIVE and state not in (STATE_RECORDING, STATE_LIVE):
        return False
    q = (query or "").strip().lower()
    if not q:
        return True
    display, tag = channel_display(key)
    hay = f"{key} {display} {tag}".lower()
    return q in hay


def group_channels(channels, states, query="", view=VIEW_ALL):
    """Split the roster into display groups, keeping manual order inside each.

    *channels* is the channels.json list of ``{"name", "enabled"}`` dicts;
    *states* maps key -> STATE_*. Returns an ordered list of
    ``(group_id, [keys])`` for the three groups (empty groups included so the
    caller can decide whether to draw a header).

    Recording channels go on top even if they were disabled mid-session
    (they really are recording). Everything else enabled is next, disabled
    last.
    """
    groups = {GROUP_RECORDING: [], GROUP_ENABLED: [], GROUP_DISABLED: []}
    for ch in channels:
        key = ch.get("name")
        if not key:
            continue
        enabled = bool(ch.get("enabled", True))
        state = states.get(key, STATE_OFFLINE)
        if not roster_matches(key, enabled, state, query, view):
            continue
        if state == STATE_RECORDING:
            groups[GROUP_RECORDING].append(key)
        elif enabled:
            groups[GROUP_ENABLED].append(key)
        else:
            groups[GROUP_DISABLED].append(key)
    return [(g, groups[g]) for g in (GROUP_RECORDING, GROUP_ENABLED, GROUP_DISABLED)]


def move_relative(channels, key, anchor, after=False):
    """Move channel *key* to just before (or after) *anchor* in *channels*.

    Used for Move up / Move down inside a display group, where the visual
    neighbour may be several positions away in the full roster (other groups
    or filtered rows in between). Returns True if the list changed.
    """
    if key == anchor:
        return False
    src = next((i for i, c in enumerate(channels) if c.get("name") == key), -1)
    if src < 0 or not any(c.get("name") == anchor for c in channels):
        return False
    item = channels.pop(src)
    dst = next(i for i, c in enumerate(channels) if c.get("name") == anchor)
    if after:
        dst += 1
    channels.insert(dst, item)
    return src != dst


# ── Per-channel cookie badge ──

# Sites where recording does not work at all without a login.
# Fishtank needs a logged-in session (cookies or email/password in config).
COOKIE_REQUIRED_PLATFORMS = frozenset({"fishtank"})

# Status-detail text the worker / platforms.py emit when a site rejected
# the login (see check_stream_ytdlp: "403/503 (cookies expired?)",
# "login required (check cookies)"; Fishtank: "could not obtain JWT").
# Rumble's "403 (Rumble blocked yt-dlp)" is Cloudflare, not cookies.
_AUTH_ERROR_RE = re.compile(
    r"cookies? expired|check cookies|login required|sign in|"
    r"\b401\b|unauthori[sz]ed|could not obtain jwt|auth(entication)? failed",
    re.I,
)

BADGE_GLYPH = "\u26A0"   # ⚠ (Segoe UI Symbol has it on every Windows)
BADGE_COLOR = "#FFB300"  # amber


def auth_error_in_status(status_entry):
    if not isinstance(status_entry, dict):
        return None
    if not str(status_entry.get("status") or "").lower().startswith("error"):
        return None
    detail = str(status_entry.get("detail") or "")
    return detail if _AUTH_ERROR_RE.search(detail) else None


# One representative key per platform for domain lookup.
_PLATFORM_SAMPLE_KEYS = {
    "kick": "x", "twitch": "twitch:x", "youtube": "youtube:x",
    "rumble": "rumble:x", "tiktok": "tiktok:x", "fishtank": "fishtank:x",
}


def platform_cookie_summary(cookie_info, *, get_status, fishtank_login=False):
    """``{platform: 'present'|'missing'|'expired'|'unknown'|'login'}``.

    *get_status* is ``msr.util.get_cookie_status_for_channel``. ``'login'``
    means Fishtank has email/password in config.ini, so cookies don't matter.
    """
    out = {}
    for plat, sample in _PLATFORM_SAMPLE_KEYS.items():
        if plat == "fishtank" and fishtank_login:
            out[plat] = "login"
        else:
            out[plat] = get_status(sample, cookie_info)
    return out


def cookie_badge(key, cookie_info, status_entry=None, *, get_status,
                 fishtank_login=False):
    """Return a tooltip string if this row needs the amber cookie badge.

    Shown only when:
      * the site rejected the login on the last check (auth error text in
        the worker's Error detail) — any platform;
      * cookies.txt has this site's auth cookie but it has expired — any
        platform (the user clearly relies on that login);
      * the site *requires* a login (Fishtank) and there is no usable cookie
        and no email/password in config.ini.
    Returns None when everything is fine.
    """
    platform, _rest = split_channel_key(key)
    label = PLATFORM_LABELS.get(platform, platform.title())
    if platform == "custom":
        label = channel_display(key)[0].split("/", 1)[0]

    rejected = auth_error_in_status(status_entry)
    if rejected:
        return f"{label} rejected the login ({rejected}) \u2014 re-export cookies.txt"

    status = get_status(key, cookie_info)
    if status == "expired":
        return f"{label} cookie expired \u2014 re-export cookies.txt"

    if platform in COOKIE_REQUIRED_PLATFORMS and not (platform == "fishtank" and fishtank_login):
        if status == "missing":
            return (f"{label} needs a login: no {label} cookie in cookies.txt \u2014 "
                    f"export cookies while logged in (or set email/password in config.ini)")
        if status == "unknown":
            return (f"{label} needs a login: cookies.txt not found or unreadable \u2014 "
                    f"export cookies.txt (or set email/password in config.ini)")
    return None


# ── Add Channel dialog (Tk) ──

def open_add_channel_dialog(root, theme, on_add, initial_text="",
                            initial_platform="kick", dark=None):
    """Small modal-ish dialog: name + platform, Enter adds, Esc closes.

    *on_add(name, platform)* returns ``(ok, error_message)``. On error the
    message is shown inline and the dialog stays open.
    """
    import tkinter as tk
    from tkinter import ttk

    plat, name = guess_platform_from_text(initial_text)
    if plat is None:
        plat = initial_platform if initial_platform in ADD_PLATFORMS else "kick"

    t = theme
    dlg = tk.Toplevel(root)
    dlg.title("Add channel")
    dlg.transient(root)
    dlg.resizable(False, False)
    dlg.configure(bg=t["bg"], padx=14, pady=12)

    tk.Label(dlg, text="Channel name or URL", bg=t["bg"], fg=t["fg"],
             font=("Segoe UI", 9)).grid(row=0, column=0, sticky="w")
    tk.Label(dlg, text="Platform", bg=t["bg"], fg=t["fg"],
             font=("Segoe UI", 9)).grid(row=0, column=1, sticky="w", padx=(8, 0))
    name_var = tk.StringVar(value=name)
    plat_var = tk.StringVar(value=PLATFORM_LABELS[plat])
    ent = tk.Entry(dlg, textvariable=name_var, width=30, font=("Segoe UI", 10),
                   bg=t["entry_bg"], fg=t["entry_fg"], insertbackground=t["fg"],
                   relief="flat", highlightthickness=1,
                   highlightbackground=t["border"], highlightcolor=t["accent"])
    ent.grid(row=1, column=0, sticky="we", ipady=3)
    combo = ttk.Combobox(dlg, textvariable=plat_var, state="readonly", width=10,
                         values=[PLATFORM_LABELS[p] for p in ADD_PLATFORMS])
    combo.grid(row=1, column=1, padx=(8, 0), sticky="we")
    hint = tk.Label(dlg, text="Tip: paste a kick.com / twitch.tv URL or type twitch:name",
                    bg=t["bg"], fg="#8a8a8a", font=("Segoe UI", 8))
    hint.grid(row=2, column=0, columnspan=2, sticky="w", pady=(4, 0))
    err = tk.Label(dlg, text="", bg=t["bg"], fg="#F44336", font=("Segoe UI", 9),
                   wraplength=320, justify="left")
    err.grid(row=3, column=0, columnspan=2, sticky="w")
    btns = tk.Frame(dlg, bg=t["bg"])
    btns.grid(row=4, column=0, columnspan=2, sticky="e", pady=(8, 0))

    label_to_key = {PLATFORM_LABELS[p]: p for p in ADD_PLATFORMS}

    def _on_name_change(*_):
        p, rest = guess_platform_from_text(name_var.get())
        if p is not None and rest != name_var.get():
            plat_var.set(PLATFORM_LABELS[p])
            name_var.set(rest)
        elif p == "custom":
            plat_var.set(PLATFORM_LABELS["custom"])
        err.configure(text="")

    name_var.trace_add("write", _on_name_change)

    def _submit(_e=None):
        n = name_var.get().strip()
        if not n:
            err.configure(text="Enter a channel name.")
            return "break"
        ok, msg = on_add(n, label_to_key.get(plat_var.get(), "kick"))
        if ok:
            dlg.destroy()
        else:
            err.configure(text=msg or "Could not add channel.")
        return "break"

    def _cancel(_e=None):
        dlg.destroy()
        return "break"

    for text, cmd, bg in (("Cancel", _cancel, t["btn_bg"]), ("Add", _submit, t["accent"])):
        tk.Button(btns, text=text, command=cmd, width=9, font=("Segoe UI", 9),
                  bg=bg, fg="#ffffff" if text == "Add" else t["btn_fg"],
                  activebackground=t["accent"], activeforeground="#ffffff",
                  relief="flat", bd=0).pack(side=tk.LEFT, padx=(6, 0), ipady=2)

    dlg.bind("<Return>", _submit)
    dlg.bind("<KP_Enter>", _submit)
    dlg.bind("<Escape>", _cancel)
    try:
        root.update_idletasks()
        x = root.winfo_rootx() + 60
        y = root.winfo_rooty() + 80
        dlg.geometry(f"+{x}+{y}")
    except tk.TclError:
        pass
    if dark is not None:
        from msr.wintheme import set_title_bar_dark
        dlg.after(10, lambda: set_title_bar_dark(dlg, dark))
    ent.focus_set()
    ent.icursor(tk.END)
    return dlg
