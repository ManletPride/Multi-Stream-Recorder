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
    """Split ``twitch:foo`` or a pasted channel URL into ``(platform, name)``.

    Plain names return ``(None, text)``; full http(s) URLs return
    ``("custom", url)`` unless a known site matches a supported channel/video shape.
    """
    from urllib.parse import parse_qs, urlsplit
    t = (text or "").strip()
    low = t.lower()
    if low.startswith(("http://", "https://")):
        try:
            parts = urlsplit(t)
        except ValueError:
            return "custom", t
        host = (parts.hostname or "").lower()
        for prefix in ("www.", "m.", "mobile."):
            if host.startswith(prefix):
                host = host[len(prefix):]
        segs = [s for s in parts.path.split("/") if s]
        if host == "kick.com" and len(segs) == 1:
            return "kick", segs[0]
        if host == "twitch.tv" and len(segs) == 1:
            return "twitch", segs[0]
        if host == "rumble.com" and len(segs) >= 2 and segs[0].lower() in ("c", "user"):
            if len(segs) == 2 or (len(segs) == 3 and segs[2].lower() == "live"):
                return "rumble", segs[1]
        if host == "youtube.com":
            if segs and segs[0].startswith("@") and (
                len(segs) == 1 or (len(segs) == 2 and segs[1].lower() in ("live", "streams", "featured", "videos"))
            ):
                return "youtube", segs[0]
            if len(segs) >= 2 and segs[0].lower() == "channel" and segs[1].startswith("UC") and (
                len(segs) == 2 or (len(segs) == 3 and segs[2].lower() in ("live", "streams"))
            ):
                return "youtube", segs[1]
            if len(segs) == 1 and segs[0].lower() == "watch":
                vid = (parse_qs(parts.query).get("v") or [""])[0].strip()
                if re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
                    return "youtube", vid
            if len(segs) == 2 and segs[0].lower() == "live" and re.fullmatch(r"[A-Za-z0-9_-]{11}", segs[1]):
                return "youtube", segs[1]
        if host == "youtu.be" and len(segs) == 1 and re.fullmatch(r"[A-Za-z0-9_-]{11}", segs[0]):
            return "youtube", segs[0]
        if host == "tiktok.com" and segs and segs[0].startswith("@") and (
            len(segs) == 1 or (len(segs) == 2 and segs[1].lower() == "live")
        ):
            handle = segs[0].lstrip("@")
            if handle:
                return "tiktok", handle
        return "custom", t
    if ":" in t:
        p, rest = t.split(":", 1)
        if p.lower() in PLATFORM_LABELS:
            return p.lower(), rest.strip()
    return None, t


def preflight_dependency_issues(enabled_channels, *, has_streamlink, has_ytdlp, has_ffmpeg, has_deno=True):
    """Return user-facing dependency warnings for the enabled channels before starting a session."""
    streamlink_channels = []
    ytdlp_channels = []
    youtube_channels = []
    for ch in enabled_channels:
        plat, _ = split_channel_key(ch)
        if plat in ("kick", "twitch"):
            streamlink_channels.append(ch)
        elif plat in ("youtube", "tiktok", "rumble", "custom"):
            ytdlp_channels.append(ch)
        if plat == "youtube":
            youtube_channels.append(ch)

    issues = []
    if streamlink_channels and not has_streamlink:
        names = ", ".join(streamlink_channels[:3])
        issues.append(f"Kick/Twitch channels ({names}) require streamlink, which is not installed.")
    if ytdlp_channels and not has_ytdlp:
        names = ", ".join(ytdlp_channels[:3])
        issues.append(f"YouTube/TikTok/Rumble/custom channels ({names}) require yt-dlp, which is not installed.")
    if youtube_channels and not has_deno:
        names = ", ".join(youtube_channels[:3])
        issues.append(
            f"YouTube channels ({names}) work best with Deno installed — without Deno, YouTube recordings "
            "may drop out every ~15 seconds (https://deno.com)."
        )
    if not has_ffmpeg:
        issues.append("ffmpeg is not installed — recordings cannot be remuxed to MP4.")
    return issues


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
# (the badge colour is the theme token "badge")


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
    dlg.withdraw()  # shown by dialogs.present() once the caption is themed and it is centred
    dlg.title("Add channel")
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
                    bg=t["bg"], fg=t["placeholder"], font=("Segoe UI", 8))
    hint.grid(row=2, column=0, columnspan=2, sticky="w", pady=(4, 0))
    err = tk.Label(dlg, text="", bg=t["bg"], fg=t["error"], font=("Segoe UI", 9),
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
                  bg=bg, fg=t["accent_fg"] if text == "Add" else t["btn_fg"],
                  activebackground=t["btn_hover"], activeforeground=t["btn_hover_fg"],
                  relief="flat", bd=0).pack(side=tk.LEFT, padx=(6, 0), ipady=2)

    dlg.bind("<Return>", _submit)
    dlg.bind("<KP_Enter>", _submit)
    dlg.bind("<Escape>", _cancel)
    from msr import dialogs
    if dark is not None:
        dialogs.apply_title_bar(dlg, dark)
    dialogs.present(dlg, root)  # themed caption first, then centred over root, then shown
    ent.focus_set()
    ent.icursor(tk.END)
    return dlg


def open_channel_telegram_dialog(parent, channel_label, upload, topic, on_save):
    """Upload and topic for one channel. ``on_save(upload, topic_text)`` returns an error or ''."""
    import tkinter as tk
    from tkinter import ttk

    from msr import dialogs

    win, frame = dialogs.new_dialog(parent, "Telegram", padding=8)
    ttk.Label(frame, text=channel_label, style="Muted.TLabel").grid(
        row=0, column=0, columnspan=2, sticky="w")
    upload_var = tk.BooleanVar(value=bool(upload))
    ttk.Checkbutton(frame, text="Upload finished recordings from this channel",
                    variable=upload_var).grid(row=1, column=0, columnspan=2, sticky="w", pady=(6, 0))
    ttk.Label(frame, text="Topic id").grid(row=2, column=0, sticky="w", pady=(6, 0))
    topic_var = tk.StringVar(value="" if not topic else str(topic))
    ent = ttk.Entry(frame, textvariable=topic_var, width=12)
    ent.grid(row=2, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
    ttk.Label(
        frame,
        text="This channel is uploaded only when this box is ticked and "
             "Upload finished recordings is on under Options. "
             "A blank topic uses the topic set there. Same group and bot.",
        style="Muted.TLabel", wraplength=320, justify="left",
    ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(4, 0))
    err = ttk.Label(frame, text="", style="Error.TLabel", wraplength=320)
    err.grid(row=4, column=0, columnspan=2, sticky="w")
    btns = ttk.Frame(frame)
    btns.grid(row=5, column=0, columnspan=2, sticky="e", pady=(10, 0))

    def _fit():
        """Size the window to its widgets. A position-only geometry stays huge on Hyprland."""
        win.update_idletasks()
        width, height = win.winfo_reqwidth(), win.winfo_reqheight()
        place = getattr(win, "_msr_place", "") or ""
        at = place.find("+")
        tail = place[at:] if at >= 0 else ""
        win.geometry(f"{max(width, 1)}x{max(height, 1)}{tail}")

    def _ok(_e=None):
        message = on_save(bool(upload_var.get()), topic_var.get().strip()) or ""
        if message:
            err.configure(text=message)
            _fit()
            return "break"
        win.destroy()
        return "break"

    def _cancel(_e=None):
        win.destroy()
        return "break"

    ttk.Button(btns, text="Cancel", command=_cancel).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(btns, text="Save", command=_ok).pack(side=tk.LEFT)
    win.bind("<Return>", _ok)
    win.bind("<Escape>", _cancel)
    dialogs.run_modal(win, parent, ent)
    return win
