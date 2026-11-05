"""Health checks for unattended runs. Tk-free (msr/health_ui.py shows the results).

Startup: ffmpeg, streamlink and yt-dlp run (a hidden --version call each), the recordings
folder is writable with enough free space, and the Twitch sign-in is still valid (one
validate call, only when instant alerts are on and a token exists).

Periodic (``[Health] check_minutes``, 0 = off): free space, recordings that stopped growing,
and a site whose go-live checks all keep failing (usually a yt-dlp / streamlink that needs an
update). The periodic pass only reads the recorder's status table and file sizes: no network,
and go-live check rates are untouched.
"""

from __future__ import annotations

import datetime
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from typing import Callable

OK, WARN, ERROR = "ok", "warn", "error"
STALL_SECONDS = 300          # a recording file that has not grown for this long is "stalled"
STALE_DAYS = 14              # yt-dlp / streamlink older than this, with a newer one out: note
SITE_STREAK = 2              # periodic checks in a row with every channel of a site failing
SITE_STREAK_SINGLE = 3       # ... when the site has only one channel (could be that channel)
STORAGE_WORDS = ("disk space", "drive not available", "not writable")
UPDATES = "updates"          # Issue.action: the Updates window can fix it


@dataclass
class Issue:
    level: str
    key: str                 # stable id: the same problem is logged once, not every pass
    text: str
    action: str = ""


@dataclass
class Report:
    issues: list = field(default_factory=list)
    checked_at: float = 0.0

    @property
    def level(self) -> str:
        levels = {i.level for i in self.issues}
        return ERROR if ERROR in levels else WARN if WARN in levels else OK


def summary(report: Report) -> str:
    """One line for the main window."""
    stamp = time.strftime("%H:%M", time.localtime(report.checked_at)) if report.checked_at else ""
    if not report.issues:
        return "\u25cf Health OK" + (f" \u00b7 {stamp}" if stamp else "")
    n = len(report.issues)
    word = "problem" if report.level == ERROR else "warning"
    return f"\u25cf {n} {word}{'s' if n > 1 else ''}: {report.issues[0].text[:48]}" + ("\u2026" if n > 1 or len(report.issues[0].text) > 48 else "")


def details(report: Report) -> str:
    if not report.issues:
        return "Everything checked is fine."
    marks = {ERROR: "\u2716", WARN: "\u26a0", OK: "\u2714"}
    return "\n".join(f"{marks.get(i.level, '-')} {i.text}" for i in report.issues)


# ------------------------------------------------------------------ startup
TOOLS = ("ffmpeg", "streamlink", "yt-dlp")


def tool_issues(versions: dict) -> list[Issue]:
    """versions: tool -> version string or None (could not run)."""
    out = []
    for name in TOOLS:
        if not versions.get(name):
            hint = ("install ffmpeg and put it on PATH (or set it in Options)" if name == "ffmpeg"
                    else "open Updates to install it")
            out.append(Issue(ERROR, f"tool:{name}", f"{name} does not run: {hint}",
                             "" if name == "ffmpeg" else UPDATES))
    return out


def check_tools(ffmpeg_path: str = "ffmpeg") -> dict:
    """A fresh hidden --version call per tool (msr.deps helpers)."""
    from msr import deps
    return {"ffmpeg": deps.check_ffmpeg(ffmpeg_path or "ffmpeg"), "streamlink": deps.check_streamlink(),
            "yt-dlp": deps.check_ytdlp()}


def probe_writable(folder: str) -> str | None:
    """None if a small temp file can be created and removed in folder, else the reason."""
    if not folder:
        return "no recordings folder set"
    if not os.path.isdir(folder):
        return "the recordings folder does not exist (it is created at the first recording)"
    try:
        with tempfile.NamedTemporaryFile(dir=folder, prefix=".msr_health_", suffix=".tmp"):
            pass
        return None
    except OSError as e:
        return f"cannot write there ({e.strerror or e})"


def disk_issues(folder: str, free_gb: float | None, min_gb: float) -> list[Issue]:
    if free_gb is None:
        return []
    if min_gb > 0 and free_gb < min_gb:
        return [Issue(ERROR, "disk", f"Only {free_gb:.1f} GB free on the recordings drive: new recordings "
                                     f"wait until {min_gb:g} GB is free")]
    warn_at = max(min_gb * 2, min_gb + 10) if min_gb > 0 else 10
    if free_gb < warn_at:
        return [Issue(WARN, "disk", f"Recordings drive is getting full: {free_gb:.1f} GB free")]
    return []


def folder_issues(folder: str, free_gb: float | None, min_gb: float,
                  probe: Callable[[str], str | None] = probe_writable) -> list[Issue]:
    why = probe(folder)
    out = [Issue(WARN if "does not exist" in (why or "") else ERROR, "folder",
                 f"Recordings folder {folder or '(none)'}: {why}")] if why else []
    return out + disk_issues(folder, free_gb, min_gb)


def twitch_issue(token: dict | None, push_enabled: bool, validate: Callable[[dict], object],
                 auth_error: type = Exception,
                 fresh: Callable[..., dict] | None = None) -> Issue | None:
    """One validate call at startup when instant alerts are on, after a refresh if due (fresh).
    Network trouble is not reported; only a refused refresh is."""
    if not push_enabled or not token:
        return None
    try:
        if fresh is None:
            validate(token)
        else:
            token = fresh()
            try:
                validate(token)
            except auth_error:
                validate(fresh(force=True, used=token.get("refresh_token")))
    except auth_error:
        return Issue(WARN, "twitch", "Twitch sign-in is no longer valid: reconnect it in Options "
                                     "(timed checks still run)")
    except Exception:
        return None
    return None


# ------------------------------------------------------------------ versions
_DATE_VERSION = re.compile(r"^(20\d\d)\.(\d{1,2})\.(\d{1,2})")


def version_date(version: str | None) -> float | None:
    """yt-dlp style versions are release dates (2026.08.19 / 2026.8.19.1)."""
    m = _DATE_VERSION.match(str(version or ""))
    if not m:
        return None
    try:
        return datetime.datetime(*(int(g) for g in m.groups())).timestamp()
    except ValueError:
        return None


def released_at(name: str, version: str | None, state: dict) -> float | None:
    """Release date of the installed version: from the version itself, else what the daily
    update check remembered from PyPI (msr.depsupdate.remember)."""
    when = version_date(version)
    if when is not None:
        return when
    rec = ((state.get("deps_check") or {}).get("released") or {}).get(name) or {}
    if version and rec.get("version") == version and rec.get("date"):
        try:
            return datetime.datetime.fromisoformat(str(rec["date"]).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def stale_issues(versions: dict, state: dict, now: float | None = None, days: int = STALE_DAYS) -> list[Issue]:
    """yt-dlp / streamlink older than `days` while a newer one is known to exist (from the
    daily check; with no check yet, an old date-versioned yt-dlp is still worth a note)."""
    now = time.time() if now is None else now
    check = state.get("deps_check") or {}
    newer = {str(n).lower() for n in (check.get("updates") or [])}
    out = []
    for name in ("yt-dlp", "streamlink"):
        ver = versions.get(name)
        when = released_at(name, ver, state)
        if not ver or when is None:
            continue
        age = int((now - when) // 86400)
        if age <= days:
            continue
        if name in newer:
            out.append(Issue(WARN, f"stale:{name}", f"{name} {ver} is {age} days old and a newer version is "
                                                    "out: open Updates (one click, the old one is kept as backup)",
                             UPDATES))
        elif not check:
            out.append(Issue(WARN, f"stale:{name}", f"{name} {ver} is {age} days old: open Updates to check",
                             UPDATES))
    return out


# ------------------------------------------------------------------ periodic
def site_of(channel_key: str) -> str:
    try:
        from msr.util import channel_key_to_dirs
        return channel_key_to_dirs(channel_key)[0].lower()
    except Exception:
        return channel_key.split(":", 1)[0].lower() if ":" in channel_key else "kick"


def tool_for_site(site: str) -> str:
    return "streamlink" if site in ("kick", "twitch") else "yt-dlp"


class Monitor:
    """State kept between periodic passes (sizes of active recordings, failing-site streaks)."""

    def __init__(self, size_of: Callable[[str], int] = os.path.getsize):
        self.size_of = size_of
        self._grow: dict[str, tuple[str, int, float]] = {}   # channel -> (file, size, last growth time)
        self._streak: dict[str, int] = {}

    def stalled(self, status: dict, now: float) -> list[Issue]:
        out, seen = [], set()
        for key, st in status.items():
            st = st or {}
            raw = st.get("raw_file")
            if str(st.get("status", "")) != "Recording" or not raw:
                continue
            seen.add(key)
            try:
                size = int(self.size_of(raw))
            except OSError:
                continue
            prev = self._grow.get(key)
            if prev is None or prev[0] != raw or size > prev[1]:
                self._grow[key] = (raw, size, now)
                continue
            idle = now - prev[2]
            if idle >= STALL_SECONDS:
                out.append(Issue(WARN, f"stall:{key}", f"{key}: recording file has not grown for {int(idle // 60)} min "
                                                       f"({os.path.basename(raw)})"))
        for key in list(self._grow):
            if key not in seen:
                del self._grow[key]
        return out

    def failing_sites(self, status: dict) -> list[Issue]:
        sites: dict[str, list[bool]] = {}
        for key, st in status.items():
            st = st or {}
            text = str(st.get("status", ""))
            detail = str(st.get("detail", "")).lower()
            failing = text == "Error" and not any(w in detail for w in STORAGE_WORDS)
            sites.setdefault(site_of(key), []).append(failing)
        out = []
        for site, flags in sites.items():
            if flags and all(flags):
                self._streak[site] = self._streak.get(site, 0) + 1
            else:
                self._streak[site] = 0
            need = SITE_STREAK if len(flags) > 1 else SITE_STREAK_SINGLE
            if self._streak[site] >= need:
                tool = tool_for_site(site)
                out.append(Issue(WARN, f"site:{site}", f"Every {site} channel ({len(flags)}) keeps failing its go-live "
                                                       f"check: {tool} probably needs an update (open Updates)",
                                 UPDATES))
        for site in list(self._streak):
            if site not in sites:
                del self._streak[site]
        return out

    def periodic(self, status: dict, now: float, folder: str, free_gb: float | None, min_gb: float) -> list[Issue]:
        return disk_issues(folder, free_gb, min_gb) + self.stalled(status, now) + self.failing_sites(status)


def merge(*groups) -> Report:
    """One report; later groups replace issues with the same key."""
    by_key: dict[str, Issue] = {}
    for group in groups:
        for issue in group or ():
            by_key[issue.key] = issue
    order = {ERROR: 0, WARN: 1, OK: 2}
    return Report(sorted(by_key.values(), key=lambda i: order.get(i.level, 3)), time.time())


def changes(old: Report | None, new: Report) -> tuple[list[Issue], list[str]]:
    """(new issues, keys that cleared) - so the log gets one line per change."""
    before = {i.key: i for i in (old.issues if old else [])}
    after = {i.key: i for i in new.issues}
    fresh = [i for k, i in after.items() if k not in before or before[k].level != i.level]
    return fresh, [k for k in before if k not in after]
