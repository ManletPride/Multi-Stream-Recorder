"""What an agent needs beyond the live channel table: health, events, finished recordings.

Used by ``msr.control`` (status file and commands). Nothing here holds or returns a secret:
cookie checks report counts, domains and expiry times, never values; Fishtank only says
whether a login is set.
"""

import logging
import os
import threading
import time
from collections import deque

FINISHED_KEEP = 50        # finished recordings remembered this run
FINISHED_IN_STATUS = 20   # of those, how many go into msr_status.json
EVENTS_KEEP = 500
EVENT_KINDS = ("live", "stopped", "remux_started", "finished", "error")
HEALTH_EVERY = 30.0       # seconds between health checks for the status file
QUEUE_EVERY = 15.0        # seconds between scans of Recorded/ for the cleanup queue


def iso(ts):
    if not ts:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts))


def _newest(folder, suffixes, since=0.0):
    best = None
    try:
        with os.scandir(folder) as it:
            for e in it:
                if not e.is_file() or not e.name.lower().endswith(suffixes):
                    continue
                m = e.stat().st_mtime
                if m >= since and (best is None or m > best[0]):
                    best = (m, e.path, e.stat().st_size)
    except OSError:
        return None
    return best


def channel_dirs(config, key, category):
    """Folders of one channel under Processed/ or Recorded/ (main and fallback drive)."""
    from msr import storage
    from msr.util import channel_key_to_dirs
    platform, user = channel_key_to_dirs(key)
    streams = config.get("Paths", "streams_dir", fallback="") or ""
    roots = [streams]
    fb = storage.fallback_root(config.get("Paths", "fallback_dir", fallback=""), streams)
    if fb:
        roots.append(fb)
    return [os.path.join(r, category, platform, *user.split("/")) for r in roots if r]


class Tracker:
    """Watches status changes (and presence) and keeps per-channel history plus an event log.

    ``seen(key, status)`` is fed from the recorder's status hook and from the status writer's
    periodic diff; a status that did not change is ignored, so both feeds can overlap.
    """

    def __init__(self, config, clock=time.time):
        self.config = config
        self.clock = clock
        self._cond = threading.Condition()
        self._events = deque(maxlen=EVENTS_KEEP)
        self.seq = 0
        self.finished = deque(maxlen=FINISHED_KEEP)
        self.channels = {}
        self._last = {}
        self._live = {}
        self._cache = {}
        self.finishing = {}       # key -> {"state": waiting_for_cleanup|remuxing, "since", "seq"}
        self.stop_seq = {}        # key -> seq of its last "stopped" event (until it records again)
        self.stop_finished = set()  # keys whose recording finished after that stop

    def info(self, key):
        return self.channels.setdefault(key, {"last_error": "", "last_error_at": None, "errors": 0,
                                              "restarts": 0, "last_live": None, "last_recorded": None})

    def emit(self, kind, key, **data):
        with self._cond:
            self.seq += 1
            ev = {"seq": self.seq, "event": kind, "channel": key, "time": self.clock(), **data}
            ev["at"] = iso(ev["time"])
            self._events.append(ev)
            self._cond.notify_all()
        return ev

    def events(self, after=0):
        with self._cond:
            return [e for e in self._events if e["seq"] > after]

    def wait(self, kinds, channel=None, timeout=300.0, after=None):
        """First event of ``kinds`` (for ``channel``) after seq ``after`` (default: from now on)."""
        end = time.monotonic() + max(0.0, float(timeout))
        with self._cond:
            mark = self.seq if after is None else int(after)
            while True:
                for e in self._events:
                    if e["seq"] > mark and e["event"] in kinds and (channel is None or e["channel"] == channel):
                        return e
                left = end - time.monotonic()
                if left <= 0:
                    return None
                self._cond.wait(min(left, 1.0))

    # -------------------------------------------------- feeds
    def seen(self, key, st):
        with self._cond:
            self._seen(key, st)

    def _seen(self, key, st):
        st = st or {}
        status = str(st.get("status", "") or "")
        detail = str(st.get("detail", "") or "")
        old = self._last.get(key)
        self._last[key] = (status, detail)
        low = status.lower()
        now = self.clock()
        info = self.info(key)
        was_recording = (old or ("",))[0].lower().startswith("recording")
        if low.startswith("recording"):
            info["last_recorded"] = info["last_live"] = now
            if not was_recording:
                self._started(key)
                self._set_live(key, True, "recording")
            return
        if old == (status, detail):
            return
        if low == "stopped" and was_recording and key not in self.finishing:
            self._stopping(key, True)   # stopped outside control (the window's right-click Stop)
        elif low == "completed":
            self._live[key] = False
            self._finished(key, True, "")
        elif low == "remux failed":
            self._live[key] = False
            self._error(info, key, f"Remux failed: {detail}", now)
            self._finished(key, False, detail)
        elif low == "error" or low.startswith("error"):
            self._error(info, key, detail or status, now)
            if "restart" in detail.lower():
                info["restarts"] += 1

    def seen_presence(self, snapshot):
        with self._cond:
            self._seen_presence(snapshot)

    def _seen_presence(self, snapshot):
        for key, p in (snapshot or {}).items():
            live = bool((p or {}).get("live"))
            if live:
                self.info(key)["last_live"] = self.clock()
            self._set_live(key, live, "presence")

    def _set_live(self, key, live, source):
        was = self._live.get(key, False)
        self._live[key] = live
        if live and not was:
            self.emit("live", key, source=source)

    def _error(self, info, key, text, now):
        info["errors"] += 1
        info["last_error"] = text[:300]
        info["last_error_at"] = now
        self.emit("error", key, error=info["last_error"])

    def _finished(self, key, ok, error, path=None):
        """Record a finished recording. Without ``path``: the newest .mp4 in the channel's
        Processed folder (or .ts in Recorded when the remux failed)."""
        from msr.library import sidecar_seconds
        size = None
        if path is None:
            since = self.clock() - 6 * 3600
            hit = None
            try:
                for folder in channel_dirs(self.config, key, "Processed" if ok else "Recorded"):
                    found = _newest(folder, (".mp4",) if ok else (".ts",), since)
                    if found and (hit is None or found[0] > hit[0]):
                        hit = found
            except Exception as e:
                logging.debug(f"agent status: finding the finished file of {key}: {e}")
            path, size = (hit[1], hit[2]) if hit else ("", None)
        elif path:
            try:
                size = os.path.getsize(path)
            except OSError:
                size = None
        entry = {"channel": key, "path": path, "size": size,
                 "duration_seconds": sidecar_seconds(path) if path and ok else None,
                 "finished_at": iso(self.clock()), "time": self.clock(), "remux_ok": ok}
        if error:
            entry["error"] = error[:300]
        self.finished.append(entry)
        self.finishing.pop(key, None)
        if key in self.stop_seq:
            self.stop_finished.add(key)
        self.emit("finished", key, path=path, remux_ok=ok)

    # -------------------------------------------------- stop -> cleanup remux -> finished
    def stopping(self, key, was_recording):
        """A channel was stopped (control or window). Returns the seq of its "stopped" event:
        a wait for "finished" after that seq cannot miss the recording's finish."""
        with self._cond:
            return self._stopping(key, was_recording)

    def _stopping(self, key, was_recording):
        ev = self.emit("stopped", key, was_recording=bool(was_recording))
        self.stop_seq[key] = ev["seq"]
        self.stop_finished.discard(key)
        self._live[key] = False
        self._last.pop(key, None)   # its next Recording (restart / re-add) is a new live start
        if was_recording:
            self.finishing[key] = {"state": "waiting_for_cleanup", "since": self.clock(), "seq": ev["seq"]}
        else:
            self.finishing.pop(key, None)
        self._last[key] = ("Stopped", "by user")
        return ev["seq"]

    def prune(self, present):
        """Channels gone from the recorder's status (removed in the window): a recording one is
        a stop; either way its next Recording is a new live start."""
        with self._cond:
            for key in [k for k in self._last if k not in present]:
                if (self._last.get(key) or ("",))[0].lower().startswith("recording") \
                        and key not in self.finishing:
                    self._stopping(key, True)
                self._last.pop(key, None)
                self._live[key] = False

    def started(self, key):
        with self._cond:
            self._started(key)

    def _started(self, key):
        self.stop_seq.pop(key, None)
        self.finishing.pop(key, None)
        self.stop_finished.discard(key)

    def finish_state(self, key):
        """For the status row: "waiting_for_cleanup" / "remuxing" while a stopped recording is
        being finished, else None; and whether it finished since the last stop."""
        with self._cond:
            f = self.finishing.get(key)
            return (f["state"] if f else None), key in self.stop_finished

    def default_after(self, key):
        """Where a "finished" wait for ``key`` starts: its last stop (until it records again)."""
        with self._cond:
            return self.stop_seq.get(key)

    def _key_for(self, platform, user_dir):
        from msr.util import channel_key_to_dirs
        for key in list(self.finishing) + [k for k in self._last if k not in self.finishing]:
            try:
                if channel_key_to_dirs(key) == (platform, user_dir):
                    return key
            except Exception:
                pass
        return None

    def on_cleanup(self, event, *args, **kwargs):
        """Hook for BackgroundCleaner._report (cleanup thread)."""
        with self._cond:
            if event == "remux_started":
                platform, user_dir, raw = args
                key = self._key_for(platform, user_dir)
                if key is None:
                    return
                if key in self.finishing:
                    self.finishing[key]["state"] = "remuxing"
                self.emit("remux_started", key, path=raw)
            elif event == "finished":
                platform, user_dir, path = args
                key = self._key_for(platform, user_dir)
                if key is None:
                    return
                ok = bool(kwargs.get("ok"))
                if not ok:
                    self._error(self.info(key), key, f"Remux failed: {kwargs.get('error', '')}", self.clock())
                self._finished(key, ok, kwargs.get("error", ""), path=path)
            elif event == "channel_done":
                self._nothing_left(args[0])
            elif event == "pass_done":
                started = kwargs.get("started") or 0
                for key in [k for k, f in self.finishing.items() if f["since"] <= started]:
                    self._nothing_left(key)

    def _nothing_left(self, key):
        """The cleanup for a stopped channel ended without a file of it (too small, none, kept)."""
        if key not in self.finishing:
            return
        try:  # a raw file is still there (locked through every pass): a later pass finishes it
            for folder in channel_dirs(self.config, key, "Recorded"):
                if _newest(folder, (".ts",)):
                    return
        except Exception:
            pass
        self.finishing.pop(key, None)
        if key in self.stop_seq:
            self.stop_finished.add(key)
        self.emit("finished", key, path="", remux_ok=None, note="no recording file was left to remux")

    def recent_finished(self, n=FINISHED_IN_STATUS):
        return list(self.finished)[-n:][::-1]

    # -------------------------------------------------- cached health / cleanup queue (status writer)

    def cached(self, name):
        return self._cache.get(name, (0.0, None))[1]

    def forget(self, name):
        self._cache.pop(name, None)

    def refresh(self, app, force=False):
        cache = self._cache
        now = time.monotonic()
        if force or now - cache.get("health", (-1e9, None))[0] >= HEALTH_EVERY:
            try:
                cache["health"] = (now, health(app.config, update=app.update_info()))
            except Exception as e:
                cache["health"] = (now, {"ok": None, "problems": [f"health check failed: {e}"]})
        if force or now - cache.get("queue", (-1e9, None))[0] >= QUEUE_EVERY:
            busy = busy_channels(app.recorder)
            try:
                cache["queue"] = (now, cleanup_queue(app.config, busy))
            except Exception as e:
                cache["queue"] = (now, [])
                logging.debug(f"agent status: cleanup queue: {e}")


# ------------------------------------------------------------------ health
def cookie_health(config):
    from msr.util import find_cookies_file, validate_cookies
    try:
        path = find_cookies_file(config)
    except Exception:
        path = None
    if not path:
        return {"found": False}
    v = validate_cookies(path)
    exp = v.get("auth_expiry")
    return {"found": True, "file": os.path.basename(path), "valid": bool(v.get("valid")),
            "cookies": v.get("total_cookies", 0), "domains": len(v.get("domains") or []),
            "earliest_auth_expiry": exp.isoformat(timespec="seconds") if exp else None,
            "auth_expired": bool(v.get("has_expired_auth")),
            "expired_domains": list(v.get("expired_domains") or []),
            "warnings": list(v.get("warnings") or [])}


def _disk(folder, min_gb):
    from msr.status import _free_gb
    free = _free_gb(folder) if folder else None
    return {"path": folder, "free_gb": free, "min_gb": min_gb,
            "low": free is not None and free < min_gb}


def health(config, update=None, tools=None):
    """Tools, disk, cookies, Fishtank login and a plain list of problems."""
    from msr import status as st
    from msr import storage
    from msr.config import fishtank_password
    if tools is None:
        tools = st._tools()
    problems = []
    try:
        problems = list(st.build_status(os.path.abspath(config.config_file), tools=tools)["problems"])
    except Exception as e:
        problems.append(f"status check failed: {e}")
    min_gb = config.getfloat("Recording", "min_disk_space_gb", fallback=5.0)
    streams = config.get("Paths", "streams_dir", fallback="") or ""
    fb = storage.fallback_root(config.get("Paths", "fallback_dir", fallback=""), streams)
    disk = {"main": _disk(streams, min_gb), "fallback": _disk(fb, min_gb) if fb else None}
    if disk["fallback"] and disk["fallback"]["low"]:
        problems.append(f"fallback drive free space ({disk['fallback']['free_gb']} GB) is below "
                        f"min_disk_space_gb ({min_gb:g})")
    try:
        cookies = cookie_health(config)
    except Exception as e:
        cookies = {"found": None, "error": str(e)}
    if cookies.get("auth_expired"):
        problems.append("login cookies have expired: " + ", ".join(cookies.get("expired_domains") or []))
    email = bool((config.get("Fishtank", "email", fallback="") or "").strip())
    fishtank = {"login_set": email and bool(fishtank_password(config)), "email_set": email}
    return {"ok": not problems, "problems": problems, "checked_at": iso(time.time()),
            "tools": tools, "disk": disk, "cookies": cookies, "fishtank": fishtank,
            "update": update or {}}


# ------------------------------------------------------------------ files
def cleanup_queue(config, busy_keys=()):
    """Raw .ts files waiting in Recorded/ (main and fallback) for the cleanup remux."""
    from msr import storage
    from msr.util import channel_key_to_dirs
    streams = config.get("Paths", "streams_dir", fallback="") or ""
    roots = [streams]
    fb = storage.fallback_root(config.get("Paths", "fallback_dir", fallback=""), streams)
    if fb:
        roots.append(fb)
    busy = set()
    for k in busy_keys:
        platform, user = channel_key_to_dirs(k)
        busy.add(os.path.normcase(os.path.join(platform, *user.split("/"))))
    out = []
    for root in roots:
        base = os.path.join(root, "Recorded")
        for dirpath, _dirs, files in os.walk(base):
            rel = os.path.normcase(os.path.relpath(dirpath, base))
            for name in files:
                if not name.lower().endswith(".ts"):
                    continue
                path = os.path.join(dirpath, name)
                try:
                    s = os.stat(path)
                except OSError:
                    continue
                out.append({"path": path, "size": s.st_size, "modified": iso(s.st_mtime),
                            "recording_now": rel in busy})
    out.sort(key=lambda r: r["path"])
    return out


def parse_since(text, now=None):
    """'24h', '7d', '90m' or a date/time 'YYYY-MM-DD[ HH:MM]' -> epoch seconds."""
    import datetime as _dt
    now = time.time() if now is None else now
    t = str(text or "").strip().lower()
    if not t:
        return 0.0
    units = {"m": 60, "h": 3600, "d": 86400, "w": 7 * 86400}
    if t[-1] in units and t[:-1].replace(".", "", 1).isdigit():
        return now - float(t[:-1]) * units[t[-1]]
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return _dt.datetime.strptime(t.upper() if "T" in fmt else t, fmt).timestamp()
        except ValueError:
            pass
    raise ValueError("since must look like 24h, 7d, 2026-10-29 or 2026-10-29 14:00")


def busy_channels(rec):
    """Channels whose files are being written or remuxed right now."""
    try:
        statuses = dict(rec.status_dict) if rec is not None else {}
    except Exception:
        return []
    out = []
    for key, st in statuses.items():
        low = str((st or {}).get("status", "")).lower()
        if low.startswith(("recording", "remux", "processing", "stitch", "finaliz")):
            out.append(key)
    return out
