"""Agent / headless control: one command set for the GUI, --headless and msr_ctl.py.

Three pieces, no tkinter here:

* ``Controller`` runs the commands (status, roster edits, start/stop, check, clip, settings,
  library, history, logs, update, quit) against an *app adapter*: the GUI (``msr/gui.py``
  marshals roster edits onto the Tk thread), the headless loop (``HeadlessApp``) or no running
  copy at all (``OfflineApp``, used by ``msr_ctl.py`` to edit the files atomically).
* ``ControlServer``: newline-delimited JSON over TCP on 127.0.0.1 only, one request per
  connection, every request must carry the random token written to ``msr_control.json`` in the
  data folder (git-ignored, deleted on exit). ``call()`` is the client.
* ``StatusWriter``: ``msr_status.json`` in the data folder, rewritten every few seconds. No
  secrets: custom URLs are reduced to their host, sensitive settings are never included.

Set ``MSR_CONTROL=0`` or ``[Control] enabled = false`` to turn the server and status file off.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import secrets
import socket
import tempfile
import threading
import time
from collections import deque

from msr import __version__
from msr.util import redact_for_log

CONTROL_FILE = "msr_control.json"
STATUS_FILE = "msr_status.json"
ENV_SWITCH = "MSR_CONTROL"
PROTOCOL = 1
# API version of the control channel (msr_control.json, ping, help, msr_status.json). Bump it only
# for a breaking change: a command, argument, reply field or exit code removed, renamed or given a
# different meaning. Adding commands, arguments or fields does not bump it. msr_ctl.py refuses to
# talk to a copy with another api_version (exit code 7) instead of guessing.
API_VERSION = 1
MAX_REQUEST = 1 << 20
STATUS_INTERVAL = 3.0

# Settings an agent may change beyond Config.DEFAULT_CONFIG (sections the app reads with fallbacks).
EXTRA_SETTINGS = {
    "Library": {"extra_dirs": ""},
    "Control": {"enabled": "true", "status_interval_seconds": "3"},
    "GUI": {"theme": ""},
}
LIVE_SETTINGS = {("Timeouts", "poll_interval_minutes")}   # pushed into a running session at once
NOW_SETTINGS = {("Clipping", "clip_length_seconds"), ("Clipping", "screenshot_format"),
                ("Clipping", "screenshot_quality"), ("Clipping", "clips_dir")}  # read per clip


class ControlError(Exception):
    """A command that could not be done. ``code`` is stable and machine-readable."""

    def __init__(self, message, code="error"):
        super().__init__(message)
        self.code = code


# ------------------------------------------------------------------ small helpers
def enabled(config) -> bool:
    if os.environ.get(ENV_SWITCH, "").strip() == "0":
        return False
    try:
        return config.getboolean("Control", "enabled", fallback=True)
    except Exception:
        return True


def _write_private(path, data: bytes) -> None:
    """Atomic write; the file is readable by this user only where the OS allows (POSIX 0600)."""
    folder = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=folder, prefix=os.path.basename(path) + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        if os.name != "nt":
            os.chmod(tmp, 0o600)
        from msr.history import _replace
        _replace(tmp, path)  # retried: a reader may hold the old file open for a moment on Windows
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def display_name(key: str) -> str:
    from msr.status import display_name as _display
    return _display(key)


def _platform(key: str) -> str:
    from msr.channel_panel import split_channel_key
    return split_channel_key(key)[0]


def _status_copy(recorder) -> dict:
    if recorder is None:
        return {}
    for _ in range(5):
        try:
            return {k: dict(v) for k, v in dict(recorder.status_dict).items()}
        except RuntimeError:  # changed size while copying (another thread); try again
            time.sleep(0.01)
    return {}


def is_running_session(recorder) -> bool:
    return bool(recorder is not None and getattr(recorder, "is_running", False))


def resolve_channel(records, ref) -> int:
    """Index of the channel ``ref`` names: 1-based number or "#N", the exact key, the key
    ignoring case, or the shown name (custom channels: ``custom:host``; or the name without
    its platform) when that is unique."""
    from msr.util import channel_dedupe_key
    if isinstance(ref, int) or (isinstance(ref, str) and ref.lstrip("#").isdigit() and ref.startswith("#")):
        n = int(str(ref).lstrip("#"))
        if 1 <= n <= len(records):
            return n - 1
        raise ControlError(f"There is no channel number {n} (the list has {len(records)}).", "not_found")
    ref = str(ref or "").strip()
    if not ref:
        raise ControlError("No channel given.", "bad_request")
    for i, rec in enumerate(records):
        if rec["name"] == ref:
            return i
    wanted = channel_dedupe_key(ref)
    for i, rec in enumerate(records):
        if channel_dedupe_key(rec["name"]) == wanted:
            return i
    from msr.channel_panel import split_channel_key
    shown = [i for i, rec in enumerate(records) if display_name(rec["name"]).lower() == ref.lower()]
    if not shown:  # the name as the roster shows it, without the platform
        shown = [i for i, rec in enumerate(records) if split_channel_key(rec["name"])[1].lower() == ref.lower()]
    if len(shown) == 1:
        return shown[0]
    if len(shown) > 1:
        raise ControlError(f"'{ref}' matches {len(shown)} channels; use the number (#N) instead.", "ambiguous")
    if ref.isdigit() and 1 <= int(ref) <= len(records):
        return int(ref) - 1
    raise ControlError(f"No channel '{ref}' in the list.", "not_found")


def _refs(args, key="channels"):
    refs = args.get(key)
    if refs is None and "channel" in args:
        refs = [args["channel"]]
    if isinstance(refs, (str, int)):
        refs = [refs]
    if not refs:
        raise ControlError("Name at least one channel.", "bad_request")
    return list(refs)


def _truthy(value) -> bool:
    return value is True or str(value).strip().lower() in ("1", "true", "yes", "y", "on")


# ------------------------------------------------------------------ roster edits (pure, on the list)
_SITE_URLS = (  # full channel URLs that map to a platform (other http(s) URLs are custom)
    (r"^https?://(?:www\.)?kick\.com/([^/?#]+)/?$", "kick"),
    (r"^https?://(?:www\.)?twitch\.tv/([^/?#]+)/?$", "twitch"),
    (r"^https?://(?:www\.|m\.)?youtube\.com/(@[^/?#]+)(?:/live|/streams)?/?$", "youtube"),
    (r"^https?://(?:www\.|m\.)?youtube\.com/channel/(UC[\w-]+)(?:/live|/streams)?/?$", "youtube"),
    (r"^https?://(?:www\.|m\.)?youtube\.com/watch\?v=([\w-]{11})(?:&.*)?$", "youtube"),
    (r"^https?://youtu\.be/([\w-]{11})(?:\?.*)?$", "youtube"),
    (r"^https?://(?:www\.)?rumble\.com/c/([^/?#]+)/?$", "rumble"),
    (r"^https?://(?:www\.)?tiktok\.com/@([^/?#]+)(?:/live)?/?$", "tiktok"),
)


def parse_add_text(text, platform=None):
    """(platform, name) for what a user types to add a channel: kick:knut, twitch:x,
    youtube:@x, a bare name (Kick, as in the GUI) or a full channel URL. A lone platform word
    (kick) is refused with a hint: "add kick knut" used to add two channels."""
    import re
    from msr.channel_panel import PLATFORM_LABELS
    text = str(text or "").strip()
    if platform:
        platform = str(platform).strip().lower()
        if platform not in PLATFORM_LABELS:
            raise ControlError(f"Unknown platform '{platform}'. Use one of: {', '.join(PLATFORM_LABELS)}.",
                               "invalid")
        if text.lower().startswith(platform + ":"):
            text = text[len(platform) + 1:]
        return platform, text
    for pattern, plat in _SITE_URLS:
        m = re.match(pattern, text, re.I)
        if m:
            return plat, m.group(1)
    if text.lower().startswith(("http://", "https://")):
        return "custom", text
    if ":" in text:
        head, rest = text.split(":", 1)
        if head.lower() in PLATFORM_LABELS:
            return head.lower(), rest.strip()
    if text.lower() in PLATFORM_LABELS:
        p = text.lower()
        raise ControlError(f"'{text}' is a platform, not a channel. Write {p}:NAME "
                           f"(for example add {p}:somename) or add NAME --platform {p}.", "invalid")
    return "kick", text


def roster_add(records, name, platform=None, enable=True) -> str:
    from msr.channel_panel import build_channel_key
    from msr.util import validate_channel_name
    platform, name = parse_add_text(name, platform)
    existing = [r["name"] for r in records]
    ok, problem = validate_channel_name(name, platform, existing)
    if not ok:
        raise ControlError(problem or "That channel name is not valid.", "invalid")
    key = build_channel_key(name, platform)
    if key in existing:
        raise ControlError(f"{key} is already in the list.", "conflict")
    records.append({"name": key, "enabled": bool(enable)})
    return key


def roster_set_enabled(records, indices, value) -> list:
    changed = []
    for i in indices:
        if records[i].get("enabled", True) != value:
            records[i]["enabled"] = value
            changed.append(records[i]["name"])
    return changed


def roster_move(records, indices, to) -> None:
    from msr.channel_panel import move_list_items
    if to in ("top", "bottom"):
        move_list_items(records, indices, to=to)
        return
    if to not in ("up", "down"):
        raise ControlError("Move to top, bottom, up or down.", "bad_request")
    order = sorted(set(indices), reverse=(to == "down"))
    step = -1 if to == "up" else 1
    for i in order:
        j = i + step
        if 0 <= j < len(records) and j not in indices:
            records[i], records[j] = records[j], records[i]


def roster_sort(records, mode) -> None:
    from msr.channel_panel import split_channel_key

    def parts(rec):
        platform, user = split_channel_key(rec["name"])
        return platform.lower(), user.lower()

    if mode == "enabled":
        records.sort(key=lambda r: not r.get("enabled", True))
    elif mode == "platform":
        records.sort(key=parts)
    elif mode == "name":
        records.sort(key=lambda r: parts(r)[1])
    else:
        raise ControlError("Sort by enabled, platform or name.", "bad_request")


def load_roster(path) -> list:
    from msr.util import coerce_channel_records
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            loaded = json.load(fh)
    except FileNotFoundError:
        return []
    except ValueError as e:
        raise ControlError(f"{os.path.basename(path)} is not valid JSON ({e.msg}); fix or restore it first.",
                           "bad_file") from None
    return coerce_channel_records(loaded)[0]


def save_roster(path, records) -> None:
    """The same write as the GUI's save_channels: history copy first, then one atomic replace."""
    from msr import history
    history.snapshot(path, min_interval=45)
    history.atomic_write(path, json.dumps(records, indent=2).encode("utf-8"))


# ------------------------------------------------------------------ settings
def known_setting(config, section, key):
    from msr.config import Config
    defaults = dict(Config.DEFAULT_CONFIG.get(section, {}))
    defaults.update(EXTRA_SETTINGS.get(section, {}))
    if key in defaults:
        return True
    try:
        return config.config.has_option(section, key)
    except Exception:
        return False


def masked(key, value):
    from msr import history
    if history.is_sensitive(key) and key not in ("auto_purge_days",):
        return "<set>" if str(value or "").strip() else ""
    return value


def validate_setting(section, key, value) -> str:
    from msr.config import _coerce_specs
    value = str(value).strip() if value is not None else ""
    for s, k, kind, _default, low, high in _coerce_specs():
        if (s, k) != (section, key):
            continue
        if kind == "bool":
            low_v = value.lower()
            if low_v in ("1", "true", "yes", "on"):
                return "true"
            if low_v in ("0", "false", "no", "off"):
                return "false"
            raise ControlError(f"{section}.{key} must be true or false.", "invalid")
        try:
            number = float(value) if kind == "float" else int(value)
        except ValueError:
            raise ControlError(f"{section}.{key} must be a{'' if kind == 'int' else ' decimal'} number.",
                               "invalid") from None
        if low is not None and number < low or high is not None and number > high:
            raise ControlError(f"{section}.{key} must be between {low} and {high if high is not None else 'any'}.",
                               "invalid")
        return str(number)
    if (section, key) == ("Clipping", "screenshot_format") and value.lower() not in ("jpg", "jpeg", "png", "webp"):
        raise ControlError("screenshot_format must be jpg, png or webp.", "invalid")
    if (section, key) in (("Paths", "streams_dir"), ("Recording", "filename_pattern"),
                          ("Advanced", "ffmpeg_path")) and not value:
        raise ControlError(f"{section}.{key} cannot be empty.", "invalid")
    if "\n" in value or "\r" in value:
        raise ControlError("A setting cannot contain a line break.", "invalid")
    return value


# ------------------------------------------------------------------ app adapters
class AppAdapter:
    """What the Controller needs from the running app. Subclasses: HeadlessApp, OfflineApp, the GUI's."""

    mode = "offline"
    presence = None
    tracker = None   # msr.agent_status.Tracker while a Service runs

    def __init__(self, config):
        self.config = config
        self.started = time.time()
        self.update_state = {}   # last update check: tag, url, zipball

    @property
    def data_dir(self):
        return os.path.dirname(os.path.abspath(self.config.config_file))

    @property
    def channels_file(self):
        path = self.config.get("Paths", "channels_file", fallback="channels.json") or "channels.json"
        return os.path.join(self.data_dir, path) if not os.path.isabs(path) else path

    recorder = None

    def records(self) -> list:
        raise NotImplementedError

    def mutate(self, fn):
        """Run fn(records) as one edit of the roster, save it and show it. Returns fn's result."""
        raise NotImplementedError

    def start_session(self) -> str:
        raise ControlError("Multi-Stream Recorder is not running.", "not_running")

    def stop_session(self) -> str:
        raise ControlError("Multi-Stream Recorder is not running.", "not_running")

    def request_quit(self, after_children=None) -> None:
        raise ControlError("Multi-Stream Recorder is not running.", "not_running")

    def reload_channels(self) -> int:
        return len(self.records())

    def settings_changed(self, section, key) -> None:
        pass

    def log_path(self) -> str:
        for handler in logging.root.handlers:
            name = getattr(handler, "baseFilename", None)
            if name:
                return name
        return os.path.join(self.config.get("Paths", "streams_dir"), "stream_recorder.log")

    def update_info(self) -> dict:
        tag = (self.update_state or {}).get("tag")
        return {"available": bool(tag), "latest": tag} if self.update_state else {}

    def watch_changed(self) -> None:
        """The roster changed: tell a running recorder which (disabled) channels to watch."""


class FileRoster:
    """channels.json as the roster (headless and offline): every edit is load, change, save."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self._records = None

    def records(self):
        with self.lock:
            if self._records is None:
                self._records = load_roster(self.path)
            return [dict(r) for r in self._records]

    def mutate(self, fn):
        with self.lock:
            work = self.records()
            result = fn(work)
            save_roster(self.path, work)
            self._records = work
            return result

    def reload(self):
        with self.lock:
            self._records = None
            return len(self.records())


class OfflineApp(AppAdapter):
    """No copy is running: edits go straight to the files (refused if a copy holds the lock)."""

    mode = "offline"

    def __init__(self, config):
        super().__init__(config)
        self.roster = FileRoster(self.channels_file)

    def records(self):
        return self.roster.records()

    def mutate(self, fn):
        return self.roster.mutate(fn)


class HeadlessApp(AppAdapter):
    """The --headless loop: ``run()`` keeps the main thread; sessions start and stop on command."""

    mode = "headless"
    auto_session = True   # adding/enabling a channel while idle starts recording (no start needed)

    def __init__(self, config, recorder_factory):
        super().__init__(config)
        self.roster = FileRoster(self.channels_file)
        self.recorder_factory = recorder_factory
        self.recorder = None
        self.last_recorder = None
        self._want_session = threading.Event()
        self._quit = threading.Event()
        self._lock = threading.Lock()
        self.after_children = None
        self.paused = False    # set by stop: stays stopped until start
        from msr.presence import PresenceStore
        self.presence = PresenceStore()   # live/offline of disabled channels (start_presence)
        self._poller = None

    def records(self):
        return self.roster.records()

    def mutate(self, fn):
        result = self.roster.mutate(fn)
        self.watch_changed()
        return result

    def reload_channels(self):
        count = self.roster.reload()
        self.watch_changed()
        return count

    def enabled_keys(self):
        return [r["name"] for r in self.records() if r.get("enabled", True)]

    def disabled_keys(self):
        return [r["name"] for r in self.records() if not r.get("enabled", True)]

    def watch_changed(self):
        rec = self.recorder
        if is_running_session(rec) and hasattr(rec, "set_watch_channels"):
            try:
                rec.set_watch_channels(self.disabled_keys())
            except Exception as e:
                logging.debug(f"control: watch channels: {e}")

    def start_presence(self, poller_factory=None):
        """The GUI's slow live/offline poll of DISABLED channels, headless too."""
        from msr.presence import PresencePoller
        factory = poller_factory or PresencePoller
        self._poller = factory(self.presence, self.records)
        self._poller.start()
        return self._poller

    def start_session(self):
        with self._lock:
            if is_running_session(self.recorder) or self._want_session.is_set():
                raise ControlError("Recording is already running.", "conflict")
            if not self.enabled_keys():
                raise ControlError("Enable at least one channel first.", "conflict")
            self.paused = False
            self._want_session.set()
        return "starting"

    def stop_session(self):
        with self._lock:
            self._want_session.clear()
            self.paused = True
            rec = self.recorder
        if not is_running_session(rec):
            raise ControlError("Recording is not running.", "conflict")
        rec.stop()  # run() on the main thread returns; the cleanup after a stop goes on here
        threading.Thread(target=self._finish_quietly, args=(rec,), daemon=True,
                         name="control-session-cleanup").start()
        return "stopped"

    def request_quit(self, after_children=None):
        self.after_children = after_children
        self._want_session.clear()
        self._quit.set()
        if self._poller is not None:
            self._poller.stop()
        rec = self.recorder
        if is_running_session(rec):
            rec.should_stop.set()  # run() returns; the main thread does the full stop + cleanup

    def run(self, start_now=True, idle=0.5):
        """Main-thread loop. Returns the last recorder (or None) for the final cleanup."""
        if start_now and self.enabled_keys():
            self._want_session.set()
        while not self._quit.is_set():
            if self._want_session.is_set():
                rec = self.recorder_factory(self.enabled_keys())
                with self._lock:
                    self.recorder = self.last_recorder = rec
                try:
                    rec.run()  # returns after a stop (control) or raises KeyboardInterrupt (Ctrl+C)
                finally:
                    with self._lock:
                        self._want_session.clear()
                        if self._quit.is_set() or not is_running_session(rec):
                            self.recorder = None
                continue
            self._quit.wait(idle)
        return self.last_recorder

    @staticmethod
    def _finish_quietly(rec):
        try:
            rec.wait_for_cleanup()
            rec.manager.shutdown()
        except Exception as e:
            logging.debug(f"control: session cleanup: {e}")


# ------------------------------------------------------------------ status
def channel_rows(app) -> list:
    rec = app.recorder
    statuses = _status_copy(rec)
    stopped = set(getattr(rec, "stopped_channels", ()) or ()) if rec is not None else set()
    runtime = {}
    try:
        runtime = dict(rec.runtime) if rec is not None else {}
    except Exception:
        pass
    presence = {}
    try:
        store = app.presence or getattr(rec, "presence", None)
        presence = store.snapshot() if store is not None else {}
    except Exception:
        pass
    rows = []
    seen = set()
    for i, r in enumerate(app.records(), 1):
        key = r["name"]
        seen.add(key)
        rows.append(_row(i, key, r.get("enabled", True), statuses.get(key), key in stopped,
                         runtime.get(f"kick_push:{key}"), presence.get(key)))
    for key, st in statuses.items():  # removed from the roster but still finishing
        if key not in seen:
            rows.append(_row(None, key, None, st, key in stopped, runtime.get(f"kick_push:{key}"), None))
    from msr.agent_status import iso
    from msr.util import channel_watch_url
    tracker = app.tracker
    for row in rows:
        key = row.pop("_key")
        try:
            # no query string: a custom URL can carry a token (same rule as the shown name)
            row["url"] = (channel_watch_url(key) or "").split("?", 1)[0].split("#", 1)[0]
        except Exception:
            row["url"] = ""
        info = dict(tracker.channels.get(key) or {}) if tracker is not None else {}
        row["last_error"] = info.get("last_error", "")
        row["last_error_at"] = iso(info.get("last_error_at"))
        row["errors"] = info.get("errors", 0)
        row["restarts"] = info.get("restarts", 0)
        row["last_live"] = iso(info.get("last_live"))
        row["last_recorded"] = iso(info.get("last_recorded"))
        state, done = tracker.finish_state(key) if tracker is not None else (None, False)
        row["finishing"] = state            # waiting_for_cleanup | remuxing | None
        row["finished_since_stop"] = done
    return rows


def _row(index, key, enabled_flag, st, stopped, push, presence):
    st = st or {}
    status = str(st.get("status", "") or ("Off" if enabled_flag is False else ""))
    low = status.lower()
    recording = low.startswith("recording")
    live = recording
    if presence is not None:
        try:
            live = live or bool(getattr(presence, "live", None) if not isinstance(presence, dict)
                                else presence.get("live"))
        except Exception:
            pass
    current = st.get("raw_file") or st.get("file") or ""
    return {
        "_key": key,
        "index": index,
        "name": display_name(key),
        "platform": _platform(key),
        "enabled": enabled_flag,
        "status": status,
        "detail": redact_for_log(str(st.get("detail", "") or "")),
        "live": live,
        "recording": recording,
        "stopped": stopped,
        "file": os.path.basename(current) if current else "",
        "size": st.get("size", ""),
        "duration": st.get("time", ""),
        "error": redact_for_log(str(st.get("detail", ""))) if "error" in low or "failed" in low else "",
        "kick_push": push,
    }


def live_status(app) -> dict:
    rec = app.recorder
    running = is_running_session(rec)
    storage_state = getattr(rec, "_storage_state", "ok") if running else None
    notice = ""
    try:
        notice = rec.storage_notice() if running else ""
    except Exception:
        pass
    cleaner = getattr(getattr(rec, "cleaner", None), "_thread", None)
    startup = getattr(rec, "_startup_cleanup_thread", None)
    rows = channel_rows(app)
    out = {
        "protocol": PROTOCOL,
        "api_version": API_VERSION,
        "app": {"name": "Multi-Stream Recorder", "version": __version__, "pid": os.getpid(),
                "mode": app.mode, "started_at": round(app.started, 1)},
        "updated_at": round(time.time(), 1),
        "session": {
            "running": running,
            "channels_watched": len(getattr(rec, "channels", []) or []) if running else 0,
            "recording": sum(1 for r in rows if r["recording"]),
            "storage": {"state": storage_state, "notice": notice},
            "jobs": {"cleanup_running": bool(cleaner is not None and cleaner.is_alive()),
                     "startup_cleanup_running": bool(startup is not None and startup.is_alive())},
        },
        "channels": rows,
        "update": app.update_info(),
    }
    tracker = app.tracker
    if tracker is not None:
        out["finished"] = tracker.recent_finished()
        out["cleanup_queue"] = tracker.cached("queue")
        out["health"] = tracker.cached("health")
    return out


class StatusWriter(threading.Thread):
    """Rewrites msr_status.json every few seconds (and at once on ``poke()``)."""

    def __init__(self, app, interval=STATUS_INTERVAL):
        super().__init__(daemon=True, name="control-status")
        self.app = app
        self.path = os.path.join(app.data_dir, STATUS_FILE)
        self.interval = max(0.5, float(interval))
        self._halt = threading.Event()
        self._poke = threading.Event()
        self._last_error = 0.0

    def poke(self):
        self._poke.set()

    def feed(self):
        """Keep the tracker current: status hook, a diff for direct status writes, presence, caches."""
        tracker = self.app.tracker
        if tracker is None:
            return
        rec = self.app.recorder
        if rec is not None:
            try:
                rec.on_status = tracker.seen
                cleaner = getattr(rec, "cleaner", None)
                if cleaner is not None:
                    cleaner.on_file = tracker.on_cleanup
            except Exception:
                pass
        statuses = _status_copy(rec)
        for key, st in statuses.items():
            tracker.seen(key, st)
        if rec is not None and statuses:
            tracker.prune(set(statuses))
        store = self.app.presence or getattr(rec, "presence", None)
        if store is not None:
            try:
                tracker.seen_presence(store.snapshot())
            except Exception:
                pass
        tracker.refresh(self.app)

    def write_once(self, running=True):
        data = live_status(self.app)
        data["app"]["running"] = running
        _write_private(self.path, json.dumps(data, indent=2).encode("utf-8"))

    def run(self):
        while not self._halt.is_set():
            try:
                rec = self.app.recorder
                if self.app.mode == "headless" and is_running_session(rec):
                    rec.update_status_from_queue()
                self.feed()
                self.write_once()
            except Exception as e:
                if time.time() - self._last_error > 300:
                    self._last_error = time.time()
                    logging.warning(f"Control: could not write {STATUS_FILE}: {e}")
            self._poke.wait(self.interval)
            self._poke.clear()

    def stop(self):
        self._halt.set()
        self._poke.set()
        if self.is_alive() and self is not threading.current_thread():
            self.join(5)  # no write may land after the final one (or after the folder is gone)
        try:
            self.write_once(running=False)
        except Exception:
            pass


# ------------------------------------------------------------------ the commands
def _plan_dict(plan, limit=200) -> dict:
    from msr import library_actions as act
    first, last = plan.span
    out = {
        "action": plan.action, "label": act.ACTION_LABELS.get(plan.action, plan.action),
        "files": plan.files, "folders": plan.folders, "bytes": plan.bytes, "channels": plan.channels,
        "first": first, "last": last, "summary": plan.summary_lines(),
        "items": [{"path": i.path, "size": i.size, "kind": i.kind, "channel": i.label, "role": i.role}
                  for i in plan.items[:limit]],
        "items_truncated": max(0, len(plan.items) - limit),
        "skipped": [{"path": p, "reason": r} for p, r in plan.skipped[:limit]],
        "skipped_count": len(plan.skipped),
        "blocked": plan.blocked,
    }
    if plan.action == act.ACTION_PERMANENT:
        out["confirm_word"] = plan.confirm_word
    return out


def _rec_dict(r) -> dict:
    return {"path": r.path, "size": r.size, "mtime": r.mtime, "category": r.category, "platform": r.platform,
            "channel": r.channel, "kind": r.kind, "when": r.when}


class Controller:
    """Runs one command. ``handle`` never raises: it returns {"ok": ...} like the wire format."""

    def __init__(self, app):
        self.app = app
        self._library_lock = threading.Lock()
        self._media_jobs = set()
        self._retry = None
        self.on_change = None  # called after anything that changes state (StatusWriter.poke)

    # -------------------------------------------------- plumbing
    def commands(self):
        return sorted(n[4:] for n in dir(self) if n.startswith("cmd_"))

    def handle(self, cmd, args=None):
        args = args or {}
        if not isinstance(args, dict):
            return {"ok": False, "code": "bad_request", "error": "args must be an object"}
        fn = getattr(self, "cmd_" + str(cmd or "").replace("-", "_"), None)
        if fn is None:
            return {"ok": False, "code": "unknown_command", "error": f"Unknown command '{cmd}'.",
                    "commands": self.commands()}
        try:
            result = fn(args)
        except ControlError as e:
            return {"ok": False, "code": e.code, "error": str(e)}
        except Exception as e:
            from msr.util import redact_for_log
            logging.exception(f"Control command {cmd} failed")
            return {"ok": False, "code": "internal", "error": redact_for_log(f"{type(e).__name__}: {e}")}
        if self.on_change is not None and cmd not in ("status", "channels", "ping", "help", "logs", "config_get"):
            try:
                self.on_change()
            except Exception:
                pass
        return {"ok": True, "result": result}

    def _recorder(self, need=True):
        rec = self.app.recorder
        if need and not is_running_session(rec):
            raise ControlError("Recording is not running (start it first).", "not_recording")
        return rec

    def _need_live(self):
        if self.app.mode == "offline":
            raise ControlError("Multi-Stream Recorder is not running.", "not_running")

    def _confirm(self, args, what):
        if not _truthy(args.get("confirm")):
            raise ControlError(f"{what} Repeat with confirm=true (CLI: --yes).", "confirm_required")

    # -------------------------------------------------- info
    def cmd_ping(self, args):
        return {"pong": True, "version": __version__, "api_version": API_VERSION, "mode": self.app.mode,
                "pid": os.getpid()}

    def cmd_help(self, args):
        return {"api_version": API_VERSION, "commands": self.commands()}

    def cmd_status(self, args):
        return live_status(self.app)

    def cmd_channels(self, args):
        return {"channels": channel_rows(self.app)}

    # -------------------------------------------------- roster
    def cmd_channel_add(self, args):
        names = args.get("names") or [args.get("name")]
        enable = not _truthy(args.get("disabled"))
        # all or nothing: one bad name adds none of them
        added = self.app.mutate(lambda recs: [roster_add(recs, n, args.get("platform"), enable=enable)
                                              for n in names])
        out = {"added": [display_name(k) for k in added]}
        out.update(self._monitor(added if enable else []))
        return out

    def _monitor(self, keys):
        """Start watching/recording newly enabled channels at once, like the GUI's Add.

        A running session gets them mid-session. Headless with no session (for example it
        started with no channel enabled) starts one, unless the user stopped recording."""
        if self.app.mode == "offline":
            return {"monitoring": False, "saved": "files (MSR not running)"}
        if self.app.tracker is not None:
            for key in keys:
                self.app.tracker.started(key)  # a new recording: waits no longer start at the old stop
        rec = self.app.recorder
        if not keys:
            return {"monitoring": is_running_session(rec)}
        if is_running_session(rec):
            for key in keys:
                rec.start_channel(key)
            return {"monitoring": True, "session": "running"}
        if getattr(self.app, "auto_session", False) and not getattr(self.app, "paused", False):
            try:
                return {"monitoring": True, "session": self.app.start_session()}
            except ControlError as e:
                if e.code != "conflict":
                    raise
            # a session is just starting: it may have read the roster before this edit
            end = time.time() + 15
            while time.time() < end and not is_running_session(self.app.recorder):
                time.sleep(0.05)
            rec = self.app.recorder
            if is_running_session(rec):
                for key in keys:
                    if key not in getattr(rec, "channels", []) and key not in getattr(rec, "processes", {}):
                        rec.start_channel(key)
            return {"monitoring": True, "session": "running" if is_running_session(rec) else "starting"}
        return {"monitoring": False,
                "hint": "Recording is stopped; run start to watch and record the enabled channels."}


    def cmd_channel_remove(self, args):
        refs = _refs(args)

        def pick(recs):
            return [recs[resolve_channel(recs, r)]["name"] for r in refs]

        keys = pick(self.app.records())
        rec = self.app.recorder
        before = _status_copy(rec)
        tracker = self.app.tracker
        for key in keys:
            if rec is not None:
                recording = str((before.get(key) or {}).get("status", "")).lower().startswith("recording")
                rec.remove_channel(key)
                if tracker is not None and key in before:
                    tracker.stopping(key, recording)
        drop = set(keys)
        self.app.mutate(lambda recs: recs.__setitem__(slice(None), [r for r in recs if r["name"] not in drop]))
        return {"removed": [display_name(k) for k in keys], "recordings_kept": True}

    def _set_enabled(self, args, value):
        if _truthy(args.get("all")):
            changed = self.app.mutate(lambda recs: roster_set_enabled(recs, list(range(len(recs))), value))
        else:
            refs = _refs(args)
            changed = self.app.mutate(lambda recs: roster_set_enabled(
                recs, [resolve_channel(recs, r) for r in refs], value))
        out = {"changed": [display_name(k) for k in changed], "enabled": value}
        if value:
            out.update(self._monitor(changed))
        else:
            out["note"] = ("A channel that is recording keeps recording until it ends; "
                           "use record_stop to stop it now.")
            if self.app.mode == "offline":
                out["saved"] = "files (MSR not running)"
        return out

    def cmd_channel_enable(self, args):
        return self._set_enabled(args, True)

    def cmd_channel_disable(self, args):
        return self._set_enabled(args, False)

    def cmd_channel_move(self, args):
        refs = _refs(args)
        to = str(args.get("to", "")).lower()
        self.app.mutate(lambda recs: roster_move(recs, [resolve_channel(recs, r) for r in refs], to))
        return {"order": [display_name(r["name"]) for r in self.app.records()]}

    def cmd_channel_sort(self, args):
        mode = str(args.get("by", args.get("mode", ""))).lower()
        self.app.mutate(lambda recs: roster_sort(recs, mode))
        return {"order": [display_name(r["name"]) for r in self.app.records()]}

    def cmd_channel_edit(self, args):
        """Rename a channel (new name and/or platform). Its recordings folder name follows the
        key, so a channel being recorded must be stopped first."""
        ref = args.get("channel")
        recs = self.app.records()
        old = recs[resolve_channel(recs, ref)]["name"]
        rec = self.app.recorder
        if is_running_session(rec) and old in getattr(rec, "processes", {}) and old not in rec.stopped_channels:
            raise ControlError("Stop this channel first (record_stop), then edit it.", "conflict")

        from msr.channel_panel import split_channel_key
        old_platform, old_user = split_channel_key(old)

        def edit(work):
            i = resolve_channel(work, old)
            entry = work.pop(i)
            try:
                key = roster_add(work, args.get("new_name") or old_user, args.get("platform") or old_platform,
                                 enable=entry.get("enabled", True) if "enabled" not in args
                                 else _truthy(args["enabled"]))
            except ControlError:
                work.insert(i, entry)
                raise
            work.insert(i, work.pop())
            return key

        key = self.app.mutate(edit)
        if rec is not None and old != key:
            rec.remove_channel(old)
        return {"old": display_name(old), "new": display_name(key)}

    def cmd_channels_reload(self, args):
        return {"channels": self.app.reload_channels()}

    # -------------------------------------------------- session and recording
    def cmd_start(self, args):
        self._need_live()
        return {"session": self.app.start_session()}

    def cmd_stop(self, args):
        self._need_live()
        before = _status_copy(self.app.recorder)
        out = {"session": self.app.stop_session()}
        tracker = self.app.tracker
        if tracker is not None:
            rec_keys = [k for k, st in before.items()
                        if str((st or {}).get("status", "")).lower().startswith("recording")]
            seqs = [tracker.stopping(k, True) for k in rec_keys]
            out["finishing"] = [display_name(k) for k in rec_keys]
            if seqs:
                out["finish_after_seq"] = min(seqs) - 1
        return out

    def cmd_record_start(self, args):
        if not getattr(self.app, "auto_session", False):
            rec = self._recorder()
        refs = _refs(args)
        recs = self.app.records()
        keys = [recs[resolve_channel(recs, r)]["name"] for r in refs]

        def enable(work):
            return roster_set_enabled(work, [resolve_channel(work, k) for k in keys], True)

        self.app.mutate(enable)  # the GUI does the same: starting a channel ticks it
        rec = self.app.recorder
        if not is_running_session(rec):  # headless and idle or stopped: an explicit start resumes
            self.app.paused = False
            out = self._monitor(keys)
            return dict(out, started=[display_name(k) for k in keys])
        for key in keys:
            if self.app.tracker is not None:
                self.app.tracker.started(key)
            rec.start_channel(key)
        return {"started": [display_name(k) for k in keys]}

    def cmd_record_stop(self, args):
        rec = self._recorder()
        recs = self.app.records()
        keys = [recs[resolve_channel(recs, r)]["name"] for r in _refs(args)]
        before = _status_copy(rec)
        results = []
        for key in keys:
            status = str((before.get(key) or {}).get("status", "") or "")
            recording = status.lower().startswith("recording")
            rec.stop_channel(key)
            row = {"name": display_name(key), "result": "stopped_recording" if recording else "was_idle",
                   "status_before": status}
            if self.app.tracker is not None:
                row["finish_after_seq"] = self.app.tracker.stopping(key, recording) - 1
            results.append(row)
        out = {"stopped": [display_name(k) for k in keys], "channels": results}
        if any(r["result"] == "was_idle" for r in results):
            out["note"] = ("A channel that was not recording has nothing to finish: no 'finished' event "
                           "will come for it.")
        return out

    def cmd_check(self, args):
        rec = self._recorder()
        if args.get("channels") or args.get("channel"):
            recs = self.app.records()
            keys = [recs[resolve_channel(recs, r)]["name"] for r in _refs(args)]
            for key in keys:
                rec.check_now(key)
            return {"checking": [display_name(k) for k in keys]}
        rec.check_all_now()
        return {"checking": "all"}

    def cmd_clip(self, args):
        if any(args.get(k) not in (None, "") for k in ("from", "to", "file")):
            return self._cut(args)
        return self._media(args, "clip")

    def _cut(self, args):
        """Clip Editor parity: cut [from, to] of a channel's recording in progress, or of any
        recording file (file=, also without a running copy). Times: seconds, M:SS or H:MM:SS
        from the file's start; a leading '-' counts back from the end; to may be 'end'."""
        from msr import clip_core as core
        config = self.app.config
        src = args.get("file")
        if src:
            src = os.path.abspath(os.path.expanduser(str(src)))
            if not os.path.isfile(src):
                raise ControlError(f"No such file: {src}", "not_found")
            if not src.lower().endswith(core.MEDIA_EXTS):
                raise ControlError("That is not a recording (.ts / .mp4 / .mkv ...).", "invalid")
        else:
            if not args.get("channel"):
                raise ControlError("Give a channel or file.", "bad_request")
            from msr.worker import find_active_recording_file
            rec = self._recorder()
            recs = self.app.records()
            key = recs[resolve_channel(recs, args.get("channel"))]["name"]
            src = find_active_recording_file(rec, key)
            if not src:
                raise ControlError(f"{display_name(key)} has no recording in progress; pass file= for a "
                                   "finished one.", "not_recording")
        if args.get("from") in (None, ""):
            raise ControlError("Give from= (and to=, default the end).", "bad_request")
        ffmpeg = config.get("Advanced", "ffmpeg_path", fallback="ffmpeg") or "ffmpeg"
        ffprobe = core.ffprobe_for(ffmpeg)
        length = core.media_length(src, ffprobe)
        if not length:
            raise ControlError("Could not read the recording's length (is ffprobe installed?).", "failed")
        try:
            start = core.parse_time(args["from"], length)
            end = core.parse_time(args.get("to") or "end", length)
        except core.ClipError as e:
            raise ControlError(str(e), "invalid") from None
        if start >= length:
            raise ControlError(f"from is past the end ({core.fmt_time(length)}).", "invalid")
        base = (config.get("Clipping", "clips_dir", fallback="") or "").strip() or \
            os.path.join(config.get("Paths", "streams_dir"), "Clips")
        job = (os.path.normcase(src), "cut")
        if job in self._media_jobs:
            raise ControlError("A clip of this file is already being made.", "busy")
        self._media_jobs.add(job)
        try:
            res = core.make_clip(ffmpeg, src, start, end, core.clips_folder_for(src, base),
                                 exact=_truthy(args.get("exact")), length=length, ffprobe=ffprobe)
        except core.ClipError as e:
            raise ControlError(str(e), "failed") from None
        finally:
            self._media_jobs.discard(job)
        logging.info(f"Clip saved (control): {res.path} ({res.duration:.1f} s)")
        out = res.as_dict()
        out.update(source=src, length=round(length, 3))
        return out

    def cmd_screenshot(self, args):
        return self._media(args, "screenshot")

    def _media(self, args, kind):
        import datetime
        from msr.util import channel_file_stem, channel_key_to_dirs
        from msr.worker import (channel_status_allows_clip, create_clip, create_screenshot,
                                find_active_recording_file, screenshot_extension)
        rec = self._recorder()
        config = self.app.config
        recs = self.app.records()
        key = recs[resolve_channel(recs, args.get("channel"))]["name"]
        st = _status_copy(rec).get(key, {})
        if not channel_status_allows_clip(st.get("status", "")):
            raise ControlError(f"{display_name(key)} is not recording right now.", "not_recording")
        job = (key, kind)
        if job in self._media_jobs:
            raise ControlError("One is already being made for this channel.", "busy")
        self._media_jobs.add(job)
        try:
            raw = find_active_recording_file(rec, key)
            if not raw:
                raise ControlError("No active recording file found for this channel.", "not_found")
            platform, username_dir = channel_key_to_dirs(key)
            base = (config.get("Clipping", "clips_dir", fallback="") or "").strip() or \
                os.path.join(config.get("Paths", "streams_dir"), "Clips")
            out_dir = os.path.join(base, platform, username_dir)
            stem = channel_file_stem(username_dir)
            stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            ffmpeg = config.get("Advanced", "ffmpeg_path", fallback="ffmpeg") or "ffmpeg"
            if kind == "clip":
                seconds = int(args.get("seconds") or config.getint("Clipping", "clip_length_seconds", fallback=30))
                if not 1 <= seconds <= 1800:
                    raise ControlError("seconds must be 1-1800.", "invalid")
                out = os.path.join(out_dir, f"{stem}_{stamp}_{seconds}s.mp4")
                ok, msg = create_clip(raw, out, seconds, ffmpeg, logging)
            else:
                fmt = config.get("Clipping", "screenshot_format", fallback="jpg")
                quality = config.getint("Clipping", "screenshot_quality", fallback=2)
                out = os.path.join(out_dir, f"{stem}_{stamp}{screenshot_extension(fmt)}")
                ok, msg = create_screenshot(raw, out, ffmpeg, logging, fmt=fmt, quality=quality)
        finally:
            self._media_jobs.discard(job)
        if not ok:
            raise ControlError(f"{kind.capitalize()} failed: {msg}", "failed")
        logging.info(f"{kind.capitalize()} saved for {display_name(key)} (control): {out}")
        return {"file": out}

    # -------------------------------------------------- dependencies
    def cmd_deps(self, args):
        """action=list (installed, no network) | check (also latest) | update (names=[...] or all=true).
        pip updates apply to the next recording; running recordings keep their version."""
        from msr import depsupdate
        action = str(args.get("action") or "list")
        ffmpeg = self.app.config.get("Advanced", "ffmpeg_path", fallback="ffmpeg") or "ffmpeg"
        if action == "list":
            return {"items": depsupdate.rows(False, ffmpeg_path=ffmpeg)}
        if action == "check":
            rows = depsupdate.rows(True, ffmpeg_path=ffmpeg)
            try:
                state = self.app.config.get("GUI", "window_state_file", fallback="window_state.json")
                depsupdate.remember(os.path.join(self.app.data_dir, state), rows)
            except Exception:
                pass
            return {"items": rows, "updates": depsupdate.updates_available(rows)}
        if action != "update":
            raise ControlError("action must be list, check or update.", "bad_request")
        names = [str(n).lower() for n in (args.get("names") or [])]
        if _truthy(args.get("all")) == bool(names):
            raise ControlError("Give names or all=true (not both).", "bad_request")
        if names:
            bad = [n for n in names if n not in depsupdate.NAMES]
            if bad:
                raise ControlError(f"Unknown: {', '.join(bad)}. Known: {', '.join(depsupdate.NAMES)}.", "not_found")
        else:
            rows = depsupdate.rows(True, ffmpeg_path=ffmpeg)
            names = [r["name"] for r in rows if r.get("updatable") and r["name"] != "msr"]
        if not depsupdate.in_venv() and not _truthy(args.get("system")):
            raise ControlError(depsupdate.SYSTEM_REFUSED, "not_venv")
        results = depsupdate.update_packages(names, allow_system=_truthy(args.get("system"))) if names else []
        for r in results:
            logging.info(f"Dependency update (control) {r['name']}: "
                         + (f"{r.get('old')} -> {r.get('new')}" if r.get("ok") else str(r.get("error"))))
        return {"results": results, "ok_all": all(r.get("ok") for r in results),
                "note": "New versions are used by recordings started from now on."}

    # -------------------------------------------------- settings
    def cmd_config_get(self, args):
        cfg = self.app.config.config
        section, key = args.get("section"), args.get("key")
        out = {}
        for s in cfg.sections():
            if section and s.lower() != str(section).lower():
                continue
            values = {k: masked(k, v) for k, v in cfg.items(s) if not key or k == key}
            if values:
                out[s] = values
        if (section or key) and not out:
            raise ControlError("No such setting.", "not_found")
        return {"settings": out, "file": os.path.basename(self.app.config.config_file)}

    def cmd_config_set(self, args):
        section, key = str(args.get("section") or ""), str(args.get("key") or "").lower()
        if not section or not key or "value" not in args:
            raise ControlError("Give section, key and value.", "bad_request")
        config = self.app.config
        if not known_setting(config, section, key):
            raise ControlError(f"Unknown setting {section}.{key}.", "not_found")
        value = validate_setting(section, key, args["value"])
        if not config.config.has_section(section):
            config.config.add_section(section)
        config.config.set(section, key, value)
        if not config.save():
            raise ControlError("config.ini could not be written (see the log).", "write_failed")
        applies = "next_start"
        rec = self.app.recorder
        if (section, key) in LIVE_SETTINGS:
            applies = "now"
            if is_running_session(rec):
                rec.set_poll_interval(float(value))
        elif (section, key) in NOW_SETTINGS:
            applies = "now"
        elif section == "Control":
            applies = "restart"
        self.app.settings_changed(section, key)
        logging.info(f"Setting {section}.{key} changed by control")
        return {"section": section, "key": key, "value": masked(key, value), "applies": applies}

    def cmd_config_reload(self, args):
        self.app.config.reload()
        return {"reloaded": True, "note": "Running recordings keep their settings until they restart."}

    # -------------------------------------------------- logs
    def cmd_logs(self, args):
        from msr.util import redact_for_log
        n = max(1, min(int(args.get("lines", 100)), 5000))
        needle = str(args.get("filter", "") or "").lower()
        path = self.app.log_path()
        lines = deque(maxlen=n)
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if not needle or needle in line.lower():
                        lines.append(redact_for_log(line.rstrip("\n")))
        except FileNotFoundError:
            pass
        return {"file": os.path.basename(path), "lines": list(lines)}

    # -------------------------------------------------- updates
    def cmd_update_check(self, args):
        from msr.util import check_for_updates
        found = {}
        done = threading.Event()

        def got(tag, url, zipball=None):
            found.update(tag=tag, url=url, zipball=zipball)
            done.set()

        check_for_updates(__version__, callback=got)
        done.wait(float(args.get("timeout", 15)))
        self.app.update_state = dict(found)
        if self.app.tracker is not None:
            self.app.tracker.forget("health")
        return {"current": __version__, "latest": found.get("tag"), "available": bool(found),
                "release_url": found.get("url")}

    def cmd_update_install(self, args):
        self._need_live()
        if not self.app.update_state:
            self.cmd_update_check({})
        if not self.app.update_state:
            raise ControlError("No newer version found.", "not_found")
        self._confirm(args, f"This downloads v{self.app.update_state['tag']}, stops all recordings and restarts.")
        from msr.update import prepare_update, spawn_updater, zipball_url_for_tag, zipball_url_is_allowed
        from msr.util import application_dir
        target = application_dir()
        if not os.access(target, os.W_OK):
            raise ControlError("The program folder is not writable.", "write_failed")
        url = self.app.update_state.get("zipball") or ""
        if not zipball_url_is_allowed(url):
            url = zipball_url_for_tag(self.app.update_state["tag"])
        staging = prepare_update(url)
        rec = self.app.recorder
        if is_running_session(rec):
            rec.stop()
        relaunch = list(getattr(self.app, "relaunch_args", None) or [])
        self.app.request_quit(after_children=lambda: spawn_updater(staging, target_dir=target,
                                                                   launcher_args=relaunch))
        return {"installing": self.app.update_state["tag"], "relaunch": [self.app.mode] + relaunch}

    # -------------------------------------------------- health, recordings, events
    def cmd_health(self, args):
        from msr.agent_status import health
        out = health(self.app.config, update=self.app.update_info())
        if self.app.mode == "offline":
            out["note"] = "MSR is not running; this is the state of the files and drives."
        return out

    def _channel_key(self, ref):
        recs = self.app.records()
        try:
            return recs[resolve_channel(recs, ref)]["name"]
        except ControlError:
            rec = self.app.recorder
            if ref in _status_copy(rec):
                return ref
            raise

    def cmd_recordings(self, args):
        """Finished recordings in Processed/ (newest first), optionally since a time / for one channel."""
        from msr.agent_status import iso, parse_since
        from msr.library import sidecar_seconds
        from msr.util import channel_key_to_dirs
        try:
            since = parse_since(args.get("since"))
        except ValueError as e:
            raise ControlError(str(e), "bad_request") from None
        stats = self._scan(files=True)
        recs = [r for r in stats.files if r.kind == "recording" and r.category == "processed"
                and r.mtime >= since]
        if args.get("channel"):
            platform, user = channel_key_to_dirs(self._channel_key(args["channel"]))
            leaf = user.split("/")[-1].lower()
            recs = [r for r in recs if r.platform.lower() == platform.lower() and r.channel.lower() == leaf]
        recs.sort(key=lambda r: -r.mtime)
        limit = max(1, min(int(args.get("limit", 50)), 5000))
        out = [{"path": r.path, "channel": f"{r.platform}:{r.channel}", "size": r.size,
                "duration_seconds": sidecar_seconds(r.path), "finished_at": iso(r.mtime), "remux_ok": True}
               for r in recs[:limit]]
        tracker = self.app.tracker
        return {"recordings": out, "count": len(recs), "truncated": max(0, len(recs) - limit),
                "this_run": tracker.recent_finished(50) if tracker is not None else []}

    def cmd_recordings_pending(self, args):
        """Raw .ts waiting for the cleanup remux, and what sits in PendingDeletion."""
        from msr.agent_status import busy_channels, cleanup_queue, iso
        raw = cleanup_queue(self.app.config, busy_channels(self.app.recorder))
        stats = self._scan(files=True)
        pending = sorted((r for r in stats.files if r.category == "pending"), key=lambda r: r.path)
        limit = max(1, min(int(args.get("limit", 500)), 20000))
        return {"raw_waiting": raw,
                "pending_deletion": [{"path": r.path, "size": r.size, "moved_at": iso(r.mtime), "kind": r.kind}
                                     for r in pending[:limit]],
                "pending_deletion_count": len(pending),
                "pending_deletion_bytes": sum(r.size for r in pending)}

    def cmd_recordings_retry(self, args):
        """Run the leftover-file cleanup (remux raw .ts in Recorded/) now. Files being recorded are skipped."""
        from msr.recorder import BackgroundCleaner
        rec = self.app.recorder
        running = (self._retry is not None and self._retry.is_alive())
        for t in (getattr(getattr(rec, "cleaner", None), "_thread", None),
                  getattr(rec, "_startup_cleanup_thread", None)):
            running = running or bool(t is not None and t.is_alive())
        if running:
            raise ControlError("A cleanup pass is already running.", "busy")
        status = getattr(rec, "status_dict", None) if rec is not None else None
        cleaner = BackgroundCleaner(self.app.config, status_dict=status if status is not None else {})
        if self.app.tracker is not None:
            cleaner.on_file = self.app.tracker.on_cleanup

        def work():
            for _ in range(3):
                if not cleaner._process_leftover_files():
                    break
                time.sleep(10)
            logging.info("Cleanup (requested by control) finished")
            if self.app.tracker is not None:
                self.app.tracker.forget("queue")

        if self.app.mode == "offline":
            work()
            return {"done": True}
        self._retry = threading.Thread(target=work, daemon=True, name="control-cleanup-retry")
        self._retry.start()
        return {"started": True, "note": "Runs in the background; see recordings_pending / logs."}

    def cmd_events(self, args):
        tracker = self.app.tracker
        if tracker is None:
            raise ControlError("Multi-Stream Recorder is not running.", "not_running")
        return {"events": tracker.events(int(args.get("after", 0) or 0)), "seq": tracker.seq}

    def cmd_wait(self, args):
        """Block until a live / stopped / remux_started / finished / error / offline / ended event (for one channel),
        or time out. A "finished" wait for a channel that was stopped starts at that stop, so a
        finish that happened between the stop and the wait is returned at once."""
        from msr.agent_status import EVENT_KINDS
        tracker = self.app.tracker
        if tracker is None:
            raise ControlError("Multi-Stream Recorder is not running.", "not_running")
        kinds = args.get("events") or args.get("event") or list(EVENT_KINDS)
        if isinstance(kinds, str):
            kinds = [k.strip() for k in kinds.split(",") if k.strip()]
        bad = [k for k in kinds if k not in EVENT_KINDS]
        if bad:
            raise ControlError(f"event must be one of {', '.join(EVENT_KINDS)}.", "bad_request")
        key = self._channel_key(args["channel"]) if args.get("channel") else None
        timeout = max(0.0, min(float(args.get("timeout", 300)), 3600.0))
        after = args.get("after")
        if after in (None, "") and key is not None and "live" in kinds:
            st = str((_status_copy(self.app.recorder).get(key) or {}).get("status", ""))
            if st.lower().startswith("recording"):  # already live and recording: nothing to wait for
                return {"seq": tracker.seq, "event": "live", "channel": key, "already": True,
                        "source": "recording", "time": time.time(),
                        "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
        if after in (None, "") and key is not None and "finished" in kinds:
            stop = tracker.default_after(key)  # stopped and not recording again: its finish counts
            if stop is not None:
                after = stop - 1
        ev = tracker.wait(set(kinds), key, timeout, None if after in (None, "") else int(after))
        if ev is None:
            raise ControlError(f"No {'/'.join(kinds)} event within {timeout:g}s.", "timeout")
        return ev

    # -------------------------------------------------- quit
    def cmd_quit(self, args):
        self._need_live()
        self.app.request_quit()
        return {"quitting": True}

    # -------------------------------------------------- library
    def _guard(self):
        from msr import library as lib
        from msr import library_actions as act
        rec = self.app.recorder
        return act.Guard(lib.roots_from_config(self.app.config), data_dir=self.app.data_dir,
                         busy=lambda: act.busy_from_status(_status_copy(rec)))

    def _scan(self, files=False):
        from msr import library as lib
        roster = [r["name"] for r in self.app.records()]
        stats = lib.scan_library(lib.roots_from_config(self.app.config), collect_files=files,
                                 collect_videos=False)
        lib.mark_roster(stats, roster)
        return stats

    def _pick(self, args, stats):
        from msr import library_actions as act
        recs = list(stats.files)
        if args.get("category"):
            recs = [r for r in recs if r.category == args["category"]]
        if args.get("platform"):
            recs = [r for r in recs if r.platform == str(args["platform"]).lower()]
        if args.get("channel"):
            want = str(args["channel"]).replace("\\", "/").lower()
            recs = [r for r in recs if r.channel.lower() == want]
        if args.get("paths"):
            wanted = {os.path.normcase(os.path.abspath(p)) for p in args["paths"]}
            recs = [r for r in recs if os.path.normcase(os.path.abspath(r.path)) in wanted]
        day = act.parse_day
        return act.filter_recs(
            recs, kinds=args.get("kinds"), text=args.get("text", ""), month=args.get("month"),
            older_than_days=args.get("older_than_days"),
            date_from=day(args["from"]) if args.get("from") else None,
            date_to=day(args["to"]) if args.get("to") else None,
            keep_newest_files=args.get("keep_newest_files"), keep_newest_bytes=args.get("keep_newest_bytes"))

    def cmd_library_summary(self, args):
        from msr import library as lib
        stats = self._scan()
        return {"summary": lib.to_dict(stats), "text": lib.summary_lines(stats)}

    def cmd_library_files(self, args):
        stats = self._scan(files=True)
        recs = self._pick(args, stats)
        limit = max(1, min(int(args.get("limit", 500)), 20000))
        return {"files": [_rec_dict(r) for r in recs[:limit]], "count": len(recs),
                "bytes": sum(r.size for r in recs), "truncated": max(0, len(recs) - limit)}

    def _plan(self, args):
        from msr import library_actions as act
        action = str(args.get("action", act.ACTION_PENDING))
        if action == act.ACTION_TRASH and os.name != "nt":
            raise ControlError("The Recycle Bin is only on Windows.", "unsupported")
        guard = self._guard()
        stats = self._scan(files=True)
        if action == act.ACTION_RMDIR:
            return act.plan_empty_dirs(guard, stats.empty_dirs), guard
        if action not in (act.ACTION_PENDING, act.ACTION_TRASH, act.ACTION_PERMANENT, act.ACTION_RESTORE):
            raise ControlError("action must be pending, trash, permanent, restore or rmdir.", "bad_request")
        if not any(args.get(k) for k in ("paths", "channel", "platform", "kinds", "text", "month",
                                          "older_than_days", "from", "to", "category",
                                          "keep_newest_files", "keep_newest_bytes")):
            raise ControlError("Say which files (paths, channel, kinds, older_than_days...): "
                               "a library action never runs on everything by default.", "bad_request")
        return act.build_plan(guard, self._pick(args, stats), action,
                              include_sidecars=not args.get("no_sidecars")), guard

    def cmd_library_plan(self, args):
        """The dry run: what an action would do. Nothing is touched."""
        plan, _guard = self._plan(args)
        return _plan_dict(plan)

    def cmd_library_execute(self, args):
        from msr import library_actions as act
        if not self._library_lock.acquire(blocking=False):
            raise ControlError("Another library action is running.", "busy")
        try:
            plan, guard = self._plan(args)
            if not plan.ok:
                raise ControlError("Nothing to do (see library_plan for why files were skipped).", "nothing")
            if plan.action == act.ACTION_PERMANENT:
                if str(args.get("confirm", "")) != plan.confirm_word:
                    raise ControlError(f"Permanent delete of {plan.files} file(s): repeat with "
                                       f"confirm=\"{plan.confirm_word}\".", "confirm_required")
            else:
                self._confirm(args, f"{act.ACTION_LABELS[plan.action]}: {plan.files or plan.folders} item(s).")
            log = act.ActionLog(os.path.join(self.app.data_dir, "library_actions.log"))
            if not log.writable():
                raise ControlError("library_actions.log cannot be written; nothing was done.", "write_failed")
            manifest = act.Manifest(os.path.join(self.app.data_dir, "library_moves.json"))
            result = act.execute(plan, guard, act.deleter_for(plan.action, guard, manifest), log=log,
                                 manifest=manifest, logger=logging.getLogger("msr.library"))
        finally:
            self._library_lock.release()
        return {"batch": result.batch, "done": result.done, "bytes": result.bytes,
                "failed": [{"path": p, "reason": r} for p, r in result.failed],
                "cancelled": result.cancelled, "summary": result.summary}

    def cmd_library_pending(self, args):
        from msr import library_actions as act
        stats = self._scan(files=True)
        config = self.app.config
        info = act.pending_info(stats, config.getint("Cleanup", "auto_purge_days", fallback=7),
                                config.getboolean("Cleanup", "purge_on_startup", fallback=True))
        return {"files": info.files, "bytes": info.bytes, "due_files": info.due_files,
                "due_bytes": info.due_bytes, "next_due": info.next_due, "text": info.lines()}

    def cmd_library_batches(self, args):
        from msr import library_actions as act
        moves = act.Manifest(os.path.join(self.app.data_dir, "library_moves.json")).load()
        batches = {}
        for m in moves:
            b = batches.setdefault(m.get("batch"), {"batch": m.get("batch"), "files": 0, "bytes": 0, "time": 0})
            b["files"] += 1
            b["bytes"] += int(m.get("size") or 0)
            b["time"] = max(b["time"], m.get("time") or 0)
        return {"batches": sorted(batches.values(), key=lambda b: -b["time"])}

    def cmd_library_suggest(self, args):
        """The Library window's clean-up suggestions (read only: nothing is touched)."""
        from msr import library_actions as act
        stats = self._scan(files=True)
        older = int(args.get("older_days") or 90)
        suggestions = act.build_suggestions(stats, older_days=older)
        if args.get("free_gb"):
            suggestions.insert(0, act.free_up_suggestion(stats, int(float(args["free_gb"]) * 1024 ** 3)))
        limit = max(1, min(int(args.get("limit", 50)), 5000))
        return {"suggestions": [
            {"key": g.key, "title": g.title, "detail": g.detail, "count": g.count, "bytes": g.bytes,
             "files": [r.path for r in g.recs[:limit]], "folders": list(g.dirs[:limit]),
             "truncated": max(0, g.count - limit)} for g in suggestions],
            "note": "Read only. To act, pass the files to library plan/run with --path."}

    def cmd_library_log(self, args):
        n = max(1, min(int(args.get("lines", 50)), 5000))
        lines = deque(maxlen=n)
        try:
            with open(os.path.join(self.app.data_dir, "library_actions.log"), "r",
                      encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    lines.append(line.rstrip("\n"))
        except FileNotFoundError:
            pass
        return {"lines": list(lines)}

    def cmd_library_restore(self, args):
        """Undo one move to PendingDeletion (batch id from library_batches / library_execute, or last)."""
        from msr import library_actions as act
        batch = str(args.get("batch") or "")
        if not batch and _truthy(args.get("last")):
            batches = self.cmd_library_batches({})["batches"]
            batch = batches[0]["batch"] if batches else ""
            if not batch:
                raise ControlError("No move to PendingDeletion to undo.", "nothing")
        if not batch:
            raise ControlError("Give the batch id (library_batches) or last.", "bad_request")
        if not self._library_lock.acquire(blocking=False):
            raise ControlError("Another library action is running.", "busy")
        try:
            guard = self._guard()
            manifest = act.Manifest(os.path.join(self.app.data_dir, "library_moves.json"))
            plan = act.restore_batch_plan(guard, manifest, batch)
            if not plan.ok:
                raise ControlError("Nothing of that batch is left to restore.", "nothing")
            self._confirm(args, f"Restore {plan.files} file(s).")
            log = act.ActionLog(os.path.join(self.app.data_dir, "library_actions.log"))
            result = act.execute(plan, guard, act.deleter_for(act.ACTION_RESTORE, guard, manifest), log=log,
                                 manifest=manifest, logger=logging.getLogger("msr.library"), batch=batch)
        finally:
            self._library_lock.release()
        return {"done": result.done, "failed": [{"path": p, "reason": r} for p, r in result.failed]}

    # -------------------------------------------------- history (Restore previous...)
    def _history_target(self, args):
        which = str(args.get("file", "")).lower()
        if which in ("channels", "channels.json"):
            return "channels", self.app.channels_file
        if which in ("config", "config.ini", "settings"):
            return "config", os.path.abspath(self.app.config.config_file)
        raise ControlError("file must be channels or config.", "bad_request")

    def cmd_history_list(self, args):
        from msr import history
        which, path = self._history_target(args)
        try:
            current = open(path, "rb").read()
        except OSError:
            current = None
        out = []
        for i, v in enumerate(history.list_versions(path), 1):
            try:
                note = history.summarize(os.path.basename(path), v.read(), current)
            except Exception:
                note = ""
            out.append({"index": i, "saved": v.stamp.isoformat(timespec="seconds"), "size": v.size, "summary": note})
        return {"file": which, "versions": out}

    def cmd_history_restore(self, args):
        from msr import history
        which, path = self._history_target(args)
        versions = history.list_versions(path)
        try:
            version = versions[int(args.get("index", 0)) - 1]
            if int(args.get("index", 0)) < 1:
                raise IndexError
        except (IndexError, ValueError):
            raise ControlError("No such version (see history_list).", "not_found") from None
        if which == "channels" and is_running_session(self.app.recorder):
            raise ControlError("Stop recording first, then restore the channel list.", "conflict")
        self._confirm(args, f"Restore the {which} file saved {version.stamp:%Y-%m-%d %H:%M:%S}.")
        history.restore(path, version)
        if which == "channels":
            count = self.app.reload_channels()
            return {"restored": which, "channels": count}
        self.app.config.reload()
        return {"restored": which, "note": "Restart Multi-Stream Recorder to use every restored setting."}


# ------------------------------------------------------------------ the server
class ControlServer:
    def __init__(self, controller, data_dir, mode, host="127.0.0.1"):
        self.controller = controller
        self.data_dir = os.path.abspath(data_dir)
        self.mode = mode
        self.host = host
        self.token = secrets.token_urlsafe(32)
        self.path = os.path.join(self.data_dir, CONTROL_FILE)
        self._sock = None
        self._thread = None
        self._closing = threading.Event()

    @property
    def port(self):
        return self._sock.getsockname()[1] if self._sock else None

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if os.name == "nt":
            sock.setsockopt(socket.SOL_SOCKET, getattr(socket, "SO_EXCLUSIVEADDRUSE", 0) or socket.SO_REUSEADDR, 1)
        sock.bind((self.host, 0))
        sock.listen(16)
        sock.settimeout(0.5)
        self._sock = sock
        _write_private(self.path, json.dumps({
            "protocol": PROTOCOL, "api_version": API_VERSION, "host": self.host, "port": self.port, "token": self.token,
            "pid": os.getpid(), "mode": self.mode, "version": __version__, "started_at": round(time.time(), 1),
        }, indent=2).encode("utf-8"))
        self._thread = threading.Thread(target=self._serve, daemon=True, name="control-server")
        self._thread.start()
        logging.info(f"Control: listening on {self.host}:{self.port} (token in {CONTROL_FILE})")
        return self

    def stop(self):
        self._closing.set()
        try:
            if self._sock:
                self._sock.close()
        except OSError:
            pass
        try:  # only ours: a newer copy may have replaced it (closed before the remove: Windows)
            with open(self.path, "r", encoding="utf-8") as fh:
                ours = json.load(fh).get("pid") == os.getpid()
            if ours:
                os.remove(self.path)
        except (OSError, ValueError):
            pass

    def _serve(self):
        while not self._closing.is_set():
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            if addr[0] not in ("127.0.0.1", "::1"):
                conn.close()
                continue
            threading.Thread(target=self._client, args=(conn,), daemon=True, name="control-client").start()

    def _client(self, conn):
        with conn:
            conn.settimeout(30)
            try:
                buf = b""
                while b"\n" not in buf:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buf += chunk
                    if len(buf) > MAX_REQUEST:
                        raise ValueError("request too large")
                reply = self.dispatch(buf.split(b"\n", 1)[0])
            except (OSError, ValueError) as e:
                reply = {"ok": False, "code": "bad_request", "error": str(e)}
            try:
                conn.sendall(json.dumps(reply).encode("utf-8") + b"\n")
            except OSError:
                pass

    def dispatch(self, line: bytes) -> dict:
        try:
            req = json.loads(line.decode("utf-8"))
            if not isinstance(req, dict):
                raise ValueError
        except (ValueError, UnicodeDecodeError):
            return {"ok": False, "code": "bad_request", "error": "Send one JSON object per line."}
        token = req.get("token")
        if not isinstance(token, str) or not hmac.compare_digest(token.encode(), self.token.encode()):
            logging.warning("Control: request with a wrong or missing token refused")
            return {"ok": False, "code": "auth", "error": "Wrong or missing token."}
        return self.controller.handle(req.get("cmd"), req.get("args") or {})


# ------------------------------------------------------------------ the client
class ApiMismatch(Exception):
    """The running copy speaks another api_version than this client."""


class NotRunning(Exception):
    pass


def read_control_file(data_dir):
    try:
        with open(os.path.join(data_dir, CONTROL_FILE), "r", encoding="utf-8") as fh:
            info = json.load(fh)
        int(info["port"]), str(info["token"])
        return info
    except (OSError, ValueError, KeyError, TypeError):
        return None


def call(data_dir, cmd, args=None, timeout=600.0, token=None):
    """Send one command to the copy running for ``data_dir``. Raises NotRunning if none answers."""
    info = read_control_file(data_dir)
    if info is None:
        raise NotRunning("no msr_control.json")
    theirs = info.get("api_version")
    if theirs != API_VERSION:
        raise ApiMismatch(
            f"The running Multi-Stream Recorder speaks control API v{theirs or '0 (older build)'}, this "
            f"msr_ctl.py speaks v{API_VERSION}. Use the msr_ctl.py next to the running copy, or "
            "restart MSR from this folder.")
    try:
        sock = socket.create_connection(("127.0.0.1", int(info["port"])), timeout=5)
    except OSError as e:  # refused: the copy that wrote the file is gone
        raise NotRunning(str(e)) from None
    with sock:  # a timeout from here on is a real one (socket.timeout), not "not running"
        deadline = time.monotonic() + float(timeout)
        sock.settimeout(5)
        sock.sendall(json.dumps({"token": token if token is not None else info["token"],
                                 "cmd": cmd, "args": args or {}}).encode("utf-8") + b"\n")
        buf = b""
        while b"\n" not in buf:
            # Short slices: Ctrl+C is seen within a second (Windows only delivers it between
            # calls), and the overall timeout still holds.
            left = deadline - time.monotonic()
            if left <= 0:
                raise socket.timeout("no reply in time")
            sock.settimeout(min(1.0, left))
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                break
            buf += chunk
    try:
        return json.loads(buf.split(b"\n", 1)[0].decode("utf-8"))
    except ValueError:
        raise NotRunning("no answer") from None


# ------------------------------------------------------------------ wiring for the app
class Service:
    """Server + status writer for one app adapter; ``start_for`` returns None when turned off."""

    def __init__(self, app):
        self.app = app
        self.controller = Controller(app)
        interval = STATUS_INTERVAL
        try:
            interval = app.config.getfloat("Control", "status_interval_seconds", fallback=STATUS_INTERVAL)
        except Exception:
            pass
        from msr.agent_status import Tracker
        app.tracker = Tracker(app.config)
        self.status = StatusWriter(app, interval)
        self.controller.on_change = self.status.poke
        self.server = ControlServer(self.controller, app.data_dir, app.mode)

    @classmethod
    def start_for(cls, app):
        if not enabled(app.config):
            logging.info("Control: off (MSR_CONTROL=0 or [Control] enabled = false)")
            return None
        service = cls(app)
        try:
            service.server.start()
        except OSError as e:
            logging.warning(f"Control: could not start the local control channel: {e}")
            service.server = None
        service.status.start()
        return service

    def stop(self):
        if self.server is not None:
            self.server.stop()
        self.status.stop()
