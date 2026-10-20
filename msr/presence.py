"""Live *presence* for every roster channel, independent of recording.

Recording workers only exist for enabled channels, so a disabled channel
never counted as live even while its streamer was. Presence is a separate,
read-only signal. Its only UI use is the roster's Live filter (the
Recording group still comes from the workers' status). Sources:

* Kick push events (``KickPushListener`` watches every Kick channel in
  channels.json while a session is running; disabled ones are "watch only"
  and can never wake or start a recorder).
* A slow, staggered poll of *disabled* channels (~10 min) via the Kick
  channel API and the existing Twitch streamlink check. Enabled channels are
  never polled here; their workers already check them.

Nothing in this module starts, stops, or wakes a recording.
"""

import logging
import random
import threading
import time

PRESENCE_POLL_SECONDS = 600       # each disabled channel ~every 10 min
PRESENCE_FIRST_DELAY = 20         # let the GUI settle before the first poll
PUSH_PRESENCE_FRESH_SECONDS = 180  # push "live" trusted this long for enabled rows


def _now():
    return time.time()


class PresenceStore:
    """Thread-safe ``key -> {live, viewers, since, source, updated}``."""

    def __init__(self, clock=_now):
        self._lock = threading.Lock()
        self._data = {}
        self._clock = clock
        self.version = 0  # bumped on every change (cheap GUI dirty check)

    def update(self, key, live, viewers=None, since=None, source=""):
        if not key:
            return
        now = self._clock()
        with self._lock:
            old = self._data.get(key) or {}
            live = bool(live)
            if live:
                since = since or (old.get("since") if old.get("live") else None) or now
                if viewers is None and old.get("live"):
                    viewers = old.get("viewers")  # push has no count; keep the poll's
            else:
                since, viewers = None, None
            new = {"live": live, "viewers": viewers, "since": since,
                   "source": source, "updated": now}
            changed = any(old.get(k) != new[k] for k in ("live", "viewers", "since", "source"))
            self._data[key] = new
            if changed:
                self.version += 1

    def get(self, key):
        with self._lock:
            v = self._data.get(key)
            return dict(v) if v else None

    def snapshot(self):
        with self._lock:
            return {k: dict(v) for k, v in self._data.items()}

    def forget(self, keep_keys):
        keep = set(keep_keys)
        with self._lock:
            for k in [k for k in self._data if k not in keep]:
                del self._data[k]
                self.version += 1


# ── Kick ──

def parse_kick_channel_json(data):
    """``/api/v2/channels/{slug}`` JSON -> ``(live, viewers, since, channel_id)``."""
    if not isinstance(data, dict):
        return None, None, None, None
    cid = data.get("id")
    try:
        cid = int(cid) if cid is not None else None
    except (TypeError, ValueError):
        cid = None
    ls = data.get("livestream")
    if not isinstance(ls, dict) or not ls:
        return False, None, None, cid
    live = ls.get("is_live")
    live = True if live is None else bool(live)
    viewers = ls.get("viewer_count", ls.get("viewers"))
    try:
        viewers = int(viewers) if viewers is not None else None
    except (TypeError, ValueError):
        viewers = None
    since = ls.get("start_time") or ls.get("created_at")
    return live, (viewers if live else None), (since if live else None), cid


def kick_slug_candidates(slug):
    """Kick URL slugs use ``-`` where usernames have ``_`` (betty_fae -> betty-fae)."""
    s = (slug or "").strip()
    out = [s]
    if "_" in s:
        out.append(s.replace("_", "-"))
    low = s.lower()
    if low != s and low not in out:
        out.append(low)
    return out


def fetch_kick_channel(slug, timeout=15):
    """Return the parsed channel JSON or None (quiet). Needs curl_cffi."""
    from msr.deps import HAS_CURL_CFFI
    if not HAS_CURL_CFFI:
        return None
    from curl_cffi import requests as curl_requests
    for cand in kick_slug_candidates(slug):
        try:
            r = curl_requests.get(f"https://kick.com/api/v2/channels/{cand}",
                                  impersonate="chrome", timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            continue
    return None


def poll_kick(slug):
    data = fetch_kick_channel(slug)
    if data is None:
        return None
    live, viewers, since, _cid = parse_kick_channel_json(data)
    return {"live": live, "viewers": viewers, "since": since}


def poll_twitch(name):
    """Existing streamlink check (no viewer count). None on failure."""
    from msr.platforms import check_stream_streamlink
    quiet = logging.getLogger("msr.presence.quiet")
    quiet.propagate = False
    quiet.addHandler(logging.NullHandler())
    try:
        res = check_stream_streamlink(f"https://twitch.tv/{name}", quiet, 30)
    except Exception:
        return None
    if not isinstance(res, tuple) or not res:
        return None
    is_live, _title, error = (tuple(res) + (None, None, None))[:3]
    if is_live is None or (error and not is_live):
        return None  # timeout / tool error: unknown, keep the last value
    return {"live": bool(is_live), "viewers": None, "since": None}


DEFAULT_POLLERS = {"kick": poll_kick, "twitch": poll_twitch}


def split_key(key):
    if ":" in (key or ""):
        p, rest = key.split(":", 1)
        return p.lower(), rest
    return "kick", key


def presence_poll_targets(channels, pollers=None):
    """Channels the presence poller may check: DISABLED ones only, on a
    platform we have a cheap check for. Enabled channels are left to their
    recording workers so their load never increases."""
    pollers = DEFAULT_POLLERS if pollers is None else pollers
    out = []
    for ch in channels:
        key = ch.get("name")
        if not key or ch.get("enabled", True):
            continue
        if split_key(key)[0] in pollers:
            out.append(key)
    return out


class PresencePoller:
    """One low-priority daemon thread; polls one channel at a time, spread
    evenly over ``interval`` seconds. Failures are silent (presence just
    keeps its last value)."""

    def __init__(self, store, get_channels, pollers=None, interval=PRESENCE_POLL_SECONDS,
                 first_delay=PRESENCE_FIRST_DELAY):
        self.store = store
        self.get_channels = get_channels
        self.pollers = DEFAULT_POLLERS if pollers is None else pollers
        self.interval = interval
        self.first_delay = first_delay
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="presence-poller")
        self._thread.start()

    def stop(self):
        self._stop.set()

    def poll_one(self, key):
        platform, name = split_key(key)
        fn = self.pollers.get(platform)
        if fn is None:
            return False
        try:
            res = fn(name)
        except Exception:
            res = None
        if not res or res.get("live") is None:
            return False
        self.store.update(key, res["live"], res.get("viewers"), res.get("since"),
                          source="poll")
        return True

    def poll_round(self, sleep=None):
        """Poll every current target once, spaced over the interval."""
        try:
            targets = presence_poll_targets(list(self.get_channels()), self.pollers)
        except Exception:
            return []
        random.shuffle(targets)
        gap = self.interval / max(len(targets), 1)
        done = []
        for i, key in enumerate(targets):
            if self._stop.is_set():
                break
            # Re-check: the user may have enabled it since the round began.
            try:
                still = key in presence_poll_targets(list(self.get_channels()), self.pollers)
            except Exception:
                still = False
            if still:
                self.poll_one(key)
                done.append(key)
            if sleep is not None and i < len(targets) - 1:
                sleep(gap * random.uniform(0.8, 1.2))
        return done

    def _run(self):
        if self._stop.wait(self.first_delay):
            return
        while not self._stop.is_set():
            started = time.monotonic()
            self.poll_round(sleep=lambda s: self._stop.wait(s))
            rest = self.interval - (time.monotonic() - started)
            if self._stop.wait(max(30.0, rest)):
                return


def combined_state(status_state, presence, enabled, now=None,
                   fresh=PUSH_PRESENCE_FRESH_SECONDS):
    """Merge the worker-derived state with presence (Live filter).

    * Worker says recording / live -> that wins.
    * Disabled channel -> presence decides (it has no worker).
    * Enabled channel -> presence only when it is a fresh push "live" (the
      worker is about to be woken); otherwise the worker's own check is
      authoritative, so a stale push cannot keep it "live".
    """
    if status_state in ("recording", "live"):
        return status_state
    if not presence or not presence.get("live"):
        return "offline"
    if not enabled:
        return "live"
    now = time.time() if now is None else now
    if presence.get("source") == "push" and now - (presence.get("updated") or 0) <= fresh:
        return "live"
    return "offline"

