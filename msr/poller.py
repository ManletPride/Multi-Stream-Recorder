"""Lightweight in-process polling coordinator for offline channels.

Replaces the old model of spawning one full Python ``mp.Process`` per idle
channel (~30-45 MB RSS each on Windows ``spawn``). Instead:

* Offline channels are scheduled by a single lightweight coordinator thread
  and checked via a small bounded thread pool.
* Startup polls are staggered gently so watching 30-50 channels does not boot
  dozens of subprocesses simultaneously.
* When Twitch OAuth is connected, due Twitch channels are checked in a single
  batched ``GET /helix/streams`` call (up to 100 channels per HTTP request),
  falling back cleanly to ``streamlink`` if unauthenticated or if Helix fails.
* Kick channels use the lightweight ``curl_cffi`` HTTP API
  (``/api/v2/channels/{slug}``), falling back to ``streamlink`` only when
  ``curl_cffi`` is unavailable or blocked (e.g. HTTP 403).
* Fishtank channels share a single ``FishtankAuth`` instance instead of N
  workers hitting ``/v1/live-streams`` independently.
* As soon as a channel is detected live (and storage is usable), the
  coordinator invokes ``on_live(channel_key)`` so ``StreamRecorder`` spawns a
  dedicated capture worker process. When that stream ends and its reconnect
  grace window expires, the worker exits cleanly and hands the channel back to
  the coordinator.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import logging
import os
import random
import threading
import time

from msr import presence as _presence
from msr import storage
from msr import twitch_eventsub as _tw
from msr.config import default_streams_dir, fishtank_password, parser_getfloat, parser_getint
from msr.deps import HAS_CURL_CFFI
from msr.platforms import (
    FishtankAuth,
    check_stream_fishtank,
    check_stream_kick_api,
    check_stream_rumble_html,
    check_stream_streamlink,
    check_stream_ytdlp,
    check_tiktok_live_webcast,
)
from msr.util import (
    find_cookies_file,
    jittered_sleep,
    parse_channel_key,
    redact_for_log,
)

# Floor applied to offline fallback polls when Kick/Twitch WebSocket push is active.
PUSH_POLL_FLOOR_S = 600.0
# Upper bound on startup stagger across all non-batched channels.
MAX_STARTUP_STAGGER_S = 8.0
STARTUP_STEP_S = 0.35
DEFAULT_MAX_WORKERS = 6
# Cooldown between full multi-endpoint TikTok webcast fallbacks when yt-dlp
# cleanly reports offline (avoids 3-5 sequential HTTP hits on every offline poll).
TIKTOK_WEBCAST_COOLDOWN_S = 300.0


def _build_channel_url(platform: str, username: str) -> tuple[str, str]:
    """Return ``(url, normalized_username)`` matching ``msr.worker.record_worker``."""
    if platform == "kick":
        return f"https://kick.com/{username}", username
    if platform == "twitch":
        return f"https://twitch.tv/{username}", username
    if platform == "youtube":
        if username.startswith("UC"):
            return f"https://youtube.com/channel/{username}/live", username
        if username.startswith("@"):
            return f"https://youtube.com/{username}/live", username
        if "watch?v=" in username or len(username) == 11:
            if "watch?v=" in username:
                return (username if username.startswith("http") else f"https://youtube.com/{username}"), username
            return f"https://youtube.com/watch?v={username}", username
        return f"https://youtube.com/@{username}/live", username
    if platform == "rumble":
        return f"https://rumble.com/c/{username}", username
    if platform == "tiktok":
        handle = username.lstrip("@")
        return f"https://www.tiktok.com/@{handle}/live", handle
    if platform == "fishtank":
        return f"fishtank:{username}", username
    if platform == "custom":
        url = username
        if "tiktok.com" in url.lower():
            base = url.split("?")[0].rstrip("/")
            if not base.endswith("/live"):
                url = base + "/live"
        return url, username
    return f"https://{platform}.com/{username}", username


class _ChannelLogger:
    """Thin logger adapter prefixing messages with ``[channel]`` for logview filtering."""

    def __init__(self, channel_key: str, base_logger: logging.Logger | None = None):
        self._tag = f"[{channel_key}]"
        self._log = base_logger or logging.getLogger("msr.poller")

    def _fmt(self, msg, args):
        text = (msg % args) if args else str(msg)
        return f"{self._tag} {redact_for_log(text)}"

    def debug(self, msg, *args, **kwargs):
        self._log.debug(self._fmt(msg, args), **kwargs)

    def info(self, msg, *args, **kwargs):
        self._log.info(self._fmt(msg, args), **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._log.warning(self._fmt(msg, args), **kwargs)

    def error(self, msg, *args, **kwargs):
        self._log.error(self._fmt(msg, args), **kwargs)


class _ChannelSlot:
    __slots__ = (
        "key", "platform", "username", "url",
        "next_due", "error_sleep", "consecutive_errors",
        "in_flight", "paused", "manual", "last_status",
    )

    def __init__(self, key: str, platform: str, username: str, url: str, next_due: float, poll_base: float):
        self.key = key
        self.platform = platform
        self.username = username
        self.url = url
        self.next_due = next_due
        self.error_sleep = poll_base
        self.consecutive_errors = 0
        self.in_flight = False
        self.paused = False
        self.manual = False
        self.last_status = None


class PollCoordinator:
    """Coordinates staggered offline checks and spawns capture workers only on go-live."""

    def __init__(
        self,
        config,
        runtime,
        status_queue,
        on_live,
        *,
        presence=None,
        data_dir: str | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        clock=time.monotonic,
        helix_streams=None,
        kick_http=None,
    ):
        self.config = config
        self.runtime = runtime
        self.status_queue = status_queue
        self.on_live = on_live
        self.presence = presence
        if data_dir is None:
            cfg_file = getattr(config, "config_file", None)
            data_dir = os.path.dirname(os.path.abspath(cfg_file)) if cfg_file else os.getcwd()
        self.data_dir = data_dir
        self._max_workers = max(1, int(max_workers))
        self._clock = clock
        self._helix_streams = helix_streams or _tw.get_live_streams
        self._kick_http = kick_http or _presence.fetch_kick_channel

        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._slots: dict[str, _ChannelSlot] = {}
        self._thread: threading.Thread | None = None
        self._pool: ThreadPoolExecutor | None = None
        self._fishtank_auth: FishtankAuth | None = None
        self._fishtank_lock = threading.Lock()
        self._token_store: _tw.TokenStore | None = None
        self._twitch_batch_in_flight = False
        self._tiktok_webcast_last: dict[str, float] = {}
        self._is_running = False

    @property
    def is_running(self) -> bool:
        return self._is_running and not self._stop.is_set()

    # ── Config accessors ────────────────────────────────────────────────────

    def _poll_base_seconds(self) -> float:
        try:
            mins = float(self.runtime.get("poll_interval_minutes", 0) or 0)
        except Exception:
            mins = 0.0
        if mins <= 0:
            mins = parser_getfloat(self.config, "Timeouts", "poll_interval_minutes", 3.0)
        return max(1.0, mins * 60.0)

    def _jitter_percent(self) -> int:
        return parser_getint(self.config, "Timeouts", "poll_jitter_percent", 20)

    def _error_backoff_max(self) -> float:
        return max(10.0, parser_getfloat(self.config, "Timeouts", "error_backoff_max_minutes", 15.0) * 60.0)

    def _check_timeout(self) -> int:
        return parser_getint(self.config, "Timeouts", "stream_check_timeout", 30)

    def _cookies_file(self) -> str | None:
        return find_cookies_file(self.config)

    def _storage_ok(self) -> tuple[bool, str]:
        primary = self.config.get("Paths", "streams_dir", fallback="") or default_streams_dir()
        fb_cfg = self.config.get("Paths", "fallback_dir", fallback="")
        fb_root = storage.fallback_root(fb_cfg, primary)
        min_gb = parser_getfloat(self.config, "Recording", "min_disk_space_gb", 5.0)
        choice = storage.choose_root(primary, fb_root, min_gb)
        return bool(choice.ok), choice.reason or ""

    def _push_active(self, slot: _ChannelSlot) -> bool:
        if slot.platform not in ("kick", "twitch"):
            return False
        try:
            return bool(self.runtime.get(f"{slot.platform}_push:{slot.key}", False))
        except Exception:
            return False

    def _get_fishtank_auth(self) -> FishtankAuth:
        with self._fishtank_lock:
            if self._fishtank_auth is None:
                ft_email = self.config.get("Fishtank", "email", fallback="")
                ft_password = fishtank_password(self.config)
                self._fishtank_auth = FishtankAuth(
                    self._cookies_file(),
                    logging.getLogger("msr.poller.fishtank"),
                    email=ft_email,
                    password=ft_password,
                )
            return self._fishtank_auth

    def _get_token_store(self) -> _tw.TokenStore:
        if self._token_store is None:
            self._token_store = _tw.TokenStore(self.data_dir)
        return self._token_store

    # ── Status publishing ───────────────────────────────────────────────────

    def _emit_status(self, key: str, payload: dict) -> None:
        try:
            self.status_queue.put((key, payload))
        except Exception:
            pass

    def _update_presence(self, key: str, live: bool, viewers=None, since=None, source="poll") -> None:
        if self.presence is None:
            return
        try:
            self.presence.update(key, live, viewers, since, source=source)
        except Exception:
            pass

    # ── Lifecycle & channel management ──────────────────────────────────────

    def start(self, channels) -> None:
        """Start the coordinator loop and stagger initial checks across *channels*."""
        with self._lock:
            if self._is_running:
                return
            self._stop.clear()
            self._wake.clear()
            self._is_running = True
            self._slots.clear()
            now = self._clock()
            poll_base = self._poll_base_seconds()

            parsed_list = []
            for ch in channels:
                parsed = parse_channel_key(ch)
                if parsed is None:
                    self._emit_status(ch, {
                        "status": "Error", "detail": "unsafe or unknown channel key",
                        "size": "", "time": "", "progress": 0,
                    })
                    continue
                parsed_list.append((ch, parsed[0], parsed[1]))

            non_batched = [item for item in parsed_list if item[1] not in ("twitch", "fishtank")]
            step = min(STARTUP_STEP_S, MAX_STARTUP_STAGGER_S / max(1, len(non_batched))) if len(non_batched) > 1 else 0.0
            non_batched_idx = 0
            for ch, platform, username in parsed_list:
                url, norm_user = _build_channel_url(platform, username)
                if platform == "twitch":
                    due = now
                elif platform == "fishtank":
                    due = now + 0.15
                else:
                    due = now + non_batched_idx * step
                    non_batched_idx += 1
                self._slots[ch] = _ChannelSlot(ch, platform, norm_user, url, due, poll_base)

            self._pool = ThreadPoolExecutor(max_workers=self._max_workers, thread_name_prefix="msr-poll")
            self._thread = threading.Thread(target=self._loop, daemon=True, name="msr-poll-coordinator")
            self._thread.start()
            self._wake.set()

    def stop(self) -> None:
        """Stop the coordinator thread and worker pool cleanly."""
        self._stop.set()
        self._is_running = False
        self._wake.set()
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=5)
        self._thread = None
        pool = self._pool
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
        self._pool = None
        with self._lock:
            self._slots.clear()
            self._twitch_batch_in_flight = False

    def add_channel(self, channel_key: str, *, immediate: bool = True) -> bool:
        """Add or reactivate *channel_key* in the coordinator."""
        parsed = parse_channel_key(channel_key)
        if parsed is None:
            return False
        platform, username = parsed
        url, norm_user = _build_channel_url(platform, username)
        now = self._clock()
        poll_base = self._poll_base_seconds()
        with self._lock:
            slot = self._slots.get(channel_key)
            if slot is None:
                slot = _ChannelSlot(channel_key, platform, norm_user, url, now if immediate else now + poll_base, poll_base)
                self._slots[channel_key] = slot
            else:
                slot.paused = False
                slot.consecutive_errors = 0
                slot.error_sleep = poll_base
                if immediate:
                    slot.next_due = now
        self._wake.set()
        return True

    def pause_channel(self, channel_key: str) -> None:
        """Pause polling for *channel_key* (e.g., stopped or handed to a live worker)."""
        with self._lock:
            slot = self._slots.get(channel_key)
            if slot is not None:
                slot.paused = True
                slot.manual = False

    def resume_channel(self, channel_key: str, *, delay: float | None = None) -> None:
        """Resume idle polling after a live worker exited cleanly."""
        now = self._clock()
        poll_base = self._poll_base_seconds()
        with self._lock:
            slot = self._slots.get(channel_key)
            if slot is None:
                return
            slot.paused = False
            slot.in_flight = False
            slot.consecutive_errors = 0
            slot.error_sleep = poll_base
            if delay is None:
                push = self._push_active(slot)
                eff = max(poll_base, PUSH_POLL_FLOOR_S) if push else poll_base
                wait = jittered_sleep(eff, self._jitter_percent())
            else:
                wait = max(0.0, float(delay))
            slot.next_due = now + wait
            detail = f"next check ~{int(wait)}s"
            if self._push_active(slot):
                detail = f"push: listening \u00b7 {detail}"
            st = {"status": "Offline", "detail": detail, "size": "", "time": "", "progress": 0}
            slot.last_status = st
            self._emit_status(channel_key, st)
        self._wake.set()

    def remove_channel(self, channel_key: str) -> None:
        with self._lock:
            self._slots.pop(channel_key, None)

    def has_idle_channel(self, channel_key: str) -> bool:
        with self._lock:
            slot = self._slots.get(channel_key)
            return bool(slot is not None and not slot.paused)

    def idle_channels(self) -> list[str]:
        with self._lock:
            return [k for k, s in self._slots.items() if not s.paused]

    def wake_channel(self, channel_key: str, *, manual: bool = False) -> bool:
        """Schedule *channel_key* for immediate checking if it is currently idle here."""
        now = self._clock()
        with self._lock:
            slot = self._slots.get(channel_key)
            if slot is None or slot.paused:
                return False
            slot.next_due = now
            if manual:
                slot.manual = True
        self._wake.set()
        return True

    def wake_many(self, channels=None, *, spread_max: float = 10.0, manual: bool = False) -> list[str]:
        """Stagger immediate checks across idle channels (all idle if *channels* is None)."""
        now = self._clock()
        with self._lock:
            if channels is None:
                targets = [k for k, s in self._slots.items() if not s.paused]
            else:
                targets = [k for k in channels if (k in self._slots and not self._slots[k].paused)]
            if not targets:
                return []
            random.shuffle(targets)
            non_tw = [k for k in targets if self._slots[k].platform != "twitch"]
            step = min(1.0, spread_max / max(1, len(non_tw))) if len(non_tw) > 1 else 0.0
            idx = 0
            for k in targets:
                slot = self._slots[k]
                if slot.platform == "twitch":
                    slot.next_due = now
                else:
                    slot.next_due = now + idx * step
                    idx += 1
                if manual:
                    slot.manual = True
        self._wake.set()
        return targets

    def on_poll_interval_changed(self, minutes: float) -> None:
        """Tighten upcoming due times when the user lowers the poll interval."""
        now = self._clock()
        new_base = max(1.0, float(minutes) * 60.0)
        with self._lock:
            for slot in self._slots.values():
                if slot.paused:
                    continue
                cap = now + (max(new_base, PUSH_POLL_FLOOR_S) if self._push_active(slot) else new_base)
                if slot.next_due > cap:
                    slot.next_due = now + random.uniform(0.0, min(3.0, new_base))
        self._wake.set()

    # ── Scheduler loop ──────────────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop.is_set():
            wait_s = self.step_once()
            if self._stop.is_set():
                break
            self._wake.wait(timeout=max(0.05, min(wait_s, 1.0)))
            self._wake.clear()

    def step_once(self) -> float:
        """Dispatch all currently due checks; return seconds until the next due slot."""
        now = self._clock()
        due_twitch: list[_ChannelSlot] = []
        due_other: list[_ChannelSlot] = []
        next_due: float | None = None

        with self._lock:
            for slot in self._slots.values():
                if slot.paused or slot.in_flight:
                    continue
                if slot.next_due <= now:
                    if slot.platform == "twitch":
                        if not self._twitch_batch_in_flight:
                            due_twitch.append(slot)
                    else:
                        slot.in_flight = True
                        due_other.append(slot)
                else:
                    if next_due is None or slot.next_due < next_due:
                        next_due = slot.next_due

            if due_twitch:
                self._twitch_batch_in_flight = True
                for slot in due_twitch:
                    slot.in_flight = True

        pool = self._pool
        if due_twitch:
            if pool is not None:
                pool.submit(self._run_twitch_batch, due_twitch)
            else:
                self._run_twitch_batch(due_twitch)
        for slot in due_other:
            if pool is not None:
                pool.submit(self._run_single_check, slot)
            else:
                self._run_single_check(slot)

        if next_due is None:
            return 1.0
        return max(0.05, next_due - self._clock())

    # ── Platform check implementations ──────────────────────────────────────

    def _helix_batch_check(self, slots: list[_ChannelSlot]) -> dict[str, dict] | None:
        """Query Helix ``GET /helix/streams`` for *slots*. Returns ``None`` to trigger streamlink fallback."""
        store = self._get_token_store()
        try:
            tok = store.get_fresh()
        except Exception:
            return None
        if not tok:
            return None
        logins = [s.username.lower() for s in slots]
        try:
            return self._helix_streams(tok, logins)
        except _tw.TwitchAuthError:
            try:
                tok2 = store.get_fresh(force=True, used=tok.get("refresh_token"))
            except Exception:
                return None
            if not tok2:
                return None
            try:
                return self._helix_streams(tok2, logins)
            except Exception:
                return None
        except Exception as exc:
            logging.debug("Twitch Helix batch streams check failed, falling back to streamlink: %s", exc)
            return None

    def _run_twitch_batch(self, slots: list[_ChannelSlot]) -> None:
        try:
            if self._stop.is_set():
                return
            for slot in slots:
                detail = "manual check" if slot.manual else "checking Twitch (Helix)"
                slot.manual = False
                self._emit_status(slot.key, {
                    "status": "Checking...", "detail": detail,
                    "size": "", "time": "", "progress": 0,
                })
            live_map = self._helix_batch_check(slots)
            if live_map is not None:
                for slot in slots:
                    if self._stop.is_set():
                        return
                    entry = live_map.get(slot.username.lower())
                    if entry is not None:
                        self._update_presence(
                            slot.key, True,
                            entry.get("viewer_count"), entry.get("started_at"),
                            source="poll",
                        )
                        self._finish_check(slot, True, entry.get("title"), None)
                    else:
                        self._update_presence(slot.key, False, None, None, source="poll")
                        self._finish_check(slot, False, None, None)
                return

            # Fallback when not signed into Twitch or Helix is temporarily unavailable.
            for slot in slots:
                if self._stop.is_set():
                    return
                self._run_single_check(slot, already_marked_checking=True)
        finally:
            with self._lock:
                self._twitch_batch_in_flight = False
                for slot in slots:
                    slot.in_flight = False
            self._wake.set()

    def _check_kick_lightweight(self, slot: _ChannelSlot, logger, timeout: int, cookies_file: str | None):
        """Use Kick's JSON API via curl_cffi; fall back to streamlink only if blocked/unavailable."""
        if HAS_CURL_CFFI or self._kick_http is not _presence.fetch_kick_channel:
            try:
                data = self._kick_http(slot.username, timeout=min(timeout, 15))
            except TypeError:
                data = self._kick_http(slot.username)
            except Exception:
                data = None
            if isinstance(data, dict):
                live, viewers, since, _cid = _presence.parse_kick_channel_json(data)
                if live is not None:
                    self._update_presence(slot.key, live, viewers, since, source="poll")
                    ls = data.get("livestream") if isinstance(data.get("livestream"), dict) else {}
                    title = ls.get("session_title") if live else None
                    return bool(live), title, None
        # Fallback: Cloudflare challenge or curl_cffi absent — use streamlink/yt-dlp.
        is_live, title, error = check_stream_kick_api(slot.username, logger, max(timeout, 45), cookies_file)
        if is_live is None:
            is_live, title, error, _, _, _ = check_stream_ytdlp(slot.url, logger, timeout, cookies_file)
        if error is None:
            self._update_presence(slot.key, bool(is_live), None, None, source="poll")
        return bool(is_live), title, error

    def _check_one(self, slot: _ChannelSlot) -> tuple[bool, str | None, str | None, str | None]:
        """Run one platform check. Returns ``(is_live, title, error, extra_detail)``."""
        logger = _ChannelLogger(slot.key)
        timeout = self._check_timeout()
        cookies_file = self._cookies_file()
        plat = slot.platform

        if plat == "kick":
            is_live, title, err = self._check_kick_lightweight(slot, logger, timeout, cookies_file)
            return is_live, title, err, None

        if plat == "rumble":
            is_live, title, _resolved, _hls, err = check_stream_rumble_html(
                slot.username, logger, timeout, cookies_file,
            )
            return bool(is_live), title, err, None

        if plat == "tiktok":
            is_live, title, err, _, _, _ = check_stream_ytdlp(slot.url, logger, timeout, cookies_file)
            if not is_live:
                now = self._clock()
                last_wc = self._tiktok_webcast_last.get(slot.key)
                # Always run webcast fallback on first check, manual check, yt-dlp error,
                # or once the cooldown has elapsed; skip on back-to-back clean offline polls.
                if err or last_wc is None or (now - last_wc) >= TIKTOK_WEBCAST_COOLDOWN_S:
                    self._tiktok_webcast_last[slot.key] = now
                    wc_live, _, wc_title, _wc_err = check_tiktok_live_webcast(
                        slot.username, cookies_file, logger, timeout,
                    )
                    if wc_live:
                        return True, wc_title or title, None, None
            return bool(is_live), title, err, None

        if plat in ("youtube", "custom"):
            is_live, title, err, _, _, _ = check_stream_ytdlp(slot.url, logger, timeout, cookies_file)
            return bool(is_live), title, err, None

        if plat == "fishtank":
            auth = self._get_fishtank_auth()
            stream_id = auth.resolve_stream_id(slot.username)
            is_live, title, err = check_stream_fishtank(stream_id, auth, logger, timeout)
            extra = getattr(auth, "last_detail", None)
            return bool(is_live), title, err, extra

        # Twitch (fallback) and any other streamlink-handled platform.
        is_live, title, err = check_stream_streamlink(slot.url, logger, timeout)
        if err is None and plat == "twitch":
            self._update_presence(slot.key, bool(is_live), None, None, source="poll")
        return bool(is_live), title, err, None

    def _run_single_check(self, slot: _ChannelSlot, *, already_marked_checking: bool = False) -> None:
        try:
            if self._stop.is_set():
                return
            with self._lock:
                if slot.paused:
                    return
                was_manual = slot.manual
                slot.manual = False
            if not already_marked_checking:
                detail = "manual check" if was_manual else f"checking {slot.platform}"
                self._emit_status(slot.key, {
                    "status": "Checking...", "detail": detail,
                    "size": "", "time": "", "progress": 0,
                })
            is_live, title, error, extra_detail = self._check_one(slot)
            self._finish_check(slot, is_live, title, error, extra_detail=extra_detail)
        except Exception as exc:
            logging.warning("[%s] Idle poll failed: %s", slot.key, redact_for_log(str(exc)))
            self._finish_check(slot, False, None, str(exc)[:60])
        finally:
            with self._lock:
                slot.in_flight = False
            self._wake.set()

    def _finish_check(
        self,
        slot: _ChannelSlot,
        is_live: bool,
        title: str | None,
        error: str | None,
        *,
        extra_detail: str | None = None,
    ) -> None:
        if self._stop.is_set():
            return
        with self._lock:
            if slot.paused or slot.key not in self._slots:
                return

        now = self._clock()
        poll_base = self._poll_base_seconds()
        jitter = self._jitter_percent()

        if error:
            with self._lock:
                slot.consecutive_errors += 1
                slot.error_sleep = min(
                    max(slot.error_sleep, poll_base) * (1 if slot.consecutive_errors == 1 else 2),
                    self._error_backoff_max(),
                )
                wait = jittered_sleep(slot.error_sleep, jitter)
                slot.next_due = now + wait
                st = {"status": "Error", "detail": error, "size": "", "time": "", "progress": 0}
                slot.last_status = st
            self._emit_status(slot.key, st)
            return

        if is_live:
            ok, reason = self._storage_ok()
            if not ok:
                with self._lock:
                    slot.next_due = now + 60.0
                    st = {
                        "status": "Error",
                        "detail": reason or "drive not available",
                        "size": "", "time": "", "progress": 0,
                    }
                    slot.last_status = st
                self._emit_status(slot.key, st)
                return

            with self._lock:
                if slot.paused:
                    return
                slot.paused = True
                slot.consecutive_errors = 0
                slot.error_sleep = poll_base
            self._emit_status(slot.key, {
                "status": "Recording", "detail": "starting",
                "size": "", "time": "", "progress": 0,
            })
            try:
                launched = self.on_live(slot.key)
            except Exception as exc:
                logging.error("[%s] Failed to spawn capture worker: %s", slot.key, exc)
                launched = False
            if launched is False:
                # Could not launch — resume polling after a short retry delay.
                self.resume_channel(slot.key, delay=15.0)
            return

        # Offline
        push_active = self._push_active(slot)
        effective_base = max(poll_base, PUSH_POLL_FLOOR_S) if push_active else poll_base
        wait = jittered_sleep(effective_base, jitter)
        detail = f"next check ~{int(wait)}s"
        if extra_detail:
            detail = f"{extra_detail} - {detail}"
        if push_active:
            detail = f"push: listening \u00b7 {detail}"
        st = {"status": "Offline", "detail": detail, "size": "", "time": "", "progress": 0}
        with self._lock:
            slot.consecutive_errors = 0
            slot.error_sleep = poll_base
            slot.next_due = now + wait
            if st != slot.last_status:
                slot.last_status = st
                emit = True
            else:
                emit = False
        if emit:
            self._emit_status(slot.key, st)
