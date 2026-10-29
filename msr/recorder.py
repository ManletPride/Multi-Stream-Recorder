"""Session orchestrator: worker processes, cleanup, Kick push listener."""
import json
import logging
import multiprocessing as mp
import os
import queue
import random
import shutil
import signal
import threading
import time

from msr import storage
from msr.deps import HAS_CURL_CFFI, HAS_WEBSOCKET
from msr.util import (
    PENDING_DELETION_FOLDER,
    channel_key_to_dirs,
    find_cookies_file,
    human_size,
    iter_channel_record_dirs,
    kill_orphan_recording_processes,
    kill_process_tree,
)
from msr.kick_realtime import (
    BROWSER_HEADERS, CENTRIFUGO_MAX_SUBS, cf_connect, cf_refresh, cf_subscribe,
    cf_unsubscribe, negotiate, new_client_id, parse_centrifugo_message, pusher_url,
)
from msr.worker import record_worker, remux_to_mp4, save_metadata

# ────────────────────────────────────────────────
#          Background Cleanup Thread
# ────────────────────────────────────────────────

# Guards _process_leftover_files() so that the per-channel cleanup thread
# (spawned by stop_channel) and the global background cleanup thread
# (spawned by stop()) never scan/remux the same .ts files concurrently.
_cleanup_lock = threading.Lock()


class BackgroundCleaner:
    """Handles remuxing and cleanup of leftover .ts files.

    Runs after recording stops (never during active recording) to remux
    raw .ts files to .mp4, save metadata sidecars, and manage the
    PendingDeletion folder.
    """

    def __init__(self, config, status_dict=None):
        self.config = config
        self.root_path = config.get('Paths', 'streams_dir')
        self.recorded_base = os.path.join(self.root_path, "Recorded")
        self.processed_base = os.path.join(self.root_path, "Processed")
        self.pending_base = os.path.join(self.root_path, PENDING_DELETION_FOLDER)
        # Optional reference to the recorder's status_dict so the cleaner can
        # skip .ts files whose worker is actively remuxing them (Bug #4 fix).
        self.status_dict = status_dict if status_dict is not None else {}
        self._thread = None
        self._stop_event = threading.Event()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="BackgroundCleaner")
        self._thread.start()
        logging.info("Background cleanup thread started")

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=15)
        logging.info("Background cleanup thread stopped")

    def _run(self):
        # Wait for file handles to be released and for any in-progress per-channel
        # remux threads (spawned by stop_channel) to finish before we start scanning.
        # We use a longer initial wait (30 s) then try to acquire the cleanup lock;
        # if another cleanup is still running we wait for it to finish (Bug #3 fix).
        logging.info("Cleanup: Waiting 30 seconds for in-flight remuxes and file handles to be released...")
        for _ in range(30):
            if self._stop_event.is_set():
                return
            time.sleep(1)

        # If a per-channel cleanup thread still holds the lock, wait for it.
        logging.info("Cleanup: Waiting for any concurrent cleanup pass to finish...")
        _cleanup_lock.acquire()
        _cleanup_lock.release()

        for pass_num in range(3):
            if self._stop_event.is_set():
                break
            found_locked = self._process_leftover_files()
            if not found_locked:
                break
            logging.info(f"Cleanup: Some files were locked, waiting 10s before retry (pass {pass_num + 1}/3)")
            for _ in range(10):
                if self._stop_event.is_set():
                    return
                time.sleep(1)

        logging.info("Background cleanup finished")

    def _process_leftover_files(self, lock_timeout=120):
        # Only one cleanup scan may run at a time across all threads.
        # Block-wait up to lock_timeout seconds so that when a per-channel
        # cleanup thread holds the lock, the _run retry loop actually gets to
        # run a second pass once the first finishes (rather than bailing out
        # immediately and marking the session as done with locked files).
        acquired = _cleanup_lock.acquire(blocking=True, timeout=lock_timeout)
        if not acquired:
            logging.warning(
                f"Cleanup: could not acquire lock after {lock_timeout}s — "
                "another cleanup pass is still running; skipping this pass"
            )
            return False

        try:
            return self._process_leftover_files_locked()
        finally:
            _cleanup_lock.release()

    def _process_leftover_files_locked(self):
        ffmpeg_path = self.config.get('Advanced', 'ffmpeg_path')
        ffmpeg_timeout = self.config.getint('Timeouts', 'ffmpeg_timeout')
        min_file_size_mb = self.config.getfloat('Recording', 'min_file_size_mb')

        found_any = False
        found_locked = False

        if not os.path.exists(self.recorded_base):
            return False

        # Bug #4 fix, corrected (v1.7.0): map every busy channel in
        # status_dict to its ON-DISK (platform, username_dir) using the same
        # derivation as record_worker, and skip those directories entirely.
        #
        # The previous guard reconstructed the key as f"{platform}:{username_dir}",
        # which never matched Kick channels (bare-name keys) or custom
        # channels (full-URL keys) — so actively-recording Kick/custom files
        # could be caught by a cleanup pass triggered from stopping a
        # DIFFERENT channel, remuxed as partials, and re-remuxed on every
        # subsequent pass.  Windows file locking prevented the raw file from
        # being moved (which is why no data was lost), but the partial MP4s
        # in Processed looked like finished recordings until shutdown.
        busy_dirs = set()
        for ch_key, st in list(self.status_dict.items()):
            if st.get("status") in ("Recording", "Remuxing...", "Processing..."):
                try:
                    busy_dirs.add(channel_key_to_dirs(ch_key))
                except Exception:
                    pass

        # Scan recorded_base dynamically so new platforms (tiktok, etc.)
        # are picked up automatically without needing a hardcoded list.
        # One extra nesting level covers custom/chaturbate/alice as well as
        # leftover .ts files still in the old custom/chaturbate bag.
        for platform, username_dir, username_path in iter_channel_record_dirs(self.recorded_base):
            processed_path = os.path.join(self.processed_base, platform, username_dir)
            pending_dir = os.path.join(self.pending_base, platform, username_dir)
            os.makedirs(processed_path, exist_ok=True)
            os.makedirs(pending_dir, exist_ok=True)

            # Bug #4 fix: if the worker for this channel is actively
            # recording, remuxing, or processing, its .ts may be open/
            # locked (or about to be handled by the worker itself).
            # Skip the entire channel directory — the worker moves its
            # own file to PendingDeletion once done.  See busy_dirs
            # construction above for why the match is done on
            # (platform, username_dir) rather than a reconstructed key.
            if (platform, username_dir) in busy_dirs:
                logging.info(
                    f"Cleanup: {platform}/{username_dir} has an active "
                    "worker (recording/remuxing) — skipping its "
                    "directory to avoid contention"
                )
                found_locked = True
                continue

            for filename in os.listdir(username_path):
                if not filename.endswith('.ts'):
                    continue

                raw_file = os.path.join(username_path, filename)

                # In-use probe 1: try to open the file for append.  ffmpeg
                # holds its output with a share mode that denies this, so
                # PermissionError is a reliable "still being written"
                # signal for ffmpeg-based recordings.
                try:
                    with open(raw_file, 'ab'):
                        pass
                except PermissionError:
                    logging.warning(f"Cleanup: {filename} is open by another process, skipping")
                    found_locked = True
                    continue
                except Exception:
                    pass  # transient error — fall through to the next probe

                # In-use probe 2: rename round-trip.  Python-based writers
                # (streamlink, yt-dlp) open with shared read/write, so the
                # append probe passes — but they don't set
                # FILE_SHARE_DELETE, so a rename fails while they hold the
                # file.  This catches an in-progress recording BEFORE we
                # waste a full remux on it, instead of discovering the
                # lock at the move-to-PendingDeletion step afterwards.
                probe_name = raw_file + ".cleanupprobe"
                try:
                    os.rename(raw_file, probe_name)
                    os.rename(probe_name, raw_file)
                except PermissionError:
                    logging.warning(
                        f"Cleanup: {filename} is locked by a writer "
                        "(rename probe) — likely still recording, skipping")
                    found_locked = True
                    continue
                except OSError:
                    # Rename failed for a non-lock reason; if the file got
                    # stuck under the probe name, restore it.
                    if os.path.exists(probe_name) and not os.path.exists(raw_file):
                        try:
                            os.rename(probe_name, raw_file)
                        except Exception:
                            pass
                    found_locked = True
                    continue

                try:
                    size1 = os.path.getsize(raw_file)
                    time.sleep(2)
                    size2 = os.path.getsize(raw_file)
                    if size2 != size1:
                        logging.warning(f"Cleanup: {filename} is still growing, skipping")
                        found_locked = True
                        continue
                    file_size = size2
                except Exception:
                    continue

                if file_size < min_file_size_mb * 1024 * 1024:
                    logging.info(f"Cleanup: Skipping {filename} — too small ({human_size(file_size)})")
                    try:
                        os.remove(raw_file)
                    except PermissionError:
                        found_locked = True
                    except Exception:
                        pass
                    continue

                found_any = True
                logging.info(f"Cleanup: Processing {filename} ({human_size(file_size)})")

                mp4_filename = filename.replace('.ts', '.mp4')
                mp4_file = os.path.join(processed_path, mp4_filename)

                # Only take the "already remuxed" shortcut if the existing
                # MP4 is plausibly complete.  A remux is roughly size-
                # preserving, so an MP4 much smaller than its .ts is a
                # stale partial (e.g. remuxed while the recording was
                # still in progress) and must be redone — remux_to_mp4
                # overwrites it (-y).
                existing_mp4_ok = False
                if os.path.exists(mp4_file):
                    try:
                        mp4_size = os.path.getsize(mp4_file)
                        existing_mp4_ok = (mp4_size > 5 * 1024**2
                                           and mp4_size >= 0.8 * file_size)
                        if mp4_size > 5 * 1024**2 and not existing_mp4_ok:
                            logging.warning(
                                f"Cleanup: existing MP4 for {filename} is only "
                                f"{human_size(mp4_size)} vs {human_size(file_size)} raw "
                                "— treating as stale partial, re-remuxing")
                    except Exception:
                        existing_mp4_ok = False

                if existing_mp4_ok:
                    logging.info(f"Cleanup: MP4 already exists for {filename}, just moving raw file")
                else:
                    file_size_gb = file_size / (1024**3)
                    scaled_timeout = max(ffmpeg_timeout, int(file_size_gb * 60) + 120)
                    success, mp4_size, error = remux_to_mp4(
                        raw_file, mp4_file, ffmpeg_path,
                        logging.getLogger(), scaled_timeout
                    )
                    if not success:
                        logging.error(f"Cleanup: Failed to process {filename}: {error}")
                        try:
                            pending_path = os.path.join(pending_dir, filename)
                            shutil.move(raw_file, pending_path)
                            logging.info(f"Cleanup: Moved unprocessable {filename} to PendingDeletion")
                        except Exception as move_err:
                            logging.warning(f"Cleanup: Could not move {filename} to PendingDeletion: {move_err}")
                        continue

                pending_path = os.path.join(pending_dir, filename)
                max_retries = 5
                moved = False
                for attempt in range(max_retries):
                    if self._stop_event.is_set():
                        return found_locked
                    try:
                        shutil.move(raw_file, pending_path)
                        logging.info(f"Cleanup: Successfully processed {mp4_filename}")
                        moved = True
                        break
                    except PermissionError:
                        if attempt < max_retries - 1:
                            wait_time = 3 * (attempt + 1)
                            logging.warning(f"Cleanup: File locked, retrying in {wait_time}s…")
                            time.sleep(wait_time)
                        else:
                            logging.warning(f"Cleanup: Could not move {filename} after {max_retries} attempts")
                            found_locked = True
                    except Exception as e:
                        logging.error(f"Cleanup: Failed to move {filename}: {e}")
                        break

        if found_any:
            logging.info("Cleanup: Pass complete")
        else:
            logging.info("Cleanup: No unprocessed .ts files found")

        return found_locked


def finish_headless_session(recorder, say=print):
    """Stop a headless session and wait for the cleanup that follows a stop, then release the Manager.

    A GUI session leaves the cleanup to a background thread while the window stays open. A
    headless process exits when this returns, so it must wait here or leftover recordings stay
    as raw .ts until the next start. Ctrl+C while waiting skips the rest (the files are picked up
    on the next start). Safe to call after ``stop()`` already ran.
    """
    recorder.stop()
    say("Recording stopped. Finishing cleanup of leftover files (Ctrl+C to skip)...")

    def _skip(signum, frame):
        raise KeyboardInterrupt

    previous = None
    try:
        previous = signal.signal(signal.SIGINT, _skip)  # the entry script ignores a second Ctrl+C
    except ValueError:  # not the main thread
        pass
    try:
        recorder.wait_for_cleanup()
    except KeyboardInterrupt:
        say("Cleanup skipped; leftover files are handled on the next start.")
    finally:
        if previous is not None:
            signal.signal(signal.SIGINT, previous)
    try:
        recorder.manager.shutdown()
    except Exception:
        pass


# ────────────────────────────────────────────────
#          Auto-Purge PendingDeletion
# ────────────────────────────────────────────────

def purge_old_pending_files(root_path, max_age_days, logger=None):
    """Delete files in PendingDeletion that are older than max_age_days.

    Returns the number of files deleted.

    Age is measured as time since the file was *moved* into PendingDeletion,
    not since it was originally recorded.  On Windows, shutil.move() preserves
    the source mtime, so we use max(mtime, ctime) — ctime reflects the last
    metadata change (i.e. the move) on Windows, giving the correct age.
    """
    if max_age_days <= 0:
        return 0

    pending_base = os.path.join(root_path, PENDING_DELETION_FOLDER)
    if not os.path.exists(pending_base):
        return 0

    if logger:
        logger.info(f"PendingDeletion purge: scanning for files older than {max_age_days} day(s)...")

    cutoff = time.time() - (max_age_days * 86400)
    deleted = 0
    skipped = 0
    empty_dirs = []

    for dirpath, dirnames, filenames in os.walk(pending_base, topdown=False):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            try:
                stat = os.stat(filepath)
                # Use the most recent of mtime and ctime.  On Windows, ctime is the
                # file-creation time (reset when the file is moved), so it correctly
                # reflects when the file landed in PendingDeletion rather than when
                # the original recording started.
                file_age_ts = max(stat.st_mtime, stat.st_ctime)
                if file_age_ts < cutoff:
                    os.remove(filepath)
                    deleted += 1
                    if logger:
                        logger.info(f"Purged: {filepath}")
                else:
                    skipped += 1
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to purge {filepath}: {e}")

        # Track empty directories for cleanup
        if not filenames and not dirnames and dirpath != pending_base:
            empty_dirs.append(dirpath)

    # Remove empty directories
    for d in empty_dirs:
        try:
            os.rmdir(d)
        except Exception:
            pass

    if logger:
        if deleted > 0:
            logger.info(f"PendingDeletion purge: deleted {deleted} file(s) older than {max_age_days} days ({skipped} retained)")
        else:
            logger.info(f"PendingDeletion purge: nothing to delete ({skipped} file(s) retained, not yet old enough)")

    return deleted


# ────────────────────────────────────────────────
#          StreamRecorder
# ────────────────────────────────────────────────

# ============ KICK PUSH NOTIFICATIONS ============
# The socket is negotiated on every (re)connect the same way kick.com's web
# client does it (msr/kick_realtime.py): Centrifugo today, Pusher with the
# returned app key/cluster as a fallback. Never hard-code a key here again
# (the old Pusher key died with error 4001 "App key ... not in this cluster").
KICK_LIVE_EVENT = "App\\Events\\StreamerIsLive"
KICK_STOP_EVENT = "App\\Events\\StopStreamBroadcast"
KICK_ID_CACHE_FILE = "kick_channel_ids.json"   # lives next to config.ini
PUSH_RECONNECT_BASE = 5        # seconds; doubles up to the cap below
PUSH_RECONNECT_MAX = 600       # ~10 min cap
PUSH_FAILURE_LOG_EVERY = 3600  # log connection failures at most hourly
PUSH_TOKEN_REFRESH_MARGIN = 300  # refresh the Centrifugo token 5 min early


def _safe_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


PUSH_RESUBSCRIBE_CHECK = 60     # seconds between "anything unsubscribed?" sweeps
PUSH_RETRY_MIN = 120            # first retry for a slug that failed to subscribe
PUSH_RETRY_MAX = 1800


def parse_kick_push_event(msg):
    """Normalise one Pusher frame (already json-decoded) from Kick.

    Returns ``{"kind": "live"|"stop"|"subscribed"|None, "channel_id": int|None,
    "viewers": int|None, "title": str, "since": str|None}``. Kick's
    StreamerIsLive payload normally has no viewer count; it is read when
    present (``viewer_count`` / ``viewers``) so newer payloads just work.
    """
    out = {"kind": None, "channel_id": None, "viewers": None, "title": "", "since": None}
    if not isinstance(msg, dict):
        return out
    event = msg.get("event", "")
    if event == "pusher_internal:subscription_succeeded":
        chan = str(msg.get("channel", ""))
        out["kind"] = "subscribed"
        out["channel_id"] = _safe_int(chan.rsplit(".", 1)[-1])
        return out
    if event not in (KICK_LIVE_EVENT, KICK_STOP_EVENT):
        return out
    raw = msg.get("data", "{}")
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (ValueError, TypeError):
        data = {}
    ls = data.get("livestream") if isinstance(data, dict) else None
    if not isinstance(ls, dict):
        ls = {}
    cid = ls.get("channel_id")
    if cid is None and isinstance(ls.get("channel"), dict):
        cid = ls["channel"].get("id")
    if cid is None:
        chan = str(msg.get("channel", ""))
        if chan.startswith("channel."):
            cid = chan.rsplit(".", 1)[-1]
    out["channel_id"] = _safe_int(cid)
    out["kind"] = "live" if event == KICK_LIVE_EVENT else "stop"
    if out["kind"] == "live":
        out["viewers"] = _safe_int(ls.get("viewer_count", ls.get("viewers")))
        out["title"] = ls.get("session_title") or ""
        out["since"] = ls.get("created_at") or ls.get("start_time")
    return out


class KickPushListener:
    """Single-connection Kick realtime listener (Centrifugo or Pusher) for
    go-live events.

    Two sets of slugs share one socket:

    * ``_wanted`` — channels with a recording worker. A StreamerIsLive event
      calls ``recorder.check_now(slug)``, the same wake path as the GUI's
      "Check Now" button. Polling is never disabled; push is additive.
    * ``_watch`` — every other Kick channel in the roster (disabled ones).
      Events only update the presence store (green dot / viewer count).
      They can NEVER wake or start a recorder.

    Runs as a daemon thread in the main process. It never records anything
    and never marks a stream as recording — the worker's normal streamlink
    check remains the source of truth for recording.

    Slugs that fail to subscribe (id lookup error, lost subscribe frame)
    are retried with back-off on a sweep thread instead of waiting for the
    next reconnect (that is why a channel like lettyvision could stay
    unsubscribed for a whole session).
    """

    def __init__(self, recorder, config, presence=None):
        self.recorder = recorder           # StreamRecorder instance (or None)
        self.config = config
        self.presence = presence           # msr.presence.PresenceStore or None
        self.logger = logging.getLogger()
        self._ws = None
        self._connected = False
        self._thread = None
        self._sweeper = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        # slug -> numeric channel_id (resolved lazily, cached to disk)
        self._ids: dict = {}
        # channel_id (int) -> slug, for routing incoming events
        self._id_to_slug: dict = {}
        # slugs with a recording worker (push wakes them)
        self._wanted: set = set()
        # presence-only slugs (never wake anything)
        self._watch: set = set()
        # slugs confirmed subscribed on the current connection
        self._subscribed: set = set()
        # retry bookkeeping for slugs that did not subscribe
        self._retry_at: dict = {}
        self._retry_gap: dict = {}
        # transport state (negotiated per connection)
        self._conn: dict = {}
        self._protocol = "centrifugo"
        self._cmd_id = 0
        self._cf_pending: dict = {}    # command id -> slug awaiting subscribe reply
        self._cf_channels: dict = {}   # slug -> subscribed centrifugo channel
        self._token_expires = 0.0
        self._healthy = False
        self._fail_logged = False
        self._last_fail_log = 0.0
        self._limit_logged = False
        self._cache_path = os.path.join(
            os.path.dirname(os.path.abspath(
                getattr(config, 'config_file', 'config.ini'))),
            KICK_ID_CACHE_FILE)
        self._load_id_cache()

    # ── public API ──

    def start(self):
        if not HAS_WEBSOCKET:
            self.logger.info(
                "Kick push: websocket-client not installed — polling only "
                "(pip install websocket-client to enable push notifications)")
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="kick-push-listener")
        self._thread.start()
        self._sweeper = threading.Thread(
            target=self._sweep_loop, daemon=True, name="kick-push-resubscribe")
        self._sweeper.start()

    def stop(self):
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def all_slugs(self):
        with self._lock:
            return set(self._wanted) | set(self._watch)

    def set_channels(self, kick_slugs, watch=None):
        """Update the recording set (and optionally the presence-only set).

        Subscribes/unsubscribes live if connected; otherwise the sets are
        picked up on the next connect."""
        new = set(kick_slugs)
        with self._lock:
            before = set(self._wanted) | set(self._watch)
            left_wanted = self._wanted - new
            self._wanted = new
            if watch is not None:
                self._watch = set(watch) - new
            else:
                self._watch -= new
            after = set(self._wanted) | set(self._watch)
        for slug in left_wanted:
            self._set_push_flag(slug, False)
        for slug in before - after:
            self._unsubscribe(slug)
        for slug in after - before:
            self._subscribe(slug)
        for slug in new & self._subscribed:
            self._set_push_flag(slug, True)

    def set_watch(self, slugs):
        """Presence-only slugs (everything in the roster that isn't recording)."""
        with self._lock:
            wanted = set(self._wanted)
        self.set_channels(wanted, watch=set(slugs))

    def pending_slugs(self, now=None):
        """Slugs we want but are not subscribed to and are due for a retry."""
        now = time.monotonic() if now is None else now
        with self._lock:
            want = set(self._wanted) | set(self._watch)
        return sorted(s for s in want - self._subscribed
                      if self._retry_at.get(s, 0) <= now)

    # ── connection loop ──

    def _log_failure(self, what):
        """Connection problems: one warning per hour, the rest at debug."""
        now = time.monotonic()
        if now - self._last_fail_log >= PUSH_FAILURE_LOG_EVERY or not self._fail_logged:
            self._fail_logged = True
            self._last_fail_log = now
            self.logger.warning(
                f"Kick push: {what} — polling continues; retrying with back-off "
                f"(further failures logged at most hourly)")
        else:
            self.logger.debug(f"Kick push: {what}")

    def _run(self):
        import websocket  # websocket-client (guarded by HAS_WEBSOCKET)
        backoff = PUSH_RECONNECT_BASE
        while not self._stop.is_set():
            self._subscribed.clear()
            self._retry_at.clear()
            self._retry_gap.clear()
            self._cf_pending.clear()
            self._cf_channels.clear()
            self._healthy = False
            try:
                conn = self._negotiate(new_client_id())
            except Exception as e:
                conn = None
                self._log_failure(f"realtime negotiation failed ({e})")
            if conn is not None:
                self._conn = conn
                self._protocol = conn["provider"]
                url = (conn["url"] if self._protocol == "centrifugo"
                       else pusher_url(conn["app_key"], conn["cluster"]))
                try:
                    self._ws = websocket.WebSocketApp(
                        url,
                        header=[f"User-Agent: {BROWSER_HEADERS['user-agent']}"],
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=lambda w, e: self._log_failure(f"socket error: {e}"),
                        on_close=lambda w, c, r: self.logger.info(
                            f"Kick push: socket closed ({c})") if self._healthy else None,
                    )
                    # ping keeps proxies from dropping the idle connection;
                    # Centrifugo's own app-level ping ({}) is answered too.
                    self._ws.run_forever(ping_interval=30, ping_timeout=10,
                                         origin="https://kick.com")
                except Exception as e:
                    self._log_failure(f"connection failed ({e})")
                finally:
                    self._connected = False
                    self._ws = None
                    # Connection is gone — clear every push flag so the workers'
                    # status line stops advertising push coverage.
                    for slug in list(self._subscribed) or list(self._wanted):
                        self._set_push_flag(slug, False)
                    self._subscribed.clear()

            if self._stop.is_set():
                break
            # A connection that got as far as subscribing was healthy.
            if self._healthy:
                backoff = PUSH_RECONNECT_BASE
                self._fail_logged = False
            self.logger.debug(f"Kick push: reconnecting in {backoff}s")
            if self._stop.wait(backoff):
                break
            backoff = min(backoff * 2, PUSH_RECONNECT_MAX)

    def _on_open(self, ws):
        if self._protocol == "centrifugo":
            # Subscriptions wait for the connect reply.
            try:
                ws.send(cf_connect(self._next_cmd_id(), self._conn.get("token", "")))
            except Exception as e:
                self._log_failure(f"connect send failed ({e})")
            return
        self._on_ready()

    def _on_ready(self):
        self._connected = True
        self._healthy = True
        with self._lock:
            wanted = sorted(self._wanted) + sorted(self._watch - self._wanted)
        self.logger.info(
            f"Kick push: connected ({self._protocol}) — subscribing {len(wanted)} channel(s)")
        for slug in wanted:
            self._subscribe(slug)

    def _next_cmd_id(self):
        self._cmd_id += 1
        return self._cmd_id

    def _negotiate(self, client_id):
        return negotiate(client_id)

    def _maybe_refresh_token(self):
        """Centrifugo guest tokens expire (~45 min); refresh before that."""
        if self._protocol != "centrifugo" or not self._connected or not self._token_expires:
            return
        if time.monotonic() < self._token_expires - PUSH_TOKEN_REFRESH_MARGIN:
            return
        ws = self._ws
        try:
            from msr.kick_realtime import AUTH_CONNECTION_PATH, http_post_json, parse_token_response
            tok = parse_token_response(http_post_json(
                AUTH_CONNECTION_PATH, {"client_id": self._conn.get("client_id")}))
            if not tok:
                raise RuntimeError("no token")
            self._conn["token"] = tok
            self._token_expires = 0  # set again from the refresh reply
            ws.send(cf_refresh(self._next_cmd_id(), tok))
        except Exception as e:
            self._log_failure(f"token refresh failed ({e}); reconnecting")
            try:
                ws.close()
            except Exception:
                pass

    def _sweep_loop(self):
        """Retry slugs that never confirmed (failed id lookup / lost frame)."""
        while not self._stop.wait(PUSH_RESUBSCRIBE_CHECK):
            if not self._connected or self._ws is None:
                continue
            self._maybe_refresh_token()
            for slug in self.pending_slugs():
                if self._stop.is_set():
                    return
                self._subscribe(slug)

    def _note_attempt(self, slug):
        gap = self._retry_gap.get(slug, PUSH_RETRY_MIN // 2) * 2
        gap = max(PUSH_RETRY_MIN, min(gap, PUSH_RETRY_MAX))
        self._retry_gap[slug] = gap
        self._retry_at[slug] = time.monotonic() + gap

    def _on_message(self, ws, raw):
        if self._protocol == "centrifugo":
            self._on_centrifugo_message(ws, raw)
            return
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return
        if msg.get("event") == "pusher:error":
            self._log_failure(f"pusher error {str(msg.get('data'))[:120]}")
            return
        ev = parse_kick_push_event(msg)
        kind = ev["kind"]
        if kind is None:
            return
        slug = self._id_to_slug.get(ev["channel_id"])
        if not slug:
            return
        if kind == "subscribed":
            self._mark_subscribed(slug, ev["channel_id"])
            return
        self._handle_live_event(slug, kind, ev)

    def _on_centrifugo_message(self, ws, raw):
        for fr in parse_centrifugo_message(raw):
            t = fr["type"]
            if t == "ping":
                try:
                    ws.send("{}")
                except Exception:
                    pass
            elif t == "reply":
                if fr["kind"] in ("connect", "refresh"):
                    ttl = (fr.get("result") or {}).get("ttl")
                    if ttl:
                        self._token_expires = time.monotonic() + float(ttl)
                    if fr["kind"] == "connect":
                        self._on_ready()
                elif fr["kind"] == "subscribe":
                    slug = self._cf_pending.pop(fr["id"], None)
                    if slug:
                        cid = self._ids.get(slug)
                        self._cf_channels[slug] = f"channel.{cid}"
                        self._mark_subscribed(slug, cid)
            elif t == "error":
                slug = self._cf_pending.pop(fr.get("id"), None)
                what = f"{slug}: " if slug else ""
                if fr.get("code") == 106 and not self._limit_logged:
                    self._limit_logged = True
                    self.logger.warning(
                        f"Kick push: subscription limit reached ({what}error 106) — "
                        f"remaining channels rely on polling")
                elif slug:
                    self.logger.info(f"Kick push: subscribe {what}error {fr.get('code')} "
                                     f"{fr.get('message')}")
                else:
                    self._log_failure(f"server error {fr.get('code')} {fr.get('message')}")
            elif t == "pub":
                ev = parse_kick_push_event(fr["msg"])
                if ev["kind"] in ("live", "stop"):
                    slug = self._id_to_slug.get(ev["channel_id"])
                    if slug:
                        self._handle_live_event(slug, ev["kind"], ev)
            elif t == "disconnect":
                self._log_failure(f"server disconnect {fr.get('code')} {fr.get('reason')}")

    def _mark_subscribed(self, slug, cid):
        self._subscribed.add(slug)
        self._retry_at.pop(slug, None)
        self._retry_gap.pop(slug, None)
        with self._lock:
            recording = slug in self._wanted
        if recording:
            self._set_push_flag(slug, True)
        self.logger.info(
            f"Kick push: listening for {slug} (channel.{cid})"
            + ("" if recording else " — presence only"))

    def _handle_live_event(self, slug, kind, ev):
        """Presence for every slug; a recorder wake ONLY for recording slugs."""
        if self.presence is not None:
            try:
                if kind == "live":
                    self.presence.update(slug, True, ev.get("viewers"), ev.get("since"),
                                         source="push")
                else:
                    self.presence.update(slug, False, source="push")
            except Exception:
                pass
        if kind != "live":
            # Informational only — the recording process notices the stream
            # ending on its own (stall detector). A push event must never be
            # able to stop or kill a recording.
            return
        with self._lock:
            recording = slug in self._wanted
        if not recording:
            self.logger.info(f"Kick push: {slug} went LIVE (disabled — presence only)")
            return
        self.logger.info(
            f"Kick push: {slug} went LIVE ({ev.get('title')!r}) — waking worker")
        if self.recorder is not None:
            self.recorder.check_now(slug)

    # ── subscription helpers ──

    def _subscribe(self, slug):
        ws = self._ws
        if ws is None:
            return  # not connected — _on_open will subscribe everything wanted
        self._note_attempt(slug)
        cid = self._resolve_channel_id(slug)
        if cid is None:
            self.logger.info(
                f"Kick push: no channel id for {slug} — polling only, "
                f"retry in {self._retry_gap.get(slug, PUSH_RETRY_MIN)}s")
            return
        self._id_to_slug[cid] = slug
        try:
            if self._protocol == "centrifugo":
                if slug in self._cf_pending.values():
                    return  # still waiting for the reply
                if len(self._cf_channels) + len(self._cf_pending) >= CENTRIFUGO_MAX_SUBS:
                    if not self._limit_logged:
                        self._limit_logged = True
                        self.logger.warning(
                            f"Kick push: more than {CENTRIFUGO_MAX_SUBS} Kick channels — "
                            f"the rest rely on polling")
                    return
                cmd = self._next_cmd_id()
                self._cf_pending[cmd] = slug
                ws.send(cf_subscribe(cmd, f"channel.{cid}"))
                return
            ws.send(json.dumps({
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"channel.{cid}"},
            }))
        except Exception as e:
            self.logger.warning(f"Kick push: subscribe failed for {slug}: {e}")

    def _unsubscribe(self, slug):
        ws = self._ws
        cid = self._ids.get(slug)
        self._subscribed.discard(slug)
        self._retry_at.pop(slug, None)
        self._retry_gap.pop(slug, None)
        chan = self._cf_channels.pop(slug, None)
        if ws is None or cid is None:
            return
        try:
            if self._protocol == "centrifugo":
                if chan:
                    ws.send(cf_unsubscribe(self._next_cmd_id(), chan))
                return
            ws.send(json.dumps({
                "event": "pusher:unsubscribe",
                "data": {"channel": f"channel.{cid}"},
            }))
        except Exception:
            pass

    # ── channel id resolution + cache ──

    def _resolve_channel_id(self, slug):
        cid = self._ids.get(slug)
        if cid is not None:
            return cid
        if not HAS_CURL_CFFI:
            self.logger.info(
                "Kick push: curl_cffi not installed — cannot resolve "
                f"channel id for {slug} (pip install curl_cffi)")
            return None
        from msr.presence import fetch_kick_channel, parse_kick_channel_json
        try:
            data = fetch_kick_channel(slug)
            live, viewers, since, cid = parse_kick_channel_json(data)
        except Exception as e:
            self.logger.warning(f"Kick push: id lookup failed for {slug}: {e}")
            return None
        if cid is None:
            self.logger.warning(f"Kick push: id lookup failed for {slug}")
            return None
        if self.presence is not None and live is not None:
            try:
                self.presence.update(slug, live, viewers, since, source="poll")
            except Exception:
                pass
        self._ids[slug] = cid
        self._save_id_cache()
        return cid

    def _load_id_cache(self):
        try:
            with open(self._cache_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            self._ids = {k: int(v) for k, v in raw.items() if _safe_int(v) is not None}
            self._id_to_slug = {v: k for k, v in self._ids.items()}
        except (OSError, ValueError, TypeError, AttributeError):
            self._ids = {}

    def _save_id_cache(self):
        try:
            with open(self._cache_path, "w", encoding="utf-8") as f:
                json.dump(self._ids, f, indent=2)
        except OSError as e:
            storage.write_failed("the Kick channel id cache", self._cache_path, e)

    # ── shared-state plumbing ──

    def _set_push_flag(self, slug, value):
        """Expose per-channel push coverage to workers via the shared runtime
        dict (same live-update mechanism as poll_interval_minutes)."""
        if self.recorder is None:
            return
        try:
            self.recorder.runtime[f"kick_push:{slug}"] = bool(value)
        except Exception:
            pass  # Manager may be shutting down


class StreamRecorder:
    """Main recorder that manages worker processes."""

    # Recordings folder watch state (see _storage_tick); defaults so a bare instance works too.
    fallback_root = None
    fallback_recorded_base = None
    _storage_state = "ok"          # "ok", "fallback" (main folder gone, fallback in use) or "down"
    _storage_notice = ""
    _storage_next_check = 0.0
    _fallback_next_scan = 0.0
    _fallback_thread = None

    def __init__(self, channels, config, presence=None, watch_channels=None):
        self.channels = channels
        self.config = config
        # Presence-only roster keys (disabled channels). Never recorded.
        self.presence = presence
        self.watch_channels = list(watch_channels or [])

        self.root_path = config.get('Paths', 'streams_dir')
        self.recorded_base = os.path.join(self.root_path, "Recorded")
        self.processed_base = os.path.join(self.root_path, "Processed")
        self.pending_base = os.path.join(self.root_path, PENDING_DELETION_FOLDER)

        # An unreachable drive must not stop the session from starting: the workers record into the
        # fallback folder meanwhile (see msr/storage.py) and the watch in run() reports it.
        try:
            os.makedirs(self.recorded_base, exist_ok=True)
            os.makedirs(self.processed_base, exist_ok=True)
            os.makedirs(self.pending_base, exist_ok=True)
        except OSError as e:
            logging.warning(f"Recordings folder {self.root_path} is not available right now "
                            f"({storage.REASONS[storage.classify(e)]}); it is checked again every few seconds")

        # Recordings folder watch (a drive that is unplugged while we run).
        self.fallback_root = storage.fallback_root(config.get('Paths', 'fallback_dir', fallback=''), self.root_path)
        self.fallback_recorded_base = (os.path.join(self.fallback_root, "Recorded")
                                       if self.fallback_root else None)

        self.manager = mp.Manager()
        self.status_queue = self.manager.Queue()
        self.status_dict = {}

        # Shared runtime settings that workers re-read every poll cycle.
        # Lets the GUI change the poll interval live, without a restart.
        self.runtime = self.manager.dict()
        self.runtime['poll_interval_minutes'] = config.getfloat(
            'Timeouts', 'poll_interval_minutes', fallback=3.0)

        # Per-channel wake events.  Setting one snaps that worker out of its
        # offline/error sleep so it checks the stream immediately ("Check Now").
        # Events are reused across worker respawns for the same channel.
        self.wake_events: dict = {}  # {channel_name: mp.Event}

        for ch in channels:
            self.status_dict[ch] = {"status": "Initializing", "detail": "", "size": "", "time": "", "progress": 0}

        # Maps channel_name -> list[mp.Process].  A channel may temporarily have
        # more than one entry while the old process is dying and the new one is
        # starting; the list is pruned of dead entries on every monitor tick.
        self.processes: dict = {}  # {channel_name: [mp.Process, ...]}
        self.should_stop = mp.Event()
        self.is_running = False
        self.stopped_channels = set()  # channels individually stopped by user
        # Guards against spawning a second worker for a channel while start_channel
        # is already in progress for that same channel (e.g. rapid double-click).
        self._spawning: set = set()
        self._spawn_lock = threading.Lock()
        self.cleaner = BackgroundCleaner(config, status_dict=self.status_dict)
        # Startup leftover cleanup runs on this thread AFTER the recorders are
        # launched (see _plan_startup_cleanup / _launch_startup_cleanup).
        # _startup_cleanup_cancel aborts it (Stop / close): the in-flight
        # remux is killed and the .ts is left for the next pass.
        self._startup_cleanup_thread = None
        self._startup_cleanup_cancel = threading.Event()
        # Pause between the two size samples of the "still growing?" check.
        self._startup_settle_seconds = 1.0
        # Kick push notifications: one Pusher socket that wakes workers the
        # instant a Kick channel goes live (supplements polling, never
        # replaces it).  No-op if websocket-client isn't installed.
        self.kick_push = KickPushListener(self, config, presence=presence)

    def _sync_kick_push(self):
        """Align the push listener's subscriptions with the currently active
        Kick channels (bare-name channel keys, minus individually stopped
        ones).  Called on session start and on per-channel start/stop."""
        try:
            tracked = set(self.channels) | set(self.processes.keys())
            kick_slugs = [ch for ch in tracked
                          if ":" not in ch and ch not in self.stopped_channels]
            # Presence-only: disabled roster channels plus individually
            # stopped ones (still shown live/offline, never woken).
            watch = [ch for ch in self.watch_channels if ":" not in ch]
            watch += [ch for ch in tracked
                      if ":" not in ch and ch in self.stopped_channels]
            self.kick_push.set_channels(kick_slugs, watch=watch)
        except Exception as e:
            logging.warning(f"Kick push: subscription sync failed: {e}")

    def set_watch_channels(self, keys):
        """Replace the presence-only roster (GUI calls this when the roster
        changes). Watch-only channels are never recorded or woken."""
        self.watch_channels = list(keys or [])
        if self.is_running:
            self._sync_kick_push()

    def _get_wake_event(self, channel_name):
        """Return the wake event for a channel, creating it on first use.

        The same event object is reused if the worker is respawned, so a
        pending 'Check Now' survives a crash-restart.
        """
        ev = self.wake_events.get(channel_name)
        if ev is None:
            ev = mp.Event()
            self.wake_events[channel_name] = ev
        return ev

    def check_now(self, channel_name):
        """Wake a single channel's worker so it checks the stream immediately."""
        if not self.is_running or channel_name in self.stopped_channels:
            return
        ev = self.wake_events.get(channel_name)
        if ev is not None:
            ev.set()
            logging.info(f"Check Now: waking worker for {channel_name}")

    def check_all_now(self):
        """Wake every active (non-stopped) worker for an immediate check.

        Wakes are spread over a few seconds so all channels don't hit their
        platforms in the same instant (same reasoning as the startup stagger).
        """
        if not self.is_running:
            return
        targets = [ch for ch in self.wake_events
                   if ch not in self.stopped_channels]
        if not targets:
            return
        logging.info(f"Check All Now: waking {len(targets)} worker(s)")

        def _staggered_wake(channels):
            random.shuffle(channels)
            for i, ch in enumerate(channels):
                ev = self.wake_events.get(ch)
                if ev is not None:
                    ev.set()
                if i < len(channels) - 1:
                    time.sleep(random.uniform(0.3, 1.2))

        threading.Thread(target=_staggered_wake, args=(targets,),
                         daemon=True, name="check-all-now").start()

    def set_poll_interval(self, minutes):
        """Update the poll interval live.  Sleeping workers pick up the new
        value on their next cycle; we also wake them (staggered) so a change
        like Relaxed→Fast takes effect immediately instead of after up to
        five more minutes of the old interval.
        """
        try:
            self.runtime['poll_interval_minutes'] = float(minutes)
        except Exception:
            return
        logging.info(f"Runtime poll interval set to {minutes} min")
        self.check_all_now()

    def update_status_from_queue(self):
        while True:
            try:
                # empty() talks to the Manager process: if that is gone it raises (EOFError,
                # BrokenPipeError...), and this runs from the supervisor loop and the GUI timer.
                if self.status_queue.empty():
                    break
                ch, new_status = self.status_queue.get_nowait()
                if ch not in self.status_dict:
                    continue
                # Ignore stale updates from a killed worker: stop_channel sets
                # the status to 'Stopped', but the worker may have queued a
                # 'Recording'/'Checking...' update just before it died.
                # Applying it would flip the dead channel back to 'Recording',
                # which both misleads the GUI and makes the cleanup busy-dir
                # guard skip the channel's directory until session end.
                if ch in self.stopped_channels:
                    continue
                self.status_dict[ch] = new_status
            except Exception as e:
                if not isinstance(e, queue.Empty):
                    self._log_tick_problem("status updates", e)
                break

    def stop_channel(self, channel_name):
        """Stop ALL worker processes for a channel and trigger cleanup for its files.

        The channel is marked as individually stopped so refresh_status can show
        'Stopped' instead of 'Offline', and so the master Stop doesn't double-kill it.

        Important: a channel may have more than one live process if start_channel was
        called while the old process was still alive (e.g. rapid stop/restart).  We
        kill every one of them so no ghost workers keep running after a 'stop'.
        """
        if not self.is_running:
            return

        # Collect recording info before killing (from any still-running process)
        st = self.status_dict.get(channel_name, {})
        size_str = st.get("size", "")
        time_str = st.get("time", "")

        # Find every process registered for this channel
        procs = self.processes.get(channel_name, [])
        alive_procs = [p for p in procs if p.is_alive()]

        if not alive_procs:
            logging.info(f"Channel {channel_name} is not actively running")
            self.status_dict[channel_name] = {
                "status": "Stopped", "detail": "by user", "size": "", "time": "", "progress": 0
            }
            # Still mark as stopped so the monitor loop doesn't auto-restart it
            self.stopped_channels.add(channel_name)
            # Drain any lingering dead processes from the list
            self.processes[channel_name] = []
            self._sync_kick_push()
            return

        # Mark it stopped BEFORE the kill: the supervisor must not see the dead worker in between
        # and restart it.
        self.stopped_channels.add(channel_name)
        # Kill them all
        for proc in alive_procs:
            logging.info(f"Stopping channel {channel_name} (PID {proc.pid})")
            self._mark_killed(proc)
            kill_process_tree(proc.pid)

        for proc in alive_procs:
            proc.join(timeout=10)
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=5)

        # Log summary for this channel
        if size_str and time_str:
            logging.info(f"Channel stopped — {channel_name}: {size_str}, {time_str}")
        else:
            logging.info(f"Channel stopped — {channel_name}: no active recording")

        # Update status to Stopped and mark as intentionally stopped
        self.stopped_channels.add(channel_name)
        self.processes[channel_name] = []
        self.status_dict[channel_name] = {
            "status": "Stopped", "detail": "by user", "size": "", "time": "", "progress": 0
        }

        # Drop the push subscription for this channel (Kick channels only —
        # set_channels handles the filtering)
        self._sync_kick_push()

        # Run cleanup for this channel's files in background.
        # We wait long enough for any in-flight remux in the killed worker process
        # to either finish or be abandoned before scanning for leftover .ts files
        # (Bug #3 fix — the old 5 s wait was insufficient for multi-GB remuxes).
        def _channel_cleanup():
            time.sleep(30)  # generous wait for large-file remux / file handle release
            self.cleaner._process_leftover_files()
            logging.info(f"Cleanup finished for {channel_name}")

        threading.Thread(target=_channel_cleanup, daemon=True,
                         name=f"cleanup-{channel_name}").start()

    def remove_channel(self, channel_name):
        """Stop any live worker and drop the channel from the current session.

        Used when the user deletes a channel from the roster.  Remove used to
        only edit the GUI list, which left ffmpeg/yt-dlp running as a ghost.
        """
        if self.is_running:
            logging.info(f"Removing channel {channel_name} — stopping worker")
            # Mark stopped first so the monitor loop cannot restart the worker
            # between kill and bookkeeping teardown.
            self.stopped_channels.add(channel_name)
            self.stop_channel(channel_name)
        with self._spawn_lock:
            self.processes.pop(channel_name, None)
        self.wake_events.pop(channel_name, None)
        if channel_name in self.status_dict:
            del self.status_dict[channel_name]
        try:
            self.channels.remove(channel_name)
        except (ValueError, AttributeError):
            pass
        # stop_channel already synced push while the channel was still in
        # processes; sync again now that it is gone so Kick unsubscribes.
        if self.is_running:
            self._sync_kick_push()

    def start_channel(self, channel_name):
        """Start (or restart) a single channel's worker while other channels continue.

        Can be used to restart a channel that was individually stopped, or to add
        a new channel mid-session.

        Before spawning a new worker this method kills any existing processes for
        the channel so we never end up with two workers racing on the same files.
        A per-channel spawn guard prevents a second call from racing through before
        the first one has registered its new process.
        """
        if not self.is_running:
            logging.warning("Cannot start channel — no active recording session")
            return

        # Prevent concurrent start_channel calls for the same channel
        with self._spawn_lock:
            if channel_name in self._spawning:
                logging.info(f"start_channel({channel_name}): already in progress, ignoring duplicate call")
                return
            self._spawning.add(channel_name)

        try:
            # Kill every currently-registered process for this channel before spawning
            # a new one.  This prevents duplicate workers and avoids the situation
            # where the monitor loop kills our old proc, sees exit-code 15 (SIGTERM),
            # and also spawns a replacement — resulting in two workers at once.
            existing = self.processes.get(channel_name, [])
            alive_existing = [p for p in existing if p.is_alive()]
            if alive_existing:
                logging.info(
                    f"start_channel({channel_name}): killing {len(alive_existing)} "
                    f"existing worker(s) before spawning replacement"
                )
                for proc in alive_existing:
                    self._mark_killed(proc)
                    kill_process_tree(proc.pid)
                for proc in alive_existing:
                    proc.join(timeout=8)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(timeout=3)

            # Clear the process list for this channel — monitor loop won't restart
            # dead procs while the channel is in self._spawning.
            self.processes[channel_name] = []

            logging.info(f"Starting channel {channel_name} mid-session")

            # Clear the individually-stopped flag (and the crash history: this is a deliberate start)
            self.stopped_channels.discard(channel_name)
            sv = self._supervision()
            sv["fast"].pop(channel_name, None)
            sv["restart_at"].pop(channel_name, None)

            # Build config dict the same way run() does
            config_dict = {section: dict(self.config.config.items(section))
                           for section in self.config.config.sections()}
            cookies_file = find_cookies_file(self.config)
            if cookies_file:
                config_dict.setdefault('Paths', {})['cookies_file'] = cookies_file

            # Initialize status
            self.status_dict[channel_name] = {
                "status": "Initializing", "detail": "", "size": "", "time": "", "progress": 0
            }

            # Spawn exactly one worker
            proc = self._spawn_worker(channel_name, config_dict)
            self.processes[channel_name] = [proc]
            logging.info(f"Started process for {channel_name} (PID {proc.pid})")

            # Re-subscribe push for this channel (covers restart of a stopped
            # channel and brand-new channels added mid-session)
            self._sync_kick_push()

        finally:
            with self._spawn_lock:
                self._spawning.discard(channel_name)

    def stop(self):
        if not self.is_running:
            return
        self.is_running = False
        logging.info("Stop requested — shutting down processes...")
        self.should_stop.set()
        # Abort a startup leftover cleanup that is still running in the
        # background; its .ts stays put and BackgroundCleaner (below) or the
        # next start picks it up.
        self._startup_cleanup_cancel.set()

        # Stop the Kick push listener first — no point waking workers that
        # are about to be killed
        try:
            self.kick_push.stop()
        except Exception:
            pass

        # Collect all alive processes across every channel
        pids_to_kill = []
        for ch, procs in self.processes.items():
            for proc in procs:
                if proc.is_alive():
                    pids_to_kill.append((ch, proc.pid))

        for ch, pid in pids_to_kill:
            logging.info(f"Killing process tree for {ch} (PID {pid})")
            self._supervision()["killed"].add(pid)
            kill_process_tree(pid)

        for ch, procs in self.processes.items():
            for proc in procs:
                proc.join(timeout=10)
                if proc.is_alive():
                    logging.warning(f"Process {ch} (PID {proc.pid}) did not terminate, force killing...")
                    proc.kill()
                    proc.join(timeout=5)

        self.processes = {}

        startup_thread = self._startup_cleanup_thread
        if startup_thread is not None and startup_thread.is_alive():
            startup_thread.join(timeout=10)
            if startup_thread.is_alive():
                logging.warning("Startup cleanup: still stopping - leaving its remaining .ts files for the next pass")

        logging.info("Checking for orphaned recording processes...")
        kill_orphan_recording_processes(logging.getLogger(), streams_dir=self._orphan_roots())

        # All workers are dead now — normalize any stale 'Recording'/'Remuxing'
        # statuses so the busy-directory guard in BackgroundCleaner doesn't
        # skip these channels' directories forever (their files are exactly
        # what the post-stop cleanup needs to process).
        for ch in list(self.status_dict.keys()):
            if self.status_dict.get(ch, {}).get("status") in (
                    "Recording", "Remuxing...", "Processing...", "Checking..."):
                self.status_dict[ch] = {
                    "status": "Stopped", "detail": "session ended",
                    "size": "", "time": "", "progress": 0,
                }

        # Start background cleanup (safe now — no recording processes running)
        self.cleaner.start()
        logging.info("All processes stopped (cleanup running in background)")

    def shutdown(self):
        """Full shutdown: stop recording, wait for cleanup, shut down Manager."""
        self.stop()
        # Wait for background cleanup to finish
        if self.cleaner._thread and self.cleaner._thread.is_alive():
            self.cleaner.stop()
        # Shut down the multiprocessing Manager server process
        try:
            self.manager.shutdown()
        except Exception:
            pass

    def wait_for_cleanup(self, timeout=None):
        """Block until the post-stop cleanup (remux of leftover .ts files) has finished.

        ``stop()`` starts that cleanup on a daemon thread. A GUI keeps running, so it finishes;
        a program that exits right after ``stop()`` (headless) would cut it off, so it calls
        this first. Unlike ``shutdown()`` this does not cancel the cleanup. Returns True if
        the cleanup is finished (or never ran), False if ``timeout`` ran out.
        """
        thread = self.cleaner._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout)
            return not thread.is_alive()
        return True

    # ------------------------------------------------------------------
    # Recordings folder watch
    #
    # Workers pick the folder themselves before every new recording (storage.choose_root). This
    # side only reports the state once (log + GUI line) and, when the main folder is back, moves
    # the footage recorded in the fallback folder into it.
    # ------------------------------------------------------------------

    STORAGE_CHECK_SECONDS = 10
    FALLBACK_SCAN_SECONDS = 300

    def _orphan_roots(self):
        """The folders whose leftover workers are ours to kill: the main one and the fallback."""
        return [r for r in (self.root_path, self.fallback_root) if r]

    def storage_notice(self):
        """A one-line warning for the GUI while the recordings folder is unreachable ('' = fine)."""
        return self._storage_notice

    def _storage_tick(self, force=False):
        now = time.monotonic()
        if not force and now < self._storage_next_check:
            return
        self._storage_next_check = now + self.STORAGE_CHECK_SECONDS
        min_gb = self.config.getfloat('Recording', 'min_disk_space_gb', fallback=5.0)
        choice = storage.choose_root(self.root_path, self.fallback_root, min_gb)
        if choice.ok and not choice.on_fallback or choice.low_space:
            state = "ok"
        elif choice.on_fallback:
            state = "fallback"
        else:
            state = "down"
        if state != self._storage_state:
            if state == "ok":
                logging.info(f"Recordings folder {self.root_path} is available again")
                self._storage_notice = ""
                self._start_fallback_return()
            elif state == "fallback":
                self._storage_notice = (f"Recordings folder not available ({choice.reason}). "
                                        f"Recording to {choice.path} until it returns.")
                logging.warning(self._storage_notice)
            else:
                self._storage_notice = (f"Recordings folder not available ({choice.reason}). "
                                        "Recording is paused until it returns.")
                logging.warning(f"{self._storage_notice} ({self.root_path})")
            self._storage_state = state
        elif state == "ok" and now >= self._fallback_next_scan:
            self._fallback_next_scan = now + self.FALLBACK_SCAN_SECONDS
            if self.fallback_root and storage.has_files(self.fallback_root):
                self._start_fallback_return()

    def _start_fallback_return(self):
        """Move the footage recorded in the fallback folder into the main folder (background thread)."""
        if not self.fallback_root:
            return
        thread = self._fallback_thread
        if thread is not None and thread.is_alive():
            return
        self._fallback_thread = threading.Thread(
            target=self._fallback_return_main, daemon=True, name="FallbackReturn")
        self._fallback_thread.start()

    def _fallback_return_main(self):
        try:
            if not storage.has_files(self.fallback_root):
                return
            logging.info(f"Fallback recordings: moving what was recorded in {self.fallback_root} "
                         f"back to {self.root_path}")
            result = storage.reconcile_fallback(self.fallback_root, self.root_path, logging.getLogger(),
                                                should_stop=self.should_stop)
            logging.info(f"Fallback recordings: {result.moved} file(s) moved back"
                         + (f", {result.young} still in use or too new (checked again later)" if result.young else "")
                         + (f", {result.failed} could not be moved (they stay in the fallback folder)"
                            if result.failed else ""))
            if result.moved_ts:
                # Unfinished raw recordings: the leftover pass turns them into MP4s like after a crash.
                self._launch_startup_cleanup(self._plan_startup_cleanup())
        except Exception:
            logging.exception("Fallback recordings: moving them back failed - the files stay in the "
                              "fallback folder and it is tried again later")

    # ------------------------------------------------------------------
    # Supervision: restart policy and a loop that survives its own errors
    # ------------------------------------------------------------------

    RESTART_FAST_SECONDS = 60       # a worker that ends sooner than this counts as a fast exit
    RESTART_BACKOFF_FIRST = 5       # delay after the first quick exit; doubles with each one in a row
    RESTART_BACKOFF_MAX = 300
    TICK_LOG_EVERY = 300            # the same loop problem is logged with a traceback this often

    def _supervision(self):
        """Per-instance supervision state (created on first use)."""
        state = self.__dict__.get("_sv_state")
        if state is None:
            state = self._sv_state = {
                "started": {},      # channel -> time.monotonic() of its last start
                "fast": {},         # channel -> consecutive fast exits
                "restart_at": {},   # channel -> time.monotonic() when it may start again
                "killed": set(),    # pids we ended on purpose (stop / restart of a channel)
                "problems": {},     # what -> [last logged, suppressed count]
            }
        return state

    def _mark_killed(self, proc):
        """Remember that WE are about to end this worker, so its exit is not mistaken for a crash."""
        pid = getattr(proc, "pid", None)
        if pid is not None:
            self._supervision()["killed"].add(pid)

    def _log_tick_problem(self, what, exc, now=None):
        """Log a problem of the supervisor loop: with a traceback the first time, then every 5 minutes."""
        now = time.monotonic() if now is None else now
        entry = self._supervision()["problems"].setdefault(what, [None, 0])
        if entry[0] is not None and now - entry[0] < self.TICK_LOG_EVERY:
            entry[1] += 1
            return
        more = f" ({entry[1]} more since the last message)" if entry[1] else ""
        logging.error(f"Supervisor: {what} failed: {type(exc).__name__}: {exc}{more} - "
                      "carrying on", exc_info=exc)
        entry[0], entry[1] = now, 0

    def _spawn_worker(self, ch, config_dict):
        worker_args = (ch, config_dict, self.should_stop, self.status_queue,
                       self._get_wake_event(ch), self.runtime)
        proc = mp.Process(target=record_worker, args=(worker_args,))
        proc.daemon = True
        proc.start()
        self._supervision()["started"][ch] = time.monotonic()
        return proc

    def _restart_delay(self, fast_exits):
        """Seconds to wait before the next start after ``fast_exits`` quick exits in a row."""
        if fast_exits <= 0:
            return 0
        return min(self.RESTART_BACKOFF_FIRST * 2 ** (fast_exits - 1), self.RESTART_BACKOFF_MAX)

    def _schedule_restart(self, ch, now, exit_code=None, reason=None):
        """Count a worker that is gone and decide when it may start again."""
        sv = self._supervision()
        ran = now - sv["started"].get(ch, now - self.RESTART_FAST_SECONDS)
        fast = sv["fast"].get(ch, 0) + 1 if ran < self.RESTART_FAST_SECONDS else 0
        sv["fast"][ch] = fast
        delay = self._restart_delay(fast)
        sv["restart_at"][ch] = now + delay
        what = reason or ("crashed" if exit_code else "exited")
        code = f" (exit code {exit_code})" if exit_code is not None else ""
        if fast <= 3 or fast % 10 == 0:
            note = f"; {fast} quick exits in a row" if fast > 1 else ""
            logging.warning(f"Process for {ch} {what}{code}{note} - restarting"
                            + (f" in {delay}s" if delay else ""))
        if delay and ch in self.status_dict and ch not in self.stopped_channels:
            self.status_dict[ch] = {"status": "Error", "detail": f"worker {what}, restart in {delay}s",
                                    "size": "", "time": "", "progress": 0}
        return delay

    def _supervise_channel(self, ch, config_dict, now):
        """One look at one channel: restart its worker if it is gone (and allowed to be)."""
        sv = self._supervision()
        if ch in self.stopped_channels:
            sv["restart_at"].pop(ch, None)
            return
        with self._spawn_lock:
            if ch in self._spawning:
                return
        procs = self.processes.get(ch, [])
        alive = [p for p in procs if p.is_alive()]
        self.processes[ch] = alive
        if alive:
            return
        if ch not in sv["restart_at"]:
            dead = [p for p in procs if not p.is_alive()]
            if not dead:
                return    # never started here (a start_channel in progress registers it itself)
            last_proc = dead[-1]
            if last_proc.pid in sv["killed"]:
                # We ended it (stop / restart of the channel): that is intentional. The exit code
                # says nothing: an outside kill (another instance's orphan check, a task manager)
                # looks identical (15 on Windows) and must be restarted.
                sv["killed"].discard(last_proc.pid)
                logging.info(f"Process for {ch} ended after we stopped it - not restarting")
                self.stopped_channels.add(ch)
                return
            code = last_proc.exitcode
            reason = ("was killed from outside" if code in (-15, 15, -9) else
                      "crashed" if code else "exited normally")
            self._schedule_restart(ch, now, code, reason)
        if now < sv["restart_at"][ch]:
            return
        del sv["restart_at"][ch]
        proc = self._spawn_worker(ch, config_dict)
        self.processes[ch] = [proc]
        logging.info(f"Restarted process for {ch} (PID {proc.pid})")

    def _supervise_failed(self, ch, exc, now):
        """Starting a worker raised (out of handles or memory...): retry with a growing delay."""
        self._log_tick_problem(f"restarting {ch}", exc, now)
        sv = self._supervision()
        sv["fast"][ch] = sv["fast"].get(ch, 0) + 1
        sv["restart_at"][ch] = now + max(self._restart_delay(sv["fast"][ch]), self.RESTART_BACKOFF_FIRST)

    def _supervise_tick(self, config_dict, now=None):
        """One pass of the supervisor loop. Nothing in here may end the loop."""
        now = time.monotonic() if now is None else now
        try:
            self.update_status_from_queue()
        except Exception as e:
            self._log_tick_problem("status updates", e, now)
        try:
            self._storage_tick()
        except Exception as e:
            self._log_tick_problem("recordings folder check", e, now)
        try:
            channels = list(self.processes.keys())
        except Exception as e:  # pragma: no cover - dict(copy) cannot realistically fail
            self._log_tick_problem("listing channels", e, now)
            return
        for ch in channels:
            if self.should_stop.is_set():
                break
            try:
                self._supervise_channel(ch, config_dict, now)
            except Exception as e:
                self._supervise_failed(ch, e, now)

    def run(self):
        if self.is_running:
            return
        # Ghost workers from a dead instance reconnect if we only kill ffmpeg.
        kill_orphan_recording_processes(
            logging.getLogger(), streams_dir=self._orphan_roots(),
        )
        self.is_running = True
        self.should_stop.clear()

        # Snapshot the leftover .ts files from previous sessions NOW (before
        # any recorder exists), but do not process them yet: recording must
        # start immediately, so the remuxing runs in the background after the
        # recorders are launched.  Returns the files to process later.
        startup_leftovers = self._plan_startup_cleanup()

        logging.info(f"Launching {len(self.channels)} recording processes")

        config_dict = {section: dict(self.config.config.items(section))
                       for section in self.config.config.sections()}

        # Resolve cookies file path and pass it through config
        cookies_file = find_cookies_file(self.config)
        if cookies_file:
            config_dict.setdefault('Paths', {})['cookies_file'] = cookies_file
            logging.info(f"Using cookies file: {cookies_file}")

        for ch in self.channels:
            proc = self._spawn_worker(ch, config_dict)
            self.processes[ch] = [proc]
            logging.info(f"Started process for {ch} (PID {proc.pid})")

        # Start the Kick push listener (harmless no-op without Kick channels
        # or without websocket-client installed)
        self._sync_kick_push()
        self.kick_push.start()

        # Recorders are up: now remux the leftovers (low priority, one file at
        # a time, in the background).
        self._launch_startup_cleanup(startup_leftovers)

        # Monitor processes — restart any that exit unexpectedly (see _supervise_channel for the
        # policy). A problem inside one pass is logged and the loop carries on: it used to end on
        # the first exception while the GUI kept showing "Recording".
        while self.is_running and not self.should_stop.is_set():
            try:
                self._supervise_tick(config_dict)
            except Exception as e:  # last safety net; the pieces are guarded individually
                self._log_tick_problem("supervisor pass", e)
            time.sleep(2)

        # Loop exited — stop() was already called or should_stop was set
        # Don't call stop() here to avoid double-stop race condition

    # ------------------------------------------------------------------
    # Startup cleanup of leftover .ts files from a previous session
    #
    # A crash / abrupt close leaves .ts files in Recorded/.  They are remuxed
    # to Processed/ and the raw file moved to PendingDeletion.  This used to
    # run synchronously before any recorder launched, so a few big leftovers
    # cost minutes of recording.  Now the files are SNAPSHOTTED before launch
    # and processed afterwards on a background thread (below-normal priority,
    # one file at a time).  Only snapshot files are ever touched, so a file
    # written by a freshly launched recorder is never picked up.
    # ------------------------------------------------------------------

    def _recording_names_are_unique(self):
        """True if new recordings always get a fresh, timestamped filename.

        Background cleanup relies on the new recorders never reusing a
        leftover's filename.  A user pattern without a timestamp (e.g.
        ``{username}``) could, so that case cleans up synchronously first.
        """
        try:
            pattern = self.config.get('Recording', 'filename_pattern',
                                      fallback='{username}_{timestamp}')
        except Exception:
            pattern = '{username}_{timestamp}'
        pattern = pattern or '{username}_{timestamp}'
        return '{timestamp}' in pattern or ('{date}' in pattern and '{time}' in pattern)

    def _snapshot_leftover_ts_files(self):
        """List the .ts files in Recorded/ right now (fast: no sizing/sleeping)."""
        entries = []
        if not os.path.exists(self.recorded_base):
            return entries
        # Scan recorded_base dynamically so new platforms are picked up
        # automatically without needing a hardcoded list.
        # One extra nesting level covers custom/chaturbate/alice.
        for platform, username_dir, username_path in iter_channel_record_dirs(self.recorded_base):
            processed_path = os.path.join(self.processed_base, platform, username_dir)
            pending_dir = os.path.join(self.pending_base, platform, username_dir)
            os.makedirs(processed_path, exist_ok=True)
            os.makedirs(pending_dir, exist_ok=True)
            try:
                names = os.listdir(username_path)
            except OSError:
                continue
            for filename in names:
                if filename.endswith('.ts'):
                    entries.append({
                        "raw_file": os.path.join(username_path, filename),
                        "processed_path": processed_path,
                        "pending_dir": pending_dir,
                    })
        return entries

    def _plan_startup_cleanup(self):
        """Snapshot leftovers before the recorders launch.

        Returns ``{"files": [...], "snapshot_time": float}`` to be processed
        in the background by ``_launch_startup_cleanup``, or an empty plan if
        there is nothing to do / it was already handled synchronously.
        """
        empty = {"files": [], "snapshot_time": time.time()}
        logging.info("Startup cleanup: checking for leftover .ts files from previous sessions...")

        # A previous session's background pass must not overlap this one.
        previous = self._startup_cleanup_thread
        if previous is not None and previous.is_alive():
            self._startup_cleanup_cancel.set()
            previous.join(timeout=30)
            if previous.is_alive():
                logging.warning(
                    "Startup cleanup: previous background pass is still running - "
                    "skipping this one (leftover files are handled when the session stops)")
                return empty
        self._startup_cleanup_cancel.clear()

        try:
            snapshot_time = time.time()
            files = self._snapshot_leftover_ts_files()
        except Exception as e:
            logging.warning(f"Startup cleanup: could not scan for leftover files: {e}")
            return empty
        plan = {"files": files, "snapshot_time": snapshot_time}
        if not files:
            logging.info("Startup cleanup: No leftover files found")
            return empty

        if not self._recording_names_are_unique():
            logging.warning(
                "Startup cleanup: filename_pattern has no timestamp, so a new recording "
                "could reuse a leftover's name - cleaning up BEFORE recording starts")
            self._process_startup_leftovers(plan, background=False)
            return empty

        logging.info(
            f"Startup cleanup: {len(files)} leftover .ts file(s) found - running in the "
            "background so recording starts now")
        return plan

    def _launch_startup_cleanup(self, plan):
        """Start the background pass for a plan from _plan_startup_cleanup."""
        if not plan or not plan.get("files"):
            return
        thread = threading.Thread(
            target=self._startup_cleanup_thread_main, args=(plan,),
            daemon=True, name="StartupCleanup")
        self._startup_cleanup_thread = thread
        thread.start()

    def _startup_cleanup_thread_main(self, plan):
        try:
            self._process_startup_leftovers(plan, background=True)
        except Exception:
            # Never let cleanup affect the recorders.
            logging.exception(
                "Startup cleanup: background pass failed - recording is unaffected; "
                "unprocessed .ts files stay for the next pass")

    def _acquire_cleanup_lock_or_cancel(self):
        """Wait for _cleanup_lock; give up (False) if the pass is cancelled."""
        while not self._startup_cleanup_cancel.is_set():
            if _cleanup_lock.acquire(timeout=1):
                return True
        return False

    def _process_startup_leftovers(self, plan, background):
        """Remux the snapshotted leftovers, one at a time.

        background=True: below-normal ffmpeg priority, serialized with the
        other cleanup passes via _cleanup_lock (per file), cancellable.
        background=False: the legacy synchronous behaviour.
        """
        processed_count = 0
        cancelled = False
        for entry in plan["files"]:
            if self._startup_cleanup_cancel.is_set():
                cancelled = True
                break
            if background:
                if not self._acquire_cleanup_lock_or_cancel():
                    cancelled = True
                    break
            try:
                result = self._process_one_startup_file(entry, plan["snapshot_time"], background)
            except Exception as e:
                logging.error(
                    f"Startup cleanup: unexpected error on {os.path.basename(entry['raw_file'])}: {e}")
                result = "error"
            finally:
                if background:
                    _cleanup_lock.release()
            if result == "done":
                processed_count += 1
            elif result == "cancelled":
                cancelled = True
                break

        if cancelled:
            logging.info(
                "Startup cleanup: stopped early - remaining .ts files are left in place "
                "for the next pass")
        if processed_count > 0:
            logging.info(f"Startup cleanup: Processed {processed_count} leftover file(s)")
        elif not cancelled:
            logging.info("Startup cleanup: No leftover files found")

    def _process_one_startup_file(self, entry, snapshot_time, background):
        """Process one snapshotted .ts.  Returns 'done', 'skipped',
        'cancelled' or 'error'."""
        ffmpeg_path = self.config.get('Advanced', 'ffmpeg_path')
        ffmpeg_timeout = self.config.getint('Timeouts', 'ffmpeg_timeout')
        min_file_size_mb = self.config.getfloat('Recording', 'min_file_size_mb')

        raw_file = entry["raw_file"]
        processed_path = entry["processed_path"]
        pending_dir = entry["pending_dir"]
        filename = os.path.basename(raw_file)

        if not os.path.exists(raw_file):
            return "skipped"  # already handled by another cleanup pass

        try:
            size1 = os.path.getsize(raw_file)
            # Written after the snapshot => a live recorder owns it, not a leftover.
            if os.path.getmtime(raw_file) > snapshot_time + 2:
                logging.warning(f"Startup cleanup: {filename} was modified after startup, skipping")
                return "skipped"
            if self._startup_cleanup_cancel.wait(self._startup_settle_seconds):
                return "cancelled"
            size2 = os.path.getsize(raw_file)
            if size2 != size1:
                logging.warning(f"Startup cleanup: {filename} still growing, skipping")
                return "skipped"
            file_size = size2
        except Exception:
            return "skipped"

        if file_size < min_file_size_mb * 1024 * 1024:
            logging.info(f"Startup cleanup: Removing tiny file {filename} ({human_size(file_size)})")
            try:
                os.remove(raw_file)
            except Exception:
                pass
            return "skipped"

        logging.info(f"Startup cleanup: Processing {filename} ({human_size(file_size)})")

        mp4_filename = filename[:-len('.ts')] + '.mp4'
        mp4_file = os.path.join(processed_path, mp4_filename)
        # Present only while a remux of this file was in progress; if the app
        # died mid-remux the marker survives and the partial MP4 is redone.
        marker = mp4_file + ".remuxing"

        mp4_ok = False
        if os.path.exists(mp4_file) and not os.path.exists(marker):
            try:
                mp4_size = os.path.getsize(mp4_file)
                mp4_ok = mp4_size > 5 * 1024**2 and mp4_size >= 0.8 * file_size
                if mp4_size > 5 * 1024**2 and not mp4_ok:
                    logging.warning(
                        f"Startup cleanup: existing MP4 for {filename} is only "
                        f"{human_size(mp4_size)} vs {human_size(file_size)} raw "
                        "- treating as stale partial, re-remuxing")
            except Exception:
                mp4_ok = False

        if mp4_ok:
            logging.info("Startup cleanup: MP4 already exists, moving raw file")
        else:
            mp4_preexisting = os.path.exists(mp4_file)
            try:
                open(marker, "w").close()
            except Exception:
                pass
            file_size_gb = file_size / (1024**3)
            scaled_timeout = max(ffmpeg_timeout, int(file_size_gb * 60) + 120)
            if background:
                success, mp4_size, error = remux_to_mp4(
                    raw_file, mp4_file, ffmpeg_path, logging.getLogger(), scaled_timeout,
                    low_priority=True, cancel_event=self._startup_cleanup_cancel)
            else:
                success, mp4_size, error = remux_to_mp4(
                    raw_file, mp4_file, ffmpeg_path, logging.getLogger(), scaled_timeout)
            if success:
                try:
                    os.remove(marker)
                except OSError:
                    pass
            elif error == "cancelled":
                # Leave the .ts for the next pass; drop the half-written MP4
                # if this remux created it.
                if not mp4_preexisting:
                    for leftover in (mp4_file, marker):
                        try:
                            os.remove(leftover)
                        except OSError:
                            pass
                logging.info(f"Startup cleanup: {filename} left in place for the next pass")
                return "cancelled"
            else:
                try:
                    os.remove(marker)
                except OSError:
                    pass
                logging.error(f"Startup cleanup: Failed to process {filename}: {error}")
                try:
                    pending_path = os.path.join(pending_dir, filename)
                    shutil.move(raw_file, pending_path)
                    logging.info(f"Startup cleanup: Moved unprocessable {filename} to PendingDeletion")
                except Exception as move_err:
                    logging.warning(f"Startup cleanup: Could not move {filename} to PendingDeletion: {move_err}")
                return "error"

        try:
            pending_path = os.path.join(pending_dir, filename)
            shutil.move(raw_file, pending_path)
            logging.info(f"Startup cleanup: Done with {mp4_filename}")
            return "done"
        except Exception as e:
            logging.warning(f"Startup cleanup: Could not move {filename}: {e}")
            return "error"