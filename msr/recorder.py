"""Session orchestrator: worker processes, cleanup, Kick push listener."""
import json
import logging
import multiprocessing as mp
import os
import random
import shutil
import threading
import time

from msr.deps import HAS_CURL_CFFI, HAS_WEBSOCKET
from msr.util import (
    PENDING_DELETION_FOLDER,
    channel_key_to_dirs,
    find_cookies_file,
    human_size,
    iter_channel_record_dirs,
    kill_orphan_ffmpeg_processes,
    kill_process_tree,
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
# Kick's public Pusher endpoint (same socket the kick.com website uses).
# If Kick rotates the app key, grab the new URL from DevTools -> Network ->
# WS filter on any kick.com page and update it here.
KICK_PUSHER_URL = (
    "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
    "?protocol=7&client=js&version=8.4.0-rc2&flash=false"
)
KICK_LIVE_EVENT = "App\\Events\\StreamerIsLive"
KICK_STOP_EVENT = "App\\Events\\StopStreamBroadcast"
KICK_ID_CACHE_FILE = "kick_channel_ids.json"   # lives next to config.ini
PUSH_RECONNECT_BASE = 5        # seconds; doubles up to the cap below
PUSH_RECONNECT_MAX = 300


def _safe_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


class KickPushListener:
    """Single-connection Pusher listener that wakes Kick workers on go-live.

    Holds ONE WebSocket subscribed to ``channel.{id}`` for every active Kick
    channel.  When Kick pushes ``App\\Events\\StreamerIsLive``, this calls
    recorder.check_now(slug) — the exact same wake path as the GUI's
    "Check Now" button.  Polling is never disabled; push is purely additive.
    If the socket is down (or websocket-client isn't installed), MSR behaves
    exactly as it did before this feature existed.

    Runs as a daemon thread in the main process.  It never records anything
    and never marks a stream live — the worker's normal streamlink check
    remains the source of truth, so spurious/duplicate events are harmless.
    """

    def __init__(self, recorder, config):
        self.recorder = recorder           # StreamRecorder instance
        self.config = config
        self.logger = logging.getLogger()
        self._ws = None
        self._thread = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        # slug -> numeric channel_id (resolved lazily, cached to disk)
        self._ids: dict = {}
        # channel_id (int) -> slug, for routing incoming events
        self._id_to_slug: dict = {}
        # slugs we want subscribed (bare Kick channel names == channel_key)
        self._wanted: set = set()
        # slugs confirmed subscribed on the current connection
        self._subscribed: set = set()
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

    def stop(self):
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def set_channels(self, kick_slugs):
        """Update the wanted-channel set.  Subscribes/unsubscribes live if
        connected; otherwise the set is picked up on the next connect."""
        new = set(kick_slugs)
        with self._lock:
            added = new - self._wanted
            removed = self._wanted - new
            self._wanted = new
        for slug in removed:
            self._set_push_flag(slug, False)
            self._unsubscribe(slug)
        for slug in added:
            self._subscribe(slug)

    # ── connection loop ──

    def _run(self):
        import websocket  # websocket-client (guarded by HAS_WEBSOCKET)
        backoff = PUSH_RECONNECT_BASE
        while not self._stop.is_set():
            self._subscribed.clear()
            connected_at = time.monotonic()
            try:
                self._ws = websocket.WebSocketApp(
                    KICK_PUSHER_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=lambda w, e: self.logger.warning(
                        f"Kick push: socket error: {e}"),
                    on_close=lambda w, c, r: self.logger.info(
                        f"Kick push: socket closed ({c})"),
                )
                # ping keeps Pusher from dropping the idle connection
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                self.logger.warning(f"Kick push: connection failed: {e}")
            finally:
                self._ws = None
                # Connection is gone — clear every push flag so the workers'
                # status line stops advertising push coverage.
                for slug in list(self._subscribed) or list(self._wanted):
                    self._set_push_flag(slug, False)
                self._subscribed.clear()

            if self._stop.is_set():
                break
            # A connection that survived a while was healthy — reset backoff
            if time.monotonic() - connected_at > 60:
                backoff = PUSH_RECONNECT_BASE
            self.logger.info(f"Kick push: reconnecting in {backoff}s")
            if self._stop.wait(backoff):
                break
            backoff = min(backoff * 2, PUSH_RECONNECT_MAX)

    def _on_open(self, ws):
        self.logger.info("Kick push: connected — subscribing channels")
        with self._lock:
            wanted = list(self._wanted)
        for slug in wanted:
            self._subscribe(slug)

    def _on_message(self, ws, raw):
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return
        event = msg.get("event", "")

        if event == "pusher_internal:subscription_succeeded":
            chan = msg.get("channel", "")          # e.g. "channel.102755229"
            cid = chan.rsplit(".", 1)[-1]
            slug = self._id_to_slug.get(_safe_int(cid))
            if slug:
                self._subscribed.add(slug)
                self._set_push_flag(slug, True)
                self.logger.info(f"Kick push: listening for {slug} ({chan})")
            return

        if event == KICK_LIVE_EVENT:
            try:
                data = json.loads(msg.get("data", "{}"))
                cid = data.get("livestream", {}).get("channel_id")
            except (ValueError, TypeError):
                cid = None
            slug = self._id_to_slug.get(_safe_int(cid))
            if slug:
                title = ""
                try:
                    title = data["livestream"].get("session_title") or ""
                except Exception:
                    pass
                self.logger.info(
                    f"Kick push: {slug} went LIVE ({title!r}) — waking worker")
                self.recorder.check_now(slug)
            return

        if event == KICK_STOP_EVENT:
            # Informational only — the recording process notices the stream
            # ending on its own (stall detector).  Never stop anything from
            # here; a push event must not be able to kill a recording.
            return

    # ── subscription helpers ──

    def _subscribe(self, slug):
        ws = self._ws
        if ws is None:
            return  # not connected — _on_open will subscribe everything wanted
        cid = self._resolve_channel_id(slug)
        if cid is None:
            self.logger.info(
                f"Kick push: no channel id for {slug} — polling only")
            return
        self._id_to_slug[cid] = slug
        try:
            ws.send(json.dumps({
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"channel.{cid}"},
            }))
        except Exception as e:
            self.logger.warning(f"Kick push: subscribe failed for {slug}: {e}")

    def _unsubscribe(self, slug):
        ws = self._ws
        cid = self._ids.get(slug)
        if ws is None or cid is None:
            return
        try:
            ws.send(json.dumps({
                "event": "pusher:unsubscribe",
                "data": {"channel": f"channel.{cid}"},
            }))
        except Exception:
            pass
        self._subscribed.discard(slug)

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
        try:
            from curl_cffi import requests as curl_requests
            r = curl_requests.get(
                f"https://kick.com/api/v2/channels/{slug}",
                impersonate="chrome", timeout=15)
            cid = int(r.json()["id"])
        except Exception as e:
            self.logger.warning(f"Kick push: id lookup failed for {slug}: {e}")
            return None
        self._ids[slug] = cid
        self._save_id_cache()
        return cid

    def _load_id_cache(self):
        try:
            with open(self._cache_path, "r", encoding="utf-8") as f:
                self._ids = {k: int(v) for k, v in json.load(f).items()}
            self._id_to_slug = {v: k for k, v in self._ids.items()}
        except (OSError, ValueError):
            self._ids = {}

    def _save_id_cache(self):
        try:
            with open(self._cache_path, "w", encoding="utf-8") as f:
                json.dump(self._ids, f, indent=2)
        except OSError:
            pass

    # ── shared-state plumbing ──

    def _set_push_flag(self, slug, value):
        """Expose per-channel push coverage to workers via the shared runtime
        dict (same live-update mechanism as poll_interval_minutes)."""
        try:
            self.recorder.runtime[f"kick_push:{slug}"] = bool(value)
        except Exception:
            pass  # Manager may be shutting down


class StreamRecorder:
    """Main recorder that manages worker processes."""

    def __init__(self, channels, config):
        self.channels = channels
        self.config = config

        self.root_path = config.get('Paths', 'streams_dir')
        self.recorded_base = os.path.join(self.root_path, "Recorded")
        self.processed_base = os.path.join(self.root_path, "Processed")
        self.pending_base = os.path.join(self.root_path, PENDING_DELETION_FOLDER)

        os.makedirs(self.recorded_base, exist_ok=True)
        os.makedirs(self.processed_base, exist_ok=True)
        os.makedirs(self.pending_base, exist_ok=True)

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
        # Kick push notifications: one Pusher socket that wakes workers the
        # instant a Kick channel goes live (supplements polling, never
        # replaces it).  No-op if websocket-client isn't installed.
        self.kick_push = KickPushListener(self, config)

    def _sync_kick_push(self):
        """Align the push listener's subscriptions with the currently active
        Kick channels (bare-name channel keys, minus individually stopped
        ones).  Called on session start and on per-channel start/stop."""
        try:
            tracked = set(self.channels) | set(self.processes.keys())
            kick_slugs = [ch for ch in tracked
                          if ":" not in ch and ch not in self.stopped_channels]
            self.kick_push.set_channels(kick_slugs)
        except Exception as e:
            logging.warning(f"Kick push: subscription sync failed: {e}")

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
        while not self.status_queue.empty():
            try:
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
            except Exception:
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

        # Kill them all
        for proc in alive_procs:
            logging.info(f"Stopping channel {channel_name} (PID {proc.pid})")
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

            # Clear the individually-stopped flag
            self.stopped_channels.discard(channel_name)

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
            worker_args = (channel_name, config_dict, self.should_stop, self.status_queue,
                           self._get_wake_event(channel_name), self.runtime)
            proc = mp.Process(target=record_worker, args=(worker_args,))
            proc.daemon = True
            proc.start()
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
            kill_process_tree(pid)

        for ch, procs in self.processes.items():
            for proc in procs:
                proc.join(timeout=10)
                if proc.is_alive():
                    logging.warning(f"Process {ch} (PID {proc.pid}) did not terminate, force killing...")
                    proc.kill()
                    proc.join(timeout=5)

        self.processes = {}

        logging.info("Checking for orphaned ffmpeg processes...")
        kill_orphan_ffmpeg_processes(logging.getLogger(), streams_dir=self.root_path)

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

    def run(self):
        if self.is_running:
            return
        self.is_running = True
        self.should_stop.clear()

        # Quick synchronous cleanup of leftover files from previous sessions
        self._quick_startup_cleanup()

        logging.info(f"Launching {len(self.channels)} recording processes")

        config_dict = {section: dict(self.config.config.items(section))
                       for section in self.config.config.sections()}

        # Resolve cookies file path and pass it through config
        cookies_file = find_cookies_file(self.config)
        if cookies_file:
            config_dict.setdefault('Paths', {})['cookies_file'] = cookies_file
            logging.info(f"Using cookies file: {cookies_file}")

        for ch in self.channels:
            worker_args = (ch, config_dict, self.should_stop, self.status_queue,
                           self._get_wake_event(ch), self.runtime)
            proc = mp.Process(target=record_worker, args=(worker_args,))
            proc.daemon = True
            proc.start()
            self.processes[ch] = [proc]
            logging.info(f"Started process for {ch} (PID {proc.pid})")

        # Start the Kick push listener (harmless no-op without Kick channels
        # or without websocket-client installed)
        self._sync_kick_push()
        self.kick_push.start()

        # Monitor processes — restart any that exit unexpectedly
        while self.is_running and not self.should_stop.is_set():
            self.update_status_from_queue()

            for ch in list(self.processes.keys()):
                if self.should_stop.is_set():
                    break

                # Skip channels individually stopped by the user
                if ch in self.stopped_channels:
                    continue

                # Skip channels that have an active start_channel() in progress
                with self._spawn_lock:
                    if ch in self._spawning:
                        continue

                procs = self.processes.get(ch, [])

                # Prune dead processes from the list
                alive = [p for p in procs if p.is_alive()]
                self.processes[ch] = alive

                if not alive:
                    # All processes for this channel have exited — examine the last
                    # one to decide whether to restart.
                    dead = [p for p in procs if not p.is_alive()]
                    if not dead:
                        # Channel was never started (shouldn't happen in run(), but
                        # handle gracefully)
                        continue

                    last_proc = dead[-1]
                    exit_code = last_proc.exitcode

                    # Exit code -15 (SIGTERM on Unix) or 15 (Windows-mapped) means
                    # we killed the process ourselves (stop_channel / stop).
                    # Do NOT restart in that case — it was intentional.
                    if exit_code in (-15, 15):
                        logging.info(
                            f"Process for {ch} exited with code {exit_code} (SIGTERM) "
                            f"— not restarting (intentional kill)"
                        )
                        self.stopped_channels.add(ch)
                        continue

                    if exit_code != 0:
                        logging.warning(f"Process for {ch} crashed (exit code {exit_code}) — restarting...")
                    else:
                        logging.info(f"Process for {ch} exited normally — restarting...")

                    worker_args = (ch, config_dict, self.should_stop, self.status_queue,
                                   self._get_wake_event(ch), self.runtime)
                    new_proc = mp.Process(target=record_worker, args=(worker_args,))
                    new_proc.daemon = True
                    new_proc.start()
                    self.processes[ch] = [new_proc]
                    logging.info(f"Restarted process for {ch} (PID {new_proc.pid})")

            time.sleep(2)

        # Loop exited — stop() was already called or should_stop was set
        # Don't call stop() here to avoid double-stop race condition

    def _quick_startup_cleanup(self):
        logging.info("Startup cleanup: checking for leftover .ts files from previous sessions...")

        ffmpeg_path = self.config.get('Advanced', 'ffmpeg_path')
        ffmpeg_timeout = self.config.getint('Timeouts', 'ffmpeg_timeout')
        min_file_size_mb = self.config.getfloat('Recording', 'min_file_size_mb')
        processed_count = 0

        if not os.path.exists(self.recorded_base):
            logging.info("Startup cleanup: No leftover files found")
            return

        # Scan recorded_base dynamically so new platforms are picked up
        # automatically without needing a hardcoded list.
        # One extra nesting level covers custom/chaturbate/alice.
        for platform, username_dir, username_path in iter_channel_record_dirs(self.recorded_base):
            processed_path = os.path.join(self.processed_base, platform, username_dir)
            pending_dir = os.path.join(self.pending_base, platform, username_dir)
            os.makedirs(processed_path, exist_ok=True)
            os.makedirs(pending_dir, exist_ok=True)

            for filename in os.listdir(username_path):
                if not filename.endswith('.ts'):
                    continue

                raw_file = os.path.join(username_path, filename)

                try:
                    size1 = os.path.getsize(raw_file)
                    time.sleep(1)
                    size2 = os.path.getsize(raw_file)
                    if size2 != size1:
                        logging.warning(f"Startup cleanup: {filename} still growing, skipping")
                        continue
                    file_size = size2
                except Exception:
                    continue

                if file_size < min_file_size_mb * 1024 * 1024:
                    logging.info(f"Startup cleanup: Removing tiny file {filename} ({human_size(file_size)})")
                    try:
                        os.remove(raw_file)
                    except Exception:
                        pass
                    continue

                logging.info(f"Startup cleanup: Processing {filename} ({human_size(file_size)})")

                mp4_filename = filename.replace('.ts', '.mp4')
                mp4_file = os.path.join(processed_path, mp4_filename)

                if os.path.exists(mp4_file) and os.path.getsize(mp4_file) > 5 * 1024**2:
                    logging.info("Startup cleanup: MP4 already exists, moving raw file")
                else:
                    file_size_gb = file_size / (1024**3)
                    scaled_timeout = max(ffmpeg_timeout, int(file_size_gb * 60) + 120)
                    success, mp4_size, error = remux_to_mp4(
                        raw_file, mp4_file, ffmpeg_path,
                        logging.getLogger(), scaled_timeout
                    )
                    if not success:
                        logging.error(f"Startup cleanup: Failed to process {filename}: {error}")
                        try:
                            pending_path = os.path.join(pending_dir, filename)
                            shutil.move(raw_file, pending_path)
                            logging.info(f"Startup cleanup: Moved unprocessable {filename} to PendingDeletion")
                        except Exception as move_err:
                            logging.warning(f"Startup cleanup: Could not move {filename} to PendingDeletion: {move_err}")
                        continue

                try:
                    pending_path = os.path.join(pending_dir, filename)
                    shutil.move(raw_file, pending_path)
                    processed_count += 1
                    logging.info(f"Startup cleanup: Done with {mp4_filename}")
                except Exception as e:
                    logging.warning(f"Startup cleanup: Could not move {filename}: {e}")

        if processed_count > 0:
            logging.info(f"Startup cleanup: Processed {processed_count} leftover file(s)")
        else:
            logging.info("Startup cleanup: No leftover files found")

