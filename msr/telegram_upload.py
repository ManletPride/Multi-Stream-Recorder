"""Background upload of finished recordings to one Telegram forum topic.

The thread never touches Tk and never runs on the recording worker. A crash
mid-send leaves the part unconfirmed, so the next start sends it again.
"""

from __future__ import annotations

import logging
import os
import queue
import shutil
import subprocess
import threading
import time

from msr.nowindow import no_window_kwargs
from msr.telegram_queue import (
    PARTS_DIR,
    QUEUE_FILE,
    UPLOAD_LIMIT_BYTES,
    AuthRejected,
    DestinationRejected,
    Flood,
    NotInstalled,
    QueueStore,
    Settings,
    build_caption,
    classify_failure,
    backoff_seconds,
    delete_after_upload,
    deletion_allowed,
    discard_note,
    guard_part,
    guard_recording,
    iter_inbox,
    load_session,
    plan_spans,
    read_note,
    read_settings,
    save_session,
    clear_session,
    scrub,
    shrink_span,
    upload_enabled,
    part_filename,
)

log = logging.getLogger(__name__)


def _hidden(cmd, timeout):
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, **no_window_kwargs())


def probe_duration(path: str, ffmpeg: str = "ffmpeg") -> float | None:
    """Length in seconds, or None when ffprobe cannot read it."""
    from msr.util import ffprobe_from_ffmpeg
    try:
        result = _hidden(
            [ffprobe_from_ffmpeg(ffmpeg), "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            60)
        return float((result.stdout or "").strip())
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None


def extract_part(src: str, dest: str, start: float, duration: float, ffmpeg: str = "ffmpeg") -> int:
    """Stream-copy one span to ``dest``. No re-encode. Returns the new file's size."""
    cmd = [ffmpeg or "ffmpeg", "-y", "-ss", f"{start:.3f}", "-i", src, "-t", f"{duration:.3f}",
           "-c", "copy", "-map", "0", "-avoid_negative_ts", "make_zero", dest]
    result = _hidden(cmd, 600)
    if result.returncode != 0 or not os.path.isfile(dest):
        tail = scrub((result.stderr or "")[-300:])
        raise OSError(tail or f"ffmpeg exited {result.returncode}")
    return os.path.getsize(dest)


def _free_bytes(path: str) -> int:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return 0


def _whole(job) -> dict:
    return {"n": 1, "m": 1, "start": 0, "duration": 0, "whole": True, "confirmed": False,
            "message_id": None, "path": job["path"], "temp": False, "size": job["size"], "tries": 0}


class Uploader:
    """One queue, one thread, one bot session. ``step`` does a single piece of work (tests call it)."""

    def __init__(self, config, data_dir, *, get_recorder=None, transport=None, membership=None,
                 installed=None, clock=None, roots=None, probe=None, split=None, free_bytes=None,
                 limit=None, ffmpeg="ffmpeg"):
        self.config = config
        self.data_dir = data_dir or "."
        self.get_recorder = get_recorder
        self.clock = clock or time.time
        self.roots = roots
        self.probe = probe or (lambda path: probe_duration(path, self._ffmpeg()))
        self.split = split or (lambda src, dest, start, dur: extract_part(src, dest, start, dur, self._ffmpeg()))
        self.free_bytes = free_bytes or _free_bytes
        self.limit = int(limit if limit is not None else UPLOAD_LIMIT_BYTES)
        self._ffmpeg_path = ffmpeg
        self.store = QueueStore(os.path.join(self.data_dir, QUEUE_FILE))
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = None
        self._tests = queue.Queue()
        self._busy = False
        self._state = "off"
        self._line = "Off"
        self._error = ""
        self._part = None
        self._percent = 0
        self._blocked = None
        self._member_fp = ""
        self._member_until = 0.0
        self._logged_delete = False
        self._logged_missing = ""
        if transport is None:
            from msr.telegram_api import BotSession
            transport = BotSession()
        self.transport = transport
        if membership is None:
            from msr.telegram_api import owner_is_member
            def membership(settings, _check=owner_is_member):
                _check(settings.bot_token, settings.chat_id, settings.owner_id)
        self.membership = membership
        if installed is None:
            from msr.telegram_api import installed as _installed
            installed = _installed
        self._installed_fn = installed

    def _ffmpeg(self) -> str:
        try:
            return self.config.get("Advanced", "ffmpeg_path", fallback=self._ffmpeg_path) or self._ffmpeg_path
        except Exception:
            return self._ffmpeg_path

    def _roots(self):
        if self.roots is not None:
            return list(self.roots)
        roots = []
        try:
            streams = self.config.get("Paths", "streams_dir", fallback="") or ""
        except Exception:
            streams = ""
        if streams:
            roots.append(streams)
        try:
            fallback = str(self.config.get("Paths", "fallback_dir", fallback="") or "").strip()
        except Exception:
            fallback = ""
        if fallback and fallback.lower() != "off":
            roots.append(fallback)
        return roots

    def _parts_root(self) -> str:
        return os.path.join(self.data_dir, PARTS_DIR)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="telegram-upload", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=3)
        try:
            self.transport.close()
        except Exception:
            log.debug("Telegram: closing the session", exc_info=True)

    def sync(self) -> None:
        """Push the enable flag into a running recorder, and say once if deletes are on."""
        rec = None
        if self.get_recorder is not None:
            try:
                rec = self.get_recorder()
            except Exception:
                rec = None
        runtime = getattr(rec, "runtime", None) if rec is not None else None
        if runtime is not None:
            try:
                runtime["telegram_upload"] = bool(upload_enabled(self.config))
            except Exception:
                log.debug("Telegram: could not update the worker flag", exc_info=True)
        if upload_enabled(self.config):
            self.start()
        if delete_after_upload(self.config) and not self._logged_delete:
            self._logged_delete = True
            log.warning("Telegram: the local recording is deleted after every part is confirmed. "
                        "A failed upload keeps the file.")

    def test_inline(self) -> dict:
        """Send the test on this thread. Used when no upload thread is running."""
        box = queue.Queue(1)
        self._run_test(box)
        try:
            return box.get_nowait()
        except queue.Empty:
            return {"ok": False, "error": "The test did not finish."}

    def test(self, timeout: float = 90) -> dict:
        """Send the fixed test message. Called off the Tk thread (the GUI uses a worker thread).

        With the upload thread running, the test is done there so Telethon stays on one thread.
        Before uploads are turned on there is no thread, and the test runs here.
        """
        if self._thread is not None and self._thread.is_alive():
            if self._busy:
                return {"ok": False, "error": "An upload is in progress. Try the test again when it finishes."}
            box = queue.Queue(1)
            self._tests.put(box)
            self._wake.set()
            try:
                return box.get(timeout=timeout)
            except queue.Empty:
                return {"ok": False, "error": "Timed out waiting to send the test message."}
        return self.test_inline()

    def status(self) -> dict:
        with self._lock:
            counts = self.store.counts()
            return {
                "enabled": upload_enabled(self.config),
                "installed": bool(self._installed_fn()),
                "state": self._state,
                "line": self._line,
                "pending": counts["pending"],
                "failed": counts["failed"],
                "done": counts["done"],
                "error": self._error,
                "current": dict(self._part) if self._part else None,
                "delete_after_upload": delete_after_upload(self.config),
            }

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.step()
            except Exception:
                log.exception("Telegram uploads")
            self._stop.wait(self._pause())

    def _pause(self) -> float:
        if not self._tests.empty():
            return 0.05
        with self._lock:
            nxt = self.store.next_time(self.clock())
        if nxt is None:
            return 1.0
        return max(0.1, min(2.0, nxt - self.clock()))

    def _set(self, state: str, line: str, error: str = "") -> None:
        self._state = state
        self._line = line
        self._error = error

    def step(self) -> str:
        box = self._pop_test()
        if box is not None:
            self._run_test(box)
            return "test"
        if self._import_one():
            return "queued"
        with self._lock:
            if not upload_enabled(self.config):
                self._set("off", "Off")
                return "off"
            if not self._installed_fn():
                line = "Telethon is not installed. pip install Telethon, then restart MSR."
                self._set("missing", line, line)
                if self._logged_missing != line:
                    self._logged_missing = line
                    log.info("Telegram: %s", line)
                return "missing"
            settings = read_settings(self.config)
            why = settings.missing()
            if why:
                self._set("incomplete", why, why)
                if self._logged_missing != why:
                    self._logged_missing = why
                    log.info("Telegram: %s", why)
                return "incomplete"
            self._logged_missing = ""
            if self._still_blocked(settings):
                return "blocked"
            job = self._pick()
            if job is None:
                counts = self.store.counts()
                waiting = f", {counts['pending']} waiting" if counts["pending"] else ""
                self._set("idle", "Idle" + waiting)
                self._part = None
                return "idle"
            action = self._action(job, settings)
            self.store.save()
        if action is None or action.get("kind") == "none":
            return "error"
        self._busy = True
        try:
            kind = self._perform(action)
        finally:
            self._busy = False
        with self._lock:
            self.store.save()
        return kind

    def _pop_test(self):
        try:
            return self._tests.get_nowait()
        except queue.Empty:
            return None

    def _import_one(self) -> bool:
        if not upload_enabled(self.config):
            return False
        for path in iter_inbox(self.data_dir):
            note = read_note(path)
            if not note:
                discard_note(path)
                return True
            src = str(note.get("path") or "")
            if not os.path.isfile(src) or os.path.islink(src):
                created = float(note.get("created") or 0)
                if created and self.clock() - created > 3600:
                    discard_note(path)
                    return True
                continue
            ok, why = guard_recording(src, self._roots())
            if not ok:
                log.warning("Telegram: not uploading %s (%s)", os.path.basename(src), why)
                discard_note(path)
                return True
            with self._lock:
                job = self.store.enqueue(
                    src, str(note.get("channel") or ""), str(note.get("platform") or ""),
                    str(note.get("title") or ""), str(note.get("recorded_at") or ""))
                if job is not None:
                    self.store.save()
            discard_note(path)
            if job is not None:
                log.info("Telegram: queued %s", job["name"])
            return True
        return False

    def _still_blocked(self, settings: Settings) -> bool:
        blocked = self._blocked
        if not blocked:
            return False
        if blocked["kind"] == "auth":
            if blocked["fp"] != settings.credential_fp():
                self._blocked = None
                return False
            self._set("paused", blocked["message"], blocked["message"])
            return True
        if blocked["fp"] != settings.dest_fp() or self.clock() >= blocked["until"]:
            self._blocked = None
            return False
        self._set("paused", blocked["message"], blocked["message"])
        return True

    def _pick(self):
        now = self.clock()
        for job in self.store.jobs:
            if job.get("state") in ("pending", "uploading") and float(job.get("next_try") or 0) <= now:
                return job
        return None

    def _job(self, job_id):
        for job in self.store.jobs:
            if job.get("id") == job_id:
                return job
        return None

    def _fail(self, job, message, settings=None) -> None:
        secrets = settings.secrets() if settings is not None else ()
        job["state"] = "error"
        job["error"] = scrub(message, *secrets)
        job["next_try"] = 0
        self._set("paused", job["error"], job["error"])
        self._part = None
        log.error("Telegram: %s — %s", job.get("name") or "recording", job["error"])

    def _action(self, job, settings: Settings):
        ok, why = guard_recording(job.get("path") or "", self._roots())
        if not ok:
            self._fail(job, why, settings)
            return {"kind": "none"}
        try:
            size = os.path.getsize(job["path"])
        except OSError:
            self._fail(job, "the recording is no longer there", settings)
            return {"kind": "none"}
        if size != int(job.get("size") or -1):
            self._fail(job, "the recording changed size and was not uploaded", settings)
            return {"kind": "none"}
        dest = {"chat_id": settings.chat_id, "topic_id": settings.topic_id}
        if job.get("dest") and job["dest"] != dest:
            if any(p.get("confirmed") for p in job.get("parts") or []):
                self._fail(job, "The group or topic changed after some parts were posted. "
                           "The local file was kept.", settings)
                return {"kind": "none"}
            job["dest"] = dest
        elif not job.get("dest"):
            job["dest"] = dest
        parts = job.get("parts") or []
        if not parts:
            return {"kind": "plan", "job_id": job["id"]}
        if all(p.get("confirmed") for p in parts):
            return {"kind": "finish", "job_id": job["id"]}
        idx = next(i for i, p in enumerate(parts) if not p.get("confirmed"))
        part = parts[idx]
        if part.get("path") and guard_part(part["path"], job["id"], self._parts_root(), job["path"]):
            self._part = {"name": job.get("name") or "", "part": part["n"], "parts": part["m"],
                          "percent": self._percent}
            self._set("uploading", self._progress_line())
            return {"kind": "upload", "job_id": job["id"], "part": idx}
        return {"kind": "split", "job_id": job["id"], "part": idx}

    def _progress_line(self) -> str:
        cur = self._part or {}
        name = cur.get("name") or "recording"
        if int(cur.get("parts") or 1) > 1:
            return f"Uploading {name}, part {cur.get('part')}/{cur.get('parts')}, {cur.get('percent') or 0}%"
        return f"Uploading {name}, {cur.get('percent') or 0}%"

    def _perform(self, action) -> str:
        kind = action.get("kind")
        with self._lock:
            job = self._job(action.get("job_id"))
            settings = read_settings(self.config)
        if job is None:
            return "idle"
        if kind == "plan":
            self._plan(job, settings)
            return "planned"
        if kind == "split":
            self._split_part(job, action["part"], settings)
            return "split"
        if kind == "upload":
            self._upload_part(job, action["part"], settings)
            return "uploaded"
        if kind == "finish":
            self._finish(job)
            return "done"
        return "error"

    def _plan(self, job, settings) -> None:
        if job["size"] <= self.limit:
            job["parts"] = [_whole(job)]
            job["state"] = "pending"
            return
        try:
            duration = self.probe(job["path"])
        except Exception:
            duration = None
            log.debug("Telegram: probing %s", job.get("name"), exc_info=True)
        spans = plan_spans(job["size"], duration, self.limit)
        if not spans or (len(spans) == 1 and spans[0].get("whole") and job["size"] > self.limit):
            self._fail(job, "This recording is over 2000 MB and could not be split without re-encoding. "
                       "The file was kept.", settings)
            return
        if spans[0].get("whole"):
            job["parts"] = [_whole(job)]
        else:
            many = len(spans)
            total = sum(span["duration"] for span in spans) or 1.0
            job["parts"] = [{
                "n": i + 1, "m": many, "start": span["start"], "duration": span["duration"],
                "expect": max(1, int(job["size"] * span["duration"] / total * 1.15)),
                "whole": False, "confirmed": False, "message_id": None, "path": "", "temp": False,
                "size": 0, "tries": 0,
            } for i, span in enumerate(spans)]
        job["state"] = "pending"
        log.info("Telegram: %s will upload as %s part(s)", job.get("name"), len(job["parts"]))

    def _split_part(self, job, idx, settings) -> None:
        part = job["parts"][idx]
        if part.get("whole"):
            part["path"] = job["path"]
            part["temp"] = False
            part["size"] = job["size"]
            return
        estimate = int(part.get("expect") or int(self.limit * 0.90))
        free = self.free_bytes(os.path.dirname(job["path"]) or ".")
        if free < estimate + 32 * 1024 * 1024:
            job["state"] = "pending"
            job["next_try"] = self.clock() + 300
            job["error"] = "Not enough free space to split off a part. The recording stays on disk."
            self._set("paused", job["error"], job["error"])
            log.warning("Telegram: %s", job["error"])
            return
        folder = os.path.join(self._parts_root(), job["id"])
        os.makedirs(folder, exist_ok=True)
        dest = os.path.join(folder, part_filename(job["name"], part["n"], part["m"]))
        try:
            size = int(self.split(job["path"], dest, float(part["start"]), float(part["duration"])))
        except Exception as exc:
            self._retry(job, settings, "Could not split the recording: " + scrub(exc, *settings.secrets()))
            return
        if size > self.limit:
            try:
                os.remove(dest)
            except OSError:
                pass
            part["tries"] = int(part.get("tries") or 0) + 1
            shrunk = shrink_span(part, size, self.limit)
            if shrunk is None or part["tries"] > 4:
                self._fail(job, "A part was still over Telegram's 2000 MB limit. The file was kept.", settings)
                return
            self._shrink(job, idx, part, shrunk)
            return
        if not guard_part(dest, job["id"], self._parts_root(), job["path"]):
            self._fail(job, "the split file was not kept in MSR's temp folder", settings)
            return
        part["path"] = dest
        part["temp"] = True
        part["size"] = size

    def _shrink(self, job, idx, part, shrunk) -> None:
        old_dur = float(part["duration"])
        old_end = float(part["start"]) + old_dur
        old_expect = int(part.get("expect") or 0)
        part["start"] = shrunk["start"]
        part["duration"] = shrunk["duration"]
        part["path"] = ""
        if old_expect and old_dur:
            part["expect"] = max(1, int(old_expect * float(shrunk["duration"]) / old_dur))
        rest_at = float(shrunk["start"]) + float(shrunk["duration"])
        rest = round(old_end - rest_at, 3)
        if rest > 0.25:
            job["parts"].insert(idx + 1, {
                "n": 0, "m": 0, "start": round(rest_at, 3), "duration": rest, "whole": False,
                "expect": max(1, int(old_expect * rest / old_dur)) if old_expect and old_dur else 0,
                "confirmed": False, "message_id": None, "path": "", "temp": False, "size": 0, "tries": 0,
            })
        many = len(job["parts"])
        for i, item in enumerate(job["parts"]):
            item["n"] = i + 1
            item["m"] = many
        log.info("Telegram: a part of %s was over the limit; it was split smaller", job.get("name"))

    def _upload_part(self, job, idx, settings) -> None:
        part = job["parts"][idx]
        if not guard_part(part.get("path") or "", job["id"], self._parts_root(), job["path"]):
            self._fail(job, "refusing to upload a file MSR did not produce for this recording", settings)
            return
        send = Settings(
            settings.enabled, settings.api_id, settings.api_hash, settings.bot_token,
            int(job["dest"]["chat_id"]), int(job["dest"]["topic_id"]), settings.owner_id,
            settings.delete_local)
        caption = build_caption(job.get("channel") or "", job.get("platform") or "",
                                job.get("title") or "", job.get("recorded_at") or "", part["n"], part["m"])
        self._part = {"name": job.get("name") or "", "part": part["n"], "parts": part["m"], "percent": 0}
        with self._lock:
            self._set("uploading", self._progress_line())
        try:
            self._ensure_member(send)
            session = load_session(self.data_dir, send.credential_fp())
            session = self.transport.connect(send, session)
            save_session(self.data_dir, session, send.credential_fp())
            result = self.transport.send_file(send, session, part["path"], caption, self._on_progress)
            if result.get("session"):
                save_session(self.data_dir, result["session"], send.credential_fp())
        except Exception as exc:
            self._on_send_error(job, send, exc)
            return
        part["confirmed"] = True
        part["message_id"] = result.get("message_id")
        job["attempts"] = 0
        job["error"] = ""
        job["state"] = "uploading"
        if part.get("temp"):
            try:
                os.remove(part["path"])
            except OSError:
                log.debug("Telegram: temp part left on disk", exc_info=True)
            part["path"] = ""
            part["temp"] = False
        log.info("Telegram: uploaded %s part %s/%s", job.get("name"), part["n"], part["m"])

    def _on_progress(self, sent: int, total: int) -> None:
        percent = int(100 * sent / total) if total else 0
        with self._lock:
            if self._part is not None:
                self._part["percent"] = percent
                self._percent = percent
                self._line = self._progress_line()

    def _ensure_member(self, settings: Settings) -> None:
        fp = settings.credential_fp() + settings.dest_fp()
        if self._member_fp == fp and self.clock() < self._member_until:
            return
        self.membership(settings)
        self._member_fp = fp
        self._member_until = self.clock() + 600

    def _on_send_error(self, job, settings, exc) -> None:
        err = classify_failure(exc)
        message = scrub(str(err), *settings.secrets())
        job["state"] = "pending"
        self._part = None
        if isinstance(err, Flood):
            job["next_try"] = self.clock() + err.seconds + 1
            job["error"] = f"Telegram asked MSR to wait {err.seconds}s"
            self._set("paused", job["error"], job["error"])
            log.warning("Telegram: waiting %ss before the next upload (%s)", err.seconds, job.get("name"))
            return
        if isinstance(err, AuthRejected):
            text = ("The bot token was rejected. Check it in Options. Uploads are paused; "
                    "recordings stay on disk.")
            self._blocked = {"kind": "auth", "fp": settings.credential_fp(), "until": 0, "message": text}
            clear_session(self.data_dir)
            job["error"] = text
            self._set("paused", text, text)
            log.error("Telegram: %s", text)
            return
        if isinstance(err, (DestinationRejected, NotInstalled)):
            text = message or "Telegram refused the group or topic. The recording stays on disk."
            self._blocked = {"kind": "config", "fp": settings.dest_fp(), "until": self.clock() + 600,
                             "message": text}
            job["error"] = text
            self._set("paused", text, text)
            log.error("Telegram: %s", text)
            return
        self._retry(job, settings, message or "the upload failed")

    def _retry(self, job, settings, message) -> None:
        job["attempts"] = int(job.get("attempts") or 0) + 1
        wait = backoff_seconds(job["attempts"])
        job["next_try"] = self.clock() + wait
        job["state"] = "pending"
        job["error"] = scrub(message, *settings.secrets())
        self._set("paused", f"{job['error']} Retrying in {wait}s.", job["error"])
        log.warning("Telegram: %s — %s; retrying in %ss", job.get("name"), job["error"], wait)

    def _finish(self, job) -> None:
        settings = read_settings(self.config)
        try:
            size = os.path.getsize(job["path"])
            mtime_ns = os.stat(job["path"]).st_mtime_ns
        except OSError:
            size, mtime_ns = -1, -1
        allowed, _why = deletion_allowed(job, size, mtime_ns, settings.delete_local)
        removed = False
        if allowed:
            removed = self._delete_original(job)
        self._cleanup_dir(job["id"])
        job["state"] = "done"
        job["error"] = "" if removed or not settings.delete_local else "Uploaded, but the local file could not be removed."
        name = job.get("name") or "recording"
        if removed:
            self._set("idle", f"Uploaded and removed {name}")
            log.info("Telegram: uploaded %s and removed the local file", name)
        else:
            self._set("idle", f"Uploaded {name}")
            log.info("Telegram: uploaded %s; the local file was kept", name)
        self._part = None

    def _delete_original(self, job) -> bool:
        path = job.get("path") or ""
        ok, why = guard_recording(path, self._roots())
        if not ok:
            log.warning("Telegram: not deleting %s (%s)", os.path.basename(path), why)
            return False
        try:
            size = os.path.getsize(path)
            mtime_ns = os.stat(path).st_mtime_ns
        except OSError:
            return False
        allowed, why = deletion_allowed(job, size, mtime_ns, True)
        if not allowed:
            log.warning("Telegram: not deleting %s (%s)", os.path.basename(path), why)
            return False
        try:
            os.remove(path)
        except OSError as exc:
            log.warning("Telegram: uploaded %s but could not remove it (%s)", os.path.basename(path), exc)
            return False
        meta = os.path.splitext(path)[0] + ".meta.json"
        if os.path.isfile(meta) and not os.path.islink(meta):
            try:
                os.remove(meta)
            except OSError:
                log.debug("Telegram: sidecar left next to a removed recording", exc_info=True)
        return True

    def _cleanup_dir(self, job_id: str) -> None:
        folder = os.path.join(self._parts_root(), job_id)
        shutil.rmtree(folder, ignore_errors=True)

    def _run_test(self, box) -> None:
        from msr.telegram_api import test_text
        try:
            if not self._installed_fn():
                box.put({"ok": False, "error": "Telethon is not installed. pip install Telethon, then restart MSR."})
                return
            settings = read_settings(self.config)
            why = settings.missing()
            if why:
                box.put({"ok": False, "error": why})
                return
            self._busy = True
            try:
                self._ensure_member(settings)
                session = load_session(self.data_dir, settings.credential_fp())
                session = self.transport.connect(settings, session)
                save_session(self.data_dir, session, settings.credential_fp())
                result = self.transport.send_text(settings, session, test_text())
                if result.get("session"):
                    save_session(self.data_dir, result["session"], settings.credential_fp())
            finally:
                self._busy = False
        except Exception as exc:
            err = classify_failure(exc)
            if isinstance(err, AuthRejected):
                text = "The bot token was rejected. Check it in Options."
                self._blocked = {"kind": "auth", "fp": read_settings(self.config).credential_fp(),
                                 "until": 0, "message": text}
                clear_session(self.data_dir)
                message = text
            else:
                message = scrub(str(err), *read_settings(self.config).secrets())
            self._set("paused", message, message)
            log.warning("Telegram: test message failed: %s", message)
            box.put({"ok": False, "error": message})
            return
        self._blocked = None
        self._set("idle", "Test message sent")
        log.info("Telegram: test message sent to the configured topic")
        box.put({"ok": True, "error": ""})


def disk_status(config, data_dir: str) -> dict:
    """Status with no uploader running (msr_ctl while MSR is closed). No secrets, no paths."""
    from msr.telegram_api import installed
    store = QueueStore(os.path.join(data_dir or ".", QUEUE_FILE))
    counts = store.counts()
    enabled = upload_enabled(config)
    if not enabled:
        state, line = "off", "Off"
    elif not installed():
        state = "missing"
        line = "Telethon is not installed"
    elif counts["pending"]:
        state, line = "idle", f"{counts['pending']} waiting"
    else:
        state, line = "idle", "Idle"
    return {
        "enabled": enabled, "installed": installed(), "state": state, "line": line,
        "pending": counts["pending"], "failed": counts["failed"], "done": counts["done"],
        "error": "", "current": None, "delete_after_upload": delete_after_upload(config),
    }
