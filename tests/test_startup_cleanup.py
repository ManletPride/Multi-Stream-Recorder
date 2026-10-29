"""Startup cleanup of leftover .ts files runs in the background.

After a crash / abrupt close, leftover ``.ts`` files used to be remuxed
synchronously BEFORE any recorder launched (a multi-minute recording gap).  They
are now snapshotted before launch and remuxed afterwards on a background thread.

All tests use temp dirs, fake ``.ts`` files and a fake remux / fake ffmpeg
process - nothing touches real recordings and no real ffmpeg runs.
"""
import configparser
import logging
import os
import queue
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import msr.recorder as recorder_mod
import msr.worker as worker_mod
from msr.recorder import StreamRecorder, _cleanup_lock


class FakeConfig:
    """Just enough of msr.config.Config for the recorder code under test."""

    def __init__(self, streams_dir, pattern="{username}_{timestamp}", min_mb="0.0001"):
        self.config = configparser.ConfigParser()
        for section, key, value in (
            ("Paths", "streams_dir", streams_dir),
            ("Advanced", "ffmpeg_path", "ffmpeg-not-used"),
            ("Timeouts", "ffmpeg_timeout", "600"),
            ("Recording", "min_file_size_mb", min_mb),
            ("Recording", "filename_pattern", pattern),
        ):
            if not self.config.has_section(section):
                self.config.add_section(section)
            self.config.set(section, key, value)

    def get(self, section, key, fallback=None):
        return self.config.get(section, key, fallback=fallback)

    def getint(self, section, key, fallback=None):
        return self.config.getint(section, key, fallback=fallback)

    def getfloat(self, section, key, fallback=None):
        return self.config.getfloat(section, key, fallback=fallback)


class FakeProcess:
    """Stands in for mp.Process: records when it was 'launched'."""
    log = None  # shared event list, set per test
    _next_pid = 1000

    def __init__(self, target=None, args=()):
        self.target, self.args = target, args
        self.daemon = False
        self.pid = None
        self.exitcode = None

    def start(self):
        FakeProcess._next_pid += 1
        self.pid = FakeProcess._next_pid
        FakeProcess.log.append(f"launch:{self.args[0][0]}")

    def is_alive(self):
        return True

    def join(self, timeout=None):
        pass

    def kill(self):
        pass


class _KickPushStub:
    def set_channels(self, *a, **k): pass
    def start(self): pass
    def stop(self): pass


class _CleanerStub:
    def __init__(self): self.started = False
    def start(self): self.started = True


def make_recorder(tmp, channels=("chanA", "chanB"), pattern="{username}_{timestamp}"):
    """A StreamRecorder without the multiprocessing Manager (no real workers)."""
    streams = os.path.join(tmp, "streams")
    cfg = FakeConfig(streams, pattern=pattern)
    rec = object.__new__(StreamRecorder)
    rec.channels = list(channels)
    rec.config = cfg
    rec.presence = None
    rec.watch_channels = []
    rec.root_path = streams
    rec.recorded_base = os.path.join(streams, "Recorded")
    rec.processed_base = os.path.join(streams, "Processed")
    rec.pending_base = os.path.join(streams, "PendingDeletion")
    for d in (rec.recorded_base, rec.processed_base, rec.pending_base):
        os.makedirs(d, exist_ok=True)
    rec.status_queue = queue.Queue()
    rec.status_dict = {}
    rec.runtime = {}
    rec.wake_events = {}
    rec.processes = {}
    rec.should_stop = threading.Event()
    rec.is_running = False
    rec.stopped_channels = set()
    rec._spawning = set()
    rec._spawn_lock = threading.Lock()
    rec.cleaner = _CleanerStub()
    rec.kick_push = _KickPushStub()
    rec._startup_cleanup_thread = None
    rec._startup_cleanup_cancel = threading.Event()
    rec._startup_settle_seconds = 0.0
    return rec


def make_ts(rec, platform, channel, name, size=2048, age_seconds=3600):
    d = os.path.join(rec.recorded_base, platform, channel)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, name)
    with open(path, "wb") as fh:
        fh.write(b"\0" * size)
    old = time.time() - age_seconds
    os.utime(path, (old, old))
    return path


def pending_path(rec, platform, channel, name):
    return os.path.join(rec.pending_base, platform, channel, name)


def processed_path(rec, platform, channel, name):
    return os.path.join(rec.processed_base, platform, channel, name)


class FakeRemux:
    """Replacement for remux_to_mp4 that records calls and writes a fake MP4."""

    def __init__(self, events=None, block=None, honour_cancel=False, fail=False, mp4_bytes=6 * 1024**2):
        self.events = events if events is not None else []
        self.block = block              # threading.Event the remux waits on
        self.honour_cancel = honour_cancel
        self.fail = fail
        self.mp4_bytes = mp4_bytes
        self.calls = []
        self.entered = threading.Event()
        self._lock = threading.Lock()
        self.in_flight = 0
        self.max_in_flight = 0

    def __call__(self, raw_file, mp4_file, ffmpeg_path, logger, timeout=600,
                 progress_cb=None, low_priority=False, cancel_event=None):
        with self._lock:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            self.calls.append({"raw": raw_file, "mp4": mp4_file,
                               "low_priority": low_priority, "cancel_event": cancel_event})
            self.events.append(f"remux:start:{os.path.basename(raw_file)}")
            self.entered.set()
            if self.block is not None:
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if self.block.wait(0.02):
                        break
                    if self.honour_cancel and cancel_event is not None and cancel_event.is_set():
                        with open(mp4_file, "wb") as fh:  # half-written output
                            fh.write(b"\0" * 1024)
                        self.events.append(f"remux:cancelled:{os.path.basename(raw_file)}")
                        return False, 0, "cancelled"
            if self.fail:
                self.events.append(f"remux:failed:{os.path.basename(raw_file)}")
                return False, 0, "code 1"
            with open(mp4_file, "wb") as fh:
                fh.write(b"\0" * self.mp4_bytes)
            self.events.append(f"remux:end:{os.path.basename(raw_file)}")
            return True, self.mp4_bytes, None
        finally:
            with self._lock:
                self.in_flight -= 1


def wait_for(predicate, timeout=10.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class RunHarness:
    """Run StreamRecorder.run() with fake processes; always stop it cleanly."""

    def __init__(self, test, rec, remux):
        self.test, self.rec, self.remux = test, rec, remux
        self.events = remux.events
        self.thread = None
        self._patches = []

    def __enter__(self):
        FakeProcess.log = self.events
        for p in (
            mock.patch.object(recorder_mod.mp, "Process", FakeProcess),
            mock.patch.object(recorder_mod, "remux_to_mp4", self.remux),
            mock.patch.object(recorder_mod, "kill_orphan_recording_processes", lambda *a, **k: None),
            mock.patch.object(recorder_mod, "find_cookies_file", lambda cfg: None),
            mock.patch.object(StreamRecorder, "_get_wake_event", lambda self, ch: None),
        ):
            p.start()
            self._patches.append(p)
        self.thread = threading.Thread(target=self.rec.run, daemon=True)
        self.thread.start()
        return self

    def stop_run(self):
        self.rec.is_running = False
        self.rec.should_stop.set()
        self.rec._startup_cleanup_cancel.set()
        self.thread.join(timeout=10)

    def __exit__(self, *exc):
        if self.remux.block is not None:
            self.remux.block.set()
        self.stop_run()
        t = self.rec._startup_cleanup_thread
        if t is not None:
            t.join(timeout=10)
        for p in reversed(self._patches):
            p.stop()
        return False


class BackgroundStartupTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.assertFalse(_cleanup_lock.locked(), "cleanup lock leaked from another test")

    def tearDown(self):
        # A failing test must not leave the module-level lock held.
        if _cleanup_lock.locked():
            try:
                _cleanup_lock.release()
            except RuntimeError:
                pass

    # -- the headline behaviour -------------------------------------------

    def test_recorders_launch_before_leftover_remux_finishes(self):
        rec = make_recorder(self.tmp)
        ts = make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        release = threading.Event()
        remux = FakeRemux(block=release)
        with self.assertLogs(level="INFO") as logs:
            with RunHarness(self, rec, remux) as h:
                self.assertTrue(remux.entered.wait(10), "background remux never started")
                # The remux is still blocked, yet both recorders are already up.
                self.assertEqual(h.events[:3], ["launch:chanA", "launch:chanB", "remux:start:chanA_20260101_000000.ts"])
                self.assertTrue(os.path.exists(ts), ".ts must stay until its mp4 is verified")
                release.set()
                self.assertTrue(wait_for(lambda: not os.path.exists(ts)))
        text = "\n".join(logs.output)
        self.assertIn("running in the background so recording starts now", text)
        # Same per-file lines as before
        self.assertIn("Startup cleanup: Processing chanA_20260101_000000.ts", text)
        self.assertIn("Startup cleanup: Done with chanA_20260101_000000.mp4", text)
        self.assertIn("Startup cleanup: Processed 1 leftover file(s)", text)
        self.assertTrue(os.path.exists(processed_path(rec, "kick", "chanA", "chanA_20260101_000000.mp4")))
        self.assertTrue(os.path.exists(pending_path(rec, "kick", "chanA", "chanA_20260101_000000.ts")))

    def test_no_leftovers_launches_normally(self):
        rec = make_recorder(self.tmp)
        remux = FakeRemux()
        with self.assertLogs(level="INFO") as logs:
            with RunHarness(self, rec, remux) as h:
                self.assertTrue(wait_for(lambda: h.events.count("launch:chanA") == 1 and "launch:chanB" in h.events))
        self.assertIn("Startup cleanup: No leftover files found", "\n".join(logs.output))
        self.assertEqual(remux.calls, [])
        self.assertIsNone(rec._startup_cleanup_thread)

    def test_pattern_without_timestamp_cleans_up_first(self):
        # '{username}' could make a new recorder reuse a leftover's filename,
        # so that configuration keeps the old "clean up, then record" order.
        rec = make_recorder(self.tmp, pattern="{username}")
        make_ts(rec, "kick", "chanA", "chanA.ts")
        remux = FakeRemux()
        with self.assertLogs(level="INFO") as logs:
            with RunHarness(self, rec, remux) as h:
                self.assertTrue(wait_for(lambda: "launch:chanB" in h.events))
        self.assertLess(h.events.index("remux:end:chanA.ts"), h.events.index("launch:chanA"))
        self.assertIn("cleaning up BEFORE recording starts", "\n".join(logs.output))
        self.assertFalse(remux.calls[0]["low_priority"])  # legacy path is unchanged

    def test_unique_pattern_detection(self):
        for pattern, expected in (
            ("{username}_{timestamp}", True),
            ("{date}_{time}_{username}", True),
            ("{username}", False),
            ("{username}_{date}", False),
            ("{title}", False),
        ):
            rec = make_recorder(os.path.join(self.tmp, "p"), pattern=pattern)
            self.assertEqual(rec._recording_names_are_unique(), expected, pattern)

    # -- snapshot semantics -------------------------------------------------

    def test_snapshot_excludes_files_created_after_launch(self):
        rec = make_recorder(self.tmp)
        old = make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        self.assertEqual([e["raw_file"] for e in plan["files"]], [old])
        # A recorder starts after the snapshot and writes into the same folder.
        live = make_ts(rec, "kick", "chanA", "chanA_20260101_000500.ts", age_seconds=0)
        live_part = make_ts(rec, "kick", "chanA", "chanA_20260101_000500.part2.ts", age_seconds=0)
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._process_startup_leftovers(plan, background=True)
        self.assertEqual([os.path.basename(c["raw"]) for c in remux.calls], ["chanA_20260101_000000.ts"])
        self.assertTrue(os.path.exists(live) and os.path.exists(live_part))

    def test_snapshot_file_rewritten_after_launch_is_skipped(self):
        rec = make_recorder(self.tmp)
        victim = make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        # Something reopened that name after the snapshot (mtime moves forward).
        future = time.time() + 60
        os.utime(victim, (future, future))
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux), self.assertLogs(level="WARNING") as logs:
            rec._process_startup_leftovers(plan, background=True)
        self.assertEqual(remux.calls, [])
        self.assertTrue(os.path.exists(victim))
        self.assertIn("modified after startup", "\n".join(logs.output))

    def test_file_already_handled_by_other_cleanup_is_skipped(self):
        rec = make_recorder(self.tmp)
        gone = make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        os.remove(gone)  # e.g. BackgroundCleaner got there first
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._process_startup_leftovers(plan, background=True)
        self.assertEqual(remux.calls, [])

    def test_mp4_name_only_replaces_the_extension(self):
        # 'a.tsb.ts'.replace('.ts', '.mp4') used to give 'a.mp4b.mp4'.
        rec = make_recorder(self.tmp)
        make_ts(rec, "kick", "chanA", "a.tsb_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._process_startup_leftovers(plan, background=True)
        self.assertEqual(os.path.basename(remux.calls[0]["mp4"]), "a.tsb_20260101_000000.mp4")

    def test_distinct_leftovers_get_distinct_mp4_names(self):
        rec = make_recorder(self.tmp)
        names = ["betty_20260101_000000.ts", "betty_20260101_000000.part2.ts", "betty_20260101_000000.part3.ts"]
        for n in names:
            make_ts(rec, "kick", "betty", n)
        plan = rec._plan_startup_cleanup()
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._process_startup_leftovers(plan, background=True)
        outs = [os.path.basename(c["mp4"]) for c in remux.calls]
        self.assertEqual(len(outs), 3)
        self.assertEqual(len(set(outs)), 3)

    # -- ordering / priority / serialization ---------------------------------

    def test_remux_is_sequential_low_priority_and_cancellable(self):
        rec = make_recorder(self.tmp)
        for i in range(4):
            make_ts(rec, "kick", f"ch{i}", f"ch{i}_20260101_00000{i}.ts")
        plan = rec._plan_startup_cleanup()
        self.assertEqual(len(plan["files"]), 4)
        remux = FakeRemux(block=threading.Event())
        remux.block.set()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._launch_startup_cleanup(plan)
            rec._startup_cleanup_thread.join(timeout=10)
        self.assertEqual(len(remux.calls), 4)
        self.assertEqual(remux.max_in_flight, 1, "remuxes must run one at a time")
        self.assertTrue(all(c["low_priority"] for c in remux.calls))
        self.assertTrue(all(c["cancel_event"] is rec._startup_cleanup_cancel for c in remux.calls))
        self.assertFalse(_cleanup_lock.locked())

    def test_holds_shared_cleanup_lock_while_remuxing(self):
        rec = make_recorder(self.tmp)
        make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        release = threading.Event()
        remux = FakeRemux(block=release)
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._launch_startup_cleanup(plan)
            self.assertTrue(remux.entered.wait(10))
            # BackgroundCleaner / per-channel cleanup use the same lock, so
            # they cannot scan the file we are remuxing.
            self.assertTrue(_cleanup_lock.locked())
            release.set()
            rec._startup_cleanup_thread.join(timeout=10)
        self.assertFalse(_cleanup_lock.locked())

    def test_waits_for_other_cleanup_pass_then_proceeds(self):
        rec = make_recorder(self.tmp)
        make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        remux = FakeRemux()
        _cleanup_lock.acquire()  # e.g. a BackgroundCleaner pass from the previous session
        try:
            with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
                rec._launch_startup_cleanup(plan)
                time.sleep(0.3)
                self.assertEqual(remux.calls, [])
                _cleanup_lock.release()
                rec._startup_cleanup_thread.join(timeout=10)
        finally:
            if _cleanup_lock.locked():
                _cleanup_lock.release()
        self.assertEqual(len(remux.calls), 1)

    # -- same post-processing as before --------------------------------------

    def test_tiny_file_is_removed_and_big_one_processed(self):
        rec = make_recorder(self.tmp)
        rec.config.config.set("Recording", "min_file_size_mb", "1")
        tiny = make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts", size=1024)
        big = make_ts(rec, "kick", "chanA", "chanA_20260101_000100.ts", size=2 * 1024**2)
        plan = rec._plan_startup_cleanup()
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux), self.assertLogs(level="INFO") as logs:
            rec._process_startup_leftovers(plan, background=True)
        self.assertFalse(os.path.exists(tiny))
        self.assertIn("Startup cleanup: Removing tiny file chanA_20260101_000000.ts", "\n".join(logs.output))
        self.assertFalse(os.path.exists(big))
        self.assertTrue(os.path.exists(pending_path(rec, "kick", "chanA", "chanA_20260101_000100.ts")))
        self.assertEqual(len(remux.calls), 1)

    def test_failed_remux_moves_raw_to_pending_deletion(self):
        rec = make_recorder(self.tmp)
        make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        remux = FakeRemux(fail=True)
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux), self.assertLogs(level="INFO") as logs:
            rec._process_startup_leftovers(plan, background=True)
        text = "\n".join(logs.output)
        self.assertIn("Failed to process chanA_20260101_000000.ts", text)
        self.assertIn("Moved unprocessable chanA_20260101_000000.ts to PendingDeletion", text)
        self.assertTrue(os.path.exists(pending_path(rec, "kick", "chanA", "chanA_20260101_000000.ts")))

    def test_complete_existing_mp4_skips_remux(self):
        rec = make_recorder(self.tmp)
        make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts", size=2048)
        plan = rec._plan_startup_cleanup()
        out = processed_path(rec, "kick", "chanA", "chanA_20260101_000000.mp4")
        with open(out, "wb") as fh:
            fh.write(b"\0" * (6 * 1024**2))
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._process_startup_leftovers(plan, background=True)
        self.assertEqual(remux.calls, [])
        self.assertTrue(os.path.exists(pending_path(rec, "kick", "chanA", "chanA_20260101_000000.ts")))

    def test_partial_mp4_from_interrupted_remux_is_redone(self):
        # The app died mid-remux last time: marker + half an MP4 are on disk.
        rec = make_recorder(self.tmp)
        make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts", size=2048)
        plan = rec._plan_startup_cleanup()
        out = processed_path(rec, "kick", "chanA", "chanA_20260101_000000.mp4")
        with open(out, "wb") as fh:
            fh.write(b"\0" * (6 * 1024**2))
        with open(out + ".remuxing", "w"):
            pass
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._process_startup_leftovers(plan, background=True)
        self.assertEqual(len(remux.calls), 1)
        self.assertFalse(os.path.exists(out + ".remuxing"))

    # -- shutdown / failure isolation ----------------------------------------

    def test_stop_during_cleanup_leaves_the_ts_for_next_start(self):
        rec = make_recorder(self.tmp)
        first = make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        second = make_ts(rec, "kick", "chanB", "chanB_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        remux = FakeRemux(block=threading.Event(), honour_cancel=True)
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux), \
                mock.patch.object(recorder_mod, "kill_orphan_recording_processes", lambda *a, **k: None):
            rec.is_running = True
            rec._launch_startup_cleanup(plan)
            self.assertTrue(remux.entered.wait(10))
            started = [os.path.basename(c["raw"]) for c in remux.calls]
            with self.assertLogs(level="INFO") as logs:
                rec.stop()                      # user clicks Stop / closes the app
            self.assertFalse(rec._startup_cleanup_thread.is_alive())
        # Nothing was deleted or moved; the half-written MP4 and marker are gone.
        self.assertTrue(os.path.exists(first) and os.path.exists(second))
        self.assertEqual(len(started), 1, "second file must not start after cancel")
        for ch in ("chanA", "chanB"):
            d = os.path.join(rec.processed_base, "kick", ch)
            self.assertEqual(os.listdir(d), [])
            self.assertEqual(os.listdir(os.path.join(rec.pending_base, "kick", ch)), [])
        self.assertIn("left in place for the next pass", "\n".join(logs.output))
        self.assertTrue(rec.cleaner.started)  # post-stop BackgroundCleaner still runs
        self.assertFalse(_cleanup_lock.locked())

    def test_next_run_retries_files_left_by_a_cancelled_pass(self):
        rec = make_recorder(self.tmp)
        ts = make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        rec._startup_cleanup_cancel.set()
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._process_startup_leftovers(plan, background=True)
        self.assertTrue(os.path.exists(ts))
        self.assertEqual(remux.calls, [])
        plan2 = rec._plan_startup_cleanup()        # next start: cancel flag reset
        self.assertFalse(rec._startup_cleanup_cancel.is_set())
        self.assertEqual([e["raw_file"] for e in plan2["files"]], [ts])

    def test_cleanup_crash_does_not_affect_recorders(self):
        rec = make_recorder(self.tmp)
        ts = make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")

        def boom(*a, **k):
            raise RuntimeError("ffmpeg exploded")

        remux = FakeRemux()
        with self.assertLogs(level="ERROR") as logs:
            with RunHarness(self, rec, remux) as h:
                with mock.patch.object(recorder_mod, "remux_to_mp4", boom):
                    self.assertTrue(wait_for(lambda: "launch:chanB" in h.events))
                    self.assertTrue(wait_for(lambda: not rec._startup_cleanup_thread.is_alive()))
                self.assertTrue(rec.is_running)
                self.assertTrue(h.thread.is_alive(), "run() loop must keep going")
        self.assertIn("ffmpeg exploded", "\n".join(logs.output))
        self.assertTrue(os.path.exists(ts), "raw file kept when its remux crashed")

    def test_background_thread_failure_is_logged_not_raised(self):
        rec = make_recorder(self.tmp)
        with mock.patch.object(rec, "_process_startup_leftovers", side_effect=RuntimeError("kaboom")):
            with self.assertLogs(level="ERROR") as logs:
                rec._startup_cleanup_thread_main({"files": [], "snapshot_time": time.time()})
        self.assertIn("background pass failed", "\n".join(logs.output))
        self.assertIn("kaboom", "\n".join(logs.output))

    def test_scan_failure_does_not_block_recording(self):
        rec = make_recorder(self.tmp)
        with mock.patch.object(rec, "_snapshot_leftover_ts_files", side_effect=OSError("disk gone")):
            with self.assertLogs(level="WARNING") as logs:
                plan = rec._plan_startup_cleanup()
        self.assertEqual(plan["files"], [])
        self.assertIn("could not scan", "\n".join(logs.output))

    def test_abandoned_mid_write_ts_is_processed_like_before(self):
        # The .ts that was open when the window was closed: partial, no
        # trailing metadata, mtime = crash time.  It is a plain leftover.
        rec = make_recorder(self.tmp)
        crashed = make_ts(rec, "kick", "vimp", "vimp_20260101_233000.ts", size=4096, age_seconds=180)
        plan = rec._plan_startup_cleanup()
        self.assertEqual([e["raw_file"] for e in plan["files"]], [crashed])
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._process_startup_leftovers(plan, background=True)
        self.assertEqual(len(remux.calls), 1)
        self.assertTrue(os.path.exists(processed_path(rec, "kick", "vimp", "vimp_20260101_233000.mp4")))
        self.assertTrue(os.path.exists(pending_path(rec, "kick", "vimp", "vimp_20260101_233000.ts")))

    def test_second_run_waits_for_previous_background_pass(self):
        rec = make_recorder(self.tmp)
        make_ts(rec, "kick", "chanA", "chanA_20260101_000000.ts")
        plan = rec._plan_startup_cleanup()
        remux = FakeRemux(block=threading.Event(), honour_cancel=True)
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            rec._launch_startup_cleanup(plan)
            self.assertTrue(remux.entered.wait(10))
            plan2 = rec._plan_startup_cleanup()   # session restarted quickly
            self.assertFalse(rec._startup_cleanup_thread.is_alive())
        # The cancelled file is picked up again by the new snapshot.
        self.assertEqual(len(plan2["files"]), 1)


class RemuxPriorityAndCancelTests(unittest.TestCase):
    """worker.remux_to_mp4 / _run_remux_cmd: low priority + cancel (fake ffmpeg)."""

    class _Lines:
        """Iterable of ffmpeg -progress lines with a close() hook."""
        def __init__(self, lines, on_line=None):
            self._lines, self._on_line = list(lines), on_line
            self.consumed = 0

        def __iter__(self):
            for line in self._lines:
                self.consumed += 1
                if self._on_line:
                    self._on_line(line)
                yield line

        def close(self):
            pass

    class _FakeProc:
        def __init__(self, stdout):
            self.stdout = stdout
            self.stderr = RemuxPriorityAndCancelTests._Lines([])
            self.returncode = 0
            self.killed = False

        def kill(self):
            self.killed = True
            self.returncode = -9

        def wait(self, timeout=None):
            return self.returncode

    def _run(self, lines=None, on_line=None, **kwargs):
        lines = lines if lines is not None else ["out_time_us=1000\n", "progress=continue\n"] * 3
        popen_calls = []
        holder = {}

        def fake_popen(cmd, **kw):
            popen_calls.append(kw)
            holder["proc"] = self._FakeProc(self._Lines(lines, on_line))
            return holder["proc"]

        with mock.patch.object(worker_mod.subprocess, "Popen", fake_popen), \
                mock.patch.object(worker_mod, "_probe_duration", lambda *a, **k: None):
            result = worker_mod._run_remux_cmd(
                ["ffmpeg", "-i", "in.ts", "out.mp4", "-y"], "ffmpeg", "in.ts", "out.mp4",
                logging.getLogger(), 30, 0, **kwargs)
        return result, popen_calls, holder["proc"]

    def test_default_is_normal_priority(self):
        (code, _), popen_calls, _ = self._run()
        self.assertEqual(code, 0)
        self.assertNotIn("preexec_fn", popen_calls[0])
        if os.name == "nt":
            self.assertEqual(popen_calls[0]["creationflags"], 0x08000000)  # CREATE_NO_WINDOW only
        else:
            self.assertNotIn("creationflags", popen_calls[0])

    def test_low_priority_sets_below_normal(self):
        (code, _), popen_calls, _ = self._run(low_priority=True)
        self.assertEqual(code, 0)
        if os.name == "nt":
            # BELOW_NORMAL_PRIORITY_CLASS, and no console window for ffmpeg (see tests/test_no_console.py)
            self.assertEqual(popen_calls[0]["creationflags"], 0x00004000 | 0x08000000)
        else:
            self.assertTrue(callable(popen_calls[0]["preexec_fn"]))

    def test_cancel_kills_ffmpeg_and_reports_cancelled(self):
        cancel = threading.Event()
        seen = []

        def on_line(line):
            seen.append(line)
            if len(seen) == 2:
                cancel.set()

        (code, _), _, proc = self._run(cancel_event=cancel, on_line=on_line)
        self.assertEqual(code, worker_mod.REMUX_CANCELLED)
        self.assertTrue(proc.killed)
        self.assertLessEqual(len(seen), 3, "must stop reading once cancelled")

    def test_remux_to_mp4_returns_cancelled_without_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = os.path.join(tmp, "x.ts")
            with open(raw, "wb") as fh:
                fh.write(b"\0" * 1024)
            calls = []

            def fake_run(*a, **k):
                calls.append(k)
                return worker_mod.REMUX_CANCELLED, ""

            with mock.patch.object(worker_mod, "_run_remux_cmd", fake_run), \
                    mock.patch.object(worker_mod.subprocess, "run", side_effect=OSError("no ffmpeg")):
                ok, size, err = worker_mod.remux_to_mp4(
                    raw, os.path.join(tmp, "x.mp4"), "ffmpeg", logging.getLogger(), 30,
                    low_priority=True, cancel_event=threading.Event())
        self.assertEqual((ok, size, err), (False, 0, "cancelled"))
        self.assertEqual(len(calls), 1, "no fallback remux after a cancel")
        self.assertTrue(calls[0]["low_priority"])

    def test_remux_to_mp4_defaults_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = os.path.join(tmp, "x.ts")
            with open(raw, "wb") as fh:
                fh.write(b"\0" * 1024)
            seen = {}

            def fake_run(*a, **k):
                seen.update(k)
                out = os.path.join(tmp, "x.mp4")
                with open(out, "wb") as fh:
                    fh.write(b"\0" * (6 * 1024**2))
                return 0, ""

            with mock.patch.object(worker_mod, "_run_remux_cmd", fake_run), \
                    mock.patch.object(worker_mod.subprocess, "run", side_effect=OSError("no ffmpeg")):
                ok, size, err = worker_mod.remux_to_mp4(
                    raw, os.path.join(tmp, "x.mp4"), "ffmpeg", logging.getLogger(), 30)
        self.assertTrue(ok)
        self.assertFalse(seen["low_priority"])
        self.assertIsNone(seen["cancel_event"])


if __name__ == "__main__":
    unittest.main()