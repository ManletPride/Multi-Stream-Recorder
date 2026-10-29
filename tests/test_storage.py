"""A recordings drive that disappears while MSR runs: detect it, keep recording elsewhere, move it back.

Everything uses temp dirs. An unreachable folder is a path *under a regular file* (the real
makedirs/open failures, no fake I/O) and "the drive comes back" is removing that file and making the
folder. Windows error codes (3, 21, 1167...) are exercised through constructed exceptions. Nothing
touches a real drive, recording or data folder, and no real capture tool runs.
"""
import contextlib
import errno
import logging
import os
import queue
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import msr.recorder as recorder_mod  # noqa: E402
import msr.worker as worker_mod  # noqa: E402
from msr import storage  # noqa: E402
from msr.config import Config, new_parser  # noqa: E402
from msr.recorder import StreamRecorder  # noqa: E402
from tests.test_startup_cleanup import FakeConfig  # noqa: E402


def winerr(code, cls=OSError):
    exc = cls(errno.ENOENT, f"fake windows error {code}")
    exc.winerror = code
    return exc


class Drive:
    """A recordings folder that can be 'unplugged' (a regular file takes its place) and 'plugged'."""

    def __init__(self, tmp, name="primary"):
        self.root = os.path.join(tmp, name)
        self.path = os.path.join(self.root, "streams")
        self.plug()

    def unplug(self):
        if os.path.isdir(self.root):
            import shutil
            shutil.rmtree(self.root)
        with open(self.root, "w") as fh:
            fh.write("not a folder")

    def plug(self):
        if os.path.isfile(self.root):
            os.remove(self.root)
        os.makedirs(self.path, exist_ok=True)


def old(path, seconds=3600):
    t = time.time() - seconds
    os.utime(path, (t, t))


def put(base, rel, text="x" * 100, age=3600):
    path = os.path.join(base, *rel.split("/"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(text)
    old(path, age)
    return path


class ClassifyTests(unittest.TestCase):
    def test_windows_codes(self):
        for code, kind in ((3, "missing"), (53, "missing"), (21, "offline"), (1167, "offline"),
                           (1117, "offline"), (5, "denied"), (112, "full")):
            self.assertEqual(storage.classify(winerr(code)), kind, code)

    def test_errnos(self):
        self.assertEqual(storage.classify(OSError(errno.ENOSPC, "x")), "full")
        self.assertEqual(storage.classify(OSError(errno.EIO, "x")), "offline")
        self.assertEqual(storage.classify(PermissionError(errno.EACCES, "x")), "denied")
        self.assertEqual(storage.classify(FileNotFoundError(errno.ENOENT, "x")), "missing")
        self.assertEqual(storage.classify(ConnectionResetError(errno.ECONNRESET, "x")), "error")

    def test_is_drive_error_needs_the_folder_to_be_gone_too(self):
        with tempfile.TemporaryDirectory() as tmp:
            drive = Drive(tmp)
            self.assertFalse(storage.is_drive_error(winerr(3, FileNotFoundError), drive.path))
            drive.unplug()
            self.assertTrue(storage.is_drive_error(winerr(3, FileNotFoundError), drive.path))
            self.assertFalse(storage.is_drive_error(ConnectionResetError(errno.ECONNRESET, "x"), drive.path))


class ChooseRootTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.drive = Drive(self.tmp.name)
        self.fallback = os.path.join(self.tmp.name, "fallback")

    def test_main_folder_wins_when_reachable(self):
        c = storage.choose_root(self.drive.path, self.fallback, 0)
        self.assertEqual((c.path, c.ok, c.on_fallback), (self.drive.path, True, False))
        self.assertFalse(os.path.exists(self.fallback), "the fallback folder is not created needlessly")

    def test_unreachable_main_uses_the_fallback(self):
        self.drive.unplug()
        c = storage.choose_root(self.drive.path, self.fallback, 0)
        self.assertEqual((c.path, c.ok, c.on_fallback), (self.fallback, True, True))
        self.assertTrue(c.reason)
        self.assertTrue(os.path.isdir(self.fallback))

    def test_drive_back_means_main_again(self):
        self.drive.unplug()
        self.assertTrue(storage.choose_root(self.drive.path, self.fallback, 0).on_fallback)
        self.drive.plug()
        self.assertFalse(storage.choose_root(self.drive.path, self.fallback, 0).on_fallback)

    def test_low_space_on_a_reachable_main_does_not_switch(self):
        c = storage.choose_root(self.drive.path, self.fallback, 5, free=lambda p: 1.0)
        self.assertEqual((c.ok, c.low_space, c.on_fallback, c.path), (False, True, False, self.drive.path))

    def test_nothing_usable_when_fallback_is_off(self):
        self.drive.unplug()
        c = storage.choose_root(self.drive.path, None, 0)
        self.assertEqual((c.ok, c.path), (False, None))
        self.assertTrue(c.reason)

    def test_fallback_that_is_also_broken(self):
        self.drive.unplug()
        blocker = os.path.join(self.tmp.name, "blocker")
        Path(blocker).write_text("x")
        c = storage.choose_root(self.drive.path, os.path.join(blocker, "fb"), 0)
        self.assertFalse(c.ok)
        self.assertIn("fallback", c.reason)

    def test_fallback_with_too_little_space_is_not_used(self):
        self.drive.unplug()
        space = {self.fallback: 0.5}
        c = storage.choose_root(self.drive.path, self.fallback, 5, free=lambda p: space.get(p, 100.0))
        self.assertFalse(c.ok)
        self.assertIn("0.5 GB", c.reason)

    def test_fallback_inside_the_main_folder_is_ignored(self):
        self.drive.unplug()
        c = storage.choose_root(self.drive.path, os.path.join(self.drive.path, "fb"), 0)
        self.assertFalse(c.ok)

    def test_fallback_root_setting(self):
        self.assertIsNone(storage.fallback_root("off"))
        self.assertIsNone(storage.fallback_root(" OFF "))
        self.assertEqual(storage.fallback_root(self.fallback), os.path.abspath(self.fallback))
        default = storage.fallback_root("")
        self.assertTrue(default.startswith(os.path.abspath(tempfile.gettempdir())))
        self.assertEqual(default, storage.fallback_root(None))


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.fb = os.path.join(self.tmp.name, "fb")
        self.main = os.path.join(self.tmp.name, "main")
        os.makedirs(self.main)
        self.log = logging.getLogger("test-reconcile")

    def test_files_move_with_their_relative_path(self):
        put(self.fb, "Processed/twitch/a/a_1.mp4", "mp4data")
        put(self.fb, "Recorded/twitch/a/a_2.ts", "tsdata")
        r = storage.reconcile_fallback(self.fb, self.main, self.log)
        self.assertEqual((r.moved, r.young, r.failed), (2, 0, 0))
        self.assertEqual(Path(self.main, "Processed", "twitch", "a", "a_1.mp4").read_text(), "mp4data")
        self.assertEqual(r.moved_ts, [os.path.join(self.main, "Recorded", "twitch", "a", "a_2.ts")])
        self.assertFalse(storage.has_files(self.fb))

    def test_a_file_that_may_still_be_written_is_left_alone(self):
        src = put(self.fb, "Recorded/twitch/a/live.ts", "growing", age=5)
        r = storage.reconcile_fallback(self.fb, self.main, self.log)
        self.assertEqual((r.moved, r.young), (0, 1))
        self.assertTrue(os.path.exists(src))
        self.assertFalse(os.path.exists(os.path.join(self.main, "Recorded")))

    def test_never_overwrites_a_different_file(self):
        put(self.main, "Processed/twitch/a/x.mp4", "MAIN-version")
        put(self.fb, "Processed/twitch/a/x.mp4", "FALLBACK-version-longer")
        r = storage.reconcile_fallback(self.fb, self.main, self.log)
        self.assertEqual(r.moved, 1)
        self.assertEqual(Path(self.main, "Processed/twitch/a/x.mp4").read_text(), "MAIN-version")
        self.assertEqual(Path(self.main, "Processed/twitch/a/x.from-fallback.mp4").read_text(),
                         "FALLBACK-version-longer")

    def test_an_earlier_verified_copy_just_drops_the_source(self):
        src = put(self.fb, "Processed/twitch/a/x.mp4", "same")
        dest = put(self.main, "Processed/twitch/a/x.mp4", "same")
        os.utime(dest, (os.path.getmtime(src),) * 2)
        r = storage.reconcile_fallback(self.fb, self.main, self.log)
        self.assertEqual(r.moved, 1)
        self.assertFalse(os.path.exists(src))
        self.assertEqual(os.listdir(os.path.dirname(dest)), ["x.mp4"])

    def test_a_failed_copy_keeps_the_source_and_leaves_no_partial_file(self):
        src = put(self.fb, "Processed/twitch/a/x.mp4", "data")
        with mock.patch.object(storage.shutil, "copy2", side_effect=OSError(errno.ENOSPC, "disk full")):
            r = storage.reconcile_fallback(self.fb, self.main, self.log)
        self.assertEqual((r.moved, r.failed), (0, 1))
        self.assertTrue(os.path.exists(src))
        names = [n for _d, _s, files in os.walk(self.main) for n in files]
        self.assertEqual(names, [])

    def test_a_short_copy_is_rejected(self):
        src = put(self.fb, "Processed/twitch/a/x.mp4", "0123456789")

        def short_copy(s, d):
            Path(d).write_text("0123")
        with mock.patch.object(storage.shutil, "copy2", side_effect=short_copy):
            r = storage.reconcile_fallback(self.fb, self.main, self.log)
        self.assertEqual((r.moved, r.failed), (0, 1))
        self.assertTrue(os.path.exists(src))
        self.assertFalse([n for _d, _s, files in os.walk(self.main) for n in files])

    def test_source_that_cannot_be_removed_is_kept_and_not_counted_as_moved(self):
        src = put(self.fb, "Processed/twitch/a/x.mp4", "data")
        real_remove = os.remove

        def locked(path):
            if path == src:
                raise PermissionError(errno.EACCES, "in use")
            real_remove(path)
        with mock.patch.object(storage.os, "remove", side_effect=locked):
            r = storage.reconcile_fallback(self.fb, self.main, self.log)
        self.assertEqual((r.moved, r.failed), (0, 1))
        self.assertTrue(os.path.exists(src))
        self.assertTrue(os.path.exists(os.path.join(self.main, "Processed/twitch/a/x.mp4")))
        # the next pass sees the verified copy and finishes the job
        r = storage.reconcile_fallback(self.fb, self.main, self.log)
        self.assertEqual(r.moved, 1)
        self.assertFalse(os.path.exists(src))

    def test_stop_event_ends_the_pass(self):
        put(self.fb, "Processed/twitch/a/x.mp4")
        stop = threading.Event()
        stop.set()
        r = storage.reconcile_fallback(self.fb, self.main, self.log, should_stop=stop)
        self.assertEqual(r.moved, 0)
        self.assertTrue(storage.has_files(self.fb))

    def test_has_files_ignores_probe_and_partial_files(self):
        put(self.fb, storage.PROBE_NAME)
        put(self.fb, "Processed/x.mp4" + storage.PART_SUFFIX)
        self.assertFalse(storage.has_files(self.fb))
        self.assertFalse(storage.has_files(os.path.join(self.tmp.name, "nope")))


class WriteWarningTests(unittest.TestCase):
    def setUp(self):
        storage._failing.clear()
        self.addCleanup(storage._failing.clear)
        self.log = logging.getLogger("test-write-warning")

    def test_one_warning_then_quiet_then_one_recovery_line(self):
        t = [1000.0]
        with self.assertLogs(self.log, "WARNING") as cm:
            for _ in range(5):
                storage.write_failed("config.ini", "C:/x/config.ini", PermissionError(errno.EACCES, "x"),
                                     logger=self.log, now=lambda: t[0])
                t[0] += 5
        self.assertEqual(len(cm.records), 1)
        self.assertIn("config.ini", cm.output[0])
        self.assertNotIn("PermissionError", cm.output[0])
        t[0] += storage.WARN_EVERY
        with self.assertLogs(self.log, "WARNING") as cm:
            storage.write_failed("config.ini", "C:/x/config.ini", OSError(errno.EIO, "x"),
                                 logger=self.log, now=lambda: t[0])
        self.assertEqual(len(cm.records), 1)
        with self.assertLogs(self.log, "INFO") as cm:
            self.assertTrue(storage.write_ok("config.ini", "C:/x/config.ini", logger=self.log))
        self.assertIn("works again", cm.output[0])
        self.assertFalse(storage.write_ok("config.ini", "C:/x/config.ini", logger=self.log))


class ConfigWriteTests(unittest.TestCase):
    def setUp(self):
        storage._failing.clear()
        self.addCleanup(storage._failing.clear)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "config.ini")
        Path(self.path).write_text("[Recording]\nquality = 720p\n[Fishtank]\npassword = P@ss%word\n",
                                   encoding="utf-8")
        self.cfg = Config(self.path)

    def test_default_fallback_dir_is_blank(self):
        self.assertEqual(self.cfg.get("Paths", "fallback_dir"), "")
        self.assertEqual(new_parser().get("Paths", "fallback_dir", fallback="x"), "x")
        self.assertIn("fallback_dir", Path(self.path).read_text())

    def test_save_writes_in_one_step_and_reports_success(self):
        self.cfg.config.set("Recording", "quality", "1080p")
        self.assertTrue(self.cfg.save())
        text = Path(self.path).read_text(encoding="utf-8")
        self.assertIn("quality = 1080p", text)
        self.assertIn("password = P@ss%word", text)
        self.assertEqual([n for n in os.listdir(self.tmp.name) if n.endswith(".tmp")], [])

    def test_a_failing_disk_is_a_warning_not_a_crash_and_the_file_is_untouched(self):
        before = Path(self.path).read_bytes()
        self.cfg.config.set("Recording", "quality", "1080p")
        with mock.patch("msr.config.history.atomic_write", side_effect=winerr(21, PermissionError)):
            with self.assertLogs(level="WARNING") as cm:
                self.assertFalse(self.cfg.save())
                self.assertFalse(self.cfg.save())          # a second failure stays quiet
        self.assertEqual(Path(self.path).read_bytes(), before)
        self.assertEqual(len([o for o in cm.output if "config.ini" in o]), 1)
        self.assertEqual(self.cfg.get("Recording", "quality"), "1080p", "settings stay in memory")
        self.assertTrue(self.cfg.save(), "it works again once the disk does")
        self.assertIn("quality = 1080p", Path(self.path).read_text())

    def test_a_crash_between_temp_and_replace_leaves_the_old_file(self):
        before = Path(self.path).read_bytes()
        with mock.patch("msr.history.os.replace", side_effect=OSError(errno.EIO, "unplugged")):
            self.assertFalse(self.cfg.save())
        self.assertEqual(Path(self.path).read_bytes(), before)
        self.assertEqual([n for n in os.listdir(self.tmp.name) if n.endswith(".tmp")], [])


class OtherWritesTests(unittest.TestCase):
    def setUp(self):
        storage._failing.clear()
        self.addCleanup(storage._failing.clear)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.blocker = os.path.join(self.tmp.name, "blocker")
        Path(self.blocker).write_text("x")
        self.bad = os.path.join(self.blocker, "sub", "file.json")

    def failing_lines(self, what):
        return [r for r in self.logged.records if what in r.getMessage()]

    def test_library_action_log_and_undo_list(self):
        from msr import library_actions as la
        with self.assertLogs(level="WARNING") as self.logged:
            la.ActionLog(self.bad).write(event="x")
            la.ActionLog(self.bad).write(event="y")
            m = la.Manifest(self.bad)
            m._save([{"a": 1}])
        self.assertEqual(len(self.failing_lines("action log")), 1)
        self.assertEqual(len(self.failing_lines("undo list")), 1)

    def test_library_cache(self):
        from msr import library
        cache = library.DurationCache(self.bad)
        cache.dirty = True
        with self.assertLogs(level="WARNING") as self.logged:
            cache.save()
        self.assertEqual(len(self.failing_lines("Library cache")), 1)
        self.assertTrue(cache.dirty, "it is retried on the next save")

    def test_window_state(self):
        from msr import winstate
        with self.assertLogs(level="WARNING") as self.logged:
            winstate.merge(self.bad, {"a": 1})
            winstate.merge(self.bad, {"a": 2})
        self.assertEqual(len(self.failing_lines("window state")), 1)

    def test_gui_window_state_helper(self):
        from msr import gui
        with self.assertLogs(level="WARNING") as self.logged:
            gui.save_window_state(self.bad, {"a": 1})
        self.assertEqual(len(self.failing_lines("window state")), 1)

    def test_io_meter_on_a_missing_drive_does_not_raise(self):
        from msr.iometer import IoSampler
        IoSampler(self.bad).sample()


class Harness:
    """record_worker run in a thread with the live check and the capture tool faked."""

    def __init__(self, test, primary, fallback_setting, user="testuser"):
        self.test = test
        self.tmp = os.path.dirname(os.path.dirname(primary))
        self.primary = primary
        self.stop = threading.Event()
        self.status = queue.Queue()
        self.sleeps = []
        self.cmds = []
        self.log = logging.getLogger(f"test-worker-{id(self)}")
        self.log.setLevel(logging.INFO)
        self.log.propagate = False
        self.records = []
        handler = logging.Handler()
        handler.emit = lambda r: self.records.append(r.getMessage())
        self.log.addHandler(handler)
        self.config = {s: dict(v) for s, v in Config.DEFAULT_CONFIG.items()}
        self.config["Paths"].update(streams_dir=primary, fallback_dir=fallback_setting,
                                    channels_file=os.path.join(self.tmp, "channels.json"))
        self.config["Recording"]["min_disk_space_gb"] = "0"
        self.key = f"twitch:{user}"
        self.on_capture = lambda n, cmd: None

    def popen(self, cmd, *a, **k):
        self.cmds.append(" ".join(str(c) for c in cmd))
        self.on_capture(len(self.cmds), cmd)
        raise RuntimeError("fake capture tool")

    def interruptible(self, seconds, wake, stop):
        self.sleeps.append(("wait", seconds))
        self.stop.set()
        return False

    def run(self):
        args = (self.key, self.config, self.stop, self.status, threading.Event(), {})
        patches = [
            mock.patch.object(worker_mod, "setup_child_logging", return_value=self.log),
            mock.patch.object(worker_mod, "check_stream_streamlink", return_value=(True, "title", None)),
            mock.patch.object(worker_mod.subprocess, "Popen", self.popen),
            mock.patch.object(worker_mod, "interruptible_sleep", self.interruptible),
            mock.patch.object(worker_mod.time, "sleep", lambda s: self.sleeps.append(("sleep", s))),
            mock.patch.object(worker_mod, "find_cookies_file", return_value=None),
            mock.patch.object(worker_mod.random, "uniform", return_value=0.0),   # no start-up stagger
        ]
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            t = threading.Thread(target=worker_mod.record_worker, args=(args,), daemon=True)
            t.start()
            t.join(30)
            self.test.assertFalse(t.is_alive(), "the worker did not stop")
        return self

    def statuses(self):
        out = []
        while not self.status.empty():
            out.append(self.status.get())
        return out


class WorkerFallbackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.drive = Drive(self.tmp.name)
        self.fb = os.path.join(self.tmp.name, "fallback")
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.tmp.name)

    def rec_dir(self, base):
        return os.path.join(base, "Recorded", "twitch", "testuser")

    def test_healthy_drive_records_to_the_main_folder_only(self):
        h = Harness(self, self.drive.path, self.fb)
        h.on_capture = lambda n, cmd: h.stop.set()
        h.run()
        self.assertTrue(os.path.isdir(self.rec_dir(self.drive.path)))
        self.assertFalse(os.path.exists(self.fb))
        self.assertIn(self.drive.path, h.cmds[0])

    def test_unplugged_drive_records_to_the_fallback_folder(self):
        self.drive.unplug()
        h = Harness(self, self.drive.path, self.fb)
        h.on_capture = lambda n, cmd: h.stop.set()
        h.run()
        self.assertTrue(os.path.isdir(self.rec_dir(self.fb)))
        self.assertIn(os.path.join(self.fb, "Recorded"), h.cmds[0])
        self.assertTrue(any("fallback folder" in m and "not available" in m for m in h.records), h.records)

    def test_drive_returning_switches_back_for_the_next_recording(self):
        self.drive.unplug()
        h = Harness(self, self.drive.path, self.fb)

        def capture(n, cmd):
            if n == 1:
                self.drive.plug()
            else:
                h.stop.set()
        h.on_capture = capture
        h.run()
        self.assertIn(os.path.join(self.fb, "Recorded"), h.cmds[0])
        self.assertIn(os.path.join(self.drive.path, "Recorded"), h.cmds[1])
        self.assertTrue(any("available again" in m for m in h.records), h.records)
        # one line per change, not one per check
        self.assertEqual(len([m for m in h.records if "not available" in m]), 1)

    def test_main_drive_lost_between_recordings_switches_to_the_fallback(self):
        h = Harness(self, self.drive.path, self.fb)

        def capture(n, cmd):
            if n == 1:
                self.drive.unplug()
            else:
                h.stop.set()
        h.on_capture = capture
        h.run()
        self.assertIn(os.path.join(self.drive.path, "Recorded"), h.cmds[0])
        self.assertIn(os.path.join(self.fb, "Recorded"), h.cmds[1])

    def test_without_a_fallback_the_worker_waits_and_says_why(self):
        self.drive.unplug()
        h = Harness(self, self.drive.path, "off")
        h.run()
        self.assertEqual(h.cmds, [])
        self.assertFalse(os.path.exists(self.fb))
        self.assertEqual(h.sleeps, [("wait", 60)], "looks again after a minute")
        status = [s for _key, s in h.statuses()]
        self.assertEqual(status[-1]["status"], "Error")
        self.assertEqual(status[-1]["detail"], "drive not available")
        self.assertEqual(len([m for m in h.records if "waiting for it to return" in m]), 1)

    def test_losing_the_drive_under_a_recording_is_not_a_crash_with_a_long_backoff(self):
        h = Harness(self, self.drive.path, "off")

        def capture(n, cmd):
            self.drive.unplug()
            raise winerr(3, FileNotFoundError)
        h.on_capture = capture
        h.run()
        waits = [s for kind, s in h.sleeps if kind == "sleep"]
        self.assertEqual(waits, [5], "no 60 s crash back-off")
        self.assertTrue(any("drive was lost" in m for m in h.records), h.records)
        self.assertFalse(any("Worker crashed" in m for m in h.records))

    def test_the_same_error_with_a_healthy_drive_is_still_a_crash(self):
        h = Harness(self, self.drive.path, "off")

        def capture(n, cmd):
            h.stop.set()
            raise winerr(3, FileNotFoundError)
        h.on_capture = capture
        h.run()
        self.assertTrue(any("Worker crashed" in m for m in h.records))
        self.assertIn(("sleep", 60), h.sleeps)


class RecorderWatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.drive = Drive(self.tmp.name)
        self.fb = os.path.join(self.tmp.name, "fallback")

    def recorder(self, fallback):
        rec = object.__new__(StreamRecorder)
        rec.config = FakeConfig(self.drive.path)
        rec.root_path = self.drive.path
        rec.fallback_root = fallback
        rec.fallback_recorded_base = os.path.join(fallback, "Recorded") if fallback else None
        rec.should_stop = threading.Event()
        return rec

    def test_states_are_reported_once_and_cleared_on_return(self):
        rec = self.recorder(self.fb)
        with mock.patch.object(rec, "_start_fallback_return") as back, self.assertLogs(level="INFO") as cm:
            rec._storage_tick(force=True)
            self.assertEqual(rec.storage_notice(), "")
            self.drive.unplug()
            rec._storage_tick(force=True)
            rec._storage_tick(force=True)
            self.assertIn("Recording to", rec.storage_notice())
            self.assertIn(self.fb, rec.storage_notice())
            back.assert_not_called()
            self.drive.plug()
            rec._storage_tick(force=True)
            self.assertEqual(rec.storage_notice(), "")
            back.assert_called_once()
        self.assertEqual(len([o for o in cm.output if "not available" in o]), 1)
        self.assertEqual(len([o for o in cm.output if "available again" in o]), 1)

    def test_without_fallback_the_notice_says_recording_is_paused(self):
        rec = self.recorder(None)
        self.drive.unplug()
        with self.assertLogs(level="WARNING"):
            rec._storage_tick(force=True)
        self.assertIn("paused", rec.storage_notice())

    def test_the_check_is_rate_limited(self):
        rec = self.recorder(self.fb)
        rec._storage_tick(force=True)
        self.drive.unplug()
        with self.assertNoLogs(level="WARNING"):
            rec._storage_tick()            # within STORAGE_CHECK_SECONDS: not looked at yet
        self.assertEqual(rec.storage_notice(), "")

    def test_files_left_in_the_fallback_are_picked_up_later(self):
        rec = self.recorder(self.fb)
        put(self.fb, "Processed/twitch/a/x.mp4")
        with mock.patch.object(rec, "_start_fallback_return") as back:
            rec._storage_tick(force=True)
        back.assert_called_once()

    def test_move_back_runs_the_leftover_pass_for_unfinished_ts_files(self):
        rec = self.recorder(self.fb)
        put(self.fb, "Processed/twitch/a/x.mp4", "mp4")
        put(self.fb, "Recorded/twitch/a/y.ts", "ts")
        with mock.patch.object(rec, "_plan_startup_cleanup", return_value="plan") as plan, \
                mock.patch.object(rec, "_launch_startup_cleanup") as launch:
            rec._fallback_return_main()
        self.assertTrue(os.path.isfile(os.path.join(self.drive.path, "Processed/twitch/a/x.mp4")))
        self.assertTrue(os.path.isfile(os.path.join(self.drive.path, "Recorded/twitch/a/y.ts")))
        plan.assert_called_once()
        launch.assert_called_once_with("plan")

    def test_move_back_never_raises(self):
        rec = self.recorder(self.fb)
        put(self.fb, "Processed/x.mp4")
        with mock.patch.object(storage, "reconcile_fallback", side_effect=RuntimeError("boom")):
            with self.assertLogs(level="ERROR"):
                rec._fallback_return_main()

    def test_session_can_start_while_the_drive_is_gone(self):
        self.drive.unplug()
        fake_manager = mock.MagicMock()
        with mock.patch.object(recorder_mod.mp, "Manager", return_value=fake_manager), \
                mock.patch.object(recorder_mod, "KickPushListener"), \
                mock.patch.object(recorder_mod, "BackgroundCleaner"):
            with self.assertLogs(level="WARNING") as cm:
                rec = StreamRecorder(["chanA"], FakeConfig(self.drive.path))
        self.assertTrue(any("not available right now" in o for o in cm.output))
        self.assertEqual(rec.fallback_root, storage.fallback_root(""))

    def test_clip_finder_looks_in_the_fallback_too(self):
        rec = self.recorder(self.fb)
        rec.recorded_base = os.path.join(self.drive.path, "Recorded")
        self.drive.unplug()
        live = put(self.fb, "Recorded/kick/someone/live.ts", age=1)
        self.assertEqual(worker_mod.find_active_recording_file(rec, "someone"), live)


class StatusTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "nt", "drive letters")
    def test_missing_drive_is_reported_without_creating_anything(self):
        from msr import status
        import string
        letter = next((c for c in reversed(string.ascii_uppercase) if not os.path.exists(f"{c}:\\")), None)
        if letter is None:
            self.skipTest("no free drive letter")
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "config.ini").write_text(f"[Paths]\nstreams_dir = {letter}:\\rec\n")
            old = os.getcwd()
            os.chdir(tmp)
            try:
                snap = status.build_status(tools={"yt-dlp": {"available": True}, "streamlink": {"available": True},
                                                  "ffmpeg": {"available": True}})
            finally:
                os.chdir(old)
        self.assertFalse(snap["config"]["recordings_folder"]["drive_available"])
        self.assertTrue(any("drive of the recordings folder" in p for p in snap["problems"]), snap["problems"])
        self.assertEqual(snap["config"]["recordings_folder"]["fallback"], "default")
        self.assertNotIn(letter + ":", str(snap), "no personal paths in the snapshot")


if __name__ == "__main__":
    unittest.main()
