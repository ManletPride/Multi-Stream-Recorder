"""Footage safety: a source recording is never deleted, moved or replaced before the output that
replaces it is verified, outputs appear atomically, and a failure keeps the original.

Everything runs against the scripted fake ffmpeg in ``tests/fake_ffmpeg.py`` (stand-in media files,
no real video) in temp folders. ``RealFfmpegTests`` runs the same flows with a real ffmpeg on small
synthetic clips when ``MSR_REAL_FFMPEG`` points at a folder containing ffmpeg/ffprobe.
"""
import datetime
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import msr.recorder as recorder_mod  # noqa: E402
import msr.worker as worker_mod  # noqa: E402
from tests.fake_ffmpeg import FakeFfmpeg, read_duration, write_media  # noqa: E402
from tests.test_startup_cleanup import FakeRemux, make_recorder, make_ts, pending_path, processed_path  # noqa: E402
from msr.nowindow import no_window_kwargs  # noqa: E402

LOG = logging.getLogger("footage-safety")
RAW_BYTES = 100 * 1024
SMALL_MP4 = 4096        # the patched "junk" threshold, so tests need no 5 MiB files


class FakeBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.fake = FakeFfmpeg(os.path.join(self.tmp, "fake"))
        self.raw = write_media(os.path.join(self.tmp, "Recorded", "a.ts"), RAW_BYTES, 600)
        self.mp4 = os.path.join(self.tmp, "Processed", "a.mp4")
        os.makedirs(os.path.dirname(self.mp4))
        for p in (mock.patch.object(worker_mod, "MIN_MP4_BYTES", SMALL_MP4),
                  mock.patch.object(worker_mod.time, "sleep", lambda s: None)):
            p.start()
            self.addCleanup(p.stop)

    def remux(self, *modes, **kwargs):
        self.fake.set_modes(*modes)
        kwargs.setdefault("timeout", 30)
        with self.fake.installed():
            return worker_mod.remux_to_mp4(self.raw, self.mp4, self.fake.ffmpeg, LOG, **kwargs)

    def part(self):
        return self.mp4 + ".part"

    def assert_raw_intact(self):
        self.assertEqual(os.path.getsize(self.raw), RAW_BYTES)
        self.assertEqual(read_duration(self.raw), 600)


class FakeFfmpegHelperTests(FakeBase):
    """The helper itself: it must be able to succeed, fail, truncate and hang on request."""

    def run_fake(self, tool, *args):
        return subprocess.run([getattr(self.fake, tool), *args], capture_output=True, text=True, timeout=60)

    def test_probe_reads_the_header(self):
        r = self.run_fake("ffprobe", "-show_entries", "format=duration,start_time", self.raw)
        self.assertIn("duration=600.0", r.stdout)
        bad = os.path.join(self.tmp, "bad.mp4")
        open(bad, "wb").write(b"\0" * 10)
        self.assertNotEqual(self.run_fake("ffprobe", bad).returncode, 0)

    def test_modes_are_consumed_in_order_and_the_last_repeats(self):
        self.fake.set_modes("fail", "ok")
        out = os.path.join(self.tmp, "o.mp4")
        codes = [self.run_fake("ffmpeg", "-i", self.raw, "-f", "mp4", out, "-y").returncode for _ in range(3)]
        self.assertEqual(codes, [1, 0, 0])
        self.assertEqual(len(self.fake.remuxes()), 3)

    def test_truncate_and_no_moov(self):
        out = os.path.join(self.tmp, "o.mp4")
        self.fake.set_modes({"mode": "truncate", "fraction": 0.25})
        self.assertEqual(self.run_fake("ffmpeg", "-i", self.raw, out, "-y").returncode, 0)
        self.assertEqual(read_duration(out), 150)
        self.assertEqual(os.path.getsize(out), RAW_BYTES // 4)
        self.fake.set_modes("no_moov")
        self.run_fake("ffmpeg", "-i", self.raw, out, "-y")
        self.assertIsNone(read_duration(out))

    def test_hang_really_hangs(self):
        self.fake.set_modes({"mode": "hang", "seconds": 30})
        out = os.path.join(self.tmp, "o.mp4")
        with self.assertRaises(subprocess.TimeoutExpired):
            self.run_fake_with_timeout(out)

    def run_fake_with_timeout(self, out):
        proc = subprocess.Popen([self.fake.ffmpeg, "-i", self.raw, out, "-y"], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        try:
            proc.communicate(timeout=1.5)
        finally:
            proc.kill()
            if os.name == "nt":      # the .cmd launcher's python child
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)


class VerifyTests(FakeBase):
    def out(self, size, duration):
        return write_media(os.path.join(self.tmp, "o.mp4"), size, duration)

    def verify(self, out=None, sources=None):
        with self.fake.installed():
            return worker_mod.verify_media_output(sources or [self.raw], out or os.path.join(self.tmp, "o.mp4"),
                                                  self.fake.ffmpeg, LOG)

    def test_complete_output_passes(self):
        self.out(RAW_BYTES, 600)
        self.assertEqual(self.verify(), (True, None))

    def test_small_loss_within_tolerance_passes(self):
        self.out(RAW_BYTES, 596)
        self.assertEqual(self.verify(), (True, None))

    def test_missing_unreadable_and_too_small(self):
        self.assertEqual(self.verify()[1], "output missing")
        with open(os.path.join(self.tmp, "o.mp4"), "wb") as f:
            f.write(b"\0" * RAW_BYTES)
        self.assertIn("unreadable", self.verify()[1])
        self.out(100, 600)
        self.assertEqual(self.verify()[1], "output too small")

    def test_truncated_output_is_rejected(self):
        self.out(RAW_BYTES // 2, 300)
        ok, why = self.verify()
        self.assertFalse(ok)
        self.assertIn("truncated", why)

    def test_a_short_duration_with_a_matching_size_is_accepted_with_a_warning(self):
        """Live captures have PTS jumps: the source's own duration is the unreliable side."""
        self.out(int(RAW_BYTES * 0.95), 300)
        with self.assertLogs(level="WARNING"):
            ok, _ = self.verify()
        self.assertTrue(ok)

    def test_unknown_source_duration_falls_back_to_size(self):
        raw = os.path.join(self.tmp, "weird.ts")
        with open(raw, "wb") as f:
            f.write(b"\0" * RAW_BYTES)
        self.out(RAW_BYTES // 2, 300)
        self.assertFalse(self.verify(sources=[raw])[0])
        self.out(RAW_BYTES, 300)
        with self.assertLogs(level="WARNING"):
            self.assertTrue(self.verify(sources=[raw])[0])

    def test_several_sources_add_up(self):
        a = write_media(os.path.join(self.tmp, "p1.mp4"), RAW_BYTES, 100)
        b = write_media(os.path.join(self.tmp, "p2.mp4"), RAW_BYTES, 200)
        self.out(2 * RAW_BYTES, 300)
        self.assertTrue(self.verify(sources=[a, b])[0])
        self.out(RAW_BYTES, 150)
        self.assertFalse(self.verify(sources=[a, b])[0])


class RemuxAtomicTests(FakeBase):
    def test_success_publishes_atomically_and_leaves_no_part(self):
        ok, size, err = self.remux("ok")
        self.assertEqual((ok, err), (True, None))
        self.assertEqual(size, RAW_BYTES)
        self.assertEqual(read_duration(self.mp4), 600)
        self.assertFalse(os.path.exists(self.part()))
        self.assert_raw_intact()

    def test_ffmpeg_is_told_to_write_the_part_not_the_final_name(self):
        self.remux("ok")
        (call,) = self.fake.remuxes()
        self.assertIn(self.part(), call["argv"])
        self.assertNotIn(self.mp4, call["argv"])
        argv = call["argv"]
        self.assertEqual(argv[argv.index("-f") + 1], "mp4")          # the .part name has no format to guess from
        self.assertLess(argv.index("-f"), argv.index(self.part()))

    def test_failure_keeps_the_raw_and_leaves_nothing_behind(self):
        ok, _, err = self.remux("fail")
        self.assertFalse(ok)
        self.assertEqual(err, "code 1")
        self.assertFalse(os.path.exists(self.mp4))
        self.assertFalse(os.path.exists(self.part()))
        self.assert_raw_intact()

    def test_the_fallback_remux_can_rescue_a_failed_first_attempt(self):
        ok, _, err = self.remux("fail", "ok")
        self.assertTrue(ok, err)
        self.assertEqual(len(self.fake.remuxes()), 2)
        self.assertEqual(read_duration(self.mp4), 600)

    def test_a_failing_remux_does_not_destroy_an_existing_mp4(self):
        """ffmpeg -y used to write straight over the final name."""
        write_media(self.mp4, RAW_BYTES, 600)
        before = open(self.mp4, "rb").read()
        ok, _, _ = self.remux("fail")
        self.assertFalse(ok)
        self.assertEqual(open(self.mp4, "rb").read(), before)

    def test_a_verified_remux_replaces_an_older_partial_mp4(self):
        with open(self.mp4, "wb") as f:
            f.write(b"\0" * (RAW_BYTES // 2))
        ok, _, _ = self.remux("ok")
        self.assertTrue(ok)
        self.assertEqual(read_duration(self.mp4), 600)

    def test_exit_0_with_a_truncated_output_is_rejected(self):
        ok, _, err = self.remux({"mode": "truncate", "fraction": 0.4})
        self.assertFalse(ok)
        self.assertIn("truncated", err)
        self.assertFalse(os.path.exists(self.mp4))
        self.assertFalse(os.path.exists(self.part()))
        self.assert_raw_intact()

    def test_exit_0_without_a_readable_output_is_rejected(self):
        for mode in ("no_moov", "no_output"):
            ok, _, err = self.remux(mode)
            self.assertFalse(ok, mode)
            self.assertFalse(os.path.exists(self.mp4), mode)
            self.assertFalse(os.path.exists(self.part()), mode)
            self.assert_raw_intact()

    def test_junk_output_is_reported_as_too_small_and_removed(self):
        ok, size, err = self.remux({"mode": "ok", "size": 100})
        self.assertEqual((ok, err), (False, "output too small"))
        self.assertFalse(os.path.exists(self.part()))
        self.assertFalse(os.path.exists(self.mp4))

    def test_a_stale_part_from_a_crash_is_replaced(self):
        with open(self.part(), "wb") as f:
            f.write(b"old junk")
        ok, _, _ = self.remux("ok")
        self.assertTrue(ok)
        self.assertFalse(os.path.exists(self.part()))

    def test_a_brief_lock_while_publishing_is_retried(self):
        real = os.replace
        calls = []

        def flaky(src, dst):
            calls.append(1)
            if len(calls) == 1:
                raise PermissionError("antivirus")
            return real(src, dst)
        with mock.patch.object(worker_mod.os, "replace", flaky):
            ok, _, _ = self.remux("ok")
        self.assertTrue(ok)
        self.assertEqual(len(calls), 2)

    def test_a_publish_that_never_works_keeps_the_raw_and_cleans_up(self):
        with mock.patch.object(worker_mod.os, "replace", side_effect=PermissionError("locked")):
            ok, _, _ = self.remux("ok")
        self.assertFalse(ok)
        self.assertFalse(os.path.exists(self.part()))
        self.assert_raw_intact()


class RemuxHangTests(FakeBase):
    def test_a_hung_ffmpeg_is_killed_after_the_stall_timeout(self):
        start = time.monotonic()
        ok, _, err = self.remux({"mode": "hang", "seconds": 60}, timeout=2)
        self.assertFalse(ok)
        self.assertEqual(err, "timeout")
        self.assertLess(time.monotonic() - start, 30)
        self.assertFalse(os.path.exists(self.part()))
        self.assert_raw_intact()

    def test_cancel_reaches_a_hung_ffmpeg(self):
        cancel = threading.Event()
        threading.Timer(1.0, cancel.set).start()
        start = time.monotonic()
        ok, _, err = self.remux({"mode": "hang", "seconds": 60}, cancel_event=cancel)
        self.assertEqual((ok, err), (False, "cancelled"))
        self.assertLess(time.monotonic() - start, 30)
        self.assertFalse(os.path.exists(self.part()))
        self.assert_raw_intact()


class CleanupSafetyTests(unittest.TestCase):
    """The two cleanup passes (stop-time and startup) with the real remux_to_mp4 and the fake ffmpeg."""

    NAME = "chan.tsunami_20260101_000000"

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.rec = make_recorder(self.tmp, channels=("chan",))
        self.cleaner = recorder_mod.BackgroundCleaner(self.rec.config)
        self.fake = FakeFfmpeg(os.path.join(self.tmp, "fake"))
        self.rec.config.config.set("Advanced", "ffmpeg_path", self.fake.ffmpeg)
        self.raw = make_ts(self.rec, "kick", "chan", self.NAME + ".ts", size=2048)
        write_media(self.raw, RAW_BYTES, 600)
        old = time.time() - 3600
        os.utime(self.raw, (old, old))
        self.mp4 = processed_path(self.rec, "kick", "chan", self.NAME + ".mp4")
        os.makedirs(os.path.dirname(self.mp4), exist_ok=True)
        for p in (mock.patch.object(worker_mod, "MIN_MP4_BYTES", SMALL_MP4),
                  mock.patch.object(recorder_mod.time, "sleep", lambda s: None)):
            p.start()
            self.addCleanup(p.stop)

    def pending(self):
        return pending_path(self.rec, "kick", "chan", self.NAME + ".ts")

    def stop_time_pass(self):
        with self.fake.installed():
            self.cleaner._process_leftover_files_locked()

    def startup_pass(self):
        plan = self.rec._plan_startup_cleanup()
        with self.fake.installed():
            self.rec._process_startup_leftovers(plan, background=False)

    # -- the partial MP4 an interrupted remux leaves (C3) --------------------------------

    def partial_mp4(self, fraction=0.9):
        os.makedirs(os.path.dirname(self.mp4), exist_ok=True)
        with open(self.mp4, "wb") as f:
            f.write(b"\0" * int(RAW_BYTES * fraction))

    def test_a_cut_off_mp4_is_not_trusted_by_the_stop_time_pass(self):
        self.partial_mp4()          # >= 80 % of the .ts, no moov: the old size rule called it complete
        self.fake.set_modes("fail")
        with self.assertLogs(level="WARNING"):
            self.stop_time_pass()
        self.assertTrue(os.path.exists(self.raw), "the raw recording must stay in Recorded/")
        self.assertFalse(os.path.exists(self.pending()))

    def test_a_cut_off_mp4_is_not_trusted_by_the_startup_pass(self):
        self.partial_mp4()
        self.fake.set_modes("fail")
        with self.assertLogs(level="WARNING"):
            self.startup_pass()
        self.assertTrue(os.path.exists(self.raw))
        self.assertFalse(os.path.exists(self.pending()))

    def test_a_cut_off_mp4_is_redone_and_only_then_the_raw_moves(self):
        for run in (self.stop_time_pass, self.startup_pass):
            self.setUp()
            self.partial_mp4()
            self.fake.set_modes("ok")
            run()
            self.assertEqual(read_duration(self.mp4), 600, run.__name__)
            self.assertTrue(os.path.exists(self.pending()), run.__name__)
            self.assertFalse(os.path.exists(self.raw), run.__name__)

    def test_a_complete_existing_mp4_is_accepted_without_a_remux(self):
        os.makedirs(os.path.dirname(self.mp4), exist_ok=True)
        write_media(self.mp4, RAW_BYTES, 600)
        self.fake.set_modes("fail")
        self.stop_time_pass()
        self.assertEqual(self.fake.remuxes(), [])
        self.assertTrue(os.path.exists(self.pending()))

    # -- a failed remux keeps the recording (H3) -----------------------------------------

    def test_failed_remux_keeps_the_raw_in_both_passes(self):
        self.fake.set_modes("fail")
        with self.assertLogs(level="INFO") as logs:
            self.stop_time_pass()
        self.assertTrue(os.path.exists(self.raw))
        self.assertFalse(os.path.exists(self.pending()))
        self.assertTrue(any("kept in Recorded" in o for o in logs.output))
        with self.assertLogs(level="INFO") as logs:
            self.startup_pass()
        self.assertTrue(os.path.exists(self.raw))
        self.assertFalse(os.path.exists(self.pending()))
        self.assertTrue(any("kept in Recorded" in o for o in logs.output))

    def test_junk_output_still_goes_to_pending_deletion(self):
        self.fake.set_modes({"mode": "ok", "size": 100})
        self.stop_time_pass()
        self.assertTrue(os.path.exists(self.pending()))
        self.assertFalse(os.path.exists(self.raw))

    def test_timeout_keeps_the_raw(self):
        self.fake.set_modes({"mode": "hang", "seconds": 60})
        with mock.patch.object(worker_mod, "_probe_duration", lambda *a, **k: 600.0):
            with self.fake.installed():
                ok, _, err = worker_mod.remux_to_mp4(self.raw, self.mp4, self.fake.ffmpeg, LOG, 2)
        self.assertEqual((ok, err), (False, "timeout"))
        self.assertTrue(os.path.exists(self.raw))

    # -- the .ts name bug (L1) ------------------------------------------------------------

    def test_only_the_extension_is_replaced(self):
        remux = FakeRemux()
        with mock.patch.object(recorder_mod, "remux_to_mp4", remux):
            self.stop_time_pass()
        (call,) = remux.calls
        self.assertEqual(os.path.basename(call["mp4"]), "chan.tsunami_20260101_000000.mp4")

    # -- leftovers of an interrupted cleanup (L2) and of an interrupted remux --------------

    def test_a_file_stranded_under_the_probe_name_comes_back(self):
        stranded = self.raw + ".cleanupprobe"
        os.rename(self.raw, stranded)
        self.assertEqual(recorder_mod.restore_stranded_probe_files(os.path.dirname(self.raw)), 1)
        self.assertTrue(os.path.exists(self.raw))
        self.assertFalse(os.path.exists(stranded))

    def test_a_stranded_file_is_never_restored_over_another(self):
        stranded = self.raw + ".cleanupprobe"
        shutil.copy(self.raw, stranded)
        with self.assertLogs(level="WARNING"):
            self.assertEqual(recorder_mod.restore_stranded_probe_files(os.path.dirname(self.raw)), 0)
        self.assertTrue(os.path.exists(stranded))

    def test_startup_snapshot_finds_a_stranded_recording(self):
        os.rename(self.raw, self.raw + ".cleanupprobe")
        entries = self.rec._snapshot_leftover_ts_files()
        self.assertEqual([e["raw_file"] for e in entries], [self.raw])

    def test_startup_removes_unfinished_remux_output_but_nothing_else(self):
        part = self.mp4 + ".part"
        os.makedirs(os.path.dirname(self.mp4), exist_ok=True)
        for path in (part, self.mp4):
            with open(path, "wb") as f:
                f.write(b"\0" * 1000)
        self.rec._plan_startup_cleanup()
        self.assertFalse(os.path.exists(part))
        self.assertTrue(os.path.exists(self.mp4))
        self.assertTrue(os.path.exists(self.raw))


class StitchSafetyTests(FakeBase):
    """finalize_recording_group: the parts are only moved away once the stitched file is verified."""

    def setUp(self):
        super().setUp()
        self.pending_dir = os.path.join(self.tmp, "Pending")
        os.makedirs(self.pending_dir)
        self.parts = []
        for name, dur in (("g_a", 100), ("g_b", 200)):
            mp4 = write_media(os.path.join(self.tmp, "Processed", name + ".mp4"), RAW_BYTES, dur)
            self.parts.append({"mp4": mp4, "meta": None, "start_wall": datetime.datetime(2026, 1, 1),
                               "elapsed": dur, "title": None})

    def stitch(self, *modes):
        self.fake.set_modes(*modes)
        with self.fake.installed():
            worker_mod.finalize_recording_group(
                self.parts, "g", os.path.join(self.tmp, "Processed"), self.pending_dir,
                self.fake.ffmpeg, 30, "chan", "kick", LOG)

    def test_a_good_stitch_replaces_the_parts(self):
        self.stitch("ok")
        final = os.path.join(self.tmp, "Processed", "g.mp4")
        self.assertEqual(read_duration(final), 300)
        self.assertEqual(sorted(os.listdir(self.pending_dir)), ["g_a.mp4", "g_b.mp4"])

    def test_a_truncated_stitch_leaves_the_parts_alone(self):
        with self.assertLogs(level="ERROR"):
            self.stitch({"mode": "truncate", "fraction": 0.4})
        self.assertEqual(os.listdir(self.pending_dir), [])
        for part in self.parts:
            self.assertTrue(os.path.exists(part["mp4"]))
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "Processed", "g.merging.mp4")))
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "Processed", "g.mp4")))

    def test_a_failed_stitch_leaves_the_parts_alone(self):
        with self.assertLogs(level="ERROR"):
            self.stitch("fail")
        self.assertEqual(os.listdir(self.pending_dir), [])
        for part in self.parts:
            self.assertTrue(os.path.exists(part["mp4"]))


class WorkerTooSmallTests(unittest.TestCase):
    def test_a_good_mp4_of_the_same_name_is_never_deleted_for_a_junk_remux(self):
        """The worker used to os.remove(mp4_file) after 'output too small'; remux no longer writes it."""
        import inspect
        src = inspect.getsource(worker_mod.record_worker)
        self.assertNotIn("Deleted undersized mp4 artifact", src)


@unittest.skipUnless(os.environ.get("MSR_REAL_FFMPEG"), "set MSR_REAL_FFMPEG to a folder with ffmpeg and ffprobe")
class RealFfmpegTests(unittest.TestCase):
    """The same flows with the real ffmpeg on tiny synthetic clips generated in a temp folder."""

    @classmethod
    def setUpClass(cls):
        folder = os.environ["MSR_REAL_FFMPEG"]
        exe = ".exe" if os.name == "nt" else ""
        cls.ffmpeg = os.path.join(folder, "ffmpeg" + exe)
        cls._tmp = tempfile.TemporaryDirectory()
        cls.clip = os.path.join(cls._tmp.name, "clip.ts")
        subprocess.run([cls.ffmpeg, "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=25",
                        "-f", "lavfi", "-i", "sine=frequency=440", "-t", "20", "-c:v", "mpeg2video",
                        "-b:v", "1M", "-c:a", "mp2", "-f", "mpegts", cls.clip, "-y"], check=True, timeout=120)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.addCleanup(self._t.cleanup)
        self.raw = os.path.join(self._t.name, "a.ts")
        shutil.copy(self.clip, self.raw)
        self.mp4 = os.path.join(self._t.name, "a.mp4")
        p = mock.patch.object(worker_mod, "MIN_MP4_BYTES", 1024)
        p.start()
        self.addCleanup(p.stop)

    def test_real_remux_is_verified_and_published(self):
        ok, size, err = worker_mod.remux_to_mp4(self.raw, self.mp4, self.ffmpeg, LOG, 120)
        self.assertTrue(ok, err)
        self.assertFalse(os.path.exists(self.mp4 + ".part"))
        self.assertEqual(worker_mod.verify_remux_output(self.raw, self.mp4, self.ffmpeg, LOG), (True, None))
        self.assertAlmostEqual(worker_mod._probe_duration(self.ffmpeg, self.mp4), 20, delta=1.5)

    def test_real_cut_off_mp4_is_rejected(self):
        # What an interrupted remux leaves behind: ffmpeg writes mdat first and the moov
        # (index) last, so a write killed at 90 % has samples but no moov.
        subprocess.run([self.ffmpeg, "-v", "error", "-i", self.raw, "-c", "copy", "-f", "mp4",
                        self.mp4, "-y"], check=True, **no_window_kwargs())
        size = os.path.getsize(self.mp4)
        with open(self.mp4, "r+b") as f:
            f.truncate(int(size * 0.9))
        ok, why = worker_mod.verify_remux_output(self.raw, self.mp4, self.ffmpeg, LOG)
        self.assertFalse(ok, why)

    def test_real_remux_of_a_missing_source_fails_cleanly(self):
        ok, _, err = worker_mod.remux_to_mp4(self.raw + ".nope", self.mp4, self.ffmpeg, LOG, 60)
        self.assertFalse(ok)
        self.assertFalse(os.path.exists(self.mp4))
        self.assertFalse(os.path.exists(self.mp4 + ".part"))


if __name__ == "__main__":
    unittest.main()
