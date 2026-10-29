"""The preview grab command and its seek order, with a fake ffmpeg and (if ffmpeg exists) a real one.

Real case behind it: a recording whose key frames are ~4 s apart (binxbasilisk, 125 frames at 30 fps).
The old fixed ``-sseof -3`` found nothing every tick, so every tick ran a second, longer seek as well."""
import logging
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

from msr import preview_state as ps
from msr import worker

PNG = b"\x89PNG\r\n\x1a\n" + b"x" * 400
LOG = logging.getLogger("t")


class FakeRun:
    """worker._hidden_run stand-in: call n writes writes[n] to the output file, or nothing when None."""

    def __init__(self, writes=()):
        self.writes = list(writes)
        self.cmds = []

    def __call__(self, cmd, timeout):
        self.cmds.append(cmd)
        data = self.writes.pop(0) if self.writes else None
        if data is not None:
            with open(cmd[-1], "wb") as f:
                f.write(data)
        return subprocess.CompletedProcess(cmd, 0, "", "")


def seek_of(cmd):
    for flag in ("-sseof", "-ss"):
        if flag in cmd:
            return flag, cmd[cmd.index(flag) + 1]
    return None


class GrabCommandTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.raw = os.path.join(self.tmp.name, "live.ts")
        with open(self.raw, "wb") as f:
            f.write(b"\0" * (128 * 1024))
        self.out = os.path.join(self.tmp.name, "chan.png")

    def grab(self, writes, **kwargs):
        fake = FakeRun(writes)
        with mock.patch.object(worker, "_hidden_run", fake), \
                mock.patch.object(worker, "_probe_duration", return_value=100.0):
            result = worker.create_preview_frame(self.raw, self.out, "ffmpeg", LOG, **kwargs)
        return result, fake

    def test_decoder_flags_come_before_the_input(self):
        (ok, _), fake = self.grab([PNG])
        self.assertTrue(ok)
        cmd = fake.cmds[0]
        i = cmd.index("-i")
        self.assertEqual(cmd[cmd.index("-threads") + 1], "1")
        self.assertEqual(cmd[cmd.index("-skip_frame") + 1], "nokey")
        self.assertLess(cmd.index("-threads"), i)
        self.assertLess(cmd.index("-skip_frame"), i)

    def test_default_order_is_three_then_six_seconds_from_the_end_then_elapsed_time(self):
        (ok, _), fake = self.grab([None, None, None])
        self.assertFalse(ok)
        self.assertEqual([seek_of(c) for c in fake.cmds],
                         [("-sseof", "-3.00"), ("-sseof", "-6.00"), ("-ss", "97.00")])

    def test_the_plain_rung_leaves_out_skip_frame_but_keeps_one_thread(self):
        (ok, _), fake = self.grab([PNG], attempts=[ps.LADDER[3]])
        self.assertTrue(ok)
        self.assertNotIn("-skip_frame", fake.cmds[0])
        self.assertEqual(fake.cmds[0][fake.cmds[0].index("-threads") + 1], "1")

    def test_given_attempts_are_tried_in_that_order_and_the_working_one_is_reported(self):
        report = {}
        (ok, _), fake = self.grab([None, PNG], attempts=list(ps.LADDER[1:]), report=report)
        self.assertTrue(ok)
        self.assertEqual([seek_of(c) for c in fake.cmds], [("-sseof", "-6.00"), ("-ss", "97.00")])
        self.assertEqual(report["attempt"], ps.LADDER[2])

    def test_a_failure_reports_nothing(self):
        report = {}
        (ok, _), _fake = self.grab([None], attempts=[ps.LADDER[0]], report=report)
        self.assertFalse(ok)
        self.assertEqual(report, {})

    def test_the_old_png_is_still_never_accepted(self):
        with open(self.out, "wb") as f:
            f.write(PNG)
        (ok, _), _fake = self.grab([None], attempts=[ps.LADDER[0]])
        self.assertFalse(ok)
        self.assertFalse(os.path.exists(self.out))

    def test_a_channel_that_needed_six_seconds_pays_for_one_ffmpeg_call_next_time(self):
        state = ps.PreviewState()
        plan = state.plan("a")
        report = {}
        (ok, _), fake = self.grab([None, PNG], attempts=plan, report=report)  # -3 found nothing, -6 did
        self.assertTrue(ok)
        self.assertEqual(len(fake.cmds), 2)
        state.record_success("a", report["attempt"])
        (ok, _), fake = self.grab([PNG], attempts=state.plan("a"))
        self.assertTrue(ok)
        self.assertEqual([seek_of(c) for c in fake.cmds], [("-sseof", "-6.00")])

    def test_a_stream_without_usable_video_runs_each_rung_once(self):
        state = ps.PreviewState()
        (ok, _), fake = self.grab([], attempts=state.plan("a"))
        self.assertFalse(ok)
        self.assertEqual(len(fake.cmds), 4)  # three fast rungs and the plain safety net, then the back-off takes over
        self.assertEqual(state.record_failure("a"), 4.0)


FFMPEG = shutil.which("ffmpeg")


@unittest.skipUnless(FFMPEG, "ffmpeg is not installed")
class RealFfmpegTests(unittest.TestCase):
    """A synthetic .ts with a key frame every 125 frames (4.17 s at 30 fps), cut short like a file
    that is still being written."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        full = os.path.join(cls.tmp.name, "full.ts")
        made = subprocess.run(
            [FFMPEG, "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi",
             "-i", "testsrc2=size=320x180:rate=30", "-t", "30", "-c:v", "libx264", "-preset", "ultrafast",
             "-threads", "1", "-g", "125", "-keyint_min", "125", "-sc_threshold", "0", "-pix_fmt", "yuv420p",
             "-f", "mpegts", full], capture_output=True, text=True, timeout=120)
        if made.returncode != 0 or not os.path.exists(full):
            raise unittest.SkipTest("this ffmpeg cannot encode the test video: " + made.stderr[-200:])
        cls.full = full

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def cut(self, percent):
        """Copy of the first *percent* of the file, on a TS packet boundary."""
        size = os.path.getsize(self.full) * percent // 100
        path = os.path.join(self.tmp.name, f"cut{percent}.ts")
        with open(self.full, "rb") as src, open(path, "wb") as dst:
            dst.write(src.read(size - size % 188))
        return path

    def old_command_finds_a_frame(self, raw, seconds):
        """The grab as it was before: default decoder, -sseof -N."""
        out = os.path.join(self.tmp.name, "old.png")
        if os.path.exists(out):
            os.remove(out)
        subprocess.run([FFMPEG, "-hide_banner", "-loglevel", "error", "-y", "-sseof", f"-{seconds}", "-i", raw,
                        "-map", "0:v:0", "-an", "-frames:v", "1", "-vf", worker.preview_scale_filter(160, 90),
                        "-f", "image2", out], capture_output=True, timeout=60)
        return os.path.exists(out) and os.path.getsize(out) > 200

    def a_cut_where_three_seconds_finds_nothing(self):
        for percent in (97, 94, 80, 96, 95, 93, 92, 90, 99, 98, 85, 75):
            raw = self.cut(percent)
            if not self.old_command_finds_a_frame(raw, 3) and self.old_command_finds_a_frame(raw, 6):
                return raw
        self.skipTest("no cut of this file reproduces the long key-frame gap")

    def test_the_old_three_second_seek_found_nothing_and_the_ladder_finds_a_frame(self):
        raw = self.a_cut_where_three_seconds_finds_nothing()
        out = os.path.join(self.tmp.name, "new.png")
        state = ps.PreviewState()
        report = {}
        ok, msg = worker.create_preview_frame(raw, out, FFMPEG, LOG, attempts=state.plan("a"), report=report)
        self.assertTrue(ok, msg)
        self.assertEqual(report["attempt"], ps.LADDER[1])  # the 3 s rung found nothing, 6 s did
        with open(out, "rb") as f:
            self.assertEqual(f.read(8), b"\x89PNG\r\n\x1a\n")  # a real frame, not an empty file
        state.record_success("a", report["attempt"])
        calls = []
        real = worker._hidden_run
        with mock.patch.object(worker, "_hidden_run", lambda cmd, timeout: calls.append(cmd) or real(cmd, timeout)):
            ok, msg = worker.create_preview_frame(raw, out, FFMPEG, LOG, attempts=state.plan("a"))
        self.assertTrue(ok, msg)
        self.assertEqual(len(calls), 1)  # remembered: no failing 3 s call first

    def test_the_fast_grab_gives_the_same_kind_of_frame_as_the_plain_one(self):
        raw = self.cut(100)
        sizes = []
        for rung in (ps.LADDER[0], ps.LADDER[3]):
            out = os.path.join(self.tmp.name, f"r{rung[2]}.png")
            ok, msg = worker.create_preview_frame(raw, out, FFMPEG, LOG, width=160, height=90, attempts=[rung])
            self.assertTrue(ok, msg)
            sizes.append(os.path.getsize(out))
        self.assertTrue(all(s > 1000 for s in sizes), sizes)  # not a black or blank frame (testsrc2 is busy)


if __name__ == "__main__":
    unittest.main()
