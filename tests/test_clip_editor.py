"""Clip Editor: the Tk-free engine (times, marks, .ts length, cut, names), the hidden editor
window, and agent parity (clip --from/--to, --file). Temp dirs only; no window is shown."""
import os
import shutil
import subprocess
import sys
import tempfile
import tkinter as tk
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import msr.control as control  # noqa: E402
from msr import clip_audio, clip_core as core  # noqa: E402
from msr.config import Config  # noqa: E402
from tests.test_control import in_dir, make_data, msr_ctl  # noqa: E402

HAS_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


def ts_packet(pts=None, stream_id=0xE0):
    """One 188-byte TS packet; with pts, a PES start carrying that PTS."""
    if pts is None:
        return b"\x47\x00\x11\x10" + b"\xff" * 184
    b = bytes([0x21 | ((pts >> 29) & 0x0E), (pts >> 22) & 0xFF, 0x01 | ((pts >> 14) & 0xFE),
               (pts >> 7) & 0xFF, 0x01 | ((pts << 1) & 0xFE)])
    pes = b"\x00\x00\x01" + bytes([stream_id]) + b"\x00\x00\x80\x80\x05" + b
    return b"\x47\x40\x11\x10" + pes + b"\xff" * (184 - len(pes))


def write_ts(path, pts_values, filler=0, mode="wb"):
    with open(path, mode) as fh:
        for p in pts_values:
            fh.write(ts_packet(p))
            for _ in range(filler):
                fh.write(ts_packet())


class TimeAndMarksTests(unittest.TestCase):
    def test_parse_time_forms(self):
        self.assertEqual(core.parse_time("90"), 90)
        self.assertEqual(core.parse_time("1:30"), 90)
        self.assertEqual(core.parse_time("01:02:03.5"), 3723.5)
        self.assertEqual(core.parse_time("-60", 600), 540)
        self.assertEqual(core.parse_time("end", 600), 600)
        with self.assertRaises(core.ClipError):
            core.parse_time("soon")
        with self.assertRaises(core.ClipError):
            core.parse_time("-5")      # counting back needs a length

    def test_fmt_time(self):
        self.assertEqual(core.fmt_time(65.25), "1:05.2")
        self.assertEqual(core.fmt_time(3725, tenths=False), "1:02:05")

    def test_marks_keep_in_before_out_and_follow_a_growing_timeline(self):
        m = core.Marks(100)
        m.set_in(10)
        m.set_out(25)
        self.assertEqual(m.clip_length, 15)
        m.nudge("in", -0.5)
        m.nudge("out", 0.5)
        self.assertEqual((m.cut_in, m.cut_out), (9.5, 25.5))
        m.nudge("in", 100)                       # never past the out point
        self.assertAlmostEqual(m.cut_in, 25.5 - m.MIN)
        m.set_out(5)                             # out before in: the in point goes
        self.assertEqual((m.cut_in, m.cut_out), (None, 5))
        self.assertFalse(m.ready)
        m.set_in(200)                            # clamped to the timeline
        self.assertEqual(m.cut_in, 100)
        m.grow(300)
        m.set_out(250)
        self.assertTrue(m.ready)


class TsLengthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "a.ts")

    def tearDown(self):
        self.tmp.cleanup()

    def test_length_from_both_ends_and_while_it_grows(self):
        start = 90000 * 10
        write_ts(self.path, [start + i * 90000 for i in range(5)], filler=3)
        self.assertAlmostEqual(core.ts_length(self.path), 4.0)
        with open(self.path, "rb") as reader:          # a reader open (shared) while it grows
            write_ts(self.path, [start + i * 90000 for i in range(5, 31)], filler=3, mode="ab")
            self.assertAlmostEqual(core.ts_length(self.path), 30.0)
            reader.read(10)

    def test_pts_wrap_and_out_of_order_starts(self):
        near = core.PTS_WRAP - 90000 * 2
        write_ts(self.path, [near + 4500, near] + [(near + i * 90000) % core.PTS_WRAP for i in range(1, 6)])
        self.assertAlmostEqual(core.ts_length(self.path), 5.0)

    def test_junk_is_none(self):
        with open(self.path, "wb") as fh:
            fh.write(b"not a transport stream" * 50)
        self.assertIsNone(core.ts_length(self.path))
        with mock.patch.object(core, "probe_length", return_value=None):
            self.assertIsNone(core.media_length(self.path))
        self.assertIsNone(core.media_length(os.path.join(self.tmp.name, "gone.ts")))


class CutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = os.path.join(self.tmp.name, "streams", "Recorded", "kick", "knut", "knut_20261029.ts")
        os.makedirs(os.path.dirname(self.src))
        write_ts(self.src, [0, 90000])
        self.calls = []

    def tearDown(self):
        self.tmp.cleanup()

    def fake_run(self, fail=False):
        def run(cmd, **kw):
            self.calls.append(cmd)
            self.assertIn("creationflags", kw) if sys.platform == "win32" else None
            if "format=start_time" in cmd:
                return subprocess.CompletedProcess(cmd, 0, "1.400000\n", "")
            if "packet=pts_time,flags" in cmd:
                return subprocess.CompletedProcess(cmd, 0, "3.4,K__,\n4.4,___,\n5.4,K__,\n6.9,___\n", "")
            if fail:
                return subprocess.CompletedProcess(cmd, 1, "", "boom\nInvalid data found\n")
            with open(cmd[-1], "wb") as fh:
                fh.write(b"\0" * 4096)
            return subprocess.CompletedProcess(cmd, 0, "", "")
        return run

    def test_stream_copy_starts_at_the_keyframe_and_reports_it(self):
        out_dir = core.clips_folder_for(self.src, os.path.join(self.tmp.name, "Clips"))
        self.assertEqual(out_dir, os.path.join(self.tmp.name, "Clips", "kick", "knut"))
        with mock.patch.object(core, "probe_length", return_value=5.0):
            res = core.make_clip("ffmpeg", self.src, 5.3, 9.0, out_dir, run=self.fake_run())
        cut = self.calls[-1]
        self.assertIn("copy", cut)
        self.assertEqual(cut[cut.index("-ss") + 1], "4.001")       # keyframe 5.4 - start 1.4
        self.assertAlmostEqual(res.start, 4.0)
        self.assertTrue(res.snapped)
        self.assertAlmostEqual(res.end, 9.0)
        self.assertTrue(os.path.isfile(res.path) and res.path.endswith(".mp4"))
        self.assertFalse(os.path.exists(res.path[:-4] + ".part.mp4"))
        with mock.patch.object(core, "probe_length", return_value=5.0):
            again = core.make_clip("ffmpeg", self.src, 5.3, 9.0, out_dir, run=self.fake_run())
        self.assertNotEqual(again.path, res.path)                   # never overwrites

    def test_exact_reencodes_from_the_asked_time(self):
        with mock.patch.object(core, "probe_length", return_value=3.7):
            res = core.make_clip("ffmpeg", self.src, 5.3, 9.0, self.tmp.name, exact=True, run=self.fake_run())
        cut = self.calls[-1]
        self.assertIn("libx264", cut)
        self.assertEqual(cut[cut.index("-ss") + 1], "5.300")
        self.assertEqual((res.start, res.snapped), (5.3, False))

    def test_failure_leaves_no_part_file_and_says_why(self):
        with self.assertRaises(core.ClipError) as ctx:
            core.make_clip("ffmpeg", self.src, 1, 3, self.tmp.name, run=self.fake_run(fail=True))
        self.assertIn("Invalid data", str(ctx.exception))
        self.assertEqual([f for f in os.listdir(self.tmp.name) if f.endswith(".mp4")], [])

    def test_bad_ranges_and_a_missing_source(self):
        with self.assertRaises(core.ClipError):
            core.make_clip("ffmpeg", self.src, 5, 5.1, self.tmp.name, run=self.fake_run())
        with self.assertRaises(core.ClipError):
            core.make_clip("ffmpeg", self.src + ".gone", 1, 5, self.tmp.name, run=self.fake_run())

    def test_names_and_the_finished_successor(self):
        name = core.clip_name("a b?.ts", 62, 3725)
        self.assertTrue(name.startswith("a_b_clip_1-02_to_1-02-05_"))
        self.assertIsNone(core.finished_successor(self.src))
        mp4 = self.src.replace("Recorded", "Processed")[:-3] + ".mp4"
        os.makedirs(os.path.dirname(mp4))
        open(mp4, "wb").close()
        self.assertEqual(core.finished_successor(self.src), mp4)

    def test_gain(self):
        data = b"\x10\x00\xf0\xff"   # 16, -16
        self.assertEqual(clip_audio.apply_gain(data, 1.0), data)
        self.assertEqual(clip_audio.apply_gain(data, 0.5), b"\x08\x00\xf8\xff")
        self.assertEqual(clip_audio.apply_gain(data, 0.0), b"\0" * 4)


@unittest.skipUnless(HAS_FFMPEG, "ffmpeg/ffprobe not installed")
class RealFfmpegTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.src = os.path.join(cls.tmp.name, "t.ts")
        subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i",
                        "testsrc=size=320x180:rate=30", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
                        "-t", "10", "-c:v", "libx264", "-g", "60", "-c:a", "aac", "-f", "mpegts", cls.src],
                       check=True, capture_output=True, **control_nowindow())

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_length_frame_and_cut(self):
        self.assertAlmostEqual(core.ts_length(self.src), 10.0, delta=0.2)
        frame = core.grab_frame("ffmpeg", self.src, 3.0, width=160)
        self.assertTrue(frame.startswith(b"P6"))
        res = core.make_clip("ffmpeg", self.src, 3.3, 6.0, os.path.join(self.tmp.name, "Clips"))
        self.assertAlmostEqual(res.start, 2.0, delta=0.1)     # keyframes every 2 s
        self.assertAlmostEqual(res.duration, 4.0, delta=0.3)
        exact = core.make_clip("ffmpeg", self.src, 3.3, 6.0, os.path.join(self.tmp.name, "Clips"), exact=True)
        self.assertAlmostEqual(exact.duration, 2.7, delta=0.15)


def control_nowindow():
    from msr.nowindow import no_window_kwargs
    return no_window_kwargs()


PPM = b"P6\n2 2\n255\n" + b"\x80" * 12


class FakeAudio:
    def __init__(self, *_a):
        self.gain, self.started, self.closed = None, None, False

    def set_gain(self, g):
        self.gain = g

    def start(self, t):
        self.started = t

    def position(self):
        return None

    def ended(self):
        return False

    def close(self):
        self.closed = True


class EditorWindowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = os.path.join(self.tmp.name, "Recorded", "kick", "knut", "k.ts")
        os.makedirs(os.path.dirname(self.src))
        write_ts(self.src, [0, 90000])
        self.root = tk.Tk()
        self.root.withdraw()
        self.length = 120.0
        self.audio = []
        from msr import clip_editor
        self.mod = clip_editor
        self.feed = mock.patch.object(clip_editor, "VideoFeed")
        self.feed.start()
        self.ed = clip_editor.ClipEditor(
            self.root, self.src, clips_base=os.path.join(self.tmp.name, "Clips"), live=True,
            follow=lambda: self.src, show=False, audio_factory=self._audio,
            frame_grabber=lambda *_a, **_k: PPM, length_reader=lambda _p: self.length)
        self.pump(lambda: self.ed.marks.length == 120.0)

    def _audio(self, *_a):
        a = FakeAudio()
        self.audio.append(a)
        return a

    def tearDown(self):
        self.ed.close()
        self.feed.stop()
        self.root.destroy()
        self.tmp.cleanup()

    def pump(self, cond, timeout=5.0):
        import time
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            self.root.update()
            if cond():
                return
            time.sleep(0.01)
        self.fail("condition not reached")

    def test_opens_near_the_live_edge_with_a_preview(self):
        self.assertAlmostEqual(self.ed.pos, 110.0)
        self.pump(lambda: self.ed._img is not None)
        self.assertIn("recording", self.ed.live_label.cget("text"))

    def test_mark_save_and_report(self):
        self.ed.seek(10)
        self.ed.mark_in()
        self.ed.seek(25)
        self.ed.mark_out()
        self.assertEqual(str(self.ed.save_btn.cget("state")), "normal")
        res = core.CutResult(os.path.join(self.tmp.name, "c.mp4"), 10, 25, 8.0, 25, 17.0, False)
        with mock.patch.object(core, "make_clip", return_value=res) as cut:
            self.ed.save()
            self.pump(lambda: "Saved" in self.ed.status.cget("text"))
        args, kw = cut.call_args
        self.assertEqual(args[2:4], (10, 25))
        self.assertEqual(args[4], os.path.join(self.tmp.name, "Clips", "kick", "knut"))
        self.assertIn("keyframe 0:08.0", self.ed.status.cget("text"))

    def test_live_edge_grows_and_jump_to_live(self):
        self.length = 300.0
        self.ed._last_length_check = 0
        self.pump(lambda: self.ed.marks.length == 300.0)
        self.ed.jump_live()
        self.assertAlmostEqual(self.ed.pos, 297.0)

    def test_play_uses_the_volume_and_stops(self):
        self.ed.volume.set(40)
        self.ed.toggle_play()
        self.assertTrue(self.ed.playing)
        self.assertEqual(self.audio[-1].gain, 0.4)
        self.ed.volume.set(10)
        self.ed._apply_volume()
        self.assertEqual(self.audio[-1].gain, 0.1)
        self.ed.toggle_play()
        self.assertFalse(self.ed.playing)
        self.assertTrue(self.audio[-1].closed)

    def test_replay_around_a_mark_stops_after_it(self):
        self.ed.seek(50)
        self.ed.mark_in()
        self.ed.replay("in")
        self.assertEqual((self.audio[-1].started, self.ed._stop_at), (48.0, 52.0))

    def test_file_gone_turns_save_off_then_follows_the_finished_mp4(self):
        self.ed.seek(10)
        self.ed.mark_in()
        self.ed.seek(20)
        self.ed.mark_out()
        os.remove(self.src)
        self.ed._last_length_check = 0
        self.pump(lambda: self.ed._gone)
        self.assertEqual(str(self.ed.save_btn.cget("state")), "disabled")
        self.assertIn("not reachable", self.ed.status.cget("text"))
        mp4 = os.path.join(self.tmp.name, "Processed", "kick", "knut", "k.mp4")
        os.makedirs(os.path.dirname(mp4))
        open(mp4, "wb").close()
        self.ed.src = self.src
        self.ed._last_length_check = 0
        self.pump(lambda: self.ed.src == mp4)
        self.assertFalse(self.ed.live)
        self.assertEqual((self.ed.marks.cut_in, self.ed.marks.cut_out), (10, 20))   # marks kept


class PlaybackFeedTests(unittest.TestCase):
    """Playback picture: one continuous rgb24 stream, newest due frame shown, late ones dropped."""

    def test_video_info_preview_size_and_command(self):
        out = '{"streams": [{"width": 1920, "height": 1080, "avg_frame_rate": "30000/1001"}]}'
        run = lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0, out, "")  # noqa: E731
        w, h, fps = core.video_info("x.ts", run=run)
        self.assertEqual((w, h), (1920, 1080))
        self.assertAlmostEqual(fps, 29.97, places=2)
        self.assertIsNone(core.video_info("x.ts", run=lambda c, **k: subprocess.CompletedProcess(c, 1, "", "")))
        self.assertEqual(core.preview_size(1920, 1080), (640, 360))
        self.assertEqual(core.preview_size(320, 240), (320, 240))          # never upscaled
        self.assertEqual(core.preview_size(1080, 1920)[1] % 2, 0)
        cmd = core.raw_video_command("ffmpeg", "x.ts", 5, (640, 360), 30)
        self.assertEqual(cmd[cmd.index("-f") + 1], "rawvideo")
        self.assertIn("rgb24", cmd)
        self.assertIn("fps=30,scale=640:360:flags=fast_bilinear", cmd)

    def test_raw_frames_are_read_whole_and_late_ones_dropped(self):
        import io
        from msr import clip_editor
        w, h = 4, 2
        frames = b"".join(bytes([i]) * (w * h * 3) for i in range(5))

        class Proc:
            stdout = io.BufferedReader(io.BytesIO(frames), buffer_size=7)   # short reads

            def kill(self):
                pass
        feed = clip_editor.VideoFeed("ffmpeg", "x.ts", 10.0, size=(w, h), fps=10, spawn=lambda cmd: Proc())
        import time
        end = time.monotonic() + 5
        while feed.decoded < 5 and time.monotonic() < end:
            time.sleep(0.01)
        self.assertEqual(feed.decoded, 5)
        self.assertIsNone(feed.take_until(9.9))                    # nothing due yet
        f = feed.take_until(10.25)                                 # frames at 10.0, 10.1, 10.2 due
        self.assertTrue(f.startswith(b"P6\n4 2\n255\n"))
        self.assertEqual(f[-1], 2)                                 # the newest due one
        self.assertEqual(feed.dropped, 2)                          # the two late ones
        self.assertEqual(feed.take_until(10.45)[-1], 4)
        feed.close()

    def test_playback_uses_the_raw_stream_at_source_fps_capped(self):
        from msr import clip_editor
        root = tk.Tk()
        root.withdraw()
        try:
            with tempfile.TemporaryDirectory() as d:
                src = os.path.join(d, "k.ts")
                write_ts(src, [0, 90000])
                with mock.patch.object(clip_editor, "VideoFeed") as feed:
                    ed = clip_editor.ClipEditor(root, src, clips_base=d, show=False, audio_factory=FakeAudio,
                                                frame_grabber=lambda *_a, **_k: PPM, length_reader=lambda _p: 60.0)
                    ed._vinfo = (1920, 1080, 59.94)
                    ed.toggle_play()
                    kw = feed.call_args.kwargs
                    self.assertEqual((kw["size"], kw["fps"]), ((640, 360), 59.94))   # source rate
                    ed.limit_fps.set(True)
                    ed._limit_changed()                                               # restarts at once
                    self.assertEqual(feed.call_args.kwargs["fps"], 30)
                    ed.toggle_play()
                    ed.limit_fps.set(False)
                    ed._vinfo = (1920, 1080, 120.0)
                    ed.toggle_play()
                    self.assertEqual(feed.call_args.kwargs["fps"], 60)                # capped
                    ed.toggle_play()
                    ed._vinfo = None
                    ed.toggle_play()
                    ed.toggle_play()
                    self.assertIsNone(feed.call_args.kwargs["size"])    # unknown: the PPM fallback
                    ed.close()
        finally:
            root.destroy()

    def test_same_size_frames_reuse_one_image(self):
        from msr import clip_editor
        root = tk.Tk()
        root.withdraw()
        try:
            with tempfile.TemporaryDirectory() as d:
                src = os.path.join(d, "k.ts")
                write_ts(src, [0, 90000])
                ed = clip_editor.ClipEditor(root, src, clips_base=d, show=False, audio_factory=FakeAudio,
                                            frame_grabber=lambda *_a, **_k: None, length_reader=lambda _p: 60.0)
                ed._show(PPM)
                first = ed._img
                ed._show(b"P6\n2 2\n255\n" + b"\x10" * 12)
                self.assertIs(ed._img, first)
                ed._show(b"P6\n4 2\n255\n" + b"\x10" * 24)
                self.assertIsNot(ed._img, first)
                ed.close()
        finally:
            root.destroy()


class FarJumpPreviewTests(unittest.TestCase):
    """Paused scrubbing: keyframe first, then the exact frame; the newest position wins."""

    def setUp(self):
        from msr import clip_editor
        self.mod = clip_editor
        self.tmp = tempfile.TemporaryDirectory()
        self.src = os.path.join(self.tmp.name, "k.ts")
        write_ts(self.src, [0, 90000])
        self.state = os.path.join(self.tmp.name, "window_state.json")
        self.root = tk.Tk()
        self.root.withdraw()
        self.calls = []
        self.ed = clip_editor.ClipEditor(self.root, self.src, clips_base=self.tmp.name, show=False,
                                         audio_factory=FakeAudio, frame_grabber=self.grab,
                                         length_reader=lambda _p: 600.0, state_file=self.state)
        import time
        end = time.monotonic() + 3
        while self.ed.marks.length != 600.0 and time.monotonic() < end:
            self.root.update()
            time.sleep(0.01)

    def grab(self, ffmpeg, src, t, keyframe=False, cancel=None):
        self.calls.append((round(t, 2), keyframe))
        return KEY if keyframe else EXACT

    def tearDown(self):
        self.ed.close()
        self.root.destroy()
        self.tmp.cleanup()

    def drain(self):
        import time
        end = time.monotonic() + 3
        while time.monotonic() < end:
            self.root.update()
            time.sleep(0.01)
            if self.ed._exact_gen == self.ed._frame_gen:
                return

    def test_frame_command_fast_keyframe_variant(self):
        key = core.frame_command("ffmpeg", "x.ts", 100, keyframe=True)
        self.assertLess(key.index("nokey"), key.index("-i"))
        self.assertIn("-noaccurate_seek", key)
        self.assertNotIn("nokey", core.frame_command("ffmpeg", "x.ts", 100))

    def test_keyframe_then_exact_and_the_cache(self):
        shown = []
        self.ed._show = lambda ppm: shown.append(ppm)
        self.ed.seek(300)
        self.drain()
        # both decodes start together; the keyframe may land first, never after the exact frame
        self.assertEqual(sorted(self.calls[-2:]), [(300.0, False), (300.0, True)])
        self.assertEqual(shown[-1], EXACT)
        self.assertIn(shown[-2:], ([KEY, EXACT], [EXACT]) if len(shown) >= 2 else ([EXACT],))
        self.ed.seek(10)
        self.drain()
        n = len(self.calls)
        self.ed.seek(300)                       # back to a cached spot: no ffmpeg at all
        self.drain()
        self.assertEqual(len(self.calls), n)
        self.assertEqual(shown[-1], EXACT)

    def test_stale_and_late_frames_are_ignored(self):
        shown = []
        self.ed._show = lambda ppm: shown.append(ppm)
        self.ed._frame_gen = 5
        self.ed._on_frame(4, EXACT, True)       # an older position: ignored
        self.ed._on_frame(5, EXACT, True)
        self.ed._on_frame(5, KEY, False)        # its keyframe arriving after the exact one
        self.assertEqual(shown, [EXACT])

    def test_grab_cancel_kills_the_ffmpeg(self):
        class Proc:
            killed = False
            returncode = None

            def communicate(self, timeout=None):
                if self.killed:
                    return b"", b""
                raise subprocess.TimeoutExpired("ffmpeg", timeout)

            def kill(self):
                self.killed = True
        proc = Proc()
        self.assertIsNone(core.grab_frame("ffmpeg", "x.ts", 5, cancel=lambda: True, spawn=lambda *a, **k: proc))
        self.assertTrue(proc.killed)

    def test_limit_setting_is_remembered(self):
        self.assertFalse(self.ed.limit_fps.get())          # default off
        self.ed.limit_fps.set(True)
        self.ed._limit_changed()
        from msr import winstate
        self.assertTrue(winstate.load(self.state)[self.mod.LIMIT_KEY])
        again = self.mod.ClipEditor(self.root, self.src, clips_base=self.tmp.name, show=False,
                                    audio_factory=FakeAudio, frame_grabber=self.grab,
                                    length_reader=lambda _p: 600.0, state_file=self.state)
        self.assertTrue(again.limit_fps.get())
        self.assertEqual(again.fps_cap(), 30)
        again.close()


KEY = b"P6\n2 2\n255\n" + b"\x01" * 12
EXACT = b"P6\n2 2\n255\n" + b"\x02" * 12


class AgentParityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, self.streams = make_data(self.tmp.name, [{"name": "knut", "enabled": True}])
        self._cd = in_dir(self.data)
        self._cd.__enter__()
        self.c = control.Controller(control.OfflineApp(Config(os.path.join(self.data, "config.ini"))))
        self.mp4 = os.path.join(self.streams, "Processed", "kick", "knut", "knut_1.mp4")
        os.makedirs(os.path.dirname(self.mp4))
        with open(self.mp4, "wb") as fh:
            fh.write(b"\0" * 2048)

    def tearDown(self):
        self._cd.__exit__(None, None, None)
        self.tmp.cleanup()

    def test_clip_a_finished_file_from_to(self):
        res = core.CutResult(os.path.join(self.streams, "Clips", "kick", "knut", "x.mp4"), 3723, 3768, 3721.5,
                             3768, 46.5, False)
        with mock.patch.object(core, "media_length", return_value=7200.0), \
                mock.patch.object(core, "make_clip", return_value=res) as cut:
            reply = self.c.handle("clip", {"file": self.mp4, "from": "1:02:03", "to": "1:02:48"})
        self.assertTrue(reply["ok"], reply)
        r = reply["result"]
        self.assertEqual((r["start"], r["snapped"], r["source"]), (3721.5, True, self.mp4))
        args, kw = cut.call_args
        self.assertEqual((args[2], args[3]), (3723.0, 3768.0))
        self.assertEqual(args[4], os.path.join(self.streams, "Clips", "kick", "knut"))
        self.assertFalse(kw["exact"])

    def test_errors(self):
        with mock.patch.object(core, "media_length", return_value=60.0):
            self.assertEqual(self.c.handle("clip", {"file": self.mp4 + ".no", "from": "1"})["code"], "not_found")
            self.assertEqual(self.c.handle("clip", {"file": self.mp4, "from": "2:00"})["code"], "invalid")
            self.assertEqual(self.c.handle("clip", {"file": self.mp4})["code"], "bad_request")
            self.assertEqual(self.c.handle("clip", {"channel": "knut", "from": "1"})["code"], "not_recording")

    def test_cli_maps_the_flags_and_runs_without_a_running_copy_for_files(self):
        p = msr_ctl.build_parser()
        cmd, args = msr_ctl.to_request(p.parse_args(["clip", "--file", self.mp4, "--from=-30", "--to", "end",
                                                     "--exact"]))
        self.assertEqual((cmd, args), ("clip", {"file": self.mp4, "from": "-30", "to": "end", "exact": True}))
        cmd, args = msr_ctl.to_request(p.parse_args(["clip", "knut", "--seconds", "20"]))
        self.assertEqual(args, {"channel": "knut", "seconds": 20})


if __name__ == "__main__":
    unittest.main()
