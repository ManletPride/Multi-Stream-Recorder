"""Clip Editor timeline zoom / scrub / waveform: the Tk-free view and wave, and the hidden editor
driven by synthetic wheel events. Temp dirs only; no window is shown, ffmpeg is faked."""
import os
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import timeline_view as tv, waveform  # noqa: E402
from tests import new_tk_root  # noqa: E402
from tests.test_clip_editor import PPM, FakeAudio, write_ts  # noqa: E402

ALT = 0x20000 if sys.platform.startswith("win") else 0x8


class ViewTests(unittest.TestCase):
    def test_zoom_keeps_anchor_and_resets_at_whole_file(self):
        v = tv.View()
        v.zoom(4, 3000, 7200)            # 2 h file
        a, b = v.span(7200)
        self.assertAlmostEqual(b - a, 1800)
        self.assertAlmostEqual(v.x(3000, 1000, 7200), 3000 / 7200 * 1000)   # same place on screen
        v.zoom(0.1, 3000, 7200)
        self.assertFalse(v.zoomed())

    def test_min_span_and_bounds(self):
        v = tv.View()
        v.zoom(1e6, 5, 10)
        a, b = v.span(10)
        self.assertAlmostEqual(b - a, tv.MIN_SPAN)
        v.pan(-100, 10)
        self.assertEqual(v.span(10)[0], 0.0)
        v.pan(100, 10)
        self.assertAlmostEqual(v.span(10)[1], 10)

    def test_marks_map_both_ways(self):
        v = tv.View()
        v.zoom(10, 600, 3600)
        for t in (v.span(3600)[0] + 1, 600, v.span(3600)[1] - 1):
            self.assertAlmostEqual(v.t(v.x(t, 700, 3600), 700, 3600), t, places=6)

    def test_follow_pages_to_a_sixth(self):
        v = tv.View()
        v.zoom(36, 0, 3600)   # 100 s window at 0
        self.assertFalse(v.follow(50, 3600))
        self.assertTrue(v.follow(150, 3600))
        a, b = v.span(3600)
        self.assertAlmostEqual(150 - a, (b - a) / 6)

    def test_growing_file_keeps_zoomed_span(self):
        v = tv.View()
        v.zoom(10, 50, 100)
        before = v.span(100)
        self.assertEqual(v.span(5000), before)
        self.assertEqual(tv.View().span(5000), (0.0, 5000))

    def test_wheel_mapping_and_step(self):
        self.assertEqual(tv.wheel_action(ALT, 120)[0], "zoom")
        self.assertEqual(tv.wheel_action(0x4, -240), ("seek", -2))
        self.assertEqual(tv.wheel_action(0x1, 120)[0], "pan")
        self.assertEqual(tv.wheel_action(0, 120 * 50)[1], 8)
        self.assertFalse(tv.is_alt(0x8, "win32"))   # NumLock on Windows
        self.assertEqual(tv.seek_step(7200), 300.0)
        self.assertEqual(tv.seek_step(10), 0.2)


class WaveTests(unittest.TestCase):
    def test_parse_and_scale(self):
        self.assertIsNone(waveform.parse_line("frame:1 pts:800 pts_time:0.1"))
        self.assertEqual(waveform.parse_line("lavfi.astats.Overall.RMS_level=-inf"), 0)
        self.assertEqual(waveform.parse_line("lavfi.astats.Overall.RMS_level=0.0"), 255)
        self.assertAlmostEqual(waveform.height_of(waveform.level_of_db(-12)), 1.0, places=2)
        self.assertEqual(waveform.height_of(waveform.level_of_db(-70)), 0.0)

    def test_columns_take_the_loudest(self):
        w = waveform.Wave()
        w.extend([0] * 10 + [255] + [0] * 9)   # 2 s, a peak at 1.0 s
        cols = w.window_columns(0, 2, 4)
        self.assertEqual(cols, [0.0, 0.0, 1.0, 0.0])
        self.assertEqual(w.window_columns(0, 2, 0), [])

    def test_command_is_mono_low_rate_and_resumes(self):
        cmd = waveform.wave_command("ffmpeg", "x.ts", 12.5)
        self.assertIn("-ss", cmd)
        self.assertIn("0:a:0?", cmd)
        self.assertIn("aresample=8000", cmd[cmd.index("-af") + 1])

    def test_cache_round_trip_and_shrink(self):
        with tempfile.TemporaryDirectory() as d:
            src = os.path.join(d, "a.ts")
            with open(src, "wb") as f:
                f.write(b"x" * 100)
            cache = os.path.join(d, "cache")
            w = waveform.Wave()
            w.extend(b"\x01\x02\x03")
            waveform.save_cache(src, cache, w)
            self.assertEqual(bytes(waveform.load_cache(src, cache).levels), b"\x01\x02\x03")
            with open(src, "wb") as f:
                f.write(b"x")
            self.assertIsNone(waveform.load_cache(src, cache))

    def test_default_cache_not_beside_recordings(self):
        with mock.patch.dict(os.environ, {"LOCALAPPDATA": r"C:\Users\u\AppData\Local"}):
            self.assertTrue(waveform.default_cache_dir().startswith(r"C:\Users\u\AppData\Local"))


class EditorZoomTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = os.path.join(self.tmp.name, "Recorded", "kick", "knut", "k.ts")
        os.makedirs(os.path.dirname(self.src))
        write_ts(self.src, [0, 90000])
        self.state = os.path.join(self.tmp.name, "window_state.json")
        self.cache = os.path.join(self.tmp.name, "wavecache")
        self.root = new_tk_root(self)
        self.root.withdraw()
        self.length = 600.0
        self.runs = []
        from msr import clip_editor
        self.feed = mock.patch.object(clip_editor, "VideoFeed")
        self.feed.start()
        self.ed = clip_editor.ClipEditor(
            self.root, self.src, clips_base=os.path.join(self.tmp.name, "Clips"), live=True,
            follow=lambda: self.src, show=False, audio_factory=lambda *_a: FakeAudio(),
            frame_grabber=lambda *_a, **_k: PPM, length_reader=lambda _p: self.length,
            state_file=self.state, wave_runner=self._lines, wave_cache_dir=self.cache)
        self.ed.timeline.configure(width=612)
        self.pump(lambda: self.ed.marks.length == 600.0)

    def _lines(self, start):
        self.runs.append((start, threading.current_thread().name))
        n = int(round((self.length - start) * waveform.RATE_HZ))
        for i in range(n):
            yield "frame:%d\n" % i
            yield "lavfi.astats.Overall.RMS_level=%s\n" % ("-6.0" if i % 2 else "-inf")

    def tearDown(self):
        self.ed.close()
        self.feed.stop()
        self.root.destroy()
        self.tmp.cleanup()

    def pump(self, cond, timeout=5.0):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            self.root.update()
            if cond():
                return
            time.sleep(0.01)
        self.fail("condition not reached")

    def wheel(self, state, delta, x=306):
        return self.ed._wheel(SimpleNamespace(state=state, delta=delta, x=x))

    def test_alt_wheel_zooms_at_the_pointer_and_double_click_resets(self):
        w = self.ed.timeline.winfo_width() - 12
        at = self.ed.view.t(300, w, 600)
        self.assertEqual(self.wheel(ALT, 120 * 3), "break")
        self.assertTrue(self.ed.view.zoomed())
        self.assertAlmostEqual(self.ed.view.t(300, w, 600), at, places=3)
        self.assertIn("\u00d7", self.ed.timeline.itemcget("zoominfo", "text"))
        self.assertTrue(self.ed.timeline.bind("<Double-Button-1>"))   # Tk cannot synthesize a double-click
        self.ed.zoom_reset()
        self.assertFalse(self.ed.view.zoomed())

    def test_ctrl_wheel_steps_with_zoom(self):
        self.ed.seek(300)
        self.wheel(0x4, -120)
        self.assertAlmostEqual(self.ed.pos, 300 + tv.seek_step(600))
        self.ed.view.zoom(60, 300, 600)       # 10 s window
        p = self.ed.pos
        self.wheel(0x4, 120)
        self.assertAlmostEqual(self.ed.pos, p - 0.2)

    def test_plain_wheel_pans_only_when_zoomed(self):
        self.wheel(0, 120)
        self.assertFalse(self.ed.view.zoomed())
        self.ed.seek(300)
        self.ed.view.zoom(10, 300, 600)
        a = self.ed.view.span(600)[0]
        self.wheel(0x1, -120)
        self.assertGreater(self.ed.view.span(600)[0], a)

    def test_scrub_and_marks_correct_when_zoomed(self):
        self.ed.view.zoom(10, 300, 600)
        a, b = self.ed.view.span(600)
        w = self.ed.timeline.winfo_width() - 12
        self.ed._scrub(SimpleNamespace(x=6 + w / 2))
        self.assertAlmostEqual(self.ed.pos, (a + b) / 2, places=1)
        self.ed.mark_in()
        x = self.ed.timeline.coords("mark_I")[0]
        self.assertAlmostEqual(self.ed.view.t(x - 6, w, 600), self.ed.pos, places=1)

    def test_playhead_follows_while_zoomed(self):
        self.ed.seek(10)
        self.ed.view.zoom(30, 10, 600)
        self.ed.seek(500)
        a, b = self.ed.view.span(600)
        self.assertTrue(a <= 500 <= b)
        self.assertTrue(self.ed.timeline.find_withtag("playhead"))

    def test_key_bindings_exist_and_ignore_entries(self):
        for seq in ("<Alt-equal>", "<Alt-minus>", "<Alt-Key-0>", "<plus>", "<minus>", "<Key-0>",
                    "<Control-Left>", "<Control-Right>"):
            self.assertTrue(self.ed.win.bind(seq), seq)
        self.ed.zoom_by(tv.ZOOM_STEP)
        self.assertTrue(self.ed.view.zoomed())
        self.ed.zoom_reset()
        with mock.patch.object(self.ed.win, "focus_get",
                               return_value=SimpleNamespace(winfo_class=lambda: "TEntry")):
            self.assertTrue(self.ed._typing())

    def test_live_growth_keeps_zoom(self):
        self.ed.view.zoom(10, 300, 600)
        span = self.ed.view.span(600)
        self.ed.marks.grow(700)
        self.assertEqual(self.ed.view.span(700), span)

    def test_waveform_off_by_default_then_reads_in_background_and_is_remembered(self):
        self.pump(lambda: True)
        self.assertFalse(self.ed.show_wave.get())
        self.assertEqual(self.runs, [])
        self.ed.show_wave.set(True)
        self.ed._wave_changed()
        self.pump(lambda: self.ed.wave is not None and self.ed.wave.seconds() >= 600)
        self.pump(lambda: self.ed.timeline.find_withtag("wave"))
        self.assertNotEqual(self.runs[0][1], "MainThread")
        from msr import winstate
        self.assertTrue(winstate.load(self.state).get("clip_editor_waveform"))
        self.pump(lambda: os.listdir(self.cache) if os.path.isdir(self.cache) else False)
        # live file grew: the next refresh reads only the new end
        self.length = 650.0
        self.ed.marks.grow(650)
        self.ed._wave_at = 1.0
        self.pump(lambda: len(self.runs) >= 2 and self.ed.wave.seconds() >= 650)
        self.assertAlmostEqual(self.runs[1][0], 600.0)
        self.ed.show_wave.set(False)
        self.ed._wave_changed()
        self.assertFalse(winstate.load(self.state).get("clip_editor_waveform"))
        self.assertFalse(self.ed.timeline.find_withtag("wave"))


if __name__ == "__main__":
    unittest.main()
