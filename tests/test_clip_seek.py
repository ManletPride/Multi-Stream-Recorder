"""Clip keyframe alignment: elapsed file time, not MPEG-TS PCR."""
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.clip_core import _SCAN, _timeline_spans, ts_length, ts_timeline
from msr.worker import (
    _clip_seek_from_packets,
    _elapsed_matches_file_age,
    _parse_ffprobe_packet_csv,
    _probe_duration,
    _ts_packet_elapsed,
    create_clip,
    create_preview_frame,
    create_screenshot,
    preview_scale_filter,
)


class ParseFfprobePacketCsvTests(unittest.TestCase):
    def test_trailing_comma_still_sees_keyframe(self):
        stdout = "10.000000,10.000000,K__,\n10.033000,10.033000,___\n"
        times, keys = _parse_ffprobe_packet_csv(stdout)
        self.assertEqual(times, [10.0, 10.033])
        self.assertEqual(keys, [10.0])

    def test_na_pts_falls_back_to_dts(self):
        stdout = "N/A,12.500000,K__\n"
        times, keys = _parse_ffprobe_packet_csv(stdout)
        self.assertEqual(times, [12.5])
        self.assertEqual(keys, [12.5])

    def test_empty(self):
        self.assertEqual(_parse_ffprobe_packet_csv(""), ([], []))
        self.assertEqual(_parse_ffprobe_packet_csv(None), ([], []))


class ClipSeekFromPacketsTests(unittest.TestCase):
    def test_kick_pcr_seek_is_elapsed_not_raw_pts(self):
        # Live Kick MPEG-TS: PCR ~10000s, file only 3 minutes, 15s clip,
        # 2s GOP. Using raw kf_pts as -ss was the frozen-first-GOP bug.
        start_time = 10000.0
        duration = 180.0
        clip_seconds = 15.0
        # Window around the cut (elapsed 165s → pts 10165).
        times = [start_time + t for t in range(153, 181)]
        keys = [start_time + t for t in range(154, 181, 2)]  # 10154, 10156, …

        result = _clip_seek_from_packets(
            times, keys, clip_seconds, duration, start_time, window_to_eof=True,
        )
        self.assertIsNotNone(result)
        kf_pts, output_duration, seek_args, target_pts = result

        self.assertAlmostEqual(target_pts, start_time + (duration - clip_seconds), places=3)
        self.assertLessEqual(kf_pts, target_pts + 0.05)
        self.assertEqual(seek_args[0], "-ss")
        ss = float(seek_args[1])
        # Elapsed (~164s), never the PCR clock (~10164s).
        self.assertLess(ss, duration)
        self.assertGreater(ss, duration - clip_seconds - 3)
        self.assertAlmostEqual(ss, (kf_pts - start_time) - 0.05, places=3)
        self.assertGreaterEqual(output_duration, clip_seconds)

    def test_seek_never_past_keyframe(self):
        start_time = 0.0
        duration = 60.0
        times = list(range(40, 61))
        keys = [40, 42, 44, 46, 48, 50, 52, 54, 56, 58]
        result = _clip_seek_from_packets(
            times, keys, 15, duration, start_time, window_to_eof=True,
        )
        kf_pts, _out, seek_args, _target = result
        ss = float(seek_args[1])
        self.assertLessEqual(ss, kf_pts - start_time)

    def test_no_packets_returns_none(self):
        self.assertIsNone(_clip_seek_from_packets([], [], 15, 60, 0.0, True))
        self.assertIsNone(_clip_seek_from_packets([1.0], [], 15, 60, 0.0, True))

    def test_no_duration_uses_sseof(self):
        times = list(range(0, 20))
        keys = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
        result = _clip_seek_from_packets(
            times, keys, 15, None, 0.0, window_to_eof=True,
        )
        self.assertIsNotNone(result)
        _kf, _out, seek_args, _target = result
        self.assertEqual(seek_args[0], "-sseof")


class ElapsedMatchesFileAgeTests(unittest.TestCase):
    def test_nicklee_pcr_reset_is_untrusted(self):
        # 16:40 clip: elapsed 2024s on a 5990s capture.
        with mock.patch("msr.worker._file_wall_age_seconds", return_value=5990.0):
            self.assertFalse(_elapsed_matches_file_age(2023.9, "nicklee.ts"))

    def test_normal_live_file_is_trusted(self):
        with mock.patch("msr.worker._file_wall_age_seconds", return_value=305.0):
            self.assertTrue(_elapsed_matches_file_age(300.0, "ok.ts"))

    def test_unknown_age_keeps_elapsed(self):
        with mock.patch("msr.worker._file_wall_age_seconds", return_value=None):
            self.assertTrue(_elapsed_matches_file_age(2024.0, "posix.ts"))

    def test_young_file_slack(self):
        with mock.patch("msr.worker._file_wall_age_seconds", return_value=40.0):
            self.assertTrue(_elapsed_matches_file_age(20.0, "young.ts"))


class ClipPcrResetTests(unittest.TestCase):
    def test_untrusted_elapsed_probes_without_duration(self):
        seen = {}

        def fake_find(ffmpeg_path, raw_file, clip_seconds, duration, logger, timeout=15):
            seen["duration"] = duration
            return None

        with tempfile.TemporaryDirectory() as td:
            raw = os.path.join(td, "live.ts")
            out = os.path.join(td, "clip.mp4")
            with open(raw, "wb") as fh:
                fh.write(b"\x00" * 2048)
            with mock.patch("msr.worker._probe_duration", return_value=2023.9):
                with mock.patch("msr.worker._elapsed_matches_file_age", return_value=False):
                    with mock.patch("msr.worker._find_clip_keyframe", side_effect=fake_find):
                        with mock.patch("msr.worker.subprocess.run") as run:
                            run.return_value = mock.Mock(returncode=1, stderr="fail\n")
                            create_clip(raw, out, 30, "ffmpeg", logging.getLogger("test"))
        self.assertIn("duration", seen)
        self.assertIsNone(seen["duration"])


class ScreenshotSeekTests(unittest.TestCase):
    def test_prefers_sseof_over_duration_ss(self):
        cmds = []

        def fake_run(cmd, **kwargs):
            cmds.append(list(cmd))
            outp = cmd[-1]
            with open(outp, "wb") as fh:
                fh.write(b"\xff" * 2048)
            return mock.Mock(returncode=0, stderr="")

        with tempfile.TemporaryDirectory() as td:
            raw = os.path.join(td, "live.ts")
            out = os.path.join(td, "shot.jpg")
            with open(raw, "wb") as fh:
                fh.write(b"\x00" * 2048)
            with mock.patch("msr.worker.subprocess.run", side_effect=fake_run):
                with mock.patch("msr.worker._probe_duration", return_value=2024.0):
                    ok, _msg = create_screenshot(
                        raw, out, "ffmpeg", logging.getLogger("test"),
                    )
        self.assertTrue(ok)
        self.assertTrue(cmds)
        self.assertIn("-sseof", cmds[0])
        self.assertNotIn("-ss", cmds[0])


class TwitchPcrClipSeekTests(unittest.TestCase):
    def test_normalized_duration_seeks_15s_before_live_edge(self):
        start_time = 12621.0
        raw_duration = 12686.033  # last PTS, not elapsed
        # The packets below really do span last−first. That span is what
        # Clip Now measures; it is not guessed from start_time.
        elapsed = raw_duration - start_time
        clip_seconds = 15.0
        times = [start_time + t for t in range(48, 66)]
        keys = [start_time + t for t in range(48, 66, 2)]
        result = _clip_seek_from_packets(
            times, keys, clip_seconds, elapsed, start_time, window_to_eof=True,
        )
        self.assertIsNotNone(result)
        kf_pts, output_duration, seek_args, _target = result
        ss = float(seek_args[1])
        self.assertEqual(seek_args[0], "-ss")
        self.assertLess(ss, elapsed)
        self.assertGreater(ss, elapsed - clip_seconds - 3)
        self.assertLess(output_duration, elapsed + 5)
        self.assertGreaterEqual(output_duration, clip_seconds)
        self.assertLess(kf_pts, start_time + elapsed)

    def test_raw_pcr_duration_would_seek_near_eof(self):
        # Guard: passing last-PTS as duration without normalizing is the
        # powdur 2.6s bug (seek ~65s into a 65s file).
        start_time = 12621.0
        raw_duration = 12686.033
        times = [start_time + t for t in range(48, 66)]
        keys = [start_time + t for t in range(48, 66, 2)]
        result = _clip_seek_from_packets(
            times, keys, 15.0, raw_duration, start_time, window_to_eof=False,
        )
        ss = float(result[2][1])
        elapsed = raw_duration - start_time
        self.assertGreater(ss, elapsed - 5)


class PreviewFrameTests(unittest.TestCase):
    def test_scale_filter_letterboxes(self):
        vf = preview_scale_filter(160, 90)
        self.assertIn("scale=160:90:force_original_aspect_ratio=decrease", vf)
        self.assertIn("pad=160:90:(ow-iw)/2:(oh-ih)/2:black", vf)

    def test_missing_file_fails_closed(self):
        ok, msg = create_preview_frame(
            os.path.join("no", "such", "file.ts"),
            os.path.join("no", "out.png"),
            "ffmpeg",
            logging.getLogger("test"),
        )
        self.assertFalse(ok)
        self.assertIn("not found", msg)


# vimp_20261114_234642.part4.ts, Clip Now at ~03:21. ffprobe reported
# start_time=2658.067 and an elapsed duration; the live edge was ~10186 s.
# Subtracting start_time twice sought the 120 s clip at ~4870 s.
VIMP_START = 2658.067
VIMP_ELAPSED = 10186.0
VIMP_START_TICK = 239226030          # 2658.067 * 90000
VIMP_ELAPSED_TICKS = 10186 * 90000


def _ts_packet(pts=None, stream_id=0xE0):
    """One 188-byte TS packet; with pts, a PES start carrying that 90 kHz PTS."""
    if pts is None:
        return b"\x47\x00\x11\x10" + b"\xff" * 184
    b = bytes([0x21 | ((pts >> 29) & 0x0E), (pts >> 22) & 0xFF, 0x01 | ((pts >> 14) & 0xFE),
               (pts >> 7) & 0xFF, 0x01 | ((pts << 1) & 0xFE)])
    pes = b"\x00\x00\x01" + bytes([stream_id]) + b"\x00\x00\x80\x80\x05" + b
    return b"\x47\x40\x11\x10" + pes + b"\xff" * (184 - len(pes))


def _write_segments(path, segments):
    """``("pts", [ticks])`` or ``("pad", nbytes)`` (rounded down to whole packets)."""
    blank = _ts_packet()
    with open(path, "wb") as fh:
        for kind, val in segments:
            if kind == "pts":
                for p in val:
                    fh.write(_ts_packet(p))
            else:
                fh.write(blank * (val // 188))


def _vimp_ticks():
    start = VIMP_START_TICK
    end = start + VIMP_ELAPSED_TICKS
    head = [start, start + 90000, start + 2 * 90000]
    tail = [end - 2 * 90000, end - 90000, end]
    return head, tail


def _write_vimp(path, pad=3 * 1024 * 1024):
    """Monotonic clock, head and tail in different reads, elapsed exactly 10186 s."""
    head, tail = _vimp_ticks()
    _write_segments(path, [("pts", head), ("pad", pad), ("pad", pad), ("pts", tail)])


def _write_reset(path):
    """Head PTS 1000 s, a jump 5000→100 in the middle, tail PTS 4000 s.

    Head-to-tail alone looks like a smooth 3000 s. The jump is only in between.
    """
    _write_segments(path, [
        ("pts", [1000 * 90000]),
        ("pad", 3 * 1024 * 1024),
        ("pts", [5000 * 90000, 100 * 90000]),
        ("pad", 3 * 1024 * 1024),
        ("pts", [4000 * 90000]),
    ])


class TimelineSpanTests(unittest.TestCase):
    def test_a_long_file_is_sampled_not_read_through(self):
        size = 6_200_000_000
        spans = _timeline_spans(size, 9, _SCAN)
        self.assertLessEqual(len(spans), 9)
        self.assertLess(sum(n for _, n in spans), 20 * 1024 * 1024)
        self.assertEqual(spans[0][0], 0)
        self.assertEqual(spans[-1][0] + spans[-1][1], size)
        for (a, alen), (b, _blen) in zip(spans, spans[1:]):
            self.assertLessEqual(a + alen, b)

    def test_a_short_file_is_covered(self):
        size = 6 * 1024 * 1024
        spans = _timeline_spans(size, 9, _SCAN)
        pos = 0
        for off, n in spans:
            self.assertEqual(off, pos)
            pos += n
        self.assertEqual(pos, size)


class PacketClockTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_vimp_elapsed_is_last_minus_first(self):
        # The old guess (duration > start_time >= 1000 → subtract start_time),
        # applied in the probe and again in the keyframe search:
        # 10186 - 2*2658.067 ≈ 4869.9, which is where the 120 s clip landed.
        path = os.path.join(self.tmp.name, "vimp.part4.ts")
        _write_vimp(path)
        clock = ts_timeline(path)
        self.assertTrue(clock["continuous"])
        self.assertAlmostEqual(clock["elapsed"], VIMP_ELAPSED, places=3)
        self.assertAlmostEqual(clock["first"], VIMP_START, places=3)
        elapsed, how = _ts_packet_elapsed(path)
        self.assertEqual(how, "pts")
        self.assertAlmostEqual(elapsed, VIMP_ELAPSED, places=3)
        wrong_twice = VIMP_ELAPSED - 2 * VIMP_START
        self.assertAlmostEqual(wrong_twice, 4869.866, delta=0.01)
        self.assertGreater(abs(elapsed - wrong_twice), 5000)

    def test_vimp_agrees_with_editor_length(self):
        path = os.path.join(self.tmp.name, "vimp.ts")
        _write_vimp(path)
        self.assertAlmostEqual(ts_timeline(path)["elapsed"], ts_length(path), places=3)

    def test_twitch_span_is_elapsed_not_the_last_pts(self):
        # powdur: format.duration was the last PTS (12686), file ~65 s.
        path = os.path.join(self.tmp.name, "powdur.ts")
        start = int(round(12621.0 * 90000))
        end = int(round(12686.033 * 90000))
        ticks = list(range(start, end, 90000))
        ticks.append(end)
        _write_segments(path, [("pts", ticks)])
        elapsed, how = _ts_packet_elapsed(path)
        self.assertEqual(how, "pts")
        self.assertAlmostEqual(elapsed, 65.033, delta=0.02)
        self.assertLess(elapsed, 100)

    def test_kick_short_and_small_start_keep_their_span(self):
        path = os.path.join(self.tmp.name, "kick.ts")
        start = 10000 * 90000
        _write_segments(path, [("pts", [start + i * 90000 for i in range(0, 181, 30)])])
        elapsed, how = _ts_packet_elapsed(path)
        self.assertEqual(how, "pts")
        self.assertAlmostEqual(elapsed, 180.0, places=3)

        small = os.path.join(self.tmp.name, "small.ts")
        origin = int(round(1.4 * 90000))
        _write_segments(small, [("pts", [origin + i * 90000 for i in range(0, 31, 5)])])
        elapsed, how = _ts_packet_elapsed(small)
        self.assertEqual(how, "pts")
        self.assertAlmostEqual(elapsed, 30.0, places=3)

    def test_a_small_reorder_is_not_a_reset(self):
        path = os.path.join(self.tmp.name, "reorder.ts")
        base = 2658 * 90000
        _write_segments(path, [("pts", [base + 4500, base, base + 90000, base + 5 * 90000])])
        clock = ts_timeline(path)
        self.assertTrue(clock["continuous"])
        self.assertAlmostEqual(clock["elapsed"], 5.0, places=3)

    def test_pts_wrap_stays_continuous(self):
        from msr.clip_core import PTS_WRAP
        path = os.path.join(self.tmp.name, "wrap.ts")
        near = PTS_WRAP - 90000 * 2
        ticks = [near + 4500, near] + [(near + i * 90000) % PTS_WRAP for i in range(1, 6)]
        _write_segments(path, [("pts", ticks)])
        clock = ts_timeline(path)
        self.assertTrue(clock["continuous"])
        self.assertAlmostEqual(clock["elapsed"], ts_length(path), places=3)
        self.assertAlmostEqual(clock["elapsed"], 5.0, delta=0.1)

    def test_backward_jump_in_one_window_is_a_reset(self):
        path = os.path.join(self.tmp.name, "jump.ts")
        _write_segments(path, [("pts", [
            1000 * 90000, 2000 * 90000, 3500 * 90000, 400 * 90000, 900 * 90000,
        ])])
        clock = ts_timeline(path)
        self.assertFalse(clock["continuous"])
        self.assertIsNone(clock["elapsed"])
        self.assertEqual(_ts_packet_elapsed(path), (None, "reset"))

    def test_mid_file_jump_is_seen_between_the_ends(self):
        # Ends alone are 1000 → 4000, a smooth 3000 s. The reset is in the middle.
        path = os.path.join(self.tmp.name, "nicklee.ts")
        _write_reset(path)
        self.assertGreater(os.path.getsize(path), 4 * 1024 * 1024)
        clock = ts_timeline(path)
        self.assertFalse(clock["continuous"])
        self.assertIsNone(clock["elapsed"])
        self.assertEqual(_probe_duration("ffmpeg", path), None)

    def test_junk_ts_keeps_the_ffprobe_duration_and_does_not_subtract(self):
        # No packet clock. ffprobe's 10186 s is already elapsed; subtracting
        # the 2658 s start (the old rule) is the bug, one subtraction deep.
        path = os.path.join(self.tmp.name, "junk.ts")
        with open(path, "wb") as fh:
            fh.write(b"not a transport stream\n" + b"\x00" * 4096)

        def fake_run(cmd, **kwargs):
            return mock.Mock(returncode=0, stdout="duration=10186.000000\nstart_time=2658.067000\n", stderr="")

        with mock.patch("msr.worker.subprocess.run", side_effect=fake_run):
            got = _probe_duration("ffmpeg", path)
        self.assertAlmostEqual(got, VIMP_ELAPSED, places=3)
        self.assertNotAlmostEqual(got, VIMP_ELAPSED - VIMP_START, delta=1)


def _ffmpeg_argv(cmds):
    for cmd in cmds:
        if cmd and os.path.basename(str(cmd[0])).startswith("ffmpeg") and "-c" in cmd:
            return cmd
    return None


class LiveEdgeClipTests(unittest.TestCase):
    """Clip Now / Screenshot / preview seek the real live edge on these numbers."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.raw = os.path.join(self.tmp.name, "vimp_20261114_234642.part4.ts")
        _write_vimp(self.raw)
        self.cmds = []

    def _run(self, packet_window_ok=True):
        def fake_run(cmd, **kwargs):
            argv = list(cmd)
            self.cmds.append(argv)
            joined = " ".join(str(p) for p in argv)
            if "format=duration" in joined:
                raise AssertionError(f"duration must come from packets, not ffprobe: {argv}")
            if "format=start_time" in joined:
                return mock.Mock(returncode=0, stdout=f"{VIMP_START:.3f}\n", stderr="")
            if "packet=pts_time" in joined:
                if not packet_window_ok:
                    return mock.Mock(returncode=0, stdout="", stderr="")
                window = float(argv[argv.index("-read_intervals") + 1].split("%")[0])
                if window < 12000:
                    return mock.Mock(returncode=0, stdout="", stderr="")
                kf = VIMP_START + VIMP_ELAPSED - 120 - 1
                return mock.Mock(
                    returncode=0,
                    stdout=f"{kf:.3f},{kf:.3f},K__,\n{kf + 1:.3f},{kf + 1:.3f},___\n",
                    stderr="",
                )
            outp = argv[-1]
            with open(outp, "wb") as fh:
                fh.write(b"\xff" * 4096)
            return mock.Mock(returncode=0, stderr="")
        return fake_run

    def test_clip_now_seeks_the_live_edge_without_a_birth_time(self):
        out = os.path.join(self.tmp.name, "clip.mp4")
        with mock.patch("msr.worker._file_wall_age_seconds", return_value=None):
            with mock.patch("msr.worker.subprocess.run", side_effect=self._run()):
                ok, msg = create_clip(self.raw, out, 120, "ffmpeg", logging.getLogger("test"))
        self.assertTrue(ok, msg)
        ffmpeg = _ffmpeg_argv(self.cmds)
        self.assertIsNotNone(ffmpeg)
        self.assertIn("-ss", ffmpeg)
        ss = float(ffmpeg[ffmpeg.index("-ss") + 1])
        # Keyframe 1 s before the cut, then the 0.05 s undershoot: ~10065.
        self.assertGreater(ss, 10000)
        self.assertLess(ss, VIMP_ELAPSED)
        self.assertGreater(ss, VIMP_ELAPSED - VIMP_START)          # not even one subtraction
        probes = [c for c in self.cmds if "-read_intervals" in c]
        self.assertTrue(probes)
        window = float(probes[0][probes[0].index("-read_intervals") + 1].split("%")[0])
        self.assertGreater(window, 12000)

    def test_pcr_reset_seeks_from_eof_without_a_birth_time(self):
        path = os.path.join(self.tmp.name, "reset.ts")
        _write_reset(path)
        out = os.path.join(self.tmp.name, "clip.mp4")
        with mock.patch("msr.worker._file_wall_age_seconds", return_value=None):
            with mock.patch("msr.worker.subprocess.run", side_effect=self._run(packet_window_ok=False)):
                with self.assertLogs("test", level="INFO") as logs:
                    ok, msg = create_clip(path, out, 120, "ffmpeg", logging.getLogger("test"))
        self.assertTrue(ok, msg)
        self.assertTrue(any("jump backwards" in line for line in logs.output))
        ffmpeg = _ffmpeg_argv(self.cmds)
        self.assertIn("-sseof", ffmpeg)
        self.assertNotIn("-ss", ffmpeg)

    def test_birth_time_still_rejects_a_short_span_the_scan_missed(self):
        # Monotonic 2024 s of packets, but the file has existed for 5990 s.
        path = os.path.join(self.tmp.name, "aged.ts")
        start = 10000 * 90000
        # 20 s steps stay under the within-window splice check, so the clock
        # looks continuous and only the birth time can reject it.
        ticks = [start + i * 20 * 90000 for i in range(0, 2024 // 20)]
        ticks.append(start + 2024 * 90000)
        _write_segments(path, [("pts", ticks)])
        out = os.path.join(self.tmp.name, "clip.mp4")
        with mock.patch("msr.worker._file_wall_age_seconds", return_value=5990.0):
            with mock.patch("msr.worker.subprocess.run", side_effect=self._run(packet_window_ok=False)):
                ok, _msg = create_clip(path, out, 30, "ffmpeg", logging.getLogger("test"))
        self.assertTrue(ok)
        ffmpeg = _ffmpeg_argv(self.cmds)
        self.assertIn("-sseof", ffmpeg)
        self.assertNotIn("-ss", ffmpeg)

    def test_screenshot_ss_fallback_uses_the_live_edge(self):
        out = os.path.join(self.tmp.name, "shot.jpg")

        def fake_run(cmd, **kwargs):
            argv = list(cmd)
            self.cmds.append(argv)
            if "-sseof" in argv:
                return mock.Mock(returncode=1, stderr="no frame\n")
            with open(argv[-1], "wb") as fh:
                fh.write(b"\xff" * 2048)
            return mock.Mock(returncode=0, stderr="")

        with mock.patch("msr.worker._file_wall_age_seconds", return_value=None):
            with mock.patch("msr.worker.subprocess.run", side_effect=fake_run):
                ok, msg = create_screenshot(self.raw, out, "ffmpeg", logging.getLogger("test"))
        self.assertTrue(ok, msg)
        ss_cmds = [c for c in self.cmds if "-ss" in c]
        self.assertEqual(len(ss_cmds), 1)
        ss = float(ss_cmds[0][ss_cmds[0].index("-ss") + 1])
        self.assertAlmostEqual(ss, VIMP_ELAPSED - 3, delta=1)
        self.assertGreater(ss, VIMP_ELAPSED - VIMP_START)

    def test_preview_ss_fallback_uses_the_live_edge(self):
        out = os.path.join(self.tmp.name, "preview.png")

        def fake_run(cmd, **kwargs):
            argv = list(cmd)
            self.cmds.append(argv)
            with open(argv[-1], "wb") as fh:
                fh.write(b"\x89PNG" + b"\xff" * 400)
            return mock.Mock(returncode=0, stderr="")

        with mock.patch("msr.worker._file_wall_age_seconds", return_value=None):
            with mock.patch("msr.worker.subprocess.run", side_effect=fake_run):
                ok, msg = create_preview_frame(
                    self.raw, out, "ffmpeg", logging.getLogger("test"),
                    attempts=[("ss", 3.0, False)],
                )
        self.assertTrue(ok, msg)
        ss_cmds = [c for c in self.cmds if "-ss" in c]
        self.assertEqual(len(ss_cmds), 1)
        ss = float(ss_cmds[0][ss_cmds[0].index("-ss") + 1])
        self.assertAlmostEqual(ss, VIMP_ELAPSED - 3, delta=1)


class TwitchClipNowTests(unittest.TestCase):
    def test_fifteen_second_clip_seeks_before_the_real_edge(self):
        # powdur: a 15 s request used to keep ~2.6 s because duration was the last PTS.
        with tempfile.TemporaryDirectory() as td:
            raw = os.path.join(td, "powdur.ts")
            start = int(round(12621.0 * 90000))
            end = start + int(round(65.033 * 90000))
            ticks = [start + i * 90000 for i in range(0, 66)]
            ticks.append(end)
            _write_segments(raw, [("pts", ticks)])
            out = os.path.join(td, "clip.mp4")
            cmds = []

            def fake_run(cmd, **kwargs):
                argv = list(cmd)
                cmds.append(argv)
                joined = " ".join(str(p) for p in argv)
                if "format=duration" in joined:
                    raise AssertionError("packet span must win over format.duration")
                if "format=start_time" in joined:
                    return mock.Mock(returncode=0, stdout="12621.000\n", stderr="")
                if "packet=pts_time" in joined:
                    window = float(argv[argv.index("-read_intervals") + 1].split("%")[0])
                    if not (12621 < window < 12690):
                        return mock.Mock(returncode=0, stdout="", stderr="")
                    kf = 12621.0 + 65.033 - 15 - 1
                    return mock.Mock(returncode=0, stdout=f"{kf:.3f},{kf:.3f},K__,\n", stderr="")
                with open(argv[-1], "wb") as fh:
                    fh.write(b"\xff" * 4096)
                return mock.Mock(returncode=0, stderr="")

            with mock.patch("msr.worker._file_wall_age_seconds", return_value=None):
                with mock.patch("msr.worker.subprocess.run", side_effect=fake_run):
                    ok, msg = create_clip(raw, out, 15, "ffmpeg", logging.getLogger("test"))
        self.assertTrue(ok, msg)
        ffmpeg = _ffmpeg_argv(cmds)
        ss = float(ffmpeg[ffmpeg.index("-ss") + 1])
        self.assertGreater(ss, 40)
        self.assertLess(ss, 65)


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg not installed")
class RealPacketClockTests(unittest.TestCase):
    def test_high_pcr_start_matches_ffprobe_elapsed(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "offset.ts")
            subprocess.run(
                ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                 "-f", "lavfi", "-i", "testsrc=size=160x120:rate=30",
                 "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
                 "-t", "3", "-c:v", "libx264", "-g", "30", "-c:a", "aac",
                 "-output_ts_offset", "2658", "-f", "mpegts", path],
                check=True, capture_output=True, timeout=60,
            )
            probe = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=start_time,duration",
                 "-of", "default=noprint_wrappers=1", path],
                check=True, capture_output=True, text=True, timeout=30,
            )
            reported = {}
            for line in probe.stdout.splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    reported[k] = float(v)
            elapsed, how = _ts_packet_elapsed(path)
            self.assertEqual(how, "pts")
            self.assertAlmostEqual(elapsed, reported["duration"], delta=0.5)
            self.assertGreater(reported["start_time"], 2650)
            self.assertLess(elapsed, 10)
            self.assertAlmostEqual(_probe_duration("ffmpeg", path), elapsed, delta=0.05)


if __name__ == "__main__":
    unittest.main()
