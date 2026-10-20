"""Clip keyframe alignment: elapsed file time, not MPEG-TS PCR."""
import logging
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.worker import (
    _clip_seek_from_packets,
    _elapsed_matches_file_age,
    _elapsed_media_duration,
    _parse_ffprobe_packet_csv,
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


class ElapsedMediaDurationTests(unittest.TestCase):
    def test_twitch_last_pts_is_converted_to_elapsed(self):
        # powdur: format.duration 12686, start_time 12621, file ~65s.
        elapsed = _elapsed_media_duration(12686.033, 12621.0)
        self.assertAlmostEqual(elapsed, 65.033, places=3)

    def test_kick_already_elapsed_duration_is_kept(self):
        self.assertAlmostEqual(_elapsed_media_duration(180.0, 10000.0), 180.0)

    def test_small_start_time_is_not_subtracted(self):
        self.assertAlmostEqual(_elapsed_media_duration(30.0, 1.4), 30.0)

    def test_none_and_zero(self):
        self.assertIsNone(_elapsed_media_duration(None, 12621.0))
        self.assertIsNone(_elapsed_media_duration(0.0, 12621.0))


class TwitchPcrClipSeekTests(unittest.TestCase):
    def test_normalized_duration_seeks_15s_before_live_edge(self):
        start_time = 12621.0
        raw_duration = 12686.033  # last PTS, not elapsed
        elapsed = _elapsed_media_duration(raw_duration, start_time)
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


if __name__ == "__main__":
    unittest.main()
