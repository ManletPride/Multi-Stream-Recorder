"""Health-review leftovers: disk space, ffprobe path, orphan ffmpeg matching."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.util import (
    _ffmpeg_is_msr_network_orphan,
    check_disk_space,
    ffprobe_from_ffmpeg,
)


class CheckDiskSpaceTests(unittest.TestCase):
    def test_enough_space(self):
        usage = mock.Mock(free=10 * (1024 ** 3))
        with mock.patch("shutil.disk_usage", return_value=usage):
            ok, gb = check_disk_space("E:\\Streams", min_gb=5)
        self.assertTrue(ok)
        self.assertAlmostEqual(gb, 10.0, places=1)

    def test_low_space(self):
        usage = mock.Mock(free=1 * (1024 ** 3))
        with mock.patch("shutil.disk_usage", return_value=usage):
            ok, gb = check_disk_space("E:\\Streams", min_gb=5)
        self.assertFalse(ok)
        self.assertAlmostEqual(gb, 1.0, places=1)

    def test_unreadable_but_writable(self):
        with mock.patch("shutil.disk_usage", side_effect=OSError("boom")):
            with mock.patch("os.makedirs"):
                with mock.patch("builtins.open", mock.mock_open()):
                    with mock.patch("os.remove"):
                        ok, gb = check_disk_space("E:\\Streams", min_gb=5)
        self.assertTrue(ok)
        self.assertIsNone(gb)

    def test_unreadable_and_unwritable_fails_closed(self):
        with mock.patch("shutil.disk_usage", side_effect=OSError("boom")):
            with mock.patch("os.makedirs", side_effect=PermissionError("denied")):
                ok, gb = check_disk_space("E:\\Streams", min_gb=5)
        self.assertFalse(ok)
        self.assertEqual(gb, 0)


class FfprobeFromFfmpegTests(unittest.TestCase):
    def test_alongside_exe_keeps_ffmpeg_directory(self):
        ffmpeg = os.path.join("opt", "ffmpeg", "bin", "ffmpeg.exe")
        got = ffprobe_from_ffmpeg(ffmpeg)
        self.assertEqual(got, os.path.join("opt", "ffmpeg", "bin", "ffprobe.exe"))
        self.assertIn(os.path.join("ffmpeg", "bin"), got)

    def test_bare_name(self):
        got = ffprobe_from_ffmpeg("ffmpeg")
        if os.name == "nt":
            self.assertEqual(got, "ffprobe.exe")
        else:
            self.assertEqual(got, "ffprobe")

    def test_directory_named_ffmpeg_is_not_rewritten(self):
        ffmpeg = os.path.join("opt", "ffmpeg", "bin", "ffmpeg")
        got = ffprobe_from_ffmpeg(ffmpeg)
        self.assertEqual(got, os.path.join("opt", "ffmpeg", "bin", "ffprobe"))


class OrphanFfmpegMatchTests(unittest.TestCase):
    def test_msr_network_capture(self):
        streams = os.path.abspath("streams_root")
        out = os.path.join(streams, "Recorded", "kick", "foo.ts")
        cmd = ["ffmpeg", "-i", "https://kick.com/hls/live.ts", out]
        self.assertTrue(_ffmpeg_is_msr_network_orphan(cmd, streams))

    def test_unrelated_http_encode_is_left_alone(self):
        streams = os.path.abspath("streams_root")
        other = os.path.abspath("encode_out")
        cmd = ["ffmpeg", "-i", "https://example.com/a.m3u8",
               os.path.join(other, "out.mp4")]
        self.assertFalse(_ffmpeg_is_msr_network_orphan(cmd, streams))

    def test_local_remux_is_left_alone(self):
        streams = os.path.abspath("streams_root")
        ts = os.path.join(streams, "Recorded", "a.ts")
        mp4 = os.path.join(streams, "Processed", "a.mp4")
        self.assertFalse(_ffmpeg_is_msr_network_orphan(
            ["ffmpeg", "-i", ts, "-c", "copy", mp4], streams
        ))

    def test_missing_streams_dir_never_matches(self):
        cmd = ["ffmpeg", "-i", "https://x", os.path.abspath("streams_root")]
        self.assertFalse(_ffmpeg_is_msr_network_orphan(cmd, None))
        self.assertFalse(_ffmpeg_is_msr_network_orphan(cmd, ""))


if __name__ == "__main__":
    unittest.main()
