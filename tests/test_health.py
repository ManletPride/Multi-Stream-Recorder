"""Health-review leftovers: disk space, ffprobe path, orphan matching, single instance."""
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.util import (
    _ffmpeg_is_msr_network_orphan,
    _is_multiprocessing_spawn,
    _plan_orphan_kills,
    _spawn_parent_pid,
    check_disk_space,
    ffprobe_from_ffmpeg,
    release_single_instance,
    single_instance_mutex_name,
    try_acquire_single_instance,
    window_title_is_msr,
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


def _spawn_cmd(parent_pid):
    return (
        'python -c "from multiprocessing.spawn import spawn_main; '
        f'spawn_main(parent_pid={parent_pid}, pipe_handle=1)" '
        "--multiprocessing-fork"
    )


def _proc(pid, name, cmdline, parent_pid):
    return {
        "pid": pid,
        "name": name,
        "cmdline": cmdline,
        "parent_pid": parent_pid,
    }


class SpawnCmdlineTests(unittest.TestCase):
    def test_spawn_main_detected(self):
        self.assertTrue(_is_multiprocessing_spawn(_spawn_cmd(37184)))
        self.assertFalse(_is_multiprocessing_spawn(
            ["python", "Multi-Stream-Recorder.py"]
        ))

    def test_parent_pid_parsed(self):
        self.assertEqual(_spawn_parent_pid(_spawn_cmd(37184)), 37184)
        self.assertIsNone(_spawn_parent_pid("python Multi-Stream-Recorder.py"))


class OrphanWorkerPlanTests(unittest.TestCase):
    def setUp(self):
        self.streams = os.path.abspath("streams_root")
        self.out = os.path.join(self.streams, "Recorded", "fishtank", "a.ts")
        self.ff_cmd = [
            "ffmpeg", "-i", "https://streams-f.fishtank.live/hls/index.m3u8",
            self.out,
        ]

    def test_kills_ghost_worker_not_just_ffmpeg(self):
        procs = [
            _proc(38460, "python.exe", "python Multi-Stream-Recorder.py", 1),
            _proc(37892, "python.exe", _spawn_cmd(38460), 38460),
            _proc(11, "ffmpeg.exe", self.ff_cmd, 37892),
            _proc(38496, "python.exe", _spawn_cmd(37184), 4),
            _proc(37752, "ffmpeg.exe", self.ff_cmd, 38496),
        ]
        keep = {38460, 37892, 11}
        workers, captures = _plan_orphan_kills(procs, self.streams, keep)
        self.assertEqual(workers, [38496])
        self.assertEqual(captures, [])

    def test_does_not_kill_this_instance(self):
        procs = [
            _proc(38460, "python.exe", "python Multi-Stream-Recorder.py", 1),
            _proc(37892, "python.exe", _spawn_cmd(38460), 38460),
            _proc(11, "ffmpeg.exe", self.ff_cmd, 37892),
        ]
        workers, captures = _plan_orphan_kills(
            procs, self.streams, {38460, 37892, 11},
        )
        self.assertEqual(workers, [])
        self.assertEqual(captures, [])

    def test_reconnect_grace_via_open_log(self):
        ghost = _proc(38496, "python.exe", _spawn_cmd(37184), 4)
        log = os.path.join(self.streams, "stream_recorder.log")
        workers, captures = _plan_orphan_kills(
            [ghost], self.streams, {1}, spawn_open_paths={38496: [log]},
        )
        self.assertEqual(workers, [38496])
        self.assertEqual(captures, [])

    def test_unrelated_spawn_left_alone(self):
        other = _proc(99, "python.exe", _spawn_cmd(50), 4)
        workers, captures = _plan_orphan_kills([other], self.streams, {1})
        self.assertEqual(workers, [])
        self.assertEqual(captures, [])

    def test_sibling_manager_killed_with_workers(self):
        procs = [
            _proc(38496, "python.exe", _spawn_cmd(37184), 4),
            _proc(18148, "python.exe", _spawn_cmd(37184), 4),
            _proc(37752, "ffmpeg.exe", self.ff_cmd, 38496),
        ]
        workers, captures = _plan_orphan_kills(procs, self.streams, {1})
        self.assertEqual(workers, [18148, 38496])
        self.assertEqual(captures, [])

    def test_capture_with_dead_parent_is_killed(self):
        ff = _proc(12200, "ffmpeg.exe", self.ff_cmd, 1)
        workers, captures = _plan_orphan_kills([ff], self.streams, {50})
        self.assertEqual(workers, [])
        self.assertEqual(captures, [12200])

    def test_streamlink_capture_finds_worker(self):
        sl_cmd = [
            "streamlink", "https://kick.com/foo", "best",
            "-o", self.out,
        ]
        procs = [
            _proc(25096, "python.exe", _spawn_cmd(37184), 4),
            _proc(9, "streamlink.exe", sl_cmd, 25096),
        ]
        workers, captures = _plan_orphan_kills(procs, self.streams, {1})
        self.assertEqual(workers, [25096])
        self.assertEqual(captures, [])

    def test_local_remux_does_not_mark_worker(self):
        ts = os.path.join(self.streams, "Recorded", "a.ts")
        mp4 = os.path.join(self.streams, "Processed", "a.mp4")
        procs = [
            _proc(20, "python.exe", _spawn_cmd(8), 4),
            _proc(21, "ffmpeg.exe", ["ffmpeg", "-i", ts, "-c", "copy", mp4], 20),
        ]
        workers, captures = _plan_orphan_kills(procs, self.streams, {1})
        self.assertEqual(workers, [])
        self.assertEqual(captures, [])

    def test_unrelated_http_encode_left_alone(self):
        other = os.path.abspath("encode_out")
        cmd = ["ffmpeg", "-i", "https://example.com/a.m3u8",
               os.path.join(other, "out.mp4")]
        ff = _proc(7, "ffmpeg.exe", cmd, 1)
        workers, captures = _plan_orphan_kills([ff], self.streams, {1})
        self.assertEqual(workers, [])
        self.assertEqual(captures, [])


class WindowTitleTests(unittest.TestCase):
    def test_main_window(self):
        self.assertTrue(window_title_is_msr("Multi-Stream Recorder v2.0.2"))

    def test_error_dialog_is_not_main_window(self):
        self.assertFalse(
            window_title_is_msr("Multi-Stream Recorder — Missing Dependencies")
        )
        self.assertFalse(window_title_is_msr(""))


class SingleInstanceTests(unittest.TestCase):
    def tearDown(self):
        release_single_instance()

    def test_mutex_name_stable_per_folder(self):
        a = single_instance_mutex_name(r"E:\Streams")
        b = single_instance_mutex_name(r"E:\Streams")
        c = single_instance_mutex_name(r"E:\Other")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertTrue(a.startswith("Local\\MultiStreamRecorder-"))

    def test_second_acquire_same_folder_fails(self):
        d = tempfile.mkdtemp(prefix="msr-lock-")
        self.addCleanup(lambda: shutil.rmtree(d, ignore_errors=True))
        self.assertTrue(try_acquire_single_instance(d))
        import msr.util as utilmod
        saved = utilmod._instance_lock
        utilmod._instance_lock = None
        try:
            self.assertFalse(try_acquire_single_instance(d))
        finally:
            utilmod._instance_lock = saved

    def test_same_process_second_call_is_ok(self):
        d = tempfile.mkdtemp(prefix="msr-lock-")
        self.addCleanup(lambda: shutil.rmtree(d, ignore_errors=True))
        self.assertTrue(try_acquire_single_instance(d))
        self.assertTrue(try_acquire_single_instance(d))


if __name__ == "__main__":
    unittest.main()
