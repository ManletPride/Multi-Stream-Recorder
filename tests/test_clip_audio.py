"""Clip Editor sound: paced silence, Linux player sink, volume baked into ffmpeg.

No real audio device. Pipe tests drive a Python process standing in for pw-play.
"""
import array
import io
import os
import shutil
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import clip_audio  # noqa: E402
from msr.nowindow import no_window_kwargs  # noqa: E402

HAS_FFMPEG = bool(shutil.which("ffmpeg"))
POSIX = clip_audio.fcntl is not None


def _pcm(frames: int, value: int = 1) -> bytes:
    return array.array("h", [value, value] * frames).tobytes()


class GainTests(unittest.TestCase):
    def test_full_scale_and_silence_do_not_walk_the_samples(self):
        data = b"\x10\x00\xf0\xff"
        self.assertIs(clip_audio.apply_gain(data, 1.0), data)
        self.assertIs(clip_audio.apply_gain(data, 1.0004), data)
        self.assertEqual(clip_audio.apply_gain(data, 0.5), b"\x08\x00\xf8\xff")
        self.assertEqual(clip_audio.apply_gain(data, 0.0), b"\0" * 4)
        loud = array.array("h", [30000, -30000]).tobytes()
        out = array.array("h")
        out.frombytes(clip_audio.apply_gain(loud, 2.0))
        self.assertEqual(list(out), [32767, -32768])

    def test_one_second_at_full_scale_is_the_same_buffer(self):
        data = _pcm(clip_audio.RATE)
        self.assertIs(clip_audio.apply_gain(data, 1.0), data)

    def test_pcm_command_bakes_a_partial_volume_and_leaves_full_scale_alone(self):
        quiet = clip_audio.pcm_command("ffmpeg", Path("a.ts"), 1.25, 1.0, 0.8)
        self.assertEqual(quiet[quiet.index("-ss") + 1], "1.250")
        self.assertEqual(quiet[quiet.index("-af") + 1], "volume=0.8000")
        self.assertEqual(quiet[-1], "pipe:1")
        full = clip_audio.pcm_command("ffmpeg", Path("a.ts"), 0, 1.0, 1.0)
        self.assertNotIn("-af", full)

    def test_player_commands_are_raw_s16_with_an_explicit_latency(self):
        for name in ("pw-play", "pw-cat"):
            cmd = clip_audio.pipe_player_command(name, 100)
            self.assertEqual(cmd[0], name)
            self.assertIn("--playback", cmd)
            self.assertIn("--raw", cmd)
            self.assertIn("--format", cmd)
            self.assertEqual(cmd[cmd.index("--format") + 1], "s16")
            self.assertIn("100ms", cmd)
            self.assertEqual(cmd[-1], "-")
            # -r is "record" and -c is a MIDI clip in current pw-cat. Long options only.
            self.assertFalse(any(part.startswith("-") and not part.startswith("--") and part != "-"
                                  for part in cmd))
        pap = clip_audio.pipe_player_command("paplay", 100)
        self.assertIn("--raw", pap)
        self.assertIn("--format=s16le", pap)
        self.assertIn("--latency-msec=100", pap)
        self.assertEqual(pap[-1], "-")
        # PipeWire 1.0.x rejects --raw ("-" is already raw PCM). Current PipeWire needs it.
        attempts = clip_audio.player_attempts("pw-cat", 100)
        self.assertEqual(len(attempts), 2)
        self.assertIn("--raw", attempts[0])
        self.assertNotIn("--raw", attempts[1])
        self.assertEqual(clip_audio.player_attempts("paplay"), [pap])


class NullSinkPaceTests(unittest.TestCase):
    def test_only_a_quarter_second_may_queue_ahead_of_the_clock(self):
        clock = _Clock()
        sink = clip_audio.NullSink(clock=clock)
        chunk = _pcm(clip_audio.CHUNK_FRAMES)
        for _ in range(clip_audio.QUEUE_BUFFERS):
            self.assertGreater(sink.free_buffers(), 0)
            sink.write(chunk)
        self.assertEqual(sink.free_buffers(), 0)
        self.assertEqual(sink.played_frames(), 0)
        self.assertEqual(sink.written_frames(), clip_audio.CHUNK_FRAMES * clip_audio.QUEUE_BUFFERS)
        clock.t += 0.05
        self.assertGreater(sink.played_frames(), 0)
        self.assertGreater(sink.free_buffers(), 0)
        self.assertFalse(sink.available)

    def test_pause_freezes_the_clock_and_resume_continues_it(self):
        clock = _Clock()
        sink = clip_audio.NullSink(clock=clock)
        sink.write(_pcm(clip_audio.CHUNK_FRAMES))
        clock.t += 0.01
        before = sink.played_frames()
        self.assertGreater(before, 0)
        sink.pause()
        self.assertEqual(sink.free_buffers(), 0)
        clock.t += 5
        self.assertEqual(sink.played_frames(), before)
        sink.resume()
        clock.t += 0.05
        self.assertGreater(sink.played_frames(), before)

    def test_feed_of_two_seconds_does_not_run_ahead_of_real_time(self):
        pcm = _pcm(clip_audio.RATE * 2)

        class Proc:
            def __init__(self):
                self.stdout = io.BytesIO(pcm)

            def kill(self):
                pass

            def wait(self, timeout=None):
                return 0

        player = clip_audio.AudioPlayer("x.ts", sink_factory=clip_audio.NullSink, spawn=lambda *a: Proc())
        player.gain = 0.8
        try:
            player.start(0.0)
            deadline = time.monotonic() + 2
            while player.sink.written_frames() == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
            written = player.sink.written_frames()
            self.assertGreater(written, 0)
            self.assertLess(written, clip_audio.RATE)          # the whole 2 s would already be in
            self.assertAlmostEqual(player._baked, 0.8)
            self.assertEqual(player._sample_gain(), 1.0)       # ffmpeg holds 0.8; Python does not scale
        finally:
            player.close()

    def test_a_slider_move_scales_the_next_chunk_only(self):
        player = clip_audio.AudioPlayer("x.ts", sink_factory=clip_audio.NullSink, spawn=lambda *a: None)
        player.gain = 0.8
        player._baked = 0.8
        self.assertEqual(player._sample_gain(), 1.0)
        player.set_gain(0.4)
        seen = []

        class Rec:
            available = False

            def free_buffers(self):
                return 1

            def write(self, data):
                seen.append(data)

        chunk = b"\x10\x00\xf0\xff" + bytes(clip_audio.CHUNK_BYTES - 4)
        player._offer(Rec(), player._generation, chunk)
        self.assertEqual(seen[0][:4], b"\x08\x00\xf8\xff")

    def test_spawn_receives_the_baked_gain(self):
        seen = []

        class Proc:
            stdout = io.BytesIO(b"")

            def kill(self):
                pass

            def wait(self, timeout=None):
                return 0

        def spawn(start, speed, gain):
            seen.append((start, speed, gain))
            return Proc()

        player = clip_audio.AudioPlayer("x.ts", sink_factory=clip_audio.NullSink, spawn=spawn)
        player.gain = 0.8
        try:
            player.start(1.5)
            deadline = time.monotonic() + 2
            while not seen and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(seen, [(1.5, 1.0, 0.8)])
        finally:
            player.close()

    def test_windows_does_not_build_a_pipe_sink(self):
        with mock.patch.object(clip_audio.sys, "platform", "win32"), \
                mock.patch.object(clip_audio, "device_count", lambda: 0), \
                mock.patch.object(clip_audio, "PipeSink", side_effect=AssertionError("pipe on windows")):
            sink = clip_audio.make_sink()
        self.assertIsInstance(sink, clip_audio.NullSink)

    def test_linux_without_a_player_is_the_paced_sink(self):
        with mock.patch.object(clip_audio.sys, "platform", "linux"), \
                mock.patch.object(clip_audio.shutil, "which", lambda name: None):
            sink = clip_audio.make_sink()
        self.assertIsInstance(sink, clip_audio.NullSink)
        self.assertFalse(sink.available)


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _proc_state(pid: int) -> str:
    stat = Path(f"/proc/{pid}/stat").read_text()
    return stat.rsplit(")", 1)[1].split()[0]


def _wait_proc_state(pid: int, *, stopped: bool, timeout: float = 5.0) -> str:
    """Poll until SIGSTOP or SIGCONT is visible. The kernel applies it after the call returns."""
    deadline = time.monotonic() + timeout
    state = _proc_state(pid)
    while (state in ("T", "t")) != stopped and time.monotonic() < deadline:
        time.sleep(0.01)
        state = _proc_state(pid)
    return state


@unittest.skipUnless(POSIX, "pipe playback is Linux")
class PipeSinkTests(unittest.TestCase):
    def _sink(self, script: str, *, latency_ms: int = 100, clock=None):
        code = script

        def spawn(argv, read_fd):
            return subprocess.Popen([sys.executable, "-c", code], stdin=read_fd,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                    **no_window_kwargs())

        sink = clip_audio.PipeSink(spawn=spawn, clock=clock, probe_s=0, latency_ms=latency_ms,
                                   which=lambda name: sys.executable if name == "pw-play" else None)
        with mock.patch.object(clip_audio, "pipe_player_command",
                               lambda name, latency_ms=100: [sys.executable, "-c", code]):
            self.assertTrue(sink.open(), "player process did not stay up")
        return sink

    def test_a_player_that_never_reads_is_dropped(self):
        sink = self._sink("import time; time.sleep(60)")
        sink._stall_s = 0.05
        try:
            sink.write(_pcm(clip_audio.CHUNK_FRAMES))
            self.assertTrue(sink.available)
            time.sleep(0.12)
            self.assertEqual(sink.free_buffers(), 0)
            self.assertFalse(sink.available)
            self.assertIsNone(sink._proc)
        finally:
            sink.close()

    def test_unconsumed_audio_has_not_been_played(self):
        sink = self._sink("import time; time.sleep(60)")
        try:
            sink.write(_pcm(1000))
            self.assertEqual(sink.played_frames(), 0)
            self.assertEqual(sink.written_frames(), 1000)
        finally:
            sink.close()

    def test_position_subtracts_the_pipe_and_the_requested_latency(self):
        # Reads until EOF, then stays up so the process exit is not what moves the playhead.
        sink = self._sink("import sys, time\nsys.stdin.buffer.read()\ntime.sleep(60)", latency_ms=100)
        try:
            extra = 1000
            total = sink._latency_frames + extra
            sink.write(_pcm(total))
            deadline = time.monotonic() + 2
            while sink._unread_frames() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(sink._unread_frames(), 0)
            self.assertEqual(sink.played_frames(), extra)
        finally:
            sink.close()

    def test_the_tail_plays_out_on_the_clock_after_the_pipe_drains(self):
        clock = _Clock()
        sink = self._sink("import sys, time\nsys.stdin.buffer.read()\ntime.sleep(60)", clock=clock)
        try:
            total = sink._latency_frames + 500
            sink.write(_pcm(total))
            deadline = time.monotonic() + 2
            while sink._unread_frames() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(sink.played_frames(), 500)
            sink.finish()
            self.assertEqual(sink.played_frames(), 500)          # tail just started
            clock.t += sink._latency_frames / clip_audio.RATE
            self.assertEqual(sink.played_frames(), total)
            self.assertTrue(sink.drained())
        finally:
            sink.close()

    def test_pause_stops_the_process_and_close_reaps_it(self):
        sink = self._sink("import time; time.sleep(60)")
        pid = sink._proc.pid
        sink.pause()
        self.assertIn(_wait_proc_state(pid, stopped=True), ("T", "t"))
        self.assertEqual(sink.free_buffers(), 0)
        sink.resume()
        self.assertNotIn(_wait_proc_state(pid, stopped=False), ("T", "t"))
        sink.pause()
        sink.close()
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_reset_replaces_the_process_and_seeks_drop_its_buffer(self):
        sink = self._sink("import time; time.sleep(60)")
        try:
            sink.write(_pcm(100))
            first = sink._proc.pid
            sink.reset()
            self.assertNotEqual(sink._proc.pid, first)
            self.assertEqual(sink.written_frames(), 0)
            self.assertEqual(sink.played_frames(), 0)
            with self.assertRaises(ProcessLookupError):
                os.kill(first, 0)
        finally:
            sink.close()

    def test_closing_the_player_unblocks_a_full_pipe_and_reaps(self):
        holder = {}

        def factory():
            sink = self._sink("import time; time.sleep(60)")
            holder["pid"] = sink._proc.pid
            holder["sink"] = sink
            return sink

        class Proc:
            def __init__(self):
                self.stdout = io.BytesIO(_pcm(clip_audio.RATE * 3))

            def kill(self):
                pass

            def wait(self, timeout=None):
                return 0

        player = clip_audio.AudioPlayer("x.ts", sink_factory=factory, spawn=lambda *a: Proc())
        try:
            player.start(0.0)
            deadline = time.monotonic() + 2
            while holder["sink"].written_frames() == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertGreater(holder["sink"].written_frames(), 0)
            started = time.monotonic()
            player.close()
            self.assertLess(time.monotonic() - started, 0.5)
        finally:
            player.close()
        with self.assertRaises(ProcessLookupError):
            os.kill(holder["pid"], 0)

    def test_the_real_spawner_kills_the_group_it_started(self):
        """close() must not leave the sleeper, including one that was stopped."""
        sink = clip_audio.PipeSink(probe_s=0, which=lambda name: sys.executable if name == "pw-play" else None)
        script = "import time; time.sleep(60)"
        with mock.patch.object(clip_audio, "pipe_player_command",
                               lambda name, latency_ms=100: [sys.executable, "-c", script]):
            self.assertTrue(sink.open())
        pid = sink._proc.pid
        self.assertTrue(sink._kill_group)
        self.assertEqual(os.getpgid(pid), pid)
        sink.pause()
        sink.close()
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        self.assertIsNone(sink._proc)
        self.assertIsNone(sink._w)
        self.assertIsNone(sink._r)


@unittest.skipUnless(HAS_FFMPEG, "ffmpeg not installed")
class VolumeFilterTests(unittest.TestCase):
    def test_ffmpeg_volume_matches_sample_scaling_within_one_lsb(self):
        samples = list(range(-32768, 32768, 17))
        src = array.array("h", samples)
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "s16le", "-ar", "48000", "-ac", "1",
               "-i", "pipe:0", "-af", "volume=0.8000", "-f", "s16le", "pipe:1"]
        proc = subprocess.run(cmd, input=src.tobytes(), capture_output=True, **no_window_kwargs())
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = array.array("h")
        out.frombytes(proc.stdout[: len(proc.stdout) // 2 * 2])
        self.assertEqual(len(out), len(samples))
        worst = max(abs(int(out[i]) - int(samples[i] * 0.8)) for i in range(len(samples)))
        self.assertLessEqual(worst, 1)
        self.assertEqual(len(out), len(samples))          # the filter adds no delay samples


class LinuxAudioHintTests(unittest.TestCase):
    def test_hint_when_no_linux_player_present(self):
        hint = clip_audio.linux_audio_hint(which=lambda _n: None, platform="linux")
        self.assertIn("pw-play", hint)
        self.assertIn("paplay", hint)
        self.assertEqual(clip_audio.linux_audio_hint(which=lambda n: "/usr/bin/pw-play" if n == "pw-play" else None,
                                                     platform="linux"), "")
        self.assertEqual(clip_audio.linux_audio_hint(which=lambda _n: None, platform="win32"), "")
