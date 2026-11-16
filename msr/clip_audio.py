"""Sound for the Clip Editor. ffmpeg decodes to raw PCM on a pipe.

Windows plays it with waveOut (ctypes), ported from Leapcut's clipper/audio_out.py.
Linux plays it by handing that PCM to pw-play, pw-cat or paplay. The sound is the master
clock: position() is how much has actually left the speakers, so the preview picture follows
it. Steady volume is an ffmpeg filter (the feed thread does not walk every sample). A slider
move still scales the next chunks here, so it answers at once. With no player, a silent sink
accepts audio only as fast as real time, so it cannot run ahead of the picture.
"""

from __future__ import annotations

import array
import ctypes
import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from ctypes import wintypes
from pathlib import Path

from msr.nowindow import no_window_kwargs

try:
    import fcntl
    import termios
except ImportError:  # Windows has no fcntl
    fcntl = None  # type: ignore[assignment]
    termios = None  # type: ignore[assignment]

RATE = 48000
CHANNELS = 2
BYTES_PER_FRAME = 2 * CHANNELS
CHUNK_FRAMES = 2048          # about 43 ms per buffer handed to Windows
QUEUE_BUFFERS = 6            # about a quarter second kept ahead
CHUNK_BYTES = CHUNK_FRAMES * BYTES_PER_FRAME
# pw-cat's own default. The picture subtracts this, plus whatever is still sitting in the pipe.
LATENCY_MS = 100
PIPE_BYTES = 65536           # cap how far the decoder may run ahead of the player
STALL_S = 1.0                # player accepted audio but never read it: give up and free the picture
PLAYERS = ("pw-play", "pw-cat", "paplay")
_log = logging.getLogger(__name__)
_silent_logged = False


def pcm_command(ffmpeg: str, video: Path, start: float, speed: float, gain: float = 1.0) -> list[str]:
    """Raw PCM from start. speed is not applied here (the editor always plays at 1.0).

    gain below full scale is ffmpeg's volume filter, so the steady playback path does not
    scale samples in Python. Full scale omits the filter and the PCM stays bit-exact.
    """
    cmd = [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-ss", f"{max(0.0, start):.3f}",
           "-i", str(video), "-vn", "-sn", "-dn"]
    if gain < 0.999:
        cmd += ["-af", f"volume={gain:.4f}"]
    return cmd + ["-f", "s16le", "-acodec", "pcm_s16le", "-ac", str(CHANNELS), "-ar", str(RATE), "pipe:1"]


def apply_gain(data: bytes, gain: float) -> bytes:
    """Scale 16-bit samples. About 1.0 returns the bytes untouched; 0 is silence.

    Values above 1 (a slider raised after ffmpeg already attenuated) are clipped to int16.
    """
    if not data or 0.999 <= gain <= 1.001:
        return data
    n = len(data) // 2 * 2
    if gain <= 0.0:
        return bytes(n)
    samples = array.array("h")
    samples.frombytes(data[:n])
    if sys.byteorder != "little":  # pragma: no cover
        samples.byteswap()
    out = array.array("h", [_clip_sample(int(s * gain)) for s in samples])
    if sys.byteorder != "little":  # pragma: no cover
        out.byteswap()
    return out.tobytes()


def _clip_sample(value: int) -> int:
    if value > 32767:
        return 32767
    if value < -32768:
        return -32768
    return value


def pipe_player_command(name: str, latency_ms: int = LATENCY_MS) -> list[str]:
    """Preferred argv for one raw s16le player. Long options only: pw-cat uses -r for record.

    The trailing ``-`` is stdin. Latency is explicit so position() can subtract the same number.
    ``--raw`` is what current PipeWire needs so libsndfile does not parse the pipe. PipeWire 1.0.x
    has no ``--raw`` (``-`` is already raw); ``player_attempts`` tries that shape when this one exits.
    """
    lat = str(int(latency_ms))
    if name in ("pw-play", "pw-cat"):
        return [name, "--playback", "--raw", "--rate", str(RATE), "--channels", str(CHANNELS),
                "--latency", f"{lat}ms", "--format", "s16", "-"]
    if name == "paplay":
        return [name, "--raw", "--format=s16le", f"--rate={RATE}", f"--channels={CHANNELS}",
                f"--latency-msec={lat}", "-"]
    raise ValueError(name)


def player_attempts(name: str, latency_ms: int = LATENCY_MS) -> list[list[str]]:
    """Argv variants, preferred first. The first one that stays running is the one we keep."""
    preferred = pipe_player_command(name, latency_ms)
    if name not in ("pw-play", "pw-cat"):
        return [preferred]
    plain = [part for part in preferred if part != "--raw"]
    if plain == preferred:
        return [preferred]
    return [preferred, plain]


# ---------------------------------------------------------------- sinks
class _RealtimeGate:
    """Lets the feed stay QUEUE_BUFFERS ahead of a clock, and no further.

    AudioPlayer._offer sleeps while free_buffers() is 0, and that sleep releases the GIL.
    A sink that reports every write as already played never hits that wait, so the decode
    thread runs the whole file at once.
    """

    def __init__(self, clock=None, rate: int = RATE) -> None:
        self._clock = clock or time.monotonic
        self._rate = rate
        self.written = 0
        self._origin = None
        self._paused = False
        self._pause_at = None
        self._paused_for = 0.0

    def _elapsed(self) -> float:
        if self._origin is None:
            return 0.0
        now = self._clock()
        end = self._pause_at if self._paused and self._pause_at is not None else now
        return max(0.0, end - self._origin - self._paused_for)

    def played_frames(self) -> int:
        return min(self.written, int(self._elapsed() * self._rate))

    def free_buffers(self) -> int:
        if self._paused:
            return 0
        ahead = self.written - self.played_frames()
        room = CHUNK_FRAMES * QUEUE_BUFFERS - ahead
        if room >= CHUNK_FRAMES:
            return max(1, room // CHUNK_FRAMES)
        return 0

    def write(self, data: bytes) -> None:
        frames = len(data) // BYTES_PER_FRAME
        if frames <= 0:
            return
        if self._origin is None and not self._paused:
            self._origin = self._clock()
        self.written += frames

    def pause(self) -> None:
        if not self._paused:
            self._paused = True
            self._pause_at = self._clock()

    def resume(self) -> None:
        if self._paused:
            if self._pause_at is not None:
                self._paused_for += self._clock() - self._pause_at
            self._pause_at = None
            self._paused = False

    def reset(self) -> None:
        self.written = 0
        self._origin = None
        self._paused = False
        self._pause_at = None
        self._paused_for = 0.0

    def drained(self) -> bool:
        return self.played_frames() >= self.written


class NullSink:
    """No sound device. Writes are accepted at real time only. available stays False, so the
    editor follows the wall clock (nothing is coming out of a speaker)."""
    available = False

    def __init__(self, *, clock=None) -> None:
        self._gate = _RealtimeGate(clock)

    def open(self) -> bool:
        return True

    def free_buffers(self) -> int:
        return self._gate.free_buffers()

    def write(self, data: bytes) -> None:
        self._gate.write(data)

    def played_frames(self) -> int:
        return self._gate.played_frames()

    def written_frames(self) -> int:
        return self._gate.written

    def pause(self) -> None:
        self._gate.pause()

    def resume(self) -> None:
        self._gate.resume()

    def reset(self) -> None:
        self._gate.reset()

    def drained(self) -> bool:
        return self._gate.drained()

    def release(self) -> None:
        """Unblock a feed thread. The silent sink never blocks inside write()."""

    def finish(self) -> None:
        """The decoder hit EOF. played_frames() still catches up on the clock."""

    def close(self) -> None:
        self.reset()


class _WaveFormat(ctypes.Structure):
    _fields_ = [("wFormatTag", wintypes.WORD), ("nChannels", wintypes.WORD), ("nSamplesPerSec", wintypes.DWORD),
                ("nAvgBytesPerSec", wintypes.DWORD), ("nBlockAlign", wintypes.WORD),
                ("wBitsPerSample", wintypes.WORD), ("cbSize", wintypes.WORD)]


class _WaveHdr(ctypes.Structure):
    pass


_WaveHdr._fields_ = [("lpData", ctypes.c_void_p), ("dwBufferLength", wintypes.DWORD),
                     ("dwBytesRecorded", wintypes.DWORD), ("dwUser", ctypes.c_size_t),
                     ("dwFlags", wintypes.DWORD), ("dwLoops", wintypes.DWORD),
                     ("lpNext", ctypes.c_void_p), ("reserved", ctypes.c_size_t)]


class _MMTime(ctypes.Structure):
    _pack_ = 1
    _fields_ = [("wType", wintypes.UINT), ("sample", wintypes.DWORD), ("pad", wintypes.BYTE * 4)]


WHDR_DONE = 0x1
TIME_SAMPLES = 0x2
WAVE_MAPPER = 0xFFFFFFFF


def device_count() -> int:
    try:
        return int(ctypes.windll.waveOutGetNumDevs())  # type: ignore[attr-defined]
    except Exception:
        try:
            return int(ctypes.windll.winmm.waveOutGetNumDevs())  # type: ignore[attr-defined]
        except Exception:
            return 0


class WinMMSink:
    """waveOut with a small ring of buffers. Everything is polled; there is no callback thread."""
    available = True

    def __init__(self) -> None:
        self._mm = ctypes.windll.winmm  # type: ignore[attr-defined]
        self._h = wintypes.HANDLE()
        self._open = False
        self._headers: list[tuple[_WaveHdr, ctypes.Array]] = []
        self._lock = threading.Lock()
        self._paused = False
        self._base_samples = 0
        self._submitted = 0
        self._setup_prototypes()

    def _setup_prototypes(self) -> None:
        mm = self._mm
        mm.waveOutOpen.argtypes = [ctypes.POINTER(wintypes.HANDLE), wintypes.UINT, ctypes.POINTER(_WaveFormat),
                                   ctypes.c_size_t, ctypes.c_size_t, wintypes.DWORD]
        for name in ("waveOutPrepareHeader", "waveOutUnprepareHeader", "waveOutWrite"):
            getattr(mm, name).argtypes = [wintypes.HANDLE, ctypes.POINTER(_WaveHdr), wintypes.UINT]
        mm.waveOutGetPosition.argtypes = [wintypes.HANDLE, ctypes.POINTER(_MMTime), wintypes.UINT]
        for name in ("waveOutPause", "waveOutRestart", "waveOutReset", "waveOutClose"):
            getattr(mm, name).argtypes = [wintypes.HANDLE]

    def open(self) -> bool:
        fmt = _WaveFormat(1, CHANNELS, RATE, RATE * BYTES_PER_FRAME, BYTES_PER_FRAME, 16, 0)
        if self._mm.waveOutOpen(ctypes.byref(self._h), WAVE_MAPPER, ctypes.byref(fmt), 0, 0, 0) != 0:
            return False
        self._open = True
        size = CHUNK_FRAMES * BYTES_PER_FRAME
        for _ in range(QUEUE_BUFFERS):
            buf = ctypes.create_string_buffer(size)
            hdr = _WaveHdr(ctypes.cast(buf, ctypes.c_void_p), size, 0, 0, WHDR_DONE, 0, None, 0)
            self._headers.append((hdr, buf))
        return True

    def free_buffers(self) -> int:
        return sum(1 for hdr, _ in self._headers if hdr.dwFlags & WHDR_DONE or not hdr.dwFlags & 2)

    def write(self, data: bytes) -> None:
        """Queue one chunk (up to CHUNK_FRAMES frames). The caller waits for free_buffers() first."""
        with self._lock:
            if not self._open:
                return
            for hdr, buf in self._headers:
                if hdr.dwFlags & WHDR_DONE or not hdr.dwFlags & 2:  # done, or never prepared (WHDR_PREPARED = 2)
                    if hdr.dwFlags & 2:
                        self._mm.waveOutUnprepareHeader(self._h, ctypes.byref(hdr), ctypes.sizeof(hdr))
                    n = min(len(data), len(buf))
                    ctypes.memmove(buf, data, n)
                    hdr.dwBufferLength = n
                    hdr.dwFlags = 0
                    self._mm.waveOutPrepareHeader(self._h, ctypes.byref(hdr), ctypes.sizeof(hdr))
                    self._mm.waveOutWrite(self._h, ctypes.byref(hdr), ctypes.sizeof(hdr))
                    self._submitted += n // BYTES_PER_FRAME
                    return

    def played_frames(self) -> int:
        """Frames that have left the speakers since the last reset."""
        if not self._open:
            return 0
        t = _MMTime(TIME_SAMPLES, 0)
        if self._mm.waveOutGetPosition(self._h, ctypes.byref(t), ctypes.sizeof(t)) != 0 or t.wType != TIME_SAMPLES:
            return max(0, self._submitted - CHUNK_FRAMES * QUEUE_BUFFERS)
        return int(t.sample)

    def pause(self) -> None:
        if self._open and not self._paused:
            self._mm.waveOutPause(self._h)
            self._paused = True

    def resume(self) -> None:
        if self._open and self._paused:
            self._mm.waveOutRestart(self._h)
            self._paused = False

    def reset(self) -> None:
        """Throw away what is queued and start counting from zero."""
        if not self._open:
            return
        with self._lock:
            self._mm.waveOutReset(self._h)
            for hdr, _ in self._headers:
                if hdr.dwFlags & 2:
                    self._mm.waveOutUnprepareHeader(self._h, ctypes.byref(hdr), ctypes.sizeof(hdr))
                hdr.dwFlags = WHDR_DONE
            self._submitted = 0
            self._paused = False

    def drained(self) -> bool:
        return all(hdr.dwFlags & WHDR_DONE or not hdr.dwFlags & 2 for hdr, _ in self._headers)

    def release(self) -> None:
        """The feed waits on free_buffers(), not inside write(), so stop can join it."""

    def finish(self) -> None:
        """waveOut drains the ring on its own; nothing to close early."""

    def close(self) -> None:
        if not self._open:
            return
        self.reset()
        self._mm.waveOutClose(self._h)
        self._open = False


def _log_silent(detail: str = "") -> None:
    global _silent_logged
    if _silent_logged:
        return
    _silent_logged = True
    msg = "Clip Editor: no working pw-play, pw-cat or paplay; playback is silent and kept to real time."
    if detail:
        msg = f"{msg} ({detail})"
    _log.info(msg)


def _pipe_unread(fd: int) -> int:
    """Bytes sitting in a pipe, without reading them. 0 where that cannot be asked."""
    if fcntl is None or termios is None or not hasattr(termios, "FIONREAD"):
        return 0
    buf = array.array("i", [0])
    try:
        fcntl.ioctl(fd, termios.FIONREAD, buf, True)
    except OSError:
        return 0
    return max(0, int(buf[0]))


def _set_pipe_size(fd: int, size: int) -> int:
    """Cap the pipe so the decoder cannot run minutes ahead. Returns the size actually in force."""
    if fcntl is None or not hasattr(fcntl, "F_SETPIPE_SZ"):
        return PIPE_BYTES
    try:
        fcntl.fcntl(fd, fcntl.F_SETPIPE_SZ, size)
    except OSError:
        pass
    try:
        return int(fcntl.fcntl(fd, fcntl.F_GETPIPE_SZ))
    except (OSError, AttributeError):
        return size


class PipeSink:
    """Play s16le PCM by piping it to pw-play, pw-cat or paplay.

    Position is samples written, minus what is still in the kernel pipe, minus the latency the
    player was asked to hold. pw-cat reads the pipe in its stream callback (one quantum at a
    time). stdio may also hold one buffer, about 40 ms, which is not subtracted: the editor
    drops a late frame rather than showing one that lags the sound.

    pause freezes the player (SIGSTOP) so the pipe stops moving and the position stays put.
    close kills the process group it started. spawn, clock and probe_s are for tests.
    """

    available = True

    def __init__(self, *, spawn=None, clock=None, which=None, probe_s: float = 0.45,
                 latency_ms: int = LATENCY_MS, stall_s: float = STALL_S) -> None:
        self._spawn = spawn or self._spawn_player
        self._clock = clock or time.monotonic
        self._which = which or shutil.which
        self._probe_s = probe_s
        self._stall_s = stall_s
        self._stall_from = None
        self._latency_frames = max(0, int(round(RATE * latency_ms / 1000.0)))
        self._argv: list[str] | None = None
        self._proc = None
        self._kill_group = False
        self._r = None
        self._w = None
        self._pipe_cap = PIPE_BYTES
        self._written_bytes = 0
        self._last_heard = 0
        self._input_closed = False
        self._paused = False
        self._epoch = 0
        self._wake = threading.Event()
        self._tail_from = None
        self._tail_base = 0
        self._tail_paused = 0.0
        self._tail_pause_mark = None
        self._lock = threading.Lock()
        self.available = True

    def open(self) -> bool:
        """Start the first player that stays up and reads PCM. False leaves nothing running."""
        for name in PLAYERS:
            exe = self._which(name)
            if not exe:
                continue
            for argv in player_attempts(name):
                argv = [exe, *argv[1:]]
                if self._start_process(argv) and self._accepts_audio():
                    if self._probe_s > 0:
                        # The probe pushed a little silence. Start clean so it is not part of the timeline.
                        self._kill_proc()
                        self._close_fds()
                        self._clear_counts()
                        if not (self._start_process(argv) and self._proc is not None
                                and self._proc.poll() is None):
                            self._kill_proc()
                            self._close_fds()
                            continue
                    self._argv = argv
                    self.available = True
                    return True
                self._kill_proc()
                self._close_fds()
        self.available = False
        return False

    def _accepts_audio(self) -> bool:
        """True if the process stays up. When probe_s > 0, it must also read some PCM.

        A player that only connects and then never reads would pin the picture on the first
        frame (position stays at the start). Tests pass probe_s=0 and skip the read.
        """
        proc = self._proc
        if proc is None or self._w is None:
            return False
        if proc.poll() is not None:
            return False
        if self._probe_s <= 0:
            return True
        # A full latency of silence, so a player that waits for its first buffer does read.
        payload = b"\0" * min(self._latency_frames * BYTES_PER_FRAME, PIPE_BYTES // 2)
        try:
            sent = os.write(self._w, payload)
        except OSError:
            return False
        if sent <= 0:
            return False
        end = time.monotonic() + self._probe_s
        while time.monotonic() < end:
            if proc.poll() is not None:
                return False
            if self._r is not None and _pipe_unread(self._r) < sent:
                return True
            time.sleep(0.02)
        return False

    def _give_up(self) -> None:
        """The player is not taking audio. Drop it so the picture can follow the wall clock."""
        self.available = False
        self._kill_proc()

    def _stalled(self) -> bool:
        if self._paused or self._input_closed or not self.available:
            self._stall_from = None
            return False
        written = self._written_bytes // BYTES_PER_FRAME
        if written < CHUNK_FRAMES or self._r is None:
            return False
        if self._unread_frames() < written:
            self._stall_from = None
            return False
        now = time.monotonic()
        if self._stall_from is None:
            self._stall_from = now
            return False
        if now - self._stall_from < self._stall_s:
            return False
        self._give_up()
        return True

    def _spawn_player(self, argv: list[str], read_fd: int):
        self._kill_group = True
        return subprocess.Popen(argv, stdin=read_fd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                start_new_session=True, **no_window_kwargs())

    def _start_process(self, argv: list[str]) -> bool:
        if fcntl is None:
            return False
        read_fd, write_fd = os.pipe()
        try:
            os.set_inheritable(read_fd, True)
            os.set_inheritable(write_fd, False)
            self._pipe_cap = _set_pipe_size(write_fd, PIPE_BYTES)
            os.set_blocking(write_fd, False)
            proc = self._spawn(argv, read_fd)
        except OSError:
            for fd in (read_fd, write_fd):
                try:
                    os.close(fd)
                except OSError:
                    pass
            return False
        self._r, self._w = read_fd, write_fd
        self._proc = proc
        self._input_closed = False
        self._paused = False
        return True

    def _unread_frames(self) -> int:
        if self._r is None:
            return 0
        return _pipe_unread(self._r) // BYTES_PER_FRAME

    def written_frames(self) -> int:
        return self._written_bytes // BYTES_PER_FRAME

    def free_buffers(self) -> int:
        if self._stalled() or self._paused or not self.available or self._w is None:
            return 0
        proc = self._proc
        if proc is None or proc.poll() is not None:
            self.available = False
            return 0
        unread = _pipe_unread(self._r) if self._r is not None else 0
        room = self._pipe_cap - unread
        need = min(CHUNK_BYTES, max(1, self._pipe_cap // 2))
        return 1 if room >= need else 0

    def write(self, data: bytes) -> None:
        """Queue one chunk. Returns quickly if stop/seek bumps the epoch or the player dies."""
        if not data:
            return
        epoch = self._epoch
        fd = self._w
        if fd is None or not self.available:
            return
        view = memoryview(data)
        while len(view):
            if epoch != self._epoch or fd != self._w or self._paused or not self.available:
                return
            try:
                n = os.write(fd, view)
            except BlockingIOError:
                self._wake.wait(0.004)
                self._wake.clear()
                continue
            except OSError:
                if epoch == self._epoch:
                    self.available = False
                return
            if n <= 0:
                if epoch == self._epoch:
                    self.available = False
                return
            view = view[n:]
            armed = False
            written = 0
            with self._lock:
                if epoch == self._epoch and fd == self._w:
                    self._written_bytes += n
                    written = self._written_bytes
                    armed = self._stall_from is None and written >= CHUNK_BYTES
            # Start the "is it actually reading?" clock once a chunk is sitting unread.
            if armed and self._r is not None and _pipe_unread(self._r) >= written:
                self._stall_from = time.monotonic()

    def _tail(self, written: int, heard: int) -> int:
        """After the pipe is empty, the player's own buffer still plays out on the clock."""
        now = self._clock()
        if self._tail_from is None:
            self._tail_from = now
            self._tail_base = heard
            self._tail_paused = 0.0
            self._tail_pause_mark = None
        if self._paused:
            if self._tail_pause_mark is None:
                self._tail_pause_mark = now
            end = self._tail_pause_mark
        else:
            if self._tail_pause_mark is not None:
                self._tail_paused += now - self._tail_pause_mark
                self._tail_pause_mark = None
            end = now
        extra = int(max(0.0, end - self._tail_from - self._tail_paused) * RATE)
        return min(written, self._tail_base + extra)

    def played_frames(self) -> int:
        """Frames that have left the speakers since the last reset."""
        if self._stalled():
            return self._last_heard
        with self._lock:
            written = self._written_bytes // BYTES_PER_FRAME
            proc = self._proc
            if not self.available or proc is None:
                return self._last_heard
            if proc.poll() is not None:
                if self._input_closed:
                    self._last_heard = written
                    return written
                self.available = False
                return self._last_heard
            heard = max(0, written - self._unread_frames() - self._latency_frames)
            if self._input_closed and self._unread_frames() == 0:
                heard = self._tail(written, heard)
            heard = min(written, heard)
            self._last_heard = heard
            return heard

    def pause(self) -> None:
        proc = self._proc
        if proc is None or self._paused or proc.poll() is not None:
            self._paused = True
            return
        try:
            os.kill(proc.pid, signal.SIGSTOP)
        except OSError:
            self.available = False
            return
        self._paused = True

    def resume(self) -> None:
        proc = self._proc
        self._paused = False
        if proc is None or proc.poll() is not None:
            return
        try:
            os.kill(proc.pid, signal.SIGCONT)
        except OSError:
            self.available = False

    def reset(self) -> None:
        """Drop queued audio and start a fresh player. A player that has not been fed is kept."""
        alive = self._proc is not None and self._proc.poll() is None
        if alive and self._written_bytes == 0 and not self._input_closed and not self._paused and self._argv:
            return
        self._epoch += 1
        self._wake.set()
        self._kill_proc()
        self._close_fds()
        self._clear_counts()
        if self._argv:
            if not (self._start_process(self._argv) and self._proc is not None and self._proc.poll() is None):
                self._kill_proc()
                self._close_fds()
                self.available = False

    def drained(self) -> bool:
        if not self.available:
            return True
        if not self._input_closed:
            return False
        return self.played_frames() >= self.written_frames()

    def release(self) -> None:
        """Unblock write() so the feed thread can be joined. reset() or close() follows."""
        self._epoch += 1
        self._wake.set()
        self._kill_proc()

    def finish(self) -> None:
        """No more PCM. Closing the write end lets the player drain and exit."""
        self._input_closed = True
        self._close_write()

    def close(self) -> None:
        self._epoch += 1
        self._wake.set()
        self._kill_proc()
        self._close_fds()
        self._clear_counts()
        self.available = False

    def _clear_counts(self) -> None:
        with self._lock:
            self._written_bytes = 0
            self._last_heard = 0
            self._input_closed = False
            self._paused = False
            self._tail_from = None
            self._tail_base = 0
            self._tail_paused = 0.0
            self._tail_pause_mark = None
            self._stall_from = None

    def _close_write(self) -> None:
        fd, self._w = self._w, None
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

    def _close_fds(self) -> None:
        self._close_write()
        fd, self._r = self._r, None
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

    def _kill_proc(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        pid = getattr(proc, "pid", None)
        if self._paused and pid:
            try:
                os.kill(pid, signal.SIGCONT)
            except OSError:
                pass
        # Only the session we started (pgid == pid). A recycled pid must not be signalled.
        if self._kill_group and pid and pid != os.getpid():
            try:
                if os.getpgid(pid) == pid:
                    os.killpg(pid, signal.SIGKILL)
            except OSError:
                pass
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=2)
        except Exception:
            pass
        self._paused = False


def make_sink():
    """The real sink when this PC has a sound device, else a silent paced one.

    Windows stays on waveOut. Linux uses pw-play, then pw-cat, then paplay.
    """
    if sys.platform == "win32" and device_count() > 0:
        try:
            sink = WinMMSink()
            if sink.open():
                return sink
        except Exception:
            pass
        return NullSink()
    if sys.platform.startswith("linux") and fcntl is not None:
        sink = None
        try:
            sink = PipeSink()
            if sink.open():
                return sink
        except Exception as exc:
            _log_silent(type(exc).__name__)
        if sink is not None:
            try:
                sink.close()
            except Exception:
                pass
        _log_silent()
    return NullSink()


# ---------------------------------------------------------------- the player
class AudioPlayer:
    """Plays a file's sound from a position, at a speed and gain; reports how far it has got.

    sink_factory and spawn exist for tests (a recording sink and a canned PCM stream)."""

    def __init__(self, video, *, ffmpeg: str = "ffmpeg", sink_factory=make_sink, spawn=None) -> None:
        self.video = Path(video)
        self.ffmpeg = ffmpeg
        self._sink_factory = sink_factory
        self._spawn = spawn or self._spawn_ffmpeg
        self.sink = None
        self.gain = 1.0
        self._baked = 1.0          # gain ffmpeg was started with; 1.0 means "no volume filter"
        self.speed = 1.0
        self._start = 0.0
        self._paused = True
        self._generation = 0
        self._thread: threading.Thread | None = None
        self._proc = None
        self._ended = threading.Event()
        self._lock = threading.Lock()

    @property
    def usable(self) -> bool:
        return bool(self.ffmpeg) or self._spawn is not self._spawn_ffmpeg

    def _spawn_ffmpeg(self, start: float, speed: float, gain: float = 1.0):
        return subprocess.Popen(pcm_command(self.ffmpeg, self.video, start, speed, gain),
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                bufsize=0, **no_window_kwargs())

    def _filter_gain(self) -> float:
        """What ffmpeg should bake in. Silence and full scale stay 1.0 so a later slider move
        can still reach the original samples (silence itself is apply_gain, which is instant)."""
        g = float(self.gain)
        if g <= 0.001 or g >= 0.999:
            return 1.0
        return g

    def _sample_gain(self) -> float:
        """Python scale on top of the baked filter. 1.0 when the slider has not moved."""
        baked = self._baked or 1.0
        g = float(self.gain)
        if g <= 0.0:
            return 0.0
        if baked <= 0.0:
            return 1.0 if g >= 0.999 else g
        ratio = g / baked
        if abs(ratio - 1.0) <= 0.001:
            return 1.0
        return ratio

    def _ensure_sink(self):
        if self.sink is None:
            self.sink = self._sink_factory()
        return self.sink

    # -- control
    def start(self, position: float, *, speed: float | None = None) -> None:
        """Begin (or restart) playing from position."""
        if speed is not None:
            self.speed = speed
        self._stop_feed()
        sink = self._ensure_sink()
        self._baked = self._filter_gain()
        sink.reset()
        with self._lock:
            self._generation += 1
            gen = self._generation
        self._start = position
        self._paused = False
        self._ended.clear()
        self._thread = threading.Thread(target=self._feed, args=(gen, position, self.speed, self._baked),
                                        daemon=True)
        self._thread.start()

    def pause(self) -> None:
        if self.sink is not None:
            self.sink.pause()
        self._paused = True

    def resume(self) -> None:
        if self.sink is not None:
            self.sink.resume()
        self._paused = False

    def stop(self) -> None:
        self._stop_feed()
        if self.sink is not None:
            self.sink.reset()
        self._paused = True

    def set_gain(self, gain: float) -> None:
        self.gain = gain

    def ended(self) -> bool:
        """The sound ran out (the file's end), not merely paused."""
        return self._ended.is_set() and (self.sink is None or self.sink.drained())

    def position(self) -> float | None:
        """Media time of what is playing now, or None before any sound has played."""
        if self.sink is None:
            return None
        frames = self.sink.played_frames()
        return self._start + frames / RATE * self.speed

    def close(self) -> None:
        self._stop_feed()
        if self.sink is not None:
            self.sink.close()
            self.sink = None

    # -- the feeding thread
    def _stop_feed(self) -> None:
        with self._lock:
            self._generation += 1
        sink = self.sink
        if sink is not None:
            try:
                sink.release()
            except Exception:
                pass
        proc, self._proc = self._proc, None
        if proc is not None:
            try:
                proc.kill()
            except Exception:
                pass
        thread, self._thread = self._thread, None
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)

    def _feed(self, gen: int, start: float, speed: float, gain: float | None = None) -> None:
        if gain is None:
            gain = self._baked
        try:
            proc = self._spawn(start, speed, gain)
        except Exception:
            self._ended.set()
            return
        self._proc = proc
        size = CHUNK_FRAMES * BYTES_PER_FRAME
        sink = self.sink
        pending = b""
        try:
            while gen == self._generation:
                data = proc.stdout.read(size - len(pending)) if hasattr(proc.stdout, "read") else b""
                if not data:
                    break
                pending += data
                if len(pending) < size:
                    continue
                self._offer(sink, gen, pending)
                pending = b""
            if pending and gen == self._generation:
                self._offer(sink, gen, pending + bytes(size - len(pending)))
        except Exception:
            pass
        finally:
            # This thread owns proc: _stop_feed may have run before self._proc was set.
            from msr.clip_core import reap
            reap(proc)
            if gen == self._generation:
                sink = self.sink
                if sink is not None:
                    try:
                        sink.finish()
                    except Exception:
                        pass
                self._ended.set()

    def _offer(self, sink, gen: int, chunk: bytes) -> None:
        while gen == self._generation and sink.free_buffers() <= 0:
            time.sleep(0.004)
        if gen == self._generation:
            sink.write(apply_gain(chunk, self._sample_gain()))
