"""Sound for the Clip Editor, with nothing to install: ffmpeg decodes to raw PCM on a pipe and
Windows' own waveOut plays it (ctypes). Adapted from Leapcut's clipper/audio_out.py.

The sound is the master clock: position() is how much has actually left the speakers, so the
preview picture follows it. Volume scales the samples (stdlib array), so it answers at once.
Without a sound device (or off Windows) a silent sink keeps the clock running.
"""

from __future__ import annotations

import ctypes
import subprocess
import threading
import time
from ctypes import wintypes
from pathlib import Path

import array
import sys

from msr.nowindow import no_window_kwargs

RATE = 48000
CHANNELS = 2
BYTES_PER_FRAME = 2 * CHANNELS
CHUNK_FRAMES = 2048          # about 43 ms per buffer handed to Windows
QUEUE_BUFFERS = 6            # about a quarter second kept ahead


def pcm_command(ffmpeg: str, video: Path, start: float, speed: float) -> list[str]:
    cmd = [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-ss", f"{max(0.0, start):.3f}",
           "-i", str(video), "-vn", "-sn", "-dn"]
    return cmd + ["-f", "s16le", "-acodec", "pcm_s16le", "-ac", str(CHANNELS), "-ar", str(RATE), "pipe:1"]


def apply_gain(data: bytes, gain: float) -> bytes:
    """Scale 16-bit samples. 1.0 returns the bytes untouched; 0 is silence."""
    if gain >= 0.999 or not data:
        return data
    if gain <= 0.0:
        return bytes(len(data))
    samples = array.array("h")
    samples.frombytes(data[: len(data) // 2 * 2])
    if sys.byteorder != "little":  # pragma: no cover
        samples.byteswap()
    out = array.array("h", [int(s * gain) for s in samples])
    if sys.byteorder != "little":  # pragma: no cover
        out.byteswap()
    return out.tobytes()


# ---------------------------------------------------------------- sinks
class NullSink:
    """No sound device: swallows audio instantly. Position is what it was told was played."""
    available = False

    def __init__(self) -> None:
        self.written = 0

    def open(self) -> bool:
        return True

    def free_buffers(self) -> int:
        return QUEUE_BUFFERS

    def write(self, data: bytes) -> None:
        self.written += len(data) // BYTES_PER_FRAME

    def played_frames(self) -> int:
        return self.written

    def pause(self) -> None: ...
    def resume(self) -> None: ...
    def reset(self) -> None:
        self.written = 0

    def drained(self) -> bool:
        return True

    def close(self) -> None: ...


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

    def close(self) -> None:
        if not self._open:
            return
        self.reset()
        self._mm.waveOutClose(self._h)
        self._open = False


def make_sink():
    """The real sink when this PC has a sound device, else a silent one."""
    if sys.platform == "win32" and device_count() > 0:
        try:
            sink = WinMMSink()
            if sink.open():
                return sink
        except Exception:
            pass
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

    def _spawn_ffmpeg(self, start: float, speed: float):
        return subprocess.Popen(pcm_command(self.ffmpeg, self.video, start, speed), stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                                **no_window_kwargs())

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
        sink.reset()
        with self._lock:
            self._generation += 1
            gen = self._generation
        self._start = position
        self._paused = False
        self._ended.clear()
        self._thread = threading.Thread(target=self._feed, args=(gen, position, self.speed), daemon=True)
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
        proc, self._proc = self._proc, None
        if proc is not None:
            try:
                proc.kill()
            except Exception:
                pass
        thread, self._thread = self._thread, None
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)

    def _feed(self, gen: int, start: float, speed: float) -> None:
        try:
            proc = self._spawn(start, speed)
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
                self._ended.set()

    def _offer(self, sink, gen: int, chunk: bytes) -> None:
        while gen == self._generation and sink.free_buffers() <= 0:
            time.sleep(0.004)
        if gen == self._generation:
            sink.write(apply_gain(chunk, self.gain))
