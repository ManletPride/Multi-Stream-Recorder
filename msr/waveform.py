"""Optional Clip Editor waveform: loudness per 0.1 s, read by ffmpeg in a background thread.

Mirrors Leapcut's sound graph (10 readings/s, one byte each over -72..0 dBFS, drawn as a filled
strip on a -60..-12 dB scale), but without numpy: ffmpeg's astats measures each 0.1 s block and
prints its RMS level. A growing live file is read incrementally (resume at the end). Cached in the
user's local app-data / temp folder, never beside the recordings.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import tempfile
import threading

from msr.nowindow import no_window_kwargs

RATE_HZ = 10
PCM_RATE = 8000
FLOOR_DB = -72.0
QUIET_DB = -60.0
LOUD_DB = -12.0
VERSION = 1


def default_cache_dir() -> str:
    base = os.environ.get("LOCALAPPDATA") or tempfile.gettempdir()
    return os.path.join(base, "Multi-Stream-Recorder", "cache", "waveforms")


def level_of_db(db: float) -> int:
    if db != db or db == float("-inf"):
        return 0
    return int(round(min(1.0, max(0.0, (db - FLOOR_DB) / -FLOOR_DB)) * 255))


def height_of(level: int) -> float:
    db = FLOOR_DB + level / 255.0 * -FLOOR_DB
    return min(1.0, max(0.0, (db - QUIET_DB) / (LOUD_DB - QUIET_DB)))


def wave_command(ffmpeg: str, src: str, start: float = 0.0) -> list[str]:
    cmd = [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin"]
    if start > 0:
        cmd += ["-ss", f"{start:.3f}"]
    return cmd + ["-i", src, "-map", "0:a:0?", "-vn", "-sn", "-dn",
                  "-af", (f"aresample={PCM_RATE},aformat=channel_layouts=mono,"
                          f"asetnsamples=n={PCM_RATE // RATE_HZ}:p=0,astats=metadata=1:reset=1,"
                          "ametadata=print:key=lavfi.astats.Overall.RMS_level:file=-"),
                  "-f", "null", "-"]


def parse_line(line: str):
    """'lavfi.astats.Overall.RMS_level=-23.5' -> level byte; other lines -> None."""
    line = line.strip()
    if not line.startswith("lavfi.astats.Overall.RMS_level="):
        return None
    raw = line.split("=", 1)[1].strip().lower()
    try:
        db = float("-inf") if raw in ("-inf", "inf", "nan") else float(raw)
    except ValueError:
        return 0
    return level_of_db(db)


class Wave:
    def __init__(self) -> None:
        self.levels = bytearray()
        self.version = 0          # bumps when levels change (the editor redraws)
        self.lock = threading.Lock()

    def seconds(self) -> float:
        return len(self.levels) / RATE_HZ

    def extend(self, values) -> None:
        with self.lock:
            self.levels.extend(values)
            self.version += 1

    def window_columns(self, t0: float, t1: float, width: int) -> list[float]:
        """One 0..1 height per pixel for t0..t1: the loudest reading under each column."""
        with self.lock:
            data = bytes(self.levels)
        count = len(data)
        if width <= 0 or t1 <= t0 or not count:
            return []
        per = (t1 - t0) * RATE_HZ / width
        out = [0.0] * width
        for x in range(width):
            first = int(t0 * RATE_HZ + x * per)
            if first >= count:
                break
            if first < 0:
                continue
            last = min(count, max(first + 1, int(math.ceil(t0 * RATE_HZ + (x + 1) * per))))
            out[x] = height_of(max(data[first:last]))
        return out


def _cache_path(src: str, cache_dir: str) -> str:
    key = hashlib.sha1(f"{VERSION}|{os.path.normcase(os.path.abspath(src))}".encode("utf-8", "replace")).hexdigest()[:24]
    return os.path.join(cache_dir, f"{key}.wave")


def load_cache(src: str, cache_dir: str) -> Wave | None:
    """A cached wave for src if the file is still at least as big as when it was cached."""
    try:
        size = os.path.getsize(src)
        with open(_cache_path(src, cache_dir), "rb") as f:
            head, _, body = f.read().partition(b"\n")
        meta = json.loads(head.decode("utf-8"))
        if meta.get("v") != VERSION or size < int(meta.get("size", 0)) or len(body) != int(meta.get("n", -1)):
            return None
    except (OSError, ValueError, TypeError):
        return None
    w = Wave()
    w.extend(body)
    return w


def save_cache(src: str, cache_dir: str, wave: Wave) -> None:
    try:
        os.makedirs(cache_dir, exist_ok=True)
        with wave.lock:
            body = bytes(wave.levels)
        head = json.dumps({"v": VERSION, "size": os.path.getsize(src), "n": len(body)}).encode()
        path = _cache_path(src, cache_dir)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(head + b"\n" + body)
        os.replace(tmp, path)
    except OSError:
        pass


class WaveJob:
    """Reads src from wave.seconds() to its current end in a daemon thread. run_lines is injectable."""

    def __init__(self, src: str, ffmpeg: str, wave: Wave, cache_dir: str, run_lines=None):
        self.src, self.ffmpeg, self.wave, self.cache_dir = src, ffmpeg, wave, cache_dir
        self.run_lines = run_lines or self._ffmpeg_lines
        self._cancel = threading.Event()
        self._proc = None
        self.thread = threading.Thread(target=self._run, daemon=True, name="clip-editor-wave")

    def start(self) -> "WaveJob":
        self.thread.start()
        return self

    def running(self) -> bool:
        return self.thread.is_alive()

    def cancel(self) -> None:
        self._cancel.set()
        p = self._proc
        if p is not None:
            try:
                p.kill()
            except OSError:
                pass

    def _ffmpeg_lines(self, start: float):
        self._proc = subprocess.Popen(wave_command(self.ffmpeg, self.src, start), stdout=subprocess.PIPE,
                                      stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL, text=True,
                                      **no_window_kwargs())
        try:
            for line in self._proc.stdout:
                yield line
        finally:
            try:
                self._proc.kill()
            except OSError:
                pass
            self._proc.wait()

    def _run(self) -> None:
        start = self.wave.seconds()
        batch = []
        try:
            for line in self.run_lines(start):
                if self._cancel.is_set():
                    return
                lv = parse_line(line)
                if lv is None:
                    continue
                batch.append(lv)
                if len(batch) >= 600:          # every minute of audio: the strip fills in from the left
                    self.wave.extend(batch)
                    batch = []
        except Exception:
            return
        if batch and not self._cancel.is_set():
            self.wave.extend(batch)
        if not self._cancel.is_set():
            save_cache(self.src, self.cache_dir, self.wave)
