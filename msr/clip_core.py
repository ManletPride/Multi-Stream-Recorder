"""Clip Editor engine, Tk-free: how long a (still growing) recording is, preview frames,
the cut itself and output names.

Everything here only reads the source. A recording's .ts is opened with shared access by
Python and ffmpeg, so the recorder keeps writing undisturbed. Every ffmpeg/ffprobe runs
hidden (msr.nowindow).
"""

from __future__ import annotations

import datetime
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass

from msr.nowindow import no_window_kwargs

MEDIA_EXTS = (".ts", ".mp4", ".mkv", ".mov", ".m4v", ".flv", ".webm")
PTS_WRAP = 1 << 33
_SCAN = 2 * 1024 * 1024        # bytes read at each end of a .ts to find its first / last timestamp


class ClipError(Exception):
    """A cut that cannot be made; the text is shown to the user as is."""


# ------------------------------------------------------------------ times
def parse_time(text, length: float | None = None) -> float:
    """'90', '1:30', '01:02:03.5' -> seconds. A leading '-' counts back from the end
    (length, e.g. the live edge); 'end' / 'live' is the end itself."""
    s = str(text).strip().lower()
    if s in ("end", "live", "now"):
        if length is None:
            raise ClipError("'end' needs a known length.")
        return float(length)
    neg = s.startswith("-")
    if neg:
        s = s[1:]
    try:
        total = 0.0
        for part in s.split(":"):
            total = total * 60 + float(part)
    except ValueError:
        raise ClipError(f"Not a time: {text!r} (use seconds, M:SS or H:MM:SS).") from None
    if neg:
        if length is None:
            raise ClipError("A time counted from the end needs a known length.")
        return max(0.0, float(length) - total)
    return total


def fmt_time(seconds: float | None, tenths: bool = True) -> str:
    if seconds is None:
        return "--:--"
    seconds = max(0.0, float(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    sec = f"{s:04.1f}" if tenths else f"{int(s):02d}"
    return f"{int(h)}:{int(m):02d}:{sec}" if h >= 1 else f"{int(m)}:{sec}"


# ------------------------------------------------------------------ length of a growing .ts
def _sync_offset(buf: bytes) -> int | None:
    for i in range(min(188, len(buf))):
        if all(i + k * 188 < len(buf) and buf[i + k * 188] == 0x47 for k in range(3)):
            return i
    return None


def _pts_values(buf: bytes):
    """PTS of every PES start (audio or video) in a chunk of an MPEG-TS, in file order."""
    off = _sync_offset(buf)
    if off is None:
        return
    for i in range(off, len(buf) - 187, 188):
        pkt = buf[i:i + 188]
        if pkt[0] != 0x47 or not pkt[1] & 0x40:
            continue
        afc = (pkt[3] >> 4) & 3
        p = 4
        if afc & 2:
            p += 1 + pkt[4]
        if not afc & 1 or p + 14 > 188:
            continue
        pes = pkt[p:]
        if pes[:3] != b"\x00\x00\x01" or not (0xC0 <= pes[3] <= 0xEF):
            continue
        if not (pes[7] >> 6) & 2:
            continue
        b = pes[9:14]
        yield ((b[0] >> 1) & 7) << 30 | b[1] << 22 | (b[2] >> 1) << 15 | b[3] << 7 | b[4] >> 1


def ts_length(path: str) -> float | None:
    """Seconds from the first to the last timestamp of an MPEG-TS, read from both ends of the
    file (fast on a 20 GB recording, works while it grows). None when none are found."""
    with open(path, "rb") as f:   # shared read: the recorder keeps writing
        head = f.read(_SCAN)
        size = os.fstat(f.fileno()).st_size
        f.seek(max(0, size - _SCAN))
        tail = f.read(_SCAN)
    heads = list(_pts_values(head))
    lasts = list(_pts_values(tail))
    if not heads or not lasts:
        return None
    ref = heads[0]
    signed = lambda p: (p - ref + PTS_WRAP // 2) % PTS_WRAP - PTS_WRAP // 2  # noqa: E731 (wrap-safe)
    first = min(signed(p) for p in heads)
    last = max(signed(p) for p in lasts)
    return max(0.0, (last - first) / 90000.0)


def probe_length(path: str, ffprobe: str = "ffprobe") -> float | None:
    try:
        r = subprocess.run([ffprobe, "-v", "error", "-show_entries", "format=duration", "-of", "json", path],
                           capture_output=True, text=True, timeout=30, **no_window_kwargs())
        return float(json.loads(r.stdout or "{}").get("format", {}).get("duration"))
    except (OSError, ValueError, TypeError, subprocess.SubprocessError):
        return None


def media_length(path: str, ffprobe: str = "ffprobe") -> float | None:
    """Length in seconds of a recording (a growing .ts too), or None if unreadable."""
    if path.lower().endswith(".ts"):
        try:
            n = ts_length(path)
        except OSError:
            return None
        if n is not None:
            return n
    return probe_length(path, ffprobe)


def keyframe_at_or_before(src: str, t: float, ffprobe: str = "ffprobe", run=subprocess.run) -> float | None:
    """Time (from the start) of the video keyframe a stream copy from t begins at, or None."""
    try:
        r = run([ffprobe, "-v", "error", "-show_entries", "format=start_time", "-of", "csv=p=0", src],
                capture_output=True, text=True, timeout=30, **no_window_kwargs())
        base = float((r.stdout or "0").strip().splitlines()[0] or 0)
        at = base + t
        r = run([ffprobe, "-v", "error", "-select_streams", "v:0", "-read_intervals", f"{max(0.0, at - 15):.3f}%{at + 0.05:.3f}",
                 "-show_entries", "packet=pts_time,flags", "-of", "csv=p=0", src],
                capture_output=True, text=True, timeout=60, **no_window_kwargs())
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return None
    best = None
    for line in (r.stdout or "").splitlines():
        parts = line.split(",")
        if len(parts) >= 2 and "K" in parts[1]:
            try:
                pts = float(parts[0])
            except ValueError:
                continue
            if pts <= at + 0.001 and (best is None or pts > best):
                best = pts
    return None if best is None else max(0.0, best - base)


def ffprobe_for(ffmpeg: str) -> str:
    d, name = os.path.split(ffmpeg or "ffmpeg")
    return os.path.join(d, name.lower().replace("ffmpeg", "ffprobe")) if d else name.lower().replace("ffmpeg", "ffprobe")


# ------------------------------------------------------------------ preview frame
def frame_command(ffmpeg: str, src: str, t: float, width: int = 640, keyframe: bool = False) -> list[str]:
    """One preview frame at t. keyframe=True: the keyframe at or before t, decoded alone (input
    seek, no decoding forward), which is the fast first look after a far jump."""
    fast = ["-skip_frame", "nokey", "-noaccurate_seek"] if keyframe else []
    return [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", *fast, "-ss", f"{max(0.0, t):.3f}",
            "-i", src, "-frames:v", "1", "-an", "-vf", f"scale={width}:-2:flags=fast_bilinear",
            "-f", "image2pipe", "-vcodec", "ppm", "pipe:1"]


def grab_frame(ffmpeg: str, src: str, t: float, width: int = 640, keyframe: bool = False,
               cancel=None, spawn=None) -> bytes | None:
    """One PPM frame at t (seconds from the start), or None. cancel() returning True kills the
    ffmpeg at once (a newer scrub position wins), so stale decodes never queue up on the disk."""
    try:
        proc = (spawn or subprocess.Popen)(frame_command(ffmpeg, src, t, width, keyframe), stdin=subprocess.DEVNULL,
                                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, **no_window_kwargs())
    except OSError:
        return None
    deadline = time.monotonic() + 20
    while True:
        try:
            out, _ = proc.communicate(timeout=0.03)
            break
        except subprocess.TimeoutExpired:
            if (cancel is not None and cancel()) or time.monotonic() > deadline:
                proc.kill()
                try:
                    proc.communicate(timeout=2)
                except (subprocess.SubprocessError, OSError):
                    pass
                return None
    return out if proc.returncode == 0 and out and out.startswith(b"P6") else None


def video_info(src: str, ffprobe: str = "ffprobe", run=subprocess.run):
    """(width, height, fps) of the first video stream, or None."""
    try:
        r = run([ffprobe, "-v", "error", "-select_streams", "v:0", "-show_entries",
                 "stream=width,height,avg_frame_rate,r_frame_rate", "-of", "json", src],
                capture_output=True, text=True, timeout=30, **no_window_kwargs())
        st = (json.loads(r.stdout or "{}").get("streams") or [{}])[0]
        w, h = int(st["width"]), int(st["height"])
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        return None
    fps = 30.0
    for key in ("avg_frame_rate", "r_frame_rate"):
        num, _, den = str(st.get(key, "")).partition("/")
        try:
            v = float(num) / float(den or 1)
        except (ValueError, ZeroDivisionError):
            continue
        if 1 <= v <= 240:
            fps = v
            break
    return w, h, fps


def preview_size(w: int, h: int, width: int = 640) -> tuple[int, int]:
    """Preview frame size for a w x h source: width wide (never upscaled), even height."""
    width = min(width, w - w % 2)
    return width, max(2, int(round(width * h / w / 2)) * 2)


def raw_video_command(ffmpeg: str, src: str, t: float, size: tuple[int, int], fps: float) -> list[str]:
    """One continuous stream of rgb24 frames at preview size for playback."""
    w, h = size
    return [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-ss", f"{max(0.0, t):.3f}", "-i", src,
            "-an", "-sn", "-vf", f"fps={fps:g},scale={w}:{h}:flags=fast_bilinear", "-pix_fmt", "rgb24",
            "-f", "rawvideo", "pipe:1"]


def video_command(ffmpeg: str, src: str, t: float, fps: int = 12, width: int = 640) -> list[str]:
    return [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-ss", f"{max(0.0, t):.3f}", "-i", src,
            "-an", "-sn", "-vf", f"fps={fps},scale={width}:-2", "-f", "image2pipe", "-vcodec", "ppm", "pipe:1"]


def read_ppm(stream) -> bytes | None:
    """One binary PPM (P6) frame from a pipe, or None at the end."""
    header = b""
    fields = []
    while len(fields) < 4:
        ch = stream.read(1)
        if not ch:
            return None
        header += ch
        if ch.isspace() and header.strip():
            fields = header.split()
    w, h, maxval = int(fields[1]), int(fields[2]), int(fields[3])
    size = w * h * (3 if maxval < 256 else 6)
    data = b""
    while len(data) < size:
        chunk = stream.read(size - len(data))
        if not chunk:
            return None
        data += chunk
    return header + data


# ------------------------------------------------------------------ in / out marks
class Marks:
    """In and out points on a timeline that may grow (a live recording)."""

    MIN = 0.2

    def __init__(self, length: float = 0.0):
        self.length = max(0.0, length)
        self.cut_in: float | None = None
        self.cut_out: float | None = None

    def _clamp(self, t: float) -> float:
        return min(max(0.0, t), self.length)

    def set_in(self, t: float) -> None:
        self.cut_in = self._clamp(t)
        if self.cut_out is not None and self.cut_out - self.cut_in < self.MIN:
            self.cut_out = None

    def set_out(self, t: float) -> None:
        self.cut_out = self._clamp(t)
        if self.cut_in is not None and self.cut_out - self.cut_in < self.MIN:
            self.cut_in = None

    def nudge(self, which: str, delta: float) -> None:
        if which == "in" and self.cut_in is not None:
            hi = (self.cut_out - self.MIN) if self.cut_out is not None else self.length
            self.cut_in = min(max(0.0, self.cut_in + delta), hi)
        elif which == "out" and self.cut_out is not None:
            lo = (self.cut_in + self.MIN) if self.cut_in is not None else 0.0
            self.cut_out = max(min(self.length, self.cut_out + delta), lo)

    def grow(self, length: float) -> None:
        self.length = max(self.length, length)

    @property
    def clip_length(self) -> float | None:
        if self.cut_in is None or self.cut_out is None:
            return None
        return self.cut_out - self.cut_in

    @property
    def ready(self) -> bool:
        return self.clip_length is not None and self.clip_length >= self.MIN


# ------------------------------------------------------------------ the cut
@dataclass
class CutResult:
    path: str
    requested_start: float
    requested_end: float
    start: float          # actual start (the keyframe a stream copy begins at)
    end: float
    duration: float
    exact: bool

    @property
    def snapped(self) -> bool:
        return not self.exact and abs(self.start - self.requested_start) > 0.05

    def as_dict(self) -> dict:
        r = {k: getattr(self, k) for k in ("path", "requested_start", "requested_end", "start", "end",
                                           "duration", "exact")}
        r = {k: (round(v, 3) if isinstance(v, float) else v) for k, v in r.items()}
        r["snapped"] = self.snapped
        r["file"] = self.path
        return r


def cut_command(ffmpeg: str, src: str, start: float, end: float, out: str, exact: bool = False) -> list[str]:
    cmd = [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-ss", f"{start:.3f}", "-i", src,
           "-t", f"{end - start:.3f}", "-map", "0:v:0?", "-map", "0:a:0?"]
    if exact:   # re-encoded: frame-exact in and out
        cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k"]
    else:       # stream copy: instant and lossless, starts at the keyframe at or before start
        cmd += ["-c", "copy", "-avoid_negative_ts", "make_zero"]
    return cmd + ["-movflags", "+faststart", "-f", "mp4", out]


def unique_path(path: str) -> str:
    stem, ext = os.path.splitext(path)
    n, out = 1, path
    while os.path.exists(out):
        n += 1
        out = f"{stem}_{n}{ext}"
    return out


def clip_name(src: str, start: float, end: float, now: datetime.datetime | None = None) -> str:
    stem = os.path.splitext(os.path.basename(src))[0]
    stem = re.sub(r"[^\w.\-]+", "_", stem)[:80].strip("_") or "clip"
    stamp = (now or datetime.datetime.now()).strftime("%Y%m%d_%H%M%S")
    t = lambda s: fmt_time(s, tenths=False).replace(":", "-")  # noqa: E731
    return f"{stem}_clip_{t(start)}_to_{t(end)}_{stamp}.mp4"


def clips_folder_for(src: str, clips_base: str) -> str:
    """Clips/<platform>/<channel> when src sits in Recorded|Processed/<platform>/<channel>/,
    else Clips/ itself."""
    parts = os.path.normpath(os.path.abspath(src)).split(os.sep)
    for i in range(len(parts) - 3, -1, -1):
        if parts[i].lower() in ("recorded", "processed", "pendingdeletion"):
            rest = parts[i + 1:-1]
            if rest:
                return os.path.join(clips_base, *rest)
            break
    return clips_base


def make_clip(ffmpeg: str, src: str, start: float, end: float, out_dir: str, *, exact: bool = False,
              length: float | None = None, ffprobe: str | None = None, run=subprocess.run) -> CutResult:
    """Cut [start, end] of src into out_dir. Returns the actual times (a stream copy starts at
    the keyframe at or before start; its real start is end - the output's length)."""
    if not os.path.isfile(src):
        raise ClipError(f"The recording is not there any more: {src}")
    if length is not None:
        end = min(end, length)
    if start < 0 or end - start < 0.2:
        raise ClipError("Mark an in point before the out point (at least 0.2 s).")
    try:
        os.makedirs(out_dir, exist_ok=True)
    except OSError as e:
        raise ClipError(f"Cannot create the clips folder {out_dir}: {e}") from None
    probe = ffprobe or ffprobe_for(ffmpeg)
    cut_from = start
    if not exact:   # a stream copy can only begin at a keyframe: cut from there, and say so
        kf = keyframe_at_or_before(src, start, probe, run=run)
        if kf is not None:
            cut_from = kf
    final = unique_path(os.path.join(out_dir, clip_name(src, start, end)))
    part = final[:-4] + ".part.mp4"
    try:
        r = run(cut_command(ffmpeg, src, cut_from + (0.001 if not exact else 0.0), end, part, exact), capture_output=True, text=True,
                timeout=3600, stdin=subprocess.DEVNULL, **no_window_kwargs())
    except (OSError, subprocess.SubprocessError) as e:
        _remove(part)
        raise ClipError(f"ffmpeg could not run: {e}") from None
    if r.returncode != 0 or not os.path.isfile(part) or os.path.getsize(part) < 1024:
        _remove(part)
        err = (r.stderr or "").strip().splitlines()[-1:] or ["no output"]
        raise ClipError(f"The cut failed: {err[0][:300]}")
    os.replace(part, final)
    got = probe_length(final, probe)
    if got is None:
        got = end - cut_from
    real_end = min(end, cut_from + got)   # the container may round the last frame up a little
    return CutResult(final, start, end, cut_from, real_end, real_end - cut_from, exact)


def _remove(path):
    try:
        os.remove(path)
    except OSError:
        pass


def finished_successor(src: str) -> str | None:
    """After a recording ends its .ts is remuxed to Processed/<same path>/<same name>.mp4 and
    the .ts moved away; the editor follows it there (same timeline)."""
    parts = os.path.normpath(src).split(os.sep)
    for i in range(len(parts) - 1, -1, -1):
        if parts[i].lower() == "recorded":
            parts[i] = "Processed" if parts[i] == "Recorded" else "processed"
            cand = os.sep.join(parts)
            cand = os.path.splitext(cand)[0] + ".mp4"
            return cand if os.path.isfile(cand) else None
    return None
