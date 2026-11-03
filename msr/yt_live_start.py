"""YouTube "record from the start" (yt-dlp --live-from-start).

When a YouTube channel is found live, the first segment of that broadcast is
recorded with --live-from-start, so the minutes before MSR noticed are kept
(YouTube keeps the DVR window, yt-dlp reads up to 120 h of it). yt-dlp then
downloads faster than real time until it reaches the live edge and follows it.

How it differs from the normal live capture:
- yt-dlp uses its own DASH fragment downloader (no --downloader ffmpeg), and
  writes video and audio to two files, <stem>.f<id>.<ext>, merged with ffmpeg
  only when the download ends. If MSR stops it (split, stop, stall) the parts
  are merged here instead (finalize), or at the next start (merge_leftovers).
- Only the first segment of a broadcast uses it. Reconnects, size/time splits
  and a second sighting of the same video id record live as before.
- No start time, a broadcast older than the limit, or DVR turned off (yt-dlp
  exits with no data) fall back to the normal live capture.
"""
import os
import re
import subprocess
import time

from msr.nowindow import no_window_kwargs

SEEN_DIR = "yt_from_start"         # in the data dir: one empty file per video id
SEEN_KEEP_SECONDS = 7 * 86400
MIN_DATA_BYTES = 64 * 1024         # less than this from yt-dlp = it never got going
CAUGHT_UP_SLACK = 45.0             # within this many seconds of the live edge = caught up
LIVE_RATE = 1.3                    # media seconds per wall second at or below = following live
RATE_MIN_GAP = 20.0                # wall seconds between rate samples

_VIDEO_ID = re.compile(r"^[\w-]{6,20}$")


def settings(config):
    """(enabled, max_hours) from [YouTube]; bad values fall back to the defaults."""
    try:
        enabled = config.getboolean("YouTube", "live_from_start", fallback=True)
    except ValueError:
        enabled = True
    try:
        max_hours = float(config.get("YouTube", "live_from_start_max_hours", fallback="12") or 12)
    except ValueError:
        max_hours = 12.0
    return enabled, max(0.0, max_hours)


def _seen_path(video_id, base_dir):
    return os.path.join(base_dir or os.getcwd(), SEEN_DIR, video_id)


def was_seen(video_id, base_dir=None, now=None):
    if not video_id or not _VIDEO_ID.match(video_id):
        return False
    path = _seen_path(video_id, base_dir)
    try:
        return (time.time() if now is None else now) - os.path.getmtime(path) < SEEN_KEEP_SECONDS
    except OSError:
        return False


def mark_seen(video_id, base_dir=None, now=None):
    """Remember this broadcast was (or tried to be) recorded from the start.

    Survives restarts, so restarting MSR mid-stream does not download the
    whole broadcast again. Old markers are pruned here.
    """
    if not video_id or not _VIDEO_ID.match(video_id):
        return
    folder = os.path.join(base_dir or os.getcwd(), SEEN_DIR)
    now = time.time() if now is None else now
    try:
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, video_id), "w", encoding="ascii"):
            pass
        os.utime(os.path.join(folder, video_id), (now, now))
        for name in os.listdir(folder):
            p = os.path.join(folder, name)
            try:
                if now - os.path.getmtime(p) > SEEN_KEEP_SECONDS:
                    os.remove(p)
            except OSError:
                pass
    except OSError:
        pass


def plan(enabled, max_hours, platform, info, is_continuation, base_dir=None, now=None):
    """Decide whether this capture starts from the beginning. Returns (use, reason)."""
    if platform != "youtube":
        return False, "not YouTube"
    if not enabled:
        return False, "off in Options"
    if is_continuation:
        return False, "reconnect: continuing live"
    info = info or {}
    vid = info.get("video_id")
    if not vid:
        return False, "no video id"
    if was_seen(vid, base_dir, now):
        return False, "already recorded from the start"
    started = info.get("release_timestamp") or info.get("timestamp")
    try:
        started = float(started) if started else None
    except (TypeError, ValueError):
        started = None
    if not started:
        return False, "start time unknown"
    age = max(0.0, (time.time() if now is None else now) - started)
    if max_hours > 0 and age > max_hours * 3600:
        return False, f"live for {age / 3600:.1f} h (limit {max_hours:g} h)"
    return True, f"live for {format_hms(age)}"


def format_hms(seconds):
    s = int(max(0, seconds or 0))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


# ---------------------------------------------------------------- files

def _part_re(name):
    # <stem>.f<numeric format id>.<ext> (e.g. .f299.mp4, .f140-drc.mp4), plus yt-dlp's -FragN / .ytdl helpers next to it
    return re.compile(re.escape(name) + r"\.f\d+(?:-\w+)?\.(mp4|m4a|webm|mkv)$")


def _belongs(name, stem_name):
    if name in (stem_name + ".mp4", stem_name + ".temp.mp4", stem_name + ".merge.mp4"):
        return True
    return bool(re.match(re.escape(stem_name) + r"\.f\d+(?:-\w+)?\.(mp4|m4a|webm|mkv)(-Frag\d+(\.part)?|\.ytdl)?$", name))


def _listing(stem):
    folder, name = os.path.split(stem)
    try:
        names = os.listdir(folder or ".")
    except OSError:
        return folder, name, []
    return folder, name, [n for n in names if _belongs(n, name)]


def media_parts(stem):
    """The per-format files yt-dlp writes (<stem>.f<id>.<ext>), biggest first."""
    folder, name, names = _listing(stem)
    rx = _part_re(name)
    paths = [os.path.join(folder, n) for n in names if rx.match(n)]
    return sorted(paths, key=_size, reverse=True)


def _size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def total_size(stem):
    folder, _name, names = _listing(stem)
    return sum(_size(os.path.join(folder, n)) for n in names)


def present(stem):
    return bool(_listing(stem)[2])


def active_file(stem):
    """The file that grows right now: yt-dlp's merge output, else the biggest part (video)."""
    temp = stem + ".temp.mp4"
    if os.path.exists(temp):
        return temp
    parts = media_parts(stem)
    if parts:
        return parts[0]
    return stem + ".mp4" if os.path.exists(stem + ".mp4") else None


def _remove_helpers(stem, keep=()):
    folder, _name, names = _listing(stem)
    for n in names:
        p = os.path.join(folder, n)
        if p in keep:
            continue
        try:
            os.remove(p)
        except OSError:
            pass


def merge_parts(parts, out_file, ffmpeg_path, logger, timeout=3600, fmt=None):
    """ffmpeg stream copy of the video/audio part files into one file. True on success."""
    cmd = [ffmpeg_path or "ffmpeg", "-hide_banner", "-loglevel", "error", "-y"]
    for p in parts:
        cmd += ["-i", p]
    for i in range(len(parts)):
        cmd += ["-map", f"{i}:v?", "-map", f"{i}:a?"]
    # -shortest: when stopped mid catch-up the audio part is ahead of the video
    cmd += ["-c", "copy", "-shortest"]
    if fmt:
        cmd += ["-f", fmt]
    cmd.append(out_file)
    try:
        r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
                           errors="replace", timeout=timeout, **no_window_kwargs())
    except (OSError, subprocess.SubprocessError) as e:
        logger.error(f"From-start merge failed to run: {e}")
        return False
    if r.returncode != 0 or _size(out_file) == 0:
        logger.error(f"From-start merge failed (exit {r.returncode}): {(r.stderr or '')[-300:]}")
        try:
            os.remove(out_file)
        except OSError:
            pass
        return False
    return True


def finalize(stem, ffmpeg_path, logger, timeout=3600):
    """Return <stem>.mp4 holding the whole from-start capture, or None.

    yt-dlp merges on its own when the broadcast ends; when MSR stopped it the
    part files are merged here. Parts are kept if the merge fails.
    """
    merged = stem + ".mp4"
    parts = [p for p in media_parts(stem) if _size(p) > 0]
    if not parts:
        _remove_helpers(stem, keep=(merged,))
        return merged if _size(merged) > 0 else None
    tmp = stem + ".merge.mp4"
    logger.info(f"From-start capture: merging {len(parts)} part file(s) into {os.path.basename(merged)}")
    if not merge_parts(parts, tmp, ffmpeg_path, logger, timeout):
        return None
    try:
        os.replace(tmp, merged)
    except OSError as e:
        logger.error(f"From-start merge: could not rename the merged file: {e}")
        return None
    _remove_helpers(stem, keep=(merged,))
    return merged


def _mtime(path):
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


def merge_leftovers(folder, ffmpeg_path, logger, now=None, recent=60.0):
    """Startup: merge part files left by a from-start capture that never finished into <stem>.ts,
    which the normal leftover .ts handling then remuxes. Returns the .ts files written."""
    try:
        names = os.listdir(folder)
    except OSError:
        return []
    now = time.time() if now is None else now
    stems = set()
    for n in names:
        m = re.match(r"^(.+)\.f\d+(?:-\w+)?\.(mp4|m4a|webm|mkv)$", n)
        if m:
            stems.add(m.group(1))
    out = []
    for name in sorted(stems):
        stem = os.path.join(folder, name)
        ts = stem + ".ts"
        if os.path.exists(ts) or os.path.exists(stem + ".mp4"):
            continue
        parts = [p for p in media_parts(stem) if _size(p) > 0]
        if not parts or any(now - _mtime(p) < recent for p in parts):
            continue   # empty, or still being written
        if merge_parts(parts, ts, ffmpeg_path, logger, fmt="mpegts"):
            logger.info(f"Cleanup: merged a stopped from-start capture into {os.path.basename(ts)}")
            _remove_helpers(stem)
            out.append(ts)
    return out


# ---------------------------------------------------------------- catch-up

class CatchUp:
    """Tracks whether the from-start download has reached the live edge.

    Caught up when the media downloaded covers the time since the broadcast
    started (minus a little slack), or when it has grown at about real-time
    speed for two samples in a row. Never goes back to catching up.
    """

    def __init__(self, started_at=None, launched_at=None):
        self.started_at = started_at
        self.launched_at = time.time() if launched_at is None else launched_at
        self.media = 0.0
        self.caught_up = False
        self._last = None          # (wall, media)
        self._slow = 0

    def update(self, media_seconds, now=None):
        now = time.time() if now is None else now
        if media_seconds is None or self.caught_up:
            return self.caught_up
        self.media = max(self.media, float(media_seconds))
        if self.started_at and self.media >= (now - self.started_at) - CAUGHT_UP_SLACK:
            self.caught_up = True
            return True
        if self._last is None:
            self._last = (now, self.media)
            return False
        dt = now - self._last[0]
        if dt < RATE_MIN_GAP:
            return False
        rate = (self.media - self._last[1]) / dt
        self._last = (now, self.media)
        if self.media > 0 and rate <= LIVE_RATE:
            self._slow += 1
        else:
            self._slow = 0
        if self._slow >= 2:
            self.caught_up = True
        return self.caught_up

    def detail(self, now=None):
        if self.caught_up:
            return "live (recorded from start)"
        now = time.time() if now is None else now
        if self.started_at:
            return f"catching up from start ({format_hms(self.media)} of {format_hms(now - self.started_at)})"
        return f"catching up from start ({format_hms(self.media)})"


class LiveFromStart:
    """One from-start capture: where its files go and how far it has got."""

    def __init__(self, stem, video_id=None, started_at=None, launched_at=None):
        self.stem = stem
        self.video_id = video_id
        self.catchup = CatchUp(started_at, launched_at)
        self.stop_event = None     # set by the monitor; ends the catch-up probe thread

    @property
    def merged_file(self):
        return self.stem + ".mp4"

    @property
    def caught_up(self):
        return self.catchup.caught_up

    def present(self):
        return present(self.stem)

    def total_size(self):
        return total_size(self.stem)

    def active_file(self):
        return active_file(self.stem)

    def detail(self):
        return self.catchup.detail()

    def sample(self, probe_duration):
        """Probe the video part's media length and update the catch-up state."""
        parts = media_parts(self.stem)
        if not parts:
            return self.caught_up
        try:
            d = probe_duration(parts[0])
        except Exception:
            d = None
        return self.catchup.update(d)
