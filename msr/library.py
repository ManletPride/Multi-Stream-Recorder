"""Library statistics: how much is recorded, from which sites and channels, and how it grows.

Read-only and fast. A scan only lists folders (``os.scandir``) and reads the file sizes and
dates the listing already carries; it never opens a video, never runs ffprobe and never
follows symlinks/junctions. Recorded time is a separate, opt-in step (``count_hours``) that
reads the small ``.meta.json`` sidecar MSR writes next to each recording and only probes the
files that have none, rate-limited, cancellable and remembered in a cache.

Layout it understands (see AGENTS.md): ``<streams_dir>/Processed|Recorded|PendingDeletion/
<platform>/<channel folder>/files`` plus the clips folder (``clips_dir`` or
``<streams_dir>/Clips``, same ``<platform>/<channel>`` shape) and optional extra folders.
Top-level folder names are matched case-insensitively.

Nothing in this module draws, deletes or writes anything except the optional duration
cache, whose location the caller chooses (the app's own data folder).
"""

from __future__ import annotations

import datetime
import json
import os
import re
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable

from msr.util import PENDING_DELETION_FOLDER, channel_key_to_dirs

VIDEO_EXT = frozenset({".mp4", ".mkv", ".ts", ".webm", ".mov", ".m4v", ".flv", ".avi", ".m4a", ".mp3"})
IMAGE_EXT = frozenset({".jpg", ".jpeg", ".png", ".webp", ".gif"})
PARTIAL_SUFFIXES = (".part", ".ytdl", ".tmp", ".temp", ".download", ".crdownload")
CATEGORY_FOLDERS = {"processed": "processed", "recorded": "recorded",
                    PENDING_DELETION_FOLDER.lower(): "pending"}
MAX_DEPTH = 8
UNSORTED = "(unsorted)"
PLATFORM_NAMES = {"kick": "Kick", "twitch": "Twitch", "youtube": "YouTube", "tiktok": "TikTok",
                  "rumble": "Rumble", "fishtank": "Fishtank", "custom": "Custom URLs",
                  "other": "Other folders", UNSORTED: "Unsorted"}
FILL_MAX_DAYS = 3650  # beyond ten years "fills in N days" is not a useful number

_DATE_RE = re.compile(r"(?<!\d)(20\d\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")
_REPARSE_POINT = 0x400  # Windows junctions and symlinks


def platform_label(platform: str) -> str:
    return PLATFORM_NAMES.get(platform.lower(), platform[:1].upper() + platform[1:])


def fmt_size(num: float) -> str:
    """1536 -> '1.5 KB' (binary steps like Explorer, which also calls them KB/GB)."""
    num = float(num)
    if abs(num) < 1024:
        return f"{int(num)} B"
    for unit in ("KB", "MB", "GB"):
        num /= 1024
        if abs(num) < 1024:
            return f"{num:.1f} {unit}"
    return f"{num / 1024:.2f} TB"


def fmt_hours(seconds: float) -> str:
    hours = seconds / 3600
    return f"{hours:,.0f} h" if hours >= 100 else f"{hours:,.1f} h"


def date_from_name(name: str) -> float | None:
    """Local-midnight timestamp of a YYYYMMDD in a file name (MSR's default
    '{username}_{timestamp}' has one), or None."""
    match = _DATE_RE.search(name)
    if not match:
        return None
    try:
        return datetime.datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).timestamp()
    except (ValueError, OverflowError, OSError):
        return None


def month_key(ts: float) -> str:
    try:
        return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m")
    except (ValueError, OverflowError, OSError):
        return "unknown"


# ------------------------------------------------------------------ data
@dataclass
class Roots:
    """Where to look. clips_dir blank = streams_dir/Clips. extra_dirs are scanned as 'Other'."""
    streams_dir: str
    clips_dir: str = ""
    extra_dirs: tuple[str, ...] = ()


def roots_from_config(config) -> Roots:
    """Roots from MSR's Config. Extra folders: config.ini [Library] extra_dirs, ';' separated
    (optional; the section need not exist)."""
    streams = (config.get("Paths", "streams_dir", fallback="") or "").strip()
    clips = (config.get("Clipping", "clips_dir", fallback="") or "").strip()
    raw = config.get("Library", "extra_dirs", fallback="") or ""
    extras = tuple(p.strip().strip('"') for p in raw.split(";") if p.strip())
    return Roots(streams, clips, extras)


class ChannelStat:
    __slots__ = ("platform", "name", "rec_bytes", "rec_files", "raw_bytes", "raw_files", "pending_bytes",
                 "pending_files", "clip_bytes", "clip_files", "shot_bytes", "shot_files", "first", "last",
                 "seconds", "timed", "in_roster", "paths")

    def __init__(self, platform: str, name: str):
        self.platform, self.name = platform, name
        self.rec_bytes = self.rec_files = self.raw_bytes = self.raw_files = 0
        self.pending_bytes = self.pending_files = self.clip_bytes = self.clip_files = 0
        self.shot_bytes = self.shot_files = 0
        self.first: float | None = None
        self.last: float | None = None
        self.seconds: float | None = None  # recorded time; None until counted
        self.timed = 0  # finished recordings whose length is known
        self.in_roster: bool | None = None  # None = roster unknown
        self.paths: dict[str, str] = {}  # category -> folder (for 'Open folder'; never exported)

    @property
    def recordings_bytes(self) -> int:
        """What this channel's recordings take: finished + raw (not yet processed)."""
        return self.rec_bytes + self.raw_bytes

    @property
    def total_bytes(self) -> int:
        return (self.rec_bytes + self.raw_bytes + self.pending_bytes + self.clip_bytes + self.shot_bytes)

    def best_folder(self) -> str | None:
        for category in ("processed", "recorded", "clips", "pending"):
            if category in self.paths:
                return self.paths[category]
        return next(iter(self.paths.values()), None)


@dataclass
class VideoRef:
    key: str       # cache key: "<category>/<relative path>"
    path: str
    mtime: int
    size: int
    channel: tuple[str, str]


@dataclass
class LibraryStats:
    roots_found: dict = field(default_factory=dict)         # category -> bool
    categories: dict = field(default_factory=dict)          # category -> [bytes, files]
    platforms: dict = field(default_factory=dict)           # platform -> [bytes, files] (recordings)
    channels: dict = field(default_factory=dict)            # (platform, name) lower -> ChannelStat
    months: dict = field(default_factory=dict)              # 'YYYY-MM' -> [bytes, files] (finished)
    oldest: float | None = None
    newest: float | None = None
    biggest_file: tuple | None = None                       # (bytes, channel label)
    bytes_7d: int = 0
    bytes_30d: int = 0
    partial_files: int = 0
    partial_bytes: int = 0
    symlinks_skipped: int = 0
    unreadable_dirs: int = 0
    files_seen: int = 0
    bytes_seen: int = 0
    scanned_at: float = 0.0
    elapsed: float = 0.0
    cancelled: bool = False
    disk: tuple | None = None                               # (total, used, free) of the streams drive
    seconds_total: float | None = None
    timed_files: int = 0
    videos: list = field(default_factory=list)              # VideoRef of finished recordings

    def cat(self, name: str) -> tuple[int, int]:
        b, f = self.categories.get(name, (0, 0))
        return b, f

    @property
    def recordings_bytes(self) -> int:
        return self.cat("processed")[0] + self.cat("raw")[0]

    @property
    def recordings_files(self) -> int:
        return self.cat("processed")[1] + self.cat("raw")[1]

    def ranked_channels(self) -> list[ChannelStat]:
        return sorted(self.channels.values(), key=lambda c: (-c.recordings_bytes, c.name.lower()))

    def ranked_platforms(self) -> list[tuple[str, int, int]]:
        rows = [(p, b, f) for p, (b, f) in self.platforms.items() if b > 0 or f > 0]
        return sorted(rows, key=lambda r: (-r[1], r[0]))


# ------------------------------------------------------------------ scanning
class Cancelled(Exception):
    pass


def _top_level_map(streams_dir: str) -> dict:
    """category -> actual folder name under streams_dir (matched case-insensitively)."""
    found: dict = {}
    try:
        with os.scandir(streams_dir) as it:
            for entry in it:
                category = CATEGORY_FOLDERS.get(entry.name.lower())
                if category and category not in found and _is_real_dir(entry):
                    found[category] = entry.path
    except OSError:
        pass
    return found


def _is_real_dir(entry) -> bool:
    try:
        return entry.is_dir(follow_symlinks=False)
    except OSError:
        return False


def _is_link(entry, st=None) -> bool:
    try:
        if entry.is_symlink():
            return True
        if st is None:
            st = entry.stat(follow_symlinks=False)
        return bool(getattr(st, "st_file_attributes", 0) & _REPARSE_POINT)
    except OSError:
        return True


def scan_library(roots: Roots, progress: Callable[[int, int, str], None] | None = None,
                 cancel: threading.Event | None = None, now: float | None = None,
                 collect_videos: bool = True) -> LibraryStats:
    """Scan the folders in `roots`. Never raises for filesystem problems (they are counted);
    returns what it has, with .cancelled set, when `cancel` is set.

    progress(files, bytes, label) is called a few times a second from this thread."""
    started = time.monotonic()
    now = time.time() if now is None else now
    stats = LibraryStats(scanned_at=now)
    state = {"last_report": 0.0, "tick": 0}

    def check(label: str) -> None:
        state["tick"] += 1
        if cancel is not None and state["tick"] % 64 == 0 and cancel.is_set():
            raise Cancelled
        if progress is not None:
            t = time.monotonic()
            if t - state["last_report"] >= 0.12:
                state["last_report"] = t
                progress(stats.files_seen, stats.bytes_seen, label)

    def channel_for(platform: str, name: str, category: str, folder: str) -> ChannelStat:
        key = (platform.lower(), name.lower())
        stat = stats.channels.get(key)
        if stat is None:
            stat = stats.channels[key] = ChannelStat(platform.lower(), name)
        stat.paths.setdefault(category, folder)
        return stat

    def add_cat(name: str, size: int) -> None:
        row = stats.categories.setdefault(name, [0, 0])
        row[0] += size
        row[1] += 1

    def widen(ch: ChannelStat, ts: float) -> None:
        ch.first = ts if ch.first is None else min(ch.first, ts)
        ch.last = ts if ch.last is None else max(ch.last, ts)

    def handle_file(entry, st, category: str, platform: str, channel: str, folder: str, rel_key: str) -> None:
        size = st.st_size
        name = entry.name
        low = name.lower()
        ext = os.path.splitext(low)[1]
        stats.files_seen += 1
        stats.bytes_seen += size
        if category == "other":
            add_cat("other", size)
            return
        ch = channel_for(platform, channel, category, folder)
        partial = low.endswith(PARTIAL_SUFFIXES)
        mtime = st.st_mtime
        if partial:
            stats.partial_files += 1
            stats.partial_bytes += size
        if category == "clips":
            if ext in IMAGE_EXT:
                add_cat("screenshots", size)
                ch.shot_bytes += size
                ch.shot_files += 1
            elif ext in VIDEO_EXT:
                add_cat("clips", size)
                ch.clip_bytes += size
                ch.clip_files += 1
            else:
                add_cat("sidecars", size)
            return
        if ext not in VIDEO_EXT and not partial:
            add_cat("sidecars", size)  # .meta.json and friends
            return
        if category == "pending":
            add_cat("pending", size)
            ch.pending_bytes += size
            ch.pending_files += 1
            return
        # Recorded/ holds raw .ts (growing or waiting for the remux); Processed/ the finished files
        finished = category == "processed" and not partial
        bucket = "processed" if finished else "raw"
        add_cat(bucket, size)
        when = date_from_name(name) or mtime
        stats.oldest = when if stats.oldest is None else min(stats.oldest, when)
        stats.newest = when if stats.newest is None else max(stats.newest, when)
        widen(ch, when)
        plat = stats.platforms.setdefault(platform.lower(), [0, 0])
        plat[0] += size
        plat[1] += 1
        if stats.biggest_file is None or size > stats.biggest_file[0]:
            stats.biggest_file = (size, f"{platform.lower()}/{channel}")
        if finished:
            ch.rec_bytes += size
            ch.rec_files += 1
            month = stats.months.setdefault(month_key(when), [0, 0])
            month[0] += size
            month[1] += 1
            age = now - mtime
            if age <= 30 * 86400:
                stats.bytes_30d += size
                if age <= 7 * 86400:
                    stats.bytes_7d += size
            if collect_videos:
                stats.videos.append(VideoRef(rel_key, entry.path, int(mtime), size,
                                             (platform.lower(), channel.lower())))
        else:
            ch.raw_bytes += size
            ch.raw_files += 1

    def walk(top: str, category: str, label: str) -> None:
        stats.roots_found[label] = True
        # (path, depth, relative parts below top)
        stack = [(top, 0, ())]
        while stack:
            path, depth, parts = stack.pop()
            try:
                it = os.scandir(path)
            except OSError:
                stats.unreadable_dirs += 1
                continue
            with it:
                while True:
                    try:
                        entry = next(it)
                    except StopIteration:
                        break
                    except OSError:
                        stats.unreadable_dirs += 1
                        break
                    check(label)
                    try:
                        st = entry.stat(follow_symlinks=False)
                    except OSError:
                        stats.unreadable_dirs += 1
                        continue
                    if _is_link(entry, st):
                        stats.symlinks_skipped += 1
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        if depth < MAX_DEPTH:
                            stack.append((entry.path, depth + 1, parts + (entry.name,)))
                        continue
                    if category == "other":
                        platform, channel = "other", parts[0] if parts else os.path.basename(top)
                    elif len(parts) >= 2:
                        platform, channel = parts[0], "/".join(parts[1:])
                    elif len(parts) == 1:
                        platform, channel = parts[0], UNSORTED
                    else:
                        platform, channel = UNSORTED, UNSORTED
                    rel_key = category + "/" + "/".join(parts + (entry.name,))
                    handle_file(entry, st, category, platform, channel, path, rel_key)

    try:
        streams = roots.streams_dir
        top = _top_level_map(streams) if streams else {}
        if not streams or not os.path.isdir(streams):
            stats.roots_found["Recordings folder"] = False
        for category, label in (("processed", "Processed"), ("recorded", "Recorded"),
                                ("pending", PENDING_DELETION_FOLDER)):
            if category in top:
                walk(top[category], category, label)
            else:
                stats.roots_found[label] = False
        clips = roots.clips_dir
        if not clips and streams:
            clips = next((e for e in _child_dirs(streams) if e.lower() == "clips"), "")
            clips = os.path.join(streams, clips) if clips else ""
        if clips and os.path.isdir(clips):
            walk(clips, "clips", "Clips")
        else:
            stats.roots_found["Clips"] = False
        for number, extra in enumerate(roots.extra_dirs, 1):
            name = f"Other folder {number}"  # numbered: a folder's own name is not exported
            if os.path.isdir(extra):
                walk(extra, "other", name)
            else:
                stats.roots_found[name] = False
    except Cancelled:
        stats.cancelled = True
    stats.elapsed = time.monotonic() - started
    if roots.streams_dir:
        stats.disk = _disk_usage(roots.streams_dir)
    for kind in ("processed", "raw", "pending", "clips", "screenshots", "sidecars", "other"):
        stats.categories.setdefault(kind, [0, 0])
    if progress is not None:
        progress(stats.files_seen, stats.bytes_seen, "done")
    return stats


def _child_dirs(path: str) -> list[str]:
    try:
        with os.scandir(path) as it:
            return [e.name for e in it if _is_real_dir(e)]
    except OSError:
        return []


def _disk_usage(folder: str):
    probe = folder
    while probe and not os.path.isdir(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            return None
        probe = parent
    try:
        u = shutil.disk_usage(probe)
        return (u.total, u.used, u.free)
    except OSError:
        return None


def mark_roster(stats: LibraryStats, roster_keys: Iterable[str] | None) -> None:
    """Flag which channels are in the roster (channels.json). None = unknown (leaves None)."""
    if roster_keys is None:
        return
    known = set()
    for key in roster_keys:
        platform, folder = channel_key_to_dirs(key)
        known.add((platform.lower(), folder.replace("\\", "/").lower()))
    for (platform, name), ch in stats.channels.items():
        ch.in_roster = (platform, name.replace("\\", "/")) in known


# ------------------------------------------------------------------ recorded time (opt-in)
class DurationCache:
    """Remembers lengths by file (relative key + mtime + size), so a file is read once."""

    VERSION = 1

    def __init__(self, path: str | None):
        self.path = path
        self.items: dict = {}
        self.dirty = False
        self._load()

    def _load(self) -> None:
        if not self.path:
            return
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict) and data.get("version") == self.VERSION and isinstance(data.get("d"), dict):
                self.items = {k: v for k, v in data["d"].items()
                              if isinstance(v, list) and len(v) == 3 and all(isinstance(x, (int, float)) for x in v)}
        except (OSError, ValueError):
            self.items = {}

    def get(self, key: str, mtime: int, size: int) -> float | None:
        row = self.items.get(key)
        if row and int(row[0]) == int(mtime) and int(row[1]) == int(size):
            return float(row[2])
        return None

    def put(self, key: str, mtime: int, size: int, seconds: float) -> None:
        self.items[key] = [int(mtime), int(size), round(float(seconds), 1)]
        self.dirty = True

    def keep_only(self, keys: set) -> None:
        stale = [k for k in self.items if k not in keys]
        for k in stale:
            del self.items[k]
        if stale:
            self.dirty = True

    def save(self) -> None:
        if not self.path or not self.dirty:
            return
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"version": self.VERSION, "d": self.items}, fh, separators=(",", ":"))
            os.replace(tmp, self.path)
            self.dirty = False
        except OSError:
            try:
                os.remove(tmp)
            except OSError:
                pass


def sidecar_seconds(video_path: str) -> float | None:
    """duration_seconds from the .meta.json MSR writes next to a finished recording."""
    base = video_path.rsplit(".", 1)[0]
    try:
        with open(base + ".meta.json", "r", encoding="utf-8") as fh:
            value = json.load(fh).get("duration_seconds")
        value = float(value)
        return value if value >= 0 else None
    except (OSError, ValueError, TypeError, AttributeError):
        return None


def ffprobe_seconds(ffprobe: str, path: str, timeout: float = 20.0) -> float | None:
    """Length of one file via ffprobe (only used on demand, one file at a time)."""
    try:
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
        out = subprocess.run(
            [ffprobe, "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", path],
            capture_output=True, text=True, timeout=timeout, creationflags=flags)
        value = float(out.stdout.strip().splitlines()[0])
        return value if value >= 0 else None
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return None


def count_hours(stats: LibraryStats, cache: DurationCache | None = None,
                probe: Callable[[str], float | None] | None = None,
                progress: Callable[[int, int], None] | None = None,
                cancel: threading.Event | None = None,
                min_interval: float = 0.05, sleep: Callable[[float], None] = time.sleep) -> tuple[int, int]:
    """Fill in recorded time for the finished recordings. Order per file: cache, sidecar,
    then `probe` (ffprobe) if given, at most one every `min_interval` seconds.
    Returns (files with a length, files without). Safe to cancel: what was counted stays."""
    known = missing = 0
    for ch in stats.channels.values():
        ch.seconds, ch.timed = None, 0
    total_seconds = 0.0
    last_probe = 0.0
    total = len(stats.videos)
    seen_keys = set()
    for index, video in enumerate(stats.videos, 1):
        if cancel is not None and cancel.is_set():
            stats.cancelled = True
            break
        seen_keys.add(video.key)
        seconds = cache.get(video.key, video.mtime, video.size) if cache else None
        if seconds is None:
            seconds = sidecar_seconds(video.path)
            if seconds is None and probe is not None:
                wait = min_interval - (time.monotonic() - last_probe)
                if wait > 0:
                    sleep(wait)
                last_probe = time.monotonic()
                seconds = probe(video.path)
            if seconds is not None and cache is not None:
                cache.put(video.key, video.mtime, video.size, seconds)
        if seconds is None:
            missing += 1
        else:
            known += 1
            total_seconds += seconds
            ch = stats.channels.get(video.channel)
            if ch is not None:
                ch.seconds = (ch.seconds or 0.0) + seconds
                ch.timed += 1
        if progress is not None and (index % 25 == 0 or index == total):
            progress(index, total)
        if cache is not None and cache.dirty and index % 200 == 0:
            cache.save()
    else:
        if cache is not None:
            cache.keep_only(seen_keys)
    if cache is not None:
        cache.save()
    stats.seconds_total = total_seconds if known else None
    stats.timed_files = known
    return known, missing


# ------------------------------------------------------------------ summary
def fill_estimate(stats: LibraryStats) -> tuple[float, float] | None:
    """(bytes per day over the last 30 days, days until the drive is full), or None when the
    numbers would not mean anything (little history, no new files, no disk figure, > 10 years)."""
    if not stats.disk or stats.bytes_30d <= 0 or stats.oldest is None:
        return None
    history_days = (stats.scanned_at - stats.oldest) / 86400
    if history_days < 7:
        return None
    span = min(30.0, history_days)
    rate = stats.bytes_30d / span
    if rate <= 0:
        return None
    days = stats.disk[2] / rate
    if days > FILL_MAX_DAYS:
        return None
    return rate, days


def _day(ts: float | None) -> str:
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d") if ts else "-"


def summary_lines(stats: LibraryStats, top: int = 3) -> list[str]:
    """The headline text, one bullet per line (plain text, safe to paste anywhere)."""
    lines: list[str] = []
    rec_b, rec_f = stats.recordings_bytes, stats.recordings_files
    done_b, done_f = stats.cat("processed")
    raw_b, raw_f = stats.cat("raw")
    head = f"{fmt_size(rec_b)} of recordings in {rec_f:,} file{'s' if rec_f != 1 else ''}"
    if raw_f:
        head += f" ({fmt_size(done_b)} finished, {fmt_size(raw_b)} raw or in progress)"
    if stats.disk:
        head += f" on a drive with {fmt_size(stats.disk[2])} free of {fmt_size(stats.disk[0])}"
    lines.append(head + ".")
    platforms = stats.ranked_platforms()
    if platforms and rec_b > 0:
        parts = [f"{platform_label(p)} {fmt_size(b)} ({b * 100 / rec_b:.0f}%)" for p, b, _f in platforms[:6]]
        if len(platforms) > 6:
            parts.append(f"{len(platforms) - 6} more")
        lines.append("By site: " + ", ".join(parts) + ".")
    ranked = [c for c in stats.ranked_channels() if c.recordings_bytes > 0]
    if ranked:
        lines.append("Biggest channels: " + ", ".join(
            f"{c.name} ({platform_label(c.platform)}) {fmt_size(c.recordings_bytes)}" for c in ranked[:top]) + ".")
    if stats.oldest and stats.newest:
        lines.append(f"Recordings from {_day(stats.oldest)} to {_day(stats.newest)}.")
    if stats.seconds_total is not None:
        note = f" (length known for {stats.timed_files:,} of {len(stats.videos):,} finished files)" \
            if stats.timed_files != len(stats.videos) else ""
        lines.append(f"Recorded time: {fmt_hours(stats.seconds_total)}{note}.")
    months = sorted(stats.months.items(), key=lambda kv: -kv[1][0])
    if months:
        m, (b, f) = months[0]
        lines.append(f"Busiest month: {m} ({fmt_size(b)}, {f:,} recordings).")
    extras = []
    pend_b, pend_f = stats.cat("pending")
    if pend_f:
        extras.append(f"{fmt_size(pend_b)} waiting in {PENDING_DELETION_FOLDER} ({pend_f:,} files)")
    clip_b, clip_f = stats.cat("clips")
    shot_b, shot_f = stats.cat("screenshots")
    if clip_f or shot_f:
        extras.append(f"clips and screenshots {fmt_size(clip_b + shot_b)} ({clip_f:,} clips, {shot_f:,} screenshots)")
    other_b, other_f = stats.cat("other")
    if other_f:
        extras.append(f"other folders {fmt_size(other_b)} ({other_f:,} files)")
    if extras:
        lines.append("Also: " + "; ".join(extras) + ".")
    est = fill_estimate(stats)
    if est:
        rate, days = est
        lines.append(f"Roughly {fmt_size(stats.bytes_30d)} was added in the last 30 days ({fmt_size(rate)} a day); "
                     f"at that pace the drive fills in about {days:,.0f} days.")
    notes = []
    missing = [name for name, ok in stats.roots_found.items()
               if not ok and (name in ("Recordings folder", "Processed") or name.startswith("Other folder"))]
    if missing:
        notes.append("not found: " + ", ".join(missing))
    if stats.unreadable_dirs:
        notes.append(f"{stats.unreadable_dirs} folder(s) could not be read")
    if stats.symlinks_skipped:
        notes.append(f"{stats.symlinks_skipped} link(s) skipped")
    if stats.cancelled:
        notes.append("scan was cancelled: numbers are partial")
    if notes:
        lines.append("Notes: " + "; ".join(notes) + ".")
    return lines


def summary_text(stats: LibraryStats) -> str:
    when = datetime.datetime.fromtimestamp(stats.scanned_at).strftime("%Y-%m-%d %H:%M")
    return f"Multi-Stream Recorder library ({when})\n" + "\n".join("- " + l for l in summary_lines(stats))


def to_dict(stats: LibraryStats, max_channels: int = 200) -> dict:
    """JSON-able snapshot for `--json --library`. Channel names and sizes only: no absolute paths."""
    est = fill_estimate(stats)
    total = stats.recordings_bytes or 1
    return {
        "scanned_at": datetime.datetime.fromtimestamp(stats.scanned_at).isoformat(timespec="seconds"),
        "scan_seconds": round(stats.elapsed, 2),
        "complete": not stats.cancelled,
        "folders_found": dict(stats.roots_found),
        "totals": {name: {"bytes": b, "files": f} for name, (b, f) in sorted(stats.categories.items())},
        "recordings": {"bytes": stats.recordings_bytes, "files": stats.recordings_files},
        "by_platform": [{"platform": p, "bytes": b, "files": f, "share": round(b / total, 4)}
                        for p, b, f in stats.ranked_platforms()],
        "channels": [{"platform": c.platform, "name": c.name, "recordings_bytes": c.recordings_bytes,
                      "recordings_files": c.rec_files + c.raw_files, "pending_bytes": c.pending_bytes,
                      "clips_bytes": c.clip_bytes + c.shot_bytes, "in_roster": c.in_roster,
                      "last": _day(c.last) if c.last else None}
                     for c in stats.ranked_channels()[:max_channels]],
        "channel_count": len(stats.channels),
        "by_month": [{"month": m, "bytes": b, "files": f} for m, (b, f) in sorted(stats.months.items())],
        "oldest": _day(stats.oldest) if stats.oldest else None,
        "newest": _day(stats.newest) if stats.newest else None,
        "disk": ({"total": stats.disk[0], "used": stats.disk[1], "free": stats.disk[2]} if stats.disk else None),
        "added_last_30_days": stats.bytes_30d,
        "fills_in_days": round(est[1]) if est else None,
        "recorded_hours": round(stats.seconds_total / 3600, 1) if stats.seconds_total is not None else None,
        "notes": {"partial_files": stats.partial_files, "unreadable_folders": stats.unreadable_dirs,
                  "links_skipped": stats.symlinks_skipped},
    }


# ------------------------------------------------------------------ background job
class ScanJob(threading.Thread):
    """Runs a scan (and, if asked, the hours count) off the UI thread.

    Messages for the UI go through `out` (a queue.Queue):
      ("progress", files, bytes, label) ("done", stats)
      ("hours_progress", done, total)   ("hours_done", stats, known, missing)
      ("error", text)
    The UI polls `out`; this thread never touches Tk."""

    def __init__(self, roots: Roots, out, *, roster: Iterable[str] | None = None,
                 cache_path: str | None = None, ffprobe: str | None = None, hours_after: bool = False,
                 hours_for: LibraryStats | None = None, probe_missing: bool = True):
        super().__init__(daemon=True, name="LibraryScan")
        self.roots, self.out = roots, out
        self.roster = list(roster) if roster is not None else None
        self.cache_path, self.ffprobe = cache_path, ffprobe
        self.hours_after, self.hours_for, self.probe_missing = hours_after, hours_for, probe_missing
        self.cancel_event = threading.Event()

    def cancel(self) -> None:
        self.cancel_event.set()

    def run(self) -> None:
        try:
            stats = self.hours_for
            if stats is None:
                stats = scan_library(self.roots, lambda f, b, l: self.out.put(("progress", f, b, l)),
                                     self.cancel_event)
                mark_roster(stats, self.roster)
                self.out.put(("done", stats))
            if (self.hours_after or self.hours_for is not None) and not self.cancel_event.is_set():
                cache = DurationCache(self.cache_path)
                probe = None
                if self.probe_missing and self.ffprobe:
                    probe = lambda path: ffprobe_seconds(self.ffprobe, path)  # noqa: E731
                known, missing = count_hours(stats, cache, probe,
                                             lambda d, t: self.out.put(("hours_progress", d, t)),
                                             self.cancel_event)
                self.out.put(("hours_done", stats, known, missing))
        except Exception as exc:  # a scan problem must never take the app down
            self.out.put(("error", f"{type(exc).__name__}: {exc}"))
