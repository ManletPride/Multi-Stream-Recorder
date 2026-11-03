"""Recordings archive, Tk-free: one channel's recordings with date, length, title and size
(from the .meta.json MSR writes; no ffprobe) and the clips cut from each (clip .meta.json, else
the Clip Editor's name pattern). Used by Library > Recordings (msr/library_browser.py) and the
control ``recordings_list`` / ``recordings_clips`` commands.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import threading
from dataclasses import dataclass, field

from msr import clip_core as core
from msr import library as lib

LIST_CATEGORIES = ("processed", "recorded")  # finished files, then raw .ts still waiting
CLIP_EXT = (".mp4", ".ts", ".mkv", ".mov", ".m4v", ".flv", ".webm")
# <recording stem with a date stamp>_cut|trim|vertical|burned|clip[_...]
_DERIVED_RE = re.compile(r"^(?P<stem>.+?\d{8}_\d{4,6})_(?P<kind>cut|trim|vertical|burned|clip)(?:_.*)?$", re.I)
_STAMP_RE = re.compile(r"(?<!\d)(20\d\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])[_\-T ]?([01]\d|2[0-3])([0-5]\d)(?:[0-5]\d)?(?!\d)")


@dataclass
class Recording:
    path: str
    name: str
    when: float          # start time: meta recording_started, else name stamp, else mtime
    seconds: float | None
    title: str
    size: int
    raw: bool            # still a raw .ts in Recorded/
    clips: list = field(default_factory=list)   # ClipRef cut from this recording
    related: list = field(default_factory=list)  # clip files in the same folders, folded under this row
    orphan: bool = False  # a clip file whose source recording is not listed


@dataclass
class ClipRef:
    path: str
    name: str
    size: int
    mtime: float
    stem: str            # source stem as written into the clip name
    source_name: str     # source file name from the sidecar ('' for name-only matches)
    t_in: float
    t_out: float
    guessed: bool        # times from the file name (older clip, no sidecar)
    label: str = ""      # set for folded clip files without times (e.g. "trim file")


# ------------------------------------------------------------------ Tk-free helpers
def fmt_length(seconds: float | None) -> str:
    if seconds is None:
        return ""
    s = int(round(seconds))
    return f"{s // 3600}:{s % 3600 // 60:02d}:{s % 60:02d}"


def fmt_when(ts: float) -> str:
    try:
        return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except (ValueError, OverflowError, OSError):
        return "?"


def stamp_from_name(name: str) -> float | None:
    """YYYYMMDD_HHMM[SS] in a file name (MSR's default) as a local timestamp; day only as fallback."""
    m = _STAMP_RE.search(name)
    if m:
        try:
            return datetime.datetime(*(int(g) for g in m.groups())).timestamp()
        except (ValueError, OverflowError, OSError):
            pass
    return lib.date_from_name(name)


def read_meta(video_path: str) -> dict:
    try:
        with open(video_path.rsplit(".", 1)[0] + lib.SIDECAR_SUFFIX, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _meta_start(meta: dict) -> float | None:
    value = meta.get("recording_started")
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d_%H%M%S", "%Y%m%d_%H%M"):
        try:
            return datetime.datetime.strptime(text, fmt).timestamp()
        except ValueError:
            pass
    try:
        return datetime.datetime.fromisoformat(text).timestamp()
    except (ValueError, OverflowError, OSError):
        return None


def make_recording(path: str, size: int, mtime: float, raw: bool) -> Recording:
    name = os.path.basename(path)
    meta = read_meta(path) if not raw else {}
    try:
        seconds = float(meta["duration_seconds"])
        seconds = seconds if seconds >= 0 else None
    except (KeyError, ValueError, TypeError):
        seconds = None
    title = meta.get("stream_title") or meta.get("title") or ""
    when = _meta_start(meta) or stamp_from_name(name) or mtime
    return Recording(path, name, when, seconds, str(title).strip(), size, raw)


def list_recordings(folders: dict, cancel: threading.Event | None = None) -> list[Recording]:
    """Video files under a channel's Processed/Recorded folders (no symlinks, no partial files)."""
    out: list[Recording] = []
    for category in LIST_CATEGORIES:
        top = folders.get(category)
        if not top or not os.path.isdir(top):
            continue
        stack = [top]
        while stack:
            if cancel is not None and cancel.is_set():
                return out
            folder = stack.pop()
            try:
                entries = list(os.scandir(folder))
            except OSError:
                continue
            for e in entries:
                try:
                    if e.is_symlink():
                        continue
                    if e.is_dir(follow_symlinks=False):
                        stack.append(e.path)
                        continue
                    low = e.name.lower()
                    if low.endswith(lib.PARTIAL_SUFFIXES) or os.path.splitext(low)[1] not in lib.VIDEO_EXT:
                        continue
                    st = e.stat(follow_symlinks=False)
                except OSError:
                    continue
                out.append(make_recording(e.path, st.st_size, st.st_mtime, category == "recorded"))
    return out


def list_clips(folder: str | None, cancel: threading.Event | None = None) -> list[ClipRef]:
    """Clips in a channel's clips folder that name their source (sidecar or name pattern)."""
    out: list[ClipRef] = []
    if not folder or not os.path.isdir(folder):
        return out
    stack = [folder]
    while stack:
        if cancel is not None and cancel.is_set():
            break
        try:
            entries = list(os.scandir(stack.pop()))
        except OSError:
            continue
        for e in entries:
            try:
                if e.is_symlink():
                    continue
                if e.is_dir(follow_symlinks=False):
                    stack.append(e.path)
                    continue
                low = e.name.lower()
                if low.endswith(lib.PARTIAL_SUFFIXES) or ".part." in low or os.path.splitext(low)[1] not in lib.VIDEO_EXT:
                    continue
                link = core.read_clip_link(e.path)
                if link is None:
                    continue
                st = e.stat(follow_symlinks=False)
            except OSError:
                continue
            out.append(ClipRef(e.path, e.name, st.st_size, st.st_mtime, link["stem"], link["source_name"],
                               link["in"], link["out"], link["guessed"]))
    return out


def link_clips(recs: list[Recording], clips: list[ClipRef]) -> list[ClipRef]:
    """Attach clips to recordings: same source file name first, then the same stem (a live .ts
    that became an .mp4 keeps its stem). Returns the clips that matched nothing."""
    by_name = {r.name.lower(): r for r in recs}
    by_stem: dict[str, Recording] = {}
    for r in sorted(recs, key=lambda r: r.raw):   # finished .mp4 wins over a leftover .ts
        by_stem.setdefault(core.safe_stem(r.name).lower(), r)
    loose = []
    for c in clips:
        rec = by_name.get(c.source_name.lower()) if c.source_name else None
        rec = rec or by_stem.get(c.stem.lower())
        if rec is None:
            loose.append(c)
        else:
            rec.clips.append(c)
    for r in recs:
        r.clips.sort(key=lambda c: c.t_in)
    return loose


def _clip_source(r: Recording):
    """(source name, source stem, link or None) when this listed file is a clip, else None."""
    link = core.read_clip_link(r.path)
    if link is not None:
        return link["source_name"], link["stem"], link
    m = _DERIVED_RE.match(os.path.splitext(r.name)[0])
    return ("", m.group("stem"), m.group("kind").lower()) if m else None


def fold_clip_files(recs: list[Recording]) -> list[Recording]:
    """Move clip files found among the recordings under their source's row (Recording.related).
    Returns the top-level rows; clips whose source is not listed stay there with orphan=True."""
    found = [(r, _clip_source(r)) for r in recs]
    sources = [r for r, src in found if src is None]
    by_name = {r.name.lower(): r for r in sources}
    by_stem: dict[str, Recording] = {}
    for r in sorted(sources, key=lambda r: r.raw):
        by_stem.setdefault(core.safe_stem(r.name).lower(), r)
        by_stem.setdefault(os.path.splitext(r.name)[0].lower(), r)
    top = []
    for r, src in found:
        if src is None:
            top.append(r)
            continue
        source_name, stem, link = src
        parent = (by_name.get(source_name.lower()) if source_name else None) or by_stem.get(stem.lower())
        if parent is None:
            r.orphan = True
            top.append(r)
            continue
        if any(os.path.normcase(c.path) == os.path.normcase(r.path) for c in parent.clips):
            continue  # already linked from the clips folder
        if isinstance(link, dict):
            ref = ClipRef(r.path, r.name, r.size, r.when, stem, source_name, link["in"], link["out"], link["guessed"])
        else:
            ref = ClipRef(r.path, r.name, r.size, r.when, stem, "", 0.0, 0.0, False, label=f"{link} file")
        parent.related.append(ref)
    for r in sources:
        r.related.sort(key=lambda c: c.name.lower())
    return top


# (id, heading, width, anchor, sort key, cell text)
COLUMNS = (
    ("date", "Date", 120, "w", lambda r: r.when, lambda r: fmt_when(r.when)),
    ("length", "Length", 70, "e", lambda r: -1 if r.seconds is None else r.seconds, lambda r: fmt_length(r.seconds)),
    ("title", "Stream title", 330, "w", lambda r: r.title.lower(), lambda r: r.title or ("\u26a0 clip, source missing" if r.orphan else "-")),
    ("clips", "Clips", 45, "e", lambda r: len(r.clips), lambda r: len(r.clips) or ""),
    ("file", "File", 250, "w", lambda r: r.name.lower(), lambda r: r.name + ("  (raw)" if r.raw else "")),
    ("size", "Size", 80, "e", lambda r: r.size, lambda r: lib.fmt_size(r.size)),
)


def clip_cells(c: ClipRef) -> list:
    """A clip's child row, in COLUMNS order."""
    if c.label:
        return [fmt_when(c.mtime), "", c.label, "", c.name, lib.fmt_size(c.size)]
    span = f"clip {fmt_length(c.t_in)} – {fmt_length(c.t_out)}" + ("  (times from name)" if c.guessed else "")
    return [fmt_when(c.mtime), fmt_length(max(0.0, c.t_out - c.t_in)), span, "", c.name, lib.fmt_size(c.size)]
_COLUMN = {c[0]: c for c in COLUMNS}


def filter_sort(recs, text: str = "", column: str = "date", descending: bool = True) -> list[Recording]:
    """Recordings whose title or file name contains every word of `text`, sorted by a column
    (ties: newest first)."""
    words = (text or "").lower().split()
    rows = [r for r in recs if all(w in (r.title + " " + r.name).lower() for w in words)]
    rows.sort(key=lambda r: r.when, reverse=True)
    rows.sort(key=_COLUMN.get(column, COLUMNS[0])[4], reverse=descending)
    return rows


def channel_label(ch: lib.ChannelStat) -> str:
    return f"{ch.name}  ({lib.platform_label(ch.platform)})"


def match_channels(labels, text: str) -> list[str]:
    """Jump-box suggestions: names starting with the text first, then ones containing it."""
    needle = (text or "").strip().lower()
    if not needle:
        return list(labels)
    starts = [l for l in labels if l.lower().startswith(needle)]
    return starts + [l for l in labels if needle in l.lower() and l not in starts]


def channel_listing(paths: dict, cancel: threading.Event | None = None):
    """(recordings with their clips, loose clips) for one channel's folders
    (``ChannelStat.paths``: processed / recorded / clips), as the Recordings tab shows them."""
    folders = {k: v for k, v in paths.items() if k in LIST_CATEGORIES}
    recs = fold_clip_files(list_recordings(folders, cancel))
    loose = link_clips(recs, list_clips(paths.get("clips"), cancel))
    return recs, loose


def _iso(ts):
    try:
        return datetime.datetime.fromtimestamp(ts).isoformat(timespec="seconds")
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def clip_dict(c: ClipRef) -> dict:
    out = {"path": c.path, "file": c.name, "size": c.size, "saved_at": _iso(c.mtime)}
    if c.label:
        out["kind"] = c.label
    else:
        out.update({"in_seconds": round(c.t_in, 3), "out_seconds": round(c.t_out, 3),
                    "length_seconds": round(max(0.0, c.t_out - c.t_in), 3), "times_from_name": c.guessed})
    return out


def recording_dict(r: Recording, clips=True) -> dict:
    out = {"path": r.path, "file": r.name, "date": _iso(r.when), "length_seconds": r.seconds,
           "length": fmt_length(r.seconds), "title": r.title, "size": r.size, "raw": r.raw,
           "clip_count": len(r.clips) + len(r.related)}
    if r.orphan:
        out["orphan_clip"] = True
    if clips:
        out["clips"] = [clip_dict(c) for c in r.clips + r.related]
    return out
