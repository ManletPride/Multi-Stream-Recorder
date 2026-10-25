"""Library actions: delete, move to PendingDeletion, restore, empty folders. No tkinter here.

Everything that can remove a file lives in this module so it can be tested with temporary
folders and reviewed in one place. The flow is always the same:

    candidates (FileRec from a scan)  ->  build_plan(guard, ...)  ->  Plan (the dry run: what
    would happen, what is refused and why)  ->  the user confirms  ->  execute(plan, guard, deleter)

* The Guard refuses anything outside the Processed / Recorded / PendingDeletion / Clips folders
  (real paths are compared, links and junctions are refused, not only at the file but all the
  way down), anything that is not a recording / clip / screenshot / sidecar, files modified in
  the last few minutes, files a recorder is writing, and every file of a channel that is
  recording right now. It is asked again right before each file is touched.
* The default action is "pending": move into MSR's own PendingDeletion folder, which the
  recorder empties on its own schedule (Cleanup/auto_purge_days). It is reversible
  (restore_*), instant on the same drive and needs no Recycle Bin space.
* Every action is written to an audit log (JSON lines) in the data folder.
"""

from __future__ import annotations

import datetime
import json
import os
import shutil
import stat
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Iterable

from msr import library as lib
from msr.util import PENDING_DELETION_FOLDER, channel_key_to_dirs

RECENT_SECONDS = 600           # a file touched this recently may still be in use
REPARSE_POINT = 0x400

ACTION_PENDING = "pending"     # move to PendingDeletion (default, reversible)
ACTION_TRASH = "trash"         # Windows Recycle Bin
ACTION_PERMANENT = "permanent"
ACTION_RESTORE = "restore"     # PendingDeletion -> where it came from
ACTION_RMDIR = "rmdir"         # remove empty folders
ACTION_LABELS = {
    ACTION_PENDING: "Move to PendingDeletion",
    ACTION_TRASH: "Move to the Recycle Bin",
    ACTION_PERMANENT: "Delete permanently",
    ACTION_RESTORE: "Restore from PendingDeletion",
    ACTION_RMDIR: "Remove empty folders",
}

# What the clean-up tools may touch. Anything else in a channel folder is left alone and listed.
DELETABLE_KINDS = frozenset({"recording", "raw", "partial", "clip", "screenshot", "sidecar", "media"})
KIND_LABELS = {"recording": "Recording", "raw": "Raw .ts", "partial": "Partial", "clip": "Clip",
               "screenshot": "Screenshot", "sidecar": "Info file", "media": "In PendingDeletion", "other": "Other"}


# ------------------------------------------------------------------ paths
def _norm(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


def inside(path: str, top: str) -> bool:
    """path is strictly below top (both compared case-insensitively on Windows)."""
    p, t = _norm(path), _norm(top)
    try:
        return p != t and os.path.commonpath([p, t]) == t
    except ValueError:  # different drives
        return False


def is_link_path(path: str) -> bool:
    """A symlink or a Windows junction / reparse point (lstat: never follows)."""
    try:
        st = os.lstat(path)
    except OSError:
        return True
    return stat.S_ISLNK(st.st_mode) or bool(getattr(st, "st_file_attributes", 0) & REPARSE_POINT)


def unique_path(folder: str, name: str) -> str:
    """folder/name, or folder/name (2) etc. when taken."""
    candidate = os.path.join(folder, name)
    if not os.path.lexists(candidate):
        return candidate
    low = name.lower()
    if low.endswith(lib.SIDECAR_SUFFIX):
        stem, ext = name[:-len(lib.SIDECAR_SUFFIX)], name[-len(lib.SIDECAR_SUFFIX):]
    else:
        stem, ext = os.path.splitext(name)
    n = 2
    while True:
        candidate = os.path.join(folder, f"{stem} ({n}){ext}")
        if not os.path.lexists(candidate):
            return candidate
        n += 1


@dataclass
class Library:
    """The real folders the tools may work in, resolved once."""
    tops: dict[str, str]          # category -> absolute folder (links refused)
    pending_top: str              # where PendingDeletion is (or will be created)
    streams_dir: str

    def category_of(self, path: str) -> str | None:
        for category, top in self.tops.items():
            if inside(path, top):
                return category
        return None


def resolve_library(roots: lib.Roots) -> Library:
    tops: dict[str, str] = {}
    streams = (roots.streams_dir or "").strip()
    if streams and os.path.isdir(streams) and not is_link_path(streams):
        for category, folder in lib._top_level_map(streams).items():
            if not is_link_path(folder):
                tops[category] = os.path.abspath(folder)
    clips = (roots.clips_dir or "").strip()
    if not clips and streams and os.path.isdir(streams):
        named = next((c for c in lib._child_dirs(streams) if c.lower() == "clips"), "")
        clips = os.path.join(streams, named) if named else ""
    if clips and os.path.isdir(clips) and not is_link_path(clips):
        tops["clips"] = os.path.abspath(clips)
    for category, top in list(tops.items()):  # never a drive root or the streams folder itself
        if os.path.dirname(top) == top or (streams and _norm(top) == _norm(streams)):
            del tops[category]
    pending = tops.get("pending") or (os.path.join(os.path.abspath(streams), PENDING_DELETION_FOLDER) if streams else "")
    return Library(tops, pending, os.path.abspath(streams) if streams else "")


# ------------------------------------------------------------------ guard
def busy_from_status(status_dict: dict | None) -> tuple[set, set]:
    """(channels, files) a recorder is working on, from the recorder's status_dict:
    channels are (platform, folder) lower-case pairs; files are paths it reports writing.
    Recording, starting, processing/remuxing and 'reconnecting' all count; so does anything
    unknown that is not clearly idle."""
    idle = {"", "offline", "stopped", "error", "completed", "remux failed", "checking...", "disabled", "off"}
    channels: set = set()
    files: set = set()
    for key, st in (status_dict or {}).items():
        try:
            status = str(st.get("status", "")).strip().lower()
            detail = str(st.get("detail", "")).lower()
        except AttributeError:
            continue
        busy = status not in idle or (status == "offline" and "reconnecting" in detail)
        if not busy:
            continue
        platform, folder = channel_key_to_dirs(key)
        channels.add((str(platform).lower(), str(folder).replace("\\", "/").lower()))
        for field_name in ("raw_file", "file"):
            value = st.get(field_name)
            if isinstance(value, str) and value:
                files.add(_norm(value))
    return channels, files


class Guard:
    """Decides whether one file or folder may be touched. check() returns None when it may,
    else a sentence saying why not."""

    def __init__(self, roots: lib.Roots, *, data_dir: str = "",
                 busy: Callable[[], tuple[set, set]] | None = None,
                 recent_seconds: int = RECENT_SECONDS, clock: Callable[[], float] = time.time):
        self.library = resolve_library(roots)
        self.data_dir = os.path.abspath(data_dir) if data_dir else ""
        self.busy = busy
        self.recent_seconds = recent_seconds
        self.clock = clock
        self._busy_channels: set = set()
        self._busy_files: set = set()
        self.refresh_busy()

    def refresh_busy(self) -> None:
        self._unknown_busy = False
        try:
            self._busy_channels, self._busy_files = (self.busy() if self.busy else (set(), set()))
        except Exception:  # if the recorder state cannot be read, stay on the safe side
            self._busy_channels, self._busy_files = {("?", "?")}, set()
            self._unknown_busy = True

    _unknown_busy = False

    @property
    def tops(self) -> dict[str, str]:
        return self.library.tops

    def channel_busy(self, platform: str, channel: str) -> bool:
        if self._unknown_busy:
            return True
        return (platform.lower(), channel.replace("\\", "/").lower()) in self._busy_channels

    def _location(self, path: str):
        """(category, top, relative parts) for a path below an allowed top, else None."""
        for category, top in self.tops.items():
            if inside(path, top):
                rel = os.path.relpath(os.path.abspath(path), top).replace("\\", "/").split("/")
                return category, top, rel
        return None

    def _problem_in_path(self, path: str, top: str, rel: list[str], last_is_dir: bool) -> str | None:
        current = top
        for part in rel[:-1] if not last_is_dir else rel:
            current = os.path.join(current, part)
            if is_link_path(current):
                return "A folder on the way is a link or junction, so it may lead somewhere else."
        real, real_top = os.path.realpath(path), os.path.realpath(top)
        if not inside(real, real_top):
            return "It resolves to a place outside the library folders."
        if self.data_dir and (inside(path, self.data_dir) or inside(real, self.data_dir)):
            return "It is inside MSR's data folder."
        return None

    def check(self, path: str, size: int | None = None, mtime: float | None = None,
              restore: bool = False) -> str | None:
        located = self._location(path)
        if located is None:
            return "It is not inside the Processed, Recorded, PendingDeletion or Clips folders."
        category, top, rel = located
        if len(rel) < 2:
            return "It sits directly in a top folder, not in a channel folder."
        problem = self._problem_in_path(path, top, rel, False)
        if problem:
            return problem
        try:
            st = os.lstat(path)
        except OSError as exc:
            return f"It cannot be read ({exc.strerror or exc})."
        if stat.S_ISLNK(st.st_mode) or getattr(st, "st_file_attributes", 0) & REPARSE_POINT:
            return "It is a link, not a file."
        if not stat.S_ISREG(st.st_mode):
            return "It is not a regular file."
        kind = lib.classify(rel[-1], category)
        if kind not in DELETABLE_KINDS:
            return "This type of file is not a recording, clip or screenshot, so it is left alone."
        if size is not None and st.st_size != size:
            return "It changed size since the preview."
        if mtime is not None and int(st.st_mtime) != int(mtime):
            return "It was modified since the preview."
        if _norm(path) in self._busy_files:
            return "A recorder is writing to it right now."
        if self.channel_busy(rel[0], "/".join(rel[1:-1])):
            return "This channel is recording right now. Stop it first."
        if not restore and category != "pending" and self.clock() - st.st_mtime < self.recent_seconds:
            return f"It was modified in the last {self.recent_seconds // 60} minutes (it may be in use)."
        return None

    def check_dir(self, path: str) -> str | None:
        located = self._location(path)
        if located is None:
            return "It is not inside the library folders."
        category, top, rel = located
        problem = self._problem_in_path(path, top, rel, True)
        if problem:
            return problem
        if is_link_path(path) or not os.path.isdir(path):
            return "It is not a plain folder."
        try:
            if os.listdir(path):
                return "It is not empty."
        except OSError as exc:
            return f"It cannot be read ({exc.strerror or exc})."
        if len(rel) >= 2 and self.channel_busy(rel[0], "/".join(rel[1:])):
            return "This channel is recording right now."
        if len(rel) == 1 and any(c[0] == rel[0].lower() for c in self._busy_channels):
            return "A channel of this site is recording right now."
        return None

    def check_destination(self, path: str) -> str | None:
        """Where a restored file would go: inside a library folder, no links on the way, free."""
        located = self._location(path)
        if located is None:
            return "The original place is outside the library folders."
        category, top, rel = located
        if category == "pending":
            return "The original place is the PendingDeletion folder itself."
        if len(rel) < 2:
            return "The original place is not a channel folder."
        current = top
        for part in rel[:-1]:
            current = os.path.join(current, part)
            if os.path.lexists(current) and is_link_path(current):
                return "A folder on the way is a link or junction."
        if self.channel_busy(rel[0], "/".join(rel[1:-1])):
            return "This channel is recording right now."
        if os.path.lexists(path):
            return "Something with that name is already there."
        return None


# ------------------------------------------------------------------ plan
@dataclass
class PlanItem:
    path: str
    size: int
    mtime: float
    category: str
    platform: str
    channel: str
    kind: str
    when: float
    role: str = "main"            # main | sidecar | folder
    rel: tuple = ()               # path below its top folder, as parts

    @property
    def label(self) -> str:
        return f"{self.platform}/{self.channel}"


@dataclass
class Plan:
    action: str
    items: list[PlanItem] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)       # (path, reason)
    blocked: dict[str, str] = field(default_factory=dict)              # channel label -> reason

    @property
    def files(self) -> int:
        return sum(1 for i in self.items if i.role != "folder")

    @property
    def folders(self) -> int:
        return sum(1 for i in self.items if i.role == "folder")

    @property
    def bytes(self) -> int:
        return sum(i.size for i in self.items)

    @property
    def channels(self) -> list[str]:
        return sorted({i.label for i in self.items}, key=str.lower)

    @property
    def span(self) -> tuple[float | None, float | None]:
        dates = [i.when for i in self.items if i.role == "main" and i.when]
        return (min(dates), max(dates)) if dates else (None, None)

    @property
    def ok(self) -> bool:
        return bool(self.items)

    @property
    def confirm_word(self) -> str:
        """What must be typed to confirm a permanent delete: the channel's name for a single
        channel, otherwise DELETE."""
        names = {i.channel for i in self.items}
        if len(names) == 1:
            return next(iter(names))
        return "DELETE"

    def by_kind(self) -> dict[str, tuple[int, int]]:
        out: dict[str, list[int]] = {}
        for item in self.items:
            row = out.setdefault(item.kind, [0, 0])
            row[0] += 1
            row[1] += item.size
        return {k: (v[0], v[1]) for k, v in out.items()}

    def summary_lines(self) -> list[str]:
        if self.action == ACTION_RMDIR:
            return [f"{self.folders} empty folder(s) would be removed."]
        first, last = self.span
        lines = [f"{self.files:,} file(s), {lib.fmt_size(self.bytes)}, from {len(self.channels)} channel(s): "
                 + ", ".join(self.channels[:5]) + (f" and {len(self.channels) - 5} more" if len(self.channels) > 5 else "")]
        if first is not None:
            lines.append(f"Recorded between {lib._day(first)} and {lib._day(last)}.")
        kinds = ", ".join(f"{n:,} {KIND_LABELS.get(k, k).lower()}" for k, (n, _b) in sorted(self.by_kind().items()))
        if kinds:
            lines.append("Of which: " + kinds + ".")
        return lines


def candidate_plan_item(rec, role: str = "main") -> PlanItem:
    return PlanItem(rec.path, rec.size, rec.mtime, rec.category, rec.platform, rec.channel, rec.kind,
                    rec.when, role, ())


def build_plan(guard: Guard, recs: Iterable, action: str, *, include_sidecars: bool = True) -> Plan:
    """The dry run: check every candidate (a FileRec or anything with the same fields) and say
    what would happen. Nothing is touched."""
    plan = Plan(action)
    guard.refresh_busy()
    seen: set[str] = set()
    recs = list(recs)
    listed = {_norm(r.path) for r in recs}
    for rec in recs:
        key = _norm(rec.path)
        if key in seen:
            continue
        seen.add(key)
        reason = None
        if action == ACTION_RESTORE and rec.category != "pending":
            reason = "It is not in PendingDeletion."
        elif action in (ACTION_PENDING,) and rec.category == "pending":
            reason = "It is already in PendingDeletion."
        elif rec.kind not in DELETABLE_KINDS:
            reason = "This type of file is left alone."
        if reason is None:
            reason = guard.check(rec.path, rec.size, rec.mtime, restore=(action == ACTION_RESTORE))
        if reason:
            plan.skipped.append((rec.path, reason))
            if "recording right now" in reason or "writing" in reason:
                plan.blocked[f"{rec.platform}/{rec.channel}"] = reason
            continue
        located = guard._location(rec.path)
        item = candidate_plan_item(rec)
        item.rel = tuple(located[2]) if located else ()
        plan.items.append(item)
        if include_sidecars and rec.kind != "sidecar":
            side = os.path.splitext(rec.path)[0] + lib.SIDECAR_SUFFIX
            if _norm(side) not in listed and _norm(side) not in seen and os.path.isfile(side):
                try:
                    st = os.lstat(side)
                except OSError:
                    continue
                problem = guard.check(side, st.st_size, st.st_mtime, restore=(action == ACTION_RESTORE))
                if problem is None:
                    seen.add(_norm(side))
                    sc = PlanItem(side, st.st_size, st.st_mtime, rec.category, rec.platform, rec.channel,
                                  "sidecar", rec.when, "sidecar", item.rel[:-1] + (os.path.basename(side),))
                    plan.items.append(sc)
    return plan


def plan_empty_dirs(guard: Guard, dirs: Iterable[str]) -> Plan:
    plan = Plan(ACTION_RMDIR)
    guard.refresh_busy()
    for path in dirs:
        reason = guard.check_dir(path)
        if reason:
            plan.skipped.append((path, reason))
            continue
        category, top, rel = guard._location(path)
        platform = rel[0] if rel else ""
        plan.items.append(PlanItem(path, 0, 0.0, category, platform.lower(), "/".join(rel[1:]) or "(site folder)",
                                   "folder", 0.0, "folder", tuple(rel)))
    return plan


# ------------------------------------------------------------------ selecting what to act on
def filter_recs(recs: Iterable, *, kinds: Iterable[str] | None = None, text: str = "",
                month: str | None = None, older_than_days: float | None = None,
                date_from: float | None = None, date_to: float | None = None,
                keep_newest_files: int | None = None, keep_newest_bytes: int | None = None,
                now: float | None = None) -> list:
    """Pick files. Info files (sidecars) are never picked on their own: they follow their video.
    keep_newest_* keeps the newest N files / bytes (by recording date) and returns the rest."""
    now = time.time() if now is None else now
    wanted = set(kinds) if kinds is not None else None
    needle = (text or "").strip().lower()
    rows = []
    for rec in recs:
        if rec.kind == "sidecar" and not (wanted and "sidecar" in wanted):
            continue
        if wanted is not None and rec.kind not in wanted:
            continue
        if needle and needle not in os.path.basename(rec.path).lower() and needle not in rec.channel.lower():
            continue
        if month and lib.month_key(rec.when) != month:
            continue
        if date_from is not None and rec.when < date_from:
            continue
        if date_to is not None and rec.when >= date_to + 86400:
            continue
        if older_than_days is not None and now - rec.when < older_than_days * 86400:
            continue
        rows.append(rec)
    if keep_newest_files is not None or keep_newest_bytes is not None:
        rows.sort(key=lambda r: (-r.when, r.path))
        kept_files = kept_bytes = 0
        rest = []
        for rec in rows:
            keep = False
            if keep_newest_files is not None and kept_files < keep_newest_files:
                keep = True
            if keep_newest_bytes is not None and kept_bytes < keep_newest_bytes:
                keep = True
            if keep:
                kept_files += 1
                kept_bytes += rec.size
            else:
                rest.append(rec)
        rows = rest
    return rows


def parse_day(text: str) -> float | None:
    """'2026-03-01' -> local midnight, None for blank/invalid."""
    try:
        return datetime.datetime.strptime(text.strip(), "%Y-%m-%d").timestamp()
    except (ValueError, AttributeError):
        return None


# ------------------------------------------------------------------ audit log and undo list
class ActionLog:
    """Append-only JSON lines: one object per file and per start/end of an action."""

    def __init__(self, path: str | None):
        self.path = path

    def write(self, **record) -> None:
        if not self.path:
            return
        record = {"time": datetime.datetime.now().isoformat(timespec="seconds"), **record}
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            pass  # an unwritable log must not stop the user from cleaning up... the UI says so

    def writable(self) -> bool:
        if not self.path:
            return False
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
            with open(self.path, "a", encoding="utf-8"):
                return True
        except OSError:
            return False


class Manifest:
    """Where files moved to PendingDeletion came from, so they can be put back. JSON in the
    data folder: {"moves": [{batch, pending, original, mtime, size, time}]}."""

    def __init__(self, path: str | None):
        self.path = path

    def load(self) -> list[dict]:
        if not self.path:
            return []
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                moves = json.load(fh).get("moves", [])
        except (OSError, ValueError, AttributeError):
            return []
        return [m for m in moves if isinstance(m, dict) and m.get("pending") and m.get("original")]

    def _save(self, moves: list[dict]) -> None:
        if not self.path:
            return
        tmp = self.path + ".tmp"
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"moves": moves}, fh, ensure_ascii=False)
            os.replace(tmp, self.path)
        except OSError:
            pass

    def add(self, records: list[dict]) -> None:
        if records:
            self._save(self.load() + records)

    def remove(self, pending_paths: Iterable[str]) -> None:
        gone = {_norm(p) for p in pending_paths}
        self._save([m for m in self.load() if _norm(m["pending"]) not in gone])

    def lookup(self, pending_path: str) -> dict | None:
        key = _norm(pending_path)
        for move in self.load():
            if _norm(move["pending"]) == key:
                return move
        return None

    def batch(self, batch_id: str) -> list[dict]:
        return [m for m in self.load() if m.get("batch") == batch_id]


# ------------------------------------------------------------------ deleters
def pending_mover(guard: Guard, clock: Callable[[], float] = time.time) -> Callable[[PlanItem], dict]:
    """Move into PendingDeletion/<same platform/channel folders>. The file's modified time is
    set to 'now': the recorder's purge counts the age from the newest of mtime/ctime, and a
    file moved with its old dates would be purged at the next start, not after the grace period."""
    def move(item: PlanItem) -> dict:
        pending_top = guard.library.pending_top
        if not pending_top:
            raise OSError("There is no recordings folder to hold PendingDeletion.")
        folder = os.path.join(pending_top, *item.rel[:-1])
        if not inside(folder, pending_top) and _norm(folder) != _norm(pending_top):
            raise OSError("The PendingDeletion path would leave its folder.")
        os.makedirs(folder, exist_ok=True)
        dest = unique_path(folder, os.path.basename(item.path))
        shutil.move(item.path, dest)
        now = clock()
        try:
            os.utime(dest, (now, now))
        except OSError:
            pass
        return {"dest": dest, "original": item.path, "mtime": item.mtime}
    return move


def permanent_deleter() -> Callable[[PlanItem], dict]:
    def remove(item: PlanItem) -> dict:
        try:
            os.remove(item.path)
        except PermissionError:
            os.chmod(item.path, stat.S_IWRITE)
            os.remove(item.path)
        return {}
    return remove


def restore_mover(guard: Guard, manifest: Manifest) -> Callable[[PlanItem], dict]:
    """Put files from PendingDeletion back where they came from (the manifest), or, for files
    MSR moved itself, into Processed (video/images) or Recorded (.ts and partial files)."""
    def move(item: PlanItem) -> dict:
        entry = manifest.lookup(item.path)
        if entry:
            dest, mtime = entry["original"], entry.get("mtime")
        else:
            raw = item.kind in ("raw", "partial") or os.path.splitext(item.path)[1].lower() == ".ts"
            category = "recorded" if raw else "processed"
            top = guard.tops.get(category) or os.path.join(guard.library.streams_dir, category.capitalize())
            dest, mtime = os.path.join(top, *item.rel), None
        problem = guard.check_destination(dest)
        if problem:
            raise OSError(problem)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.move(item.path, dest)
        if mtime:
            try:
                os.utime(dest, (mtime, mtime))
            except OSError:
                pass
        return {"dest": dest, "restored": True}
    return move


def _shfileop_buffer(path: str) -> str:
    """SHFileOperation wants 'path\\0\\0' (a list of paths ending in an empty string)."""
    return os.path.abspath(path) + "\0\0"


def recycle_bin_problem(sample_path: str, total_bytes: int) -> str | None:
    """None when sending this much to the Recycle Bin is expected to keep the files recoverable.
    Windows silently deletes for good what does not fit in the bin, so unless Windows can tell
    us the bin's limit for this drive and everything fits, the option is not offered."""
    if os.name != "nt":
        return "The Recycle Bin is only used on Windows."
    try:
        import ctypes
        import winreg
        root = os.path.splitdrive(os.path.abspath(sample_path))[0] + "\\"
        buf = ctypes.create_unicode_buffer(64)
        if not ctypes.windll.kernel32.GetVolumeNameForVolumeMountPointW(root, buf, 64):
            return "Windows would not tell which volume this is, so the Recycle Bin limit is unknown."
        guid = buf.value.rstrip("\\").split("\\")[-1]  # Volume{...}
        guid = guid[len("Volume"):] if guid.lower().startswith("volume") else guid
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                            r"Software\Microsoft\Windows\CurrentVersion\Explorer\BitBucket\Volume" + "\\" + guid) as key:
            def value(name):
                try:
                    return winreg.QueryValueEx(key, name)[0]
                except OSError:
                    return None
            nuke, cap = value("NukeOnDelete"), value("MaxCapacity")
    except OSError:
        return "The Recycle Bin limit for this drive is not set explicitly, so it cannot be checked."
    except Exception:
        return "The Recycle Bin limit could not be read."
    if nuke:
        return "The Recycle Bin is set to delete files immediately on this drive."
    if not cap:
        return "The Recycle Bin limit for this drive is unknown."
    if total_bytes > int(cap) * 1024 * 1024:
        return f"This is more than the Recycle Bin's limit for this drive ({int(cap):,} MB): Windows would delete it for good."
    return None


def recycle_deleter() -> Callable[[PlanItem], dict]:
    """Windows Recycle Bin through SHFileOperationW with FOF_ALLOWUNDO (no extra dependency)."""
    def trash(item: PlanItem) -> dict:
        if os.name != "nt":
            raise OSError("The Recycle Bin is only available on Windows.")
        import ctypes
        from ctypes import wintypes

        class SHFILEOPSTRUCTW(ctypes.Structure):
            _pack_ = 1 if ctypes.sizeof(ctypes.c_void_p) == 4 else 8
            _fields_ = [("hwnd", wintypes.HWND), ("wFunc", wintypes.UINT), ("pFrom", ctypes.c_wchar_p),
                        ("pTo", ctypes.c_wchar_p), ("fFlags", ctypes.c_ushort), ("fAnyOperationsAborted", wintypes.BOOL),
                        ("hNameMappings", ctypes.c_void_p), ("lpszProgressTitle", ctypes.c_wchar_p)]
        FO_DELETE, FOF_SILENT, FOF_NOCONFIRMATION, FOF_ALLOWUNDO, FOF_NOERRORUI = 3, 0x4, 0x10, 0x40, 0x400
        buffer = ctypes.create_unicode_buffer(_shfileop_buffer(item.path), len(item.path) + 8)
        op = SHFILEOPSTRUCTW()
        op.wFunc = FO_DELETE
        op.pFrom = ctypes.cast(buffer, ctypes.c_wchar_p)
        op.fFlags = FOF_ALLOWUNDO | FOF_NOCONFIRMATION | FOF_NOERRORUI | FOF_SILENT
        code = ctypes.windll.shell32.SHFileOperationW(ctypes.byref(op))
        if code or op.fAnyOperationsAborted or os.path.exists(item.path):
            raise OSError(f"The Recycle Bin refused it (code {code}).")
        return {}
    return trash


# ------------------------------------------------------------------ execute
@dataclass
class Result:
    action: str
    batch: str
    total: int = 0
    done: int = 0
    bytes: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)
    cancelled: bool = False
    moves: list[dict] = field(default_factory=list)
    started: float = 0.0
    finished: float = 0.0

    @property
    def summary(self) -> str:
        what = {ACTION_PENDING: "Moved to PendingDeletion", ACTION_TRASH: "Moved to the Recycle Bin",
                ACTION_PERMANENT: "Deleted permanently", ACTION_RESTORE: "Restored",
                ACTION_RMDIR: "Removed"}.get(self.action, self.action)
        unit = "folder(s)" if self.action == ACTION_RMDIR else "file(s)"
        text = f"{what}: {self.done:,} {unit}" + ("" if self.action == ACTION_RMDIR else f" ({lib.fmt_size(self.bytes)})")
        if self.cancelled:
            text += f". Cancelled after {self.done:,} of {self.total:,}"
        if self.failed:
            text += f". {len(self.failed)} could not be handled (see the log)"
        return text + "."


def execute(plan: Plan, guard: Guard, deleter: Callable[[PlanItem], dict] | None, *,
            progress: Callable[[int, int, int], None] | None = None,
            cancel: threading.Event | None = None, log: ActionLog | None = None,
            manifest: Manifest | None = None, logger=None, batch: str | None = None) -> Result:
    """Do the plan. The guard is asked again right before every file; a cancel stops between
    files (a video and its info file go together). Never raises for a file problem."""
    result = Result(plan.action, batch or uuid.uuid4().hex[:12], total=plan.files or plan.folders,
                    started=time.time())
    log = log or ActionLog(None)
    log.write(event="start", batch=result.batch, action=plan.action, files=plan.files, folders=plan.folders,
              bytes=plan.bytes, channels=plan.channels)
    guard.refresh_busy()
    items = list(plan.items)
    if plan.action == ACTION_RMDIR:
        items.sort(key=lambda i: len(i.rel), reverse=True)  # deepest first
    pending_records: list[dict] = []
    restored: list[str] = []
    try:
        for item in items:
            if item.role != "sidecar" and cancel is not None and cancel.is_set():
                result.cancelled = True
                break
            if plan.action == ACTION_RMDIR:
                reason = guard.check_dir(item.path)
            else:
                reason = guard.check(item.path, item.size, item.mtime, restore=(plan.action == ACTION_RESTORE))
            if reason:
                result.failed.append((item.path, "skipped: " + reason))
                log.write(event="skip", batch=result.batch, action=plan.action, path=item.path, reason=reason)
                continue
            try:
                if plan.action == ACTION_RMDIR:
                    os.rmdir(item.path)
                    info = {}
                else:
                    info = deleter(item) or {}
            except Exception as exc:  # one bad file must not stop the rest
                result.failed.append((item.path, f"{type(exc).__name__}: {exc}"))
                log.write(event="error", batch=result.batch, action=plan.action, path=item.path, error=str(exc))
                continue
            result.done += 1
            result.bytes += item.size
            log.write(event="done", batch=result.batch, action=plan.action, path=item.path, size=item.size,
                      dest=info.get("dest"))
            if plan.action == ACTION_PENDING and info.get("dest"):
                record = {"batch": result.batch, "pending": info["dest"], "original": item.path,
                          "mtime": item.mtime, "size": item.size, "time": time.time()}
                pending_records.append(record)
                result.moves.append(record)
            if plan.action == ACTION_RESTORE:
                restored.append(item.path)
            if progress is not None:
                progress(result.done, result.total, result.bytes)
    finally:
        if manifest is not None:
            manifest.add(pending_records)
            if restored:
                manifest.remove(restored)
        result.finished = time.time()
        log.write(event="end", batch=result.batch, action=plan.action, done=result.done, failed=len(result.failed),
                  bytes=result.bytes, cancelled=result.cancelled)
        if logger is not None:
            try:
                logger.info(f"Library: {result.summary}")
                for path, why in result.failed[:20]:
                    logger.warning(f"Library: {os.path.basename(path)}: {why}")
            except Exception:
                pass
    return result


def deleter_for(action: str, guard: Guard, manifest: Manifest | None = None) -> Callable[[PlanItem], dict] | None:
    return {ACTION_PENDING: lambda: pending_mover(guard), ACTION_PERMANENT: permanent_deleter,
            ACTION_TRASH: recycle_deleter,
            ACTION_RESTORE: lambda: restore_mover(guard, manifest or Manifest(None))}.get(action, lambda: None)()


class ActionJob(threading.Thread):
    """Runs execute() off the UI thread. Messages for the UI (a queue.Queue):
    ("act_progress", done, total, bytes) and ("act_done", Result)."""

    def __init__(self, plan: Plan, guard: Guard, out, *, deleter=None, log: ActionLog | None = None,
                 manifest: Manifest | None = None, logger=None):
        super().__init__(daemon=True, name="LibraryAction")
        self.plan, self.guard, self.out = plan, guard, out
        self.deleter = deleter if deleter is not None else deleter_for(plan.action, guard, manifest)
        self.log, self.manifest, self.logger = log, manifest, logger
        self.cancel_event = threading.Event()

    def cancel(self) -> None:
        self.cancel_event.set()

    def run(self) -> None:
        try:
            result = execute(self.plan, self.guard, self.deleter,
                             progress=lambda d, t, b: self.out.put(("act_progress", d, t, b)),
                             cancel=self.cancel_event, log=self.log, manifest=self.manifest, logger=self.logger)
            self.out.put(("act_done", result))
        except Exception as exc:
            self.out.put(("error", f"{type(exc).__name__}: {exc}"))


def restore_batch_plan(guard: Guard, manifest: Manifest, batch_id: str) -> Plan:
    """The plan that undoes one 'move to PendingDeletion' (only files that are still there)."""
    recs = []
    for move in manifest.batch(batch_id):
        path = move["pending"]
        if not os.path.isfile(path):
            continue
        located = guard._location(path)
        if located is None:
            continue
        category, _top, rel = located
        st = os.lstat(path)
        recs.append(lib.FileRec(path, st.st_size, st.st_mtime, st.st_ctime, category, rel[0].lower(),
                                "/".join(rel[1:-1]), lib.classify(rel[-1], category), st.st_mtime))
    return build_plan(guard, recs, ACTION_RESTORE, include_sidecars=False)


def pending_recs(stats: lib.LibraryStats) -> list:
    return [r for r in stats.files if r.category == "pending"]


# ------------------------------------------------------------------ PendingDeletion card
@dataclass
class PendingInfo:
    files: int = 0
    bytes: int = 0
    oldest: float | None = None        # when the oldest file landed there (the recorder's rule)
    due_files: int = 0                 # old enough for the automatic purge
    due_bytes: int = 0
    next_due: float | None = None      # when the next file becomes old enough
    purge_days: int = 0
    purge_on_startup: bool = True

    def lines(self, now: float | None = None) -> list[str]:
        now = time.time() if now is None else now
        if not self.files:
            return ["PendingDeletion is empty."]
        out = [f"{self.files:,} file(s), {lib.fmt_size(self.bytes)} waiting."]
        if self.oldest:
            out.append(f"Oldest has been there {max(0, (now - self.oldest) / 86400):.1f} day(s).")
        if self.purge_days <= 0 or not self.purge_on_startup:
            out.append("Automatic purge is off (Cleanup settings): nothing leaves this folder by itself.")
        else:
            out.append(f"Automatic purge: files older than {self.purge_days} day(s) are deleted when MSR starts "
                       "(not while it runs).")
            if self.due_files:
                out.append(f"{self.due_files:,} file(s) ({lib.fmt_size(self.due_bytes)}) are already old enough and "
                           "go at the next start.")
            if self.next_due:
                out.append("Next file becomes old enough on " + lib._day(self.next_due) + ".")
        return out


def pending_info(stats: lib.LibraryStats, purge_days: int, purge_on_startup: bool = True,
                 now: float | None = None) -> PendingInfo:
    """Age is the recorder's own rule (recorder.purge_old_pending_files): the newer of mtime and ctime."""
    now = time.time() if now is None else now
    info = PendingInfo(purge_days=int(purge_days or 0), purge_on_startup=bool(purge_on_startup))
    horizon = info.purge_days * 86400
    for rec in pending_recs(stats):
        moved = max(rec.mtime, rec.ctime)
        info.files += 1
        info.bytes += rec.size
        info.oldest = moved if info.oldest is None else min(info.oldest, moved)
        if info.purge_days > 0:
            if now - moved >= horizon:
                info.due_files += 1
                info.due_bytes += rec.size
            else:
                due = moved + horizon
                info.next_due = due if info.next_due is None else min(info.next_due, due)
    return info


# ------------------------------------------------------------------ clean-up suggestions
@dataclass
class Suggestion:
    key: str
    title: str
    detail: str
    recs: list = field(default_factory=list)
    dirs: list = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.recs) + len(self.dirs)

    @property
    def bytes(self) -> int:
        return sum(r.size for r in self.recs)


def _finished(stats):
    return [r for r in stats.files if r.kind == "recording" and r.category == "processed"]


def free_up_suggestion(stats: lib.LibraryStats, target_bytes: int) -> Suggestion:
    """Oldest finished recordings first until together they free `target_bytes` (preview only)."""
    picked, total = [], 0
    for rec in sorted(_finished(stats), key=lambda r: (r.when, r.path)):
        if total >= target_bytes:
            break
        picked.append(rec)
        total += rec.size
    reached = total >= target_bytes
    detail = (f"Oldest recordings first: {len(picked):,} file(s), {lib.fmt_size(total)}"
              + ("" if reached else f" (everything there is; {lib.fmt_size(target_bytes - total)} short)"))
    return Suggestion("free", f"Free up {lib.fmt_size(target_bytes)}", detail, picked)


def build_suggestions(stats: lib.LibraryStats, *, older_days: int = 90, now: float | None = None) -> list[Suggestion]:
    now = time.time() if now is None else now
    done = _finished(stats)
    out: list[Suggestion] = []
    largest = sorted(done, key=lambda r: -r.size)[:10]
    out.append(Suggestion("largest", "Largest 10 recordings", "The biggest single files.", largest))
    oldest = sorted(done, key=lambda r: (r.when, r.path))[:25]
    out.append(Suggestion("oldest", "Oldest 25 recordings", "The earliest recordings in the library.", oldest))
    old = [r for r in done if now - r.when >= older_days * 86400]
    out.append(Suggestion("older", f"Recordings older than {older_days} days", "Finished recordings by their date.", old))
    strangers = []
    for rec in done:
        ch = stats.channels.get((rec.platform, rec.channel.lower()))
        if ch is not None and ch.in_roster is False:
            strangers.append(rec)
    out.append(Suggestion("roster", "Channels not in my roster",
                          "Recordings of channels that are no longer in channels.json.", strangers))
    leftovers = [r for r in stats.files
                 if (r.kind == "partial" and now - r.mtime > 86400)
                 or (r.kind == "raw" and r.category == "recorded" and now - r.mtime > 2 * 86400)]
    out.append(Suggestion("leftover", "Leftover raw / partial files",
                          "Raw .ts files not touched for 2 days and partial downloads older than a day.", leftovers))
    groups: dict[tuple, list] = {}
    for rec in stats.files:
        if rec.kind in ("recording", "clip"):
            groups.setdefault((os.path.basename(rec.path).lower(), rec.size), []).append(rec)
    extras = []
    for copies in groups.values():
        if len(copies) > 1:
            copies.sort(key=lambda r: (r.category != "processed", r.mtime, r.path))
            extras.extend(copies[1:])
    out.append(Suggestion("duplicates", "Duplicates (same name and size)",
                          "Extra copies; the first one found (Processed first) is kept.", extras))
    media = {os.path.splitext(r.path)[0].lower() for r in stats.files if r.kind in ("recording", "raw", "clip", "media")}
    orphans = [r for r in stats.files if r.kind == "sidecar"
               and r.path[:-len(lib.SIDECAR_SUFFIX)].lower() not in media]
    out.append(Suggestion("orphans", "Info files without a video", "A .meta.json whose video is gone.", orphans))
    out.append(Suggestion("empty", "Empty folders", "Folders with nothing in them.", [], list(stats.empty_dirs)))
    return out
