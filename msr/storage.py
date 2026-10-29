"""A recordings drive that goes away while MSR runs (unplugged USB or external disk, a dropped share).

Standard library only, no Tk. The pieces:

* ``classify`` / ``probe_dir`` / ``quick_check``: is the folder reachable, and if not, why (a short
  reason with no file contents).
* ``fallback_root`` / ``choose_root``: where a worker records while the main folder is gone. The
  fallback is ``[Paths] fallback_dir`` (``off`` disables it, blank = a folder in the system temp
  folder). It is used only when the main folder is unreachable, never when it is merely low on space.
* ``reconcile_fallback``: when the main folder is back, move what was recorded in the fallback into
  it (same relative path). Nothing is deleted before the copy is verified.
* ``write_failed`` / ``write_ok``: one warning per failing file (repeated at most every 10 minutes,
  and one line when it works again) instead of a silent ``pass`` or a log line per failed write.
"""
import errno
import hashlib
import logging
import os
import shutil
import tempfile
import threading
import time
from typing import NamedTuple, Optional

PROBE_NAME = ".msr_write_probe"
PART_SUFFIX = ".msrpart"
FALLBACK_OFF = "off"
MIN_AGE_SECONDS = 300      # a fallback file touched more recently than this may still be in use

# Windows error codes: path/drive not found, device not ready, not connected, network gone, I/O error.
_MISSING_WINERRORS = {2, 3, 53, 67}
_OFFLINE_WINERRORS = {21, 55, 64, 121, 483, 1117, 1167, 1231}
_DENIED_WINERRORS = {5, 19}
_FULL_WINERRORS = {39, 112}
_OFFLINE_ERRNOS = {getattr(errno, name) for name in ("EIO", "ENODEV", "ENXIO", "ENOTCONN", "ESTALE", "EHOSTDOWN")
                   if hasattr(errno, name)}

REASONS = {
    "missing": "the drive or share is not available",
    "offline": "the drive stopped responding (unplugged or disconnected?)",
    "denied": "access is denied (read-only or no permission)",
    "full": "the disk is full",
    "error": "the folder cannot be used",
}
UNAVAILABLE_KINDS = ("missing", "offline")


class Probe(NamedTuple):
    ok: bool
    kind: str = ""
    reason: str = ""


class RootChoice(NamedTuple):
    path: Optional[str]            # the folder to record into (None when nothing is usable)
    ok: bool
    on_fallback: bool = False
    low_space: bool = False        # the main folder is reachable but below min_disk_space_gb
    free_gb: Optional[float] = None
    reason: str = ""               # why the main folder is not used (never contains file contents)


def classify(exc):
    """'missing', 'offline', 'denied', 'full' or 'error' for an OSError."""
    code = getattr(exc, "winerror", None)
    if code in _MISSING_WINERRORS:
        return "missing"
    if code in _OFFLINE_WINERRORS:
        return "offline"
    if code in _FULL_WINERRORS:
        return "full"
    if code in _DENIED_WINERRORS:
        return "denied"
    number = getattr(exc, "errno", None)
    if number == errno.ENOSPC:
        return "full"
    if number in _OFFLINE_ERRNOS:
        return "offline"
    if isinstance(exc, PermissionError) or number in (errno.EACCES, errno.EPERM, errno.EROFS):
        return "denied"
    if isinstance(exc, FileNotFoundError) or number == errno.ENOENT:
        return "missing"
    return "error"


def probe_dir(path):
    """Create ``path`` if needed and write/remove a tiny file in it."""
    try:
        os.makedirs(path, exist_ok=True)
        probe = os.path.join(path, PROBE_NAME)
        with open(probe, "wb") as fh:
            fh.write(b"ok")
        try:
            os.remove(probe)
        except OSError:
            pass
        return Probe(True)
    except OSError as exc:
        kind = classify(exc)
        return Probe(False, kind, REASONS[kind])


def free_gb(path):
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return None


def quick_check(path):
    """Cheap reachability test: ``disk_usage`` first (as before), the write probe only if that fails."""
    if free_gb(path) is not None:
        return Probe(True)
    return probe_dir(path)


def is_drive_error(exc, path):
    """True for an OSError that means ``path``'s drive is gone (not for a network or parsing error)."""
    return classify(exc) in UNAVAILABLE_KINDS and not quick_check(path).ok


def fallback_root(setting, primary=None):
    """The fallback recording folder for a ``[Paths] fallback_dir`` value (None = disabled).

    The default (blank) is one folder per main folder, so two instances with different recordings
    folders never share a fallback (the orphan check treats a fallback as that instance's own).
    """
    value = (setting or "").strip()
    if value.lower() == FALLBACK_OFF:
        return None
    if value:
        return os.path.abspath(os.path.expandvars(os.path.expanduser(value.strip('"'))))
    key = os.path.normcase(os.path.abspath(primary or "default"))
    tag = hashlib.sha1(key.encode("utf-8", "replace")).hexdigest()[:8]
    return os.path.join(tempfile.gettempdir(), "MultiStreamRecorder", f"Recordings-{tag}")


def _inside(path, folder):
    try:
        a = os.path.normcase(os.path.abspath(path))
        b = os.path.normcase(os.path.abspath(folder))
        return os.path.commonpath([a, b]) == b
    except ValueError:
        return False  # different drives


def choose_root(primary, fallback, min_free_gb, probe=None, free=None):
    """Pick the folder to record into: the main one when reachable, else the fallback."""
    probe = probe or quick_check
    free = free or free_gb
    first = probe(primary)
    if first.ok:
        space = free(primary)
        if space is not None and space < min_free_gb:
            return RootChoice(primary, False, low_space=True, free_gb=space, reason="low disk space")
        return RootChoice(primary, True, free_gb=space)
    reason = f"{first.reason}"
    if not fallback or _inside(fallback, primary):
        return RootChoice(None, False, reason=reason)
    second = probe(fallback)
    if not second.ok:
        return RootChoice(None, False, reason=f"{reason}; the fallback folder is not usable either "
                                                f"({second.reason})")
    space = free(fallback)
    if space is not None and space < min_free_gb:
        return RootChoice(None, False, free_gb=space, reason=f"{reason}; the fallback folder has only "
                                                              f"{space:.1f} GB free")
    return RootChoice(fallback, True, on_fallback=True, free_gb=space, reason=reason)


# ── bringing the fallback recordings back ──

class ReturnSummary(NamedTuple):
    moved: int
    young: int
    failed: int
    moved_ts: list


def has_files(folder):
    """True if ``folder`` holds at least one file that is not just a leftover probe."""
    try:
        for _dir, _dirs, names in os.walk(folder):
            if any(n != PROBE_NAME and not n.endswith(PART_SUFFIX) for n in names):
                return True
    except OSError:
        pass
    return False


def _move_verified(src, dest, logger):
    """Copy ``src`` to ``dest`` (never over an existing different file), verify, then remove ``src``.

    Returns 'moved', 'copied' (copy verified but the source could not be removed) or 'failed'.
    """
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    size = os.path.getsize(src)
    if os.path.exists(dest):
        if os.path.getsize(dest) == size and abs(os.path.getmtime(dest) - os.path.getmtime(src)) <= 2:
            # copied by an earlier pass (copy2 keeps the time): just drop the source
            os.remove(src)
            return "moved"
        stem, ext = os.path.splitext(dest)
        n = 1
        dest = f"{stem}.from-fallback{ext}"
        while os.path.exists(dest):
            n += 1
            dest = f"{stem}.from-fallback-{n}{ext}"
    part = dest + PART_SUFFIX
    try:
        shutil.copy2(src, part)
        if os.path.getsize(part) != size:
            raise OSError("copy has a different size")
        os.replace(part, dest)
    except OSError:
        try:
            os.remove(part)
        except OSError:
            pass
        raise
    try:
        os.remove(src)
    except OSError as exc:
        logger.warning(f"Fallback recordings: copied {os.path.basename(dest)} back but could not remove "
                       f"the fallback copy yet ({classify(exc)}); it is retried later")
        return "copied"
    return "moved"


def reconcile_fallback(fallback, primary, logger=None, min_age=MIN_AGE_SECONDS, should_stop=None, now=time.time):
    """Move the files recorded in ``fallback`` into ``primary`` (same relative paths)."""
    logger = logger or logging.getLogger()
    moved = young = failed = 0
    moved_ts = []
    try:
        entries = [(d, n) for d, _dirs, names in os.walk(fallback) for n in names]
    except OSError:
        entries = []
    for folder, name in entries:
        if should_stop is not None and should_stop.is_set():
            break
        if name == PROBE_NAME or name.endswith(PART_SUFFIX):
            continue
        src = os.path.join(folder, name)
        try:
            if now() - os.path.getmtime(src) < min_age:
                young += 1
                continue
            dest = os.path.join(primary, os.path.relpath(src, fallback))
            result = _move_verified(src, dest, logger)
        except OSError as exc:
            failed += 1
            write_failed("a recording moved back from the fallback folder", src, exc, logger=logger)
            continue
        if result == "moved":
            moved += 1
            if name.lower().endswith(".ts"):
                moved_ts.append(dest)
        else:
            failed += 1
    return ReturnSummary(moved, young, failed, moved_ts)


# ── one warning per failing file ──

_failing = {}
_failing_lock = threading.Lock()
WARN_EVERY = 600


def write_failed(what, path, exc, logger=None, now=time.monotonic):
    """Log that ``what`` could not be written to ``path`` (first time, then every 10 minutes)."""
    kind = classify(exc) if isinstance(exc, OSError) else "error"
    key = (what, os.path.abspath(str(path)) if path else "")
    t = now()
    with _failing_lock:
        last = _failing.get(key)
        if last is not None and t - last < WARN_EVERY:
            return False
        _failing[key] = t
    (logger or logging.getLogger()).warning(
        f"Could not write {what} ({REASONS[kind]}): {path}. The app keeps running and tries again later.")
    return True


def write_ok(what, path, logger=None):
    """Call after a write that worked: one line if it had been failing."""
    key = (what, os.path.abspath(str(path)) if path else "")
    with _failing_lock:
        was = _failing.pop(key, None)
    if was is not None:
        (logger or logging.getLogger()).info(f"Writing {what} works again: {path}")
        return True
    return False
