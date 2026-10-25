"""Last-20-versions history for small saved files (channels.json, config.ini).

``snapshot(path)`` is called right before the file is overwritten, so the state you had
a moment ago can always be brought back (the Restore previous... dialog). Versions live
beside the file in ``msr_history/<file name>/<stamp>.bak`` (git-ignored). A history
failure never blocks a save: it is logged and the save goes on.

config.ini can hold the Fishtank email and password, so version *summaries* never print
the value of a sensitive key (password, email, token, cookie, secret, key, auth): they
only say it changed. The history copies themselves are full copies, as a restore needs.
"""

from __future__ import annotations

import configparser
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

KEEP = 20
HISTORY_DIRNAME = "msr_history"
_STAMP = "%Y%m%d-%H%M%S-%f"
_SENSITIVE = re.compile(r"pass|secret|token|cookie|e-?mail|auth|key|session|login", re.IGNORECASE)
MASK = "\u2022\u2022\u2022\u2022"


@dataclass(frozen=True)
class Version:
    path: Path
    stamp: datetime
    size: int

    def read(self) -> bytes:
        return self.path.read_bytes()


def history_folder(path) -> Path:
    path = Path(path)
    return path.parent / HISTORY_DIRNAME / path.name


def list_versions(path) -> list[Version]:
    """Saved versions of `path`, newest first."""
    folder = history_folder(path)
    found = []
    try:
        entries = list(folder.iterdir())
    except OSError:
        return []
    for entry in entries:
        if entry.suffix != ".bak":
            continue
        try:
            stamp = datetime.strptime(entry.stem, _STAMP)
            found.append(Version(entry, stamp, entry.stat().st_size))
        except (ValueError, OSError):
            continue
    return sorted(found, key=lambda v: v.stamp, reverse=True)


def snapshot(path, *, keep: int = KEEP, min_interval: float = 0, now: datetime | None = None) -> Path | None:
    """Copy the current `path` into its history before it is overwritten.

    Skipped when the file is missing or empty, when it is identical to the newest saved
    version, or when the newest version is younger than `min_interval` seconds (a burst of
    quick saves keeps the state from before the burst). Returns the new copy or None.
    Never raises.
    """
    try:
        path = Path(path)
        if not path.is_file():
            return None
        data = path.read_bytes()
        if not data:
            return None
        now = now or datetime.now()
        versions = list_versions(path)
        if versions:
            newest = versions[0]
            if newest.read() == data:
                return None
            if min_interval and 0 <= (now - newest.stamp).total_seconds() < min_interval:
                return None
        folder = history_folder(path)
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / f"{now.strftime(_STAMP)}.bak"
        while target.exists():  # two saves in the same microsecond
            now = now.replace(microsecond=(now.microsecond + 1) % 1_000_000)
            target = folder / f"{now.strftime(_STAMP)}.bak"
        target.write_bytes(data)
        for old in list_versions(path)[keep:]:
            try:
                old.path.unlink()
            except OSError:
                pass
        return target
    except Exception as exc:  # history must never stop a save
        logging.warning("Could not keep a history copy of %s: %s", path, exc)
        return None


def atomic_write(path, data: bytes) -> None:
    """Write via a temp file in the same folder and os.replace, so a crash can't leave half a file."""
    path = Path(path)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def restore(path, version: Version, *, keep: int = KEEP) -> None:
    """Put `version` back as `path`. The file as it is now is kept in the history first."""
    data = version.read()
    snapshot(path, keep=keep + 1)  # the pre-restore state, so a restore can itself be undone
    atomic_write(path, data)
    # the restored copy stays in the list too; trim to the limit
    for old in list_versions(path)[keep:]:
        try:
            old.path.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------- summaries (no secrets)
def is_sensitive(key: str) -> bool:
    return bool(_SENSITIVE.search(key or ""))


def _shorten(text: str, limit: int = 40) -> str:
    text = text.replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 1] + "\u2026"


def _list(names: list[str], limit: int = 3) -> str:
    shown = ", ".join(names[:limit])
    return shown + (f" +{len(names) - limit} more" if len(names) > limit else "")


def _ini(data: bytes) -> dict[str, str] | None:
    parser = configparser.ConfigParser(interpolation=None, strict=False)
    try:
        parser.read_string(data.decode("utf-8-sig", errors="replace"))
    except configparser.Error:
        return None
    return {f"{s}.{k}": v for s in parser.sections() for k, v in parser.items(s)}


def _channels(data: bytes) -> dict[str, bool] | None:
    try:
        loaded = json.loads(data.decode("utf-8-sig"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(loaded, list):
        return None
    out: dict[str, bool] = {}
    for item in loaded:
        if isinstance(item, str):
            out[item] = True
        elif isinstance(item, dict) and isinstance(item.get("name"), str):
            out[item["name"]] = bool(item.get("enabled", True))
    return out


def summarize(name: str, version: bytes, current: bytes | None) -> str:
    """One line on how `version` differs from `current`. Sensitive config values are masked."""
    if current is not None and version == current:
        return "Same as the current file"
    suffix = Path(name).suffix.lower()
    if suffix == ".ini":
        old, new = _ini(version), _ini(current or b"")
        if old is None:
            return "Settings file (unreadable)"
        if new is None or current is None:
            return f"{len(old)} settings"
        keys = sorted(set(old) | set(new))
        parts = []
        for key in keys:
            if old.get(key) == new.get(key):
                continue
            if is_sensitive(key.split(".", 1)[-1]):
                parts.append(f"{key} {MASK} (differs)")
            else:
                before, after = old.get(key), new.get(key)
                parts.append(f"{key}: {_shorten(after) if after is not None else '(none)'} \u2192 "
                             f"{_shorten(before) if before is not None else '(none)'}")
        shown = "; ".join(parts[:4]) + (f"; +{len(parts) - 4} more" if len(parts) > 4 else "")
        return f"Restoring changes: {shown}" if parts else "Same settings (formatting differs)"
    if suffix == ".json":
        old, new = _channels(version), _channels(current or b"")
        if old is None:
            return "Saved data"
        if new is None or current is None:
            return f"{len(old)} channels"
        added = [n for n in old if n not in new]
        removed = [n for n in new if n not in old]
        flipped = [n for n in old if n in new and old[n] != new[n]]
        parts = []
        if added:
            parts.append(f"brings back {len(added)} ({_list(added)})")
        if removed:
            parts.append(f"drops {len(removed)} ({_list(removed)})")
        if flipped:
            parts.append(f"{len(flipped)} enabled/disabled differ ({_list(flipped)})")
        if not parts:
            parts.append("same channels, different order")
        return f"{len(old)} channels; restoring " + "; ".join(parts)
    return f"{len(version)} bytes"