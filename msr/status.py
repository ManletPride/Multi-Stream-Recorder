"""Read-only status snapshot for `Multi-Stream-Recorder.py --json`.

Nothing here writes a file, creates a folder, takes the single-instance lock, opens a
window or reads a secret. The snapshot is meant to be pasted into a bug report or read
by a script/AI assistant, so it holds no passwords, cookies, e-mail addresses or
absolute paths: folders are reported as set/exists/free space, files by name only.
"""
from __future__ import annotations

import configparser
import json
import os
import platform
import shutil
import sys

from msr import __version__
from msr.config import Config, default_streams_dir
from msr.history import is_sensitive

# Shown from config.ini; Paths, Fishtank and anything that looks secret are never copied.
SAFE_SECTIONS = ("Recording", "Timeouts", "Cleanup", "Advanced", "GUI", "Clipping")
PATH_KEYS = {"ffmpeg_path", "clips_dir", "window_state_file"}  # may hold a personal path


def _read_config(path: str) -> tuple[configparser.ConfigParser, bool]:
    parser = configparser.ConfigParser()
    present = os.path.isfile(path)
    if present:
        try:
            parser.read(path, encoding="utf-8")
        except (configparser.Error, OSError, UnicodeDecodeError):
            present = False
    return parser, present


def _value(parser, section: str, key: str) -> str:
    default = Config.DEFAULT_CONFIG.get(section, {}).get(key, "")
    return parser.get(section, key, fallback=default)


def safe_settings(parser) -> dict:
    out: dict = {}
    for section in SAFE_SECTIONS:
        keys = Config.DEFAULT_CONFIG.get(section, {})
        shown = {}
        for key in keys:
            if key in PATH_KEYS or is_sensitive(key):
                continue
            shown[key] = _value(parser, section, key)
        if shown:
            out[section] = shown
    return out


def display_name(channel_key: str) -> str:
    """The roster name as shown, except custom URLs, whose query strings can carry tokens."""
    if channel_key.startswith("custom:"):
        from urllib.parse import urlparse
        host = urlparse(channel_key.split(":", 1)[1].strip()).hostname or "?"
        return f"custom:{host}"
    return channel_key


def channel_summary(channels_path: str) -> dict:
    """{'file': name, 'found', 'total', 'enabled', 'by_platform', 'channels': [{name, enabled}]}"""
    from msr.util import coerce_channel_records, parse_channel_key
    info: dict = {"file": os.path.basename(channels_path), "found": False, "total": 0,
                  "enabled": 0, "skipped": 0, "by_platform": {}, "channels": []}
    try:
        with open(channels_path, "r", encoding="utf-8") as fh:
            loaded = json.load(fh)
    except FileNotFoundError:
        return info
    except (OSError, ValueError):
        info["found"] = True
        info["error"] = "channels file is not valid JSON"
        return info
    info["found"] = True
    records, skipped = coerce_channel_records(loaded)
    info["skipped"] = len(skipped)
    info["total"] = len(records)
    info["enabled"] = sum(1 for r in records if r["enabled"])
    platforms: dict[str, int] = {}
    for rec in records:
        parsed = parse_channel_key(rec["name"])
        platform_name = str(parsed[0]) if parsed else "unknown"
        platforms[platform_name] = platforms.get(platform_name, 0) + 1
    info["by_platform"] = dict(sorted(platforms.items()))
    info["channels"] = [{"name": display_name(r["name"]), "enabled": r["enabled"]} for r in records]
    return info


def is_running(streams_dir: str) -> bool | None:
    """Is another copy holding the single-instance lock for this recordings folder?
    Checks without taking the lock or creating anything. None = could not tell."""
    from msr.util import single_instance_lock_path, single_instance_mutex_name
    try:
        if os.name == "nt":
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.OpenMutexW.restype = ctypes.c_void_p
            handle = kernel32.OpenMutexW(0x00100000, False, single_instance_mutex_name(streams_dir))  # SYNCHRONIZE
            if handle:
                kernel32.CloseHandle(ctypes.c_void_p(handle))
                return True
            return False
        lock = single_instance_lock_path(streams_dir)
        if not os.path.isfile(lock):
            return False
        import fcntl
        with open(lock, "a+") as fh:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        return False
    except Exception:
        return None


def _tools() -> dict:
    from msr import deps
    return {
        "yt-dlp": {"available": bool(deps.HAS_YTDLP), "version": deps.YTDLP_VERSION},
        "streamlink": {"available": bool(deps.HAS_STREAMLINK), "version": deps.STREAMLINK_VERSION},
        "ffmpeg": {"available": bool(deps.HAS_FFMPEG), "version": deps.FFMPEG_VERSION},
        "deno": {"available": bool(deps.HAS_DENO), "version": deps.DENO_VERSION},
        "psutil": {"available": bool(deps.HAS_PSUTIL)},
        "curl_cffi": {"available": bool(deps.HAS_CURL_CFFI)},
        "tray": {"available": bool(deps.HAS_TRAY)},
        "notifications": {"available": bool(deps.HAS_NOTIFICATIONS)},
    }


def _free_gb(folder: str) -> float | None:
    probe = folder
    while probe and not os.path.isdir(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            return None
        probe = parent
    try:
        return round(shutil.disk_usage(probe).free / 1024 ** 3, 1)
    except OSError:
        return None


def _roster_keys(channels_path: str) -> list[str] | None:
    """The raw roster names (None when channels.json is missing or unreadable)."""
    from msr.util import coerce_channel_records
    try:
        with open(channels_path, "r", encoding="utf-8") as fh:
            records, _skipped = coerce_channel_records(json.load(fh))
    except (OSError, ValueError):
        return None
    return [r["name"] for r in records]


def library_snapshot(parser, streams: str, channels_path: str) -> dict:
    """Sizes per site/channel/month for `--json --library` (see msr/library.py).

    Opt-in because it walks the whole recordings folder. It only lists folders and reads file
    sizes (no ffprobe, no video is opened), writes nothing (no cache) and exports no paths."""
    from msr import library
    try:
        roots = library.roots_from_config(parser)
    except configparser.Error:
        roots = library.Roots(streams)
    if not roots.streams_dir:
        roots = library.Roots(streams, roots.clips_dir, roots.extra_dirs)
    stats = library.scan_library(roots, collect_videos=False)
    library.mark_roster(stats, _roster_keys(channels_path))
    return library.to_dict(stats)


def build_status(config_path: str = "config.ini", tools: dict | None = None, library: bool = False) -> dict:
    """The whole snapshot as a JSON-able dict. Paths in config.ini are resolved from the
    current directory (the data dir) exactly like the app does. `library=True` adds a
    "library" section (the slow, opt-in part)."""
    parser, present = _read_config(config_path)
    streams = (parser.get("Paths", "streams_dir", fallback="") or "").strip() or default_streams_dir()
    channels_path = parser.get("Paths", "channels_file", fallback="") or "channels.json"
    channels = channel_summary(channels_path)
    tool_info = tools if tools is not None else _tools()

    free = _free_gb(streams)
    try:
        min_free = float(_value(parser, "Recording", "min_disk_space_gb"))
    except ValueError:
        min_free = 0.0
    problems = []
    for name in ("yt-dlp", "streamlink", "ffmpeg"):
        if not tool_info.get(name, {}).get("available"):
            problems.append(f"{name} is not installed")
    if not channels["found"]:
        problems.append(f"{channels['file']} not found")
    elif channels.get("error"):
        problems.append(channels["error"])
    elif not channels["enabled"]:
        problems.append("no channel is enabled")
    if not os.path.isdir(streams):
        problems.append("recordings folder does not exist yet")
    if free is not None and min_free and free < min_free:
        problems.append(f"free disk space ({free} GB) is below min_disk_space_gb ({min_free:g})")
    if not present:
        problems.append("config.ini not found - defaults shown (it is created on first normal start)")

    snapshot = {
        "app": {"name": "Multi-Stream Recorder", "version": __version__},
        "system": {"python": platform.python_version(), "os": sys.platform},
        "running": is_running(streams),
        "config": {
            "file": os.path.basename(config_path),
            "found": present,
            "settings": safe_settings(parser),
            "recordings_folder": {"exists": os.path.isdir(streams), "free_gb": free},
            "fishtank_login_set": bool(parser.get("Fishtank", "email", fallback="").strip()
                                       and parser.get("Fishtank", "password", fallback="").strip()),
            "cookies_file_set": bool(parser.get("Paths", "cookies_file", fallback="").strip()),
        },
        "channels": channels,
        "tools": tool_info,
        "problems": problems,
        "ok": not problems,
    }
    if library:
        snapshot["library"] = library_snapshot(parser, streams, channels_path)
    return snapshot


def render(status: dict) -> str:
    """ASCII-only JSON, safe for any console encoding."""
    return json.dumps(status, indent=2, sort_keys=False)
