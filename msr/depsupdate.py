"""Dependencies: what MSR runs on, what is newer, and pip updates. Tk-free (GUI dialog, control
API and msr_ctl.py share it).

Rows: MSR itself (GitHub releases; installed by the existing in-app updater), the recording
tools yt-dlp and streamlink, ffmpeg (reported only: MSR never installs it, so a system/PATH copy is
left to its owner), and the Python packages from requirements.txt.

pip runs in this interpreter, hidden, one package at a time. A running recording keeps the
program it already started; the new version is used from the next recording on. If an update
fails, the version that was there before is put back.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

from msr.nowindow import no_window_kwargs

# (distribution name on PyPI, label, kind) - kind "tool" = runs as a program while recording
PACKAGES = (
    ("yt-dlp", "yt-dlp", "tool"),
    ("streamlink", "streamlink", "tool"),
    ("curl_cffi", "curl_cffi", "package"),
    ("websocket-client", "websocket-client", "package"),
    ("psutil", "psutil", "package"),
    ("Pillow", "Pillow", "package"),
    ("pystray", "pystray", "package"),
    ("plyer", "plyer", "package"),
    ("colorama", "colorama", "package"),
)
NAMES = ("msr", "ffmpeg") + tuple(p[0].lower() for p in PACKAGES)
STATE_KEY = "deps_check"            # window_state.json: last check time and what had updates
DAY = 24 * 3600
PIP_TIMEOUT = 600


class DepsError(Exception):
    """Shown to the user as is."""


# ------------------------------------------------------------------ versions
def vkey(v: str | None):
    """A sortable key for version strings like 2026.10.22, 8.0.0, 0.16.3b1."""
    parts = []
    for tok in re.split(r"[.\-+_]", str(v or "")):
        m = re.match(r"(\d+)(.*)", tok)
        if m:
            parts.append((int(m.group(1)), m.group(2) == "", m.group(2)))
        elif tok:
            parts.append((-1, False, tok))
    return tuple(parts)


def installed_version(dist: str) -> str | None:
    try:
        from importlib import metadata
        return metadata.version(dist)
    except Exception:
        return None


def _get_json(url: str, timeout: float = 10.0, opener=None):
    req = urllib.request.Request(url, headers={"User-Agent": "MultiStreamRecorder-deps",
                                               "Accept": "application/json"})
    with (opener or urllib.request.urlopen)(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def latest_pypi(dist: str, opener=None) -> str:
    return str(_get_json(f"https://pypi.org/pypi/{dist}/json", opener=opener)["info"]["version"])


def latest_msr(opener=None) -> tuple[str, str]:
    from msr.util import GITHUB_OWNER, GITHUB_REPO
    data = _get_json(f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/releases/latest", opener=opener)
    return str(data.get("tag_name", "")).lstrip("vV"), str(data.get("html_url", ""))


def _offline(e: Exception) -> bool:
    return isinstance(e, (urllib.error.URLError, OSError, TimeoutError))


# ------------------------------------------------------------------ ffmpeg
def ffmpeg_row(ffmpeg_path: str = "ffmpeg", data_dir: str | None = None) -> dict:
    from msr import deps
    path = (ffmpeg_path or "ffmpeg").strip() or "ffmpeg"
    resolved = shutil.which(path) if not os.path.isabs(path) else (path if os.path.isfile(path) else None)
    version = deps.check_ffmpeg(path) if resolved else None
    if not resolved:
        source = "not found"
    elif os.path.isabs(path):
        source = f"set in config.ini ([Advanced] ffmpeg_path): {resolved}"
    else:
        source = f"system PATH: {resolved}"
    return {"name": "ffmpeg", "label": "ffmpeg", "kind": "tool", "installed": version, "latest": None,
            "status": "not_installed" if not version else "external", "source": source, "updatable": False,
            "note": "Not installed by MSR, so MSR does not update it. Update it where it came from "
                    "(e.g. gyan.dev build or winget)." if version else
                    "ffmpeg is needed to finish recordings. Install it and put it on PATH."}


# ------------------------------------------------------------------ rows
def _status(installed, latest):
    if not installed:
        return "not_installed"
    if not latest:
        return "unknown"
    return "update_available" if vkey(latest) > vkey(installed) else "up_to_date"


def rows(check: bool = False, *, ffmpeg_path: str = "ffmpeg", opener=None, version_of=installed_version) -> list[dict]:
    """One dict per item. check=False lists installed versions only (no network)."""
    from msr import __version__
    out = []
    msr = {"name": "msr", "label": "Multi-Stream Recorder", "kind": "app", "installed": __version__,
           "latest": None, "status": "unchecked", "source": "this app", "updatable": False, "note": ""}
    if check:
        try:
            tag, url = latest_msr(opener)
            msr.update(latest=tag or None, status=_status(__version__, tag or None), url=url)
            msr["updatable"] = msr["status"] == "update_available"
            if msr["updatable"]:
                msr["note"] = "Installed by the app's own updater (the banner / Update in the window)."
        except Exception as e:
            msr.update(status="unknown", note=_why(e))
    out.append(msr)
    out.append(ffmpeg_row(ffmpeg_path))
    for dist, label, kind in PACKAGES:
        ver = version_of(dist)
        row = {"name": dist.lower(), "label": label, "kind": kind, "installed": ver, "latest": None,
               "status": "unchecked" if ver else "not_installed",
               "source": f"pip ({os.path.basename(sys.executable)})", "updatable": False, "note": ""}
        if check:
            try:
                latest = latest_pypi(dist, opener)
                row.update(latest=latest, status=_status(ver, latest))
            except Exception as e:
                row.update(status="unknown" if ver else "not_installed", note=_why(e))
            row["updatable"] = row["status"] in ("update_available",) or (row["status"] == "not_installed"
                                                                          and row["latest"] is not None)
        out.append(row)
    return out


def _why(e: Exception) -> str:
    if isinstance(e, urllib.error.HTTPError):
        return "No published release found." if e.code == 404 else f"Update server answered HTTP {e.code}."
    if _offline(e):
        return "Could not reach the update server (offline?)."
    return f"Check failed: {type(e).__name__}: {str(e)[:150]}"


def updates_available(rs: list[dict]) -> list[str]:
    return [r["name"] for r in rs if r.get("status") == "update_available"]


# ------------------------------------------------------------------ pip
def pip_command(python: str, spec: str) -> list[str]:
    return [python, "-m", "pip", "install", "--upgrade", "--disable-pip-version-check", "--no-input",
            "--progress-bar", "off", spec]


def classify_pip_error(text: str) -> str:
    t = text or ""
    low = t.lower()
    if any(s in low for s in ("permission denied", "access is denied", "winerror 5", "winerror 32",
                              "being used by another process")):
        return ("Windows would not let pip replace the files: a recording may be using this tool right now, "
                "or the Python folder needs administrator rights. Try again when no recording runs.")
    if any(s in low for s in ("could not fetch", "newconnectionerror", "temporary failure in name resolution",
                              "failed to establish a new connection", "no matching distribution",
                              "connection aborted", "read timed out", "getaddrinfo failed")):
        return "pip could not download the package (offline, or PyPI not reachable)."
    if "no module named pip" in low:
        return "pip is not installed for this Python."
    tail = [ln for ln in t.strip().splitlines() if ln.strip()][-1:] or ["pip failed"]
    return tail[0][:300]


def update_packages(names, *, python: str | None = None, run=subprocess.run, version_of=installed_version,
                    log=None) -> list[dict]:
    """pip-upgrade each named package (PyPI names, any case). Puts the old version back when an
    update fails or leaves the package unimportable. Returns one result per name."""
    python = python or sys.executable
    known = {p[0].lower(): p[0] for p in PACKAGES}
    results = []
    for name in names:
        key = str(name).lower()
        if key in ("msr", "ffmpeg"):
            results.append({"name": key, "ok": False, "error": "msr: use the app's own updater (window banner); "
                            "ffmpeg: not installed by MSR." if key == "msr" else
                            "ffmpeg is not installed by MSR; update it where it came from."})
            continue
        if key not in known:
            results.append({"name": key, "ok": False, "error": f"Unknown package: {name}"})
            continue
        dist = known[key]
        old = version_of(dist)
        res = {"name": key, "old": old, "new": old, "ok": False, "rolled_back": False}
        if log:
            log(f"Updating {dist} (installed {old or 'none'})\u2026")
        try:
            r = run(pip_command(python, dist), capture_output=True, text=True, timeout=PIP_TIMEOUT,
                    stdin=subprocess.DEVNULL, **no_window_kwargs())
            failed = r.returncode != 0
            err = classify_pip_error((r.stderr or "") + "\n" + (r.stdout or "")) if failed else ""
        except subprocess.TimeoutExpired:
            failed, err = True, f"pip took longer than {PIP_TIMEOUT // 60} minutes and was stopped."
        except OSError as e:
            failed, err = True, f"Could not run pip: {e}"
        new = version_of(dist)
        if not failed and new is None:
            failed, err = True, "pip finished but the package is gone."
        if failed:
            res["error"] = err
            if old and version_of(dist) != old:
                try:   # put the previous version back
                    rb = run(pip_command(python, f"{dist}=={old}"), capture_output=True, text=True,
                             timeout=PIP_TIMEOUT, stdin=subprocess.DEVNULL, **no_window_kwargs())
                    res["rolled_back"] = rb.returncode == 0 and version_of(dist) == old
                except (OSError, subprocess.SubprocessError):
                    pass
                if not res["rolled_back"]:
                    res["error"] += f" Putting {old} back also failed: run 'python -m pip install {dist}=={old}'."
            res["new"] = version_of(dist)
        else:
            res.update(ok=True, new=new)
        if log:
            log(f"{dist}: " + (f"{old} -> {new}" if res["ok"] else res["error"]))
        results.append(res)
    return results


# ------------------------------------------------------------------ daily check bookkeeping
def due(state: dict, now: float | None = None) -> bool:
    last = (state.get(STATE_KEY) or {}).get("time", 0)
    try:
        return (now or time.time()) - float(last) >= DAY
    except (TypeError, ValueError):
        return True


def remember(state_file: str, rs: list[dict], now: float | None = None) -> None:
    from msr import winstate
    winstate.merge(state_file, {STATE_KEY: {"time": now or time.time(), "updates": updates_available(rs)}})


def remembered_updates(state: dict) -> list[str]:
    return list((state.get(STATE_KEY) or {}).get("updates") or [])
