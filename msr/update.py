"""Download a GitHub release zipball and stage an in-app update.

The running GUI cannot overwrite its own ``msr/`` tree. ``prepare_update``
fills a staging folder; ``spawn_updater`` must be started *after* this
process has killed its workers, immediately before ``os._exit``.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from urllib.parse import urlparse

from msr import __version__
from msr.apply_update import ERROR_FILE
from msr.util import GITHUB_OWNER, GITHUB_REPO, application_dir

ALLOWED_HOSTS = frozenset({
    "github.com",
    "codeload.github.com",
    "api.github.com",
    "objects.githubusercontent.com",
})
# Program files an update may bring. Keep in step with msr/apply_update.py (a test checks).
# Never user data: config, cookies, channels, logs, history, caches, .venv (see is_private).
ALLOWED_FILES = frozenset({
    "Multi-Stream-Recorder.py",
    "requirements.txt",
    "channels_fishtank.json",
    "run_msr.pyw",
    "run_msr.bat",
    "msr_ctl.py",
    "create_shortcut.py",
})
ALLOWED_DIRS = ("msr", "assets", "docs")
ALLOWED_DIR = "msr"   # the package; kept for older callers
PRIVATE_NAMES = frozenset({
    "config.ini", "channels.json", "cookies.txt", "window_state.json", "kick_channel_ids.json",
    "msr_control.json", "msr_status.json", "library_cache.json", "library_moves.json",
    "library_actions.log", ".env", "run_msr.local.bat",
})
PRIVATE_DIRS = frozenset({".venv", "venv", "msr_history", "msr_update_backup", "__pycache__", "themes"})


def is_private(rel):
    """True for anything that is user data or build output, wherever it sits."""
    parts = rel.replace("\\", "/").split("/")
    name = parts[-1].lower()
    if any(p.lower() in PRIVATE_DIRS for p in parts[:-1]):
        return True
    return (name in PRIVATE_NAMES or name.endswith((".pyc", ".pyo", ".har", ".log"))
            or (name.startswith("cookies") and name.endswith(".txt")) or name.startswith("config.ini."))


def is_program_file(rel):
    """Relative path (with /) that an update installs."""
    if is_private(rel):
        return False
    parts = rel.split("/")
    return rel in ALLOWED_FILES or (len(parts) > 1 and parts[0] in ALLOWED_DIRS)
MAX_ZIP_BYTES = 50 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 80 * 1024 * 1024
MAX_MEMBER_BYTES = 8 * 1024 * 1024


def zipball_url_is_allowed(url):
    """True if *url* is HTTPS GitHub (API, web, or codeload)."""
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme != "https":
        return False
    host = (parsed.hostname or "").lower()
    return host in ALLOWED_HOSTS


def zipball_url_for_tag(tag):
    """Public archive URL for a release tag (``v2.0.3`` or ``2.0.3``)."""
    raw = str(tag or "").strip()
    if not raw:
        raise ValueError("missing release tag")
    if not raw.lower().startswith("v"):
        raw = "v" + raw
    return (
        f"https://github.com/{GITHUB_OWNER}/{GITHUB_REPO}"
        f"/archive/refs/tags/{raw}.zip"
    )


def _archive_prefix(names):
    """GitHub zipballs nest everything under ``Repo-1.2.3/``."""
    tops = set()
    for name in names:
        rel = name.replace("\\", "/").lstrip("/")
        if not rel:
            continue
        tops.add(rel.split("/", 1)[0])
    if len(tops) == 1:
        return next(iter(tops)) + "/"
    return ""


def _safe_relpath(filename, prefix):
    rel = filename.replace("\\", "/").lstrip("/")
    if prefix and rel.startswith(prefix):
        rel = rel[len(prefix):]
    rel = rel.lstrip("/")
    if not rel or rel.endswith("/"):
        return None
    parts = rel.split("/")
    if any(p in ("", ".", "..") for p in parts):
        return None
    return rel if is_program_file(rel) else None


def extract_allowlisted(zip_path, staging_dir):
    """Extract only the program files (ALLOWED_FILES and ALLOWED_DIRS, never private files).

    Returns the list of relative paths written. Raises on a zip-bomb-ish
    archive or if nothing allowlisted is present.
    """
    os.makedirs(staging_dir, exist_ok=True)
    written = []
    total = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        prefix = _archive_prefix(zf.namelist())
        for info in zf.infolist():
            if info.is_dir():
                continue
            rel = _safe_relpath(info.filename, prefix)
            if rel is None:
                continue
            size = info.file_size
            if size > MAX_MEMBER_BYTES:
                raise ValueError(f"update member too large: {rel}")
            total += size
            if total > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("update zip uncompressed size is too large")
            dest = os.path.normpath(os.path.join(staging_dir, rel))
            staging_abs = os.path.abspath(staging_dir)
            dest_abs = os.path.abspath(dest)
            try:
                if os.path.commonpath([staging_abs, dest_abs]) != staging_abs:
                    continue
            except ValueError:
                continue
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with zf.open(info, "r") as src, open(dest, "wb") as out:
                shutil.copyfileobj(src, out)
            written.append(rel)
    if not written:
        raise ValueError("update zip had no Multi-Stream-Recorder files")
    return written


def staging_is_valid(staging_dir):
    launcher = os.path.join(staging_dir, "Multi-Stream-Recorder.py")
    init_py = os.path.join(staging_dir, "msr", "__init__.py")
    return os.path.isfile(launcher) and os.path.isfile(init_py)


def download_zipball(url, dest_path, timeout=60):
    if not zipball_url_is_allowed(url):
        raise ValueError("update URL is not GitHub HTTPS")
    req = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json, application/zip",
        "User-Agent": f"MultiStreamRecorder/{__version__}",
    })
    written = 0
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        final = resp.geturl() or url
        if not zipball_url_is_allowed(final):
            raise ValueError("update redirected off GitHub")
        parent = os.path.dirname(dest_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(dest_path, "wb") as out:
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_ZIP_BYTES:
                    raise ValueError("update zip is too large")
                out.write(chunk)
    if written < 64:
        raise ValueError("update zip was empty")
    return dest_path


def prepare_update(zipball_url, work_dir=None):
    """Download and extract. Returns the staging directory path."""
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="msr-update-")
    zip_path = os.path.join(work_dir, "release.zip")
    staging = os.path.join(work_dir, "staging")
    os.makedirs(staging, exist_ok=True)
    download_zipball(zipball_url, zip_path)
    extract_allowlisted(zip_path, staging)
    try:
        os.remove(zip_path)
    except OSError:
        pass
    if not staging_is_valid(staging):
        raise ValueError(
            "update zip did not contain Multi-Stream-Recorder.py and msr/"
        )
    return staging


def spawn_updater(staging_dir, target_dir=None, python_exe=None, launcher=None,
                  pid=None, launcher_args=()):
    """Start the helper that copies *staging_dir* after this PID exits.

    ``launcher_args`` are passed to the relaunched MSR (headless: ``--headless --data-dir ...``).

    Call this only after worker child processes have been killed, and
    immediately before ``os._exit``.
    """
    target_dir = os.path.abspath(target_dir or application_dir())
    python_exe = python_exe or sys.executable
    launcher = launcher or os.path.join(target_dir, "Multi-Stream-Recorder.py")
    pid = os.getpid() if pid is None else pid
    helper_src = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "apply_update.py")
    helper_dst = os.path.join(os.path.dirname(staging_dir), "msr_apply_update.py")
    shutil.copy2(helper_src, helper_dst)
    args = [
        python_exe, helper_dst,
        "--pid", str(pid),
        "--staging", staging_dir,
        "--target", target_dir,
        "--python", python_exe,
        "--launcher", launcher,
    ]
    args += [f"--arg={a}" for a in launcher_args]
    kwargs = {"close_fds": True}
    if os.name == "nt":
        flags = 0
        flags |= getattr(subprocess, "DETACHED_PROCESS", 0)
        flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
        kwargs["creationflags"] = flags
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(args, **kwargs)


def update_error_path(target_dir=None):
    return os.path.join(target_dir or application_dir(), ERROR_FILE)
