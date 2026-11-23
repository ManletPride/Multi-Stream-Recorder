"""Download a GitHub release zipball and stage an in-app update.

The running GUI cannot overwrite its own ``msr/`` tree. ``prepare_update``
fills a staging folder; ``spawn_updater`` must be started *after* this
process has killed its workers, immediately before ``os._exit``.

``plan_app_update`` decides what "Update" means before anything is downloaded:
a normal install gets the release zip, a git checkout is never overwritten
(fast-forward with git when that is safe), and a copy whose files are already
newer than the running process restarts instead of downloading.
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from urllib.parse import urlparse

from msr import __version__
from msr.apply_update import ERROR_FILE
from msr.nowindow import no_window_kwargs
from msr.util import GITHUB_OWNER, GITHUB_REPO, application_dir, version_key

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
    """True if *url* is HTTPS GitHub for this repository (or GitHub's signed redirect host)."""
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme != "https":
        return False
    host = (parsed.hostname or "").lower()
    if host not in ALLOWED_HOSTS:
        return False
    if host == "objects.githubusercontent.com":
        return True
    path = (parsed.path or "").lower()
    repo_slug = f"/{GITHUB_OWNER}/{GITHUB_REPO}/".lower()
    if host == "api.github.com":
        return path.startswith(f"/repos{repo_slug}")
    return path.startswith(repo_slug)


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
            if total + size > MAX_UNCOMPRESSED_BYTES:
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
            member_written = 0
            with zf.open(info, "r") as src, open(dest, "wb") as out:
                while True:
                    chunk = src.read(64 * 1024)
                    if not chunk:
                        break
                    member_written += len(chunk)
                    total += len(chunk)
                    if member_written > MAX_MEMBER_BYTES:
                        raise ValueError(f"update member too large: {rel}")
                    if total > MAX_UNCOMPRESSED_BYTES:
                        raise ValueError("update zip uncompressed size is too large")
                    out.write(chunk)
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
    from msr.tls import https_context
    with urllib.request.urlopen(req, timeout=timeout, context=https_context()) as resp:
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
    if launcher is None:
        gui_launcher = os.path.join(target_dir, "run_msr.pyw")
        if "--headless" not in launcher_args and os.path.isfile(gui_launcher):
            launcher = gui_launcher
        else:
            launcher = os.path.join(target_dir, "Multi-Stream-Recorder.py")
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


# ------------------------------------------------------------------ what "Update" should do

# Shown in the Updates dialog Source / note column, and as the restart prompt title.
GIT_NOTE = "git checkout: updates with git pull"
RESTART_TITLE = "Restart to finish updating"
ZIP_NOTE = "Installed by the app's own updater (the banner / Update in the window)."
BUSY_MESSAGE = "An update is already in progress."
NO_RELEASE_MESSAGE = "No newer version found."
GIT_PULL_COMMAND = "git pull --ff-only"

_VERSION_RE = re.compile(r'(?m)^__version__\s*=\s*["\']([^"\']+)["\']')


def disk_version(target_dir):
    """``__version__`` written in ``msr/__init__.py`` on disk.

    The running process keeps the version it imported. After a git pull (or a
    copied update) without a restart, the file can be newer than that.
    """
    path = os.path.join(target_dir or "", "msr", "__init__.py")
    try:
        with open(path, encoding="utf-8") as fh:
            text = fh.read(65536)
    except OSError:
        return None
    match = _VERSION_RE.search(text)
    return match.group(1).strip() if match else None


def disk_is_newer(target_dir, running_version):
    on_disk = disk_version(target_dir)
    if not on_disk:
        return False
    try:
        return version_key(on_disk) > version_key(running_version)
    except (TypeError, ValueError):
        return False


def is_git_checkout(target_dir):
    """True when ``target_dir`` is a git working tree (``.git`` directory or file).

    A worktree's ``.git`` is a file that points at the real git dir. Either one
    means a release zip would leave the checkout dirty and diverged.
    """
    git_path = os.path.join(target_dir or "", ".git")
    return os.path.isdir(git_path) or os.path.isfile(git_path)


def source_note(target_dir, running_version):
    """Updates-dialog note for this install. Empty for a normal zip install."""
    if disk_is_newer(target_dir, running_version):
        return RESTART_TITLE
    if is_git_checkout(target_dir):
        return GIT_NOTE
    return ""


def banner_text(running_version, target_dir, tag=None):
    """Header-banner text, or None when there is nothing to show.

    A copy already newer on disk says so, even when a release tag is also known:
    the click restarts instead of downloading.
    """
    if disk_is_newer(target_dir, running_version):
        return "  Restart to finish updating — click to restart"
    if tag:
        return f"  v{tag} available — click to install"
    return None


def _as_release(obj):
    """``{"tag", "url", "zipball"}`` from a banner dict or an Updates-dialog row."""
    if not obj:
        return None
    tag = str(obj.get("tag") or obj.get("latest") or "").strip().lstrip("vV")
    if not tag:
        return None
    zipball = obj.get("zipball")
    if zipball is None:
        zipball = obj.get("zipball_url") or ""
    return {"tag": tag, "url": str(obj.get("url") or ""), "zipball": str(zipball or "")}


def choose_release(offered, stored):
    """The release to install.

    The Updates dialog's row wins. It was just read from ``releases/latest``.
    The banner tag is only a fallback: it is whatever the last check stored,
    and a window left open across a publish still has none.
    """
    return _as_release(offered) or _as_release(stored)


def resolved_zipball(tag, zipball):
    """Zip URL for ``tag``, keeping ``zipball`` only when it is GitHub HTTPS."""
    if zipball_url_is_allowed(zipball or ""):
        return zipball
    return zipball_url_for_tag(tag)


def fetch_latest_release(running_version, opener=None):
    """Ask GitHub when no tag was stored. ``(release, "")`` or ``(None, why)``."""
    from msr.depsupdate import _why, latest_msr
    try:
        tag, url, zipball = latest_msr(opener)
    except Exception as exc:
        return None, _why(exc)
    found = _as_release({"tag": tag, "url": url, "zipball": zipball})
    if found is None or version_key(found["tag"]) <= version_key(running_version):
        return None, NO_RELEASE_MESSAGE
    return found, ""


def _git_env():
    from msr.config import clean_subprocess_env
    env = clean_subprocess_env()
    # A credential prompt would block the worker thread with no window to type into.
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GCM_INTERACTIVE"] = "never"
    return env


def _git(target_dir, args, timeout, run=None):
    cmd = ["git", "-C", os.path.abspath(target_dir), *args]
    kwargs = dict(
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=timeout, stdin=subprocess.DEVNULL, env=_git_env(),
    )
    # subprocess.run is called with **no_window_kwargs() itself so the console guard sees it.
    # An injected runner gets the same arguments.
    if run is not None:
        return run(cmd, **kwargs, **no_window_kwargs())
    return subprocess.run(cmd, **kwargs, **no_window_kwargs())


def inspect_git(target_dir, run=None):
    """Whether ``target_dir`` can be updated with ``git pull --ff-only``.

    ``tracks_origin`` means the current branch's upstream remote is ``origin``.
    ``clean`` means ``git status --porcelain`` is empty (ignored files do not count).
    """
    info = {
        "checkout": is_git_checkout(target_dir), "clean": False, "tracks_origin": False,
        "branch": None, "upstream": None, "detached": False, "error": "",
    }
    if not info["checkout"]:
        return info
    try:
        status = _git(target_dir, ["status", "--porcelain"], 30, run)
    except (OSError, subprocess.SubprocessError) as exc:
        info["error"] = f"git could not read this checkout ({exc})."
        return info
    if status.returncode != 0:
        info["error"] = (status.stderr or status.stdout or "git status failed").strip()
        return info
    info["clean"] = (status.stdout or "").strip() == ""
    try:
        branch = _git(target_dir, ["rev-parse", "--abbrev-ref", "HEAD"], 20, run)
        upstream = _git(target_dir, ["rev-parse", "--abbrev-ref", "@{upstream}"], 20, run)
    except (OSError, subprocess.SubprocessError) as exc:
        info["error"] = f"git could not read this checkout ({exc})."
        return info
    name = (branch.stdout or "").strip()
    if branch.returncode != 0 or name in ("", "HEAD"):
        info["detached"] = True
    else:
        info["branch"] = name
    if upstream.returncode == 0:
        tracked = (upstream.stdout or "").strip()
        info["upstream"] = tracked or None
        info["tracks_origin"] = tracked.startswith("origin/") and not info["detached"]
    return info


def git_pull_ff_only(target_dir, run=None):
    """Run ``git pull --ff-only``. ``(ok, output)``. Never prompts."""
    try:
        result = _git(target_dir, ["pull", "--ff-only"], 180, run)
    except subprocess.TimeoutExpired:
        return False, "git pull --ff-only took too long and was stopped."
    except (OSError, subprocess.SubprocessError) as exc:
        return False, f"Could not run git pull --ff-only: {exc}"
    text = "\n".join(part.strip() for part in ((result.stdout or ""), (result.stderr or "")) if part.strip())
    if result.returncode == 0:
        return True, text or "Updated."
    return False, text or "git pull --ff-only failed."


def git_command(target_dir):
    """The command shown when MSR will not pull by itself."""
    return f'git -C "{target_dir}" pull --ff-only'


def _git_why(info):
    if info.get("error"):
        text = info["error"].strip()
        return text if text.endswith(".") else text + "."
    parts = []
    if not info.get("clean"):
        parts.append("The working tree has local changes.")
    if info.get("detached"):
        parts.append("HEAD is detached, so there is no branch to fast-forward.")
    elif not info.get("tracks_origin"):
        if info.get("upstream"):
            parts.append(f"This branch tracks {info['upstream']}, not origin.")
        else:
            parts.append("This branch does not track origin.")
    return " ".join(parts) or "This branch cannot be fast-forwarded from origin."


def _recording_line(recording):
    return "\n\nActive recordings will be stopped first." if recording else ""


def _empty_git():
    return {
        "checkout": False, "clean": False, "tracks_origin": False,
        "branch": None, "upstream": None, "detached": False, "error": "",
    }


def plan_app_update(target_dir, running_version, release=None, *, recording=False, git_info=None):
    """How to update the app in ``target_dir``. No network and no git unless ``git_info`` is omitted
    on a checkout (then one ``git status`` / upstream read).

    ``release`` is ``{"tag", "url", "zipball"}`` (see :func:`choose_release`).

    ``action`` is ``zip`` (download the release), ``restart`` (files on disk are already newer),
    ``git_pull`` (clean branch tracking origin), ``git_manual`` (checkout, but pull is not safe),
    or ``blocked``. ``confirm`` is true when the caller asks yes/no with ``title`` and ``message``
    — that is the only prompt. ``message`` is also what to show when the update cannot start.
    """
    target_dir = os.path.abspath(target_dir or application_dir())
    release = _as_release(release)
    on_disk = disk_version(target_dir)
    base = {
        "action": "blocked", "title": "Cannot update", "message": NO_RELEASE_MESSAGE,
        "confirm": False, "tag": (release or {}).get("tag"), "url": (release or {}).get("url"),
        "zipball": None, "code": "not_found", "command": GIT_PULL_COMMAND,
    }

    if on_disk and version_key(on_disk) > version_key(running_version):
        base.update(
            action="restart", title=RESTART_TITLE, confirm=True, code="",
            tag=on_disk, zipball=None,
            message=(
                f"The files on disk are already v{on_disk} (this copy is still v{running_version}).\n\n"
                "Restart to finish updating?"
                f"{_recording_line(recording)}"
            ),
        )
        return base

    if git_info is None:
        git_info = inspect_git(target_dir) if is_git_checkout(target_dir) else _empty_git()
    if git_info.get("checkout"):
        tag = (release or {}).get("tag")
        newer = bool(tag) and version_key(tag) > version_key(running_version)
        if not newer:
            base["message"] = NO_RELEASE_MESSAGE
            return base
        why = _git_why(git_info)
        command = git_command(target_dir)
        if git_info.get("clean") and git_info.get("tracks_origin") and not git_info.get("error"):
            branch = git_info.get("branch") or "This branch"
            upstream = git_info.get("upstream") or "origin"
            base.update(
                action="git_pull", title="Update with git", confirm=True, code="",
                command=GIT_PULL_COMMAND,
                message=(
                    "This copy is a git checkout, so MSR will not overwrite it with a release zip.\n\n"
                    f"{branch} tracks {upstream}. v{tag} is the latest release. The working tree is clean.\n"
                    f"Run {GIT_PULL_COMMAND} and restart Multi-Stream Recorder?"
                    f"{_recording_line(recording)}"
                ),
            )
            return base
        base.update(
            action="git_manual", title="Git checkout", confirm=False, code="unsupported",
            command=GIT_PULL_COMMAND,
            message=(
                "This copy is a git checkout, so MSR will not overwrite it with a release zip.\n\n"
                f"{why}\n\n"
                f"Update it yourself with {GIT_PULL_COMMAND}, then restart:\n"
                f"{command}"
            ),
        )
        return base

    tag = (release or {}).get("tag")
    if not tag or version_key(tag) <= version_key(running_version):
        base["message"] = NO_RELEASE_MESSAGE
        return base
    try:
        zipball = resolved_zipball(tag, release.get("zipball"))
    except ValueError as exc:
        base.update(message=str(exc), code="invalid")
        return base
    extra = "Active recordings will be stopped first.\n\n" if recording else ""
    base.update(
        action="zip", title="Install update", confirm=True, code="", zipball=zipball,
        message=(
            f"Download v{tag} from GitHub and restart Multi-Stream Recorder?\n\n"
            f"{extra}"
            "config.ini, channels.json, cookies.txt, and recordings stay.\n"
            "The app will quit and reopen on the new version."
        ),
    )
    return base


def relaunch_app(target_dir=None, args=()):
    """Start MSR again. No file copy.

    Call only after worker processes are dead, immediately before the process exits.
    The new process inherits ``MSR_HOME``, so a ``--data-dir`` session keeps its folder
    even though the working directory is the install folder (the same relaunch the
    zip helper uses).
    """
    from msr.apply_update import relaunch
    target_dir = os.path.abspath(target_dir or application_dir())
    launcher = os.path.join(target_dir, "Multi-Stream-Recorder.py")
    relaunch(sys.executable, launcher, target_dir, tuple(args))
