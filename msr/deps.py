"""Optional/required tool detection (yt-dlp, streamlink, ffmpeg, …)."""
import importlib.util
import json
import os
import sys
import subprocess

from msr.nowindow import no_window_kwargs


def interpreter_script_dirs(executable=None):
    """Folders where pip puts console scripts for *this* interpreter.

    A venv's python/pythonw already live in ``Scripts`` (``bin``); a system
    install keeps them in ``<python>/Scripts``. Only existing dirs are returned.
    """
    exe = os.path.abspath(executable or sys.executable or "")
    base = os.path.dirname(exe)
    out = []
    for d in (base, os.path.join(base, "Scripts"), os.path.join(base, "bin")):
        if d and os.path.isdir(d) and d not in out:
            out.append(d)
    return out


def ensure_interpreter_scripts_on_path(environ=None, executable=None):
    """Put this interpreter's script folders first on PATH.

    yt-dlp and streamlink are run as commands. Without this, launching the
    venv's python (or pythonw from a shortcut) without activating the venv
    finds no yt-dlp, or an unrelated system streamlink. Child processes
    inherit the updated PATH. Returns the dirs that were added.
    """
    environ = os.environ if environ is None else environ
    parts = [p for p in environ.get("PATH", "").split(os.pathsep) if p]
    norm = {os.path.normcase(os.path.normpath(p)) for p in parts}
    added = [d for d in interpreter_script_dirs(executable)
             if os.path.normcase(os.path.normpath(d)) not in norm]
    # Prepend even if already present later, so pip-installed tools win.
    front = interpreter_script_dirs(executable)
    rest = [p for p in parts if os.path.normcase(os.path.normpath(p))
            not in {os.path.normcase(os.path.normpath(d)) for d in front}]
    environ["PATH"] = os.pathsep.join(front + rest)
    return added


ensure_interpreter_scripts_on_path()

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    psutil = None
    HAS_PSUTIL = False

try:
    from colorama import init as colorama_init
    colorama_init(autoreset=True)
    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False

# Tray and notification libraries are only needed by the GUI. They are imported on first
# use (module __getattr__ below), so recording workers never load pystray, PIL or plyer.
_LAZY_GUI_NAMES = ("HAS_TRAY", "pystray", "Image", "ImageDraw",
                   "HAS_NOTIFICATIONS", "plyer_notification")


def _load_gui_extras():
    found = {}
    try:
        import pystray as _pystray
        from PIL import Image as _Image, ImageDraw as _ImageDraw
        found.update(HAS_TRAY=True, pystray=_pystray, Image=_Image, ImageDraw=_ImageDraw)
    except Exception:
        # ImportError, or on Linux pystray's Xorg backend raising
        # Xlib.error.DisplayNameError at import time without a display
        found.update(HAS_TRAY=False, pystray=None, Image=None, ImageDraw=None)
    try:
        from plyer import notification as _plyer_notification
        found.update(HAS_NOTIFICATIONS=True, plyer_notification=_plyer_notification)
    except ImportError:
        found.update(HAS_NOTIFICATIONS=False, plyer_notification=None)
    globals().update(found)


def __getattr__(name):
    if name in _LAZY_GUI_NAMES:
        _load_gui_extras()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def check_ytdlp():
    """Check if yt-dlp is installed and return version string or None."""
    try:
        result = subprocess.run(
            ["yt-dlp", "--version"],
            capture_output=True, text=True, timeout=5,
            **no_window_kwargs()
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return None


def check_streamlink():
    """Check if streamlink is installed and return version string or None."""
    try:
        result = subprocess.run(
            ["streamlink", "--version"],
            capture_output=True, text=True, timeout=5,
            **no_window_kwargs()
        )
        if result.returncode == 0:
            # Output is like "streamlink 6.11.0"
            return result.stdout.strip().replace("streamlink ", "")
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return None


def check_ffmpeg(path="ffmpeg"):
    """Check if ffmpeg runs and return its version string or None.

    ``path`` is the ``[Advanced] ffmpeg_path`` value: the bare name ``ffmpeg`` is looked
    up on PATH, an absolute or relative path is run as given.
    """
    path = (path or "ffmpeg").strip() or "ffmpeg"
    try:
        result = subprocess.run(
            [path, "-version"],
            capture_output=True, text=True, timeout=5,
            **no_window_kwargs()
        )
        if result.returncode == 0:
            # First line like "ffmpeg version N-113753-..."
            first_line = result.stdout.split('\n')[0]
            return first_line.replace("ffmpeg version ", "").split(" ")[0] or "unknown"
        return None
    except (OSError, subprocess.TimeoutExpired):
        return None


def check_ffprobe(path="ffprobe"):
    """True if the ffprobe binary at ``path`` runs (pass ``util.ffprobe_from_ffmpeg(...)``)."""
    try:
        result = subprocess.run(
            [path or "ffprobe", "-version"],
            capture_output=True, text=True, timeout=5,
            **no_window_kwargs()
        )
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


# The four "--version" probes run once, in parallel, in the main process; the results are
# handed to recording workers (spawned children inherit the environment) through
# TOOL_VERSIONS_ENV, so a worker start does not launch four more processes.
TOOL_VERSIONS_ENV = "MSR_TOOL_VERSIONS"


def _tool_versions():
    cached = os.environ.get(TOOL_VERSIONS_ENV)
    if cached:
        try:
            found = json.loads(cached)
            if isinstance(found, dict):
                return found
        except ValueError:
            pass
    from concurrent.futures import ThreadPoolExecutor
    checks = {"yt-dlp": check_ytdlp, "streamlink": check_streamlink,
              "ffmpeg": check_ffmpeg, "deno": check_deno}
    with ThreadPoolExecutor(max_workers=len(checks)) as pool:
        futures = {name: pool.submit(fn) for name, fn in checks.items()}
        found = {name: f.result() for name, f in futures.items()}
    os.environ[TOOL_VERSIONS_ENV] = json.dumps(found)
    return found


# The ffmpeg the program is configured to use. Starts as the PATH lookup above;
# ``configure_ffmpeg`` re-points it at ``[Advanced] ffmpeg_path`` once the config is known.
FFMPEG_PATH = "ffmpeg"


def configure_ffmpeg(path):
    """Use ``path`` (from config.ini) as the ffmpeg to check; update the ``FFMPEG_*`` flags.

    The bare default ``ffmpeg`` reuses the result already measured at import, so it does not
    start another process. Returns the version string, or None if it does not run. Read
    these through ``deps.HAS_FFMPEG`` / ``deps.FFMPEG_VERSION`` (not ``from ... import``)
    to see the update.
    """
    global FFMPEG_PATH, FFMPEG_VERSION, HAS_FFMPEG
    path = (path or "ffmpeg").strip() or "ffmpeg"
    if path == FFMPEG_PATH and FFMPEG_VERSION is not None:
        HAS_FFMPEG = True
        return FFMPEG_VERSION
    FFMPEG_PATH = path
    FFMPEG_VERSION = check_ffmpeg(path)
    HAS_FFMPEG = FFMPEG_VERSION is not None
    return FFMPEG_VERSION

# Check if curl_cffi is available for browser impersonation (needed for Cloudflare-protected sites)
HAS_CURL_CFFI = False
try:
    HAS_CURL_CFFI = importlib.util.find_spec("curl_cffi") is not None
except Exception:
    pass

# Check if websocket-client is available (optional — enables Kick push
# notifications so recordings start seconds after a stream goes live,
# instead of waiting for the next poll).  pip install websocket-client
HAS_WEBSOCKET = False
try:
    HAS_WEBSOCKET = importlib.util.find_spec("websocket") is not None
except Exception:
    pass


def check_deno():
    """Check if Deno is installed and return version string or None.

    Deno is an optional but recommended dependency for YouTube recording.
    yt-dlp uses it to solve YouTube's JS-based 'n challenge' (nsig), which
    produces stable HLS segment URLs.  Without it, recordings may drop out
    every ~15 seconds as YouTube serves short-lived URLs to unsolved clients.
    """
    try:
        result = subprocess.run(
            ["deno", "--version"],
            capture_output=True, text=True, timeout=5,
            **no_window_kwargs()
        )
        if result.returncode == 0:
            # First line is like "deno 2.3.1"
            first_line = result.stdout.split('\n')[0]
            return first_line.replace("deno ", "").split(" ")[0]
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return None


_VERSIONS = _tool_versions()
YTDLP_VERSION = _VERSIONS.get("yt-dlp")
HAS_YTDLP = YTDLP_VERSION is not None
STREAMLINK_VERSION = _VERSIONS.get("streamlink")
HAS_STREAMLINK = STREAMLINK_VERSION is not None
FFMPEG_VERSION = _VERSIONS.get("ffmpeg")
HAS_FFMPEG = FFMPEG_VERSION is not None
DENO_VERSION = _VERSIONS.get("deno")
HAS_DENO = DENO_VERSION is not None
