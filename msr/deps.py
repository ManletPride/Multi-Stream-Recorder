"""Optional/required tool detection (yt-dlp, streamlink, ffmpeg, …)."""
import importlib.util
import json
import logging
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

# One INFO line when the tray will not run. No traceback: pystray's Xorg backend
# logs "Failed to dock icon" twice (AssertionError) on a desktop with no XEmbed
# tray, which is every Hyprland and GNOME session.
TRAY_DISABLED_INFO = (
    "System tray is off (no working tray on this desktop); the window stays open. "
    "On Hyprland or GNOME, install PyGObject into the venv (or create it with "
    "--system-site-packages) so the AppIndicator tray can load."
)
# Set when Linux has no usable tray. Empty on Windows, and when the tray did load.
TRAY_NOTE = ""


def appindicator_backend_importable() -> bool:
    """True when pystray's appindicator backend can import.

    That backend is StatusNotifierItem (what Hyprland and GNOME bars actually
    show). It needs PyGObject, Gtk 3, and AyatanaAppIndicator3 or AppIndicator3.
    Any failure means "not available" — this must not raise or log a traceback.
    """
    try:
        import gi
        gi.require_version("Gtk", "3.0")
        from gi.repository import Gtk  # noqa: F401
    except Exception:
        return False
    try:
        gi.require_version("AppIndicator3", "0.1")
        from gi.repository import AppIndicator3  # noqa: F401
        return True
    except ValueError:
        pass
    except Exception:
        return False
    try:
        gi.require_version("AyatanaAppIndicator3", "0.1")
        from gi.repository import AyatanaAppIndicator3  # noqa: F401
        return True
    except Exception:
        return False


def prefer_linux_tray_backend(environ=None, platform=None, indicator_available=None):
    """On Linux, point pystray at appindicator when that backend can import.

    Call this before ``import pystray``. Returns ``"appindicator"`` when this
    call set ``PYSTRAY_BACKEND``, otherwise None. An existing value is the
    user's choice and is left alone. Windows and macOS are not touched.
    """
    environ = os.environ if environ is None else environ
    platform = sys.platform if platform is None else platform
    if platform in ("win32", "darwin") or not str(platform).startswith("linux"):
        return None
    if (environ.get("PYSTRAY_BACKEND") or "").strip():
        return None
    if indicator_available is None:
        available = appindicator_backend_importable()
    else:
        available = bool(indicator_available)
    if not available:
        return None
    environ["PYSTRAY_BACKEND"] = "appindicator"
    return "appindicator"


def pystray_backend_name(module) -> str:
    """``xorg`` / ``appindicator`` / ``win32`` / ``darwin`` from the Icon class pystray bound."""
    icon = getattr(module, "Icon", None)
    name = getattr(icon, "__module__", "") or ""
    tail = name.rsplit(".", 1)[-1]
    return tail[1:] if tail.startswith("_") else tail


def xembed_tray_present() -> bool:
    """True when an XEmbed tray owns ``_NET_SYSTEM_TRAY_S<screen>``.

    Hyprland has no such owner (its bar is StatusNotifierItem). A missing
    Xlib or display is "no tray", not an error.
    """
    try:
        import Xlib.X
        import Xlib.display
    except Exception:
        return False
    display = None
    try:
        display = Xlib.display.Display()
        screen = display.get_default_screen()
        atom = display.intern_atom("_NET_SYSTEM_TRAY_S%d" % screen)
        owner = display.get_selection_owner(atom)
        return owner != Xlib.X.NONE
    except Exception:
        return False
    finally:
        if display is not None:
            try:
                display.close()
            except Exception:
                pass


def tray_can_start(has_tray: bool, backend: str, xembed_present: bool) -> bool:
    """False when the tray libraries are missing, or Xorg has nothing to dock into.

    Other backends (win32, darwin, appindicator, gtk) are not second-guessed here.
    """
    if not has_tray:
        return False
    if backend == "xorg" and not xembed_present:
        return False
    return True


def hide_to_tray_decision(ready: bool, dock_failed: bool, icon_alive: bool) -> str:
    """``withdraw`` only when a tray icon is up and has not failed to dock.

    Anything else is ``stay``: minimize and the tray's Hide action must not
    put the window into a tray that is not there.
    """
    if ready and icon_alive and not dock_failed:
        return "withdraw"
    return "stay"


class DockFailureFilter(logging.Filter):
    """Drop pystray's ``Failed to dock icon`` error, traceback included."""

    MESSAGE = "Failed to dock icon"

    def __init__(self):
        super().__init__()
        self.failed = False
        self.dropped = 0

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name.startswith("pystray") and record.getMessage() == self.MESSAGE:
            self.failed = True
            self.dropped += 1
            return False
        return True


def install_dock_filter(dock_filter: DockFailureFilter) -> None:
    """Attach *dock_filter* to the pystray loggers and to handlers already installed.

    A filter on a parent logger does not see records logged on ``pystray._base``,
    so the emitting loggers get it too. Handlers that already exist (the log file,
    the Log tab) also drop it, in case a future pystray logs under another name.
    """
    for name in ("pystray", "pystray._base", "pystray._xorg"):
        logging.getLogger(name).addFilter(dock_filter)
    for handler in logging.getLogger().handlers:
        handler.addFilter(dock_filter)


def _load_gui_extras():
    global TRAY_NOTE
    found = {}
    try:
        prefer_linux_tray_backend()  # before pystray: it reads PYSTRAY_BACKEND at import
    except Exception:
        pass
    try:
        import pystray as _pystray
        from PIL import Image as _Image, ImageDraw as _ImageDraw
        has_tray = True
        if sys.platform.startswith("linux"):
            backend = pystray_backend_name(_pystray)
            xembed = xembed_tray_present() if backend == "xorg" else True
            if not tray_can_start(True, backend, xembed):
                has_tray = False
                TRAY_NOTE = TRAY_DISABLED_INFO
        found.update(HAS_TRAY=has_tray, pystray=_pystray, Image=_Image, ImageDraw=_ImageDraw)
    except Exception:
        # ImportError, or on Linux pystray's Xorg backend raising
        # Xlib.error.DisplayNameError at import time without a display.
        # No traceback. The GUI logs TRAY_NOTE once if a tray was wanted.
        if sys.platform.startswith("linux"):
            TRAY_NOTE = TRAY_DISABLED_INFO
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
