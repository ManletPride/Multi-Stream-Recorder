"""Optional/required tool detection (yt-dlp, streamlink, ffmpeg, …)."""
import importlib
import subprocess

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    psutil = None
    HAS_PSUTIL = False

try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init(autoreset=True)
    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False

try:
    import pystray
    from PIL import Image, ImageDraw
    HAS_TRAY = True
except ImportError:
    HAS_TRAY = False
    pystray = None
    Image = None
    ImageDraw = None
except Exception:
    # On Linux, pystray's Xorg backend tries to connect to a display at
    # import time and raises Xlib.error.DisplayNameError (not ImportError)
    HAS_TRAY = False
    pystray = None
    Image = None
    ImageDraw = None

try:
    from plyer import notification as plyer_notification
    HAS_NOTIFICATIONS = True
except ImportError:
    HAS_NOTIFICATIONS = False
    plyer_notification = None

def check_ytdlp():
    """Check if yt-dlp is installed and return version string or None."""
    try:
        result = subprocess.run(
            ["yt-dlp", "--version"],
            capture_output=True, text=True, timeout=5
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
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            # Output is like "streamlink 6.11.0"
            return result.stdout.strip().replace("streamlink ", "")
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return None


def check_ffmpeg():
    """Check if ffmpeg is installed and return version string or None."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            # First line like "ffmpeg version N-113753-..."
            first_line = result.stdout.split('\n')[0]
            return first_line.replace("ffmpeg version ", "").split(" ")[0]
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return None


YTDLP_VERSION = check_ytdlp()
HAS_YTDLP = YTDLP_VERSION is not None
STREAMLINK_VERSION = check_streamlink()
HAS_STREAMLINK = STREAMLINK_VERSION is not None
FFMPEG_VERSION = check_ffmpeg()
HAS_FFMPEG = FFMPEG_VERSION is not None

# Check if curl_cffi is available for browser impersonation (needed for Cloudflare-protected sites)
HAS_CURL_CFFI = False
try:
    import importlib
    HAS_CURL_CFFI = importlib.util.find_spec("curl_cffi") is not None
except Exception:
    pass

# Check if websocket-client is available (optional — enables Kick push
# notifications so recordings start seconds after a stream goes live,
# instead of waiting for the next poll).  pip install websocket-client
HAS_WEBSOCKET = False
try:
    import importlib
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
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            # First line is like "deno 2.3.1"
            first_line = result.stdout.split('\n')[0]
            return first_line.replace("deno ", "").split(" ")[0]
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return None


DENO_VERSION = check_deno()
HAS_DENO = DENO_VERSION is not None
