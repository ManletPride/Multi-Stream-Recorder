r"""
Multi-Stream Recorder
=====================

A desktop application for simultaneously recording live streams from
Kick, Twitch, YouTube, and any site supported by yt-dlp.

Records live streams using:
  • yt-dlp for Kick, YouTube, and custom URLs
  • streamlink for Twitch (optimized, fast, ad-blocking)
  • ffmpeg for remuxing raw .ts recordings to .mp4

Features:
  • Concurrent multi-channel recording with per-channel worker processes
  • Smart polling with jitter (avoids rate limiting) and exponential backoff on errors
  • Fast reconnect (15s polling for 3 minutes after a stream drops)
  • Dark/light theme GUI with system tray, notifications, and log viewer
  • Channel checkboxes — keep a roster, enable/disable per session
  • Custom URL support — any yt-dlp-compatible site or direct .m3u8 links
  • Cookie validation with auth token expiry monitoring
  • Automatic .ts → .mp4 remux with metadata sidecar files
  • Configurable output filenames, polling intervals, and cleanup
  • Headless/CLI mode for background operation (--headless flag)
  • Version update checking via GitHub releases API

YouTube URL formats supported:
  • @username (e.g., @KirscheVerstahl)
  • Channel ID (e.g., UCxxxxxxxxxxxxxxxx)
  • Direct video URL (e.g., watch?v=FaE2vM9h0ok or FaE2vM9h0ok)

Author: ManletPride
Built with assistance from Claude (Anthropic) and Grok (xAI).

License: MIT
Repository: https://github.com/ManletPride/Multi-Stream-Recorder
"""

__version__ = "1.5.1"

# ============ STDLIB IMPORTS ============
import subprocess
import time
import datetime
import os
import json
import logging
import multiprocessing as mp
import random
import threading
import shutil
import sys
import signal
import argparse
import configparser
from pathlib import Path

# ============ THIRD-PARTY IMPORTS ============
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
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

try:
    from plyer import notification as plyer_notification
    HAS_NOTIFICATIONS = True
except ImportError:
    HAS_NOTIFICATIONS = False

# ============ DEPENDENCY AVAILABILITY CHECKS ============
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


# ============ CONFIGURATION MANAGEMENT ============
class Config:
    """Manages application configuration from config.ini"""

    DEFAULT_CONFIG = {
        'Paths': {
            'streams_dir': 'E:\\Streams',
            'channels_file': 'channels.json',
            'cookies_file': '',  # auto-detected if empty
        },
        'Recording': {
            'quality': 'best',
            'max_record_hours': '12.0',
            'max_file_size_gb': '8.0',   # split recording when file exceeds this size (0 = disabled)
            'min_disk_space_gb': '5.0',
            'min_file_size_mb': '2.0',
            # Pattern tokens: {username}, {platform}, {date}, {time}, {timestamp}, {title}
            'filename_pattern': '{username}_{timestamp}',
        },
        'Timeouts': {
            'stream_check_timeout': '30',
            'ffmpeg_timeout': '600',
            'poll_interval_minutes': '3',     # base interval for offline stream checks
            'poll_jitter_percent': '20',      # random ±% added to each poll (avoids synchronized bursts)
            'error_backoff_max_minutes': '15', # max delay after server errors (backoff resets on success)
            'reconnect_grace_minutes': '3',   # fast 15s polling after a stream drops unexpectedly
            'file_creation_timeout': '60',    # seconds to wait for output file to appear
        },
        'Cleanup': {
            'auto_purge_days': '7',          # delete PendingDeletion files older than N days (0=disabled)
            'purge_on_startup': 'true',
        },
        'Advanced': {
            'verbose': 'true',
            'streamlink_debug': 'false',
            'ffmpeg_path': 'ffmpeg',
            'concurrent_fragments': '3',
        },
        'GUI': {
            'dark_mode': 'true',
            'minimize_to_tray': 'true',
            'notifications': 'true',
            'window_state_file': 'window_state.json',
        },
        'Fishtank': {
            # Email and password for fishtank.live login.
            # Used by the recorder to obtain a fresh MistServer JWT via
            # POST /v1/auth/log-in — the only reliable auth method since
            # the GET /v1/auth session-check endpoint returns null once the
            # 15-minute Supabase access token expires.
            # Leave blank to fall back to cookie-jar auth (requires fresh cookies).
            'email': '',
            'password': '',
        },
    }

    def __init__(self, config_file='config.ini'):
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self._load_or_create()

    def _load_or_create(self):
        if os.path.exists(self.config_file):
            self.config.read(self.config_file)
            updated = False
            for section, options in self.DEFAULT_CONFIG.items():
                if not self.config.has_section(section):
                    self.config.add_section(section)
                    updated = True
                for key, value in options.items():
                    if not self.config.has_option(section, key):
                        self.config.set(section, key, value)
                        updated = True
            if updated:
                with open(self.config_file, 'w') as f:
                    self.config.write(f)
        else:
            for section, options in self.DEFAULT_CONFIG.items():
                self.config.add_section(section)
                for key, value in options.items():
                    self.config.set(section, key, value)
            with open(self.config_file, 'w') as f:
                self.config.write(f)
            logging.info(f"Created default config file: {self.config_file}")

    # Convenience accessors
    def get(self, section, key, fallback=None):
        return self.config.get(section, key, fallback=fallback)

    def getfloat(self, section, key, fallback=None):
        return self.config.getfloat(section, key, fallback=fallback)

    def getint(self, section, key, fallback=None):
        return self.config.getint(section, key, fallback=fallback)

    def getboolean(self, section, key, fallback=None):
        return self.config.getboolean(section, key, fallback=fallback)


# ────────────────────────────────────────────────
#          Constants & Helpers
# ────────────────────────────────────────────────

PENDING_DELETION_FOLDER = "PendingDeletion"


def validate_startup(config):
    """Validate dependencies and config at startup.

    Returns (errors: list[str], warnings: list[str]).
    Errors are fatal — the program cannot work.  Warnings are non-fatal
    but the user should be aware.
    """
    errors = []
    warnings = []

    # ── Required dependencies ──
    if not HAS_FFMPEG:
        errors.append(
            "ffmpeg not found in PATH.  ffmpeg is required for remuxing recordings.\n"
            "  Install: https://ffmpeg.org/download.html\n"
            "  Windows: download, extract, add bin/ folder to system PATH"
        )

    if not HAS_YTDLP:
        errors.append(
            "yt-dlp not found in PATH.  yt-dlp is required for Kick and YouTube recording.\n"
            "  Install: pip install yt-dlp"
        )

    if not HAS_STREAMLINK:
        warnings.append(
            "streamlink not found in PATH.  Twitch recording will not work.\n"
            "  Install: pip install streamlink"
        )

    # ── Config validation ──
    streams_dir = config.get('Paths', 'streams_dir')

    # Check if streams_dir drive exists (Windows)
    if os.name == 'nt' and len(streams_dir) >= 2 and streams_dir[1] == ':':
        drive = streams_dir[:3]  # e.g. "E:\"
        if not os.path.exists(drive):
            errors.append(
                f"Drive '{drive}' does not exist.  Check streams_dir in config.ini.\n"
                f"  Current value: {streams_dir}"
            )

    # Try to create streams_dir (catches permission errors early)
    if not errors:  # only if drive exists
        try:
            os.makedirs(streams_dir, exist_ok=True)
        except PermissionError:
            errors.append(
                f"Permission denied creating '{streams_dir}'.\n"
                f"  Check that you have write access to this location."
            )
        except OSError as e:
            errors.append(f"Cannot create streams directory '{streams_dir}': {e}")

    # Validate numeric config values
    try:
        max_hours = config.getfloat('Recording', 'max_record_hours')
        if max_hours <= 0:
            warnings.append("max_record_hours is <= 0 — recordings will have no time limit")
    except (ValueError, configparser.Error):
        warnings.append("max_record_hours is not a valid number — using default (12)")

    try:
        max_size = config.getfloat('Recording', 'max_file_size_gb')
        if max_size < 0:
            warnings.append("max_file_size_gb is negative — using default (8 GB)")
        elif max_size == 0:
            warnings.append("max_file_size_gb is 0 — file size splitting is disabled")
    except (ValueError, configparser.Error):
        warnings.append("max_file_size_gb is not a valid number — using default (8 GB)")

    try:
        min_space = config.getfloat('Recording', 'min_disk_space_gb')
        if min_space < 0:
            warnings.append("min_disk_space_gb is negative — disk space check disabled")
    except (ValueError, configparser.Error):
        warnings.append("min_disk_space_gb is not a valid number — using default (5)")

    # Check channels file
    channels_file = config.get('Paths', 'channels_file')
    if not os.path.exists(channels_file):
        warnings.append(
            f"Channels file '{channels_file}' not found — will be created on first use.\n"
            "  Add channels via the GUI or create the file manually."
        )
    else:
        try:
            with open(channels_file, 'r') as f:
                data = json.load(f)
            if not isinstance(data, list):
                warnings.append(f"'{channels_file}' should contain a JSON array, e.g. [\"twitch:zackrawrr\", \"betty-fae\"]")
            elif len(data) == 0:
                warnings.append("No channels configured.  Add channels via the GUI or edit channels.json.")
        except json.JSONDecodeError as e:
            warnings.append(f"'{channels_file}' contains invalid JSON: {e}")

    # ── Optional dependency notes ──
    if not HAS_PSUTIL:
        warnings.append("psutil not installed — process cleanup may be less reliable.  Install: pip install psutil")

    if not HAS_DENO:
        # Only warn if YouTube channels are actually enabled — no point warning users
        # who record Twitch/Kick only.
        youtube_enabled = False
        channels_file = config.get('Paths', 'channels_file')
        if os.path.exists(channels_file):
            try:
                with open(channels_file, 'r') as f:
                    ch_data = json.load(f)
                youtube_enabled = any(
                    ch.get('enabled', False) and
                    (str(ch.get('name', '')).startswith('youtube:') or
                     not any(str(ch.get('name', '')).startswith(p)
                             for p in ('twitch:', 'custom:', 'rumble:')))
                    for ch in ch_data if isinstance(ch, dict)
                )
            except Exception:
                pass
        if youtube_enabled:
            warnings.append(
                "Deno not found in PATH — YouTube n-challenge solving will be degraded.\n"
                "  Without Deno, yt-dlp cannot solve YouTube's JS challenge, which may cause\n"
                "  recordings to drop out every ~15s as YouTube serves short-lived stream URLs.\n"
                "  Install Deno: https://deno.com  (then restart MSR)"
            )

    return errors, warnings


def validate_channel_name(name, platform, existing_channels):
    """Validate a channel name before adding.

    Returns (is_valid: bool, error_message: str | None).
    """
    if not name:
        return False, "Channel name cannot be empty."

    # Custom platform: user MUST paste a full URL
    if platform == "custom":
        if not (name.startswith('http://') or name.startswith('https://')):
            return False, "Custom channels require a full URL.\n  Example: https://rumble.com/some-stream.html"
        if len(name) > 500:
            return False, "URL is too long (max 500 characters)."
        if not HAS_YTDLP:
            return False, "Custom URLs require yt-dlp, which is not installed.\n  Install: pip install yt-dlp"
        # Check duplicate
        ch_key = f"custom:{name}"
        if ch_key in existing_channels:
            return False, "This URL is already in the list."
        return True, None

    if len(name) > 100:
        return False, "Channel name is too long (max 100 characters)."

    # Check for obviously invalid characters
    invalid_chars = set(' \t\n\r<>"|?*')
    found = invalid_chars & set(name)
    if found:
        return False, f"Channel name contains invalid characters: {', '.join(repr(c) for c in found)}"

    # Check for URL pasting (common mistake — except for custom platform)
    if name.startswith('http://') or name.startswith('https://'):
        return False, "Paste just the channel name, not the full URL.\n  Example: 'asmongold' instead of 'https://kick.com/asmongold'\n\n  For arbitrary URLs, select 'custom' as the platform."

    # Build full channel key and check for duplicate
    ch_key = f"{platform}:{name}" if platform != "kick" else name
    if ch_key in existing_channels:
        return False, f"'{ch_key}' is already in the list."

    # Platform-specific warnings
    if platform == "twitch" and not HAS_STREAMLINK:
        return False, "Cannot add Twitch channels — streamlink is not installed.\n  Install: pip install streamlink"

    if platform in ["kick", "youtube", "rumble"] and not HAS_YTDLP:
        return False, f"Cannot add {platform.title()} channels — yt-dlp is not installed.\n  Install: pip install yt-dlp"

    if platform == "fishtank":
        normalised = name.lower().replace(" ", "").replace("-", "")
        all_aliases = set(FishtankAuth.CAMERA_ALIASES.keys()) | set(FishtankAuth.CAMERA_ALIASES.values())
        if normalised not in all_aliases and name not in all_aliases:
            known = ", ".join(sorted(
                k for k in FishtankAuth.CAMERA_ALIASES
                if not k.endswith("-5")
            ))
            return False, (
                f"Unknown fishtank camera '{name}'.\n"
                f"  Known names: {known}\n"
                f"  Or use the raw stream ID (e.g. dirc-5, dmrm-5)"
            )
        ch_key = f"fishtank:{name}"
        if ch_key in existing_channels:
            return False, f"'{ch_key}' is already in the list."

    return True, None


def human_size(size_bytes):
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KiB', 'MiB', 'GiB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TiB"


def format_elapsed(seconds):
    """Format elapsed time in H:MM:SS or M:SS format."""
    if seconds < 0:
        return "???"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h:d}:{m:02d}:{s:02d}"
    return f"{m:d}:{s:02d}"


def text_progress_bar(percentage, width=10):
    """Create a text-based progress bar."""
    filled = int(width * percentage / 100)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {int(percentage)}%"


def jittered_sleep(base_seconds, jitter_pct=20):
    """Return a sleep duration with random jitter applied.

    Example: base=180s, jitter_pct=20 → random value in [144, 216].
    This prevents synchronized request bursts when monitoring multiple channels.
    """
    jitter_fraction = jitter_pct / 100.0
    low = base_seconds * (1 - jitter_fraction)
    high = base_seconds * (1 + jitter_fraction)
    return random.uniform(low, high)


def check_disk_space(path, min_gb=5.0):
    """Return (has_enough, free_gb)."""
    try:
        stat = shutil.disk_usage(path)
        free_gb = stat.free / (1024**3)
        return free_gb >= min_gb, free_gb
    except Exception as e:
        logging.error(f"Failed to check disk space: {e}")
        return True, 0  # assume OK if check fails


# ── GitHub repository for version checks ──
# Update these before publishing to GitHub
GITHUB_OWNER = "ManletPride"   # ← Author's GitHub username
GITHUB_REPO = "Multi-Stream-Recorder"   # ← GitHub repo name


def check_for_updates(current_version, callback=None):
    """Check GitHub releases for a newer version.  Runs in a background thread.

    Completely non-blocking and failure-safe.  If the check fails for ANY
    reason (no internet, 404, timeout, rate-limited, JSON error), it silently
    does nothing.

    Args:
        current_version: The running version string (e.g. "3.2b")
        callback: Optional function(latest_tag, release_url) called on the
                  main thread (via root.after) if a newer version is found.
    """
    def _check():
        try:
            import urllib.request
            import urllib.error

            url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/releases/latest"
            req = urllib.request.Request(url, headers={
                'Accept': 'application/vnd.github+json',
                'User-Agent': f'MultiStreamRecorder/{current_version}',
            })

            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            latest_tag = data.get('tag_name', '').lstrip('vV')
            release_url = data.get('html_url', '')

            if not latest_tag or not release_url:
                return

            # Compare versions: strip non-numeric suffixes for comparison
            # "3.2b" → (3, 2), "3.10" → (3, 10)
            def _version_tuple(v):
                import re
                nums = re.findall(r'\d+', v)
                return tuple(int(n) for n in nums) if nums else (0,)

            current_t = _version_tuple(current_version)
            latest_t = _version_tuple(latest_tag)

            if latest_t > current_t and callback:
                callback(latest_tag, release_url)

        except Exception:
            pass  # silently ignore ALL failures

    t = threading.Thread(target=_check, daemon=True, name="version-check")
    t.start()


def find_cookies_file(config):
    """Locate a cookies.txt file.  Checks config value first, then common locations."""
    explicit = config.get('Paths', 'cookies_file', fallback='')
    if explicit and os.path.isfile(explicit):
        return explicit
    streams_dir = config.get('Paths', 'streams_dir')
    for candidate in [
        os.path.join(streams_dir, 'cookies.txt'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookies.txt'),
    ]:
        if os.path.isfile(candidate):
            return candidate
    return None


def validate_cookies(cookies_path):
    """Parse and validate a Netscape-format cookies.txt file.

    Returns a dict with:
        'valid': bool — file exists and has valid cookie lines
        'path': str — resolved path
        'domains': list[str] — unique domains found
        'total_cookies': int — number of cookie entries
        'auth_expiry': datetime | None — soonest expiry among auth cookies
        'has_expired_auth': bool — True if critical auth cookies have expired
        'expired_domains': list[str] — domains with expired auth cookies
        'warnings': list[str] — human-readable issues
    """
    result = {
        'valid': False, 'path': cookies_path or '', 'domains': [],
        'total_cookies': 0, 'auth_expiry': None, 'has_expired_auth': False,
        'expired_domains': [], 'warnings': [],
    }
    if not cookies_path or not os.path.isfile(cookies_path):
        result['warnings'].append("No cookies.txt file found")
        return result

    # Auth cookies that actually matter for stream access.
    # Only these affect the indicator color and "expires in Xd" display.
    AUTH_COOKIES = {
        # Twitch
        'auth-token', 'api_token', 'login', 'persistent',
        # YouTube (logged-in features)
        'VISITOR_INFO1_LIVE', 'VISITOR_PRIVACY_METADATA',
        '__Secure-ROLLOUT_TOKEN', '__Secure-1PSID', '__Secure-3PSID',
        'SID', 'HSID', 'SSID', 'APISID', 'SAPISID', 'LOGIN_INFO',
        # Kick
        'session_token', 'cookie_preferences_set_v1',
        # Fansly
        'f-s-c',
        # Chaturbate
        'sbr',
        # Rumble
        'a_s',
        # Fishtank
        'sb-wcsaaupukpdmqdjcgaoo-auth-token',
    }

    now = time.time()
    domains = set()
    meaningful_domains = set()   # domains with at least one non-zero-expiry cookie
    expired_auth_domains = set()
    auth_earliest = None
    count = 0
    has_netscape_header = False

    try:
        with open(cookies_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if line.startswith('# Netscape HTTP Cookie File') or line.startswith('# HTTP Cookie File'):
                    has_netscape_header = True
                    continue
                if not line:
                    continue
                # HttpOnly cookies are prefixed '#HttpOnly_' in the Netscape format —
                # strip the prefix so the line parses normally.  Plain comments skip.
                if line.startswith('#HttpOnly_'):
                    line = line[len('#HttpOnly_'):]
                elif line.startswith('#'):
                    continue

                parts = line.split('\t')
                if len(parts) < 7:
                    continue  # skip malformed lines

                domain = parts[0].lstrip('.')
                cookie_name = parts[5] if len(parts) > 5 else ''
                try:
                    expiry = int(parts[4])
                except (ValueError, IndexError):
                    expiry = 0

                domains.add(domain)
                count += 1

                # A domain only counts as "meaningful" (green dot) if it has at least
                # one cookie with a real expiry — expiry=0 means a session/tracking
                # cookie that any anonymous visit produces (e.g. yt-dlp writing back
                # "country=us"), which has no authentication value.
                if expiry > 0:
                    meaningful_domains.add(domain)

                # Only track expiry for auth cookies (session=0 is ignored)
                if expiry > 0 and cookie_name in AUTH_COOKIES:
                    if expiry < now:
                        expired_auth_domains.add(domain)
                    elif auth_earliest is None or expiry < auth_earliest:
                        auth_earliest = expiry

        result['total_cookies'] = count
        result['domains'] = sorted(domains)
        result['meaningful_domains'] = sorted(meaningful_domains)
        result['expired_domains'] = sorted(expired_auth_domains)
        result['has_expired_auth'] = len(expired_auth_domains) > 0

        if auth_earliest:
            result['auth_expiry'] = datetime.datetime.fromtimestamp(auth_earliest)

        if count == 0:
            result['warnings'].append("cookies.txt exists but contains no valid cookie entries")
        elif not has_netscape_header:
            result['warnings'].append("cookies.txt missing Netscape header — may not work with yt-dlp")
            result['valid'] = count > 0
        else:
            result['valid'] = True

        if expired_auth_domains:
            domains_str = ", ".join(list(expired_auth_domains)[:3])
            result['warnings'].append(f"Expired auth cookies for: {domains_str}")

            if any('youtube' in d for d in expired_auth_domains):
                result['warnings'].append("YouTube auth cookies expired — may cause 403/503 errors")
            if any('kick' in d for d in expired_auth_domains):
                result['warnings'].append("Kick auth cookies expired — may affect stream detection")
            if any('twitch' in d for d in expired_auth_domains):
                result['warnings'].append("Twitch auth cookies expired — subscriber-only streams may fail")

    except Exception as e:
        result['warnings'].append(f"Failed to read cookies file: {e}")

    return result


def get_cookie_domain_for_channel(channel_key):
    """Return the cookie domain to match for a given channel key.

    Examples:
        'twitch:saruei'                          → 'twitch.tv'
        'youtube:@OhDough'                       → 'youtube.com'
        'xqc'  (bare Kick name)                  → 'kick.com'
        'custom:https://fansly.com/live/YuukoVT' → 'fansly.com'
        'custom:https://chaturbate.com/alice/'   → 'chaturbate.com'
    """
    from urllib.parse import urlparse
    if channel_key.startswith('twitch:'):
        return 'twitch.tv'
    elif channel_key.startswith('youtube:'):
        return 'youtube.com'
    elif channel_key.startswith('fishtank:'):
        return 'fishtank.live'
    elif channel_key.startswith('custom:'):
        url = channel_key.split(':', 1)[1]
        try:
            host = (urlparse(url).hostname or '').lower()
            if host.startswith('www.'):
                host = host[4:]
            return host  # e.g. 'fansly.com', 'chaturbate.com', 'rumble.com'
        except Exception:
            return None
    else:
        return 'kick.com'  # bare name = Kick


# Cookie status constants
COOKIE_STATUS_PRESENT = 'present'   # valid, non-expired cookie found
COOKIE_STATUS_MISSING = 'missing'   # no cookie for this domain at all
COOKIE_STATUS_EXPIRED = 'expired'   # domain present but auth cookie expired
COOKIE_STATUS_UNKNOWN = 'unknown'   # can't determine (no cookies file, etc.)

COOKIE_DOT_COLORS = {
    COOKIE_STATUS_PRESENT: '#4CAF50',   # green
    COOKIE_STATUS_MISSING: '#F44336',   # red
    COOKIE_STATUS_EXPIRED: '#FF9800',   # orange
    COOKIE_STATUS_UNKNOWN: '#888888',   # grey
}


def get_cookie_status_for_channel(channel_key, cookie_info):
    """Return one of the COOKIE_STATUS_* constants for this channel."""
    if not cookie_info or not cookie_info.get('valid'):
        return COOKIE_STATUS_UNKNOWN
    target = get_cookie_domain_for_channel(channel_key)
    if not target:
        return COOKIE_STATUS_UNKNOWN

    def _matches(cookie_domain, target_domain):
        return cookie_domain == target_domain or cookie_domain.endswith('.' + target_domain)

    domains = cookie_info.get('domains', [])
    meaningful_domains = cookie_info.get('meaningful_domains', domains)  # fallback for old cache
    expired_domains = cookie_info.get('expired_domains', [])

    # A domain in `domains` but not `meaningful_domains` only has expiry=0 cookies
    # (anonymous tracking cookies written by yt-dlp) — treat as missing.
    if not any(_matches(d, target) for d in meaningful_domains):
        return COOKIE_STATUS_MISSING
    if any(_matches(d, target) for d in expired_domains):
        return COOKIE_STATUS_EXPIRED
    return COOKIE_STATUS_PRESENT


def extract_domain_from_url(url):
    """Extract a clean domain name from a URL for use as a folder/display name.

    Example: 'https://www.rumble.com/some-stream' → 'rumble'
    """
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.hostname or ''
        # Strip www. and common TLDs for a clean name
        host = host.lower().replace('www.', '')
        # Use the main domain part (e.g. 'rumble' from 'rumble.com')
        parts = host.split('.')
        if len(parts) >= 2:
            return parts[-2]  # e.g. 'rumble' from 'rumble.com'
        return host or 'custom'
    except Exception:
        return 'custom'


def parse_custom_url(url):
    """Parse a custom URL into (platform_name, channel_name) for display.

    Tries to extract the site brand and the username/channel from the path.
    Falls back gracefully to ('unknown', 'unknown') for unparseable URLs.

    Examples:
        'https://chaturbate.com/tatumwest0/'     → ('chaturbate', 'tatumwest0')
        'https://odysee.com/@SomeChannel:5/live' → ('odysee', 'SomeChannel')
        'https://example.com/stream.m3u8'        → ('example', 'unknown')
    """
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = (parsed.hostname or '').lower().replace('www.', '')

        # Platform = main domain name (e.g. 'chaturbate' from 'chaturbate.com')
        host_parts = host.split('.')
        platform = host_parts[-2] if len(host_parts) >= 2 else (host or 'unknown')

        # Channel = first meaningful path segment that looks like a username
        path_parts = [s for s in parsed.path.split('/') if s]

        # Common path segments that are NOT usernames
        SKIP_SEGMENTS = {'live', 'stream', 'watch', 'embed', 'channel', 'user',
                         'c', 's', 'v', 'video', 'videos', 'clip', 'clips',
                         'category', 'browse', 'directory', 'search', 'about'}

        channel = None
        for seg in path_parts:
            # Skip file-like segments (stream.m3u8, page.html, etc.)
            if '.' in seg:
                ext = seg.rsplit('.', 1)[1].lower()
                if ext in ('m3u8', 'html', 'htm', 'php', 'asp', 'aspx', 'js', 'json', 'xml', 'ts'):
                    continue

            # Clean up the segment
            clean = seg.strip('/').lstrip('@')

            # Handle Odysee-style claim IDs: @Channel:5 → Channel
            if ':' in clean:
                clean = clean.split(':')[0]

            if clean.lower() not in SKIP_SEGMENTS and len(clean) > 0:
                channel = clean
                break

        return (platform, channel or 'unknown')

    except Exception:
        return ('unknown', 'unknown')


def build_filename(pattern, username, platform, title=None):
    """Build output filename from pattern and metadata."""
    now = datetime.datetime.now()
    replacements = {
        '{username}': username,
        '{platform}': platform,
        '{date}': now.strftime('%Y%m%d'),
        '{time}': now.strftime('%H%M%S'),
        '{timestamp}': now.strftime('%Y%m%d_%H%M%S'),
        '{title}': _sanitize_filename(title) if title else 'untitled',
    }
    result = pattern
    for token, value in replacements.items():
        result = result.replace(token, value)
    return _sanitize_filename(result)


def _sanitize_filename(name):
    """Remove characters that are invalid in filenames."""
    if not name:
        return 'untitled'
    # Replace common bad chars
    for ch in r'<>:"/\|?*':
        name = name.replace(ch, '_')
    # Collapse multiple underscores / strip
    while '__' in name:
        name = name.replace('__', '_')
    return name.strip('. _')[:200]  # cap length


# ────────────────────────────────────────────────
#          Process Management
# ────────────────────────────────────────────────

def kill_process_tree(pid, logger=None):
    """Kill a process and all its children.

    Uses psutil when available (cross-platform, reliable).
    Falls back to taskkill /T on Windows or os.kill on Unix.
    """
    if HAS_PSUTIL:
        try:
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)
            # Kill children first, then parent
            for child in children:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            try:
                parent.kill()
            except psutil.NoSuchProcess:
                pass
            # Wait for all to finish
            gone, alive = psutil.wait_procs(children + [parent], timeout=5)
            if alive and logger:
                logger.warning(f"Some processes still alive after kill: {[p.pid for p in alive]}")
            elif logger:
                logger.info(f"Killed process tree for PID {pid} ({len(children)} children)")
        except psutil.NoSuchProcess:
            pass  # already gone
        except Exception as e:
            if logger:
                logger.warning(f"psutil tree kill failed for PID {pid}: {e}")
            # Fall through to OS-level fallback
            _kill_process_tree_fallback(pid, logger)
        return

    _kill_process_tree_fallback(pid, logger)


def _kill_process_tree_fallback(pid, logger=None):
    """Fallback process tree kill without psutil."""
    if os.name == 'nt':
        try:
            result = subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(pid)],
                capture_output=True, text=True, timeout=10
            )
            if logger:
                if result.returncode == 0:
                    logger.info(f"Killed process tree for PID {pid}")
                elif "not found" not in result.stderr.lower():
                    logger.warning(f"taskkill returned {result.returncode}: {result.stderr.strip()}")
        except Exception as e:
            if logger:
                logger.warning(f"taskkill failed for PID {pid}: {e}")
            try:
                os.kill(pid, 9)
            except Exception:
                pass
    else:
        try:
            os.killpg(os.getpgid(pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception:
            try:
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass


def kill_orphan_ffmpeg_processes(logger=None):
    """Safety net: kill orphaned ffmpeg processes that are downloading streams.

    Only called during shutdown.  Skips local remux operations.
    """
    if HAS_PSUTIL:
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and 'ffmpeg' in proc.info['name'].lower():
                        cmdline = ' '.join(proc.info.get('cmdline') or [])
                        if 'https://' in cmdline or 'http://' in cmdline:
                            if logger:
                                logger.info(f"Killing orphaned ffmpeg (PID {proc.pid}) — was downloading a stream")
                            proc.kill()
                        elif logger:
                            logger.info(f"Skipping ffmpeg PID {proc.pid} — appears to be local remux")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception as e:
            if logger:
                logger.warning(f"Orphan ffmpeg check failed: {e}")
        return

    # Fallback for Windows without psutil
    if os.name != 'nt':
        return
    try:
        result = subprocess.run(
            ['tasklist', '/FI', 'IMAGENAME eq ffmpeg.exe', '/FO', 'CSV', '/NH'],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0 or 'ffmpeg' not in result.stdout.lower():
            return
        if logger:
            logger.info("Found ffmpeg processes — cannot inspect command lines without psutil, skipping orphan kill")
    except Exception:
        pass


# ────────────────────────────────────────────────
#          Logging
# ────────────────────────────────────────────────

def setup_logging(root_path):
    """Setup logging for main process."""
    os.makedirs(root_path, exist_ok=True)
    log_file = os.path.join(root_path, "stream_recorder.log")

    formatter = logging.Formatter(
        "%(asctime)s [PID %(process)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(file_handler)
    logging.root.addHandler(console_handler)


def setup_child_logging(root_path, channel_key):
    """Setup logging for child process."""
    log_file = os.path.join(root_path, "stream_recorder.log")

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [PID %(process)d] [%(channel)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(channel)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))

    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(file_handler)
    logging.root.addHandler(console_handler)

    return logging.LoggerAdapter(logging.getLogger(), {'channel': channel_key})


# ────────────────────────────────────────────────
#          Stream Checking Functions
# ────────────────────────────────────────────────

def check_stream_kick_api(channel_name, logger, timeout=15, cookies_file=None):
    """Check if a Kick stream is live using streamlink's Kick plugin.

    Streamlink 8.x has a built-in JS challenge solver for Kick's Cloudflare
    protection, which is far more reliable than yt-dlp or curl_cffi for
    bypassing their bot detection.

    Returns (is_live: bool, stream_title: str | None, error: str | None).
    Returns (None, None, error) if streamlink check is inconclusive and
    caller should fall back to yt-dlp.
    """
    if not HAS_STREAMLINK:
        return None, None, "streamlink not available"

    url = f"https://kick.com/{channel_name}"
    check_cmd = ["streamlink", "--json", url]
    logger.info(f"Kick check (streamlink): {' '.join(check_cmd)}")

    try:
        result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=timeout)
        logger.info(f"Kick streamlink check returncode={result.returncode}")

        if result.stdout:
            try:
                data = json.loads(result.stdout)

                # streamlink --json returns {"streams": {...}} when live
                # and {"error": "..."} when offline or errored
                if "streams" in data and data["streams"]:
                    # Channel is live — streamlink found available streams
                    # Try to extract title from metadata if available
                    title = data.get("metadata", {}).get("title")
                    logger.info(f"Kick streamlink: channel is LIVE — title={title!r}")
                    return True, title, None
                elif "error" in data:
                    error_msg = data["error"]
                    error_lower = error_msg.lower()
                    if "403" in error_lower or "forbidden" in error_lower:
                        logger.warning(f"Kick streamlink: Cloudflare 403 — {error_msg}")
                        return None, None, "403 (Cloudflare)"  # fall back
                    elif "no playable streams" in error_lower or "could not find" in error_lower:
                        logger.info("Kick streamlink: channel is offline (no streams)")
                        return False, None, None
                    else:
                        logger.info(f"Kick streamlink: offline or error — {error_msg}")
                        return False, None, None
                else:
                    logger.info("Kick streamlink: no streams found (offline)")
                    return False, None, None

            except json.JSONDecodeError:
                pass

        # Check stderr for common patterns
        if result.stderr:
            stderr_lower = result.stderr.lower()
            if "403" in stderr_lower:
                logger.warning(f"Kick streamlink: 403 in stderr")
                return None, None, "403 (Cloudflare)"
            elif "no plugin" in stderr_lower:
                logger.warning("Kick streamlink: no plugin for Kick URLs")
                return None, None, "no kick plugin"

        if result.returncode != 0:
            logger.info("Kick streamlink: non-zero exit (likely offline)")
            return False, None, None

        return None, None, "unexpected streamlink output"

    except subprocess.TimeoutExpired:
        logger.warning("Kick streamlink check timed out")
        return None, None, "timeout"
    except FileNotFoundError:
        return None, None, "streamlink not found"
    except Exception as e:
        logger.warning(f"Kick streamlink check error: {e}")
        return None, None, str(e)




def check_stream_rumble_html(channel_name, logger, timeout=20, cookies_file=None):
    """Check if a Rumble channel is live by scraping the channel page HTML.

    Fetches ``https://rumble.com/c/CHANNEL`` with cookies and looks for the
    ``thumbnail__thumb--live`` CSS class, which Rumble adds to the thumbnail
    wrapper of any currently-live video.  When found, the adjacent
    ``videostream__link`` href gives the live video page URL directly.

    The live video URL is then passed to yt-dlp (single video page, not a
    playlist) to resolve the actual HLS stream for recording.

    Returns (is_live: bool, stream_title: str | None, resolved_url: str | None,
             error: str | None).
    resolved_url is the full ``https://rumble.com/vXXXXX-slug.html`` URL of
    the live video, ready to hand to yt-dlp for recording.
    """
    import urllib.request
    import urllib.error
    import http.cookiejar
    import re as _re

    channel_url = f"https://rumble.com/c/{channel_name}"
    logger.info(f"Rumble HTML check: {channel_url}")

    try:
        # Build opener with cookie file if available
        opener = urllib.request.build_opener()
        if cookies_file:
            try:
                cj = http.cookiejar.MozillaCookieJar(cookies_file)
                cj.load(ignore_discard=True, ignore_expires=True)
                opener.add_handler(urllib.request.HTTPCookieProcessor(cj))
            except Exception as ce:
                logger.debug(f"Rumble HTML check: could not load cookies ({ce}), proceeding without")

        req = urllib.request.Request(
            channel_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        )
        with opener.open(req, timeout=timeout) as resp:
            html = resp.read().decode("utf-8", errors="replace")

    except urllib.error.HTTPError as e:
        logger.warning(f"Rumble HTML check HTTP error: {e.code}")
        return False, None, None, f"HTTP {e.code}"
    except urllib.error.URLError as e:
        logger.warning(f"Rumble HTML check URL error: {e.reason}")
        return False, None, None, str(e.reason)
    except Exception as e:
        logger.warning(f"Rumble HTML check error: {e}")
        return False, None, None, str(e)

    # Find every thumbnail__thumb--live occurrence in the HTML (not CSS).
    # CSS occurrences are followed by { while HTML ones are followed by "
    live_video_urls = []
    for m in _re.finditer(r'thumbnail__thumb--live"', html):
        # Look for the next videostream__link href within ~2000 chars
        chunk = html[m.start(): m.start() + 2000]
        href_m = _re.search(r'href="(/v[a-z0-9]+-[^"]+\.html)', chunk)
        if href_m:
            slug = href_m.group(1)
            full_url = "https://rumble.com" + slug
            if full_url not in live_video_urls:
                live_video_urls.append(full_url)

    if not live_video_urls:
        logger.info("Rumble HTML check: no live stream found on channel page")
        return False, None, None, None

    live_url = live_video_urls[0]
    logger.info(f"Rumble HTML check: found live video URL — {live_url}")
    # Title will be resolved by yt-dlp when it processes the video URL
    return True, None, live_url, None


def _find_rumble_live_url(url, logger, timeout, cookies_file, impersonate=False):
    """Scan a Rumble channel/user page playlist for a live stream entry.

    Rumble channel and user pages return a playlist when scraped.  Using
    ``--playlist-items 1`` only grabs whichever video is first (usually a
    pinned trailer or VOD), so we need to scan the whole playlist to find
    any item whose ``live_status`` is ``"is_live"``.

    Returns the URL of the live stream entry, or None if none found.
    Only called when ``url`` looks like a Rumble channel/user page (not a
    direct video URL).
    """
    import re as _re
    cmd = ["yt-dlp", "--flat-playlist", "--dump-json"]
    if impersonate and HAS_CURL_CFFI:
        cmd.extend(["--impersonate", "chrome"])
    if cookies_file:
        cmd.extend(["--cookies", cookies_file])
    cmd.append(url)

    logger.info(f"Rumble playlist scan: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("Rumble playlist scan timed out")
        return None
    except Exception as e:
        logger.warning(f"Rumble playlist scan error: {e}")
        return None

    if result.returncode != 0 and not result.stdout:
        logger.warning(f"Rumble playlist scan failed (code {result.returncode})")
        return None

    # --flat-playlist emits one JSON object per line
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        live_status = entry.get("live_status", "")
        entry_url = entry.get("url") or entry.get("webpage_url")

        if live_status == "is_live" and entry_url:
            title = entry.get("title", "(unknown)")
            logger.info(f"Rumble playlist scan: found live entry — {title!r} → {entry_url}")
            return entry_url
        # Rumble sometimes omits live_status in flat-playlist; fall back to is_live flag
        if entry.get("is_live") and entry_url:
            title = entry.get("title", "(unknown)")
            logger.info(f"Rumble playlist scan: found is_live entry — {title!r} → {entry_url}")
            return entry_url

    logger.info("Rumble playlist scan: no live entry found in playlist")
    return None


def _is_rumble_channel_url(url):
    """Return True if *url* is a Rumble channel/user page (not a direct video URL).

    Direct Rumble video URLs look like /vXXXXX-slug.html
    Channel/user pages look like /c/ChannelName, /user/Username, etc.
    """
    import re as _re
    if "rumble.com" not in url.lower():
        return False
    path = url.split("rumble.com", 1)[-1].split("?")[0].rstrip("/")
    # Direct video URLs start with /v followed by alphanumerics then a dash
    if _re.match(r"^/v[a-z0-9]+-", path, _re.IGNORECASE):
        return False
    return True


def check_stream_ytdlp(url, logger, timeout=30, cookies_file=None):
    """Check if a stream is live using yt-dlp (Kick, YouTube, custom URLs).

    Returns (is_live: bool, stream_title: str | None, error: str | None,
             used_impersonation: bool, resolved_url: str | None).
    The fourth value indicates whether browser impersonation was needed,
    so the recording command can use the same flag.
    The fifth value is the resolved video URL if yt-dlp found a different
    URL than the one provided (e.g. Rumble channel page -> video URL).
    """
    # ── Rumble channel/user pages: scan playlist for live entry ──────────
    # Unlike YouTube's @handle/live, Rumble has no "give me the live stream"
    # URL convention — channel pages return a full playlist and --playlist-items 1
    # just grabs whatever is pinned first (usually a trailer/VOD).  We scan
    # the flat playlist to find any entry that is actually live.
    if _is_rumble_channel_url(url):
        live_url = _find_rumble_live_url(url, logger, timeout, cookies_file, impersonate=False)
        if live_url is None and HAS_CURL_CFFI:
            # Channel page may be Cloudflare-gated — retry with impersonation
            logger.info("Rumble channel scan: retrying with --impersonate chrome")
            live_url = _find_rumble_live_url(url, logger, timeout, cookies_file, impersonate=True)
        if live_url is None:
            # No live stream found in playlist right now — report as offline
            return False, None, None, False, None
        # Found a live entry — check it directly to get full metadata & confirm live status
        logger.info(f"Rumble channel scan resolved live URL: {live_url}")
        url = live_url  # proceed with the specific live video URL from here on

    if not HAS_YTDLP:
        logger.error("yt-dlp not installed — cannot check Kick/YouTube streams")
        return False, None, "yt-dlp not installed", False, None

    check_cmd = ["yt-dlp", "--dump-json", "--playlist-items", "1"]
    if cookies_file:
        check_cmd.extend(["--cookies", cookies_file])
    check_cmd.append(url)
    logger.info(f"Check cmd (yt-dlp): {' '.join(check_cmd)}")

    try:
        check = subprocess.run(check_cmd, capture_output=True, text=True, timeout=timeout)
        logger.info(f"Check returncode={check.returncode}")

        if check.stderr and check.returncode != 0:
            stderr_snippet = check.stderr[:200]
            logger.info(f"Check stderr: {stderr_snippet}")

        if check.returncode == 0 and check.stdout:
            try:
                data = json.loads(check.stdout)
                is_live = data.get("is_live", False) or data.get("live_status") == "is_live"
                title = data.get("title") or data.get("fulltitle")

                if not is_live and data.get("live_status") == "is_upcoming":
                    logger.info("Stream is scheduled but not live yet")
                    return False, title, "scheduled (not started)", False, None

                # Check if yt-dlp resolved to a different URL (e.g. channel page -> video)
                resolved = data.get("webpage_url") or data.get("url")
                resolved_url = None
                if resolved and resolved != url:
                    logger.info(f"Resolved URL: {resolved}")
                    resolved_url = resolved

                # For custom URLs: if yt-dlp can extract formats, treat as recordable
                # even if is_live isn't explicitly set (e.g. direct .m3u8 links).
                # BUT: if the URL resolved to a different page (channel -> video),
                # trust yt-dlp's is_live flag — a resolved VOD should NOT be treated
                # as live just because it has formats.
                if not is_live and data.get("formats") and not resolved_url:
                    logger.info(f"yt-dlp found extractable stream (not explicitly live): title={title!r}")
                    is_live = True  # treat as recordable
                elif not is_live and resolved_url:
                    live_status = data.get("live_status", "unknown")
                    logger.info(f"Resolved video is not live (live_status={live_status!r}) — treating as offline")

                logger.info(f"yt-dlp found stream: is_live={is_live}, title={title!r}")
                return is_live, title, None, False, resolved_url
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse yt-dlp JSON: {e}")
                return False, None, "JSON parse error", False, None

        # Parse common error conditions from stderr
        if check.stderr:
            stderr_lower = check.stderr.lower()
            if "private video" in stderr_lower or "members-only" in stderr_lower:
                return False, None, "members-only or private", False, None
            elif "this live event will begin" in stderr_lower:
                return False, None, "scheduled but not started", False, None
            elif "video unavailable" in stderr_lower or "no video formats" in stderr_lower:
                logger.warning("Video unavailable — might be offline or /live redirect failed")
                return False, None, "video unavailable", False, None
            elif "unable to extract" in stderr_lower:
                logger.warning("Could not extract stream info — channel might not be live")
                return False, None, "extraction failed", False, None
            elif "http error 403" in stderr_lower or "http error 503" in stderr_lower:
                # Try again with browser impersonation if curl_cffi is available
                if HAS_CURL_CFFI:
                    logger.info("HTTP 403/503 — retrying with --impersonate chrome")
                    impersonate_cmd = ["yt-dlp", "--impersonate", "chrome",
                                       "--dump-json", "--playlist-items", "1"]
                    if cookies_file:
                        impersonate_cmd.extend(["--cookies", cookies_file])
                    impersonate_cmd.append(url)
                    try:
                        retry = subprocess.run(impersonate_cmd, capture_output=True,
                                               text=True, timeout=timeout)
                        if retry.returncode == 0 and retry.stdout:
                            data = json.loads(retry.stdout)
                            is_live = data.get("is_live", False) or data.get("live_status") == "is_live"
                            title = data.get("title") or data.get("fulltitle")
                            if not is_live and data.get("formats"):
                                is_live = True
                            # Check resolved URL for impersonation path too
                            resolved = data.get("webpage_url") or data.get("url")
                            resolved_url = None
                            if resolved and resolved != url:
                                logger.info(f"Resolved URL: {resolved}")
                                resolved_url = resolved
                            # Don't treat resolved VODs as live
                            if is_live and not (data.get("is_live", False) or data.get("live_status") == "is_live") and resolved_url:
                                live_status = data.get("live_status", "unknown")
                                logger.info(f"Resolved video is not live (live_status={live_status!r}) — treating as offline")
                                is_live = False
                            logger.info(f"Impersonation succeeded: is_live={is_live}, title={title!r}")
                            return is_live, title, None, True, resolved_url
                        else:
                            logger.warning("Impersonation retry also failed")
                    except Exception as e:
                        logger.warning(f"Impersonation retry error: {e}")
                logger.error("HTTP 403/503 — cookies may be expired or invalid")
                return False, None, "403/503 (cookies expired?)", False, None
            elif "sign in" in stderr_lower or "login required" in stderr_lower:
                logger.error("Login required — cookies may be missing or expired")
                return False, None, "login required (check cookies)", False, None

        return False, None, None, False, None

    except subprocess.TimeoutExpired:
        logger.warning("Stream check timed out")
        return False, None, "timeout", False, None
    except FileNotFoundError:
        logger.error("yt-dlp not found in PATH")
        return False, None, "yt-dlp not found", False, None
    except Exception as e:
        logger.error(f"Unexpected error checking stream: {e}")
        return False, None, str(e), False, None


class FishtankAuth:
    """Manages JWT authentication for fishtank.live streams.

    Fishtank uses MistServer for streaming.  Every stream URL requires a
    short-lived JWT obtained from api.fishtank.live/v1/auth.  This class
    fetches and caches the token, refreshing it only when it has expired
    (tokens are valid for ~24 hours).

    Usage (in a worker):
        auth = FishtankAuth(cookies_file, logger, email="", password="")
        jwt = auth.get_jwt()          # returns None on failure
        url = auth.build_stream_url("dirc-5")
    """

    # Base URLs
    _AUTH_URL    = "https://api.fishtank.live/v1/auth"
    _LOGIN_URL   = "https://api.fishtank.live/v1/auth/log-in"
    _STREAMS_URL = "https://api.fishtank.live/v1/live-streams"
    # Stream host is read dynamically from the loadBalancer API field.
    # Fishtank rotates between streams-b, streams-c, etc. per session.
    _DEFAULT_STREAM_HOST = "streams-c.fishtank.live"

    # Stream IDs recognised as fishtank camera names
    CAMERA_ALIASES = {
        "director":   "dirc-5",
        "dirc":       "dirc-5",
        "dorm":       "dmrm-5",
        "dmrm":       "dmrm-5",
        "confessional":"cfsl-5",
        "cfsl":       "cfsl-5",
        # Season 5: "Balcony" renamed to "East Wing" (bkny-5 stream ID unchanged)
        "eastwing":   "bkny-5",
        "east":       "bkny-5",
        "bkny":       "bkny-5",
        "balcony":    "bkny-5",   # old alias kept for backwards compatibility
        "foyer":      "foyr-5",
        "foyr":       "foyr-5",
        "kitchen":    "ktch-5",
        "ktch":       "ktch-5",
        "bar":        "brrr-5",
        "brrr":       "brrr-5",
        "jacuzzi":    "jckz-5",
        "jckz":       "jckz-5",
        "dining":     "dnrm-5",
        "dnrm":       "dnrm-5",
        "glassroom":  "gsrm-5",
        "gsrm":       "gsrm-5",
        "corridor":   "codr-5",
        "codr":       "codr-5",
        # Season 5: "Hallway Up" renamed to "West Wing" (hwup-5 stream ID unchanged)
        "westwing":   "hwup-5",
        "west":       "hwup-5",
        "hwup":       "hwup-5",
        "hallwayup":  "hwup-5",   # old alias kept for backwards compatibility
        # Season 5: "Hallway Down" renamed to "Hallway" (hwdn-5 stream ID unchanged)
        "hallway":    "hwdn-5",
        "hallwaydown":"hwdn-5",   # old alias kept for backwards compatibility
        "hwdn":       "hwdn-5",
        "closet":     "dmcl-5",
        "dmcl":       "dmcl-5",
        "cameraman":  "cameraman2-5",
        "cameraman2": "cameraman2-5",
        "cam":        "cameraman2-5",
        "barptz":     "brpz-5",
        "brpz":       "brpz-5",
        "market":     "mrke-5",
        "mrke":       "mrke-5",
        "jungleroom": "br4j-5",
        "jungle":     "br4j-5",
        "br4j":       "br4j-5",
        # Season 5: three additional rooms detected in API — names TBD pending unlock
        # From the website unlock countdown (as of 2026-03-21):
        #   bbcl-5  — unlocks today
        #   br3g-5  — unlocks in ~8 days
        #   bare-5  — unlocks in ~12 days
        # Raw stream IDs work now; friendly name aliases will be added once confirmed.
        "bbcl":       "bbcl-5",
        "computerlab":"bbcl-5",
        "bare":       "bare-5",
        "bare5":      "bare-5",
        "br3g":       "br3g-5",
        "br3g5":      "br3g-5",
    }

    def __init__(self, cookies_file, logger, email="", password=""):
        self._cookies_file = cookies_file
        self._logger = logger
        self._email = email
        self._password = password
        self._jwt = None
        self._jwt_exp = 0       # unix timestamp when current JWT expires
        self._stream_host = self._DEFAULT_STREAM_HOST  # fallback host (any online stream)
        self._stream_hosts = {}  # per-stream hosts from loadBalancer, e.g. {"dirc-5": "streams-f.fishtank.live"}
        self._all_stream_names = {}  # all stream id→name from API, populated by get_live_streams

    # ── Public interface ──────────────────────────────────────────────────

    def resolve_stream_id(self, name):
        """Resolve a camera name or raw stream-ID to a canonical stream ID.

        Accepts:
            - Canonical IDs:   "dirc-5", "dmrm-5", …
            - Friendly names:  "director", "dorm", "bar", …
        Returns the canonical ID string, or the original value if not found.
        """
        normalised = name.lower().replace(" ", "").replace("-", "")
        return self.CAMERA_ALIASES.get(normalised, name)

    def get_jwt(self, force_refresh=False):
        """Return a valid JWT, refreshing from the API if needed.

        Returns the JWT string, or None if authentication failed.
        """
        now = time.time()
        # Refresh if expired (with 5-minute buffer) or forced
        if force_refresh or self._jwt is None or now >= (self._jwt_exp - 300):
            self._refresh()
        return self._jwt

    def build_stream_url(self, stream_id, quality="maxbps"):
        """Build the MistServer TS progressive HTTP URL for a given stream ID.

        Returns (url, jwt) tuple, or (None, None) if auth failed.

        MistServer serves HLS (confirmed from browser HAR):

            https://<host>/hls/live+<stream_id>/index.m3u8?jwt=<token>

        Note: uses literal + (not %2b) and ?jwt= (not ?tkn=).
        The same 24h live_stream_token is used for all endpoints.
        """
        jwt = self.get_jwt()
        if not jwt:
            return None, None
        # Use per-stream host if known, fall back to global cached host.
        # Director Mode (dirc-5) is often on a different node than the other
        # streams — always use the stream-specific host from loadBalancer.
        host = self._stream_hosts.get(stream_id, self._stream_host)
        # HLS master playlist — confirmed from browser HAR as the actual
        # protocol used. Uses literal + (not %2b) and ?jwt= parameter.
        url = f"https://{host}/hls/live+{stream_id}/index.m3u8?jwt={jwt}"
        return url, jwt

    def get_live_streams(self):
        """Fetch the list of currently-live stream IDs from the API.

        Host discovery uses the thumbnail endpoint rather than the loadBalancer
        API field.  The loadBalancer field rotates to a different node on every
        API call (round-robin) and that node may not be actually serving the
        stream.  The thumbnail endpoint (/live%2B<id>.jpeg) is served directly
        by the real streaming node with no auth required, so a 200 response
        from a given host means that host is currently serving the stream.

        Returns a dict mapping stream_id → stream_name, or {} on failure.
        """
        raw = self._fetch_json(self._STREAMS_URL)
        if raw is None:
            return {}
        try:
            data = json.loads(raw)
            status = data.get("liveStreamStatus", {})
            lb = data.get("loadBalancer", {})
            streams = {}
            all_stream_names = {}
            for s in data.get("liveStreams", []):
                sid = s["id"]
                all_stream_names[sid] = s["name"]
                if status.get(sid) == "online":
                    streams[sid] = s["name"]

            self._all_stream_names = all_stream_names

            # Discover the real serving host for each online stream via thumbnail.
            # Use the loadBalancer host as the starting point for the thumbnail
            # request — the thumbnail always responds from the real node.
            import urllib.request, urllib.error
            for sid in list(streams.keys()):
                candidate_host = lb.get(sid, self._stream_host)
                thumb_url = (
                    f"https://{candidate_host}/live%2B{sid}.jpeg"
                )
                try:
                    req = urllib.request.Request(
                        thumb_url, method="HEAD",
                        headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        # The response URL's host is the real serving node
                        real_host = resp.url.split("//")[1].split("/")[0]
                        if real_host and "fishtank.live" in real_host:
                            self._stream_hosts[sid] = real_host
                            self._stream_host = real_host
                        else:
                            # Responded from candidate_host directly
                            self._stream_hosts[sid] = candidate_host
                            self._stream_host = candidate_host
                except Exception:
                    # Fallback: trust the loadBalancer host as-is
                    if lb.get(sid):
                        self._stream_hosts[sid] = lb[sid]
                        self._stream_host = lb[sid]

            if self._stream_host != self._DEFAULT_STREAM_HOST:
                self._logger.info(
                    f"[fishtank] Stream serving host: {self._stream_host}")
            return streams
        except Exception as e:
            self._logger.warning(f"[fishtank] Failed to parse live streams: {e}")
            return {}

    def _fetch_json(self, url):
        """Fetch a URL and return the response body as a string.

        Tries curl_cffi first (handles HTTP/3, Cloudflare, better TLS),
        falls back to urllib.  Returns None on failure.
        """
        headers = self._common_headers()

        # ── curl_cffi (preferred — handles h3 and Cloudflare) ─────────────
        if HAS_CURL_CFFI:
            try:
                from curl_cffi import requests as cffi_requests
                resp = cffi_requests.get(
                    url, headers=headers,
                    impersonate="chrome", timeout=15,
                )
                if resp.status_code == 200:
                    return resp.text
                self._logger.warning(
                    f"[fishtank] curl_cffi HTTP {resp.status_code} for {url}")
                return None
            except Exception as e:
                self._logger.warning(
                    f"[fishtank] curl_cffi fetch failed ({e}), retrying with urllib")

        # ── urllib fallback ────────────────────────────────────────────────
        import urllib.request, urllib.error
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.read().decode("utf-8")
        except Exception as e:
            self._logger.warning(f"[fishtank] Failed to fetch {url}: {e}")
            return None

    # ── Private helpers ───────────────────────────────────────────────────

    def _extract_supabase_jwt(self):
        """Extract the Supabase access token from cookies.txt.

        The cookie named 'sb-wcsaaupukpdmqdjcgaoo-auth-token' holds a
        JSON-encoded array [access_token, refresh_token] stored as a
        URL-encoded string.  We parse it out and return the access token.

        Returns the JWT string, or None if not found / already expired.
        """
        import urllib.parse, base64
        if not self._cookies_file or not os.path.isfile(self._cookies_file):
            return None
        try:
            with open(self._cookies_file, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split('\t')
                    if len(parts) < 7:
                        continue
                    name = parts[5]
                    if 'sb-' in name and 'auth-token' in name:
                        value = urllib.parse.unquote(parts[6])
                        # Value is a JSON array: ["<access_token>", "<refresh_token>"]
                        tokens = json.loads(value)
                        if isinstance(tokens, list) and len(tokens) >= 1:
                            access_token = tokens[0]
                            # Quick expiry check — don't bother sending an
                            # obviously expired token
                            exp = self._decode_jwt_exp(access_token)
                            if exp < time.time():
                                self._logger.warning(
                                    "[fishtank] Supabase access token in cookies.txt "
                                    "has expired — please re-export cookies from your "
                                    "browser while logged into fishtank.live"
                                )
                                return None
                            return access_token
        except Exception as e:
            self._logger.warning(f"[fishtank] Could not extract Supabase JWT from cookies: {e}")
        return None

    def _refresh(self):
        """Fetch a fresh MistServer JWT from the fishtank auth endpoint.

        Authentication strategy (in priority order):
          1. POST /v1/auth/log-in with email+password from config.ini — the
             only method that works reliably regardless of cookie freshness.
             The GET /v1/auth session-check endpoint returns {"session":null}
             once the 15-minute Supabase access token expires, even with
             valid cookies.
          2. Authorization: Bearer header using the Supabase token extracted
             from cookies.txt (works only within 15 min of cookie export).
          3. Cookie jar fallback (legacy).

        If all paths fail, self._jwt is set to None and the error is logged.
        """
        import urllib.request, urllib.error, http.cookiejar
        self._logger.info("[fishtank] Refreshing JWT")

        # ── Strategy 1: POST login with email+password (most reliable) ────
        if self._email and self._password:
            jwt = self._login_with_credentials()
            if jwt:
                self._jwt = jwt
                self._jwt_exp = self._decode_jwt_exp(jwt)
                exp_dt = datetime.datetime.fromtimestamp(
                    self._jwt_exp).strftime("%Y-%m-%d %H:%M:%S")
                self._logger.info(
                    f"[fishtank] MistServer JWT obtained via login, "
                    f"valid for 24h, expires {exp_dt}")
                return
            self._logger.warning(
                "[fishtank] Login with credentials failed — falling back to cookie methods")

        # ── Strategy 2: Authorization: Bearer header ──────────────────────
        supabase_token = self._extract_supabase_jwt()
        if supabase_token:
            try:
                headers = self._common_headers()
                headers["Authorization"] = f"Bearer {supabase_token}"
                # Use curl_cffi if available to handle HTTP/3
                raw_text = None
                if HAS_CURL_CFFI:
                    try:
                        from curl_cffi import requests as cffi_requests
                        r = cffi_requests.get(
                            self._AUTH_URL, headers=headers,
                            impersonate="chrome", timeout=15)
                        if r.status_code == 200:
                            raw_text = r.text
                    except Exception:
                        pass
                if raw_text is None:
                    req = urllib.request.Request(self._AUTH_URL, headers=headers)
                    with urllib.request.urlopen(req, timeout=15) as resp:
                        raw_text = resp.read().decode("utf-8")
                raw = raw_text
                data = json.loads(raw)
                jwt = (
                    # live_stream_token is the 24h MistServer JWT
                    (data.get("session") or {}).get("live_stream_token")
                    or data.get("token") or data.get("jwt")
                    or data.get("accessToken") or data.get("mistToken")
                    or (data.get("session") or {}).get("access_token")
                )
                if jwt:
                    self._jwt = jwt
                    self._jwt_exp = self._decode_jwt_exp(jwt)
                    exp_dt = datetime.datetime.fromtimestamp(
                        self._jwt_exp).strftime("%Y-%m-%d %H:%M:%S")
                    self._logger.info(
                        f"[fishtank] MistServer JWT obtained via Bearer auth, "
                        f"valid for 24h, expires {exp_dt}")
                    return
                # Auth succeeded but response format unexpected — log all keys
                # so we can adapt in the future
                self._logger.warning(
                    f"[fishtank] Auth response had no recognised token field "
                    f"(keys: {list(data.keys())}) — raw: {raw[:200]}"
                )
                # Fall through to strategy 2
            except urllib.error.HTTPError as e:
                self._logger.warning(
                    f"[fishtank] Bearer auth HTTP {e.code} — falling back to cookie jar")
            except Exception as e:
                self._logger.warning(
                    f"[fishtank] Bearer auth error ({e}) — falling back to cookie jar")

        # ── Strategy 2: Cookie jar (legacy fallback) ──────────────────────
        try:
            opener = urllib.request.build_opener()
            if self._cookies_file and os.path.isfile(self._cookies_file):
                try:
                    cj = http.cookiejar.MozillaCookieJar(self._cookies_file)
                    cj.load(ignore_discard=True, ignore_expires=True)
                    opener.add_handler(urllib.request.HTTPCookieProcessor(cj))
                except Exception as ce:
                    self._logger.warning(
                        f"[fishtank] Could not load cookies for fallback: {ce}")

            # Try curl_cffi first for h3 support
            raw = None
            if HAS_CURL_CFFI:
                try:
                    from curl_cffi import requests as cffi_requests
                    import http.cookiejar as _cj_mod
                    cffi_headers = self._common_headers()
                    r2 = cffi_requests.get(
                        self._AUTH_URL, headers=cffi_headers,
                        impersonate="chrome", timeout=15)
                    if r2.status_code == 200:
                        raw = r2.text
                except Exception:
                    pass
            if raw is None:
                req = urllib.request.Request(
                    self._AUTH_URL, headers=self._common_headers())
                with opener.open(req, timeout=15) as resp:
                    raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            jwt = (
                # live_stream_token is the 24h MistServer JWT
                (data.get("session") or {}).get("live_stream_token")
                or data.get("token") or data.get("jwt")
                or data.get("accessToken") or data.get("mistToken")
                or (data.get("session") or {}).get("access_token")
            )
            if jwt:
                self._jwt = jwt
                self._jwt_exp = self._decode_jwt_exp(jwt)
                exp_dt = datetime.datetime.fromtimestamp(
                    self._jwt_exp).strftime("%Y-%m-%d %H:%M:%S")
                self._logger.info(
                    f"[fishtank] MistServer JWT obtained via cookie jar, "
                    f"valid for 24h, expires {exp_dt}")
                return

            # Both strategies failed — give the user a clear action to take
            self._logger.error(
                f"[fishtank] Authentication failed — fishtank returned "
                f"{raw[:200]}\n"
                f"  ► Your fishtank.live session has most likely expired.\n"
                f"  ► Fix: log into fishtank.live in your browser, then "
                f"re-export cookies.txt via Cookie-Editor and replace "
                f"E:\\Streams\\cookies.txt with the new file."
            )
            self._jwt = None

        except urllib.error.HTTPError as e:
            self._logger.error(
                f"[fishtank] Auth HTTP error {e.code}: {e.reason}")
            self._jwt = None
        except urllib.error.URLError as e:
            self._logger.error(f"[fishtank] Auth URL error: {e.reason}")
            self._jwt = None
        except Exception as e:
            self._logger.error(f"[fishtank] Auth unexpected error: {e}")
            self._jwt = None

    def _login_with_credentials(self):
        """POST to /v1/auth/log-in with email+password to get a fresh JWT.

        This is the most reliable auth method — it works regardless of cookie
        freshness and returns a 24h live_stream_token directly.

        Returns the live_stream_token JWT string, or None on failure.
        """
        import json as _json

        payload = _json.dumps({
            "email": self._email,
            "password": self._password,
        }).encode("utf-8")

        headers = self._common_headers()
        headers["Content-Type"] = "application/json"

        try:
            if HAS_CURL_CFFI:
                from curl_cffi import requests as cffi_requests
                resp = cffi_requests.post(
                    self._LOGIN_URL,
                    data=payload,
                    headers=headers,
                    impersonate="chrome",
                    timeout=15,
                )
                if resp.status_code == 200:
                    data = _json.loads(resp.text)
                    return (data.get("session") or {}).get("live_stream_token")
                self._logger.warning(
                    f"[fishtank] Login HTTP {resp.status_code}")
                return None

            # urllib fallback
            import urllib.request, urllib.error
            req = urllib.request.Request(
                self._LOGIN_URL,
                data=payload,
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = _json.loads(resp.read().decode("utf-8"))
            return (data.get("session") or {}).get("live_stream_token")

        except Exception as e:
            self._logger.warning(f"[fishtank] Login error: {e}")
            return None

    @staticmethod
    def _common_headers():
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Origin": "https://www.fishtank.live",
            "Referer": "https://www.fishtank.live/",
        }

    @staticmethod
    def _decode_jwt_exp(jwt_string):
        """Extract the 'exp' claim from a JWT without verifying the signature.

        Returns a unix timestamp, defaulting to (now + 23h) if decoding fails.
        """
        import base64
        try:
            payload_b64 = jwt_string.split(".")[1]
            # Add padding if needed
            padding = 4 - len(payload_b64) % 4
            if padding != 4:
                payload_b64 += "=" * padding
            payload = json.loads(base64.b64decode(payload_b64).decode("utf-8"))
            return int(payload.get("exp", time.time() + 82800))
        except Exception:
            return int(time.time() + 82800)  # fallback: treat as valid for 23h


def check_stream_fishtank(stream_id, auth, logger, timeout=20):
    """Check if a fishtank.live camera stream is currently live.

    Primary check: /v1/live-streams API liveStreamStatus field.
    Fallback check: some streams (e.g. dirc-5 / Director Mode) are served
    from a dedicated node and never appear in liveStreamStatus even when live.
    For those we probe the stream URL directly with a GET request and treat
    a non-404 response as live.

    Args:
        stream_id: Canonical stream ID (e.g. "dirc-5") or friendly name.
        auth: A FishtankAuth instance.
        logger: Logger adapter.
        timeout: Request timeout in seconds.

    Returns (is_live: bool, stream_name: str | None, error: str | None).
    """
    import urllib.request, urllib.error

    resolved_id = auth.resolve_stream_id(stream_id)

    # Fetch the streams list — also populates auth._all_stream_names and
    # caches the load balancer host from any online stream
    live_streams = auth.get_live_streams()
    if not live_streams and not auth._all_stream_names:
        return False, None, "failed to reach fishtank API"

    # Look up the friendly name for logging
    stream_name = (
        live_streams.get(resolved_id)
        or auth._all_stream_names.get(resolved_id)
        or resolved_id
    )

    # ── Fast path: stream is in liveStreamStatus ─────────────────────────
    if resolved_id in live_streams:
        logger.info(f"[fishtank] Stream '{resolved_id}' ({stream_name}) is listed as online")
        jwt = auth.get_jwt()
        if not jwt:
            return False, stream_name, "could not obtain JWT for stream"
        return True, stream_name, None

    # ── Fallback: stream absent from liveStreamStatus — probe directly ────
    # Director Mode and some other streams are never in liveStreamStatus even
    # when live (confirmed from browser HAR: dirc-5 absent from status dict
    # while clearly playing in browser).  We attempt a direct GET to the
    # stream URL; anything other than 404 means the stream is accessible.
    logger.info(
        f"[fishtank] Stream '{resolved_id}' not in status list — probing directly")

    jwt = auth.get_jwt()
    if not jwt:
        return False, stream_name, "could not obtain JWT for probe"

    probe_host = auth._stream_hosts.get(resolved_id, auth._stream_host)
    # Probe the HLS playlist — same endpoint we record from
    probe_url = f"https://{probe_host}/hls/live+{resolved_id}/index.m3u8?jwt={jwt}"
    try:
        req = urllib.request.Request(
            probe_url, method="GET",
            headers=FishtankAuth._common_headers(),
        )
        # We only need the response headers — close immediately
        resp = urllib.request.urlopen(req, timeout=timeout)
        resp.close()
        logger.info(
            f"[fishtank] Stream '{resolved_id}' probe succeeded "
            f"(HTTP {resp.status}) — treating as live")
        return True, stream_name, None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.info(
                f"[fishtank] Stream '{resolved_id}' probe returned 404 — offline")
            return False, None, None
        # Any other HTTP status (200, 206, 302…) means the stream exists
        logger.info(
            f"[fishtank] Stream '{resolved_id}' probe HTTP {e.code} — treating as live")
        return True, stream_name, None
    except urllib.error.URLError as e:
        logger.warning(
            f"[fishtank] Stream '{resolved_id}' probe failed: {e.reason}")
        return False, None, str(e.reason)
    except Exception as e:
        logger.warning(
            f"[fishtank] Stream '{resolved_id}' probe error: {e}")
        return False, None, str(e)


def resolve_best_fishtank_variant(master_url, jwt, logger, timeout=10):
    """Fetch a MistServer HLS master playlist and return the highest-bandwidth variant URL.

    MistServer's master playlist (`index.m3u8`) lists multiple quality renditions.
    ffmpeg, when given a master playlist directly, simply picks whichever rendition
    appears *first* in the file — and MistServer does not guarantee a consistent
    ordering across different streaming nodes or reconnections.  This means ffmpeg
    can silently grab the 360p/500kbps rendition one session and 1080p/5000kbps
    the next, depending solely on how the backend ordered the playlist that time.

    This function parses the master playlist, finds the EXT-X-STREAM-INF entry
    with the highest BANDWIDTH value, and returns its absolute URL so ffmpeg
    receives a specific variant playlist rather than the master.

    Returns the best-variant URL string, or master_url unchanged if the playlist
    cannot be fetched/parsed (fail-open so recording still proceeds).
    """
    import urllib.request, urllib.error, re as _re

    try:
        req = urllib.request.Request(
            master_url,
            headers=FishtankAuth._common_headers(),
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"[fishtank] Could not fetch master playlist for variant selection: {e} — using master URL")
        return master_url

    # Parse EXT-X-STREAM-INF lines.  Each entry looks like:
    #   #EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,FRAME-RATE=30,...
    #   <relative or absolute URI on the next line>
    best_bandwidth = -1
    best_uri = None
    best_resolution = "unknown"   # track resolution of the winning entry, not the last-parsed
    best_frame_rate = None        # track frame rate of the winning entry
    lines = content.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXT-X-STREAM-INF:"):
            # Extract BANDWIDTH value
            bw_match = _re.search(r"BANDWIDTH=(\d+)", line)
            bandwidth = int(bw_match.group(1)) if bw_match else 0
            # RESOLUTION value is terminated by a comma or end-of-field — NOT \S+ which would
            # greedily consume FRAME-RATE=...,CODECS=... into the captured group.
            res_match = _re.search(r"RESOLUTION=(\d+x\d+)", line)
            resolution = res_match.group(1) if res_match else "unknown"
            # FRAME-RATE is a decimal like 30.000 or 29.97
            fr_match = _re.search(r"FRAME-RATE=([\d.]+)", line)
            frame_rate = fr_match.group(1) if fr_match else None
            # URI is on the very next non-empty line
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines):
                uri = lines[j].strip()
                if uri and not uri.startswith("#"):
                    if bandwidth > best_bandwidth:
                        best_bandwidth = bandwidth
                        best_uri = uri
                        best_resolution = resolution   # only update when this entry wins
                        best_frame_rate = frame_rate
            i = j + 1
            continue
        i += 1

    if best_uri is None:
        logger.info("[fishtank] Master playlist has no EXT-X-STREAM-INF entries — using master URL")
        return master_url, ""

    # Resolve relative URI against the master playlist base URL
    if best_uri.startswith("http://") or best_uri.startswith("https://"):
        # Absolute URI — use as-is; it already contains whatever auth it needs
        variant_url = best_uri
    else:
        # Relative path — resolve against the directory of the master URL
        base = master_url.split("?")[0]  # strip query string from master
        base_dir = base.rsplit("/", 1)[0]
        variant_url = f"{base_dir}/{best_uri}"
        # Only append the JWT if the variant URI doesn't already carry an auth
        # token of its own.  MistServer variant entries include ?tkn=<jwt>
        # directly in the URI; appending a second ?jwt= produces a malformed
        # double-token URL that the server rejects.
        has_own_token = "?tkn=" in best_uri or "?jwt=" in best_uri or "&tkn=" in best_uri or "&jwt=" in best_uri
        if not has_own_token and "?jwt=" in master_url:
            jwt_param = master_url.split("?jwt=", 1)[1]
            variant_url = f"{variant_url}?jwt={jwt_param}"

    # Build a compact stream-info string for the status table
    # e.g. "1080p · 30fps · 4.3Mbps" — best-effort, gracefully omits unknowns
    _res = best_resolution if best_resolution and best_resolution != "unknown" else None
    _kbps = best_bandwidth // 1000 if best_bandwidth > 0 else None
    _parts = []
    if _res:
        # Normalise "1920x1080" → "1080p", "1280x720" → "720p", passthrough otherwise
        if "x" in _res:
            try:
                _h = int(_res.split("x", 1)[1])
                _parts.append(f"{_h}p")
            except ValueError:
                _parts.append(_res)
        else:
            _parts.append(_res)
    if best_frame_rate:
        # Round to nearest integer: "30.000" → "30fps", "29.97" → "30fps"
        try:
            _fps = round(float(best_frame_rate))
            _parts.append(f"{_fps}fps")
        except ValueError:
            pass
    if _kbps:
        if _kbps >= 1000:
            _parts.append(f"{_kbps/1000:.1f}Mbps")
        else:
            _parts.append(f"{_kbps}kbps")
    stream_info_str = " · ".join(_parts)

    logger.info(
        f"[fishtank] Selected best variant: {best_bandwidth // 1000}kbps "
        f"— using variant playlist instead of master"
    )
    return variant_url, stream_info_str


def build_recording_command_fishtank(stream_url, raw_file, config, verbose):
    """Build an ffmpeg command to record a fishtank MistServer stream.

    We use ffmpeg directly rather than yt-dlp because the URL already contains
    the token as ?jwt= and yt-dlp's generic extractor would mangle query params.

    URL format (HLS master playlist, confirmed from browser HAR):
        https://<host>/hls/live+<stream_id>/index.m3u8?jwt=<24h_jwt>

    ffmpeg follows the HLS playlist, fetches segments, and writes MPEG-TS (.ts).
    """
    ffmpeg_path = config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg')
    loglevel = "verbose" if verbose else "warning"
    cmd = [
        ffmpeg_path,
        "-loglevel", loglevel,
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto,hls",
        # Increase analyzeduration and probesize to avoid repeated
        # "Consider increasing analyzeduration/probesize" warnings on
        # fishtank HLS streams that have slow segment delivery.
        "-analyzeduration", "2000000",
        "-probesize", "10000000",
        "-headers", (
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36\r\n"
            "Origin: https://www.fishtank.live\r\n"
            "Referer: https://www.fishtank.live/\r\n"
        ),
        "-i", stream_url,
        "-c", "copy",
        "-f", "mpegts",
        raw_file,
    ]
    return cmd


def check_stream_streamlink(url, logger, timeout=30):
    """Check if Twitch stream is live using streamlink.

    Returns (is_live: bool, stream_title: str | None, error: str | None).
    """
    check_cmd = ["streamlink", "--json", url]
    logger.info(f"Check cmd: {' '.join(check_cmd)}")

    try:
        check = subprocess.run(check_cmd, capture_output=True, text=True, timeout=timeout)
        logger.info(f"Check returncode={check.returncode}")

        title = None
        if check.returncode == 0 and check.stdout:
            try:
                data = json.loads(check.stdout)
                # streamlink --json returns metadata.title for some plugins
                metadata = data.get("metadata", {})
                title = metadata.get("title") or metadata.get("author")
            except json.JSONDecodeError:
                pass

            if '"error"' not in check.stdout or ('"url"' in check.stdout or '"playback_url"' in check.stdout):
                return True, title, None
        return False, title, None

    except subprocess.TimeoutExpired:
        logger.warning("Stream check timed out")
        return False, None, "timeout"
    except FileNotFoundError:
        logger.error("streamlink not found in PATH")
        return False, None, "streamlink not found"
    except Exception as e:
        logger.error(f"Unexpected error checking stream: {e}")
        return False, None, str(e)


# ────────────────────────────────────────────────
#          Recording Functions
# ────────────────────────────────────────────────

def build_recording_command_ytdlp(url, raw_file, config, verbose, streamlink_debug,
                                  cookies_file=None, impersonate=False):
    """Build yt-dlp command for live stream recording (Kick, YouTube, custom).

    For live HLS streams, yt-dlp's default fragment-based downloader buffers
    everything in memory and only writes on completion — which never happens
    for a live stream.  Instead, we use ffmpeg as an external downloader with
    --hls-use-mpegts, which writes a continuous MPEG-TS stream directly to the
    output file in real-time.

    Args:
        impersonate: If True, adds --impersonate chrome for Cloudflare-protected
                     sites (requires curl_cffi).
    """
    cmd = [
        "yt-dlp",
    ]

    # Browser impersonation for Cloudflare-protected sites (e.g. Rumble)
    if impersonate and HAS_CURL_CFFI:
        cmd.extend(["--impersonate", "chrome"])

    cmd.extend([
        url,
        "-f", "b",                     # "b" = best single format (suppresses yt-dlp warning vs "best")
        "-o", raw_file,
        "--no-part",
        "--no-mtime",
        "--retries", "10",
        "--fragment-retries", "10",
        # Force ffmpeg as external downloader for live HLS — this writes
        # directly to the output file instead of buffering fragments
        "--downloader", "ffmpeg",
        "--hls-use-mpegts",
        # NOTE: Do NOT add --downloader-args "ffmpeg:-re" — the -re flag is an
        # INPUT option in ffmpeg, but yt-dlp's --downloader-args appends it
        # after the output file, causing "Error parsing options for output file"
        # and ffmpeg exit code 4294967274 (-22 / EINVAL).  Live HLS streams
        # are inherently rate-limited by the server, so -re isn't needed.
    ])

    if cookies_file:
        cmd.extend(["--cookies", cookies_file])

    # For YouTube live streams, use the web player client directly.
    # This avoids the n-challenge JS solver entirely (which produces noisy
    # warnings when the bundled solver script is out of date), and is
    # sufficient for live HLS streams where the JS challenge doesn't apply.
    if "youtube.com" in url or "youtu.be" in url:
        cmd.extend(["--extractor-args", "youtube:player_client=web"])

    if verbose or streamlink_debug:
        cmd.append("--verbose")

    return cmd


def build_recording_command_streamlink_kick(url, raw_file, quality, verbose, streamlink_debug):
    """Build a streamlink fallback command for Kick recording.

    Used as a fallback when yt-dlp fails to create output for Kick streams.
    Streamlink 6.2+ supports Kick natively.  Always uses debug logging for
    diagnostics.
    """
    cmd = [
        "streamlink",
        "--http-header", "User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        url, quality,
        "--retry-streams", "10",
        "--retry-max", "3",
        "--retry-open", "3",
        "--stream-segment-threads", "3",
        "--stream-segment-timeout", "60",
        "-o", raw_file,
        # Always use debug for the fallback path so we can diagnose issues
        "--loglevel", "debug",
    ]

    return cmd


def build_recording_command_streamlink(url, raw_file, quality, platform, config, verbose, streamlink_debug):
    """Build streamlink command for Twitch/Kick recording."""
    cmd = ["streamlink"]

    if platform == "twitch":
        cmd.extend([
            "--twitch-disable-ads",
            "--twitch-low-latency",
            "--http-header", "User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ])
    elif platform == "kick":
        cmd.extend([
            "--http-header", "User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ])
    elif platform == "youtube":
        cmd.extend([
            "--http-header", "User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ])

    cmd.extend([
        url, quality,
        "--retry-streams", "30",
        "--retry-max", "10",
        "--retry-open", "3",
        "--stream-segment-threads", "3",
        "--stream-segment-timeout", "60",
        "-o", raw_file,
    ])

    if streamlink_debug:
        cmd += ["--loglevel", "debug"]
    elif verbose:
        cmd += ["--loglevel", "info"]
    else:
        cmd += ["--loglevel", "warning"]

    return cmd


def _stderr_reader_thread(proc, logger, tool_name, verbose=False):
    """Background thread that reads and logs stderr from the recording process.

    When verbose=False, aggressively filters ffmpeg's stream info, HLS metadata,
    and segment-level logging.  Only errors/warnings pass through.  Known benign
    patterns (like ffmpeg keepalive retries) are suppressed entirely.
    """
    NOISY_PATTERNS = (
        # HLS fragment-level noise
        '[hls @', 'skip (', "opening 'http", 'prefetch:',
        '[tcp @', 'starting connection', 'successfully connected',
        '[https @', '[tls @', '[aviocontext @', 'statistics:',
        'ext-x-', 'cuepoint', 'daterange', 'program-date-time',
        # ffmpeg stream info (printed on every connect/reconnect)
        'input #', 'output #', 'stream #', 'stream mapping',
        'duration:', 'variant_bitrate', 'metadata:', 'program 0',
        'encoder', 'press [q]', 'last message repeated',
        'handler_name', '[mpegts @', '[h264 @', '[aac @',
        'reinit context', 'increasing reorder',
        'parser not found', 'pix_fmt',
        '[vist#', '[aist#',             # ffmpeg internal stream context verbose lines
        # ffmpeg progress lines
        'size=', 'bitrate=', 'speed=',
        # yt-dlp debug noise
        '[debug]', 'format sorted', 'invoking ffmpeg',
        'command-line config', 'encodings:', 'loaded ',
        'optional libraries', 'proxy map', 'request handlers',
        'plugin directories', 'js runtimes',
    )

    # Patterns that contain "error"/"fail" keywords but are actually benign
    BENIGN_ERROR_PATTERNS = (
        'keepalive request failed',     # ffmpeg HLS keepalive retry (IPv6, harmless)
        'retrying with new connection', # ffmpeg successfully retries, no data loss
    )

    # Patterns suppressed even in verbose mode — lines that are genuinely harmless
    # but can flood the log at hundreds or thousands of lines per recording session.
    ALWAYS_SUPPRESS = (
        'timestamp discontinuity',      # MistServer HLS segments have inconsistent timestamps
                                        # at segment boundaries; ffmpeg corrects the offset
                                        # automatically with no data loss.  Fires ~2x per segment
                                        # (~every 2s), producing thousands of lines per session.
        'non-monotonic dts',            # Same root cause as timestamp discontinuity — MistServer
                                        # sends segments with out-of-order decode timestamps when
                                        # reconnecting to the master URL fallback.  ffmpeg fixes
                                        # them in place (incrementing each by 1) with no data loss.
                                        # Can produce hundreds of lines in a single burst.
        'consider increasing the value for the',  # ffmpeg advisory suggesting higher analyzeduration
                                        # / probesize when falling back to master URL.  Harmless;
                                        # our values are already set generously.
    )

    try:
        for line in proc.stderr:
            line = line.rstrip()
            if not line:
                continue
            lower = line.lower()

            # Always suppress these regardless of verbose setting
            if any(pat in lower for pat in ALWAYS_SUPPRESS):
                continue

            # Check if line looks like an error but is actually benign noise
            is_benign = any(pat in lower for pat in BENIGN_ERROR_PATTERNS)

            if not is_benign and any(kw in lower for kw in ['error', 'warning', 'fail', 'unable', 'denied', 'forbidden']):
                logger.warning(f"[{tool_name}] {line}")
            elif verbose:
                logger.info(f"[{tool_name}] {line}")
            else:
                if any(pat in lower for pat in NOISY_PATTERNS) or is_benign:
                    continue
                logger.info(f"[{tool_name}] {line}")
    except Exception:
        pass


def monitor_recording_process(proc, raw_file, start_time, max_record_hours,
                              platform, logger, status_queue, channel_key,
                              stop_event, last_status, file_creation_timeout=60,
                              tool_name_override=None, verbose=False,
                              max_file_size_gb=0.0, stream_info=""):
    """Monitor a recording subprocess and update status via queue.

    Spawns a background thread to read stderr for real-time logging.
    Detects zero-byte stalls, file growth stalls, max duration limits,
    file creation timeouts, and optional max-file-size splits.

    Returns (last_status, split_requested) where split_requested=True means
    the caller should immediately start a new segment rather than entering
    the reconnect grace period.
    """
    tool_name = tool_name_override or ("yt-dlp" if platform in ["kick", "youtube", "rumble", "custom"] else "streamlink")
    zero_byte_strikes = 0
    file_appeared = False
    split_requested = False

    # Stall detection: kill the process if the file stops growing for this long.
    # streamlink's --retry-streams keeps it alive even after the stream ends, so
    # without this the worker would never notice the stream dropped.
    STALL_TIMEOUT = 90  # seconds without file growth before terminating
    last_size = 0
    last_growth_time = time.monotonic()

    # Max-file-size limit (0 = disabled)
    max_file_size_bytes = int(max_file_size_gb * 1024 ** 3) if max_file_size_gb > 0 else 0

    # Start stderr reader thread
    stderr_thread = threading.Thread(
        target=_stderr_reader_thread, args=(proc, logger, tool_name, verbose),
        daemon=True, name=f"stderr-{channel_key}",
    )
    stderr_thread.start()

    while proc.poll() is None and not stop_event.is_set():
        elapsed = time.monotonic() - start_time

        try:
            if os.path.exists(raw_file):
                file_appeared = True
                size = os.path.getsize(raw_file)

                # Track file growth for stall detection
                if size > last_size:
                    last_size = size
                    last_growth_time = time.monotonic()

                stall_duration = time.monotonic() - last_growth_time

                # After 30 seconds with file existing but 0 bytes, something is wrong
                if elapsed > 30 and size == 0:
                    zero_byte_strikes += 1
                    if zero_byte_strikes > 6:  # 30 seconds of checking
                        logger.error("File exists but remains 0 bytes after 60+ seconds — terminating")
                        kill_process_tree(proc.pid, logger)
                        break
                # File grew at some point but has now stalled — stream likely ended
                elif last_size > 0 and elapsed > 60 and stall_duration > STALL_TIMEOUT:
                    logger.warning(
                        f"File hasn't grown in {stall_duration:.0f}s (last size: {human_size(last_size)}) "
                        f"— stream likely ended, terminating {tool_name}"
                    )
                    kill_process_tree(proc.pid, logger)
                    break
                else:
                    zero_byte_strikes = 0

                # Max-file-size split: gracefully stop so the caller can remux
                # this segment and immediately start a fresh one.
                if max_file_size_bytes > 0 and size >= max_file_size_bytes:
                    logger.info(
                        f"File reached {human_size(size)} — splitting "
                        f"(limit: {max_file_size_gb:.1f} GB)"
                    )
                    split_requested = True
                    kill_process_tree(proc.pid, logger)
                    break

                progress_pct = min(100, (elapsed / (max_record_hours * 3600)) * 100) if max_record_hours > 0 else 50

                # Build detail string: stream info when available, otherwise empty
                detail = stream_info if stream_info else ""

                new_status = {
                    "status": "Recording",
                    "detail": detail,
                    "size": human_size(size),
                    "time": format_elapsed(elapsed),
                    "progress": progress_pct,
                }
                if new_status != last_status:
                    status_queue.put((channel_key, new_status))
                    last_status = new_status.copy()

            elif not file_appeared and elapsed > file_creation_timeout:
                # File was never created — yt-dlp is probably stuck
                logger.error(
                    f"Output file was never created after {file_creation_timeout}s — "
                    f"killing {tool_name} (PID {proc.pid})"
                )
                kill_process_tree(proc.pid, logger)
                break

        except Exception as e:
            logger.warning(f"Error checking file size: {e}")

        time.sleep(5)

    # Wait for stderr thread to finish reading
    stderr_thread.join(timeout=5)

    return last_status, split_requested


def _probe_duration(ffmpeg_path, raw_file):
    """Return the duration of *raw_file* in seconds using ffprobe (or ffmpeg -i).

    Returns None if the duration cannot be determined.
    """
    # Derive ffprobe path from ffmpeg path (lives alongside ffmpeg)
    _ffmpeg_dir = os.path.dirname(ffmpeg_path)
    if os.name == "nt":
        ffprobe_path = os.path.join(_ffmpeg_dir, "ffprobe.exe") if _ffmpeg_dir else "ffprobe.exe"
    else:
        ffprobe_path = os.path.join(_ffmpeg_dir, "ffprobe") if _ffmpeg_dir else "ffprobe"
    try:
        result = subprocess.run(
            [ffprobe_path, "-v", "error",
             "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1",
             raw_file],
            capture_output=True, text=True, timeout=15,
        )
        val = result.stdout.strip()
        if val and val != "N/A":
            return float(val)
    except Exception:
        pass
    # Fallback: parse "Duration: HH:MM:SS.ss" from ffmpeg -i stderr
    try:
        result = subprocess.run(
            [ffmpeg_path, "-i", raw_file, "-hide_banner"],
            capture_output=True, text=True, timeout=15,
        )
        import re as _re
        m = _re.search(r"Duration:\s*(\d+):(\d{2}):(\d{2})\.(\d+)", result.stderr)
        if m:
            h, mn, s, cs = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
            return h * 3600 + mn * 60 + s + cs / 100
    except Exception:
        pass
    return None


def _run_remux_cmd(ffmpeg_cmd, ffmpeg_path, raw_file, mp4_file, logger, timeout,
                   file_size, label="Remux"):
    """Run a single ffmpeg remux command, streaming progress to the logger.

    ffmpeg is invoked with ``-progress pipe:1 -stats_period 5`` so it writes
    structured key=value progress lines to stdout every 5 seconds.  Errors
    still go to stderr.  A reader thread collects both streams; the main thread
    waits for the process to finish (or timeout).

    Progress log lines are emitted:
      • Every 10 percentage-points of completion (10 %, 20 %, … 90 %)
      • OR at least every 60 seconds if the file is very large and slow

    Returns ``(returncode, stderr_text)``.
    """
    import re as _re

    # Probe source duration so we can compute % complete from out_time_ms.
    total_secs = _probe_duration(ffmpeg_path, raw_file)

    # Inject progress reporting into the command.  Insert before the output
    # path (last positional argument, just before "-y").
    progress_flags = ["-progress", "pipe:1", "-stats_period", "5"]
    # Replace "-loglevel error" with just error-level logging (keep stderr clean)
    cmd = list(ffmpeg_cmd)
    # Insert progress flags right before the output file ("-y" is last; output
    # file is second-to-last)
    insert_pos = len(cmd) - 2  # before <output> -y
    cmd = cmd[:insert_pos] + progress_flags + cmd[insert_pos:]

    stdout_lines = []
    stderr_lines = []

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    def _read_stream(stream, buf):
        for line in stream:
            buf.append(line.rstrip())
        stream.close()

    t_err = threading.Thread(target=_read_stream, args=(proc.stderr, stderr_lines), daemon=True)
    t_err.start()

    # ── Progress state ────────────────────────────────────────────────────
    remux_start   = time.monotonic()
    last_log_time = remux_start
    last_log_pct  = -10        # force a log at 0 % on first update
    current_block = {}         # accumulates key=value pairs for one progress block
    LOG_PCT_STEP  = 10         # log every N percentage points
    LOG_TIME_GAP  = 60         # log at least every N seconds (large/slow files)

    def _maybe_log_progress():
        nonlocal last_log_time, last_log_pct

        out_time_us = current_block.get("out_time_us") or current_block.get("out_time_ms")
        if out_time_us is None:
            return
        try:
            elapsed_media_us = int(out_time_us)
        except ValueError:
            return

        now = time.monotonic()
        wall_elapsed = now - remux_start

        # Percentage complete (requires known duration)
        pct = None
        if total_secs and total_secs > 0:
            pct = min(100.0, (elapsed_media_us / 1_000_000) / total_secs * 100)

        # Decide whether to emit a log line
        pct_trigger  = (pct is not None) and (pct - last_log_pct >= LOG_PCT_STEP)
        time_trigger = (now - last_log_time) >= LOG_TIME_GAP

        if not (pct_trigger or time_trigger):
            return

        # Build the progress message
        speed_str = current_block.get("speed", "").strip()

        if pct is not None:
            bar = text_progress_bar(pct, width=10)
            # ETA from wall time + pct
            if pct > 1:
                total_est = wall_elapsed / (pct / 100)
                eta_secs  = max(0, total_est - wall_elapsed)
                eta_str   = f"ETA {format_elapsed(eta_secs)}"
            else:
                eta_str = "ETA calculating…"
            msg = f"{label}: {bar}  speed={speed_str}  {eta_str}"
        else:
            # No duration info — show elapsed wall time and speed only
            media_secs = elapsed_media_us / 1_000_000
            msg = (f"{label}: processed {format_elapsed(media_secs)} of stream  "
                   f"speed={speed_str}  wall={format_elapsed(wall_elapsed)}")

        logger.info(msg)
        last_log_time = now
        if pct is not None:
            last_log_pct = pct

    # ── Read stdout (ffmpeg progress blocks) ─────────────────────────────
    for line in proc.stdout:
        line = line.rstrip()
        stdout_lines.append(line)
        if "=" in line:
            key, _, val = line.partition("=")
            current_block[key.strip()] = val.strip()
            if key.strip() == "progress":
                # "progress=continue" or "progress=end" marks end of a block
                _maybe_log_progress()
                if val.strip() == "end":
                    current_block = {}
                else:
                    current_block = {}
    proc.stdout.close()

    # Wait for process with timeout
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        t_err.join(timeout=5)
        return -1, "\n".join(stderr_lines)

    t_err.join(timeout=5)
    return proc.returncode, "\n".join(stderr_lines)


def remux_to_mp4(raw_file, mp4_file, ffmpeg_path, logger, timeout=600):
    """Remux recorded .ts file to MP4 format."""
    # Probe for timed_id3 codec (common in Twitch streams)
    has_timed_id3 = False
    try:
        probe = subprocess.run(
            [ffmpeg_path, "-i", raw_file, "-hide_banner"],
            capture_output=True, text=True, timeout=10
        )
        if "timed_id3" in probe.stderr.lower():
            has_timed_id3 = True
            logger.info("Detected timed_id3 codec — will exclude from output")
    except Exception:
        pass

    if has_timed_id3:
        ffmpeg_cmd = [
            ffmpeg_path, "-i", raw_file,
            "-map", "0:v?", "-map", "0:a?",
            "-c", "copy", "-movflags", "+faststart",
            "-loglevel", "error", mp4_file, "-y",
        ]
    else:
        ffmpeg_cmd = [
            ffmpeg_path, "-i", raw_file,
            "-c", "copy", "-map", "0",
            "-movflags", "+faststart",
            "-loglevel", "error", mp4_file, "-y",
        ]

    file_size = os.path.getsize(raw_file) if os.path.exists(raw_file) else 0
    logger.info("Starting remux...")

    try:
        returncode, stderr_text = _run_remux_cmd(
            ffmpeg_cmd, ffmpeg_path, raw_file, mp4_file,
            logger, timeout, file_size, label="Remux"
        )

        if returncode == -1:
            logger.error("Remux timed out")
            return False, 0, "timeout"

        if returncode == 0 and os.path.exists(mp4_file):
            mp4_size = os.path.getsize(mp4_file)
            if mp4_size > 5 * 1024**2:
                logger.info(f"Remux successful: {human_size(mp4_size)}")
                return True, mp4_size, None
            else:
                logger.error(f"Remux produced small file: {human_size(mp4_size)}")
                return False, mp4_size, "output too small"
        else:
            import ctypes
            signed_code = ctypes.c_int32(returncode).value
            logger.error(f"Remux failed — returncode: {returncode} (signed: {signed_code})")
            if stderr_text:
                logger.error(f"FFmpeg stderr: {stderr_text}")

            # Fallback: strip metadata streams
            if signed_code == -22 or has_timed_id3 is False:
                logger.warning("Attempting fallback remux without metadata streams")
                fallback_cmd = [
                    ffmpeg_path, "-i", raw_file,
                    "-map", "0:v?", "-map", "0:a?",
                    "-c", "copy", "-movflags", "+faststart",
                    "-loglevel", "error", mp4_file, "-y",
                ]
                try:
                    fb_code, fb_stderr = _run_remux_cmd(
                        fallback_cmd, ffmpeg_path, raw_file, mp4_file,
                        logger, timeout, file_size, label="Fallback remux"
                    )
                    if fb_code == 0 and os.path.exists(mp4_file):
                        mp4_size = os.path.getsize(mp4_file)
                        if mp4_size > 5 * 1024**2:
                            logger.info(f"Fallback remux successful: {human_size(mp4_size)}")
                            return True, mp4_size, None
                    logger.error(f"Fallback remux also failed: {fb_code}")
                    if fb_stderr:
                        logger.error(f"Fallback stderr: {fb_stderr}")
                except Exception as e:
                    logger.error(f"Fallback remux error: {e}")
            return False, 0, f"code {signed_code}"

    except FileNotFoundError:
        logger.error("ffmpeg not found in PATH")
        return False, 0, "ffmpeg not found"
    except Exception as e:
        logger.error(f"Remux error: {e}")
        return False, 0, str(e)



def save_metadata(mp4_file, username, platform, start_time_str, duration_seconds, title=None):
    """Save a JSON metadata sidecar alongside the MP4 file."""
    meta_file = mp4_file.rsplit('.', 1)[0] + '.meta.json'
    metadata = {
        'version': __version__,
        'channel': username,
        'platform': platform,
        'recording_started': start_time_str,
        'duration_seconds': round(duration_seconds, 1),
        'duration_human': format_elapsed(duration_seconds),
        'stream_title': title,
        'file': os.path.basename(mp4_file),
        'recorded_at': datetime.datetime.now().isoformat(),
    }
    try:
        with open(meta_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.warning(f"Failed to save metadata: {e}")


# ────────────────────────────────────────────────
#          Recording Worker
# ────────────────────────────────────────────────

def record_worker(args):
    """Worker process to record a single channel."""
    (channel_entry, config_dict, stop_event, status_queue) = args

    # Reconstruct config in child process
    config = configparser.ConfigParser()
    config.read_dict(config_dict)

    # Parse channel entry
    if ":" in channel_entry:
        platform, username = channel_entry.split(":", 1)
    else:
        platform = "kick"
        username = channel_entry

    channel_key = channel_entry

    # Get configuration
    root_path = config.get('Paths', 'streams_dir')
    quality = config.get('Recording', 'quality')
    max_record_hours = config.getfloat('Recording', 'max_record_hours')
    min_file_size_mb = config.getfloat('Recording', 'min_file_size_mb')
    min_disk_space_gb = config.getfloat('Recording', 'min_disk_space_gb')
    max_file_size_gb  = config.getfloat('Recording', 'max_file_size_gb')
    verbose = config.getboolean('Advanced', 'verbose')
    streamlink_debug = config.getboolean('Advanced', 'streamlink_debug')
    ffmpeg_path = config.get('Advanced', 'ffmpeg_path')
    stream_check_timeout = config.getint('Timeouts', 'stream_check_timeout')
    ffmpeg_timeout = config.getint('Timeouts', 'ffmpeg_timeout')
    poll_interval_minutes = config.getfloat('Timeouts', 'poll_interval_minutes')
    poll_jitter_percent = config.getint('Timeouts', 'poll_jitter_percent')
    error_backoff_max_minutes = config.getfloat('Timeouts', 'error_backoff_max_minutes')
    reconnect_grace_minutes = config.getint('Timeouts', 'reconnect_grace_minutes')
    file_creation_timeout = config.getint('Timeouts', 'file_creation_timeout')
    filename_pattern = config.get('Recording', 'filename_pattern')

    # Cookies file
    cookies_file = config.get('Paths', 'cookies_file', fallback='') or None
    # In child process the auto-detect ran in the parent; re-check here
    if not cookies_file:
        for candidate in [
            os.path.join(root_path, 'cookies.txt'),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookies.txt'),
        ]:
            if os.path.isfile(candidate):
                cookies_file = candidate
                break

    # Setup logging
    logger = setup_child_logging(root_path, channel_key)
    logger.info(f"Worker STARTED (PID {os.getpid()})")
    if cookies_file:
        logger.info(f"Using cookies file: {cookies_file}")

    # Build URL
    if platform == "kick":
        url = f"https://kick.com/{username}"
    elif platform == "twitch":
        url = f"https://twitch.tv/{username}"
    elif platform == "youtube":
        if username.startswith("UC"):
            url = f"https://youtube.com/channel/{username}/live"
        elif username.startswith("@"):
            url = f"https://youtube.com/{username}/live"
        elif "watch?v=" in username or len(username) == 11:
            if "watch?v=" in username:
                url = username if username.startswith("http") else f"https://youtube.com/{username}"
            else:
                url = f"https://youtube.com/watch?v={username}"
        else:
            url = f"https://youtube.com/@{username}/live"
    elif platform == "rumble":
        url = f"https://rumble.com/c/{username}"
    elif platform == "fishtank":
        # username is the camera name / stream ID (e.g. "director", "dirc-5")
        # The real URL is built per-poll using the JWT; store a placeholder
        url = f"fishtank:{username}"
    elif platform == "custom":
        # Custom: the username IS the full URL
        url = username
        # Override username with domain for display/filename/directory purposes
        username = extract_domain_from_url(url)
    else:
        url = f"https://{platform}.com/{username}"

    # Directory paths (but DON'T create them yet — wait until we actually record)
    recorded_base = os.path.join(root_path, "Recorded")
    processed_base = os.path.join(root_path, "Processed")
    pending_base = os.path.join(root_path, PENDING_DELETION_FOLDER)

    recorded_path = os.path.join(recorded_base, platform, username)
    processed_path = os.path.join(processed_base, platform, username)
    pending_dir = os.path.join(pending_base, platform, username)

    last_status = None
    stream_title = None  # populated on check

    # ── Polling state ──
    # Normal offline: flat interval with jitter — no backoff.
    # Error: exponential backoff from poll_interval up to error_backoff_max.
    # Reconnect: fast 15s polling for reconnect_grace_minutes after a stream drops.
    poll_base_seconds = poll_interval_minutes * 60
    error_sleep_seconds = poll_base_seconds          # current error backoff (grows on consecutive errors)
    error_backoff_max_seconds = error_backoff_max_minutes * 60
    consecutive_errors = 0

    # Fast reconnect state
    reconnect_mode = False
    reconnect_deadline = 0  # monotonic timestamp when grace period expires
    RECONNECT_POLL_INTERVAL = 15  # seconds between checks during grace period

    # Consecutive small-file / bad-stream detection.
    # If the stream keeps producing tiny recordings that fail the remux size check
    # (e.g. a stub HLS playlist serving ~20 s of garbage), we back off rather than
    # hammering the server in a tight loop.  Counter resets on any successful remux.
    consecutive_small_remux_fails = 0
    SMALL_REMUX_FAIL_LIMIT = 3        # enter backoff after this many in a row
    SMALL_REMUX_BACKOFF_BASE = 60     # first backoff: 60 s
    SMALL_REMUX_BACKOFF_MAX = 900     # cap at 15 min
    _small_remux_backoff = SMALL_REMUX_BACKOFF_BASE

    # Stream metadata shown in the status table (set when variant is resolved)
    stream_info = ""

    # Fishtank: create a per-worker auth manager that caches the JWT
    fishtank_auth = None
    if platform == "fishtank":
        ft_email = config.get('Fishtank', 'email', fallback='')
        ft_password = config.get('Fishtank', 'password', fallback='')
        fishtank_auth = FishtankAuth(cookies_file, logger,
                                     email=ft_email, password=ft_password)
        auth_method = "email+password" if (ft_email and ft_password) else "cookie jar"
        logger.info(
            f"[fishtank] Auth manager initialised for stream '{username}' "
            f"(auth method: {auth_method})")

    # Initial stagger: randomize the very first check so workers don't all fire at once
    initial_delay = random.uniform(0, min(poll_base_seconds, 10))
    if initial_delay > 1:
        logger.info(f"Initial stagger: waiting {initial_delay:.0f}s before first check")
        time.sleep(initial_delay)

    while not stop_event.is_set():
        try:
            # Check disk space
            has_space, free_gb = check_disk_space(root_path, min_disk_space_gb)
            if not has_space:
                new_status = {
                    "status": "Error",
                    "detail": f"Low disk space: {free_gb:.1f}GB",
                    "size": "", "time": "", "progress": 0,
                }
                if new_status != last_status:
                    status_queue.put((channel_key, new_status))
                    last_status = new_status.copy()
                logger.error(f"Insufficient disk space: {free_gb:.1f}GB available, {min_disk_space_gb}GB required")
                time.sleep(300)
                continue

            new_status = {"status": "Checking...", "detail": "", "size": "", "time": "", "progress": 0}
            if new_status != last_status:
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()

            # Check if stream is live
            need_impersonate = False
            recording_url = url  # may be overridden by resolved URL
            if platform == "kick":
                # Try streamlink for Kick first — it has a JS challenge solver
                # for Cloudflare that yt-dlp lacks.  Use longer timeout since
                # the first check may need to launch a headless browser.
                kick_timeout = max(stream_check_timeout, 45)
                is_live, stream_title, error = check_stream_kick_api(
                    username, logger, kick_timeout, cookies_file)
                if is_live is None:
                    # streamlink check failed — fall back to yt-dlp
                    logger.info("Kick streamlink check inconclusive — falling back to yt-dlp")
                    is_live, stream_title, error, need_impersonate, resolved_url = check_stream_ytdlp(url, logger, stream_check_timeout, cookies_file)
                    if resolved_url:
                        recording_url = resolved_url
                        logger.info(f"Using resolved URL for recording: {recording_url}")
            elif platform == "rumble":
                is_live, stream_title, rumble_live_url, error = check_stream_rumble_html(username, logger, stream_check_timeout, cookies_file)
                if is_live and rumble_live_url:
                    # HTML scraper found the live video page URL.
                    # Pass it directly to yt-dlp (single video page — works fine).
                    recording_url = rumble_live_url
                    logger.info(f"Rumble live video URL: {recording_url}")
                    # Use yt-dlp to confirm live status and get the stream title
                    is_live, stream_title, error, need_impersonate, resolved_url = check_stream_ytdlp(
                        rumble_live_url, logger, stream_check_timeout, cookies_file
                    )
                    if resolved_url:
                        recording_url = resolved_url
            elif platform in ["youtube", "custom"]:
                is_live, stream_title, error, need_impersonate, resolved_url = check_stream_ytdlp(url, logger, stream_check_timeout, cookies_file)
                # If yt-dlp resolved to a different URL (e.g. Rumble channel -> video),
                # use the resolved URL for recording so yt-dlp can actually download it
                if resolved_url:
                    recording_url = resolved_url
                    logger.info(f"Using resolved URL for recording: {recording_url}")
            elif platform == "fishtank":
                stream_id = fishtank_auth.resolve_stream_id(username)
                is_live, stream_title, error = check_stream_fishtank(
                    stream_id, fishtank_auth, logger, stream_check_timeout
                )
                if is_live:
                    # Build the authenticated HLS URL fresh each time we're about to record
                    hls_url, jwt = fishtank_auth.build_stream_url(stream_id)
                    if hls_url:
                        # Resolve the best-quality variant from the master playlist
                        # so ffmpeg always records at the highest available bitrate
                        # rather than whichever rendition the server lists first.
                        recording_url, stream_info = resolve_best_fishtank_variant(
                            hls_url, jwt, logger, timeout=stream_check_timeout
                        )
                    else:
                        is_live = False
                        error = "could not build stream URL (JWT missing)"
            else:
                is_live, stream_title, error = check_stream_streamlink(url, logger, stream_check_timeout)

            if error:
                consecutive_errors += 1
                new_status = {"status": "Error", "detail": error, "size": "", "time": "", "progress": 0}
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()
                # Exponential backoff for errors (doubles each time, capped)
                error_sleep_seconds = min(error_sleep_seconds * 2, error_backoff_max_seconds)
                sleep_time = jittered_sleep(error_sleep_seconds, poll_jitter_percent)
                logger.warning(f"Error (#{consecutive_errors}): {error} — backing off {sleep_time:.0f}s")
                time.sleep(sleep_time)
                continue

            if not is_live:
                # Successful check (no error), reset error backoff
                consecutive_errors = 0
                error_sleep_seconds = poll_base_seconds

                if reconnect_mode:
                    # We were recently recording — check if grace period expired
                    if time.monotonic() < reconnect_deadline:
                        remaining = int(reconnect_deadline - time.monotonic())
                        new_status = {"status": "Offline", "detail": f"reconnecting ({remaining}s left)", "size": "", "time": "", "progress": 0}
                        if new_status != last_status:
                            status_queue.put((channel_key, new_status))
                            last_status = new_status.copy()
                        logger.info(f"Stream dropped — fast polling ({remaining}s remaining in grace period)")
                        time.sleep(RECONNECT_POLL_INTERVAL)
                        continue
                    else:
                        # Grace period expired — stream didn't come back
                        logger.info("Reconnect grace period expired — resuming normal offline polling")
                        reconnect_mode = False

                # Normal offline — flat interval with jitter (no backoff)
                sleep_time = jittered_sleep(poll_base_seconds, poll_jitter_percent)
                new_status = {"status": "Offline", "detail": f"next check ~{int(sleep_time)}s", "size": "", "time": "", "progress": 0}
                if new_status != last_status:
                    status_queue.put((channel_key, new_status))
                    last_status = new_status.copy()
                logger.info(f"Stream offline — sleeping {sleep_time:.0f}s (base {poll_interval_minutes}min ±{poll_jitter_percent}%)")
                time.sleep(sleep_time)
                continue

            # Stream is live — reset error state and reconnect mode
            consecutive_errors = 0
            error_sleep_seconds = poll_base_seconds
            if reconnect_mode:
                logger.info("Stream reconnected — resuming recording")
                reconnect_mode = False

            # Create directories only when we're about to record
            os.makedirs(recorded_path, exist_ok=True)
            os.makedirs(processed_path, exist_ok=True)
            os.makedirs(pending_dir, exist_ok=True)
            logger.info(f"Folders ready: {recorded_path}")

            logger.info("Stream detected as live — starting capture")

            # Build output filename
            base_name = build_filename(filename_pattern, username, platform, stream_title)
            raw_file = os.path.join(recorded_path, f"{base_name}.ts")
            start_wall = datetime.datetime.now()
            start_time = time.monotonic()

            new_status = {"status": "Recording", "detail": "starting", "size": "0 B", "time": "0:00", "progress": 0}
            if new_status != last_status:
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()

            # Build recording command
            if platform == "kick":
                # Use streamlink for Kick — it has a JS challenge solver for
                # Cloudflare that yt-dlp lacks.  Same approach as Twitch.
                record_cmd = build_recording_command_streamlink(url, raw_file, quality, platform, config, verbose, streamlink_debug)
            elif platform == "fishtank":
                # recording_url was set to the JWT-bearing HLS URL in the check block
                record_cmd = build_recording_command_fishtank(recording_url, raw_file, config, verbose)
            elif platform in ["youtube", "rumble", "custom"]:
                record_cmd = build_recording_command_ytdlp(recording_url, raw_file, config, verbose,
                                                           streamlink_debug, cookies_file,
                                                           impersonate=need_impersonate)
            else:
                record_cmd = build_recording_command_streamlink(url, raw_file, quality, platform, config, verbose, streamlink_debug)

            logger.info(f"Starting recording: {' '.join(record_cmd)}")

            proc = subprocess.Popen(
                record_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            # Monitor
            last_status, split_requested = monitor_recording_process(
                proc, raw_file, start_time, max_record_hours,
                platform, logger, status_queue, channel_key,
                stop_event, last_status, file_creation_timeout,
                verbose=verbose,
                max_file_size_gb=max_file_size_gb,
                stream_info=stream_info,
            )

            # Clean up process tree
            if proc.poll() is None:
                logger.info("Stopping recording — killing process tree...")
                kill_process_tree(proc.pid, logger)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning("Process did not terminate after tree kill, force killing...")
                    try:
                        proc.kill()
                        proc.wait(timeout=5)
                    except Exception:
                        pass

            exit_code = proc.returncode
            elapsed = time.monotonic() - start_time
            tool_name = "yt-dlp" if platform in ["kick", "youtube", "rumble", "custom"] else ("ffmpeg" if platform == "fishtank" else "streamlink")
            logger.info(f"{tool_name} exited with code: {exit_code}")

            # If the recording ended on its own (not user-initiated stop) and
            # we captured at least some data, the stream likely dropped.
            # Enter fast reconnect mode to re-check quickly.
            # Exception: a size-based split should skip the grace period entirely —
            # the stream is still live, so remux and immediately start the next segment.
            if split_requested:
                reconnect_mode = False   # don't wait — fall straight through to remux+restart
            elif not stop_event.is_set() and elapsed > 10:
                reconnect_mode = True
                reconnect_deadline = time.monotonic() + (reconnect_grace_minutes * 60)
                logger.info(f"Recording ended after {format_elapsed(elapsed)} — entering {reconnect_grace_minutes}min reconnect grace period")

            time.sleep(2)  # let file handles be released

            # ── Streamlink fallback for Kick ──
            # If yt-dlp failed to create the file for a Kick stream, try
            # streamlink as a fallback.  Recent streamlink versions (6.x+)
            # support Kick natively and handle HLS differently.
            if not os.path.exists(raw_file) and platform == "kick" and not stop_event.is_set():
                logger.warning("yt-dlp failed to create output file for Kick — trying streamlink fallback...")
                new_status = {"status": "Recording", "detail": "fallback (streamlink)", "size": "0 B", "time": "0:00", "progress": 0}
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()

                fallback_cmd = build_recording_command_streamlink_kick(url, raw_file, quality, verbose, streamlink_debug)
                logger.info(f"Fallback recording: {' '.join(fallback_cmd)}")

                start_time = time.monotonic()
                start_wall = datetime.datetime.now()

                try:
                    proc = subprocess.Popen(
                        fallback_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                    )

                    last_status, _ = monitor_recording_process(
                        proc, raw_file, start_time, max_record_hours,
                        platform, logger, status_queue, channel_key,
                        stop_event, last_status, file_creation_timeout,
                        tool_name_override="streamlink", verbose=verbose,
                        max_file_size_gb=max_file_size_gb,
                    )

                    if proc.poll() is None:
                        kill_process_tree(proc.pid, logger)
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            try:
                                proc.kill()
                                proc.wait(timeout=5)
                            except Exception:
                                pass

                    elapsed = time.monotonic() - start_time
                    logger.info(f"streamlink (fallback) exited with code: {proc.returncode}")
                    time.sleep(2)

                except FileNotFoundError:
                    logger.error("streamlink not found — cannot use as fallback for Kick")
                except Exception as e:
                    logger.error(f"Streamlink fallback failed: {e}")

            # Check if we actually recorded something
            if not os.path.exists(raw_file):
                logger.error("Recording file was never created!")
                new_status = {"status": "Error", "detail": "file not created", "size": "", "time": "", "progress": 0}
                status_queue.put((channel_key, new_status))
                time.sleep(60)
                continue

            file_size = os.path.getsize(raw_file)
            logger.info(f"Recording finished — file size: {human_size(file_size)}")

            if file_size < min_file_size_mb * 1024 * 1024:
                logger.warning(f"Recording too small ({human_size(file_size)}) — deleting")
                try:
                    os.remove(raw_file)
                except Exception as e:
                    logger.error(f"Failed to delete small file: {e}")
                new_status = {"status": "Offline", "detail": "no data captured", "size": "", "time": "", "progress": 0}
                status_queue.put((channel_key, new_status))
                time.sleep(random.uniform(5, 15))
                continue

            # Remux to MP4
            new_status = {"status": "Remuxing...", "detail": human_size(file_size), "size": "", "time": "", "progress": 0}
            if new_status != last_status:
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()

            mp4_file = os.path.join(processed_path, f"{base_name}.mp4")
            # Scale timeout based on file size: base timeout or 1 minute per GB, whichever is larger
            file_size_gb = file_size / (1024**3)
            scaled_timeout = max(ffmpeg_timeout, int(file_size_gb * 60) + 120)
            if scaled_timeout > ffmpeg_timeout:
                logger.info(f"Large file ({human_size(file_size)}) — remux timeout scaled to {scaled_timeout}s")
            success, mp4_size, error = remux_to_mp4(raw_file, mp4_file, ffmpeg_path, logger, scaled_timeout)

            if success:
                # Successful remux — reset bad-stream counters
                consecutive_small_remux_fails = 0
                _small_remux_backoff = SMALL_REMUX_BACKOFF_BASE

                # Save metadata sidecar
                save_metadata(
                    mp4_file, username, platform,
                    start_wall.isoformat(),
                    elapsed,
                    stream_title,
                )

                # Move raw file to pending deletion
                pending_path = os.path.join(pending_dir, os.path.basename(raw_file))
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        shutil.move(raw_file, pending_path)
                        logger.info(f"Moved raw to: {pending_path}")
                        break
                    except PermissionError as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"File locked, retrying in 2s… (attempt {attempt + 1}/{max_retries})")
                            time.sleep(2)
                        else:
                            logger.error(f"Move failed after {max_retries} attempts: {e}")
                            logger.info(f"File will be cleaned up on next run: {raw_file}")
                    except Exception as e:
                        logger.error(f"Move failed: {e}")
                        break

                new_status = {
                    "status": "Completed",
                    "detail": human_size(mp4_size),
                    "size": "", "time": "", "progress": 100,
                }
            else:
                logger.error(f"Remux failed: {error}")
                new_status = {"status": "Remux failed", "detail": error or "unknown", "size": "", "time": "", "progress": 0}

                # ── Bad-stream backoff (Bug #1 fix) ──────────────────────────
                # "output too small" means the stream is handing us stub/garbage
                # data that passes the raw min_file_size_mb check but produces a
                # worthless remux.  Delete the tiny mp4 artifact and back off
                # instead of hammering the server in a tight 5-second loop.
                if error == "output too small":
                    # Delete the useless sub-threshold mp4 that was written
                    if os.path.exists(mp4_file):
                        try:
                            os.remove(mp4_file)
                            logger.info(f"Deleted undersized mp4 artifact: {os.path.basename(mp4_file)}")
                        except Exception as del_err:
                            logger.warning(f"Could not delete undersized mp4: {del_err}")

                    # Also move the raw .ts to PendingDeletion so it doesn't
                    # accumulate in the Recorded directory
                    try:
                        pending_path = os.path.join(pending_dir, os.path.basename(raw_file))
                        shutil.move(raw_file, pending_path)
                        logger.info(f"Moved bad raw .ts to PendingDeletion: {os.path.basename(raw_file)}")
                    except Exception:
                        pass

                    consecutive_small_remux_fails += 1
                    if consecutive_small_remux_fails >= SMALL_REMUX_FAIL_LIMIT:
                        # Stream is consistently serving garbage — treat as
                        # degraded/offline and apply a growing backoff so we
                        # don't keep hammering its CDN.
                        backoff = min(_small_remux_backoff, SMALL_REMUX_BACKOFF_MAX)
                        _small_remux_backoff = min(_small_remux_backoff * 2, SMALL_REMUX_BACKOFF_MAX)
                        logger.warning(
                            f"Stream has produced {consecutive_small_remux_fails} consecutive "
                            f"undersized recordings — treating as degraded, backing off {backoff}s"
                        )
                        new_status = {
                            "status": "Offline",
                            "detail": f"bad stream data, retry in {backoff}s",
                            "size": "", "time": "", "progress": 0,
                        }
                        if new_status != last_status:
                            status_queue.put((channel_key, new_status))
                            last_status = new_status.copy()
                        # Disable reconnect mode so normal offline polling resumes
                        reconnect_mode = False
                        time.sleep(backoff)
                        continue

            if new_status != last_status:
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()

            time.sleep(5)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received — exiting")
            break
        except Exception as e:
            logger.error(f"Worker crashed: {e}", exc_info=True)
            status_queue.put((channel_key, {"status": "Error", "detail": str(e)[:50], "size": "", "time": "", "progress": 0}))
            time.sleep(60)

    logger.info("Worker STOPPED")


# ────────────────────────────────────────────────
#          Background Cleanup Thread
# ────────────────────────────────────────────────

# Guards _process_leftover_files() so that the per-channel cleanup thread
# (spawned by stop_channel) and the global background cleanup thread
# (spawned by stop()) never scan/remux the same .ts files concurrently.
_cleanup_lock = threading.Lock()


class BackgroundCleaner:
    """Handles remuxing and cleanup of leftover .ts files.

    Runs after recording stops (never during active recording) to remux
    raw .ts files to .mp4, save metadata sidecars, and manage the
    PendingDeletion folder.
    """

    def __init__(self, config):
        self.config = config
        self.root_path = config.get('Paths', 'streams_dir')
        self.recorded_base = os.path.join(self.root_path, "Recorded")
        self.processed_base = os.path.join(self.root_path, "Processed")
        self.pending_base = os.path.join(self.root_path, PENDING_DELETION_FOLDER)
        self._thread = None
        self._stop_event = threading.Event()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="BackgroundCleaner")
        self._thread.start()
        logging.info("Background cleanup thread started")

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=15)
        logging.info("Background cleanup thread stopped")

    def _run(self):
        # Wait for file handles to be released and for any in-progress per-channel
        # remux threads (spawned by stop_channel) to finish before we start scanning.
        # We use a longer initial wait (30 s) then try to acquire the cleanup lock;
        # if another cleanup is still running we wait for it to finish (Bug #3 fix).
        logging.info("Cleanup: Waiting 30 seconds for in-flight remuxes and file handles to be released...")
        for _ in range(30):
            if self._stop_event.is_set():
                return
            time.sleep(1)

        # If a per-channel cleanup thread still holds the lock, wait for it.
        logging.info("Cleanup: Waiting for any concurrent cleanup pass to finish...")
        _cleanup_lock.acquire()
        _cleanup_lock.release()

        for pass_num in range(3):
            if self._stop_event.is_set():
                break
            found_locked = self._process_leftover_files()
            if not found_locked:
                break
            logging.info(f"Cleanup: Some files were locked, waiting 10s before retry (pass {pass_num + 2}/3)")
            for _ in range(10):
                if self._stop_event.is_set():
                    return
                time.sleep(1)

        logging.info("Background cleanup finished")

    def _process_leftover_files(self):
        # Only one cleanup scan may run at a time across all threads (Bug #2 fix).
        # This prevents the per-channel cleanup thread and the global background
        # cleanup thread from racing on the same .ts files simultaneously.
        if not _cleanup_lock.acquire(blocking=False):
            logging.info("Cleanup: another cleanup pass is already running — skipping duplicate")
            return False

        try:
            return self._process_leftover_files_locked()
        finally:
            _cleanup_lock.release()

    def _process_leftover_files_locked(self):
        ffmpeg_path = self.config.get('Advanced', 'ffmpeg_path')
        ffmpeg_timeout = self.config.getint('Timeouts', 'ffmpeg_timeout')
        min_file_size_mb = self.config.getfloat('Recording', 'min_file_size_mb')

        found_any = False
        found_locked = False

        for platform in ["twitch", "youtube", "kick", "rumble", "fishtank", "custom"]:
            platform_dir = os.path.join(self.recorded_base, platform)
            if not os.path.exists(platform_dir):
                continue

            for username_dir in os.listdir(platform_dir):
                username_path = os.path.join(platform_dir, username_dir)
                if not os.path.isdir(username_path):
                    continue

                processed_path = os.path.join(self.processed_base, platform, username_dir)
                pending_dir = os.path.join(self.pending_base, platform, username_dir)
                os.makedirs(processed_path, exist_ok=True)
                os.makedirs(pending_dir, exist_ok=True)

                for filename in os.listdir(username_path):
                    if not filename.endswith('.ts'):
                        continue

                    raw_file = os.path.join(username_path, filename)

                    try:
                        size1 = os.path.getsize(raw_file)
                        time.sleep(2)
                        size2 = os.path.getsize(raw_file)
                        if size2 != size1:
                            logging.warning(f"Cleanup: {filename} is still growing, skipping")
                            found_locked = True
                            continue
                        file_size = size2
                    except Exception:
                        continue

                    if file_size < min_file_size_mb * 1024 * 1024:
                        logging.info(f"Cleanup: Skipping {filename} — too small ({human_size(file_size)})")
                        try:
                            os.remove(raw_file)
                        except PermissionError:
                            found_locked = True
                        except Exception:
                            pass
                        continue

                    found_any = True
                    logging.info(f"Cleanup: Processing {filename} ({human_size(file_size)})")

                    mp4_filename = filename.replace('.ts', '.mp4')
                    mp4_file = os.path.join(processed_path, mp4_filename)

                    if os.path.exists(mp4_file) and os.path.getsize(mp4_file) > 5 * 1024**2:
                        logging.info(f"Cleanup: MP4 already exists for {filename}, just moving raw file")
                    else:
                        file_size_gb = file_size / (1024**3)
                        scaled_timeout = max(ffmpeg_timeout, int(file_size_gb * 60) + 120)
                        success, mp4_size, error = remux_to_mp4(
                            raw_file, mp4_file, ffmpeg_path,
                            logging.getLogger(), scaled_timeout
                        )
                        if not success:
                            logging.error(f"Cleanup: Failed to process {filename}: {error}")
                            try:
                                pending_path = os.path.join(pending_dir, filename)
                                shutil.move(raw_file, pending_path)
                                logging.info(f"Cleanup: Moved unprocessable {filename} to PendingDeletion")
                            except Exception as move_err:
                                logging.warning(f"Cleanup: Could not move {filename} to PendingDeletion: {move_err}")
                            continue

                    pending_path = os.path.join(pending_dir, filename)
                    max_retries = 5
                    moved = False
                    for attempt in range(max_retries):
                        if self._stop_event.is_set():
                            return found_locked
                        try:
                            shutil.move(raw_file, pending_path)
                            logging.info(f"Cleanup: Successfully processed {mp4_filename}")
                            moved = True
                            break
                        except PermissionError:
                            if attempt < max_retries - 1:
                                wait_time = 3 * (attempt + 1)
                                logging.warning(f"Cleanup: File locked, retrying in {wait_time}s…")
                                time.sleep(wait_time)
                            else:
                                logging.warning(f"Cleanup: Could not move {filename} after {max_retries} attempts")
                                found_locked = True
                        except Exception as e:
                            logging.error(f"Cleanup: Failed to move {filename}: {e}")
                            break

        if found_any:
            logging.info("Cleanup: Pass complete")
        else:
            logging.info("Cleanup: No unprocessed .ts files found")

        return found_locked


# ────────────────────────────────────────────────
#          Auto-Purge PendingDeletion
# ────────────────────────────────────────────────

def purge_old_pending_files(root_path, max_age_days, logger=None):
    """Delete files in PendingDeletion that are older than max_age_days.

    Returns the number of files deleted.

    Age is measured as time since the file was *moved* into PendingDeletion,
    not since it was originally recorded.  On Windows, shutil.move() preserves
    the source mtime, so we use max(mtime, ctime) — ctime reflects the last
    metadata change (i.e. the move) on Windows, giving the correct age.
    """
    if max_age_days <= 0:
        return 0

    pending_base = os.path.join(root_path, PENDING_DELETION_FOLDER)
    if not os.path.exists(pending_base):
        return 0

    if logger:
        logger.info(f"PendingDeletion purge: scanning for files older than {max_age_days} day(s)...")

    cutoff = time.time() - (max_age_days * 86400)
    deleted = 0
    skipped = 0
    empty_dirs = []

    for dirpath, dirnames, filenames in os.walk(pending_base, topdown=False):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            try:
                stat = os.stat(filepath)
                # Use the most recent of mtime and ctime.  On Windows, ctime is the
                # file-creation time (reset when the file is moved), so it correctly
                # reflects when the file landed in PendingDeletion rather than when
                # the original recording started.
                file_age_ts = max(stat.st_mtime, stat.st_ctime)
                if file_age_ts < cutoff:
                    os.remove(filepath)
                    deleted += 1
                    if logger:
                        logger.info(f"Purged: {filepath}")
                else:
                    skipped += 1
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to purge {filepath}: {e}")

        # Track empty directories for cleanup
        if not filenames and not dirnames and dirpath != pending_base:
            empty_dirs.append(dirpath)

    # Remove empty directories
    for d in empty_dirs:
        try:
            os.rmdir(d)
        except Exception:
            pass

    if logger:
        if deleted > 0:
            logger.info(f"PendingDeletion purge: deleted {deleted} file(s) older than {max_age_days} days ({skipped} retained)")
        else:
            logger.info(f"PendingDeletion purge: nothing to delete ({skipped} file(s) retained, not yet old enough)")

    return deleted


# ────────────────────────────────────────────────
#          StreamRecorder
# ────────────────────────────────────────────────

class StreamRecorder:
    """Main recorder that manages worker processes."""

    def __init__(self, channels, config):
        self.channels = channels
        self.config = config

        self.root_path = config.get('Paths', 'streams_dir')
        self.recorded_base = os.path.join(self.root_path, "Recorded")
        self.processed_base = os.path.join(self.root_path, "Processed")
        self.pending_base = os.path.join(self.root_path, PENDING_DELETION_FOLDER)

        os.makedirs(self.recorded_base, exist_ok=True)
        os.makedirs(self.processed_base, exist_ok=True)
        os.makedirs(self.pending_base, exist_ok=True)

        self.manager = mp.Manager()
        self.status_queue = self.manager.Queue()
        self.status_dict = {}

        for ch in channels:
            self.status_dict[ch] = {"status": "Initializing", "detail": "", "size": "", "time": "", "progress": 0}

        # Maps channel_name -> list[mp.Process].  A channel may temporarily have
        # more than one entry while the old process is dying and the new one is
        # starting; the list is pruned of dead entries on every monitor tick.
        self.processes: dict = {}  # {channel_name: [mp.Process, ...]}
        self.should_stop = mp.Event()
        self.is_running = False
        self.stopped_channels = set()  # channels individually stopped by user
        # Guards against spawning a second worker for a channel while start_channel
        # is already in progress for that same channel (e.g. rapid double-click).
        self._spawning: set = set()
        self._spawn_lock = threading.Lock()
        self.cleaner = BackgroundCleaner(config)

    def update_status_from_queue(self):
        while not self.status_queue.empty():
            try:
                ch, new_status = self.status_queue.get_nowait()
                if ch in self.status_dict:
                    self.status_dict[ch] = new_status
            except Exception:
                break

    def stop_channel(self, channel_name):
        """Stop ALL worker processes for a channel and trigger cleanup for its files.

        The channel is marked as individually stopped so refresh_status can show
        'Stopped' instead of 'Offline', and so the master Stop doesn't double-kill it.

        Important: a channel may have more than one live process if start_channel was
        called while the old process was still alive (e.g. rapid stop/restart).  We
        kill every one of them so no ghost workers keep running after a 'stop'.
        """
        if not self.is_running:
            return

        # Collect recording info before killing (from any still-running process)
        st = self.status_dict.get(channel_name, {})
        size_str = st.get("size", "")
        time_str = st.get("time", "")

        # Find every process registered for this channel
        procs = self.processes.get(channel_name, [])
        alive_procs = [p for p in procs if p.is_alive()]

        if not alive_procs:
            logging.info(f"Channel {channel_name} is not actively running")
            self.status_dict[channel_name] = {
                "status": "Stopped", "detail": "by user", "size": "", "time": "", "progress": 0
            }
            # Still mark as stopped so the monitor loop doesn't auto-restart it
            self.stopped_channels.add(channel_name)
            # Drain any lingering dead processes from the list
            self.processes[channel_name] = []
            return

        # Kill them all
        for proc in alive_procs:
            logging.info(f"Stopping channel {channel_name} (PID {proc.pid})")
            kill_process_tree(proc.pid)

        for proc in alive_procs:
            proc.join(timeout=10)
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=5)

        # Log summary for this channel
        if size_str and time_str:
            logging.info(f"Channel stopped — {channel_name}: {size_str}, {time_str}")
        else:
            logging.info(f"Channel stopped — {channel_name}: no active recording")

        # Update status to Stopped and mark as intentionally stopped
        self.stopped_channels.add(channel_name)
        self.processes[channel_name] = []
        self.status_dict[channel_name] = {
            "status": "Stopped", "detail": "by user", "size": "", "time": "", "progress": 0
        }

        # Run cleanup for this channel's files in background.
        # We wait long enough for any in-flight remux in the killed worker process
        # to either finish or be abandoned before scanning for leftover .ts files
        # (Bug #3 fix — the old 5 s wait was insufficient for multi-GB remuxes).
        def _channel_cleanup():
            time.sleep(30)  # generous wait for large-file remux / file handle release
            self.cleaner._process_leftover_files()
            logging.info(f"Cleanup finished for {channel_name}")

        threading.Thread(target=_channel_cleanup, daemon=True,
                         name=f"cleanup-{channel_name}").start()

    def start_channel(self, channel_name):
        """Start (or restart) a single channel's worker while other channels continue.

        Can be used to restart a channel that was individually stopped, or to add
        a new channel mid-session.

        Before spawning a new worker this method kills any existing processes for
        the channel so we never end up with two workers racing on the same files.
        A per-channel spawn guard prevents a second call from racing through before
        the first one has registered its new process.
        """
        if not self.is_running:
            logging.warning("Cannot start channel — no active recording session")
            return

        # Prevent concurrent start_channel calls for the same channel
        with self._spawn_lock:
            if channel_name in self._spawning:
                logging.info(f"start_channel({channel_name}): already in progress, ignoring duplicate call")
                return
            self._spawning.add(channel_name)

        try:
            # Kill every currently-registered process for this channel before spawning
            # a new one.  This prevents duplicate workers and avoids the situation
            # where the monitor loop kills our old proc, sees exit-code 15 (SIGTERM),
            # and also spawns a replacement — resulting in two workers at once.
            existing = self.processes.get(channel_name, [])
            alive_existing = [p for p in existing if p.is_alive()]
            if alive_existing:
                logging.info(
                    f"start_channel({channel_name}): killing {len(alive_existing)} "
                    f"existing worker(s) before spawning replacement"
                )
                for proc in alive_existing:
                    kill_process_tree(proc.pid)
                for proc in alive_existing:
                    proc.join(timeout=8)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(timeout=3)

            # Clear the process list for this channel — monitor loop won't restart
            # dead procs while the channel is in self._spawning.
            self.processes[channel_name] = []

            logging.info(f"Starting channel {channel_name} mid-session")

            # Clear the individually-stopped flag
            self.stopped_channels.discard(channel_name)

            # Build config dict the same way run() does
            config_dict = {section: dict(self.config.config.items(section))
                           for section in self.config.config.sections()}
            cookies_file = find_cookies_file(self.config)
            if cookies_file:
                config_dict.setdefault('Paths', {})['cookies_file'] = cookies_file

            # Initialize status
            self.status_dict[channel_name] = {
                "status": "Initializing", "detail": "", "size": "", "time": "", "progress": 0
            }

            # Spawn exactly one worker
            worker_args = (channel_name, config_dict, self.should_stop, self.status_queue)
            proc = mp.Process(target=record_worker, args=(worker_args,))
            proc.daemon = True
            proc.start()
            self.processes[channel_name] = [proc]
            logging.info(f"Started process for {channel_name} (PID {proc.pid})")

        finally:
            with self._spawn_lock:
                self._spawning.discard(channel_name)

    def stop(self):
        if not self.is_running:
            return
        self.is_running = False
        logging.info("Stop requested — shutting down processes...")
        self.should_stop.set()

        # Collect all alive processes across every channel
        pids_to_kill = []
        for ch, procs in self.processes.items():
            for proc in procs:
                if proc.is_alive():
                    pids_to_kill.append((ch, proc.pid))

        for ch, pid in pids_to_kill:
            logging.info(f"Killing process tree for {ch} (PID {pid})")
            kill_process_tree(pid)

        for ch, procs in self.processes.items():
            for proc in procs:
                proc.join(timeout=10)
                if proc.is_alive():
                    logging.warning(f"Process {ch} (PID {proc.pid}) did not terminate, force killing...")
                    proc.kill()
                    proc.join(timeout=5)

        self.processes = {}

        logging.info("Checking for orphaned ffmpeg processes...")
        kill_orphan_ffmpeg_processes(logging.getLogger())

        # Start background cleanup (safe now — no recording processes running)
        self.cleaner.start()
        logging.info("All processes stopped (cleanup running in background)")

    def shutdown(self):
        """Full shutdown: stop recording, wait for cleanup, shut down Manager."""
        self.stop()
        # Wait for background cleanup to finish
        if self.cleaner._thread and self.cleaner._thread.is_alive():
            self.cleaner.stop()
        # Shut down the multiprocessing Manager server process
        try:
            self.manager.shutdown()
        except Exception:
            pass

    def run(self):
        if self.is_running:
            return
        self.is_running = True
        self.should_stop.clear()

        # Quick synchronous cleanup of leftover files from previous sessions
        self._quick_startup_cleanup()

        logging.info(f"Launching {len(self.channels)} recording processes")

        config_dict = {section: dict(self.config.config.items(section))
                       for section in self.config.config.sections()}

        # Resolve cookies file path and pass it through config
        cookies_file = find_cookies_file(self.config)
        if cookies_file:
            config_dict.setdefault('Paths', {})['cookies_file'] = cookies_file
            logging.info(f"Using cookies file: {cookies_file}")

        for ch in self.channels:
            worker_args = (ch, config_dict, self.should_stop, self.status_queue)
            proc = mp.Process(target=record_worker, args=(worker_args,))
            proc.daemon = True
            proc.start()
            self.processes[ch] = [proc]
            logging.info(f"Started process for {ch} (PID {proc.pid})")

        # Monitor processes — restart any that exit unexpectedly
        while self.is_running and not self.should_stop.is_set():
            self.update_status_from_queue()

            for ch in list(self.processes.keys()):
                if self.should_stop.is_set():
                    break

                # Skip channels individually stopped by the user
                if ch in self.stopped_channels:
                    continue

                # Skip channels that have an active start_channel() in progress
                with self._spawn_lock:
                    if ch in self._spawning:
                        continue

                procs = self.processes.get(ch, [])

                # Prune dead processes from the list
                alive = [p for p in procs if p.is_alive()]
                self.processes[ch] = alive

                if not alive:
                    # All processes for this channel have exited — examine the last
                    # one to decide whether to restart.
                    dead = [p for p in procs if not p.is_alive()]
                    if not dead:
                        # Channel was never started (shouldn't happen in run(), but
                        # handle gracefully)
                        continue

                    last_proc = dead[-1]
                    exit_code = last_proc.exitcode

                    # Exit code -15 (SIGTERM on Unix) or 15 (Windows-mapped) means
                    # we killed the process ourselves (stop_channel / stop).
                    # Do NOT restart in that case — it was intentional.
                    if exit_code in (-15, 15):
                        logging.info(
                            f"Process for {ch} exited with code {exit_code} (SIGTERM) "
                            f"— not restarting (intentional kill)"
                        )
                        self.stopped_channels.add(ch)
                        continue

                    if exit_code != 0:
                        logging.warning(f"Process for {ch} crashed (exit code {exit_code}) — restarting...")
                    else:
                        logging.info(f"Process for {ch} exited normally — restarting...")

                    worker_args = (ch, config_dict, self.should_stop, self.status_queue)
                    new_proc = mp.Process(target=record_worker, args=(worker_args,))
                    new_proc.daemon = True
                    new_proc.start()
                    self.processes[ch] = [new_proc]
                    logging.info(f"Restarted process for {ch} (PID {new_proc.pid})")

            time.sleep(2)

        # Loop exited — stop() was already called or should_stop was set
        # Don't call stop() here to avoid double-stop race condition

    def _quick_startup_cleanup(self):
        logging.info("Startup cleanup: checking for leftover .ts files from previous sessions...")

        ffmpeg_path = self.config.get('Advanced', 'ffmpeg_path')
        ffmpeg_timeout = self.config.getint('Timeouts', 'ffmpeg_timeout')
        min_file_size_mb = self.config.getfloat('Recording', 'min_file_size_mb')
        processed_count = 0

        for platform in ["twitch", "youtube", "kick", "rumble", "fishtank", "custom"]:
            platform_dir = os.path.join(self.recorded_base, platform)
            if not os.path.exists(platform_dir):
                continue

            for username_dir in os.listdir(platform_dir):
                username_path = os.path.join(platform_dir, username_dir)
                if not os.path.isdir(username_path):
                    continue

                processed_path = os.path.join(self.processed_base, platform, username_dir)
                pending_dir = os.path.join(self.pending_base, platform, username_dir)
                os.makedirs(processed_path, exist_ok=True)
                os.makedirs(pending_dir, exist_ok=True)

                for filename in os.listdir(username_path):
                    if not filename.endswith('.ts'):
                        continue

                    raw_file = os.path.join(username_path, filename)

                    try:
                        size1 = os.path.getsize(raw_file)
                        time.sleep(1)
                        size2 = os.path.getsize(raw_file)
                        if size2 != size1:
                            logging.warning(f"Startup cleanup: {filename} still growing, skipping")
                            continue
                        file_size = size2
                    except Exception:
                        continue

                    if file_size < min_file_size_mb * 1024 * 1024:
                        logging.info(f"Startup cleanup: Removing tiny file {filename} ({human_size(file_size)})")
                        try:
                            os.remove(raw_file)
                        except Exception:
                            pass
                        continue

                    logging.info(f"Startup cleanup: Processing {filename} ({human_size(file_size)})")

                    mp4_filename = filename.replace('.ts', '.mp4')
                    mp4_file = os.path.join(processed_path, mp4_filename)

                    if os.path.exists(mp4_file) and os.path.getsize(mp4_file) > 5 * 1024**2:
                        logging.info("Startup cleanup: MP4 already exists, moving raw file")
                    else:
                        file_size_gb = file_size / (1024**3)
                        scaled_timeout = max(ffmpeg_timeout, int(file_size_gb * 60) + 120)
                        success, mp4_size, error = remux_to_mp4(
                            raw_file, mp4_file, ffmpeg_path,
                            logging.getLogger(), scaled_timeout
                        )
                        if not success:
                            logging.error(f"Startup cleanup: Failed to process {filename}: {error}")
                            try:
                                pending_path = os.path.join(pending_dir, filename)
                                shutil.move(raw_file, pending_path)
                                logging.info(f"Startup cleanup: Moved unprocessable {filename} to PendingDeletion")
                            except Exception as move_err:
                                logging.warning(f"Startup cleanup: Could not move {filename} to PendingDeletion: {move_err}")
                            continue

                    try:
                        pending_path = os.path.join(pending_dir, filename)
                        shutil.move(raw_file, pending_path)
                        processed_count += 1
                        logging.info(f"Startup cleanup: Done with {mp4_filename}")
                    except Exception as e:
                        logging.warning(f"Startup cleanup: Could not move {filename}: {e}")

        if processed_count > 0:
            logging.info(f"Startup cleanup: Processed {processed_count} leftover file(s)")
        else:
            logging.info("Startup cleanup: No leftover files found")


# ────────────────────────────────────────────────
#          Notifications
# ────────────────────────────────────────────────

class NotificationThrottle:
    """Rate-limits desktop notifications to prevent spam.

    Rules:
        - Global cooldown: minimum 30 seconds between any two notifications
        - Per-channel error dedup: same channel+error only notifies once
        - Burst limit: after 5 notifications in 2 minutes, suppresses until quiet
    """

    def __init__(self, cooldown=30, burst_limit=5, burst_window=120):
        self._cooldown = cooldown
        self._burst_limit = burst_limit
        self._burst_window = burst_window
        self._last_sent = 0
        self._recent_times = []        # timestamps of recent notifications
        self._sent_errors = set()       # (channel, error_snippet) dedup keys
        self._suppressed_count = 0

    def should_send(self, category="info", channel="", detail=""):
        """Check if a notification should be sent.  Returns True if allowed."""
        now = time.time()

        # Global cooldown
        if now - self._last_sent < self._cooldown:
            self._suppressed_count += 1
            return False

        # Burst detection: too many recent notifications
        self._recent_times = [t for t in self._recent_times if now - t < self._burst_window]
        if len(self._recent_times) >= self._burst_limit:
            self._suppressed_count += 1
            return False

        # Error dedup: don't re-notify same channel+error
        if category == "error" and channel:
            key = (channel, detail[:50] if detail else "")
            if key in self._sent_errors:
                return False
            self._sent_errors.add(key)

        self._last_sent = now
        self._recent_times.append(now)
        return True

    def reset(self):
        """Reset all state (call when starting/stopping recording)."""
        self._last_sent = 0
        self._recent_times.clear()
        self._sent_errors.clear()
        self._suppressed_count = 0

    @property
    def suppressed_count(self):
        return self._suppressed_count


# Global throttle instance
_notif_throttle = NotificationThrottle()


def send_notification(title, message, timeout=5, category="info", channel="", detail=""):
    """Send a desktop toast notification with rate limiting.

    Args:
        title: Notification title
        message: Notification body
        timeout: Display duration in seconds
        category: One of 'info', 'error', 'recording', 'complete' — used for throttling
        channel: Channel name for per-channel dedup
        detail: Error detail string for dedup
    """
    if not HAS_NOTIFICATIONS:
        return
    if not _notif_throttle.should_send(category, channel, detail):
        return
    try:
        plyer_notification.notify(
            title=title,
            message=message,
            app_name=f"Multi-Stream Recorder v{__version__}",
            timeout=timeout,
        )
    except Exception:
        pass  # notifications are best-effort


# ────────────────────────────────────────────────
#          Window State Persistence
# ────────────────────────────────────────────────

def load_window_state(state_file):
    """Load saved window geometry and preferences."""
    try:
        if os.path.isfile(state_file):
            with open(state_file, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_window_state(state_file, state):
    """Save window geometry and preferences."""
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


# ────────────────────────────────────────────────
#          System Tray Icon
# ────────────────────────────────────────────────

def create_tray_icon_image(recording=False):
    """Create a simple tray icon image using PIL.

    Green circle when recording, grey when idle.
    """
    size = 64
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    # Outer circle
    color = (76, 175, 80, 255) if recording else (128, 128, 128, 255)
    draw.ellipse([4, 4, size - 4, size - 4], fill=color)
    # Inner dot
    inner_color = (255, 255, 255, 255)
    draw.ellipse([20, 20, size - 20, size - 20], fill=inner_color)
    return img


# ────────────────────────────────────────────────
#          GUI Log Handler
# ────────────────────────────────────────────────

class QueueLogHandler(logging.Handler):
    """A logging handler that puts log records into a queue for the GUI to read."""

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_queue.put(msg)
        except Exception:
            pass


# ────────────────────────────────────────────────
#          GUI
# ────────────────────────────────────────────────

def main_gui(config):
    """Main GUI window with dark mode, system tray, log viewer, and notifications."""
    import tkinter as tk
    from tkinter import ttk, messagebox
    import queue as stdlib_queue

    # ── Theme colors ──
    DARK = {
        'bg': '#0a1628', 'fg': '#d4d4d4', 'accent': '#1a5fb4',
        'border': '#1a3a5c',
        'entry_bg': '#112240', 'entry_fg': '#d4d4d4',
        'listbox_bg': '#0d1f3c', 'listbox_fg': '#cccccc',
        'select_bg': '#1a5fb4', 'select_fg': '#ffffff',
        'tree_bg': '#0a1628', 'tree_fg': '#d4d4d4', 'tree_field': '#0d1f3c',
        'tree_heading_bg': '#132d5e', 'tree_heading_fg': '#d4d4d4',
        'btn_bg': '#132d5e', 'btn_fg': '#d4d4d4',
        'log_bg': '#081422', 'log_fg': '#b5cea8',
        'tab_bg': '#112240', 'tab_fg': '#d4d4d4',
        'start_bg': '#1b7a2b', 'stop_bg': '#b71c1c',
        'rec_fg': '#4ec94e', 'offline_fg': '#5a6a8a', 'error_fg': '#f44747',
        'completed_fg': '#4ec9b0', 'remux_fg': '#dcdcaa', 'check_fg': '#6a7a9a',
    }
    LIGHT = {
        'bg': '#f0f0f0', 'fg': '#1e1e1e', 'accent': '#0078d4',
        'border': '#c0c0c0',
        'entry_bg': '#ffffff', 'entry_fg': '#1e1e1e',
        'listbox_bg': '#ffffff', 'listbox_fg': '#1e1e1e',
        'select_bg': '#0078d4', 'select_fg': '#ffffff',
        'tree_bg': '#ffffff', 'tree_fg': '#1e1e1e', 'tree_field': '#ffffff',
        'tree_heading_bg': '#e0e0e0', 'tree_heading_fg': '#1e1e1e',
        'btn_bg': '#e0e0e0', 'btn_fg': '#1e1e1e',
        'log_bg': '#ffffff', 'log_fg': '#1e1e1e',
        'tab_bg': '#f0f0f0', 'tab_fg': '#1e1e1e',
        'start_bg': '#4CAF50', 'stop_bg': '#F44336',
        'rec_fg': '#006400', 'offline_fg': '#696969', 'error_fg': '#B22222',
        'completed_fg': '#2E8B57', 'remux_fg': '#DAA520', 'check_fg': '#808080',
    }

    # ── Load window state ──
    state_file = config.get('GUI', 'window_state_file', fallback='window_state.json')
    win_state = load_window_state(state_file)
    notifications_enabled = config.getboolean('GUI', 'notifications', fallback=True)
    minimize_to_tray = config.getboolean('GUI', 'minimize_to_tray', fallback=True) and HAS_TRAY

    # ── Create root window ──
    root = tk.Tk()
    root.title(f"Multi-Stream Recorder v{__version__}")

    # tk variables must be created AFTER tk.Tk()
    dark_mode = tk.BooleanVar(value=win_state.get('dark_mode',
                              config.getboolean('GUI', 'dark_mode', fallback=True)))

    # ── Windows: dark title bar via DWM API ──
    def set_title_bar_dark(dark=True):
        """Use Windows DWM API to set title bar color.

        Windows 11 and Windows 10 20H1+ support DWMWA_USE_IMMERSIVE_DARK_MODE.
        Attribute 20 works on Windows 11 build 22000+ and Win10 20H1+.
        Attribute 19 works on earlier Windows 11 insider/pre-release builds.
        We try both for maximum compatibility.
        """
        if os.name != 'nt':
            return
        try:
            import ctypes
            hwnd = ctypes.windll.user32.GetParent(root.winfo_id())
            value = ctypes.c_int(1 if dark else 0)
            # Try attribute 20 first (standard), then 19 (pre-release Win11)
            hr = ctypes.windll.dwmapi.DwmSetWindowAttribute(
                hwnd, 20, ctypes.byref(value), ctypes.sizeof(value)
            )
            if hr != 0:  # S_OK = 0
                ctypes.windll.dwmapi.DwmSetWindowAttribute(
                    hwnd, 19, ctypes.byref(value), ctypes.sizeof(value)
                )
            # Force title bar repaint by toggling size slightly
            root.update_idletasks()
        except Exception:
            pass  # older Windows or non-Windows — silently skip

    # Restore geometry
    geom = win_state.get('geometry', '1100x760')
    root.geometry(geom)
    root.minsize(960, 720)

    root.grid_rowconfigure(0, weight=1)
    root.grid_columnconfigure(0, weight=0)
    root.grid_columnconfigure(1, weight=1)

    # ── Log queue for GUI log viewer ──
    log_queue = stdlib_queue.Queue()
    gui_log_handler = QueueLogHandler(log_queue)
    gui_log_handler.setLevel(logging.INFO)
    gui_log_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(message)s", datefmt="%H:%M:%S"
    ))
    logging.root.addHandler(gui_log_handler)

    # ── Notification tracking (avoid spamming) ──
    _notified_live = set()  # channels we already sent a "live" notification for

    # ── Apply theme ──
    style = ttk.Style()

    def apply_theme(*_args):
        t = DARK if dark_mode.get() else LIGHT
        style.theme_use('clam')

        root.configure(bg=t['bg'])

        # Windows: set dark/light title bar
        set_title_bar_dark(dark_mode.get())

        # ttk styles — set bordercolor to match theme (clam theme uses these)
        border_color = t['border']
        style.configure('.', background=t['bg'], foreground=t['fg'],
                        fieldbackground=t['tree_field'],
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color)
        style.configure('TFrame', background=t['bg'])
        style.configure('TLabel', background=t['bg'], foreground=t['fg'])
        style.configure('TNotebook', background=t['bg'], borderwidth=0,
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color, tabmargins=[0, 0, 0, 0])
        style.configure('TNotebook.Tab', background=t['tab_bg'], foreground=t['tab_fg'],
                        padding=[12, 4],
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color)
        style.map('TNotebook.Tab',
                  background=[('selected', t['accent']), ('!selected', t['tab_bg'])],
                  foreground=[('selected', '#ffffff'), ('!selected', t['tab_fg'])],
                  lightcolor=[('selected', border_color), ('!selected', border_color)],
                  darkcolor=[('selected', border_color), ('!selected', border_color)],
                  bordercolor=[('selected', border_color), ('!selected', border_color)])
        style.configure('Treeview', background=t['tree_field'], foreground=t['tree_fg'],
                        fieldbackground=t['tree_field'], rowheight=22,
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color)
        style.configure('Treeview.Heading', background=t['tree_heading_bg'],
                        foreground=t['tree_heading_fg'], font=("Segoe UI", 10, "bold"),
                        bordercolor=border_color, lightcolor=t['tree_heading_bg'],
                        darkcolor=border_color)
        style.map('Treeview', background=[('selected', t['select_bg'])],
                  foreground=[('selected', t['select_fg'])])
        style.configure('TCombobox', fieldbackground=t['entry_bg'], foreground=t['entry_fg'],
                        background=t['btn_bg'], arrowcolor=t['fg'],
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color)
        style.map('TCombobox', fieldbackground=[('readonly', t['entry_bg'])],
                  foreground=[('readonly', t['entry_fg'])],
                  bordercolor=[('focus', t['accent'])])
        # Style the dropdown list (Tk popdown)
        root.option_add('*TCombobox*Listbox.background', t['entry_bg'])
        root.option_add('*TCombobox*Listbox.foreground', t['entry_fg'])
        root.option_add('*TCombobox*Listbox.selectBackground', t['select_bg'])
        root.option_add('*TCombobox*Listbox.selectForeground', t['select_fg'])
        style.configure('TCheckbutton', background=t['bg'], foreground=t['fg'],
                        indicatorcolor=t['entry_bg'], indicatorrelief='flat')
        style.map('TCheckbutton',
                  background=[('active', t['bg']), ('pressed', t['bg'])],
                  indicatorcolor=[('selected', t['accent']), ('pressed', t['accent'])])
        style.configure('TButton', background=t['btn_bg'], foreground=t['btn_fg'],
                        bordercolor=border_color, lightcolor=border_color,
                        darkcolor=border_color, padding=[8, 4])
        style.map('TButton',
                  background=[('active', t['accent']), ('pressed', t['accent'])],
                  foreground=[('active', '#ffffff'), ('pressed', '#ffffff')])
        style.configure('Vertical.TScrollbar', background=t['btn_bg'],
                        bordercolor=border_color, arrowcolor=t['fg'],
                        troughcolor=t['tree_field'],
                        lightcolor=border_color, darkcolor=border_color)
        style.map('Vertical.TScrollbar',
                  background=[('active', t['accent']), ('pressed', t['accent'])])

        # Tag colors for treeview
        tree.tag_configure("recording", foreground=t['rec_fg'], font=("Segoe UI", 10, "bold"))
        tree.tag_configure("completed", foreground=t['completed_fg'])
        tree.tag_configure("offline", foreground=t['offline_fg'])
        tree.tag_configure("checking", foreground=t['check_fg'])
        tree.tag_configure("remuxing", foreground=t['remux_fg'])
        tree.tag_configure("error", foreground=t['error_fg'])
        tree.tag_configure("stopped", foreground=t['offline_fg'])
        tree.tag_configure("unknown", foreground=t['offline_fg'])

        # Tk widgets (non-ttk) — set all frame backgrounds
        for w in [frame_left, ch_tree_frame, platform_frame, btn_small_frame, move_btn_frame,
                  frame_right, bottom_bar, btn_frame, toggle_frame]:
            w.configure(bg=t['bg'])
        ch_tree.tag_configure("enabled", foreground=t['fg'])
        ch_tree.tag_configure("disabled", foreground="#777777")
        try:
            dot_canvas.configure(bg=t['bg'])
            _redraw_dot_canvas()
        except NameError:
            pass  # not yet defined on first apply_theme call during startup
        entry.configure(bg=t['entry_bg'], fg=t['entry_fg'], insertbackground=t['fg'],
                        highlightbackground=border_color, highlightcolor=t['accent'],
                        highlightthickness=1, relief='flat')
        platform_label.configure(bg=t['bg'], fg=t['fg'])
        status_label.configure(bg=t['bg'], fg=t['fg'])
        poll_label.configure(bg=t['bg'], fg=t['fg'])
        cookie_frame.configure(bg=t['bg'])
        cookie_label.configure(bg=t['bg'], fg=t['fg'])
        cookie_indicator.configure(bg=t['bg'])

        for btn, bg_key in [(add_btn, 'btn_bg'), (remove_btn, 'btn_bg'),
                            (up_btn, 'btn_bg'), (down_btn, 'btn_bg')]:
            btn.configure(bg=t[bg_key], fg=t['btn_fg'], activebackground=t['accent'],
                          activeforeground='#ffffff',
                          highlightbackground=border_color, relief='flat', bd=1)
        start_button.configure(bg=t['start_bg'], activebackground=t['start_bg'],
                               fg='#ffffff', activeforeground='#ffffff',
                               relief='flat', bd=0)
        stop_button.configure(bg=t['stop_bg'], activebackground=t['stop_bg'],
                              fg='#ffffff', activeforeground='#ffffff',
                              relief='flat', bd=0)

        log_text.configure(bg=t['log_bg'], fg=t['log_fg'], insertbackground=t['log_fg'],
                           highlightbackground=border_color, highlightcolor=border_color,
                           highlightthickness=1, relief='flat')

        # Status bar at bottom
        status_bar.configure(bg=t['bg'], fg=t['offline_fg'])
        update_label.configure(bg=t['bg'])

        # Context menu theming — borderwidth=0 removes the bright system border
        ctx_menu.configure(bg=t['entry_bg'], fg=t['fg'],
                          activebackground=t['accent'], activeforeground='#ffffff',
                          borderwidth=0, activeborderwidth=0, relief='flat')
        status_ctx_menu.configure(bg=t['entry_bg'], fg=t['fg'],
                                  activebackground=t['accent'], activeforeground='#ffffff',
                                  borderwidth=0, activeborderwidth=0, relief='flat')

        # Save dark_mode preference
        try:
            state = load_window_state(state_file)
            state['dark_mode'] = dark_mode.get()
            save_window_state(state_file, state)
        except Exception:
            pass

    # ── Load channels ──
    # channels is a list of dicts: [{"name": "twitch:saruei", "enabled": True}, ...]
    # Backward-compatible with old format: ["twitch:saruei", "betty-fae"]
    channels_file = config.get('Paths', 'channels_file')
    channels = []
    try:
        with open(channels_file, "r") as f:
            loaded = json.load(f)
            if isinstance(loaded, list):
                for item in loaded:
                    if isinstance(item, str):
                        # Old format — migrate: string → enabled dict
                        channels.append({"name": item, "enabled": True})
                    elif isinstance(item, dict) and "name" in item:
                        # New format
                        channels.append({"name": item["name"], "enabled": item.get("enabled", True)})
                    elif isinstance(item, list) and len(item) >= 1 and isinstance(item[0], str):
                        # Very old format — list of single-element lists
                        channels.append({"name": item[0], "enabled": True})
    except FileNotFoundError:
        logging.info("No existing channels file found, creating new one")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse channels file: {e}")
    except Exception as e:
        logging.error(f"Error loading channels: {e}")

    def save_channels():
        try:
            with open(channels_file, "w") as f:
                json.dump(channels, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save channels: {e}")

    def get_enabled_channels():
        """Return list of channel name strings that are checked/enabled."""
        return [ch["name"] for ch in channels if ch.get("enabled", True)]

    def get_all_channel_names():
        """Return list of all channel name strings."""
        return [ch["name"] for ch in channels]

    # ── Left panel — Channel list with checkboxes ──
    frame_left = tk.Frame(root, padx=12, pady=12)
    frame_left.grid(row=0, column=0, sticky="ns", padx=(10, 5), pady=10)

    # Container so ch_tree and the dot canvas sit side by side
    ch_tree_frame = tk.Frame(frame_left)
    ch_tree_frame.pack(fill=tk.BOTH, expand=True)

    # Treeview — no cookie column; dots are drawn on a Canvas overlay instead
    ch_tree = ttk.Treeview(ch_tree_frame, columns=("check", "name"), show="tree",
                           height=22, selectmode="extended")
    ch_tree.column("#0", width=0, stretch=False)  # hidden tree column
    ch_tree.column("check", width=28, anchor="center", stretch=False)
    ch_tree.column("name", width=270, anchor="w")
    ch_tree.heading("check", text="")
    ch_tree.heading("name", text="")
    ch_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    # Narrow Canvas strip for cookie dots — drawn independently of row tags
    DOT_CANVAS_W = 18
    dot_canvas = tk.Canvas(ch_tree_frame, width=DOT_CANVAS_W, highlightthickness=0,
                           borderwidth=0)
    dot_canvas.pack(side=tk.LEFT, fill=tk.Y)

    def _redraw_dot_canvas():
        """Repaint the dot canvas to match current treeview row positions."""
        t = DARK if dark_mode.get() else LIGHT
        dot_canvas.configure(bg=t['bg'])
        dot_canvas.delete("all")
        for item in ch_tree.get_children():
            bbox = ch_tree.bbox(item)
            if not bbox:
                continue  # row is scrolled out of view
            x, y, w, h = bbox
            ch_name = ch_tree.item(item, "values")[1]  # col 0=check, 1=name
            _, color = _cookie_dot_for_channel(ch_name)
            cx = DOT_CANVAS_W // 2
            cy = y + h // 2
            r = 4
            dot_canvas.create_oval(cx - r, cy - r, cx + r, cy + r,
                                   fill=color, outline="")

    # Redraw dots whenever the treeview content or scroll position changes
    ch_tree.bind("<<TreeviewSelect>>", lambda e: dot_canvas.after(10, _redraw_dot_canvas))
    ch_tree.bind("<Configure>", lambda e: dot_canvas.after(10, _redraw_dot_canvas))
    ch_tree.bind("<MouseWheel>", lambda e: dot_canvas.after(10, _redraw_dot_canvas))
    ch_tree.bind("<Button-4>", lambda e: dot_canvas.after(10, _redraw_dot_canvas))  # Linux scroll up
    ch_tree.bind("<Button-5>", lambda e: dot_canvas.after(10, _redraw_dot_canvas))  # Linux scroll down
    # Also schedule a periodic redraw to catch scroll and theme changes
    def _dot_refresh_loop():
        _redraw_dot_canvas()
        dot_canvas.after(500, _dot_refresh_loop)
    dot_canvas.after(100, _dot_refresh_loop)

    # ── Tooltip explaining the cookie dot column ──
    class _Tooltip:
        def __init__(self, widget, text):
            self._widget = widget
            self._text = text
            self._tip = None
            widget.bind("<Enter>", self._show, add="+")
            widget.bind("<Leave>", self._hide, add="+")
        def _show(self, event=None):
            if self._tip:
                return
            x = self._widget.winfo_rootx() + 20
            y = self._widget.winfo_rooty() + self._widget.winfo_height() + 4
            self._tip = tw = tk.Toplevel(self._widget)
            tw.wm_overrideredirect(True)
            tw.wm_geometry(f"+{x}+{y}")
            t = DARK if dark_mode.get() else LIGHT
            tk.Label(tw, text=self._text, justify=tk.LEFT,
                     background=t['entry_bg'], foreground=t['fg'],
                     relief='solid', borderwidth=1,
                     font=("Segoe UI", 8), padx=6, pady=4).pack()
        def _hide(self, event=None):
            if self._tip:
                self._tip.destroy()
                self._tip = None

    _TOOLTIP_TEXT = "● Green  = cookie present\n● Orange = auth cookie expired\n● Red    = no cookie for this site\n○ Grey   = unknown / no cookies.txt"
    _Tooltip(ch_tree, _TOOLTIP_TEXT)
    _Tooltip(dot_canvas, _TOOLTIP_TEXT)

    CHECK_ON = "☑"
    CHECK_OFF = "☐"

    # Pre-fill the cookie cache immediately — before the first tree draw —
    # so dots have real colours from the start rather than going grey first.
    # This is just the data parse; widget updates happen later in update_cookie_status().
    _cookie_info_cache = [validate_cookies(find_cookies_file(config))]

    def _cookie_dot_for_channel(ch_name):
        """Return (dot_char, fg_color) for the cookie indicator column."""
        info = _cookie_info_cache[0]
        if info is None:
            return ("○", COOKIE_DOT_COLORS[COOKIE_STATUS_UNKNOWN])
        status = get_cookie_status_for_channel(ch_name, info)
        dot = "●" if status != COOKIE_STATUS_UNKNOWN else "○"
        return (dot, COOKIE_DOT_COLORS[status])

    # Simple row tags — just enabled/disabled for dimming.
    # Cookie dot colours are drawn by the Canvas, not by row tags.
    ch_tree.tag_configure("enabled", foreground="")   # themed by apply_theme
    ch_tree.tag_configure("disabled", foreground="#777777")

    def _populate_channel_tree():
        """Rebuild the channel treeview from the channels list."""
        ch_tree.delete(*ch_tree.get_children())
        for ch in channels:
            check = CHECK_ON if ch.get("enabled", True) else CHECK_OFF
            tag = "enabled" if ch.get("enabled", True) else "disabled"
            ch_tree.insert("", tk.END, values=(check, ch["name"]), tags=(tag,))
        dot_canvas.after(20, _redraw_dot_canvas)

    _populate_channel_tree()

    def _toggle_channel_check(event):
        """Toggle checkbox when user clicks in the check column."""
        region = ch_tree.identify_region(event.x, event.y)
        col = ch_tree.identify_column(event.x)
        item = ch_tree.identify_row(event.y)
        if not item:
            return
        # Only toggle if clicking the check column (#1)
        if col == "#1":
            idx = ch_tree.index(item)
            if 0 <= idx < len(channels):
                channels[idx]["enabled"] = not channels[idx].get("enabled", True)
                enabled = channels[idx]["enabled"]
                check = CHECK_ON if enabled else CHECK_OFF
                tag = "enabled" if enabled else "disabled"
                ch_tree.item(item, values=(check, channels[idx]["name"]), tags=(tag,))
                dot_canvas.after(20, _redraw_dot_canvas)
                save_channels()

    ch_tree.bind("<ButtonRelease-1>", _toggle_channel_check)

    def _double_click_toggle(event):
        """Double-clicking anywhere on a row toggles its enabled state."""
        item = ch_tree.identify_row(event.y)
        if not item:
            return
        idx = ch_tree.index(item)
        if 0 <= idx < len(channels):
            channels[idx]["enabled"] = not channels[idx].get("enabled", True)
            enabled = channels[idx]["enabled"]
            check = CHECK_ON if enabled else CHECK_OFF
            tag = "enabled" if enabled else "disabled"
            ch_tree.item(item, values=(check, channels[idx]["name"]), tags=(tag,))
            dot_canvas.after(20, _redraw_dot_canvas)
            save_channels()

    ch_tree.bind("<Double-ButtonRelease-1>", _double_click_toggle)

    platform_frame = tk.Frame(frame_left)
    platform_frame.pack(fill=tk.X, pady=(10, 5))
    platform_label = tk.Label(platform_frame, text="Platform:", font=("Segoe UI", 9))
    platform_label.pack(side=tk.LEFT)
    platform_var = tk.StringVar(value="kick")
    ttk.Combobox(platform_frame, textvariable=platform_var,
                 values=["kick", "twitch", "youtube", "rumble", "fishtank", "custom"], state="readonly", width=10).pack(side=tk.LEFT, padx=6)

    entry = tk.Entry(frame_left, width=34, font=("Segoe UI", 10), borderwidth=0, relief="flat")
    entry.pack(pady=6)

    btn_small_frame = tk.Frame(frame_left)
    btn_small_frame.pack(pady=4)

    recorder = None

    def add_channel():
        platform = platform_var.get()
        name = entry.get().strip()
        if not name:
            return

        existing_names = get_all_channel_names()
        is_valid, error_msg = validate_channel_name(name, platform, existing_names)
        if not is_valid:
            messagebox.showwarning("Invalid Channel", error_msg)
            return

        if platform == "custom":
            ch_name = f"custom:{name}"  # name is the full URL
        elif platform != "kick":
            ch_name = f"{platform}:{name}"
        else:
            ch_name = name

        ch = {"name": ch_name, "enabled": True}
        channels.append(ch)
        ch_tree.insert("", tk.END, values=(CHECK_ON, ch_name), tags=("enabled",))
        dot_canvas.after(20, _redraw_dot_canvas)
        save_channels()
        if recorder and recorder.is_running:
            recorder.start_channel(ch_name)
        elif recorder:
            recorder.status_dict[ch_name] = {"status": "Initializing", "detail": "", "size": "", "time": ""}
        entry.delete(0, tk.END)

    def remove_selected():
        selected_items = ch_tree.selection()
        # Get indices in reverse order to avoid shifting
        indices = sorted([ch_tree.index(item) for item in selected_items], reverse=True)
        for idx in indices:
            if 0 <= idx < len(channels):
                ch_name = channels[idx]["name"]
                del channels[idx]
                if recorder and ch_name in recorder.status_dict:
                    del recorder.status_dict[ch_name]
        _populate_channel_tree()
        save_channels()

    def _move_channel_up():
        """Move selected channel up in the list."""
        selected = ch_tree.selection()
        if not selected:
            return
        idx = ch_tree.index(selected[0])
        if idx <= 0 or idx >= len(channels):
            return
        channels[idx], channels[idx - 1] = channels[idx - 1], channels[idx]
        _populate_channel_tree()
        save_channels()
        # Re-select the moved item
        new_item = ch_tree.get_children()[idx - 1]
        ch_tree.selection_set(new_item)
        ch_tree.see(new_item)

    def _move_channel_down():
        """Move selected channel down in the list."""
        selected = ch_tree.selection()
        if not selected:
            return
        idx = ch_tree.index(selected[0])
        if idx < 0 or idx >= len(channels) - 1:
            return
        channels[idx], channels[idx + 1] = channels[idx + 1], channels[idx]
        _populate_channel_tree()
        save_channels()
        # Re-select the moved item
        new_item = ch_tree.get_children()[idx + 1]
        ch_tree.selection_set(new_item)
        ch_tree.see(new_item)

    add_btn = tk.Button(btn_small_frame, text="Add", command=add_channel, width=10,
                        font=("Segoe UI", 9))
    add_btn.pack(side=tk.LEFT, padx=4)
    remove_btn = tk.Button(btn_small_frame, text="Remove", command=remove_selected, width=10,
                           font=("Segoe UI", 9))
    remove_btn.pack(side=tk.LEFT, padx=4)

    # Move Up/Down buttons for channel reordering
    move_btn_frame = tk.Frame(frame_left)
    move_btn_frame.pack(pady=2)
    up_btn = tk.Button(move_btn_frame, text="▲", command=_move_channel_up, width=3,
                       font=("Segoe UI", 8))
    up_btn.pack(side=tk.LEFT, padx=2)
    down_btn = tk.Button(move_btn_frame, text="▼", command=_move_channel_down, width=3,
                         font=("Segoe UI", 8))
    down_btn.pack(side=tk.LEFT, padx=2)

    # ── Cookie status indicator ──
    cookie_frame = tk.Frame(frame_left)
    cookie_frame.pack(fill=tk.X, pady=(8, 0))

    cookie_indicator = tk.Label(cookie_frame, text="●", font=("Segoe UI", 10))
    cookie_indicator.pack(side=tk.LEFT, padx=(0, 4))
    cookie_label = tk.Label(cookie_frame, text="Cookies: checking...", font=("Segoe UI", 8))
    cookie_label.pack(side=tk.LEFT)

    def update_cookie_status():
        """Check cookies.txt, update the global indicator, and refresh per-channel dots."""
        cookies_path = find_cookies_file(config)
        info = validate_cookies(cookies_path)

        # Refresh the cache so per-channel dots update on the next tree redraw
        _cookie_info_cache[0] = info

        base_text = f"{info['total_cookies']} entries, {len(info['domains'])} domains"

        if not cookies_path:
            cookie_indicator.configure(text="○", fg="#888888")
            cookie_label.configure(text="No cookies.txt found")
        elif not info['valid']:
            cookie_indicator.configure(text="●", fg="#F44336")
            warning = info['warnings'][0] if info['warnings'] else "Invalid format"
            cookie_label.configure(text=f"Cookies: {warning}")
        elif info['has_expired_auth']:
            cookie_indicator.configure(text="●", fg="#FF9800")
            domains = ", ".join(info['expired_domains'][:2])
            cookie_label.configure(text=f"Cookies: auth expired ({domains})")
        else:
            expiry_note = ""
            if info['auth_expiry']:
                days_left = (info['auth_expiry'] - datetime.datetime.now()).days
                if days_left < 7:
                    expiry_note = f" (auth renew in {days_left}d)"
                    if days_left < 2:
                        cookie_indicator.configure(text="●", fg="#FF9800")
                    else:
                        cookie_indicator.configure(text="●", fg="#4CAF50")
                else:
                    cookie_indicator.configure(text="●", fg="#4CAF50")
            else:
                cookie_indicator.configure(text="●", fg="#4CAF50")
            cookie_label.configure(text=f"Cookies: {base_text}{expiry_note}")

        # Refresh dots to reflect the updated cache
        dot_canvas.after(20, _redraw_dot_canvas)

    # Run initial check, then re-check every 5 minutes
    update_cookie_status()

    def _periodic_cookie_check():
        update_cookie_status()
        root.after(300_000, _periodic_cookie_check)  # 5 minutes

    root.after(300_000, _periodic_cookie_check)

    # ── Right-click context menu on channel list ──
    ctx_menu = tk.Menu(root, tearoff=0)

    def _start_selected_channel_from_list():
        """Start recording a channel from the channel list (mid-session)."""
        selected_items = ch_tree.selection()
        if not selected_items or not recorder or not recorder.is_running:
            return
        for item in selected_items:
            idx = ch_tree.index(item)
            if 0 <= idx < len(channels):
                ch_name = channels[idx]["name"]
                # Enable the channel if it's not already
                if not channels[idx].get("enabled", True):
                    channels[idx]["enabled"] = True
                    _populate_channel_tree()
                    save_channels()
                # Start it in the running session
                recorder.start_channel(ch_name)

    def _stop_selected_channel_from_list():
        """Stop recording a channel from the channel list."""
        selected_items = ch_tree.selection()
        if not selected_items or not recorder or not recorder.is_running:
            return
        for item in selected_items:
            idx = ch_tree.index(item)
            if 0 <= idx < len(channels):
                ch_name = channels[idx]["name"]
                recorder.stop_channel(ch_name)

    def show_context_menu(event):
        # Select the item under cursor if not already selected
        item = ch_tree.identify_row(event.y)
        if item:
            if item not in ch_tree.selection():
                ch_tree.selection_set(item)

        # Rebuild menu dynamically
        ctx_menu.delete(0, tk.END)

        # If a recording session is active, show Start/Stop Recording option
        if recorder and recorder.is_running and ch_tree.selection():
            idx = ch_tree.index(ch_tree.selection()[0])
            if 0 <= idx < len(channels):
                ch_name = channels[idx]["name"]
                st = recorder.status_dict.get(ch_name, {})
                status_lower = st.get("status", "").lower()
                if status_lower in ("stopped",) or ch_name not in recorder.status_dict:
                    ctx_menu.add_command(label="Start Recording", command=_start_selected_channel_from_list)
                elif "recording" in status_lower or "checking" in status_lower or "offline" in status_lower or "initializing" in status_lower:
                    ctx_menu.add_command(label="Stop Recording", command=_stop_selected_channel_from_list)
                else:
                    ctx_menu.add_command(label="Start Recording", command=_start_selected_channel_from_list)
                ctx_menu.add_separator()

        ctx_menu.add_command(label="Open in Browser", command=open_channel_url)
        ctx_menu.add_command(label="Copy Name", command=copy_channel_name)
        ctx_menu.add_separator()
        ctx_menu.add_command(label="Toggle Selected", command=_toggle_selected_channels)
        ctx_menu.add_command(label="Enable All", command=_enable_all_channels)
        ctx_menu.add_command(label="Disable All", command=_disable_all_channels)
        ctx_menu.add_separator()
        ctx_menu.add_command(label="Remove", command=remove_selected)

        try:
            ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            ctx_menu.grab_release()

    def copy_channel_name():
        selected_items = ch_tree.selection()
        if selected_items:
            names = []
            for item in selected_items:
                idx = ch_tree.index(item)
                if 0 <= idx < len(channels):
                    names.append(channels[idx]["name"])
            root.clipboard_clear()
            root.clipboard_append("\n".join(names))

    def open_channel_url():
        selected_items = ch_tree.selection()
        if selected_items:
            idx = ch_tree.index(selected_items[0])
            if 0 <= idx < len(channels):
                ch = channels[idx]["name"]
                if ch.startswith("custom:"):
                    url = ch.split(":", 1)[1]
                elif ch.startswith("twitch:"):
                    url = f"https://twitch.tv/{ch.split(':', 1)[1]}"
                elif ch.startswith("youtube:"):
                    name = ch.split(':', 1)[1]
                    url = f"https://youtube.com/@{name}/live" if not name.startswith("UC") else f"https://youtube.com/channel/{name}/live"
                else:
                    url = f"https://kick.com/{ch}"
                import webbrowser
                webbrowser.open(url)

    def _toggle_selected_channels():
        """Toggle enabled state for all selected channels."""
        for item in ch_tree.selection():
            idx = ch_tree.index(item)
            if 0 <= idx < len(channels):
                channels[idx]["enabled"] = not channels[idx].get("enabled", True)
        _populate_channel_tree()
        save_channels()

    def _enable_all_channels():
        for ch in channels:
            ch["enabled"] = True
        _populate_channel_tree()
        save_channels()

    def _disable_all_channels():
        for ch in channels:
            ch["enabled"] = False
        _populate_channel_tree()
        save_channels()

    ch_tree.bind("<Button-3>", show_context_menu)

    # ── Keyboard shortcuts ──
    entry.bind("<Return>", lambda e: add_channel())
    ch_tree.bind("<Delete>", lambda e: remove_selected())

    def _about_dialog():
        deps = []
        deps.append(f"yt-dlp {YTDLP_VERSION}" if HAS_YTDLP else "yt-dlp: not found")
        deps.append(f"streamlink {STREAMLINK_VERSION}" if HAS_STREAMLINK else "streamlink: not found")
        deps.append(f"ffmpeg {FFMPEG_VERSION}" if HAS_FFMPEG else "ffmpeg: not found")
        deps.append(f"psutil: {'yes' if HAS_PSUTIL else 'no'}")
        deps.append(f"pystray: {'yes' if HAS_TRAY else 'no'}")
        deps.append(f"curl_cffi: {'yes' if HAS_CURL_CFFI else 'no'} (browser impersonation)")
        deps_str = "\n".join(deps)

        messagebox.showinfo(
            f"Multi-Stream Recorder v{__version__}",
            f"Multi-Stream Recorder v{__version__}\n\n"
            f"Supports: Kick, Twitch, YouTube Live\n\n"
            f"Dependencies:\n{deps_str}\n\n"
            f"Streams directory:\n{config.get('Paths', 'streams_dir')}"
        )

    root.bind("<Control-q>", lambda e: _full_quit())
    root.bind("<F1>", lambda e: _about_dialog())

    # ── Right panel — Tabbed notebook ──
    frame_right = tk.Frame(root)
    frame_right.grid(row=0, column=1, sticky="nsew", padx=(5, 10), pady=10)
    frame_right.grid_rowconfigure(0, weight=1)
    frame_right.grid_columnconfigure(0, weight=1)

    notebook = ttk.Notebook(frame_right)
    notebook.grid(row=0, column=0, sticky="nsew")

    # ── Tab 1: Status ──
    status_tab = ttk.Frame(notebook)
    notebook.add(status_tab, text="  Status  ")

    status_label = tk.Label(status_tab, text="Live Recording Status",
                            font=("Segoe UI", 13, "bold"), anchor="w")
    status_label.pack(anchor="w", padx=10, pady=(10, 6))

    columns = ("Channel", "Status", "Size", "Elapsed", "Platform")
    tree = ttk.Treeview(status_tab, columns=columns, show="headings", height=20)
    tree.heading("Channel", text="Channel")
    tree.heading("Status", text="Status")
    tree.heading("Size", text="Size")
    tree.heading("Elapsed", text="Elapsed")
    tree.heading("Platform", text="Platform")
    tree.column("Channel", width=200, anchor="w")
    tree.column("Status", width=300, anchor="w")
    tree.column("Size", width=90, anchor="center")
    tree.column("Elapsed", width=80, anchor="center")
    tree.column("Platform", width=75, anchor="center")
    tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

    # ── Status tree right-click context menu ──
    status_ctx_menu = tk.Menu(root, tearoff=0)

    def _get_selected_status_channel():
        """Get the internal channel name from the selected status tree row."""
        selected = tree.selection()
        if not selected:
            return None
        values = tree.item(selected[0], "values")
        if not values:
            return None
        display_name = values[0]  # The display name shown in the Channel column
        # Map display name back to the internal channel name
        if recorder:
            for ch_name in recorder.status_dict:
                if _get_display_name(ch_name) == display_name:
                    return ch_name
        return None

    def _stop_selected_channel():
        ch_name = _get_selected_status_channel()
        if not ch_name or not recorder or not recorder.is_running:
            return
        st = recorder.status_dict.get(ch_name, {})
        if st.get("status", "").lower() in ("stopped",):
            return  # already stopped
        recorder.stop_channel(ch_name)

    def _start_selected_channel():
        ch_name = _get_selected_status_channel()
        if not ch_name or not recorder or not recorder.is_running:
            return
        st = recorder.status_dict.get(ch_name, {})
        if st.get("status", "").lower() not in ("stopped",):
            return  # only restart channels that were individually stopped
        recorder.start_channel(ch_name)

    def _open_status_channel_url():
        ch_name = _get_selected_status_channel()
        if not ch_name:
            return
        if ch_name.startswith("custom:"):
            url = ch_name.split(":", 1)[1]
        elif ch_name.startswith("twitch:"):
            url = f"https://twitch.tv/{ch_name.split(':', 1)[1]}"
        elif ch_name.startswith("youtube:"):
            name = ch_name.split(':', 1)[1]
            url = f"https://youtube.com/@{name}/live" if not name.startswith("UC") else f"https://youtube.com/channel/{name}/live"
        else:
            url = f"https://kick.com/{ch_name}"
        import webbrowser
        webbrowser.open(url)

    def _show_status_context_menu(event):
        """Show context menu on status tree with options appropriate to channel state."""
        item = tree.identify_row(event.y)
        if not item:
            return
        tree.selection_set(item)

        # Rebuild menu based on channel state
        status_ctx_menu.delete(0, tk.END)

        ch_name = _get_selected_status_channel()
        if ch_name and recorder and recorder.is_running:
            st = recorder.status_dict.get(ch_name, {})
            status_lower = st.get("status", "").lower()

            if status_lower == "stopped":
                status_ctx_menu.add_command(label="Restart Channel", command=_start_selected_channel)
            else:
                status_ctx_menu.add_command(label="Stop Channel", command=_stop_selected_channel)

            status_ctx_menu.add_separator()

        status_ctx_menu.add_command(label="Open in Browser", command=_open_status_channel_url)

        try:
            status_ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            status_ctx_menu.grab_release()

    tree.bind("<Button-3>", _show_status_context_menu)

    # ── Tab 2: Logs ──
    log_tab = ttk.Frame(notebook)
    notebook.add(log_tab, text="  Logs  ")

    log_toolbar = ttk.Frame(log_tab)
    log_toolbar.pack(fill=tk.X, padx=10, pady=(8, 4))

    auto_scroll_var = tk.BooleanVar(value=True)
    ttk.Checkbutton(log_toolbar, text="Auto-scroll", variable=auto_scroll_var).pack(side=tk.LEFT)

    def clear_logs():
        log_text.configure(state=tk.NORMAL)
        log_text.delete("1.0", tk.END)
        log_text.configure(state=tk.DISABLED)

    ttk.Button(log_toolbar, text="Clear", command=clear_logs).pack(side=tk.RIGHT)

    log_text = tk.Text(log_tab, wrap=tk.WORD, font=("Consolas", 9), state=tk.DISABLED,
                       borderwidth=0, relief="flat")
    log_scroll = ttk.Scrollbar(log_tab, orient=tk.VERTICAL, command=log_text.yview)
    log_text.configure(yscrollcommand=log_scroll.set)
    log_scroll.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 10), pady=(0, 10))
    log_text.pack(fill=tk.BOTH, expand=True, padx=(10, 0), pady=(0, 10))

    # ── Bottom bar — buttons and toggles ──
    bottom_bar = tk.Frame(root)
    bottom_bar.grid(row=1, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))

    btn_frame = tk.Frame(bottom_bar)
    btn_frame.pack(side=tk.LEFT, padx=10)

    start_button = tk.Button(
        btn_frame, text=" Start Recording ", command=lambda: start_recording(),
        bg="#4CAF50", fg="white", font=("Segoe UI", 11, "bold"), width=18, height=2,
    )
    start_button.pack(side=tk.LEFT, padx=(0, 12))

    stop_button = tk.Button(
        btn_frame, text=" Stop Recording ", command=lambda: stop_recording(),
        bg="#F44336", fg="white", font=("Segoe UI", 11, "bold"), width=18, height=2,
        state=tk.DISABLED,
    )
    stop_button.pack(side=tk.LEFT)

    toggle_frame = tk.Frame(bottom_bar)
    toggle_frame.pack(side=tk.RIGHT, padx=10)

    dark_check = ttk.Checkbutton(toggle_frame, text="Dark Mode", variable=dark_mode,
                                 command=lambda: apply_theme())
    dark_check.pack(side=tk.RIGHT, padx=8)

    # ── Polling speed selector ──
    POLL_PRESETS = {
        "Relaxed (5 min)": 5.0,
        "Normal (3 min)": 3.0,
        "Fast (1 min)": 1.0,
    }

    poll_label = tk.Label(toggle_frame, text="Polling:", font=("Segoe UI", 9))
    poll_label.pack(side=tk.RIGHT, padx=(0, 2))

    current_poll = config.getfloat('Timeouts', 'poll_interval_minutes', fallback=3.0)
    # Find the closest preset name, or default to "Normal"
    poll_default = "Normal (3 min)"
    for name, val in POLL_PRESETS.items():
        if abs(val - current_poll) < 0.1:
            poll_default = name
            break
    poll_var = tk.StringVar(value=poll_default)

    def on_poll_change(*_args):
        selected = poll_var.get()
        minutes = POLL_PRESETS.get(selected, 3.0)
        config.config.set('Timeouts', 'poll_interval_minutes', str(minutes))
        # Write to config.ini so the workers pick it up on next start
        try:
            with open(config.config_file, 'w') as f:
                config.config.write(f)
        except Exception:
            pass
        logging.info(f"Polling interval changed to {minutes} minutes ({selected})")

    poll_combo = ttk.Combobox(toggle_frame, textvariable=poll_var,
                              values=list(POLL_PRESETS.keys()), state="readonly", width=14)
    poll_combo.pack(side=tk.RIGHT, padx=(0, 6))
    poll_combo.bind("<<ComboboxSelected>>", on_poll_change)

    about_btn = ttk.Button(toggle_frame, text="About", command=_about_dialog, width=6)
    about_btn.pack(side=tk.RIGHT, padx=(0, 4))

    # ── Status bar at very bottom ──
    dep_parts = []
    if HAS_YTDLP:
        dep_parts.append(f"yt-dlp {YTDLP_VERSION}")
    if HAS_STREAMLINK:
        dep_parts.append(f"streamlink {STREAMLINK_VERSION}")
    if HAS_FFMPEG:
        dep_parts.append(f"ffmpeg")
    dep_str = " | ".join(dep_parts) if dep_parts else "No recording tools found"

    status_bar = tk.Label(root, text=f"  v{__version__}  —  {dep_str}  —  Ctrl+Q quit, F1 about, Enter add, Del remove",
                          font=("Segoe UI", 8), anchor="w", padx=6, pady=2)
    status_bar.grid(row=2, column=0, columnspan=2, sticky="ew")

    # ── Version update check ──
    update_label = tk.Label(root, text="", font=("Segoe UI", 8, "bold"),
                            fg="#4FC3F7", cursor="hand2", anchor="e", padx=10, pady=2)
    # Overlaid on the right side of the status bar row
    update_label.grid(row=2, column=1, sticky="e")
    update_label.grid_remove()  # hidden until an update is found

    _update_url = [None]  # mutable container for the release URL

    def _on_update_found(latest_tag, release_url):
        """Called from version check thread — schedules GUI update on main thread."""
        _update_url[0] = release_url
        def _show():
            update_label.configure(text=f"  v{latest_tag} available ↗")
            update_label.grid()
        root.after(0, _show)

    def _open_release(event=None):
        if _update_url[0]:
            import webbrowser
            webbrowser.open(_update_url[0])

    update_label.bind("<Button-1>", _open_release)

    # Delay the check by 10 seconds so it doesn't slow startup
    def _delayed_version_check():
        check_for_updates(__version__, callback=_on_update_found)
    root.after(10_000, _delayed_version_check)

    # ── Refresh functions ──
    def _get_platform_label(ch_name):
        """Extract a display platform from a channel name string."""
        if ch_name.startswith("twitch:"):
            return "Twitch"
        elif ch_name.startswith("youtube:"):
            return "YouTube"
        elif ch_name.startswith("fishtank:"):
            return "Fishtank"
        elif ch_name.startswith("rumble:"):
            return "Rumble"
        elif ch_name.startswith("custom:"):
            platform, _ = parse_custom_url(ch_name.split(":", 1)[1])
            return platform.capitalize()
        else:
            return "Kick"

    def _get_display_name(ch_name):
        """Extract a clean display name for the status table."""
        if ch_name.startswith("custom:"):
            _, channel = parse_custom_url(ch_name.split(":", 1)[1])
            return channel
        elif ":" in ch_name:
            return ch_name.split(":", 1)[1]
        return ch_name

    def refresh_status():
        if recorder and recorder.is_running:
            recorder.update_status_from_queue()

            # Preserve the currently selected channel name so selection survives refresh
            selected_display_name = None
            sel = tree.selection()
            if sel:
                vals = tree.item(sel[0], "values")
                if vals:
                    selected_display_name = vals[0]

            for item in tree.get_children():
                tree.delete(item)

            # Show all channels tracked by the recorder (includes individually stopped ones)
            for ch_name in recorder.status_dict:
                st = recorder.status_dict.get(ch_name, {"status": "Unknown", "detail": "", "size": "", "time": ""})
                curr = st["status"].lower()

                # Build the Status cell text.
                # While recording: show stream info (resolution/bitrate) if available,
                # otherwise fall back to any detail text (e.g. "starting", "fallback (streamlink)").
                # For all other states: status + detail in parens as before.
                if "recording" in curr:
                    detail = st.get("detail", "")
                    if detail:
                        display = f"Recording  {detail}"
                    else:
                        display = "Recording"
                else:
                    display = st["status"]
                    if st.get("detail"):
                        display += f" ({st['detail']})"

                platform_label = _get_platform_label(ch_name)

                if "recording" in curr:
                    tag = "recording"
                    if notifications_enabled and ch_name not in _notified_live and "starting" not in st.get("detail", ""):
                        _notified_live.add(ch_name)
                        send_notification("Stream Recording", f"Now recording: {ch_name}",
                                          category="recording", channel=ch_name)
                elif "remuxing" in curr or "pending" in curr:
                    tag = "remuxing"
                elif "checking" in curr or "initializing" in curr:
                    tag = "checking"
                elif "error" in curr or "failed" in curr:
                    tag = "error"
                    if notifications_enabled:
                        send_notification("Recording Error", f"{ch_name}: {display}",
                                          category="error", channel=ch_name, detail=display)
                elif "completed" in curr:
                    tag = "completed"
                    _notified_live.discard(ch_name)
                    if notifications_enabled:
                        send_notification("Recording Complete", f"{ch_name}: {st.get('detail', '')}",
                                          category="complete", channel=ch_name)
                elif "offline" in curr:
                    tag = "offline"
                    _notified_live.discard(ch_name)
                elif "stopped" in curr:
                    tag = "stopped"
                    _notified_live.discard(ch_name)
                else:
                    tag = "unknown"

                display_name = _get_display_name(ch_name)
                item_id = tree.insert("", tk.END, values=(display_name, display, st["size"], st["time"], platform_label), tags=(tag,))

                # Restore selection if this was the previously selected channel
                if display_name == selected_display_name:
                    tree.selection_set(item_id)

            # Update tray icon tooltip if tray exists
            if tray_icon is not None:
                active = sum(1 for ch_name in recorder.status_dict
                             if recorder.status_dict.get(ch_name, {}).get("status", "").lower().startswith("recording"))
                tray_icon.title = f"Multi-Stream Recorder — {active} recording" if active else "Multi-Stream Recorder — idle"

        root.after(2500, refresh_status)

    def refresh_logs():
        """Pull log messages from queue and append to log viewer."""
        count = 0
        while count < 50:  # process up to 50 messages per tick to avoid blocking
            try:
                msg = log_queue.get_nowait()
                log_text.configure(state=tk.NORMAL)
                log_text.insert(tk.END, msg + "\n")
                # Keep log buffer reasonable (max ~5000 lines)
                line_count = int(log_text.index('end-1c').split('.')[0])
                if line_count > 5000:
                    log_text.delete("1.0", f"{line_count - 4000}.0")
                log_text.configure(state=tk.DISABLED)
                if auto_scroll_var.get():
                    log_text.see(tk.END)
                count += 1
            except stdlib_queue.Empty:
                break
        root.after(500, refresh_logs)

    # ── Recording controls ──
    def start_recording():
        nonlocal recorder
        enabled = get_enabled_channels()
        if not enabled:
            messagebox.showwarning("No channels", "Enable at least one channel (click the checkbox).")
            return
        if recorder and recorder.is_running:
            messagebox.showinfo("Already running", "Recording is active.")
            return

        # Warn about large number of concurrent recordings
        if len(enabled) >= 15:
            if not messagebox.askyesno("High Channel Count",
                    f"You're about to monitor {len(enabled)} channels simultaneously.\n\n"
                    "This may strain your CPU, RAM, and disk I/O. Each channel spawns "
                    "its own process.\n\nContinue?"):
                return

        # Pre-flight checks — warn about channels that can't be recorded
        twitch_channels = [ch for ch in enabled if ch.startswith("twitch:")]
        ytdlp_channels = [ch for ch in enabled if not ch.startswith("twitch:")]
        issues = []

        if twitch_channels and not HAS_STREAMLINK:
            names = ", ".join(twitch_channels[:3])
            issues.append(f"Twitch channels ({names}) require streamlink, which is not installed.")

        if ytdlp_channels and not HAS_YTDLP:
            names = ", ".join(ytdlp_channels[:3])
            issues.append(f"Kick/YouTube/custom channels ({names}) require yt-dlp, which is not installed.")

        if not HAS_FFMPEG:
            issues.append("ffmpeg is not installed — recordings cannot be remuxed to MP4.")

        if issues:
            msg = "The following issues may prevent recording:\n\n" + "\n\n".join(issues) + "\n\nStart anyway?"
            if not messagebox.askyesno("Dependency Warning", msg):
                return

        logging.info("=" * 60)
        logging.info("Starting recording session...")
        for ch in enabled:
            logging.info(f"  Channel: {ch}")
        disabled_count = len(channels) - len(enabled)
        if disabled_count > 0:
            logging.info(f"  ({disabled_count} channel(s) disabled — not monitoring)")
        logging.info("=" * 60)

        recorder = StreamRecorder(enabled, config)
        threading.Thread(target=recorder.run, daemon=True).start()

        start_button.config(state=tk.DISABLED)
        stop_button.config(state=tk.NORMAL)
        _notified_live.clear()
        _notif_throttle.reset()

        if notifications_enabled:
            send_notification("Recording Started", f"Monitoring {len(enabled)} channel(s)")

    def stop_recording():
        nonlocal recorder
        if not recorder or not recorder.is_running:
            return

        # Collect recording summary before stopping (include all tracked channels)
        summary_parts = []
        active_count = 0
        try:
            for ch_name, st in recorder.status_dict.items():
                if st.get("status", "").lower().startswith("recording"):
                    active_count += 1
                    size_str = st.get("size", "")
                    time_str = st.get("time", "")
                    if size_str and time_str:
                        summary_parts.append(f"  {ch_name}: {size_str}, {time_str}")
        except Exception:
            pass

        logging.info("Stop requested — terminating...")
        recorder.stop()

        # Log recording summary
        if summary_parts:
            logging.info(f"Recording summary — {active_count} stream(s) captured:")
            for part in summary_parts:
                logging.info(part)
        else:
            logging.info("Recording summary — no active streams were being captured")

        # Update the GUI to show "Stopped" for all channels
        for item in tree.get_children():
            tree.delete(item)
        for ch_name in recorder.status_dict:
            display_name = _get_display_name(ch_name)
            platform_label = _get_platform_label(ch_name)
            tree.insert("", tk.END, values=(display_name, "Stopped", "", "", platform_label), tags=("stopped",))

        recorder = None

        start_button.config(state=tk.NORMAL)
        stop_button.config(state=tk.DISABLED)
        _notified_live.clear()
        _notif_throttle.reset()

        if notifications_enabled:
            send_notification("Recording Stopped", "All recordings have been stopped.")

    # ── System tray ──
    tray_icon = None

    def setup_tray():
        nonlocal tray_icon
        if not HAS_TRAY or not minimize_to_tray:
            return

        def show_window(icon=None, item=None):
            root.after(0, root.deiconify)
            root.after(50, root.lift)

        def hide_to_tray(icon=None, item=None):
            root.after(0, root.withdraw)

        def quit_app(icon=None, item=None):
            root.after(0, _full_quit)

        def tray_start(icon=None, item=None):
            root.after(0, start_recording)

        def tray_stop(icon=None, item=None):
            root.after(0, stop_recording)

        menu = pystray.Menu(
            pystray.MenuItem("Show Window", show_window, default=True),
            pystray.MenuItem("Hide to Tray", hide_to_tray),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Start Recording", tray_start),
            pystray.MenuItem("Stop Recording", tray_stop),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit", quit_app),
        )

        icon_img = create_tray_icon_image(recording=False)
        tray_icon = pystray.Icon("multi_stream_recorder", icon_img, "Multi-Stream Recorder", menu)
        threading.Thread(target=tray_icon.run, daemon=True).start()

    setup_tray()

    # ── Window close behavior ──
    def on_close():
        """Always fully quit when the window X is clicked."""
        # Save window state
        try:
            state = {
                'geometry': root.geometry(),
                'dark_mode': dark_mode.get(),
            }
            save_window_state(state_file, state)
        except Exception:
            pass

        _full_quit()

    def _full_quit():
        """Fully terminate the application — stop recorder, tray, and exit.
        
        Kills ALL child processes by walking the process tree, then calls
        os._exit(0) as a final backstop.  This prevents orphaned workers.
        """
        # 1. Try graceful recorder shutdown
        try:
            if recorder and recorder.is_running:
                recorder.stop()
        except Exception:
            pass
        try:
            if recorder:
                recorder.manager.shutdown()
        except Exception:
            pass

        # 2. Kill ALL child processes of this Python process (nuclear option)
        #    This catches any workers/manager processes that survived stop()
        try:
            import psutil
            parent = psutil.Process(os.getpid())
            children = parent.children(recursive=True)
            for child in children:
                try:
                    child.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            psutil.wait_procs(children, timeout=3)
        except Exception:
            pass

        # 3. Stop tray and destroy window
        try:
            if tray_icon:
                tray_icon.stop()
        except Exception:
            pass
        try:
            root.destroy()
        except Exception:
            pass
        # Force exit — this MUST be the last line and MUST execute
        os._exit(0)

    root.protocol("WM_DELETE_WINDOW", on_close)

    # ── Apply theme and start refresh loops ──
    apply_theme()
    # Re-apply dark title bar after window is fully mapped (100ms + 500ms delays)
    # The DWM attribute only takes effect once the HWND is realized
    root.after(100, lambda: set_title_bar_dark(dark_mode.get()))
    root.after(500, lambda: set_title_bar_dark(dark_mode.get()))
    root.after(1500, refresh_status)
    root.after(500, refresh_logs)

    try:
        root.mainloop()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt detected — shutting down...")
        _full_quit()


# ────────────────────────────────────────────────
#          Headless / CLI Mode
# ────────────────────────────────────────────────

def main_headless(config):
    """Run recording without a GUI.  Suitable for background tasks or services.

    Ctrl+C performs a clean shutdown.
    """
    channels_file = config.get('Paths', 'channels_file')
    channels = []
    try:
        with open(channels_file, "r") as f:
            loaded = json.load(f)
            if isinstance(loaded, list):
                for item in loaded:
                    if isinstance(item, str):
                        channels.append(item)  # old format
                    elif isinstance(item, dict) and "name" in item:
                        if item.get("enabled", True):
                            channels.append(item["name"])  # new format, only enabled
                    elif isinstance(item, list) and len(item) >= 1 and isinstance(item[0], str):
                        channels.append(item[0])  # very old format
    except FileNotFoundError:
        logging.error(f"Channels file not found: {channels_file}")
        logging.error("Create a channels.json with a list of channels, e.g.: [\"twitch:zackrawrr\", \"asmongold\"]")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading channels: {e}")
        sys.exit(1)

    if not channels:
        logging.error("No channels configured.  Add channels to channels.json first.")
        sys.exit(1)

    print("\n" + "=" * 80)
    print(f"Multi-Stream Recorder v{__version__} — HEADLESS MODE")
    print("Starting recording session...")
    for ch in channels:
        print(f"  • {ch}")
    print("Press Ctrl+C to stop.")
    print("=" * 80 + "\n")

    recorder = StreamRecorder(channels, config)
    shutdown_requested = threading.Event()

    def signal_handler(signum, frame):
        if not shutdown_requested.is_set():
            shutdown_requested.set()
            print("\nShutdown requested — stopping all recordings...")
            recorder.stop()
            sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run in main thread
    try:
        recorder.run()
    except KeyboardInterrupt:
        if not shutdown_requested.is_set():
            shutdown_requested.set()
            print("\nKeyboardInterrupt — stopping all recordings...")
            recorder.stop()


# ────────────────────────────────────────────────
#          Entry Point
# ────────────────────────────────────────────────

def main():
    """Parse arguments and start the recorder."""
    parser = argparse.ArgumentParser(
        description=f"Multi-Stream Recorder v{__version__}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s                     Launch GUI (default)
  %(prog)s --headless          Run without GUI (Ctrl+C to stop)
  %(prog)s --config my.ini     Use a custom config file
        """,
    )
    parser.add_argument('--headless', action='store_true',
                        help='Run without GUI (background mode)')
    parser.add_argument('--config', default='config.ini',
                        help='Path to config file (default: config.ini)')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')

    args = parser.parse_args()

    if os.name == 'nt':
        mp.set_start_method('spawn', force=True)

    config = Config(args.config)

    setup_logging(config.get('Paths', 'streams_dir'))

    logging.info(f"Multi-Stream Recorder v{__version__} starting...")
    logging.info(f"yt-dlp available: {HAS_YTDLP} (version: {YTDLP_VERSION})")
    logging.info(f"streamlink available: {HAS_STREAMLINK} (version: {STREAMLINK_VERSION})")
    logging.info(f"ffmpeg available: {HAS_FFMPEG} (version: {FFMPEG_VERSION})")
    logging.info(f"psutil available: {HAS_PSUTIL}")
    logging.info(f"curl_cffi available: {HAS_CURL_CFFI} (browser impersonation)")
    logging.info(f"deno available: {HAS_DENO} (version: {DENO_VERSION}) (YouTube n-challenge solving)")
    logging.info(f"System tray available: {HAS_TRAY}")
    logging.info(f"Notifications available: {HAS_NOTIFICATIONS}")
    logging.info(f"Streams directory: {config.get('Paths', 'streams_dir')}")

    # ── Startup validation ──
    errors, warnings = validate_startup(config)

    for w in warnings:
        logging.warning(f"STARTUP WARNING: {w.splitlines()[0]}")

    if errors:
        for e in errors:
            logging.error(f"STARTUP ERROR: {e.splitlines()[0]}")

        if not args.headless:
            # Show a GUI error dialog before bailing
            try:
                import tkinter as tk
                from tkinter import messagebox
                _err_root = tk.Tk()
                _err_root.withdraw()
                detail = "\n\n".join(errors)
                messagebox.showerror(
                    "Multi-Stream Recorder — Missing Dependencies",
                    f"The following critical issues were found:\n\n{detail}\n\n"
                    "The program cannot start until these are resolved."
                )
                _err_root.destroy()
            except Exception:
                pass

            print("\n" + "=" * 60)
            print("FATAL: Cannot start — missing critical dependencies:")
            print("=" * 60)
            for e in errors:
                print(f"\n  ✗ {e}")
            print()
            sys.exit(1)
        else:
            print("\nFATAL: Cannot start — missing critical dependencies:")
            for e in errors:
                print(f"  ✗ {e}")
            sys.exit(1)

    # Log warnings to console too
    if warnings:
        for w in warnings:
            first_line = w.splitlines()[0]
            print(f"  ⚠ {first_line}")

    # Auto-purge PendingDeletion on startup
    if config.getboolean('Cleanup', 'purge_on_startup', fallback=True):
        max_age = config.getint('Cleanup', 'auto_purge_days', fallback=7)
        if max_age > 0:
            purge_old_pending_files(
                config.get('Paths', 'streams_dir'),
                max_age,
                logging.getLogger(),
            )

    if args.headless:
        main_headless(config)
    else:
        main_gui(config)


if __name__ == "__main__":
    main()
