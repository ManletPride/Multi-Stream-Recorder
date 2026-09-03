"""Shared helpers: paths, cookies, logging, process control, validation."""
import configparser
import datetime
import json
import logging
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
import urllib.request

from msr.deps import (
    HAS_CURL_CFFI, HAS_DENO, HAS_FFMPEG, HAS_PSUTIL,
    HAS_STREAMLINK, HAS_YTDLP, psutil,
)

# ────────────────────────────────────────────────
#          Constants & Helpers
# ────────────────────────────────────────────────

PENDING_DELETION_FOLDER = "PendingDeletion"

# Channel keys are either a bare Kick name or ``platform:rest``.
# Kick is never stored with a prefix — a colon in a Kick name used to be
# treated as a platform directory (``..\..\Users\Public:out``).
KNOWN_PLATFORMS = frozenset({
    "kick", "twitch", "youtube", "rumble", "tiktok", "fishtank", "custom",
})
_PREFIXED_PLATFORMS = frozenset(KNOWN_PLATFORMS - {"kick"})
_UNSAFE_NAME_CHARS = set(' \t\n\r<>"|?*:/\\')


def validate_startup(config):
    """Validate dependencies and config at startup.

    Returns (errors: list[str], warnings: list[str]).
    Errors are fatal — the program cannot work.  Warnings are non-fatal
    but the user should be aware.
    """
    errors = []
    warnings = []
    warnings.extend(getattr(config, 'coerce_warnings', None) or [])

    # ── Required dependencies ──
    if not HAS_FFMPEG:
        errors.append(
            "ffmpeg not found in PATH.  ffmpeg is required for remuxing recordings.\n"
            "  Install: https://ffmpeg.org/download.html\n"
            "  Windows: download, extract, add bin/ folder to system PATH"
        )

    if not HAS_YTDLP:
        warnings.append(
            "yt-dlp not found in PATH.  YouTube, Rumble, TikTok, and custom URL "
            "recording will not work.\n"
            "  Install: pip install \"yt-dlp[default]\""
        )

    if not HAS_STREAMLINK:
        warnings.append(
            "streamlink not found in PATH.  Kick and Twitch recording will not work.\n"
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

    # Numeric keys are coerced to defaults in Config._coerce_values before
    # this runs. 0 remains a documented "off" for time limit / split / quota.
    try:
        max_hours = config.getfloat('Recording', 'max_record_hours', fallback=12.0)
        if max_hours is not None and max_hours <= 0:
            warnings.append("max_record_hours is <= 0 — recordings will have no time limit")
    except (ValueError, TypeError, configparser.Error):
        pass

    try:
        max_size = config.getfloat('Recording', 'max_file_size_gb', fallback=8.0)
        if max_size == 0:
            warnings.append("max_file_size_gb is 0 — file size splitting is disabled")
    except (ValueError, TypeError, configparser.Error):
        pass

    try:
        min_space = config.getfloat('Recording', 'min_disk_space_gb', fallback=5.0)
        if min_space == 0:
            warnings.append("min_disk_space_gb is 0 — disk space quota check is disabled")
    except (ValueError, TypeError, configparser.Error):
        pass

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


def _unsafe_channel_name(name):
    """True if *name* cannot be a Kick/Twitch/… folder component."""
    if not name or name in (".", ".."):
        return True
    if name.startswith("//") or name.startswith("\\\\"):
        return True
    if any(c in _UNSAFE_NAME_CHARS for c in name):
        return True
    if "\x00" in name:
        return True
    return False


def parse_channel_key(channel_key):
    """Return ``(platform, name)`` or ``None`` if the key is unknown or unsafe.

    Kick keys are a bare name (no prefix). Everything else is
    ``platform:rest``. Unknown prefixes, ``file:`` custom URLs, and path
    metacharacters in names are rejected so they cannot become directory
    components.
    """
    if not isinstance(channel_key, str):
        return None
    key = channel_key.strip()
    if not key:
        return None
    if key.startswith("custom:"):
        url = key.split(":", 1)[1].strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            return None
        if len(url) > 500:
            return None
        return "custom", url
    if ":" in key:
        platform, name = key.split(":", 1)
        platform = platform.lower().strip()
        name = name.strip()
        if platform not in _PREFIXED_PLATFORMS:
            return None
        if _unsafe_channel_name(name) or len(name) > 100:
            return None
        return platform, name
    if _unsafe_channel_name(key) or len(key) > 100:
        return None
    return "kick", key


def coerce_channel_records(loaded):
    """Normalize a channels.json list to ``({name, enabled}, …)``.

    Drops entries whose keys fail ``parse_channel_key`` (unknown platform,
    Kick name with ``:``, ``file:`` URL, path traversal). Returns
    ``(records, skipped_names)``.
    """
    records = []
    skipped = []
    seen = set()
    if not isinstance(loaded, list):
        return records, skipped
    for item in loaded:
        enabled = True
        if isinstance(item, str):
            name = item
        elif isinstance(item, dict) and "name" in item:
            name = item["name"]
            enabled = item.get("enabled", True)
        elif isinstance(item, list) and len(item) >= 1 and isinstance(item[0], str):
            name = item[0]
        else:
            continue
        if not isinstance(name, str):
            continue
        if parse_channel_key(name) is None:
            skipped.append(name)
            continue
        if name in seen:
            skipped.append(name)
            continue
        seen.add(name)
        records.append({"name": name, "enabled": bool(enabled)})
    return records, skipped


def validate_channel_name(name, platform, existing_channels):
    """Validate a channel name before adding.

    Returns (is_valid: bool, error_message: str | None).
    """
    if not name:
        return False, "Channel name cannot be empty."

    platform = (platform or "").lower().strip()
    if platform not in KNOWN_PLATFORMS:
        return False, f"Unknown platform '{platform}'."

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

    # Check for obviously invalid characters. Colon is included so a Kick
    # name cannot be split into a fake platform directory on load.
    invalid_chars = set(' \t\n\r<>"|?*:/\\')
    found = invalid_chars & set(name)
    if found:
        return False, f"Channel name contains invalid characters: {', '.join(repr(c) for c in found)}"

    if name in (".", ".."):
        return False, "Channel name is not a valid folder name."

    # Check for URL pasting (common mistake — except for custom platform)
    if name.startswith('http://') or name.startswith('https://'):
        return False, "Paste just the channel name, not the full URL.\n  Example: 'asmongold' instead of 'https://kick.com/asmongold'\n\n  For arbitrary URLs, select 'custom' as the platform."

    # Build full channel key and check for duplicate
    ch_key = f"{platform}:{name}" if platform != "kick" else name
    if ch_key in existing_channels:
        return False, f"'{ch_key}' is already in the list."

    # Platform-specific tool requirements
    if platform in ("kick", "twitch") and not HAS_STREAMLINK:
        return False, (
            f"Cannot add {platform.title()} channels — streamlink is not installed.\n"
            "  Install: pip install streamlink"
        )

    if platform in ("youtube", "rumble", "tiktok") and not HAS_YTDLP:
        return False, (
            f"Cannot add {platform.title()} channels — yt-dlp is not installed.\n"
            "  Install: pip install \"yt-dlp[default]\""
        )

    if platform == "fishtank":
        from msr.platforms import FishtankAuth, is_known_fishtank_camera
        if not is_known_fishtank_camera(name):
            known = ", ".join(sorted(
                k for k in FishtankAuth.CAMERA_ALIASES
                if not k[-1].isdigit()
            )[:20])
            return False, (
                f"Unknown fishtank camera '{name}'.\n"
                f"  Known names: {known}, …\n"
                f"  Or paste the raw stream id from GET /v1/live-streams "
                f"(e.g. dirc-5, bar-5). Room names change every season."
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


def interruptible_sleep(seconds, wake_event, stop_event=None):
    """Sleep for up to `seconds`, but wake immediately if wake_event is set.

    Used by the offline/error/reconnect wait paths so the GUI's "Check Now"
    can short-circuit a long poll timer.  Waits in short slices so a global
    stop is also noticed promptly instead of blocking out the full sleep.

    Returns True if woken early by wake_event (manual check requested),
    False on a normal timeout or stop.
    """
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if stop_event is not None and stop_event.is_set():
            return False
        # Wait in ≤2s slices so stop_event stays responsive
        if wake_event.wait(timeout=min(remaining, 2.0)):
            wake_event.clear()
            return True


def check_disk_space(path, min_gb=5.0):
    """Return ``(has_enough, free_gb)``.

    ``free_gb`` is ``None`` when usage could not be measured but the path
    is still writable. ``has_enough`` is False when free space is below
    *min_gb*, or when the path cannot be written (fail closed — a
    permission/path error must not keep recording until the volume fills).
    """
    try:
        stat = shutil.disk_usage(path)
        free_gb = stat.free / (1024**3)
        return free_gb >= min_gb, free_gb
    except Exception as e:
        logging.error(f"Failed to check disk space: {e}")
        try:
            os.makedirs(path, exist_ok=True)
            probe = os.path.join(path, ".msr_write_probe")
            with open(probe, "wb") as fh:
                fh.write(b"ok")
            try:
                os.remove(probe)
            except OSError:
                pass
            logging.warning(
                "Could not read free space; streams directory is writable — "
                "continuing without a quota check"
            )
            return True, None
        except Exception as write_err:
            logging.error(
                f"streams directory is not writable ({write_err}) — pausing"
            )
            return False, 0


def ffprobe_from_ffmpeg(ffmpeg_path):
    """Return the ffprobe binary that lives next to *ffmpeg_path*.

    ffmpeg and ffprobe ship as a pair. A configured ``C:\\ffmpeg\\bin\\ffmpeg.exe``
    must use that folder's ffprobe — ``str.replace("ffmpeg", "ffprobe")`` would
    also rewrite the directory name.
    """
    ffmpeg_path = (ffmpeg_path or "ffmpeg").strip() or "ffmpeg"
    directory = os.path.dirname(ffmpeg_path)
    use_exe = ffmpeg_path.lower().endswith(".exe") or (os.name == "nt" and not directory)
    name = "ffprobe.exe" if use_exe else "ffprobe"
    if directory:
        return os.path.join(directory, name)
    return name


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


def application_dir():
    """Directory of the user-facing script (or the project root).

    ``__file__`` inside this package is ``msr/util.py``, so cookies.txt sitting
    next to ``Multi-Stream-Recorder.py`` would be missed without this.
    """
    main_file = getattr(sys.modules.get("__main__"), "__file__", None)
    if main_file:
        return os.path.dirname(os.path.abspath(main_file))
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def find_cookies_file(config):
    """Locate a cookies.txt file.  Checks config value first, then common locations."""
    explicit = config.get('Paths', 'cookies_file', fallback='')
    if explicit and os.path.isfile(explicit):
        return explicit
    streams_dir = config.get('Paths', 'streams_dir')
    for candidate in [
        os.path.join(streams_dir, 'cookies.txt'),
        os.path.join(application_dir(), 'cookies.txt'),
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
        # TikTok
        'sessionid', 'sessionid_ss', 'sid_tt', 'sid_guard',
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
        'tiktok:somecreator'                     → 'tiktok.com'
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
    elif channel_key.startswith('tiktok:'):
        return 'tiktok.com'
    elif channel_key.startswith('rumble:'):
        return 'rumble.com'
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

        # Common path segments that are NOT usernames (site chrome, categories,
        # playlists). Used so custom URLs like chaturbate.com/alice/ can be
        # nested under custom/chaturbate/alice instead of one bag per site.
        SKIP_SEGMENTS = {
            'live', 'stream', 'watch', 'embed', 'channel', 'user', 'users',
            'c', 's', 'v', 'video', 'videos', 'clip', 'clips',
            'category', 'browse', 'directory', 'search', 'about',
            'hls', 'playlist', 'master', 'index', 'api', 'login', 'signup',
            'followed-cams', 'tags', 'discover', 'accounts', 'promotions',
            'contest', 'privacy', 'terms', 'affiliate', 'static', 'cdn',
            'player', 'room', 'rooms', 'cam', 'cams', 'webcam',
            'female', 'male', 'couple', 'trans',
        }

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
    # Collapse multiple underscores
    while '__' in name:
        name = name.replace('__', '_')
    # Strip leading/trailing dots and spaces only — NOT underscores.
    # Usernames like _avamartinez are valid and should be preserved in filenames.
    return name.strip('. ')[:200]  # cap length


def sanitize_path_component(name):
    """Sanitize a username/channel name for safe use as a folder name on Windows.

    Unlike _sanitize_filename this preserves leading underscores (valid in
    TikTok/Twitter handles like _avamartinez) and mid-string periods (boo.tleg),
    while stripping characters Windows won't accept in folder names.
    """
    if not name:
        return 'unknown'
    # Replace Windows-forbidden folder name characters
    for ch in r'<>:"/\|?*':
        name = name.replace(ch, '_')
    # Windows silently rejects or misbehaves with folder names ending in '.' or ' '
    name = name.rstrip('. ')
    # A name that became empty or just underscores after sanitization
    return name[:100] if name.strip('_') else 'unknown'


def custom_url_folder_names(url):
    """Map a custom URL to ``(file_username, relative_dir)``.

    ``relative_dir`` is joined under ``Recorded/custom/`` (and the matching
    Processed / Clips / PendingDeletion trees).

    * TikTok custom URLs stay a single handle folder (``custom/<handle>``)
      so existing recordings do not move.
    * Every other site uses ``<site>/<handle>`` when the URL path contains
      a username (e.g. chaturbate.com/alice/ → ``chaturbate/alice``).
    * If the path has no username (direct ``.m3u8``, site root, …) the
      previous one-folder-per-site bag is kept (``<site>``).
    """
    site, handle = parse_custom_url(url)
    site_dir = sanitize_path_component(
        site if site and site != "unknown"
        else (extract_domain_from_url(url) or "custom")
    )
    if "tiktok.com" in (url or "").lower():
        if handle and handle != "unknown":
            name = sanitize_path_component(handle)
            return name, name
        return site_dir, site_dir
    if handle and handle != "unknown":
        user_dir = sanitize_path_component(handle)
        return user_dir, os.path.join(site_dir, user_dir)
    return site_dir, site_dir


def channel_key_to_dirs(channel_key):
    """Map a channel_key to its on-disk (platform, username_dir) pair.

    This MUST mirror record_worker's directory derivation exactly, because
    BackgroundCleaner uses it to match status_dict entries (keyed by
    channel_key) to directories under Recorded/.  Keys are parsed by
    ``parse_channel_key`` (allowlisted platforms; Kick is a bare name).
    Unsafe keys return ``('unknown', 'unknown')`` so they cannot traverse
    out of the recordings tree.

    with wrinkles copied from record_worker:
      • tiktok channels drop a leading '@' — record_worker does
        `username = username.lstrip('@')` before building paths, so
        'tiktok:@qvc' records into Recorded/tiktok/qvc, NOT '.../@qvc'.
      • custom tiktok.com URLs use the parsed handle as a single folder,
      • other custom URLs with a path username nest as ``site/user``
        (chaturbate.com/alice/ → custom/chaturbate/alice); URLs with no
        username stay in the site bag (``custom/chaturbate``).

    Note the '@' is stripped only for tiktok.  YouTube handles keep theirs:
    record_worker uses '@handle' in the URL but never reassigns username, so
    'youtube:@foo' really does record into Recorded/youtube/@foo.
    """
    parsed = parse_channel_key(channel_key)
    if parsed is None:
        return "unknown", "unknown"
    platform, username = parsed

    if platform == "tiktok":
        username = username.lstrip('@')
    elif platform == "custom":
        _, rel = custom_url_folder_names(username)
        return sanitize_path_component(platform), rel

    return sanitize_path_component(platform), sanitize_path_component(username)


def channel_file_stem(username_dir):
    """Last folder component — safe to use in a filename.

    Custom URLs nest as ``chaturbate/mode_bad``. Putting that whole relative
    dir in a filename makes Windows treat the ``\\`` as another directory
    (``.../mode_bad/chaturbate/mode_bad_….mp4``).
    """
    stem = os.path.basename(os.path.normpath(username_dir or ""))
    return stem or "unknown"


def iter_channel_record_dirs(recorded_base):
    """Yield ``(platform, rel_dir, abs_path)`` under Recorded/.

    Walks one extra directory level so ``custom/chaturbate/alice`` is
    visited as well as leftover ``.ts`` files still sitting in the old
    ``custom/chaturbate`` bag.
    """
    if not recorded_base or not os.path.isdir(recorded_base):
        return
    try:
        platforms = os.listdir(recorded_base)
    except OSError:
        return
    for platform in platforms:
        platform_dir = os.path.join(recorded_base, platform)
        if not os.path.isdir(platform_dir):
            continue
        try:
            names = os.listdir(platform_dir)
        except OSError:
            continue
        for name in names:
            path = os.path.join(platform_dir, name)
            if not os.path.isdir(path):
                continue
            yield platform, name, path
            try:
                subnames = os.listdir(path)
            except OSError:
                continue
            for sub in subnames:
                subpath = os.path.join(path, sub)
                if os.path.isdir(subpath):
                    yield platform, os.path.join(name, sub), subpath


def channel_watch_url(channel_key):
    """Return a browser URL for this channel key, or None if unknown.

    Mirrors ``record_worker`` URL construction so Open in Browser hits the
    same site the recorder uses. Kick is the only platform stored as a
    bare name; everything else is ``platform:name``.
    """
    if not channel_key:
        return None
    if channel_key.startswith("custom:"):
        return channel_key.split(":", 1)[1]
    if ":" in channel_key:
        platform, name = channel_key.split(":", 1)
    else:
        platform, name = "kick", channel_key

    if platform == "kick":
        return f"https://kick.com/{name}"
    if platform == "twitch":
        return f"https://twitch.tv/{name}"
    if platform == "youtube":
        if name.startswith("UC"):
            return f"https://youtube.com/channel/{name}/live"
        if name.startswith("@"):
            return f"https://youtube.com/{name}/live"
        if "watch?v=" in name:
            return name if name.startswith("http") else f"https://youtube.com/{name}"
        if len(name) == 11:
            return f"https://youtube.com/watch?v={name}"
        return f"https://youtube.com/@{name}/live"
    if platform == "rumble":
        return f"https://rumble.com/c/{name}"
    if platform == "tiktok":
        handle = name.lstrip("@")
        return f"https://www.tiktok.com/@{handle}/live"
    if platform == "fishtank":
        return "https://www.fishtank.live/"
    return None


def open_local_path(path):
    """Open a file or directory in the OS file browser."""
    if os.name == "nt":
        os.startfile(path)
    elif sys.platform == "darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])


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


def _ffmpeg_is_msr_network_orphan(cmdline, streams_dir):
    """True if this ffmpeg cmdline is an MSR network capture under *streams_dir*.

    Requires both an http(s) input (live download, not a local remux/clip)
    and the streams directory in the argv so a user's unrelated encode of
    some other URL is left alone.
    """
    if not cmdline or not streams_dir:
        return False
    if isinstance(cmdline, (list, tuple)):
        text = " ".join(str(p) for p in cmdline)
    else:
        text = str(cmdline)
    lower = text.lower()
    if "http://" not in lower and "https://" not in lower:
        return False
    marker = os.path.normcase(os.path.abspath(streams_dir))
    haystack = os.path.normcase(text)
    if marker in haystack:
        return True
    return marker.replace("\\", "/") in haystack.replace("\\", "/")


def kill_orphan_ffmpeg_processes(logger=None, streams_dir=None):
    """Safety net: kill leftover MSR network-ffmpeg after workers are gone.

    Only called during shutdown. Local remux/clip ffmpeg (no http) is skipped.
    Network ffmpeg is killed only when its command line includes *streams_dir*,
    so a user encode/download running at the same time is not SIGKILL'd.
    """
    if not streams_dir:
        if logger:
            logger.info("Orphan ffmpeg check skipped — no streams directory given")
        return

    if HAS_PSUTIL:
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if not (proc.info['name'] and 'ffmpeg' in proc.info['name'].lower()):
                        continue
                    cmdline = proc.info.get('cmdline') or []
                    if _ffmpeg_is_msr_network_orphan(cmdline, streams_dir):
                        if logger:
                            logger.info(
                                f"Killing orphaned ffmpeg (PID {proc.pid}) — "
                                f"MSR network capture under {streams_dir}"
                            )
                        proc.kill()
                    elif logger:
                        logger.info(
                            f"Skipping ffmpeg PID {proc.pid} — not an MSR network capture"
                        )
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception as e:
            if logger:
                logger.warning(f"Orphan ffmpeg check failed: {e}")
        return

    # Fallback for Windows without psutil: cannot inspect command lines safely.
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

# Query keys whose values must never hit the log. HLS/CDN URLs (Fishtank
# JWTs, Chaturbate session tokens, signed CloudFront URLs, …) carry these
# on the argv we otherwise print in full.
_SENSITIVE_QUERY_KEYS = {
    'jwt', 'tkn', 'token', 'access_token', 'refresh_token',
    'auth', 'authorization', 'sig', 'signature', 'key',
    'api_key', 'apikey', 'password', 'passwd', 'pwd', 'secret',
    'session', 'sessionid', 'playback_token', 'live_stream_token',
    'hash',
}
_SENSITIVE_QUERY_SUBSTR = ('token', 'jwt', 'secret', 'passwd', 'password', 'signature')
_SENSITIVE_HEADER_RE = re.compile(
    r'(?i)(\b(?:Authorization|Cookie|Set-Cookie|X-Api-Key)\s*:\s*)(.*?)(?=\r\n|$)',
)
_QUERY_PARAM_RE = re.compile(r'([?&])([^=&\s]+)=([^&\s]*)')


def redact_for_log(text):
    """Strip secret-bearing query values and header fields from a log string.

    Used before logging subprocess argv or resolved stream URLs so a 24h
    Fishtank JWT (or a Cookie/Authorization header ffmpeg was given) does
    not land in stream_recorder.log.
    """
    if not text:
        return text
    text = str(text)

    def _param(m):
        sep, key, _val = m.group(1), m.group(2), m.group(3)
        kl = key.lower()
        if kl in _SENSITIVE_QUERY_KEYS or any(p in kl for p in _SENSITIVE_QUERY_SUBSTR):
            return f"{sep}{key}=***"
        return m.group(0)

    text = _QUERY_PARAM_RE.sub(_param, text)
    text = _SENSITIVE_HEADER_RE.sub(r'\1***', text)
    return text


def redact_cmd_for_log(cmd):
    """Join a subprocess argv for logging, with secrets stripped."""
    return ' '.join(redact_for_log(a) for a in cmd)


class RedactLogFilter(logging.Filter):
    """Redact secrets in every log record, including tracebacks and %(channel)s.

    Argv logging already calls ``redact_for_log``. ffmpeg/yt-dlp stderr,
    exception text, and the worker's channel tag (custom URLs with ``?jwt=``)
    used to bypass that. Attaching this filter to the root logger covers
    those paths without finding every ``logger.info``.
    """

    def filter(self, record):
        try:
            record.msg = redact_for_log(record.getMessage())
            record.args = ()
        except Exception:
            if isinstance(getattr(record, "msg", None), str):
                record.msg = redact_for_log(record.msg)
        channel = getattr(record, "channel", None)
        if channel is not None:
            record.channel = redact_for_log(str(channel))
        if record.exc_info and record.exc_info[0] is not None:
            record.exc_text = redact_for_log(
                "".join(traceback.format_exception(*record.exc_info))
            )
        elif record.exc_text:
            record.exc_text = redact_for_log(record.exc_text)
        return True


def _ensure_redact_filter(logger):
    if not any(isinstance(f, RedactLogFilter) for f in logger.filters):
        logger.addFilter(RedactLogFilter())


def rotate_log_if_needed(log_file, max_bytes, backup_count):
    """Roll stream_recorder.log over at startup if it has grown too large.

    Deliberately NOT logging.handlers.RotatingFileHandler: every recording
    worker is a separate process that opens this same file, and mid-run
    rollover requires renaming a file that those other processes still hold
    open — which fails outright on Windows.  Rotating once at startup, before
    any worker exists, sidesteps the problem entirely.  The trade-off is that
    the active log can exceed max_bytes during a single long session; it gets
    trimmed at the next launch.

    Keeps stream_recorder.log.1 … .N, oldest discarded.
    """
    try:
        if max_bytes <= 0 or not os.path.isfile(log_file):
            return
        if os.path.getsize(log_file) < max_bytes:
            return

        oldest = f"{log_file}.{backup_count}"
        if os.path.exists(oldest):
            os.remove(oldest)
        for i in range(backup_count - 1, 0, -1):
            src, dst = f"{log_file}.{i}", f"{log_file}.{i + 1}"
            if os.path.exists(src):
                os.replace(src, dst)
        if backup_count > 0:
            os.replace(log_file, f"{log_file}.1")
        else:
            os.remove(log_file)
    except Exception:
        pass  # logging must never be the thing that stops the app starting


def setup_logging(root_path, config=None):
    """Setup logging for main process."""
    os.makedirs(root_path, exist_ok=True)
    log_file = os.path.join(root_path, "stream_recorder.log")

    # Rotate before the handler opens the file, and before any worker
    # process is spawned (see rotate_log_if_needed for why that matters).
    max_mb = 20.0
    backups = 3
    if config is not None:
        try:
            max_mb = config.getfloat('Cleanup', 'max_log_size_mb', fallback=20.0)
            backups = config.getint('Cleanup', 'log_backup_count', fallback=3)
        except Exception:
            pass
    rotate_log_if_needed(log_file, int(max_mb * 1024 * 1024), backups)

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
    _ensure_redact_filter(logging.root)


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
    _ensure_redact_filter(logging.root)

    return logging.LoggerAdapter(
        logging.getLogger(), {'channel': redact_for_log(channel_key)}
    )

