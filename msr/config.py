"""config.ini defaults and accessors."""
import logging
import os
import configparser


def default_streams_dir():
    """Recordings folder for a fresh config.ini (not ``E:\\Streams``).

    Windows: ``%USERPROFILE%\\Videos\\Multi-Stream Recorder``
    POSIX: ``~/Videos/Multi-Stream Recorder``
    Existing config.ini values are left alone.
    """
    if os.name == "nt":
        base = os.environ.get("USERPROFILE") or os.path.expanduser("~")
        videos = os.path.join(base, "Videos")
    else:
        videos = os.path.join(os.path.expanduser("~"), "Videos")
    return os.path.join(videos, "Multi-Stream Recorder")


def parser_getfloat(parser, section, key, default):
    """float from a ConfigParser; invalid/missing → *default* (never raises)."""
    try:
        return parser.getfloat(section, key, fallback=default)
    except (ValueError, TypeError, configparser.Error):
        return default


def parser_getint(parser, section, key, default):
    try:
        return parser.getint(section, key, fallback=default)
    except (ValueError, TypeError, configparser.Error):
        return default


def parser_getboolean(parser, section, key, default):
    try:
        return parser.getboolean(section, key, fallback=default)
    except (ValueError, TypeError, configparser.Error):
        return default


# (section, key, kind, default, min_v, max_v)
# kind is 'float', 'int', or 'bool'. min_v/max_v None = no clamp.
# Values outside the range are reset to *default* (not silently clamped),
# except where 0 is a documented "off" (max_record_hours, max_file_size_gb,
# min_disk_space_gb, auto_purge_days, max_log_size_mb).
def _coerce_specs():
    return (
        ("Recording", "max_record_hours", "float", 12.0, 0.0, None),
        ("Recording", "max_file_size_gb", "float", 8.0, 0.0, None),
        ("Recording", "min_disk_space_gb", "float", 5.0, 0.0, None),
        ("Recording", "min_file_size_mb", "float", 2.0, 0.0, None),
        ("Timeouts", "stream_check_timeout", "int", 30, 1, None),
        ("Timeouts", "ffmpeg_timeout", "int", 600, 1, None),
        ("Timeouts", "poll_interval_minutes", "float", 3.0, 0.5, 120.0),
        ("Timeouts", "poll_jitter_percent", "int", 20, 0, 100),
        ("Timeouts", "error_backoff_max_minutes", "float", 15.0, 0.0, None),
        ("Timeouts", "reconnect_grace_minutes", "int", 3, 0, None),
        ("Timeouts", "file_creation_timeout", "int", 60, 1, None),
        ("Cleanup", "auto_purge_days", "int", 7, 0, None),
        ("Cleanup", "max_log_size_mb", "float", 20.0, 0.0, None),
        ("Cleanup", "log_backup_count", "int", 3, 0, None),
        ("Advanced", "concurrent_fragments", "int", 3, 1, None),
        ("Clipping", "clip_length_seconds", "int", 30, 5, 1800),
        ("Clipping", "screenshot_quality", "int", 2, 0, 100),
        ("Recording", "split_on_resolution_change", "bool", True, None, None),
        ("Cleanup", "purge_on_startup", "bool", True, None, None),
        ("Advanced", "verbose", "bool", False, None, None),
        ("Advanced", "streamlink_debug", "bool", False, None, None),
        ("GUI", "dark_mode", "bool", True, None, None),
        ("GUI", "minimize_to_tray", "bool", True, None, None),
        ("GUI", "notifications", "bool", True, None, None),
    )


class Config:
    """Manages application configuration from config.ini"""

    DEFAULT_CONFIG = {
        'Paths': {
            'streams_dir': '',  # filled with default_streams_dir() at load
            'channels_file': 'channels.json',
            'cookies_file': '',  # auto-detected if empty
        },
        'Recording': {
            'quality': 'best',
            'max_record_hours': '12.0',
            'max_file_size_gb': '8.0',   # split when file exceeds this size (0 = off); next capture
                                         # starts first (overlap), then the closed file remuxes in
                                         # the background. Resolution-change splits still stop first.
            'split_on_resolution_change': 'true',  # end + start a fresh segment if the live video's
                                                    # resolution changes mid-stream (e.g. TikTok multi-
                                                    # guest battles), instead of muxing two resolutions
                                                    # into one file, which corrupts playback
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
            # stream_recorder.log is rotated at startup once it passes this
            # size (0 = never rotate).  Rotation happens at launch rather than
            # continuously because recording workers share the file — see
            # rotate_log_if_needed().
            'max_log_size_mb': '20',
            'log_backup_count': '3',
        },
        'Advanced': {
            # Passes --verbose to yt-dlp/streamlink AND disables the stderr
            # noise filter, so a single YouTube recording can emit ~17 MiB of
            # log an hour (mostly ffmpeg's per-segment HLS cuepoint chatter).
            # Default off — turn on only when diagnosing a specific failure.
            'verbose': 'false',
            'streamlink_debug': 'false',
            'ffmpeg_path': 'ffmpeg',
            'concurrent_fragments': '3',
            # yt-dlp --extractor-args youtube:player_client value.
            # Blank = let yt-dlp choose (recommended).  Its defaults favour
            # clients that don't require a PO Token; pinning the wrong one
            # here causes "No video formats found!" on otherwise-live
            # streams.  Only set this to work around a YouTube regression.
            'youtube_player_client': '',
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
        'Clipping': {
            # Length of the instant clip cut from the live .ts, in seconds.
            # Remembered across restarts; changeable live from the GUI's
            # Clip Length selector.
            'clip_length_seconds': '30',
            # Where clips/screenshots are written. Blank = streams_dir/Clips.
            'clips_dir': '',
            # Screenshot output format: jpg (default), png, or webp.
            # jpg is ~5x smaller than png at visually indistinguishable
            # quality for a 1080p frame; png is lossless but ~2 MB a shot.
            'screenshot_format': 'jpg',
            # Quality knob — meaning depends on the format above:
            #   jpg  : 2–31,  LOWER is better (2 = near-lossless)
            #   webp : 0–100, HIGHER is better (80–90 is a good range)
            #   png  : ignored (always lossless)
            'screenshot_quality': '2',
        },
    }

    def __init__(self, config_file='config.ini'):
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.coerce_warnings = []
        self._load_or_create()

    def _write(self):
        with open(self.config_file, 'w', encoding='utf-8') as f:
            self.config.write(f)

    def _load_or_create(self):
        defaults = self.DEFAULT_CONFIG
        if os.path.exists(self.config_file):
            self.config.read(self.config_file, encoding='utf-8')
            updated = False
            for section, options in defaults.items():
                if not self.config.has_section(section):
                    self.config.add_section(section)
                    updated = True
                for key, value in options.items():
                    if not self.config.has_option(section, key):
                        if section == 'Paths' and key == 'streams_dir':
                            value = default_streams_dir()
                        self.config.set(section, key, value)
                        updated = True
            if updated:
                self._write()
        else:
            for section, options in defaults.items():
                self.config.add_section(section)
                for key, value in options.items():
                    if section == 'Paths' and key == 'streams_dir':
                        value = default_streams_dir()
                    self.config.set(section, key, value)
            self._write()
            logging.info(f"Created default config file: {self.config_file}")

        self.coerce_warnings = self._coerce_values()

    def _coerce_values(self):
        """Replace unparseable or out-of-range values with defaults. Persist if needed."""
        warnings = []
        changed = False

        streams = (self.get('Paths', 'streams_dir', fallback='') or '').strip()
        if not streams:
            path = default_streams_dir()
            self.config.set('Paths', 'streams_dir', path)
            warnings.append(f"streams_dir was empty — using {path}")
            changed = True

        shot = (self.get('Clipping', 'screenshot_format', fallback='jpg') or 'jpg').strip().lower()
        if shot not in ('jpg', 'jpeg', 'png', 'webp'):
            self.config.set('Clipping', 'screenshot_format', 'jpg')
            warnings.append(
                f"screenshot_format '{shot}' is not jpg/png/webp — using jpg"
            )
            changed = True

        ffmpeg_path = (self.get('Advanced', 'ffmpeg_path', fallback='') or '').strip()
        if not ffmpeg_path:
            self.config.set('Advanced', 'ffmpeg_path', 'ffmpeg')
            warnings.append("ffmpeg_path was empty — using ffmpeg")
            changed = True

        pattern = (self.get('Recording', 'filename_pattern', fallback='') or '').strip()
        if not pattern:
            self.config.set('Recording', 'filename_pattern', '{username}_{timestamp}')
            warnings.append("filename_pattern was empty — using {username}_{timestamp}")
            changed = True

        for section, key, kind, default, min_v, max_v in _coerce_specs():
            raw = self.get(section, key, fallback=None)
            if kind == 'bool':
                try:
                    self.config.getboolean(section, key)
                except (ValueError, TypeError, configparser.Error):
                    self.config.set(section, key, 'true' if default else 'false')
                    warnings.append(
                        f"{section}.{key}={raw!r} is not a boolean — using {default}"
                    )
                    changed = True
                continue
            try:
                if kind == 'float':
                    val = self.config.getfloat(section, key)
                else:
                    val = self.config.getint(section, key)
            except (ValueError, TypeError, configparser.Error):
                self.config.set(section, key, str(default))
                warnings.append(
                    f"{section}.{key}={raw!r} is not a valid {kind} — using {default}"
                )
                changed = True
                continue
            if min_v is not None and val < min_v:
                self.config.set(section, key, str(default))
                warnings.append(
                    f"{section}.{key}={val} is below {min_v} — using {default}"
                )
                changed = True
            elif max_v is not None and val > max_v:
                self.config.set(section, key, str(default))
                warnings.append(
                    f"{section}.{key}={val} is above {max_v} — using {default}"
                )
                changed = True

        if changed:
            try:
                self._write()
            except OSError as e:
                logging.warning(f"Could not write coerced config.ini: {e}")
        return warnings

    # Convenience accessors
    def get(self, section, key, fallback=None):
        return self.config.get(section, key, fallback=fallback)

    def getfloat(self, section, key, fallback=None):
        return parser_getfloat(self.config, section, key, fallback)

    def getint(self, section, key, fallback=None):
        return parser_getint(self.config, section, key, fallback)

    def getboolean(self, section, key, fallback=None):
        return parser_getboolean(self.config, section, key, fallback)
