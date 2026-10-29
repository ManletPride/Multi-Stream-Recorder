r"""
Multi-Stream Recorder
=====================

A desktop application for simultaneously recording live streams from
Kick, Twitch, YouTube, Rumble, TikTok, Fishtank.live, and any site
supported by yt-dlp.

Records live streams using:
  • streamlink for Kick and Twitch
  • yt-dlp for YouTube, Rumble, TikTok, and custom URLs
  • ffmpeg for HLS (Fishtank, Rumble channel-page playlists, Chaturbate CMAF)
    and for remuxing raw .ts recordings to .mp4

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

Implementation lives in the ``msr`` package (gui, worker, platforms).
This file is the user-facing entry point so ``python Multi-Stream-Recorder.py``
keeps working; recording workers import ``msr.worker.record_worker`` directly
so Windows spawn does not re-run the GUI.
"""

import msr.nowindow  # noqa: F401  (must come first: hides child console windows)
from msr import startup_error as _startup_error
import argparse
import json
import logging
import multiprocessing as mp
import os
import signal
import sys
import threading

# A spawned recording worker re-runs this file as ``__mp_main__`` before it unpickles
# ``msr.worker.record_worker``. Skip the GUI-side imports there: they loaded tkinter, the
# whole GUI and the tray/notification libraries into every worker (about 1.3 s per start).
if __name__ != "__mp_main__":
    try:
        from msr import __version__
        from msr.config import Config
        from msr.paths import ENV_VAR as DATA_DIR_ENV, activate_data_dir, resolve_data_dir
        from msr import deps as _deps
        from msr.deps import (
            DENO_VERSION, HAS_CURL_CFFI, HAS_DENO,
            HAS_NOTIFICATIONS, HAS_PSUTIL, HAS_STREAMLINK, HAS_TRAY, HAS_YTDLP,
            STREAMLINK_VERSION, YTDLP_VERSION,
        )
        from msr.gui import main_gui
        from msr.recorder import StreamRecorder, finish_headless_session, purge_old_pending_files
        from msr.util import (
            activate_existing_instance,
            coerce_channel_records,
            kill_orphan_recording_processes,
            redact_for_log,
            setup_logging,
            try_acquire_single_instance,
            validate_startup,
        )
    except Exception as _import_error:  # no console under pythonw: leave a trace instead of vanishing
        _startup_error.report_crash(_import_error, headless='--headless' in sys.argv)
        raise


def _start_control(app):
    """The local control channel + msr_status.json (msr/control.py). Never stops a start."""
    try:
        from msr import control
        return control.Service.start_for(app)
    except Exception as e:
        logging.warning(f"Control: not available ({type(e).__name__}: {e})")
        return None


def main_headless(config):
    """Run recording without a GUI.  Suitable for background tasks or services.

    Recording of the enabled channels starts at once. While it runs, msr_ctl.py (or any client
    of msr/control.py) can stop/start the session, edit the roster and settings, and quit.
    Ctrl+C performs a clean shutdown.
    """
    from msr.control import HeadlessApp
    channels_file = config.get('Paths', 'channels_file')
    channels = []
    try:
        with open(channels_file, "r") as f:
            loaded = json.load(f)
        records, skipped = coerce_channel_records(loaded)
        for raw in skipped:
            logging.warning(
                "Ignoring unsafe or unknown channel in roster: %s",
                redact_for_log(raw),
            )
        channels = [r["name"] for r in records if r.get("enabled", True)]
    except FileNotFoundError:
        logging.warning(f"Channels file not found: {channels_file} - starting with an empty list "
                        "(add channels with msr_ctl.py add <name>)")
    except Exception as e:
        logging.error(f"Error loading channels: {e}")
        sys.exit(1)

    app = HeadlessApp(config, lambda keys: StreamRecorder(keys, config))
    service = _start_control(app)

    if not channels and service is None:
        logging.error("No channels configured.  Add channels to channels.json first.")
        sys.exit(1)

    print("\n" + "=" * 80)
    print(f"Multi-Stream Recorder v{__version__} - HEADLESS MODE")
    if channels:
        print("Starting recording session...")
    else:
        print("No channel is enabled yet - waiting for commands (msr_ctl.py).")
    for ch in channels:
        print(f"   {ch}")
    if service is not None:
        print("Control: msr_ctl.py status | stop | start | quit   (see README, Agent control)")
    print("Press Ctrl+C to stop.")
    print("=" * 80 + "\n")

    shutdown_requested = threading.Event()

    def signal_handler(signum, frame):
        # Only ask the main thread to unwind: the stop and the cleanup that follows it run
        # below, in order, instead of inside a signal handler followed by an instant exit.
        if not shutdown_requested.is_set():
            shutdown_requested.set()
            print("\nShutdown requested - stopping all recordings...")
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        app.run(start_now=bool(channels))
    except KeyboardInterrupt:
        shutdown_requested.set()
    if service is not None:
        service.stop()
    if app.last_recorder is not None:
        finish_headless_session(app.last_recorder)


def _safe_console():
    """A redirected stdout/stderr on Windows is cp1252: printing the arrows/bullets below
    raised UnicodeEncodeError and killed the launch. Replace what the console can't show."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except Exception:  # pythonw (no stream) or a wrapper without reconfigure
            pass


def main():
    """Parse arguments and start the recorder."""
    _safe_console()
    parser = argparse.ArgumentParser(
        description=f"Multi-Stream Recorder v{__version__}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s                     Launch GUI (default)
  %(prog)s --headless          Run without GUI (Ctrl+C to stop)
  %(prog)s --config my.ini     Use a custom config file
  %(prog)s --json              Print a read-only status snapshot as JSON and exit
  %(prog)s --json --library    The same plus sizes per site/channel/month (lists folders, slower)
  %(prog)s --data-dir D:\\msr-data   Code here; config, channels, cookies from D:\\msr-data
        """,
    )
    parser.add_argument('--headless', action='store_true',
                        help='Run without GUI (background mode)')
    parser.add_argument('--config', default='config.ini',
                        help='Path to config file (default: config.ini, relative to the data dir)')
    parser.add_argument('--data-dir', default=None,
                        help=f'Folder holding config.ini, channels.json, window_state.json, '
                             f'kick_channel_ids.json (default: ${DATA_DIR_ENV} or the current directory)')
    parser.add_argument('--json', action='store_true',
                        help='Print a read-only status snapshot (settings, roster, tools, problems) '
                             'as JSON and exit. Starts nothing; no secrets or personal paths')
    parser.add_argument('--library', action='store_true',
                        help='With --json: also scan the recordings folders (read-only, sizes only, '
                             'nothing is written) and add a "library" section')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')

    args = parser.parse_args()
    if args.library and not args.json:
        parser.error('--library only works together with --json')

    if os.name == 'nt':
        mp.set_start_method('spawn', force=True)

    data_dir = resolve_data_dir(args.data_dir)
    if data_dir:
        try:
            activate_data_dir(data_dir)
        except FileNotFoundError as e:
            print(f"ERROR: {e}")
            sys.exit(2)

    if args.json:
        from msr.status import build_status, render
        print(render(build_status(os.path.abspath(args.config), library=args.library)))
        return

    config = Config(os.path.abspath(args.config))

    setup_logging(config.get('Paths', 'streams_dir'), config)

    _deps.configure_ffmpeg(config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg'))

    logging.info(f"Multi-Stream Recorder v{__version__} starting...")
    logging.info(f"yt-dlp available: {HAS_YTDLP} (version: {YTDLP_VERSION})")
    logging.info(f"streamlink available: {HAS_STREAMLINK} (version: {STREAMLINK_VERSION})")
    logging.info(f"ffmpeg available: {_deps.HAS_FFMPEG} (version: {_deps.FFMPEG_VERSION})")
    logging.info(f"psutil available: {HAS_PSUTIL}")
    logging.info(f"curl_cffi available: {HAS_CURL_CFFI} (browser impersonation)")
    logging.info(f"deno available: {HAS_DENO} (version: {DENO_VERSION}) (YouTube n-challenge solving)")
    logging.info(f"System tray available: {HAS_TRAY}")
    logging.info(f"Notifications available: {HAS_NOTIFICATIONS}")
    logging.info(f"Streams directory: {config.get('Paths', 'streams_dir')}")
    logging.info(f"Data directory: {os.getcwd()} (config: {config.config_file})")

    streams_dir = config.get('Paths', 'streams_dir')
    if not try_acquire_single_instance(streams_dir, logging.getLogger()):
        logging.warning(
            "Another Multi-Stream Recorder is already running for this "
            "recordings folder — exiting"
        )
        if args.headless:
            print(
                "Multi-Stream Recorder is already running for this "
                "recordings folder. Stop that copy first."
            )
            sys.exit(1)
        activated = activate_existing_instance()
        if not activated:
            try:
                import tkinter as tk
                from tkinter import messagebox
                _root = tk.Tk()
                _root.withdraw()
                messagebox.showinfo(
                    "Multi-Stream Recorder",
                    "Multi-Stream Recorder is already running for this "
                    "recordings folder.\n\n"
                    "If you do not see the window, check the system tray "
                    "or a --headless copy.",
                    parent=_root,
                )
                _root.destroy()
            except Exception:
                print(
                    "Multi-Stream Recorder is already running for this "
                    "recordings folder."
                )
        sys.exit(0)

    # Ghost workers from a crashed previous copy treat ffmpeg SIGTERM as a
    # reconnect. Reap them before this instance records anything.
    # a leftover worker may be recording into the fallback folder (main drive was gone)
    from msr.storage import fallback_root as _fallback_root
    kill_orphan_recording_processes(
        logging.getLogger(),
        streams_dir=[streams_dir, _fallback_root(config.get('Paths', 'fallback_dir', fallback=''), streams_dir)])

    errors, warnings = validate_startup(config)

    for w in warnings:
        logging.warning(f"STARTUP WARNING: {w.splitlines()[0]}")

    if errors:
        for e in errors:
            logging.error(f"STARTUP ERROR: {e.splitlines()[0]}")

        detail = "\n\n".join(errors)
        _startup_error.report_failure(
            "Multi-Stream Recorder - Cannot start",
            f"The following critical issues were found:\n\n{detail}\n\n"
            "The program cannot start until these are resolved.",
            headless=args.headless,
        )
        sys.exit(1)

    if warnings:
        for w in warnings:
            first_line = w.splitlines()[0]
            print(f"  ⚠ {first_line}")

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
    try:
        main()
    except (SystemExit, KeyboardInterrupt):
        raise
    except BaseException as _error:
        _startup_error.report_crash(_error, headless='--headless' in sys.argv)
        raise
