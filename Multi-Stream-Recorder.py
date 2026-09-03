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

import argparse
import json
import logging
import multiprocessing as mp
import os
import signal
import sys
import threading

from msr import __version__
from msr.config import Config
from msr.deps import (
    DENO_VERSION, FFMPEG_VERSION, HAS_CURL_CFFI, HAS_DENO, HAS_FFMPEG,
    HAS_NOTIFICATIONS, HAS_PSUTIL, HAS_STREAMLINK, HAS_TRAY, HAS_YTDLP,
    STREAMLINK_VERSION, YTDLP_VERSION,
)
from msr.gui import main_gui
from msr.recorder import StreamRecorder, purge_old_pending_files
from msr.util import coerce_channel_records, redact_for_log, setup_logging, validate_startup


def main_headless(config):
    """Run recording without a GUI.  Suitable for background tasks or services.

    Ctrl+C performs a clean shutdown.
    """
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

    try:
        recorder.run()
    except KeyboardInterrupt:
        if not shutdown_requested.is_set():
            shutdown_requested.set()
            print("\nKeyboardInterrupt — stopping all recordings...")
            recorder.stop()


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

    setup_logging(config.get('Paths', 'streams_dir'), config)

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

    errors, warnings = validate_startup(config)

    for w in warnings:
        logging.warning(f"STARTUP WARNING: {w.splitlines()[0]}")

    if errors:
        for e in errors:
            logging.error(f"STARTUP ERROR: {e.splitlines()[0]}")

        if not args.headless:
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
    main()
