# Multi-Stream Recorder v3.3.0

A major architecture, security, and performance release: offline channels are now watched by a lightweight in-process poller instead of one idle Python process per channel, secrets and remote control are hardened across Windows and Linux, and disk, CPU, and UI responsiveness are noticeably smoother.

## Architecture & Performance

- **On-demand worker processes (`msr/poller.py`).** Previously, watching 20 offline channels meant 20 separate idle Python worker processes. A single lightweight in-process polling coordinator now watches offline channels and spawns a recording worker only when a channel goes live (handing it back to the coordinator once the stream ends).
  - Startup checks are staggered so large rosters don't spike CPU or network all at once.
  - **Twitch:** When signed in via **Options > Twitch**, offline channels are checked in batches using Twitch's Helix API (`GET /helix/streams`, up to 100 logins per request).
  - **Kick:** Offline channels are probed via a lightweight HTTP API check instead of launching `streamlink` on every poll.
  - **Fishtank:** Cameras share a single authenticated session while polling.
- **Global remux concurrency cap.** Simultaneous `.ts` → `.mp4` remuxes across all workers and background cleanup are capped at 2 at a time, preventing disk-I/O thrashing when several streams finish together.
- **Single-process pinned live previews & private cache.** Double-clicking a live preview tile to pin the 960×540 view now runs a single 960×540 `ffmpeg` grab and downsamples it in-process with Pillow for the 160×90 strip tile instead of spawning a second `ffmpeg` process. Preview thumbnails are stored in a private XDG cache directory (`$XDG_CACHE_HOME/multi-stream-recorder/previews`, mode `0700`) and cleaned up when MSR quits.
- **Smoother Clip Editor playback.** Live volume slider adjustment (`apply_gain`) now uses a cached 65,536-entry signed-int16 lookup table instead of scaling samples in a Python loop, and `VideoFeed` reuses a single pre-allocated frame buffer per raw video frame.
- **Faster TikTok offline checks.** `cookies.txt` parsing is cached by `(mtime_ns, size)`, confirmed offline rooms (`status == 4`) short-circuit immediately without redundant fallback requests, and webcast fallback probes are rate-limited to a 5-minute cooldown per channel.
- **Lower idle overhead across the app.** `msr_status.json` is written immediately on changes and on a 60-second heartbeat when idle instead of rewriting unchanged JSON every few seconds; pending-remux folder scans (`Recorded/`) use directory-mtime caching; the Logs tab tracks filtered line counts in O(1); and the Telegram upload worker waits on an event when idle instead of polling in a loop.
- **Modular GUI codebase.** `main_gui()` in `msr/gui.py` has been decomposed into dedicated panel modules (`msr/roster_panel.py`, `msr/status_panel.py`, `msr/preview_strip.py`, `msr/bottom_bar.py`), cutting `msr/gui.py` by more than half and eliminating forward-reference `NameError` guards.

## Security

- **Context-bound encryption (v2).** Encrypted secrets (`config.ini` `[Fishtank]` / `[Telegram]`, `twitch_token.json`, `telegram_session.json`) now bind each ciphertext to its config key or file purpose via AES-GCM AAD on non-Windows and DPAPI optional entropy on Windows, preventing ciphertext swapping across fields while remaining fully compatible with existing v1 blobs.
- **Least-privilege worker credentials & environment scrubbing.** `[Fishtank]` credentials are only handed to Fishtank workers, `[Telegram]` credentials stay in the main process, and `MSR_FISHTANK_PASSWORD` is stripped from child tool environments (`ffmpeg`, `ffprobe`, `streamlink`, `yt-dlp`, `pip`, `git`, and non-Fishtank workers).
- **Remote control host-path & binary guards.** `msr_ctl.py` / `ControlServer` now refuses (`forbidden`) remote `config set` modifications to host paths and executable binaries (`Advanced.ffmpeg_path`, `Clipping.clips_dir`, `Paths.channels_file`, `Paths.fallback_dir`, `GUI.window_state_file`). Change those locally in Options or `config.ini`.
- **Safer backups & POSIX temp paths.** `config.ini.broken-*` backups strip secrets just like `msr_history/`; POSIX fallback storage and preview caches use private `0700` XDG cache directories instead of world-shared `/tmp`; the Twitch token lock file lives in `data_dir/.twitch_token.lock`; and custom-URL credentials are redacted in status and events.
- **Multi-user Linux caveats documented.** `SECURITY.md` now documents `/proc/<pid>/cmdline` visibility on shared Linux hosts without `hidepid` and the tradeoffs of `secret.key` vs. a desktop OS keyring.

## UX & Cross-Platform

- **Missing-`ffmpeg` GUI banner.** Starting the GUI without `ffmpeg` available now displays a clear warning banner with a button to open Options instead of failing later when a recording starts.
- **Tray, quit confirmation & layout polish.** Added `[GUI] minimize_to_tray` and a confirmation prompt when quitting while recordings are active. The `[Telegram]` section in Options starts collapsed by default, `[Sounds]` is clearly marked Windows-only, and the minimum window size is now 1060×620 so controls never clip.
- **URL auto-detection in Add Channel.** Pasting a YouTube, TikTok, or Rumble channel URL automatically maps it to the native platform key instead of falling back to `custom:`.
- **Launcher & `.venv` resilience.** `run_msr.bat` falls back across `py`, `python`, and `python3` on Windows. On Linux, first-run `.venv` setup treats `PyGObject` as best-effort so missing native GTK/cairo dev headers never block installation or startup.
- **In-app updater housekeeping.** Updating MSR now also updates `run_msr.pyw`, removes retired source files cleanly, and deletes temporary staging directories.

## Fixed

- **Twitch token refresh deadlock.** Fixed a non-reentrant lock acquisition in `msr/twitch_eventsub.py` that could hang when refreshing an expired Twitch token while holding `token_lock`.
- **TikTok stream resolution.** Separated TikTok live-check probing from recording stream-URL resolution so recordings don't fail on stale probe URLs.

## Upgrade notes

- **Drop-in upgrade:** Your existing `config.ini`, `channels.json`, `cookies.txt`, `window_state.json`, Twitch sign-in, Telegram configuration, and recordings folders are fully compatible — no manual migration is required.
- **Legacy v1 encrypted secrets** continue to decrypt seamlessly and are automatically upgraded to v2 context-bound encryption the next time they are saved.
- **Remote automation (`msr_ctl.py`):** If any external script sets `Advanced.ffmpeg_path`, `Clipping.clips_dir`, `Paths.channels_file`, `Paths.fallback_dir`, or `GUI.window_state_file` remotely, those calls now return `forbidden` by design and should be configured locally.
- **In-app update:** Standard installs can update from the in-app banner or **Updates** dialog; git checkouts update with `git pull --ff-only` and restart.

Full list of changes: [CHANGELOG.md](https://github.com/ManletPride/Multi-Stream-Recorder/blob/v3.3.0/CHANGELOG.md).
