# Multi-Stream Recorder v3.3.0

Offline channels are watched by one in-process poller instead of one idle Python process each. Secrets stay bound to the field they belong to. Quitting while a stream is recording asks you to confirm.

## Architecture and performance

- **On-demand recording workers (`msr/poller.py`).** A `PollCoordinator` checks offline channels in-process and starts a capture worker only when a channel goes live. When the stream ends and the reconnect grace period expires, the worker exits and the channel goes back to the poller. Startup checks are staggered.
  - **Twitch:** with **Options > Twitch** signed in, offline channels are checked in batches (`GET /helix/streams`, up to 100 logins). Otherwise MSR falls back to streamlink.
  - **Kick:** a lightweight HTTP check, with streamlink only if that check is blocked or curl_cffi is missing.
  - **Fishtank:** cameras share one login while polling.
  - **Push fallback:** when Kick or Twitch push is connected, the offline safety-net poll waits at least 10 minutes (`PUSH_POLL_FLOOR_S = 600`).
- **Remuxes are capped at 2 at a time** across workers and background cleanup, so several streams ending together do not thrash the disk.
- **Pinned previews use one ffmpeg grab.** Double-clicking a tile grabs one 960×540 frame and, when Pillow is installed, scales it in-process for the 160×90 tile. Preview PNGs go to `%TEMP%\msr-previews` on Windows, and on Linux and macOS to `$XDG_CACHE_HOME/multi-stream-recorder/previews` (or `~/.cache/multi-stream-recorder/previews`, mode `0700`). They are deleted when MSR quits.
- **Clip Editor.** The volume path uses a cached 65,536-entry sample table, and the preview reuses one frame buffer.
- **TikTok offline checks** cache `cookies.txt` by mtime and size, stop early when `api-live/user/room` reports status 4, and wait 5 minutes between webcast fallback probes for a channel that yt-dlp already called offline.
- **Less idle work.** `msr_status.json` is rewritten when it changes, and at most once a minute when it does not. The pending-remux scan caches directory mtimes for 60 seconds. The Logs tab counts filtered lines without rescanning the whole view. The Telegram uploader waits on an event when the queue is empty.
- **The window code is split** into `msr/roster_panel.py`, `msr/status_panel.py`, `msr/preview_strip.py`, and `msr/bottom_bar.py`. `msr/gui.py` is under half the size it was in 3.2.0.

## Security

- **Context-bound encryption (v2).** The cipher on Linux and macOS is the same as 3.2.0: an HMAC-SHA256 keystream and an HMAC-SHA256 MAC (`msr/filecrypt.py`). v2 folds the config key or file purpose into that MAC, so a ciphertext cannot be moved into another field. Windows uses DPAPI optional entropy for the same binding (`v2:` prefix in `msr/dpapi.py`). A v1 blob still decrypts. It is rewritten as v2 the next time that secret is saved.
- **Workers see fewer secrets.** Fishtank credentials go only to Fishtank workers. Telegram credentials stay in the main process. `MSR_FISHTANK_PASSWORD` is removed from the environment of ffmpeg, ffprobe, streamlink, yt-dlp, pip, git, and non-Fishtank workers.
- **Remote `config set` refuses path and program settings** with code `forbidden`: `Advanced.ffmpeg_path`, `Clipping.clips_dir`, `Paths.channels_file`, `Paths.fallback_dir`, `GUI.window_state_file`. Change those in Options or `config.ini`.
- **Backups and temp paths.** `config.ini.broken-*` copies have secrets stripped, the same as `msr_history/`. On Linux and macOS the default fallback folder and the preview cache are private `0700` directories under the XDG cache, not `/tmp`. The Twitch token lock file is `data_dir/.twitch_token.lock`. Custom-URL credentials are redacted in status and events.
- **`SECURITY.md`** notes `/proc/<pid>/cmdline` on a shared Linux machine, and that `secret.key` is a file in the user config directory rather than an OS keyring.

## Window and launchers

- **No ffmpeg:** the Status tab shows a banner with **Open Options**.
- **Quit while recording** asks you to confirm. `[GUI] minimize_to_tray` is the same setting as in 3.2.0.
- **Options.** Telegram setup stays collapsed until Telegram is enabled. Sounds say they are Windows-only.
- **Minimum window** is 1060×620, down from 1060×720, so the window fits a 1080p laptop at 150% scaling.
- **Add channel** treats a YouTube, TikTok, or Rumble channel URL as that platform (including `youtu.be` and `rumble.com/user/...` in the window).
- **`run_msr.bat`** tries `pyw -3`, then `pythonw`, `py -3`, and `python`.
- **Linux `.venv` setup** installs PyGObject in a second step. Missing GTK headers skip the tray and do not fail the rest of the install.
- **In-app update.** A window install restarts through `run_msr.pyw`. The helper deletes program files the new release no longer ships, and removes its staging folder.

## Fixed

- **Twitch token refresh no longer hangs on its own lock.** The thread lock was already re-entrant. The lock file was not: a refresh that already held `token_lock` took it again, and Windows file locks do not nest. The same thread now skips the second file lock.
- **A resolved recording URL is used only when it is http or https**, so a non-http result from yt-dlp is not passed to the recorder.

## Upgrade notes

- Existing `config.ini`, `channels.json`, cookies, window state, Twitch sign-in, Telegram setup, and recordings stay as they are.
- A secret saved by 3.2.0 still opens. It becomes a v2 blob the next time that secret is written.
- A remote `config set` of the path settings listed above now returns `forbidden`.
- Install from the in-app banner or the Updates dialog. A git checkout updates with `git pull` and a restart.

Full list of changes: [CHANGELOG.md](https://github.com/ManletPride/Multi-Stream-Recorder/blob/v3.3.0/CHANGELOG.md).
