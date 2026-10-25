# Changelog

## Unreleased

UI overhaul: themed popups, calmer Logs tab, Restore previous, window memory, Library tab, status snapshot.

- Popups and dialogs are themed (dark/light), parented to the window and centred; the native message boxes are gone. A test guards this.
- Logs tab follows new lines only while you are scrolled to the bottom; a **New lines** button appears when you are scrolled back.
- Fixed a startup crash (the window flashed and vanished under `pythonw`) caused by the Log filter box reusing the roster filter's name; a hidden-window smoke test now launches the real UI in dark and light mode.
- **Restore previous...**: the last 20 versions of `channels.json` and `config.ini` are kept in `msr_history/` (git-ignored) and can be restored from the `...` button beside About. Passwords and other secrets are masked in the list.
- Windows and dialogs remember their size and place (clamped back on screen if a monitor is gone) and can be dragged by their background.
- Log tab: filter box, level drop-down, search highlighting, coloured warnings/errors, Ctrl+F, **Open log file**.
- **Library tab**: size of your recordings per site, channel and month (headline numbers, donut and bar charts with hover details, sortable channel table with **Open folder**, **Copy summary**, optional **Count hours**). Scans in a background thread with Cancel, only lists folders and reads file sizes, never changes or deletes anything. Optional `[Library] extra_dirs` in `config.ini`. `--json --library` adds the same numbers to the status snapshot.
- `--json` prints a read-only status snapshot (no secrets, no personal paths); `llms.txt` is a manual for AI assistants.

## v2.1.0 (2026-10)

New roster panel, Kick realtime fix, reconnect fix, install cleanup.

### Roster and GUI
- Left panel redesigned: automatic **Recording / Watching / Off** groups, platform tags, search, **All / Enabled / Live** filter, **+ Add channel** dialog (paste a name, `platform:name` or URL), right-click menu, and an amber per-row cookie badge when a site needs or rejected your cookies.
- Live presence for every channel. Off channels are never recorded but still show as live in the Live filter (Kick push, plus a slow ~10 min check for other platforms). The per-row status dots from the first draft were removed.
- Dark title bars on dialogs in dark mode (shared `msr/wintheme.py`).
- Status tab live preview strip (click to select, double-click to pin a larger preview); Recording first ordering.

### Recording
- **Kick push fixed.** Kick retired the old Pusher key; MSR now negotiates the realtime transport the way kick.com does and speaks Centrifugo (Pusher kept as a fallback). No key is hardcoded. Connection failures back off up to ~10 min and are logged about once an hour.
- **Reconnect window** now starts after the remux finishes, with a 30 s follow-up phase (`reconnect_extended_minutes`, default 10) after the 15 s grace phase.
- Log rotation during a run (cross-process lock), not only at startup.
- `--data-dir` / `MSR_HOME` to keep config, cookies and channels outside the code folder.
- Child console windows are hidden under `pythonw`; `run_msr.pyw` / `run_msr.bat` launchers.

### Install
- Launchers are generic: data folder is `MSR_DATA` if set, else the repo folder; they use `.venv` automatically; per-machine overrides go in an untracked `run_msr.local.bat`.
- MSR puts its own interpreter's `Scripts` folder first on PATH, so a venv works without activation (previously yt-dlp was not found, or a different system streamlink was used).
- `yt-dlp[default]>=2025.11.12` (needs `yt-dlp-ejs` for YouTube).
- README rewritten; Fishtank camera reference moved to `docs/fishtank.md`.

### Also in this release (from the status-previews branch)
- Status tab live-frame strip for Recording/Remuxing channels: ffmpeg writes a 160×90 PNG every 4s on a worker thread. Worker status includes `raw_file` so the GUI does not `listdir` the recordings folder on the 1s tick.
- Roster **Filter** + **Enabled** only; status **Recording first**; preview / roster / status share roster order and selection. Do not nest `update()` / `update_idletasks` inside `<<TreeviewSelect>>` (that froze the GUI on Windows).
- Double-click a strip tile to pin a 480×270 preview above the others (Close, double-click the same tile, or double-click the large image to dismiss).
- **One instance per recordings folder.** A second launch focuses the existing window (or tells you a `--headless` copy is running) instead of starting another worker set. Stop / quit / relaunch also kill leftover `record_worker` processes, not only ffmpeg — killing ffmpeg alone left ghost workers that treated SIGTERM as a drop and reconnected.
- Status-bar **vX available** installs the GitHub release zipball (launcher + `msr/`, optional `requirements.txt` / `channels_fishtank.json`). Confirm → download → stop recordings → quit → helper copies files → relaunch. `config.ini`, `channels.json`, cookies, and recordings are not touched. `--headless` still only logs; it does not auto-update.
- Clip Now / Screenshot follow the live edge after a Kick HLS ad splice (PCR reset). Previews already used `-sseof`; screenshots had preferred `-ss duration-N`, and clips used lastPTS−start. Screenshots try `-sseof` first. Clips distrust elapsed when it is far behind the file’s age and cut from EOF.

## v2.0.2

User-facing summary: [RELEASE_NOTES_v2.0.2.md](RELEASE_NOTES_v2.0.2.md).

- Fishtank As Seen On mini-season (catalog 2026-09-07): stream ids are slugs (`director-as-seen-on`) rather than `name-5`. Raw hyphenated catalog ids are accepted; `fishtank:director` records the current Director cam (S5 `dirc-5` still works as a raw id). `channels_fishtank.json` is the new house roster. Grid mosaic (`grid-as-seen-on`) is treated as live when it appears only in `liveStreamStatus`.

## v2.0.1

User-facing summary: [RELEASE_NOTES_v2.0.1.md](RELEASE_NOTES_v2.0.1.md).

- Size splits are **make-before-break**: when a file hits `max_file_size_gb`, the next capture starts while the current one is still writing. The old file is closed only after the new `.ts` has data, so the seam overlaps instead of dropping live seconds. Remux of the closed file still runs in a background thread; status stays Recording with a `remuxing N%` suffix (row uses the remux color). Clip Now / Screenshot keep working. If the next file does not appear in time, it falls back to a short gap. Resolution-change splits still stop the old file first (mixed resolutions in one file corrupt playback). Stream-end remux is still on the worker. Previously an 8 GB Kick split paused capture for ~90s of remux (kimmee 2026-09-03: 21:11:51–21:13:32).
- TikTok: when yt-dlp reports offline, confirm via `/api-live/user/room`, `roomId` on the profile **and** `/live` page, then both Webcast hosts. Misses log at info (24/7 news LIVEs such as `@aljazeeraenglish` often omit `roomId` on the profile, so the old fallback stayed silent).

## v2.0.0

User-facing summary and upgrade steps: [RELEASE_NOTES_v2.0.0.md](RELEASE_NOTES_v2.0.0.md).

Code lives in the `msr` package; `Multi-Stream-Recorder.py` is the launcher only. Fully quit and relaunch after pulling these changes — a running GUI does not reload modules. This is a major version because v1.8.0 was a single file; installing v2 without `msr/` will not run.

### Clips

- **Clip Now** and **Screenshot** buttons on the Status header (same actions as right-click).
- Keyframe alignment uses **elapsed** file time (`kf_pts - format.start_time`), not MPEG-TS PCR PTS. Using `-copyts` plus output `-ss` of a ~10000s Kick PCR left audio at 0s and video at ~2s (frozen first GOP). 15s Kick clips after the fix start video at ~0.18s.
- Twitch `format.duration` can be the last PCR (e.g. 12686s) rather than elapsed file time. Clip Now then sought near EOF (powdur 15s request → **2.6s** clip; Screenshot Now wrote no frame). Duration is converted with `_elapsed_media_duration()` before the cut.
- Rumble: channel-page HLS playlist is recorded with ffmpeg (Rumble Referer) so yt-dlp never has to fetch Cloudflare-gated `/vXXXX` pages. If that playlist is missing, the first yt-dlp call uses `--impersonate chrome` instead of 403ing and retrying. Misleading “cookies expired?” log on Rumble 403 is gone.
- One in-flight clip/screenshot per channel; extra clicks are ignored until it finishes.
- **Open Clips Folder** on the status-row context menu.

Live 15s Clip Now checked on Kick, YouTube, TikTok, Twitch (after the duration fix), Rumble HLS, and Chaturbate CMAF: audio and video start together; lengths are ~15–20s. The pre-fix Twitch powdur clip was 2.6s.

### GUI / session

- Status header meters (left of Screenshot / Clip Now) show **whole-disk write** (`MB/s`) and **NIC download** (`Mbps`). Hover for this app’s recording write rate and summed stream Mbps. Requires `psutil`; the text turns yellow/red when the recordings disk stays write-busy.
- Bottom toolbar labels sit immediately left of their own dropdowns: `Clip Length: [15 sec]` then `Polling: [Normal]`. Check Now is no longer next to the clip-length combo.
- **Clips** button on the Status header (left of Screenshot) opens Explorer on that channel’s clips/screenshots folder. Same action on status-row and roster right-click.
- Custom Chaturbate (and other `site.com/user/` URLs) record into `custom/<site>/<user>/` instead of one bag per site. Direct `.m3u8` / unparseable paths keep the old `custom/<site>/` folder. TikTok custom URLs stay `custom/<handle>/`.
- Clip/screenshot filenames use the room handle only. Using the nested folder path (`chaturbate\\mode_bad`) as the filename created an extra `chaturbate` directory under the room folder.
- Channel keys are allowlisted on add and on `channels.json` load. Kick names cannot contain `:`, `/`, or `\\` (a colon used to be treated as a platform folder). Custom entries must be `http://` or `https://`.
- yt-dlp’s ffmpeg downloader gets the same network protocol whitelist as direct ffmpeg (`file` omitted) via `--downloader-args ffmpeg_i:…`.
- Log redaction applies to ffmpeg/yt-dlp stderr, exception text, and the worker channel tag, not only constructed argv. Fishtank auth failures no longer dump raw JSON.
- Fresh `config.ini` defaults `streams_dir` to `%USERPROFILE%\Videos\Multi-Stream Recorder` (Windows) or `~/Videos/Multi-Stream Recorder` (POSIX), not `E:\Streams`. Existing configs are unchanged.
- Invalid numeric/boolean `config.ini` values are reset to defaults and saved, instead of warning and then crashing the worker on `getfloat`.
- GitHub Actions runs `python -m unittest discover -s tests -t .` on Windows and Ubuntu, Python 3.10 and 3.12.
- README **Legal / acceptable use** plus [SECURITY.md](SECURITY.md): personal archival, site ToS, cookies are your login, Chaturbate is adult.
- Status-header MSR write rate uses worker `size_bytes` instead of `listdir` on the GUI tick. **Stop Recording** and per-channel stop join workers off the UI thread.
- About lists all platforms, Deno, recordings vs clips folders, and that those paths are changed in `config.ini`.
- Roster **Top** button (next to ▲ ▼) and right-click **Move to Top** / **Move to Bottom** jump selected channels as a block so a new row does not need dozens of arrow clicks.
- Status table updates rows in place (no full rebuild every 2.5s).
- Status selection is the hidden `_key` channel id, not the display name.
- “Recording Complete” notifications fire once per recording, not every refresh.
- Removing a channel from the roster stops its worker (`StreamRecorder.remove_channel`).
- **Open in Browser** works for Rumble, TikTok, and Fishtank (previously all opened as Kick). YouTube handles stored with `@` no longer become `@@handle`.
- Rumble channels use `rumble.com` cookies in the roster indicator, not Kick's.
- Fishtank: room names/IDs are a seasonal snapshot, not a live map. Raw catalog ids (`bar-5`, `ben-5`, `computer-lab2-5`) can be added without an alias. `fishtank:bar` now resolves to `bar-5` (current catalog); `brrr-5` is still accepted. README documents the stable API/HLS endpoints.

### Safety / logging

- Command and URL logs redact `jwt=`, tokens, Cookie, and Authorization.
- Network ffmpeg whitelist is `http,https,tcp,tls,crypto,hls` (no `file://`).
- `psutil` is exported from `msr.deps` for orphan-ffmpeg cleanup.
- Missing yt-dlp is a warning, not a fatal startup error (Kick/Twitch only need streamlink). Kick channels require streamlink to add, not yt-dlp.
- Cookie dots redraw on scroll/data change, not every 500ms while idle.
- GUI log queue is bounded (drop-oldest at 2000 pending lines) so verbose sessions cannot grow RAM without bound.
- `check_disk_space` fails closed if the streams directory is unreadable *and* unwritable; a usage-stat error on a still-writable path no longer pretends free space is 0 GB while continuing.
- Kick no longer logs “yt-dlp failed” on a streamlink miss; the retry is an alternate streamlink command. Exit logs name streamlink for Kick/Twitch/TikTok.
- Orphan-ffmpeg shutdown only kills network ffmpeg whose command line includes the streams directory (local remux and unrelated encodes are skipped).
- ffprobe path is derived once (`ffprobe_from_ffmpeg`); no more `str.replace("ffmpeg", "ffprobe")` that rewrote a directory named `ffmpeg`.

`.gitignore` also excludes `.venv/`, `venv/`, and `.msr_write_probe`. `msr/`, `tests/`, `AGENTS.md`, `CHANGELOG.md`, `SECURITY.md`, and `.github/` are source and must be committed when this ships.

## v1.8.0

See [RELEASE_NOTES_v1.8.0.md](RELEASE_NOTES_v1.8.0.md).
