# Agent notes — Multi-Stream Recorder

Read this before changing code. The project is **not** a single-file script anymore. Several clip and GUI bugs were already fixed; reintroducing the old approaches will regress them.

**Ship status (2026-09-02):** local tree is **v2.0.0 unreleased** (was tracked as 1.8.1 while the package split landed). `origin/main` is still the v1.8.0 single-file app. Do **not** push until the user asks. More features are planned before GitHub.

Fully quit and relaunch after Python edits — the GUI does not reload `msr/`.

## Layout

| Path | Role |
| --- | --- |
| `Multi-Stream-Recorder.py` | Thin launcher only (`main()`, `--headless`). Users still run this. |
| `msr/gui.py` | Tkinter UI (`main_gui`) |
| `msr/worker.py` | `record_worker` (multiprocessing target), clips, remux, monitor |
| `msr/platforms.py` | Per-site live checks, Fishtank auth, ffmpeg/yt-dlp/streamlink command builders |
| `msr/recorder.py` | `StreamRecorder`, `BackgroundCleaner`, Kick push (`KICK_PUSHER_URL`) |
| `msr/util.py` | Paths, cookies, logging, process kill, validation, log redaction |
| `msr/config.py` | `Config` / `config.ini` defaults (`default_streams_dir()`, coerce invalid numbers) |
| `msr/deps.py` | `HAS_*` flags, tool versions, optional `psutil` / tray / plyer imports |
| `msr/iometer.py` | Status-header disk write / NIC download rates (`IoSampler`) |
| `msr/__init__.py` | `__version__` (currently `2.0.0`) |
| `SECURITY.md` | Vulnerability reporting; what is stored on disk |
| `.github/workflows/tests.yml` | unittest on Windows + Ubuntu, Python 3.10 and 3.12 |
| `tests/` | unittest (`python -m unittest discover -s tests -t .`) |

Do not fold these back into one file without an explicit request. On Windows, `mp.set_start_method('spawn')` pickles `msr.worker.record_worker` — that function **must stay top-level in `msr.worker`**.

After editing Python, the running GUI does **not** reload modules. The user must fully quit and relaunch to pick up clip/worker changes.

## Instant clips (`create_clip` in `msr/worker.py`)

Kick/live MPEG-TS PCR often sits at ~10000s while the `.ts` on disk is only minutes long. Twitch can also report `format.duration` as the **last PCR** (e.g. 12686) instead of elapsed file time (e.g. 67). `_elapsed_media_duration()` must convert that before computing the cut; otherwise a 15s Clip Now seeks to ~65s of a 67s file (powdur clip was **2.6s**) and screenshots miss the frame.

**Correct:** convert duration to elapsed if it looks like last-PTS (`duration > start_time` and `start_time >= 1000`). Probe the last video keyframe at or before `(elapsed - clip_seconds)`, then input `-ss` to that keyframe’s **elapsed** time (`kf_pts - format.start_time`), tiny undershoot (~0.05s), never past the keyframe. `-c copy`, `-t` extended to the live edge, `-avoid_negative_ts make_zero`. No output `-ss`, no `-copyts`.

**Wrong (causes a frozen first GOP):** `-copyts` plus output `-ss {raw PCR pts_time}`. Audio starts at 0s, video at ~2s (Kick GOP). Verified on binxbasilisk 15s clips: bad cut video start **1.99s**; after the elapsed-seek fix, **~0.18s** (AAC/B-frame delay, not a freeze).

15s clips are the harshest test because a 2s GOP is a large fraction of the file.

Verified live 15s clips (video start is AAC/B-frame delay, not a freeze):

| Platform | File | Length | Video start |
| --- | --- | --- | --- |
| Kick | binxbasilisk (earlier session) | ~15s | ~0.18s after elapsed-seek fix |
| YouTube | burntpeanut247_20260902_183558 | 19.9s | 0.10s, first packet keyframe |
| TikTok | ghbk52_20260902_184231 | 16.9s | 0.08s (portrait 640×1280 is normal) |
| Twitch | powdur — **before** duration fix | **2.6s** | A/V aligned but cut at live edge |
| Rumble | NewsmaxTV_20260902_190523 | 15.3s | 0.11s, first packet keyframe |
| Chaturbate CMAF | chaturbate_20260902_190837 | 15.8s | 0.25s, both A/V present (merge held) |

Related UI:

- **Clip Now** and **Screenshot** buttons on the Status header (same actions as right-click). Debounced per channel+kind.
- **Clips** button (left of Screenshot) opens that channel’s clips/screenshots folder (`clips_dir` or `streams_dir/Clips`). Does not need an active recording. Same action as right-click **Open Clips Folder**.
- Clip/screenshot **filenames** use `channel_file_stem(username_dir)` (last folder only). Do not interpolate a nested `username_dir` (`chaturbate\\mode_bad`) into the filename — Windows treats the `\\` as another directory.
- Status rows store the real channel key in a hidden `_key` column — never map selection by display name (`kick foo` vs `twitch:foo`).
- One clip ffmpeg at a time per channel; ignore repeat clicks until it finishes.
- Roster right-click also has **Open Clips Folder** (uses the left-list selection, not the status row).
- **Open in Browser** uses `channel_watch_url()` in `msr/util.py` — rumble/tiktok/fishtank are not Kick. YouTube `@handle` must not become `@@handle`.

## Other behavior that must not regress

- **Remove channel** calls `StreamRecorder.remove_channel()` so the worker is killed; do not only delete the GUI list entry.
- **Complete toasts** are edge-triggered (`_notified_complete`). Do not notify every 2.5s refresh while status stays Completed.
- **Status table** updates existing rows in place. Do not delete-and-rebuild the tree every tick.
- **Logs:** `redact_cmd_for_log` / `redact_for_log` strip `jwt=`, tokens, Cookie/Authorization. `RedactLogFilter` on the root logger also redacts stderr lines, exception text, and `%(channel)s` (custom URLs with `?jwt=`). Do not log raw Fishtank auth JSON.
- **Network ffmpeg** (`platforms.py`): protocol whitelist is `http,https,tcp,tls,crypto,hls` — **no `file`**. Direct ffmpeg (Fishtank/Rumble HLS/CMAF) uses `-protocol_whitelist`. yt-dlp recordings pass the same list as `--downloader-args ffmpeg_i:…` (must be `ffmpeg_i`, not `ffmpeg:`, or yt-dlp appends it after `-i`). Local clip/remux reads do not use that whitelist.
- **Channel keys:** `parse_channel_key` allowlists platforms. Kick is a bare name — `:` `/` `\\` are rejected so `..\..\Users\Public:out` cannot become a platform directory. Custom URLs must be `http://` or `https://`. `coerce_channel_records` re-validates `channels.json` on load; `record_worker` refuses unsafe keys. `channel_key_to_dirs` returns `('unknown', 'unknown')` for garbage instead of joining raw `..`.
- **`kill_orphan_ffmpeg_processes`** uses `psutil` from `msr.deps` (exported even when checking `HAS_PSUTIL`). Pass `streams_dir` — only network ffmpeg whose argv includes that directory is killed.
- **`record_worker` imports:** `random`, `sanitize_path_component`, `custom_url_folder_names`, `parse_channel_key`, `redact_for_log`, `text_progress_bar`, `find_cookies_file`, `ffprobe_from_ffmpeg`, `parser_getfloat` / `parser_getint` / `parser_getboolean`, `default_streams_dir`. Missing any of these is a spawn-loop `NameError`.
- **`streams_dir` default** is `default_streams_dir()` (`%USERPROFILE%\Videos\Multi-Stream Recorder` / `~/Videos/Multi-Stream Recorder`), not `E:\Streams`. Never overwrite an existing `streams_dir` in `config.ini`.
- **Config coercion:** `Config._coerce_values` rewrites unparseable/out-of-range numbers and bools to defaults before workers spawn. Workers still use `parser_get*` so a bad dict cannot `ValueError` on `getfloat`. Do not warn “using default” without actually setting the value.
- **Custom URL folders:** `custom_url_folder_names()` is the single mapping used by `record_worker` and `channel_key_to_dirs`. Chaturbate-style `https://site.com/user/` goes to `custom/site/user/`. No path username → keep the old `custom/site/` bag. TikTok custom URLs stay `custom/<handle>/` (do not nest under `tiktok/`, existing files live there). Background cleaner walks one extra directory level so both the bag and nested user folders are remuxed.
- **Cookie dots** redraw from tree events / `yview`, not a 500ms idle loop.
- **Disk / NIC meters** (Status header, left of Screenshot) are whole-disk write and whole-NIC download from `psutil`. MSR-only write rate uses worker `size_bytes` on the status dict — do **not** `listdir` the recordings folder on the GUI 1s tick. Tooltip holds stream Mbps from the status detail string. Hidden when `psutil` is missing. Sample with `root.after(1000)`. On Windows, map `streams_dir` to `PhysicalDriveN` via `IOCTL_STORAGE_GET_DEVICE_NUMBER`.
- **Stop Recording** / per-channel stop join process trees on a background thread. Do not call `recorder.stop()` or `stop_channel()` on the Tk thread (the GUI freezes for 10s×N). `_full_quit` may still stop synchronously before `os._exit`.
- **Bottom toolbar** packs `side=RIGHT` (first = rightmost). Pack each combobox *before* its label so the caption sits to the left of *that* dropdown: `Clip Length: [combo]  Polling: [combo]  Dark Mode`. Do not pack the label first — that put “Clip Length:” on the poll combo and “Check Now” on the clip combo.
- **GUI log queue** is bounded (`Queue(maxsize=2000)`, drop-oldest). Do not use an unbounded `Queue`.
- **`check_disk_space`** fails closed when the path is not writable. Do not `return True, 0` on `disk_usage` errors.
- **Kick records with streamlink.** Do not log “yt-dlp failed” on a missing Kick file; the retry is another streamlink argv.
- **ffprobe path** is `ffprobe_from_ffmpeg()` in `msr/util.py`. Do not `ffmpeg_path.replace("ffmpeg", "ffprobe")`.
- **Cookies path:** `application_dir()` is the launcher directory, not `msr/`.
- **Fishtank JWT refresh** in `platforms.py` needs `import threading`.
- **Fishtank cameras** change every season (and in the off-season). Keep `CAMERA_ALIASES` as a convenience snapshot; do not scrape `fishtank.live` HTML (Next.js CSR). Catalog is `GET /v1/live-streams`. Login/HLS URL shape stays. Accept raw stream ids (`is_fishtank_stream_id`, e.g. `bar-5`, `computer-lab2-5`) so a new season does not require a code edit. Add aliases when a season's catalog is confirmed; do not delete old ones.
- **Open in Browser** goes through `channel_watch_url()` — do not hardcode Kick as the fallback for prefixed platforms.
- **Rumble cookie domain** is `rumble.com`, not Kick.
- **Rumble 403:** record the channel-page HLS URL with ffmpeg (`build_recording_command_rumble_hls`) when `videos[].url` is in the page JSON. Otherwise yt-dlp must use `--impersonate chrome` on the *first* video-page fetch (plain dump-json 403s). Do not tell the user their cookies expired for that first 403. `parse_rumble_channel_html` must capture the **whole** `<script type="application/json">` body (`(.*?)` inside the tag), not `\{.*?\}` which stops at the first nested `}` and drops the HLS URL.
- **Chaturbate / CMAF:** split video+audio playlists go through `build_recording_command_ffmpeg_merge`, not yt-dlp live merge. A 15s clip must contain **both** h264 and AAC.
- **Cookies:** MSR never refreshes `cookies.txt`. It only reads it (roster dots every 5 minutes) and passes `--cookies` to tools. User re-exports from the browser. Do not replace the whole jar with a Rumble-only dump — merge would wipe YouTube/Twitch/Kick/TikTok rows.
- **Git:** ignore `config.ini`, `cookies.txt`, `channels.json`, `window_state.json`, `kick_channel_ids.json`, recordings dirs, `.venv/`, `venv/`, `.msr_write_probe`. Do **not** ignore `msr/`, `tests/`, `AGENTS.md`, `CHANGELOG.md`, `SECURITY.md`, `.github/`, or `channels_fishtank.json`.
- Worker status while recording includes **`size_bytes`** (int) for the header meter. Do not drop that field; the GUI must not `listdir` recording folders on the 1s tick.

## Run

```
python Multi-Stream-Recorder.py
python Multi-Stream-Recorder.py --headless
python -m unittest discover -s tests -t .
```

Python 3.10+. ffmpeg + ffprobe are required. yt-dlp is needed for YouTube/Rumble/TikTok/custom; streamlink for Kick/Twitch. See README.md for optional deps and `config.ini`. Legal/security notes: README *Legal / acceptable use* and `SECURITY.md`.
