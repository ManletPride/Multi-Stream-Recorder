# Agent notes — Multi-Stream Recorder

Read this before changing code. The project is **not** a single-file script anymore. Several clip and GUI bugs were already fixed; reintroducing the old approaches will regress them.

**Ship status (2026-10-20):** branch `release/2026-10` is **v2.1.0** (roster panel, presence, Kick Centrifugo push, reconnect fix, install cleanup), proposed for `main` via PR. Fully quit and relaunch after Python edits — the GUI does not reload `msr/`.

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
| `msr/update.py` | GitHub zipball download + allowlisted extract for in-app update |
| `msr/apply_update.py` | Temp helper: wait for PID, copy staged files, relaunch (no `msr` import) |
| `msr/config.py` | `Config` / `config.ini` defaults (`default_streams_dir()`, coerce invalid numbers) |
| `msr/deps.py` | `HAS_*` flags, tool versions, optional `psutil` / tray / plyer imports |
| `msr/iometer.py` | Status-header disk write / NIC download rates (`IoSampler`) |
| `msr/__init__.py` | `__version__` (currently `2.1.0`) |
| `SECURITY.md` | Vulnerability reporting; what is stored on disk |
| `.github/workflows/tests.yml` | unittest on Windows + Ubuntu, Python 3.10 and 3.12 |
| `tests/` | unittest (`python -m unittest discover -s tests -t .`) |

Do not fold these back into one file without an explicit request. On Windows, `mp.set_start_method('spawn')` pickles `msr.worker.record_worker` — that function **must stay top-level in `msr.worker`**.

After editing Python, the running GUI does **not** reload modules. The user must fully quit and relaunch to pick up clip/worker changes.

## Instant clips (`create_clip` in `msr/worker.py`)

Kick/live MPEG-TS PCR often sits at ~10000s while the `.ts` on disk is only minutes long. Twitch can also report `format.duration` as the **last PCR** (e.g. 12686) instead of elapsed file time (e.g. 67). `_elapsed_media_duration()` must convert that before computing the cut; otherwise a 15s Clip Now seeks to ~65s of a 67s file (powdur clip was **2.6s**) and screenshots miss the frame.

**Correct:** convert duration to elapsed if it looks like last-PTS (`duration > start_time` and `start_time >= 1000`). Probe the last video keyframe at or before `(elapsed - clip_seconds)`, then input `-ss` to that keyframe’s **elapsed** time (`kf_pts - format.start_time`), tiny undershoot (~0.05s), never past the keyframe. `-c copy`, `-t` extended to the live edge, `-avoid_negative_ts make_zero`. No output `-ss`, no `-copyts`. If elapsed is far behind the file’s birth time (Kick HLS ad PCR reset; nicklee 2024s vs 5990s), **ignore duration** and probe/cut with `-sseof` instead — `-ss` from the start hits the first occurrence of the new clock, minutes behind live.

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
- **Size-split is make-before-break.** Hitting `max_file_size_gb` starts the next capture *while the current process is still writing*. Monitor calls `on_size_limit`, which spawns the next file and waits until it has ≥64 KiB, then the old process is killed. The two files overlap by a few seconds — no hole. Remux of the closed file is a worker thread; status stays `Recording` with `remuxing N%`. Do not kill-then-restart on size limit (kimmee lost ~90s of remux, then still a live-check gap). If the next file never appears, fall back to kill + restart (short gap). **Resolution-change splits still kill first** so one file does not mix two frame sizes. Stream-end remux stays synchronous. `find_active_recording_file` is newest `.ts` by mtime. `channel_status_allows_clip` is Recording or Remuxing. `wait_until_file_has_data` is the overlap wait.
- Roster right-click also has **Open Clips Folder** (uses the left-list selection, not the status row).
- **Open in Browser** uses `channel_watch_url()` in `msr/util.py` — rumble/tiktok/fishtank are not Kick. YouTube `@handle` must not become `@@handle`.

## Experimental live previews (`experiment/status-previews`)

Status-tab stills from the live `.ts`. Constants in `msr/gui.py`: `PREVIEW_W`/`PREVIEW_H` (160×90), `PREVIEW_BIG_W`/`PREVIEW_BIG_H` (480×270), `PREVIEW_MS` (4000).

- `create_preview_frame` / `create_screenshot` in `msr/worker.py` — seek-before-EOF (`-sseof` first, elapsed `-ss` fallback). Do **not** prefer `-ss duration-N` for screenshots: after a Kick ad PCR reset that seeks minutes behind live (preview stays current). PNG for previews (tk.PhotoImage has no JPEG), `CREATE_NO_WINDOW`. Preview failures are quiet.
- **Clip Now** after a PCR reset: if ffprobe elapsed is far behind file birth (`_elapsed_matches_file_age`, nicklee 2024s vs 5990s), treat duration as missing and probe/cut with `-sseof` like the no-duration path. Trusted elapsed still uses keyframe elapsed `-ss` (Kick/Twitch PCR). No output `-ss`, no `-copyts`.
- Worker **Recording** status includes **`raw_file`**. The GUI preview loop must use that path. Do **not** `listdir` recording folders on the 1s IO tick. Fallback `find_active_recording_file` is OK on the 4s preview pass if `raw_file` is missing.
- Grab on a daemon thread, one ffmpeg at a time; skip a channel if Clip/Screenshot is in flight on it. Keep `PhotoImage` refs (`_preview_photos`, `_preview_big_photo`) or Tk drops the picture.
- Click tile / status row / roster stay on the same channel (`_sync_nav`). **Never** call `update()` or `update_idletasks` from `<<TreeviewSelect>>` or `_sync_nav` — that nested the Tk loop and froze the GUI on Windows. `see()` / canvas scroll only in `after_idle`. Hold `_nav_lock` around `tree.move` in `_reorder_status_rows`.
- Roster **Filter** + **Enabled**: resolve rows by **channel name**, never `ch_tree.index` (a filtered tree’s row index is not `channels[]`).
- **Recording first** uses `ordered_status_keys` then `tree.move`. Do not delete-and-rebuild the status tree.
- Double-click a tile pins a 480×270 copy above the strip (extra ffmpeg only for that channel). Close / double-click the same tile / double-click the large image dismisses it. Drop the pin if that channel stops recording.

## Left roster panel (`ui/left-panel`)

- Pure logic lives in `msr/channel_panel.py` (tested in `tests/test_channel_panel.py`); widgets are still built in `main_gui`.
- Roster iids are `c:<channel key>` (group headers `h:<group>`). Never map rows by display text: the name column shows the bare name and the platform is a separate tag. channels.json keys are unchanged.
- Groups are titled **Recording**, **Watching** (enabled, not recording) and **Off** (disabled; collapsible). Internal group ids stay `recording` / `enabled` / `disabled`, and the collapse flag is still `roster_disabled_collapsed` in window_state, so old state files keep working. Manual order is kept inside groups. View toggle is `roster_view` (all/enabled/live); the old `roster_enabled_only` is still read.
- **No status dots** (removed on request; do not re-add). Live state (`channel_live_state` from `recorder.status_dict` merged with presence by `msr.presence.combined_state`) only drives the Recording group and the **Live** filter, refreshed on the 2.5s `refresh_status` tick (`_roster_live_tick`). The tree is rebuilt only when the grouped/filtered layout changes.
- **Presence** (`msr/presence.py`): `PresenceStore` holds `{live, viewers, since, source}` per key for ALL channels. Sources: `KickPushListener` watches every Kick channel (enabled = `_wanted`, wakes the worker; disabled/stopped = `_watch`, presence only and must never call `check_now`/start anything), and `PresencePoller` polls DISABLED channels only (Kick `/api/v2/channels/{slug}` via curl_cffi, Twitch via `check_stream_streamlink`), one at a time spread over ~10 min, failures silent. Enabled channels are never polled by presence. Push needs a running session; the poller runs whenever the GUI is open.
- **Kick realtime transport** (`msr/kick_realtime.py`): no hard-coded Pusher key (the old `32cbd69e...` key died, error 4001). Every (re)connect POSTs `web.kick.com/api/v1/realtime/connection` like the web client; Centrifugo is primary (guest token from `/v1/realtime/auth/connection`, JSON protocol: `connect` -> `subscribe {"channel":"channel.<id>"}`, answer server ping `{}` with `{}`, `refresh` the ~45 min token), Pusher with the returned `app_key`/`cluster` is the fallback. Publications are `{"event": "App\\Events\\StreamerIsLive"|"...StopStreamBroadcast", "data": "<json>"}` on `channel.<id>`, normalised to Pusher shape and parsed by `parse_kick_push_event`. Max 128 subs per connection (error 106); MSR caps at 120. Connection failures warn at most hourly, back-off up to 10 min; polling is unaffected.
- Kick push: slugs that never confirm `subscription_succeeded` (failed id lookup, lost frame) are retried by the `kick-push-resubscribe` sweep with 2-30 min back-off. Before this they stayed unsubscribed until the next reconnect (the lettyvision symptom). Id lookup tries `_` -> `-` slug variants.
- Amber cookie badge = `cookie_badge()`: auth-error text in a worker Error detail, an expired auth cookie for the site, or Fishtank with no cookie and no config login. Drawn on the side canvas, enabled rows only.

- **Dialog title bars:** `msr/wintheme.py` `set_title_bar_dark(win, dark)` (DWM attribute 20, then 19, on the `wm_frame()` HWND after `update_idletasks`, then SWP_FRAMECHANGED to repaint). The main window, the Add channel dialog, and the custom poll/clip `simpledialog` prompts (via `watch_new_dialogs` before the blocking call) use it; `apply_theme` re-themes open dialogs. Native `messagebox` dialogs are OS-owned and can't be themed.

## Other behavior that must not regress

- **Remove channel** calls `StreamRecorder.remove_channel()` so the worker is killed; do not only delete the GUI list entry.
- **Complete toasts** are edge-triggered (`_notified_complete`). Do not notify every 2.5s refresh while status stays Completed.
- **Status table** updates existing rows in place. Do not delete-and-rebuild the tree every tick.
- **Logs:** `redact_cmd_for_log` / `redact_for_log` strip `jwt=`, tokens, Cookie/Authorization. `RedactLogFilter` on the root logger also redacts stderr lines, exception text, and `%(channel)s` (custom URLs with `?jwt=`). Do not log raw Fishtank auth JSON.
- **Network ffmpeg** (`platforms.py`): protocol whitelist is `http,https,tcp,tls,crypto,hls` — **no `file`**. Direct ffmpeg (Fishtank/Rumble HLS/CMAF) uses `-protocol_whitelist`. yt-dlp recordings pass the same list as `--downloader-args ffmpeg_i:…` (must be `ffmpeg_i`, not `ffmpeg:`, or yt-dlp appends it after `-i`). Local clip/remux reads do not use that whitelist.
- **Channel keys:** `parse_channel_key` allowlists platforms. Kick is a bare name — `:` `/` `\\` are rejected so `..\..\Users\Public:out` cannot become a platform directory. Custom URLs must be `http://` or `https://`. `coerce_channel_records` re-validates `channels.json` on load; `record_worker` refuses unsafe keys. `channel_key_to_dirs` returns `('unknown', 'unknown')` for garbage instead of joining raw `..`.
- **In-app update:** banner click runs `msr.update.prepare_update` (GitHub zipball, allowlisted extract) then `_full_quit(after_children=spawn_updater)`. Spawn the helper **after** killing children or `_full_quit` will kill it. Helper is `msr/apply_update.py` copied to temp (stdlib only, no `msr` import). Never overwrite `config.ini` / `channels.json` / `cookies.txt`. HTTPS GitHub hosts only.
- **Single instance** per `streams_dir`: `try_acquire_single_instance` in `main()` (Windows mutex / POSIX flock). A second launch focuses the existing `Multi-Stream Recorder v…` window and exits; `--headless` prints and exits 1. Do not skip this or two GUIs will record the same cameras.
- **`kill_orphan_recording_processes`** (old name `kill_orphan_ffmpeg_processes` still wraps it) uses `psutil` from `msr.deps`. Pass `streams_dir`. Kill leftover **`record_worker` spawn processes** (and their capture tree), not only ffmpeg — ffmpeg SIGTERM is a reconnect. Skip local remux/clip (no http) and unrelated encodes. Called at startup, `StreamRecorder.stop()`, and `_full_quit`. Never kill `keep_pid`’s tree.
- **Startup cleanup** (`StreamRecorder.run` in `msr/recorder.py`): `_plan_startup_cleanup()` snapshots leftover `.ts` files BEFORE the workers launch; `_launch_startup_cleanup()` remuxes that snapshot afterwards on the `StartupCleanup` thread (below-normal ffmpeg via `remux_to_mp4(low_priority=True, cancel_event=...)`, one file at a time, `_cleanup_lock` per file, cancelled by `stop()`). Never scan live folders from this path; only snapshot files are processed. A `filename_pattern` without a timestamp falls back to the synchronous order. Tests: `tests/test_startup_cleanup.py`.
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
- **Fishtank cameras** change every season (and in the off-season). Keep `CAMERA_ALIASES` as a convenience snapshot; do not scrape `fishtank.live` HTML (Next.js CSR). Catalog is `GET /v1/live-streams`. Login/HLS URL shape stays. Accept raw stream ids (`is_fishtank_stream_id`, e.g. `bar-5`, `computer-lab2-5`, `director-as-seen-on`) so a new season does not require a code edit. Add aliases when a season's catalog is confirmed; do not delete old ones. As Seen On (2026-09) remapped `director` to `director-as-seen-on`; keep `dirc` → `dirc-5`.
- **Open in Browser** goes through `channel_watch_url()` — do not hardcode Kick as the fallback for prefixed platforms.
- **Rumble cookie domain** is `rumble.com`, not Kick.
- **Rumble 403:** record the channel-page HLS URL with ffmpeg (`build_recording_command_rumble_hls`) when `videos[].url` is in the page JSON. Otherwise yt-dlp must use `--impersonate chrome` on the *first* video-page fetch (plain dump-json 403s). Do not tell the user their cookies expired for that first 403. `parse_rumble_channel_html` must capture the **whole** `<script type="application/json">` body (`(.*?)` inside the tag), not `\{.*?\}` which stops at the first nested `}` and drops the HLS URL.
- **Chaturbate / CMAF:** split video+audio playlists go through `build_recording_command_ffmpeg_merge`, not yt-dlp live merge. A 15s clip must contain **both** h264 and AAC.
- **TikTok offline fallback** (`check_tiktok_live_webcast`): yt-dlp `"not currently live"` is not authoritative for 24/7 LIVEs. Try `/api-live/user/room?uniqueId=` first (no roomId needed), then `roomId` on the profile **and** `/@user/live`, then `webcast.us` / `webcast.tiktok.com`. Status 2 = live; leftover roomId with status 4 is not. Log misses at **info**, not debug.
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
