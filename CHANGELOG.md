# Changelog

## Unreleased (proposed v2.2.0)

A big interface release: a Library tab that shows (and can clean up) what your recordings take, sixteen themes, a calmer Logs tab, and a fix so recording starts at once after a crash. No new dependencies, and your `config.ini`, `channels.json`, cookies and recordings are used exactly as before.

### Added

**Library tab (new)**
- Size of your recordings per site, channel and month: headline numbers, donut and bar charts with hover details, a sortable channel table with **Open folder**, **Copy summary**, and an optional **Count hours**. It scans on a background thread with Cancel, only lists folders and reads file sizes, and never changes anything. Extra folders can be added with `[Library] extra_dirs` in `config.ini`.
- **File management**: click a donut slice, bar or month to drill in; select channels and press **Delete...** (button, right-click or Delete key) to get a preview with filters (older than N days, keep the newest N files or GB, date range). The default is a reversible **Move to PendingDeletion** with **Undo**; **Delete permanently** needs a typed confirmation; the Recycle Bin is offered when it is large enough. New **Files**, **PendingDeletion** (Empty now / Restore) and **Clean up** tabs.
- Safety guards: only the Processed, Recorded, PendingDeletion and Clips folders are touched, never a channel that is recording, never a recent file, nothing while a scan is running, and every action is written to `library_actions.log`.

**Themes (new)**
- A **Theme...** button in the bottom bar opens a list; the whole window changes live as you click, **OK** keeps the choice and **Cancel** puts the old theme back. No restart.
- Six built-in themes: **PowerShell Blue** (still the default), Neutral Dark, Light, High Contrast, Midnight Purple, Paper.
- Ten more, built from the palettes of community themes on [Omarchy](https://omarchy.org/themes/) (colours only): dark **Arc Blueberry**, **Catppuccin Mocha OLED**, **Rose Pine**, **Gruvbox Material**, **Dracula**, **Flexoki Dark**, **Vesper**, **Tokyo Night OLED**; light **Tokyo Day**, **Solarized Light**. A colour that would be too faint to read is nudged until it meets the same contrast rules as every other theme (text 4.5:1, charts 3:1), and Start/Stop, OK/Error and the six chart colours are tested to stay clearly different. The Theme window shows each theme's credit; sources and licences are in `THIRD_PARTY_NOTICES.md`.
- Your own themes: put `*.json` files in a `themes` folder in the data folder (the window has **Open themes folder** and **Reload**, and writes a sample). Only known colour names with `#rrggbb` values are accepted; a bad file is ignored and listed.
- The Library charts, tooltips, roster, log, dialogs, drop-downs, menus and the Windows title bar follow the theme.

**Logs and window**
- Logs tab: filter box, level drop-down, search highlighting, coloured warnings and errors, **Ctrl+F**, **Open log file**.
- Windows and dialogs remember their size and place (moved back on screen if a monitor is gone) and can be dragged by their background, including the empty part of the tab bar.
- **Restore previous...** (the `...` button beside About) keeps the last 20 versions of `channels.json` and `config.ini` in `msr_history/` and restores one in a themed window. Passwords and other secrets are masked in the list.
- **Streams** button beside Clips opens the recordings folder. It creates nothing and tells you if the folder does not exist yet.

**For scripts and AI assistants**
- `--json` prints a read-only status snapshot (settings, roster, tools, problems) and exits; no secrets and no personal paths. `--json --library` adds the Library numbers.
- `llms.txt` is a manual for AI assistants.
- **Agent control**: everything the window does can now be done by a script, a bot or an AI agent, in the window or `--headless`. A running copy accepts commands on a local, token-protected channel (127.0.0.1 only, token in `msr_control.json`), writes a live `msr_status.json`, and `msr_ctl.py` is the command-line client (JSON output, exit codes). With nothing running, it edits `channels.json` and `config.ini` safely. Destructive commands need an explicit confirmation. You can turn it off with `[Control] enabled = false` or `MSR_CONTROL=0`.
- `docs/AGENT_GUIDE.md`: a quick start for agents. `msr_ctl.py help --json` prints a machine-readable catalog of every command (arguments, safety level, exit codes, examples), and `api_version` (in `msr_control.json`, `ping`, the status file and the catalog) makes `msr_ctl.py` refuse a copy of another version with exit code 7.
- Agent control, more: `msr_ctl.py health` (tools, free disk on the main and fallback drives, cookie expiry without values, Fishtank login set, problems), `recordings [--since]`, `recordings pending`, `recordings retry`, `wait --event live|finished|error [--channel] [--timeout]` (exit code 6 on timeout), `events`, `library suggest`, `library log`, `library restore --last`, `enable/disable --all`. `msr_status.json` adds a health block, the recordings finished this run, the cleanup queue and per-channel URL, last error, error and restart counts, last live and last recorded times. Headless now shows disabled channels' live/offline state, checks for updates, and can `update install --yes` (it restarts headless with the same data folder).
- `--headless` no longer exits when no channel is enabled or `channels.json` is missing; it waits for commands, and the first channel added or enabled starts recording at once (no `start` needed).

**Unplugged drives**
- New setting `[Paths] fallback_dir` (blank = a folder in the system temp folder, `off` = pause): where recording continues if the recordings folder becomes unreachable while MSR runs. See Fixed below.

### Changed
- The **Dark Mode** checkbox is replaced by the Theme window. The choice is saved as `[GUI] theme`. Existing installs migrate by themselves (`dark_mode = true` becomes PowerShell Blue, `false` becomes Light) and the old key is kept in step, so an older version still works.
- All popups and dialogs are themed, centred and attached to the window; the native Windows message boxes are gone.
- The Logs tab follows new lines only while you are at the bottom; a **New lines** button appears when you have scrolled back.
- The disk/network meter has its own thin row under the Status header so the buttons fit at the default width.
- Leftover `.ts` files from a previous session are now remuxed in the background (see Fixed): one file at a time, at below-normal CPU priority, and cancelled cleanly when you stop or quit (the `.ts` is left for the next pass; a `.ts` is still only moved to PendingDeletion after its MP4 is verified).
- Theme credits are fuller: Tokyo Night / Tokyo Night OLED / Tokyo Day credit Enkia (MIT), Arc Blueberry credits BeardedBear (Bearded Theme: MIT as of 2021, GPL-3.0 since 2023-04-14; only colour values are used) and Vale's Omarchy theme, and Omarchy itself is credited once. The Theme window shows the author and licence, and `THIRD_PARTY_NOTICES.md` now holds the full licence text of every source and says plainly where a port has no licence file (Catppuccin's README claims MIT; the repository has no LICENSE file).
- The live preview grab is cheaper: ffmpeg decodes on one thread and only key frames (`-threads 1 -skip_frame nokey`). On two real recordings that cut the CPU time per grab about 10x and about 2x and the run time per grab from roughly 130 to 80 ms and from 125 to 100 ms; the frames are the same as the full decode gives. The picture can now be up to one key-frame interval (typically 2-4 s) older than the live edge. If a stream gives no frame that way, the old full decode is tried as a last resort.
- Each channel remembers which seek worked (3 s from the end, else 6 s, else the elapsed-time seek) instead of trying the 3 s one first every tick, and retries a shorter one after every 10 good grabs in case the stream's key-frame spacing changed. A channel whose grabs all fail is retried after 4, 8, 16, 32 and then 60 s, so a stream with no usable video no longer spawns several ffmpeg and ffprobe runs every 4 s.
- The preview strip is not refreshed while the Status tab is hidden or the window is minimised or in the tray, and refreshes at once when it is shown again.

### Fixed
- After `record-stop`, `wait --event finished` no longer warns "not recording" while the stopped recording is still being remuxed, and it also returns a finish that happened between the stop and the wait. The status shows `finishing` per channel, and new `stopped` / `remux_started` events are emitted. Stopping one channel now starts its remux after about 3 seconds instead of 30 when its processes are confirmed gone; files that are still locked are retried as before.
- Each channel's first live check now runs at once when recording starts or a channel is added (there was a random wait of up to 10 seconds first); later checks keep their spacing. Same in the window and headless.
- `msr_ctl.py`: Ctrl+C exits with a short "Interrupted." and code 130 instead of a traceback, also during `wait`; `record-stop` says per channel whether a recording was stopped or the channel was idle; `wait --event finished` warns when the channel is not recording.
- Headless start with an empty channel list no longer says "Add channels via the GUI": it names `msr_ctl.py add NAME`.
- **An in-app update can no longer leave a half-updated install.** The update now checks that every downloaded Python file is intact, keeps the current program files in `msr_update_backup/`, checks each copied file, and puts the old files back (removing any the update added) if anything goes wrong; the reason is shown at the next start. If the update changes `requirements.txt`, the next start says to run `pip install -r requirements.txt`.
- **The Fishtank password is no longer copied into `msr_history/`.** History copies of `config.ini` hold a placeholder instead of any password, secret or token, and **Restore previous...** keeps your current password. Copies made before this version are unchanged (delete `msr_history/config.ini/` to remove them). The password can also be given in the `MSR_FISHTANK_PASSWORD` environment variable instead of `config.ini`.
- The window state and the Kick channel id cache are written atomically, so a crash or a full disk mid-save keeps the previous file.
- **Faster start-up, and recording starts sooner.** Measured on a Windows PC with a temporary data folder and three placeholder channels (median of 5 runs): the window appears in about 1.4 s instead of 3.1 s, and a headless start reaches its first recording worker in about 1.8 s instead of 5.9 s. Each recording worker no longer loads the GUI, tkinter, the tray and notification libraries, or re-runs the four yt-dlp/streamlink/ffmpeg/deno version checks (the main process runs them once, in parallel, and passes the result on); the leftover-recorder check before the window no longer asks every process for its parent one by one (about 1 s on its own).
- **A remux that is cut off can no longer cost you the recording.** The remux wrote straight to the final MP4, so a crash or Stop mid-remux left half a file that the next cleanup took for finished (it only compared sizes) and then moved the `.ts` to PendingDeletion, which is purged after 7 days. The remux now writes `name.mp4.part`, checks it (big enough, readable, no shorter than the `.ts`), and only then renames it onto the final name; a failed attempt leaves any existing MP4 and the `.ts` untouched, and an existing MP4 is only trusted after the same check. Unfinished `.mp4.part` files are deleted at startup.
- **A failed remux keeps the `.ts` where it is.** ffmpeg missing, a timeout, a full disk or an antivirus lock used to send the only copy to PendingDeletion; it now stays in Recorded and is retried on the next pass or start. Genuinely junk output ("output too small") still goes to PendingDeletion.
- **A hung ffmpeg no longer blocks a remux forever.** The timeout was only checked after ffmpeg closed its output, so a stalled one (network share, locked file) never timed out and a cancel was never seen. ffmpeg is now killed after `ffmpeg_timeout` seconds without progress (or at once on cancel).
- **Files with `.ts` inside the name** (`name.tsunami.ts`) were converted to `name.mp4unami.mp4` by the stop-time cleanup; only the extension is replaced now.
- **A recording stranded by an interrupted cleanup comes back.** If MSR died between the two renames of the in-use check, `x.ts` stayed hidden as `x.ts.cleanupprobe` and was never processed; it is renamed back (never over another file) at the next pass. A stitched reconnect file is also verified before its segments are moved away.
- **`max_record_hours` is enforced.** The setting (default 12) only drove the progress bar, so a 24/7 stream recorded until the size split. A recording now starts a new part when it reaches the limit, with the same overlapping hand-off as the size split, so no live seconds are lost. `0` still means no limit.
- **Fishtank: an expired token or a server error is no longer "live".** The direct stream probe treated every status except 404 as live, so 401/403 (token expired), 429 and 5xx started a capture that could not work. 401/403 now refresh the token and 429/5xx count as an error. The token's expiry is also read correctly (it is base64url, and some tokens decoded to a wrong 23-hour expiry).
- **Secrets are redacted from every log line.** The redaction filter sat on the root logger, which does not see records from named loggers, so those could reach the log file, console or Logs tab with a `?jwt=` intact. The file, console and Logs-tab handlers now carry the filter too.
- **Stream titles with a newline or tab no longer stop a recording.** Control characters are replaced in file and folder names, and Windows device names (`nul`, `con`, `com1`...) get a `_` prefix. `Foo`, `foo` and `foo.` on Kick or Twitch are one channel (one folder on Windows), so the second is skipped on load and refused when adding.
- **An error inside a worker no longer leaves its capture running.** The capture process is now stopped on the worker's error paths, so the worker cannot start a second capture of the same stream after its back-off. Its unused stdout is no longer a pipe that could fill up and stall it.
- Kick-only rosters no longer get the "Deno not found" YouTube warning; a blank `ffmpeg_path` falls back to `ffmpeg` everywhere; a beta build (`3.2b`) is now offered the final `3.2` release; `importlib.util` is imported explicitly in `msr/deps.py`.
- **No console window flashes at startup while leftover recordings are remuxed.** After an abrupt close, the background remux of the unfinished `.ts` files started ffmpeg at below-normal priority, and that priority setting switched off the "no console window" default, so a window titled with the ffmpeg path opened and closed for every file. Every child process (ffmpeg, ffprobe, streamlink, yt-dlp, deno, taskkill) now goes through one helper, `msr.nowindow.no_window_kwargs()`, and the global default keeps the flag even when a launch sets other creation flags. A test fails if a new launch skips the helper.
- **A second MSR instance's recordings can no longer be killed by this one's start-up cleanup.** The "leftover recorder" check matched the recordings folder as plain text, so with `E:\streams` it also hit workers recording into `E:\streams2` or `E:\streams-test` (or a copy inside the folder). Paths are now compared by folder, only for the places MSR itself writes (`Recorded`, `processed`, `PendingDeletion`, `Clips`, the log, the instance lock), a worker whose parent MSR is still running is never treated as a leftover, and a leftover that was recording into the fallback folder is found too. The default fallback folder is now one per recordings folder.
- **A recorder killed from outside is restarted.** Exit code 15 used to count as "stopped by the user", so a worker ended by another program (or another instance's cleanup) stayed dead until a manual restart. MSR now remembers which workers it stopped itself and treats every other exit as a crash.
- **A worker that keeps dying right after it starts no longer restarts every 2 seconds.** Quick exits (under a minute) back off 5 s, 10 s, 20 s ... up to 5 minutes, log a few lines instead of one per try, and the channel row shows when the next try is. A worker that ran for a minute or more restarts at once as before, and starting a channel by hand clears the history.
- **One error no longer ends the recorder supervisor.** A failure while reading status updates, checking storage, or starting one worker (for example out of handles) is logged once and retried; the other channels keep being watched and restarted. A dead status manager no longer raises into the window's timer.
- **Recording starts immediately after a crash or abrupt close.** Leftover `.ts` files used to be remuxed before any recorder launched, so a few big files meant minutes of missed stream. Recorders now start first and the leftovers are remuxed afterwards; only files that existed before launch are touched. If `filename_pattern` has no timestamp (a new recording could reuse a leftover's name) the old clean-up-first order is kept.
- A partial MP4 left by a remux that a crash interrupted is detected and redone instead of being trusted.
- Leftover `x.tsb.ts` style names map to `x.tsb.mp4` (only the extension is replaced).
- The large preview and the strip tile no longer freeze on an old frame: when ffmpeg decoded no frame near the live edge (a key frame farther away than 3 s) it exited 0 and the PNG left by an earlier grab or stream was taken for a new one, so the later seeks were never tried. The grab now deletes the old file first, and pinning a tile starts from the current tile instead of the cached large PNG.
- A preview that stops updating is now visible: after 10 s without a fresh frame the tile name and the large pane title show the age (for example `binxbasilisk · 12 s`) and the border turns to the theme's warning colour. A channel that stopped recording keeps its last frame for 90 s, marked `ended`, instead of vanishing.
- Streams whose key frames are about 4 s apart (the old fixed 3 s seek found no frame there, so every tick ran a second seek) are grabbed with one ffmpeg run once the working seek is known.
- Starting from a redirected Windows console (cp1252) no longer fails on characters the console cannot show.
- A `%` in `config.ini` is no longer a crash. A Fishtank password such as `P@ss%word` or a `filename_pattern` with `100%` used to make the settings unreadable, so MSR did not start or no worker could start. Values are read as typed. (If you ever wrote `%%` to work around this, write a single `%` now.)
- A `config.ini` that cannot be parsed (an option twice, text before the first `[section]`, a file saved in another encoding) no longer stops MSR from starting silently. A UTF-8 BOM or a Windows-1252 file is simply read. Otherwise MSR runs with its built-in settings, says so in the log, and leaves your file as it is. Only when it has to rewrite the file does it first save a copy next to it as `config.ini.broken-<date>-<time>`.
- A recordings folder on a drive that is not there (an unplugged external disk) now shows the "Drive ... does not exist" message instead of MSR vanishing. The log file goes to the data folder until the drive is back.
- A start that fails under `pythonw` (the double-click launcher) now leaves `msr_startup_error.log` in the data folder, with the error and no passwords or tokens, and shows a message window when it can.
- Headless mode (`--headless`): Ctrl+C now stops, then finishes the clean-up of leftover `.ts` files (about 30 s plus the remux time; press Ctrl+C again to skip) before it exits. It used to exit at once and leave them for the next start.
- `ffmpeg_path` in `[Advanced]` is honoured at start-up: ffmpeg no longer has to be on PATH when the configured path works, a wrong path is reported with the setting's name, and ffprobe next to it is checked (a warning if it is missing).
- Unplugging the recordings drive (USB, external, a dropped network share) while MSR runs no longer ends in silent failures. Each channel notices before its next recording, keeps recording into a fallback folder (`fallback_dir`, default: a folder in the Windows temp folder; `off` = pause), and records into the main folder again once it is back. A live recording already running on the lost drive is ended after 30 s instead of hanging, and the channel carries on in the fallback at once instead of waiting through the 60 s crash back-off.
- The Status tab shows a red line while the folder is unreachable (and one notification), `--json` reports `drive_available`, and the log says it once instead of once per check.
- When the drive returns, what was recorded in the fallback folder is moved back to the same place under `streams_dir` (a copy is checked before the original is removed; a file that may still be written is left for a later pass; nothing is ever overwritten), and unfinished `.ts` files go through the same clean-up as after a crash.
- Pressing Start while the drive is gone no longer fails; `config.ini` and `channels.json` are written in one step and a failed save is a warning (settings stay in memory, the old file is kept) instead of an error in the window; the Library cache and action log, window state and Kick id cache warn once instead of failing silently.

### Developer notes
- Every colour comes from `msr/themes.py` (semantic tokens) and the palette data in `msr/theme_palettes.py`; a test fails on any hard-coded colour elsewhere. Adding a theme is one entry in `msr/theme_palettes.py`.
- The test suite grew from 299 to 652 tests, including a hidden-window smoke test that launches the real UI in every theme. `AGENTS.md` documents the new modules.
- New files that stay out of git: `themes/`, `msr_history/`, `library_cache.json`, `library_moves.json`, `library_actions.log`, `msr_startup_error.log`, `config.ini.broken-*`. Recordings made while the drive was away are in `fallback_dir` (outside the repo).

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
