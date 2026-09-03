# Changelog

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
