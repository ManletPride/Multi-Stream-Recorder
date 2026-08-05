# Multi-Stream Recorder

A desktop application for simultaneously recording live streams from **Kick**, **Twitch**, **YouTube**, **Rumble**, **TikTok**, **Fishtank.live**, and any site supported by yt-dlp. Set it up, press record, and walk away — it monitors channels, auto-records when they go live, and produces clean MP4 files.

![Dark Mode Screenshot](screenshots/dark-mode.png)

## Features

* **Multi-platform** — Record from Kick, Twitch, YouTube Live, Rumble, TikTok, Fishtank.live, and 1,800+ sites via custom URLs
* **Split-track HLS** — Automatically detects and records CMAF streams with separate video/audio playlists (used by Chaturbate and other CDN-backed platforms)
* **Concurrent recording** — Monitor and record multiple streams simultaneously
* **Automatic detection** — Polls channels and starts recording the moment a stream goes live
* **Kick push notifications** — Kick recordings start within *seconds* of a stream going live: MSR holds a WebSocket to Kick's own real-time event feed (the same one the kick.com website uses) and reacts to the go-live event instantly, instead of waiting for the next poll. Polling continues underneath as an automatic fallback
* **Smart polling** — Configurable check intervals with jitter to avoid rate limiting; exponential backoff on errors only
* **Instant clips & screenshots** — While a channel is recording, right-click it and pick **Clip Now** to stream-copy the last N seconds of its live .ts into a standalone MP4, or **Screenshot Now** to grab a still — neither interrupts the ongoing recording. Clip length is adjustable from a toolbar selector (15 sec to 30 min)
* **Custom poll rate** — Pick a preset (1/3/5 min) or set any custom interval from 30 seconds to 2 hours; changes apply instantly to running sessions
* **Check Now** — Skip the poll timer entirely: one button checks every enabled channel immediately, or right-click a single channel to check just that one
* **Fast reconnect** — If a stream drops briefly (streamer disconnect), re-detects within 15 seconds
* **File splitting** — Automatically splits recordings at a configurable size limit (default 8 GB), and on mid-stream resolution changes that would otherwise corrupt playback
* **Clean MP4 output** — Automatically remuxes raw .ts recordings to .mp4 with ffmpeg, and warns if a finished recording has no audio track
* **Cloudflare bypass** — Kick streams use streamlink's built-in JS challenge solver; Rumble uses browser impersonation fallback
* **Dark mode GUI** — Full dark/light theme with system tray support and desktop notifications
* **Live stream info** — Status table shows resolution, frame rate, and bitrate for all active recordings
* **Cookie support** — Use browser cookies for authenticated access (subscriber-only streams, age-gated content), with an indicator showing whether yours are valid or expiring
* **Per-channel control** — Start or stop individual channels mid-session via right-click context menu
* **Recording metadata** — JSON sidecar files with channel info, stream title, duration, and timestamps
* **Micro-fragment throttle** — Detects CDN reset storms (repeated sub-30s recordings) and backs off rather than accumulating dozens of tiny files
* **Auto-cleanup** — Configurable retention period for processed files
* **Robust shutdown** — No orphaned processes, no zombie ffmpeg instances

## Quick Start

### 1. Install Prerequisites

**Python 3.10+** is required. Then install the external tools:

**ffmpeg** (required):

```
# Windows — download from https://www.gyan.dev/ffmpeg/builds/
# Add the bin/ folder to your system PATH

# Linux
sudo apt install ffmpeg

# macOS
brew install ffmpeg
```

**yt-dlp** (required for YouTube, Rumble, TikTok, custom URLs):

```
pip install "yt-dlp[default]"
```

**streamlink** (required for Twitch and Kick):

```
pip install streamlink
```

### 2. Install Python Dependencies

```
pip install -r requirements.txt
```

Or install individually:

```
pip install "yt-dlp[default]"     # Required for YouTube, Rumble, TikTok, custom URLs
pip install streamlink            # Required for Twitch and Kick
pip install psutil                # Recommended — cleaner process management
pip install pystray Pillow plyer  # Optional — tray icon & notifications
pip install curl_cffi             # Optional — Rumble Cloudflare bypass; Kick push channel-ID lookup
pip install websocket-client      # Optional — Kick push notifications (instant go-live detection)
```

### 3. Run

```
python Multi-Stream-Recorder.py
```

On first launch, the program creates a `config.ini` with sensible defaults. Edit `streams_dir` to set where recordings are saved.

### 4. Add Channels

Use the GUI to add channels:

1. Select a platform from the dropdown (kick, twitch, youtube, rumble, tiktok, fishtank, custom)
2. Enter the channel name (e.g., `asmongold` for Kick, `saruei` for Twitch)
3. For custom URLs, paste the full URL:
   - Rumble channels: `https://rumble.com/c/ChannelName`
   - Chaturbate: `https://chaturbate.com/username/`
   - Any yt-dlp supported site: paste the stream URL
4. Click **Add** or press **Enter**

Press **Start Recording** — the program will monitor all channels and record any that go live.

## Cookies Setup

Cookies are optional but recommended for YouTube (avoids throttling) and required for subscriber-only content on any platform.

### Exporting Cookies

1. Install the [Get cookies.txt LOCALLY](https://chromewebstore.google.com/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc) browser extension (Chrome/Edge)
2. Visit the streaming site and log in
3. Click the extension icon → **Export** → save as `cookies.txt`
4. Place the file in your streams directory (e.g., `E:\Streams\cookies.txt`)

The program auto-detects `cookies.txt` in your streams directory or the script's folder. The cookie indicator in the GUI shows whether your cookies are valid and warns when auth tokens are expiring.

## Configuration

All settings are in `config.ini`, auto-created on first run:

```
[Paths]
streams_dir = E:\Streams          # Where recordings are saved
channels_file = channels.json     # Channel list (managed by GUI)
cookies_file =                    # Auto-detected if empty

[Recording]
quality = best                    # Stream quality
max_record_hours = 12.0           # Auto-stop after N hours (0 = no limit)
max_file_size_gb = 8.0            # Split recording when file exceeds this size (0 = disabled)
split_on_resolution_change = true # Split when the live video's resolution changes mid-stream
                                   # (e.g. TikTok multi-guest battles), instead of muxing two
                                   # resolutions into one file, which corrupts playback
min_disk_space_gb = 5.0           # Pause if disk space falls below
min_file_size_mb = 2.0            # Delete recordings smaller than this
filename_pattern = {username}_{timestamp}  # Output filename pattern

[Timeouts]
poll_interval_minutes = 3         # How often to check offline channels (fractional values OK, e.g. 0.7)
poll_jitter_percent = 20          # Random ±% added to each check
error_backoff_max_minutes = 15    # Max delay on server errors
reconnect_grace_minutes = 3       # Fast polling after a stream drops

[Cleanup]
auto_purge_days = 7               # Delete old temp files (0 = disabled)
purge_on_startup = true           # Clean up when program starts
max_log_size_mb = 20              # Rotate stream_recorder.log at startup past this size (0 = never)
log_backup_count = 3              # How many rotated logs to keep (.1 … .N)

[Clipping]
clip_length_seconds = 30          # Default length for "Clip Now" — also settable live from
                                   # the toolbar's Clip Length selector (15 sec to 30 min)
clips_dir =                       # Where clips/screenshots are saved (blank = streams_dir\Clips)
screenshot_format = jpg           # jpg (default), png, or webp
screenshot_quality = 2            # jpg: 2–31 (lower is better) | webp: 50–100 (higher is better)
                                   # png: ignored — always lossless

[Advanced]
verbose = false                   # Extra logging — see "Logging" below before turning this on
ffmpeg_path = ffmpeg              # Path to ffmpeg binary
youtube_player_client =           # yt-dlp player_client override (blank = yt-dlp's default,
                                   # which is what you want; see YouTube troubleshooting)

[GUI]
dark_mode = true                  # Start in dark mode
minimize_to_tray = true           # Minimize to system tray
notifications = true              # Desktop notifications
```

### Filename Patterns

The `filename_pattern` setting supports these tokens:

| Token | Example | Description |
| --- | --- | --- |
| `{username}` | `asmongold` | Channel name |
| `{platform}` | `twitch` | Platform name |
| `{timestamp}` | `20260211_213445` | Date and time |
| `{date}` | `20260211` | Date only |
| `{time}` | `213445` | Time only |
| `{title}` | `Playing_Elden_Ring` | Stream title (sanitized) |

Default: `{username}_{timestamp}` → `asmongold_20260211_213445.mp4`

## Directory Structure

```
E:\Streams\
├── Recorded\              # Raw recordings organized by platform
│   ├── kick\
│   │   └── asmongold\
│   │       ├── asmongold_20260211_213445.ts
│   │       └── asmongold_20260211_213445.meta.json
│   ├── twitch\
│   │   └── saruei\
│   ├── youtube\
│   │   └── OhDough\
│   ├── custom\
│   │   └── chaturbate\
│   └── fishtank\
│       └── director\
├── Processed\             # Remuxed MP4 files
│   ├── kick\
│   ├── twitch\
│   ├── youtube\
│   └── custom\
│   └── fishtank\
├── Clips\                 # Clips and screenshots grabbed from live recordings
│   ├── kick\
│   └── twitch\
├── PendingDeletion\       # Temp files awaiting cleanup
├── channels.json          # Channel list
├── config.ini             # Configuration
├── kick_channel_ids.json  # Cached Kick channel IDs (auto-created; used by push notifications)
└── cookies.txt            # Browser cookies (optional)
```

## Keyboard Shortcuts

| Key | Action |
| --- | --- |
| `Enter` | Add channel from text field |
| `Delete` | Remove selected channel |
| `Ctrl+Q` | Quit application |
| `F1` | About dialog |
| Double-click on channel | Toggle channel on/off |
| Right-click on channel | Context menu (Start/Stop Recording, Check Now, Open in Browser, Copy, Sort, Remove) |
| Right-click on status | Context menu (Restart/Stop Channel, Check Now, Clip Now, Screenshot Now, Open in Browser) |

## How It Works

1. **Monitoring**: Each channel gets its own worker process. Workers check if the stream is live at the configured polling interval with random jitter.
2. **Push notifications (Kick)**: Alongside polling, a single background WebSocket subscribes to Kick's real-time event feed for every enabled Kick channel. When Kick announces a stream going live, the matching worker is woken instantly for an immediate check — the same mechanism as the Check Now button. If the socket is ever down, workers simply continue polling; push supplements polling, never replaces it.
3. **Detection**: Kick streams are checked via streamlink (with Cloudflare JS challenge solver). Twitch streams are checked via streamlink. YouTube and custom URLs use yt-dlp's `--dump-json`. Rumble channel pages are resolved to their current live video URL.
4. **Recording**: Live streams are recorded as MPEG-TS files. Kick and Twitch use streamlink. YouTube, Rumble, and standard custom URLs use yt-dlp with ffmpeg as the HLS downloader. Custom URLs whose streams have separate video and audio playlists (CMAF/split-track HLS) are recorded using a direct ffmpeg command that follows both playlists concurrently and muxes them in real time.
5. **Stream info**: Once the output file reaches ~1.5 MB, a background ffprobe thread reads it and updates the status display with measured resolution, frame rate, and bitrate.
6. **Reconnection**: If a recording drops unexpectedly (process exits after >10 seconds of recording), the worker enters a 3-minute fast-poll mode (every 15 seconds) to catch stream reconnects.
7. **Processing**: When you click Stop (or the stream ends), raw .ts files are remuxed to .mp4 with ffmpeg (including `+faststart` for seekability), metadata sidecars are saved, and the originals are moved to PendingDeletion.

## Instant Clips & Screenshots

Right-click a channel that's currently recording in the status list for two options:

- **Clip Now** — stream-copies the last N seconds (set by the **Clip Length** toolbar selector) out of that channel's live .ts file into its own MP4, saved under `Clips\{platform}\{channel}\`.
- **Screenshot Now** — grabs a single frame from near the current end of the same .ts file as a still image.

Screenshots default to **JPEG at near-lossless quality** (~200–400 KB for a 1080p frame). Set `screenshot_format` in `config.ini` to `png` for lossless output (~2 MB per 1080p frame) or `webp` for the smallest files. Note that the quality scales run in opposite directions between formats — jpg is 2–31 where lower is better, webp is 50–100 where higher is better — so change `screenshot_quality` to match whenever you switch formats; out-of-range values are clamped to a sane default with a warning in the log.

Both read the .ts file the worker is still actively writing — the same way the stream-info probe and resolution-change watcher already do — so the main recording is never paused, restarted, or otherwise touched. Clips use `-c copy` (no re-encoding), so cutting a clip out of a multi-hour file takes a fraction of a second regardless of how long the recording has been running. Because the cut point isn't guaranteed to land exactly on a keyframe, ffmpeg snaps to the nearest preceding one, so a clip may start up to a couple of seconds earlier than requested rather than opening on a broken frame.

## Logging

MSR writes to `stream_recorder.log` in your streams directory, and mirrors the same output to the console and the GUI's **Logs** tab.

`verbose` in `config.ini` is **off by default, and should usually stay off.** Turning it on does two things: it passes `--verbose` to yt-dlp/streamlink, and it disables the stderr noise filter that normally discards per-segment chatter. Turn it on to diagnose a specific failure, then turn it back off.

A handful of patterns are dropped even in verbose mode, because they flood the log without ever being useful. The worst offender is ffmpeg's HLS demuxer announcing every playlist tag it doesn't parse (`Skip ('#EXT-X-DATERANGE…`, `#EXT-X-CUEPOINT`) — YouTube re-advertises its full set of ad markers on every playlist refresh, so in one 24/7 YouTube session those lines alone were **84% of the entire log**. "Skipped a tag I don't handle" is ffmpeg working correctly, so it's suppressed unconditionally; genuine errors and warnings are never filtered in either mode.

The log is rotated at startup once it passes `max_log_size_mb`, keeping `log_backup_count` older copies (`stream_recorder.log.1` … `.3`). Rotation happens at launch rather than continuously because recording workers are separate processes writing to the same file, and renaming a file they hold open fails on Windows. A single very long session can therefore exceed the limit; it gets trimmed the next time you start MSR.

The GUI's Logs tab trims itself to the most recent few thousand lines, so leaving MSR running for days won't grow its memory use.

## Polling Behavior

The polling system is designed to be responsive without being abusive to servers:

| State | Check Interval | Behavior |
| --- | --- | --- |
| **Offline** | Every 3 min ± jitter | Flat interval, no backoff. Catches streams within minutes of going live. |
| **Offline (Kick, push active)** | Instant + polling | Kick's event feed wakes the worker within seconds of go-live; the poll timer keeps running as a fallback. |
| **Error** | Doubles each time, max 15 min | Exponential backoff on server errors. Resets immediately on success. |
| **Reconnect** | Every 15 seconds for 3 min | Fast polling after a stream drops unexpectedly. |
| **Recording** | Continuous | No polling needed — the recording process handles the stream. |

The GUI includes a **Polling** dropdown to switch between Relaxed (5 min), Normal (3 min), and Fast (1 min) presets — or select **Custom…** to enter any interval from 0.5 to 120 minutes. A floor of 30 seconds is enforced; checking more often than that risks rate limiting or IP bans, which is exactly what the jitter system exists to prevent. Interval changes apply to running sessions immediately — workers don't need a restart, and they don't finish out their old sleep first.

The **Check Now** button skips the poll timer entirely and immediately checks every enabled channel (with a small stagger between them, so a long channel list doesn't hit its platforms all at once). To check a single channel, right-click it in the channel list or status table and select **Check Now** — available whenever the channel is offline or in error backoff. Use this when a stream just went live and you don't want to wait out the current polling cycle.

### Kick Push Notifications

Polling has an unavoidable blind spot: a stream that goes live right after a check isn't caught until the next one, so the first minute or three of a broadcast can be missed. For Kick channels, MSR closes this gap with push notifications.

At session start, MSR opens a single WebSocket to Kick's real-time event feed (the Pusher service that powers the kick.com website's own live notifications) and subscribes to every enabled Kick channel. When Kick pushes its go-live event, the matching worker is woken immediately — recordings typically begin within seconds of the broadcast starting. In the status table, Kick channels covered by push show it in the detail column:

```
Offline    push: listening · next check ~57s
```

Design notes:

- **Push supplements polling; it never replaces it.** The poll timer keeps running underneath. If the WebSocket disconnects, is blocked, or the required packages aren't installed, Kick channels behave exactly as they did before this feature existed — the `push: listening` tag disappearing from the detail column is the indicator that you're on polling only.
- **The push event is a hint, not a truth.** A go-live event triggers the normal streamlink liveness check before any recording starts, so spurious or duplicate events are harmless.
- **One connection, all channels.** Adding, removing, starting, or stopping Kick channels mid-session updates the subscriptions live.
- Channel IDs are resolved once per channel and cached in `kick_channel_ids.json`, so the Cloudflare-protected lookup happens only on the first session per channel.

Requires two optional packages: `pip install websocket-client curl_cffi`. Without them, Kick channels fall back to polling with a note in the log.

## Troubleshooting

**"ffmpeg not found"** — Install ffmpeg and make sure it's in your system PATH. Test with `ffmpeg -version`.

**YouTube 403/503 errors** — Usually expired cookies (re-export them; the GUI indicator warns you). Otherwise update yt-dlp: `pip install -U "yt-dlp[default]"`. Keep the `[default]` extra — it pulls in `yt-dlp-ejs` for YouTube's JS challenge.

**YouTube: detected as live, but recording dies immediately with "No video formats found!"** — YouTube requires a GVS PO Token for certain player clients, and binds it to the video ID. If a client is pinned that needs one, yt-dlp finds no downloadable formats even though the stream is plainly live — the give-away is that detection keeps working while every recording ends in seconds (often tripping the micro-fragment throttle). Leave `youtube_player_client` blank in `config.ini`: yt-dlp's defaults deliberately favour clients that don't need a PO Token. Only set it to work around a specific regression, and install [Deno](https://deno.com/) so the JS challenge can be solved properly.

**TikTok stream not detected** — Check that `cookies.txt` has current TikTok cookies; `msToken` and `ttwid` expire periodically and need re-exporting from a logged-in session. US-based streamers on TikTok's regional infrastructure are handled automatically by a Webcast API fallback, which logs `"Webcast API confirms LIVE"` when it fires.

**TikTok: "No video formats found"** — The channel was added as a `custom:` URL pointing at the bare profile instead of the live endpoint. Re-add it using the `tiktok` platform dropdown.

**Kick 403 errors / streams not recording** — Kick needs streamlink 8.0+ (`pip install -U streamlink`), which has the built-in Cloudflare challenge solver. Also check `%APPDATA%\streamlink\plugins\` for an old third-party `kick.py` and delete it — it overrides the built-in plugin and breaks the bypass.

**Kick push: no "push: listening" in the status column** — Push needs two optional packages: `pip install websocket-client curl_cffi`. The session-start log says which is missing. Recording still works normally via polling either way.

**Kick push: was working, stopped working** — If the socket connects but no `Kick push: listening for <channel>` lines follow, Kick has rotated their Pusher application key. Open any kick.com page with DevTools → Network → **WS**, copy the new `wss://ws-usX.pusher.com/app/...` URL, and update `KICK_PUSHER_URL` near the top of the script.

**Rumble streams not detected** — Check the log for the `Rumble HTML check:` line to confirm it's fetching the right channel page. For 403s, install `curl_cffi` for browser impersonation. Note that Rumble periodically restructures its channel pages; if detection breaks after a site change, the `parse_rumble_channel_html` function is the single place that reads their page format.

**Twitch recordings have no audio** — Rare, and usually a streamlink version issue: `pip install -U streamlink`.

**Fishtank recordings have no audio** — Fishtank's CDN sometimes serves video-only segments on certain cameras (notably Cameraman). Server-side; the recorder can't fix it, but logs `⚠ NO AUDIO TRACK` after remux so affected files are easy to find.

**Fishtank: many short recordings in a row** — Their CDN resetting connections every 20–30 seconds during busy periods. MSR detects this after 5 consecutive short recordings and backs off. Normal behavior.

**Large .ts files left in Recorded** — Raw recordings that were never remuxed, usually from force-quitting instead of using Stop. Restart and they're cleaned up automatically. To remux one by hand: `ffmpeg -i recording.ts -c copy -movflags +faststart output.mp4`

**Screenshot fails with a decode error** — The very end of a live .ts is an incomplete frame, so grabs are taken ~3 seconds behind the live edge, retrying further back if needed. A failure here usually means the recording only just started; wait a few seconds and try again.

**Clip fails: "could not read recording duration"** — Fixed in v1.8.0. MPEG-TS has no container-level duration field, so ffprobe derives one by seeking to the end of the file; on some live captures that derivation returns N/A even though the recording is perfectly healthy. Clipping no longer requires it — it falls back to seeking relative to the end of the file (`-sseof`), the same method screenshots use, which is why screenshots kept working when clips didn't.

**Program won't close** — Use Ctrl+Q if the window is unresponsive. Shutdown is layered: graceful stop → process tree kill → orphan cleanup → `os._exit` as a final backstop.


## Fishtank.live

[Fishtank.live](https://www.fishtank.live/) is a reality TV live streaming site with multiple camera feeds broadcasting simultaneously. MSR supports recording any of its cameras with automatic authentication and live detection.

### Setup

Add a `[Fishtank]` section to your `config.ini` with your fishtank.live account credentials:

```ini
[Fishtank]
email = your@email.com
password = yourpassword
```

A free account is sufficient. The program handles authentication automatically — it logs in, obtains a 24-hour stream token, and refreshes it as needed. No manual cookie export is required.

### Adding Cameras

Select **fishtank** from the platform dropdown and enter a camera name:

```
fishtank:director
fishtank:kitchen
fishtank:bar
```

Or use raw stream IDs directly (e.g. `dirc-5`, `dmrm-5`) if you know them.

### Available Cameras

All Season 5 cameras are supported:

| Camera name | Room | Access |
|---|---|---|
| `director` | Director Mode | Free |
| `dorm` | Dorm | Normal |
| `dormalt`, `dorm2`, `dmrm2` | Dorm Alternate | Normal |
| `closet` | Closet | Normal |
| `kitchen` | Kitchen | Normal |
| `bar` | Bar | Normal |
| `barptz` | Bar PTZ | Season Pass |
| `hallway` | Hallway | Normal |
| `dining` | Dining Room | Normal |
| `market` | Market | Normal |
| `marketalt`, `market2` | Market Alternate | Normal |
| `foyer` | Foyer | Normal |
| `glassroom` | Glassroom | Normal |
| `corridor` | Corridor | Normal |
| `eastwing` | East Wing | Normal |
| `westwing` | West Wing | Normal |
| `laundry`, `laundryroom` | Laundry Room | Season Pass |
| `jungle` | Jungle Room | Season Pass |
| `computerlab`, `bbcl` | Computer Lab | Season Pass |
| `jobboard`, `job`, `jobb` | Job Board | Normal |
| `confessional` | Confessional | Season Pass |
| `cameraman` | Cameraman | Season Pass XL |
| `arena` | Arena | Normal |
| `goofactory`, `goofact`, `goo`, `br3g` | Goo Factory | Normal |

Short aliases also work — `cam` for Cameraman, `dirc` for Director, `dmrm` for Dorm, etc. Raw stream IDs like `dirc-5` and `dmrm-5` are accepted directly. Some rooms require a **season pass** subscription.

> **Season 5 renames**: `balcony` → `eastwing` (`bkny-5`), `hallwayup` → `westwing` (`hwup-5`), `hallwaydown` → `hallway` (`hwdn-5`), `jacuzzi` → `laundry` (`jckz-5`). The old aliases still work for backwards compatibility.

> **Fishtank-only preset**: A `channels_fishtank.json` file is available in the repository with all 24 Season 5 cameras pre-populated. Rename it to `channels.json` and enable whichever cameras you want to record.

### How It Works

Detection and recording both use fishtank.live's HLS streams served by MistServer. On each poll the program queries the live-streams API to check which cameras are active, then records via ffmpeg's HLS downloader. The JWT token is obtained from the API at login and refreshed automatically — a background thread monitors the token's expiry and renews it proactively with a 5-minute buffer, preventing coverage gaps on streams like Cameraman that are issued short-lived 30-minute tokens.

Before each recording starts, MSR fetches the HLS master playlist and selects the highest-bandwidth variant automatically — this ensures recordings are always captured at the best available quality rather than whichever rendition the server happens to list first.

After each recording is remuxed, MSR verifies that the output MP4 contains an audio track. Fishtank's CDN occasionally delivers video-only HLS segments; a clear warning is logged immediately so affected files are easy to identify.

## Platform Notes

**Kick**: Uses streamlink for both detection and recording. Streamlink 8.x includes a built-in Cloudflare JS challenge solver that handles Kick's aggressive bot detection. Cookies are optional. Streams are recorded in 1080p by default. With `websocket-client` and `curl_cffi` installed, Kick channels also get **push notifications** — go-live events arrive over Kick's own WebSocket feed and recordings start within seconds instead of waiting for the next poll (see *Kick Push Notifications* above). **Important**: Remove any old third-party `kick.py` plugins from `%APPDATA%\streamlink\plugins\` — they override the built-in plugin and break Cloudflare bypass.

**Twitch**: Uses streamlink with `--twitch-disable-ads` and `--twitch-low-latency`. Cookies are optional but enable subscriber-only features.


**TikTok**: Uses yt-dlp for detection and streamlink for recording. Add channels by selecting **tiktok** from the platform dropdown and entering the username (with or without `@`). Valid TikTok session cookies are required — export them from a logged-in browser session using the Get cookies.txt extension and place the file in your streams directory. The `msToken` and `ttwid` cookies are the critical ones; when they expire, re-export.

TikTok's CDN delivers mobile-portrait video (typically 540×960 or 720×1280) at 25 fps. This is normal — it reflects what the streamer's phone is broadcasting.

**US-TTP regional detection**: A majority of US-based TikTok streamers (especially gaming streamers) run on TikTok's US regional infrastructure (`webcast.us.tiktok.com`). yt-dlp's TikTok extractor currently hardcodes the global endpoint (`webcast.tiktok.com`), which returns "offline" for these accounts even when they are actively live. MSR includes a fallback that directly queries both regional Webcast API endpoints whenever yt-dlp reports a TikTok channel as offline. If the fallback confirms the stream is live, recording proceeds normally — no user action required. The log will note `"Webcast API confirms LIVE"` when this override fires.

**YouTube**: Uses yt-dlp. Cookies are recommended to avoid throttling. Install [Deno](https://deno.com/) so yt-dlp can solve YouTube's JS n-challenge — without it, recordings tend to drop out every ~15 seconds. Player client selection is left to yt-dlp by default, which is deliberate: YouTube requires PO Tokens for some clients, so pinning one in `youtube_player_client` is a good way to break recording (see Troubleshooting). YouTube Live DVR streams work but may produce "keepalive request failed" messages in verbose mode — harmless.

**Rumble**: Select **rumble** from the platform dropdown and enter the channel name (e.g. `BadlandsMedia`), or add the full channel URL as a custom URL. Both `/c/Name` and `/user/Name` styles work — MSR tries `/c/` first and falls back to `/user/` automatically.

Live detection reads the JSON payload Rumble embeds in its channel page (the same data the site renders its video grid from), which is considerably more durable than matching CSS classes in the markup. A video counts as live only when its `live` flag is set — an *ended* livestream keeps its DVR playlist and can still show thousands of concurrent viewers, so treating "has a stream URL" as "is live" would re-record finished broadcasts on every poll. Once a live entry is found, its video page URL is handed to yt-dlp to resolve the actual stream. For custom Rumble URLs, yt-dlp's playlist scan is tried first and this HTML check acts as a fallback, so detection survives the extractor lagging behind a site redesign. If Cloudflare blocks access, install `curl_cffi` for browser impersonation.

**Fishtank.live**: Uses ffmpeg's HLS downloader with a token obtained from the Fishtank API. Requires a `[Fishtank]` section in `config.ini` with your account email and password. All Season 5 cameras are supported. Tokens are refreshed proactively by a background thread (5-minute buffer before expiry) — important because some streams like Cameraman are issued 30-minute tokens rather than the usual 24-hour ones. After each remux, audio track presence is verified and a warning is logged if a recording is silent. No cookies required.

**Custom URLs**: Uses yt-dlp, which supports [1,800+ sites](https://github.com/yt-dlp/yt-dlp/blob/master/supportedsites.md). Direct `.m3u8` HLS links also work. Select "custom" from the platform dropdown and paste the full URL.

For platforms that serve CMAF HLS with separate video and audio playlists, MSR automatically detects the split-track format during the stream check and routes recording through a direct ffmpeg command rather than yt-dlp's download pipeline. This handles sites like Chaturbate where yt-dlp would otherwise fail to merge the tracks for a live stream. No configuration is required — detection and routing happen transparently.

## Requirements

| Dependency | Required | Purpose |
| --- | --- | --- |
| Python 3.10+ | Yes | Runtime |
| ffmpeg + ffprobe | Yes | Remux .ts → .mp4; stream info detection |
| yt-dlp | Yes | YouTube, Rumble, custom URL recording |
| streamlink | Yes | Kick and Twitch stream recording |
| psutil | Recommended | Clean process management |
| curl_cffi | Recommended | Rumble Cloudflare bypass; Fishtank.live API (HTTP/3); Kick push channel-ID lookup |
| websocket-client | Recommended | Kick push notifications (instant go-live detection) |
| pystray + Pillow | Optional | System tray icon |
| plyer | Optional | Desktop notifications |

## Author

Created by ManletPride, built with assistance from Claude (Anthropic) and Grok (xAI).

## License

MIT License. See [LICENSE](LICENSE) for details.
