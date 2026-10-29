# v1.8.0 — Instant Clips & Screenshots

Grab a clip or a screenshot from a stream **while it's still recording**, without stopping and restarting the channel. This release also fixes Rumble and YouTube recording, both of which broke due to site-side changes.

## Instant Clips & Screenshots

Right-click any channel that's currently recording in the status list:

- **Clip Now** — saves the last N seconds of the live stream as its own MP4
- **Screenshot Now** — grabs a still frame

Neither one pauses, restarts, or otherwise touches the ongoing recording. Clips are a pure stream copy (no re-encoding), so pulling 30 seconds out of a multi-hour recording takes a fraction of a second no matter how long the stream has been running.

Clip length is set from a new **Clip Length** selector in the toolbar — presets from 15 seconds to 5 minutes, or a custom value up to 30 minutes. The setting applies immediately and persists between sessions.

Output goes to `Clips\{platform}\{channel}\` in your streams directory. Screenshots default to JPEG (~200–400 KB for a 1080p frame); PNG and WebP are available via `screenshot_format`.

## Fixes

**Rumble live detection** — Rumble redesigned their channel pages and removed the CSS class detection relied on, so live channels were never detected. Detection now reads the JSON payload the site renders its video grid from, which is considerably more durable. Rumble channels also resolve proper stream titles now, and `/user/Name` channels work alongside `/c/Name`.

**YouTube recording** — YouTube now requires a PO Token for the `web` player client, which MSR was pinning. Streams were detected as live but every recording failed instantly with `No video formats found!`. Client selection is now left to yt-dlp, which prefers clients that don't need one. Override with `youtube_player_client` if you ever need to.

**TikTok clips and cleanup** — TikTok channels entered as `@handle` recorded to one folder but were looked up in another. This broke clips and screenshots, and also silently disabled the guard that stops cleanup touching an in-progress recording.

**Logging** — `verbose` defaulted to on despite the docs saying otherwise, and ffmpeg's HLS ad-cuepoint chatter could account for **84% of the entire log** on a 24/7 YouTube channel. That chatter is now always suppressed, and `stream_recorder.log` rotates at startup instead of growing forever.

**Interface** — Fixed the Clip Length label rendering white in dark mode, and the About button being clipped to "At" at narrower window widths.

## New config options

```ini
[Clipping]
clip_length_seconds = 30   # Length for "Clip Now" (also set from the toolbar)
clips_dir =                # Blank = streams_dir\Clips
screenshot_format = jpg    # jpg, png, or webp
screenshot_quality = 2     # jpg: 2–31 (lower is better) | webp: 50–100 (higher is better)

[Advanced]
youtube_player_client =    # Blank = let yt-dlp choose (recommended)

[Cleanup]
max_log_size_mb = 20       # Rotate the log at startup past this size (0 = never)
log_backup_count = 3       # How many rotated logs to keep
```

## Upgrading

Drop in the new `Multi-Stream-Recorder.py` over the old one (v1.8.0 is still a single file). New settings are added to your existing `config.ini` automatically on first launch. The `msr\` package arrives in [v2.0.0](RELEASE_NOTES_v2.0.0.md).

One thing to note: settings already present in your `config.ini` keep their current values, so **existing users need to set `verbose = false` manually** to get the quieter logging. Fresh installs get it by default.

No changes to channel lists, recordings, or directory layout — `Clips\` is created alongside `Recorded\` and `Processed\` the first time you save a clip.
