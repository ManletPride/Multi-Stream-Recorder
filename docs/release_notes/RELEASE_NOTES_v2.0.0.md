# v2.0.0 — Package split, clip fixes, and hardening

v1.8.0 was still a single script. This release splits the app into a package, fixes clips on Kick/Twitch live MPEG-TS, and adds the Status-header tools, nested custom-URL folders, and the security work that landed after 1.8.0.

That is a **breaking install change**: copying only `Multi-Stream-Recorder.py` is no longer enough.

## Upgrading from v1.8.0

Copy **both** of these into the same folder (the folder you already run from):

- `Multi-Stream-Recorder.py` (launcher)
- the `msr\` directory (GUI, workers, per-site recorders)

Fully **quit and relaunch**. The running GUI does not reload `msr/`.

Your `config.ini`, `channels.json`, `cookies.txt`, and recordings stay where they are. New settings are added to an existing `config.ini` on first launch. Fresh installs default `streams_dir` to `%USERPROFILE%\Videos\Multi-Stream Recorder` (Windows) or `~/Videos/Multi-Stream Recorder` (Linux/macOS); an existing `streams_dir` is not overwritten.

Optional: `tests\`, `SECURITY.md`, and `.github\` if you want tests or the security notes.

## What you will notice

**Clips and screenshots**

- **Clip Now**, **Screenshot**, and **Clips** buttons on the Status header (same actions as right-click).
- Kick/Twitch live `.ts` files often have a PCR clock of ~10000s while the file is only minutes long. Cuts now use **elapsed** file time, so a 15s clip is ~15s instead of a couple of seconds at the live edge, and the first GOP is not frozen.
- One clip or screenshot at a time per channel; extra clicks wait until it finishes.

**Status and roster**

- Disk write and NIC download meters (needs `psutil`). Hover for this app’s recording totals.
- **Clips** opens that channel’s clips/screenshots folder even if it is not recording.
- **Top** / **Move to Top** / **Move to Bottom** on the roster.
- **Open in Browser** uses the right site for Rumble, TikTok, and Fishtank (not Kick). YouTube `@handle` is no longer doubled to `@@handle`.

**Recordings**

- Chaturbate-style `https://site.com/user/` URLs go in `custom/<site>/<user>/` instead of one bag per site.
- Rumble records the channel-page HLS playlist with ffmpeg when it is present, so yt-dlp does not have to open Cloudflare-gated video pages.

**Safety**

- Channel names and custom URLs are checked on add and when `channels.json` loads.
- Network ffmpeg cannot follow `file://` URLs.
- Logs redact JWTs, Cookie, and Authorization headers.

See [CHANGELOG.md](CHANGELOG.md) for the full list and [SECURITY.md](SECURITY.md) for what is stored on disk.
