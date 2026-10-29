# v2.0.1 — Size splits no longer pause recording

Hitting the 8 GB file limit (or whatever you set in `max_file_size_gb`) no longer stops the recording while the closed file remuxes. The next file starts first; the two overlap by a few seconds instead of dropping live audio. Clip Now and Screenshot keep working, and the status row shows remux progress.

This release also improves TikTok live detection when yt-dlp reports a 24/7 channel as offline.

## Upgrading from v2.0.0

Copy the updated `msr\` folder (and `Multi-Stream-Recorder.py` if it changed) over your existing install. Fully **quit and relaunch**. The running GUI does not reload `msr/`.

Your `config.ini`, `channels.json`, `cookies.txt`, and recordings stay where they are. No new config keys.

Still on v1.8.0? You also need the `msr\` folder — see [RELEASE_NOTES_v2.0.0.md](RELEASE_NOTES_v2.0.0.md).

## What you will notice

**Size splits (default 8 GB)**

- The next file starts **while the current one is still recording**.
- The old file is closed only after the new `.ts` has data, so the join **overlaps** by a few seconds instead of leaving a hole.
- Remux of the closed file runs in the **background**. Status stays Recording and shows `remuxing N%` (the row uses the remux color).
- **Clip Now** and **Screenshot** keep working during that remux.
- If the next file does not appear in time, MSR falls back to stop-then-restart (a short gap).
- A mid-stream **resolution change** (for example a TikTok guest battle) still ends the current file first, so one recording does not mix two frame sizes.

v2.0.0 stopped capture for the whole remux (~90 seconds on an 8 GB Kick file) and blocked Clip Now until it finished.

**TikTok**

- When yt-dlp reports a channel offline, MSR confirms via TikTok’s live APIs. 24/7 news LIVEs often look offline to yt-dlp.

See [CHANGELOG.md](CHANGELOG.md) for the full list.
