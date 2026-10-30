# Multi-Stream Recorder 3.0.0

> Released 2026-10-30.

Multi-Stream Recorder watches your favourite live channels on Kick, Twitch, YouTube, Rumble, TikTok and any yt-dlp site, and records each one automatically as an MP4. 3.0 is the biggest update so far.

## Highlights

### Clip Editor
Cut a clip out of a recording, even while it is still recording. Open it with the new **DVR** button right next to **Clip Now**, from a channel's right-click menu, or from **Library > Files**.
- Timeline over the whole recording with **Jump to live**; scrub with an instant preview.
- **Play with sound** and a volume slider, so you can cut right on a line of dialogue.
- Smooth preview at the stream's own frame rate up to 60 fps, or **Limit preview to 30 fps** on slower PCs.
- **Mark In / Mark Out** (I / O keys), nudge and replay buttons.
- **Save clip** (or Ctrl+S / Enter) is a lossless copy from the nearest keyframe; tick **Exact** for frame-exact edges.

### Library
A new tab that shows how much space your recordings take per site, channel and month, with charts, and lets you clean up safely: every action is previewed first, the default is a reversible move to PendingDeletion with Undo, and recording channels and recent files are never touched.

### Easy install, its own environment
Double-click `run_msr.pyw`. The first launch sets up MSR's own Python environment (`.venv`) in about a minute and installs everything there; after that it starts at once. Updating MSR's packages never changes the Python other programs on your computer use. If setup can't run (for example offline), MSR still starts and tries again next time. Prefer to manage Python yourself? Set `MSR_NO_VENV=1` or `[Advanced] use_venv = false`.

### Updates dialog
Click the version text at the bottom left, or **Updates…**: see the installed and latest versions of MSR, yt-dlp, streamlink, ffmpeg and the other packages, and update them with one click. Running recordings are not interrupted, a failed update is rolled back, and a daily background check shows **Updates •** when something new is out.

### Control from scripts, bots and AI agents
Everything the window does can be done from the command line with `msr_ctl.py`, in the window or in `--headless` mode: add and remove channels, start and stop, clip, wait for a stream to go live or a recording to finish, check health, manage the library, update. It only listens on your own computer (127.0.0.1) with a random token. See `docs/AGENT_GUIDE.md`.

### Looks
- 16 themes with a live preview (PowerShell Blue stays the default) plus your own theme files.
- An app icon and its own taskbar entry; a tray icon with a red dot while recording.
- Themed dialogs everywhere, windows that remember their size and place, a friendlier Logs tab with filter and search.

## Safer and more reliable
- **Recording starts immediately** after a crash; leftover files are converted in the background.
- **Unplugged drive?** Recording continues in a fallback folder and moves back when the drive returns.
- **Your footage is protected:** every MP4 is checked before the original `.ts` is moved, a failed conversion keeps the original, a stalled ffmpeg is stopped.
- **In-app updates are all-or-nothing** with a backup and automatic rollback, and from 3.0 on they bring every program file (launchers, `msr_ctl.py`, icon, docs) while never touching your settings, cookies or `.venv`.
- **Privacy:** stream URLs, tokens and passwords are redacted from logs, status files and settings history.
- **No console windows** flash up for ffmpeg, yt-dlp or streamlink.
- Faster startup (the window appears in about half the time), `max_record_hours` is enforced, Fishtank token handling fixed, and many smaller fixes. Full list in `CHANGELOG.md`.
- Removed: the unused `[Advanced] concurrent_fragments` setting (old configs that have it still load).

## Upgrading from 2.1.0
- Close MSR first.
- **Download this release instead of using the in-app update.** The 2.1.0 in-app updater only replaces the main program files, so it would skip the new launcher (`.venv` setup), `msr_ctl.py`, the icon files and the docs. Updates from 3.0.0 on deliver all of these.
  - **Git:** `git pull` in your MSR folder.
  - **ZIP:** download the 3.0.0 ZIP from the GitHub release and extract it over your MSR folder (replace files). Your settings files are not in the ZIP, so nothing of yours is overwritten.
- Start MSR with `run_msr.pyw` (double-click) or `run_msr.bat`. Optional: `python create_shortcut.py` makes a desktop shortcut with the new icon; it launches through `run_msr.pyw`, so it uses MSR's own environment (`.venv`).
- Your `config.ini`, `channels.json`, cookies and recordings are kept and used as before; the old Dark Mode setting is migrated to a theme.
- The first launch takes about a minute (setting up `.venv`); later launches are fast.
- Requirements are unchanged: Python 3.10+ (3.12 tested), ffmpeg on PATH, Deno recommended for YouTube.

## Thanks
Theme palettes are credited in `THIRD_PARTY_NOTICES.md`. Built by ManletPride with assistance from Claude (Anthropic) and Grok (xAI). MIT License.
