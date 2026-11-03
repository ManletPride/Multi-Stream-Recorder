# Multi-Stream Recorder v3.1.2

A small feature release for the Clip Editor: zoom into the timeline, and an optional audio waveform.

## New

- **Zoom into the Clip Editor timeline.** On a long recording, one pixel of the timeline can cover many seconds, which makes exact In/Out marks hard. Now you can zoom in:
  - **Alt+mouse wheel** (or the **+** and **-** keys) zooms in and out around the mouse pointer.
  - **0** or a **double-click** on the timeline shows the whole recording again.
  - **Ctrl+mouse wheel** or **Ctrl+Left/Right** steps the playhead. The step gets smaller the further you zoom in, so you can land on the exact moment.
  - **Mouse wheel** or **Shift+mouse wheel** pans the zoomed view left and right.
  - While playing, the view follows the playhead so it never runs off screen.
  - A small bar shows which part of the recording you are looking at.
- **Optional waveform.** Tick **Waveform** in the Clip Editor to see the audio level under the timeline, so you can spot talking, laughs or loud moments at a glance. It is off by default and MSR remembers your choice. It is built in the background with ffmpeg, so the editor never freezes, and saved on your PC so reopening the same recording is instant. On a recording that is still going, it keeps up as the file grows, and reads the last stretch once more when the recording ends or a new part starts.

## Upgrade notes

- Replace the program files as usual, or use the in-app update. Your `config.ini`, `channels.json`, cookies and recordings stay as they are.
- No new settings and no new packages. The waveform uses the ffmpeg you already have, and its cache lives in `%LOCALAPPDATA%\Multi-Stream-Recorder\cache\waveforms` (safe to delete).

Full list of changes: [CHANGELOG.md](https://github.com/ManletPride/Multi-Stream-Recorder/blob/v3.1.2/CHANGELOG.md).