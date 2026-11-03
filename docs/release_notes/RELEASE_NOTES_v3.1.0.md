# Multi-Stream Recorder v3.1.0

A feature release on top of 3.0: record YouTube broadcasts **from the start**, opt-in **Twitch go-live push**, a new **Options** tab, a **Health** line, optional **sounds**, a **Recordings** list in the Library, much more **agent control**, and encrypted secrets on Windows. No new Python packages. Your `config.ini`, `channels.json`, cookies and recordings are used as before.

## New

- **YouTube: record from the start** (experimental). When MSR finds a YouTube broadcast live within its first 15 minutes, it records from the very beginning, then follows live. A start between two checks is no longer lost.
  - Set the limit in **Options > YouTube** (0-7200 min, 0 = no limit), or turn it off. The limit is never shorter than the check interval + jitter + 2 minutes.
  - While it catches up, the status says *catching up from start*. Clip Now, Screenshot and the live Clip Editor show *Available once caught up to live*. The preview shows how far it has downloaded, not live.
  - Once it reaches live, MSR closes that part and carries on with a normal live recording, so Clip Now works again.
- **Twitch go-live push** (opt-in). **Options > Twitch > Connect**: enter the code at twitch.tv/activate once, and MSR hears about go-live within seconds, like Kick. Up to 10 channels (Twitch's limit). Polling keeps running for everything. **Manage on Twitch** and **Disconnect** are right there.
- **Options tab**: every common setting in one place, grouped, with a hint for what 0 or blank means and when each change applies. Fishtank **Test login**, ffmpeg **Check** and an **Updates** shortcut.
- **Health line** in the bottom bar: checks tools, free space, stalled recordings and sites that keep failing, at start and every 10 minutes (`[Health] check_minutes`). It also tells you when yt-dlp or streamlink is out of date.
- **Sounds** (off by default): a go-live chime, a problem alert and a clip tick, each with a Test button, quiet while Windows is locked.
- **Library > Recordings**: each channel's recordings with date, length and stream title, search and sort, Play, Open folder and Clip Editor. Clips are listed under the recording they were cut from.
- **Agent control** (`msr_ctl.py`, same API version): Twitch status / connect / disconnect, change any Options setting with the same checks as the window, sound tests, `health --check-now`, browse recordings and their clips, and `subscribe` for a live event stream (new events: `health_problem`, `disk_low`, `update_available`, `library_action`). See [docs/AGENT_GUIDE.md](docs/AGENT_GUIDE.md).

## Improved

- **First-time setup window** shows each step, the elapsed time and a small live log. If setup fails, the log stays open with a Continue button. Themed from the first frame, no white flash.
- **Library Clean up** now holds the old Files tab: suggestions on top, a tickable file list below.
- After a Kick or Twitch push connection drops and comes back, MSR runs one catch-up check so a go-live in between isn't missed. Ordinary reconnects log a single line.
- Agent commands in the window run through one queue on the window's thread. That makes them steadier under load, and a command that can't start within 30 s says so (`busy`).
- Only one Twitch sign-in can run at a time, whether it starts from the window or from agent control.

## Fixed

- From-start YouTube recordings no longer make Clip Now fail with "no active recording" or cut an older leftover file.
- The recordings folder can't be changed while recording (it applies at the next start).

## Security

- On Windows, **`twitch_token.json` and the Fishtank password are encrypted** with your Windows account (DPAPI). Existing plain values are converted on first start. Other systems are unchanged.
- **`msr_control.json`** (the agent control port and token) can be read only by your Windows user.
- Twitch reconnects only follow `wss://*.twitch.tv` addresses, and MSR only opens `twitch.tv/activate` links.
- Options warns when `cookies.txt` is in a synced folder (OneDrive, Dropbox, Google Drive...). Cookies are your browser login.

## Upgrade notes

- **Going back to an older version?** After 3.1 encrypts it, older MSR versions see the **Fishtank password as blank**. Type it again there. The Twitch sign-in also has to be redone in an older version.
- **YouTube from the start saves two files** for a broadcast it catches from the beginning: the from-start part and the live part after it.
- **yt-dlp marks `--live-from-start` as experimental.** If YouTube changes something it may stop working; turn it off in Options > YouTube and MSR records live as before. Downloading a finished broadcast (VOD) is a separate, future feature.
- In-app update from 3.0 works. From 2.1, download the release instead (see the 3.0 notes).
- New settings (all optional, defaults shown): `[YouTube] live_from_start = true`, `live_from_start_max_minutes = 15`; `[Twitch] push_enabled = false`; `[Health] check_minutes = 10`; `[Sounds] go_live / problems / clip_saved = false`, `quiet_while_locked = true`.

## Thanks

MSR stands on these projects:
- [yt-dlp](https://github.com/yt-dlp/yt-dlp): YouTube, Rumble, TikTok and custom capture, including `--live-from-start`
- [streamlink](https://streamlink.github.io/): Kick and Twitch
- [FFmpeg](https://ffmpeg.org/): remux, merge, clips, previews
- [Twitch EventSub](https://dev.twitch.tv/docs/eventsub/): go-live push
- [websocket-client](https://github.com/websocket-client/websocket-client) and [curl_cffi](https://github.com/lexiforest/curl_cffi): realtime push and site requests
- Theme palettes from the [Omarchy](https://omarchy.org/themes/) community (see THIRD_PARTY_NOTICES.md)

Full list of changes: [CHANGELOG.md](CHANGELOG.md).