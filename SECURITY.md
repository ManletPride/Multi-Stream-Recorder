# Security

## Reporting a vulnerability

Please **do not** open a public GitHub issue for a vulnerability that could put users' logins or recordings at risk.

Email the maintainer through GitHub (`ManletPride`) or open a [private security advisory](https://github.com/ManletPride/Multi-Stream-Recorder/security/advisories/new) on the repository. Include:

- MSR version (`python Multi-Stream-Recorder.py --version`)
- OS
- Steps to reproduce
- What you expected vs what happened

You should hear back. Do not share `cookies.txt`, `config.ini` passwords, or unredacted `stream_recorder.log` in public.

## What this app stores

All of this stays on **your machine**. MSR does not phone home except a GitHub “latest release” check from the GUI. If you click the update banner, it also downloads that release’s source zip from GitHub (HTTPS, github.com / api.github.com / codeload.github.com only) and copies `Multi-Stream-Recorder.py` plus `msr/` over the install folder. It does not send `config.ini`, cookies, or recordings.

| File | Sensitivity |
| --- | --- |
| `cookies.txt` | **Your site logins.** Treat it like a password. Left as a Netscape jar on purpose: yt-dlp and streamlink read it themselves, and MSR never rewrites it. |
| `config.ini` `[Fishtank]` email/password | Password encrypted at rest. Windows: DPAPI (`password_dpapi`). Linux and other systems: `password_box`, unlocked by `~/.config/multi-stream-recorder/secret.key` (mode 0600, outside the data folder). The email is not a secret and stays in the file. Login is the reliable auth method; cookies only work for about 15 minutes after export. |
| `channels.json` | Who you record. |
| `twitch_token.json` | Twitch push sign-in. Windows: DPAPI. Elsewhere: the same user key as the other secrets (`box`). Mode 0600 on Linux. |
| `config.ini` `[Telegram]` `api_hash`, `bot_token` | Bot credentials. Not stored in the clear. Windows: DPAPI (`api_hash_dpapi`, `bot_token_dpapi`). Linux and other systems: `api_hash_box` / `bot_token_box`, unlocked by the same `secret.key` as the Fishtank password (mode 0600, outside the data folder). The api id, group id, topic id and your user id are not secrets. |
| `telegram_session.json` | The bot's MTProto session, encrypted the same way (DPAPI or the user key). Mode 0600. Not a login to your personal account. |
| `msr_control.json` | Control port and token, readable only by your user; removed when MSR quits. Not encrypted: the client has to read it as JSON, and it does not outlive the process. |
| `stream_recorder.log` | Commands and URLs; JWTs and Cookie headers are redacted, but do not paste raw logs in public. |
| Recordings / clips | Copyrighted and possibly adult content you chose to save. |

`.gitignore` excludes those files. Never `git add -f` them.

On Linux and other non-Windows systems an existing plain Fishtank password or Twitch token is encrypted the next time MSR starts. If the key file is missing or the wrong length, a value that is still plain is left as it is, and a ciphertext MSR cannot open stays in the file and asks you to enter it again. MSR does not delete the saved value and does not crash.

The user key, when it is written, is 32 random bytes in binary mode. A newline or Ctrl+Z in those bytes used to be rewritten on Windows, so the file was no longer a valid key. On Windows the key file is not created with mode 0600, because Python 3.14 turns that mode into an access list a later open cannot read. Windows still stores the secrets with DPAPI; the key file is the Linux (and other non-Windows) store.

Left unencrypted on purpose: `cookies.txt`, the Fishtank email, the Twitch public client id, `MSR_FISHTANK_PASSWORD` (it is only in the environment), and the `msr_control.json` token.

## What MSR will and will not do

- Custom URLs must be `http://` or `https://`. `file:` and unknown schemes are rejected.
- Network ffmpeg (Fishtank, Rumble HLS, Chaturbate merge, yt-dlp’s ffmpeg downloader) uses a protocol whitelist **without** `file`.
- Channel names cannot contain `:` `/` `\` that would turn into extra directories.
- Logs go through `redact_for_log` (argv, stderr, exceptions, worker channel tag).
- Twitch reconnect URLs must be `wss://*.twitch.tv`; only `twitch.tv/activate` links are opened.
- Worker processes only receive the credentials they need: Telegram credentials stay in the main process, `[Fishtank]` credentials are only handed to Fishtank workers, and `MSR_FISHTANK_PASSWORD` is stripped from child tool environments (`ffmpeg`, `ffprobe`, `streamlink`, `yt-dlp`, `pip`, `git`, etc.).
- Encrypted secrets (v2 format) are context-bound to their config field or file purpose (AES-GCM AAD on non-Windows, DPAPI optional entropy on Windows) so a ciphertext cannot be swapped across fields, while legacy v1 blobs remain readable.
- Automatic backups of `config.ini` (`msr_history/` and `config.ini.broken-*`) strip secret values before writing.
- Remote control (`msr_ctl.py` / `ControlServer`) refuses changes to host paths and executable binaries (`Advanced.ffmpeg_path`, `Clipping.clips_dir`, `Paths.channels_file`, `Paths.fallback_dir`, `GUI.window_state_file`) with `forbidden`.
- On POSIX systems, default fallback storage and live preview thumbnail caches live under private `0700` `$XDG_CACHE_HOME/multi-stream-recorder/` directories (cleaned on exit for previews), and the Twitch token lock file lives in the data folder (`.twitch_token.lock`) rather than world-shared `/tmp`.
- The Options tab warns when `cookies.txt` is in a synced folder (OneDrive, Dropbox...).
- MSR **never** refreshes `cookies.txt`. When tokens expire, you re-export from a logged-in browser.

A custom URL is fetched and recorded as you pasted it. Only add URLs you trust.

## Threat model (short)

This is a same-user desktop app. Someone with your OS user account can already read your files. The design goals are:

1. A hostile livestream playlist must not pull `file://` paths into a recording.
2. A malicious `channels.json` must not write outside the recordings folder.
3. Sharing a log or screenshot should not leak JWTs, session cookies, or a Telegram bot token.
4. A Telegram upload goes only to the configured group, into the global topic or that channel's topic, from a bot login. MSR does not read group messages and does not log in as the user. The local file is deleted only when that option is on and every part was confirmed.

### Multi-user Linux caveats

- **`/proc/<pid>/cmdline` visibility:** While recording streams that authenticate through `ffmpeg` command-line arguments — specifically Fishtank stream URLs carrying `?jwt=...` and split-track merges or streams passing `-headers` (`Cookie:` / auth headers) — those arguments are visible in `/proc/<pid>/cmdline` (and tools like `ps`) to other local users on Linux systems whose `/proc` is mounted without `hidepid` (e.g., `hidepid=2` or `hidepid=invisible`). Logs and status files redact these values, and `MSR_FISHTANK_PASSWORD` is stripped from child environments, but `ffmpeg` itself requires per-input URLs and headers on its command line. On a shared Linux host without `hidepid`, keep that exposure in mind before recording authenticated streams.
- **Linux key file vs. OS keyring:** On Linux and other non-Windows systems, encrypted secrets (`*_box`, `twitch_token.json`, `telegram_session.json`) are unlocked by `$XDG_CONFIG_HOME/multi-stream-recorder/secret.key` (mode `0600` in a `0700` directory) rather than a desktop keyring daemon (such as Freedesktop Secret Service / GNOME Keyring / KWallet). That avoids D-Bus/keyring dependencies so headless and minimal setups work out of the box, and keeps a copied `config.ini` unreadable without `secret.key`, though any process running as the same Linux user can read `secret.key` (matching the same-user boundary of Windows DPAPI).
