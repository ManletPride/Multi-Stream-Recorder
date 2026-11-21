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
| `cookies.txt` | **Your site logins.** Treat it like a password. |
| `config.ini` `[Fishtank]` email/password | Password encrypted at rest. Windows: DPAPI (`password_dpapi`). Linux and other systems: `password_box`, unlocked by `~/.config/multi-stream-recorder/secret.key` (mode 0600, outside the data folder). The email is not a secret and stays in the file. Login is the reliable auth method; cookies only work for about 15 minutes after export. |
| `channels.json` | Who you record. |
| `twitch_token.json` | Twitch push sign-in. Windows: DPAPI. Elsewhere: the same user key as the other secrets (`box`). Mode 0600 on Linux. |
| `config.ini` `[Telegram]` `api_hash`, `bot_token` | Bot credentials. Not stored in the clear. Windows: DPAPI (`api_hash_dpapi`, `bot_token_dpapi`). Linux and other systems: `api_hash_box` / `bot_token_box`, unlocked by the same `secret.key` as the Fishtank password (mode 0600, outside the data folder). The api id, group id, topic id and your user id are not secrets. |
| `telegram_session.json` | The bot's MTProto session, encrypted the same way (DPAPI or the user key). Mode 0600. Not a login to your personal account. |
| `msr_control.json` | Control port and token, readable only by your user; removed when MSR quits. |
| `stream_recorder.log` | Commands and URLs; JWTs and Cookie headers are redacted, but do not paste raw logs in public. |
| Recordings / clips | Copyrighted and possibly adult content you chose to save. |

`.gitignore` excludes those files. Never `git add -f` them.

## What MSR will and will not do

- Custom URLs must be `http://` or `https://`. `file:` and unknown schemes are rejected.
- Network ffmpeg (Fishtank, Rumble HLS, Chaturbate merge, yt-dlp’s ffmpeg downloader) uses a protocol whitelist **without** `file`.
- Channel names cannot contain `:` `/` `\` that would turn into extra directories.
- Logs go through `redact_for_log` (argv, stderr, exceptions, worker channel tag).
- Twitch reconnect URLs must be `wss://*.twitch.tv`; only `twitch.tv/activate` links are opened.
- The Options tab warns when `cookies.txt` is in a synced folder (OneDrive, Dropbox...).
- MSR **never** refreshes `cookies.txt`. When tokens expire, you re-export from a logged-in browser.

A custom URL is fetched and recorded as you pasted it. Only add URLs you trust.

## Threat model (short)

This is a same-user desktop app. Someone with your Windows account can already read your files. The design goals are:

1. A hostile livestream playlist must not pull `file://` paths into a recording.
2. A malicious `channels.json` must not write outside the recordings folder.
3. Sharing a log or screenshot should not leak JWTs, session cookies, or a Telegram bot token.
4. A Telegram upload goes only to the one group topic in config, from a bot login. MSR does not read group messages and does not log in as the user. The local file is deleted only when that option is on and every part was confirmed.
