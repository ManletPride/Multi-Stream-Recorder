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

All of this stays on **your machine**. MSR does not phone home except a GitHub “latest release” check from the GUI.

| File | Sensitivity |
| --- | --- |
| `cookies.txt` | **Your site logins.** Treat it like a password. |
| `config.ini` `[Fishtank]` email/password | Plaintext. Login is the reliable auth method; cookies only work for about 15 minutes after export. |
| `channels.json` | Who you record. |
| `stream_recorder.log` | Commands and URLs; JWTs and Cookie headers are redacted, but do not paste raw logs in public. |
| Recordings / clips | Copyrighted and possibly adult content you chose to save. |

`.gitignore` excludes those files. Never `git add -f` them.

## What MSR will and will not do

- Custom URLs must be `http://` or `https://`. `file:` and unknown schemes are rejected.
- Network ffmpeg (Fishtank, Rumble HLS, Chaturbate merge, yt-dlp’s ffmpeg downloader) uses a protocol whitelist **without** `file`.
- Channel names cannot contain `:` `/` `\` that would turn into extra directories.
- Logs go through `redact_for_log` (argv, stderr, exceptions, worker channel tag).
- MSR **never** refreshes `cookies.txt`. When tokens expire, you re-export from a logged-in browser.

A custom URL is fetched and recorded as you pasted it. Only add URLs you trust.

## Threat model (short)

This is a same-user desktop app. Someone with your Windows account can already read your files. The design goals are:

1. A hostile livestream playlist must not pull `file://` paths into a recording.
2. A malicious `channels.json` must not write outside the recordings folder.
3. Sharing a log or screenshot should not leak JWTs or session cookies.
