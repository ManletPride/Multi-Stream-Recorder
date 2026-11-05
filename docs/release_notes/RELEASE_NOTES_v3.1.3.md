# Multi-Stream Recorder v3.1.3

A small fix release for Twitch instant alerts.

## Fixed

- **Twitch sign-in no longer drops off after a few hours.** With instant alerts on, MSR used to show *Twitch sign-in is no longer valid: reconnect it in Options* and turn push off, often after about four hours. Twitch's refresh tokens for a public (device-code) app can be used only once. More than one part of MSR could refresh at the same time, or keep an old copy in memory, so a spent refresh token was sent again and Twitch refused it. Everything that refreshes the sign-in (the push listener, Options, agent control, and the health check) now goes through one locked token store that always re-reads the saved file first. A refused refresh reloads and retries once with a newer saved token; a 401 from Twitch's validate or Helix forces one refresh and retries. The startup health check refreshes before it validates, so an expired access token alone no longer shows that warning. Successful refreshes are logged (with no token values).

## Upgrade notes

- Replace the program files as usual, or use the in-app update. Your `config.ini`, `channels.json`, cookies, Twitch sign-in and recordings stay as they are.
- If Options still says the Twitch sign-in is no longer valid from before this fix, reconnect Twitch once in Options. After that it should stay connected.
- No new settings and no new packages.

Full list of changes: [CHANGELOG.md](https://github.com/ManletPride/Multi-Stream-Recorder/blob/v3.1.3/CHANGELOG.md).
