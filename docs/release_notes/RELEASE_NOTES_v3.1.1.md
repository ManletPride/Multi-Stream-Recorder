# Multi-Stream Recorder v3.1.1

A small fix release for the update check.

## Fixed

- **The update check works on PCs that were missing a security certificate.** GitHub now uses a certificate from a newer authority (Sectigo, "Root E46"). Some Windows PCs don't have that authority's root certificate yet. Python, which MSR runs on, couldn't check it, so the Updates window said *Could not check* and the update banner never showed up. MSR now uses the Windows certificate store plus the standard [certifi](https://github.com/certifi/python-certifi) certificate list (already installed with streamlink). It trusts nothing less than before. The same fix applies to the in-app update download.
- **Clearer messages when a check fails.** The note says what actually went wrong (for example *the server's certificate is not trusted on this PC*), instead of always guessing "offline?".
- **The Updates footer no longer says "Everything is up to date" when a check failed.** It now says, for example, *Could not check Multi-Stream Recorder; other items are up to date.*

## Behind the scenes

- The Library > Recordings toolbar is narrower, so the Library still fits the smallest window size with the wider fonts on Linux.
- Changing the encrypted Fishtank password copy (`password_dpapi`) through agent control is refused with the same error code (`invalid`) on every system.
- A Windows test now handles the built-in Administrator account (used by GitHub's test machines).

## Upgrade notes

- **Important: on affected PCs, 3.0.0 and 3.1.0 may not show the update notice for this release.** The broken check is the one that would tell you about it. If your Updates window says *Could not check* for Multi-Stream Recorder, download 3.1.1 from the [release page](https://github.com/ManletPride/Multi-Stream-Recorder/releases/latest) and replace the program files. Your `config.ini`, `channels.json`, cookies and recordings stay as they are.
- No new settings and no new packages.

Full list of changes: [CHANGELOG.md](https://github.com/ManletPride/Multi-Stream-Recorder/blob/v3.1.1/CHANGELOG.md).