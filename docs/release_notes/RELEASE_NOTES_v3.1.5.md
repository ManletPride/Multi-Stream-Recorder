A short fix release. The Updates window's Update button now updates the app.

## Fixed

- **The Update button updates the app.** In the Updates window, choosing Multi-Stream Recorder and pressing Update used to close the dialog and do nothing when MSR had been open since before that release came out. The button now uses the release that check just found, so the update starts. If it cannot start, the window shows the reason. There is one confirmation.

- **A git checkout is offered git pull.** When this copy is a git checkout, Update offers `git pull --ff-only` and a restart if the working tree is clean and the branch tracks origin. Otherwise the window explains and shows the command. A normal install still downloads the GitHub zip.

- **Newer files on disk offer a restart.** When the files on disk are already newer than the running copy, MSR offers a restart. The prompt is "Restart to finish updating".

- **The header banner re-checks daily.** A window left open checks for a new release again with the daily update pass, and the banner shows that release.

## Upgrade notes

- If you are on 3.1.3 or 3.1.4 and the Update button does nothing, restart MSR, then click the update banner or reopen Updates.
- No new packages.
- Your settings and recordings are kept.

Full list of changes: [CHANGELOG.md](https://github.com/ManletPride/Multi-Stream-Recorder/blob/v3.1.5/CHANGELOG.md).
