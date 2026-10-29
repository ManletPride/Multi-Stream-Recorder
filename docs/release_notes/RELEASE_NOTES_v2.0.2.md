# v2.0.2 — Fishtank As Seen On cameras

Fishtank.live started an **As Seen On** mini-season. Camera ids are now slugs (`director-as-seen-on`) instead of Season 5 codes (`dirc-5`). v2.0.1 would reject the new ids, and `fishtank:director` still pointed at the old Director stream.

This release records the current house. Login and HLS are unchanged.

## Upgrading from v2.0.1

Copy the updated `msr\` folder (and `channels_fishtank.json` if you use that roster) over your existing install. Fully **quit and relaunch**. The running GUI does not reload `msr/`.

Your `config.ini`, `channels.json`, `cookies.txt`, and recordings stay where they are. No new config keys.

Still on v1.8.0? You also need the `msr\` folder — see [RELEASE_NOTES_v2.0.0.md](RELEASE_NOTES_v2.0.0.md).

## What you will notice

**Fishtank**

- `fishtank:director` records the current **Director** camera (`director-as-seen-on`). Season 5 `dirc-5` still works if you paste that id.
- Add the other rooms by alias (`mirror`, `firstfloor`, `breakroom`, …) or by raw catalog id (`director-as-seen-on`, `first-floor-alt-as-seen-on`, …).
- **FX** and **Vault** need a season pass. **Mirror** is `dressing-room-as-seen-on` on the API.
- Optional: `fishtank:grid` is the whole-house mosaic.
- `channels_fishtank.json` is this mini-season roster. Do not replace your whole `channels.json` unless you want only Fishtank.

See [CHANGELOG.md](CHANGELOG.md) and the *Fishtank.live* section in [README.md](README.md) for the alias table.
