> **Status:** Fishtank.live support is built in, but the show is between seasons and has no live cameras right now. Its camera list changes every season, so it may need an update when a new one starts.

## Login

Configure your Fishtank account in **Options > Fishtank**, via `MSR_FISHTANK_PASSWORD`, or in `config.ini`:

```ini
[Fishtank]
email = you@example.com
password = yourpassword
```

A free account is sufficient. The program handles authentication automatically — it logs in, obtains a stream token, and refreshes it as needed. No manual cookie export is required. MSR encrypts that password the next time it saves `config.ini` (Windows DPAPI, or a user-only key on Linux) and leaves the `password` line blank. `[Fishtank]` credentials are passed only to Fishtank workers, and `MSR_FISHTANK_PASSWORD` is stripped from child tool environments.

## Adding cameras

Select **fishtank** from the platform dropdown and enter either a friendly alias (if we still have one) or the **raw stream id**:

```
fishtank:director
fishtank:mirror
fishtank:director-as-seen-on
fishtank:dirc-5
```

New-season and off-season cameras that are not in the alias table are still valid if you paste the catalog id (`director-as-seen-on`, `ben-5`, `computer-lab2-5`, …). Numeric seasons look like `name-5`; slug seasons look like `name-as-seen-on`.

## Finding stream IDs

The site loads cameras from `GET https://api.fishtank.live/v1/live-streams` (the camera list itself does not need a login; HLS still does). Each entry in `liveStreams` has:

| Field | Meaning |
| --- | --- |
| `id` | Stream id to paste into MSR (`director-as-seen-on`, `dirc-5`, …) |
| `name` | Display name this season (Director, Mirror, Bar, …) |
| `access` | `public`, `normal`, or `season_pass` |
| `season` | Season number (`5`) or slug (`as-seen-on`) used in the id |

`liveStreamStatus` and `loadBalancer` can list extra ids that are not in `liveStreams` (the grid mosaic is one). Copy ids from those maps too — not from the page source (Next.js CSR).

## As Seen On aliases (current, 2026-09)

Mini-season catalog confirmed 2026-09-10. `fishtank:director` now records `director-as-seen-on` (Season 5 `dirc-5` still works if you paste that id). Mirror is `dressing-room-as-seen-on`. FX and Vault need a season pass.

| Camera name | Room | Stream id | Access |
|---|---|---|---|
| `director` | Director | `director-as-seen-on` | Normal |
| `fx` | FX | `fx-as-seen-on` | Season Pass |
| `firstfloor` | First Floor | `first-floor-as-seen-on` | Public |
| `firstflooralt`, `firstalt` | First Floor Alt | `first-floor-alt-as-seen-on` | Public |
| `firstfloorptz`, `firstptz` | First Floor PTZ | `first-floor-ptz-as-seen-on` | Normal |
| `counter` | Counter | `counter-as-seen-on` | Public |
| `mirror`, `dressingroom` | Mirror | `dressing-room-as-seen-on` | Public |
| `breakroom` | Break Room | `break-room-as-seen-on` | Public |
| `secondfloor` | Second Floor | `second-floor-as-seen-on` | Public |
| `basement` | Basement | `basement-as-seen-on` | Public |
| `vault` | Vault | `vault-as-seen-on` | Season Pass |
| `inventory` | Inventory | `inventory-as-seen-on` | Public |
| `secondfloorptz`, `secondptz` | Second Floor PTZ | `second-floor-ptz-as-seen-on` | Normal |
| `rathew` | Rathew | `rathew-as-seen-on` | Normal |
| `grid` | Grid mosaic | `grid-as-seen-on` | (status only) |

`channels_fishtank.json` is this roster. Rename it to `channels.json` only if you want that house and nothing else.

## Stable endpoints

These are what MSR actually talks to; they should survive a season change:

- Login: `POST https://api.fishtank.live/v1/auth/log-in`
- Catalog / liveness: `GET https://api.fishtank.live/v1/live-streams`
- HLS: `https://<streams-*.fishtank.live>/hls/live+<id>/index.m3u8?jwt=<token>`

The stream host (`streams-c`, `streams-f`, …) is discovered from the API, not hardcoded.

## Season 5 aliases (snapshot)

Friendly names MSR still recognises from the Season 5 house. `director` now points at the As Seen On cam; use `dirc` or `dirc-5` for the old Director Mode id. Other S5 names still resolve to their old ids.

| Camera name | Room (Season 5) | Access (Season 5) |
|---|---|---|
| `dirc` | Director Mode (`dirc-5`) | Free |
| `dorm` | Dorm | Normal |
| `dormalt`, `dorm2`, `dmrm2` | Dorm Alternate | Normal |
| `closet` | Closet | Normal |
| `kitchen` | Kitchen | Normal |
| `bar` | Bar (`bar-5`; S5 house was `brrr-5`) | Normal |
| `barptz` | Bar PTZ | Season Pass |
| `baralt`, `bar2`, `brrr2` | Bar Alternate | Normal |
| `hallway` | Hallway | Normal |
| `dining` | Dining Room | Normal |
| `market` | Market | Normal |
| `marketalt`, `market2` | Market Alternate | Normal |
| `foyer` | Foyer | Normal |
| `glassroom` | Glassroom | Normal |
| `corridor` | Corridor | Normal |
| `eastwing` | East Wing | Normal |
| `westwing` | West Wing | Normal |
| `laundry`, `laundryroom` | Laundry Room | Season Pass |
| `jungle` | Jungle Room | Season Pass |
| `computerlab`, `bbcl` | Computer Lab | Season Pass |
| `jobboard`, `job`, `jobb` | Job Board | Normal |
| `confessional` | Confessional | Season Pass |
| `cameraman` | Cameraman | Season Pass XL |
| `arena` | Arena | Normal |
| `goofactory`, `goofact`, `goo`, `br3g` | Goo Factory | Normal |

Short aliases also work — `cam` for Cameraman, `dirc` for Director, `dmrm` for Dorm. Season 5 renames (`balcony` → `eastwing`, `hallwayup` → `westwing`, `jacuzzi` → `laundry`) still resolve.

`channels_fishtank.json` is the As Seen On roster above, not Season 5. Season 5 friendly names other than `director` still resolve to their old ids.

## How It Works

Detection and recording both use fishtank.live's HLS streams served by MistServer. While cameras are offline, the in-process polling coordinator shares a single authenticated Fishtank session and queries the live-streams API to see which ids are `online`, spawning a recording worker only when a camera goes live and recording via ffmpeg's HLS downloader. The JWT comes from the login API and is refreshed automatically — a background thread renews it with a 5-minute buffer, which matters for cameras that get short-lived 30-minute tokens (Cameraman in Season 5).

Before each recording starts, MSR fetches the HLS master playlist and selects the highest-bandwidth variant, rather than whichever rendition the server lists first.

After each remux, MSR checks that the MP4 has an audio track. Fishtank's CDN occasionally delivers video-only HLS segments; a warning is logged so those files are easy to find.

## Platform Notes

**Kick**: Uses a lightweight HTTP API check for offline polling and streamlink for recording (`quality = best` in `config.ini`, which is streamlink's best available rendition). Streamlink 8.x includes a built-in Cloudflare JS challenge solver that handles Kick's aggressive bot detection. Cookies are optional. With `websocket-client` and `curl_cffi` installed, Kick channels also get instant push notifications (see `README.md`). **Important**: Remove any old third-party `kick.py` plugins from `%APPDATA%\streamlink\plugins\` — they override the built-in plugin and break Cloudflare bypass.

**Twitch**: When signed in via **Options > Twitch**, offline channels are checked in batches using Twitch's Helix API (`GET /helix/streams`, up to 100 channels per request) alongside optional EventSub instant push notifications. Recording uses streamlink with `--twitch-disable-ads` and `--twitch-low-latency`. Cookies are optional but enable subscriber-only features.

**TikTok**: Uses yt-dlp for detection (separate from recording stream-URL resolution) and streamlink/yt-dlp for recording. Add channels by selecting **tiktok** from the platform dropdown and entering the username (with or without `@`), or by pasting a TikTok URL directly. Valid TikTok session cookies are required — export them from a logged-in browser session using the Get cookies.txt extension and place the file in your streams directory. The `msToken` and `ttwid` cookies are the critical ones; when they expire, re-export.

TikTok's CDN delivers mobile-portrait video (typically 540×960 or 720×1280) at 25 fps. This is normal — it reflects what the streamer's phone is broadcasting.

**US-TTP regional detection**: A majority of US-based TikTok streamers (especially gaming streamers) run on TikTok's US regional infrastructure (`webcast.us.tiktok.com`). yt-dlp's TikTok extractor currently hardcodes the global endpoint (`webcast.tiktok.com`), which returns "offline" for these accounts even when they are actively live. MSR includes a fallback that directly queries regional Webcast API endpoints when yt-dlp reports a TikTok channel as offline (with cached cookie parsing, immediate short-circuiting on confirmed offline `status == 4` rooms, and a per-channel 5-minute cooldown between fallback probes). If the fallback confirms the stream is live, recording proceeds normally — no user action required. The log will note `"Webcast API confirms LIVE"` when this override fires.

