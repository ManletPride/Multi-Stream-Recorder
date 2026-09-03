"""Per-site live checks, auth, and ffmpeg/yt-dlp/streamlink command builders."""
import base64
import datetime
import html as _html
import json
import logging
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from msr.deps import HAS_CURL_CFFI, HAS_STREAMLINK, HAS_YTDLP
from msr.util import redact_cmd_for_log, redact_for_log

# ────────────────────────────────────────────────
#          Stream Checking Functions
# ────────────────────────────────────────────────

def check_stream_kick_api(channel_name, logger, timeout=15, cookies_file=None):
    """Check if a Kick stream is live using streamlink's Kick plugin.

    Streamlink 8.x has a built-in JS challenge solver for Kick's Cloudflare
    protection, which is far more reliable than yt-dlp or curl_cffi for
    bypassing their bot detection.

    Returns (is_live: bool, stream_title: str | None, error: str | None).
    Returns (None, None, error) if streamlink check is inconclusive and
    caller should fall back to yt-dlp.
    """
    if not HAS_STREAMLINK:
        return None, None, "streamlink not available"

    url = f"https://kick.com/{channel_name}"
    check_cmd = ["streamlink", "--json", url]
    logger.info(f"Kick check (streamlink): {redact_cmd_for_log(check_cmd)}")

    try:
        result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=timeout)
        logger.info(f"Kick streamlink check returncode={result.returncode}")

        if result.stdout:
            try:
                data = json.loads(result.stdout)

                # streamlink --json returns {"streams": {...}} when live
                # and {"error": "..."} when offline or errored
                if "streams" in data and data["streams"]:
                    # Channel is live — streamlink found available streams
                    # Try to extract title from metadata if available
                    title = data.get("metadata", {}).get("title")
                    logger.info(f"Kick streamlink: channel is LIVE — title={title!r}")
                    return True, title, None
                elif "error" in data:
                    error_msg = data["error"]
                    error_lower = error_msg.lower()
                    if "403" in error_lower or "forbidden" in error_lower:
                        logger.warning(f"Kick streamlink: Cloudflare 403 — {error_msg}")
                        return None, None, "403 (Cloudflare)"  # fall back
                    elif "no playable streams" in error_lower or "could not find" in error_lower:
                        logger.info("Kick streamlink: channel is offline (no streams)")
                        return False, None, None
                    else:
                        logger.info(f"Kick streamlink: offline or error — {error_msg}")
                        return False, None, None
                else:
                    logger.info("Kick streamlink: no streams found (offline)")
                    return False, None, None

            except json.JSONDecodeError:
                pass

        # Check stderr for common patterns
        if result.stderr:
            stderr_lower = result.stderr.lower()
            if "403" in stderr_lower:
                logger.warning(f"Kick streamlink: 403 in stderr")
                return None, None, "403 (Cloudflare)"
            elif "no plugin" in stderr_lower:
                logger.warning("Kick streamlink: no plugin for Kick URLs")
                return None, None, "no kick plugin"

        if result.returncode != 0:
            logger.info("Kick streamlink: non-zero exit (likely offline)")
            return False, None, None

        return None, None, "unexpected streamlink output"

    except subprocess.TimeoutExpired:
        logger.warning("Kick streamlink check timed out")
        return None, None, "timeout"
    except FileNotFoundError:
        return None, None, "streamlink not found"
    except Exception as e:
        logger.warning(f"Kick streamlink check error: {e}")
        return None, None, str(e)




def parse_rumble_channel_html(page_html, logger):
    """Find the currently-live video in a Rumble channel page's HTML.

    Rumble renders its channel video grid from a JSON payload embedded in
    ``<rum-videos-grid><script type="application/json">{"items":[…]}</script>``.
    Each item carries explicit livestream fields, which is far more stable
    to read than the rendered markup:

        live               true only while the stream is actually broadcasting
        livestream_status  0 = normal VOD
                           1 = livestream that has ENDED (DVR replay available)
                           2 = live right now
        videos[0].url      direct HLS playlist for the stream

    Only ``live: true`` / status 2 counts as live.  Status 1 matters because
    an ended stream keeps its DVR playlist and can still show thousands of
    concurrent viewers — treating "has an HLS URL" as "is live" would
    re-record finished broadcasts on every poll.

    Returns (live_url: str | None, title: str | None, hls_url: str | None).
    """
    import re as _re
    import html as _html

    # ── Primary: the embedded JSON grid payload ──
    # Capture the whole script body (not \{.*?\} which stops at the first
    # nested closing brace and drops videos[].url — the HLS playlist).
    for block in _re.findall(
            r'<script type="application/json">\s*(.*?)\s*</script>',
            page_html, _re.S):
        try:
            data = json.loads(block)
        except Exception:
            try:
                data = json.loads(_html.unescape(block))
            except Exception:
                continue
        if not isinstance(data, dict):
            continue
        for item in data.get("items") or []:
            if not isinstance(item, dict):
                continue
            if item.get("live") is not True and item.get("livestream_status") != 2:
                continue
            url = item.get("url") or ""
            if not url and item.get("relative_url"):
                url = "https://rumble.com" + item["relative_url"]
            if not url:
                continue
            title = item.get("title") or None
            hls = None
            for v in item.get("videos") or []:
                if isinstance(v, dict) and v.get("url"):
                    hls = v["url"]
                    break
            return url, title, hls

    # ── Fallback: the pre-2026 rendered markup.  Rumble dropped this class
    # when they moved the grid to a JSON payload, but it costs nothing to
    # keep in case some page variants still ship it. ──
    for m in _re.finditer(r'thumbnail__thumb--live"', page_html):
        chunk = page_html[m.start(): m.start() + 2000]
        href_m = _re.search(r'href="(/v[a-z0-9]+-[^"]+\.html)', chunk)
        if href_m:
            logger.info("Rumble: matched legacy thumbnail__thumb--live markup")
            return "https://rumble.com" + href_m.group(1), None, None

    return None, None, None


def fetch_rumble_page(page_url, logger, timeout=20, cookies_file=None):
    """Fetch a Rumble page as a browser would.

    Returns (html: str | None, error: str | None).  Raising is avoided so
    callers can treat a fetch failure the same as "nothing live found".
    """
    import urllib.request
    import urllib.error
    import http.cookiejar

    opener = urllib.request.build_opener()
    if cookies_file:
        try:
            cj = http.cookiejar.MozillaCookieJar(cookies_file)
            cj.load(ignore_discard=True, ignore_expires=True)
            opener.add_handler(urllib.request.HTTPCookieProcessor(cj))
        except Exception as ce:
            logger.debug(f"Rumble fetch: could not load cookies ({ce}), proceeding without")

    page_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    req = urllib.request.Request(page_url, headers=page_headers)
    try:
        with opener.open(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace"), None
    except urllib.error.HTTPError as e:
        if e.code in (403, 503) and HAS_CURL_CFFI:
            try:
                from curl_cffi import requests as cffi_requests
                resp = cffi_requests.get(
                    page_url,
                    headers=page_headers,
                    impersonate="chrome",
                    timeout=timeout,
                )
                if resp.status_code == 200 and resp.text:
                    logger.info("Rumble page fetch: urllib 403, curl_cffi impersonate succeeded")
                    return resp.text, None
                return None, f"HTTP {resp.status_code}"
            except Exception as ce:
                logger.debug(f"Rumble curl_cffi fallback failed: {ce}")
        return None, f"HTTP {e.code}"
    except urllib.error.URLError as e:
        return None, str(e.reason)
    except Exception as e:
        return None, str(e)


def check_stream_rumble_html(channel_name, logger, timeout=20, cookies_file=None):
    """Check if a Rumble channel is live by reading the channel page.

    Tries ``/c/CHANNEL`` first and falls back to ``/user/CHANNEL`` on a 404,
    since Rumble splits creators across both path styles.  Parsing is done by
    parse_rumble_channel_html() — see there for the detection rules.

    Returns (is_live, stream_title, resolved_url, hls_url, error).
    resolved_url is the ``https://rumble.com/vXXXXX-slug.html`` page.
    hls_url is the direct playlist from the page JSON when present — prefer
    that for recording so yt-dlp never has to fetch the Cloudflare-gated
    video page.
    """
    page_html = None
    last_error = None
    for path in ("c", "user"):
        channel_url = f"https://rumble.com/{path}/{channel_name}"
        logger.info(f"Rumble HTML check: {redact_for_log(channel_url)}")
        page_html, last_error = fetch_rumble_page(channel_url, logger, timeout, cookies_file)
        if page_html is not None:
            break
        if last_error == "HTTP 404" and path == "c":
            logger.info(f"Rumble: /c/{channel_name} not found — trying /user/{channel_name}")
            continue
        logger.warning(f"Rumble HTML check failed: {last_error}")
        return False, None, None, None, last_error

    if page_html is None:
        return False, None, None, None, last_error or "channel page unavailable"

    live_url, title, hls_url = parse_rumble_channel_html(page_html, logger)

    if not live_url:
        logger.info("Rumble HTML check: no live stream found on channel page")
        return False, None, None, None, None

    logger.info(f"Rumble HTML check: found live video URL — {redact_for_log(live_url)}")
    if title:
        logger.info(f"Rumble stream title: {title}")
    if hls_url:
        logger.info(f"Rumble HTML check: HLS playlist on channel page — {redact_for_log(hls_url)}")
    return True, title, live_url, hls_url, None


def _find_rumble_live_url(url, logger, timeout, cookies_file, impersonate=False):
    """Scan a Rumble channel/user page playlist for a live stream entry.

    Rumble channel and user pages return a playlist when scraped.  Using
    ``--playlist-items 1`` only grabs whichever video is first (usually a
    pinned trailer or VOD), so we need to scan the whole playlist to find
    any item whose ``live_status`` is ``"is_live"``.

    Returns the URL of the live stream entry, or None if none found.
    Only called when ``url`` looks like a Rumble channel/user page (not a
    direct video URL).
    """
    import re as _re
    cmd = ["yt-dlp", "--flat-playlist", "--dump-json"]
    if impersonate and HAS_CURL_CFFI:
        cmd.extend(["--impersonate", "chrome"])
    if cookies_file:
        cmd.extend(["--cookies", cookies_file])
    cmd.append(url)

    logger.info(f"Rumble playlist scan: {redact_cmd_for_log(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("Rumble playlist scan timed out")
        return None
    except Exception as e:
        logger.warning(f"Rumble playlist scan error: {e}")
        return None

    if result.returncode != 0 and not result.stdout:
        logger.warning(f"Rumble playlist scan failed (code {result.returncode})")
        return None

    # --flat-playlist emits one JSON object per line
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        live_status = entry.get("live_status", "")
        entry_url = entry.get("url") or entry.get("webpage_url")

        if live_status == "is_live" and entry_url:
            title = entry.get("title", "(unknown)")
            logger.info(f"Rumble playlist scan: found live entry — {title!r} → {redact_for_log(entry_url)}")
            return entry_url
        # Rumble sometimes omits live_status in flat-playlist; fall back to is_live flag
        if entry.get("is_live") and entry_url:
            title = entry.get("title", "(unknown)")
            logger.info(f"Rumble playlist scan: found is_live entry — {title!r} → {redact_for_log(entry_url)}")
            return entry_url

    logger.info("Rumble playlist scan: no live entry found in playlist")

    # Fallback: read the channel page directly.  yt-dlp's flat-playlist scan
    # depends on its Rumble extractor keeping up with site changes; the page's
    # own embedded JSON is the same data the site renders from, so it keeps
    # working when the extractor lags behind a redesign.  Only worth doing on
    # the non-impersonated pass — the second call would just repeat it.
    if not impersonate:
        logger.info("Rumble playlist scan: falling back to channel page HTML")
        page_html, err = fetch_rumble_page(url, logger, timeout, cookies_file)
        if page_html is None:
            logger.warning(f"Rumble channel page fetch failed: {err}")
            return None
        live_url, title, _hls = parse_rumble_channel_html(page_html, logger)
        if live_url:
            logger.info(f"Rumble HTML fallback: found live entry — {title!r} → {redact_for_log(live_url)}")
            return live_url
        logger.info("Rumble HTML fallback: no live entry on channel page")

    return None


def _is_rumble_channel_url(url):
    """Return True if *url* is a Rumble channel/user page (not a direct video URL).

    Direct Rumble video URLs look like /vXXXXX-slug.html
    Channel/user pages look like /c/ChannelName, /user/Username, etc.
    """
    import re as _re
    if "rumble.com" not in url.lower():
        return False
    path = url.split("rumble.com", 1)[-1].split("?")[0].rstrip("/")
    # Direct video URLs start with /v followed by alphanumerics then a dash
    if _re.match(r"^/v[a-z0-9]+-", path, _re.IGNORECASE):
        return False
    return True


def check_tiktok_live_webcast(username, cookies_file, logger, timeout=30):
    """Check TikTok live status via direct Webcast API call.

    yt-dlp hardcodes webcast.tiktok.com, but US-TTP accounts (the majority
    of US-based TikTok streamers) use webcast.us.tiktok.com.  Calling the
    global endpoint for a US-TTP room returns status 4 (offline) even when
    the stream is live, which is the root cause of TikTok US live streams
    not being detected.

    This function:
      1. Fetches the profile page and extracts the roomId from the embedded
         JSON (same field yt-dlp reads successfully).
      2. Queries BOTH webcast.us.tiktok.com and webcast.tiktok.com until one
         returns status 2 (live).

    Returns (is_live: bool, room_id: str | None, title: str | None, error: str | None).
    """
    import urllib.request
    import http.cookiejar
    import re as _re

    profile_url = f"https://www.tiktok.com/@{username}"

    # Build a cookie jar from the Netscape cookies.txt so TikTok doesn't
    # serve a stripped-down page or a bot-detection redirect.
    cj = http.cookiejar.MozillaCookieJar()
    if cookies_file:
        try:
            cj.load(cookies_file, ignore_discard=True, ignore_expires=True)
        except Exception as e:
            logger.warning(f"TikTok webcast check: could not load cookies: {e}")

    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [
        ('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/124.0.0.0 Safari/537.36'),
        ('Accept-Language', 'en-US,en;q=0.9'),
        ('Referer', 'https://www.tiktok.com/'),
    ]

    # ── Step 1: get roomId from profile page ─────────────────────────────
    try:
        with opener.open(profile_url, timeout=timeout) as resp:
            html = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        return False, None, None, f"profile fetch failed: {e}"

    m = _re.search(r'"roomId"\s*:\s*"(\d+)"', html)
    if not m:
        return False, None, None, "no roomId in profile page (user not live or page changed)"

    room_id = m.group(1)
    logger.info(f"TikTok webcast check: roomId={room_id} for @{username}")

    # ── Step 2: query Webcast API — US endpoint first, global as fallback ─
    webcast_hosts = [
        'webcast.us.tiktok.com',   # US-TTP accounts (most US streamers)
        'webcast.tiktok.com',       # Global TikTok accounts
    ]

    for host in webcast_hosts:
        api_url = (f"https://{host}/webcast/room/info/"
                   f"?aid=1988&room_id={room_id}")
        try:
            with opener.open(api_url, timeout=timeout) as resp:
                data = json.loads(resp.read().decode('utf-8', errors='replace'))
        except Exception as e:
            logger.debug(f"TikTok webcast check ({host}): request failed: {e}")
            continue

        room_data = data.get('data') or {}
        status = room_data.get('status')
        logger.debug(f"TikTok webcast check ({host}): status={status}")

        if status == 2:  # 2 = live, 4 = offline/ended
            title_data = room_data.get('title') or ''
            logger.info(f"TikTok webcast check: LIVE via {host} (status=2)")
            return True, room_id, title_data or None, None

    return False, room_id, None, None


def check_stream_ytdlp(url, logger, timeout=30, cookies_file=None):
    """Check if a stream is live using yt-dlp (Kick, YouTube, custom URLs).

    Returns (is_live: bool, stream_title: str | None, error: str | None,
             used_impersonation: bool, resolved_url: str | None,
             format_urls: dict | None).
    The fourth value indicates whether browser impersonation was needed,
    so the recording command can use the same flag.
    The fifth value is the resolved video URL if yt-dlp found a different
    URL than the one provided (e.g. Rumble channel page -> video URL).
    The sixth value is a {'video': url, 'audio': url} dict when the stream
    has separate video-only and audio-only HLS tracks (e.g. Chaturbate CMAF).
    When present, the caller should use build_recording_command_ffmpeg_merge
    instead of yt-dlp to avoid the live-stream buffering deadlock.
    """
    # ── Rumble channel/user pages: scan playlist for live entry ──────────
    # Unlike YouTube's @handle/live, Rumble has no "give me the live stream"
    # URL convention — channel pages return a full playlist and --playlist-items 1
    # just grabs whatever is pinned first (usually a trailer/VOD).  We scan
    # the flat playlist to find any entry that is actually live.
    if _is_rumble_channel_url(url):
        live_url = _find_rumble_live_url(url, logger, timeout, cookies_file, impersonate=False)
        if live_url is None and HAS_CURL_CFFI:
            # Channel page may be Cloudflare-gated — retry with impersonation
            logger.info("Rumble channel scan: retrying with --impersonate chrome")
            live_url = _find_rumble_live_url(url, logger, timeout, cookies_file, impersonate=True)
        if live_url is None:
            # No live stream found in playlist right now — report as offline
            return False, None, None, False, None, None
        # Found a live entry — check it directly to get full metadata & confirm live status
        logger.info(f"Rumble channel scan resolved live URL: {redact_for_log(live_url)}")
        url = live_url  # proceed with the specific live video URL from here on

    if not HAS_YTDLP:
        logger.error("yt-dlp not installed — cannot check Kick/YouTube streams")
        return False, None, "yt-dlp not installed", False, None, None

    # Rumble's video pages are Cloudflare-gated. A plain yt-dlp fetch 403s
    # even when the channel page (and cookies) are fine; --impersonate
    # chrome is required on the *first* attempt, not as a retry.
    rumble_page = "rumble.com" in url.lower()
    impersonate_first = rumble_page and HAS_CURL_CFFI

    check_cmd = ["yt-dlp", "--dump-json", "--playlist-items", "1"]
    if impersonate_first:
        check_cmd.extend(["--impersonate", "chrome"])
    if cookies_file:
        check_cmd.extend(["--cookies", cookies_file])
    check_cmd.append(url)
    logger.info(f"Check cmd (yt-dlp): {redact_cmd_for_log(check_cmd)}")

    try:
        check = subprocess.run(check_cmd, capture_output=True, text=True, timeout=timeout)
        logger.info(f"Check returncode={check.returncode}")

        # Check for outdated yt-dlp-ejs challenge solver regardless of returncode —
        # the warning appears in stderr even on successful checks and will silently
        # cause failures once YouTube tightens enforcement.
        if check.stderr and "challenge solver lib script version" in check.stderr.lower() and "is not supported" in check.stderr.lower():
            logger.warning(
                "yt-dlp challenge solver (yt-dlp-ejs) is outdated — YouTube checks may fail. "
                "Fix: pip install -U \"yt-dlp[default]\""
            )

        if check.stderr and check.returncode != 0:
            stderr_snippet = redact_for_log(check.stderr[:200])
            logger.info(f"Check stderr: {stderr_snippet}")

        if check.returncode == 0 and check.stdout:
            try:
                data = json.loads(check.stdout)
                is_live = data.get("is_live", False) or data.get("live_status") == "is_live"
                title = data.get("title") or data.get("fulltitle")

                if not is_live and data.get("live_status") == "is_upcoming":
                    logger.info("Stream is scheduled but not live yet")
                    return False, title, "scheduled (not started)", False, None, None

                # Check if yt-dlp resolved to a different URL (e.g. channel page -> video)
                resolved = data.get("webpage_url") or data.get("url")
                resolved_url = None
                if resolved and resolved != url:
                    logger.info(f"Resolved URL: {redact_for_log(resolved)}")
                    resolved_url = resolved

                # For custom URLs: if yt-dlp can extract formats, treat as recordable
                # even if is_live isn't explicitly set (e.g. direct .m3u8 links).
                # BUT: if the URL resolved to a different page (channel -> video),
                # trust yt-dlp's is_live flag — a resolved VOD should NOT be treated
                # as live just because it has formats.
                if not is_live and data.get("formats") and not resolved_url:
                    logger.info(f"yt-dlp found extractable stream (not explicitly live): title={title!r}")
                    is_live = True  # treat as recordable
                elif not is_live and resolved_url:
                    live_status = data.get("live_status", "unknown")
                    logger.info(f"Resolved video is not live (live_status={live_status!r}) — treating as offline")

                logger.info(f"yt-dlp found stream: is_live={is_live}, title={title!r}")

                # Detect split video+audio HLS tracks (e.g. Chaturbate CMAF).
                # When present, yt-dlp's bestvideo+bestaudio merge requires
                # buffering both streams until completion — which deadlocks on
                # live streams.  Return the URLs so the caller can use ffmpeg
                # directly instead.
                format_urls = None
                formats = data.get("formats", [])
                if formats:
                    video_fmts = [f for f in formats
                                  if f.get("vcodec") not in (None, "none")
                                  and f.get("url")]
                    audio_fmts = [f for f in formats
                                  if f.get("vcodec") in (None, "none")
                                  and f.get("url")]
                    if video_fmts and audio_fmts:
                        best_v = max(video_fmts, key=lambda f: f.get("tbr") or f.get("vbr") or 0)
                        best_a = max(audio_fmts, key=lambda f: f.get("tbr") or f.get("abr") or 0)
                        # manifest_url is the master HLS playlist (long-lived JWT).
                        # Individual chunklist URLs carry short-lived session tokens
                        # that may expire between the check and record phases — so we
                        # prefer the master URL and let ffmpeg negotiate its own session.
                        manifest_url = best_v.get("manifest_url") or best_a.get("manifest_url")
                        # Carry yt-dlp's exact per-format http_headers through to ffmpeg.
                        # These are what yt-dlp uses internally when hitting the CDN —
                        # guessing a different header set risks Cloudflare rejections.
                        http_headers = best_v.get("http_headers") or best_a.get("http_headers") or {}
                        format_urls = {
                            "video": best_v["url"],
                            "audio": best_a["url"],
                            "manifest": manifest_url,
                            "http_headers": http_headers,
                            # Stream info fields for the status display
                            "width":  best_v.get("width"),
                            "height": best_v.get("height"),
                            "fps":    best_v.get("fps"),
                            "tbr":    best_v.get("tbr") or best_v.get("vbr"),
                        }
                        logger.info(
                            f"Split tracks detected — video: {best_v.get('format_id')} "
                            f"({best_v.get('tbr', '?')}k), audio: {best_a.get('format_id')}"
                            + (f", manifest: {redact_for_log(manifest_url[:80])}…"
                               if manifest_url else "")
                        )

                return is_live, title, None, impersonate_first, resolved_url, format_urls
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse yt-dlp JSON: {e}")
                return False, None, "JSON parse error", False, None, None

        # Parse common error conditions from stderr
        if check.stderr:
            stderr_lower = check.stderr.lower()
            if "private video" in stderr_lower or "members-only" in stderr_lower:
                return False, None, "members-only or private", False, None, None
            elif "this live event will begin" in stderr_lower:
                return False, None, "scheduled but not started", False, None, None
            elif "video unavailable" in stderr_lower or "no video formats" in stderr_lower:
                logger.warning("Video unavailable — might be offline or /live redirect failed")
                return False, None, "video unavailable", False, None, None
            elif "unable to extract" in stderr_lower:
                logger.warning("Could not extract stream info — channel might not be live")
                return False, None, "extraction failed", False, None, None
            elif "http error 403" in stderr_lower or "http error 503" in stderr_lower:
                # Already impersonated on the first try (Rumble) — don't
                # repeat the same command. Otherwise retry with chrome TLS.
                if HAS_CURL_CFFI and not impersonate_first:
                    logger.info("HTTP 403/503 — retrying with --impersonate chrome")
                    impersonate_cmd = ["yt-dlp", "--impersonate", "chrome",
                                       "--dump-json", "--playlist-items", "1"]
                    if cookies_file:
                        impersonate_cmd.extend(["--cookies", cookies_file])
                    impersonate_cmd.append(url)
                    try:
                        retry = subprocess.run(impersonate_cmd, capture_output=True,
                                               text=True, timeout=timeout)
                        if retry.returncode == 0 and retry.stdout:
                            data = json.loads(retry.stdout)
                            is_live = data.get("is_live", False) or data.get("live_status") == "is_live"
                            title = data.get("title") or data.get("fulltitle")
                            if not is_live and data.get("formats"):
                                is_live = True
                            # Check resolved URL for impersonation path too
                            resolved = data.get("webpage_url") or data.get("url")
                            resolved_url = None
                            if resolved and resolved != url:
                                logger.info(f"Resolved URL: {redact_for_log(resolved)}")
                                resolved_url = resolved
                            # Don't treat resolved VODs as live
                            if is_live and not (data.get("is_live", False) or data.get("live_status") == "is_live") and resolved_url:
                                live_status = data.get("live_status", "unknown")
                                logger.info(f"Resolved video is not live (live_status={live_status!r}) — treating as offline")
                                is_live = False
                            logger.info(f"Impersonation succeeded: is_live={is_live}, title={title!r}")
                            return is_live, title, None, True, resolved_url, None
                        else:
                            logger.warning("Impersonation retry also failed")
                    except Exception as e:
                        logger.warning(f"Impersonation retry error: {e}")
                if rumble_page:
                    logger.error(
                        "Rumble HTTP 403 — Cloudflare blocked yt-dlp even with "
                        "impersonation. Update: pip install -U \"yt-dlp[default]\" curl_cffi. "
                        "A logged-in Rumble session in cookies.txt can also help."
                    )
                    return False, None, "403 (Rumble blocked yt-dlp)", impersonate_first, None, None
                logger.error("HTTP 403/503 — cookies may be expired or invalid")
                return False, None, "403/503 (cookies expired?)", False, None, None
            elif "sign in" in stderr_lower or "login required" in stderr_lower:
                logger.error("Login required — cookies may be missing or expired")
                return False, None, "login required (check cookies)", False, None, None

        return False, None, None, False, None, None

    except subprocess.TimeoutExpired:
        logger.warning("Stream check timed out")
        return False, None, "timeout", False, None, None
    except FileNotFoundError:
        logger.error("yt-dlp not found in PATH")
        return False, None, "yt-dlp not found", False, None, None
    except Exception as e:
        logger.error(f"Unexpected error checking stream: {e}")
        return False, None, str(e), False, None, None


class FishtankAuth:
    """Manages JWT authentication for fishtank.live streams.

    Fishtank uses MistServer for streaming.  Every stream URL requires a
    short-lived JWT obtained from api.fishtank.live/v1/auth.  This class
    fetches and caches the token, refreshing it only when it has expired
    (tokens are valid for ~24 hours).

    Usage (in a worker):
        auth = FishtankAuth(cookies_file, logger, email="", password="")
        jwt = auth.get_jwt()          # returns None on failure
        url = auth.build_stream_url("dirc-5")
    """

    # Base URLs
    _AUTH_URL    = "https://api.fishtank.live/v1/auth"
    _LOGIN_URL   = "https://api.fishtank.live/v1/auth/log-in"
    _STREAMS_URL = "https://api.fishtank.live/v1/live-streams"
    # Stream host is read dynamically from the loadBalancer API field.
    # Fishtank rotates between streams-b, streams-c, etc. per session.
    _DEFAULT_STREAM_HOST = "streams-c.fishtank.live"

    # Friendly names → stream IDs. Room names and IDs change every season
    # (and again in the off-season). Keep old entries so existing rosters
    # still resolve; new cameras can be added as the raw API id (see
    # is_fishtank_stream_id). Do not scrape fishtank.live HTML — the grid
    # is client-rendered; the catalog is GET /v1/live-streams.
    CAMERA_ALIASES = {
        "director":   "dirc-5",
        "dirc":       "dirc-5",
        "dorm":       "dmrm-5",
        "dmrm":       "dmrm-5",
        "confessional":"cfsl-5",
        "cfsl":       "cfsl-5",
        # Season 5: "Balcony" renamed to "East Wing" (bkny-5 stream ID unchanged)
        "eastwing":   "bkny-5",
        "east":       "bkny-5",
        "bkny":       "bkny-5",
        "balcony":    "bkny-5",   # old alias kept for backwards compatibility
        "foyer":      "foyr-5",
        "foyr":       "foyr-5",
        "kitchen":    "ktch-5",
        "ktch":       "ktch-5",
        # S5 house used brrr-5; the 2026-09 catalog lists Bar as bar-5.
        "bar":        "bar-5",
        "bar5":       "bar-5",
        "brrr":       "brrr-5",
        # Season 5: "Jacuzzi" renamed to "Laundry Room" (jckz-5 stream ID unchanged)
        "laundry":    "jckz-5",
        "laundryroom":"jckz-5",
        "jacuzzi":    "jckz-5",   # old alias kept for backwards compatibility
        "jckz":       "jckz-5",
        "dining":     "dnrm-5",
        "dnrm":       "dnrm-5",
        "glassroom":  "gsrm-5",
        "gsrm":       "gsrm-5",
        "corridor":   "codr-5",
        "codr":       "codr-5",
        # Season 5: "Hallway Up" renamed to "West Wing" (hwup-5 stream ID unchanged)
        "westwing":   "hwup-5",
        "west":       "hwup-5",
        "hwup":       "hwup-5",
        "hallwayup":  "hwup-5",   # old alias kept for backwards compatibility
        # Season 5: "Hallway Down" renamed to "Hallway" (hwdn-5 stream ID unchanged)
        "hallway":    "hwdn-5",
        "hallwaydown":"hwdn-5",   # old alias kept for backwards compatibility
        "hwdn":       "hwdn-5",
        "closet":     "dmcl-5",
        "dmcl":       "dmcl-5",
        "cameraman":  "cameraman2-5",
        "cameraman2": "cameraman2-5",
        "cam":        "cameraman2-5",
        "barptz":     "brpz-5",
        "brpz":       "brpz-5",
        # Season 5: "Bar Alternate" confirmed via HAR capture (2026-04-02)
        "baralt":          "brrr2-5",
        "baralternate":    "brrr2-5",
        "bar2":            "brrr2-5",
        "brrr2":           "brrr2-5",
        "market":     "mrke-5",
        "mrke":       "mrke-5",
        # Season 5: second Market camera confirmed via HAR capture (2026-03-26)
        "marketalt":  "mrke2-5",
        "marketalternate": "mrke2-5",
        "market2":    "mrke2-5",
        "mrke2":      "mrke2-5",
        "jungleroom": "br4j-5",
        "jungle":     "br4j-5",
        "br4j":       "br4j-5",
        # Season 5: "Computer Lab" confirmed (bbcl-5)
        "computerlab": "bbcl-5",
        "complab":     "bbcl-5",
        "bbcl":        "bbcl-5",
        "bbcl5":       "bbcl-5",
        # Season 5: br3g-5 still pending official name
        # Season 5: "Arena" officially unveiled 2026-03-26; bare-5 stream ID confirmed via HAR
        "arena":      "bare-5",
        "bare":       "bare-5",
        "bare5":      "bare-5",
        "br3g":       "br3g-5",
        "br3g5":      "br3g-5",
        #Season 5: Goo Factory revealed
        "goofactory":     "br3g-5",
        "goofact":        "br3g-5",
        "goo":            "br3g-5",
        # Season 5: additional cameras confirmed via HAR capture (2026-03-25)
        "dormalt":    "dmrm2-5",
        "dormsalt":   "dmrm2-5",
        "dormalternate": "dmrm2-5",
        "dorm2":      "dmrm2-5",
        "dmrm2":      "dmrm2-5",
        "jobboard":   "jobb-5",
        "job":        "jobb-5",
        "jobs":       "jobb-5",
        "jobb":       "jobb-5",
    }

    # How many seconds before expiry to proactively refresh the JWT.
    # Fishtank issues 30-minute tokens for some streams (cameraman), so
    # refreshing 5 minutes early guarantees the token is always fresh.
    _JWT_REFRESH_BUFFER = 300   # 5 minutes

    def __init__(self, cookies_file, logger, email="", password=""):
        self._cookies_file = cookies_file
        self._logger = logger
        self._email = email
        self._password = password
        self._jwt = None
        self._jwt_exp = 0       # unix timestamp when current JWT expires
        self._stream_host = self._DEFAULT_STREAM_HOST  # fallback host (any online stream)
        self._stream_hosts = {}  # per-stream hosts from loadBalancer, e.g. {"dirc-5": "streams-f.fishtank.live"}
        self._all_stream_names = {}  # all stream id→name from API, populated by get_live_streams
        self._refresh_thread = None
        self._refresh_stop = threading.Event()

    # ── Public interface ──────────────────────────────────────────────────

    def start_background_refresh(self):
        """Start a background thread that proactively refreshes the JWT before it expires.

        This prevents the ~36-minute coverage gap that occurs when a short-lived
        30-minute token (as issued by Fishtank for the cameraman stream) expires
        while the worker is in a reconnect loop and not actively calling get_jwt().
        The thread wakes up every 60 seconds, checks whether the current token is
        within _JWT_REFRESH_BUFFER seconds of expiry, and silently refreshes it.
        """
        if self._refresh_thread and self._refresh_thread.is_alive():
            return
        self._refresh_stop.clear()

        def _refresh_loop():
            while not self._refresh_stop.wait(timeout=60):
                now = time.time()
                if self._jwt is not None and now >= (self._jwt_exp - self._JWT_REFRESH_BUFFER):
                    remaining = max(0, self._jwt_exp - now)
                    self._logger.info(
                        f"[fishtank] JWT expires in {remaining:.0f}s — proactively refreshing"
                    )
                    self._refresh()

        self._refresh_thread = threading.Thread(
            target=_refresh_loop,
            daemon=True,
            name="fishtank-jwt-refresh",
        )
        self._refresh_thread.start()
        self._logger.info("[fishtank] Background JWT refresh thread started")

    def stop_background_refresh(self):
        """Stop the background JWT refresh thread."""
        self._refresh_stop.set()
        if self._refresh_thread:
            self._refresh_thread.join(timeout=5)
            self._refresh_thread = None

    def resolve_stream_id(self, name):
        """Resolve a camera name or raw stream-ID to a canonical stream ID.

        Accepts:
            - Canonical IDs:   "dirc-5", "dmrm-5", …
            - Friendly names:  "director", "dorm", "bar", …
        Returns the canonical ID string, or the original value if not found.
        """
        normalised = name.lower().replace(" ", "").replace("-", "")
        return self.CAMERA_ALIASES.get(normalised, name)

    def get_jwt(self, force_refresh=False):
        """Return a valid JWT, refreshing from the API if needed.

        Returns the JWT string, or None if authentication failed.
        """
        now = time.time()
        # Refresh if expired (with 5-minute buffer) or forced
        if force_refresh or self._jwt is None or now >= (self._jwt_exp - 300):
            self._refresh()
        return self._jwt

    def build_stream_url(self, stream_id, quality="maxbps"):
        """Build the MistServer TS progressive HTTP URL for a given stream ID.

        Returns (url, jwt) tuple, or (None, None) if auth failed.

        MistServer serves HLS (confirmed from browser HAR):

            https://<host>/hls/live+<stream_id>/index.m3u8?jwt=<token>

        Note: uses literal + (not %2b) and ?jwt= (not ?tkn=).
        The same 24h live_stream_token is used for all endpoints.
        """
        jwt = self.get_jwt()
        if not jwt:
            return None, None
        # Use per-stream host if known, fall back to global cached host.
        # Director Mode (dirc-5) is often on a different node than the other
        # streams — always use the stream-specific host from loadBalancer.
        host = self._stream_hosts.get(stream_id, self._stream_host)
        # HLS master playlist — confirmed from browser HAR as the actual
        # protocol used. Uses literal + (not %2b) and ?jwt= parameter.
        url = f"https://{host}/hls/live+{stream_id}/index.m3u8?jwt={jwt}"
        return url, jwt

    def get_live_streams(self):
        """Fetch the list of currently-live stream IDs from the API.

        Host discovery uses the thumbnail endpoint rather than the loadBalancer
        API field.  The loadBalancer field rotates to a different node on every
        API call (round-robin) and that node may not be actually serving the
        stream.  The thumbnail endpoint (/live%2B<id>.jpeg) is served directly
        by the real streaming node with no auth required, so a 200 response
        from a given host means that host is currently serving the stream.

        Returns a dict mapping stream_id → stream_name, or {} on failure.
        """
        raw = self._fetch_json(self._STREAMS_URL)
        if raw is None:
            return {}
        try:
            data = json.loads(raw)
            status = data.get("liveStreamStatus", {})
            lb = data.get("loadBalancer", {})
            streams = {}
            all_stream_names = {}
            for s in data.get("liveStreams", []):
                sid = s["id"]
                all_stream_names[sid] = s["name"]
                if status.get(sid) == "online":
                    streams[sid] = s["name"]

            self._all_stream_names = all_stream_names

            # Discover the real serving host for each online stream via thumbnail.
            # Use the loadBalancer host as the starting point for the thumbnail
            # request — the thumbnail always responds from the real node.
            import urllib.request, urllib.error
            for sid in list(streams.keys()):
                candidate_host = lb.get(sid, self._stream_host)
                thumb_url = (
                    f"https://{candidate_host}/live%2B{sid}.jpeg"
                )
                try:
                    req = urllib.request.Request(
                        thumb_url, method="HEAD",
                        headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        # The response URL's host is the real serving node
                        real_host = resp.url.split("//")[1].split("/")[0]
                        if real_host and "fishtank.live" in real_host:
                            self._stream_hosts[sid] = real_host
                            self._stream_host = real_host
                        else:
                            # Responded from candidate_host directly
                            self._stream_hosts[sid] = candidate_host
                            self._stream_host = candidate_host
                except Exception:
                    # Fallback: trust the loadBalancer host as-is
                    if lb.get(sid):
                        self._stream_hosts[sid] = lb[sid]
                        self._stream_host = lb[sid]

            if self._stream_host != self._DEFAULT_STREAM_HOST:
                self._logger.info(
                    f"[fishtank] Stream serving host: {self._stream_host}")
            return streams
        except Exception as e:
            self._logger.warning(f"[fishtank] Failed to parse live streams: {e}")
            return {}

    def _fetch_json(self, url):
        """Fetch a URL and return the response body as a string.

        Tries curl_cffi first (handles HTTP/3, Cloudflare, better TLS),
        falls back to urllib.  Returns None on failure.
        """
        headers = self._common_headers()

        # ── curl_cffi (preferred — handles h3 and Cloudflare) ─────────────
        if HAS_CURL_CFFI:
            try:
                from curl_cffi import requests as cffi_requests
                resp = cffi_requests.get(
                    url, headers=headers,
                    impersonate="chrome", timeout=15,
                )
                if resp.status_code == 200:
                    return resp.text
                self._logger.warning(
                    f"[fishtank] curl_cffi HTTP {resp.status_code} for {url}")
                return None
            except Exception as e:
                self._logger.warning(
                    f"[fishtank] curl_cffi fetch failed ({e}), retrying with urllib")

        # ── urllib fallback ────────────────────────────────────────────────
        import urllib.request, urllib.error
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.read().decode("utf-8")
        except Exception as e:
            self._logger.warning(f"[fishtank] Failed to fetch {redact_for_log(url)}: {redact_for_log(e)}")
            return None

    # ── Private helpers ───────────────────────────────────────────────────

    def _extract_supabase_jwt(self):
        """Extract the Supabase access token from cookies.txt.

        The cookie named 'sb-wcsaaupukpdmqdjcgaoo-auth-token' holds a
        JSON-encoded array [access_token, refresh_token] stored as a
        URL-encoded string.  We parse it out and return the access token.

        Returns the JWT string, or None if not found / already expired.
        """
        import urllib.parse, base64
        if not self._cookies_file or not os.path.isfile(self._cookies_file):
            return None
        try:
            with open(self._cookies_file, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split('\t')
                    if len(parts) < 7:
                        continue
                    name = parts[5]
                    if 'sb-' in name and 'auth-token' in name:
                        value = urllib.parse.unquote(parts[6])
                        # Value is a JSON array: ["<access_token>", "<refresh_token>"]
                        tokens = json.loads(value)
                        if isinstance(tokens, list) and len(tokens) >= 1:
                            access_token = tokens[0]
                            # Quick expiry check — don't bother sending an
                            # obviously expired token
                            exp = self._decode_jwt_exp(access_token)
                            if exp < time.time():
                                self._logger.warning(
                                    "[fishtank] Supabase access token in cookies.txt "
                                    "has expired — please re-export cookies from your "
                                    "browser while logged into fishtank.live"
                                )
                                return None
                            return access_token
        except Exception as e:
            self._logger.warning(f"[fishtank] Could not extract Supabase JWT from cookies: {e}")
        return None

    def _refresh(self):
        """Fetch a fresh MistServer JWT from the fishtank auth endpoint.

        Authentication strategy (in priority order):
          1. POST /v1/auth/log-in with email+password from config.ini — the
             only method that works reliably regardless of cookie freshness.
             The GET /v1/auth session-check endpoint returns {"session":null}
             once the 15-minute Supabase access token expires, even with
             valid cookies.
          2. Authorization: Bearer header using the Supabase token extracted
             from cookies.txt (works only within 15 min of cookie export).
          3. Cookie jar fallback (legacy).

        If all paths fail, self._jwt is set to None and the error is logged.
        """
        import urllib.request, urllib.error, http.cookiejar
        self._logger.info("[fishtank] Refreshing JWT")

        # ── Strategy 1: POST login with email+password (most reliable) ────
        if self._email and self._password:
            jwt = self._login_with_credentials()
            if jwt:
                self._jwt = jwt
                self._jwt_exp = self._decode_jwt_exp(jwt)
                exp_dt = datetime.datetime.fromtimestamp(
                    self._jwt_exp).strftime("%Y-%m-%d %H:%M:%S")
                self._logger.info(
                    f"[fishtank] MistServer JWT obtained via login, "
                    f"valid for 24h, expires {exp_dt}")
                return
            self._logger.warning(
                "[fishtank] Login with credentials failed — falling back to cookie methods")

        # ── Strategy 2: Authorization: Bearer header ──────────────────────
        supabase_token = self._extract_supabase_jwt()
        if supabase_token:
            try:
                headers = self._common_headers()
                headers["Authorization"] = f"Bearer {supabase_token}"
                # Use curl_cffi if available to handle HTTP/3
                raw_text = None
                if HAS_CURL_CFFI:
                    try:
                        from curl_cffi import requests as cffi_requests
                        r = cffi_requests.get(
                            self._AUTH_URL, headers=headers,
                            impersonate="chrome", timeout=15)
                        if r.status_code == 200:
                            raw_text = r.text
                    except Exception:
                        pass
                if raw_text is None:
                    req = urllib.request.Request(self._AUTH_URL, headers=headers)
                    with urllib.request.urlopen(req, timeout=15) as resp:
                        raw_text = resp.read().decode("utf-8")
                raw = raw_text
                data = json.loads(raw)
                jwt = (
                    # live_stream_token is the 24h MistServer JWT
                    (data.get("session") or {}).get("live_stream_token")
                    or data.get("token") or data.get("jwt")
                    or data.get("accessToken") or data.get("mistToken")
                    or (data.get("session") or {}).get("access_token")
                )
                if jwt:
                    self._jwt = jwt
                    self._jwt_exp = self._decode_jwt_exp(jwt)
                    exp_dt = datetime.datetime.fromtimestamp(
                        self._jwt_exp).strftime("%Y-%m-%d %H:%M:%S")
                    self._logger.info(
                        f"[fishtank] MistServer JWT obtained via Bearer auth, "
                        f"valid for 24h, expires {exp_dt}")
                    return
                # Auth succeeded but response format unexpected — log all keys
                # so we can adapt in the future
                self._logger.warning(
                    f"[fishtank] Auth response had no recognised token field "
                    f"(keys: {list(data.keys())})"
                )
                # Fall through to strategy 2
            except urllib.error.HTTPError as e:
                self._logger.warning(
                    f"[fishtank] Bearer auth HTTP {e.code} — falling back to cookie jar")
            except Exception as e:
                self._logger.warning(
                    f"[fishtank] Bearer auth error ({e}) — falling back to cookie jar")

        # ── Strategy 2: Cookie jar (legacy fallback) ──────────────────────
        try:
            opener = urllib.request.build_opener()
            if self._cookies_file and os.path.isfile(self._cookies_file):
                try:
                    cj = http.cookiejar.MozillaCookieJar(self._cookies_file)
                    cj.load(ignore_discard=True, ignore_expires=True)
                    opener.add_handler(urllib.request.HTTPCookieProcessor(cj))
                except Exception as ce:
                    self._logger.warning(
                        f"[fishtank] Could not load cookies for fallback: {ce}")

            # Try curl_cffi first for h3 support
            raw = None
            if HAS_CURL_CFFI:
                try:
                    from curl_cffi import requests as cffi_requests
                    import http.cookiejar as _cj_mod
                    cffi_headers = self._common_headers()
                    r2 = cffi_requests.get(
                        self._AUTH_URL, headers=cffi_headers,
                        impersonate="chrome", timeout=15)
                    if r2.status_code == 200:
                        raw = r2.text
                except Exception:
                    pass
            if raw is None:
                req = urllib.request.Request(
                    self._AUTH_URL, headers=self._common_headers())
                with opener.open(req, timeout=15) as resp:
                    raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            jwt = (
                # live_stream_token is the 24h MistServer JWT
                (data.get("session") or {}).get("live_stream_token")
                or data.get("token") or data.get("jwt")
                or data.get("accessToken") or data.get("mistToken")
                or (data.get("session") or {}).get("access_token")
            )
            if jwt:
                self._jwt = jwt
                self._jwt_exp = self._decode_jwt_exp(jwt)
                exp_dt = datetime.datetime.fromtimestamp(
                    self._jwt_exp).strftime("%Y-%m-%d %H:%M:%S")
                self._logger.info(
                    f"[fishtank] MistServer JWT obtained via cookie jar, "
                    f"valid for 24h, expires {exp_dt}")
                return

            # Both strategies failed — give the user a clear action to take
            self._logger.error(
                f"[fishtank] Authentication failed (response omitted — may "
                f"contain tokens).\n"
                f"  ► Your fishtank.live session has most likely expired.\n"
                f"  ► Fix: log into fishtank.live in your browser, then "
                f"re-export cookies.txt via Cookie-Editor and replace "
                f"the cookies.txt next to your recordings."
            )
            self._jwt = None

        except urllib.error.HTTPError as e:
            self._logger.error(
                f"[fishtank] Auth HTTP error {e.code}: {e.reason}")
            self._jwt = None
        except urllib.error.URLError as e:
            self._logger.error(f"[fishtank] Auth URL error: {e.reason}")
            self._jwt = None
        except Exception as e:
            self._logger.error(f"[fishtank] Auth unexpected error: {e}")
            self._jwt = None

    def _login_with_credentials(self):
        """POST to /v1/auth/log-in with email+password to get a fresh JWT.

        This is the most reliable auth method — it works regardless of cookie
        freshness and returns a 24h live_stream_token directly.

        Returns the live_stream_token JWT string, or None on failure.
        """
        import json as _json

        payload = _json.dumps({
            "email": self._email,
            "password": self._password,
        }).encode("utf-8")

        headers = self._common_headers()
        headers["Content-Type"] = "application/json"

        try:
            if HAS_CURL_CFFI:
                from curl_cffi import requests as cffi_requests
                resp = cffi_requests.post(
                    self._LOGIN_URL,
                    data=payload,
                    headers=headers,
                    impersonate="chrome",
                    timeout=15,
                )
                if resp.status_code == 200:
                    data = _json.loads(resp.text)
                    return (data.get("session") or {}).get("live_stream_token")
                self._logger.warning(
                    f"[fishtank] Login HTTP {resp.status_code}")
                return None

            # urllib fallback
            import urllib.request, urllib.error
            req = urllib.request.Request(
                self._LOGIN_URL,
                data=payload,
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = _json.loads(resp.read().decode("utf-8"))
            return (data.get("session") or {}).get("live_stream_token")

        except Exception as e:
            self._logger.warning(f"[fishtank] Login error: {e}")
            return None

    @staticmethod
    def _common_headers():
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Origin": "https://www.fishtank.live",
            "Referer": "https://www.fishtank.live/",
        }

    @staticmethod
    def _decode_jwt_exp(jwt_string):
        """Extract the 'exp' claim from a JWT without verifying the signature.

        Returns a unix timestamp, defaulting to (now + 23h) if decoding fails.
        """
        import base64
        try:
            payload_b64 = jwt_string.split(".")[1]
            # Add padding if needed
            padding = 4 - len(payload_b64) % 4
            if padding != 4:
                payload_b64 += "=" * padding
            payload = json.loads(base64.b64decode(payload_b64).decode("utf-8"))
            return int(payload.get("exp", time.time() + 82800))
        except Exception:
            return int(time.time() + 82800)  # fallback: treat as valid for 23h


# Catalog ids look like dirc-5, cameraman2-5, computer-lab2-5 (trailing -N is
# the season). Names in CAMERA_ALIASES go stale; a matching raw id is enough.
_FISHTANK_STREAM_ID_RE = re.compile(
    r"^[a-z0-9]+(?:-[a-z0-9]+)*-\d+$", re.IGNORECASE
)


def is_fishtank_stream_id(name):
    """True if *name* looks like a Fishtank MistServer stream id."""
    return bool(name and _FISHTANK_STREAM_ID_RE.match(str(name).strip()))


def is_known_fishtank_camera(name):
    """True if *name* is a CAMERA_ALIASES key/value or a raw stream id."""
    if not name:
        return False
    raw = str(name).strip()
    if is_fishtank_stream_id(raw):
        return True
    normalised = raw.lower().replace(" ", "").replace("-", "")
    aliases = FishtankAuth.CAMERA_ALIASES
    return normalised in aliases or raw in aliases or raw in aliases.values()


def check_stream_fishtank(stream_id, auth, logger, timeout=20):
    """Check if a fishtank.live camera stream is currently live.

    Primary check: /v1/live-streams API liveStreamStatus field.
    Fallback check: some streams (e.g. dirc-5 / Director Mode) are served
    from a dedicated node and never appear in liveStreamStatus even when live.
    For those we probe the stream URL directly with a GET request and treat
    a non-404 response as live.

    Args:
        stream_id: Canonical stream ID (e.g. "dirc-5") or friendly name.
        auth: A FishtankAuth instance.
        logger: Logger adapter.
        timeout: Request timeout in seconds.

    Returns (is_live: bool, stream_name: str | None, error: str | None).
    """
    import urllib.request, urllib.error

    resolved_id = auth.resolve_stream_id(stream_id)

    # Fetch the streams list — also populates auth._all_stream_names and
    # caches the load balancer host from any online stream
    live_streams = auth.get_live_streams()
    if not live_streams and not auth._all_stream_names:
        return False, None, "failed to reach fishtank API"

    # Look up the friendly name for logging
    stream_name = (
        live_streams.get(resolved_id)
        or auth._all_stream_names.get(resolved_id)
        or resolved_id
    )

    # ── Fast path: stream is in liveStreamStatus ─────────────────────────
    if resolved_id in live_streams:
        logger.info(f"[fishtank] Stream '{resolved_id}' ({stream_name}) is listed as online")
        jwt = auth.get_jwt()
        if not jwt:
            return False, stream_name, "could not obtain JWT for stream"
        return True, stream_name, None

    # ── Fallback: stream absent from liveStreamStatus — probe directly ────
    # Director Mode and some other streams are never in liveStreamStatus even
    # when live (confirmed from browser HAR: dirc-5 absent from status dict
    # while clearly playing in browser).  We attempt a direct GET to the
    # stream URL; anything other than 404 means the stream is accessible.
    logger.info(
        f"[fishtank] Stream '{resolved_id}' not in status list — probing directly")

    jwt = auth.get_jwt()
    if not jwt:
        return False, stream_name, "could not obtain JWT for probe"

    probe_host = auth._stream_hosts.get(resolved_id, auth._stream_host)
    # Probe the HLS playlist — same endpoint we record from
    probe_url = f"https://{probe_host}/hls/live+{resolved_id}/index.m3u8?jwt={jwt}"
    try:
        req = urllib.request.Request(
            probe_url, method="GET",
            headers=FishtankAuth._common_headers(),
        )
        # We only need the response headers — close immediately
        resp = urllib.request.urlopen(req, timeout=timeout)
        resp.close()
        logger.info(
            f"[fishtank] Stream '{resolved_id}' probe succeeded "
            f"(HTTP {resp.status}) — treating as live")
        return True, stream_name, None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.info(
                f"[fishtank] Stream '{resolved_id}' probe returned 404 — offline")
            return False, None, None
        # Any other HTTP status (200, 206, 302…) means the stream exists
        logger.info(
            f"[fishtank] Stream '{resolved_id}' probe HTTP {e.code} — treating as live")
        return True, stream_name, None
    except urllib.error.URLError as e:
        logger.warning(
            f"[fishtank] Stream '{resolved_id}' probe failed: {e.reason}")
        return False, None, str(e.reason)
    except Exception as e:
        logger.warning(
            f"[fishtank] Stream '{resolved_id}' probe error: {e}")
        return False, None, str(e)


def resolve_best_fishtank_variant(master_url, jwt, logger, timeout=10):
    """Fetch a MistServer HLS master playlist and return the highest-bandwidth variant URL.

    MistServer's master playlist (`index.m3u8`) lists multiple quality renditions.
    ffmpeg, when given a master playlist directly, simply picks whichever rendition
    appears *first* in the file — and MistServer does not guarantee a consistent
    ordering across different streaming nodes or reconnections.  This means ffmpeg
    can silently grab the 360p/500kbps rendition one session and 1080p/5000kbps
    the next, depending solely on how the backend ordered the playlist that time.

    This function parses the master playlist, finds the EXT-X-STREAM-INF entry
    with the highest BANDWIDTH value, and returns its absolute URL so ffmpeg
    receives a specific variant playlist rather than the master.

    Returns the best-variant URL string, or master_url unchanged if the playlist
    cannot be fetched/parsed (fail-open so recording still proceeds).
    """
    import urllib.request, urllib.error, re as _re

    try:
        req = urllib.request.Request(
            master_url,
            headers=FishtankAuth._common_headers(),
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"[fishtank] Could not fetch master playlist for variant selection: {e} — using master URL")
        return master_url, ""

    # Parse EXT-X-STREAM-INF lines.  Each entry looks like:
    #   #EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,FRAME-RATE=30,...
    #   <relative or absolute URI on the next line>
    best_bandwidth = -1
    best_uri = None
    best_resolution = "unknown"   # track resolution of the winning entry, not the last-parsed
    best_frame_rate = None        # track frame rate of the winning entry
    lines = content.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXT-X-STREAM-INF:"):
            # Extract BANDWIDTH value
            bw_match = _re.search(r"BANDWIDTH=(\d+)", line)
            bandwidth = int(bw_match.group(1)) if bw_match else 0
            # RESOLUTION value is terminated by a comma or end-of-field — NOT \S+ which would
            # greedily consume FRAME-RATE=...,CODECS=... into the captured group.
            res_match = _re.search(r"RESOLUTION=(\d+x\d+)", line)
            resolution = res_match.group(1) if res_match else "unknown"
            # FRAME-RATE is a decimal like 30.000 or 29.97
            fr_match = _re.search(r"FRAME-RATE=([\d.]+)", line)
            frame_rate = fr_match.group(1) if fr_match else None
            # URI is on the very next non-empty line
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines):
                uri = lines[j].strip()
                if uri and not uri.startswith("#"):
                    if bandwidth > best_bandwidth:
                        best_bandwidth = bandwidth
                        best_uri = uri
                        best_resolution = resolution   # only update when this entry wins
                        best_frame_rate = frame_rate
            i = j + 1
            continue
        i += 1

    if best_uri is None:
        logger.info("[fishtank] Master playlist has no EXT-X-STREAM-INF entries — using master URL")
        return master_url, ""

    # Resolve relative URI against the master playlist base URL
    if best_uri.startswith("http://") or best_uri.startswith("https://"):
        # Absolute URI — use as-is; it already contains whatever auth it needs
        variant_url = best_uri
    else:
        # Relative path — resolve against the directory of the master URL
        base = master_url.split("?")[0]  # strip query string from master
        base_dir = base.rsplit("/", 1)[0]
        variant_url = f"{base_dir}/{best_uri}"
        # Only append the JWT if the variant URI doesn't already carry an auth
        # token of its own.  MistServer variant entries include ?tkn=<jwt>
        # directly in the URI; appending a second ?jwt= produces a malformed
        # double-token URL that the server rejects.
        has_own_token = "?tkn=" in best_uri or "?jwt=" in best_uri or "&tkn=" in best_uri or "&jwt=" in best_uri
        if not has_own_token and "?jwt=" in master_url:
            jwt_param = master_url.split("?jwt=", 1)[1]
            variant_url = f"{variant_url}?jwt={jwt_param}"

    # Build a compact stream-info string for the status table
    # e.g. "1080p · 30fps · 4.3Mbps" — best-effort, gracefully omits unknowns
    _res = best_resolution if best_resolution and best_resolution != "unknown" else None
    _kbps = best_bandwidth // 1000 if best_bandwidth > 0 else None
    _parts = []
    if _res:
        # Normalise "1920x1080" → "1080p", "1280x720" → "720p", passthrough otherwise
        if "x" in _res:
            try:
                _h = int(_res.split("x", 1)[1])
                _parts.append(f"{_h}p")
            except ValueError:
                _parts.append(_res)
        else:
            _parts.append(_res)
    if best_frame_rate:
        # Round to nearest integer: "30.000" → "30fps", "29.97" → "30fps"
        try:
            _fps = round(float(best_frame_rate))
            _parts.append(f"{_fps}fps")
        except ValueError:
            pass
    if _kbps:
        if _kbps >= 1000:
            _parts.append(f"{_kbps/1000:.1f}Mbps")
        else:
            _parts.append(f"{_kbps}kbps")
    stream_info_str = " · ".join(_parts)

    # Warn if the best available variant is suspiciously low — this can happen
    # when Fishtank's CDN is degraded and only serving a stub/audio-only rendition.
    # These sessions typically produce tiny files that fail the remux size check.
    LOW_BITRATE_WARN_KBPS = 500
    if best_bandwidth > 0 and (best_bandwidth // 1000) < LOW_BITRATE_WARN_KBPS:
        logger.warning(
            f"[fishtank] Selected best variant: {best_bandwidth // 1000}kbps "
            f"— unusually low, stream may be degraded"
        )
    else:
        logger.info(
            f"[fishtank] Selected best variant: {best_bandwidth // 1000}kbps "
            f"— using variant playlist instead of master"
        )
    return variant_url, stream_info_str


# Input protocols allowed when ffmpeg is pulling a *network* stream (Fishtank
# HLS, Chaturbate CMAF merge). `file` is intentionally omitted: a hostile
# playlist can reference file:// paths, and the output .ts is written by the
# muxer regardless of this input whitelist.
FFMPEG_NETWORK_PROTOCOLS = "http,https,tcp,tls,crypto,hls"
_FFMPEG_NETWORK_PROTOCOLS = FFMPEG_NETWORK_PROTOCOLS


def build_recording_command_rumble_hls(stream_url, raw_file, config, verbose):
    """Record a Rumble HLS playlist with ffmpeg (no yt-dlp video-page fetch).

    The channel page JSON already exposes the live playlist. Hitting that
    URL with a Rumble Referer avoids Cloudflare 403s on rumble.com/vXXXX.
    """
    ffmpeg_path = config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg')
    loglevel = "verbose" if verbose else "warning"
    return [
        ffmpeg_path,
        "-loglevel", loglevel,
        "-protocol_whitelist", _FFMPEG_NETWORK_PROTOCOLS,
        "-headers", (
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36\r\n"
            "Origin: https://rumble.com\r\n"
            "Referer: https://rumble.com/\r\n"
        ),
        "-i", stream_url,
        "-c", "copy",
        "-f", "mpegts",
        raw_file,
    ]


def build_recording_command_fishtank(stream_url, raw_file, config, verbose):
    """Build an ffmpeg command to record a fishtank MistServer stream.

    We use ffmpeg directly rather than yt-dlp because the URL already contains
    the token as ?jwt= and yt-dlp's generic extractor would mangle query params.

    URL format (HLS master playlist, confirmed from browser HAR):
        https://<host>/hls/live+<stream_id>/index.m3u8?jwt=<24h_jwt>

    ffmpeg follows the HLS playlist, fetches segments, and writes MPEG-TS (.ts).
    """
    ffmpeg_path = config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg')
    loglevel = "verbose" if verbose else "warning"
    cmd = [
        ffmpeg_path,
        "-loglevel", loglevel,
        "-protocol_whitelist", _FFMPEG_NETWORK_PROTOCOLS,
        # Increase analyzeduration and probesize to avoid repeated
        # "Consider increasing analyzeduration/probesize" warnings on
        # fishtank HLS streams that have slow segment delivery.
        "-analyzeduration", "2000000",
        "-probesize", "10000000",
        "-headers", (
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36\r\n"
            "Origin: https://www.fishtank.live\r\n"
            "Referer: https://www.fishtank.live/\r\n"
        ),
        "-i", stream_url,
        "-c", "copy",
        "-f", "mpegts",
        raw_file,
    ]
    return cmd


def build_recording_command_ffmpeg_merge(video_url, audio_url, raw_file, config, verbose, manifest_url=None, http_headers=None):
    """Build an ffmpeg command to mux split video+audio HLS streams in real-time.

    Used for CMAF/fMP4 HLS streams (e.g. Chaturbate) where yt-dlp exposes
    separate video-only and audio-only tracks with no pre-muxed combined stream.

    yt-dlp's bestvideo+bestaudio requires downloading both tracks and running a
    separate merge pass at the end — which never happens for a live stream that
    never ends.  Driving ffmpeg directly with both HLS URLs sidesteps this:
    ffmpeg follows both playlists concurrently and writes a muxed MPEG-TS stream
    to disk continuously, identical to the fishtank approach.

    Preferred: pass manifest_url (the HLS master playlist).  The master URL
    carries a long-lived JWT token.  Individual chunklist URLs (video_url /
    audio_url) carry short-lived session tokens that may expire between the
    yt-dlp check phase and ffmpeg startup.  When ffmpeg reads the master
    playlist directly, the server issues it a fresh session for the chunklists.

    http_headers: the http_headers dict from yt-dlp's format JSON, passed
    verbatim to ffmpeg via -headers.  Using yt-dlp's exact headers avoids
    Cloudflare rejections caused by mismatched header fingerprints.  Falls
    back to a plain browser UA if not provided.
    """
    ffmpeg_path = config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg')
    loglevel = "verbose" if verbose else "warning"

    # Build the -headers string from yt-dlp's own http_headers dict so ffmpeg
    # presents the exact same fingerprint to the CDN that yt-dlp used.
    if http_headers:
        headers = "".join(f"{k}: {v}\r\n" for k, v in http_headers.items())
    else:
        headers = (
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36\r\n"
        )

    if video_url and audio_url:
        # Preferred: two-input merge using the individual video+audio chunklist
        # URLs (with session tokens).  The master JWT URL is likely single-use —
        # yt-dlp consumes it during --dump-json format enumeration, invalidating
        # it before ffmpeg can reuse it.  Session tokens on chunklist URLs survive
        # independently and work for the duration of the stream (VLC confirms this).
        cmd = [
            ffmpeg_path,
            "-loglevel", loglevel,
            "-protocol_whitelist", _FFMPEG_NETWORK_PROTOCOLS,
            "-headers", headers,
            "-i", video_url,
            "-i", audio_url,
            "-c", "copy",
            "-map", "0:v",
            "-map", "1:a",
            "-f", "mpegts",
            raw_file,
        ]
    elif manifest_url:
        # Fallback: master HLS playlist when individual track URLs aren't available.
        cmd = [
            ffmpeg_path,
            "-loglevel", loglevel,
            "-protocol_whitelist", _FFMPEG_NETWORK_PROTOCOLS,
            "-headers", headers,
            "-i", manifest_url,
            "-c", "copy",
            "-f", "mpegts",
            raw_file,
        ]
    else:
        # Should not reach here — caller guarantees at least one URL is set.
        raise ValueError("build_recording_command_ffmpeg_merge: no usable URL provided")
    return cmd


def check_stream_streamlink(url, logger, timeout=30):
    """Check if Twitch stream is live using streamlink.

    Returns (is_live: bool, stream_title: str | None, error: str | None).
    """
    check_cmd = ["streamlink", "--json", url]
    logger.info(f"Check cmd: {redact_cmd_for_log(check_cmd)}")

    try:
        check = subprocess.run(check_cmd, capture_output=True, text=True, timeout=timeout)
        logger.info(f"Check returncode={check.returncode}")

        title = None
        if check.returncode == 0 and check.stdout:
            try:
                data = json.loads(check.stdout)
                # streamlink --json returns metadata.title for some plugins
                metadata = data.get("metadata", {})
                title = metadata.get("title") or metadata.get("author")
            except json.JSONDecodeError:
                pass

            if '"error"' not in check.stdout or ('"url"' in check.stdout or '"playback_url"' in check.stdout):
                return True, title, None
        return False, title, None

    except subprocess.TimeoutExpired:
        logger.warning("Stream check timed out")
        return False, None, "timeout"
    except FileNotFoundError:
        logger.error("streamlink not found in PATH")
        return False, None, "streamlink not found"
    except Exception as e:
        logger.error(f"Unexpected error checking stream: {e}")
        return False, None, str(e)


# ────────────────────────────────────────────────
#          Recording Functions
# ────────────────────────────────────────────────

def build_recording_command_ytdlp(url, raw_file, config, verbose, streamlink_debug,
                                  cookies_file=None, impersonate=False):
    """Build yt-dlp command for live stream recording (Kick, YouTube, custom).

    For live HLS streams, yt-dlp's default fragment-based downloader buffers
    everything in memory and only writes on completion — which never happens
    for a live stream.  Instead, we use ffmpeg as an external downloader with
    --hls-use-mpegts, which writes a continuous MPEG-TS stream directly to the
    output file in real-time.

    Args:
        impersonate: If True, adds --impersonate chrome for Cloudflare-protected
                     sites (requires curl_cffi).
    """
    cmd = [
        "yt-dlp",
    ]

    # Browser impersonation for Cloudflare-protected sites (e.g. Rumble)
    if impersonate and HAS_CURL_CFFI:
        cmd.extend(["--impersonate", "chrome"])

    cmd.extend([
        url,
        "-f", "bestvideo+bestaudio/b/best",  # merged tracks first (e.g. Chaturbate), fallback to single-file "b" or "best"
        "-o", raw_file,
        "--no-part",
        "--no-mtime",
        "--retries", "10",
        "--fragment-retries", "10",
        # Force ffmpeg as external downloader for live HLS — this writes
        # directly to the output file instead of buffering fragments
        "--downloader", "ffmpeg",
        "--hls-use-mpegts",
        # Input-side whitelist (ffmpeg_i: must come before -i). ffmpeg's
        # default HLS whitelist includes file:// — a hostile playlist
        # could otherwise pull local paths into the .ts.
        "--downloader-args",
        f"ffmpeg_i:-protocol_whitelist {FFMPEG_NETWORK_PROTOCOLS}",
        # NOTE: Do NOT add --downloader-args "ffmpeg:-re" — the -re flag is an
        # INPUT option in ffmpeg, but yt-dlp's --downloader-args appends it
        # after the output file, causing "Error parsing options for output file"
        # and ffmpeg exit code 4294967274 (-22 / EINVAL).  Live HLS streams
        # are inherently rate-limited by the server, so -re isn't needed.
    ])

    if cookies_file:
        cmd.extend(["--cookies", cookies_file])

    # YouTube player client selection.
    #
    # This used to be hardcoded to `player_client=web` to sidestep the
    # n-challenge JS solver.  That backfired: YouTube now requires a GVS PO
    # Token for the `web` client's formats (and binds that token to the video
    # ID), so forcing `web` without a PO Token provider makes yt-dlp report
    #     "No video formats found!"
    # and the recording dies instantly — while the *check* command, which
    # never forced a client, kept reporting the stream as live.  That
    # mismatch is what made this look like a detection bug rather than a
    # download one.
    #
    # yt-dlp's own defaults (tv,ios,web — or tv,web with cookies) are chosen
    # to prefer clients that do NOT currently need a PO Token, so the right
    # move is to stay out of the way and let it pick.  The JS challenge that
    # forcing `web` was avoiding is handled properly nowadays by yt-dlp-ejs
    # with Deno installed (see requirements.txt).
    #
    # Left configurable because YouTube changes which clients work every few
    # months: set youtube_player_client in [Advanced] to override without
    # editing code (e.g. "tv", "mweb", "default,-web").  Blank = yt-dlp's
    # default, which is what you want unless you're working around a
    # regression.
    if "youtube.com" in url or "youtu.be" in url:
        player_client = (config.get('Advanced', 'youtube_player_client', fallback='') or '').strip()
        if player_client:
            cmd.extend(["--extractor-args", f"youtube:player_client={player_client}"])

    if verbose or streamlink_debug:
        cmd.append("--verbose")

    return cmd


def build_recording_command_streamlink_kick(url, raw_file, quality, verbose, streamlink_debug):
    """Build an alternate streamlink command for Kick recording.

    Kick's primary recorder is already streamlink. This shorter-retry,
    always-debug argv is used only when that first command exits without
    creating a file.
    """
    cmd = [
        "streamlink",
        "--http-header", "User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        url, quality,
        "--retry-streams", "10",
        "--retry-max", "3",
        "--retry-open", "3",
        "--stream-segment-threads", "3",
        "--stream-segment-timeout", "60",
        "-o", raw_file,
        # Always use debug for the fallback path so we can diagnose issues
        "--loglevel", "debug",
    ]

    return cmd


def build_recording_command_streamlink(url, raw_file, quality, platform, config, verbose, streamlink_debug):
    """Build streamlink command for Twitch/Kick recording."""
    cmd = ["streamlink"]

    if platform == "twitch":
        cmd.extend([
            "--twitch-disable-ads",
            "--twitch-low-latency",
            "--http-header", "User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ])
    elif platform == "kick":
        cmd.extend([
            "--http-header", "User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ])
    elif platform == "youtube":
        cmd.extend([
            "--http-header", "User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ])

    cmd.extend([
        url, quality,
        "--retry-streams", "30",
        "--retry-max", "10",
        "--retry-open", "3",
        "--stream-segment-threads", "3",
        "--stream-segment-timeout", "60",
        "-o", raw_file,
    ])

    if streamlink_debug:
        cmd += ["--loglevel", "debug"]
    elif verbose:
        cmd += ["--loglevel", "info"]
    else:
        cmd += ["--loglevel", "warning"]

    return cmd

