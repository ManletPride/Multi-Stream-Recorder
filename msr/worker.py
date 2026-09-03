"""Clips, remux, process monitor, and the per-channel recording worker.

``record_worker`` is the multiprocessing target — it must stay a top-level
function in this module so Windows ``spawn`` can pickle it.
"""
import configparser
import datetime
import json
import logging
import os
import random
import shutil
import subprocess
import threading
import time

from msr import __version__
from msr.config import (
    default_streams_dir,
    parser_getboolean,
    parser_getfloat,
    parser_getint,
)
from msr.deps import HAS_CURL_CFFI, HAS_YTDLP
from msr.platforms import (
    FishtankAuth,
    build_recording_command_ffmpeg_merge,
    build_recording_command_fishtank,
    build_recording_command_rumble_hls,
    build_recording_command_streamlink,
    build_recording_command_streamlink_kick,
    build_recording_command_ytdlp,
    check_stream_fishtank,
    check_stream_kick_api,
    check_stream_rumble_html,
    check_stream_streamlink,
    check_stream_ytdlp,
    check_tiktok_live_webcast,
    resolve_best_fishtank_variant,
)
from msr.util import (
    PENDING_DELETION_FOLDER,
    build_filename,
    channel_key_to_dirs,
    check_disk_space,
    custom_url_folder_names,
    ffprobe_from_ffmpeg,
    parse_channel_key,
    format_elapsed,
    human_size,
    interruptible_sleep,
    jittered_sleep,
    find_cookies_file,
    kill_process_tree,
    redact_cmd_for_log,
    redact_for_log,
    sanitize_path_component,
    setup_child_logging,
    text_progress_bar,
)

# ────────────────────────────────────────────────
#          Instant Clips & Screenshots
# ────────────────────────────────────────────────
#
# Lets the GUI cut a short clip (or grab a screenshot) out of a channel's
# .ts file *while it's still being recorded*, without touching the
# recording worker at all. This works because:
#
#   1. The raw file lives at a deterministic path (see channel_key_to_dirs)
#      and only one .ts is actively growing per channel at a time.
#   2. Reading a file that another process is still appending to is already
#      how this app gets stream info (_probe_stream_info_thread) and watches
#      for resolution changes (_watch_resolution_change_thread) — ffmpeg/
#      ffprobe just read whatever has been flushed to disk and stop there,
#      the same way `tail` would. No lock contention with the writer.
#   3. Everything below runs as a plain stream copy (-c copy), so it's a
#      repackage, not a re-encode — a 30-second clip out of a multi-hour
#      file takes a fraction of a second regardless of the recording's
#      total length.

def find_active_recording_file(recorder, channel_key):
    """Locate the .ts file currently being written for a channel.

    Resolves the on-disk directory the same way BackgroundCleaner does
    (channel_key_to_dirs), then returns the most recently modified .ts
    file in it. A channel directory normally holds at most one live .ts —
    older ones are moved to PendingDeletion by the worker as soon as
    they're remuxed — so "newest by mtime" reliably means "the one still
    growing," without needing the worker to publish its file path over IPC.

    Returns None if the channel has no recognizable directory or no .ts
    file is present (e.g. the stream is still being detected/starting up).
    """
    try:
        platform, username_dir = channel_key_to_dirs(channel_key)
    except Exception:
        return None
    channel_dir = os.path.join(recorder.recorded_base, platform, username_dir)
    if not os.path.isdir(channel_dir):
        return None
    try:
        ts_files = [
            os.path.join(channel_dir, f) for f in os.listdir(channel_dir)
            if f.lower().endswith('.ts')
        ]
    except Exception:
        return None
    if not ts_files:
        return None
    return max(ts_files, key=os.path.getmtime)


def _parse_ffprobe_packet_csv(stdout):
    """Parse ffprobe ``csv=p=0`` packet lines into (pts_times, keyframe_pts).

    ``-show_entries packet=pts_time,dts_time,flags -of csv=p=0`` often
    emits a trailing comma (``pts,dts,K__,``). Empty fields must be
    dropped or *flags* is ``''`` and every keyframe is missed.
    """
    times, keys = [], []
    for line in (stdout or "").splitlines():
        parts = [p.strip() for p in line.split(",") if p.strip()]
        if len(parts) < 2:
            continue
        flags = parts[-1]
        ts = None
        for raw in parts[:-1]:
            if raw.upper() != "N/A":
                try:
                    ts = float(raw)
                    break
                except ValueError:
                    continue
        if ts is None:
            continue
        times.append(ts)
        if "K" in flags.upper():
            keys.append(ts)
    return times, keys


def _elapsed_media_duration(raw_duration, start_time=0.0):
    """Return elapsed file time from an ffprobe duration + format.start_time.

    Live MPEG-TS (Twitch especially, Kick sometimes) reports
    ``format.duration`` as the *last PCR/PTS* (e.g. 12686) rather than
    last-minus-start (e.g. 67). A 15s Clip Now then computes
    ``cut = 12671``, seeks to elapsed ~65s of a 67s file, and only gets
    the last GOP — the powdur clip was 2.6s. Screenshots using
    ``-ss duration-3`` miss the file entirely.

    Kick already worked because ffprobe gave *elapsed* duration there
    (180 < start_time 10000, so this does not subtract). Only treat
    duration as last-PTS when it is greater than a large PCR start_time.
    """
    if raw_duration is None or raw_duration <= 0:
        return None
    start_time = start_time or 0.0
    # PCR clocks on Kick/Twitch live TS sit in the thousands of seconds.
    # A 90s start_time with a genuine elapsed duration of 180 must not
    # be rewritten to 90. 1000s is below observed live PCR (~10k–12k)
    # and above normal file-start offsets.
    if start_time >= 1000.0 and raw_duration > start_time:
        return raw_duration - start_time
    return raw_duration


def _clip_seek_from_packets(times, keys, clip_seconds, duration, start_time,
                            window_to_eof, past_cut=2.0):
    """Pick the clip start keyframe and ffmpeg seek args from a packet window.

    ``times`` / ``keys`` are PTS values from ffprobe. Input ``-ss`` is
    elapsed file time (``kf_pts - start_time``), never the raw PCR PTS —
    Kick MPEG-TS clocks often sit at ~10000s while the capture is only
    minutes long.

    Returns ``(kf_pts, output_duration, input_seek_args, target_pts)``
    or None if the window has no packets/keyframes.
    """
    if not times or not keys:
        return None

    have_duration = duration is not None and duration > 0
    min_pts, end_pts = min(times), max(times)

    target_pts = None
    if have_duration:
        cut = max(0.0, duration - clip_seconds)
        guessed = start_time + cut
        if min_pts - 1.0 <= guessed <= end_pts + 1.0:
            target_pts = guessed
    if target_pts is None:
        target_pts = (end_pts - clip_seconds) if window_to_eof else (end_pts - past_cut)

    preceding = [k for k in keys if k <= target_pts + 0.05]
    kf_pts = preceding[-1] if preceding else keys[0]

    after_window = 0.0 if window_to_eof else max(0.0, clip_seconds - past_cut)
    from_kf_to_end = (end_pts - kf_pts) + after_window
    if have_duration:
        from_kf_to_end = max(from_kf_to_end, (start_time + duration) - kf_pts)
    output_duration = max(clip_seconds, from_kf_to_end) + 0.5

    # Seek *to* the keyframe (tiny undershoot, never past it): seeking
    # mid-GOP makes video wait for the next IDR and recreates the freeze.
    if have_duration:
        kf_elapsed = max(0.0, kf_pts - start_time)
        input_seek_args = ["-ss", f"{max(0.0, kf_elapsed - 0.05):.3f}"]
    else:
        input_seek_args = ["-sseof", f"-{from_kf_to_end:.3f}"]

    return kf_pts, output_duration, input_seek_args, target_pts


def _find_clip_keyframe(ffmpeg_path, raw_file, clip_seconds, duration, logger, timeout=15):
    """Locate the video keyframe Clip Now should cut from.

    A plain ``-ss`` + ``-c copy`` seek on MPEG-TS lands mid-GOP: audio
    (every packet is independently decodable) starts immediately, but video
    has to wait for the next IDR. ``-avoid_negative_ts make_zero`` then puts
    audio at t=0 and video at t=+GOP — the clip opens frozen until that
    first keyframe (the cobbruvs 15s clip: audio at 0s, video at 1.2s).
    Cutting *at* the preceding keyframe and dropping earlier audio makes
    both streams start together.

    Returns (kf_pts, output_duration, input_seek_args) or None if probing
    fails — the caller then falls back to an unaligned seek. input_seek_args
    use elapsed time (kf_pts - start_time), which is what ffmpeg's input
    ``-ss`` actually seeks on MPEG-TS.
    """
    # Look back far enough to catch a full live GOP (HLS is typically 2s;
    # some YouTube/Kick variants go to ~10s).
    lookback = 12.0

    ffprobe_path = ffprobe_from_ffmpeg(ffmpeg_path)

    def _run_probe(extra_args):
        cmd = [
            ffprobe_path, "-v", "error",
            "-probesize", "50M", "-analyzeduration", "20M",
            "-select_streams", "v:0",
            "-show_entries", "packet=pts_time,dts_time,flags",
            "-of", "csv=p=0",
            *extra_args,
            raw_file,
        ]
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except Exception:
            return None

    start_time = 0.0
    try:
        st = subprocess.run(
            [ffprobe_path, "-v", "error",
             "-show_entries", "format=start_time",
             "-of", "default=noprint_wrappers=1:nokey=1", raw_file],
            capture_output=True, text=True, timeout=timeout,
        )
        val = (st.stdout or "").strip()
        if val and val != "N/A":
            start_time = float(val)
    except Exception:
        pass

    elapsed = _elapsed_media_duration(duration, start_time)
    if (
        elapsed is not None and duration is not None
        and abs(elapsed - duration) > 1.0
    ):
        logger.info(
            f"Clip: duration {duration:.1f}s is last PTS (start_time "
            f"{start_time:.1f}s) — using elapsed {elapsed:.1f}s"
        )
    duration = elapsed

    # Probe a small window around the cut point — not the whole clip, so a
    # 30-minute Clip Now doesn't dump half an hour of packets. The +2s past
    # the cut lets us identify the last keyframe *at or before* the cut
    # without needing the rest of the clip.
    past_cut = 2.0
    have_duration = duration is not None and duration > 0
    if have_duration:
        cut = max(0.0, duration - clip_seconds)
        probe_from = max(0.0, cut - lookback)
        # -read_intervals is in pts_time, not elapsed-from-zero.
        extra = ["-read_intervals",
                 f"{start_time + probe_from:.3f}%+{lookback + past_cut:.3f}"]
        window_to_eof = probe_from + lookback + past_cut >= duration - 0.5
    elif clip_seconds > lookback + 5:
        extra = ["-sseof", f"-{clip_seconds + lookback:.3f}",
                 "-read_intervals", f"%+{lookback + past_cut:.3f}"]
        window_to_eof = False
    else:
        extra = ["-sseof", f"-{clip_seconds + lookback:.3f}"]
        window_to_eof = True

    result = _run_probe(extra)
    if result is None or not (result.stdout or "").strip():
        return None

    times, keys = _parse_ffprobe_packet_csv(result.stdout)
    aligned = _clip_seek_from_packets(
        times, keys, clip_seconds, duration, start_time, window_to_eof, past_cut,
    )
    if aligned is None:
        return None

    kf_pts, output_duration, input_seek_args, target_pts = aligned
    snap = target_pts - kf_pts
    if snap >= 0:
        snap_note = f"{snap:.2f}s before requested start"
    else:
        snap_note = f"{-snap:.2f}s after requested start — no earlier keyframe in window"
    logger.info(f"Clip: keyframe-aligned at pts {kf_pts:.3f}s ({snap_note})")
    return kf_pts, output_duration, input_seek_args


def create_clip(raw_file, output_file, clip_seconds, ffmpeg_path, logger, timeout=60):
    """Cut the last *clip_seconds* out of raw_file into output_file (.mp4).

    Probes the current duration, finds the last video keyframe at or before
    (duration - clip_seconds), and stream-copies from that keyframe's
    *elapsed* position (PTS minus format.start_time). MPEG-TS PCR often
    runs hours ahead of the file; using the raw PTS as ``-ss`` misses the
    cut and leaves audio starting at 0 while video waits for the next IDR.
    ``-t`` is extended so the clip still reaches the live edge. If the
    recording is younger than the requested clip length, the whole file
    is used instead.

    Returns (success: bool, message: str) — message is an error reason on
    failure, or the actual clip length used on success.
    """
    if not os.path.exists(raw_file):
        return False, "recording file not found"

    duration = _probe_duration(ffmpeg_path, raw_file)

    # Duration is a nice-to-have, not a requirement.  Some live MPEG-TS
    # captures never report one (see _probe_duration for why), and refusing
    # to clip in that case is wrong: ffmpeg can seek relative to the END of
    # the file with -sseof without knowing the total length at all.  That's
    # the same fallback create_screenshot uses, which is exactly why
    # screenshots kept working on files where clipping bailed out.
    aligned = _find_clip_keyframe(ffmpeg_path, raw_file, clip_seconds, duration, logger)
    output_time_args = None
    if aligned is not None:
        kf_pts, actual_length, seek_args = aligned
        # No output -ss / -copyts: those mixed PCR PTS with elapsed seeks
        # on Kick MPEG-TS and left audio at t=0, video at t=+GOP.
        output_time_args = ["-t", f"{actual_length:.3f}"]
        seek_note = " (keyframe-aligned)"
    elif duration is not None and duration > 0:
        start_offset = max(0.0, duration - clip_seconds)
        actual_length = duration - start_offset
        seek_args = ["-ss", f"{start_offset:.2f}"]
        seek_note = ""
        logger.info("Clip: keyframe probe failed — using unaligned input seek")
    else:
        seek_args = ["-sseof", f"-{clip_seconds:.2f}"]
        actual_length = clip_seconds  # best estimate; may be shorter if the
                                      # recording is younger than clip_seconds
        seek_note = " (end-relative seek — duration unavailable)"
        logger.info("Clip: recording duration unreadable, seeking from end of file instead")

    if output_time_args is None:
        output_time_args = ["-t", f"{clip_seconds:.2f}"]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    def _base_cmd(map_args):
        return [
            ffmpeg_path, "-hide_banner", "-y",
            *seek_args,
            "-i", raw_file,
            *output_time_args,
            "-c", "copy", *map_args,
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart",
            "-loglevel", "error",
            output_file,
        ]

    def _run(cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return "timeout"
        except FileNotFoundError:
            return "not_found"
        except Exception as e:
            return str(e)

    # Map only video/audio, not every stream. Some platforms (Twitch, notably)
    # mux a timed_id3 metadata track into the .ts that MP4 can't hold — the
    # main remux_to_mp4() path already strips this; clips need the same
    # treatment or ffmpeg fails with "Could not write header (incorrect
    # codec parameters?)" trying to box a stream type MP4 doesn't support.
    result = _run(_base_cmd(["-map", "0:v?", "-map", "0:a?"]))

    if isinstance(result, str):
        return False, {"timeout": "ffmpeg timed out", "not_found": "ffmpeg not found"}.get(result, result)

    if (result.returncode != 0 or not os.path.exists(output_file)) and "Could not write header" in (result.stderr or ""):
        # Last resort: drop audio too, in case a second/odd audio track is
        # itself the incompatible stream. A silent clip beats no clip.
        logger.warning("Clip: header write failed with audio mapped — retrying video-only")
        result = _run(_base_cmd(["-map", "0:v:0"]))
        if isinstance(result, str):
            return False, {"timeout": "ffmpeg timed out", "not_found": "ffmpeg not found"}.get(result, result)

    if result.returncode != 0 or not os.path.exists(output_file):
        err = (result.stderr or "").strip().splitlines()
        return False, err[-1] if err else f"ffmpeg exited {result.returncode}"

    out_size = os.path.getsize(output_file)
    if out_size < 1024:
        try:
            os.remove(output_file)
        except Exception:
            pass
        return False, "output clip is empty — try again in a few seconds"

    logger.info(f"Clip saved: {output_file} ({human_size(out_size)}, "
                f"~{actual_length:.0f}s){seek_note}")
    return True, f"{actual_length:.0f}s"


#: Encoder arguments per screenshot format.  The quality knob means a
#: different thing in each codec, which is why they're mapped separately
#: rather than passing a bare -q:v to everything:
#:
#:   jpg   -q:v  2–31, LOWER is better.  ~2 is visually lossless and lands
#:               around 200–400 KB for 1080p — roughly a fifth of PNG.
#:   webp  -quality 0–100, HIGHER is better.  Smaller than JPEG at
#:               equivalent quality, but less universally accepted by
#:               older image viewers and some chat clients.
#:   png   lossless; -q:v is silently ignored by the encoder (which is why
#:               the old code's "-q:v 2" did nothing and every grab came
#:               out at full ~2 MB).  compression_level trades CPU for size
#:               but stays lossless, so it only ever saves a little.
SCREENSHOT_FORMATS = {
    "jpg":  {"ext": ".jpg",  "args": lambda q: ["-q:v", str(q)]},
    "jpeg": {"ext": ".jpg",  "args": lambda q: ["-q:v", str(q)]},
    "webp": {"ext": ".webp", "args": lambda q: ["-quality", str(q), "-lossless", "0"]},
    "png":  {"ext": ".png",  "args": lambda q: ["-compression_level", "9"]},
}


def screenshot_extension(fmt):
    """Return the file extension for a configured screenshot format."""
    return SCREENSHOT_FORMATS.get(str(fmt).strip().lower(), SCREENSHOT_FORMATS["jpg"])["ext"]


def create_screenshot(raw_file, output_file, ffmpeg_path, logger, offset_seconds=3.0,
                      timeout=30, fmt="jpg", quality=2):
    """Grab a single frame from near the current end of raw_file.

    Unlike create_clip, this has to actually *decode* video to produce a
    still image — and that's what makes the very end of a live .ts file
    hostile: the final fraction of a second on disk is a partially-flushed
    TS packet and an incomplete GOP, with the frame's remaining slices
    still buffered in the recording process. Decoding into that region
    fails ("error while decoding MB …, bytestream -7"), and because
    ffmpeg treats a failed decode of a single frame as a non-fatal
    condition, it can also exit 0 having written no file at all — which
    is exactly the two errors this replaced.

    So instead of grabbing the literal last frame, seek to a few seconds
    *before* the end, where the data is complete, and retry with a
    progressively larger backoff if the first attempt still lands in a
    damaged region. Seeking is done from the start (-ss on the input)
    using the duration ffprobe reports, since ffprobe only counts complete
    data — this is the same reason create_clip's stream copy never hit
    the problem.

    fmt selects the output codec (see SCREENSHOT_FORMATS); the extension of
    output_file should already match it — use screenshot_extension().
    """
    if not os.path.exists(raw_file):
        return False, "recording file not found"

    fmt_key = str(fmt).strip().lower()
    spec = SCREENSHOT_FORMATS.get(fmt_key)
    if spec is None:
        logger.warning(f"Unknown screenshot_format '{fmt}' — falling back to jpg")
        fmt_key, spec = "jpg", SCREENSHOT_FORMATS["jpg"]

    # The jpg and webp quality scales run in opposite directions, so a value
    # left over from switching formats can silently produce a garbage image
    # (webp at "2" is near-unreadable, not near-lossless).  Clamp anything
    # outside a format's sane range back to a good default rather than
    # handing it to ffmpeg as-is.
    if fmt_key in ("jpg", "jpeg") and not 2 <= quality <= 31:
        logger.warning(f"screenshot_quality {quality} out of range for jpg (2–31) — using 2")
        quality = 2
    elif fmt_key == "webp" and not 50 <= quality <= 100:
        logger.warning(f"screenshot_quality {quality} out of range for webp (50–100) — using 85")
        quality = 85

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    duration = _probe_duration(ffmpeg_path, raw_file)

    # Back off further from the live edge on each retry. The first offset is
    # normally enough; the later ones cover streams with long GOPs or a
    # recorder that buffers more before flushing.
    attempts = [offset_seconds, offset_seconds * 2, offset_seconds * 4]
    last_error = "no frame could be decoded"

    for attempt_num, offset in enumerate(attempts, start=1):
        if duration is not None and duration > 0:
            seek_args = ["-ss", f"{max(0.0, duration - offset):.2f}"]
        else:
            # ffprobe couldn't read a duration (very new file) — fall back to
            # end-relative seeking and hope there's enough complete data.
            seek_args = ["-sseof", f"-{offset:.2f}"]

        cmd = [
            ffmpeg_path, "-hide_banner", "-y",
            *seek_args,
            "-i", raw_file,
            "-map", "0:v:0", "-an",     # video only — ignore audio/timed_id3
            "-frames:v", "1", *spec["args"](quality),
            "-loglevel", "error",
            output_file,
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return False, "ffmpeg timed out"
        except FileNotFoundError:
            return False, "ffmpeg not found"
        except Exception as e:
            return False, str(e)

        # A written file is the only real success signal here: ffmpeg can
        # report returncode 0 while producing nothing when the frame it
        # tried to decode was incomplete, so returncode alone is not enough.
        wrote_file = os.path.exists(output_file) and os.path.getsize(output_file) > 1024
        if result.returncode == 0 and wrote_file:
            out_size = os.path.getsize(output_file)
            logger.info(f"Screenshot saved: {output_file} ({human_size(out_size)}, "
                        f"~{offset:.0f}s behind live)")
            return True, "ok"

        err_lines = (result.stderr or "").strip().splitlines()
        last_error = err_lines[-1] if err_lines else f"ffmpeg exited {result.returncode} without writing a frame"

        # Clear any truncated/empty leftover before the next attempt so a
        # stale partial file can't be mistaken for a success.
        if os.path.exists(output_file) and not wrote_file:
            try:
                os.remove(output_file)
            except Exception:
                pass

        if attempt_num < len(attempts):
            logger.warning(f"Screenshot: frame at ~{offset:.0f}s behind live was incomplete "
                           f"— retrying further back")

    return False, last_error


def _stderr_reader_thread(proc, logger, tool_name, verbose=False):
    """Background thread that reads and logs stderr from the recording process.

    When verbose=False, aggressively filters ffmpeg's stream info, HLS metadata,
    and segment-level logging.  Only errors/warnings pass through.  Known benign
    patterns (like ffmpeg keepalive retries) are suppressed entirely.
    """
    NOISY_PATTERNS = (
        # HLS fragment-level noise
        '[hls @', 'skip (', "opening 'http", 'prefetch:',
        '[tcp @', 'starting connection', 'successfully connected',
        '[https @', '[tls @', '[aviocontext @', 'statistics:',
        'ext-x-', 'cuepoint', 'daterange', 'program-date-time',
        # ffmpeg stream info (printed on every connect/reconnect)
        'input #', 'output #', 'stream #', 'stream mapping',
        'duration:', 'variant_bitrate', 'metadata:', 'program 0',
        'encoder', 'press [q]', 'last message repeated',
        'handler_name', '[mpegts @', '[h264 @', '[aac @',
        'reinit context', 'increasing reorder',
        'parser not found', 'pix_fmt',
        '[vist#', '[aist#',             # ffmpeg internal stream context verbose lines
        # ffmpeg progress lines
        'size=', 'bitrate=', 'speed=',
        # yt-dlp debug noise
        '[debug]', 'format sorted', 'invoking ffmpeg',
        'command-line config', 'encodings:', 'loaded ',
        'optional libraries', 'proxy map', 'request handlers',
        'plugin directories', 'js runtimes',
    )

    # Patterns that contain "error"/"fail" keywords but are actually benign
    BENIGN_ERROR_PATTERNS = (
        'keepalive request failed',     # ffmpeg HLS keepalive retry (IPv6, harmless)
        'retrying with new connection', # ffmpeg successfully retries, no data loss
    )

    # Patterns suppressed even in verbose mode — lines that are genuinely harmless
    # but can flood the log at hundreds or thousands of lines per recording session.
    ALWAYS_SUPPRESS = (
        'timestamp discontinuity',      # MistServer HLS segments have inconsistent timestamps
                                        # at segment boundaries; ffmpeg corrects the offset
                                        # automatically with no data loss.  Fires ~2x per segment
                                        # (~every 2s), producing thousands of lines per session.
        'non-monotonic dts',            # Same root cause as timestamp discontinuity — MistServer
                                        # sends segments with out-of-order decode timestamps when
                                        # reconnecting to the master URL fallback.  ffmpeg fixes
                                        # them in place (incrementing each by 1) with no data loss.
                                        # Can produce hundreds of lines in a single burst.
        'consider increasing the value for the',  # ffmpeg advisory suggesting higher analyzeduration
                                        # / probesize when falling back to master URL.  Harmless;
                                        # our values are already set generously.
        "skip ('#ext-x-",               # ffmpeg's HLS demuxer announcing every playlist tag it
                                        # doesn't handle — overwhelmingly #EXT-X-DATERANGE and
                                        # #EXT-X-CUEPOINT ad markers.  YouTube re-advertises the
                                        # full set of ad cuepoints in EVERY playlist refresh, so
                                        # this scales with (ad breaks × refreshes) and utterly
                                        # dominates the log: 1,309 of 1,530 lines (84% of all
                                        # bytes) in one 24/7 YouTube session.  It carries no
                                        # diagnostic value — "skipped a tag I don't parse" is
                                        # ffmpeg working correctly — so it's suppressed even in
                                        # verbose mode, where it would otherwise bury the output
                                        # that verbose was turned on to see.
    )

    # Per-pattern occurrence counters for throttling repetitive warnings.
    # Patterns listed here will be logged freely up to their limit, then
    # a single "muted" notice is emitted and further hits are suppressed.
    THROTTLE_PATTERNS = {
        'found duplicated moov atom': (3, "MOOV atom duplicate warnings muted after 3 hits "
                                          "(benign: each CMAF segment carries its own header box)"),
    }
    _throttle_counts = {pat: 0 for pat in THROTTLE_PATTERNS}

    try:
        for line in proc.stderr:
            line = line.rstrip()
            if not line:
                continue
            lower = line.lower()

            # Always suppress these regardless of verbose setting
            if any(pat in lower for pat in ALWAYS_SUPPRESS):
                continue

            # Throttle repetitive-but-benign warnings after N occurrences
            _throttled = False
            for pat, (limit, mute_msg) in THROTTLE_PATTERNS.items():
                if pat in lower:
                    _throttle_counts[pat] += 1
                    if _throttle_counts[pat] == limit:
                        logger.info(f"[{tool_name}] (Muting: {mute_msg})")
                    if _throttle_counts[pat] >= limit:
                        _throttled = True
                    break
            if _throttled:
                continue

            # Check if line looks like an error but is actually benign noise
            is_benign = any(pat in lower for pat in BENIGN_ERROR_PATTERNS)

            if not is_benign and any(kw in lower for kw in ['error', 'warning', 'fail', 'unable', 'denied', 'forbidden']):
                logger.warning(f"[{tool_name}] {redact_for_log(line)}")
            elif verbose:
                logger.info(f"[{tool_name}] {redact_for_log(line)}")
            else:
                if any(pat in lower for pat in NOISY_PATTERNS) or is_benign:
                    continue
                logger.info(f"[{tool_name}] {redact_for_log(line)}")
    except Exception:
        pass


def _probe_stream_info_thread(raw_file, stream_info_ref, ffmpeg_path, logger, min_size_bytes=1_500_000):
    """Background thread: once raw_file reaches min_size_bytes, run ffprobe and
    write a "WxH · Xfps · Y.YMbps" string into stream_info_ref[0].

    Works for all recorder backends (yt-dlp, streamlink, direct ffmpeg) since it
    reads the actual output file rather than parsing tool-specific stderr output.
    ffprobe is typically bundled alongside ffmpeg in the same directory.

    Only fires when stream_info_ref[0] is still None (i.e. not pre-populated by
    the Chaturbate/Fishtank format-data path).  min_size_bytes gives ffprobe
    enough data for a reliable bitrate measurement (~1.5 MB ≈ a few seconds of
    video at typical live-stream bitrates).
    """
    import json as _json
    import subprocess as _sp

    ffprobe = ffprobe_from_ffmpeg(ffmpeg_path)

    # Wait for the file to grow enough for a reliable probe (up to 60s).
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if os.path.exists(raw_file) and os.path.getsize(raw_file) >= min_size_bytes:
                break
        except OSError:
            pass
        time.sleep(1)
    else:
        return  # file never appeared or grew — give up

    try:
        result = _sp.run(
            [ffprobe, "-v", "quiet", "-print_format", "json",
             "-show_streams", "-show_format", "-select_streams", "v:0", raw_file],
            capture_output=True, text=True, timeout=10,
        )
        data = _json.loads(result.stdout)
        streams = data.get("streams", [])
        if not streams:
            return
        vs = streams[0]
        parts = []
        w, h = vs.get("width"), vs.get("height")
        if w and h:
            parts.append(f"{w}x{h}")
        rfr = vs.get("r_frame_rate", "")
        if rfr and "/" in rfr:
            try:
                num, den = rfr.split("/")
                fps = round(int(num) / int(den))
                if fps > 0:
                    parts.append(f"{fps}fps")
            except (ValueError, ZeroDivisionError):
                pass
        br = vs.get("bit_rate") or data.get("format", {}).get("bit_rate")
        if br:
            try:
                parts.append(f"{int(br) / 1_000_000:.1f}Mbps")
            except ValueError:
                pass
        if parts:
            stream_info_ref[0] = " · ".join(parts)
    except Exception as e:
        logger.debug(f"ffprobe stream info probe failed: {e}")


def _watch_resolution_change_thread(raw_file, resolution_change_ref, ffmpeg_path, logger,
                                    stop_event, min_size_bytes=1_500_000,
                                    check_interval=20):
    """Background thread: detect a mid-recording resolution change and flag a split.

    Recording is done via stream copy (no re-encode), writing the live HLS
    feed straight to a raw .ts file. If the source resolution changes partway
    through — the case reported for TikTok multi-guest "battles", where the
    layout switches from a single portrait feed to a multi-guest grid with a
    different frame size — the raw file ends up with two different frame
    sizes muxed together. ffmpeg can't renegotiate resolution mid-copy, so
    the segment plays back glitched from that point on.

    Rather than trying to transcode on the fly, this establishes a baseline
    resolution once the file has enough data, then periodically re-probes
    the *tail* of the growing file (ffprobe -sseof) to see the current live
    frame size. If it differs from the baseline, resolution_change_ref[0] is
    set to True so the caller can gracefully end this segment and start a
    fresh one — the same mechanism already used for max_file_size_gb splits.
    """
    import json as _json

    ffprobe = ffprobe_from_ffmpeg(ffmpeg_path)

    def _probe(extra_args=None):
        try:
            cmd = [ffprobe, "-v", "quiet", "-print_format", "json",
                   "-show_streams", "-select_streams", "v:0"]
            if extra_args:
                cmd.extend(extra_args)
            cmd.append(raw_file)
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            data = _json.loads(result.stdout)
            streams = data.get("streams", [])
            if not streams:
                return None
            w, h = streams[0].get("width"), streams[0].get("height")
            return (w, h) if (w and h) else None
        except Exception as e:
            logger.debug(f"Resolution-change probe failed: {e}")
            return None

    # Wait for the file to grow enough for a reliable baseline probe.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if stop_event.is_set():
            return
        try:
            if os.path.exists(raw_file) and os.path.getsize(raw_file) >= min_size_bytes:
                break
        except OSError:
            pass
        time.sleep(1)
    else:
        return  # file never appeared or grew — give up

    baseline = _probe()
    if not baseline:
        return  # couldn't establish a baseline — nothing to compare against

    while not stop_event.is_set():
        # Interruptible sleep so we don't delay shutdown by up to check_interval.
        for _ in range(check_interval):
            if stop_event.is_set():
                return
            time.sleep(1)

        # Sample near the live edge (-sseof) rather than the start of the file,
        # so we catch the *current* resolution, not the one from minutes ago.
        current = _probe(["-sseof", "-5"])
        if not current:
            continue
        if current != baseline:
            logger.info(
                f"Resolution changed mid-stream: {baseline[0]}x{baseline[1]} -> "
                f"{current[0]}x{current[1]} — ending segment to avoid a corrupted file"
            )
            resolution_change_ref[0] = True
            return


def monitor_recording_process(proc, raw_file, start_time, max_record_hours,
                              platform, logger, status_queue, channel_key,
                              stop_event, last_status, file_creation_timeout=60,
                              tool_name_override=None, verbose=False,
                              max_file_size_gb=0.0, stream_info="",
                              ffmpeg_path="ffmpeg", watch_resolution_changes=False,
                              resolution_check_interval=20):
    """Monitor a recording subprocess and update status via queue.

    Spawns a background thread to read stderr for real-time logging.
    Detects zero-byte stalls, file growth stalls, max duration limits,
    file creation timeouts, optional max-file-size splits, and (when
    watch_resolution_changes=True) a mid-stream resolution change — the
    segment is ended and split_requested is set so recording resumes at
    the new resolution in a fresh file instead of corrupting one file.

    When stream_info is empty the monitor will attempt to auto-detect it by
    parsing ffmpeg's Video stream descriptor from stderr.  The detected string
    (e.g. "1920x1080 · 30fps · 4.9Mbps") replaces "starting" in the status
    detail once the stream descriptor appears in the log.

    Returns (last_status, split_requested) where split_requested=True means
    the caller should immediately start a new segment rather than entering
    the reconnect grace period.
    """
    tool_name = tool_name_override or ("yt-dlp" if platform in ["kick", "youtube", "rumble", "custom"] else "streamlink")
    zero_byte_strikes = 0
    file_appeared = False
    split_requested = False

    # Stall detection: kill the process if the file stops growing for this long.
    # streamlink's --retry-streams keeps it alive even after the stream ends, so
    # without this the worker would never notice the stream dropped.
    STALL_TIMEOUT = 90  # seconds without file growth before terminating
    last_size = 0
    last_growth_time = time.monotonic()

    # Max-file-size limit (0 = disabled)
    max_file_size_bytes = int(max_file_size_gb * 1024 ** 3) if max_file_size_gb > 0 else 0

    # stream_info_ref: one-element mutable list shared between the probe thread
    # and the monitor loop.  Pre-populate with whatever was passed in (Fishtank/
    # Chaturbate set it from format data); leave None for probe-thread platforms.
    stream_info_ref = [stream_info if stream_info else None]

    # Start stderr reader thread.
    stderr_thread = threading.Thread(
        target=_stderr_reader_thread, args=(proc, logger, tool_name, verbose),
        daemon=True, name=f"stderr-{channel_key}",
    )
    stderr_thread.start()

    # Stream-info probe thread: runs ffprobe on the output file once it has
    # enough data (~1.5 MB).  Populates stream_info_ref[0] with a
    # "WxH · Xfps · Y.YMbps" string for all platforms.  Skipped when
    # stream_info was pre-populated (Chaturbate/Fishtank format-data path).
    if stream_info_ref[0] is None:
        probe_thread = threading.Thread(
            target=_probe_stream_info_thread,
            args=(raw_file, stream_info_ref, ffmpeg_path, logger),
            daemon=True, name=f"probe-{channel_key}",
        )
        probe_thread.start()

    # Resolution-change watch thread: scoped to this call only (its own stop
    # event, set when this monitor loop exits for any reason) so it doesn't
    # keep polling a finished segment's file after a split/reconnect starts
    # the next one.
    resolution_change_ref = [False]
    res_watch_stop_event = threading.Event()
    if watch_resolution_changes:
        res_watch_thread = threading.Thread(
            target=_watch_resolution_change_thread,
            args=(raw_file, resolution_change_ref, ffmpeg_path, logger, res_watch_stop_event),
            kwargs={"check_interval": resolution_check_interval},
            daemon=True, name=f"reswatch-{channel_key}",
        )
        res_watch_thread.start()

    while proc.poll() is None and not stop_event.is_set():
        elapsed = time.monotonic() - start_time

        try:
            if os.path.exists(raw_file):
                file_appeared = True
                size = os.path.getsize(raw_file)

                # Track file growth for stall detection
                if size > last_size:
                    last_size = size
                    last_growth_time = time.monotonic()

                stall_duration = time.monotonic() - last_growth_time

                # After 30 seconds with file existing but 0 bytes, something is wrong
                if elapsed > 30 and size == 0:
                    zero_byte_strikes += 1
                    if zero_byte_strikes > 6:  # 30 seconds of checking
                        logger.error("File exists but remains 0 bytes after 60+ seconds — terminating")
                        kill_process_tree(proc.pid, logger)
                        break
                # File grew at some point but has now stalled — stream likely ended
                elif last_size > 0 and elapsed > 60 and stall_duration > STALL_TIMEOUT:
                    logger.warning(
                        f"File hasn't grown in {stall_duration:.0f}s (last size: {human_size(last_size)}) "
                        f"— stream likely ended, terminating {tool_name}"
                    )
                    kill_process_tree(proc.pid, logger)
                    break
                else:
                    zero_byte_strikes = 0

                # Resolution-change split: the live feed's frame size changed
                # (e.g. a TikTok multi-guest battle starting/ending) — end this
                # segment now so the resolution change doesn't corrupt one file.
                if resolution_change_ref[0]:
                    split_requested = True
                    kill_process_tree(proc.pid, logger)
                    break

                # Max-file-size split: gracefully stop so the caller can remux
                # this segment and immediately start a fresh one.
                if max_file_size_bytes > 0 and size >= max_file_size_bytes:
                    logger.info(
                        f"File reached {human_size(size)} — splitting "
                        f"(limit: {max_file_size_gb:.1f} GB)"
                    )
                    split_requested = True
                    kill_process_tree(proc.pid, logger)
                    break

                progress_pct = min(100, (elapsed / (max_record_hours * 3600)) * 100) if max_record_hours > 0 else 50

                # Build detail string: use pre-populated stream_info, auto-detected
                # info from the stderr parser, or empty while waiting for detection.
                detail = stream_info_ref[0] if stream_info_ref[0] else "starting"

                new_status = {
                    "status": "Recording",
                    "detail": detail,
                    "size": human_size(size),
                    "size_bytes": int(size),
                    "time": format_elapsed(elapsed),
                    "progress": progress_pct,
                }
                if new_status != last_status:
                    status_queue.put((channel_key, new_status))
                    last_status = new_status.copy()

            elif not file_appeared and elapsed > file_creation_timeout:
                # File was never created — yt-dlp is probably stuck
                logger.error(
                    f"Output file was never created after {file_creation_timeout}s — "
                    f"killing {tool_name} (PID {proc.pid})"
                )
                kill_process_tree(proc.pid, logger)
                break

        except Exception as e:
            logger.warning(f"Error checking file size: {e}")

        time.sleep(5)

    # Stop the resolution-watch thread (scoped to this call) so it doesn't
    # keep polling this now-finished segment's file in the background.
    res_watch_stop_event.set()

    # Wait for stderr thread to finish reading
    stderr_thread.join(timeout=5)

    return last_status, split_requested


def _probe_duration(ffmpeg_path, raw_file):
    """Return the duration of *raw_file* in seconds using ffprobe (or ffmpeg -i).

    Tries several strategies, because a live-growing MPEG-TS is a much harder
    case than a finished file:

      1. format=duration — works for finished files and most growing ones.
      2. the video stream's own duration — MPEG-TS has no container-level
         duration field, so ffprobe has to derive one by seeking to the end
         and reading the last timestamp.  That derivation reports N/A on some
         live captures (notably when PTS values are near the 33-bit MPEG-TS
         wraparound, or when the tail packet is mid-write), while the
         per-stream value still resolves.
      3. parsing "Duration:" out of `ffmpeg -i` stderr.

    Strategies 1 and 2 get an enlarged probesize/analyzeduration, since the
    defaults can bail out before finding timestamps on a portrait/low-bitrate
    stream.

    Returns elapsed seconds, never a raw PCR last-PTS. See
    ``_elapsed_media_duration``. Returns None if every strategy fails.
    """
    ffprobe_path = ffprobe_from_ffmpeg(ffmpeg_path)

    _big = ["-probesize", "50M", "-analyzeduration", "20M"]
    start_time = 0.0
    try:
        fmt = subprocess.run(
            [ffprobe_path, "-v", "error", *_big,
             "-show_entries", "format=duration,start_time",
             "-of", "default=noprint_wrappers=1", raw_file],
            capture_output=True, text=True, timeout=20,
        )
        parsed = {}
        for line in (fmt.stdout or "").splitlines():
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            val = val.strip()
            if not val or val.upper() == "N/A":
                continue
            try:
                parsed[key.strip()] = float(val)
            except ValueError:
                continue
        start_time = parsed.get("start_time") or 0.0
        raw = parsed.get("duration")
        elapsed = _elapsed_media_duration(raw, start_time)
        if elapsed:
            return elapsed
    except Exception:
        pass

    try:
        result = subprocess.run(
            [ffprobe_path, "-v", "error", *_big,
             "-select_streams", "v:0",
             "-show_entries", "stream=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", raw_file],
            capture_output=True, text=True, timeout=20,
        )
        for val in (result.stdout or "").split():
            val = val.strip()
            if val and val != "N/A":
                try:
                    d = float(val)
                except ValueError:
                    continue
                elapsed = _elapsed_media_duration(d, start_time)
                if elapsed:
                    return elapsed
    except Exception:
        pass

    # Fallback: parse "Duration: HH:MM:SS.ss" from ffmpeg -i stderr
    try:
        result = subprocess.run(
            [ffmpeg_path, "-i", raw_file, "-hide_banner"],
            capture_output=True, text=True, timeout=20,
        )
        import re as _re
        m = _re.search(r"Duration:\s*(\d+):(\d{2}):(\d{2})\.(\d+)", result.stderr)
        if m:
            h, mn, s, cs = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
            total = h * 3600 + mn * 60 + s + cs / 100
            elapsed = _elapsed_media_duration(total, start_time)
            if elapsed:
                return elapsed
    except Exception:
        pass
    return None


def _run_remux_cmd(ffmpeg_cmd, ffmpeg_path, raw_file, mp4_file, logger, timeout,
                   file_size, label="Remux"):
    """Run a single ffmpeg remux command, streaming progress to the logger.

    ffmpeg is invoked with ``-progress pipe:1 -stats_period 5`` so it writes
    structured key=value progress lines to stdout every 5 seconds.  Errors
    still go to stderr.  A reader thread collects both streams; the main thread
    waits for the process to finish (or timeout).

    Progress log lines are emitted:
      • Every 10 percentage-points of completion (10 %, 20 %, … 90 %)
      • OR at least every 60 seconds if the file is very large and slow

    Returns ``(returncode, stderr_text)``.
    """
    import re as _re

    # Probe source duration so we can compute % complete from out_time_ms.
    total_secs = _probe_duration(ffmpeg_path, raw_file)

    # Inject progress reporting into the command.  Insert before the output
    # path (last positional argument, just before "-y").
    progress_flags = ["-progress", "pipe:1", "-stats_period", "5"]
    # Replace "-loglevel error" with just error-level logging (keep stderr clean)
    cmd = list(ffmpeg_cmd)
    # Insert progress flags right before the output file ("-y" is last; output
    # file is second-to-last)
    insert_pos = len(cmd) - 2  # before <output> -y
    cmd = cmd[:insert_pos] + progress_flags + cmd[insert_pos:]

    stdout_lines = []
    stderr_lines = []

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    def _read_stream(stream, buf):
        for line in stream:
            buf.append(line.rstrip())
        stream.close()

    t_err = threading.Thread(target=_read_stream, args=(proc.stderr, stderr_lines), daemon=True)
    t_err.start()

    # ── Progress state ────────────────────────────────────────────────────
    remux_start   = time.monotonic()
    last_log_time = remux_start
    last_log_pct  = -10        # force a log at 0 % on first update
    current_block = {}         # accumulates key=value pairs for one progress block
    LOG_PCT_STEP  = 10         # log every N percentage points
    LOG_TIME_GAP  = 60         # log at least every N seconds (large/slow files)

    def _maybe_log_progress():
        nonlocal last_log_time, last_log_pct

        out_time_us = current_block.get("out_time_us") or current_block.get("out_time_ms")
        if out_time_us is None:
            return
        try:
            elapsed_media_us = int(out_time_us)
        except ValueError:
            return

        now = time.monotonic()
        wall_elapsed = now - remux_start

        # Percentage complete (requires known duration)
        pct = None
        if total_secs and total_secs > 0:
            pct = min(100.0, (elapsed_media_us / 1_000_000) / total_secs * 100)

        # Decide whether to emit a log line
        pct_trigger  = (pct is not None) and (pct - last_log_pct >= LOG_PCT_STEP)
        time_trigger = (now - last_log_time) >= LOG_TIME_GAP

        if not (pct_trigger or time_trigger):
            return

        # Build the progress message
        speed_str = current_block.get("speed", "").strip()

        if pct is not None:
            bar = text_progress_bar(pct, width=10)
            # ETA from wall time + pct
            if pct > 1:
                total_est = wall_elapsed / (pct / 100)
                eta_secs  = max(0, total_est - wall_elapsed)
                eta_str   = f"ETA {format_elapsed(eta_secs)}"
            else:
                eta_str = "ETA calculating…"
            msg = f"{label}: {bar}  speed={speed_str}  {eta_str}"
        else:
            # No duration info — show elapsed wall time and speed only
            media_secs = elapsed_media_us / 1_000_000
            msg = (f"{label}: processed {format_elapsed(media_secs)} of stream  "
                   f"speed={speed_str}  wall={format_elapsed(wall_elapsed)}")

        logger.info(msg)
        last_log_time = now
        if pct is not None:
            last_log_pct = pct

    # ── Read stdout (ffmpeg progress blocks) ─────────────────────────────
    for line in proc.stdout:
        line = line.rstrip()
        stdout_lines.append(line)
        if "=" in line:
            key, _, val = line.partition("=")
            current_block[key.strip()] = val.strip()
            if key.strip() == "progress":
                # "progress=continue" or "progress=end" marks end of a block
                _maybe_log_progress()
                if val.strip() == "end":
                    current_block = {}
                else:
                    current_block = {}
    proc.stdout.close()

    # Wait for process with timeout
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        t_err.join(timeout=5)
        return -1, "\n".join(stderr_lines)

    t_err.join(timeout=5)
    return proc.returncode, "\n".join(stderr_lines)


def remux_to_mp4(raw_file, mp4_file, ffmpeg_path, logger, timeout=600):
    """Remux recorded .ts file to MP4 format."""
    # Probe for timed_id3 codec (common in Twitch streams)
    has_timed_id3 = False
    try:
        probe = subprocess.run(
            [ffmpeg_path, "-i", raw_file, "-hide_banner"],
            capture_output=True, text=True, timeout=10
        )
        if "timed_id3" in probe.stderr.lower():
            has_timed_id3 = True
            logger.info("Detected timed_id3 codec — will exclude from output")
    except Exception:
        pass

    if has_timed_id3:
        ffmpeg_cmd = [
            ffmpeg_path, "-i", raw_file,
            "-map", "0:v?", "-map", "0:a?",
            "-c", "copy", "-movflags", "+faststart",
            "-loglevel", "error", mp4_file, "-y",
        ]
    else:
        ffmpeg_cmd = [
            ffmpeg_path, "-i", raw_file,
            "-c", "copy", "-map", "0",
            "-movflags", "+faststart",
            "-loglevel", "error", mp4_file, "-y",
        ]

    file_size = os.path.getsize(raw_file) if os.path.exists(raw_file) else 0
    logger.info("Starting remux...")

    try:
        returncode, stderr_text = _run_remux_cmd(
            ffmpeg_cmd, ffmpeg_path, raw_file, mp4_file,
            logger, timeout, file_size, label="Remux"
        )

        if returncode == -1:
            logger.error("Remux timed out")
            return False, 0, "timeout"

        if returncode == 0 and os.path.exists(mp4_file):
            mp4_size = os.path.getsize(mp4_file)
            if mp4_size > 5 * 1024**2:
                logger.info(f"Remux successful: {human_size(mp4_size)}")
                return True, mp4_size, None
            else:
                logger.error(f"Remux produced small file: {human_size(mp4_size)}")
                return False, mp4_size, "output too small"
        else:
            import ctypes
            signed_code = ctypes.c_int32(returncode).value
            logger.error(f"Remux failed — returncode: {returncode} (signed: {signed_code})")
            if stderr_text:
                logger.error(f"FFmpeg stderr: {stderr_text}")

            # Fallback: strip metadata streams
            if signed_code == -22 or has_timed_id3 is False:
                logger.warning("Attempting fallback remux without metadata streams")
                fallback_cmd = [
                    ffmpeg_path, "-i", raw_file,
                    "-map", "0:v?", "-map", "0:a?",
                    "-c", "copy", "-movflags", "+faststart",
                    "-loglevel", "error", mp4_file, "-y",
                ]
                try:
                    fb_code, fb_stderr = _run_remux_cmd(
                        fallback_cmd, ffmpeg_path, raw_file, mp4_file,
                        logger, timeout, file_size, label="Fallback remux"
                    )
                    if fb_code == 0 and os.path.exists(mp4_file):
                        mp4_size = os.path.getsize(mp4_file)
                        if mp4_size > 5 * 1024**2:
                            logger.info(f"Fallback remux successful: {human_size(mp4_size)}")
                            return True, mp4_size, None
                    logger.error(f"Fallback remux also failed: {fb_code}")
                    if fb_stderr:
                        logger.error(f"Fallback stderr: {fb_stderr}")
                except Exception as e:
                    logger.error(f"Fallback remux error: {e}")
            return False, 0, f"code {signed_code}"

    except FileNotFoundError:
        logger.error("ffmpeg not found in PATH")
        return False, 0, "ffmpeg not found"
    except Exception as e:
        logger.error(f"Remux error: {e}")
        return False, 0, str(e)



def save_metadata(mp4_file, username, platform, start_time_str, duration_seconds, title=None):
    """Save a JSON metadata sidecar alongside the MP4 file."""
    meta_file = mp4_file.rsplit('.', 1)[0] + '.meta.json'
    metadata = {
        'version': __version__,
        'channel': username,
        'platform': platform,
        'recording_started': start_time_str,
        'duration_seconds': round(duration_seconds, 1),
        'duration_human': format_elapsed(duration_seconds),
        'stream_title': title,
        'file': os.path.basename(mp4_file),
        'recorded_at': datetime.datetime.now().isoformat(),
    }
    try:
        with open(meta_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.warning(f"Failed to save metadata: {e}")


def finalize_recording_group(parts, group_base_name, processed_path, pending_dir,
                              ffmpeg_path, ffmpeg_timeout, username, platform, logger):
    """Stitch a reconnect-continuity group into one continuous final MP4.

    Some sources (e.g. Chaturbate's LL-HLS/CMAF delivery) periodically rotate
    the manifest session, which makes the recording process exit cleanly even
    though the stream never actually went down. The worker loop already
    reconnects and resumes recording within `reconnect_grace_minutes`, but
    without this step each reconnect would produce its own small, separate
    final file ("clips") instead of one continuous recording.

    `parts` is a list of dicts, in recording order, one per segment that was
    interrupted by an unplanned drop and picked back up within the grace
    period. Each dict has: mp4, meta, start_wall, elapsed, title.

    If there's only one part, no reconnect ever happened for this session —
    it's already sitting at the correct final filename, so there's nothing
    to do. With two or more parts, they're losslessly concatenated (stream
    copy, no re-encode) into a single file so the end user sees one
    recording instead of several.
    """
    if not parts or len(parts) == 1:
        return

    logger.info(f"Stitching {len(parts)} reconnect segments into one continuous recording...")

    final_file = os.path.join(processed_path, f"{group_base_name}.mp4")
    merge_tmp = os.path.join(processed_path, f"{group_base_name}.merging.mp4")
    list_file = os.path.join(processed_path, f"{group_base_name}.concat_list.txt")

    try:
        with open(list_file, 'w', encoding='utf-8') as f:
            for part in parts:
                # ffmpeg's concat demuxer needs embedded single quotes escaped
                safe_path = part['mp4'].replace("'", r"'\''")
                f.write(f"file '{safe_path}'\n")

        concat_cmd = [
            ffmpeg_path, "-y", "-f", "concat", "-safe", "0",
            "-i", list_file, "-c", "copy", merge_tmp,
        ]
        result = subprocess.run(
            concat_cmd, capture_output=True, text=True, timeout=ffmpeg_timeout
        )

        if result.returncode != 0 or not os.path.exists(merge_tmp) or os.path.getsize(merge_tmp) == 0:
            logger.error(
                f"Segment stitching failed (ffmpeg exit {result.returncode}) — "
                f"leaving {len(parts)} segments as separate files. "
                f"stderr: {(result.stderr or '')[-500:]}"
            )
            if os.path.exists(merge_tmp):
                try:
                    os.remove(merge_tmp)
                except Exception:
                    pass
            return

        # Move the now-superseded individual parts (mp4 + metadata sidecar)
        # out of the way into PendingDeletion — same treatment as raw .ts
        # files — instead of hard-deleting them, in case the stitch needs
        # to be redone or inspected.
        for part in parts:
            for src in (part['mp4'], part.get('meta')):
                if src and os.path.exists(src):
                    try:
                        dest = os.path.join(pending_dir, os.path.basename(src))
                        shutil.move(src, dest)
                    except Exception as e:
                        logger.warning(
                            f"Could not move stitched part {os.path.basename(src)} "
                            f"to PendingDeletion: {e}"
                        )

        shutil.move(merge_tmp, final_file)

        total_elapsed = sum(p['elapsed'] for p in parts)
        merged_size = os.path.getsize(final_file)
        logger.info(
            f"Stitched recording complete: {os.path.basename(final_file)} "
            f"({human_size(merged_size)}, {format_elapsed(total_elapsed)} total "
            f"across {len(parts)} segments)"
        )

        save_metadata(
            final_file, username, platform,
            parts[0]['start_wall'].isoformat(),
            total_elapsed,
            parts[0].get('title'),
        )

    except subprocess.TimeoutExpired:
        logger.error(f"Segment stitching timed out — leaving {len(parts)} segments as separate files")
        if os.path.exists(merge_tmp):
            try:
                os.remove(merge_tmp)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Segment stitching failed: {e} — leaving {len(parts)} segments as separate files")
    finally:
        if os.path.exists(list_file):
            try:
                os.remove(list_file)
            except Exception:
                pass


# ────────────────────────────────────────────────
#          Recording Worker
# ────────────────────────────────────────────────

def record_worker(args):
    """Worker process to record a single channel."""
    (channel_entry, config_dict, stop_event, status_queue, wake_event, runtime) = args

    # Reconstruct config in child process
    config = configparser.ConfigParser()
    config.read_dict(config_dict)

    channel_key = channel_entry

    # Get configuration
    root_path = config.get('Paths', 'streams_dir', fallback='') or default_streams_dir()
    quality = config.get('Recording', 'quality', fallback='best')
    max_record_hours = parser_getfloat(config, 'Recording', 'max_record_hours', 12.0)
    min_file_size_mb = parser_getfloat(config, 'Recording', 'min_file_size_mb', 2.0)
    min_disk_space_gb = parser_getfloat(config, 'Recording', 'min_disk_space_gb', 5.0)
    max_file_size_gb  = parser_getfloat(config, 'Recording', 'max_file_size_gb', 8.0)
    verbose = parser_getboolean(config, 'Advanced', 'verbose', False)
    streamlink_debug = parser_getboolean(config, 'Advanced', 'streamlink_debug', False)
    ffmpeg_path = config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg') or 'ffmpeg'
    stream_check_timeout = parser_getint(config, 'Timeouts', 'stream_check_timeout', 30)
    ffmpeg_timeout = parser_getint(config, 'Timeouts', 'ffmpeg_timeout', 600)
    poll_interval_minutes = parser_getfloat(config, 'Timeouts', 'poll_interval_minutes', 3.0)
    poll_jitter_percent = parser_getint(config, 'Timeouts', 'poll_jitter_percent', 20)
    error_backoff_max_minutes = parser_getfloat(config, 'Timeouts', 'error_backoff_max_minutes', 15.0)
    reconnect_grace_minutes = parser_getint(config, 'Timeouts', 'reconnect_grace_minutes', 3)
    file_creation_timeout = parser_getint(config, 'Timeouts', 'file_creation_timeout', 60)
    filename_pattern = config.get('Recording', 'filename_pattern', fallback='{username}_{timestamp}') or '{username}_{timestamp}'

    # Cookies file
    cookies_file = config.get('Paths', 'cookies_file', fallback='') or None
    # In child process the auto-detect ran in the parent; re-check here
    if not cookies_file:
        cookies_file = find_cookies_file(config)

    # Setup logging
    logger = setup_child_logging(root_path, channel_key)
    parsed = parse_channel_key(channel_entry)
    if parsed is None:
        logger.error(
            "Refusing to record — channel key is not a known platform "
            "or contains unsafe path characters"
        )
        return
    platform, username = parsed
    logger.info(f"Worker STARTED (PID {os.getpid()})")
    if cookies_file:
        logger.info(f"Using cookies file: {cookies_file}")

    # Build URL. username_dir is set here only for custom URLs (may be nested
    # site/user); everyone else sanitizes the username after this block.
    username_dir = None
    if platform == "kick":
        url = f"https://kick.com/{username}"
    elif platform == "twitch":
        url = f"https://twitch.tv/{username}"
    elif platform == "youtube":
        if username.startswith("UC"):
            url = f"https://youtube.com/channel/{username}/live"
        elif username.startswith("@"):
            url = f"https://youtube.com/{username}/live"
        elif "watch?v=" in username or len(username) == 11:
            if "watch?v=" in username:
                url = username if username.startswith("http") else f"https://youtube.com/{username}"
            else:
                url = f"https://youtube.com/watch?v={username}"
        else:
            url = f"https://youtube.com/@{username}/live"
    elif platform == "rumble":
        url = f"https://rumble.com/c/{username}"
    elif platform == "tiktok":
        handle = username.lstrip('@')
        url = f"https://www.tiktok.com/@{handle}/live"
        username = handle  # use actual handle for folders/filenames
    elif platform == "fishtank":
        # username is the camera name / stream ID (e.g. "director", "dirc-5")
        # The real URL is built per-poll using the JWT; store a placeholder
        url = f"fishtank:{username}"
    elif platform == "custom":
        # Custom: the username field IS the full URL
        url = username
        # TikTok live streams are served from /@user/live, not the bare profile page.
        # Silently rewrite so yt-dlp hits the right endpoint even if the user pasted
        # the profile URL rather than the /live URL.
        if 'tiktok.com' in url.lower():
            base = url.split('?')[0].rstrip('/')
            if not base.endswith('/live'):
                url = base + '/live'
                logger.info(f"TikTok: profile URL rewritten to live endpoint: {redact_for_log(url)}")
        username, username_dir = custom_url_folder_names(url)
    else:
        url = f"https://{platform}.com/{username}"

    # Directory paths (but DON'T create them yet — wait until we actually record)
    recorded_base = os.path.join(root_path, "Recorded")
    processed_base = os.path.join(root_path, "Processed")
    pending_base = os.path.join(root_path, PENDING_DELETION_FOLDER)

    # Sanitize the username for use as a folder name: preserves leading underscores
    # and mid-string periods (e.g. _avamartinez, boo.tleg) but strips characters
    # that Windows forbids in folder names and trailing dots/spaces.
    # Custom URLs already have a sanitized (possibly nested) relative dir.
    if username_dir is None:
        username_dir = sanitize_path_component(username)
    recorded_path = os.path.join(recorded_base, platform, username_dir)
    processed_path = os.path.join(processed_base, platform, username_dir)
    pending_dir = os.path.join(pending_base, platform, username_dir)

    last_status = None
    stream_title = None  # populated on check

    # ── Polling state ──
    # Normal offline: flat interval with jitter — no backoff.
    # Error: exponential backoff from poll_interval up to error_backoff_max.
    # Reconnect: fast 15s polling for reconnect_grace_minutes after a stream drops.
    poll_base_seconds = poll_interval_minutes * 60
    error_sleep_seconds = poll_base_seconds          # current error backoff (grows on consecutive errors)
    error_backoff_max_seconds = error_backoff_max_minutes * 60
    consecutive_errors = 0

    # Fast reconnect state
    reconnect_mode = False
    reconnect_deadline = 0  # monotonic timestamp when grace period expires
    RECONNECT_POLL_INTERVAL = 15  # seconds between checks during grace period

    # Consecutive small-file / bad-stream detection.
    # If the stream keeps producing tiny recordings that fail the remux size check
    # (e.g. a stub HLS playlist serving ~20 s of garbage), we back off rather than
    # hammering the server in a tight loop.  Counter resets on any successful remux.
    consecutive_small_remux_fails = 0
    SMALL_REMUX_FAIL_LIMIT = 3        # enter backoff after this many in a row
    SMALL_REMUX_BACKOFF_BASE = 60     # first backoff: 60 s
    SMALL_REMUX_BACKOFF_MAX = 900     # cap at 15 min
    _small_remux_backoff = SMALL_REMUX_BACKOFF_BASE

    # Worker crash backoff: after an unexpected exception the worker restarts
    # after a short delay.  Without backoff this creates a tight crash loop
    # (e.g. the resolve_best_fishtank_variant unpack bug caused 55 crashes in
    # ~100 minutes).  We grow the delay exponentially up to a cap so repeated
    # crashes back off gracefully.  Resets to base on any successful recording.
    _crash_backoff = 60               # current sleep after a crash (seconds)
    CRASH_BACKOFF_BASE = 60           # initial delay
    CRASH_BACKOFF_MAX = 900           # cap at 15 min

    # Micro-fragment throttle: when the server is repeatedly dropping the
    # connection after only a few seconds (e.g. the cameraman 20:12–20:30 storm
    # that produced ~30 × 20-second files), we apply a growing backoff rather
    # than immediately reconnecting.  This avoids hammering the CDN and filling
    # PendingDeletion with dozens of tiny but technically-valid files.
    # Triggers after MICRO_FRAG_LIMIT consecutive recordings shorter than
    # MICRO_FRAG_MIN_SECONDS, regardless of file size (so it catches fragments
    # that pass the min_file_size_mb check).  Resets on any recording that runs
    # longer than MICRO_FRAG_MIN_SECONDS.
    MICRO_FRAG_MIN_SECONDS = 30       # recordings shorter than this count as micro-fragments
    MICRO_FRAG_LIMIT = 5              # consecutive micro-fragments before throttling
    MICRO_FRAG_BACKOFF_BASE = 30      # first throttle sleep (seconds)
    MICRO_FRAG_BACKOFF_MAX = 300      # cap at 5 minutes
    _consecutive_micro_frags = 0
    _micro_frag_backoff = MICRO_FRAG_BACKOFF_BASE

    # ── Reconnect-continuity grouping ──
    # When a stream drops and reconnects within reconnect_grace_minutes, each
    # segment is still recorded/remuxed independently (unchanged below), but
    # we track the completed segments here so they can be stitched into one
    # continuous final file instead of being left as separate small clips.
    # A deliberate max_file_size_gb split is NOT grouped — that's an
    # intentional chunk boundary, not an unplanned drop.
    group_parts = []          # completed segments belonging to the current session
    group_base_name = None    # shared base filename for the current session

    def _finalize_pending_group():
        nonlocal group_parts, group_base_name
        if group_parts:
            finalize_recording_group(
                group_parts, group_base_name, processed_path, pending_dir,
                ffmpeg_path, ffmpeg_timeout, username, platform, logger,
            )
        group_parts = []
        group_base_name = None

    # Stream metadata shown in the status table (set when variant is resolved)
    stream_info = ""

    # Fishtank: create a per-worker auth manager that caches the JWT
    fishtank_auth = None
    if platform == "fishtank":
        ft_email = config.get('Fishtank', 'email', fallback='')
        ft_password = config.get('Fishtank', 'password', fallback='')
        fishtank_auth = FishtankAuth(cookies_file, logger,
                                     email=ft_email, password=ft_password)
        auth_method = "email+password" if (ft_email and ft_password) else "cookie jar"
        logger.info(
            f"[fishtank] Auth manager initialised for stream '{username}' "
            f"(auth method: {auth_method})")
        fishtank_auth.start_background_refresh()

    # Initial stagger: randomize the very first check so workers don't all fire at once
    initial_delay = random.uniform(0, min(poll_base_seconds, 10))
    if initial_delay > 1:
        logger.info(f"Initial stagger: waiting {initial_delay:.0f}s before first check")
        time.sleep(initial_delay)

    while not stop_event.is_set():
        try:
            # Live-refresh the poll interval from the shared runtime dict so
            # GUI changes (including custom rates) apply without a restart.
            try:
                _live_poll = float(runtime.get('poll_interval_minutes', poll_interval_minutes))
                if abs(_live_poll - poll_interval_minutes) > 0.001:
                    logger.info(f"Poll interval updated: {poll_interval_minutes}min → {_live_poll}min")
                    poll_interval_minutes = _live_poll
                    poll_base_seconds = poll_interval_minutes * 60
                    # Keep the error backoff floor in sync with the new base
                    error_sleep_seconds = min(error_sleep_seconds, error_backoff_max_seconds)
            except Exception:
                pass  # Manager may be unavailable during shutdown — keep last known value

            # Check disk space
            has_space, free_gb = check_disk_space(root_path, min_disk_space_gb)
            if not has_space:
                if free_gb is None:
                    detail = "disk unreadable"
                    log_msg = "Cannot measure or write to streams directory — pausing"
                else:
                    detail = f"Low disk space: {free_gb:.1f}GB"
                    log_msg = (
                        f"Insufficient disk space: {free_gb:.1f}GB available, "
                        f"{min_disk_space_gb}GB required"
                    )
                new_status = {
                    "status": "Error",
                    "detail": detail,
                    "size": "", "time": "", "progress": 0,
                }
                if new_status != last_status:
                    status_queue.put((channel_key, new_status))
                    last_status = new_status.copy()
                logger.error(log_msg)
                if interruptible_sleep(300, wake_event, stop_event):
                    logger.info("Manual check requested — re-checking disk space now")
                continue

            new_status = {"status": "Checking...", "detail": "", "size": "", "time": "", "progress": 0}
            if new_status != last_status:
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()

            # Check if stream is live
            need_impersonate = False
            format_urls = None
            rumble_hls_url = None
            recording_url = url  # may be overridden by resolved URL
            if platform == "kick":
                # Try streamlink for Kick first — it has a JS challenge solver
                # for Cloudflare that yt-dlp lacks.  Use longer timeout since
                # the first check may need to launch a headless browser.
                kick_timeout = max(stream_check_timeout, 45)
                is_live, stream_title, error = check_stream_kick_api(
                    username, logger, kick_timeout, cookies_file)
                if is_live is None:
                    # streamlink check failed — fall back to yt-dlp
                    logger.info("Kick streamlink check inconclusive — falling back to yt-dlp")
                    is_live, stream_title, error, need_impersonate, resolved_url, format_urls = check_stream_ytdlp(url, logger, stream_check_timeout, cookies_file)
                    if resolved_url:
                        recording_url = resolved_url
                        logger.info(f"Using resolved URL for recording: {redact_for_log(recording_url)}")
            elif platform == "rumble":
                is_live, stream_title, rumble_live_url, rumble_hls_url, error = check_stream_rumble_html(
                    username, logger, stream_check_timeout, cookies_file)
                if is_live and rumble_live_url:
                    recording_url = rumble_live_url
                    logger.info(f"Rumble live video URL: {redact_for_log(recording_url)}")
                    if rumble_hls_url:
                        # Channel page already gave us the playlist. Recording
                        # that with ffmpeg skips yt-dlp's Cloudflare-gated
                        # video-page fetch (the 403 on rumble.com/vXXXX).
                        recording_url = rumble_hls_url
                        logger.info(
                            "Rumble: recording HLS from channel page "
                            "(not fetching the video page with yt-dlp)"
                        )
                    else:
                        is_live, stream_title, error, need_impersonate, resolved_url, format_urls = check_stream_ytdlp(
                            rumble_live_url, logger, stream_check_timeout, cookies_file
                        )
                        if resolved_url:
                            recording_url = resolved_url
            elif platform == "tiktok":
                is_live, stream_title, error, need_impersonate, resolved_url, format_urls = check_stream_ytdlp(url, logger, stream_check_timeout, cookies_file)
                if resolved_url:
                    recording_url = resolved_url
                    logger.info(f"Using resolved URL for recording: {redact_for_log(recording_url)}")
                # yt-dlp often reports 24/7 / US-TTP LIVEs as offline. Confirm
                # via api-live/user/room, /live HTML roomId, then Webcast.
                if not is_live:
                    logger.info("TikTok: yt-dlp reported offline — cross-checking via TikTok live APIs")
                    wc_live, _, wc_title, wc_err = check_tiktok_live_webcast(
                        username, cookies_file, logger, stream_check_timeout)
                    if wc_live:
                        logger.info("TikTok: live API confirms LIVE (yt-dlp said offline)")
                        is_live = True
                        stream_title = wc_title or stream_title
                        error = None
                    else:
                        logger.info(f"TikTok live API fallback: {wc_err or 'not live'}")
            elif platform in ["youtube", "custom"]:
                is_live, stream_title, error, need_impersonate, resolved_url, format_urls = check_stream_ytdlp(url, logger, stream_check_timeout, cookies_file)
                # If yt-dlp resolved to a different URL (e.g. Rumble channel -> video),
                # use the resolved URL for recording so yt-dlp can actually download it
                if resolved_url:
                    recording_url = resolved_url
                    logger.info(f"Using resolved URL for recording: {redact_for_log(recording_url)}")
            elif platform == "fishtank":
                stream_id = fishtank_auth.resolve_stream_id(username)
                is_live, stream_title, error = check_stream_fishtank(
                    stream_id, fishtank_auth, logger, stream_check_timeout
                )
                if is_live:
                    # Build the authenticated HLS URL fresh each time we're about to record
                    hls_url, jwt = fishtank_auth.build_stream_url(stream_id)
                    if hls_url:
                        # Resolve the best-quality variant from the master playlist
                        # so ffmpeg always records at the highest available bitrate
                        # rather than whichever rendition the server lists first.
                        recording_url, stream_info = resolve_best_fishtank_variant(
                            hls_url, jwt, logger, timeout=stream_check_timeout
                        )
                    else:
                        is_live = False
                        error = "could not build stream URL (JWT missing)"
            else:
                is_live, stream_title, error = check_stream_streamlink(url, logger, stream_check_timeout)

            if error:
                consecutive_errors += 1
                new_status = {"status": "Error", "detail": error, "size": "", "time": "", "progress": 0}
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()
                # Exponential backoff for errors (doubles each time, capped)
                error_sleep_seconds = min(error_sleep_seconds * 2, error_backoff_max_seconds)
                sleep_time = jittered_sleep(error_sleep_seconds, poll_jitter_percent)
                logger.warning(f"Error (#{consecutive_errors}): {error} — backing off {sleep_time:.0f}s")
                if interruptible_sleep(sleep_time, wake_event, stop_event):
                    logger.info("Manual check requested — skipping error backoff")
                continue

            if not is_live:
                # Successful check (no error), reset error backoff
                consecutive_errors = 0
                error_sleep_seconds = poll_base_seconds

                if reconnect_mode:
                    # We were recently recording — check if grace period expired
                    if time.monotonic() < reconnect_deadline:
                        remaining = int(reconnect_deadline - time.monotonic())
                        new_status = {"status": "Offline", "detail": f"reconnecting ({remaining}s left)", "size": "", "time": "", "progress": 0}
                        if new_status != last_status:
                            status_queue.put((channel_key, new_status))
                            last_status = new_status.copy()
                        logger.info(f"Stream dropped — fast polling ({remaining}s remaining in grace period)")
                        interruptible_sleep(RECONNECT_POLL_INTERVAL, wake_event, stop_event)
                        continue
                    else:
                        # Grace period expired — stream didn't come back
                        logger.info("Reconnect grace period expired — resuming normal offline polling")
                        reconnect_mode = False
                        _finalize_pending_group()

                # Normal offline — flat interval with jitter (no backoff)
                sleep_time = jittered_sleep(poll_base_seconds, poll_jitter_percent)
                detail = f"next check ~{int(sleep_time)}s"
                if platform == "kick":
                    # Push coverage flag is set by KickPushListener in the
                    # main process via the shared runtime dict.  When the
                    # socket is down the tag simply disappears and the detail
                    # reads exactly like pre-1.7.0 — that absence IS the
                    # degraded-mode indicator.
                    try:
                        if runtime.get(f"kick_push:{channel_key}", False):
                            detail = f"push: listening · {detail}"
                    except Exception:
                        pass  # Manager unavailable during shutdown
                new_status = {"status": "Offline", "detail": detail, "size": "", "time": "", "progress": 0}
                if new_status != last_status:
                    status_queue.put((channel_key, new_status))
                    last_status = new_status.copy()
                logger.info(f"Stream offline — sleeping {sleep_time:.0f}s (base {poll_interval_minutes}min ±{poll_jitter_percent}%)")
                if interruptible_sleep(sleep_time, wake_event, stop_event):
                    logger.info("Manual check requested — checking now")
                    new_status = {"status": "Checking...", "detail": "manual check", "size": "", "time": "", "progress": 0}
                    status_queue.put((channel_key, new_status))
                    last_status = new_status.copy()
                continue

            # Stream is live — reset error state and reconnect mode
            consecutive_errors = 0
            error_sleep_seconds = poll_base_seconds
            is_continuation = reconnect_mode
            if reconnect_mode:
                logger.info("Stream reconnected — resuming recording")
                reconnect_mode = False

            # Create directories only when we're about to record
            os.makedirs(recorded_path, exist_ok=True)
            os.makedirs(processed_path, exist_ok=True)
            os.makedirs(pending_dir, exist_ok=True)
            logger.info(f"Folders ready: {recorded_path}")

            logger.info("Stream detected as live — starting capture")

            # Build output filename. If this is a reconnect within the
            # current session, keep the same base name (as a numbered part)
            # so the segments can be stitched back into one continuous file
            # afterwards — instead of starting a fresh, independently-named
            # clip. A genuinely new session (first connect, or the segment
            # right after a deliberate max_file_size_gb split) closes out
            # and stitches whatever group was pending first.
            if is_continuation and group_parts and group_base_name:
                base_name = f"{group_base_name}.part{len(group_parts) + 1}"
            else:
                _finalize_pending_group()
                group_base_name = build_filename(filename_pattern, username, platform, stream_title)
                base_name = group_base_name
            raw_file = os.path.join(recorded_path, f"{base_name}.ts")
            start_wall = datetime.datetime.now()
            start_time = time.monotonic()

            new_status = {"status": "Recording", "detail": "starting", "size": "0 B", "time": "0:00", "progress": 0}
            if new_status != last_status:
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()

            # Build recording command
            if platform == "kick":
                # Use streamlink for Kick — it has a JS challenge solver for
                # Cloudflare that yt-dlp lacks.  Same approach as Twitch.
                record_cmd = build_recording_command_streamlink(url, raw_file, quality, platform, config, verbose, streamlink_debug)
            elif platform == "fishtank":
                # recording_url was set to the JWT-bearing HLS URL in the check block
                record_cmd = build_recording_command_fishtank(recording_url, raw_file, config, verbose)
            elif platform == "rumble" and rumble_hls_url:
                record_cmd = build_recording_command_rumble_hls(
                    rumble_hls_url, raw_file, config, verbose)
            elif platform in ["youtube", "rumble", "custom"]:
                if format_urls:
                    # Split video+audio tracks (e.g. Chaturbate CMAF) — drive ffmpeg
                    # directly to avoid yt-dlp's live-stream merge deadlock.
                    record_cmd = build_recording_command_ffmpeg_merge(
                        format_urls.get("video"), format_urls.get("audio"), raw_file, config, verbose,
                        manifest_url=format_urls.get("manifest"),
                        http_headers=format_urls.get("http_headers"),
                    )
                    # Build stream_info from the format data we already have.
                    # best_v holds the winning video format dict stored in format_urls.
                    _tbr  = format_urls.get("tbr")
                    _w, _h = format_urls.get("width"), format_urls.get("height")
                    _fps  = format_urls.get("fps")
                    _parts = []
                    if _w and _h:
                        _parts.append(f"{_w}x{_h}")
                    if _fps:
                        try:
                            _parts.append(f"{round(float(_fps))}fps")
                        except (ValueError, TypeError):
                            pass
                    if _tbr:
                        try:
                            _parts.append(f"{_tbr/1000:.1f}Mbps")
                        except (ValueError, TypeError):
                            pass
                    if _parts:
                        stream_info = " · ".join(_parts)
                else:
                    record_cmd = build_recording_command_ytdlp(recording_url, raw_file, config, verbose,
                                                               streamlink_debug, cookies_file,
                                                               impersonate=need_impersonate)
            else:
                record_cmd = build_recording_command_streamlink(url, raw_file, quality, platform, config, verbose, streamlink_debug)

            logger.info(f"Starting recording: {redact_cmd_for_log(record_cmd)}")

            proc = subprocess.Popen(
                record_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            # Monitor
            last_status, split_requested = monitor_recording_process(
                proc, raw_file, start_time, max_record_hours,
                platform, logger, status_queue, channel_key,
                stop_event, last_status, file_creation_timeout,
                verbose=verbose,
                max_file_size_gb=max_file_size_gb,
                stream_info=stream_info,
                ffmpeg_path=config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg') or 'ffmpeg',
                watch_resolution_changes=parser_getboolean(
                    config, 'Recording', 'split_on_resolution_change', True),
            )

            # Clean up process tree
            if proc.poll() is None:
                logger.info("Stopping recording — killing process tree...")
                kill_process_tree(proc.pid, logger)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning("Process did not terminate after tree kill, force killing...")
                    try:
                        proc.kill()
                        proc.wait(timeout=5)
                    except Exception:
                        pass

            exit_code = proc.returncode
            elapsed = time.monotonic() - start_time
            if platform == "fishtank" or format_urls or rumble_hls_url:
                tool_name = "ffmpeg"
            elif platform in ("kick", "twitch", "tiktok"):
                tool_name = "streamlink"
            else:
                tool_name = "yt-dlp"
            logger.info(f"{tool_name} exited with code: {exit_code}")

            # If the recording ended on its own (not user-initiated stop) and
            # we captured at least some data, the stream likely dropped.
            # Enter fast reconnect mode to re-check quickly.
            # Exception: a size-based split should skip the grace period entirely —
            # the stream is still live, so remux and immediately start the next segment.
            if split_requested:
                reconnect_mode = False   # don't wait — fall straight through to remux+restart
            elif not stop_event.is_set() and elapsed > 10:
                reconnect_mode = True
                reconnect_deadline = time.monotonic() + (reconnect_grace_minutes * 60)
                logger.info(f"Recording ended after {format_elapsed(elapsed)} — entering {reconnect_grace_minutes}min reconnect grace period")

            # ── Micro-fragment throttle ─────────────────────────────────────
            # If the server keeps dropping us after only a few seconds (e.g.
            # a CDN reset storm), count consecutive short recordings and apply
            # a growing backoff.  This prevents dozens of tiny files accumulating
            # and reduces unnecessary load on the server.
            if not stop_event.is_set() and not split_requested:
                if elapsed < MICRO_FRAG_MIN_SECONDS:
                    _consecutive_micro_frags += 1
                    if _consecutive_micro_frags >= MICRO_FRAG_LIMIT:
                        throttle_sleep = min(_micro_frag_backoff, MICRO_FRAG_BACKOFF_MAX)
                        _micro_frag_backoff = min(_micro_frag_backoff * 2, MICRO_FRAG_BACKOFF_MAX)
                        logger.warning(
                            f"Micro-fragment storm detected: {_consecutive_micro_frags} consecutive "
                            f"recordings under {MICRO_FRAG_MIN_SECONDS}s — throttling for {throttle_sleep}s"
                        )
                        reconnect_mode = False
                        time.sleep(throttle_sleep)
                else:
                    # Long enough recording — reset fragment counter and backoff
                    if _consecutive_micro_frags >= MICRO_FRAG_LIMIT:
                        logger.info(
                            f"Recording ran {format_elapsed(elapsed)} — micro-fragment throttle reset"
                        )
                    _consecutive_micro_frags = 0
                    _micro_frag_backoff = MICRO_FRAG_BACKOFF_BASE

            time.sleep(2)  # let file handles be released

            # ── Alternate streamlink command for Kick ──
            # Kick already records with streamlink. If that first command
            # exited without creating a file, retry with a shorter-retry,
            # always-debug streamlink argv (not yt-dlp — Kick does not use it).
            if not os.path.exists(raw_file) and platform == "kick" and not stop_event.is_set():
                logger.warning(
                    "streamlink did not create output for Kick — retrying with "
                    "alternate streamlink options..."
                )
                new_status = {"status": "Recording", "detail": "retry (streamlink)", "size": "0 B", "time": "0:00", "progress": 0}
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()

                fallback_cmd = build_recording_command_streamlink_kick(url, raw_file, quality, verbose, streamlink_debug)
                logger.info(f"Fallback recording: {redact_cmd_for_log(fallback_cmd)}")

                start_time = time.monotonic()
                start_wall = datetime.datetime.now()

                try:
                    proc = subprocess.Popen(
                        fallback_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                    )

                    last_status, _ = monitor_recording_process(
                        proc, raw_file, start_time, max_record_hours,
                        platform, logger, status_queue, channel_key,
                        stop_event, last_status, file_creation_timeout,
                        tool_name_override="streamlink", verbose=verbose,
                        max_file_size_gb=max_file_size_gb,
                        ffmpeg_path=config.get('Advanced', 'ffmpeg_path', fallback='ffmpeg'),
                    )

                    if proc.poll() is None:
                        kill_process_tree(proc.pid, logger)
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            try:
                                proc.kill()
                                proc.wait(timeout=5)
                            except Exception:
                                pass

                    elapsed = time.monotonic() - start_time
                    logger.info(f"streamlink (fallback) exited with code: {proc.returncode}")
                    time.sleep(2)

                except FileNotFoundError:
                    logger.error("streamlink not found — cannot use as fallback for Kick")
                except Exception as e:
                    logger.error(f"Streamlink fallback failed: {e}")

            # Check if we actually recorded something
            if not os.path.exists(raw_file):
                # yt-dlp silently changes the extension when merging bestvideo+bestaudio
                # (e.g. -o foo.ts → actually writes foo.mp4).  Check for that before giving up.
                mp4_candidate = os.path.splitext(raw_file)[0] + ".mp4"
                if os.path.exists(mp4_candidate):
                    logger.info(f"yt-dlp merged formats — output is .mp4: {os.path.basename(mp4_candidate)}")
                    raw_file = mp4_candidate
                else:
                    logger.error("Recording file was never created!")
                    new_status = {"status": "Error", "detail": "file not created", "size": "", "time": "", "progress": 0}
                    status_queue.put((channel_key, new_status))
                    time.sleep(60)
                    continue

            file_size = os.path.getsize(raw_file)
            logger.info(f"Recording finished — file size: {human_size(file_size)}")

            if file_size < min_file_size_mb * 1024 * 1024:
                logger.warning(f"Recording too small ({human_size(file_size)}) — deleting")
                try:
                    os.remove(raw_file)
                except Exception as e:
                    logger.error(f"Failed to delete small file: {e}")
                new_status = {"status": "Offline", "detail": "no data captured", "size": "", "time": "", "progress": 0}
                status_queue.put((channel_key, new_status))
                time.sleep(random.uniform(5, 15))
                continue

            # Remux to MP4 (or skip if yt-dlp already produced an MP4)
            mp4_file = os.path.join(processed_path, f"{base_name}.mp4")
            already_mp4 = raw_file.endswith(".mp4")

            if already_mp4:
                # yt-dlp merged bestvideo+bestaudio directly into an MP4 — no remux needed.
                # Move it straight to processed and synthesize the success values.
                logger.info("Raw file is already MP4 — skipping remux, moving directly to processed")
                new_status = {"status": "Processing...", "detail": human_size(file_size), "size": "", "time": "", "progress": 0}
                if new_status != last_status:
                    status_queue.put((channel_key, new_status))
                    last_status = new_status.copy()
                try:
                    shutil.move(raw_file, mp4_file)
                    mp4_size = os.path.getsize(mp4_file)
                    success, error = True, None
                    logger.info(f"Moved to processed: {os.path.basename(mp4_file)} ({human_size(mp4_size)})")
                except Exception as _mv_err:
                    logger.error(f"Failed to move MP4 to processed: {_mv_err}")
                    success, mp4_size, error = False, 0, str(_mv_err)
            else:
                new_status = {"status": "Remuxing...", "detail": human_size(file_size), "size": "", "time": "", "progress": 0}
                if new_status != last_status:
                    status_queue.put((channel_key, new_status))
                    last_status = new_status.copy()
                # Scale timeout based on file size: base timeout or 1 minute per GB, whichever is larger
                file_size_gb = file_size / (1024**3)
                scaled_timeout = max(ffmpeg_timeout, int(file_size_gb * 60) + 120)
                if scaled_timeout > ffmpeg_timeout:
                    logger.info(f"Large file ({human_size(file_size)}) — remux timeout scaled to {scaled_timeout}s")
                success, mp4_size, error = remux_to_mp4(raw_file, mp4_file, ffmpeg_path, logger, scaled_timeout)

            if success:
                # Successful remux — reset bad-stream counters and crash backoff
                consecutive_small_remux_fails = 0
                _small_remux_backoff = SMALL_REMUX_BACKOFF_BASE
                _crash_backoff = CRASH_BACKOFF_BASE
                _consecutive_micro_frags = 0
                _micro_frag_backoff = MICRO_FRAG_BACKOFF_BASE

                # Save metadata sidecar
                save_metadata(
                    mp4_file, username, platform,
                    start_wall.isoformat(),
                    elapsed,
                    stream_title,
                )

                # ── Audio stream sanity check ───────────────────────────────
                # Fishtank's cameraman stream sometimes delivers video-only HLS
                # segments (no audio track at all), producing silent MP4s that
                # are useless for fan clips.  Detect this early so it shows up
                # clearly in the log rather than being discovered later.
                try:
                    probe_cmd = [
                        ffprobe_from_ffmpeg(ffmpeg_path),
                        "-v", "quiet",
                        "-print_format", "json",
                        "-show_streams",
                        "-select_streams", "a",
                        mp4_file,
                    ]
                    probe_result = subprocess.run(
                        probe_cmd, capture_output=True, text=True, timeout=30
                    )
                    if probe_result.returncode == 0:
                        probe_data = json.loads(probe_result.stdout or "{}")
                        audio_streams = probe_data.get("streams", [])
                        if not audio_streams:
                            logger.warning(
                                f"⚠ NO AUDIO TRACK in {os.path.basename(mp4_file)} — "
                                f"stream delivered video-only segments (silent recording)"
                            )
                        else:
                            codec = audio_streams[0].get("codec_name", "?")
                            channels = audio_streams[0].get("channels", "?")
                            if platform == "fishtank":
                                logger.info(
                                    f"Audio OK: {codec}, {channels}ch"
                                )
                except Exception as _probe_err:
                    logger.debug(f"Audio check skipped: {_probe_err}")

                # Move raw file to pending deletion.
                # Kick/streamlink sometimes holds a file handle open for several
                # seconds after the process exits.  Retry generously (up to ~60 s)
                # so the file doesn't get stranded in Recorded until next startup.
                # Skip if already_mp4 — the file was already moved to processed above.
                if not already_mp4:
                    pending_path = os.path.join(pending_dir, os.path.basename(raw_file))
                    max_retries = 6
                    _move_wait = [5, 10, 10, 15, 15, 15]  # seconds between attempts
                    for attempt in range(max_retries):
                        try:
                            shutil.move(raw_file, pending_path)
                            logger.info(f"Moved raw to: {pending_path}")
                            break
                        except PermissionError as e:
                            if attempt < max_retries - 1:
                                wait = _move_wait[attempt]
                                logger.warning(f"File locked, retrying in {wait}s… (attempt {attempt + 1}/{max_retries})")
                                time.sleep(wait)
                            else:
                                logger.error(f"Move failed after {max_retries} attempts: {e}")
                                logger.info(f"File will be cleaned up on next run: {raw_file}")
                        except Exception as e:
                            logger.error(f"Move failed: {e}")
                            break

                # Track this segment as part of the current continuity group so
                # it can be stitched together with any sibling reconnect
                # segments once the session is known to be over.
                group_parts.append({
                    'mp4': mp4_file,
                    'meta': mp4_file.rsplit('.', 1)[0] + '.meta.json',
                    'start_wall': start_wall,
                    'elapsed': elapsed,
                    'title': stream_title,
                })

                new_status = {
                    "status": "Completed",
                    "detail": human_size(mp4_size),
                    "size": "", "time": "", "progress": 100,
                }
            else:
                logger.error(f"Remux failed: {error}")
                new_status = {"status": "Remux failed", "detail": error or "unknown", "size": "", "time": "", "progress": 0}

                # ── Bad-stream backoff (Bug #1 fix) ──────────────────────────
                # "output too small" means the stream is handing us stub/garbage
                # data that passes the raw min_file_size_mb check but produces a
                # worthless remux.  Delete the tiny mp4 artifact and back off
                # instead of hammering the server in a tight 5-second loop.
                if error == "output too small":
                    # Delete the useless sub-threshold mp4 that was written
                    if os.path.exists(mp4_file):
                        try:
                            os.remove(mp4_file)
                            logger.info(f"Deleted undersized mp4 artifact: {os.path.basename(mp4_file)}")
                        except Exception as del_err:
                            logger.warning(f"Could not delete undersized mp4: {del_err}")

                    # Also move the raw .ts to PendingDeletion so it doesn't
                    # accumulate in the Recorded directory
                    try:
                        pending_path = os.path.join(pending_dir, os.path.basename(raw_file))
                        shutil.move(raw_file, pending_path)
                        logger.info(f"Moved bad raw .ts to PendingDeletion: {os.path.basename(raw_file)}")
                    except Exception:
                        pass

                    consecutive_small_remux_fails += 1
                    if consecutive_small_remux_fails >= SMALL_REMUX_FAIL_LIMIT:
                        # Stream is consistently serving garbage — treat as
                        # degraded/offline and apply a growing backoff so we
                        # don't keep hammering its CDN.
                        backoff = min(_small_remux_backoff, SMALL_REMUX_BACKOFF_MAX)
                        _small_remux_backoff = min(_small_remux_backoff * 2, SMALL_REMUX_BACKOFF_MAX)
                        logger.warning(
                            f"Stream has produced {consecutive_small_remux_fails} consecutive "
                            f"undersized recordings — treating as degraded, backing off {backoff}s"
                        )
                        new_status = {
                            "status": "Offline",
                            "detail": f"bad stream data, retry in {backoff}s",
                            "size": "", "time": "", "progress": 0,
                        }
                        if new_status != last_status:
                            status_queue.put((channel_key, new_status))
                            last_status = new_status.copy()
                        # Disable reconnect mode so normal offline polling resumes
                        reconnect_mode = False
                        _finalize_pending_group()
                        time.sleep(backoff)
                        continue

            if new_status != last_status:
                status_queue.put((channel_key, new_status))
                last_status = new_status.copy()

            time.sleep(5)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received — exiting")
            break
        except Exception as e:
            logger.error(f"Worker crashed: {e}", exc_info=True)
            status_queue.put((channel_key, {"status": "Error", "detail": str(e)[:50], "size": "", "time": "", "progress": 0}))
            logger.warning(f"Worker restarting in {_crash_backoff}s (backoff)")
            time.sleep(_crash_backoff)
            _crash_backoff = min(_crash_backoff * 2, CRASH_BACKOFF_MAX)

    # Stitch together any reconnect segments still pending when the worker
    # stops (manual stop, app shutdown, or KeyboardInterrupt) so nothing is
    # left behind as unmerged clips.
    _finalize_pending_group()

    logger.info("Worker STOPPED")

