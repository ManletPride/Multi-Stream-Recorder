"""Regression tests for leftover efficiency and polish optimizations:
- Global concurrency cap on background remuxes (msr/worker.py)
- TikTok cookie-jar caching, explicit offline short-circuit, and webcast cooldown
- Clip Editor LUT-backed apply_gain() and single-copy VideoFeed._read_raw()
- LogFollower O(1) incremental filtered stats (no full-buffer rescan per append)
- XDG preview cache location + exit cleanup (msr/preview_strip.py)
- Idle Recorded/ mtime-gated cleanup_queue() cache (msr/agent_status.py)
"""
import array
import io
import json
import logging
import os
import queue
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from msr import agent_status  # noqa: E402
from msr import clip_audio  # noqa: E402
from msr import clip_editor  # noqa: E402
from msr import logview  # noqa: E402
from msr import platforms  # noqa: E402
from msr import poller as poller_mod  # noqa: E402
from msr.config import Config  # noqa: E402
from msr.preview_strip import PreviewStrip  # noqa: E402
from msr import worker as worker_mod  # noqa: E402


class RemuxConcurrencyCapTests(unittest.TestCase):
    def test_remux_slot_caps_parallel_remuxes_and_honours_cancel(self):
        sem = threading.BoundedSemaphore(2)
        active = 0
        peak = 0
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def hold():
            nonlocal active, peak
            with worker_mod._RemuxSlot(sem) as ok:
                self.assertTrue(ok)
                with lock:
                    active += 1
                    peak = max(peak, active)
                try:
                    barrier.wait(timeout=2)
                except threading.BrokenBarrierError:
                    pass
                time.sleep(0.05)
                with lock:
                    active -= 1

        threads = [threading.Thread(target=hold) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
        self.assertEqual(peak, 2)

        # Cancelled caller exits immediately without acquiring a held slot
        sem2 = threading.BoundedSemaphore(1)
        sem2.acquire()
        try:
            cancel = threading.Event()
            cancel.set()
            with worker_mod._RemuxSlot(sem2, cancel_event=cancel) as ok:
                self.assertFalse(ok)
        finally:
            sem2.release()


class TikTokWebcastEfficiencyTests(unittest.TestCase):
    def test_cookie_jar_cached_by_mtime_and_size(self):
        platforms._tiktok_cj_cache.clear()
        with tempfile.TemporaryDirectory() as tmp:
            jar = os.path.join(tmp, "cookies.txt")
            with open(jar, "w", encoding="utf-8") as fh:
                fh.write("# Netscape HTTP Cookie File\n")
            logger = logging.getLogger("test.tiktok")
            with mock.patch("http.cookiejar.MozillaCookieJar.load", autospec=True) as load_mock:
                cj1 = platforms._load_tiktok_cookiejar(jar, logger)
                cj2 = platforms._load_tiktok_cookiejar(jar, logger)
                self.assertIs(cj1, cj2)
                self.assertEqual(load_mock.call_count, 1)

    def test_explicit_status_4_short_circuits_without_extra_requests(self):
        calls = []

        def fake_get(url, opener, timeout, logger):
            calls.append(url)
            return json.dumps({
                "data": {
                    "user": {"roomId": "999", "status": 4},
                    "liveRoom": {"status": 4, "title": "Ended"},
                }
            })

        with mock.patch.object(platforms, "_tiktok_http_get", side_effect=fake_get):
            live, rid, title, err = platforms.check_tiktok_live_webcast(
                "alice", None, logging.getLogger("test.tiktok"), timeout=5,
            )
        self.assertFalse(live)
        self.assertEqual(rid, "999")
        self.assertEqual(len(calls), 1, "must not hit profile/live/webcast after status=4")

    def test_poller_applies_webcast_cooldown_on_clean_offline_polls(self):
        clock_now = [100.0]
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(os.path.join(tmp, "config.ini"))
            coord = poller_mod.PollCoordinator(
                cfg, {"poll_interval_minutes": 1.0}, queue.Queue(),
                on_live=lambda ch: True, data_dir=tmp, clock=lambda: clock_now[0],
            )
            slot = poller_mod._ChannelSlot(
                "tiktok:alice", "tiktok", "alice", "https://www.tiktok.com/@alice/live", 0.0, 60.0,
            )
            with mock.patch.object(poller_mod, "check_stream_ytdlp",
                                   return_value=(False, None, None, False, None, None)), \
                    mock.patch.object(poller_mod, "check_tiktok_live_webcast",
                                      return_value=(False, None, None, "offline")) as wc:
                coord._check_one(slot)
                self.assertEqual(wc.call_count, 1)
                # 60s later (< 300s cooldown): skipped
                clock_now[0] += 60.0
                coord._check_one(slot)
                self.assertEqual(wc.call_count, 1)
                # Past 300s cooldown: runs again
                clock_now[0] += 300.0
                coord._check_one(slot)
                self.assertEqual(wc.call_count, 2)


class ClipEditorInnerLoopTests(unittest.TestCase):
    def test_apply_gain_lut_matches_exact_int16_scaling_and_clipping(self):
        samples = array.array("h", [-32768, -20000, -16, -1, 0, 1, 16, 20000, 32767])
        raw = samples.tobytes()
        for gain in (0.25, 0.5, 0.75, 1.5, 2.0):
            got = array.array("h")
            got.frombytes(clip_audio.apply_gain(raw, gain))
            expected = [clip_audio._clip_sample(int(s * gain)) for s in samples]
            self.assertEqual(list(got), expected, f"gain={gain}")

    def test_video_feed_read_raw_returns_valid_ppm_bytes(self):
        w, h = 4, 2
        payload = bytes(range(w * h * 3))

        class FakeProc:
            def __init__(self):
                self.stdout = io.BytesIO(payload)

            def kill(self):
                pass

            def wait(self, timeout=None):
                return 0

        feed = clip_editor.VideoFeed("ffmpeg", "dummy.mp4", 0.0, size=(w, h), spawn=lambda cmd: FakeProc())
        try:
            deadline = time.monotonic() + 2.0
            while feed.decoded < 1 and time.monotonic() < deadline:
                time.sleep(0.01)
            frame = feed.take_until(1.0)
            self.assertEqual(frame, f"P6\n{w} {h}\n255\n".encode("ascii") + payload)
        finally:
            feed.close()


class LogFollowerIncrementalStatsTests(unittest.TestCase):
    def test_filtered_append_updates_counts_without_rescanning_buffer(self):
        class FakeText:
            def __init__(self):
                self.lines = []

            def configure(self, **kw):
                pass

            def insert(self, _where, text, *_tags):
                self.lines.append(text)

            def delete(self, _a, _b):
                self.lines.clear()

            def index(self, _spec):
                return f"{len(self.lines) + 1}.0"

            def yview(self):
                return (0.0, 1.0)

            def see(self, _where):
                pass

        stats = []
        lf = logview.LogFollower(FakeText(), on_stats=lambda s, t: stats.append((s, t)))
        lf.append(["10:00:00 info alpha", "10:00:01 ERROR beta"])
        lf.set_filter("beta")
        self.assertEqual(stats[-1], (1, 2))

        # Appending non-matching lines must NOT call matches() on existing buffer items
        with mock.patch.object(logview, "matches", wraps=logview.matches) as m:
            lf.append(["10:00:02 info gamma", "10:00:03 info delta"])
            self.assertEqual(m.call_count, 2, "only the 2 new messages should be checked")
        self.assertEqual(stats[-1], (1, 4))

        lf.append(["10:00:04 ERROR beta again"])
        self.assertEqual(stats[-1], (2, 5))


class PreviewCacheAndQueueCacheTests(unittest.TestCase):
    def test_preview_cache_uses_xdg_on_posix_and_cleans_up_pngs_only(self):
        if os.name == "nt":
            self.skipTest("POSIX XDG cache test")
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {"XDG_CACHE_HOME": tmp}):
                cache_dir = PreviewStrip._cache_dir()
                self.assertEqual(cache_dir, os.path.join(tmp, "multi-stream-recorder", "previews"))
                png = os.path.join(cache_dir, "chan.png")
                other = os.path.join(cache_dir, "keep.txt")
                outside = os.path.join(tmp, "outside.png")
                for p in (png, other, outside):
                    with open(p, "w", encoding="utf-8") as fh:
                        fh.write("x")
                os.symlink(outside, os.path.join(cache_dir, "link.png"))
                removed = PreviewStrip.cleanup_cache()
                self.assertEqual(removed, 1)
                self.assertFalse(os.path.exists(png))
                self.assertTrue(os.path.exists(other))
                self.assertTrue(os.path.exists(outside), "symlinks must never be followed")

    def test_cleanup_queue_skips_redundant_idle_walks_until_mtime_changes(self):
        agent_status._cleanup_queue_cache.clear()
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(os.path.join(tmp, "config.ini"))
            streams = os.path.join(tmp, "streams")
            rec_dir = os.path.join(streams, "Recorded")
            os.makedirs(rec_dir)
            cfg.config.set("Paths", "streams_dir", streams)
            cfg.config.set("Paths", "fallback_dir", "off")

            with mock.patch.object(agent_status.os, "walk", wraps=os.walk) as walk_mock:
                self.assertEqual(agent_status.cleanup_queue(cfg, ()), [])
                self.assertEqual(walk_mock.call_count, 1)
                # Second idle call with unchanged mtime skips os.walk
                self.assertEqual(agent_status.cleanup_queue(cfg, ()), [])
                self.assertEqual(walk_mock.call_count, 1)
                # Adding a .ts file updates directory mtime and invalidates cache
                ts_path = os.path.join(rec_dir, "clip.ts")
                with open(ts_path, "wb") as fh:
                    fh.write(b"1234")
                # Bump mtime explicitly in case filesystem tick is coarse
                st = os.stat(rec_dir)
                os.utime(rec_dir, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
                got = agent_status.cleanup_queue(cfg, ())
                self.assertEqual(len(got), 1)
                self.assertEqual(got[0]["path"], ts_path)
                self.assertEqual(walk_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
