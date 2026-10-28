"""Library statistics (msr/library.py) on temporary folder trees. Nothing here touches real data."""
import json
import os
import queue
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from msr import library as lib

DAY = 86400


def put(path: Path, size: int = 10, mtime: float | None = None, meta_seconds: float | None = None):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    if meta_seconds is not None:
        path.with_name(path.name.rsplit(".", 1)[0] + ".meta.json").write_text(
            json.dumps({"duration_seconds": meta_seconds}), encoding="utf-8")
    return path


class Tree(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "streams"
        self.root.mkdir()
        self.now = time.time()

    def build(self):
        r = self.root
        put(r / "Processed/kick/alice/alice_20260101_120000.mp4", 1000, meta_seconds=3600)
        put(r / "Processed/kick/alice/alice_20260215_120000.mp4", 3000, meta_seconds=7200)
        put(r / "Processed/kick/alice/alice_20260215_120000.part2.mp4", 500)
        put(r / "Processed/twitch/bob/bob_20260301_000000.mp4", 2000, meta_seconds=100)
        put(r / "Processed/custom/chaturbate/carol/carol_20260310_101010.mp4", 700)
        put(r / "Processed/youtube/@dave/dave_20260320_101010.mp4", 400)
        put(r / "Recorded/kick/alice/alice_20260401_120000.ts", 5000)
        put(r / "Recorded/kick/alice/alice_20260402_120000.mp4.part", 111)
        put(r / "PendingDeletion/twitch/bob/bob_20250101_000000.mp4", 900)
        put(r / "Clips/twitch/bob/clip1.mp4", 300)
        put(r / "Clips/twitch/bob/shot1.jpg", 50)
        return lib.scan_library(lib.Roots(str(r)), now=self.now)


class ScanTests(Tree):
    def test_totals_by_category(self):
        s = self.build()
        self.assertEqual(s.cat("processed"), (1000 + 3000 + 500 + 2000 + 700 + 400, 6))
        self.assertEqual(s.cat("raw"), (5000 + 111, 2))
        self.assertEqual(s.cat("pending"), (900, 1))
        self.assertEqual(s.cat("clips"), (300, 1))
        self.assertEqual(s.cat("screenshots"), (50, 1))
        self.assertEqual(s.cat("sidecars")[1], 3)  # the three .meta.json files
        self.assertEqual(s.recordings_bytes, 7600 + 5111)
        self.assertEqual(s.partial_files, 1)
        self.assertFalse(s.cancelled)
        self.assertTrue(all(s.roots_found[k] for k in ("Processed", "Recorded", "PendingDeletion", "Clips")))

    def test_platform_shares_and_channel_ranking(self):
        s = self.build()
        platforms = {p: b for p, b, _ in s.ranked_platforms()}
        self.assertEqual(platforms["kick"], 1000 + 3000 + 500 + 5000 + 111)
        self.assertEqual(platforms["twitch"], 2000)
        self.assertEqual(platforms["custom"], 700)
        self.assertEqual(platforms["youtube"], 400)
        self.assertEqual(s.ranked_platforms()[0][0], "kick")
        ranked = s.ranked_channels()
        self.assertEqual(ranked[0].name, "alice")
        self.assertEqual(ranked[0].recordings_bytes, 9611)
        names = {(c.platform, c.name) for c in ranked}
        self.assertIn(("custom", "chaturbate/carol"), names)  # custom URLs nest as site/user
        self.assertIn(("youtube", "@dave"), names)
        bob = next(c for c in ranked if c.name == "bob")
        self.assertEqual((bob.pending_bytes, bob.clip_bytes, bob.shot_bytes), (900, 300, 50))
        self.assertEqual(s.biggest_file, (5000, "kick/alice"))

    def test_dates_come_from_names_then_mtime(self):
        s = self.build()
        day = lambda ts: time.strftime("%Y-%m-%d", time.localtime(ts))
        self.assertEqual(day(s.oldest), "2026-01-01")
        self.assertEqual(day(s.newest), "2026-04-02")
        self.assertEqual(sorted(s.months), ["2026-01", "2026-02", "2026-03"])  # finished files only
        self.assertEqual(s.months["2026-03"][1], 3)
        put(self.root / "Processed/kick/zed/no_date_in_name.mp4", 5, mtime=time.mktime((2024, 5, 6, 12, 0, 0, 0, 0, -1)))
        s = lib.scan_library(lib.Roots(str(self.root)), now=self.now)
        self.assertEqual(day(s.oldest), "2024-05-06")
        self.assertIn("2024-05", s.months)

    def test_recent_growth_uses_modification_time(self):
        put(self.root / "Processed/kick/a/a_1.mp4", 100, mtime=self.now - 2 * DAY)
        put(self.root / "Processed/kick/a/a_2.mp4", 200, mtime=self.now - 20 * DAY)
        put(self.root / "Processed/kick/a/a_3.mp4", 400, mtime=self.now - 90 * DAY)
        s = lib.scan_library(lib.Roots(str(self.root)), now=self.now)
        self.assertEqual((s.bytes_7d, s.bytes_30d), (100, 300))

    def test_top_level_folder_names_match_case_insensitively(self):
        put(self.root / "processed/kick/alice/a_20260101_000000.mp4", 10)
        put(self.root / "recorded/kick/alice/a_20260102_000000.ts", 20)
        put(self.root / "clips/kick/alice/c.mp4", 30)
        s = lib.scan_library(lib.Roots(str(self.root)))
        self.assertEqual((s.cat("processed")[0], s.cat("raw")[0], s.cat("clips")[0]), (10, 20, 30))

    def test_clips_dir_override_and_extra_folders(self):
        clips = Path(self.tmp.name) / "my clips"
        extra = Path(self.tmp.name) / "archive"
        put(clips / "kick/alice/c.mp4", 30)
        put(extra / "season1/ep1.mp4", 1000)
        put(extra / "loose.mp4", 5)
        put(self.root / "Clips/kick/alice/ignored.mp4", 99)  # not used: an override is set
        s = lib.scan_library(lib.Roots(str(self.root), str(clips), (str(extra), str(extra / "nope"))))
        self.assertEqual(s.cat("clips"), (30, 1))
        self.assertEqual(s.cat("other"), (1005, 2))
        self.assertEqual(s.recordings_bytes, 0)  # extra folders are not "recordings" and make no channels
        self.assertEqual(s.channels.get(("other", "season1")), None)
        self.assertTrue(s.roots_found["Other folder 1"])
        self.assertFalse(s.roots_found["Other folder 2"])

    def test_missing_folders_are_reported_not_raised(self):
        s = lib.scan_library(lib.Roots(str(Path(self.tmp.name) / "nope")))
        self.assertFalse(s.roots_found["Recordings folder"])
        self.assertEqual(s.files_seen, 0)
        self.assertIn("not found: Recordings folder", " ".join(lib.summary_lines(s)))
        s = lib.scan_library(lib.Roots(""))
        self.assertEqual(s.recordings_bytes, 0)
        self.assertEqual(lib.scan_library(lib.Roots(str(self.root))).recordings_files, 0)  # empty folder

    def test_unreadable_folder_is_counted_and_the_rest_still_scanned(self):
        self.build()
        real = os.scandir
        bad = str(self.root / "Processed" / "twitch" / "bob")

        def flaky(path="."):
            if str(path) == bad:
                raise PermissionError("denied")
            return real(path)
        with mock.patch.object(lib.os, "scandir", flaky):
            s = lib.scan_library(lib.Roots(str(self.root)))
        self.assertEqual(s.unreadable_dirs, 1)
        self.assertEqual(s.cat("processed")[1], 5)  # six finished files minus bob's one
        self.assertIn("could not be read", " ".join(lib.summary_lines(s)))

    def test_symlinks_and_junctions_are_not_followed(self):
        self.build()
        outside = Path(self.tmp.name) / "outside"
        put(outside / "big.mp4", 99999)
        link = self.root / "Processed" / "kick" / "linked"
        made = 0
        try:
            os.symlink(outside, link, target_is_directory=True)
            made += 1
            os.symlink(outside / "big.mp4", self.root / "Processed" / "kick" / "alice" / "file_link.mp4")
            made += 1
        except (OSError, NotImplementedError):
            if os.name == "nt" and made == 0:  # junctions need no privilege
                import subprocess
                done = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(outside)],
                                      capture_output=True, text=True)
                made += 1 if done.returncode == 0 else 0
        if not made:
            self.skipTest("neither symlinks nor junctions available here")
        s = lib.scan_library(lib.Roots(str(self.root)))
        self.assertEqual(s.symlinks_skipped, made)
        self.assertNotIn(("kick", "linked"), s.channels)
        self.assertEqual(s.recordings_bytes, 7600 + 5111)

    def test_unicode_and_odd_names(self):
        put(self.root / "Processed/twitch/\u00dcn\u00efc\u00f6de_\ud55c\uae00/x_20260105_010101.mp4", 123)
        put(self.root / "Processed/kick/_under_score/y.mp4", 7)
        put(self.root / "Processed/kick/dot.name/z.mp4", 8)
        put(self.root / "Processed/loose.mp4", 9)
        put(self.root / "Processed/kick/loose_in_platform.mp4", 4)
        s = lib.scan_library(lib.Roots(str(self.root)))
        names = {c.name for c in s.channels.values()}
        self.assertTrue({"\u00dcn\u00efc\u00f6de_\ud55c\uae00", "_under_score", "dot.name", lib.UNSORTED} <= names)
        self.assertEqual(s.recordings_bytes, 123 + 7 + 8 + 9 + 4)
        lib.summary_text(s).encode("utf-8")

    def test_case_variants_of_a_channel_are_one_channel(self):
        put(self.root / "Processed/kick/Alice/a_20260101_000000.mp4", 10)
        put(self.root / "Recorded/kick/alice/a_20260102_000000.ts", 20)
        s = lib.scan_library(lib.Roots(str(self.root)))
        self.assertEqual(len(s.channels), 1)
        self.assertEqual(next(iter(s.channels.values())).recordings_bytes, 30)

    def test_cancel_returns_partial_results_flagged(self):
        for i in range(400):
            put(self.root / f"Processed/kick/c{i % 5}/f{i}.mp4", 1)
        cancel = threading.Event()
        cancel.set()
        s = lib.scan_library(lib.Roots(str(self.root)), cancel=cancel)
        self.assertTrue(s.cancelled)
        self.assertLess(s.files_seen, 400)
        self.assertIn("partial", " ".join(lib.summary_lines(s)))

    def test_progress_is_reported_and_ends_with_done(self):
        for i in range(50):
            put(self.root / f"Processed/kick/c/f{i}.mp4", 1)
        calls = []
        lib.scan_library(lib.Roots(str(self.root)), progress=lambda f, b, l: calls.append((f, b, l)))
        self.assertEqual(calls[-1], (50, 50, "done"))

    def test_scan_changes_nothing_on_disk(self):
        self.build()
        before = sorted((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in self.root.rglob("*"))
        lib.scan_library(lib.Roots(str(self.root)))
        after = sorted((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in self.root.rglob("*"))
        self.assertEqual(before, after)

    def test_a_large_library_scans_quickly(self):
        for c in range(20):
            d = self.root / "Processed" / "kick" / f"chan{c}"
            d.mkdir(parents=True)
            for i in range(250):
                (d / f"chan{c}_202601{(i % 28) + 1:02d}_000000_{i}.mp4").write_bytes(b"x")
        t = time.monotonic()
        s = lib.scan_library(lib.Roots(str(self.root)))
        took = time.monotonic() - t
        self.assertEqual(s.cat("processed")[1], 5000)
        self.assertLess(took, 5.0)


class RosterTests(Tree):
    def test_in_roster_flags(self):
        s = self.build()
        lib.mark_roster(s, ["alice", "twitch:BOB", "custom:https://chaturbate.com/carol/", "tiktok:@qvc"])
        flags = {(c.platform, c.name): c.in_roster for c in s.channels.values()}
        self.assertTrue(flags[("kick", "alice")])
        self.assertTrue(flags[("twitch", "bob")])
        self.assertFalse(flags[("youtube", "@dave")])  # on disk but not in the roster any more
        lib.mark_roster(s, None)  # unknown roster leaves the flags alone
        self.assertTrue(flags[("kick", "alice")])


class SummaryTests(Tree):
    def test_headline_lines(self):
        s = self.build()
        text = "\n".join(lib.summary_lines(s))
        self.assertIn("recordings in 8 files", text)
        self.assertIn("By site: Kick", text)
        self.assertIn("Biggest channels: alice (Kick)", text)
        self.assertIn("Recordings from 2026-01-01 to 2026-04-02", text)
        self.assertIn("waiting in PendingDeletion", text)
        self.assertIn("clips and screenshots", text)
        self.assertNotIn(self.tmp.name, lib.summary_text(s))

    def test_fill_estimate_needs_real_history(self):
        put(self.root / "Processed/kick/a/a_20260101_000000.mp4", 1000, mtime=self.now - 40 * DAY)
        put(self.root / "Processed/kick/a/a_20260901_000000.mp4", 3000, mtime=self.now - 1 * DAY)
        s = lib.scan_library(lib.Roots(str(self.root)), now=self.now)
        self.assertEqual(s.bytes_30d, 3000)
        s.bytes_30d = 30 * 10**9  # pretend 30 GB came in over the last 30 days
        s.oldest = self.now - 100 * DAY
        s.disk = (10**12, 5 * 10**11, 4 * 10**11)
        rate, days = lib.fill_estimate(s)
        self.assertAlmostEqual(rate, 10**9)
        self.assertAlmostEqual(days, 400)
        self.assertIn("fills in about 400 days", "\n".join(lib.summary_lines(s)))
        s.disk = (10**12, 5 * 10**11, 10**15)
        self.assertIsNone(lib.fill_estimate(s))  # > 10 years is not a useful number
        s.disk = (10**12, 5 * 10**11, 4 * 10**11)
        s.oldest = self.now - 3 * DAY
        self.assertIsNone(lib.fill_estimate(s))  # a few days of history say nothing
        s.oldest = self.now - 100 * DAY
        s.bytes_30d = 0
        self.assertIsNone(lib.fill_estimate(s))
        s.disk = None
        s.bytes_30d = 5
        self.assertIsNone(lib.fill_estimate(s))

    def test_to_dict_is_json_and_has_no_paths(self):
        s = self.build()
        lib.mark_roster(s, ["alice"])
        data = lib.to_dict(s)
        text = json.dumps(data)
        self.assertNotIn(self.tmp.name, text)
        self.assertNotIn("Users", text)
        self.assertEqual(data["recordings"]["bytes"], 12711)
        self.assertEqual(data["by_platform"][0]["platform"], "kick")
        self.assertEqual(data["channels"][0]["name"], "alice")
        self.assertTrue(data["channels"][0]["in_roster"])
        self.assertEqual(data["recorded_hours"], None)
        self.assertEqual(lib.to_dict(s, max_channels=1)["channel_count"], len(s.channels))
        self.assertEqual(len(lib.to_dict(s, max_channels=1)["channels"]), 1)

    def test_sizes_and_hours_format(self):
        self.assertEqual(lib.fmt_hours(3600 * 5.5), "5.5 h")
        self.assertEqual(lib.fmt_hours(3600 * 1234), "1,234 h")
        self.assertEqual(lib.platform_label("kick"), "Kick")
        self.assertEqual(lib.platform_label("youtube"), "YouTube")
        self.assertEqual(lib.platform_label("newsite"), "Newsite")


class HoursTests(Tree):
    def test_sidecars_give_hours_without_probing(self):
        s = self.build()
        probe = mock.Mock(return_value=None)
        known, missing = lib.count_hours(s, None, None)
        self.assertEqual((known, missing), (3, 3))  # alice x2 + bob have sidecars; .part2, carol, dave do not
        self.assertAlmostEqual(s.seconds_total, 3600 + 7200 + 100)
        alice = s.channels[("kick", "alice")]
        self.assertAlmostEqual(alice.seconds, 10800)
        self.assertEqual(alice.timed, 2)
        probe.assert_not_called()
        self.assertIn("length known for 3 of 6", "\n".join(lib.summary_lines(s)))

    def test_probe_fills_the_rest_rate_limited_and_cached(self):
        s = self.build()
        cache_file = Path(self.tmp.name) / "data" / "cache.json"
        cache_file.parent.mkdir()
        cache = lib.DurationCache(str(cache_file))
        probe = mock.Mock(return_value=60.0)
        sleeps = []
        known, missing = lib.count_hours(s, cache, probe, min_interval=0.5, sleep=sleeps.append)
        self.assertEqual((known, missing), (6, 0))
        self.assertEqual(probe.call_count, 3)
        self.assertGreaterEqual(len(sleeps), 2)  # throttled between probes
        self.assertTrue(cache_file.exists())
        # second run: everything from the cache, nothing probed
        probe2 = mock.Mock(return_value=999.0)
        s2 = lib.scan_library(lib.Roots(str(self.root)), now=self.now)
        lib.count_hours(s2, lib.DurationCache(str(cache_file)), probe2)
        probe2.assert_not_called()
        self.assertAlmostEqual(s2.seconds_total, s.seconds_total)

    def test_a_changed_file_is_read_again(self):
        s = self.build()
        cache = lib.DurationCache(None)
        probe = mock.Mock(return_value=10.0)
        lib.count_hours(s, cache, probe, min_interval=0, sleep=lambda x: None)
        n = probe.call_count
        target = self.root / "Processed/custom/chaturbate/carol/carol_20260310_101010.mp4"
        target.write_bytes(b"y" * 9999)
        s = lib.scan_library(lib.Roots(str(self.root)), now=self.now)
        lib.count_hours(s, cache, probe, min_interval=0, sleep=lambda x: None)
        self.assertEqual(probe.call_count, n + 1)

    def test_cancel_keeps_what_was_counted(self):
        s = self.build()
        cancel = threading.Event()
        seen = []

        def probe(path):
            seen.append(path)
            cancel.set()
            return 5.0
        lib.count_hours(s, lib.DurationCache(None), probe, cancel=cancel, min_interval=0, sleep=lambda x: None)
        self.assertTrue(s.cancelled)
        self.assertLess(len(seen), 4)

    def test_bad_cache_file_and_bad_sidecars(self):
        bad = Path(self.tmp.name) / "c.json"
        bad.write_text("{nope")
        self.assertEqual(lib.DurationCache(str(bad)).items, {})
        bad.write_text(json.dumps({"version": 1, "d": {"a": "x", "b": [1, 2, 3]}}))
        self.assertEqual(list(lib.DurationCache(str(bad)).items), ["b"])
        v = put(self.root / "Processed/kick/q/q_1.mp4", 5)
        v.with_name("q_1.meta.json").write_text("[1]")
        self.assertIsNone(lib.sidecar_seconds(str(v)))
        v.with_name("q_1.meta.json").write_text(json.dumps({"duration_seconds": "abc"}))
        self.assertIsNone(lib.sidecar_seconds(str(v)))

    def test_stale_cache_entries_are_pruned_after_a_full_count(self):
        s = self.build()
        cache = lib.DurationCache(str(Path(self.tmp.name) / "c.json"))
        cache.put("processed/gone/file.mp4", 1, 1, 5)
        lib.count_hours(s, cache, None)
        self.assertNotIn("processed/gone/file.mp4", cache.items)


class JobTests(Tree):
    def drain(self, out, until):
        got = []
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                got.append(out.get(timeout=0.2))
            except queue.Empty:
                continue
            if got[-1][0] in until:
                break
        return got

    def test_job_scans_in_a_thread_and_reports(self):
        self.build()
        out = queue.Queue()
        job = lib.ScanJob(lib.Roots(str(self.root)), out, roster=["alice"],
                          cache_path=str(Path(self.tmp.name) / "c.json"), hours_after=True)
        job.start()
        msgs = self.drain(out, {"hours_done", "error"})
        job.join(5)
        kinds = [m[0] for m in msgs]
        self.assertIn("done", kinds)
        self.assertEqual(kinds[-1], "hours_done")
        stats = msgs[-1][1]
        self.assertTrue(stats.channels[("kick", "alice")].in_roster)
        self.assertEqual(msgs[-1][2:], (3, 3))

    def test_job_errors_become_messages(self):
        out = queue.Queue()
        with mock.patch.object(lib, "scan_library", side_effect=RuntimeError("boom")):
            job = lib.ScanJob(lib.Roots(str(self.root)), out)
            job.start()
            msgs = self.drain(out, {"error"})
        self.assertEqual(msgs[-1], ("error", "RuntimeError: boom"))

    def test_roots_from_config(self):
        class Cfg:
            def __init__(self, d):
                self.d = d

            def get(self, section, key, fallback=""):
                return self.d.get((section, key), fallback)
        r = lib.roots_from_config(Cfg({("Paths", "streams_dir"): " X ", ("Clipping", "clips_dir"): "",
                                       ("Library", "extra_dirs"): ' "A" ; B ;; '}))
        self.assertEqual((r.streams_dir, r.clips_dir, r.extra_dirs), ("X", "", ("A", "B")))
        r = lib.roots_from_config(Cfg({}))
        self.assertEqual((r.streams_dir, r.extra_dirs), ("", ()))


if __name__ == "__main__":
    unittest.main()