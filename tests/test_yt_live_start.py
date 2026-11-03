"""YouTube record-from-start: args, plan, files, catch-up, monitor, worker, settings.

Temp folders only; subprocess, network and time are faked. No real downloads.
"""
import contextlib
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
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import msr.worker as worker_mod  # noqa: E402
from msr import control, platforms, yt_live_start as ls  # noqa: E402
from msr import options_panel as op  # noqa: E402
from msr.config import Config, new_parser, _coerce_specs  # noqa: E402

LOG = logging.getLogger("test-yt-live-start")
LOG.addHandler(logging.NullHandler())
LOG.propagate = False


def cfg(**adv):
    p = new_parser()
    p.read_dict({s: dict(v) for s, v in Config.DEFAULT_CONFIG.items()})
    for k, v in adv.items():
        p.set("Advanced", k, v)
    return p


def touch(path, size=0, mtime=None):
    with open(path, "wb") as f:
        f.write(b"\0" * size)
    if mtime:
        os.utime(path, (mtime, mtime))
    return path


class TmpCase(unittest.TestCase):
    def setUp(self):
        t = tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.tmp = t.name


class ArgsTests(unittest.TestCase):
    def test_from_start_args(self):
        cmd = platforms.build_recording_command_ytdlp_from_start(
            "https://youtube.com/watch?v=abcdefghijk", os.path.join("R", "chan_2026"), cfg(), False, False,
            cookies_file="c.txt")
        self.assertEqual(cmd[:2], ["yt-dlp", "https://youtube.com/watch?v=abcdefghijk"])
        self.assertIn("--live-from-start", cmd)
        self.assertEqual(cmd[cmd.index("-o") + 1], os.path.join("R", "chan_2026") + ".%(ext)s")
        self.assertEqual(cmd[cmd.index("--merge-output-format") + 1], "mp4")
        self.assertEqual(cmd[cmd.index("--cookies") + 1], "c.txt")
        # yt-dlp's own DASH downloader: no external ffmpeg downloader, no mpegts
        self.assertNotIn("--downloader", cmd)
        self.assertNotIn("--hls-use-mpegts", cmd)
        self.assertNotIn("--verbose", cmd)

    def test_from_start_args_escape_percent_and_player_client(self):
        cmd = platforms.build_recording_command_ytdlp_from_start(
            "u", "R/100%_title", cfg(youtube_player_client="tv"), True, False)
        self.assertEqual(cmd[cmd.index("-o") + 1], "R/100%%_title.%(ext)s")
        self.assertIn("youtube:player_client=tv", cmd)
        self.assertIn("--verbose", cmd)

    def test_normal_live_args_unchanged(self):
        cmd = platforms.build_recording_command_ytdlp("https://youtube.com/@x/live", "out.ts", cfg(), False, False)
        self.assertNotIn("--live-from-start", cmd)
        self.assertIn("--hls-use-mpegts", cmd)
        self.assertEqual(cmd[cmd.index("-o") + 1], "out.ts")


class CheckInfoTests(unittest.TestCase):
    def test_check_fills_video_id_and_start_time(self):
        data = {"id": "abcdefghijk", "is_live": True, "live_status": "is_live", "title": "t",
                "release_timestamp": 1700000000, "timestamp": 1699999000, "formats": [],
                "webpage_url": "https://www.youtube.com/watch?v=abcdefghijk"}
        done = mock.Mock(returncode=0, stdout=json.dumps(data), stderr="")
        info = {}
        with mock.patch.object(platforms.subprocess, "run", return_value=done), \
                mock.patch.object(platforms, "HAS_YTDLP", True):
            live, title, err, *_ = platforms.check_stream_ytdlp("https://youtube.com/@x/live", LOG, info=info)
        self.assertTrue(live)
        self.assertIsNone(err)
        self.assertEqual(info["video_id"], "abcdefghijk")
        self.assertEqual(info["release_timestamp"], 1700000000)
        self.assertEqual(info["live_status"], "is_live")


class PlanTests(TmpCase):
    NOW = 1_800_000_000

    def plan(self, info=None, enabled=True, max_hours=12, cont=False, platform="youtube"):
        info = {"video_id": "abcdefghijk", "release_timestamp": self.NOW - 600} if info is None else info
        return ls.plan(enabled, max_hours, platform, info, cont, base_dir=self.tmp, now=self.NOW)

    def test_first_segment_uses_from_start(self):
        use, why = self.plan()
        self.assertTrue(use)
        self.assertIn("10:00", why)

    def test_reconnect_continues_live(self):
        self.assertEqual(self.plan(cont=True), (False, "reconnect: continuing live"))

    def test_same_broadcast_once_even_after_restart(self):
        ls.mark_seen("abcdefghijk", self.tmp, now=self.NOW)
        self.assertFalse(self.plan()[0])
        self.assertTrue(os.path.exists(os.path.join(self.tmp, ls.SEEN_DIR, "abcdefghijk")))

    def test_long_broadcast_falls_back(self):
        use, why = self.plan({"video_id": "abcdefghijk", "release_timestamp": self.NOW - 13 * 3600})
        self.assertFalse(use)
        self.assertIn("limit 12 h", why)
        self.assertTrue(self.plan({"video_id": "abcdefghijk", "release_timestamp": self.NOW - 13 * 3600},
                                  max_hours=0)[0], "0 = no limit")

    def test_unknown_start_or_id_or_off_or_other_site(self):
        self.assertFalse(self.plan({"video_id": "abcdefghijk"})[0])
        self.assertFalse(self.plan({"release_timestamp": self.NOW})[0])
        self.assertFalse(self.plan(enabled=False)[0])
        self.assertFalse(self.plan(platform="twitch")[0])
        # timestamp is used when release_timestamp is missing
        self.assertTrue(self.plan({"video_id": "abcdefghijk", "timestamp": self.NOW - 60})[0])

    def test_seen_markers_expire_and_bad_ids_are_ignored(self):
        ls.mark_seen("old_video_1", self.tmp, now=self.NOW - ls.SEEN_KEEP_SECONDS - 10)
        self.assertFalse(ls.was_seen("old_video_1", self.tmp, now=self.NOW))
        ls.mark_seen("new_video_1", self.tmp, now=self.NOW)   # prunes the old one
        self.assertFalse(os.path.exists(os.path.join(self.tmp, ls.SEEN_DIR, "old_video_1")))
        ls.mark_seen("../evil", self.tmp, now=self.NOW)
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "evil")))

    def test_settings_defaults_and_bad_values(self):
        self.assertEqual(ls.settings(cfg()), (True, 12.0))
        p = cfg()
        p.set("YouTube", "live_from_start", "maybe")
        p.set("YouTube", "live_from_start_max_hours", "x")
        self.assertEqual(ls.settings(p), (True, 12.0))
        p.set("YouTube", "live_from_start", "false")
        p.set("YouTube", "live_from_start_max_hours", "3")
        self.assertEqual(ls.settings(p), (False, 3.0))


class CatchUpTests(unittest.TestCase):
    def test_reaching_the_live_edge_by_start_time(self):
        c = ls.CatchUp(started_at=1000.0)
        self.assertFalse(c.update(100, now=1600))
        self.assertIn("catching up from start (1:40 of 10:00)", c.detail(now=1600))
        self.assertTrue(c.update(560, now=1600))
        self.assertEqual(c.detail(), "live (recorded from start)")
        self.assertTrue(c.update(0, now=9999), "never goes back")

    def test_real_time_growth_means_caught_up(self):
        c = ls.CatchUp(started_at=None)
        c.update(10, now=0)
        c.update(400, now=30)       # 13x: catching up
        self.assertFalse(c.caught_up)
        c.update(430, now=60)       # ~1x
        self.assertFalse(c.caught_up, "one slow sample is not enough")
        c.update(460, now=90)
        self.assertTrue(c.caught_up)

    def test_close_samples_and_missing_probe_are_ignored(self):
        c = ls.CatchUp()
        c.update(1, now=0)
        c.update(2, now=5)
        c.update(None, now=50)
        c.update(3, now=10)
        self.assertFalse(c.caught_up)


class FilesTests(TmpCase):
    def test_parts_sizes_and_active_file(self):
        stem = os.path.join(self.tmp, "chan_2026")
        self.assertFalse(ls.present(stem))
        v = touch(stem + ".f299.mp4", 3000)
        a = touch(stem + ".f140.mp4", 1000)
        touch(stem + ".f299.mp4.ytdl", 10)
        touch(stem + ".f299.mp4-Frag7", 5)
        touch(os.path.join(self.tmp, "chan_2026.final.mp4"), 99)      # not ours
        touch(os.path.join(self.tmp, "chan_2026.part2.ts"), 99)        # a reconnect part
        touch(os.path.join(self.tmp, "chan_2026x.f299.mp4"), 99)       # another recording
        self.assertTrue(ls.present(stem))
        self.assertEqual(ls.media_parts(stem), [v, a])
        self.assertEqual(ls.total_size(stem), 4015)
        self.assertEqual(ls.active_file(stem), v)
        t = touch(stem + ".temp.mp4", 1)
        self.assertEqual(ls.active_file(stem), t, "yt-dlp's merge output while it merges")

    def fake_ffmpeg(self, ok=True):
        calls = []

        def run(cmd, **kw):
            calls.append(cmd)
            if ok:
                touch(cmd[-1], 500)
            return mock.Mock(returncode=0 if ok else 1, stderr="boom")
        return calls, mock.patch.object(ls.subprocess, "run", side_effect=run)

    def test_finalize_merges_parts_after_a_stop(self):
        stem = os.path.join(self.tmp, "c")
        touch(stem + ".f299.mp4", 3000)
        touch(stem + ".f140.mp4", 1000)
        touch(stem + ".f299.mp4.ytdl", 10)
        calls, p = self.fake_ffmpeg()
        with p:
            out = ls.finalize(stem, "ffmpeg", LOG)
        self.assertEqual(out, stem + ".mp4")
        cmd = calls[0]
        self.assertEqual(cmd.count("-i"), 2)
        self.assertIn("copy", cmd)
        self.assertIn("-shortest", cmd)
        self.assertEqual(sorted(os.listdir(self.tmp)), ["c.mp4"])

    def test_finalize_keeps_yt_dlp_merge_and_keeps_parts_on_failure(self):
        stem = os.path.join(self.tmp, "c")
        touch(stem + ".mp4", 100)
        touch(stem + ".f299.mp4.ytdl", 10)
        calls, p = self.fake_ffmpeg()
        with p:
            self.assertEqual(ls.finalize(stem, "ffmpeg", LOG), stem + ".mp4")
        self.assertEqual(calls, [], "yt-dlp already merged")
        self.assertEqual(os.listdir(self.tmp), ["c.mp4"])
        stem2 = os.path.join(self.tmp, "d")
        touch(stem2 + ".f299.mp4", 3000)
        calls, p = self.fake_ffmpeg(ok=False)
        with p:
            self.assertIsNone(ls.finalize(stem2, "ffmpeg", LOG))
        self.assertTrue(os.path.exists(stem2 + ".f299.mp4"), "parts kept for the next start")
        self.assertFalse(os.path.exists(stem2 + ".merge.mp4"))

    def test_leftovers_become_a_ts_unless_recent(self):
        now = time.time()
        touch(os.path.join(self.tmp, "a.f299.mp4"), 3000, mtime=now - 600)
        touch(os.path.join(self.tmp, "a.f140.mp4"), 1000, mtime=now - 600)
        touch(os.path.join(self.tmp, "b.f299.mp4"), 3000, mtime=now)        # still being written
        touch(os.path.join(self.tmp, "keep.final.mp4"), 3000, mtime=now - 600)
        calls, p = self.fake_ffmpeg()
        with p:
            out = ls.merge_leftovers(self.tmp, "ffmpeg", LOG, now=now)
        self.assertEqual(out, [os.path.join(self.tmp, "a.ts")])
        self.assertIn("mpegts", calls[0])
        self.assertEqual(sorted(os.listdir(self.tmp)), ["a.ts", "b.f299.mp4", "keep.final.mp4"])


class FakeProc:
    def __init__(self):
        self.pid = 4242
        self.returncode = None
        self.stderr = io.StringIO("")

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode


class FakeClock:
    def __init__(self, on_tick):
        self.t = 1000.0
        self.on_tick = on_tick

    def monotonic(self):
        return self.t

    def time(self):
        return self.t

    def sleep(self, s):
        self.t += s
        self.on_tick(self.t)


class MonitorTests(TmpCase):
    def run_monitor(self, caught_up_at, max_gb=1e-6, max_hours=0, end_at=400):
        stem = os.path.join(self.tmp, "c")
        fs = ls.LiveFromStart(stem, "abcdefghijk", started_at=None)
        proc = FakeProc()
        statuses = queue.Queue()
        splits = []

        def tick(t):
            touch(stem + ".f299.mp4", int((t - 1000) * 1000) + 1000)   # grows every tick
            touch(stem + ".f140.mp4", 100)
            if t >= caught_up_at:
                fs.catchup.caught_up = True
            if t >= 1000 + end_at:
                proc.returncode = 0

        clock = FakeClock(tick)
        tick(clock.t)

        def on_limit():
            splits.append(clock.t)
            return True

        with mock.patch.object(worker_mod, "time", clock), \
                mock.patch.object(worker_mod, "_start_catchup_thread") as cu, \
                mock.patch.object(worker_mod, "kill_process_tree", side_effect=lambda *a: setattr(proc, "returncode", 1)):
            _last, split = worker_mod.monitor_recording_process(
                proc, fs.merged_file, clock.t, max_hours, "youtube", LOG, statuses, "youtube:@x",
                threading.Event(), None, 120, max_file_size_gb=max_gb, ffmpeg_path="ffmpeg",
                watch_resolution_changes=True, on_size_limit=on_limit, from_start=fs)
        cu.assert_called_once()
        out = []
        while not statuses.empty():
            out.append(statuses.get()[1])
        return split, splits, out, stem

    def test_no_split_while_catching_up_then_split_at_live_edge(self):
        split, splits, out, stem = self.run_monitor(caught_up_at=1100)
        self.assertTrue(split)
        self.assertEqual(len(splits), 1)
        self.assertGreaterEqual(splits[0], 1100, "size split waited for the live edge")
        self.assertTrue(any(s["detail"].startswith("catching up from start") for s in out))
        self.assertTrue(all(s["raw_file"] == stem + ".f299.mp4" for s in out), "health watches a growing file")

    def test_time_limit_also_waits(self):
        split, splits, _out, _ = self.run_monitor(caught_up_at=1200, max_gb=0, max_hours=0.01)
        self.assertTrue(split)
        self.assertGreaterEqual(splits[0], 1200)

    def test_no_stall_kill_and_no_file_timeout_during_catch_up(self):
        split, splits, out, _ = self.run_monitor(caught_up_at=10**9, max_gb=0, end_at=1300)
        self.assertFalse(split)
        self.assertEqual(splits, [])
        self.assertIn("catching up", out[-1]["detail"])


class WorkerHarness:
    """record_worker for a YouTube channel with the check, capture, merge and remux faked."""

    def __init__(self, test, root, from_start_files=True, release_age=300, enabled="true"):
        self.test = test
        self.stop = threading.Event()
        self.status = queue.Queue()
        self.cmds = []
        self.records = []
        self.log = logging.getLogger(f"test-yt-worker-{id(self)}")
        self.log.setLevel(logging.INFO)
        self.log.propagate = False
        h = logging.Handler()
        h.emit = lambda r: self.records.append(r.getMessage())
        self.log.addHandler(h)
        self.config = {s: dict(v) for s, v in Config.DEFAULT_CONFIG.items()}
        self.config["Paths"].update(streams_dir=root, fallback_dir="off")
        self.config["Recording"].update(min_disk_space_gb="0", min_file_size_mb="0")
        self.config["YouTube"]["live_from_start"] = enabled
        self.from_start_files = from_start_files
        self.release_age = release_age
        self.max_captures = 2

    def check(self, url, logger, timeout=30, cookies_file=None, info=None):
        if info is not None:
            info.update(video_id="abcdefghijk", release_timestamp=time.time() - self.release_age,
                        live_status="is_live")
        return True, "title", None, False, "https://www.youtube.com/watch?v=abcdefghijk", None

    def popen(self, cmd, *a, **k):
        self.cmds.append(list(cmd))
        if len(self.cmds) >= self.max_captures:
            self.stop.set()
        proc = FakeProc()
        proc.returncode = 0
        if "--live-from-start" in cmd:
            stem = cmd[cmd.index("-o") + 1][:-len(".%(ext)s")]
            if self.from_start_files:
                touch(stem + ".f299.mp4", 200_000)
                touch(stem + ".f140.mp4", 50_000)
        else:
            out = cmd[cmd.index("-o") + 1] if "-o" in cmd else cmd[-1]
            touch(out, 200_000)
        return proc

    def finalize(self, stem, ffmpeg_path, logger, timeout=3600):
        parts = ls.media_parts(stem)
        if not parts:
            return None
        for p in parts:
            os.remove(p)
        return touch(stem + ".mp4", 300_000)

    def run(self):
        args = ("youtube:@chan", self.config, self.stop, self.status, threading.Event(), {})
        patches = [
            mock.patch.object(worker_mod, "setup_child_logging", return_value=self.log),
            mock.patch.object(worker_mod, "check_stream_ytdlp", self.check),
            mock.patch.object(worker_mod.subprocess, "Popen", self.popen),
            mock.patch.object(worker_mod, "monitor_recording_process",
                              lambda proc, raw, *a, **k: (None, False)),
            mock.patch.object(worker_mod.yt_live_start, "finalize", self.finalize),
            mock.patch.object(worker_mod, "process_finished_raw", return_value=(True, 300_000, None)),
            mock.patch.object(worker_mod, "finalize_recording_group"),
            mock.patch.object(worker_mod, "kill_process_tree"),
            mock.patch.object(worker_mod, "interruptible_sleep", lambda *a: False),
            mock.patch.object(worker_mod.time, "sleep", lambda s: None),
            mock.patch.object(worker_mod, "find_cookies_file", return_value=None),
            mock.patch.object(worker_mod.random, "uniform", return_value=0.0),
        ]
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            t = threading.Thread(target=worker_mod.record_worker, args=(args,), daemon=True)
            t.start()
            t.join(30)
            self.test.assertFalse(t.is_alive(), "the worker did not stop")
        return self


class WorkerTests(TmpCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.tmp)       # the seen-markers live in the data dir (cwd)
        self.root = os.path.join(self.tmp, "streams")

    def test_first_segment_from_start_then_live(self):
        h = WorkerHarness(self, self.root).run()
        self.assertIn("--live-from-start", h.cmds[0])
        self.assertNotIn("--live-from-start", h.cmds[1], "same broadcast again: live")
        self.assertTrue(any("Recording from the start" in m for m in h.records), h.records)
        self.assertTrue(os.path.exists(os.path.join(self.tmp, ls.SEEN_DIR, "abcdefghijk")))

    def test_no_dvr_falls_back_to_live_at_once(self):
        h = WorkerHarness(self, self.root, from_start_files=False).run()
        self.assertIn("--live-from-start", h.cmds[0])
        self.assertNotIn("--live-from-start", h.cmds[1])
        self.assertTrue(any("got no data" in m for m in h.records), h.records)
        self.assertFalse(any("never created" in m for m in h.records))

    def test_restart_mid_broadcast_does_not_start_over(self):
        ls.mark_seen("abcdefghijk")
        h = WorkerHarness(self, self.root)
        h.max_captures = 1
        h.run()
        self.assertNotIn("--live-from-start", h.cmds[0])

    def test_off_or_old_broadcast_records_live(self):
        h = WorkerHarness(self, self.root, enabled="false")
        h.max_captures = 1
        h.run()
        self.assertNotIn("--live-from-start", h.cmds[0])
        h = WorkerHarness(self, self.root, release_age=13 * 3600)
        h.max_captures = 1
        h.run()
        self.assertNotIn("--live-from-start", h.cmds[0])
        self.assertTrue(any("limit 12 h" in m for m in h.records), h.records)


class SettingsTests(unittest.TestCase):
    def test_defaults_and_validation(self):
        self.assertEqual(Config.DEFAULT_CONFIG["YouTube"],
                         {"live_from_start": "true", "live_from_start_max_hours": "12"})
        specs = {(s, k): kind for s, k, kind, *_ in _coerce_specs()}
        self.assertEqual(specs[("YouTube", "live_from_start")], "bool")
        self.assertEqual(control.validate_setting("YouTube", "live_from_start", "off"), "false")
        self.assertEqual(control.validate_setting("YouTube", "live_from_start_max_hours", "6"), "6.0")
        with self.assertRaises(control.ControlError):
            control.validate_setting("YouTube", "live_from_start_max_hours", "-1")
        with self.assertRaises(control.ControlError):
            control.validate_setting("YouTube", "live_from_start", "maybe")

    def test_config_set_and_get_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            cwd = os.getcwd()
            os.chdir(d)
            try:
                c = Config(os.path.join(d, "config.ini"))
                ctl = control.Controller(control.OfflineApp(c))
                r = ctl.handle("config_set", {"section": "YouTube", "key": "live_from_start", "value": "off"})
                self.assertTrue(r["ok"], r)
                self.assertEqual(r["result"]["value"], "false")
                r = ctl.handle("config_set", {"section": "YouTube", "key": "live_from_start_max_hours",
                                              "value": "4"})
                self.assertTrue(r["ok"], r)
                got = ctl.handle("config_get", {"section": "YouTube"})["result"]["settings"]["YouTube"]
                self.assertEqual(got, {"live_from_start": "false", "live_from_start_max_hours": "4.0"})
                self.assertEqual(ls.settings(Config(os.path.join(d, "config.ini")).config), (False, 4.0))
            finally:
                os.chdir(cwd)

    def test_options_rows(self):
        rows = {(r["section"], r["key"]): r for r in op.all_rows()}
        r = rows[("YouTube", "live_from_start")]
        self.assertEqual(r["kind"], "bool")
        self.assertTrue(r["hint"])
        self.assertEqual(rows[("YouTube", "live_from_start_max_hours")]["hint"], "0 = no limit")


class OptionsPanelTests(unittest.TestCase):
    def test_toggle_saves_and_shows_hint(self):
        import tkinter as tk
        from tests.test_options_panel import FakeApi, FakeConfig
        try:
            root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        root.withdraw()
        self.addCleanup(root.destroy)
        cfgf = FakeConfig()
        saved = []

        def apply(s, k, v):
            v = control.validate_setting(s, k, v)
            cfgf.v[(s, k)] = v
            saved.append((s, k, v))
            return v
        p = op.OptionsPanel(root, cfgf, ".", apply, api=FakeApi(), environ={},
                            ask_dir=lambda i: "", ask_file=lambda i: "")
        v = p.vars[("YouTube", "live_from_start")]
        self.assertTrue(v.get(), "on by default")
        self.assertIn("before MSR noticed", p.hints[("YouTube", "live_from_start")].cget("text"))
        v.set(False)
        p._commit("YouTube", "live_from_start", "false")
        self.assertIn(("YouTube", "live_from_start", "false"), saved)


if __name__ == "__main__":
    unittest.main()
