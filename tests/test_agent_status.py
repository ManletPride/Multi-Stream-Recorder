"""Agent control, second round: health, finished recordings, events/wait, the cleanup queue,
headless presence, library suggest/log/restore --last, enable --all, the url field and the
headless in-app update (a fake release; GitHub is never contacted).

Temp data folders only; the fake recorder never starts a process.
"""

import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import zipfile
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import msr.control as control  # noqa: E402
from msr import agent_status  # noqa: E402
from msr.config import Config  # noqa: E402
from tests.test_control import (SECRET_EMAIL, SECRET_PASSWORD, Headless, in_dir,  # noqa: E402
                                make_data, msr_ctl, roster)

COOKIE_VALUE = "cookie-value-must-not-leak"
_tools = mock.patch.multiple("msr.util", HAS_STREAMLINK=True, HAS_YTDLP=True)


def setUpModule():
    _tools.start()


def tearDownModule():
    _tools.stop()


def touch(path, size=10, age_days=0.0, duration=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(b"x" * size)
    if duration is not None:
        with open(path.rsplit(".", 1)[0] + ".meta.json", "w", encoding="utf-8") as fh:
            json.dump({"duration_seconds": duration}, fh)
    if age_days:
        t = time.time() - age_days * 86400
        os.utime(path, (t, t))
    return path


class TrackerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, self.streams = make_data(self.tmp.name, ["knut"])
        self.config = Config(os.path.join(self.data, "config.ini"))
        self.t = agent_status.Tracker(self.config)

    def tearDown(self):
        self.tmp.cleanup()

    def test_live_finished_error_and_restart_history(self):
        self.t.seen("knut", {"status": "Checking..."})
        self.t.seen("knut", {"status": "Recording", "time": "0:01"})
        self.t.seen("knut", {"status": "Recording", "time": "0:02"})  # same session: one live event
        mp4 = touch(os.path.join(self.streams, "Processed", "kick", "knut", "knut_1.mp4"), 2048, duration=61.5)
        self.t.seen("knut", {"status": "Completed", "detail": "2 KB"})
        self.t.seen("knut", {"status": "Completed", "detail": "2 KB"})  # repeated: not a 2nd recording
        self.t.seen("knut", {"status": "Error", "detail": "worker crashed, restart in 5s"})
        raw = touch(os.path.join(self.streams, "Recorded", "kick", "knut", "knut_2.ts"), 99)
        self.t.seen("knut", {"status": "Remux failed", "detail": "ffmpeg exit 1"})
        kinds = [e["event"] for e in self.t.events()]
        self.assertEqual(kinds, ["live", "finished", "error", "error", "finished"])
        ok, bad = self.t.finished
        self.assertEqual((ok["path"], ok["size"], ok["duration_seconds"], ok["remux_ok"]), (mp4, 2048, 61.5, True))
        self.assertEqual((bad["path"], bad["remux_ok"], bad["error"]), (raw, False, "ffmpeg exit 1"))
        info = self.t.channels["knut"]
        self.assertEqual((info["errors"], info["restarts"]), (2, 1))
        self.assertIn("ffmpeg exit 1", info["last_error"])
        self.assertIsNotNone(info["last_recorded"])
        self.assertEqual(self.t.recent_finished()[0]["path"], raw, "newest first")

    def test_presence_goes_live_once(self):
        self.t.seen_presence({"twitch:two": {"live": True}})
        self.t.seen_presence({"twitch:two": {"live": True}})
        self.t.seen_presence({"twitch:two": {"live": False}})
        self.t.seen_presence({"twitch:two": {"live": True}})
        self.assertEqual([e["event"] for e in self.t.events()], ["live", "live"])

    def test_wait_returns_the_next_matching_event_or_none(self):
        self.t.emit("error", "other")
        threading.Timer(0.2, lambda: self.t.emit("live", "knut")).start()
        ev = self.t.wait({"live"}, "knut", timeout=5)
        self.assertEqual((ev["event"], ev["channel"]), ("live", "knut"))
        started = time.monotonic()
        self.assertIsNone(self.t.wait({"finished"}, None, timeout=0.3))
        self.assertGreaterEqual(time.monotonic() - started, 0.25)
        # "after" replays what happened between two waits
        self.assertEqual(self.t.wait({"error"}, None, timeout=0, after=0)["channel"], "other")

    def test_parse_since(self):
        now = 1_000_000.0
        self.assertEqual(agent_status.parse_since("24h", now), now - 86400)
        self.assertEqual(agent_status.parse_since("7d", now), now - 7 * 86400)
        self.assertEqual(agent_status.parse_since("", now), 0.0)
        self.assertGreater(agent_status.parse_since("2026-10-29"), 0)
        with self.assertRaises(ValueError):
            agent_status.parse_since("yesterday")


class OfflineAgentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, self.streams = make_data(self.tmp.name, ["knut", "twitch:two"])
        self._cd = in_dir(self.data)
        self._cd.__enter__()
        self.config = Config(os.path.join(self.data, "config.ini"))
        self.c = control.Controller(control.OfflineApp(self.config))

    def tearDown(self):
        self._cd.__exit__(None, None, None)
        self.tmp.cleanup()

    def ok(self, cmd, args=None):
        reply = self.c.handle(cmd, args)
        self.assertTrue(reply["ok"], reply)
        return reply["result"]

    def test_health_reports_disk_cookies_fishtank_without_secrets(self):
        with open(os.path.join(self.streams, "cookies.txt"), "w", encoding="utf-8") as fh:
            fh.write("# Netscape HTTP Cookie File\n")
            fh.write(f".twitch.tv\tTRUE\t/\tTRUE\t1000\tauth-token\t{COOKIE_VALUE}\n")
            fh.write(f".kick.com\tTRUE\t/\tTRUE\t{int(time.time()) + 86400 * 30}\tsession_token\t{COOKIE_VALUE}\n")
        h = self.ok("health")
        self.assertEqual(h["disk"]["main"]["path"], self.streams)
        self.assertIsNotNone(h["disk"]["main"]["free_gb"])
        self.assertEqual(h["disk"]["main"]["min_gb"], 5.0)
        self.assertIn("fallback", h["disk"])
        self.assertTrue(h["cookies"]["found"])
        self.assertTrue(h["cookies"]["auth_expired"])
        self.assertIn("twitch.tv", h["cookies"]["expired_domains"])
        self.assertTrue(any("expired" in p for p in h["problems"]))
        self.assertEqual(h["fishtank"], {"login_set": True, "email_set": True})
        self.assertIn("ffmpeg", h["tools"])
        text = json.dumps(h)
        for secret in (COOKIE_VALUE, SECRET_PASSWORD, SECRET_EMAIL):
            self.assertNotIn(secret, text)

    def test_recordings_since_channel_and_pending(self):
        new = touch(os.path.join(self.streams, "Processed", "kick", "knut", "new.mp4"), 3000, duration=12.0)
        touch(os.path.join(self.streams, "Processed", "kick", "knut", "old.mp4"), 3000, age_days=10)
        touch(os.path.join(self.streams, "Processed", "twitch", "two", "t.mp4"), 3000)
        res = self.ok("recordings", {"since": "2d", "channel": "knut"})
        self.assertEqual([r["path"] for r in res["recordings"]], [new])
        self.assertEqual(res["recordings"][0]["duration_seconds"], 12.0)
        self.assertEqual(self.ok("recordings")["count"], 3)
        self.assertEqual(self.c.handle("recordings", {"since": "soon"})["code"], "bad_request")

        raw = touch(os.path.join(self.streams, "Recorded", "kick", "knut", "left.ts"), 500)
        gone = touch(os.path.join(self.streams, "PendingDeletion", "kick", "knut", "gone.mp4"), 700)
        pend = self.ok("recordings_pending")
        self.assertEqual([r["path"] for r in pend["raw_waiting"]], [raw])
        self.assertFalse(pend["raw_waiting"][0]["recording_now"])
        self.assertEqual([r["path"] for r in pend["pending_deletion"]], [gone])

    def test_retry_runs_the_cleanup_pass(self):
        from msr.recorder import BackgroundCleaner
        with mock.patch.object(BackgroundCleaner, "_process_leftover_files", return_value=False) as run:
            self.assertEqual(self.ok("recordings_retry"), {"done": True})
        run.assert_called_once()

    def test_enable_disable_all_and_url(self):
        self.ok("channel_disable", {"all": True})
        self.assertTrue(all(not r["enabled"] for r in roster(self.data)))
        self.ok("channel_enable", {"all": True})
        self.assertTrue(all(r["enabled"] for r in roster(self.data)))
        rows = {r["name"]: r for r in self.ok("channels")["channels"]}
        self.assertEqual(rows["knut"]["url"], "https://kick.com/knut")
        self.assertEqual(rows["twitch:two"]["url"], "https://twitch.tv/two")

    def test_library_suggest_log_and_restore_last(self):
        old = touch(os.path.join(self.streams, "Processed", "kick", "knut", "a.mp4"), 4000, age_days=200)
        touch(os.path.join(self.streams, "Processed", "kick", "knut", "b.mp4"), 4000)
        sug = {s["key"]: s for s in self.ok("library_suggest", {"older_days": 100, "free_gb": 1e-6})["suggestions"]}
        self.assertEqual(sug["older"]["files"], [old])
        self.assertEqual(sug["free"]["files"], [old], "oldest first, just enough")
        self.assertTrue(os.path.exists(old), "suggest is read only")

        self.ok("library_execute", {"action": "pending", "paths": [old], "confirm": True})
        self.assertFalse(os.path.exists(old))
        self.assertTrue(self.ok("library_log", {"lines": 5})["lines"])
        self.assertEqual(self.c.handle("library_restore", {"last": True})["code"], "confirm_required")
        self.ok("library_restore", {"last": True, "confirm": True})
        self.assertTrue(os.path.exists(old))

    def test_cli_enable_all_and_wait_offline(self):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch("sys.stdout", out), mock.patch("sys.stderr", err):
            self.assertEqual(msr_ctl.main(["--data-dir", self.data, "disable", "--all"]), 0)
            self.assertEqual(msr_ctl.main(["--data-dir", self.data, "wait", "--timeout", "1"]),
                             msr_ctl.EXIT_NOT_RUNNING)
        self.assertFalse(any(r["enabled"] for r in roster(self.data)))


class LiveAgentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, self.streams = make_data(self.tmp.name, [{"name": "knut", "enabled": True},
                                                            {"name": "twitch:two", "enabled": False}])
        with open(os.path.join(self.data, "config.ini"), "a", encoding="utf-8") as fh:
            fh.write("[Control]\nstatus_interval_seconds = 0.5\n")
        self.h = Headless(self.data, start_now=True)

    def tearDown(self):
        self.h.close()
        self.tmp.cleanup()

    def test_status_file_has_health_finished_queue_and_channel_history(self):
        self.assertTrue(self.h.wait(lambda: self.h.recorders and self.h.recorders[0].is_running))
        path = os.path.join(self.data, control.STATUS_FILE)

        def status():
            try:
                with open(path, encoding="utf-8") as fh:
                    return json.load(fh)
            except (OSError, ValueError):
                return {}

        self.assertTrue(self.h.wait(lambda: status().get("health") and any(
            r.get("last_recorded") for r in status().get("channels", [])), timeout=10))
        st = status()
        self.assertIn("problems", st["health"])
        self.assertEqual(st["finished"], [])
        self.assertIsInstance(st["cleanup_queue"], list)
        row = st["channels"][0]
        for field in ("url", "last_error", "last_error_at", "errors", "restarts", "last_live", "last_recorded"):
            self.assertIn(field, row)
        self.assertNotIn(SECRET_PASSWORD, json.dumps(st))

    def test_wait_over_the_socket_and_cli_timeout_exit_code(self):
        reply = self.h.call("wait", {"events": ["live"], "channel": "knut", "timeout": 10, "after": 0})
        self.assertTrue(reply["ok"], reply)
        self.assertEqual(reply["result"]["channel"], "knut")
        out, err = io.StringIO(), io.StringIO()
        with mock.patch("sys.stdout", out), mock.patch("sys.stderr", err):
            code = msr_ctl.main(["--data-dir", self.data, "wait", "--event", "finished", "--timeout", "0.5"])
        self.assertEqual(code, 6, err.getvalue())
        self.assertEqual(self.h.call("wait", {"events": ["nope"]})["code"], "bad_request")
        self.assertTrue(self.h.call("events")["result"]["events"])

    def test_presence_headless_marks_disabled_channels_live(self):
        from msr.presence import PresencePoller
        poller = self.h.app.start_presence(lambda store, get: PresencePoller(
            store, get, pollers={"twitch": lambda name: {"live": True, "viewers": 3}}, first_delay=3600))
        try:
            self.assertEqual(poller.poll_round(), ["twitch:two"])
        finally:
            poller.stop()
        rows = {r["name"]: r for r in self.h.call("status")["result"]["channels"]}
        self.assertTrue(rows["twitch:two"]["live"])
        self.assertFalse(rows["twitch:two"]["recording"])

    def test_roster_change_updates_the_watch_list(self):
        self.assertTrue(self.h.wait(lambda: self.h.recorders and self.h.recorders[0].is_running))
        rec = self.h.recorders[0]
        rec.set_watch_channels = mock.MagicMock()
        self.h.call("channel_add", {"name": "twitch:three", "disabled": True})
        rec.set_watch_channels.assert_called_with(["twitch:two", "twitch:three"])


class HeadlessUpdateTests(unittest.TestCase):
    """update install --yes headless: fake release zip, fake download, the helper is not started."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, _ = make_data(self.tmp.name, ["knut"])
        self.target = os.path.join(self.tmp.name, "program")
        os.makedirs(os.path.join(self.target, "msr"))
        self.zip = os.path.join(self.tmp.name, "fake-release.zip")
        with zipfile.ZipFile(self.zip, "w") as z:
            z.writestr("owner-repo-abc/Multi-Stream-Recorder.py", "print('v9')\n")
            z.writestr("owner-repo-abc/msr/__init__.py", "__version__ = '9.9.9'\n")
        self.h = Headless(self.data, start_now=True)

    def tearDown(self):
        self.h.close()
        self.tmp.cleanup()

    def test_install_quits_and_relaunches_headless_with_the_same_data_dir(self):
        import shutil
        app = self.h.app
        app.relaunch_args = ["--headless", "--data-dir", self.data, "--config",
                             os.path.join(self.data, "config.ini")]
        app.update_state = {"tag": "9.9.9", "url": "https://github.com/x/y/releases/tag/v9.9.9",
                            "zipball": "https://api.github.com/repos/x/y/zipball/v9.9.9"}
        self.assertTrue(self.h.wait(lambda: self.h.recorders and self.h.recorders[0].is_running))
        self.assertEqual(self.h.call("update_install")["code"], "confirm_required")
        with mock.patch("msr.update.download_zipball", side_effect=lambda url, dest: shutil.copy(self.zip, dest)) \
                as dl, mock.patch("msr.util.application_dir", return_value=self.target), \
                mock.patch("msr.update.spawn_updater") as spawn:
            reply = self.h.call("update_install", {"confirm": True})
            self.assertTrue(reply["ok"], reply)
            self.assertEqual(reply["result"]["relaunch"][:2], ["headless", "--headless"])
            self.assertTrue(self.h.wait(lambda: not self.h.thread.is_alive()), "the app quits")
            self.assertIsNotNone(app.after_children)
            app.after_children()   # what main_headless does after the final cleanup
        dl.assert_called_once()
        staging = spawn.call_args.args[0]
        self.assertTrue(os.path.isfile(os.path.join(staging, "Multi-Stream-Recorder.py")))
        self.assertEqual(spawn.call_args.kwargs["target_dir"], self.target)
        self.assertEqual(spawn.call_args.kwargs["launcher_args"], app.relaunch_args)
        self.assertIn("stop", self.h.recorders[0].calls)

    def test_spawn_and_helper_pass_the_arguments_through(self):
        from msr import apply_update, update
        staging = os.path.join(self.tmp.name, "work", "staging")
        os.makedirs(staging)
        with mock.patch("subprocess.Popen") as popen:
            update.spawn_updater(staging, target_dir=self.target, python_exe="py", launcher="L",
                                 pid=1, launcher_args=["--headless", "--data-dir", "D:\\x y"])
        args = popen.call_args.args[0]
        self.assertIn("--arg=--headless", args)
        self.assertIn("--arg=D:\\x y", args)
        with mock.patch.object(apply_update, "wait_for_pid", return_value=True), \
                mock.patch.object(apply_update, "apply_staged_update"), \
                mock.patch.object(apply_update.time, "sleep"), \
                mock.patch.object(apply_update.subprocess, "Popen") as popen:
            code = apply_update.main(["--pid", "1", "--staging", staging, "--target", self.target,
                                      "--python", "py", "--launcher", "L"] + args[args.index("--arg=--headless"):])
        self.assertEqual(code, 0)
        self.assertEqual(popen.call_args.args[0], ["py", "L", "--headless", "--data-dir", "D:\\x y"])


class SmallTextTests(unittest.TestCase):
    def test_headless_warning_mentions_msr_ctl(self):
        import inspect
        from msr import util
        self.assertNotIn("via the GUI", inspect.getsource(util.validate_startup))
        self.assertIn("msr_ctl.py add", inspect.getsource(util.validate_startup))

    def test_update_check_can_be_turned_off(self):
        spec = __import__("importlib").util.spec_from_file_location(
            "msr_main_for_test", os.path.join(ROOT, "Multi-Stream-Recorder.py"))
        main = __import__("importlib").util.module_from_spec(spec)
        spec.loader.exec_module(main)
        app = mock.MagicMock()
        with mock.patch.dict(os.environ, {"MSR_NO_UPDATE_CHECK": "1"}):
            self.assertIsNone(main._schedule_update_check(app))
        with mock.patch.dict(os.environ, {"MSR_NO_UPDATE_CHECK": ""}), \
                mock.patch.object(main, "check_for_updates",
                                  side_effect=lambda v, callback: callback("9.9.9", "u", "z")):
            main._schedule_update_check(app, delay=0).join(5)
        self.assertEqual(app.update_state["tag"], "9.9.9")


class PolishTests(unittest.TestCase):
    """Ctrl+C and timeouts in msr_ctl, record_stop's per-channel result, the immediate first check."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, _ = make_data(self.tmp.name, [{"name": "knut", "enabled": True},
                                                 {"name": "twitch:two", "enabled": True}])
        self.h = Headless(self.data, start_now=False)

    def tearDown(self):
        self.h.close()
        self.tmp.cleanup()

    def test_ctrl_c_during_wait_exits_130_without_traceback(self):
        import _thread
        err = io.StringIO()
        timer = threading.Timer(1.0, _thread.interrupt_main)
        started = time.monotonic()
        with mock.patch("sys.stderr", err), mock.patch("sys.stdout", io.StringIO()):
            timer.start()
            code = msr_ctl.run(["--data-dir", self.data, "wait", "--event", "live", "--timeout", "60"])
        self.assertEqual(code, 130)
        self.assertLess(time.monotonic() - started, 10, "Ctrl+C is seen while blocked in recv")
        self.assertIn("Interrupted.", err.getvalue())
        self.assertNotIn("Traceback", err.getvalue())

    def test_client_timeout_holds_while_blocked_in_recv(self):
        import socket
        started = time.monotonic()
        with self.assertRaises(socket.timeout):
            control.call(self.data, "wait", {"events": ["live"], "timeout": 30}, timeout=1.5)
        self.assertLess(time.monotonic() - started, 4)

    def test_cli_wait_timeout_is_exit_6_and_the_client_outlives_the_server(self):
        seen = {}
        real = control.call

        def spy(data_dir, cmd, args=None, timeout=600.0, token=None):
            seen.setdefault(cmd, timeout)
            return real(data_dir, cmd, args, timeout=timeout, token=token)

        started = time.monotonic()
        with mock.patch.object(msr_ctl.control, "call", side_effect=spy), \
                mock.patch("sys.stderr", io.StringIO()), mock.patch("sys.stdout", io.StringIO()):
            code = msr_ctl.run(["--data-dir", self.data, "--timeout", "1", "wait", "--event", "live",
                                "--timeout", "2"])
        self.assertEqual(code, 6)
        self.assertGreaterEqual(time.monotonic() - started, 1.9)
        self.assertGreater(seen["wait"], 2 + 20)

    def test_record_stop_says_whether_a_recording_was_stopped(self):
        class Rec:
            is_running = True
            status_dict = {"knut": {"status": "Checking..."}, "twitch:two": {"status": "Recording"}}
            stopped = []

            def stop_channel(self, key):
                self.stopped.append(key)

        app = control.OfflineApp(self.h.config)
        app.recorder = Rec()
        res = control.Controller(app).handle("record_stop", {"channels": ["knut", "twitch:two"]})["result"]
        self.assertEqual([(c["name"], c["result"]) for c in res["channels"]],
                         [("knut", "was_idle"), ("twitch:two", "stopped_recording")])
        self.assertEqual(res["stopped"], ["knut", "twitch:two"])
        self.assertIn("no 'finished' event", res["note"])

    def test_wait_finished_warns_when_nothing_records(self):
        rows = [{"name": "knut", "index": 1, "recording": False},
                {"name": "twitch:two", "index": 2, "recording": True}]
        reply = {"ok": True, "result": {"channels": rows}}
        for channel, warned in (("knut", True), ("#2", False), ("", False)):
            err = io.StringIO()
            with mock.patch.object(msr_ctl.control, "call", return_value=reply), mock.patch("sys.stderr", err):
                msr_ctl._warn_if_nothing_to_finish(self.data, {"events": ["finished"], "channel": channel})
            self.assertEqual("not recording" in err.getvalue(), warned, channel)

    def test_first_live_check_has_no_initial_stagger(self):
        import inspect
        from msr import worker
        src = inspect.getsource(worker.record_worker)
        self.assertNotIn("Initial stagger:", src)
        self.assertNotIn("initial_delay", src)
        self.assertIn("jittered_sleep", src, "later polls keep their spacing")


class FinishingTests(unittest.TestCase):
    """record-stop -> cleanup remux -> finished, race-free, and the per-channel finishing state."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, self.streams = make_data(self.tmp.name, [{"name": "knut", "enabled": True},
                                                            {"name": "twitch:saruei", "enabled": True}])
        self._cd = in_dir(self.data)
        self._cd.__enter__()
        self.config = Config(os.path.join(self.data, "config.ini"))
        self.app = control.OfflineApp(self.config)
        self.app.tracker = self.t = agent_status.Tracker(self.config)

        class Rec:
            is_running = True
            status_dict = {"knut": {"status": "Recording"}, "twitch:saruei": {"status": "Recording"}}

            def stop_channel(self, key):
                self.status_dict[key] = {"status": "Stopped", "detail": "by user"}

            def start_channel(self, key):
                self.status_dict[key] = {"status": "Initializing"}

        self.app.recorder = Rec()
        self.c = control.Controller(self.app)

    def tearDown(self):
        self._cd.__exit__(None, None, None)
        self.tmp.cleanup()

    def rows(self):
        return {r["name"]: r for r in control.channel_rows(self.app)}

    def test_finish_between_stop_and_wait_is_not_missed(self):
        res = self.c.handle("record_stop", {"channels": ["twitch:saruei"]})["result"]
        row = res["channels"][0]
        self.assertEqual(row["result"], "stopped_recording")
        self.assertEqual(self.rows()["twitch:saruei"]["finishing"], "waiting_for_cleanup")
        raw = touch(os.path.join(self.streams, "Recorded", "twitch", "saruei", "s.ts"), 100)
        self.t.on_cleanup("remux_started", "twitch", "saruei", raw)
        self.assertEqual(self.rows()["twitch:saruei"]["finishing"], "remuxing")
        os.remove(raw)
        mp4 = touch(os.path.join(self.streams, "Processed", "twitch", "saruei", "s.mp4"), 5000)
        self.t.on_cleanup("finished", "twitch", "saruei", mp4, ok=True)   # before anyone waits
        # the default wait for a stopped channel starts at its stop: returns at once
        ev = self.c.handle("wait", {"events": ["finished"], "channel": "twitch:saruei", "timeout": 0})["result"]
        self.assertEqual((ev["event"], ev["path"], ev["remux_ok"]), ("finished", mp4, True))
        # and so does an explicit --after finish_after_seq
        ev2 = self.c.handle("wait", {"events": ["finished"], "after": row["finish_after_seq"], "timeout": 0})
        self.assertEqual(ev2["result"]["seq"], ev["seq"])
        kinds = [e["event"] for e in self.t.events() if e["channel"] == "twitch:saruei"]
        self.assertEqual(kinds, ["stopped", "remux_started", "finished"])
        r = self.rows()["twitch:saruei"]
        self.assertEqual((r["finishing"], r["finished_since_stop"]), (None, True))
        self.assertEqual(self.t.recent_finished()[0]["size"], 5000)

    def test_restart_means_a_new_wait_waits_for_the_new_recording(self):
        self.c.handle("record_stop", {"channels": ["knut"]})
        self.t.on_cleanup("finished", "kick", "knut", "", ok=True)
        self.app.mode = "headless"  # a running copy (the offline adapter only edits files)
        self.c.handle("record_start", {"channels": ["knut"]})
        reply = self.c.handle("wait", {"events": ["finished"], "channel": "knut", "timeout": 0.2})
        self.assertEqual(reply.get("code"), "timeout", reply)

    def test_cleanup_without_a_file_still_ends_the_wait(self):
        self.c.handle("record_stop", {"channels": ["knut"]})
        left = touch(os.path.join(self.streams, "Recorded", "kick", "knut", "k.ts"), 100)
        self.t.on_cleanup("channel_done", "knut")           # locked through every pass: still finishing
        self.assertEqual(self.rows()["knut"]["finishing"], "waiting_for_cleanup")
        os.remove(left)
        self.t.on_cleanup("channel_done", "knut")
        ev = self.c.handle("wait", {"events": ["finished"], "channel": "knut", "timeout": 0})["result"]
        self.assertIsNone(ev["remux_ok"])
        self.assertIn("no recording file", ev["note"])

    def test_session_stop_marks_recording_channels_finishing(self):
        self.app.stop_session = lambda: "stopped"
        self.app.mode = "headless"
        res = self.c.handle("stop")["result"]
        self.assertEqual(sorted(res["finishing"]), ["knut", "twitch:saruei"])
        self.t.on_cleanup("pass_done", started=time.time() + 1)
        kinds = [e["event"] for e in self.t.events()]
        self.assertEqual(kinds.count("finished"), 2)

    def test_window_stop_is_seen_from_the_status(self):
        self.t.seen("knut", {"status": "Recording"})
        self.t.seen("knut", {"status": "Stopped", "detail": "by user"})
        self.assertEqual(self.t.finish_state("knut"), ("waiting_for_cleanup", False))

    def test_cli_does_not_warn_while_finishing(self):
        rows = [{"name": "knut", "index": 1, "recording": False, "finishing": "remuxing",
                 "finished_since_stop": False}]
        err = io.StringIO()
        with mock.patch.object(msr_ctl.control, "call", return_value={"ok": True, "result": {"channels": rows}}), \
                mock.patch("sys.stderr", err):
            msr_ctl._warn_if_nothing_to_finish(self.data, {"events": ["finished"], "channel": "knut"})
        self.assertEqual(err.getvalue(), "")


class RestartAndRemoveTests(unittest.TestCase):
    """Live fires for every recording start; remove of a recording channel is a stop."""

    setUp = FinishingTests.setUp
    tearDown = FinishingTests.tearDown

    def kinds(self, key):
        return [e["event"] for e in self.t.events() if e["channel"] == key]

    def test_live_fires_again_after_a_stop_and_restart(self):
        self.t.seen("knut", {"status": "Recording"})
        self.c.handle("record_stop", {"channels": ["knut"]})
        self.t.seen("knut", {"status": "Stopped"})
        self.t.seen("knut", {"status": "Recording"})
        self.assertEqual(self.kinds("knut"), ["live", "stopped", "live"])

    def test_remove_emits_stopped_and_a_re_add_goes_live_again(self):
        st = self.app.recorder.status_dict
        self.app.recorder.remove_channel = lambda key: st.pop(key, None)
        self.t.seen("twitch:saruei", st["twitch:saruei"])
        self.c.handle("channel_remove", {"channels": ["twitch:saruei"]})
        ev = [e for e in self.t.events() if e["event"] == "stopped"][-1]
        self.assertEqual((ev["channel"], ev["was_recording"]), ("twitch:saruei", True))
        st["twitch:saruei"] = {"status": "Recording"}   # re-added worker records
        self.t.seen("twitch:saruei", st["twitch:saruei"])
        self.assertEqual(self.kinds("twitch:saruei"), ["live", "stopped", "live"])

    def test_removed_in_the_window_is_a_stop_too(self):
        self.t.seen("knut", {"status": "Recording"})
        self.t.prune({"twitch:saruei"})
        self.t.seen("knut", {"status": "Recording"})
        self.assertEqual(self.kinds("knut"), ["live", "stopped", "live"])

    def test_wait_live_on_a_recording_channel_returns_at_once(self):
        ev = self.c.handle("wait", {"events": ["live"], "channel": "knut", "timeout": 0})["result"]
        self.assertEqual((ev["event"], ev["channel"], ev["already"]), ("live", "knut", True))
        self.app.recorder.status_dict["knut"] = {"status": "Stopped"}
        reply = self.c.handle("wait", {"events": ["live"], "channel": "knut", "timeout": 0.1})
        self.assertEqual(reply.get("code"), "timeout", reply)


class OfflineAndIdleStopTests(unittest.TestCase):
    """A stream that ends on its own emits offline (and ended after the grace period);
    stopping a channel that is not recording emits nothing."""

    setUp = FinishingTests.setUp
    tearDown = FinishingTests.tearDown

    def kinds(self, key):
        return [e["event"] for e in self.t.events() if e["channel"] == key]

    def test_record_stop_on_an_idle_channel_is_was_idle_without_an_event(self):
        st = self.app.recorder.status_dict
        st["knut"] = {"status": "Offline", "detail": "next check ~60s"}
        self.t.seen("knut", st["knut"])
        row = self.c.handle("record_stop", {"channels": ["knut"]})["result"]["channels"][0]
        self.assertEqual(row["result"], "was_idle")
        self.c.handle("record_stop", {"channels": ["knut"]})   # and again, already stopped
        self.assertEqual(self.kinds("knut"), [])

    def test_remove_of_an_idle_channel_emits_nothing(self):
        st = self.app.recorder.status_dict
        st["knut"] = {"status": "Stopped"}
        self.app.recorder.remove_channel = lambda key: st.pop(key, None)
        self.c.handle("channel_remove", {"channels": ["knut"]})
        self.assertEqual(self.kinds("knut"), [])

    def test_stream_end_emits_offline_then_ended(self):
        self.t.seen("knut", {"status": "Recording"})
        mark = {"status": "Offline", "detail": "stream ended, reconnect grace period", "event": "offline",
                "was_recording": True, "duration_seconds": 460, "in_grace_period": True}
        self.t.seen("knut", mark)
        self.t.seen("knut", dict(mark))          # the status file feed sees it again: no duplicate
        ev = self.c.handle("wait", {"events": ["offline"], "channel": "knut", "after": 0, "timeout": 0})["result"]
        self.assertEqual((ev["event"], ev["was_recording"], ev["duration_seconds"], ev["in_grace_period"]),
                         ("offline", True, 460, True))
        self.t.seen("knut", {"status": "Offline", "detail": "reconnecting (100s left)"})
        self.t.seen("knut", {"status": "Offline", "detail": "reconnect grace period over", "event": "ended"})
        self.assertEqual(self.kinds("knut"), ["live", "offline", "ended"])

    def rows(self):
        return {r["name"]: r for r in control.channel_rows(self.app)}

    def test_status_detail_is_redacted(self):
        self.app.recorder.status_dict["knut"] = {"status": "Error", "detail": (
            "open https://rr1.googlevideo.com/videoplayback/expire/1/ip/2600:4040::1/id/abc.1/itag/311/sig/S")}
        row = self.rows()["knut"]
        for text in (row["detail"], row["error"]):
            self.assertNotIn("2600:", text)
            self.assertIn("id=abc itag=311 [redacted]", text)


class CleanerReportTests(unittest.TestCase):
    def test_cleanup_pass_reports_remux_started_and_finished(self):
        from msr import recorder as recorder_mod
        with tempfile.TemporaryDirectory() as tmp:
            raw = touch(os.path.join(tmp, "Recorded", "kick", "knut", "k.ts"), 4096)
            values = {("Paths", "streams_dir"): tmp, ("Advanced", "ffmpeg_path"): "ffmpeg"}
            config = mock.MagicMock()
            config.get.side_effect = lambda s, k, fallback=None: values.get((s, k), fallback)
            config.getint.return_value = 60
            config.getfloat.return_value = 0.0
            cleaner = recorder_mod.BackgroundCleaner(config)
            got = []
            cleaner.on_file = lambda *a, **k: got.append((a, k))

            def fake_remux(src, dst, *a, **k):
                with open(dst, "wb") as fh:
                    fh.write(b"m" * 4096)
                return True, 4096, None

            with mock.patch.object(recorder_mod, "remux_to_mp4", fake_remux), \
                    mock.patch.object(recorder_mod.time, "sleep"):
                cleaner._process_leftover_files()
            mp4 = os.path.join(tmp, "Processed", "kick", "knut", "k.mp4")
            self.assertEqual([a[0] for a, _k in got], ["remux_started", "finished"])
            self.assertEqual(got[0][0], ("remux_started", "kick", "knut", raw))
            self.assertEqual(got[1], (("finished", "kick", "knut", mp4), {"ok": True}))


class CatalogAndApiTests(unittest.TestCase):
    def test_catalog_covers_every_cli_command_and_every_protocol_command(self):
        cat = msr_ctl.catalog()
        leaves = {path for path, _p, _h in msr_ctl._leaves(msr_ctl.build_parser())}
        self.assertEqual(leaves, set(msr_ctl.CATALOG), "every CLI command has a catalog row and back")
        self.assertEqual({c["command"] for c in cat["commands"]}, leaves)
        reached = {p for c in cat["commands"] for p in c["protocol_commands"]}
        server = set(control.Controller(control.OfflineApp(mock.MagicMock())).commands())
        self.assertEqual(server - reached, set(), "a protocol command has no CLI example")
        self.assertEqual(reached - server - {"ping"}, set())
        for c in cat["commands"]:
            self.assertIn(c["safety"], ("read", "write", "session", "destructive", "depends on the command"))
            self.assertTrue(c["examples"])
        destructive = {c["command"] for c in cat["commands"] if c["safety"] == "destructive"}
        self.assertTrue({"library run", "library restore", "history restore", "update install"} <= destructive)
        self.assertEqual(cat["exit_codes"]["6"], "timed out")
        self.assertEqual(cat["api_version"], control.API_VERSION)

    def test_help_json_needs_no_running_copy(self):
        out = io.StringIO()
        with mock.patch("sys.stdout", out):
            self.assertEqual(msr_ctl.run(["--data-dir", tempfile.gettempdir(), "help", "--json"]), 0)
        self.assertEqual(json.loads(out.getvalue())["api_version"], control.API_VERSION)

    def test_api_version_everywhere_and_mismatch_is_exit_7(self):
        with tempfile.TemporaryDirectory() as tmp:
            data, _ = make_data(tmp, ["knut"])
            h = Headless(data, start_now=False)
            try:
                info = control.read_control_file(data)
                self.assertEqual(info["api_version"], control.API_VERSION)
                self.assertEqual(h.call("ping")["result"]["api_version"], control.API_VERSION)
                self.assertEqual(h.call("status")["result"]["api_version"], control.API_VERSION)
                err = io.StringIO()
                with mock.patch.object(control, "API_VERSION", control.API_VERSION + 1), \
                        mock.patch("sys.stderr", err), mock.patch("sys.stdout", io.StringIO()):
                    code = msr_ctl.run(["--data-dir", data, "status"])
                self.assertEqual(code, 7)
                self.assertIn("api_mismatch", err.getvalue())
            finally:
                h.close()


if __name__ == "__main__":
    unittest.main()
