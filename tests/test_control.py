"""Agent / headless control (msr/control.py, msr_ctl.py): protocol, auth, roster/settings/library
commands, concurrency with a (fake) recorder, the headless loop and the CLI's offline edits.

Temp data folders only; the fake recorder never starts a process.
"""

import contextlib
import io
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import msr.control as control  # noqa: E402
from msr import history  # noqa: E402
from msr.config import Config  # noqa: E402

import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location("msr_ctl_for_test", os.path.join(ROOT, "msr_ctl.py"))
msr_ctl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(msr_ctl)

SECRET_PASSWORD = "hunter2-very-secret"
_tools = mock.patch.multiple("msr.util", HAS_STREAMLINK=True, HAS_YTDLP=True)  # add-channel checks


def setUpModule():
    _tools.start()


def tearDownModule():
    _tools.stop()
SECRET_EMAIL = "someone@example.invalid"


def make_data(tmp, channels=None):
    data = os.path.join(tmp, "data")
    streams = os.path.join(tmp, "streams")
    os.makedirs(data)
    os.makedirs(streams)
    with open(os.path.join(data, "config.ini"), "w", encoding="utf-8") as fh:
        fh.write(f"[Paths]\nstreams_dir = {streams}\nchannels_file = channels.json\n"
                 f"[Fishtank]\nemail = {SECRET_EMAIL}\npassword = {SECRET_PASSWORD}\n")
    if channels is not None:
        with open(os.path.join(data, "channels.json"), "w", encoding="utf-8") as fh:
            json.dump(channels, fh)
    return data, streams


@contextlib.contextmanager
def in_dir(path):
    old = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


def roster(data):
    with open(os.path.join(data, "channels.json"), encoding="utf-8") as fh:
        return json.load(fh)


class FakeRecorder:
    """Enough of StreamRecorder for the control layer; a thread scribbles on status_dict."""

    def __init__(self, channels):
        self.channels = list(channels)
        self.status_dict = {c: {"status": "Checking...", "detail": "", "size": "", "time": ""} for c in channels}
        self.stopped_channels = set()
        self.processes = {c: [] for c in channels}
        self.runtime = {}
        self.is_running = False
        self.should_stop = threading.Event()
        self.calls = []
        self.manager = mock.MagicMock()
        self.cleaner = mock.MagicMock(_thread=None)
        self.lock = threading.Lock()

    def run(self):
        self.is_running = True
        self.calls.append("run")
        n = 0
        while not self.should_stop.is_set():
            n += 1
            for ch in list(self.status_dict):  # the supervisor's updates, racing the readers
                self.status_dict[ch] = {"status": "Recording" if n % 2 else "Offline", "detail": "",
                                        "size": f"{n} MB", "time": "0:01", "raw_file": f"/x/{ch}.ts"}
            time.sleep(0.002)

    def stop(self):
        self.calls.append("stop")
        self.is_running = False
        self.should_stop.set()

    def wait_for_cleanup(self, timeout=None):
        self.calls.append("wait_for_cleanup")
        return True

    def update_status_from_queue(self):
        pass

    def storage_notice(self):
        return ""

    def start_channel(self, ch):
        self.calls.append(("start_channel", ch))
        self.stopped_channels.discard(ch)
        self.processes[ch] = []
        self.status_dict[ch] = {"status": "Initializing"}

    def stop_channel(self, ch):
        self.calls.append(("stop_channel", ch))
        self.stopped_channels.add(ch)

    def remove_channel(self, ch):
        self.calls.append(("remove_channel", ch))
        self.status_dict.pop(ch, None)
        self.processes.pop(ch, None)

    def check_now(self, ch):
        self.calls.append(("check_now", ch))

    def check_all_now(self):
        self.calls.append("check_all_now")

    def set_poll_interval(self, minutes):
        self.calls.append(("set_poll_interval", minutes))


class Headless:
    """A HeadlessApp running its main loop on a thread, with a live control Service."""

    def __init__(self, data, start_now=True):
        self.data = data
        self._cwd = os.getcwd()
        os.chdir(data)
        self.config = Config(os.path.join(data, "config.ini"))
        self.recorders = []

        def factory(keys):
            rec = FakeRecorder(keys)
            self.recorders.append(rec)
            return rec

        self.app = control.HeadlessApp(self.config, factory)
        self.service = control.Service.start_for(self.app)
        self.thread = threading.Thread(target=self.app.run, kwargs={"start_now": start_now, "idle": 0.02},
                                       daemon=True)
        self.thread.start()

    def call(self, cmd, args=None, token=None):
        return control.call(self.data, cmd, args, timeout=20, token=token)

    def wait(self, predicate, timeout=5):
        end = time.time() + timeout
        while time.time() < end:
            if predicate():
                return True
            time.sleep(0.01)
        return False

    def close(self):
        self.app.request_quit()
        self.thread.join(5)
        self.service.stop()
        os.chdir(self._cwd)


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, _ = make_data(self.tmp.name, ["kickone", "twitch:two"])
        self.h = Headless(self.data, start_now=False)

    def tearDown(self):
        self.h.close()
        self.tmp.cleanup()

    def test_control_file_has_port_and_token_and_binds_loopback(self):
        info = control.read_control_file(self.data)
        self.assertEqual(info["host"], "127.0.0.1")
        self.assertEqual(info["pid"], os.getpid())
        self.assertGreaterEqual(len(info["token"]), 32)
        self.assertEqual(self.h.service.server._sock.getsockname()[0], "127.0.0.1")
        if os.name != "nt":
            self.assertEqual(os.stat(os.path.join(self.data, control.CONTROL_FILE)).st_mode & 0o777, 0o600)

    def test_ping(self):
        reply = self.h.call("ping")
        self.assertTrue(reply["ok"])
        self.assertEqual(reply["result"]["mode"], "headless")

    def test_wrong_and_missing_token_are_refused(self):
        for bad in ("nope", ""):
            reply = self.h.call("channel_add", {"name": "intruder"}, token=bad)
            self.assertEqual(reply, {"ok": False, "code": "auth", "error": "Wrong or missing token."})
        info = control.read_control_file(self.data)
        with socket.create_connection(("127.0.0.1", info["port"]), timeout=5) as s:
            s.sendall(json.dumps({"cmd": "quit"}).encode() + b"\n")
            self.assertEqual(json.loads(s.makefile().readline())["code"], "auth")
        self.assertNotIn("intruder", json.dumps(roster(self.data)))
        self.assertTrue(self.h.thread.is_alive(), "an unauthenticated quit must not quit")

    def test_garbage_and_unknown_commands(self):
        info = control.read_control_file(self.data)
        with socket.create_connection(("127.0.0.1", info["port"]), timeout=5) as s:
            s.sendall(b"this is not json\n")
            self.assertEqual(json.loads(s.makefile().readline())["code"], "bad_request")
        reply = self.h.call("format_disk")
        self.assertEqual(reply["code"], "unknown_command")
        self.assertIn("status", reply["commands"])

    def test_control_file_removed_on_stop(self):
        self.h.service.stop()
        self.assertFalse(os.path.exists(os.path.join(self.data, control.CONTROL_FILE)))
        with self.assertRaises(control.NotRunning):
            control.call(self.data, "ping")


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, _ = make_data(self.tmp.name, [{"name": "kickone", "enabled": True},
                                                 {"name": "twitch:two", "enabled": False}])
        self.h = Headless(self.data, start_now=True)
        self.assertTrue(self.h.wait(lambda: self.h.recorders and self.h.recorders[-1].is_running))

    def tearDown(self):
        self.h.close()
        self.tmp.cleanup()

    def test_headless_starts_enabled_channels_at_once(self):
        self.assertEqual(self.h.recorders[0].channels, ["kickone"])

    def test_stop_then_start_makes_a_new_session(self):
        self.assertEqual(self.h.call("stop")["result"]["session"], "stopped")
        self.assertTrue(self.h.wait(lambda: self.h.app.recorder is None))
        self.assertFalse(self.h.call("check")["ok"])
        self.assertEqual(self.h.call("stop")["code"], "conflict")
        self.assertTrue(self.h.call("start")["ok"])
        self.assertTrue(self.h.wait(lambda: len(self.h.recorders) == 2 and self.h.recorders[1].is_running))
        self.assertEqual(self.h.call("start")["code"], "conflict")

    def test_record_start_stop_check_and_remove(self):
        rec = self.h.recorders[0]
        self.assertTrue(self.h.call("record_start", {"channels": ["two"]})["ok"])  # shown name
        self.assertIn(("start_channel", "twitch:two"), rec.calls)
        self.assertTrue(roster(self.data)[1]["enabled"], "starting a channel ticks it, as in the GUI")
        self.assertTrue(self.h.call("record_stop", {"channels": ["#1"]})["ok"])
        self.assertIn(("stop_channel", "kickone"), rec.calls)
        self.h.call("check", {"channels": ["KICKONE"]})
        self.assertIn(("check_now", "kickone"), rec.calls)
        self.h.call("check")
        self.assertIn("check_all_now", rec.calls)
        reply = self.h.call("channel_remove", {"channels": ["twitch:two"]})
        self.assertTrue(reply["result"]["recordings_kept"])
        self.assertIn(("remove_channel", "twitch:two"), rec.calls)
        self.assertEqual([r["name"] for r in roster(self.data)], ["kickone"])

    def test_add_while_recording_starts_it(self):
        self.h.call("channel_add", {"name": "youtube:@someone"})
        self.assertIn(("start_channel", "youtube:@someone"), self.h.recorders[0].calls)

    def test_poll_interval_is_applied_live_and_saved(self):
        reply = self.h.call("config_set", {"section": "Timeouts", "key": "poll_interval_minutes", "value": "1"})
        self.assertEqual(reply["result"]["applies"], "now")
        self.assertIn(("set_poll_interval", 1.0), self.h.recorders[0].calls)
        self.assertIn("poll_interval_minutes = 1.0", open(os.path.join(self.data, "config.ini")).read())

    def test_status_file_is_written_and_has_no_secrets(self):
        path = os.path.join(self.data, control.STATUS_FILE)
        self.assertTrue(self.h.wait(lambda: os.path.isfile(path)))
        self.h.service.status.poke()
        self.assertTrue(self.h.wait(lambda: json.load(open(path))["session"]["running"]))
        status = json.load(open(path))
        self.assertEqual(status["app"]["mode"], "headless")
        self.assertEqual([c["name"] for c in status["channels"]], ["kickone", "twitch:two"])
        for text in (open(path).read(), json.dumps(self.h.call("status")),
                     json.dumps(self.h.call("config_get"))):
            self.assertNotIn(SECRET_PASSWORD, text)
            self.assertNotIn(SECRET_EMAIL, text)

    def test_concurrent_clients_while_recording(self):
        """Many clients at once, the recorder rewriting status_dict and the status writer running:
        every add lands, channels.json and msr_status.json are always valid JSON."""
        errors = []
        path = os.path.join(self.data, control.STATUS_FILE)
        stop_reading = threading.Event()

        def reader():
            while not stop_reading.is_set():
                for name in (path, os.path.join(self.data, "channels.json")):
                    try:
                        with open(name, encoding="utf-8") as fh:
                            json.load(fh)
                    except FileNotFoundError:
                        pass
                    except (ValueError, PermissionError) as e:
                        if not isinstance(e, PermissionError):
                            errors.append(f"{os.path.basename(name)}: {e}")
                self.h.service.status.poke()

        def client(i):
            for cmd, args in (("channel_add", {"name": f"twitch:user{i}"}), ("status", {}),
                              ("channel_disable", {"channels": [f"twitch:user{i}"]}), ("channels", {})):
                reply = self.h.call(cmd, args)
                if not reply.get("ok"):
                    errors.append(reply)

        r = threading.Thread(target=reader, daemon=True)
        r.start()
        workers = [threading.Thread(target=client, args=(i,)) for i in range(16)]
        for w in workers:
            w.start()
        for w in workers:
            w.join(30)
        stop_reading.set()
        r.join(5)
        self.assertEqual(errors, [])
        names = {c["name"]: c["enabled"] for c in roster(self.data)}
        for i in range(16):
            self.assertIs(names[f"twitch:user{i}"], False)
        self.assertGreater(len(history.list_versions(os.path.join(self.data, "channels.json"))), 0)

    def test_quit_ends_the_loop_and_stops_recording(self):
        self.assertTrue(self.h.call("quit")["ok"])
        self.h.thread.join(5)
        self.assertFalse(self.h.thread.is_alive())
        self.assertTrue(self.h.recorders[0].should_stop.is_set())


class OfflineCommandTests(unittest.TestCase):
    """The Controller on the files (msr_ctl.py with nothing running)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, self.streams = make_data(self.tmp.name, ["kickone"])
        self._cd = in_dir(self.data)
        self._cd.__enter__()
        self.config = Config(os.path.join(self.data, "config.ini"))
        self.ctl = control.Controller(control.OfflineApp(self.config))

    def tearDown(self):
        self._cd.__exit__(None, None, None)
        self.tmp.cleanup()

    def ok(self, cmd, args=None):
        reply = self.ctl.handle(cmd, args)
        self.assertTrue(reply["ok"], reply)
        return reply["result"]

    def err(self, cmd, args=None):
        reply = self.ctl.handle(cmd, args)
        self.assertFalse(reply["ok"], reply)
        return reply["code"]

    def test_roster_edits(self):
        self.ok("channel_add", {"names": ["twitch:Abc", "https://example.com/live?token=abc123"]})
        self.assertEqual(self.err("channel_add", {"name": "twitch:abc"}), "invalid")  # duplicate, any case
        self.assertEqual(self.err("channel_add", {"name": "bad name"}), "invalid")
        self.ok("channel_add", {"name": "zed", "platform": "rumble", "disabled": True})
        self.assertEqual([r["name"] for r in roster(self.data)],
                         ["kickone", "twitch:Abc", "custom:https://example.com/live?token=abc123", "rumble:zed"])
        self.assertFalse(roster(self.data)[3]["enabled"])
        rows = self.ok("channels")["channels"]
        self.assertEqual(rows[2]["name"], "custom:example.com", "custom URLs are shown by host only")
        self.assertNotIn("abc123", json.dumps(rows))
        self.ok("channel_disable", {"channels": ["custom:example.com", "#1"]})
        self.assertEqual([r["enabled"] for r in roster(self.data)], [False, True, False, False])
        self.ok("channel_move", {"channels": ["rumble:zed"], "to": "top"})
        self.ok("channel_move", {"channels": ["twitch:abc"], "to": "up"})
        self.assertEqual(roster(self.data)[1]["name"], "twitch:Abc")
        self.ok("channel_sort", {"by": "enabled"})
        self.assertEqual(roster(self.data)[0]["name"], "twitch:Abc")
        self.ok("channel_edit", {"channel": "twitch:Abc", "new_name": "def"})
        self.assertEqual(roster(self.data)[0], {"name": "twitch:def", "enabled": True})
        self.ok("channel_remove", {"channels": ["twitch:def", "zed"]})
        self.assertEqual(len(roster(self.data)), 2)
        self.assertEqual(self.err("channel_remove", {"channels": ["nobody"]}), "not_found")
        self.assertEqual(self.err("start"), "not_running")

    def test_settings(self):
        got = self.ok("config_get", {"section": "Fishtank"})["settings"]["Fishtank"]
        self.assertEqual(got, {"email": "<set>", "password": "<set>"})
        self.assertEqual(self.err("config_set", {"section": "Timeouts", "key": "poll_interval_minutes",
                                                 "value": "0.1"}), "invalid")
        self.assertEqual(self.err("config_set", {"section": "Recording", "key": "max_record_hours",
                                                 "value": "lots"}), "invalid")
        self.assertEqual(self.err("config_set", {"section": "Nope", "key": "x", "value": "1"}), "not_found")
        self.assertEqual(self.err("config_set", {"section": "Recording", "key": "quality",
                                                 "value": "best\n[Paths]"}), "invalid")
        self.ok("config_set", {"section": "Advanced", "key": "verbose", "value": "yes"})
        self.assertEqual(Config(os.path.join(self.data, "config.ini")).get("Advanced", "verbose"), "true")
        res = self.ok("config_set", {"section": "Fishtank", "key": "password", "value": "new-secret"})
        self.assertEqual(res["value"], "<set>")
        self.assertTrue(history.list_versions(os.path.join(self.data, "config.ini")))

    def test_history_list_and_restore(self):
        self.ok("channel_add", {"name": "twitch:a"})
        time.sleep(0.01)
        self.ok("channel_add", {"name": "twitch:b"})
        versions = self.ok("history_list", {"file": "channels"})["versions"]
        self.assertTrue(versions)
        self.assertEqual(self.err("history_restore", {"file": "channels", "index": 1}), "confirm_required")
        self.ok("history_restore", {"file": "channels", "index": len(versions), "confirm": True})
        self.assertEqual(roster(self.data), ["kickone"], "the oldest copy is the file as it was")

    def test_logs_are_redacted(self):
        log = os.path.join(self.streams, "stream_recorder.log")
        with open(log, "w", encoding="utf-8") as fh:
            fh.write("one\nurl https://x/?token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abcdefghijk\nthree\n")
        with mock.patch.object(control.OfflineApp, "log_path", return_value=log):
            lines = self.ok("logs", {"lines": 2})["lines"]
        self.assertEqual(len(lines), 2)
        self.assertNotIn("eyJzdWIiOiIxIn0", "".join(lines))

    def test_library_plan_confirm_execute_restore(self):
        folder = os.path.join(self.streams, "Processed", "kick", "kickone")
        os.makedirs(folder)
        video = os.path.join(folder, "kickone_20260101_120000.mp4")
        with open(video, "wb") as fh:
            fh.write(b"x" * 2048)
        old = time.time() - 40 * 86400
        os.utime(video, (old, old))
        self.assertEqual(self.err("library_plan", {"action": "pending"}), "bad_request")
        plan = self.ok("library_plan", {"action": "pending", "channel": "kickone"})
        self.assertEqual(plan["files"], 1)
        self.assertTrue(os.path.isfile(video), "a plan touches nothing")
        self.assertEqual(self.err("library_execute", {"action": "pending", "channel": "kickone"}),
                         "confirm_required")
        perm = self.ok("library_plan", {"action": "permanent", "channel": "kickone"})
        self.assertEqual(self.err("library_execute", {"action": "permanent", "channel": "kickone",
                                                      "confirm": True}), "confirm_required")
        self.assertEqual(perm["confirm_word"], "kickone")
        done = self.ok("library_execute", {"action": "pending", "channel": "kickone", "confirm": True})
        self.assertEqual(done["done"], 1)
        self.assertFalse(os.path.exists(video))
        self.assertEqual(self.ok("library_pending")["files"], 1)
        batches = self.ok("library_batches")["batches"]
        self.assertEqual(batches[0]["batch"], done["batch"])
        self.ok("library_restore", {"batch": done["batch"], "confirm": True})
        self.assertTrue(os.path.isfile(video))
        self.assertTrue(os.path.isfile(os.path.join(self.data, "library_actions.log")))

    def test_library_refuses_a_channel_that_is_recording(self):
        folder = os.path.join(self.streams, "Processed", "kick", "kickone")
        os.makedirs(folder)
        video = os.path.join(folder, "kickone_20260101_120000.mp4")
        with open(video, "wb") as fh:
            fh.write(b"x" * 2048)
        os.utime(video, (time.time() - 90 * 86400,) * 2)
        rec = FakeRecorder(["kickone"])
        rec.status_dict["kickone"] = {"status": "Recording"}
        self.ctl.app.recorder = rec
        plan = self.ok("library_plan", {"action": "pending", "channel": "kickone"})
        self.assertEqual(plan["files"], 0)
        self.assertTrue(plan["blocked"])


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, self.streams = make_data(self.tmp.name, ["kickone"])

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = msr_ctl.main(["--data-dir", self.data, "--json", *argv])
        text = out.getvalue() or err.getvalue()
        return code, json.loads(text)

    def test_offline_edits_and_exit_codes(self):
        code, reply = self.run_cli("add", "twitch:someone", "--disabled")
        self.assertEqual(code, 0, reply)
        self.assertEqual(roster(self.data)[1], {"name": "twitch:someone", "enabled": False})
        self.assertEqual(self.run_cli("enable", "someone")[0], 0)
        self.assertEqual(self.run_cli("config", "set", "Timeouts.poll_interval_minutes", "5")[0], 0)
        self.assertEqual(self.run_cli("config", "set", "Timeouts.poll_interval_minutes", "0")[0], 1)
        self.assertEqual(self.run_cli("start")[0], 3)
        self.assertEqual(self.run_cli("history", "restore", "channels", "1")[0], 5)
        code, reply = self.run_cli("status")
        self.assertEqual(code, 0)
        self.assertFalse(reply["result"]["app"]["running"])
        self.assertEqual(self.run_cli("remove", "nobody")[0], 1)
        self.assertEqual(os.listdir(self.streams), [], "offline edits never write in the recordings folder")

    def test_value_from_stdin_stays_out_of_argv(self):
        with mock.patch("sys.stdin", io.StringIO("s3cret-from-stdin\n")):
            code, reply = self.run_cli("config", "set", "Fishtank.password", "--stdin")
        self.assertEqual(code, 0)
        self.assertNotIn("s3cret", json.dumps(reply))
        self.assertIn("s3cret-from-stdin", open(os.path.join(self.data, "config.ini")).read())

    def test_offline_edit_refused_while_a_copy_holds_the_lock(self):
        before = roster(self.data)
        with mock.patch("msr.status.is_running", return_value=True):
            code, reply = self.run_cli("add", "twitch:late")
        self.assertEqual(code, 3)
        self.assertEqual(roster(self.data), before)

    def test_cli_talks_to_the_running_copy(self):
        h = Headless(self.data, start_now=True)
        try:
            self.assertTrue(h.wait(lambda: h.recorders and h.recorders[-1].is_running))
            code, reply = self.run_cli("status")
            self.assertEqual(code, 0)
            self.assertEqual(reply["result"]["app"]["mode"], "headless")
            self.assertEqual(self.run_cli("record-stop", "kickone")[0], 0)
            self.assertIn(("stop_channel", "kickone"), h.recorders[0].calls)
            code, reply = self.run_cli("call", "nope")
            self.assertEqual(code, 1)
            info = control.read_control_file(self.data)
            info["token"] = "x" * 43
            with open(os.path.join(self.data, control.CONTROL_FILE), "w") as fh:
                json.dump(info, fh)
            self.assertEqual(self.run_cli("ping")[0], 4)
        finally:
            h.close()


class SwitchTests(unittest.TestCase):
    def test_env_and_config_turn_it_off(self):
        cfg = mock.MagicMock()
        cfg.getboolean.return_value = True
        with mock.patch.dict(os.environ, {"MSR_CONTROL": "0"}):
            self.assertFalse(control.enabled(cfg))
        with mock.patch.dict(os.environ, {"MSR_CONTROL": ""}):
            self.assertTrue(control.enabled(cfg))
            cfg.getboolean.return_value = False
            self.assertFalse(control.enabled(cfg))

    def test_gitignore_keeps_the_control_files_out(self):
        text = open(os.path.join(ROOT, ".gitignore"), encoding="utf-8").read()
        for name in (control.CONTROL_FILE, control.STATUS_FILE):
            self.assertIn(name, text)


if __name__ == "__main__":
    unittest.main()


class GuiControlTests(unittest.TestCase):
    """The real window (hidden) answers control commands and its roster follows them."""

    def test_gui_roster_settings_and_quit(self):
        try:
            import tkinter  # noqa: F401
        except ImportError:
            self.skipTest("tkinter not available")
        import subprocess
        from msr.nowindow import no_window_kwargs
        with tempfile.TemporaryDirectory() as tmp:
            data, _streams = make_data(tmp, ["kickone"])
            with open(os.path.join(data, "config.ini"), "a", encoding="utf-8") as fh:
                fh.write("[GUI]\nminimize_to_tray = false\nnotifications = false\n")
            env = dict(os.environ, MSR_SMOKE_TEST="1", MSR_SMOKE_KEEP="1")
            env.pop("MSR_HOME", None)
            env.pop("MSR_CONTROL", None)
            proc = subprocess.Popen([sys.executable, os.path.join(ROOT, "Multi-Stream-Recorder.py"),
                                     "--data-dir", data], cwd=ROOT, env=env, stdout=subprocess.DEVNULL,
                                    stderr=subprocess.PIPE, **no_window_kwargs())
            try:
                end = time.time() + 60
                while control.read_control_file(data) is None and time.time() < end and proc.poll() is None:
                    time.sleep(0.2)
                self.assertIsNotNone(control.read_control_file(data), "the window never offered control")
                reply = control.call(data, "status", timeout=30)
                self.assertEqual(reply["result"]["app"]["mode"], "gui")
                self.assertTrue(control.call(data, "channel_add", {"name": "twitch:viaagent", "disabled": True},
                                             timeout=30)["ok"])
                self.assertEqual(roster(data)[-1], {"name": "twitch:viaagent", "enabled": False})
                rows = control.call(data, "channels", timeout=30)["result"]["channels"]
                self.assertEqual([r["name"] for r in rows], ["kickone", "twitch:viaagent"])
                control.call(data, "channel_disable", {"channels": ["kickone"]}, timeout=30)
                self.assertEqual(control.call(data, "start", timeout=30)["code"], "conflict",
                                 "nothing enabled: refused without a dialog")
                reply = control.call(data, "config_set", {"section": "Timeouts", "key": "poll_interval_minutes",
                                                          "value": "1"}, timeout=30)
                self.assertTrue(reply["ok"], reply)
                got = control.call(data, "config_get", {"section": "Fishtank"}, timeout=30)
                self.assertEqual(got["result"]["settings"]["Fishtank"]["password"], "<set>")
                self.assertTrue(control.call(data, "quit", timeout=30)["ok"])
                proc.wait(30)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(10)
                err = proc.stderr.read().decode("utf-8", "replace")
                proc.stderr.close()
            self.assertEqual(proc.returncode, 0, err[-2000:])
            self.assertFalse(os.path.exists(os.path.join(data, control.CONTROL_FILE)))
            with open(os.path.join(data, control.STATUS_FILE), encoding="utf-8") as fh:
                self.assertFalse(json.load(fh)["app"]["running"])
            self.assertNotIn(SECRET_PASSWORD, err)


class AddSyntaxTests(unittest.TestCase):
    def test_every_platform_prefix_and_url(self):
        cases = {
            "kick:knut": ("kick", "knut"), "knut": ("kick", "knut"), "KICK:Knut": ("kick", "Knut"),
            "twitch:asmongold": ("twitch", "asmongold"), "youtube:@someone": ("youtube", "@someone"),
            "rumble:someone": ("rumble", "someone"), "tiktok:someone": ("tiktok", "someone"),
            "fishtank:cam-1": ("fishtank", "cam-1"),
            "https://kick.com/knut": ("kick", "knut"), "https://www.twitch.tv/saruei/": ("twitch", "saruei"),
            "https://www.youtube.com/@someone/live": ("youtube", "@someone"),
            "https://youtube.com/channel/UCabcdefghijklmnopqrstuv": ("youtube", "UCabcdefghijklmnopqrstuv"),
            "https://www.youtube.com/watch?v=FaE2vM9h0ok": ("youtube", "FaE2vM9h0ok"),
            "https://rumble.com/c/someone": ("rumble", "someone"),
            "https://www.tiktok.com/@someone/live": ("tiktok", "someone"),
            "https://example.com/stream.m3u8": ("custom", "https://example.com/stream.m3u8"),
        }
        for text, want in cases.items():
            self.assertEqual(control.parse_add_text(text), want, text)
        self.assertEqual(control.parse_add_text("@x", "youtube"), ("youtube", "@x"))
        self.assertEqual(control.parse_add_text("kick:knut", "kick"), ("kick", "knut"))

    def test_a_lone_platform_word_is_an_error_with_a_hint(self):
        for word in ("kick", "YouTube", "twitch"):
            with self.assertRaises(control.ControlError) as cm:
                control.parse_add_text(word)
            self.assertIn(":NAME", str(cm.exception))

    def test_keys_match_what_the_gui_stores(self):
        recs = []
        self.assertEqual(control.roster_add(recs, "kick:knut"), "knut")  # Kick keys are bare names
        self.assertEqual(control.roster_add(recs, "https://www.twitch.tv/saruei"), "twitch:saruei")
        self.assertEqual(control.roster_add(recs, "youtube:@someone"), "youtube:@someone")

    def test_cli_add_kick_knut_adds_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            data, _ = make_data(tmp, [])
            err = io.StringIO()
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
                code = msr_ctl.main(["--data-dir", data, "add", "kick", "knut"])
            self.assertEqual(code, 1)
            self.assertIn("kick:NAME", err.getvalue())
            self.assertEqual(roster(data), [], "all or nothing: 'knut' was not added either")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(msr_ctl.main(["--data-dir", data, "add", "kick:knut"]), 0)
            self.assertEqual(roster(data), [{"name": "knut", "enabled": True}])
            self.assertIn("MSR not running", out.getvalue())
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                msr_ctl.main(["--data-dir", data, "status"])
            self.assertIn("MSR is not running - changes are saved to the files; start MSR to record.",
                          out.getvalue())


class HeadlessAutoStartTests(unittest.TestCase):
    """Headless started with no channels: adding one must start watching and recording it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, _ = make_data(self.tmp.name, [])
        self.h = Headless(self.data, start_now=True)

    def tearDown(self):
        self.h.close()
        self.tmp.cleanup()

    def recording(self, name):
        rows = self.h.call("status")["result"]["channels"]
        return any(r["name"] == name and r["recording"] for r in rows)

    def test_add_while_idle_records_without_further_commands(self):
        self.assertEqual(self.h.recorders, [], "nothing to record yet")
        reply = self.h.call("channel_add", {"name": "twitch:saruei"})
        self.assertTrue(reply["result"]["monitoring"], reply)
        self.assertTrue(self.h.wait(lambda: self.h.recorders and self.h.recorders[0].is_running))
        self.assertEqual(self.h.recorders[0].channels, ["twitch:saruei"])
        self.assertTrue(self.h.wait(lambda: self.recording("twitch:saruei")), "the live channel records")
        # a second add joins the running session
        self.h.call("channel_add", {"name": "kick:knut"})
        self.assertIn(("start_channel", "knut"), self.h.recorders[0].calls)
        self.assertEqual(len(self.h.recorders), 1)

    def test_enable_while_idle_starts_and_while_running_joins(self):
        self.h.call("channel_add", {"name": "twitch:a", "disabled": True})
        self.assertEqual(self.h.recorders, [])
        self.assertTrue(self.h.call("channel_enable", {"channels": ["twitch:a"]})["result"]["monitoring"])
        self.assertTrue(self.h.wait(lambda: self.h.recorders and self.h.recorders[0].is_running))
        self.h.call("channel_add", {"name": "twitch:b", "disabled": True})
        self.h.call("channel_enable", {"channels": ["twitch:b"]})
        self.assertIn(("start_channel", "twitch:b"), self.h.recorders[0].calls)

    def test_stop_means_stopped_until_start(self):
        self.h.call("channel_add", {"name": "twitch:a"})
        self.assertTrue(self.h.wait(lambda: self.h.recorders and self.h.recorders[0].is_running))
        self.h.call("stop")
        self.assertTrue(self.h.wait(lambda: self.h.app.recorder is None))
        reply = self.h.call("channel_add", {"name": "twitch:b"})["result"]
        self.assertFalse(reply["monitoring"])
        self.assertIn("start", reply["hint"])
        self.assertEqual(len(self.h.recorders), 1)
        self.h.call("start")
        self.assertTrue(self.h.wait(lambda: len(self.h.recorders) == 2 and self.h.recorders[1].is_running))
        self.assertEqual(self.h.recorders[1].channels, ["twitch:a", "twitch:b"])

    def test_record_start_while_idle_starts_a_session(self):
        self.h.call("channel_add", {"name": "twitch:a", "disabled": True})
        self.assertTrue(self.h.call("record_start", {"channels": ["twitch:a"]})["ok"])
        self.assertTrue(self.h.wait(lambda: self.h.recorders and self.h.recorders[0].is_running))
