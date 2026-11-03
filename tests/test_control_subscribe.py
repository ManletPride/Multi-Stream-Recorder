"""subscribe: one open connection streaming events as JSON lines, heartbeats, clean
disconnect, a cap on subscribers, and the CLI. Headless app with a fake recorder, temp folders."""

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

from msr import control  # noqa: E402
from tests.test_control import Headless, make_data, msr_ctl  # noqa: E402


class SubscribeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data, _ = make_data(self.tmp.name, ["kickone"])
        self.h = Headless(self.data, start_now=False)
        self.addCleanup(self.h.close)
        self.tracker = self.h.app.tracker

    def raw(self, args=None, token=None):
        info = control.read_control_file(self.data)
        s = socket.create_connection(("127.0.0.1", info["port"]), timeout=5)
        s.sendall(json.dumps({"token": token or info["token"], "cmd": "subscribe", "args": args or {}}).encode() + b"\n")
        return s, s.makefile("r", encoding="utf-8")

    def test_events_heartbeats_and_filter(self):
        stream = control.subscribe(self.data, {"events": ["disk_low", "live"], "heartbeat": 1, "max_seconds": 4})
        ack = next(stream)
        self.assertTrue(ack["ok"])
        self.tracker.emit("update_available", None, latest="v9")   # filtered out
        self.tracker.emit("disk_low", None, drive="main")
        self.tracker.emit("live", "kickone")
        got = list(stream)
        events = [o["event"]["event"] for o in got if o.get("type") == "event"]
        self.assertEqual(events, ["disk_low", "live"])
        self.assertTrue(any(o.get("type") == "heartbeat" for o in got))
        self.assertEqual(got[-1], {"type": "bye", "reason": "max_seconds"})

    def test_after_replays_and_channel_filter(self):
        first = self.tracker.emit("live", "kickone")["seq"]
        self.tracker.emit("live", "other")
        stream = control.subscribe(self.data, {"after": first - 1, "channel": "kickone", "max_seconds": 1.5})
        objs = list(stream)
        evs = [o["event"] for o in objs if o.get("type") == "event"]
        self.assertEqual([(e["event"], e["channel"]) for e in evs], [("live", "kickone")])

    def test_wrong_token_and_bad_event_refused(self):
        s, f = self.raw(token="nope")
        with s, f:
            self.assertEqual(json.loads(f.readline())["code"], "auth")
        s, f = self.raw({"events": ["boom"]})
        with s, f:
            self.assertEqual(json.loads(f.readline())["code"], "bad_request")
        self.assertEqual(self.h.service.controller.handle("subscribe")["code"], "bad_request",
                         "an in-process call explains streaming")

    def test_subscriber_cap_and_disconnect_frees_a_slot(self):
        socks = []
        try:
            for _ in range(control.MAX_SUBSCRIBERS):
                s, f = self.raw({"heartbeat": 1})
                self.assertTrue(json.loads(f.readline())["ok"])
                socks.append((s, f))
            s, f = self.raw()
            with s:
                self.assertEqual(json.loads(f.readline())["code"], "busy")
            socks.pop()[0].close()   # a client leaves: the server notices and frees its slot
            end = time.time() + 10
            while time.time() < end:
                s, f = self.raw()
                with s:
                    if json.loads(f.readline()).get("ok"):
                        break
                time.sleep(0.2)
            else:
                self.fail("the slot was never freed")
        finally:
            for s, _f in socks:
                s.close()

    def test_server_stop_says_bye(self):
        s, f = self.raw({"heartbeat": 30})
        with s:
            self.assertTrue(json.loads(f.readline())["ok"])
            threading.Timer(0.3, self.h.service.server.stop).start()
            s.settimeout(10)
            self.assertEqual(json.loads(f.readline()), {"type": "bye", "reason": "server stopping"})

    def test_cli_prints_json_lines(self):
        threading.Timer(0.5, lambda: self.tracker.emit("health_problem", None, problem="x", source="health")).start()
        out = io.StringIO()
        with mock.patch("sys.stdout", out), mock.patch("sys.stderr", io.StringIO()):
            code = msr_ctl.run(["--data-dir", self.data, "--json", "subscribe", "--event", "health_problem",
                                "--for", "2", "--heartbeat", "1"])
        self.assertEqual(code, 0)
        lines = [json.loads(x) for x in out.getvalue().splitlines()]
        self.assertTrue(lines[0]["ok"])
        self.assertIn("health_problem", [o["event"]["event"] for o in lines if o.get("type") == "event"])


if __name__ == "__main__":
    unittest.main()
