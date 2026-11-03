"""App-wide events: health_problem, disk_low, update_available, library_action. Temp folders,
fake recorder, no network."""

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import health as mhealth  # noqa: E402
from msr.agent_status import EVENT_KINDS, Tracker  # noqa: E402
from tests.test_control import Headless, make_data  # noqa: E402


def kinds(tracker):
    return [e["event"] for e in tracker.events()]


class TrackerEventTests(unittest.TestCase):
    def setUp(self):
        self.t = Tracker(None)

    def test_kinds_are_known(self):
        for k in ("health_problem", "disk_low", "update_available", "library_action"):
            self.assertIn(k, EVENT_KINDS)

    def test_health_problem_once_per_new_problem(self):
        self.t.seen_health({"problems": ["ffmpeg missing"]})
        self.t.seen_health({"problems": ["ffmpeg missing"]})
        report = mhealth.Report([mhealth.Issue(mhealth.WARN, "stall:x", "x: file has not grown")], 1.0)
        self.t.seen_health({"problems": ["ffmpeg missing"]}, report)
        self.t.seen_health({"problems": []})
        self.t.seen_health({"problems": ["ffmpeg missing"]})  # came back: announced again
        evs = self.t.events()
        self.assertEqual([(e["event"], e["source"]) for e in evs],
                         [("health_problem", "health"), ("health_problem", "monitor"), ("health_problem", "health")])
        self.assertIsNone(evs[0]["channel"])
        self.assertEqual(evs[1]["issue"], "stall:x")

    def test_disk_low_on_the_transition_only(self):
        low = {"path": "D:/s", "free_gb": 1.0, "min_gb": 5.0, "low": True}
        ok = dict(low, free_gb=50.0, low=False)
        for d in (ok, low, low, ok, low):
            self.t.seen_health({"problems": [], "disk": {"main": d, "fallback": None}})
        evs = [e for e in self.t.events() if e["event"] == "disk_low"]
        self.assertEqual(len(evs), 2)
        self.assertEqual((evs[0]["drive"], evs[0]["free_gb"]), ("main", 1.0))

    def test_update_available_once_per_version(self):
        for info in ({}, {"available": True, "latest": "v9"}, {"available": True, "latest": "v9"},
                     {"available": True, "latest": "v10"}):
            self.t.seen_update(info)
        self.assertEqual([e["latest"] for e in self.t.events()], ["v9", "v10"])


class LibraryActionEventTests(unittest.TestCase):
    def test_a_library_action_is_an_event_and_wait_sees_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            data, streams = make_data(tmp, ["alice"])
            f = Path(streams) / "Processed/kick/alice/alice_20200101_000000.mp4"
            f.parent.mkdir(parents=True)
            f.write_bytes(b"x" * 10)
            old = time.time() - 400 * 86400
            os.utime(f, (old, old))
            h = Headless(data, start_now=False)
            try:
                seq = h.call("events")["result"]["seq"]
                r = h.call("library_execute", {"action": "pending", "paths": [str(f)], "confirm": True})
                self.assertTrue(r["ok"], r)
                ev = h.call("wait", {"events": ["library_action"], "after": seq, "timeout": 5})
                self.assertTrue(ev["ok"], ev)
                self.assertEqual((ev["result"]["action"], ev["result"]["done"]), ("pending", 1))
                self.assertIsNone(ev["result"]["channel"])
            finally:
                h.close()
        from msr import library_actions
        self.assertNotIn(h.app.tracker.on_library, library_actions._listeners, "stop removes the listener")


if __name__ == "__main__":
    unittest.main()
