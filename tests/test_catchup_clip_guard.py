"""YouTube from-start catch-up: Clip Now / Screenshot / the live Clip Editor are refused with a
clear reason (no live .ts exists yet), and the capture switches to a normal live recording once
caught up. Temp dirs only; no window, ffmpeg or sound."""
import os
import sys
import tempfile
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import msr.control as control  # noqa: E402
from msr import worker  # noqa: E402
from msr.config import Config  # noqa: E402
from tests.test_control import in_dir, make_data  # noqa: E402
from tests.test_yt_live_start import MonitorTests  # noqa: E402

KEY = "youtube:@x"


class ReasonTests(unittest.TestCase):
    def test_reason_only_for_from_start_rows(self):
        self.assertEqual(worker.clip_unavailable_reason({"status": "Recording", "from_start": True}),
                         "Available once caught up to live")
        self.assertIsNone(worker.clip_unavailable_reason({"status": "Recording"}))
        self.assertIsNone(worker.clip_unavailable_reason(None))


class FakeRecorder:
    def __init__(self, base, row):
        self.recorded_base = base
        self.status_dict = {KEY: row}
        self.is_running = True


class ControlGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data, self.streams = make_data(self.tmp.name, [{"name": KEY, "enabled": True}])
        self._cd = in_dir(self.data)
        self._cd.__enter__()
        self.c = control.Controller(control.OfflineApp(Config(os.path.join(self.data, "config.ini"))))
        base = os.path.join(self.streams, "Recorded")
        folder = os.path.join(base, "youtube", "x")
        os.makedirs(folder)
        # an older leftover .ts: must never be cut while the from-start parts are written
        with open(os.path.join(folder, "old.ts"), "wb") as fh:
            fh.write(b"\0" * 2048)
        self.rec = FakeRecorder(base, {"status": "Recording", "from_start": True,
                                       "raw_file": os.path.join(folder, "new.f299.mp4")})

    def tearDown(self):
        self._cd.__exit__(None, None, None)
        self.tmp.cleanup()

    def call(self, cmd, args):
        with mock.patch.object(control.Controller, "_recorder", return_value=self.rec), \
                mock.patch.object(worker, "create_clip") as clip, \
                mock.patch.object(worker, "create_screenshot") as shot:
            reply = self.c.handle(cmd, args)
        self.assertFalse(clip.called or shot.called)
        return reply

    def test_clip_now_and_screenshot_refused_with_reason(self):
        for cmd in ("clip", "screenshot"):
            r = self.call(cmd, {"channel": KEY})
            self.assertFalse(r["ok"], r)
            self.assertEqual(r["code"], "busy")
            self.assertIn("Available once caught up to live", r["error"])

    def test_clip_from_to_refused_with_reason(self):
        r = self.call("clip", {"channel": KEY, "from": "-30"})
        self.assertEqual(r["code"], "busy")
        self.assertIn("catching up", r["error"])


class SplitWhenCaughtUpTests(MonitorTests):
    def test_switches_to_live_once_caught_up_without_any_limit(self):
        split, splits, out, _ = self.run_monitor(caught_up_at=1100, max_gb=0)
        self.assertTrue(split)
        self.assertEqual(len(splits), 1)
        self.assertGreaterEqual(splits[0], 1100)
        self.assertTrue(all(s.get("from_start") for s in out), "rows carry the clip guard flag")

    def test_no_switch_while_catching_up(self):
        split, splits, _out, _ = self.run_monitor(caught_up_at=10 ** 9, max_gb=0, end_at=300)
        self.assertFalse(split)
        self.assertEqual(splits, [])


if __name__ == "__main__":
    unittest.main()
