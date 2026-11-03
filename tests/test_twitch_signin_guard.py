"""One Twitch device-code sign-in at a time across Options and control. Fake Twitch, hidden Tk."""
import os
import sys
import tempfile
import time
import tkinter as tk
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import control, options_panel as op, twitch_connect as tc  # noqa: E402
from msr.config import Config  # noqa: E402
from tests.test_control import make_data  # noqa: E402
from tests.test_control_twitch import FakeTwitch, fast_job  # noqa: E402
from tests.test_options_panel import FakeConfig  # noqa: E402


def until(cond, timeout=5, pump=None):
    end = time.monotonic() + timeout
    while time.monotonic() < end and not cond():
        if pump:
            pump()
        time.sleep(0.02)
    return cond()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data, _ = make_data(self.tmp.name, ["kickone"])
        self.api = FakeTwitch(approve_after=10 ** 9)
        self.addCleanup(self._stop_all)

    def _stop_all(self):
        job = tc.active()
        if job:
            job.cancel()
            until(lambda: tc.active() is None)

    def _ctl(self):
        app = control.HeadlessApp(Config(os.path.join(self.data, "config.ini")), lambda keys: None)
        self.addCleanup(app.close)
        ctl = control.Controller(app)
        ctl.twitch_api, ctl.twitch_job = self.api, fast_job
        return ctl

    def _options_job(self):
        job = fast_job("cid", self.data, self.api)
        job.owner = "options"
        job.start()
        self.assertTrue(until(lambda: job.info.get("user_code")))
        return job


class GuardTests(Base):
    def test_second_job_is_refused_until_first_ends(self):
        first = self._options_job()
        with self.assertRaises(tc.SignInBusy) as cm:
            fast_job("cid", self.data, self.api).start()
        self.assertIs(cm.exception.job, first)
        first.cancel()
        self.assertTrue(until(lambda: tc.active() is None))
        fast_job("cid", self.data, self.api).start().cancel()

    def test_control_gets_busy_with_code_and_can_cancel_options_flow(self):
        job = self._options_job()
        ctl = self._ctl()
        r = ctl.handle("twitch_connect")
        self.assertFalse(r["ok"])
        self.assertEqual(r["code"], "busy")
        self.assertIn("already in progress", r["error"])
        self.assertEqual(r["user_code"], "ABCD-EFGH")
        self.assertTrue(r["verification_url"].startswith("https://www.twitch.tv/activate"))
        self.assertEqual(r["started_by"], "options")
        self.assertTrue(ctl.handle("twitch_connect", {"cancel": True})["ok"])
        self.assertTrue(until(lambda: tc.active() is None))
        self.assertFalse(job.running())
        ok = ctl.handle("twitch_connect")
        self.assertTrue(ok["ok"], ok)
        self.assertIs(tc.active().owner, "control")


class PanelGuardTests(Base):
    def setUp(self):
        super().setUp()
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)

    def test_panel_shows_control_flow_and_cancel_clears_it(self):
        ctl = self._ctl()
        self.assertTrue(ctl.handle("twitch_connect")["ok"])
        made = []
        p = op.OptionsPanel(self.root, FakeConfig(), self.data, lambda s, k, v: v, api=self.api,
                            connect_job=lambda *a, **k: made.append(1), opener=lambda u: None)
        p.frame.pack()
        p.connect()
        self.assertEqual(made, [], "no second flow started")
        self.root.update()
        self.assertIn("already in progress", p.status.cget("text"))
        self.assertEqual(p._code, "ABCD-EFGH")
        self.assertEqual(str(p.connect_btn.cget("state")), "disabled")
        r = ctl.handle("twitch_connect")
        self.assertTrue(r["ok"] and r["result"]["already"], r)
        p.cancel()
        self.assertTrue(until(lambda: p._foreign is None, pump=self.root.update))
        self.assertIsNone(tc.active())
        self.assertNotIn("in progress", p.status.cget("text"))
        st = ctl.handle("twitch_status")["result"]
        self.assertFalse(st.get("connecting"))

    def test_control_refused_while_panel_flow_runs(self):
        p = op.OptionsPanel(self.root, FakeConfig(), self.data, lambda s, k, v: v, api=self.api,
                            connect_job=fast_job, opener=lambda u: None)
        p.frame.pack()
        p.connect()
        self.assertTrue(until(lambda: p._code, pump=self.root.update))
        r = self._ctl().handle("twitch_connect")
        self.assertEqual((r["ok"], r["code"], r["started_by"]), (False, "busy", "options"))
        self.assertEqual(r["user_code"], "ABCD-EFGH")
        p.cancel()
        self.assertTrue(until(lambda: p.job is None, pump=self.root.update))

