"""Options tab (msr/options_panel.py) + Twitch connect job, hidden root, mocked Twitch."""
import os
import queue
import tempfile
import time
import tkinter as tk
import unittest

from msr import options_panel as op
from msr import twitch_connect as tc
from msr import twitch_eventsub as te


class FakeConfig:
    def __init__(self, **vals):
        self.v = {("Twitch", "push_enabled"): "false", ("Timeouts", "poll_interval_minutes"): "3"}
        self.v.update(vals)
        self.config_file = "config.ini"

    def get(self, s, k, fallback=None):
        return self.v.get((s, k), fallback)

    def getboolean(self, s, k, fallback=None):
        v = self.v.get((s, k))
        return fallback if v is None else str(v).lower() == "true"


class FakeApi:
    TwitchAuthError = te.TwitchAuthError

    def __init__(self):
        self.tok = None
        self.revoked = []
        self.polls = 0
        self.approve_after = 2
        self.validate_fails = False

    def load_token(self, d):
        return dict(self.tok) if self.tok else None

    def save_token(self, d, t):
        self.tok = dict(t)

    def clear_token(self, d):
        self.tok = None

    def needs_refresh(self, tok, now=None):
        return False

    def refresh(self, tok):
        raise te.TwitchAuthError("refresh refused")

    def validate(self, tok):
        if self.validate_fails:
            raise te.TwitchAuthError("token invalid")
        return {"login": "davidtest", "user_id": "7"}

    def revoke(self, tok):
        self.revoked.append(tok["access_token"])

    def device_start(self, cid):
        return {"device_code": "dc", "user_code": "ABCDEFGH", "expires_in": 1800, "interval": 1,
                "verification_uri": "https://www.twitch.tv/activate?device-code=ABCDEFGH"}

    def device_poll(self, cid, dc):
        self.polls += 1
        if self.polls >= self.approve_after:
            return {"client_id": cid, "access_token": "at", "refresh_token": "rt", "expires_at": 9e12}
        return None


class TestConnectJob(unittest.TestCase):
    def drain(self, job):
        job.thread.join(5)
        out = []
        while not job.events.empty():
            out.append(job.events.get())
        return out

    def test_connects_and_saves(self):
        api = FakeApi()
        job = tc.ConnectJob(te.DEFAULT_CLIENT_ID, ".", api=api, sleep=lambda s: None).start()
        evs = self.drain(job)
        self.assertEqual(evs[0][0], "code")
        self.assertEqual(evs[0][2], "ABCDEFGH")
        self.assertEqual(evs[-1], ("connected", "davidtest"))
        self.assertEqual(api.tok["login"], "davidtest")

    def test_expired(self):
        api = FakeApi()
        api.approve_after = 10 ** 9
        t = [0.0]

        def sleep(s):
            t[0] += 1000
        job = tc.ConnectJob("cid", ".", api=api, sleep=sleep, clock=lambda: t[0]).start()
        self.assertEqual(self.drain(job)[-1], ("expired",))

    def test_cancel(self):
        api = FakeApi()
        api.approve_after = 10 ** 9
        job = tc.ConnectJob("cid", ".", api=api)
        job.cancel()
        job._sleep = lambda s: None
        job.start()
        self.assertEqual(self.drain(job)[-1], ("cancelled",))

    def test_network_error_text_has_no_details(self):
        api = FakeApi()
        api.device_start = lambda cid: (_ for _ in ()).throw(OSError("secret-ish path"))
        job = tc.ConnectJob("cid", ".", api=api).start()
        ev = self.drain(job)[-1]
        self.assertEqual(ev, ("error", "could not reach Twitch (OSError)"))

    def test_status_and_disconnect(self):
        api = FakeApi()
        self.assertEqual(tc.status_of(".", "cid", api), ("none", None))
        api.tok = {"client_id": "cid", "access_token": "at", "refresh_token": "rt", "login": "me"}
        self.assertEqual(tc.status_of(".", "cid", api), ("connected", "me"))
        self.assertEqual(tc.status_of(".", "other", api), ("none", None))
        api.validate_fails = True
        self.assertEqual(tc.check_online(".", "cid", api), ("expired", "me"))
        tc.disconnect(".", api)
        self.assertEqual((api.tok, api.revoked), (None, ["at"]))


class TestPanel(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)
        self.saved = []
        self.changed = []
        self.opened = []
        self.cfg = FakeConfig()
        self.api = FakeApi()

    def apply(self, s, k, v):
        from msr import control
        try:
            v = control.validate_setting(s, k, v)
        except control.ControlError as e:
            raise ValueError(str(e))
        self.cfg.v[(s, k)] = v
        self.saved.append((s, k, v))
        return v

    def make(self):
        p = op.OptionsPanel(self.root, self.cfg, ".", self.apply,
                            on_twitch_changed=lambda: self.changed.append(1),
                            api=self.api, opener=self.opened.append)
        p.frame.pack()
        return p

    def pump(self, until, timeout=5):
        end = time.monotonic() + timeout
        while time.monotonic() < end and not until():
            self.root.update()
            time.sleep(0.02)

    def test_initial_status_and_tooltip_text(self):
        p = self.make()
        self.assertEqual(p.status.cget("text"), "Not connected")
        self.assertEqual(str(p.disconnect_btn.cget("state")), "disabled")
        self.assertIn("about 10 channels", op.TWITCH_TIP)
        self.assertIn("No permissions requested", op.TWITCH_TIP)

    def test_connect_flow_shows_code_then_connected(self):
        class Job(tc.ConnectJob):
            def __init__(s, cid, d, api):
                super().__init__(cid, d, api=api, sleep=lambda x: time.sleep(0.05))
        p = self.make()
        p.connect_job = Job
        p.connect()
        self.pump(lambda: p._code)
        self.assertEqual(p._code, "ABCDEFGH")
        self.assertIn("twitch.tv/activate", p.link.cget("text"))
        p.open_link()
        self.assertEqual(self.opened, ["https://www.twitch.tv/activate?device-code=ABCDEFGH"])
        clip = []
        p.frame.clipboard_clear = lambda: clip.clear()
        p.frame.clipboard_append = clip.append  # never touch the real clipboard
        p.copy_code()
        self.assertEqual(clip, ["ABCDEFGH"])
        self.pump(lambda: p.status.cget("text").startswith("Connected"))
        self.assertEqual(p.status.cget("text"), "Connected as davidtest")
        self.assertIn(("Twitch", "push_enabled", "true"), self.saved)
        self.assertTrue(self.changed)
        self.assertFalse(p.code_row.winfo_ismapped())
        p.disconnect()
        self.assertEqual(p.status.cget("text"), "Not connected")
        self.assertEqual(self.api.revoked, ["at"])

    def test_generic_setting_validates(self):
        p = self.make()
        var = p.vars[("Timeouts", "poll_interval_minutes")]
        var.set("0.1")
        p._commit("Timeouts", "poll_interval_minutes", var.get())
        self.assertEqual(self.saved, [])
        self.assertEqual(var.get(), "3")  # reverted to stored value
        self.assertIn("between", p.message.cget("text"))
        p._commit("Timeouts", "poll_interval_minutes", "5")
        self.assertEqual(self.saved, [("Timeouts", "poll_interval_minutes", "5.0")])

    def test_push_toggle_saves_and_notifies(self):
        p = self.make()
        p.push_var.set(True)
        p._toggle_push()
        self.assertEqual(self.saved[-1], ("Twitch", "push_enabled", "true"))
        self.assertEqual(self.changed, [1])

    def test_activate_marks_expired(self):
        self.api.tok = {"client_id": te.DEFAULT_CLIENT_ID, "access_token": "at",
                        "refresh_token": "rt", "login": "me"}
        self.api.validate_fails = True
        p = self.make()
        self.assertEqual(p.status.cget("text"), "Connected as me")
        p.activate()
        self.pump(lambda: p.status.cget("text").startswith("Expired"))
        self.assertTrue(p.status.cget("text").startswith("Expired"))


if __name__ == "__main__":
    unittest.main()
