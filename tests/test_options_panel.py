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

    def test_wheel_over_a_child_stays_bound_and_button_4_scrolls_up(self):
        from msr.wheel import SEQUENCES
        p = self.make()
        self.addCleanup(lambda: p._wheel_unmap(type("E", (), {"widget": p.frame})()))
        p._wheel_on()
        for seq in SEQUENCES:
            self.assertTrue(p.canvas.bind_all(seq), seq)
        p.frame.winfo_pointerxy = lambda: (1, 1)
        p.frame.winfo_containing = lambda x, y: p.body
        p._wheel_off()
        self.assertTrue(p.canvas.bind_all("<Button-4>"))
        p.frame.winfo_containing = lambda x, y: self.root
        p._wheel_off()
        self.assertEqual(p.canvas.bind_all("<Button-4>"), "")
        scrolled = []
        p.canvas.yview = lambda: (0.0, 0.4)
        p.canvas.yview_scroll = lambda n, what: scrolled.append((n, what))

        class Ev:
            num, delta = 4, 0

        p._wheel(Ev())
        self.assertEqual(scrolled, [(-1, "units")])

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
        self.pump(lambda: p.status.cget("text") == "Not connected")
        self.assertEqual(p.status.cget("text"), "Not connected")
        self.assertEqual(self.api.revoked, ["at"])

    def test_manage_opens_connections_page(self):
        p = self.make()
        self.assertEqual(p.manage_btn.cget("text"), "Manage on Twitch")
        p.manage_btn.invoke()
        self.assertEqual(self.opened, ["https://www.twitch.tv/settings/connections"])

    def test_disconnect_failure_still_clears_and_notes(self):
        def boom(tok):
            raise OSError("offline")
        self.api.revoke = boom
        self.api.tok = {"client_id": te.DEFAULT_CLIENT_ID, "access_token": "SECRETTOK", "refresh_token": "rt"}
        p = self.make()
        with self.assertLogs("msr.options", "WARNING") as logs:
            p.disconnect()
            self.pump(lambda: p.status.cget("text") == "Not connected")
        self.assertIsNone(self.api.tok)
        self.assertIn("Manage on Twitch", logs.output[0])
        self.assertNotIn("SECRETTOK", "".join(logs.output))
        self.assertIn("Manage on Twitch", p.message.cget("text"))


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


class TestDisconnectRealFile(unittest.TestCase):
    """Real eventsub token file in a temp MSR_DATA; only the HTTP call is faked."""

    def test_revoke_params_then_file_removed(self):
        d = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(d, ignore_errors=True))
        old = os.environ.get("MSR_DATA")
        os.environ["MSR_DATA"] = d
        self.addCleanup(lambda: os.environ.pop("MSR_DATA") if old is None else os.environ.__setitem__("MSR_DATA", old))
        te.save_token(d, {"client_id": "cid", "access_token": "at1", "refresh_token": "rt"})
        path = te.token_path(d)
        self.assertTrue(os.path.exists(path))
        calls = []

        def fake_request(method, url, headers=None, form=None, body=None, timeout=15):
            calls.append((method, url, form, os.path.exists(path)))
            return 200, {}
        from unittest import mock
        with mock.patch.object(te, "http_request", fake_request), \
                mock.patch.object(te.revoke, "__defaults__", (fake_request,)):
            self.assertTrue(tc.disconnect(d))
        self.assertEqual(calls, [("POST", "https://id.twitch.tv/oauth2/revoke",
                                  {"client_id": "cid", "token": "at1"}, True)])
        self.assertFalse(os.path.exists(path))

    def test_revoke_failure_still_removes_file(self):
        d = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(d, ignore_errors=True))
        te.save_token(d, {"client_id": "cid", "access_token": "at1"})
        from unittest import mock

        def offline(*a, **k):
            raise OSError("offline")
        with mock.patch.object(te.revoke, "__defaults__", (offline,)):
            self.assertFalse(tc.disconnect(d))
        self.assertFalse(os.path.exists(te.token_path(d)))



if __name__ == "__main__":
    unittest.main()
