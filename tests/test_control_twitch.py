"""Twitch sign-in over control (twitch_status / twitch_connect / twitch_disconnect).
Fake Twitch API (no network), temp data folders; the token never appears in a reply or log."""

import json
import logging
import os
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import control, twitch_connect as tc  # noqa: E402
from msr import twitch_eventsub as te  # noqa: E402
from msr.config import Config  # noqa: E402
from tests.test_control import Headless, make_data  # noqa: E402

TOKEN = "access-SECRET-abc123"
REFRESH = "refresh-SECRET-xyz789"


class FakeTwitch:
    DEFAULT_CLIENT_ID = te.DEFAULT_CLIENT_ID
    TwitchAuthError = te.TwitchAuthError

    def __init__(self, approve_after=2, uri="https://www.twitch.tv/activate", revoke_fails=False):
        self.saved = None
        self.polls = 0
        self.approve_after = approve_after
        self.uri = uri
        self.revoke_fails = revoke_fails
        self.revoked = []
        self.lock = threading.Lock()

    def device_start(self, client_id):
        return {"device_code": "dev", "user_code": "ABCD-EFGH", "verification_uri": self.uri,
                "expires_in": 600, "interval": 1}

    def device_poll(self, client_id, device_code):
        with self.lock:
            self.polls += 1
            if self.polls < self.approve_after:
                return None
        return {"access_token": TOKEN, "refresh_token": REFRESH, "client_id": client_id,
                "expires_at": time.time() + 3600}

    def validate(self, tok):
        return {"login": "someone", "user_id": "42"}

    def load_token(self, data_dir):
        return dict(self.saved) if self.saved else None

    def save_token(self, data_dir, tok):
        self.saved = dict(tok)

    def clear_token(self, data_dir):
        self.saved = None

    def needs_refresh(self, tok, now=None):
        return False

    def refresh(self, tok):
        return tok

    def revoke(self, tok):
        self.revoked.append(tok["client_id"])
        if self.revoke_fails:
            raise OSError("offline")


def fast_job(cid, data_dir, api):
    return tc.ConnectJob(cid, data_dir, api=api, sleep=lambda s: time.sleep(0.01))


class TwitchCommandTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data, _ = make_data(self.tmp.name, ["kickone"])

    def _ctl(self, api, app=None):
        app = app or control.HeadlessApp(Config(os.path.join(self.data, "config.ini")), lambda keys: None)
        self.addCleanup(app.close)
        ctl = control.Controller(app)
        ctl.twitch_api, ctl.twitch_job = api, fast_job
        return ctl

    def assertNoSecret(self, obj):
        text = json.dumps(obj)
        self.assertNotIn(TOKEN, text)
        self.assertNotIn(REFRESH, text)

    def test_connect_gives_code_then_wait_reports_success(self):
        api = FakeTwitch(approve_after=30)
        ctl = self._ctl(api)
        with self.assertLogs(level="INFO") as logs:
            first = ctl.handle("twitch_connect")
            self.assertTrue(first["ok"], first)
            res = first["result"]
            self.assertEqual(res["user_code"], "ABCD-EFGH")
            self.assertTrue(res["verification_url"].startswith(te.ACTIVATE_URL))
            st = ctl.handle("twitch_status")["result"]
            self.assertTrue(st["connecting"])
            again = ctl.handle("twitch_connect")["result"]
            self.assertTrue(again["already"], "a second connect reuses the running sign-in")
            done = ctl.handle("twitch_connect", {"wait": True, "timeout": 20})
        self.assertTrue(done["ok"], done)
        self.assertEqual(done["result"]["login"], "someone")
        st = ctl.handle("twitch_status")["result"]
        self.assertEqual((st["state"], st["login"], st["connecting"]), ("connected", "someone", False))
        self.assertTrue(ctl.app.config.getboolean("Twitch", "push_enabled"), "connecting turns alerts on")
        for reply in (first, again, done, st):
            self.assertNoSecret(reply)
        self.assertNotIn(TOKEN, "\n".join(logs.output))
        with open(os.path.join(self.data, "config.ini"), encoding="utf-8") as fh:
            self.assertNotIn(TOKEN, fh.read())

    def test_only_the_twitch_activate_link_is_handed_out(self):
        ctl = self._ctl(FakeTwitch(approve_after=1000, uri="https://evil.example/activate"))
        res = ctl.handle("twitch_connect")["result"]
        self.assertTrue(res["verification_url"].startswith(te.ACTIVATE_URL + "?"), res)
        self.assertTrue(ctl.handle("twitch_connect", {"cancel": True})["ok"])
        end = time.time() + 5
        while ctl.handle("twitch_status")["result"]["connecting"] and time.time() < end:
            time.sleep(0.02)
        self.assertEqual(ctl.handle("twitch_status")["result"].get("last_result"), "cancelled")

    def test_disconnect_revokes_and_deletes_even_if_revoke_fails(self):
        for fails in (False, True):
            api = FakeTwitch(revoke_fails=fails)
            api.saved = {"access_token": TOKEN, "client_id": te.DEFAULT_CLIENT_ID, "login": "someone",
                         "refresh_token": REFRESH, "expires_at": time.time() + 3600}
            ctl = self._ctl(api)
            self.assertEqual(ctl.handle("twitch_status")["result"]["state"], "connected")
            out = ctl.handle("twitch_disconnect")
            self.assertTrue(out["ok"], out)
            self.assertEqual(out["result"]["revoked"], not fails)
            self.assertIsNone(api.saved)
            self.assertEqual(len(api.revoked), 1)
            self.assertNoSecret(out)
            self.assertEqual(ctl.handle("twitch_status")["result"]["state"], "none")

    def test_offline_connect_needs_wait(self):
        ctl = self._ctl(FakeTwitch(), app=control.OfflineApp(Config(os.path.join(self.data, "config.ini"))))
        self.assertEqual(ctl.handle("twitch_connect")["code"], "not_running")
        self.assertTrue(ctl.handle("twitch_status")["ok"])

    def test_headless_over_the_socket(self):
        h = Headless(self.data, start_now=False)
        try:
            api = FakeTwitch(approve_after=3)
            h.service.controller.twitch_api, h.service.controller.twitch_job = api, fast_job
            first = h.call("twitch_connect")
            self.assertEqual(first["result"]["user_code"], "ABCD-EFGH")
            done = h.call("twitch_connect", {"wait": True, "timeout": 15})
            self.assertTrue(done["ok"], done)
            self.assertEqual(h.call("twitch_status")["result"]["state"], "connected")
            self.assertNoSecret([first, done])
        finally:
            h.close()


if __name__ == "__main__":
    logging.basicConfig()
    unittest.main()
