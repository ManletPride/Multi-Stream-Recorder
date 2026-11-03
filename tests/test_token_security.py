"""Twitch token at rest (DPAPI), reconnect/verification URL checks, cookie sync warning."""
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from msr import options_panel as op
from msr import twitch_connect as tc
from msr import twitch_eventsub as te
from msr import twitch_push as tp

FAKE = {"client_id": "cid", "access_token": "fake-at", "refresh_token": "fake-rt", "expires_at": 1}


class TempData(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.d = self.tmp.name
        p = mock.patch.dict(os.environ, {"MSR_DATA": self.d})
        p.start()
        self.addCleanup(p.stop)
        te._noted.clear()

    def raw(self):
        with open(te.token_path(self.d), encoding="utf-8") as f:
            return f.read()


@unittest.skipUnless(sys.platform == "win32", "DPAPI is Windows only")
class TestDpapi(TempData):
    def test_round_trip_is_encrypted(self):
        te.save_token(self.d, FAKE)
        text = self.raw()
        self.assertNotIn("fake-at", text)
        self.assertEqual(json.loads(text)["v"], 2)
        self.assertEqual(te.load_token(self.d), FAKE)

    def test_plaintext_is_migrated(self):
        with open(te.token_path(self.d), "w", encoding="utf-8") as f:
            json.dump(FAKE, f)
        with self.assertLogs(level="INFO"):
            self.assertEqual(te.load_token(self.d), FAKE)
        self.assertIn("dpapi", json.loads(self.raw()))
        self.assertEqual(te.load_token(self.d), FAKE)

    def test_undecryptable_is_not_connected(self):
        with open(te.token_path(self.d), "w", encoding="utf-8") as f:
            json.dump({"v": 2, "dpapi": "bm90LWEtcmVhbC1ibG9i"}, f)
        with self.assertLogs(level="INFO") as cm:
            self.assertIsNone(te.load_token(self.d))
        self.assertIn("Connect Twitch", "".join(cm.output))
        self.assertEqual(tc.status_of(self.d, "cid"), ("none", None))


class TestNonWindows(TempData):
    def test_plaintext_fallback(self):
        with mock.patch.object(te, "dpapi_available", return_value=False):
            with self.assertLogs(level="INFO") as cm:
                te.save_token(self.d, FAKE)
            self.assertNotIn("fake-at", "".join(cm.output))
            self.assertEqual(json.loads(self.raw()), FAKE)
            self.assertEqual(te.load_token(self.d), FAKE)

    def test_garbage_never_crashes(self):
        with open(te.token_path(self.d), "w", encoding="utf-8") as f:
            f.write('{"v": 2, "dpapi": "!!!"}')
        self.assertIsNone(te.load_token(self.d))


class TestReconnectUrl(unittest.TestCase):
    def test_accept_reject(self):
        ok = ["wss://eventsub.wss.twitch.tv/ws?x=1", "wss://twitch.tv/ws"]
        bad = ["ws://eventsub.wss.twitch.tv/ws", "wss://evil.com/ws", "wss://twitch.tv.evil.com/ws",
               "wss://eviltwitch.tv/ws", "https://eventsub.wss.twitch.tv/ws", None, 5, "",
               "wss://user@evil.com/?.twitch.tv"]
        for u in ok:
            self.assertEqual(te.safe_reconnect_url(u), u)
        for u in bad:
            self.assertIsNone(te.safe_reconnect_url(u), u)

    def test_listener_ignores_bad_url(self):
        lst = tp.TwitchPushListener(recorder=None, config=None, data_dir=".")
        ws = mock.Mock()
        msg = json.dumps({"metadata": {"message_type": "session_reconnect", "message_id": "m1"},
                          "payload": {"session": {"id": "s", "reconnect_url": "wss://evil.com/ws"}}})
        with self.assertLogs(level="WARNING"):
            lst._on_message(ws, msg)
        self.assertIsNone(lst._reconnect_url)
        ws.close.assert_called_once()
        msg2 = msg.replace("wss://evil.com/ws", "wss://eventsub.wss.twitch.tv/ws").replace("m1", "m2")
        lst._on_message(ws, msg2)
        self.assertEqual(lst._reconnect_url, "wss://eventsub.wss.twitch.tv/ws")


class TestVerificationUri(unittest.TestCase):
    def test_fallback(self):
        good = "https://www.twitch.tv/activate?device-code=ABC"
        self.assertEqual(te.safe_verification_uri(good, "ABC"), good)
        for bad in ("https://evil.com/activate", "http://www.twitch.tv/activate",
                    "https://www.twitch.tv/activateevil.com", None, ""):
            self.assertEqual(te.safe_verification_uri(bad, "AB CD"),
                             "https://www.twitch.tv/activate?device-code=AB%20CD")

    def test_connect_job_uses_fallback(self):
        api = mock.Mock(wraps=te)
        api.device_start = lambda cid: {"device_code": "d", "user_code": "XYZ", "expires_in": 1,
                                        "verification_uri": "https://phish.example/activate"}
        api.device_poll = lambda cid, dc: None
        job = tc.ConnectJob("cid", ".", api=api, sleep=lambda s: None, clock=iter([0, 0, 5, 5, 5]).__next__)
        job._run()
        ev = job.events.get_nowait()
        self.assertEqual(ev[1], "https://www.twitch.tv/activate?device-code=XYZ")


class TestCookieWarning(unittest.TestCase):
    def test_paths(self):
        warn = [r"C:\Users\d\OneDrive\cookies.txt", r"C:\Users\d\OneDrive - Work\x\cookies.txt",
                r"C:\Users\d\Dropbox\cookies.txt", r"G:\My Drive\cookies.txt",
                r"C:\Users\d\Google Drive\cookies.txt", r"C:\Users\d\Desktop\cookies.txt"]
        fine = [r"E:\streams\cookies.txt", r"C:\Users\d\Documents\desktop.txt", "", None,
                r"C:\OneDriveStuff2\cookies.txt"]
        for p in warn:
            self.assertTrue(op.in_synced_folder(p), p)
        for p in fine:
            self.assertFalse(op.in_synced_folder(p), p)


class TestCookieWarningPanel(unittest.TestCase):
    def test_label_follows_setting(self):
        import tkinter as tk
        from tests.test_options_panel import FakeApi, FakeConfig
        try:
            root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        root.withdraw()
        self.addCleanup(root.destroy)
        cfg = FakeConfig()
        cfg.v[("Paths", "cookies_file")] = r"C:\Users\d\OneDrive\cookies.txt"

        def apply(s, k, v):
            cfg.v[(s, k)] = v
            return v
        p = op.OptionsPanel(root, cfg, ".", apply, api=FakeApi(), opener=lambda u: None)
        self.assertIn("synced or shared", p.cookie_warn.cget("text"))
        p._commit("Paths", "cookies_file", r"E:\streams\cookies.txt")
        self.assertEqual(p.cookie_warn.cget("text"), "")


if __name__ == "__main__":
    unittest.main()
