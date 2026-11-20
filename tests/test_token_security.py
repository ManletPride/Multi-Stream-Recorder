"""Twitch token at rest (DPAPI on Windows, user key elsewhere), reconnect checks."""
import json
import logging
import os
import stat
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
        self.keys = os.path.join(self.d, "keys")
        p = mock.patch.dict(os.environ, {"MSR_DATA": self.d, "MSR_SECRET_DIR": self.keys})
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
    def _linux(self):
        return mock.patch.object(te, "dpapi_available", return_value=False)

    def test_round_trip_is_encrypted(self):
        with self._linux():
            with self.assertLogs(level="DEBUG") as cm:
                logging.getLogger().debug("token-save")
                self.assertTrue(te.save_token(self.d, FAKE))
            self.assertNotIn("fake-at", "".join(cm.output))
            self.assertNotIn("fake-rt", "".join(cm.output))
            doc = json.loads(self.raw())
            self.assertEqual(doc["v"], 2)
            self.assertIn("box", doc)
            self.assertNotIn("dpapi", doc)
            self.assertNotIn("fake-at", self.raw())
            self.assertEqual(te.load_token(self.d), FAKE)

    def test_plaintext_is_migrated(self):
        with self._linux():
            with open(te.token_path(self.d), "w", encoding="utf-8") as f:
                json.dump(FAKE, f)
            with self.assertLogs(level="INFO") as cm:
                self.assertEqual(te.load_token(self.d), FAKE)
            self.assertIn("now encrypted", "".join(cm.output))
            self.assertNotIn("fake-at", "".join(cm.output))
            self.assertIn("box", json.loads(self.raw()))
            self.assertNotIn("fake-at", self.raw())
            self.assertEqual(te.load_token(self.d), FAKE)

    def test_copied_file_without_the_key_cannot_decrypt(self):
        with self._linux():
            te.save_token(self.d, FAKE)
            blob = json.loads(self.raw())["box"]
            os.environ["MSR_SECRET_DIR"] = os.path.join(self.d, "other-keys")
            with self.assertLogs(level="INFO") as cm:
                self.assertIsNone(te.load_token(self.d))
            self.assertIn("Connect Twitch", "".join(cm.output))
            self.assertNotIn("fake-at", "".join(cm.output))
            self.assertEqual(json.loads(self.raw())["box"], blob)
            self.assertFalse(os.path.isfile(os.path.join(self.d, "other-keys", "secret.key")))

    def test_corrupt_key_keeps_the_plaintext_token(self):
        with self._linux():
            os.makedirs(self.keys)
            with open(os.path.join(self.keys, "secret.key"), "wb") as fh:
                fh.write(b"short")
            with open(te.token_path(self.d), "w", encoding="utf-8") as f:
                json.dump(FAKE, f)
            with self.assertLogs(level="WARNING") as cm:
                self.assertEqual(te.load_token(self.d), FAKE)
            blob = "".join(cm.output)
            self.assertTrue("left unchanged" in blob or "not lost" in blob)
            self.assertNotIn("fake-at", blob)
            self.assertEqual(te.load_token(self.d)["access_token"], "fake-at")
            with open(os.path.join(self.keys, "secret.key"), "rb") as fh:
                self.assertEqual(fh.read(), b"short")

    def test_token_file_mode(self):
        with self._linux():
            te.save_token(self.d, FAKE)
            if os.name == "nt":
                return
            self.assertEqual(stat.S_IMODE(os.stat(te.token_path(self.d)).st_mode) & 0o777, 0o600)
            key = os.path.join(self.keys, "secret.key")
            self.assertEqual(stat.S_IMODE(os.stat(key).st_mode) & 0o777, 0o600)
            self.assertEqual(stat.S_IMODE(os.stat(self.keys).st_mode) & 0o777, 0o700)

    def test_windows_dpapi_path_is_unchanged(self):
        def crypt(data, protect):
            if protect:
                return b"W:" + data
            if not data.startswith(b"W:"):
                raise OSError("DPAPI call failed")
            return data[2:]

        with mock.patch.object(te, "dpapi_available", return_value=True), \
                mock.patch.object(te, "_dpapi", side_effect=crypt):
            te.save_token(self.d, FAKE)
            doc = json.loads(self.raw())
            self.assertEqual(set(doc), {"v", "dpapi"})
            self.assertEqual(doc["v"], 2)
            self.assertNotIn("fake-at", self.raw())
            self.assertNotIn("box", doc)
            self.assertEqual(te.load_token(self.d), FAKE)
            with open(te.token_path(self.d), "w", encoding="utf-8") as f:
                json.dump(FAKE, f)
            with self.assertLogs(level="INFO"):
                self.assertEqual(te.load_token(self.d), FAKE)
            self.assertIn("dpapi", json.loads(self.raw()))
            self.assertNotIn("box", json.loads(self.raw()))
            self.assertNotIn("fake-at", self.raw())

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
