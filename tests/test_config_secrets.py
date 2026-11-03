"""Fishtank password at rest in config.ini (DPAPI on Windows, plaintext elsewhere)."""
import configparser
import os
import sys
import tempfile
import tkinter as tk
import unittest
from unittest import mock

from msr import config as cm
from msr import options_panel as op
from tests.test_options_panel import FakeApi

FAKE_PW = "FAKE-pw-%1;x"


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        p = mock.patch.dict(os.environ, {"MSR_DATA": self.tmp.name})
        p.start()
        self.addCleanup(p.stop)
        os.environ.pop(cm.FISHTANK_PASSWORD_ENV, None)
        cm._noted.clear()
        self.path = os.path.join(self.tmp.name, "config.ini")

    def write_plain(self, pw=FAKE_PW, extra=""):
        with open(self.path, "w", encoding="utf-8") as f:
            f.write(f"[Paths]\nstreams_dir = {self.tmp.name}\n[Fishtank]\nemail = a@b.c\n"
                    f"password = {pw}\n{extra}[Recording]\nquality = 720p\n")

    def text(self):
        with open(self.path, encoding="utf-8") as f:
            return f.read()

    def old_reader(self):
        p = configparser.ConfigParser(interpolation=None)
        p.read(self.path, encoding="utf-8")
        return p


@unittest.skipUnless(sys.platform == "win32", "DPAPI is Windows only")
class TestWindows(Base):
    def test_migration_encrypts_and_keeps_other_settings(self):
        self.write_plain()
        c = cm.Config(self.path)
        self.assertEqual(cm.fishtank_password(c), FAKE_PW)
        text = self.text()
        self.assertNotIn(FAKE_PW, text)
        self.assertIn("password_dpapi", text)
        old = self.old_reader()
        self.assertEqual(old.get("Fishtank", "password"), "")  # what an older MSR sees
        self.assertEqual(old.get("Recording", "quality"), "720p")
        self.assertEqual(old.get("Fishtank", "email"), "a@b.c")
        self.assertEqual(cm.fishtank_password(cm.Config(self.path)), FAKE_PW)

    def test_round_trip_via_save(self):
        c = cm.Config(self.path)
        c.config.set("Fishtank", "password", FAKE_PW)
        self.assertTrue(c.save())
        self.assertNotIn(FAKE_PW, self.text())
        self.assertEqual(cm.fishtank_password(cm.Config(self.path)), FAKE_PW)
        # the raw parser (status.py) decrypts too
        self.assertEqual(cm.fishtank_password(self.old_reader()), FAKE_PW)

    def test_undecryptable_is_missing_not_a_crash(self):
        self.write_plain(pw="", extra="password_dpapi = bm90LWEtcmVhbC1ibG9i\n")
        with self.assertLogs(level="WARNING") as logs:
            c = cm.Config(self.path)
            self.assertEqual(cm.fishtank_password(c), "")
        self.assertIn("enter it again", "".join(logs.output))
        self.assertEqual(cm.secret_is_saved(c, "Fishtank", "password"), "unreadable")
        self.assertTrue(c.save())
        old = self.old_reader()
        self.assertEqual(old.get("Recording", "quality"), "720p")
        self.assertEqual(old.get("Fishtank", "password_dpapi"), "bm90LWEtcmVhbC1ibG9i")
        self.write_plain(pw="", extra="password_dpapi = !!!\n")
        self.assertEqual(cm.fishtank_password(cm.Config(self.path)), "")

    def test_env_still_wins(self):
        self.write_plain()
        with mock.patch.dict(os.environ, {cm.FISHTANK_PASSWORD_ENV: "env-FAKE"}):
            self.assertEqual(cm.fishtank_password(cm.Config(self.path)), "env-FAKE")

    def test_options_field_saves_encrypted(self):
        try:
            root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        root.withdraw()
        self.addCleanup(root.destroy)
        c = cm.Config(self.path)

        def apply(s, k, v):
            c.config.set(s, k, v)
            c.save()
            return v
        p = op.OptionsPanel(root, c, self.tmp.name, apply, api=FakeApi(), environ={})
        h = p.hints[("Fishtank", "password")]
        self.assertEqual(h.cget("text"), "not set")
        v = p.vars[("Fishtank", "password")]
        v.set(FAKE_PW)
        p._commit_field(next(r for r in op.all_rows() if r["key"] == "password"), v)
        self.assertEqual(v.get(), "")
        self.assertNotIn(FAKE_PW, self.text())
        self.assertNotIn(FAKE_PW, p.message.cget("text"))
        self.assertEqual(h.cget("text"), "saved - type to replace")
        self.assertEqual(cm.fishtank_password(cm.Config(self.path)), FAKE_PW)


class TestNonWindows(Base):
    def test_plaintext_unchanged(self):
        with mock.patch.object(cm.dpapi, "available", return_value=False):
            self.write_plain()
            c = cm.Config(self.path)
            self.assertEqual(cm.fishtank_password(c), FAKE_PW)
            c.save()
            self.assertIn(f"password = {FAKE_PW}", self.text())
            self.assertNotIn("password_dpapi", self.text())

    def test_windows_value_on_linux_is_missing(self):
        with mock.patch.object(cm.dpapi, "available", return_value=False):
            self.write_plain(pw="", extra="password_dpapi = AAAA\n")
            with self.assertLogs(level="WARNING"):
                self.assertEqual(cm.fishtank_password(cm.Config(self.path)), "")
            self.assertIn("password_dpapi = AAAA", self.text())
