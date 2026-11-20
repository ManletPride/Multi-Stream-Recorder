"""Fishtank password at rest in config.ini.

Windows: DPAPI (`password_dpapi`). Elsewhere: `password_box` plus the user key
outside the data folder. A copied config.ini does not decrypt on its own.
"""
import configparser
import os
import stat
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
        self.keys = os.path.join(self.tmp.name, "keys")
        p = mock.patch.dict(os.environ, {"MSR_DATA": self.tmp.name, "MSR_SECRET_DIR": self.keys})
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
    def _linux(self):
        return mock.patch.object(cm.dpapi, "available", return_value=False)

    def test_plaintext_is_encrypted_on_load(self):
        with self._linux():
            self.write_plain()
            c = cm.Config(self.path)
            self.assertEqual(cm.fishtank_password(c), FAKE_PW)
            text = self.text()
            self.assertNotIn(FAKE_PW, text)
            self.assertIn("password_box", text)
            self.assertNotIn("password_dpapi", text)
            old = self.old_reader()
            self.assertEqual(old.get("Fishtank", "password"), "")
            self.assertEqual(old.get("Recording", "quality"), "720p")
            self.assertEqual(old.get("Fishtank", "email"), "a@b.c")
            self.assertEqual(cm.fishtank_password(cm.Config(self.path)), FAKE_PW)

    def test_round_trip_via_save(self):
        with self._linux():
            c = cm.Config(self.path)
            c.config.set("Fishtank", "password", FAKE_PW)
            self.assertTrue(c.save())
            self.assertNotIn(FAKE_PW, self.text())
            self.assertIn("password_box", self.text())
            self.assertEqual(cm.fishtank_password(cm.Config(self.path)), FAKE_PW)
            self.assertEqual(cm.fishtank_password(self.old_reader()), FAKE_PW)

    def test_copied_config_without_the_key_cannot_decrypt(self):
        with self._linux():
            self.write_plain()
            cm.Config(self.path)
            box = self.old_reader().get("Fishtank", "password_box")
            self.assertTrue(box)
            self.assertNotIn(FAKE_PW, box)
            copied = os.path.join(self.tmp.name, "copied.ini")
            with open(copied, "w", encoding="utf-8") as fh:
                fh.write(self.text())
            os.environ["MSR_SECRET_DIR"] = os.path.join(self.tmp.name, "other-keys")
            with self.assertLogs(level="WARNING") as logs:
                got = cm.fishtank_password(cm.Config(copied))
            self.assertEqual(got, "")
            self.assertIn("enter it again", "".join(logs.output))
            self.assertNotIn(FAKE_PW, "".join(logs.output))
            with open(copied, encoding="utf-8") as fh:
                copied_text = fh.read()
            self.assertIn(box, copied_text)
            self.assertNotIn(FAKE_PW, copied_text)
            self.assertFalse(os.path.isfile(os.path.join(self.tmp.name, "other-keys", "secret.key")))
            self.assertEqual(cm.secret_is_saved(cm.Config(copied).config, "Fishtank", "password"),
                             "unreadable")

    def test_corrupt_key_keeps_the_plaintext(self):
        with self._linux():
            os.makedirs(self.keys, mode=0o700)
            with open(os.path.join(self.keys, "secret.key"), "wb") as fh:
                fh.write(b"short")
            self.write_plain()
            with self.assertLogs(level="WARNING") as logs:
                c = cm.Config(self.path)
            self.assertEqual(cm.fishtank_password(c), FAKE_PW)
            self.assertIn(f"password = {FAKE_PW}", self.text())
            blob = "".join(logs.output)
            self.assertIn("left unchanged", blob)
            self.assertNotIn(FAKE_PW, blob)
            with open(os.path.join(self.keys, "secret.key"), "rb") as fh:
                self.assertEqual(fh.read(), b"short")

    def test_wrong_key_keeps_the_ciphertext(self):
        with self._linux():
            self.write_plain()
            cm.Config(self.path)
            box = self.old_reader().get("Fishtank", "password_box")
            key = os.path.join(self.keys, "secret.key")
            with open(key, "wb") as fh:
                fh.write(b"\x00" * 32)
            with self.assertLogs(level="WARNING") as logs:
                self.assertEqual(cm.fishtank_password(cm.Config(self.path)), "")
            self.assertIn(box, self.text())
            self.assertNotIn(FAKE_PW, self.text())
            self.assertIn("enter it again", "".join(logs.output))
            self.assertNotIn(FAKE_PW, "".join(logs.output))
            with open(key, "rb") as fh:
                self.assertEqual(fh.read(), b"\x00" * 32)

    def test_key_file_mode(self):
        with self._linux():
            self.write_plain()
            cm.Config(self.path)
            if os.name == "nt":
                return
            key = os.path.join(self.keys, "secret.key")
            self.assertEqual(stat.S_IMODE(os.stat(key).st_mode) & 0o777, 0o600)
            self.assertEqual(stat.S_IMODE(os.stat(self.keys).st_mode) & 0o777, 0o700)

    def test_key_round_trips_when_it_contains_newlines(self):
        # Windows text-mode writes turn \\n / Ctrl+Z in the key into a
        # different file. The next decrypt must still see those 32 bytes.
        from msr import filecrypt
        nasty = bytes(range(32))
        real = filecrypt.os.urandom

        def urandom(n, real=real):
            return nasty if n == 32 else real(n)

        with self._linux(), mock.patch.object(filecrypt.os, "urandom", urandom):
            self.write_plain()
            c = cm.Config(self.path)
            self.assertEqual(cm.fishtank_password(c), FAKE_PW)
            key = os.path.join(self.keys, "secret.key")
            with open(key, "rb") as fh:
                self.assertEqual(fh.read(), nasty)
            self.assertEqual(cm.fishtank_password(cm.Config(self.path)), FAKE_PW)

    def test_windows_dpapi_path_is_unchanged(self):
        with mock.patch.object(cm.dpapi, "available", return_value=True), \
                mock.patch.object(cm.dpapi, "encrypt_text", return_value="DPAPICIPHER"), \
                mock.patch.object(cm.dpapi, "decrypt_text", return_value=FAKE_PW):
            self.write_plain()
            c = cm.Config(self.path)
            text = self.text()
            self.assertIn("password_dpapi = DPAPICIPHER", text)
            self.assertNotIn("password_box", text)
            self.assertNotIn(FAKE_PW, text)
            self.assertEqual(cm.fishtank_password(c), FAKE_PW)
            c.config.set("Fishtank", "password", FAKE_PW)
            self.assertTrue(c.save())
            self.assertIn("password_dpapi = DPAPICIPHER", self.text())
            self.assertNotIn("password_box", self.text())

    def test_windows_value_on_linux_is_missing(self):
        with self._linux():
            self.write_plain(pw="", extra="password_dpapi = AAAA\n")
            with self.assertLogs(level="WARNING"):
                self.assertEqual(cm.fishtank_password(cm.Config(self.path)), "")
            self.assertIn("password_dpapi = AAAA", self.text())
