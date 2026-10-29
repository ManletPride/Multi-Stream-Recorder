"""Portable streams_dir default and config coercion."""
import configparser
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.config import (
    Config,
    default_streams_dir,
    parser_getboolean,
    parser_getfloat,
    parser_getint,
)


class DefaultStreamsDirTests(unittest.TestCase):
    def test_not_e_streams(self):
        path = default_streams_dir()
        self.assertTrue(path)
        self.assertNotEqual(os.path.normcase(path), os.path.normcase(r"E:\Streams"))
        self.assertTrue(path.endswith("Multi-Stream Recorder"))
        self.assertIn("Videos", path.replace("\\", "/").split("/"))

    def test_new_config_uses_portable_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            ini = os.path.join(tmp, "config.ini")
            cfg = Config(ini)
            got = cfg.get("Paths", "streams_dir")
            self.assertEqual(got, default_streams_dir())
            self.assertTrue(os.path.isfile(ini))
            with open(ini, encoding="utf-8") as fh:
                text = fh.read()
            self.assertNotIn("E:\\Streams", text)
            self.assertNotIn("E:\\\\Streams", text)


class CoerceConfigTests(unittest.TestCase):
    def _cfg(self, body):
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".ini", delete=False, encoding="utf-8",
        )
        tmp.write(body)
        tmp.close()
        self.addCleanup(lambda: os.path.exists(tmp.name) and os.remove(tmp.name))
        return Config(tmp.name), tmp.name

    def test_garbage_numbers_become_defaults(self):
        cfg, path = self._cfg(
            "[Recording]\n"
            "max_record_hours = banana\n"
            "max_file_size_gb = nope\n"
            "min_disk_space_gb = xyz\n"
            "[Paths]\n"
            "streams_dir = C:\\Keep\\This\n"
        )
        self.assertAlmostEqual(cfg.getfloat("Recording", "max_record_hours"), 12.0)
        self.assertAlmostEqual(cfg.getfloat("Recording", "max_file_size_gb"), 8.0)
        self.assertAlmostEqual(cfg.getfloat("Recording", "min_disk_space_gb"), 5.0)
        self.assertEqual(cfg.get("Paths", "streams_dir"), r"C:\Keep\This")
        self.assertTrue(any("max_record_hours" in w for w in cfg.coerce_warnings))
        with open(path, encoding="utf-8") as fh:
            saved = fh.read()
        self.assertIn("12.0", saved)
        self.assertIn("C:\\Keep\\This", saved)

    def test_out_of_range_poll_reset(self):
        cfg, _ = self._cfg(
            "[Timeouts]\n"
            "poll_interval_minutes = 0.01\n"
        )
        self.assertAlmostEqual(cfg.getfloat("Timeouts", "poll_interval_minutes"), 3.0)

    def test_invalid_bool_reset(self):
        cfg, _ = self._cfg(
            "[Advanced]\n"
            "verbose = maybe\n"
        )
        self.assertFalse(cfg.getboolean("Advanced", "verbose"))

    def test_zero_time_limit_kept(self):
        cfg, _ = self._cfg(
            "[Recording]\n"
            "max_record_hours = 0\n"
        )
        self.assertAlmostEqual(cfg.getfloat("Recording", "max_record_hours"), 0.0)

    def test_parser_helpers_do_not_raise(self):
        p = configparser.ConfigParser()
        p.add_section("Recording")
        p.set("Recording", "max_record_hours", "nope")
        self.assertEqual(parser_getfloat(p, "Recording", "max_record_hours", 12.0), 12.0)
        p.set("Recording", "split_on_resolution_change", "maybe")
        self.assertTrue(parser_getboolean(p, "Recording", "split_on_resolution_change", True))
        self.assertEqual(parser_getint(p, "Timeouts", "missing", 30), 30)


if __name__ == "__main__":
    unittest.main()


# ---- values with a percent sign, and a config.ini that cannot be parsed --------------------

from msr.config import new_parser  # noqa: E402

SECRET = "hunter2-SECRET"


def _ini(tmp, text, encoding="utf-8"):
    path = os.path.join(tmp, "config.ini")
    with open(path, "wb") as f:
        f.write(text.encode(encoding) if isinstance(text, str) else text)
    return path


def _bytes(path):
    with open(path, "rb") as f:
        return f.read()


class PercentValueTests(unittest.TestCase):
    """A ``%`` in a password or filename pattern is data, not interpolation syntax."""

    PASSWORD = "P@ss%word"
    PATTERN = "{username}_100%_{timestamp}"

    def make(self, tmp):
        return _ini(tmp, f"[Fishtank]\nemail = a@b.c\npassword = {self.PASSWORD}\n"
                         f"[Recording]\nfilename_pattern = {self.PATTERN}\n")

    def test_values_are_read_literally(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(self.make(tmp))
            self.assertEqual(cfg.get("Fishtank", "password"), self.PASSWORD)
            self.assertEqual(cfg.get("Recording", "filename_pattern"), self.PATTERN)

    def test_items_and_worker_read_dict_path(self):
        """StreamRecorder hands dict(items(section)) to each worker, which read_dict()s it."""
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(self.make(tmp))
            as_dict = {s: dict(cfg.config.items(s)) for s in cfg.config.sections()}
            worker_side = new_parser()
            worker_side.read_dict(as_dict)
            self.assertEqual(worker_side.get("Fishtank", "password"), self.PASSWORD)
            self.assertEqual(worker_side.get("Recording", "filename_pattern"), self.PATTERN)

    def test_percent_survives_save_and_reload(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.make(tmp)
            cfg = Config(path)
            cfg.config.set("GUI", "theme", "dark")
            cfg.save()
            again = Config(path)
            self.assertEqual(again.get("Fishtank", "password"), self.PASSWORD)
            self.assertEqual(again.get("Recording", "filename_pattern"), self.PATTERN)

    def test_status_reader_accepts_percent(self):
        from msr.status import _read_config
        with tempfile.TemporaryDirectory() as tmp:
            parser, present = _read_config(self.make(tmp))
            self.assertTrue(present)
            self.assertEqual(parser.get("Recording", "filename_pattern"), self.PATTERN)


class MalformedConfigTests(unittest.TestCase):
    """A config.ini that does not parse must not stop the start or be overwritten."""

    def test_missing_section_header_keeps_the_file_and_uses_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = f"password = {SECRET}\n[Paths]\nstreams_dir = D:/keep\n"
            path = _ini(tmp, original)
            cfg = Config(path)  # must not raise
            self.assertEqual(_bytes(path), original.encode(), "the user's file must stay as it is")
            self.assertEqual(os.listdir(tmp), ["config.ini"], "no backup is needed when nothing is written")
            self.assertTrue(cfg._unreadable)
            self.assertTrue(cfg.get("Paths", "streams_dir"))  # defaults are usable
            note = " ".join(cfg.coerce_warnings) + str(cfg.load_problem)
            self.assertIn("config.ini could not be read", cfg.coerce_warnings[0])
            self.assertNotIn(SECRET, note, "the warning must not quote the file")

    def test_save_after_unreadable_backs_up_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = f"password = {SECRET}\n"
            path = _ini(tmp, original)
            cfg = Config(path)
            cfg.save()
            backups = [n for n in os.listdir(tmp) if n.startswith("config.ini.broken-")]
            self.assertEqual(len(backups), 1)
            self.assertEqual(_bytes(os.path.join(tmp, backups[0])), original.encode())
            cfg.save()  # a second save does not make a second copy
            self.assertEqual(len([n for n in os.listdir(tmp) if n.startswith("config.ini.broken-")]), 1)

    def test_duplicate_option_is_read_leniently(self):
        with tempfile.TemporaryDirectory() as tmp:
            text = "[Fishtank]\npassword = first\npassword = second\n[GUI]\ntheme = dark\n"
            path = _ini(tmp, text)
            cfg = Config(path)
            self.assertEqual(cfg.get("Fishtank", "password"), "second")
            self.assertEqual(cfg.get("GUI", "theme"), "dark")
            self.assertFalse(cfg._unreadable)
            self.assertIn("password", cfg.load_problem)
            self.assertNotIn("first", cfg.load_problem + " ".join(cfg.coerce_warnings))
            # Defaults were missing, so the file had to be rewritten: the original is kept first.
            backups = [n for n in os.listdir(tmp) if n.startswith("config.ini.broken-")]
            self.assertEqual(len(backups), 1)
            self.assertEqual(_bytes(os.path.join(tmp, backups[0])), text.encode())

    def test_utf8_bom_is_accepted_without_a_problem(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _ini(tmp, b"\xef\xbb\xbf[Paths]\nstreams_dir = D:/bom\n")
            cfg = Config(path)
            self.assertEqual(cfg.get("Paths", "streams_dir"), "D:/bom")
            self.assertIsNone(cfg.load_problem)

    def test_windows_1252_file_is_read_not_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _ini(tmp, "[Paths]\nstreams_dir = D:/vid\u00e9os\n", encoding="cp1252")
            cfg = Config(path)
            self.assertEqual(cfg.get("Paths", "streams_dir"), "D:/vid\u00e9os")
            self.assertIn("UTF-8", cfg.load_problem)

    def test_undecodable_bytes_use_defaults_and_keep_the_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = b"[Paths]\nstreams_dir = D:/\x81\x8d\x8f\x90\x9d\n"
            path = _ini(tmp, data)
            cfg = Config(path)
            self.assertTrue(cfg._unreadable)
            self.assertEqual(_bytes(path), data)

    def test_unreadable_file_is_not_replaced_at_startup_by_defaults(self):
        """Coercion and 'add missing defaults' write at load time; not for an unreadable file."""
        with tempfile.TemporaryDirectory() as tmp:
            data = b"streams_dir = nowhere\n"
            path = _ini(tmp, data)
            Config(path)
            self.assertEqual(_bytes(path), data)
