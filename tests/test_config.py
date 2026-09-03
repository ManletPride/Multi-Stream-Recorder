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
