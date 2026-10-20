"""Data-dir resolution (--data-dir / MSR_HOME) and the shared log rotation."""
import logging
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.paths import ENV_VAR, activate_data_dir, resolve_data_dir
from msr.util import SharedRotatingFileHandler


class TestResolveDataDir(unittest.TestCase):
    def test_none_keeps_current_behaviour(self):
        self.assertIsNone(resolve_data_dir(None, {}))
        self.assertIsNone(resolve_data_dir("  ", {ENV_VAR: ""}))

    def test_env_var_used(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(resolve_data_dir(None, {ENV_VAR: d}), os.path.abspath(d))

    def test_cli_beats_env(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            self.assertEqual(resolve_data_dir(a, {ENV_VAR: b}), os.path.abspath(a))

    def test_quotes_stripped(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(resolve_data_dir(f'"{d}"', {}), os.path.abspath(d))


class TestActivateDataDir(unittest.TestCase):
    def setUp(self):
        self._cwd = os.getcwd()

    def tearDown(self):
        os.chdir(self._cwd)

    def test_chdir_and_export(self):
        env = {}
        with tempfile.TemporaryDirectory() as d:
            activate_data_dir(os.path.abspath(d), env)
            self.assertEqual(os.path.realpath(os.getcwd()), os.path.realpath(d))
            self.assertEqual(env[ENV_VAR], os.path.abspath(d))
            # relative runtime files now resolve inside the data dir
            self.assertEqual(os.path.realpath(os.path.abspath("config.ini")),
                             os.path.realpath(os.path.join(d, "config.ini")))
            os.chdir(self._cwd)

    def test_missing_dir_raises_and_does_not_create(self):
        with tempfile.TemporaryDirectory() as d:
            missing = os.path.join(d, "nope")
            with self.assertRaises(FileNotFoundError):
                activate_data_dir(missing, {})
            self.assertFalse(os.path.exists(missing))


class TestSharedRotatingFileHandler(unittest.TestCase):
    def test_rotates_during_run_and_keeps_writing(self):
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "stream_recorder.log")
            h = SharedRotatingFileHandler(log, max_bytes=2000, backup_count=2, check_interval=0)
            h.setFormatter(logging.Formatter("%(message)s"))
            lg = logging.getLogger("msr-test-rotate")
            lg.propagate = False
            lg.addHandler(h)
            try:
                for i in range(200):
                    lg.warning("line %04d %s", i, "x" * 40)
            finally:
                lg.removeHandler(h)
                h.close()
            self.assertLess(os.path.getsize(log), 2000 + 100)
            self.assertTrue(os.path.exists(log + ".1"))
            self.assertTrue(os.path.exists(log + ".2"))
            self.assertFalse(os.path.exists(log + ".3"))
            self.assertFalse(os.path.exists(log + ".rotating"))
            with open(log, encoding="utf-8") as f:
                live = f.read()
            with open(log + ".1", encoding="utf-8") as f:
                newest_backup = f.read()
            self.assertNotIn("\x00", live)          # append mode: no NUL gap after truncate
            self.assertIn("line 0199", live + newest_backup)

    def test_zero_max_bytes_never_rotates(self):
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "a.log")
            h = SharedRotatingFileHandler(log, max_bytes=0, backup_count=3, check_interval=0)
            rec = logging.LogRecord("x", logging.INFO, __file__, 1, "y" * 500, None, None)
            for _ in range(10):
                h.emit(rec)
            h.close()
            self.assertFalse(os.path.exists(log + ".1"))


if __name__ == "__main__":
    unittest.main()
