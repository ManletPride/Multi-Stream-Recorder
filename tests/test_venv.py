"""MSR's own .venv (msr/venv.py), the launcher, and msr_ctl's deps hand-off. venv/pip are faked
(the real bootstrap was checked by hand in a temp copy; see CHANGELOG)."""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import venv  # noqa: E402

_spec = importlib.util.spec_from_file_location("msr_ctl_for_venv_test", os.path.join(ROOT, "msr_ctl.py"))
msr_ctl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(msr_ctl)


class FakeTools:
    """subprocess.run stand-in for `python -m venv` and `pip install -r`."""

    def __init__(self, venv_rc=0, pip_rc=0, pip_err="", venv_err="", home=None):
        self.venv_rc, self.pip_rc, self.pip_err, self.venv_err = venv_rc, pip_rc, pip_err, venv_err
        self.home = home or os.path.dirname(sys.executable)
        self.calls = []

    def __call__(self, cmd, **kw):
        self.calls.append((cmd, kw))
        if cmd[1:3] == ["-m", "venv"]:
            if self.venv_rc == 0:
                d = cmd[3]
                sub = os.path.join(d, "Scripts" if os.name == "nt" else "bin")
                os.makedirs(sub, exist_ok=True)
                for n in (["python.exe", "pythonw.exe"] if os.name == "nt" else ["python"]):
                    open(os.path.join(sub, n), "w").close()
                with open(os.path.join(d, "pyvenv.cfg"), "w") as fh:
                    fh.write(f"home = {self.home}\n")
            return subprocess.CompletedProcess(cmd, self.venv_rc, "", self.venv_err)
        assert cmd[1:4] == ["-m", "pip", "install"], cmd
        return subprocess.CompletedProcess(cmd, self.pip_rc, "", self.pip_err)


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = self.tmp.name
        with open(os.path.join(self.root, "requirements.txt"), "w") as fh:
            fh.write("yt-dlp\n")
        e = mock.patch.dict(os.environ)
        e.start()
        self.addCleanup(e.stop)
        os.environ.pop(venv.OPT_OUT_ENV, None)


class StateTests(Base):
    def test_fresh_setup_then_ready(self):
        self.assertEqual(venv.check(self.root)[0], "missing")
        tools = FakeTools()
        steps = []
        ok, msg = venv.ensure(self.root, progress=steps.append, run=tools)
        self.assertTrue(ok, msg)
        self.assertEqual(venv.check(self.root), ("ready", ""))
        self.assertEqual(len(tools.calls), 2)
        self.assertIn("-r", tools.calls[1][0])
        for _, kw in tools.calls:
            self.assertEqual(kw["stdin"], subprocess.DEVNULL)
            if os.name == "nt":
                self.assertTrue(kw["creationflags"] & 0x08000000)
        self.assertTrue(steps)
        m = venv.read_marker(self.root)
        self.assertTrue(m["complete"])
        # second run: nothing to do
        tools2 = FakeTools()
        self.assertEqual(venv.ensure(self.root, run=tools2), (True, ""))
        self.assertEqual(tools2.calls, [])

    def test_requirements_change_reinstalls_without_rebuild(self):
        venv.ensure(self.root, run=FakeTools())
        with open(os.path.join(self.root, "requirements.txt"), "a") as fh:
            fh.write("psutil\n")
        self.assertEqual(venv.check(self.root)[0], "install")
        tools = FakeTools()
        self.assertTrue(venv.ensure(self.root, run=tools)[0])
        self.assertEqual([c[0][1:3] for c in tools.calls], [["-m", "pip"]])

    def test_corrupt_moved_and_python_gone_rebuild(self):
        for breakit, why in (
                (lambda: os.remove(venv.venv_python(self.root)), "damaged"),
                (lambda: self._marker(root=os.path.join(self.root, "elsewhere")), "moved"),
                (lambda: self._marker(complete=False), "did not finish"),
                (lambda: self._cfg_home(os.path.join(self.root, "no-python-here")), "gone")):
            venv.ensure(self.root, run=FakeTools())
            breakit()
            state, reason = venv.check(self.root)
            self.assertEqual(state, "rebuild")
            self.assertIn(why, reason)
            open(os.path.join(venv.venv_dir(self.root), "junk.txt"), "w").close()
            ok, _ = venv.ensure(self.root, run=FakeTools())
            self.assertTrue(ok)
            self.assertFalse(os.path.exists(os.path.join(venv.venv_dir(self.root), "junk.txt")))
            self.assertEqual(venv.check(self.root)[0], "ready")

    def test_never_deletes_a_folder_that_is_not_a_venv(self):
        d = venv.venv_dir(self.root)
        os.makedirs(d)
        open(os.path.join(d, "mine.txt"), "w").close()
        ok, msg = venv.ensure(self.root, run=FakeTools())
        self.assertFalse(ok)
        self.assertIn("leaves it alone", msg)
        self.assertTrue(os.path.isfile(os.path.join(d, "mine.txt")))

    def _marker(self, **kw):
        p = os.path.join(venv.venv_dir(self.root), venv.MARKER)
        m = json.load(open(p))
        m.update(kw)
        json.dump(m, open(p, "w"))

    def _cfg_home(self, home):
        with open(os.path.join(venv.venv_dir(self.root), "pyvenv.cfg"), "w") as fh:
            fh.write(f"home = {home}\n")


class FailureTests(Base):
    def test_no_venv_module(self):
        ok, msg = venv.ensure(self.root, run=FakeTools(venv_rc=1, venv_err="No module named venv"))
        self.assertFalse(ok)
        self.assertIn("cannot make virtual environments", msg)

    def test_offline_then_retry_next_launch(self):
        ok, msg = venv.ensure(self.root, run=FakeTools(pip_rc=1, pip_err="ERROR: Could not fetch URL"))
        self.assertFalse(ok)
        self.assertIn("offline", msg)
        self.assertEqual(venv.check(self.root)[0], "rebuild")      # half-done: rebuilt next launch
        self.assertTrue(venv.ensure(self.root, run=FakeTools())[0])

    def test_permission(self):
        ok, msg = venv.ensure(self.root, run=FakeTools(venv_rc=1, venv_err="[WinError 5] Access is denied"))
        self.assertIn("No permission", msg)

    def test_timeout_and_oserror(self):
        def slow(cmd, **kw):
            raise subprocess.TimeoutExpired(cmd, 1)
        self.assertIn("too long", venv.ensure(self.root, run=slow)[1])

        def gone(cmd, **kw):
            raise FileNotFoundError("python missing")
        self.assertFalse(venv.ensure(self.root, run=gone)[0])

    def test_unexpected_error_is_an_answer_not_a_crash(self):
        def bad(cmd, **kw):
            raise UnicodeDecodeError("cp1252", b"\x81", 0, 1, "undefined")
        ok, msg = venv.ensure(self.root, run=bad)
        self.assertFalse(ok)
        self.assertIn("UnicodeDecodeError", msg)

    def test_output_is_decoded_leniently(self):
        tools = FakeTools()
        venv.ensure(self.root, run=tools)
        for _cmd, kw in tools.calls:
            self.assertEqual((kw["encoding"], kw["errors"]), ("utf-8", "replace"))


class OptOutAndPrepareTests(Base):
    def test_env_and_config_opt_out(self):
        self.assertEqual(venv.opted_out(None, self.root), "")
        os.environ[venv.OPT_OUT_ENV] = "1"
        self.assertIn(venv.OPT_OUT_ENV, venv.opted_out(None, self.root))
        os.environ.pop(venv.OPT_OUT_ENV)
        with tempfile.TemporaryDirectory() as data:
            with open(os.path.join(data, "config.ini"), "w") as fh:
                fh.write("[Advanced]\nuse_venv = false\n")
            self.assertIn("use_venv", venv.opted_out(data, self.root))
            with open(os.path.join(data, "config.ini"), "w") as fh:
                fh.write("[Advanced]\nuse_venv = true\n")
            self.assertEqual(venv.opted_out(data, self.root), "")
        from msr.config import Config
        self.assertEqual(Config.DEFAULT_CONFIG["Advanced"]["use_venv"], "true")

    def test_prepare(self):
        os.environ[venv.OPT_OUT_ENV] = "yes"
        self.assertEqual(venv.prepare(self.root, show_window=False), (None, ""))
        os.environ.pop(venv.OPT_OUT_ENV)
        with mock.patch.object(venv, "ensure", return_value=(False, "Could not download packages.")):
            py, notice = venv.prepare(self.root, show_window=False)
        self.assertIsNone(py)
        self.assertIn("tries again next launch", notice)
        self.assertTrue(os.path.isfile(os.path.join(self.root, venv.ERROR_FILE)))
        venv.ensure(self.root, run=FakeTools())
        py, notice = venv.prepare(self.root, show_window=False)
        self.assertEqual(py, venv.venv_python(self.root, gui=True))
        self.assertEqual(notice, "")

    def test_launch_notice_is_taken_once(self):
        os.environ[venv.NOTICE_ENV] = "x failed"
        self.assertEqual(venv.take_launch_notice(), "x failed")
        self.assertNotIn(venv.NOTICE_ENV, os.environ)

    def test_error_file_ignored(self):
        lines = open(os.path.join(ROOT, ".gitignore"), encoding="utf-8").read().splitlines()
        self.assertIn(".venv/", lines)
        self.assertIn(venv.ERROR_FILE, lines)


class LaunchDataDirTests(unittest.TestCase):
    def test_command_line_data_dir_wins_and_is_not_added_twice(self):
        env = {"MSR_DATA": r"E:\env", "MSR_HOME": r"E:\home"}
        self.assertEqual(venv.launch_data_dir(["--data-dir", r"D:\d", "--headless"], env), (r"D:\d", []))
        self.assertEqual(venv.launch_data_dir([r'--data-dir="D:\d"'], env), (r"D:\d", []))

    def test_msr_data_is_passed_on_and_msr_home_is_not(self):
        self.assertEqual(venv.launch_data_dir([], {"MSR_DATA": r' "E:\env" '}), (r"E:\env", ["--data-dir", r"E:\env"]))
        self.assertEqual(venv.launch_data_dir(["--headless"], {"MSR_HOME": r"E:\home"}), (r"E:\home", []))
        self.assertEqual(venv.launch_data_dir([], {}), ("", []))


class CtlHandoffTests(unittest.TestCase):
    def test_handoff_to_venv_python(self):
        seen = []

        def run(cmd, **kw):
            seen.append((cmd, kw))
            return subprocess.CompletedProcess(cmd, 0, "out\n", "")
        with mock.patch.object(venv, "in_venv", return_value=False), \
             mock.patch.object(venv, "check", return_value=("ready", "")), \
             mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MSR_CTL_VENV_HANDOFF", None)
            self.assertEqual(msr_ctl._deps_in_venv(["deps", "list"], run=run), 0)
        self.assertEqual(seen[0][0][0], venv.venv_python(ROOT))
        self.assertEqual(seen[0][0][-2:], ["deps", "list"])
        self.assertEqual(seen[0][1]["env"]["MSR_CTL_VENV_HANDOFF"], "1")

    def test_no_handoff_when_inside_or_no_venv(self):
        with mock.patch.object(venv, "in_venv", return_value=True):
            self.assertIsNone(msr_ctl._deps_in_venv(["deps", "list"], run=None))
        with mock.patch.object(venv, "in_venv", return_value=False), \
             mock.patch.object(venv, "check", return_value=("missing", "")):
            self.assertIsNone(msr_ctl._deps_in_venv(["deps", "list"], run=None))


if __name__ == "__main__":
    unittest.main()
