"""Startup safety: a missing drive, a start with no console, headless shutdown, ffmpeg_path.

Everything uses temp dirs and a temp data folder. Subprocesses run ``--headless`` (no dialogs)
with a fake config; nothing starts a recording or touches a real data folder.
"""
import ast
import importlib.util
import json
import logging
import os
import string
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from msr import deps, startup_error, util  # noqa: E402
from tests import HAS_FFMPEG  # noqa: E402
from msr.config import Config  # noqa: E402
from msr.recorder import StreamRecorder, finish_headless_session  # noqa: E402

SECRET = "hunter2-SECRET"


def missing_drive_path():
    """A path on a drive letter that does not exist (Windows), else a path under a plain file."""
    if os.name == "nt":
        for letter in reversed(string.ascii_uppercase):
            if not os.path.exists(f"{letter}:\\"):
                return f"{letter}:\\msr-test-streams"
    return None


def path_under_a_file(tmp):
    blocker = os.path.join(tmp, "not-a-folder")
    with open(blocker, "w") as f:
        f.write("x")
    return os.path.join(blocker, "streams")


def bad_streams_dir(tmp):
    return missing_drive_path() or path_under_a_file(tmp)


class LogDirFallbackTests(unittest.TestCase):
    def setUp(self):
        self.old_cwd = os.getcwd()
        self.tmp = tempfile.TemporaryDirectory()
        os.chdir(self.tmp.name)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        os.chdir(self.old_cwd)
        self.tmp.cleanup()

    def test_usable_folder_is_used_as_is(self):
        folder = os.path.join(self.tmp.name, "rec")
        self.assertEqual(util.resolve_log_dir(folder), (folder, None))
        self.assertTrue(os.path.isdir(folder))

    def test_unusable_folder_falls_back_instead_of_raising(self):
        bad = bad_streams_dir(self.tmp.name)
        folder, problem = util.resolve_log_dir(bad)
        self.assertTrue(problem)
        self.assertTrue(os.path.isdir(folder))
        self.assertNotEqual(os.path.normcase(folder), os.path.normcase(bad))

    @unittest.skipUnless(os.name == "nt", "drive letters are a Windows thing")
    def test_missing_drive_letter(self):
        folder, problem = util.resolve_log_dir(missing_drive_path())
        self.assertTrue(problem)
        self.assertTrue(os.path.isdir(folder))

    def test_setup_logging_does_not_raise_for_a_missing_folder(self):
        root = logging.getLogger()
        before = list(root.handlers)
        env = {k: os.environ.get(k) for k in (util.LOG_MAX_BYTES_ENV, util.LOG_BACKUPS_ENV)}
        try:
            log_file = util.setup_logging(bad_streams_dir(self.tmp.name))
            self.assertTrue(os.path.isfile(log_file))
            self.assertEqual(os.path.basename(log_file), "stream_recorder.log")
        finally:
            for handler in list(root.handlers):
                if handler not in before:
                    root.removeHandler(handler)
                    handler.close()
            for key, value in env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_validate_startup_reports_the_folder_instead_of_the_log_crashing_first(self):
        cfg = Config(os.path.join(self.tmp.name, "config.ini"))
        cfg.config.set("Paths", "streams_dir", bad_streams_dir(self.tmp.name))
        cfg.config.set("Paths", "channels_file", os.path.join(self.tmp.name, "channels.json"))
        with mock.patch.object(deps, "configure_ffmpeg", return_value="1.0"), \
                mock.patch.object(deps, "check_ffprobe", return_value=True):
            errors, _warnings = util.validate_startup(cfg)
        text = "\n".join(errors)
        self.assertTrue("does not exist" in text or "Cannot create streams directory" in text, text)


class StartupSubprocessTests(unittest.TestCase):
    """Run the real entry script headless against a streams folder that cannot exist."""

    def run_main(self, tmp, code=None):
        data = Path(tmp) / "data"
        data.mkdir()
        (data / "config.ini").write_text(
            f"[Paths]\nstreams_dir = {bad_streams_dir(tmp)}\n[Fishtank]\npassword = P@ss%word\n",
            encoding="utf-8")
        env = {k: v for k, v in os.environ.items() if k != "MSR_HOME"}
        env["PYTHONPATH"] = str(ROOT)
        if code is None:
            cmd = [sys.executable, str(ROOT / "Multi-Stream-Recorder.py"), "--headless",
                   "--data-dir", str(data)]
        else:
            cmd = [sys.executable, "-c", code, "--headless", "--data-dir", str(data)]
        done = subprocess.run(cmd, capture_output=True, text=True, timeout=120, cwd=tmp, env=env)
        return done, data

    @unittest.skipUnless(HAS_FFMPEG, "ffmpeg not installed: the ffmpeg error is reported first")
    def test_missing_drive_shows_the_friendly_message_headless(self):
        with tempfile.TemporaryDirectory() as tmp:
            done, data = self.run_main(tmp)
            self.assertEqual(done.returncode, 1, done.stderr[-2000:])
            self.assertNotIn("Traceback", done.stderr)
            self.assertTrue("does not exist" in done.stderr or "Cannot create streams directory" in done.stderr,
                            done.stderr[-2000:])
            log = data / "msr_startup_error.log"
            self.assertTrue(log.is_file(), "the message must also be saved for a start with no console")
            self.assertNotIn("P@ss", log.read_text(encoding="utf-8"))

    def test_uncaught_error_in_main_is_written_to_the_error_log(self):
        code = (
            "import sys, runpy, msr.util as u\n"
            "def boom(config):\n"
            "    raise RuntimeError('boom https://x.example/p?jwt=abc.def.ghi')\n"
            "u.validate_startup = boom\n"
            f"runpy.run_path({str(ROOT / 'Multi-Stream-Recorder.py')!r}, run_name='__main__')\n")
        with tempfile.TemporaryDirectory() as tmp:
            done, data = self.run_main(tmp, code)
            self.assertNotEqual(done.returncode, 0)
            text = (data / "msr_startup_error.log").read_text(encoding="utf-8")
            self.assertIn("RuntimeError", text)
            self.assertIn("boom", text)
            self.assertNotIn("abc.def.ghi", text, "the token must be redacted")

    def test_import_failure_is_written_to_the_error_log(self):
        code = (
            "import sys, runpy\n"
            "sys.modules['msr.gui'] = None\n"  # makes `from msr.gui import ...` raise ImportError
            f"runpy.run_path({str(ROOT / 'Multi-Stream-Recorder.py')!r}, run_name='__main__')\n")
        with tempfile.TemporaryDirectory() as tmp:
            done, data = self.run_main(tmp, code)
            self.assertNotEqual(done.returncode, 0)
            # at import time the data folder is not active yet: the log is in the current folder
            log = Path(tmp) / "msr_startup_error.log"
            self.assertTrue(log.is_file(), done.stderr[-1500:])
            self.assertIn("msr.gui", log.read_text(encoding="utf-8"))


class StartupErrorModuleTests(unittest.TestCase):
    def test_write_appends_and_caps_the_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = startup_error.write_error_log("one", tmp)
            startup_error.write_error_log("two", tmp)
            text = Path(first).read_text(encoding="utf-8")
            self.assertIn("one", text)
            self.assertIn("two", text)
            for _ in range(60):
                startup_error.write_error_log("x" * 2000, tmp)
            self.assertLessEqual(Path(first).stat().st_size, startup_error.MAX_LOG_BYTES + 4000)

    def test_exception_text_withholds_config_values_and_redacts_tokens(self):
        import configparser
        try:
            raise configparser.ParsingError(SECRET)
        except configparser.ParsingError as exc:
            text = startup_error.safe_exception_text(exc)
            self.assertNotIn(SECRET, text)
            self.assertNotIn(SECRET, startup_error.short_message(exc))
            self.assertIn("ParsingError", text)
        try:
            raise ValueError("bad url https://x/y?jwt=aaa.bbb.ccc")
        except ValueError as exc:
            self.assertNotIn("aaa.bbb.ccc", startup_error.safe_exception_text(exc))

    def test_report_failure_shows_a_dialog_only_with_a_gui(self):
        shown = []
        with tempfile.TemporaryDirectory() as tmp:
            startup_error.report_failure("T", "problem text", headless=True, directory=tmp,
                                         show=lambda *a: shown.append(a))
            self.assertEqual(shown, [])
            path = startup_error.report_failure("T", "problem text", headless=False, directory=tmp,
                                                show=lambda *a: shown.append(a))
            self.assertEqual(len(shown), 1)
            self.assertIn("problem text", shown[0][1])
            self.assertIn(path, shown[0][1])

    def test_dialog_goes_through_the_shared_helper_with_a_parent(self):
        fake_root = mock.MagicMock()
        with mock.patch.object(startup_error, "dialogs_allowed", return_value=True), \
                mock.patch("tkinter.Tk", return_value=fake_root), \
                mock.patch("msr.dialogs.show_error") as show_error:
            self.assertTrue(startup_error.show_dialog("Title", "Body"))
        show_error.assert_called_once_with(fake_root, "Title", "Body")
        fake_root.destroy.assert_called_once()

    def test_no_dialog_under_tests_ci_or_without_a_display(self):
        with mock.patch.dict(os.environ, {"MSR_NO_DIALOGS": "1"}), mock.patch("tkinter.Tk") as tk_:
            self.assertFalse(startup_error.show_dialog("Title", "Body"))
        tk_.assert_not_called()
        env = {k: v for k, v in os.environ.items() if k not in ("MSR_NO_DIALOGS", "MSR_SMOKE_TEST")}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertTrue(startup_error.dialogs_allowed() or sys.platform.startswith("linux"))
            os.environ["MSR_SMOKE_TEST"] = "1"
            self.assertFalse(startup_error.dialogs_allowed())
            os.environ.pop("MSR_SMOKE_TEST")
            if sys.platform.startswith("linux"):
                os.environ.pop("DISPLAY", None)
                os.environ.pop("WAYLAND_DISPLAY", None)
                self.assertFalse(startup_error.dialogs_allowed())

    def test_dialog_failure_is_not_an_error(self):
        with mock.patch.object(startup_error, "dialogs_allowed", return_value=True), \
                mock.patch("tkinter.Tk", side_effect=RuntimeError("no display")):
            self.assertFalse(startup_error.show_dialog("Title", "Body"))

    def test_launcher_failure_is_logged(self):
        """run_msr.pyw must not die silently when it cannot start the program."""
        code = (
            "import subprocess, runpy\n"
            "import msr.nowindow\n"
            "import msr.startup_error as se\n"
            "se.show_dialog = lambda *a, **k: False\n"
            "def fail(*a, **k):\n"
            "    raise FileNotFoundError('pythonw missing')\n"
            "subprocess.Popen = fail\n"
            "runpy.run_path('run_msr.pyw', run_name='__main__')\n")
        with tempfile.TemporaryDirectory() as tmp:
            env = dict(os.environ, MSR_DATA=tmp, PYTHONPATH=str(ROOT), MSR_NO_VENV="1")  # never build .venv here
            done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                                  timeout=60, cwd=str(ROOT), env=env)
            self.assertEqual(done.returncode, 0, done.stderr[-1500:])
            text = (Path(tmp) / "msr_startup_error.log").read_text(encoding="utf-8")
            self.assertIn("FileNotFoundError", text)

    def test_error_log_name_is_git_ignored(self):
        ignored = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        self.assertIn(startup_error.LOG_NAME, ignored)
        self.assertIn("config.ini.broken-*", ignored)


def load_entry_module():
    spec = importlib.util.spec_from_file_location("msr_entry_for_test", ROOT / "Multi-Stream-Recorder.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeRecorder:
    def __init__(self, calls, interrupt=True):
        self.calls = calls
        self.interrupt = interrupt
        self.manager = mock.MagicMock()
        self.manager.shutdown.side_effect = lambda: calls.append("manager.shutdown")

    def run(self):
        self.calls.append("run")
        if self.interrupt:
            raise KeyboardInterrupt

    def stop(self):
        self.calls.append("stop")

    def wait_for_cleanup(self, timeout=None):
        self.calls.append("wait_for_cleanup")
        return True


class HeadlessShutdownTests(unittest.TestCase):
    def test_finish_session_stops_then_waits_for_cleanup_then_releases_the_manager(self):
        calls = []
        said = []
        finish_headless_session(FakeRecorder(calls), say=said.append)
        self.assertEqual(calls, ["stop", "wait_for_cleanup", "manager.shutdown"])
        self.assertTrue(said)

    def test_ctrl_c_during_the_wait_skips_the_rest(self):
        calls = []
        rec = FakeRecorder(calls)
        rec.wait_for_cleanup = mock.Mock(side_effect=KeyboardInterrupt)
        said = []
        finish_headless_session(rec, say=said.append)
        self.assertIn("manager.shutdown", calls)
        self.assertTrue(any("skipped" in s for s in said))

    def test_wait_for_cleanup_joins_the_cleanup_thread_without_cancelling_it(self):
        release = threading.Event()
        done = []

        def work():
            release.wait(5)
            done.append(True)

        thread = threading.Thread(target=work, daemon=True)
        thread.start()
        recorder = StreamRecorder.__new__(StreamRecorder)
        recorder.cleaner = mock.MagicMock(_thread=thread)
        self.assertFalse(recorder.wait_for_cleanup(timeout=0.05), "still running: not finished")
        release.set()
        self.assertTrue(recorder.wait_for_cleanup(timeout=5))
        self.assertEqual(done, [True], "the cleanup ran to the end")
        recorder.cleaner.stop.assert_not_called()

    def test_wait_for_cleanup_without_a_cleaner_thread(self):
        recorder = StreamRecorder.__new__(StreamRecorder)
        recorder.cleaner = mock.MagicMock(_thread=None)
        self.assertTrue(recorder.wait_for_cleanup())

    def test_main_headless_runs_the_cleanup_after_ctrl_c(self):
        entry = load_entry_module()
        calls = []
        with tempfile.TemporaryDirectory() as tmp:
            channels = Path(tmp) / "channels.json"
            channels.write_text(json.dumps(["somechannel"]), encoding="utf-8")
            config = mock.MagicMock()
            config.get.return_value = str(channels)
            with mock.patch.object(entry, "StreamRecorder", return_value=FakeRecorder(calls)), \
                    mock.patch.object(entry.signal, "signal"), \
                    mock.patch("builtins.print"):
                entry.main_headless(config)
        self.assertEqual(calls, ["run", "stop", "wait_for_cleanup", "manager.shutdown"])

    def test_signal_handler_does_not_exit_the_process(self):
        entry = load_entry_module()
        handlers = {}
        calls = []
        with tempfile.TemporaryDirectory() as tmp:
            channels = Path(tmp) / "channels.json"
            channels.write_text(json.dumps(["somechannel"]), encoding="utf-8")
            config = mock.MagicMock()
            config.get.return_value = str(channels)
            rec = FakeRecorder(calls, interrupt=False)

            def run():
                calls.append("run")
                handlers[entry.signal.SIGINT](entry.signal.SIGINT, None)  # Ctrl+C arrives

            rec.run = run
            with mock.patch.object(entry, "StreamRecorder", return_value=rec), \
                    mock.patch.object(entry.signal, "signal",
                                      side_effect=lambda sig, fn: handlers.__setitem__(sig, fn)), \
                    mock.patch("builtins.print"):
                entry.main_headless(config)  # must return, not raise SystemExit
        self.assertEqual(calls, ["run", "stop", "wait_for_cleanup", "manager.shutdown"])


class FfmpegPathTests(unittest.TestCase):
    CONFIGURED = os.path.join(os.sep, "opt", "fake-tools", "my-ffmpeg-build.exe")

    def setUp(self):
        for name in ("FFMPEG_PATH", "FFMPEG_VERSION", "HAS_FFMPEG"):
            patcher = mock.patch.object(deps, name, getattr(deps, name))
            patcher.start()
            self.addCleanup(patcher.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def fake_run(self, calls):
        """Only the configured path 'works'; the bare name 'ffmpeg' is not on PATH."""
        def run(cmd, **kwargs):
            calls.append(cmd[0])
            if cmd[0] in ("ffmpeg", "ffprobe"):
                raise FileNotFoundError(cmd[0])
            return subprocess.CompletedProcess(cmd, 0, stdout="ffmpeg version N-999-fake Copyright\n", stderr="")
        return run

    def config(self, ffmpeg_path):
        cfg = Config(os.path.join(self.tmp.name, "config.ini"))
        cfg.config.set("Paths", "streams_dir", os.path.join(self.tmp.name, "rec"))
        cfg.config.set("Advanced", "ffmpeg_path", ffmpeg_path)
        cfg.config.set("Paths", "channels_file", os.path.join(self.tmp.name, "channels.json"))
        return cfg

    def test_check_ffmpeg_runs_the_given_path(self):
        calls = []
        with mock.patch.object(deps.subprocess, "run", self.fake_run(calls)):
            self.assertEqual(deps.check_ffmpeg(self.CONFIGURED), "N-999-fake")
            self.assertIsNone(deps.check_ffmpeg("ffmpeg"))
        self.assertEqual(calls, [self.CONFIGURED, "ffmpeg"])

    def test_configured_path_is_enough_when_ffmpeg_is_not_on_path(self):
        calls = []
        deps.HAS_FFMPEG, deps.FFMPEG_VERSION, deps.FFMPEG_PATH = False, None, "ffmpeg"  # as measured at import
        with mock.patch.object(deps.subprocess, "run", self.fake_run(calls)):
            errors, _warnings = util.validate_startup(self.config(self.CONFIGURED))
        self.assertFalse([e for e in errors if "ffmpeg" in e.lower()], errors)
        self.assertTrue(deps.HAS_FFMPEG)
        self.assertEqual(deps.FFMPEG_VERSION, "N-999-fake")
        self.assertIn(self.CONFIGURED, calls)

    def test_bad_configured_path_is_an_error_that_names_the_setting(self):
        deps.HAS_FFMPEG, deps.FFMPEG_VERSION, deps.FFMPEG_PATH = True, "7.0", "ffmpeg"
        with mock.patch.object(deps.subprocess, "run", side_effect=FileNotFoundError("x")):
            errors, _warnings = util.validate_startup(self.config(self.CONFIGURED))
        text = "\n".join(errors)
        self.assertIn("ffmpeg_path", text)
        self.assertIn(self.CONFIGURED, text)
        self.assertFalse(deps.HAS_FFMPEG)

    def test_ffprobe_next_to_the_configured_ffmpeg_is_checked_as_a_warning(self):
        probed = []

        def run(cmd, **kwargs):
            if os.path.basename(cmd[0]).startswith("ffprobe"):
                probed.append(cmd[0])
                raise FileNotFoundError(cmd[0])
            return subprocess.CompletedProcess(cmd, 0, stdout="ffmpeg version 7.1 x\n", stderr="")

        with mock.patch.object(deps.subprocess, "run", run):
            errors, warnings = util.validate_startup(self.config(self.CONFIGURED))
        self.assertFalse([e for e in errors if "ffmpeg" in e.lower()], errors)
        self.assertEqual(probed, [util.ffprobe_from_ffmpeg(self.CONFIGURED)])
        self.assertTrue(any("ffprobe" in w for w in warnings))

    def test_default_name_reuses_the_import_time_result(self):
        deps.HAS_FFMPEG, deps.FFMPEG_VERSION, deps.FFMPEG_PATH = True, "7.0", "ffmpeg"
        with mock.patch.object(deps.subprocess, "run", side_effect=AssertionError("must not run")):
            self.assertEqual(deps.configure_ffmpeg("ffmpeg"), "7.0")
            self.assertEqual(deps.configure_ffmpeg(""), "7.0")

    def test_gui_reads_the_flag_at_use_time(self):
        """gui.py must not copy HAS_FFMPEG at import: configure_ffmpeg updates it later."""
        tree = ast.parse((ROOT / "msr" / "gui.py").read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "msr.deps":
                names = {a.name for a in node.names}
                self.assertFalse({"HAS_FFMPEG", "FFMPEG_VERSION"} & names)


if __name__ == "__main__":
    unittest.main()
