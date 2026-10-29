"""Audit batch F: atomic writes (M6), update rollback (M12), credentials (M13), test hygiene (M11)."""

import io
import json
import os
import shutil
import stat
import sys
import tempfile
import unittest
import zipfile
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import msr.apply_update as apply_mod  # noqa: E402
import msr.history as history  # noqa: E402
import msr.recorder as recorder_mod  # noqa: E402
import msr.update as update_mod  # noqa: E402
import msr.config as config_mod  # noqa: E402


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def tree(root):
    out = {}
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            p = os.path.join(dirpath, name)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, root).replace(os.sep, "/")] = fh.read()
    return out


class FakeResponse(io.BytesIO):
    def __init__(self, data, url):
        super().__init__(data)
        self._url = url

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def release_zip(files, prefix="Multi-Stream-Recorder-9.9.9/"):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for rel, text in files.items():
            zf.writestr(prefix + rel, text)
    return buf.getvalue()


OLD = {
    "Multi-Stream-Recorder.py": "print('old launcher')\n",
    "requirements.txt": "psutil\n",
    "msr/__init__.py": '__version__ = "1.0.0"\n',
    "msr/worker.py": "OLD = 1\n",
}
NEW = {
    "Multi-Stream-Recorder.py": "print('new launcher')\n",
    "requirements.txt": "psutil\n",
    "msr/__init__.py": '__version__ = "9.9.9"\n',
    "msr/worker.py": "NEW = 1\n",
    "msr/brand_new.py": "X = 2\n",
    "tests/test_x.py": "ignored\n",
}


class UpdateTests(unittest.TestCase):
    """A fake GitHub release archive into a fake install; never the real one."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="msr-update-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.install = os.path.join(self.tmp, "install")
        for rel, text in OLD.items():
            write(os.path.join(self.install, rel), text)
        write(os.path.join(self.install, "config.ini"), "[Paths]\nstreams_dir = x\n")
        write(os.path.join(self.install, "channels.json"), "[]\n")
        self.before = tree(self.install)

    def stage(self, files):
        url = "https://codeload.github.com/ManletPride/Multi-Stream-Recorder/zip/refs/tags/v9.9.9"
        with mock.patch.object(update_mod.urllib.request, "urlopen",
                               return_value=FakeResponse(release_zip(files), url)):
            return update_mod.prepare_update(url, work_dir=os.path.join(self.tmp, "work"))

    def test_a_good_update_is_applied_and_the_old_files_are_kept(self):
        apply_mod.apply_staged_update(self.stage(NEW), self.install)
        self.assertEqual(read(os.path.join(self.install, "msr", "worker.py")), "NEW = 1\n")
        self.assertTrue(os.path.isfile(os.path.join(self.install, "msr", "brand_new.py")))
        self.assertFalse(os.path.exists(os.path.join(self.install, "tests")))
        backup = os.path.join(self.install, getattr(apply_mod, "BACKUP_DIR", "msr_update_backup"))
        self.assertEqual(read(os.path.join(backup, "msr", "worker.py")), "OLD = 1\n")
        self.assertEqual(read(os.path.join(self.install, "config.ini")), "[Paths]\nstreams_dir = x\n")

    def test_a_copy_failure_rolls_everything_back(self):
        staging = self.stage(NEW)
        real_copy = shutil.copy2
        seen = []

        def flaky_copy(src, dst, *a, **k):
            if os.path.abspath(src).startswith(os.path.abspath(staging)):
                seen.append(dst)
                if len(seen) == 3:
                    raise OSError("disk full")
            return real_copy(src, dst, *a, **k)
        with mock.patch.object(apply_mod.shutil, "copy2", flaky_copy):
            with self.assertRaises(Exception) as ctx:
                apply_mod.apply_staged_update(staging, self.install)
        self.assertIn("rolled back", str(ctx.exception))
        after = {k: v for k, v in tree(self.install).items()
                 if not k.startswith(apply_mod.BACKUP_DIR + "/")}
        self.assertEqual(after, self.before)

    def test_a_damaged_download_changes_nothing(self):
        broken = dict(NEW, **{"msr/worker.py": "def broken(:\n"})
        staging = self.stage(broken)
        with self.assertRaises(RuntimeError):
            apply_mod.apply_staged_update(staging, self.install)
        self.assertEqual(tree(self.install), self.before)

    def test_changed_requirements_leave_a_notice(self):
        apply_mod.apply_staged_update(self.stage(dict(NEW, **{"requirements.txt": "psutil\nnewdep\n"})),
                                      self.install)
        notice = os.path.join(self.install, apply_mod.NOTICE_FILE)
        self.assertIn("pip install -r requirements.txt", read(notice))

    def test_unchanged_requirements_leave_no_notice(self):
        apply_mod.apply_staged_update(self.stage(NEW), self.install)
        self.assertFalse(os.path.exists(os.path.join(self.install, apply_mod.NOTICE_FILE)))

    def test_the_helper_reports_a_failure_and_does_not_relaunch(self):
        staging = self.stage(dict(NEW, **{"msr/worker.py": "def broken(:\n"}))
        with mock.patch.object(apply_mod, "relaunch") as relaunch, \
                mock.patch.object(apply_mod, "wait_for_pid", return_value=True), \
                mock.patch.object(apply_mod.time, "sleep"):
            code = apply_mod.main(["--pid", "1", "--staging", staging, "--target", self.install,
                                   "--python", sys.executable, "--launcher", "x"])
        self.assertEqual(code, 1)
        relaunch.assert_not_called()
        self.assertIn("damaged", read(os.path.join(self.install, apply_mod.ERROR_FILE)))


class HistorySecretTests(unittest.TestCase):
    INI = "[Paths]\nstreams_dir = D:\\s\n\n[Fishtank]\nemail = a@b.c\npassword = hunter2-FAKE\n"

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="msr-hist-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.ini = os.path.join(self.tmp, "config.ini")
        write(self.ini, self.INI)

    def test_history_copies_do_not_hold_the_password(self):
        copy = history.snapshot(self.ini)
        text = read(copy)
        self.assertNotIn("hunter2-FAKE", text)
        self.assertIn("streams_dir = D:\\s", text)
        self.assertIn("email = a@b.c", text)
        if os.name != "nt":
            self.assertEqual(stat.S_IMODE(os.stat(copy).st_mode), 0o600)

    def test_restore_keeps_the_current_password(self):
        history.snapshot(self.ini)
        write(self.ini, self.INI.replace("D:\\s", "E:\\new").replace("hunter2-FAKE", "current-FAKE"))
        version = history.list_versions(self.ini)[0]
        history.restore(self.ini, version)
        text = read(self.ini)
        self.assertIn("streams_dir = D:\\s", text)
        self.assertIn("password = current-FAKE", text)
        self.assertNotIn(history.KEPT_OUT, text)

    def test_the_summary_does_not_report_the_kept_out_value(self):
        history.snapshot(self.ini)
        version = history.list_versions(self.ini)[0]
        with open(self.ini, "rb") as fh:
            current = fh.read()
        self.assertEqual(history.summarize("config.ini", version.read(), current),
                         "Same settings (formatting differs)")

    def test_channels_json_is_copied_unchanged(self):
        path = os.path.join(self.tmp, "channels.json")
        write(path, '["token_fan", {"name": "x", "enabled": true}]\n')
        self.assertEqual(read(history.snapshot(path)), read(path))


class FishtankPasswordTests(unittest.TestCase):
    class Cfg:
        def get(self, section, option, fallback=None):
            return "from-file-FAKE" if (section, option) == ("Fishtank", "password") else fallback

    def test_environment_wins_over_config(self):
        with mock.patch.dict(os.environ, {"MSR_FISHTANK_PASSWORD": "from-env-FAKE"}):
            self.assertEqual(config_mod.fishtank_password(self.Cfg()), "from-env-FAKE")
        with mock.patch.dict(os.environ, {"MSR_FISHTANK_PASSWORD": ""}):
            self.assertEqual(config_mod.fishtank_password(self.Cfg()), "from-file-FAKE")


class AtomicSaveTests(unittest.TestCase):
    """A failure in the middle of a save must leave the previous file whole (M6)."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="msr-atomic-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def failing_json(self, module):
        fake = mock.Mock(wraps=json)
        fake.dump.side_effect = OSError("disk full")
        fake.dumps.side_effect = OSError("disk full")
        return mock.patch.object(module, "json", fake)

    def test_kick_id_cache(self):
        path = os.path.join(self.tmp, "kick_channel_ids.json")
        write(path, '{"a": 1}')
        listener = object.__new__(recorder_mod.KickPushListener)
        listener._cache_path = path
        listener._ids = {"a": 1, "b": 2}
        with self.failing_json(recorder_mod), self.assertLogs(level="WARNING"):
            listener._save_id_cache()
        self.assertEqual(read(path), '{"a": 1}')
        listener._save_id_cache()
        self.assertEqual(json.loads(read(path)), {"a": 1, "b": 2})

    def test_window_state(self):
        try:
            import msr.gui as gui
        except ImportError as e:  # no tkinter
            self.skipTest(str(e))
        path = os.path.join(self.tmp, "window_state.json")
        write(path, '{"w": 1}')
        with self.failing_json(gui), self.assertLogs(level="WARNING"):
            gui.save_window_state(path, {"w": 2})
        self.assertEqual(read(path), '{"w": 1}')


if __name__ == "__main__":
    unittest.main()
