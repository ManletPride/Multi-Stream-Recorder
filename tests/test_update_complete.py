"""In-app updates bring every program file (launchers, msr_ctl, assets, docs) and never user
data or .venv; a 3.0 -> 3.x update end to end on temp folders."""
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from unittest import mock

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from msr import apply_update, update  # noqa: E402


def _write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data if isinstance(data, bytes) else data.encode())


def _read(path):
    with open(path, "rb") as fh:
        return fh.read().decode()


PROGRAM_30 = {
    "Multi-Stream-Recorder.py": "print('3.0')\n",
    "requirements.txt": "yt-dlp\n",
    "channels_fishtank.json": "[]\n",
    "run_msr.pyw": "print('launcher 3.0')\n",
    "run_msr.bat": "@echo off\nrem 3.0\n",
    "msr_ctl.py": "print('ctl 3.0')\n",
    "create_shortcut.py": "print('shortcut 3.0')\n",
    "msr/__init__.py": '__version__ = "3.0.0"\n',
    "msr/venv.py": "X = 30\n",
    "assets/icon.svg": "<svg>3.0</svg>",
    "docs/AGENT_GUIDE.md": "guide 3.0\n",
}
USER_FILES = {
    "config.ini": "[Fishtank]\npassword = secret\n",
    "channels.json": "[]\n",
    "cookies.txt": "# cookies\n",
    "run_msr.local.bat": "set MSR_DATA=D:\\x\n",
    ".venv/pyvenv.cfg": "home = C:\\py\n",
    ".venv/msr_venv.json": "{}\n",
    "msr_history/config.ini/1.ini": "old\n",
    "themes/mine.json": "{}\n",
    "stream_recorder.log": "log\n",
}


class ListTests(unittest.TestCase):
    def test_lists_in_step(self):
        self.assertEqual(set(apply_update.ROOT_FILES), set(update.ALLOWED_FILES))
        self.assertEqual(tuple(apply_update.PROGRAM_DIRS), tuple(update.ALLOWED_DIRS))
        self.assertEqual(apply_update.PRIVATE_NAMES, update.PRIVATE_NAMES)
        self.assertEqual(apply_update.PRIVATE_DIRS, update.PRIVATE_DIRS)

    def test_launchers_ctl_assets_docs_included(self):
        for rel in ("run_msr.pyw", "run_msr.bat", "msr_ctl.py", "create_shortcut.py",
                    "assets/icon.ico", "assets/icon_32.png", "docs/AGENT_GUIDE.md",
                    "docs/release_notes/RELEASE_NOTES_v2.0.0.md", "msr/venv.py"):
            self.assertTrue(update.is_program_file(rel), rel)

    def test_private_and_venv_never(self):
        for rel in ("config.ini", "channels.json", "cookies.txt", "cookies-yt.txt", ".env",
                    "run_msr.local.bat", "window_state.json", "msr_control.json", "msr_status.json",
                    ".venv/Scripts/python.exe", "msr/.venv/x.py", "docs/msr_history/a.ini",
                    "msr/__pycache__/x.pyc", "docs/config.ini", "assets/cookies.txt", "docs/x.har",
                    "msr/x.log", "docs/config.ini.broken-1", "tests/test_x.py", "README.md",
                    "msr_update_backup/msr/x.py", "themes/x.json"):
            self.assertFalse(update.is_program_file(rel), rel)
            self.assertIsNone(update._safe_relpath("pkg/" + rel, "pkg/"), rel)

    def test_every_tracked_program_file_is_covered(self):
        try:
            files = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True,
                                   check=True).stdout.split("\n")
        except (OSError, subprocess.CalledProcessError):
            self.skipTest("no git")
        for rel in files:
            if rel.startswith(("msr/", "assets/", "docs/")) or rel in ("run_msr.pyw", "run_msr.bat", "msr_ctl.py"):
                self.assertTrue(update.is_program_file(rel), rel)


class Update30To3xTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.install = os.path.join(self.tmp.name, "MSR")
        for rel, data in {**PROGRAM_30, **USER_FILES}.items():
            _write(os.path.join(self.install, rel), data)
        prefix = "Multi-Stream-Recorder-3.1.0/"
        self.release = {rel: data.replace("3.0", "3.1") for rel, data in PROGRAM_30.items()}
        self.release["docs/release_notes/RELEASE_NOTES_v3.1.0.md"] = "new notes\n"
        self.release["assets/icon_64.png"] = "png"
        self.release["requirements.txt"] = "yt-dlp\nnewpkg\n"
        extras = {"config.ini": "[x]\nhacked = 1\n", "tests/test_a.py": "x\n", "README.md": "readme\n",
                  ".venv/pyvenv.cfg": "home = evil\n", "docs/cookies.txt": "c\n"}
        self.zip = os.path.join(self.tmp.name, "r.zip")
        with zipfile.ZipFile(self.zip, "w") as zf:
            for rel, data in {**self.release, **extras}.items():
                zf.writestr(prefix + rel, data)

    def _stage(self):
        staging = os.path.join(self.tmp.name, "staging")
        written = update.extract_allowlisted(self.zip, staging)
        self.assertTrue(update.staging_is_valid(staging))
        self.assertEqual(sorted(written), sorted(self.release))
        return staging

    def test_full_update(self):
        apply_update.apply_staged_update(self._stage(), self.install)
        for rel, data in self.release.items():
            self.assertEqual(_read(os.path.join(self.install, rel)), data, rel)
        for rel, data in USER_FILES.items():
            self.assertEqual(_read(os.path.join(self.install, rel)), data, rel)
        self.assertFalse(os.path.exists(os.path.join(self.install, "tests")))
        self.assertFalse(os.path.exists(os.path.join(self.install, "README.md")))
        notice = _read(os.path.join(self.install, apply_update.NOTICE_FILE))
        self.assertIn(".venv", notice)
        self.assertIn("next launch", notice)
        backup = os.path.join(self.install, apply_update.BACKUP_DIR)
        self.assertEqual(_read(os.path.join(backup, "run_msr.pyw")), PROGRAM_30["run_msr.pyw"])
        self.assertEqual(_read(os.path.join(backup, "assets", "icon.svg")), PROGRAM_30["assets/icon.svg"])
        self.assertFalse(os.path.exists(os.path.join(backup, "config.ini")))
        self.assertFalse(os.path.exists(os.path.join(backup, ".venv")))

    def test_failed_copy_rolls_back_everything(self):
        staging = self._stage()
        real = apply_update._copy_files
        calls = []

        def flaky(src, dst, rels):
            calls.append(1)
            if len(calls) == 2:          # 1 = backup, 2 = the update itself
                real(src, dst, rels[: len(rels) // 2])
                raise OSError("disk full")
            return real(src, dst, rels)
        with mock.patch.object(apply_update, "_copy_files", flaky):
            with self.assertRaises(RuntimeError):
                apply_update.apply_staged_update(staging, self.install)
        for rel, data in {**PROGRAM_30, **USER_FILES}.items():
            self.assertEqual(_read(os.path.join(self.install, rel)), data, rel)
        self.assertFalse(os.path.exists(os.path.join(self.install, "assets", "icon_64.png")))
        self.assertFalse(os.path.exists(os.path.join(self.install, "docs", "release_notes",
                                                     "RELEASE_NOTES_v3.1.0.md")))

    def test_broken_launcher_refused(self):
        staging = self._stage()
        _write(os.path.join(staging, "run_msr.pyw"), "def broken(:\n")
        with self.assertRaises(RuntimeError):
            apply_update.apply_staged_update(staging, self.install)
        self.assertEqual(_read(os.path.join(self.install, "run_msr.pyw")), PROGRAM_30["run_msr.pyw"])


class RemovedSettingTests(unittest.TestCase):
    def test_old_concurrent_fragments_loads_quietly(self):
        from msr.config import Config
        self.assertNotIn("concurrent_fragments", Config.DEFAULT_CONFIG["Advanced"])
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "config.ini")
            _write(path, f"[Paths]\nstreams_dir = {d}\n[Advanced]\nconcurrent_fragments = 0\nverbose = true\n")
            with self.assertNoLogs(level="WARNING"):
                cfg = Config(path)
            self.assertTrue(cfg.getboolean("Advanced", "verbose"))
            self.assertTrue(cfg.save())
            Config(path)                               # reloads fine after a save


if __name__ == "__main__":
    unittest.main()
