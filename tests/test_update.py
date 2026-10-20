"""In-app update: zipball allowlist extract, URL checks, staged copy."""
import io
import os
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.apply_update import apply_staged_update, pid_alive
from msr.update import (
    _archive_prefix,
    _safe_relpath,
    extract_allowlisted,
    staging_is_valid,
    zipball_url_for_tag,
    zipball_url_is_allowed,
)


def _build_zip(entries):
    """entries: dict relative_name -> bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    buf.seek(0)
    return buf


class ZipballUrlTests(unittest.TestCase):
    def test_github_https_ok(self):
        self.assertTrue(zipball_url_is_allowed(
            "https://github.com/ManletPride/Multi-Stream-Recorder/archive/refs/tags/v2.0.3.zip"
        ))
        self.assertTrue(zipball_url_is_allowed(
            "https://api.github.com/repos/ManletPride/Multi-Stream-Recorder/zipball/v2.0.3"
        ))
        self.assertTrue(zipball_url_is_allowed(
            "https://codeload.github.com/ManletPride/Multi-Stream-Recorder/zip/refs/tags/v2.0.3"
        ))

    def test_rejects_non_github(self):
        self.assertFalse(zipball_url_is_allowed("https://evil.example/update.zip"))
        self.assertFalse(zipball_url_is_allowed("http://github.com/x/y/z.zip"))
        self.assertFalse(zipball_url_is_allowed("file:///tmp/x.zip"))
        self.assertFalse(zipball_url_is_allowed(""))

    def test_tag_url_adds_v(self):
        url = zipball_url_for_tag("2.0.3")
        self.assertIn("/tags/v2.0.3.zip", url)
        self.assertTrue(zipball_url_is_allowed(url))


class SafeRelpathTests(unittest.TestCase):
    def test_allowlisted_root_files(self):
        prefix = "Multi-Stream-Recorder-2.0.3/"
        self.assertEqual(
            _safe_relpath(prefix + "Multi-Stream-Recorder.py", prefix),
            "Multi-Stream-Recorder.py",
        )
        self.assertEqual(
            _safe_relpath(prefix + "msr/__init__.py", prefix),
            "msr/__init__.py",
        )
        self.assertEqual(
            _safe_relpath(prefix + "channels_fishtank.json", prefix),
            "channels_fishtank.json",
        )

    def test_skips_user_data_and_tests(self):
        prefix = "pkg/"
        self.assertIsNone(_safe_relpath(prefix + "config.ini", prefix))
        self.assertIsNone(_safe_relpath(prefix + "channels.json", prefix))
        self.assertIsNone(_safe_relpath(prefix + "cookies.txt", prefix))
        self.assertIsNone(_safe_relpath(prefix + "tests/test_x.py", prefix))
        self.assertIsNone(_safe_relpath(prefix + "msr/__pycache__/x.pyc", prefix))

    def test_skips_dotdot(self):
        self.assertIsNone(_safe_relpath("pkg/msr/../../outside.py", "pkg/"))
        self.assertIsNone(_safe_relpath("../evil.py", ""))


class ExtractAllowlistTests(unittest.TestCase):
    def test_extracts_only_allowlisted(self):
        prefix = "Multi-Stream-Recorder-2.0.3/"
        buf = _build_zip({
            prefix + "Multi-Stream-Recorder.py": b"print('launcher')\n",
            prefix + "msr/__init__.py": b'__version__ = "2.0.3"\n',
            prefix + "msr/gui.py": b"# gui\n",
            prefix + "requirements.txt": b"yt-dlp\n",
            prefix + "channels_fishtank.json": b"[]\n",
            prefix + "config.ini": b"secret=1\n",
            prefix + "channels.json": b"[]\n",
            prefix + "tests/test_x.py": b"assert False\n",
            prefix + "AGENTS.md": b"nope\n",
        })
        with tempfile.TemporaryDirectory() as td:
            zpath = os.path.join(td, "r.zip")
            with open(zpath, "wb") as fh:
                fh.write(buf.getvalue())
            staging = os.path.join(td, "staging")
            written = extract_allowlisted(zpath, staging)
            self.assertTrue(staging_is_valid(staging))
            self.assertIn("Multi-Stream-Recorder.py", written)
            self.assertIn("msr/__init__.py", written)
            self.assertTrue(os.path.isfile(os.path.join(staging, "requirements.txt")))
            self.assertFalse(os.path.exists(os.path.join(staging, "config.ini")))
            self.assertFalse(os.path.exists(os.path.join(staging, "channels.json")))
            self.assertFalse(os.path.exists(os.path.join(staging, "tests")))
            self.assertFalse(os.path.exists(os.path.join(staging, "AGENTS.md")))

    def test_rejects_empty_allowlist(self):
        buf = _build_zip({"pkg/README.md": b"hi\n"})
        with tempfile.TemporaryDirectory() as td:
            zpath = os.path.join(td, "r.zip")
            with open(zpath, "wb") as fh:
                fh.write(buf.getvalue())
            with self.assertRaises(ValueError):
                extract_allowlisted(zpath, os.path.join(td, "staging"))

    def test_archive_prefix(self):
        names = [
            "Multi-Stream-Recorder-2.0.3/Multi-Stream-Recorder.py",
            "Multi-Stream-Recorder-2.0.3/msr/__init__.py",
        ]
        self.assertEqual(_archive_prefix(names), "Multi-Stream-Recorder-2.0.3/")


class ApplyStagedTests(unittest.TestCase):
    def test_copy_overwrites_program_keeps_user_files(self):
        with tempfile.TemporaryDirectory() as td:
            staging = os.path.join(td, "staging")
            target = os.path.join(td, "install")
            os.makedirs(os.path.join(staging, "msr"))
            os.makedirs(target)
            with open(os.path.join(staging, "Multi-Stream-Recorder.py"), "w") as fh:
                fh.write("new-launcher\n")
            with open(os.path.join(staging, "msr", "__init__.py"), "w") as fh:
                fh.write('__version__ = "9.9.9"\n')
            with open(os.path.join(target, "Multi-Stream-Recorder.py"), "w") as fh:
                fh.write("old-launcher\n")
            os.makedirs(os.path.join(target, "msr"))
            with open(os.path.join(target, "msr", "__init__.py"), "w") as fh:
                fh.write('__version__ = "0.0.1"\n')
            with open(os.path.join(target, "config.ini"), "w") as fh:
                fh.write("keep=me\n")
            with open(os.path.join(target, "channels.json"), "w") as fh:
                fh.write("[]\n")
            apply_staged_update(staging, target)
            with open(os.path.join(target, "Multi-Stream-Recorder.py")) as fh:
                self.assertEqual(fh.read(), "new-launcher\n")
            with open(os.path.join(target, "msr", "__init__.py")) as fh:
                self.assertIn("9.9.9", fh.read())
            with open(os.path.join(target, "config.ini")) as fh:
                self.assertEqual(fh.read(), "keep=me\n")
            with open(os.path.join(target, "channels.json")) as fh:
                self.assertEqual(fh.read(), "[]\n")

    def test_missing_staging_raises(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(RuntimeError):
                apply_staged_update(td, td)


class PidAliveTests(unittest.TestCase):
    def test_self_is_alive(self):
        self.assertTrue(pid_alive(os.getpid()))

    def test_bogus_pid(self):
        self.assertFalse(pid_alive(-1))


class DownloadGuardTests(unittest.TestCase):
    def test_download_rejects_bad_url(self):
        from msr.update import download_zipball
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(ValueError):
                download_zipball("https://example.com/x.zip", os.path.join(td, "x.zip"))


if __name__ == "__main__":
    unittest.main()
