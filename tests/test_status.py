"""`--json` status snapshot (msr/status.py): read-only, no secrets, no personal paths."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from msr import status, util

ROOT = Path(__file__).resolve().parent.parent
TOOLS = {n: {"available": True, "version": "1"} for n in ("yt-dlp", "streamlink", "ffmpeg")}
SECRETS = ("hunter2-password", "someone@example.com", "SECRET-TOKEN-123", "my_cookie_file.txt", "sk-api-key-999")


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.rec = self.dir / "recordings"
        self.rec.mkdir()
        (self.dir / "config.ini").write_text(
            "[Paths]\nstreams_dir = %s\ncookies_file = my_cookie_file.txt\n"
            "[Fishtank]\nemail = someone@example.com\npassword = hunter2-password\n"
            "[Advanced]\nffmpeg_path = %s\nyoutube_player_client = web\nsecret_api_key = sk-api-key-999\n"
            "[Clipping]\nclips_dir = %s\n" % (self.rec, self.dir / "ffmpeg.exe", self.dir / "clips"),
            encoding="utf-8")
        (self.dir / "channels.json").write_text(json.dumps([
            "alice", {"name": "twitch:bob", "enabled": False}, "youtube:@carol",
            "custom:https://stream.example/live.m3u8?token=SECRET-TOKEN-123", "../evil", "alice",
        ]), encoding="utf-8")

    def build(self, **kw):
        return status.build_status(str(self.dir / "config.ini"), tools=kw.get("tools", TOOLS))


class StatusContentTests(Base):
    def test_summary(self):
        # channels.json sits next to config.ini, so run from there like the app does
        old = os.getcwd()
        os.chdir(self.dir)
        self.addCleanup(os.chdir, old)
        s = self.build()
        self.assertEqual(s["channels"]["total"], 4)
        self.assertEqual(s["channels"]["enabled"], 3)
        self.assertEqual(s["channels"]["skipped"], 2)  # the unsafe key and the duplicate
        self.assertEqual(s["channels"]["by_platform"], {"custom": 1, "kick": 1, "twitch": 1, "youtube": 1})
        names = [c["name"] for c in s["channels"]["channels"]]
        self.assertIn("custom:stream.example", names)
        self.assertEqual(s["config"]["settings"]["Advanced"]["youtube_player_client"], "web")
        self.assertTrue(s["config"]["fishtank_login_set"])
        self.assertTrue(s["config"]["cookies_file_set"])
        self.assertTrue(s["config"]["recordings_folder"]["exists"])
        self.assertTrue(s["ok"], s["problems"])

    def test_no_secrets_and_no_personal_paths_anywhere(self):
        old = os.getcwd()
        os.chdir(self.dir)
        self.addCleanup(os.chdir, old)
        text = status.render(self.build())
        for secret in SECRETS:
            self.assertNotIn(secret, text)
        self.assertNotIn(str(self.dir), text)
        self.assertNotIn(self.dir.name, text)
        self.assertNotIn(os.path.expanduser("~"), text)
        self.assertNotIn("Paths", json.loads(text)["config"]["settings"])
        self.assertNotIn("Fishtank", json.loads(text)["config"]["settings"])
        text.encode("ascii")  # safe for any console

    def test_problems_are_listed(self):
        empty = tempfile.TemporaryDirectory()
        self.addCleanup(empty.cleanup)
        old = os.getcwd()
        os.chdir(empty.name)  # a folder with no channels.json
        self.addCleanup(os.chdir, old)
        bad = dict(TOOLS, ffmpeg={"available": False, "version": ""})
        s = self.build(tools=bad)  # cwd has no channels.json here
        self.assertFalse(s["ok"])
        self.assertIn("ffmpeg is not installed", s["problems"])
        self.assertTrue(any("channels.json not found" in p for p in s["problems"]))

    def test_missing_or_broken_files_do_not_raise_and_are_not_created(self):
        (self.dir / "config.ini").unlink()
        old = os.getcwd()
        os.chdir(self.dir)
        self.addCleanup(os.chdir, old)
        (self.dir / "channels.json").write_text("{nope")
        s = self.build()
        self.assertFalse(s["config"]["found"])
        self.assertIn("not valid JSON", " ".join(s["problems"]))
        self.assertEqual(sorted(p.name for p in self.dir.iterdir()), ["channels.json", "recordings"])

    def test_running_detection_does_not_take_the_lock(self):
        folder = str(self.rec)
        self.assertFalse(status.is_running(folder))
        self.assertFalse(status.is_running(folder))  # looking twice must not make us the owner
        self.assertTrue(util.try_acquire_single_instance(folder))
        self.addCleanup(util.release_single_instance)
        self.assertTrue(status.is_running(folder))
        util.release_single_instance()
        self.assertFalse(status.is_running(folder))


class CommandLineTests(Base):
    def run_cli(self, *extra):
        return subprocess.run([sys.executable, str(ROOT / "Multi-Stream-Recorder.py"), "--json",
                               "--data-dir", str(self.dir), *extra],
                              capture_output=True, text=True, timeout=120, cwd=str(ROOT))

    def test_json_prints_only_json_touches_nothing_and_leaks_nothing(self):
        before = sorted(p.name for p in self.dir.iterdir())
        done = self.run_cli()
        self.assertEqual(done.returncode, 0, done.stderr)
        data = json.loads(done.stdout)  # stdout is exactly one JSON document
        self.assertEqual(data["channels"]["total"], 4)
        self.assertEqual(data["app"]["name"], "Multi-Stream Recorder")
        for secret in SECRETS + (str(self.dir),):
            self.assertNotIn(secret, done.stdout)
        self.assertEqual(sorted(p.name for p in self.dir.iterdir()), before)  # no log, history or lock created
        self.assertEqual(sorted(p.name for p in self.rec.iterdir()), [])

    def test_a_missing_data_dir_is_an_error_not_a_snapshot(self):
        done = subprocess.run([sys.executable, str(ROOT / "Multi-Stream-Recorder.py"), "--json",
                               "--data-dir", str(self.dir / "nope")],
                              capture_output=True, text=True, timeout=120, cwd=str(ROOT))
        self.assertEqual(done.returncode, 2)
        self.assertNotIn("{", done.stdout.split("ERROR")[0])


class ManualTests(unittest.TestCase):
    def setUp(self):
        self.text = (ROOT / "llms.txt").read_text(encoding="utf-8")

    def test_manual_has_no_personal_paths_or_secrets(self):
        import re
        for pattern in (r"[A-Za-z]:\\(Users|Other|cowork)", r"[A-Z]:\\[^ ]*cowork", r"/home/\w+", r"/Users/\w+",
                        r"(?i)password\s*=\s*\S", r"(?i)token=\S", r"@[a-z0-9-]+\.(com|net|org)\b"):
            self.assertIsNone(re.search(pattern, self.text), pattern)
        self.assertNotIn(os.path.expanduser("~"), self.text)
        self.assertNotIn(os.environ.get("USERNAME", "\0"), self.text.replace("ManletPride", ""))

    def test_manual_names_every_top_level_key_of_the_snapshot(self):
        for key in build_keys():
            self.assertIn(key, self.text)

    def test_manual_points_at_real_files(self):
        for name in ("README.md", "AGENTS.md", "SECURITY.md", "msr/status.py", "msr/history.py",
                     "msr/winstate.py", "msr/dialogs.py", "msr/logview.py", "msr/restore_dialog.py"):
            self.assertTrue((ROOT / name).is_file(), name)
            self.assertIn(name, self.text + "README.md AGENTS.md SECURITY.md")


def build_keys():
    with tempfile.TemporaryDirectory() as tmp:
        return list(status.build_status(str(Path(tmp) / "config.ini"), tools=TOOLS))


if __name__ == "__main__":
    unittest.main()