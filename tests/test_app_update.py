"""App update decision: the release the dialog just found, a git checkout, files already newer.

Nothing is downloaded and git is never really run. The GUI asks with the plan's one prompt.
"""
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr.update import (  # noqa: E402
    GIT_NOTE,
    GIT_PULL_COMMAND,
    NO_RELEASE_MESSAGE,
    RESTART_TITLE,
    banner_text,
    choose_release,
    disk_is_newer,
    disk_version,
    fetch_latest_release,
    git_command,
    git_pull_ff_only,
    inspect_git,
    is_git_checkout,
    plan_app_update,
    source_note,
    zipball_url_is_allowed,
)

ZIP_314 = "https://api.github.com/repos/ManletPride/Multi-Stream-Recorder/zipball/v3.1.4"
HTML_314 = "https://github.com/ManletPride/Multi-Stream-Recorder/releases/tag/v3.1.4"
DIALOG_ROW = {
    "name": "msr", "latest": "3.1.4", "url": HTML_314, "zipball": ZIP_314,
    "status": "update_available", "updatable": True,
}


class _Resp:
    def __init__(self, payload: bytes):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def read(self):
        return self._payload


def _release_opener(tag="v3.1.4", zipball=ZIP_314):
    import json

    def opener(req, timeout=None):
        return _Resp(json.dumps({
            "tag_name": tag,
            "html_url": HTML_314,
            "zipball_url": zipball,
        }).encode())

    return opener


class ReleaseChoiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.plain = self.tmp.name
        self.addCleanup(self.tmp.cleanup)

    def test_stale_banner_uses_the_release_the_dialog_just_found(self):
        # The startup check never stored a tag. The Updates row did.
        chosen = choose_release(DIALOG_ROW, None)
        self.assertEqual(chosen["tag"], "3.1.4")
        self.assertEqual(chosen["zipball"], ZIP_314)
        plan = plan_app_update(self.plain, "3.1.3", chosen, recording=True)
        self.assertEqual(plan["action"], "zip")
        self.assertEqual(plan["tag"], "3.1.4")
        self.assertEqual(plan["zipball"], ZIP_314)
        self.assertTrue(zipball_url_is_allowed(plan["zipball"]))
        self.assertTrue(plan["confirm"])
        self.assertEqual(plan["title"], "Install update")
        self.assertIn("Download v3.1.4 from GitHub and restart Multi-Stream Recorder?", plan["message"])
        self.assertIn("Active recordings will be stopped first.", plan["message"])
        self.assertIn("config.ini, channels.json, cookies.txt, and recordings stay.", plan["message"])
        self.assertNotEqual(plan["action"], "git_pull")

    def test_dialog_release_wins_over_a_stale_banner_tag(self):
        stale = {"tag": "3.0.0", "url": "https://example/old", "zipball": "https://api.github.com/repos/a/b/zipball/v3.0.0"}
        chosen = choose_release(DIALOG_ROW, stale)
        self.assertEqual(chosen["tag"], "3.1.4")
        self.assertEqual(chosen["url"], HTML_314)
        plan = plan_app_update(self.plain, "3.1.3", chosen)
        self.assertEqual(plan["zipball"], ZIP_314)

    def test_banner_tag_is_the_fallback_when_the_dialog_has_no_release(self):
        chosen = choose_release({"latest": None, "status": "unchecked"}, {"tag": "3.1.4", "url": HTML_314, "zipball": ZIP_314})
        self.assertEqual(chosen["tag"], "3.1.4")
        plan = plan_app_update(self.plain, "3.1.3", chosen, recording=False)
        self.assertEqual(plan["action"], "zip")
        self.assertNotIn("Active recordings will be stopped first.", plan["message"])

    def test_disallowed_zipball_falls_back_to_the_tag_url(self):
        plan = plan_app_update(self.plain, "3.1.3", {"tag": "3.1.4", "url": HTML_314, "zipball": "https://evil.example/x.zip"})
        self.assertEqual(plan["action"], "zip")
        self.assertIn("/archive/refs/tags/v3.1.4.zip", plan["zipball"])
        self.assertTrue(zipball_url_is_allowed(plan["zipball"]))

    def test_no_tag_is_a_visible_refusal_not_a_silent_return(self):
        plan = plan_app_update(self.plain, "3.1.3", None)
        self.assertEqual(plan["action"], "blocked")
        self.assertEqual(plan["message"], NO_RELEASE_MESSAGE)
        self.assertFalse(plan["confirm"])
        self.assertIn("No newer version", plan["message"])

    def test_recheck_finds_the_release_when_nothing_was_stored(self):
        found, why = fetch_latest_release("3.1.3", opener=_release_opener())
        self.assertEqual(why, "")
        self.assertEqual(found["tag"], "3.1.4")
        self.assertEqual(found["zipball"], ZIP_314)
        plan = plan_app_update(self.plain, "3.1.3", found)
        self.assertEqual(plan["action"], "zip")
        self.assertEqual(plan["zipball"], ZIP_314)

    def test_recheck_says_why_when_github_is_unreachable_or_not_newer(self):
        def offline(req, timeout=None):
            raise urllib.error.URLError("timed out")

        found, why = fetch_latest_release("3.1.3", opener=offline)
        self.assertIsNone(found)
        self.assertIn("offline", why.lower())

        found, why = fetch_latest_release("3.1.4", opener=_release_opener(tag="v3.1.4"))
        self.assertIsNone(found)
        self.assertEqual(why, NO_RELEASE_MESSAGE)

    def test_gui_does_not_return_silently_when_the_tag_or_the_busy_flag_is_set(self):
        with open(os.path.join(ROOT, "msr", "gui.py"), encoding="utf-8") as fh:
            gui = fh.read()
        with open(os.path.join(ROOT, "msr", "deps_dialog.py"), encoding="utf-8") as fh:
            dialog = fh.read()
        self.assertNotIn("if _update_busy[0] or not _update_tag[0]:\n            return", gui)
        self.assertIn("choose_release", gui)
        self.assertIn("plan_app_update", gui)
        self.assertIn("BUSY_MESSAGE", gui)
        self.assertNotIn("Updating the app stops running recordings", dialog)


class GitCheckoutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.checkout = self.tmp.name
        self.addCleanup(self.tmp.cleanup)

    def _git(self, kind):
        path = os.path.join(self.checkout, ".git")
        if kind == "file":
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("gitdir: /nowhere\n")
        else:
            os.makedirs(path)

    def test_dot_git_file_or_directory_is_a_checkout(self):
        self.assertFalse(is_git_checkout(self.checkout))
        self._git("file")
        self.assertTrue(is_git_checkout(self.checkout))
        os.remove(os.path.join(self.checkout, ".git"))
        self._git("dir")
        self.assertTrue(is_git_checkout(self.checkout))
        self.assertEqual(source_note(self.checkout, "3.1.3"), GIT_NOTE)

    def test_clean_branch_tracking_origin_offers_pull_and_does_not_zip(self):
        self._git("dir")
        info = {
            "checkout": True, "clean": True, "tracks_origin": True, "detached": False,
            "branch": "main", "upstream": "origin/main", "error": "",
        }
        plan = plan_app_update(self.checkout, "3.1.3", DIALOG_ROW, recording=True, git_info=info)
        self.assertEqual(plan["action"], "git_pull")
        self.assertNotEqual(plan["action"], "zip")
        self.assertIsNone(plan["zipball"])
        self.assertTrue(plan["confirm"])
        self.assertIn("will not overwrite it with a release zip", plan["message"])
        self.assertIn(GIT_PULL_COMMAND, plan["message"])
        self.assertIn("main tracks origin/main", plan["message"])
        self.assertIn("Active recordings will be stopped first.", plan["message"])
        self.assertEqual(plan["command"], GIT_PULL_COMMAND)

    def test_dirty_or_untracked_branch_explains_and_shows_the_command(self):
        self._git("file")
        for info, needle in (
            ({"checkout": True, "clean": False, "tracks_origin": True, "detached": False,
              "branch": "main", "upstream": "origin/main", "error": ""},
             "local changes"),
            ({"checkout": True, "clean": True, "tracks_origin": False, "detached": False,
              "branch": "feature", "upstream": None, "error": ""},
             "does not track origin"),
            ({"checkout": True, "clean": True, "tracks_origin": False, "detached": False,
              "branch": "feature", "upstream": "fork/main", "error": ""},
             "tracks fork/main, not origin"),
            ({"checkout": True, "clean": True, "tracks_origin": False, "detached": True,
              "branch": None, "upstream": None, "error": ""},
             "HEAD is detached"),
        ):
            plan = plan_app_update(self.checkout, "3.1.3", DIALOG_ROW, git_info=info)
            self.assertEqual(plan["action"], "git_manual", info)
            self.assertFalse(plan["confirm"])
            self.assertIsNone(plan["zipball"])
            self.assertIn("git checkout", plan["message"].lower())
            self.assertIn(needle, plan["message"])
            self.assertIn(GIT_PULL_COMMAND, plan["message"])
            self.assertIn(git_command(self.checkout), plan["message"])

    def test_inspect_git_reads_status_and_upstream_without_a_real_repo(self):
        self._git("dir")
        calls = []

        def run(cmd, **kw):
            calls.append(cmd[3:])
            self.assertIs(kw["stdin"], subprocess.DEVNULL)
            key = tuple(cmd[3:])
            answers = {
                ("status", "--porcelain"): (0, "", ""),
                ("rev-parse", "--abbrev-ref", "HEAD"): (0, "main\n", ""),
                ("rev-parse", "--abbrev-ref", "@{upstream}"): (0, "origin/main\n", ""),
            }
            code, out, err = answers[key]
            return subprocess.CompletedProcess(cmd, code, out, err)

        info = inspect_git(self.checkout, run=run)
        self.assertEqual(calls, [
            ["status", "--porcelain"],
            ["rev-parse", "--abbrev-ref", "HEAD"],
            ["rev-parse", "--abbrev-ref", "@{upstream}"],
        ])
        self.assertTrue(info["clean"])
        self.assertTrue(info["tracks_origin"])
        self.assertEqual(info["branch"], "main")
        self.assertEqual(info["upstream"], "origin/main")

        def pull(cmd, **_kw):
            self.assertEqual(cmd[3:], ["pull", "--ff-only"])
            return subprocess.CompletedProcess(cmd, 0, "Already up to date.\n", "")

        ok, text = git_pull_ff_only(self.checkout, run=pull)
        self.assertTrue(ok)
        self.assertIn("Already up to date", text)

        def fail(cmd, **_kw):
            return subprocess.CompletedProcess(cmd, 1, "", "fatal: Not possible to fast-forward\n")

        ok, text = git_pull_ff_only(self.checkout, run=fail)
        self.assertFalse(ok)
        self.assertIn("Not possible to fast-forward", text)

    def test_a_normal_install_is_still_a_zip(self):
        self.assertFalse(is_git_checkout(self.checkout))
        plan = plan_app_update(self.checkout, "3.1.3", DIALOG_ROW)
        self.assertEqual(plan["action"], "zip")
        self.assertEqual(plan["zipball"], ZIP_314)


class DiskNewerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder = self.tmp.name
        os.makedirs(os.path.join(self.folder, "msr"))
        self.addCleanup(self.tmp.cleanup)

    def _write(self, version, git=False):
        with open(os.path.join(self.folder, "msr", "__init__.py"), "w", encoding="utf-8") as fh:
            fh.write(f'__version__ = "{version}"\n')
        if git:
            os.makedirs(os.path.join(self.folder, ".git"))

    def test_files_already_newer_restart_instead_of_downloading(self):
        self._write("3.1.4", git=True)
        self.assertEqual(disk_version(self.folder), "3.1.4")
        self.assertTrue(disk_is_newer(self.folder, "3.1.3"))
        self.assertFalse(disk_is_newer(self.folder, "3.1.4"))
        self.assertFalse(disk_is_newer(self.folder, "3.2.0"))
        self.assertEqual(source_note(self.folder, "3.1.3"), RESTART_TITLE)
        self.assertEqual(banner_text("3.1.3", self.folder, "3.1.4"),
                         "  Restart to finish updating — click to restart")
        info = {
            "checkout": True, "clean": True, "tracks_origin": True, "detached": False,
            "branch": "main", "upstream": "origin/main", "error": "",
        }
        plan = plan_app_update(self.folder, "3.1.3", DIALOG_ROW, recording=True, git_info=info)
        self.assertEqual(plan["action"], "restart")
        self.assertEqual(plan["title"], RESTART_TITLE)
        self.assertIn("Restart to finish updating?", plan["message"])
        self.assertIn("already v3.1.4", plan["message"])
        self.assertIn("this copy is still v3.1.3", plan["message"])
        self.assertIn("Active recordings will be stopped first.", plan["message"])
        self.assertTrue(plan["confirm"])
        self.assertIsNone(plan["zipball"])

    def test_same_version_on_disk_still_downloads_a_newer_release(self):
        self._write("3.1.3")
        self.assertEqual(source_note(self.folder, "3.1.3"), "")
        self.assertEqual(banner_text("3.1.3", self.folder, "3.1.4"),
                         "  v3.1.4 available — click to install")
        plan = plan_app_update(self.folder, "3.1.3", DIALOG_ROW)
        self.assertEqual(plan["action"], "zip")

    def test_unreadable_init_is_not_treated_as_newer(self):
        self.assertIsNone(disk_version(self.folder))
        self.assertFalse(disk_is_newer(self.folder, "3.1.3"))
        plan = plan_app_update(self.folder, "3.1.3", DIALOG_ROW)
        self.assertEqual(plan["action"], "zip")


class BannerTextTests(unittest.TestCase):
    def test_no_tag_and_not_ahead_hides_the_banner(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(banner_text("3.1.4", d, None))


if __name__ == "__main__":
    unittest.main()
