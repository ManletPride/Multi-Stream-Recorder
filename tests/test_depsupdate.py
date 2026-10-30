"""Dependency check/update (msr/depsupdate.py, the Updates dialog, control 'deps', msr_ctl deps).
Network and pip are mocked; nothing is installed."""

import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import tkinter as tk
import unittest
import urllib.error
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import depsupdate  # noqa: E402
import msr.control as control  # noqa: E402
from msr.config import Config  # noqa: E402

_spec = importlib.util.spec_from_file_location("msr_ctl_for_deps_test", os.path.join(ROOT, "msr_ctl.py"))
msr_ctl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(msr_ctl)

LATEST = {"yt-dlp": "2099.1.1", "streamlink": "8.5.0", "curl_cffi": "0.16.0", "websocket-client": "1.9.0",
          "psutil": "7.0.0", "Pillow": "99.0", "pystray": "0.19.5", "plyer": "2.1.0", "colorama": "0.4.6"}
INSTALLED = dict(LATEST, **{"yt-dlp": "2026.8.19", "Pillow": "12.3.0", "plyer": None})


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def opener(req, timeout=None):
    url = req.full_url
    if "api.github.com" in url:
        return _Resp(json.dumps({"tag_name": "v99.0.0", "html_url": "https://example/rel"}).encode())
    name = url.split("/pypi/")[1].split("/")[0]
    return _Resp(json.dumps({"info": {"version": LATEST[name]}}).encode())


def offline(req, timeout=None):
    raise urllib.error.URLError("getaddrinfo failed")


def no_ffmpeg(path="ffmpeg", data_dir=None):
    return {"name": "ffmpeg", "label": "ffmpeg", "kind": "tool", "installed": "7.0", "latest": None,
            "status": "external", "source": "system PATH: x", "updatable": False, "note": "n"}


class FakePip:
    """Tracks an installed-version table; each call is a pip install command."""

    def __init__(self, table, fail=None, fail_rollback=False):
        self.table, self.fail, self.fail_rollback, self.calls = dict(table), fail or {}, fail_rollback, []

    def version_of(self, dist):
        return self.table.get(dist)

    def run(self, cmd, **kw):
        self.calls.append((cmd, kw))
        assert cmd[1:4] == ["-m", "pip", "install"], cmd
        spec = cmd[-1]
        dist, _, pin = spec.partition("==")
        if pin:
            if self.fail_rollback:
                return subprocess.CompletedProcess(cmd, 1, "", "ERROR: rollback broke")
            self.table[dist] = pin
            return subprocess.CompletedProcess(cmd, 0, "ok", "")
        if dist in self.fail:
            mode = self.fail[dist]
            if mode == "half":                  # pip broke the install halfway
                self.table[dist] = "broken"
            return subprocess.CompletedProcess(cmd, 1, "", {
                "perm": "ERROR: [WinError 32] The process cannot access the file because it is being used by another process",
                "offline": "WARNING: Retrying ... NewConnectionError ... ERROR: Could not fetch URL",
                "half": "ERROR: something odd"}[mode if mode != "half" else "half"])
        self.table[dist] = LATEST.get(dist, "1.0")
        return subprocess.CompletedProcess(cmd, 0, "Successfully installed", "")


class VersionTests(unittest.TestCase):
    def test_vkey(self):
        self.assertGreater(depsupdate.vkey("2026.10.26"), depsupdate.vkey("2026.8.19"))
        self.assertGreater(depsupdate.vkey("8.10.0"), depsupdate.vkey("8.9.9"))
        self.assertGreater(depsupdate.vkey("1.0"), depsupdate.vkey("1.0b1"))
        self.assertEqual(depsupdate.vkey("1.2.3"), depsupdate.vkey("1.2.3"))


class RowTests(unittest.TestCase):
    def setUp(self):
        p = mock.patch.object(depsupdate, "ffmpeg_row", no_ffmpeg)
        p.start()
        self.addCleanup(p.stop)

    def test_list_without_network(self):
        rs = depsupdate.rows(False, opener=offline, version_of=INSTALLED.get)
        by = {r["name"]: r for r in rs}
        self.assertEqual([r["name"] for r in rs], list(depsupdate.NAMES))
        self.assertEqual(by["yt-dlp"]["status"], "unchecked")
        self.assertEqual(by["plyer"]["status"], "not_installed")
        self.assertFalse(by["ffmpeg"]["updatable"])

    def test_check(self):
        with mock.patch("msr.__version__", "2.1.0"):
            rs = depsupdate.rows(True, opener=opener, version_of=INSTALLED.get)
        by = {r["name"]: r for r in rs}
        self.assertEqual(by["yt-dlp"]["status"], "update_available")
        self.assertTrue(by["yt-dlp"]["updatable"])
        self.assertEqual(by["streamlink"]["status"], "up_to_date")
        self.assertEqual(by["msr"]["latest"], "99.0.0")
        self.assertTrue(by["plyer"]["updatable"])          # missing package can be installed
        self.assertEqual(set(depsupdate.updates_available(rs)), {"msr", "yt-dlp", "pillow"})

    def test_offline(self):
        rs = depsupdate.rows(True, opener=offline, version_of=INSTALLED.get)
        for r in rs:
            if r["name"] != "ffmpeg":
                self.assertIn(r["status"], ("unknown", "not_installed"))
                self.assertIn("offline", r["note"])
        self.assertEqual(depsupdate.updates_available(rs), [])

    def test_http_404(self):
        def gh404(req, timeout=None):
            if "github" in req.full_url:
                raise urllib.error.HTTPError(req.full_url, 404, "nf", {}, None)
            return opener(req, timeout)
        rs = depsupdate.rows(True, opener=gh404, version_of=INSTALLED.get)
        self.assertIn("No published release", rs[0]["note"])


class FfmpegTests(unittest.TestCase):
    def test_path_ffmpeg_reported_not_updatable(self):
        with mock.patch("shutil.which", return_value=r"C:\ff\ffmpeg.exe"), \
             mock.patch("msr.deps.check_ffmpeg", return_value="7.1"):
            r = depsupdate.ffmpeg_row("ffmpeg")
        self.assertEqual((r["installed"], r["status"], r["updatable"]), ("7.1", "external", False))
        self.assertIn("PATH", r["source"])

    def test_missing(self):
        with mock.patch("shutil.which", return_value=None):
            r = depsupdate.ffmpeg_row("ffmpeg")
        self.assertEqual(r["status"], "not_installed")
        self.assertFalse(r["updatable"])


class UpdateTests(unittest.TestCase):
    def setUp(self):
        p = mock.patch.object(depsupdate, "in_venv", return_value=True)
        p.start()
        self.addCleanup(p.stop)

    def test_refused_outside_a_venv(self):
        pip = FakePip(INSTALLED)
        with mock.patch.object(depsupdate, "in_venv", return_value=False):
            res = depsupdate.update_packages(["yt-dlp"], run=pip.run, version_of=pip.version_of)
            self.assertEqual(res[0]["code"], "not_venv")
            self.assertIn("other programs", res[0]["error"])
            self.assertEqual(pip.calls, [])
            res = depsupdate.update_packages(["yt-dlp"], run=pip.run, version_of=pip.version_of, allow_system=True)
            self.assertTrue(res[0]["ok"])
    def test_success_hidden_and_this_python(self):
        pip = FakePip(INSTALLED)
        res = depsupdate.update_packages(["YT-DLP"], python="py.exe", run=pip.run, version_of=pip.version_of)
        self.assertEqual(res, [{"name": "yt-dlp", "old": "2026.8.19", "new": "2099.1.1", "ok": True,
                                "rolled_back": False}])
        cmd, kw = pip.calls[0]
        self.assertEqual(cmd[0], "py.exe")
        self.assertIn("--upgrade", cmd)
        self.assertEqual(kw["stdin"], subprocess.DEVNULL)
        if os.name == "nt":
            self.assertTrue(kw.get("creationflags", 0) & 0x08000000)   # CREATE_NO_WINDOW

    def test_file_in_use(self):
        pip = FakePip(INSTALLED, fail={"streamlink": "perm"})
        r = depsupdate.update_packages(["streamlink"], run=pip.run, version_of=pip.version_of)[0]
        self.assertFalse(r["ok"])
        self.assertIn("recording may be using", r["error"])
        self.assertEqual(r["new"], "8.5.0")
        self.assertEqual(len(pip.calls), 1)        # version unchanged: nothing to roll back

    def test_offline_message(self):
        pip = FakePip(INSTALLED, fail={"yt-dlp": "offline"})
        r = depsupdate.update_packages(["yt-dlp"], run=pip.run, version_of=pip.version_of)[0]
        self.assertIn("offline", r["error"])

    def test_rollback(self):
        pip = FakePip(INSTALLED, fail={"yt-dlp": "half"})
        r = depsupdate.update_packages(["yt-dlp"], run=pip.run, version_of=pip.version_of)[0]
        self.assertFalse(r["ok"])
        self.assertTrue(r["rolled_back"])
        self.assertEqual(pip.table["yt-dlp"], "2026.8.19")
        self.assertEqual(pip.calls[1][0][-1], "yt-dlp==2026.8.19")

    def test_rollback_fails(self):
        pip = FakePip(INSTALLED, fail={"yt-dlp": "half"}, fail_rollback=True)
        r = depsupdate.update_packages(["yt-dlp"], run=pip.run, version_of=pip.version_of)[0]
        self.assertFalse(r["rolled_back"])
        self.assertIn("yt-dlp==2026.8.19", r["error"])

    def test_pip_missing_or_timeout(self):
        def boom(cmd, **kw):
            raise subprocess.TimeoutExpired(cmd, 1)
        r = depsupdate.update_packages(["psutil"], run=boom, version_of=INSTALLED.get)[0]
        self.assertIn("longer than", r["error"])

        def nopy(cmd, **kw):
            raise FileNotFoundError("no python")
        r = depsupdate.update_packages(["psutil"], run=nopy, version_of=INSTALLED.get)[0]
        self.assertIn("Could not run pip", r["error"])

    def test_msr_ffmpeg_unknown_refused(self):
        pip = FakePip(INSTALLED)
        res = depsupdate.update_packages(["msr", "ffmpeg", "nope"], run=pip.run, version_of=pip.version_of)
        self.assertTrue(all(not r["ok"] for r in res))
        self.assertIn("own updater", res[0]["error"])
        self.assertEqual(pip.calls, [])

    def test_one_update_at_a_time(self):
        pip = FakePip(INSTALLED)
        with depsupdate._UPDATING:   # e.g. the dialog is mid-update while msr_ctl asks too
            r = depsupdate.update_packages(["yt-dlp"], run=pip.run, version_of=pip.version_of)[0]
        self.assertEqual(r["code"], "busy")
        self.assertEqual(pip.calls, [])
        self.assertTrue(depsupdate.update_packages(["yt-dlp"], run=pip.run, version_of=pip.version_of)[0]["ok"])

    def test_pip_output_decoded_leniently(self):
        pip = FakePip(INSTALLED)
        depsupdate.update_packages(["yt-dlp"], run=pip.run, version_of=pip.version_of)
        self.assertEqual((pip.calls[0][1]["encoding"], pip.calls[0][1]["errors"]), ("utf-8", "replace"))


class DailyTests(unittest.TestCase):
    def test_due_and_remember(self):
        self.assertTrue(depsupdate.due({}))
        self.assertTrue(depsupdate.due({"deps_check": {"time": "x"}}))
        self.assertFalse(depsupdate.due({"deps_check": {"time": 1000}}, now=1000 + 3600))
        self.assertTrue(depsupdate.due({"deps_check": {"time": 1000}}, now=1000 + depsupdate.DAY))
        with tempfile.TemporaryDirectory() as d:
            f = os.path.join(d, "window_state.json")
            with open(f, "w") as fh:
                json.dump({"geometry": "1x1"}, fh)
            depsupdate.remember(f, [{"name": "yt-dlp", "status": "update_available"}], now=5)
            from msr import winstate
            st = winstate.load(f)
            self.assertEqual(st["geometry"], "1x1")
            self.assertEqual(depsupdate.remembered_updates(st), ["yt-dlp"])

    def test_default_on(self):
        self.assertEqual(Config.DEFAULT_CONFIG["Updates"]["daily_check"], "true")


class ControlTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        path = os.path.join(self.tmp.name, "config.ini")
        with open(path, "w") as fh:
            fh.write(f"[Paths]\nstreams_dir = {os.path.join(self.tmp.name, 's')}\n")
        self.ctl = control.Controller(control.OfflineApp(Config(path)))
        p = mock.patch.object(depsupdate, "in_venv", return_value=True)
        p.start()
        self.addCleanup(p.stop)
        for target, new in (("ffmpeg_row", no_ffmpeg),):
            p = mock.patch.object(depsupdate, target, new)
            p.start()
            self.addCleanup(p.stop)

    def test_list_check_update(self):
        r = self.ctl.handle("deps", {"action": "list"})
        self.assertTrue(r["ok"])
        self.assertEqual(len(r["result"]["items"]), len(depsupdate.NAMES))
        fake_rows = [{"name": "yt-dlp", "status": "update_available", "updatable": True},
                     {"name": "msr", "status": "update_available", "updatable": True}]
        with mock.patch.object(depsupdate, "rows", return_value=fake_rows):
            r = self.ctl.handle("deps", {"action": "check"})
            self.assertEqual(r["result"]["updates"], ["yt-dlp", "msr"])
            self.assertTrue(os.path.isfile(os.path.join(self.tmp.name, "window_state.json")))
            with mock.patch.object(depsupdate, "update_packages",
                                   return_value=[{"name": "yt-dlp", "ok": True, "old": "1", "new": "2"}]) as up:
                r = self.ctl.handle("deps", {"action": "update", "all": True})
        up.assert_called_once_with(["yt-dlp"], allow_system=False)          # msr is left to its own updater
        self.assertTrue(r["result"]["ok_all"])

    def test_refused_outside_venv(self):
        with mock.patch.object(depsupdate, "in_venv", return_value=False), \
             mock.patch.object(depsupdate, "update_packages") as up:
            r = self.ctl.handle("deps", {"action": "update", "names": ["yt-dlp"]})
            self.assertEqual(r["code"], "not_venv")
            up.assert_not_called()
            up.return_value = [{"name": "yt-dlp", "ok": True}]
            r = self.ctl.handle("deps", {"action": "update", "names": ["yt-dlp"], "system": True})
            self.assertTrue(r["ok"])
            self.assertTrue(up.call_args.kwargs["allow_system"])

    def test_bad_requests(self):
        self.assertEqual(self.ctl.handle("deps", {"action": "zap"})["code"], "bad_request")
        self.assertEqual(self.ctl.handle("deps", {"action": "update"})["code"], "bad_request")
        self.assertEqual(self.ctl.handle("deps", {"action": "update", "names": ["nope"]})["code"], "not_found")

    def test_api_version_unchanged(self):
        self.assertEqual(control.API_VERSION, 1)


class CliTests(unittest.TestCase):
    def req(self, *argv):
        return msr_ctl.to_request(msr_ctl.build_parser().parse_args(list(argv)))

    def test_parse(self):
        self.assertEqual(self.req("deps", "list"), ("deps", {"action": "list"}))
        self.assertEqual(self.req("deps", "update", "--all"), ("deps", {"action": "update", "all": True}))
        self.assertEqual(self.req("deps", "update", "yt-dlp"), ("deps", {"action": "update", "names": ["yt-dlp"]}))
        for bad in (("deps", "update"), ("deps", "update", "x", "--all"), ("deps", "check", "x")):
            with self.assertRaises(SystemExit):
                self.req(*bad)
        self.assertNotIn("deps", msr_ctl.LIVE_ONLY)
        self.assertEqual(self.req("deps", "update", "pillow", "--system"),
                         ("deps", {"action": "update", "names": ["pillow"], "system": True}))

    def test_human(self):
        out = msr_ctl.human("deps", {"ok": True, "result": {"results": [
            {"name": "yt-dlp", "ok": False, "error": "x", "rolled_back": True}], "note": "n"}})
        self.assertIn("previous version restored", out)


class DialogTests(unittest.TestCase):
    def test_dialog_check_and_update(self):
        try:
            root = tk.Tk()
        except tk.TclError:
            self.skipTest("no display")
        root.withdraw()
        self.addCleanup(root.destroy)
        from msr import deps_dialog, dialogs
        rows_unchecked = [{"name": "yt-dlp", "label": "yt-dlp", "installed": "1", "latest": None,
                           "status": "unchecked", "source": "pip", "updatable": False, "note": ""}]
        rows_checked = [dict(rows_unchecked[0], latest="2", status="update_available", updatable=True)]
        rows_done = [dict(rows_unchecked[0], installed="2", latest="2", status="up_to_date")]
        cfg = mock.Mock()
        cfg.getboolean.return_value = True
        badges = []
        seq = iter([rows_unchecked, rows_checked, rows_done])
        with tempfile.TemporaryDirectory() as d, \
             mock.patch.object(dialogs, "present", lambda w, p: None), \
             mock.patch.object(depsupdate, "rows", lambda *a, **k: next(seq)), \
             mock.patch.object(depsupdate, "update_packages",
                               return_value=[{"name": "yt-dlp", "ok": True, "old": "1", "new": "2"}]) as up:
            win = deps_dialog.open_deps_dialog(root, config=cfg, state_file=os.path.join(d, "ws.json"),
                                               on_badge=badges.append)
            self.assertEqual(win.state(), "withdrawn")
            st = win._deps_state
            self._wait(root, st)
            self.assertEqual(badges[-1], ["yt-dlp"])
            win.nametowidget  # noqa
            deps_dialog_buttons = [w for w in win.winfo_children()[0].winfo_children()]
            self.assertTrue(deps_dialog_buttons)
            # Update all
            [b for b in self._buttons(win) if b.cget("text") == "Update all"][0].invoke()
            self._wait(root, st)
            up.assert_called_once()
            self.assertEqual(up.call_args[0][0], ["yt-dlp"])
            self.assertEqual(badges[-1], [])
            self.assertIs(deps_dialog.open_deps_dialog(root, config=cfg, state_file="x", check_now=False), win)
            win.event_generate("<Escape>")
            root.update()
            self.assertFalse(win.winfo_exists())

    def _buttons(self, w):
        out = []
        for c in w.winfo_children():
            if c.winfo_class() == "TButton":
                out.append(c)
            out.extend(self._buttons(c))
        return out

    def _wait(self, root, st):
        import time
        end = time.time() + 5
        root.update()
        while (st["busy"]) and time.time() < end:
            root.update()
            time.sleep(0.02)
        root.update()
        self.assertFalse(st["busy"])

    def test_summary(self):
        s = deps_dialog_summary = __import__("msr.deps_dialog", fromlist=["summary"]).summary
        self.assertIn("previous version restored",
                      s([{"name": "a", "ok": False, "error": "e", "rolled_back": True}]))
        self.assertIn("1 -> 2", deps_dialog_summary([{"name": "a", "ok": True, "old": "1", "new": "2"}]))


if __name__ == "__main__":
    unittest.main()
