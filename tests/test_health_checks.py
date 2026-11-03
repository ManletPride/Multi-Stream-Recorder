"""Health checks (msr/health.py) and the bottom-bar status (msr/health_ui.py). Temp folders,
fake tools / sizes / status tables; hidden windows, no network."""
import io
import json
import os
import tempfile
import time
import tkinter as tk
import unittest

from msr import depsupdate, dialogs, health
from msr import health_ui


import logging
logging.getLogger("msr.health.test").propagate = False
logging.getLogger("msr.health.test").addHandler(logging.NullHandler())


class Cfg:
    def __init__(self, **v):
        self.v = {("Recording", "min_disk_space_gb"): "5", ("Health", "check_minutes"): "10"}
        self.v.update(v)

    def get(self, s, k, fallback=None):
        return self.v.get((s, k), fallback)

    def getfloat(self, s, k, fallback=None):
        v = self.v.get((s, k))
        return fallback if v is None else float(v)


class StartupTests(unittest.TestCase):
    def test_tools(self):
        issues = health.tool_issues({"ffmpeg": "7.1", "streamlink": None, "yt-dlp": "2026.8.19"})
        self.assertEqual([i.key for i in issues], ["tool:streamlink"])
        self.assertEqual(issues[0].action, health.UPDATES)
        self.assertEqual(len(health.tool_issues({})), 3)

    def test_folder_writable_and_space(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(health.probe_writable(d))
            self.assertEqual(os.listdir(d), [])          # the probe file is gone again
            self.assertEqual(health.folder_issues(d, 500.0, 5), [])
            self.assertEqual(health.folder_issues(d, 12.0, 5)[0].level, health.WARN)   # below 15 GB
            self.assertEqual(health.folder_issues(d, 3.0, 5)[0].level, health.ERROR)
            self.assertEqual(health.folder_issues(d, 8.0, 0)[0].level, health.WARN)    # 10 GB when no minimum
            missing = health.folder_issues(os.path.join(d, "nope"), 500.0, 5)
            self.assertEqual(missing[0].level, health.WARN)
            locked = health.folder_issues(d, 500.0, 5, probe=lambda _f: "cannot write there (denied)")
            self.assertEqual(locked[0].level, health.ERROR)

    def test_twitch_single_validate(self):
        calls = []

        class Rejected(Exception):
            pass

        def reject(tok):
            calls.append(tok)
            raise Rejected()
        self.assertIsNone(health.twitch_issue(None, True, reject, Rejected))
        self.assertIsNone(health.twitch_issue({"a": 1}, False, reject, Rejected))
        self.assertEqual(calls, [])
        self.assertEqual(health.twitch_issue({"a": 1}, True, reject, Rejected).key, "twitch")
        self.assertEqual(len(calls), 1)
        self.assertIsNone(health.twitch_issue({"a": 1}, True, lambda t: (_ for _ in ()).throw(OSError()), Rejected))

    def test_stale_versions(self):
        now = time.mktime((2026, 11, 3, 0, 0, 0, 0, 0, -1))
        versions = {"yt-dlp": "2026.08.19", "streamlink": "8.6.2"}
        # no daily check yet: an old date-versioned yt-dlp gets a note
        self.assertEqual([i.key for i in health.stale_issues(versions, {}, now)], ["stale:yt-dlp"])
        state = {"deps_check": {"time": now, "updates": ["streamlink"],
                                "released": {"streamlink": {"version": "8.6.2", "date": "2026-09-01T10:00:00Z"}}}}
        got = health.stale_issues(versions, state, now)
        self.assertEqual([i.key for i in got], ["stale:streamlink"])   # yt-dlp: no newer one known
        self.assertIn("days old", got[0].text)
        fresh = {"yt-dlp": "2026.10.30"}
        self.assertEqual(health.stale_issues(fresh, {}, now), [])
        self.assertIsNone(health.version_date("8.6.2"))


class PeriodicTests(unittest.TestCase):
    def test_stalled_recording(self):
        sizes = {"a.ts": 100}
        m = health.Monitor(size_of=lambda p: sizes[p])
        st = {"kick:alice": {"status": "Recording", "raw_file": "a.ts"}}
        self.assertEqual(m.stalled(st, 0), [])
        self.assertEqual(m.stalled(st, 200), [])
        got = m.stalled(st, 400)
        self.assertEqual(got[0].key, "stall:kick:alice")
        sizes["a.ts"] = 150                                 # grows again: fine
        self.assertEqual(m.stalled(st, 500), [])
        self.assertEqual(m.stalled({}, 600), [])
        self.assertEqual(m._grow, {})

    def test_site_failing_needs_a_streak(self):
        m = health.Monitor()
        st = {"twitch:a": {"status": "Error", "detail": "403"}, "twitch:b": {"status": "Error", "detail": "x"},
              "kick:c": {"status": "Offline"}}
        self.assertEqual(m.failing_sites(st), [])
        got = m.failing_sites(st)
        self.assertEqual([i.key for i in got], ["site:twitch"])
        self.assertIn("streamlink", got[0].text)
        st["twitch:b"] = {"status": "Offline"}               # one works: not the tool
        self.assertEqual(m.failing_sites(st), [])
        disk = {"youtube:x": {"status": "Error", "detail": "Low disk space: 1.0GB"},
                "youtube:y": {"status": "Error", "detail": "drive not available"}}
        for _ in range(4):
            self.assertEqual(m.failing_sites(disk), [])     # storage errors are not the tool's fault

    def test_single_channel_site_waits_longer(self):
        m = health.Monitor()
        st = {"rumble:x": {"status": "Error", "detail": "boom"}}
        self.assertEqual(m.failing_sites(st), [])
        self.assertEqual(m.failing_sites(st), [])
        self.assertIn("yt-dlp", m.failing_sites(st)[0].text)

    def test_merge_summary_changes(self):
        a = health.Issue(health.WARN, "disk", "low")
        b = health.Issue(health.ERROR, "tool:yt-dlp", "yt-dlp does not run")
        r = health.merge([a], [b])
        self.assertEqual(r.level, health.ERROR)
        self.assertEqual(r.issues[0].key, "tool:yt-dlp")
        self.assertIn("2 problems", health.summary(r))
        self.assertTrue(health.summary(health.merge()).startswith("\u25cf Health OK"))
        new, cleared = health.changes(None, r)
        self.assertEqual(len(new), 2)
        new, cleared = health.changes(r, health.merge([a]))
        self.assertEqual((new, cleared), ([], ["tool:yt-dlp"]))

    def test_interval(self):
        self.assertEqual(health_ui.interval_minutes(Cfg()), 10)
        for text, want in (("0", 0), ("0.2", 1), ("x", 10), ("30", 30)):
            cfg = Cfg()
            cfg.v[("Health", "check_minutes")] = text
            self.assertEqual(health_ui.interval_minutes(cfg), want)


class PypiDateTests(unittest.TestCase):
    def test_one_request_gives_latest_and_installed_date(self):
        body = {"info": {"version": "8.7.0"}, "releases": {"8.6.2": [{"upload_time_iso_8601": "2026-09-01T10:00:00Z"}]}}
        urls = []

        class Resp(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def opener(req, timeout=None):
            urls.append(req.full_url)
            return Resp(json.dumps(body).encode())
        self.assertEqual(depsupdate.pypi_info("streamlink", opener, "8.6.2"), ("8.7.0", "2026-09-01T10:00:00Z"))
        self.assertEqual(depsupdate.pypi_info("streamlink", opener, "1.0"), ("8.7.0", None))
        self.assertEqual(len(urls), 2)
        rows = depsupdate.rows(True, opener=opener, version_of=lambda d: "8.6.2" if d == "streamlink" else None)
        sl = next(r for r in rows if r["name"] == "streamlink")
        self.assertEqual(sl["installed_released"], "2026-09-01T10:00:00Z")
        with tempfile.TemporaryDirectory() as d:
            f = os.path.join(d, "window_state.json")
            depsupdate.remember(f, rows, now=5)
            with open(f, encoding="utf-8") as fh:
                saved = json.load(fh)[depsupdate.STATE_KEY]
            self.assertEqual(saved["released"]["streamlink"], {"version": "8.6.2", "date": "2026-09-01T10:00:00Z"})
            self.assertIn("streamlink", saved["updates"])


class ControllerTests(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.status = {}
        self.updates = []
        self.versions = {"ffmpeg": "7", "streamlink": "8.6.2", "yt-dlp": "2026.10.30"}
        self.ctl = health_ui.HealthController(
            self.root, config=Cfg(), get_status=lambda: self.status, get_folder=lambda: self.tmp.name,
            free_gb=lambda _f: 500.0, get_versions=lambda: dict(self.versions), get_state=lambda: {},
            open_updates=lambda: self.updates.append(1), autostart=False,
            logger=__import__('logging').getLogger('msr.health.test'))
        self.addCleanup(self.ctl.stop)
        self.ctl.label.pack()

    def wait(self):
        self.ctl.report = None
        end = time.time() + 10
        while self.ctl.report is None and time.time() < end:
            self.root.update()
            time.sleep(0.02)
        self.assertIsNotNone(self.ctl.report)

    def test_ok_then_warning_then_details(self):
        self.ctl.run(startup=True)
        self.wait()
        self.assertTrue(self.ctl.label.cget("text").startswith("\u25cf Health OK"))
        self.assertEqual(str(self.ctl.label.cget("style")), "Ok.TLabel")
        self.versions["yt-dlp"] = None
        self.ctl.check_now()
        self.wait()
        self.assertIn("yt-dlp does not run", self.ctl.label.cget("text"))
        self.assertEqual(str(self.ctl.label.cget("style")), "Error.TLabel")
        shown = []
        orig = dialogs._reveal
        dialogs._reveal = lambda win, on_top: shown.append(win)   # hidden: no window appears
        self.addCleanup(setattr, dialogs, "_reveal", orig)
        self.ctl.show_details()
        texts = [w.cget("text") for w in self.ctl._body.winfo_children()]
        self.assertTrue(any("yt-dlp does not run" in t for t in texts))
        updates = [w for w in self.ctl._dialog.winfo_children()[0].winfo_children()[-1].winfo_children()
                   if w.cget("text") == "Updates\u2026"]
        updates[0].invoke()
        self.assertEqual(self.updates, [1])
        self.ctl._dialog.destroy()

    def test_periodic_keeps_startup_issues_and_schedules(self):
        self.versions["streamlink"] = None
        self.ctl.run(startup=True)
        self.wait()
        self.assertIsNotNone(self.ctl._next)                 # next periodic pass is planned
        self.ctl.run(startup=False)
        self.wait()
        self.assertEqual([i.key for i in self.ctl.report.issues], ["tool:streamlink"])
        self.ctl.config.v[("Health", "check_minutes")] = "0"
        self.ctl.run(startup=False)
        self.wait()
        self.assertIsNone(self.ctl._next)                    # 0 = periodic checks off


if __name__ == "__main__":
    unittest.main()
