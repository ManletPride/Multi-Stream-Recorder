"""Terminal status viewer (msr/tui_data.py, msr_tui.py): parsing, formatting and a headless run.

Temp data folders only; nothing talks to a real MSR (log fetching is faked)."""
import asyncio
import importlib.util
import json
import os
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import control, themes, tui_data as td  # noqa: E402

HAS_TEXTUAL = importlib.util.find_spec("textual") is not None
THEME = themes.THEMES[themes.DEFAULT_THEME]


def fake_status(now, running=True):
    return {
        "protocol": 1, "api_version": 1,
        "app": {"name": "Multi-Stream Recorder", "version": "9.9.9", "pid": 1, "mode": "gui",
                "started_at": now - 100, "running": running},
        "updated_at": now - 1,
        "session": {"running": True, "recording": 2},
        "health": {"disk": {"main": {"path": "X", "free_gb": 812.4, "min_gb": 5, "low": False}}},
        "channels": [
            {"name": "kickone", "platform": "kick", "status": "Recording", "detail": "", "live": True,
             "recording": True, "size": "1.2 GB", "duration": "1:02:03", "last_live": "2026-11-04T12:00:00"},
            {"name": "youtube:abc", "platform": "youtube", "status": "Recording",
             "detail": "catching up from start", "live": True, "recording": True, "size": "", "duration": ""},
            {"name": "twitch:two", "platform": "twitch", "status": "Offline", "detail": "", "live": True,
             "recording": False},
            {"name": "three", "platform": "kick", "status": "Checking...", "live": False, "recording": False},
            {"name": "four", "platform": "rumble", "status": "Error", "detail": "drive not available",
             "error": "drive not available", "live": False, "recording": False},
            {"name": "five", "platform": "tiktok", "status": "Offline", "live": False, "recording": False},
        ],
    }


class ClassifyTests(unittest.TestCase):
    def test_states(self):
        snap = td.parse_status(fake_status(time.time()), theme=THEME)
        self.assertEqual([c.state for c in snap.channels],
                         [td.RECORDING, td.CATCHING_UP, td.LIVE, td.CHECKING, td.ERROR, td.OFFLINE])

    def test_processing_and_failed(self):
        self.assertEqual(td.classify({"status": "Remuxing..."}), td.PROCESSING)
        self.assertEqual(td.classify({"status": "Remux failed"}), td.ERROR)
        self.assertEqual(td.classify({}), td.OFFLINE)

    def test_colors_come_from_theme(self):
        snap = td.parse_status(fake_status(time.time()), theme=THEME)
        self.assertEqual(snap.channels[0].color, THEME.rec_fg)
        self.assertEqual(snap.channels[4].color, THEME.error_fg)
        for state, token in td.STATE_TOKEN.items():
            self.assertTrue(getattr(THEME, token).startswith("#"), state)


class ParseTests(unittest.TestCase):
    def test_header_fields(self):
        snap = td.parse_status(fake_status(time.time()), theme=THEME)
        self.assertTrue(snap.running)
        self.assertEqual((snap.version, snap.mode, snap.recording, snap.disk), ("9.9.9", "gui", 2, "812.4 GB"))
        self.assertEqual(snap.channels[0].status, "Recording")
        self.assertEqual(snap.channels[3].duration, "-")
        self.assertEqual(snap.channels[4].status, "Error (drive not available)")

    def test_stale_and_closed(self):
        now = time.time()
        stale = td.parse_status(fake_status(now - 60), now=now, theme=THEME)
        self.assertFalse(stale.running)
        self.assertIn("stale", stale.reason)
        closed = td.parse_status(fake_status(now, running=False), now=now, theme=THEME)
        self.assertFalse(closed.running)
        self.assertIn("closed", closed.reason)

    def test_tolerates_garbage(self):
        snap = td.parse_status({"channels": [None, "x", {}]}, theme=THEME)
        self.assertFalse(snap.running)
        self.assertEqual(len(snap.channels), 1)
        self.assertEqual(snap.disk, "?")

    def test_read_snapshot_missing_and_broken(self):
        with tempfile.TemporaryDirectory() as d:
            snap = td.read_snapshot(d)
            self.assertFalse(snap.found)
            self.assertIn("no msr_status.json", snap.reason)
            with open(os.path.join(d, control.STATUS_FILE), "w", encoding="utf-8") as fh:
                fh.write("{broken")
            self.assertIn("could not read", td.read_snapshot(d).reason)
            self.assertEqual(os.listdir(d), [control.STATUS_FILE])  # wrote nothing


class FormatTests(unittest.TestCase):
    def test_format(self):
        self.assertEqual(td.format_age(5), "5s")
        self.assertEqual(td.format_age(125), "2m 5s")
        self.assertEqual(td.format_age(7300), "2h 1m")
        self.assertEqual(td.format_age(None), "?")
        self.assertEqual(td.format_gb(2048), "2.00 TB")
        self.assertEqual(td.format_gb(None), "?")
        self.assertEqual(td.short_time(None), "-")
        self.assertEqual(td.short_time("2020-01-02T03:04:05"), "Jan 02 03:04")
        today = time.strftime("%Y-%m-%dT10:11:12")
        self.assertEqual(td.short_time(today), "10:11:12")

    def test_data_dir_order(self):
        env = {"MSR_HOME": "/h", "MSR_DATA": "/d"}
        self.assertEqual(td.resolve_data_dir("/c", env), os.path.abspath("/c"))
        self.assertEqual(td.resolve_data_dir(None, env), os.path.abspath("/h"))
        self.assertEqual(td.resolve_data_dir(None, {"MSR_DATA": '"/d"'}), os.path.abspath("/d"))
        self.assertEqual(td.resolve_data_dir(None, {}), os.getcwd())

    def test_fetch_logs(self):
        calls = []

        def fake_call(data_dir, cmd, args, timeout):
            calls.append((cmd, args))
            return {"ok": True, "result": {"lines": [f"l{i}" for i in range(20)]}}

        self.assertEqual(td.fetch_logs("x", 3, call=fake_call), ["l17", "l18", "l19"])
        self.assertEqual(calls, [("logs", {"lines": 3})])

        def down(*a, **k):
            raise control.NotRunning("no")
        self.assertEqual(td.fetch_logs("x", call=down), [])
        self.assertEqual(td.fetch_logs("x", call=lambda *a, **k: {"ok": False}), [])


def _load_tui():
    spec = importlib.util.spec_from_file_location("msr_tui_for_test", os.path.join(ROOT, "msr_tui.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class NoTextualTests(unittest.TestCase):
    def test_friendly_message(self):
        import contextlib
        import io
        mod = _load_tui()
        mod.App = None
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(mod.main([]), 1)
        self.assertIn("pip install", err.getvalue())


@unittest.skipUnless(HAS_TEXTUAL, "textual not installed (optional: requirements-tui.txt)")
class HeadlessAppTests(unittest.TestCase):
    def test_run_headless(self):
        mod = _load_tui()
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, control.STATUS_FILE), "w", encoding="utf-8") as fh:
                json.dump(fake_status(time.time()), fh)
            open(os.path.join(d, control.CONTROL_FILE), "w").close()  # "control on" (logs faked)
            app = mod.StatusApp(d, interval=60, fetch_logs=lambda _d: ["INFO hello", "ERROR boom"])

            async def go():
                async with app.run_test(size=(120, 30)) as pilot:
                    await pilot.pause(0.3)
                    table = app.query_one(mod.DataTable)
                    self.assertEqual(table.row_count, 6)
                    self.assertEqual(app.log_lines, ["INFO hello", "ERROR boom"])
                    self.assertIn("running", str(app.query_one("#header").render()))
                    os.remove(os.path.join(d, control.STATUS_FILE))
                    await pilot.press("r")
                    await pilot.pause(0.1)
                    self.assertFalse(app.snapshot.running)
                    self.assertEqual(table.row_count, 1)  # "(no channels)"
                    await pilot.press("q")
            asyncio.run(go())
            self.assertEqual(sorted(os.listdir(d)), [control.CONTROL_FILE])


if __name__ == "__main__":
    unittest.main()
