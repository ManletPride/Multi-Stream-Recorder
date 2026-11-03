"""First-time setup window (msr/venv.py): pip lines -> status text, live streaming, queue
hand-off, and the failure view. Fake pip / temp folders only; the window stays hidden."""
import os
import queue
import subprocess
import sys
import tempfile
import time
import unittest

from msr import venv


PIP_LINES = [
    "Collecting yt-dlp>=2025.11.12 (from yt-dlp[default]>=2025.11.12->-r requirements.txt (line 9))",
    "  Downloading yt_dlp-2026.8.19-py3-none-any.whl.metadata (175 kB)",
    "Collecting streamlink>=8.0.0 (from -r requirements.txt (line 12))",
    "  Using cached streamlink-8.6.2-py3-none-any.whl.metadata (2.1 kB)",
    "Collecting certifi (from requests->streamlink)",
    "Downloading certifi-2026.1.4-py3-none-any.whl (163 kB)",
    "Installing collected packages: certifi, yt-dlp, streamlink",
    "Successfully installed certifi-2026.1.4 streamlink-8.6.2 yt-dlp-2026.8.19",
]


class FakeProc:
    def __init__(self, lines, rc=0):
        self.stdout = iter(line + "\n" for line in lines)
        self.rc = rc

    def wait(self):
        return self.rc

    def kill(self):
        pass


class ParseTests(unittest.TestCase):
    def test_pip_lines_become_status(self):
        p = venv.PipProgress(top_level=2)
        got = [p.feed(line) for line in PIP_LINES]
        self.assertEqual(got[0], "Looking up package 1 of 2: yt-dlp")
        self.assertEqual(got[1], "Downloading yt-dlp 2026.8.19 (175 kB)")
        self.assertEqual(got[2], "Looking up package 2 of 2: streamlink")
        self.assertEqual(got[3], "Using the cached download of streamlink")
        self.assertEqual(got[4], "Looking up certifi (dependency, 3 packages so far)")
        self.assertEqual(got[5], "Downloading certifi 2026.1.4 (163 kB)")
        self.assertTrue(got[6].startswith("Installing 3 packages"))
        self.assertEqual(p.total, 3)
        self.assertEqual(got[7], "Installed 3 packages")
        self.assertIsNone(p.feed("  some other noise"))
        self.assertEqual(p.feed("ERROR: Could not find a version"), "ERROR: Could not find a version")

    def test_count_requirements(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "r.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("# c\nyt-dlp[default]>=1  # x\n\n-r other.txt\nstreamlink\n   # indented\n")
            self.assertEqual(venv.count_requirements(path), 2)
            self.assertEqual(venv.count_requirements(os.path.join(d, "missing.txt")), 0)

    def test_drain_hands_over_in_order_with_a_limit(self):
        q = queue.Queue()
        for i in range(5):
            q.put(("status", f"s{i}") if i % 2 else ("line", f"l{i}"))
        status, lines = [], []
        self.assertEqual(venv.drain(q, status.append, lines.append, limit=3), 3)
        self.assertEqual((status, lines), (["s1"], ["l0", "l2"]))
        self.assertEqual(venv.drain(q, status.append, lines.append), 2)
        self.assertEqual(venv.fmt_elapsed(75.9), "1:15")


class StreamTests(unittest.TestCase):
    def test_real_subprocess_lines_arrive_live(self):
        seen = []
        code = "import sys,time\nfor i in range(3):\n    print('line', i, flush=True)\nsys.exit(2)"
        r = venv._stream([sys.executable, "-c", code], 30, seen.append)
        self.assertEqual(seen, ["line 0", "line 1", "line 2"])
        self.assertEqual(r.returncode, 2)

    def test_timeout_kills(self):
        code = "import time\nprint('x', flush=True)\ntime.sleep(30)"
        start = time.time()
        with self.assertRaises(subprocess.TimeoutExpired):
            venv._stream([sys.executable, "-c", code], 1, lambda _l: None)
        self.assertLess(time.time() - start, 20)


class EnsureStreamingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = self.tmp.name
        with open(os.path.join(self.root, "requirements.txt"), "w") as fh:
            fh.write("yt-dlp\nstreamlink\n")
        os.environ.pop(venv.OPT_OUT_ENV, None)

    def fake_venv(self, cmd, **_kw):
        d = cmd[3]
        sub = os.path.join(d, "Scripts" if os.name == "nt" else "bin")
        os.makedirs(sub, exist_ok=True)
        for n in (["python.exe", "pythonw.exe"] if os.name == "nt" else ["python"]):
            open(os.path.join(sub, n), "w").close()
        with open(os.path.join(d, "pyvenv.cfg"), "w") as fh:
            fh.write(f"home = {os.path.dirname(sys.executable)}\n")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    def test_status_log_and_file(self):
        status, lines = [], []
        ok, msg = venv.ensure(self.root, progress=status.append, run=self.fake_venv, log=lines.append,
                              popen=lambda cmd, **kw: FakeProc(PIP_LINES))
        self.assertTrue(ok, msg)
        self.assertIn("Looking up package 1 of 2: yt-dlp", status)
        self.assertIn("Downloading certifi 2026.1.4 (163 kB)", status)
        self.assertEqual(status[-1], "Done. Starting Multi-Stream Recorder\u2026")
        self.assertIn(PIP_LINES[-1], lines)
        with open(venv.setup_log_path(self.root), encoding="utf-8") as fh:
            text = fh.read()
        self.assertIn(PIP_LINES[0], text)
        self.assertIn("Setup finished.", text)

    def test_failure_message_and_log_kept(self):
        bad = PIP_LINES[:2] + ["ERROR: Could not fetch URL https://pypi.org/simple/x/"]
        ok, msg = venv.ensure(self.root, run=self.fake_venv, log=lambda _l: None,
                              popen=lambda cmd, **kw: FakeProc(bad, rc=1))
        self.assertFalse(ok)
        self.assertIn("Could not download packages", msg)
        with open(venv.setup_log_path(self.root), encoding="utf-8") as fh:
            self.assertIn("Could not fetch URL", fh.read())


class WindowTests(unittest.TestCase):
    def window(self):
        win = venv._progress_window(show=False)
        if win is None:
            self.skipTest("no display")
        return win

    def test_success_streams_then_closes(self):
        win = self.window()
        got = {}

        def fake_ensure(root, progress, log):
            for line in PIP_LINES:
                log(line)
            progress("Installed 3 packages")
            got["status"] = True
            return True, ""
        orig = win.set_status
        win.set_status = lambda t: (got.setdefault("seen", []).append(t), orig(t))
        self.assertEqual(venv.run_with_window(win, "X", ensure_fn=fake_ensure), (True, ""))
        self.assertEqual(win.lines, len(PIP_LINES))
        self.assertIn("Installed 3 packages", got["seen"])

    def test_failure_keeps_log_and_names_file(self):
        win = self.window()
        seen = {}

        def press_continue():
            if win.button.winfo_manager():
                seen["status"] = win.status.cget("text")
                seen["log"] = win.log.get("1.0", "end")
                win.button.invoke()
            else:
                win.root.after(50, press_continue)
        win.root.after(50, press_continue)
        res = venv.run_with_window(win, "C:/msr", ensure_fn=lambda root, progress, log: (log("ERROR: boom"), (False, "no network"))[1])
        self.assertEqual(res, (False, "no network"))
        self.assertIn("no network", seen["status"])
        self.assertIn(venv.SETUP_LOG, seen["status"])
        self.assertIn("ERROR: boom", seen["log"])


class WindowLookTests(unittest.TestCase):
    def test_themed_caption_scrollbar_and_vertical_resize(self):
        from tkinter import ttk
        from msr import themes
        win = venv._progress_window(show=False)
        if win is None:
            self.skipTest("no display")
        root = win.root
        self.addCleanup(root.destroy)
        self.assertTrue(win.themed)
        self.assertTrue(getattr(root, "_msr_title_bar", False))   # dialogs.apply_title_bar ran
        style = ttk.Style(root)
        self.assertEqual(style.theme_use(), "clam")                # not the white native look
        t = themes.current()
        self.assertEqual(str(style.lookup("Vertical.TScrollbar", "troughcolor")).lower(),
                         str(style.lookup(".", "troughcolor")).lower())
        self.assertEqual(str(style.lookup(".", "background")).lower(), t["bg"].lower())
        self.assertEqual(tuple(bool(x) for x in root.resizable()), (True, True))
        mw, mh = root.minsize()
        self.assertGreater(mh, 100)
        # taller window: the log takes the extra height
        root.geometry("+-3000+-3000")
        root.deiconify()
        root.update()
        before = win.log.winfo_height()
        root.geometry(f"{root.winfo_width()}x{root.winfo_height() + 200}")
        root.update()
        self.assertGreater(win.log.winfo_height(), before + 150)
        win.failed("x", "C:/msr/msr_venv_setup.log")
        root.update()
        self.assertTrue(win.button.winfo_ismapped())


if __name__ == "__main__":
    unittest.main()
