"""The real window builds and survives its first refresh ticks (a launch that dies silently
under pythonw looks like "a window flashes and goes away"), and no UI name is defined twice."""
import ast
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GUI = ROOT / "msr" / "gui.py"
WIDGET_MODULES = {"tk", "ttk", "tkinter"}


def _own_scope_nodes(func):
    """Statements of `func` itself, not of the functions/classes defined inside it."""
    stack = list(func.body)
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        stack.extend(ast.iter_child_nodes(node))


def duplicate_ui_names(source: str, function: str = "main_gui") -> dict[str, list[int]]:
    """Names that main_gui binds more than once to a widget, or to a widget and a def.

    main_gui is one huge closure: a second `filter_entry = ttk.Entry(...)` far below the first
    silently replaces it for every function that uses the name (apply_theme then called
    `.configure(bg=...)` on a ttk.Entry and the app died on start)."""
    tree = ast.parse(source)
    func = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == function)
    bound: dict[str, list[int]] = {}
    for node in _own_scope_nodes(func):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            callee = node.value.func
            if (isinstance(callee, ast.Attribute) and isinstance(callee.value, ast.Name)
                    and callee.value.id in WIDGET_MODULES):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        bound.setdefault(target.id, []).append(node.lineno)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            bound.setdefault(node.name, []).append(node.lineno)
    return {name: sorted(lines) for name, lines in bound.items() if len(lines) > 1}


class DuplicateNameTests(unittest.TestCase):
    def test_the_checker_catches_the_original_bug(self):
        src = ("def main_gui():\n    e = tk.Entry(a)\n    def theme():\n        e.configure(bg='x')\n"
               "    e = ttk.Entry(b)\n    def f(): pass\n    def f(): pass\n    def g():\n        h = tk.Frame()\n"
               "        h = tk.Frame()\n")
        self.assertEqual(duplicate_ui_names(src), {"e": [2, 5], "f": [6, 7]})  # nested g's own h is its business

    def test_main_gui_binds_no_widget_or_function_name_twice(self):
        self.assertEqual(duplicate_ui_names(GUI.read_text(encoding="utf-8")), {})


class LaunchSmokeTests(unittest.TestCase):
    def launch(self, dark: bool):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            (data / "rec").mkdir()
            (data / "config.ini").write_text(
                f"[Paths]\nstreams_dir = {data / 'rec'}\n[GUI]\nminimize_to_tray = false\nnotifications = false\n",
                encoding="utf-8")
            (data / "channels.json").write_text("[]", encoding="utf-8")
            (data / "window_state.json").write_text(json.dumps({"dark_mode": dark}), encoding="utf-8")
            env = dict(os.environ, MSR_SMOKE_TEST="1")
            env.pop("MSR_HOME", None)
            return subprocess.run(
                [sys.executable, str(ROOT / "Multi-Stream-Recorder.py"), "--data-dir", str(data)],
                capture_output=True, text=True, timeout=90, cwd=str(ROOT), env=env)

    def check(self, dark):
        try:
            import tkinter
            probe = tkinter.Tk()
            probe.destroy()
        except Exception as exc:  # no display (headless CI)
            self.skipTest(f"no display: {exc}")
        done = self.launch(dark)
        self.assertEqual(done.returncode, 0, done.stderr[-3000:])
        self.assertNotIn("Traceback", done.stderr)  # also catches errors in the first refresh callbacks

    def test_dark_theme_builds_and_runs(self):
        self.check(True)

    def test_light_theme_builds_and_runs(self):
        self.check(False)


STREAMS_BUTTON_SCRIPT = r"""
import json, os, sys, tkinter as tk
from tkinter import ttk
from unittest import mock
sys.path.insert(0, sys.argv[1])
os.chdir(sys.argv[2])
os.environ["MSR_SMOKE_TEST"] = "1"
out = {}

def fake_mainloop(self, n=0):
    try:
        self.update_idletasks(); self.update()
        def find(w, text):
            for c in w.winfo_children():
                if c.winfo_class() == "TButton" and c.cget("text") == text:
                    return c
                r = find(c, text)
                if r:
                    return r
        streams, clips = find(self, "Streams"), find(self, "Clips")
        out["found"] = streams is not None and clips is not None
        out["same_row"] = streams.master is clips.master
        out["left_of_clips"] = streams.winfo_x() < clips.winfo_x()
        header = clips.master
        out["header_req"], out["header_width"] = header.winfo_reqwidth(), header.winfo_width()
        out["root_width"] = self.winfo_width()
        import msr.util, msr.gui
        with mock.patch.object(msr.gui._dialogs, "show_info") as info, \
                mock.patch.object(msr.util, "open_local_path") as opener:
            streams.invoke()                      # folder exists: opens it
            out["opened"] = [c.args[0] for c in opener.call_args_list]
            out["popups_when_present"] = info.call_count
            os.rmdir(sys.argv[3])
            streams.invoke()                      # folder missing: themed message, nothing created
            out["opened_after_missing"] = len(opener.call_args_list)
            out["popup"] = [str(a) for a in info.call_args.args[1:]] if info.called else None
            out["recreated"] = os.path.exists(sys.argv[3])
    except BaseException:
        import traceback; traceback.print_exc(file=sys.stderr)
    sys.stdout.write(json.dumps(out)); sys.stdout.flush(); os._exit(0)

tk.Tk.mainloop = fake_mainloop
from msr.config import Config
from msr.gui import main_gui
main_gui(Config(os.path.abspath("config.ini")))
"""


class StreamsButtonTests(unittest.TestCase):
    def test_streams_button_sits_by_clips_fits_and_opens_the_folder_without_creating_it(self):
        try:
            import tkinter
            tkinter.Tk().destroy()
        except Exception as exc:
            self.skipTest(f"no display: {exc}")
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            rec = data / "rec"
            rec.mkdir()
            (data / "config.ini").write_text(f"[Paths]\nstreams_dir = {rec}\n[GUI]\nminimize_to_tray = false\n"
                                             "notifications = false\n", encoding="utf-8")
            (data / "channels.json").write_text("[]", encoding="utf-8")
            done = subprocess.run([sys.executable, "-c", STREAMS_BUTTON_SCRIPT, str(ROOT), str(data), str(rec)],
                                  capture_output=True, text=True, timeout=90, cwd=str(ROOT))
        self.assertEqual(done.returncode, 0, done.stderr[-3000:])
        self.assertNotIn("Traceback", done.stderr)
        out = json.loads(done.stdout)
        self.assertTrue(out["found"] and out["same_row"] and out["left_of_clips"], out)
        self.assertEqual(out["opened"], [str(rec)])                       # same helper as Clips
        self.assertEqual(out["popups_when_present"], 0)
        self.assertEqual(out["opened_after_missing"], 1)                  # nothing more opened
        self.assertIn("does not exist", " ".join(out["popup"]))           # themed message instead
        self.assertFalse(out["recreated"])                                # creates nothing
        # The header's buttons fit the row at the default 1100 px window and at the 1060 px minimum.
        self.assertLessEqual(out["header_req"], 1060 - 40, out)


LIBRARY_MANAGE_SCRIPT = r"""
import gc, json, os, sys, time, tkinter as tk
from tkinter import ttk
sys.path.insert(0, sys.argv[1])
os.chdir(sys.argv[2])
os.environ["MSR_SMOKE_TEST"] = "1"
out = {"deleted": []}

def fake_mainloop(self, n=0):
    try:
        from msr import library_actions as act, library_dialog as ldialog, library_window as lw, dialogs
        self.update_idletasks(); self.update()
        panel = next(o for o in gc.get_objects() if isinstance(o, lw.LibraryPanel))
        tabs = panel.frame.master                      # the app's notebook
        tabs.select(panel.frame)                        # opening the tab starts the scan
        end = time.time() + 30
        while (panel.stats is None or panel.busy) and time.time() < end:
            self.update(); time.sleep(0.02)
        out["scanned"] = panel.stats is not None and bool(panel.rows)
        out["tabs"] = [panel.tabs.tab(t, "text").strip() for t in panel.tabs.tabs()]
        out["panel_req"], out["root_width"] = panel.frame.winfo_reqwidth(), self.winfo_width()
        out["tab_req"] = {}
        for t in panel.tabs.tabs():
            panel.tabs.select(t); self.update_idletasks()
            out["tab_req"][panel.tabs.tab(t, "text").strip()] = self.nametowidget(t).winfo_reqwidth()
        panel.tabs.select(panel.tabs.tabs()[1])
        # nothing may really be deleted: a fake mover records what it was asked to move
        def fake(item):
            out["deleted"].append(os.path.basename(item.path))
            return {}
        panel.mgr.ctx.deleters[act.ACTION_PENDING] = fake
        dialogs_seen = []
        def ask(parent, title, intro, recs=(), dirs=(), **kw):
            d = ldialog.ActionDialog(panel.mgr, parent, title, intro, recs, dirs, **kw)
            d.refresh()
            out["dialog_req"] = [d.win.winfo_reqwidth(), d.win.winfo_reqheight()]
            out["dark"] = dialogs.is_dark(d.win)
            out["go_text"] = d.go_btn.cget("text")
            out["allowed"] = d.allowed()
            out["permanent_blocked"] = (d.set_method(act.ACTION_PERMANENT), d.refresh(), d.allowed())[2]
            d.set_method(act.ACTION_PENDING); d.refresh()
            ok = d.confirm()
            return d.result
        panel.mgr.ask = ask
        first = panel.tree.get_children()[0]
        panel.tree.selection_set(first)
        self.update()
        out["delete_enabled"] = "disabled" not in panel.delete_btn.state()
        panel.delete_selected()
        end = time.time() + 30
        while (panel.busy or panel._rescan_pending or not panel.out.empty() or "rescanned" not in panel.status.cget("text")) and time.time() < end:
            self.update(); time.sleep(0.02)
        out["status"] = panel.status.cget("text")
        out["files_left"] = sorted(os.listdir(os.path.join(sys.argv[3], "Processed", "kick", "alice")))
    except BaseException:
        import traceback; traceback.print_exc(file=sys.stderr)
    sys.stdout.write(json.dumps(out)); sys.stdout.flush(); os._exit(0)

tk.Tk.mainloop = fake_mainloop
from msr.config import Config
from msr.gui import main_gui
main_gui(Config(os.path.abspath("config.ini")))
"""


class LibraryManageSmokeTests(unittest.TestCase):
    """The real app, hidden, on a temporary library. The mover is replaced by a fake that only
    records, so nothing is ever deleted, and no test points at real recordings."""

    def run_script(self, dark: bool):
        try:
            import tkinter
            tkinter.Tk().destroy()
        except Exception as exc:
            self.skipTest(f"no display: {exc}")
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            rec = data / "rec"
            old = time.time() - 40 * 86400
            for name in ("alice_20260101_120000.mp4", "alice_20260215_120000.mp4"):
                f = rec / "Processed" / "kick" / "alice" / name
                f.parent.mkdir(parents=True, exist_ok=True)
                f.write_bytes(b"x" * 1000)
                os.utime(f, (old, old))
            (data / "config.ini").write_text(f"[Paths]\nstreams_dir = {rec}\n[GUI]\nminimize_to_tray = false\n"
                                             "notifications = false\n", encoding="utf-8")
            (data / "channels.json").write_text("[]", encoding="utf-8")
            (data / "window_state.json").write_text(json.dumps({"dark_mode": dark}), encoding="utf-8")
            done = subprocess.run([sys.executable, "-c", LIBRARY_MANAGE_SCRIPT, str(ROOT), str(data), str(rec)],
                                  capture_output=True, text=True, timeout=120, cwd=str(ROOT))
        self.assertEqual(done.returncode, 0, done.stderr[-3000:])
        self.assertNotIn("Traceback", done.stderr)
        out = json.loads(done.stdout)
        self.assertTrue(out["scanned"], out)
        self.assertEqual(out["tabs"], ["Overview", "Channels", "Files", "PendingDeletion", "Clean up"])
        self.assertLessEqual(out["panel_req"], 1060 - 40, out)               # fits the minimum window
        self.assertTrue(out["delete_enabled"])
        self.assertEqual(out["dark"], dark)
        self.assertTrue(out["allowed"])                                       # the safe move needs no typing
        self.assertFalse(out["permanent_blocked"])                            # permanent needs the typed word
        self.assertLessEqual(out["dialog_req"][0], 1000)
        self.assertEqual(sorted(out["deleted"]), ["alice_20260101_120000.mp4", "alice_20260215_120000.mp4"])
        self.assertEqual(len(out["files_left"]), 2)                           # the fake moved nothing
        self.assertIn("2 file(s)", out["status"])

    def test_dark(self):
        self.run_script(True)

    def test_light(self):
        self.run_script(False)


if __name__ == "__main__":
    unittest.main()