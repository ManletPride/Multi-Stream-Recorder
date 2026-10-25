"""The real window builds and survives its first refresh ticks (a launch that dies silently
under pythonw looks like "a window flashes and goes away"), and no UI name is defined twice."""
import ast
import json
import os
import subprocess
import sys
import tempfile
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


if __name__ == "__main__":
    unittest.main()