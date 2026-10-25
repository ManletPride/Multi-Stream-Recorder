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


if __name__ == "__main__":
    unittest.main()