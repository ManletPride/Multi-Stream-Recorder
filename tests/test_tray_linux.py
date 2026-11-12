"""Linux tray: prefer AppIndicator, and never hide the window into a tray that is not there.

Windows is not retargeted. PYSTRAY_BACKEND, when the user already set it, is kept.
"""
import ast
import io
import json
import logging
import os
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from msr import deps
from msr.nowindow import no_window_kwargs

ROOT = Path(__file__).resolve().parent.parent
GUI = ROOT / "msr" / "gui.py"


class BackendChoiceTests(unittest.TestCase):
    def test_linux_with_appindicator_sets_the_backend(self):
        env = {}
        self.assertEqual(
            deps.prefer_linux_tray_backend(env, platform="linux", indicator_available=True),
            "appindicator")
        self.assertEqual(env["PYSTRAY_BACKEND"], "appindicator")

    def test_linux_without_appindicator_leaves_the_environment_alone(self):
        env = {}
        self.assertIsNone(
            deps.prefer_linux_tray_backend(env, platform="linux", indicator_available=False))
        self.assertNotIn("PYSTRAY_BACKEND", env)

    def test_an_existing_choice_is_not_replaced(self):
        env = {"PYSTRAY_BACKEND": "xorg"}
        self.assertIsNone(
            deps.prefer_linux_tray_backend(env, platform="linux", indicator_available=True))
        self.assertEqual(env["PYSTRAY_BACKEND"], "xorg")

    def test_windows_and_macos_are_not_touched(self):
        for platform in ("win32", "darwin"):
            env = {}
            self.assertIsNone(
                deps.prefer_linux_tray_backend(env, platform=platform, indicator_available=True))
            self.assertNotIn("PYSTRAY_BACKEND", env)

    def test_blank_backend_counts_as_unset(self):
        env = {"PYSTRAY_BACKEND": "   "}
        self.assertEqual(
            deps.prefer_linux_tray_backend(env, platform="linux", indicator_available=True),
            "appindicator")

    def test_prefer_is_called_before_pystray_is_imported(self):
        tree = ast.parse((ROOT / "msr" / "deps.py").read_text(encoding="utf-8"))
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_load_gui_extras")
        events = []
        for node in ast.walk(fn):
            if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "prefer_linux_tray_backend":
                events.append((node.lineno, "prefer"))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "pystray" or alias.name.startswith("pystray."):
                        events.append((node.lineno, "import"))
        events.sort()
        kinds = [kind for _, kind in events]
        self.assertIn("prefer", kinds)
        self.assertIn("import", kinds)
        self.assertLess(kinds.index("prefer"), kinds.index("import"))

    def test_backend_name_from_the_icon_class(self):
        class Icon:
            pass
        Icon.__module__ = "pystray._xorg"
        self.assertEqual(deps.pystray_backend_name(type("M", (), {"Icon": Icon})), "xorg")
        Icon.__module__ = "pystray._appindicator"
        self.assertEqual(deps.pystray_backend_name(type("M", (), {"Icon": Icon})), "appindicator")
        Icon.__module__ = "pystray._win32"
        self.assertEqual(deps.pystray_backend_name(type("M", (), {"Icon": Icon})), "win32")


class _Gi:
    """Stand-in for PyGObject. require_version records what was requested."""

    def __init__(self, available):
        self.available = set(available)

    def require_version(self, name, version):
        if name not in self.available:
            raise ValueError(name)


class IndicatorImportTests(unittest.TestCase):
    def _with_gi(self, available, body):
        import types
        saved = sys.modules.get("gi"), sys.modules.get("gi.repository")
        gi = types.ModuleType("gi")
        gi.__path__ = []
        helper = _Gi(available)
        gi.require_version = helper.require_version
        repo = types.ModuleType("gi.repository")
        for name in available:
            setattr(repo, name, type(name, (), {}))
        gi.repository = repo
        sys.modules["gi"] = gi
        sys.modules["gi.repository"] = repo
        try:
            return body()
        finally:
            for key, value in (("gi", saved[0]), ("gi.repository", saved[1])):
                if value is None:
                    sys.modules.pop(key, None)
                else:
                    sys.modules[key] = value

    def test_either_indicator_typelib_counts(self):
        for names in (("Gtk", "AppIndicator3"), ("Gtk", "AyatanaAppIndicator3")):
            with self.subTest(names=names):
                self.assertTrue(self._with_gi(names, deps.appindicator_backend_importable))

    def test_gtk_without_an_indicator_does_not_count(self):
        self.assertFalse(self._with_gi(("Gtk",), deps.appindicator_backend_importable))

    def test_no_gi_does_not_count(self):
        # A missing gi is an ImportError inside the function. Hide a fake that raises.
        import builtins
        real_import = builtins.__import__

        def boom(name, *args, **kwargs):
            if name == "gi" or name.startswith("gi."):
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", boom):
            self.assertFalse(deps.appindicator_backend_importable())


class NoTrayFallbackTests(unittest.TestCase):
    def test_xorg_without_a_tray_does_not_start(self):
        self.assertFalse(deps.tray_can_start(True, "xorg", False))

    def test_xorg_with_a_tray_may_start(self):
        self.assertTrue(deps.tray_can_start(True, "xorg", True))

    def test_windows_backend_does_not_need_an_xembed_tray(self):
        self.assertTrue(deps.tray_can_start(True, "win32", False))
        self.assertTrue(deps.tray_can_start(True, "appindicator", False))
        self.assertTrue(deps.tray_can_start(True, "", True))

    def test_missing_libraries_do_not_start(self):
        self.assertFalse(deps.tray_can_start(False, "win32", True))
        self.assertFalse(deps.tray_can_start(False, "xorg", False))

    def test_hide_only_when_the_icon_is_up(self):
        self.assertEqual(deps.hide_to_tray_decision(True, False, True), "withdraw")
        self.assertEqual(deps.hide_to_tray_decision(False, False, True), "stay")
        self.assertEqual(deps.hide_to_tray_decision(True, True, True), "stay")
        self.assertEqual(deps.hide_to_tray_decision(True, False, False), "stay")
        self.assertEqual(deps.hide_to_tray_decision(False, True, False), "stay")

    def test_dock_failure_is_one_dropped_error_and_not_a_traceback(self):
        filt = deps.DockFailureFilter()
        log = logging.getLogger("pystray._base")
        log.addFilter(filt)
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setLevel(logging.DEBUG)
        root = logging.getLogger()
        old_level = root.level
        root.setLevel(logging.DEBUG)
        root.addHandler(handler)
        try:
            try:
                raise AssertionError("not docked")
            except AssertionError:
                log.error("Failed to dock icon", exc_info=True)
                log.error("Failed to dock icon", exc_info=True)
            log.error("pystray lost the display")
        finally:
            root.setLevel(old_level)
            root.removeHandler(handler)
            log.removeFilter(filt)
        text = buf.getvalue()
        self.assertTrue(filt.failed)
        self.assertEqual(filt.dropped, 2)
        self.assertNotIn("Failed to dock icon", text)
        self.assertNotIn("Traceback", text)
        self.assertNotIn("AssertionError", text)
        self.assertIn("pystray lost the display", text)

    def test_install_puts_the_filter_on_the_logger_pystray_uses(self):
        filt = deps.DockFailureFilter()
        deps.install_dock_filter(filt)
        try:
            self.assertIn(filt, logging.getLogger("pystray._base").filters)
            logging.getLogger("pystray._base").error(
                "Failed to dock icon", exc_info=RuntimeError("dock"))
            self.assertEqual(filt.dropped, 1)
        finally:
            logging.getLogger("pystray._base").removeFilter(filt)
            logging.getLogger("pystray").removeFilter(filt)
            logging.getLogger("pystray._xorg").removeFilter(filt)
            for handler in logging.getLogger().handlers:
                handler.removeFilter(filt)

    def test_gui_withdraws_the_main_window_only_from_the_guarded_tray_path(self):
        tree = ast.parse(GUI.read_text(encoding="utf-8"))
        withdraws = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "withdraw":
                continue
            owner = node.func.value
            if isinstance(owner, ast.Name) and owner.id == "root":
                withdraws.append(node.lineno)
        # Smoke-test launch withdraws so nothing is mapped. The other call is the
        # tray hide, and it has to go through hide_to_tray_decision.
        self.assertEqual(len(withdraws), 2, withdraws)
        src = GUI.read_text(encoding="utf-8")
        self.assertNotIn("root.after(0, root.withdraw)", src)
        hide = next(n for n in ast.walk(tree)
                    if isinstance(n, ast.FunctionDef) and n.name == "_withdraw_for_tray")
        called = []
        for node in ast.walk(hide):
            if isinstance(node, ast.Call):
                func = node.func
                called.append(func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", ""))
        self.assertIn("hide_to_tray_decision", called)
        self.assertIn("withdraw", called)
        close = next(n for n in ast.walk(tree)
                     if isinstance(n, ast.FunctionDef) and n.name == "on_close")
        close_calls = []
        for node in ast.walk(close):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                close_calls.append(node.func.attr)
        self.assertNotIn("withdraw", close_calls)


def _child(code, env_extra=None):
    env = dict(os.environ)
    env.pop("PYSTRAY_BACKEND", None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["MSR_TOOL_VERSIONS"] = json.dumps(
        {"yt-dlp": "1", "streamlink": "1", "ffmpeg": "1", "deno": "1"})
    env.update(env_extra or {})
    done = subprocess.run(
        [sys.executable, "-c", code], cwd=str(ROOT), env=env,
        capture_output=True, text=True, timeout=60, **no_window_kwargs())
    return done


class BackendBeforeImportTests(unittest.TestCase):
    """A fresh interpreter: the env var is set before pystray's module body runs."""

    def test_appindicator_is_selected_before_pystray_imports(self):
        fake = Path(self._fake_tree(("Gtk", "AyatanaAppIndicator3"), ayatana_only=True))
        seen = fake / "seen.txt"
        code = textwrap.dedent(f"""
            import os, sys
            sys.path.insert(0, {str(fake)!r})
            sys.path.insert(0, {str(ROOT)!r})
            import msr.deps as d
            d.HAS_TRAY  # loads pystray
            print("HAS", d.HAS_TRAY)
        """)
        done = _child(code)
        self.assertEqual(done.returncode, 0, done.stderr[-2000:])
        self.assertEqual(seen.read_text(encoding="utf-8"), "appindicator")
        self.assertNotIn("Traceback", done.stderr)
        self.assertNotIn("Failed to dock icon", done.stderr)

    def _fake_tree(self, typelibs, ayatana_only=False):
        import tempfile
        path = Path(tempfile.mkdtemp(prefix="msr-tray-"))
        self.addCleanup(lambda: __import__("shutil").rmtree(path, ignore_errors=True))
        gi = path / "gi"
        (gi / "repository").mkdir(parents=True)
        blocked = ("AppIndicator3",) if ayatana_only else ()
        (gi / "__init__.py").write_text(
            "def require_version(name, version):\n"
            "    ok = {'Gtk': '3.0', 'AppIndicator3': '0.1', 'AyatanaAppIndicator3': '0.1'}\n"
            f"    blocked = {blocked!r}\n"
            "    if name in blocked or ok.get(name) != version:\n"
            "        raise ValueError(name)\n",
            encoding="utf-8")
        names = "\n".join(f"class {name}:\n    pass\n" for name in typelibs)
        (gi / "repository" / "__init__.py").write_text(names, encoding="utf-8")
        tray = path / "pystray"
        tray.mkdir()
        seen = path / "seen.txt"
        (tray / "__init__.py").write_text(
            "import os\n"
            f"open({str(seen)!r}, 'w').write(os.environ.get('PYSTRAY_BACKEND', ''))\n"
            "class Icon:\n"
            "    pass\n"
            "class Menu:\n"
            "    pass\n"
            "class MenuItem:\n"
            "    pass\n",
            encoding="utf-8")
        return path


class LiveDisplayTests(unittest.TestCase):
    """What this X server actually does. Skips cleanly when the pieces are missing."""

    def test_probe_does_not_raise_and_import_does_not_traceback(self):
        if not os.environ.get("DISPLAY"):
            self.skipTest("no DISPLAY")
        present = deps.xembed_tray_present()
        self.assertIsInstance(present, bool)
        code = textwrap.dedent(f"""
            import os, sys, json
            sys.path.insert(0, {str(ROOT)!r})
            os.environ.pop("PYSTRAY_BACKEND", None)
            import msr.deps as d
            backend = d.pystray_backend_name(d.pystray) if d.pystray is not None else ""
            print("RESULT" + json.dumps({{
                "has": d.HAS_TRAY, "backend": backend, "note": d.TRAY_NOTE,
                "xembed": d.xembed_tray_present(),
            }}))
        """)
        done = _child(code)
        self.assertEqual(done.returncode, 0, done.stderr[-2000:])
        self.assertNotIn("Traceback", done.stderr)
        self.assertNotIn("Failed to dock icon", done.stderr)
        line = next(l for l in done.stdout.splitlines() if l.startswith("RESULT"))
        got = json.loads(line[len("RESULT"):])
        if got["backend"] == "xorg" and not got["xembed"]:
            self.assertFalse(got["has"])
            self.assertIn("System tray is off", got["note"])
        if got["backend"] == "appindicator":
            self.assertTrue(got["has"])
            self.assertEqual(got["note"], "")

    def test_a_real_xorg_icon_that_cannot_dock_logs_nothing(self):
        """The user's failure: pystray Xorg logs 'Failed to dock icon' twice, with a traceback."""
        if not os.environ.get("DISPLAY"):
            self.skipTest("no DISPLAY")
        code = textwrap.dedent(r"""
            import io, logging, os, threading, time
            os.environ["PYSTRAY_BACKEND"] = "xorg"
            try:
                from PIL import Image
                import pystray
            except Exception as exc:
                print("SKIP", type(exc).__name__)
                raise SystemExit(0)
            from msr.deps import DockFailureFilter, install_dock_filter, xembed_tray_present
            buf = io.StringIO()
            root = logging.getLogger()
            root.setLevel(logging.DEBUG)
            handler = logging.StreamHandler(buf)
            handler.setLevel(logging.DEBUG)
            root.addHandler(handler)
            filt = DockFailureFilter()
            install_dock_filter(filt)
            present = xembed_tray_present()
            icon = pystray.Icon("msr-probe", Image.new("RGB", (32, 32), (20, 40, 80)), "probe")
            errors = []
            def run():
                try:
                    icon.run()
                except Exception as exc:
                    errors.append(repr(exc))
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            deadline = time.time() + 3
            while time.time() < deadline and not filt.failed and thread.is_alive():
                time.sleep(0.05)
            try:
                icon.stop()
            except Exception as exc:
                errors.append("stop " + repr(exc))
            thread.join(timeout=3)
            text = buf.getvalue()
            print("RESULT", filt.failed, filt.dropped, present, len(errors), thread.is_alive())
            print("LOG", repr(text))
            if errors:
                print("ERRORS", errors)
        """)
        # The snippet imports msr from cwd, which _child sets to the repo.
        done = _child(code)
        if done.returncode != 0:
            self.fail(done.stderr[-2000:] or done.stdout[-2000:])
        if "SKIP" in done.stdout and "RESULT" not in done.stdout:
            self.skipTest(done.stdout.strip())
        self.assertNotIn("Traceback", done.stderr)
        self.assertNotIn("Failed to dock icon", done.stdout)
        self.assertNotIn("AssertionError", done.stdout)
        line = next(l for l in done.stdout.splitlines() if l.startswith("RESULT"))
        failed, dropped, present, n_errors, alive = line.split()[1:]
        self.assertEqual(n_errors, "0")
        self.assertEqual(alive, "False")
        if present == "False":
            self.assertEqual(failed, "True")
            self.assertGreaterEqual(int(dropped), 1)


if __name__ == "__main__":
    unittest.main()
