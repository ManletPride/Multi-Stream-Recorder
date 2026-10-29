"""No console window for any child process on Windows (under pythonw every one would open one).

The cause that was seen: the background remux of leftover .ts files started ffmpeg with a priority
class as ``creationflags``, which switched the global CREATE_NO_WINDOW default off, so each file
opened (and closed) a console window at startup.

Nothing real is launched: ``subprocess.run``/``Popen`` are replaced by recorders and Windows is
simulated through ``msr.nowindow.is_windows``.
"""
import ast
import io
import logging
import os
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import msr.deps as deps_mod  # noqa: E402
import msr.library as library_mod  # noqa: E402
import msr.nowindow as nowindow  # noqa: E402
import msr.util as util_mod  # noqa: E402
import msr.worker as worker_mod  # noqa: E402

NO_WINDOW = 0x08000000
BELOW_NORMAL = 0x00004000
DETACHED = 0x00000008
NEW_CONSOLE = 0x00000010


class FlagTests(unittest.TestCase):
    def test_flags_are_added_and_kept(self):
        self.assertEqual(nowindow.no_window_flags(), NO_WINDOW)
        self.assertEqual(nowindow.no_window_flags(None), NO_WINDOW)
        self.assertEqual(nowindow.no_window_flags(BELOW_NORMAL), BELOW_NORMAL | NO_WINDOW)
        self.assertEqual(nowindow.no_window_flags(NO_WINDOW), NO_WINDOW)

    def test_a_deliberate_console_is_left_alone(self):
        self.assertEqual(nowindow.no_window_flags(NEW_CONSOLE), NEW_CONSOLE)
        self.assertEqual(nowindow.no_window_flags(DETACHED | 0x200), DETACHED | 0x200)

    def test_kwargs_on_windows(self):
        with mock.patch.object(nowindow, "is_windows", lambda: True):
            self.assertEqual(nowindow.no_window_kwargs(), {"creationflags": NO_WINDOW})
            self.assertEqual(nowindow.no_window_kwargs(BELOW_NORMAL), {"creationflags": BELOW_NORMAL | NO_WINDOW})

    def test_kwargs_elsewhere_are_empty(self):
        """creationflags makes Popen raise off Windows."""
        with mock.patch.object(nowindow, "is_windows", lambda: False):
            self.assertEqual(nowindow.no_window_kwargs(), {})
            self.assertEqual(nowindow.no_window_kwargs(BELOW_NORMAL), {})
        if os.name != "nt":
            subprocess.run([sys.executable, "-c", "pass"], **nowindow.no_window_kwargs())


class FakeSubprocess(types.SimpleNamespace):
    pass


def fake_module():
    seen = []

    class Popen:
        def __init__(self, *args, **kwargs):
            seen.append(kwargs)
    return FakeSubprocess(Popen=Popen), seen


class InstallTests(unittest.TestCase):
    def test_default_flag_is_added(self):
        mod, seen = fake_module()
        self.assertTrue(nowindow.install(mod, windows=True))
        mod.Popen(["x"])
        self.assertEqual(seen[-1]["creationflags"], NO_WINDOW)

    def test_a_priority_class_no_longer_turns_the_default_off(self):
        """The reported bug: creationflags=BELOW_NORMAL_PRIORITY_CLASS skipped CREATE_NO_WINDOW."""
        mod, seen = fake_module()
        nowindow.install(mod, windows=True)
        mod.Popen(["ffmpeg"], creationflags=BELOW_NORMAL)
        self.assertEqual(seen[-1]["creationflags"], BELOW_NORMAL | NO_WINDOW)

    def test_detached_and_new_console_are_untouched(self):
        mod, seen = fake_module()
        nowindow.install(mod, windows=True)
        mod.Popen(["x"], creationflags=DETACHED)
        mod.Popen(["x"], creationflags=NEW_CONSOLE)
        self.assertEqual([s["creationflags"] for s in seen], [DETACHED, NEW_CONSOLE])

    def test_installed_once_and_never_off_windows(self):
        mod, _ = fake_module()
        self.assertTrue(nowindow.install(mod, windows=True))
        self.assertFalse(nowindow.install(mod, windows=True))
        mod2, _ = fake_module()
        original = mod2.Popen
        self.assertFalse(nowindow.install(mod2, windows=False))
        self.assertIs(mod2.Popen, original)


class FakeStartupInfo:
    dwFlags = 0


class Recorder:
    """Stand-in for subprocess.run / Popen that records the keyword arguments."""

    def __init__(self):
        self.calls = []

    def run(self, cmd, **kwargs):
        self.calls.append((cmd, kwargs))
        return types.SimpleNamespace(returncode=0, stdout="1.0\n", stderr="")

    def assert_hidden(self, test, minimum=1):
        test.assertGreaterEqual(len(self.calls), minimum)
        for cmd, kw in self.calls:
            test.assertTrue(kw.get("creationflags", 0) & NO_WINDOW, f"console window for {cmd!r}: {kw}")


class CallSiteTests(unittest.TestCase):
    """Windows is simulated; each launch must carry CREATE_NO_WINDOW."""

    def setUp(self):
        self.rec = Recorder()
        for p in (mock.patch.object(nowindow, "is_windows", lambda: True),
                  mock.patch.object(subprocess, "run", self.rec.run),
                  mock.patch.object(subprocess, "STARTUPINFO", FakeStartupInfo, create=True),
                  mock.patch.object(subprocess, "STARTF_USESHOWWINDOW", 1, create=True)):
            p.start()
            self.addCleanup(p.stop)

    def test_tool_checks_at_startup(self):
        deps_mod.check_ytdlp()
        deps_mod.check_streamlink()
        deps_mod.check_ffmpeg("C:\\Other Programs\\ffmpeg\\bin\\ffmpeg.exe")
        deps_mod.check_ffprobe("ffprobe")
        deps_mod.check_deno()
        self.rec.assert_hidden(self, 5)

    def test_hidden_run_helper(self):
        worker_mod._hidden_run(["ffprobe", "x"], 5)
        self.rec.assert_hidden(self)

    def test_library_ffprobe(self):
        self.assertEqual(library_mod.ffprobe_seconds("ffprobe", "a.mp4"), 1.0)
        self.rec.assert_hidden(self)

    def test_taskkill_fallback(self):
        with mock.patch.object(util_mod, "os", types.SimpleNamespace(name="nt")):
            util_mod._kill_process_tree_fallback(1234)
        self.rec.assert_hidden(self)

    def test_duration_probe_before_a_remux(self):
        worker_mod._probe_duration("ffmpeg", "a.ts")
        self.rec.assert_hidden(self)


class RemuxTests(unittest.TestCase):
    """The reported case: the leftover-.ts remux (low priority) and the normal remux."""

    class _Proc:
        def __init__(self):
            self.stdout = io.StringIO("out_time_us=1000\nprogress=end\n")
            self.stderr = io.StringIO("")
            self.returncode = 0
            self.pid = 4242

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

        def kill(self):
            pass

    def run_remux(self, **kwargs):
        calls = []

        def fake_popen(cmd, **kw):
            calls.append(kw)
            return self._Proc()
        with mock.patch.object(nowindow, "is_windows", lambda: True), \
                mock.patch.object(worker_mod.subprocess, "Popen", fake_popen), \
                mock.patch.object(worker_mod, "_probe_duration", lambda *a, **k: None):
            worker_mod._run_remux_cmd(["ffmpeg", "-i", "in.ts", "out.mp4", "-y"], "ffmpeg", "in.ts", "out.mp4",
                                      logging.getLogger("t"), 30, 0, **kwargs)
        return calls

    def test_low_priority_remux_has_no_window_and_keeps_its_priority(self):
        (kw,) = self.run_remux(low_priority=True)
        self.assertEqual(kw["creationflags"], BELOW_NORMAL | NO_WINDOW)

    def test_normal_remux_has_no_window(self):
        (kw,) = self.run_remux()
        self.assertEqual(kw["creationflags"], NO_WINDOW)

    def test_priority_kwargs_helper(self):
        with mock.patch.object(nowindow, "is_windows", lambda: True):
            self.assertEqual(worker_mod._lower_priority_popen_kwargs(), {"creationflags": BELOW_NORMAL | NO_WINDOW})


# ── Source guard ─────────────────────────────────────────────────────────────

LAUNCHERS = {("subprocess", f) for f in ("run", "Popen", "check_output", "check_call", "call", "getoutput")}
LAUNCHERS |= {("_sp", f) for f in ("run", "Popen", "check_output", "check_call", "call")}
LAUNCHERS |= {("os", f) for f in ("system", "popen", "startfile", "spawnl", "spawnv", "spawnlp", "spawnvp")}

# (file, function) -> why a launch there does not use ``**no_window_kwargs()`` literally.
ALLOWED = {
    ("msr/worker.py", "_run_remux_cmd"): "**popen_kwargs: no_window_kwargs() or the low-priority variant of it (RemuxTests)",
    ("msr/worker.py", "_hidden_run"): "kwargs dict built from no_window_kwargs() (CallSiteTests)",
    ("msr/apply_update.py", "relaunch"): "detached relaunch of pythonw: explicit DETACHED_PROCESS | CREATE_NO_WINDOW",
    ("msr/update.py", "spawn_updater"): "detached updater helper: explicit DETACHED_PROCESS | CREATE_NO_WINDOW",
    ("run_msr.pyw", "<module>"): "the no-console launcher itself: explicit CREATE_NO_WINDOW, runs before msr is importable",
    ("msr/gui.py", "_notepad"): "opens Notepad, a GUI program the user asked for (View log)",
    ("msr/util.py", "open_local_path"): "user-triggered file/folder opener: os.startfile / open / xdg-open, GUI programs",
    ("msr/util.py", "reveal_in_file_browser"): "user-triggered Explorer/Finder: GUI programs",
}
MUST_MENTION = {
    "msr/apply_update.py": "CREATE_NO_WINDOW",
    "msr/update.py": "CREATE_NO_WINDOW",
    "run_msr.pyw": "CREATE_NO_WINDOW",
}


def _sources():
    for pattern in ("*.py", "*.pyw", "msr/*.py"):
        for path in sorted(ROOT.glob(pattern)):
            yield path.relative_to(ROOT).as_posix(), path


def _find_launches(rel, path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node

    def enclosing(node):
        while node in parents:
            node = parents[node]
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return node.name
        return "<module>"

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and (f.value.id, f.attr) in LAUNCHERS:
            helper = any(
                kw.arg is None and isinstance(kw.value, ast.Call)
                and getattr(kw.value.func, "id", getattr(kw.value.func, "attr", "")) == "no_window_kwargs"
                for kw in node.keywords)
            yield f"{f.value.id}.{f.attr}", node.lineno, enclosing(node), helper


class SourceGuardTests(unittest.TestCase):
    def test_every_launch_hides_its_console_or_is_allowlisted(self):
        bad, used = [], set()
        for rel, path in _sources():
            for what, line, func, helper in _find_launches(rel, path):
                if helper:
                    continue
                if (rel, func) in ALLOWED:
                    used.add((rel, func))
                    continue
                bad.append(f"{rel}:{line} {what}() in {func}()")
        self.assertEqual(
            bad, [], "child process launched without **no_window_kwargs() (a console window under pythonw). "
                     "Add it, or allowlist the call with a reason in tests/test_no_console.py:\n" + "\n".join(bad))
        self.assertEqual(sorted(set(ALLOWED) - used), [], "stale allowlist entries")

    def test_allowlisted_detached_launchers_still_set_the_flag(self):
        for rel, needle in MUST_MENTION.items():
            self.assertIn(needle, (ROOT / rel).read_text(encoding="utf-8"), rel)

    def test_entry_script_installs_the_default_before_anything_else(self):
        tree = ast.parse((ROOT / "Multi-Stream-Recorder.py").read_text(encoding="utf-8"))
        first = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))][0]
        self.assertEqual([a.name for a in first.names], ["msr.nowindow"])

    def test_modules_that_launch_import_the_helper(self):
        for rel, path in _sources():
            launches = [x for x in _find_launches(rel, path) if x[0].split(".")[0] != "os" and x[3]]
            if launches:
                self.assertIn("no_window_kwargs", path.read_text(encoding="utf-8").split("def ")[0], rel)


if __name__ == "__main__":
    unittest.main()
