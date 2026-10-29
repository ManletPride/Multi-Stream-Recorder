"""Audit batch E (startup cost): M2, L12, L14, and the orphan scan's per-process ppid lookups."""

import ast
import json
import os
import subprocess
import sys
import unittest
from unittest import mock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import msr.util as util  # noqa: E402
from msr.nowindow import no_window_kwargs  # noqa: E402

ENTRY = os.path.join(REPO, "Multi-Stream-Recorder.py")

# Counts every process the child starts (the deps version probes are subprocess.run calls).
_COUNT_LAUNCHES = (
    "import subprocess\n"
    "launched = []\n"
    "_init = subprocess.Popen.__init__\n"
    "def _counting(self, args, *a, **k):\n"
    "    launched.append(args)\n"
    "    return _init(self, args, *a, **k)\n"
    "subprocess.Popen.__init__ = _counting\n"
)


def run_child(code, env_extra=None, drop=()):
    env = dict(os.environ)
    for k in drop:
        env.pop(k, None)
    env.update(env_extra or {})
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env, capture_output=True,
                       text=True, timeout=120, **no_window_kwargs())
    lines = [l for l in r.stdout.splitlines() if l.startswith("RESULT")]
    if not lines:
        raise AssertionError(f"child failed:\n{r.stdout}\n{r.stderr}")
    return json.loads(lines[-1][6:])


class WorkerBootstrapTests(unittest.TestCase):
    """What a spawned recording worker loads before record_worker runs (M2)."""

    def test_the_spawn_bootstrap_does_not_load_the_gui(self):
        got = run_child(
            "import runpy, sys, json\n"
            f"runpy.run_path({ENTRY!r}, run_name='__mp_main__')\n"
            "import msr.worker\n"
            "heavy = [m for m in ('tkinter', 'msr.gui', 'pystray', 'PIL', 'plyer') if m in sys.modules]\n"
            "print('RESULT' + json.dumps(heavy))\n")
        self.assertEqual(got, [])

    def test_a_worker_reuses_the_parents_tool_versions(self):
        versions = {"yt-dlp": "1.0", "streamlink": "2.0", "ffmpeg": None, "deno": "3.0"}
        got = run_child(
            _COUNT_LAUNCHES +
            "import json\n"
            "import msr.deps as d\n"
            "print('RESULT' + json.dumps([len(launched), d.YTDLP_VERSION, d.HAS_STREAMLINK,"
            " d.HAS_FFMPEG, d.DENO_VERSION]))\n",
            env_extra={"MSR_TOOL_VERSIONS": json.dumps(versions)})
        self.assertEqual(got, [0, "1.0", True, False, "3.0"])

    def test_the_main_process_probes_once_and_hands_the_result_on(self):
        got = run_child(
            _COUNT_LAUNCHES +
            "import json, os\n"
            "import msr.deps as d\n"
            "print('RESULT' + json.dumps([len(launched), json.loads(os.environ['MSR_TOOL_VERSIONS'])['yt-dlp'] == d.YTDLP_VERSION]))\n",
            drop=("MSR_TOOL_VERSIONS",))
        self.assertEqual(got, [4, True])

    def test_gui_extras_load_on_first_use(self):
        got = run_child(
            "import json, sys\n"
            "import msr.deps as d\n"
            "before = 'pystray' in sys.modules or 'plyer' in sys.modules\n"
            "has = d.HAS_TRAY\n"
            "print('RESULT' + json.dumps([before, isinstance(has, bool), hasattr(d, 'plyer_notification')]))\n")
        self.assertEqual(got, [False, True, True])


class OrphanScanTests(unittest.TestCase):
    """kill_orphan_recording_processes runs before the window and before any recording."""

    def scan(self, ppid_map):
        seen = []

        def fake_iter(attrs=None):
            seen.append(list(attrs or []))
            return iter([])
        with mock.patch.object(util, "HAS_PSUTIL", True), \
                mock.patch.object(util.psutil, "process_iter", fake_iter), \
                mock.patch.object(util, "_ppid_map", return_value=ppid_map):
            util.kill_orphan_recording_processes(None, streams_dir=os.path.join(REPO, "nowhere"))
        return seen

    @unittest.skipUnless(util.HAS_PSUTIL, "psutil not installed")
    def test_parents_come_from_one_snapshot(self):
        self.assertEqual(self.scan({10: 1}), [["pid", "name", "cmdline"]])

    @unittest.skipUnless(util.HAS_PSUTIL, "psutil not installed")
    def test_without_the_snapshot_it_asks_each_process(self):
        self.assertIn("ppid", self.scan({})[0])

    @unittest.skipUnless(util.HAS_PSUTIL, "psutil not installed")
    def test_the_snapshot_matches_psutil(self):
        snap = util._ppid_map()
        self.assertTrue(snap)
        me = os.getpid()
        self.assertEqual(snap.get(me), util.psutil.Process(me).ppid())


class SourceHygieneTests(unittest.TestCase):
    def test_no_function_re_imports_a_module_imported_at_the_top(self):
        """A local ``import urllib.request`` makes ``urllib`` local to the whole function (L12)."""
        bad = []
        for rel in ("msr/platforms.py", "msr/util.py", "msr/deps.py", "msr/worker.py"):
            with open(os.path.join(REPO, rel), encoding="utf-8") as f:
                tree = ast.parse(f.read())
            top = set()
            for node in tree.body:
                if isinstance(node, ast.Import):
                    top |= {(a.name, a.asname) for a in node.names}
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    for sub in ast.walk(node):
                        if isinstance(sub, ast.Import):
                            bad += [f"{rel}:{sub.lineno} {a.name}" for a in sub.names
                                    if (a.name, a.asname) in top]
        self.assertEqual(bad, [])

    def test_the_mutex_hash_is_marked_not_for_security(self):
        calls = []
        real = util.hashlib.sha1

        def sha1(*a, **k):
            calls.append(k)
            return real(*a, **k)
        with mock.patch.object(util.hashlib, "sha1", sha1):
            util.single_instance_mutex_name("x")
        self.assertEqual(calls, [{"usedforsecurity": False}])


if __name__ == "__main__":
    unittest.main()
