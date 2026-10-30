"""Audit batch D (small correctness fixes): M1, M5, M7, M8, M9, L4, L5, L9, L10."""

import base64
import io
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import msr.platforms as platforms  # noqa: E402
import msr.util as util  # noqa: E402
import msr.worker as worker_mod  # noqa: E402
from msr.config import Config  # noqa: E402
from tests.test_storage import Drive, Harness  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class FakeProc:
    """A capture process that runs until something kills it."""

    def __init__(self, pid=4242):
        self.pid = pid
        self.returncode = None
        self.stderr = io.StringIO("")
        self.stdout = None

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        self.returncode = -9


def quiet_logger(name):
    log = logging.getLogger(name)
    log.propagate = False
    log.handlers[:] = [logging.NullHandler()]
    return log


# ── M1: max_record_hours ─────────────────────────────────────────────

class MaxRecordHoursTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.raw = os.path.join(self.tmp.name, "a.ts")
        with open(self.raw, "wb") as f:
            f.write(b"x" * 4096)
        self.proc = FakeProc()
        self.killed = []

        def kill(pid, logger=None):
            self.killed.append(pid)
            self.proc.returncode = -9
        p = mock.patch.object(worker_mod, "kill_process_tree", kill)
        p.start()
        self.addCleanup(p.stop)

    def monitor(self, hours, elapsed, on_limit=None):
        stop = threading.Event()
        out = {}

        def run():
            out["r"] = worker_mod.monitor_recording_process(
                self.proc, self.raw, time.monotonic() - elapsed, hours, "twitch",
                quiet_logger("m1"), mock.Mock(), "twitch:x", stop, None,
                stream_info="1080p", on_size_limit=on_limit)
        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(3)
        stop.set()          # the old code never splits on time; end the loop for it
        self.proc.returncode = self.proc.returncode if self.proc.returncode is not None else 0
        t.join(15)
        return out["r"]

    def test_a_segment_past_the_limit_is_split(self):
        calls = []
        _status, split = self.monitor(1.0, 3601, on_limit=lambda: calls.append(1) or True)
        self.assertTrue(split)
        self.assertEqual(self.killed, [4242])
        self.assertEqual(calls, [1], "the time split uses the same overlap hand-off as the size split")

    def test_under_the_limit_or_off_keeps_recording(self):
        for hours, elapsed in ((1.0, 100), (0.0, 10 ** 6)):
            self.proc = FakeProc()
            self.killed.clear()
            _status, split = self.monitor(hours, elapsed)
            self.assertFalse(split, (hours, elapsed))
            self.assertEqual(self.killed, [])


# ── M5: Fishtank probe status codes and JWT decoding ──────────────────

class FakeAuth:
    def __init__(self):
        self._all_stream_names = {"other-5": "Other"}  # cam-5 is never listed, so it is probed
        self._stream_hosts = {}
        self._stream_host = "example.invalid"
        self.refreshes = []

    def resolve_stream_id(self, s):
        return s

    def get_live_streams(self):
        return {}

    def get_jwt(self, force_refresh=False):
        self.refreshes.append(force_refresh)
        return "tok"


class FishtankTests(unittest.TestCase):
    def probe(self, code):
        auth = FakeAuth()
        err = urllib.error.HTTPError("https://example.invalid", code, "x", {}, None)
        with mock.patch("urllib.request.urlopen", side_effect=err):
            live, _name, error = platforms.check_stream_fishtank("cam-5", auth, quiet_logger("m5"))
        return live, error, auth

    def test_auth_and_server_errors_are_not_live(self):
        for code in (401, 403, 429, 500, 503):
            live, error, _auth = self.probe(code)
            self.assertFalse(live, code)
            self.assertIn(str(code), error or "")

    def test_a_rejected_token_is_refreshed(self):
        _live, _error, auth = self.probe(401)
        self.assertIn(True, auth.refreshes)

    def test_404_offline_and_other_codes_unchanged(self):
        self.assertEqual(self.probe(404)[:2], (False, None))
        self.assertTrue(self.probe(405)[0])

    def test_jwt_expiry_is_read_from_a_base64url_payload(self):
        exp = int(time.time()) + 3600
        # find a payload whose base64url form has '-' or '_' (std base64 would choke)
        for pad in range(2000):
            raw = json.dumps({"exp": exp, "p": "?>~" * (pad % 5 + 1), "n": pad}).encode()
            b64 = base64.urlsafe_b64encode(raw).decode().rstrip("=")
            if "-" in b64 or "_" in b64:
                break
        self.assertTrue("-" in b64 or "_" in b64)
        jwt = f"e30.{b64}.sig"
        self.assertEqual(platforms.FishtankAuth._decode_jwt_exp(jwt), exp)


# ── M7: redaction for named child loggers ─────────────────────────────

class RedactionTests(unittest.TestCase):
    def test_child_logger_records_are_redacted_in_the_log_file(self):
        root = logging.getLogger()
        before = list(root.handlers)
        env = {k: os.environ.get(k) for k in (util.LOG_MAX_BYTES_ENV, util.LOG_BACKUPS_ENV)}
        tmp = tempfile.TemporaryDirectory()
        try:
            log_file = util.setup_logging(tmp.name)
            logging.getLogger("msr.test.child").warning("probe https://h.invalid/x?jwt=SECRET2 failed")
            for h in root.handlers:
                h.flush()
            with open(log_file, encoding="utf-8") as f:
                text = f.read()
            self.assertIn("probe", text)
            self.assertNotIn("SECRET2", text)
        finally:
            for handler in list(root.handlers):
                if handler not in before:
                    root.removeHandler(handler)
                    handler.close()
            for key, value in env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            tmp.cleanup()

    def test_the_gui_log_handler_redacts_child_logger_records(self):
        try:
            from msr.gui import QueueLogHandler
        except ImportError as e:     # no tkinter
            self.skipTest(str(e))
        import queue
        q = queue.Queue()
        handler = QueueLogHandler(q)
        log = logging.getLogger("msr.test.gui-child")
        log.propagate = False
        log.addHandler(handler)
        try:
            log.warning("url https://h.invalid/x?jwt=SECRET3")
        finally:
            log.removeHandler(handler)
        got = q.get_nowait()
        text = got.getMessage() if isinstance(got, logging.LogRecord) else str(got)
        self.assertNotIn("SECRET3", text)


# ── M8: titles and folder names ───────────────────────────────────────

class NameTests(unittest.TestCase):
    def test_control_characters_are_replaced(self):
        for fn in (util._sanitize_filename, util.sanitize_path_component):
            out = fn("line one\nline\ttwo\x00\x1f end")
            self.assertIsNone(re.search(r"[\x00-\x1f]", out), (fn.__name__, out))

    def test_a_title_with_a_newline_can_be_created(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, util._sanitize_filename("Big\nnews!\r\n") + ".ts")
            open(path, "wb").close()
            self.assertTrue(os.path.exists(path))

    def test_windows_device_names_are_prefixed(self):
        for name in ("nul", "CON", "com1", "lpt9", "aux.ts", "NUL "):
            self.assertNotEqual(util.sanitize_path_component(name).split(".")[0].strip().upper(),
                                name.split(".")[0].strip().upper(), name)
            self.assertTrue(util._sanitize_filename(name).startswith("_"), name)
        for name in ("console", "nullify", "com10", "auxiliary"):
            self.assertEqual(util.sanitize_path_component(name), name)

    def test_case_variants_of_one_kick_or_twitch_channel_are_one_entry(self):
        records, skipped = util.coerce_channel_records(
            ["Foo", "foo", "foo.", "twitch:Bar", "twitch:bar", "youtube:Baz", "youtube:baz"])
        self.assertEqual([r["name"] for r in records], ["Foo", "twitch:Bar", "youtube:Baz", "youtube:baz"])
        self.assertEqual(skipped, ["foo", "foo.", "twitch:bar"])

    def test_adding_a_case_variant_is_refused(self):
        with mock.patch.object(util, "HAS_STREAMLINK", True):
            ok, msg = util.validate_channel_name("XQC", "kick", ["xqc"])
            self.assertFalse(ok)
            ok, _ = util.validate_channel_name("Other", "kick", ["xqc"])
            self.assertTrue(ok)


# ── M9: capture process reaped on errors, stdout not piped ────────────

class CaptureReapTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.drive = Drive(self.tmp.name)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.tmp.name)

    def test_an_error_after_spawn_kills_the_capture_and_stdout_is_not_a_pipe(self):
        h = Harness(self, self.drive.path, os.path.join(self.tmp.name, "fb"))
        procs, kwargs, killed = [], [], []

        def popen(cmd, *a, **k):
            kwargs.append(k)
            procs.append(FakeProc())
            return procs[-1]

        def monitor(*a, **k):
            h.stop.set()
            raise RuntimeError("boom between spawn and kill")

        def kill(pid, logger=None):
            killed.append(pid)
            for p in procs:
                if p.pid == pid:
                    p.returncode = -9
        h.popen = popen
        with mock.patch.object(worker_mod, "monitor_recording_process", monitor), \
                mock.patch.object(worker_mod, "kill_process_tree", kill):
            h.run()
        self.assertEqual(len(procs), 1)
        self.assertIn(4242, killed, "the capture kept running after the worker error")
        self.assertIsNot(kwargs[0].get("stdout"), subprocess.PIPE)


# ── L4: Deno warning only for YouTube channels ────────────────────────

class DenoWarningTests(unittest.TestCase):
    def warnings_for(self, channels):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(os.path.join(tmp, "config.ini"))
            cfg.config.set("Paths", "streams_dir", os.path.join(tmp, "streams"))
            ch = os.path.join(tmp, "channels.json")
            with open(ch, "w") as f:
                json.dump(channels, f)
            cfg.config.set("Paths", "channels_file", ch)
            with mock.patch.object(util.deps, "configure_ffmpeg", return_value="1.0"), \
                    mock.patch.object(util.deps, "check_ffprobe", return_value=True), \
                    mock.patch.object(util, "HAS_DENO", False):
                _errors, warnings = util.validate_startup(cfg)
        return [w for w in warnings if "Deno" in w]

    def test_kick_only_gets_no_deno_warning(self):
        self.assertEqual(self.warnings_for([{"name": "xqc", "enabled": True},
                                            {"name": "twitch:foo", "enabled": True}]), [])

    def test_an_enabled_youtube_channel_still_warns(self):
        self.assertEqual(len(self.warnings_for([{"name": "youtube:@foo", "enabled": True}])), 1)
        self.assertEqual(self.warnings_for([{"name": "youtube:@foo", "enabled": False}]), [])


# ── L5: blank ffmpeg_path ─────────────────────────────────────────────

class FfmpegPathGuardTests(unittest.TestCase):
    def test_every_ffmpeg_path_read_has_the_blank_guard(self):
        bad = []
        for rel in ("msr/worker.py", "msr/platforms.py", "msr/gui.py"):
            with open(os.path.join(REPO, rel), encoding="utf-8") as f:
                for n, line in enumerate(f, 1):
                    if "'ffmpeg_path', fallback='ffmpeg')" in line and "or 'ffmpeg'" not in line:
                        bad.append(f"{rel}:{n}")
        self.assertEqual(bad, [])


# ── L9: version comparison ────────────────────────────────────────────

class VersionTests(unittest.TestCase):
    def test_order(self):
        k = util.version_key
        self.assertLess(k("3.2b"), k("3.2"))
        self.assertLess(k("3.2b1"), k("3.2"))
        self.assertLess(k("3.2b1"), k("3.2b2"))
        self.assertLess(k("3.2rc1"), k("3.2"))
        self.assertLess(k("3.2"), k("3.10"))
        self.assertEqual(k("3.2"), k("3.2.0"))
        self.assertEqual(k("v3.2"), k("3.2"))
        self.assertLess(k("3.1"), k("3.2b"))

    def test_a_beta_user_is_offered_the_final_release(self):
        class Resp(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False
        body = json.dumps({"tag_name": "v3.2", "html_url": "https://example.invalid/r"}).encode()
        got = []
        with mock.patch("urllib.request.urlopen", return_value=Resp(body)), \
                mock.patch.object(util, "threading") as fake_threading:
            util.check_for_updates("3.2b", callback=lambda tag, url, zipball: got.append(tag))
            call = fake_threading.Thread.call_args
            target = call.kwargs.get("target") or call.args[0]
            target()
        self.assertEqual(got, ["3.2"])


# ── L10: importlib.util ───────────────────────────────────────────────

class ImportlibTests(unittest.TestCase):
    def test_deps_imports_importlib_util_explicitly(self):
        with open(os.path.join(REPO, "msr", "deps.py"), encoding="utf-8") as f:
            src = f.read()
        self.assertIn("importlib.util.find_spec", src)
        self.assertIsNone(re.search(r"^\s*import importlib\s*$", src, re.M),
                          "a bare 'import importlib' does not bind importlib.util")


if __name__ == "__main__":
    unittest.main()
