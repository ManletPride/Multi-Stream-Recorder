"""Worker-process log lines reach the Logs tab once, and still once in the file.

The GUI handler lives in the main process. A recording worker used to write
stream_recorder.log only, so a long remux never showed up in the Logs tab.
"""

import logging
import multiprocessing as mp
import os
import queue
import re
import tempfile
import threading
import time
import unittest
import uuid
from unittest import mock

import msr.util as util
from msr.logbridge import LogBridge, WorkerLogHandler, prepare_for_queue
from msr import logview as lv
from msr.worker import record_worker


def _start_methods():
    methods = ["spawn"]
    try:
        mp.get_context("fork")
    except ValueError:
        return methods
    methods.append("fork")
    return methods


def _child_log(root, channel, log_queue, marker):
    """Top-level so spawn can import it. Writes the file and, if given, the queue."""
    logger = util.setup_child_logging(root, channel, log_queue)
    logger.info("info %s", marker)
    logger.warning("warn %s", marker)
    try:
        raise RuntimeError("boom-" + marker)
    except RuntimeError:
        logger.exception("failed %s", marker)
    logger.info("secret https://h.invalid/a?jwt=SECRET9 %s", marker)
    for handler in logging.root.handlers:
        try:
            handler.flush()
        except Exception:
            pass


def _spam(log_queue):
    log = logging.getLogger("msr.test.workerlog.spam")
    log.handlers[:] = []
    log.propagate = False
    log.setLevel(logging.INFO)
    log.addHandler(WorkerLogHandler(log_queue))
    i = 0
    while True:
        log.info("spam %s", i)
        i += 1


def _once(log_queue, message):
    log = logging.getLogger("msr.test.workerlog.once")
    log.handlers[:] = []
    log.propagate = False
    log.setLevel(logging.INFO)
    log.addHandler(WorkerLogHandler(log_queue))
    log.info("%s", message)
    for handler in log.handlers:
        try:
            handler.flush()
        except Exception:
            pass


def _join_or_kill(proc, timeout):
    proc.join(timeout)
    if proc.is_alive():
        proc.kill()
        proc.join(5)
    return not proc.is_alive()


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []
        self.hits = []

    def emit(self, record):
        message = record.getMessage()
        self.messages.append(message)
        if "AFTER-KILL" in message:
            self.hits.append(message)


class WorkerLogForwardTests(unittest.TestCase):
    def setUp(self):
        self._handlers = list(logging.root.handlers)
        self._level = logging.root.level
        self._env = {k: os.environ.get(k) for k in (util.LOG_MAX_BYTES_ENV, util.LOG_BACKUPS_ENV)}
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def tearDown(self):
        for handler in list(logging.root.handlers):
            if handler not in self._handlers:
                logging.root.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
        for handler in self._handlers:
            if handler not in logging.root.handlers:
                logging.root.addHandler(handler)
        logging.root.setLevel(self._level)
        for key, value in self._env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _wait_for(self, pred, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            time.sleep(0.05)
        return False

    def test_a_worker_record_reaches_the_gui_handler_and_the_file_once(self):
        for method in _start_methods():
            with self.subTest(method=method):
                self._assert_forwarded(method)

    def _assert_forwarded(self, method):
        from msr.gui import QueueLogHandler

        ctx = mp.get_context(method)
        marker = uuid.uuid4().hex
        log_file = util.setup_logging(self.tmp.name)
        gui_queue = queue.Queue()
        sink = QueueLogHandler(gui_queue)
        sink.setLevel(logging.INFO)
        sink.setFormatter(lv.GuiFormatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
        manager = ctx.Manager()
        bridge = LogBridge(manager.Queue(maxsize=2000), sink)
        bridge.start()
        proc = ctx.Process(target=_child_log, args=(self.tmp.name, "vimp", bridge.queue, marker))
        proc.daemon = True
        try:
            proc.start()
            self.assertTrue(_join_or_kill(proc, 30), "worker did not exit")
            self.assertEqual(proc.exitcode, 0, "worker failed")
            with open(log_file, encoding="utf-8") as fh:
                file_text = fh.read()
            info_line = f"info {marker}"
            self.assertEqual(file_text.count(info_line), 1, file_text)
            self.assertEqual(file_text.count(f"warn {marker}"), 1, file_text)
            self.assertEqual(file_text.count(f"failed {marker}"), 1, file_text)
            self.assertNotIn("SECRET9", file_text)
            self.assertNotIn("[WARNING]", file_text)  # the file format has no level tag
            self.assertRegex(
                file_text,
                rf"\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}} \[PID {proc.pid}\] \[vimp\] {info_line}",
            )

            found = []

            def ready():
                while True:
                    try:
                        found.append(gui_queue.get_nowait())
                    except queue.Empty:
                        break
                return any(info_line in line for line in found) and any(
                    f"warn {marker}" in line for line in found) and any(
                    f"failed {marker}" in line for line in found) and any(
                    "jwt=***" in line and marker in line for line in found)

            self.assertTrue(self._wait_for(ready), f"gui sink missing lines: {found!r}")
            info = next(line for line in found if info_line in line)
            warn = next(line for line in found if f"warn {marker}" in line)
            failed = next(line for line in found if f"failed {marker}" in line)
            secret = next(line for line in found if "jwt=" in line and marker in line)
            self.assertRegex(info, rf"^\d\d:\d\d:\d\d \[PID {proc.pid}\] \[vimp\] {info_line}$")
            self.assertRegex(warn, rf"^\d\d:\d\d:\d\d \[WARNING\] \[PID {proc.pid}\] \[vimp\] warn {marker}$")
            self.assertIn("[ERROR]", failed.splitlines()[0])
            self.assertIn(f"[PID {proc.pid}] [vimp]", failed.splitlines()[0])
            self.assertIn("boom-" + marker, failed)
            self.assertNotIn("SECRET9", secret)
            self.assertIn("jwt=***", secret)
            self.assertEqual(lv.line_level(info), "INFO")
            self.assertEqual(lv.line_level(warn), "WARNING")
            self.assertEqual(lv.line_level(failed), "ERROR")
            self.assertTrue(lv.matches(warn, lv.split_query("vimp warn"), lv.LEVEL_WARNING))
            self.assertFalse(lv.matches(info, lv.split_query("vimp"), lv.LEVEL_WARNING))
            self.assertTrue(lv.matches(failed, [], lv.LEVEL_ERROR))
            self.assertFalse(lv.matches(warn, [], lv.LEVEL_ERROR))
            self.assertEqual(sum(1 for line in found if info_line in line), 1)
        finally:
            bridge.stop(timeout=2)
            proc.join(1)
            if proc.is_alive():
                proc.kill()
                proc.join(2)
            manager.shutdown()

    def test_without_a_queue_the_file_is_still_written_once(self):
        """Headless workers are not given a queue. The file is the only sink."""
        ctx = mp.get_context("spawn")
        marker = "headless-" + uuid.uuid4().hex
        log_file = util.setup_logging(self.tmp.name)
        proc = ctx.Process(target=_child_log, args=(self.tmp.name, "vimp", None, marker))
        proc.daemon = True
        proc.start()
        try:
            self.assertTrue(_join_or_kill(proc, 30))
            self.assertEqual(proc.exitcode, 0)
            with open(log_file, encoding="utf-8") as fh:
                text = fh.read()
            self.assertEqual(text.count(f"info {marker}"), 1, text)
            self.assertIn(f"[PID {proc.pid}] [vimp]", text)
        finally:
            if proc.is_alive():
                proc.kill()
                proc.join(2)

    def test_a_killed_writer_does_not_wedge_the_queue_or_shutdown(self):
        for method in _start_methods():
            with self.subTest(method=method):
                self._assert_kill_does_not_wedge(method)

    def _assert_kill_does_not_wedge(self, method):
        ctx = mp.get_context(method)
        manager = ctx.Manager()
        log_queue = manager.Queue(maxsize=200)
        sink = _ListHandler()
        sink.setLevel(logging.INFO)
        bridge = LogBridge(log_queue, sink)
        bridge.start()
        spammer = ctx.Process(target=_spam, args=(log_queue,))
        spammer.daemon = True
        follow = ctx.Process(target=_once, args=(log_queue, "AFTER-KILL"))
        follow.daemon = True
        try:
            spammer.start()
            time.sleep(0.3)
            spammer.terminate()
            self.assertTrue(_join_or_kill(spammer, 5))
            follow.start()
            self.assertTrue(_join_or_kill(follow, 20))
            self.assertTrue(
                self._wait_for(lambda: "AFTER-KILL" in sink.hits),
                f"later record did not arrive (got {sink.hits}, n={len(sink.messages)})")
            started = time.monotonic()
            self.assertTrue(bridge.stop(timeout=2))
            self.assertLess(time.monotonic() - started, 2.5)
            started = time.monotonic()
            manager.shutdown()
            manager = None
            self.assertLess(time.monotonic() - started, 3)
        finally:
            bridge.stop(timeout=1)
            for proc in (spammer, follow):
                if proc.is_alive():
                    proc.kill()
                    proc.join(2)
            if manager is not None:
                manager.shutdown()

    def test_a_full_queue_drops_the_oldest_and_does_not_block(self):
        q = queue.Queue(maxsize=1)
        handler = WorkerLogHandler(q)
        log = logging.getLogger("msr.test.workerlog.full")
        log.handlers[:] = [handler]
        log.propagate = False
        log.setLevel(logging.INFO)
        try:
            started = time.monotonic()
            for i in range(30):
                log.info("n %s", i)
            self.assertLess(time.monotonic() - started, 2)
            self.assertEqual(q.get_nowait().getMessage(), "n 29")
            self.assertTrue(q.empty())
        finally:
            log.handlers[:] = []

    def test_a_full_manager_queue_does_not_block_the_caller(self):
        ctx = mp.get_context("spawn")
        manager = ctx.Manager()
        try:
            q = manager.Queue(maxsize=1)
            handler = WorkerLogHandler(q)
            log = logging.getLogger("msr.test.workerlog.mgrfull")
            log.handlers[:] = [handler]
            log.propagate = False
            log.setLevel(logging.INFO)
            started = time.monotonic()
            for i in range(20):
                log.info("n %s", i)
            self.assertLess(time.monotonic() - started, 5)
            self.assertEqual(q.get_nowait().getMessage(), "n 19")
        finally:
            log.handlers[:] = []
            manager.shutdown()

    def test_bridge_delivers_to_the_handler_and_stop_returns_while_idle(self):
        q = queue.Queue()
        sink = _ListHandler()
        sink.setLevel(logging.INFO)
        bridge = LogBridge(q, sink)
        bridge.start()
        q.put(prepare_for_queue(logging.LogRecord(
            "t", logging.INFO, __file__, 1, "hello-bridge", (), None)))
        q.put(prepare_for_queue(logging.LogRecord(
            "t", logging.DEBUG, __file__, 1, "hidden-debug", (), None)))
        self.assertTrue(self._wait_for(lambda: "hello-bridge" in sink.messages))
        started = time.monotonic()
        self.assertTrue(bridge.stop(timeout=2))
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertIn("hello-bridge", sink.messages)
        self.assertNotIn("hidden-debug", sink.messages)

    def test_spawn_passes_the_log_queue_to_the_worker(self):
        from msr.recorder import StreamRecorder

        rec = StreamRecorder.__new__(StreamRecorder)
        rec.wake_events = {}
        rec.should_stop = threading.Event()
        rec.status_queue = object()
        rec.runtime = {}
        rec.log_queue = object()
        captured = {}

        class FakeProcess:
            def __init__(self, target=None, args=()):
                captured["target"] = target
                captured["args"] = args
                self.daemon = False
                self.pid = 1

            def start(self):
                pass

        with mock.patch("msr.recorder.mp.Process", FakeProcess):
            rec._spawn_worker("kick:vimp", {})
        self.assertIs(captured["target"], record_worker)
        self.assertIs(captured["args"][0][6], rec.log_queue)

    def test_stop_joins_the_bridge_when_the_session_is_already_idle(self):
        from msr.recorder import StreamRecorder

        rec = StreamRecorder.__new__(StreamRecorder)
        rec.is_running = False
        calls = []

        class Bridge:
            def stop(self, timeout=None):
                calls.append(timeout)
                return True

        rec._log_bridge = Bridge()
        rec.stop()
        self.assertEqual(calls, [2.0])
        self.assertIsNone(rec._log_bridge)


if __name__ == "__main__":
    unittest.main()
