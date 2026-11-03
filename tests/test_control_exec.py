"""Queued control execution (msr/control_exec.py): order, timeouts, shutdown, and that the GUI
adapter never touches Tk off the Tk thread. Hidden windows, temp folders, fake recorder."""

import os
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import control, control_exec  # noqa: E402
from msr.config import Config  # noqa: E402
from tests import new_tk_root  # noqa: E402
from tests.test_control import FakeRecorder, make_data  # noqa: E402


class SerialQueueTests(unittest.TestCase):
    def test_parallel_callers_run_in_order_on_the_owner(self):
        ex = control_exec.SerialExecutor("test lane")
        self.addCleanup(ex.close)
        seen, threads_seen = [], set()

        def caller(n):
            for i in range(20):
                ex.call(lambda i=i: (seen.append((n, i)), threads_seen.add(threading.current_thread())))
        ts = [threading.Thread(target=caller, args=(n,)) for n in range(16)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(20)
        self.assertEqual(len(seen), 16 * 20)
        for n in range(16):  # each caller's jobs in the order it sent them
            self.assertEqual([i for m, i in seen if m == n], list(range(20)))
        self.assertEqual(threads_seen, {ex.owner})

    def test_a_job_not_started_in_time_is_busy_and_never_runs(self):
        q = control_exec.SerialQueue("window", timeout=0.2)
        q.owner = threading.Thread()  # nobody drains
        ran = []
        with control_exec.command_name("channel_add"):
            with self.assertRaises(control.ControlError) as cm:
                q.call(lambda: ran.append(1))
        self.assertEqual(cm.exception.code, "busy")
        self.assertIn("channel_add", str(cm.exception))
        q.drain()
        self.assertEqual(ran, [], "a timed-out job must not run later (a retry would do it twice)")

    def test_a_job_that_started_but_runs_long_reports_timeout(self):
        ex = control_exec.SerialExecutor("slow lane", timeout=0.1)
        self.addCleanup(ex.close)
        with self.assertRaises(control.ControlError) as cm:
            ex.call(lambda: time.sleep(0.5), what="check")
        self.assertEqual(cm.exception.code, "timeout")
        self.assertIn("check", str(cm.exception))

    def test_errors_reach_the_caller(self):
        ex = control_exec.SerialExecutor("lane")
        self.addCleanup(ex.close)
        with self.assertRaises(control.ControlError) as cm:
            ex.call(lambda: (_ for _ in ()).throw(control.ControlError("nope", "conflict")))
        self.assertEqual(cm.exception.code, "conflict")

    def test_close_fails_queued_and_new_callers_at_once(self):
        q = control_exec.SerialQueue("window", timeout=10)
        q.owner = threading.Thread()
        result = []

        def waiter():
            try:
                q.call(lambda: 1, what="status")
            except control.ControlError as e:
                result.append(e.code)
        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.1)
        start = time.monotonic()
        q.close()
        t.join(5)
        self.assertEqual(result, ["busy"])
        self.assertLess(time.monotonic() - start, 2)
        with self.assertRaises(control.ControlError):
            q.call(lambda: 1)

    def test_owner_calls_run_inline(self):
        q = control_exec.SerialQueue("window")
        q.owner = threading.current_thread()
        self.assertEqual(q.call(lambda: 5), 5)


class TkBridgeTests(unittest.TestCase):
    """The real Tk loop with the GUI adapter: every hook and every after() on the Tk thread."""

    def setUp(self):
        self.root = new_tk_root(self)
        self.addCleanup(self.root.destroy)
        self.tk_thread = threading.current_thread()
        self.off_thread = []
        real_after = self.root.after

        def after(ms, fn=None, *a):
            self._check("after")
            return real_after(ms, fn, *a)
        self.root.after = after
        self.jobs = control_exec.SerialQueue("window", timeout=10)
        self.pump = control_exec.TkPump(self.root, self.jobs).start()
        self.addCleanup(self.pump.stop)
        self.channels = [{"name": "kickone", "enabled": True}]
        self.var = __import__("tkinter").StringVar(master=self.root)
        self.quits = []
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        data, _ = make_data(self.tmp.name, [])
        self.rec = FakeRecorder(["kickone"])
        self.rec.is_running = True
        hooks = {
            "get_recorder": lambda: self.rec,
            "get_presence": lambda: None,
            "channels": self._hook(lambda: self.channels),
            "after_edit": self._hook(lambda: self.var.set(str(len(self.channels)))),
            "start": self._hook(lambda: "starting"),
            "stop": self._hook(lambda: None),
            "schedule_quit": self._hook(lambda ac=None: self.quits.append(ac)),
            "reload": self._hook(lambda: len(self.channels)),
            "reflect": self._hook(lambda s, k: self.var.set(f"{s}.{k}")),
            "update_tag": lambda: None,
            "sound_test": self._hook(lambda event: event == "live"),
            "health_report": self._hook(lambda: None),
        }
        self.app = control_exec.GuiBridge(Config(os.path.join(data, "config.ini")), self.jobs, hooks)
        self.addCleanup(self.app.close)
        self.ctl = control.Controller(self.app)

    def _check(self, what):
        if threading.current_thread() is not self.tk_thread:
            self.off_thread.append((what, threading.current_thread().name))

    def _hook(self, fn):
        def wrapped(*a):
            self._check(getattr(fn, "__name__", "hook"))
            return fn(*a)
        return wrapped

    def _pump_until(self, threads, timeout=30):
        end = time.time() + timeout
        while any(t.is_alive() for t in threads) and time.time() < end:
            self.root.update()
            time.sleep(0.005)
        self.assertFalse(any(t.is_alive() for t in threads), "commands did not finish")

    def test_many_parallel_commands_stay_on_the_tk_thread(self):
        replies = []
        with mock.patch.multiple("msr.util", HAS_STREAMLINK=True, HAS_YTDLP=True):
            def worker(n):
                replies.append(self.ctl.handle("channel_add", {"name": f"twitch:agent{n}", "disabled": True}))
                replies.append(self.ctl.handle("channels"))
                replies.append(self.ctl.handle("check", {"channel": "kickone"}))
            ts = [threading.Thread(target=worker, args=(n,)) for n in range(24)]
            for t in ts:
                t.start()
            self._pump_until(ts)
        self.assertTrue(all(r["ok"] for r in replies), [r for r in replies if not r["ok"]][:3])
        self.assertEqual(len(self.channels), 25)
        self.assertEqual(self.var.get(), "25")
        self.assertEqual(self.off_thread, [])
        checks = [c for c in self.rec.calls if c == ("check_now", "kickone")]
        self.assertEqual(len(checks), 24)

    def test_recorder_calls_share_one_ordered_lane_off_the_tk_thread(self):
        lane_threads = []
        orig = self.rec.start_channel

        def start_channel(ch):
            lane_threads.append(threading.current_thread())
            orig(ch)
        self.rec.start_channel = start_channel
        ts = [threading.Thread(target=self.ctl.handle, args=("record_start", {"channel": "kickone"}))
              for _ in range(10)]
        for t in ts:
            t.start()
        self._pump_until(ts)
        self.assertEqual(len(lane_threads), 10)
        self.assertEqual(len(set(lane_threads)), 1)
        self.assertIsNot(lane_threads[0], self.tk_thread)
        self.assertEqual(self.off_thread, [])

    def test_quit_and_settings_are_queued_to_the_tk_thread(self):
        def worker():
            self.app.settings_changed("Timeouts", "poll_interval_minutes")
            self.ctl.handle("quit")
        t = threading.Thread(target=worker)
        t.start()
        self._pump_until([t])
        end = time.time() + 5
        while not self.quits and time.time() < end:
            self.root.update()
        self.assertEqual(self.quits, [None])
        self.assertEqual(self.var.get(), "Timeouts.poll_interval_minutes")
        self.assertEqual(self.off_thread, [])

    def test_sound_and_health_hooks_run_on_the_tk_thread(self):
        out = []

        def worker():
            out.append(self.ctl.handle("sound_test", {"event": "live"}))
            out.append(self.ctl.handle("health"))
        t = threading.Thread(target=worker)
        t.start()
        self._pump_until([t])
        self.assertTrue(out[0]["result"]["played"])
        self.assertTrue(out[1]["ok"], out[1])
        self.assertEqual(self.off_thread, [])

    def test_a_blocked_window_gives_busy_naming_the_command(self):
        self.jobs.timeout = 0.2
        reply = self.ctl.handle("channels")  # main thread is the Tk thread: runs inline
        self.assertTrue(reply["ok"])
        out = []
        t = threading.Thread(target=lambda: out.append(self.ctl.handle("channels")))
        t.start()
        t.join(5)  # the Tk loop is not pumped meanwhile
        self.assertEqual(out[0]["code"], "busy")
        self.assertIn("channels", out[0]["error"])


class HeadlessSerialTests(unittest.TestCase):
    def test_parallel_roster_edits_all_land(self):
        with tempfile.TemporaryDirectory() as tmp:
            data, _ = make_data(tmp, [])
            app = control.HeadlessApp(Config(os.path.join(data, "config.ini")), FakeRecorder)
            self.addCleanup(app.close)
            app.paused = True
            ctl = control.Controller(app)
            with mock.patch.multiple("msr.util", HAS_STREAMLINK=True, HAS_YTDLP=True):
                ts = [threading.Thread(target=ctl.handle, args=("channel_add", {"name": f"twitch:h{n}"}))
                      for n in range(20)]
                for t in ts:
                    t.start()
                for t in ts:
                    t.join(20)
            self.assertEqual(sorted(r["name"] for r in app.records()), sorted(f"twitch:h{n}" for n in range(20)))


if __name__ == "__main__":
    unittest.main()
