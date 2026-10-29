"""Supervisor and orphan kill: a worker of another instance is never a target, an outside kill is a
crash (restart), quick exits back off, and one failure never ends the supervisor loop.

No real process is started or killed: process tables are synthetic rows and workers are fakes.
"""
import logging
import os
import queue
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import msr.recorder as recorder_mod  # noqa: E402
from msr.recorder import StreamRecorder  # noqa: E402
from msr.util import (  # noqa: E402
    _argv_includes_streams_dir,
    _ffmpeg_is_msr_network_orphan,
    _plan_orphan_kills,
)
from tests.test_startup_cleanup import FakeConfig  # noqa: E402


def spawn_cmd(parent_pid):
    return ('python -c "from multiprocessing.spawn import spawn_main; '
            f'spawn_main(parent_pid={parent_pid}, pipe_handle=1)" --multiprocessing-fork')


def proc(pid, name, cmdline, parent_pid):
    return {"pid": pid, "name": name, "cmdline": cmdline, "parent_pid": parent_pid}


def capture(out):
    return ["ffmpeg", "-i", "https://example.com/live/index.m3u8", out]


class OrphanPathMatchingTests(unittest.TestCase):
    """C2: the instance's folder is matched by path component, not by substring."""

    def setUp(self):
        self.root = os.path.abspath("streams_root")
        self.rec = os.path.join(self.root, "Recorded", "kick", "a", "a_1.ts")

    def sibling(self, suffix):
        return os.path.join(self.root + suffix, "Recorded", "kick", "a", "a_1.ts")

    def test_own_capture_still_matches(self):
        self.assertTrue(_ffmpeg_is_msr_network_orphan(capture(self.rec), self.root))
        self.assertTrue(_ffmpeg_is_msr_network_orphan(capture(self.rec), [self.root]))

    def test_sibling_folders_with_the_same_prefix_do_not_match(self):
        for suffix in ("2", "-test", "_old", ".bak", " copy"):
            self.assertFalse(_ffmpeg_is_msr_network_orphan(capture(self.sibling(suffix)), self.root), suffix)
            self.assertFalse(_argv_includes_streams_dir(self.sibling(suffix), self.root), suffix)

    def test_another_instance_nested_inside_does_not_match(self):
        nested = os.path.join(self.root, "test", "Recorded", "kick", "a", "a_1.ts")
        self.assertFalse(_ffmpeg_is_msr_network_orphan(capture(nested), self.root))
        self.assertFalse(_argv_includes_streams_dir(os.path.join(self.root, "test", "stream_recorder.log"), self.root))

    def test_what_msr_keeps_under_the_folder_matches(self):
        for rel in ("stream_recorder.log", "stream_recorder.log.1", ".msr_instance.lock",
                    os.path.join("Recorded", "x.ts"), os.path.join("processed", "x.mp4"),
                    os.path.join("PendingDeletion", "x.ts"), os.path.join("Clips", "x.mp4")):
            self.assertTrue(_argv_includes_streams_dir(os.path.join(self.root, rel), self.root), rel)
        self.assertTrue(_argv_includes_streams_dir(self.root, self.root))

    def test_dotdot_cannot_smuggle_a_sibling_in(self):
        sneaky = os.path.join(self.root, "Recorded", "..", "..", os.path.basename(self.root) + "2", "Recorded", "a.ts")
        self.assertFalse(_ffmpeg_is_msr_network_orphan(capture(sneaky), self.root))

    def test_option_equals_path(self):
        cmd = ["streamlink", "https://kick.com/a", "best", f"--output={self.rec}"]
        self.assertTrue(_ffmpeg_is_msr_network_orphan(cmd, self.root))
        cmd = ["streamlink", "https://kick.com/a", "best", f"--output={self.sibling('2')}"]
        self.assertFalse(_ffmpeg_is_msr_network_orphan(cmd, self.root))

    def test_a_command_line_string(self):
        self.assertTrue(_argv_includes_streams_dir(f'ffmpeg -i x "{self.rec}"', self.root))
        self.assertFalse(_argv_includes_streams_dir(f'ffmpeg -i x "{self.sibling("2")}"', self.root))

    @unittest.skipUnless(os.name == "nt", "Windows paths are case-insensitive")
    def test_windows_case_and_slashes(self):
        self.assertTrue(_argv_includes_streams_dir(self.rec.upper().replace("\\", "/"), self.root))

    def test_two_folders_main_and_fallback(self):
        fallback = os.path.abspath("fallback_root")
        out = os.path.join(fallback, "Recorded", "kick", "a", "a_1.ts")
        self.assertFalse(_ffmpeg_is_msr_network_orphan(capture(out), self.root))
        self.assertTrue(_ffmpeg_is_msr_network_orphan(capture(out), [self.root, fallback]))
        self.assertFalse(_ffmpeg_is_msr_network_orphan(capture(out), [self.root, None, ""]))


class OrphanPlanTests(unittest.TestCase):
    def setUp(self):
        self.root = os.path.abspath("streams_root")
        self.mine = os.path.join(self.root, "Recorded", "kick", "a.ts")

    def plan(self, procs, keep=(1,), roots=None, open_paths=None):
        return _plan_orphan_kills(procs, roots or self.root, set(keep), open_paths)

    def test_a_sibling_instances_live_worker_is_left_alone(self):
        """The reported bug: E:\\streams2's worker was planned for kill when streams_dir was E:\\streams."""
        other = os.path.join(self.root + "2", "Recorded", "kick", "b.ts")
        procs = [proc(500, "python.exe", "python Multi-Stream-Recorder.py", 4),
                 proc(501, "python.exe", spawn_cmd(500), 500),
                 proc(502, "ffmpeg.exe", capture(other), 501)]
        self.assertEqual(self.plan(procs), ([], []))

    def test_a_sibling_workers_open_log_is_not_ours(self):
        ghost = proc(38496, "python.exe", spawn_cmd(37184), 4)
        log = os.path.join(self.root + "2", "stream_recorder.log")
        self.assertEqual(self.plan([ghost], open_paths={38496: [log]}), ([], []))

    def test_a_sibling_capture_without_a_parent_is_left_alone(self):
        ff = proc(12200, "ffmpeg.exe", capture(os.path.join(self.root + "-test", "Recorded", "a.ts")), 1)
        self.assertEqual(self.plan([ff]), ([], []))

    def test_real_leftovers_are_still_found(self):
        procs = [proc(38496, "python.exe", spawn_cmd(37184), 4),
                 proc(37752, "ffmpeg.exe", capture(self.mine), 38496),
                 proc(12200, "ffmpeg.exe", capture(self.mine), 1)]
        workers, captures = self.plan(procs)
        self.assertEqual(workers, [38496])
        self.assertEqual(captures, [12200])

    def test_a_worker_whose_owner_is_still_running_is_not_an_orphan(self):
        """Even when the folder matches: a live parent means a live instance owns it."""
        procs = [proc(700, "python.exe", "python Multi-Stream-Recorder.py", 4),
                 proc(701, "python.exe", spawn_cmd(700), 700),
                 proc(702, "ffmpeg.exe", capture(self.mine), 701),
                 proc(703, "python.exe", spawn_cmd(700), 700)]
        self.assertEqual(self.plan(procs), ([], []))

    def test_the_same_worker_is_a_leftover_once_its_owner_is_gone(self):
        procs = [proc(701, "python.exe", spawn_cmd(700), 4), proc(702, "ffmpeg.exe", capture(self.mine), 701)]
        self.assertEqual(self.plan(procs), ([701], []))

    def test_workers_of_this_instance_are_never_planned(self):
        procs = [proc(1, "python.exe", "python Multi-Stream-Recorder.py", 4),
                 proc(701, "python.exe", spawn_cmd(1), 1),
                 proc(702, "ffmpeg.exe", capture(self.mine), 701)]
        self.assertEqual(self.plan(procs, keep=(1, 701, 702)), ([], []))

    def test_leftover_recording_into_the_fallback_is_found_with_both_roots(self):
        fallback = os.path.abspath("fallback_root")
        out = os.path.join(fallback, "Recorded", "kick", "a.ts")
        procs = [proc(38496, "python.exe", spawn_cmd(37184), 4), proc(37752, "ffmpeg.exe", capture(out), 38496)]
        self.assertEqual(self.plan(procs), ([], []))
        self.assertEqual(self.plan(procs, roots=[self.root, fallback]), ([38496], []))


class FakeProc:
    _pids = iter(range(5000, 99999))

    def __init__(self, target=None, args=(), exitcode=None, alive=True):
        self.args = args
        self.pid = next(FakeProc._pids)
        self.exitcode = exitcode
        self.daemon = False
        self._alive = alive

    def start(self):
        self._alive = True

    def is_alive(self):
        return self._alive

    def die(self, code):
        self._alive, self.exitcode = False, code

    def join(self, timeout=None):
        pass

    def kill(self):
        self.die(-9)


def make_rec(tmp, channels=("kick1", "kick2")):
    rec = object.__new__(StreamRecorder)
    rec.channels = list(channels)
    rec.config = FakeConfig(os.path.join(tmp, "streams"))
    rec.root_path = os.path.join(tmp, "streams")
    rec.fallback_root = None
    rec.should_stop = threading.Event()
    rec.is_running = True
    rec.stopped_channels = set()
    rec._spawning = set()
    rec._spawn_lock = threading.Lock()
    rec.status_queue = queue.Queue()
    rec.status_dict = {ch: {"status": "Recording", "detail": "", "size": "", "time": "", "progress": 0}
                       for ch in channels}
    rec.runtime = {}
    rec.wake_events = {}
    rec.processes = {}
    return rec


class SupervisorBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.rec = make_rec(self.tmp.name)
        self.spawned = []
        patches = [
            mock.patch.object(recorder_mod.mp, "Process", self.make_process),
            mock.patch.object(StreamRecorder, "_get_wake_event", lambda s, ch: None),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def make_process(self, target=None, args=()):
        p = FakeProc(target, args)
        self.spawned.append(p)
        return p

    def start_all(self, now=1000.0):
        sv = self.rec._supervision()
        for ch in self.rec.channels:
            p = FakeProc()
            self.rec.processes[ch] = [p]
            sv["started"][ch] = now
        return {ch: self.rec.processes[ch][0] for ch in self.rec.channels}

    def tick(self, now):
        self.rec._supervise_tick({}, now=now)


class RestartPolicyTests(SupervisorBase):
    def test_an_outside_kill_is_a_crash_and_the_worker_comes_back(self):
        """Exit code 15 (TerminateProcess from psutil, e.g. another instance's orphan check) used to
        mean "we stopped it": the channel was never restarted."""
        procs = self.start_all()
        procs["kick1"].die(15)
        with self.assertLogs(level="WARNING") as cm:
            self.tick(1100.0)
        self.assertNotIn("kick1", self.rec.stopped_channels)
        self.assertTrue(any("killed from outside" in o for o in cm.output), cm.output)
        self.tick(1100.0 + 5.1)
        self.assertEqual(len(self.spawned), 1)
        self.assertIs(self.rec.processes["kick1"][0], self.spawned[0])

    def test_our_own_kill_is_not_restarted(self):
        procs = self.start_all()
        self.rec._mark_killed(procs["kick1"])
        procs["kick1"].die(-9)
        self.tick(1100.0)
        self.tick(1200.0)
        self.assertEqual(self.spawned, [])
        self.assertIn("kick1", self.rec.stopped_channels)
        self.assertEqual(self.rec._supervision()["killed"], set())

    def test_stop_channel_marks_the_channel_stopped_before_it_kills(self):
        procs = self.start_all()
        seen = {}

        def fake_kill(pid, logger=None):
            seen["stopped_first"] = "kick1" in self.rec.stopped_channels
            for p in procs.values():
                if p.pid == pid:
                    p.die(15)
        rec = self.rec
        rec._sync_kick_push = lambda: None
        rec.cleaner = mock.Mock()
        with mock.patch.object(recorder_mod, "kill_process_tree", fake_kill), \
                mock.patch.object(recorder_mod.threading, "Thread"):
            rec.stop_channel("kick1")
        self.assertTrue(seen["stopped_first"])
        self.tick(1100.0)
        self.assertEqual(self.spawned, [])

    def test_start_channel_forgets_the_crash_history(self):
        rec = self.rec
        procs = self.start_all()
        procs["kick1"].die(1)
        self.tick(1001.0)
        self.assertIn("kick1", rec._supervision()["restart_at"])
        rec._sync_kick_push = lambda: None
        with mock.patch.object(recorder_mod, "find_cookies_file", lambda c: None):
            rec.start_channel("kick1")
        self.assertNotIn("kick1", rec._supervision()["restart_at"])
        self.assertNotIn("kick1", rec._supervision()["fast"])
        self.assertEqual(len(self.spawned), 1)

    def test_quick_exits_back_off_5_10_20_up_to_5_minutes(self):
        self.start_all(now=0.0)
        delays, now = [], 0.0
        for _ in range(9):
            for p in list(self.rec.processes["kick1"]):
                p.die(1)
            with mock.patch("logging.warning"):
                self.tick(now)
            at = self.rec._supervision()["restart_at"]["kick1"]
            delays.append(round(at - now))
            now = at
            self.tick(now)                      # time is up: it starts again
            self.assertEqual(len(self.rec.processes["kick1"]), 1)
            self.rec._supervision()["started"]["kick1"] = now   # it ran for 0 s
        self.assertEqual(delays, [5, 10, 20, 40, 80, 160, 300, 300, 300])

    def test_nothing_starts_before_the_delay_is_over(self):
        procs = self.start_all(now=0.0)
        procs["kick1"].die(1)
        self.tick(1.0)
        self.tick(3.0)
        self.assertEqual(self.spawned, [])
        self.tick(7.0)
        self.assertEqual(len(self.spawned), 1)

    def test_a_worker_that_lived_a_while_restarts_with_the_shortest_delay_again(self):
        procs = self.start_all(now=0.0)
        sv = self.rec._supervision()
        sv["fast"]["kick1"] = 6
        procs["kick1"].die(1)
        self.tick(3600.0)                      # it ran for an hour: back at once, history cleared
        self.assertEqual(sv["fast"]["kick1"], 0)
        self.assertEqual(len(self.spawned), 1)

    def test_the_log_is_not_one_line_per_quick_exit(self):
        self.start_all(now=0.0)
        now, lines = 0.0, 0
        for _ in range(25):
            for p in list(self.rec.processes["kick1"]):
                p.die(1)
            with self.assertLogs(level="DEBUG") as cm:
                logging.debug("marker")
                self.tick(now)
            lines += len([o for o in cm.output if "kick1" in o and "restarting" in o])
            now = self.rec._supervision()["restart_at"]["kick1"]
            self.tick(now)
        self.assertLess(lines, 8, "3 at the start, then every 10th")
        self.assertGreaterEqual(lines, 4)

    def test_the_gui_row_says_when_the_next_try_is(self):
        procs = self.start_all(now=0.0)
        procs["kick1"].die(1)
        self.tick(1.0)
        row = self.rec.status_dict["kick1"]
        self.assertEqual(row["status"], "Error")
        self.assertIn("restart in 5s", row["detail"])

    def test_a_channel_stopped_while_waiting_is_not_restarted(self):
        procs = self.start_all(now=0.0)
        procs["kick1"].die(1)
        self.tick(1.0)
        self.rec.stopped_channels.add("kick1")
        self.tick(100.0)
        self.assertEqual(self.spawned, [])
        self.assertNotIn("kick1", self.rec._supervision()["restart_at"])


class SupervisorLoopSurvivalTests(SupervisorBase):
    def test_a_failing_start_does_not_stop_the_other_channels_or_the_loop(self):
        procs = self.start_all(now=0.0)
        procs["kick1"].die(1)
        procs["kick2"].die(1)
        self.tick(1.0)
        calls = []

        def spawn(ch, cfg):
            calls.append(ch)
            if ch == "kick1":
                raise OSError("out of handles")
            return FakeProc()
        with mock.patch.object(self.rec, "_spawn_worker", spawn), self.assertLogs(level="ERROR") as cm:
            self.tick(10.0)
        self.assertEqual(calls, ["kick1", "kick2"], "kick2 is still supervised in the same pass")
        self.assertEqual(len(self.rec.processes["kick2"]), 1)
        self.assertIn("kick1", self.rec._supervision()["restart_at"], "kick1 is retried later")
        self.assertTrue(any("out of handles" in o for o in cm.output))
        with mock.patch.object(self.rec, "_spawn_worker", spawn):
            self.tick(10.2)                      # too early: no spin
        self.assertEqual(calls, ["kick1", "kick2"])

    def test_a_dead_status_manager_does_not_stop_supervision(self):
        procs = self.start_all(now=0.0)
        procs["kick1"].die(1)

        class DeadQueue:
            def empty(self):
                raise EOFError("manager is gone")
        self.rec.status_queue = DeadQueue()
        with self.assertLogs(level="ERROR") as cm:
            self.tick(1.0)
            self.tick(2.0)
        self.assertEqual(len([o for o in cm.output if "status updates" in o]), 1, "one message, not one per pass")
        self.tick(10.0)
        self.assertEqual(len(self.spawned), 1, "the dead worker is still restarted")

    def test_update_status_from_queue_never_raises(self):
        class BrokenQueue:
            def empty(self):
                raise BrokenPipeError()
        self.rec.status_queue = BrokenQueue()
        with self.assertLogs(level="ERROR"):
            self.rec.update_status_from_queue()   # the GUI timer calls this directly

    def test_status_updates_still_arrive(self):
        self.rec.status_queue.put(("kick1", {"status": "Offline", "detail": "", "size": "", "time": "", "progress": 0}))
        self.rec.update_status_from_queue()
        self.assertEqual(self.rec.status_dict["kick1"]["status"], "Offline")

    def test_storage_check_failure_is_contained(self):
        procs = self.start_all(now=0.0)
        procs["kick1"].die(1)
        with mock.patch.object(self.rec, "_storage_tick", side_effect=RuntimeError("disk gone")), \
                self.assertLogs(level="ERROR"):
            self.tick(10.0)
            self.tick(20.0)
        self.assertEqual(len(self.spawned), 1)

    def test_run_loop_survives_an_exception_in_a_pass(self):
        """H2: the run() loop used to end on the first exception while is_running stayed True."""
        rec = self.rec
        rec.channels = []
        rec.cleaner = mock.Mock()
        rec.kick_push = mock.Mock()
        rec._startup_cleanup_thread = None
        rec._startup_cleanup_cancel = threading.Event()
        rec._plan_startup_cleanup = lambda: {"files": [], "snapshot_time": 0}
        rec._launch_startup_cleanup = lambda plan: None
        rec._sync_kick_push = lambda: None
        passes = []

        def tick(config_dict, now=None):
            passes.append(1)
            if len(passes) <= 2:
                raise RuntimeError("pass failed")
            if len(passes) >= 5:
                rec.should_stop.set()
        rec._supervise_tick = tick
        real_sleep = time.sleep
        with mock.patch.object(recorder_mod, "kill_orphan_recording_processes", lambda *a, **k: None), \
                mock.patch.object(recorder_mod, "find_cookies_file", lambda c: None), \
                mock.patch.object(recorder_mod.time, "sleep", lambda s: real_sleep(0.001)), \
                self.assertLogs(level="ERROR") as cm:
            rec.is_running = False
            t = threading.Thread(target=rec.run, daemon=True)
            t.start()
            t.join(20)
        self.assertFalse(t.is_alive())
        self.assertGreaterEqual(len(passes), 5, "the loop kept going after the failures")
        self.assertEqual(len([o for o in cm.output if "supervisor pass" in o]), 1)


class OrphanRootsTests(unittest.TestCase):
    def test_recorder_passes_the_main_and_the_fallback_folder(self):
        rec = object.__new__(StreamRecorder)
        rec.root_path, rec.fallback_root = "A", "B"
        self.assertEqual(rec._orphan_roots(), ["A", "B"])
        rec.fallback_root = None
        self.assertEqual(rec._orphan_roots(), ["A"])

    def test_default_fallback_is_unique_per_main_folder(self):
        from msr import storage
        a = storage.fallback_root("", os.path.abspath("streams_a"))
        b = storage.fallback_root("", os.path.abspath("streams_b"))
        self.assertNotEqual(a, b)
        self.assertEqual(a, storage.fallback_root("", os.path.abspath("streams_a")))
        self.assertEqual(storage.fallback_root("off", "x"), None)

    def test_orphan_check_accepts_a_list_and_kills_nothing_for_an_empty_one(self):
        from msr import util
        with mock.patch.object(util, "psutil") as ps:
            util.kill_orphan_recording_processes(logging.getLogger("t"), streams_dir=[None, ""])
            ps.process_iter.assert_not_called()


if __name__ == "__main__":
    unittest.main()
