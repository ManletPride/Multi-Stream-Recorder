"""Post-reconnect catch-up check + quiet drop logging (Kick + Twitch push)."""
import logging
import os
import tempfile
import threading
import unittest
from unittest import mock

from msr import recorder as rmod
from msr.recorder import CatchUp, KickPushListener, StreamRecorder
from tests.test_twitch_push import TOK, FakeApi, FakeConfig, FakePresence, WELCOME, FakeWs
from msr.twitch_push import TwitchPushListener


class FakeRec:
    def __init__(self):
        self.catchups = []
        self.runtime = {}
        self.woken = []

    def catch_up_check(self, channels, source="push"):
        self.catchups.append((list(channels), source))

    def check_now(self, key):
        self.woken.append(key)


class ListHandler(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


class TestCatchUpDebounce(unittest.TestCase):
    def test_once_per_gap(self):
        t = [1000.0]
        c = CatchUp(gap=300, clock=lambda: t[0])
        self.assertTrue(c.due())
        t[0] += 299
        self.assertFalse(c.due())
        t[0] += 2
        self.assertTrue(c.due())


class TestRecorderCatchUp(unittest.TestCase):
    def make(self):
        r = StreamRecorder.__new__(StreamRecorder)
        r.is_running = True
        r.wake_events = {k: threading.Event() for k in ("a", "b", "rec", "stopped", "twitch:x")}
        r.stopped_channels = {"stopped"}
        r.status_dict = {"rec": {"status": "Recording"}, "a": {"status": "Offline"}}
        return r

    def test_skips_recording_and_stopped_and_spreads(self):
        r = self.make()
        sleeps = []
        with mock.patch.object(rmod.time, "sleep", side_effect=sleeps.append), \
                mock.patch.object(rmod.threading, "Thread") as T:
            T.side_effect = lambda target, **kw: mock.Mock(start=target)
            with self.assertLogs(level="INFO") as cm:
                got = r.catch_up_check(["a", "b", "rec", "stopped", "unknown"], "Kick push")
        self.assertEqual(sorted(got), ["a", "b"])
        self.assertTrue(r.wake_events["a"].is_set() and r.wake_events["b"].is_set())
        self.assertFalse(r.wake_events["rec"].is_set() or r.wake_events["stopped"].is_set())
        self.assertEqual(len(sleeps), 1)
        self.assertIn("catch-up check of 2 channel(s)", cm.output[0])

    def test_spread_capped_for_many_channels(self):
        r = self.make()
        r.wake_events = {f"c{i}": threading.Event() for i in range(80)}
        r.status_dict = {}
        sleeps = []
        with mock.patch.object(rmod.time, "sleep", side_effect=sleeps.append), \
                mock.patch.object(rmod.threading, "Thread") as T:
            T.side_effect = lambda target, **kw: mock.Mock(start=target)
            r.catch_up_check(list(r.wake_events), "Kick push")
        self.assertLessEqual(sum(sleeps), rmod.CATCHUP_SPREAD_MAX * 1.3 + 1)

    def test_not_running(self):
        r = self.make()
        r.is_running = False
        self.assertEqual(r.catch_up_check(["a"]), [])


class KickCfg:
    def __init__(self, d):
        self.config_file = os.path.join(d, "config.ini")


class TestKickReconnect(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.rec = FakeRec()
        self.l = KickPushListener(self.rec, KickCfg(self.tmp.name))
        self.addCleanup(self.l._stop.set)
        self.l._wanted = {"alice", "bob"}

    def test_first_connect_logs_normally(self):
        with self.assertLogs(level="INFO") as cm:
            self.l._on_ready()
            self.l._mark_subscribed("alice", 1)
        text = "\n".join(cm.output)
        self.assertIn("subscribing 2 channel(s)", text)
        self.assertIn("listening for alice", text)
        self.assertEqual(self.rec.catchups, [])

    def test_reconnect_one_summary_and_debounced_catchup(self):
        self.l._ever_connected = True
        self.l._dropped_at = rmod.time.monotonic() - 6.2
        with mock.patch.object(rmod.threading, "Timer") as T:
            self.l._on_ready()
        gap = T.call_args[1]["args"][0]
        with self.assertLogs(level="DEBUG") as cm:
            self.l._mark_subscribed("alice", 1)
            self.l._mark_subscribed("bob", 2)
            self.l._reconnect_summary(gap)
        infos = [r.getMessage() for r in cm.records if r.levelno >= logging.INFO]
        self.assertEqual(len(infos), 1, infos)
        self.assertRegex(infos[0], r"dropped by server, reconnected in 6 s - re-subscribed 2 channel\(s\) "
                                   r"\(catch-up check queued\)")
        self.assertEqual(self.rec.catchups, [(["alice", "bob"], "Kick push")])
        # second flap inside 5 min: no second sweep
        with mock.patch.object(rmod.threading, "Timer"):
            self.l._on_ready()
        self.l._reconnect_summary(5)
        self.assertEqual(len(self.rec.catchups), 1)

    def test_socket_error_after_connect_is_quiet(self):
        self.l._healthy = True
        with self.assertLogs(level="DEBUG") as cm:
            self.l._on_socket_error(None, Exception("Connection to remote host was lost."))
            self.l._on_socket_close(None, None, None)
        self.assertFalse([r for r in cm.records if r.levelno >= logging.INFO])

    def test_socket_error_before_connect_still_warns(self):
        with self.assertLogs(level="WARNING"):
            self.l._on_socket_error(None, Exception("refused"))

    def test_websocket_goodbye_error_filtered(self):
        h = ListHandler()
        ws_log = logging.getLogger("websocket")
        ws_log.addHandler(h)
        self.addCleanup(ws_log.removeHandler, h)
        ws_log.error("Connection to remote host was lost. - goodbye")
        ws_log.error("something else")
        self.assertEqual([r.getMessage() for r in h.records], ["something else"])


class TestTwitchReconnect(unittest.TestCase):
    def test_resubscribe_summary_and_catchup(self):
        rec = FakeRec()
        api = FakeApi(token=dict(TOK))
        l = TwitchPushListener(rec, FakeConfig(), presence=FakePresence(), api=api)
        l._tok = dict(TOK)
        l.set_channels(["twitch:bob", "twitch:amy"])
        l._on_message(FakeWs(), WELCOME)
        l._subscribe_all()            # first connect
        l._subs.clear()
        l._session_id = "S2"
        with self.assertLogs(level="INFO") as cm:
            l._subscribe_all(reconnect=True, gap=6.4)
        infos = [m for m in cm.output if m.startswith("INFO")]
        self.assertEqual(len(infos), 1, infos)
        self.assertIn("reconnected in 6 s - re-subscribed 2 channel(s) (catch-up check queued)", infos[0])
        self.assertEqual(rec.catchups, [(["twitch:amy", "twitch:bob"], "Twitch push")])

    def test_welcome_after_drop_passes_reconnect(self):
        l = TwitchPushListener(FakeRec(), FakeConfig(), api=FakeApi(token=dict(TOK)))
        started = []
        with mock.patch("msr.twitch_push.threading.Thread") as T:
            T.side_effect = lambda target, **kw: started.append(kw.get("kwargs")) or mock.Mock()
            l._on_message(FakeWs(), WELCOME)
            l._session_id = None
            l._dropped_at = l.clock() - 3
            l._on_message(FakeWs(), WELCOME.replace("session_welcome-1", "w2"))
        self.assertEqual(started[0]["reconnect"], False)
        self.assertTrue(started[1]["reconnect"])
        self.assertAlmostEqual(started[1]["gap"], 3, delta=1)


if __name__ == "__main__":
    unittest.main()
