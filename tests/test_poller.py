"""Tests for the lightweight in-process polling coordinator (msr/poller.py)
and its integration with StreamRecorder, Twitch Helix batch polling, Kick HTTP
polling, and on-demand worker spawning / clean hand-back.
"""
import os
import queue
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from msr.config import Config  # noqa: E402
from msr import poller as poller_mod  # noqa: E402
from msr import presence as presence_mod  # noqa: E402
from msr import twitch_eventsub as tw_mod  # noqa: E402
from msr.recorder import StreamRecorder  # noqa: E402


def _make_config(tmp: str) -> Config:
    cfg = Config(os.path.join(tmp, "config.ini"))
    streams = os.path.join(tmp, "streams")
    os.makedirs(streams, exist_ok=True)
    cfg.config.set("Paths", "streams_dir", streams)
    cfg.config.set("Recording", "min_disk_space_gb", "0.0")
    cfg.config.set("Timeouts", "poll_interval_minutes", "3")
    cfg.config.set("Timeouts", "poll_jitter_percent", "0")
    cfg.save()
    return cfg


def _drain(q: queue.Queue) -> list[tuple[str, dict]]:
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


class HelixBatchHelperTests(unittest.TestCase):
    def test_get_live_streams_chunks_by_100_and_parses_live_only(self):
        calls = []

        def fake_req(method, url, headers=None, **kw):
            calls.append((method, url, headers))
            return 200, {
                "data": [
                    {"user_login": "Alice", "type": "live", "title": "Live Now", "viewer_count": 42},
                    {"user_login": "Rerun", "type": "rerun", "title": "Not live"},
                ]
            }

        tok = {"access_token": "tok123", "client_id": "cid123"}
        logins = [f"user{i}" for i in range(125)]
        got = tw_mod.get_live_streams(tok, logins, request=fake_req)
        self.assertEqual(len(calls), 2)
        self.assertIn("user_login=user0", calls[0][1])
        self.assertIn("user_login=user100", calls[1][1])
        self.assertIn("alice", got)
        self.assertEqual(got["alice"]["viewer_count"], 42)
        self.assertNotIn("rerun", got)

    def test_get_live_streams_401_raises_auth_error(self):
        def fake_req(method, url, headers=None, **kw):
            raise tw_mod.TwitchHTTPError(401, "unauthorized")

        with self.assertRaises(tw_mod.TwitchAuthError):
            tw_mod.get_live_streams({"access_token": "x", "client_id": "c"}, ["alice"], request=fake_req)


class PollCoordinatorTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.cfg = _make_config(self.tmp)

    def test_twitch_batch_helix_checks_all_due_channels_in_one_call_and_spawns_only_live(self):
        clock_now = [100.0]
        q = queue.Queue()
        launched = []
        helix_calls = []
        pres = presence_mod.PresenceStore()

        def fake_helix(tok, logins):
            helix_calls.append(list(logins))
            return {"alice": {"user_login": "alice", "title": "Gaming", "viewer_count": 99, "started_at": "2026-01-01T00:00:00Z"}}

        coord = poller_mod.PollCoordinator(
            self.cfg, {"poll_interval_minutes": 3.0}, q,
            on_live=lambda ch: (launched.append(ch), True)[1],
            presence=pres, data_dir=self.tmp,
            clock=lambda: clock_now[0], helix_streams=fake_helix,
        )
        fake_store = mock.Mock()
        fake_store.get_fresh.return_value = {"access_token": "a", "client_id": "c", "refresh_token": "r"}
        coord._token_store = fake_store

        # Register 3 Twitch channels directly (synchronous step_once without thread pool).
        coord.add_channel("twitch:alice", immediate=True)
        coord.add_channel("twitch:bob", immediate=True)
        coord.add_channel("twitch:carol", immediate=True)
        coord.step_once()

        self.assertEqual(len(helix_calls), 1, "all due Twitch channels batched into 1 Helix call")
        self.assertEqual(set(helix_calls[0]), {"alice", "bob", "carol"})
        self.assertEqual(launched, ["twitch:alice"])
        self.assertFalse(coord.has_idle_channel("twitch:alice"), "live channel paused while worker runs")
        self.assertTrue(coord.has_idle_channel("twitch:bob"))
        self.assertTrue(coord.has_idle_channel("twitch:carol"))
        self.assertTrue(pres.get("twitch:alice")["live"])
        self.assertFalse(pres.get("twitch:bob")["live"])

    def test_twitch_batch_refreshes_on_401_and_falls_back_to_streamlink_when_unauthenticated(self):
        clock_now = [50.0]
        q = queue.Queue()
        launched = []

        # 1) 401 refresh path
        attempts = []

        def flaky_helix(tok, logins):
            attempts.append(tok["access_token"])
            if tok["access_token"] == "stale":
                raise tw_mod.TwitchAuthError("expired")
            return {"alice": {"user_login": "alice", "title": "Live", "viewer_count": 5}}

        coord = poller_mod.PollCoordinator(
            self.cfg, {"poll_interval_minutes": 3.0}, q,
            on_live=lambda ch: (launched.append(ch), True)[1],
            data_dir=self.tmp, clock=lambda: clock_now[0], helix_streams=flaky_helix,
        )
        store = mock.Mock()
        store.get_fresh.side_effect = [
            {"access_token": "stale", "client_id": "c", "refresh_token": "r1"},
            {"access_token": "fresh", "client_id": "c", "refresh_token": "r2"},
        ]
        coord._token_store = store
        coord.add_channel("twitch:alice", immediate=True)
        coord.step_once()
        self.assertEqual(attempts, ["stale", "fresh"])
        self.assertEqual(launched, ["twitch:alice"])

        # 2) Unauthenticated -> streamlink fallback
        coord2 = poller_mod.PollCoordinator(
            self.cfg, {"poll_interval_minutes": 3.0}, q,
            on_live=lambda ch: (launched.append(ch), True)[1],
            data_dir=self.tmp, clock=lambda: clock_now[0],
        )
        empty_store = mock.Mock()
        empty_store.get_fresh.return_value = None
        coord2._token_store = empty_store
        coord2.add_channel("twitch:dave", immediate=True)
        with mock.patch.object(poller_mod, "check_stream_streamlink", return_value=(False, None, None)) as sl:
            coord2.step_once()
        sl.assert_called_once()
        self.assertTrue(coord2.has_idle_channel("twitch:dave"))

    def test_kick_uses_lightweight_http_and_falls_back_to_streamlink_only_when_blocked(self):
        clock_now = [200.0]
        q = queue.Queue()
        launched = []
        pres = presence_mod.PresenceStore()

        def fake_kick(slug, timeout=15):
            if slug == "livechan":
                return {"id": 1, "livestream": {"is_live": True, "viewer_count": 120, "session_title": "Hi"}}
            if slug == "offchan":
                return {"id": 2, "livestream": None}
            return None  # simulated Cloudflare 403 block

        coord = poller_mod.PollCoordinator(
            self.cfg, {"poll_interval_minutes": 3.0}, q,
            on_live=lambda ch: (launched.append(ch), True)[1],
            presence=pres, data_dir=self.tmp,
            clock=lambda: clock_now[0], kick_http=fake_kick,
        )
        coord.add_channel("livechan", immediate=True)
        coord.add_channel("offchan", immediate=True)
        coord.add_channel("blockedchan", immediate=True)

        with mock.patch.object(poller_mod, "check_stream_kick_api", return_value=(False, None, None)) as fallback:
            coord.step_once()

        self.assertEqual(launched, ["livechan"])
        # Streamlink fallback was called ONLY for the blocked channel, not livechan/offchan!
        fallback.assert_called_once()
        self.assertEqual(fallback.call_args[0][0], "blockedchan")
        self.assertTrue(pres.get("livechan")["live"])
        self.assertFalse(pres.get("offchan")["live"])

    def test_push_active_relaxes_offline_poll_to_10_minutes(self):
        clock_now = [1000.0]
        q = queue.Queue()
        coord = poller_mod.PollCoordinator(
            self.cfg,
            {"poll_interval_minutes": 3.0, "kick_push:offchan": True},
            q,
            on_live=lambda ch: True,
            data_dir=self.tmp,
            clock=lambda: clock_now[0],
            kick_http=lambda slug, timeout=15: {"id": 7, "livestream": None},
        )
        coord.add_channel("offchan", immediate=True)
        coord.step_once()
        slot = coord._slots["offchan"]
        self.assertGreaterEqual(slot.next_due - clock_now[0], 600.0)
        events = _drain(q)
        offline_ev = [st for ch, st in events if ch == "offchan" and st["status"] == "Offline"][-1]
        self.assertIn("push: listening", offline_ev["detail"])

    def test_startup_stagger_spreads_non_batched_channels(self):
        clock_now = [10.0]
        q = queue.Queue()
        coord = poller_mod.PollCoordinator(
            self.cfg, {"poll_interval_minutes": 3.0}, q,
            on_live=lambda ch: True,
            data_dir=self.tmp, clock=lambda: clock_now[0],
        )
        # Prevent background thread from immediately mutating due times during inspection
        with mock.patch.object(coord, "_loop", lambda: None):
            coord.start(["kick1", "kick2", "kick3", "twitch:a", "twitch:b"])
        try:
            self.assertEqual(coord._slots["twitch:a"].next_due, 10.0)
            self.assertEqual(coord._slots["twitch:b"].next_due, 10.0)
            dues = [coord._slots[k].next_due for k in ("kick1", "kick2", "kick3")]
            self.assertEqual(dues, sorted(dues))
            self.assertGreater(dues[-1], dues[0])
        finally:
            coord.stop()


class StreamRecorderCoordinatorIntegrationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.cfg = _make_config(self.tmp)

    def test_worker_handoff_resumes_poller_without_crash_restart(self):
        """A worker that emits event='worker_done' and exits 0 hands back to the poller."""
        rec = object.__new__(StreamRecorder)
        rec.config = self.cfg
        rec.channels = ["twitch:alice"]
        rec.processes = {}
        rec.status_dict = {"twitch:alice": {"status": "Recording", "detail": "", "size": "", "time": "", "progress": 0}}
        rec.status_queue = queue.Queue()
        rec.stopped_channels = set()
        rec.wake_events = {}
        rec.runtime = {"poll_interval_minutes": 3.0}
        rec._spawning = set()
        rec._spawn_lock = threading.Lock()
        rec.should_stop = threading.Event()
        rec.is_running = True

        class DeadCleanProc:
            pid = 999
            exitcode = 0

            def is_alive(self):
                return False

        resumed = []

        class FakePoller:
            is_running = True

            def resume_channel(self, ch):
                resumed.append(ch)

        rec.poller = FakePoller()
        rec.processes["twitch:alice"] = [DeadCleanProc()]
        rec.status_queue.put(("twitch:alice", {
            "status": "Offline", "detail": "idle",
            "size": "", "time": "", "progress": 0, "event": "worker_done",
        }))

        spawned = []
        with mock.patch.object(rec, "_spawn_worker", lambda ch, cfg: spawned.append(ch)), \
                mock.patch.object(rec, "_storage_tick", lambda: None):
            rec._supervise_tick({}, now=100.0)

        self.assertEqual(spawned, [], "clean hand-back must not crash-restart the worker")
        self.assertEqual(resumed, ["twitch:alice"], "channel must resume in idle poller")
        self.assertEqual(rec.processes["twitch:alice"], [])
        self.assertNotIn("twitch:alice", rec.stopped_channels)

    def test_launch_live_worker_sets_exit_when_offline_and_scopes_config(self):
        rec = object.__new__(StreamRecorder)
        rec.config = self.cfg
        rec.config.config.set("Fishtank", "password", "secret-ft")
        rec.channels = ["twitch:alice"]
        rec.processes = {"twitch:alice": []}
        rec.status_dict = {"twitch:alice": {"status": "Offline", "detail": "", "size": "", "time": "", "progress": 0}}
        rec.stopped_channels = set()
        rec.wake_events = {}
        rec.runtime = {}
        rec._spawning = set()
        rec._spawn_lock = threading.Lock()
        rec.should_stop = threading.Event()
        rec.is_running = True

        seen_cfg = {}

        class FakeProc:
            pid = 42

            def is_alive(self):
                return True

        def fake_spawn(ch, cfg):
            seen_cfg[ch] = cfg
            return FakeProc()

        with mock.patch.object(rec, "_spawn_worker", fake_spawn):
            ok = rec._launch_live_worker("twitch:alice")

        self.assertTrue(ok)
        self.assertEqual(seen_cfg["twitch:alice"]["_Worker"]["exit_when_offline"], "true")
        self.assertNotIn("Fishtank", seen_cfg["twitch:alice"])


if __name__ == "__main__":
    unittest.main()
