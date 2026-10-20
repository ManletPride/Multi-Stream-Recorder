"""Presence for all channels: push parsing, watch-only safety, poller targets."""
import json
import os
import tempfile
import unittest

from msr import presence as pr
from msr.recorder import KickPushListener, KICK_LIVE_EVENT, KICK_STOP_EVENT, parse_kick_push_event


class _FakeConfig:
    def __init__(self, d):
        self.config_file = os.path.join(d, "config.ini")


class _FakeRecorder:
    def __init__(self):
        self.woken = []
        self.started = []
        self.runtime = {}

    def check_now(self, slug):
        self.woken.append(slug)

    def start_channel(self, slug):  # must never be called by push
        self.started.append(slug)


def _frame(event, cid, **ls):
    ls.setdefault("channel_id", cid)
    return json.dumps({"event": event, "channel": f"channel.{cid}",
                       "data": json.dumps({"livestream": ls})})


class TestPushParsing(unittest.TestCase):
    def test_live_with_viewers(self):
        ev = parse_kick_push_event(json.loads(_frame(
            KICK_LIVE_EVENT, 42, session_title="hi", viewer_count=207,
            created_at="2026-10-20T10:00:00Z")))
        self.assertEqual(ev["kind"], "live")
        self.assertEqual(ev["channel_id"], 42)
        self.assertEqual(ev["viewers"], 207)
        self.assertEqual(ev["title"], "hi")

    def test_live_without_viewers_and_stop(self):
        ev = parse_kick_push_event(json.loads(_frame(KICK_LIVE_EVENT, 7)))
        self.assertEqual((ev["kind"], ev["viewers"]), ("live", None))
        ev = parse_kick_push_event(json.loads(_frame(KICK_STOP_EVENT, 7)))
        self.assertEqual((ev["kind"], ev["channel_id"]), ("stop", 7))

    def test_channel_id_from_channel_name_fallback(self):
        msg = {"event": KICK_STOP_EVENT, "channel": "channel.99", "data": "{}"}
        self.assertEqual(parse_kick_push_event(msg)["channel_id"], 99)

    def test_subscription_and_junk(self):
        ev = parse_kick_push_event({"event": "pusher_internal:subscription_succeeded",
                                    "channel": "channel.5"})
        self.assertEqual((ev["kind"], ev["channel_id"]), ("subscribed", 5))
        self.assertIsNone(parse_kick_push_event({"event": "pusher:pong"})["kind"])
        self.assertIsNone(parse_kick_push_event(None)["kind"])
        self.assertEqual(parse_kick_push_event(
            {"event": KICK_LIVE_EVENT, "data": "not json"})["kind"], "live")


class TestListenerSafety(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        with open(os.path.join(self.tmp.name, "kick_channel_ids.json"), "w") as f:
            json.dump({"vimp": 1, "acx6": 2, "lettyvision": 3}, f)
        self.rec = _FakeRecorder()
        self.store = pr.PresenceStore()
        self.l = KickPushListener(self.rec, _FakeConfig(self.tmp.name), presence=self.store)
        self.l.set_channels(["vimp"], watch=["acx6", "lettyvision"])

    def tearDown(self):
        self.tmp.cleanup()

    def test_disabled_channel_live_updates_presence_but_never_records(self):
        self.l._on_message(None, _frame(KICK_LIVE_EVENT, 2, viewer_count=207))
        p = self.store.get("acx6")
        self.assertTrue(p["live"])
        self.assertEqual(p["viewers"], 207)
        self.assertEqual(p["source"], "push")
        self.assertEqual(self.rec.woken, [])
        self.assertEqual(self.rec.started, [])
        self.l._on_message(None, _frame(KICK_STOP_EVENT, 2))
        self.assertFalse(self.store.get("acx6")["live"])

    def test_enabled_channel_live_wakes_worker(self):
        self.l._on_message(None, _frame(KICK_LIVE_EVENT, 1))
        self.assertEqual(self.rec.woken, ["vimp"])
        self.assertTrue(self.store.get("vimp")["live"])

    def test_watch_only_subscription_sets_no_push_flag(self):
        self.l._on_message(None, json.dumps(
            {"event": "pusher_internal:subscription_succeeded", "channel": "channel.2"}))
        self.l._on_message(None, json.dumps(
            {"event": "pusher_internal:subscription_succeeded", "channel": "channel.1"}))
        self.assertNotIn("kick_push:acx6", self.rec.runtime)
        self.assertTrue(self.rec.runtime["kick_push:vimp"])

    def test_unconfirmed_slugs_are_retried(self):
        # lettyvision bug: a slug whose subscribe never confirmed stayed
        # unsubscribed for the whole connection. Now it is "pending".
        self.l._on_message(None, json.dumps(
            {"event": "pusher_internal:subscription_succeeded", "channel": "channel.1"}))
        self.assertEqual(self.l.pending_slugs(), ["acx6", "lettyvision"])
        self.l._note_attempt("lettyvision")
        self.assertEqual(self.l.pending_slugs(), ["acx6"])

    def test_enabling_moves_slug_from_watch_to_wanted(self):
        self.l.set_channels(["vimp", "acx6"])
        self.assertEqual(self.l.all_slugs(), {"vimp", "acx6", "lettyvision"})
        self.l._on_message(None, _frame(KICK_LIVE_EVENT, 2))
        self.assertEqual(self.rec.woken, ["acx6"])

    def test_no_recorder_is_fine(self):
        l = KickPushListener(None, _FakeConfig(self.tmp.name), presence=self.store)
        l.set_channels([], watch=["acx6"])
        l._on_message(None, _frame(KICK_LIVE_EVENT, 2))
        self.assertTrue(self.store.get("acx6")["live"])


class TestPoller(unittest.TestCase):
    CH = [
        {"name": "vimp", "enabled": True},
        {"name": "acx6", "enabled": False},
        {"name": "twitch:foo", "enabled": False},
        {"name": "twitch:bar", "enabled": True},
        {"name": "youtube:@x", "enabled": False},
    ]

    def test_targets_are_disabled_only(self):
        pollers = {"kick": None, "twitch": None}
        self.assertEqual(pr.presence_poll_targets(self.CH, pollers), ["acx6", "twitch:foo"])

    def test_poll_round_only_polls_disabled_and_updates_store(self):
        calls = []

        def kick(name):
            calls.append(("kick", name))
            return {"live": True, "viewers": 99, "since": None}

        def twitch(name):
            calls.append(("twitch", name))
            return None  # failure: quiet, no update

        store = pr.PresenceStore()
        p = pr.PresencePoller(store, lambda: self.CH, {"kick": kick, "twitch": twitch})
        p.poll_round()
        self.assertCountEqual(calls, [("kick", "acx6"), ("twitch", "foo")])
        self.assertEqual(store.get("acx6")["viewers"], 99)
        self.assertIsNone(store.get("twitch:foo"))
        self.assertIsNone(store.get("vimp"))

    def test_poller_exception_is_quiet(self):
        def boom(name):
            raise RuntimeError("x")
        p = pr.PresencePoller(pr.PresenceStore(), lambda: self.CH, {"kick": boom})
        self.assertEqual(p.poll_round(), ["acx6"])


class TestModel(unittest.TestCase):
    def test_kick_channel_json(self):
        self.assertEqual(pr.parse_kick_channel_json({"id": 5, "livestream": None}),
                         (False, None, None, 5))
        live = pr.parse_kick_channel_json(
            {"id": "5", "livestream": {"is_live": True, "viewer_count": 207,
                                       "start_time": "t"}})
        self.assertEqual(live, (True, 207, "t", 5))
        self.assertEqual(pr.parse_kick_channel_json(None), (None, None, None, None))

    def test_slug_candidates(self):
        self.assertEqual(pr.kick_slug_candidates("betty_fae"), ["betty_fae", "betty-fae"])

    def test_store_keeps_viewers_across_push_and_clears_on_stop(self):
        t = [100.0]
        s = pr.PresenceStore(clock=lambda: t[0])
        s.update("a", True, 50, source="poll")
        t[0] = 200.0
        s.update("a", True, None, source="push")
        self.assertEqual(s.get("a")["viewers"], 50)
        self.assertEqual(s.get("a")["since"], 100.0)
        s.update("a", False, source="push")
        self.assertEqual(s.get("a"), {"live": False, "viewers": None, "since": None,
                                      "source": "push", "updated": 200.0})

    def test_combined_state(self):
        live_poll = {"live": True, "source": "poll", "updated": 0}
        live_push = {"live": True, "source": "push", "updated": 1000}
        self.assertEqual(pr.combined_state("recording", None, True), "recording")
        self.assertEqual(pr.combined_state("offline", live_poll, False), "live")
        self.assertEqual(pr.combined_state("offline", live_poll, True, now=1000), "offline")
        self.assertEqual(pr.combined_state("offline", live_push, True, now=1060), "live")
        self.assertEqual(pr.combined_state("offline", live_push, True, now=5000), "offline")
        self.assertEqual(pr.combined_state("offline", {"live": False}, False), "offline")


if __name__ == "__main__":
    unittest.main()
