"""Twitch EventSub push: helpers + listener with a fake API and fake socket."""
import json
import os
import tempfile
import unittest

from msr import twitch_eventsub as te
from msr.twitch_push import TwitchPushListener


def frame(mtype, payload, **meta):
    md = {"message_id": meta.pop("mid", mtype + "-1"), "message_type": mtype}
    md.update(meta)
    return json.dumps({"metadata": md, "payload": payload})


WELCOME = frame("session_welcome", {"session": {
    "id": "AQoQexAWVYKSTIu4ec_2VAxyuhAB", "status": "connected",
    "keepalive_timeout_seconds": 30, "reconnect_url": None}})
KEEPALIVE = frame("session_keepalive", {})
RECONNECT = frame("session_reconnect", {"session": {
    "id": "AQoQexAWVYKSTIu4ec_2VAxyuhAB", "status": "reconnecting",
    "keepalive_timeout_seconds": None, "reconnect_url": "wss://eventsub.wss.twitch.tv?x=1"}})


def online(login, bid, mid="n1"):
    return frame("notification", {
        "subscription": {"id": "s-" + login, "type": "stream.online", "status": "enabled",
                         "condition": {"broadcaster_user_id": bid}},
        "event": {"id": "9001", "broadcaster_user_id": bid, "broadcaster_user_login": login,
                  "type": "live", "started_at": "2026-11-01T15:00:00Z"}},
        mid=mid, subscription_type="stream.online")


class TestParse(unittest.TestCase):
    def test_frames(self):
        w = te.parse_message(WELCOME)
        self.assertEqual((w["type"], w["session_id"], w["keepalive"]),
                         ("welcome", "AQoQexAWVYKSTIu4ec_2VAxyuhAB", 30))
        self.assertEqual(te.parse_message(KEEPALIVE)["type"], "keepalive")
        r = te.parse_message(RECONNECT)
        self.assertEqual(r["reconnect_url"], "wss://eventsub.wss.twitch.tv?x=1")
        n = te.parse_message(online("Foo", "42"))
        self.assertEqual((n["type"], n["login"], n["broadcaster_id"]), ("notification", "foo", "42"))
        self.assertEqual(te.parse_message("not json")["type"], "unknown")
        self.assertEqual(te.parse_message(b"[]")["type"], "unknown")

    def test_ws_url(self):
        self.assertEqual(te.ws_url(30), "wss://eventsub.wss.twitch.tv/ws?keepalive_timeout_seconds=30")


class FakeRequest:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, url, headers=None, form=None, body=None, timeout=15):
        self.calls.append((method, url, headers, form, body))
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return 200, r


TOK = {"client_id": "cid", "access_token": "at", "refresh_token": "rt", "expires_at": 0}


class TestHttp(unittest.TestCase):
    def test_device_poll_pending_then_token(self):
        req = FakeRequest([te.TwitchHTTPError(400, "authorization_pending"),
                           {"access_token": "a", "refresh_token": "r", "expires_in": 100}])
        self.assertIsNone(te.device_poll("cid", "dc", request=req))
        tok = te.device_poll("cid", "dc", request=req, now=lambda: 1000)
        self.assertEqual((tok["access_token"], tok["expires_at"]), ("a", 1100))
        self.assertEqual(req.calls[0][3]["grant_type"], te.DEVICE_GRANT)
        self.assertNotIn("client_secret", req.calls[0][3])

    def test_device_denied(self):
        req = FakeRequest([te.TwitchHTTPError(400, "access_denied")])
        with self.assertRaises(te.TwitchAuthError):
            te.device_poll("cid", "dc", request=req)

    def test_refresh_public_client(self):
        req = FakeRequest([{"access_token": "a2", "refresh_token": "r2", "expires_in": 50}])
        tok = te.refresh(dict(TOK, login="me", user_id="1"), request=req, now=lambda: 10)
        self.assertEqual((tok["refresh_token"], tok["login"]), ("r2", "me"))
        self.assertEqual(req.calls[0][3], {"client_id": "cid", "grant_type": "refresh_token",
                                           "refresh_token": "rt"})
        req = FakeRequest([te.TwitchHTTPError(400, "Invalid refresh token")])
        with self.assertRaises(te.TwitchAuthError):
            te.refresh(TOK, request=req)

    def test_subscription_body_and_statuses(self):
        req = FakeRequest([{"data": [{"id": "sub1"}], "total_cost": 1, "max_total_cost": 10},
                           te.TwitchHTTPError(409), te.TwitchHTTPError(429),
                           te.TwitchHTTPError(401)])
        self.assertEqual(te.create_subscription(TOK, "sess", "42", request=req),
                         ("ok", "sub1", 1, 10))
        m, url, headers, _, body = req.calls[0]
        self.assertEqual(headers, {"Authorization": "Bearer at", "Client-Id": "cid"})
        self.assertEqual(body["transport"], {"method": "websocket", "session_id": "sess"})
        self.assertEqual(body["condition"], {"broadcaster_user_id": "42"})
        self.assertEqual(te.create_subscription(TOK, "s", "1", request=req)[0], "exists")
        self.assertEqual(te.create_subscription(TOK, "s", "1", request=req)[0], "limit")
        with self.assertRaises(te.TwitchAuthError):
            te.create_subscription(TOK, "s", "1", request=req)

    def test_lookup_batches(self):
        req = FakeRequest([{"data": [{"login": "a", "id": "1"}]}, {"data": []}])
        out = te.lookup_user_ids(TOK, [f"u{i}" for i in range(150)], request=req)
        self.assertEqual(len(req.calls), 2)
        self.assertEqual(out, {"a": "1"})

    def test_token_store_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(te.load_token(d))
            te.save_token(d, TOK)
            self.assertEqual(te.load_token(d)["access_token"], "at")
            te.clear_token(d)
            self.assertIsNone(te.load_token(d))


class FakeConfig:
    def __init__(self, enabled=True, cid="cid"):
        self.v = {("Twitch", "push_enabled"): enabled, ("Twitch", "client_id"): cid}

    def getboolean(self, s, k, fallback=None):
        return self.v.get((s, k), fallback)

    def get(self, s, k, fallback=None):
        return self.v.get((s, k), fallback)


class FakeApi:
    """te with network calls replaced."""
    TwitchAuthError = te.TwitchAuthError
    parse_message = staticmethod(te.parse_message)
    ws_url = staticmethod(te.ws_url)

    def __init__(self, token=None, limit_after=None, auth_fail=False):
        self.token = token
        self.created = []
        self.deleted = []
        self.limit_after = limit_after
        self.auth_fail = auth_fail

    def load_token(self, d):
        return self.token

    def save_token(self, d, t):
        self.token = t

    def needs_refresh(self, tok, now=None):
        return False

    def refresh(self, tok):
        return dict(tok, refresh_token=tok["refresh_token"] + "+")

    def validate(self, tok):
        return {}

    def lookup_user_ids(self, tok, logins):
        return {l: str(100 + i) for i, l in enumerate(sorted(logins))}

    def create_subscription(self, tok, session, bid):
        if self.auth_fail:
            raise te.TwitchAuthError("subscription refused (401)")
        if self.limit_after is not None and len(self.created) >= self.limit_after:
            return ("limit", None, None, None)
        self.created.append((session, bid))
        return ("ok", "sub-" + bid, len(self.created), 10)

    def delete_subscription(self, tok, sub_id):
        self.deleted.append(sub_id)
        return True


class FakeRecorder:
    def __init__(self):
        self.woken = []
        self.runtime = {}

    def check_now(self, key):
        self.woken.append(key)


class FakePresence:
    def __init__(self):
        self.updates = []

    def update(self, key, live, viewers=None, since=None, source=""):
        self.updates.append((key, live, source))


class FakeWs:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def make(api=None, cfg=None):
    rec, pres = FakeRecorder(), FakePresence()
    api = api or FakeApi(token=dict(TOK))
    lst = TwitchPushListener(rec, cfg or FakeConfig(), presence=pres, api=api)
    lst._tok = dict(TOK)
    return lst, rec, pres, api


class TestListener(unittest.TestCase):
    def welcome(self, lst):
        lst._on_message(FakeWs(), WELCOME)
        lst._subscribe_all()  # run inline instead of the thread

    def test_subscribes_wanted_first_and_wakes_on_online(self):
        lst, rec, pres, api = make()
        lst.set_channels(["twitch:bob", "kickname", "twitch:amy"], watch=["twitch:zed"])
        self.welcome(lst)
        self.assertEqual(sorted(lst.covered()), ["amy", "bob", "zed"])
        self.assertEqual(api.created[0][0], "AQoQexAWVYKSTIu4ec_2VAxyuhAB")
        self.assertTrue(rec.runtime["twitch_push:twitch:bob"])
        self.assertNotIn("twitch_push:twitch:zed", rec.runtime)
        lst._on_message(FakeWs(), online("bob", lst._ids["bob"]))
        lst._on_message(FakeWs(), online("bob", lst._ids["bob"]))  # duplicate id ignored
        lst._on_message(FakeWs(), online("zed", lst._ids["zed"], mid="n2"))
        self.assertEqual(rec.woken, ["twitch:bob"])
        self.assertEqual([u[0] for u in pres.updates], ["twitch:bob", "twitch:zed"])

    def test_cost_limit_prefers_recording_channels(self):
        lst, rec, pres, api = make(api=FakeApi(token=dict(TOK), limit_after=2))
        with self.assertLogs(level="WARNING") as cm:
            lst.set_channels(["twitch:c", "twitch:d"], watch=["twitch:a", "twitch:b"])
            self.welcome(lst)
        self.assertEqual(sorted(lst.covered()), ["c", "d"])
        self.assertIn("rely on polling", "\n".join(cm.output))

    def test_reconnect_keeps_subscriptions(self):
        lst, rec, pres, api = make()
        lst.set_channels(["twitch:bob"])
        self.welcome(lst)
        ws = FakeWs()
        lst._on_message(ws, RECONNECT)
        self.assertTrue(ws.closed)
        self.assertEqual(lst._reconnect_url, "wss://eventsub.wss.twitch.tv?x=1")
        n = len(api.created)
        lst._on_message(FakeWs(), WELCOME.replace("session_welcome-1", "w2"))
        self.assertEqual(len(api.created), n)  # no resubscribe on the moved session
        self.assertEqual(lst.covered(), {"bob"})

    def test_auth_failure_turns_push_off(self):
        lst, rec, pres, api = make(api=FakeApi(token=dict(TOK), auth_fail=True))
        lst.set_channels(["twitch:bob"])
        with self.assertLogs(level="WARNING") as cm:
            self.welcome(lst)
        self.assertTrue(lst._auth_failed and lst._stop.is_set())
        self.assertIn("turned off", "\n".join(cm.output))
        self.assertFalse(rec.runtime.get("twitch_push:twitch:bob"))

    def test_removed_channel_unsubscribes(self):
        lst, rec, pres, api = make()
        lst.set_channels(["twitch:bob", "twitch:amy"])
        self.welcome(lst)
        lst.set_channels(["twitch:amy"])
        self.assertEqual(api.deleted, ["sub-" + lst._ids["bob"]])
        self.assertFalse(rec.runtime["twitch_push:twitch:bob"])

    def test_revocation(self):
        lst, rec, pres, api = make()
        lst.set_channels(["twitch:bob"])
        self.welcome(lst)
        rev = frame("revocation", {"subscription": {"id": "x", "type": "stream.online",
                                                    "status": "user_removed",
                                                    "condition": {"broadcaster_user_id": lst._ids["bob"]}}})
        with self.assertLogs(level="INFO"):
            lst._on_message(FakeWs(), rev)
        self.assertEqual(lst.covered(), set())

    def test_start_degrades_quietly(self):
        for cfg, api in ((FakeConfig(enabled=False), FakeApi(token=dict(TOK))),
                         (FakeConfig(cid=""), FakeApi(token=dict(TOK))),
                         (FakeConfig(), FakeApi(token=None)),
                         (FakeConfig(cid="other"), FakeApi(token=dict(TOK)))):
            lst = TwitchPushListener(FakeRecorder(), cfg, api=api)
            lst.start()
            self.assertIsNone(lst._thread)

    def test_watchdog_closes_silent_socket(self):
        t = [0.0]
        lst, *_ = make()
        lst.clock = lambda: t[0]
        lst._keepalive = 1
        ws = FakeWs()
        lst._ws = ws
        lst._last_msg = 0.0
        t[0] = 100.0
        lst._stop.wait = lambda s: False
        lst._watchdog(ws)
        self.assertTrue(ws.closed)


if __name__ == "__main__":
    unittest.main()
