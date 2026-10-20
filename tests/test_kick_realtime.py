"""Kick realtime: negotiation parsing + Centrifugo frames (recorded 2026-10-20)."""
import json
import os
import tempfile
import unittest

from msr import kick_realtime as kr
from msr import presence as pr
from msr.recorder import KickPushListener

# Real responses / frames captured from web.kick.com and
# wss://realtime.us-west-2.platform.kick.com on 2026-10-20 (chat text trimmed).
NEGOTIATE_CENTRIFUGO = {"data": {"connections": [{"credentials": {
    "url": "wss://realtime.us-west-2.platform.kick.com/connection/websocket"},
    "provider": "centrifugo"}], "mode": "websocket"}, "message": "success"}
TOKEN = {"data": {"token": "eyJhbGciOiJIUzI1NiJ9.e30.sig"}, "message": "success"}
CONNECT_REPLY = ('{"id":1,"connect":{"client":"b02318ee-d064-4870-8e23-4ad71b4a896c",'
                 '"version":"6.9.7 PRO","expires":true,"ttl":2700,"ping":25,"pong":true}}')
SUB_REPLY = '{"id":2,"subscribe":{}}'
SUB_DENIED = '{"id":6,"error":{"code":103,"message":"permission denied"}}'
SUB_LIMIT = '{"id":130,"error":{"code":106,"message":"limit exceeded"}}'
CHAT_PUSH = ('{"push":{"channel":"chatrooms.72252222.v2","pub":{"data":{"event":'
             '"App\\\\Events\\\\ChatMessageEvent","data":"{\\"id\\":\\"3ae6\\",'
             '\\"chatroom_id\\":72252222,\\"content\\":\\"hi\\"}"}}}}')


def _pub(channel, event, payload):
    return json.dumps({"push": {"channel": channel, "pub": {"data": {
        "event": event, "data": json.dumps(payload)}}}})


LIVE_PUSH = _pub("channel.2", "App\\Events\\StreamerIsLive",
                 {"livestream": {"id": 133301512, "channel_id": 2,
                                 "session_title": "hello", "created_at": "2026-10-20 06:47:42"}})
# StopStreamBroadcast carries livestream.channel (kick.com player reads
# livestream.channel.is_banned); channel id also comes from the channel name.
STOP_PUSH = _pub("channel.2", "App\\Events\\StopStreamBroadcast",
                 {"livestream": {"id": 133301512, "channel": {"id": 2, "is_banned": False}}})


class TestNegotiation(unittest.TestCase):
    def test_centrifugo(self):
        self.assertEqual(kr.parse_connection_response(NEGOTIATE_CENTRIFUGO),
                         {"provider": "centrifugo", "url":
                          "wss://realtime.us-west-2.platform.kick.com/connection/websocket"})

    def test_pusher_fallback_uses_returned_key(self):
        resp = {"data": {"mode": "websocket", "connections": [
            {"provider": "pusher", "credentials": {"app_key": "abc123", "cluster": "us3"}}]}}
        c = kr.parse_connection_response(resp)
        self.assertEqual(c, {"provider": "pusher", "app_key": "abc123", "cluster": "us3"})
        self.assertTrue(kr.pusher_url("abc123", "us3").startswith(
            "wss://ws-us3.pusher.com/app/abc123?"))

    def test_bad_responses(self):
        self.assertIsNone(kr.parse_connection_response(None))
        self.assertIsNone(kr.parse_connection_response({"data": {"mode": "sse", "connections": []}}))
        self.assertIsNone(kr.parse_connection_response({"data": {"mode": "websocket", "connections": [
            {"provider": "centrifugo", "credentials": {"url": "http://evil"}}]}}))

    def test_negotiate_fetches_token(self):
        calls = []

        def post(path, body):
            calls.append((path, body))
            return NEGOTIATE_CENTRIFUGO if path == kr.CONNECTION_PATH else TOKEN
        c = kr.negotiate("cid-1", post=post)
        self.assertEqual(c["token"], TOKEN["data"]["token"])
        self.assertEqual(calls[0][1]["client"], {"id": "cid-1", "type": "web"})
        self.assertEqual(calls[1], (kr.AUTH_CONNECTION_PATH, {"client_id": "cid-1"}))

    def test_negotiate_failure_raises(self):
        with self.assertRaises(RuntimeError):
            kr.negotiate("x", post=lambda p, b: {"data": {}})


class TestFrames(unittest.TestCase):
    def test_commands(self):
        self.assertEqual(json.loads(kr.cf_subscribe(3, "channel.9")),
                         {"id": 3, "subscribe": {"channel": "channel.9"}})
        self.assertEqual(json.loads(kr.cf_connect(1, "t")),
                         {"id": 1, "connect": {"token": "t", "name": "js"}})

    def test_parse_multi_line_and_ping(self):
        fr = kr.parse_centrifugo_message(CONNECT_REPLY + "\n" + SUB_REPLY + "\n{}")
        self.assertEqual([f["type"] for f in fr], ["reply", "reply", "ping"])
        self.assertEqual(fr[0]["kind"], "connect")
        self.assertEqual(fr[0]["result"]["ttl"], 2700)
        self.assertEqual(fr[1]["kind"], "subscribe")

    def test_errors_and_pubs(self):
        e = kr.parse_centrifugo_message(SUB_LIMIT)[0]
        self.assertEqual((e["type"], e["id"], e["code"]), ("error", 130, 106))
        p = kr.parse_centrifugo_message(CHAT_PUSH)[0]
        self.assertEqual(p["msg"]["event"], "App\\Events\\ChatMessageEvent")
        p = kr.parse_centrifugo_message(LIVE_PUSH)[0]
        self.assertEqual(p["channel"], "channel.2")
        self.assertIn("StreamerIsLive", p["msg"]["event"])
        self.assertEqual(kr.parse_centrifugo_message("garbage\n"), [])


class _WS:
    def __init__(self):
        self.sent = []

    def send(self, s):
        self.sent.append(s)

    def close(self):
        pass


class _Rec:
    def __init__(self):
        self.woken, self.runtime = [], {}

    def check_now(self, slug):
        self.woken.append(slug)


class TestListenerCentrifugo(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        with open(os.path.join(self.tmp.name, "kick_channel_ids.json"), "w") as f:
            json.dump({"vimp": 1, "acx6": 2}, f)

        class C:
            config_file = os.path.join(self.tmp.name, "config.ini")
        self.rec, self.store, self.ws = _Rec(), pr.PresenceStore(), _WS()
        self.l = KickPushListener(self.rec, C(), presence=self.store)
        self.l.set_channels(["vimp"], watch=["acx6"])
        self.l._protocol = "centrifugo"
        self.l._conn = {"provider": "centrifugo", "token": "tok", "client_id": "c"}
        self.l._ws = self.ws

    def tearDown(self):
        self.tmp.cleanup()

    def _feed(self, raw):
        self.l._on_message(self.ws, raw)

    def test_full_flow(self):
        self.l._on_open(self.ws)
        self.assertEqual(json.loads(self.ws.sent[0])["connect"]["token"], "tok")
        self._feed(CONNECT_REPLY)  # -> subscribe both
        subs = {json.loads(s)["subscribe"]["channel"]: json.loads(s)["id"]
                for s in self.ws.sent[1:]}
        self.assertEqual(set(subs), {"channel.1", "channel.2"})
        for cid in subs.values():
            self._feed(json.dumps({"id": cid, "subscribe": {}}))
        self.assertEqual(self.l._subscribed, {"vimp", "acx6"})
        self.assertTrue(self.rec.runtime["kick_push:vimp"])
        self.assertNotIn("kick_push:acx6", self.rec.runtime)
        self.assertEqual(self.l.pending_slugs(), [])

        self._feed("{}")
        self.assertEqual(self.ws.sent[-1], "{}")  # pong

        self._feed(LIVE_PUSH)  # acx6 is disabled: presence only
        self.assertTrue(self.store.get("acx6")["live"])
        self.assertEqual(self.rec.woken, [])
        self._feed(STOP_PUSH)
        self.assertFalse(self.store.get("acx6")["live"])

        self._feed(_pub("channel.1", "App\\Events\\StreamerIsLive",
                        {"livestream": {"channel_id": 1}}))
        self.assertEqual(self.rec.woken, ["vimp"])
        self._feed(CHAT_PUSH)  # ignored
        self.assertEqual(self.rec.woken, ["vimp"])

    def test_subscribe_error_leaves_slug_pending_for_retry(self):
        self.l._on_open(self.ws)
        self._feed(CONNECT_REPLY)
        first = json.loads(self.ws.sent[1])["id"]
        self._feed(json.dumps({"id": first, "error": {"code": 106, "message": "limit exceeded"}}))
        self.assertTrue(self.l._limit_logged)
        self.assertEqual(len(self.l._subscribed), 0)
        self.l._retry_at.clear()
        self.assertEqual(len(self.l.pending_slugs()), 2)

    def test_failure_logging_is_rate_limited(self):
        with self.assertLogs(level="DEBUG") as cm:
            for _ in range(5):
                self.l._log_failure("socket error: 4001")
        warnings = [r for r in cm.records if r.levelname == "WARNING"]
        self.assertEqual(len(warnings), 1)

    def test_token_ttl_recorded(self):
        self._feed(CONNECT_REPLY)
        self.assertGreater(self.l._token_expires, 0)


if __name__ == "__main__":
    unittest.main()
