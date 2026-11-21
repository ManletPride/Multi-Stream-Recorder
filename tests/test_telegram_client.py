"""Telethon calls must actually run. Fakes of the transport cannot see this.

Telethon's methods are coroutines unless ``telethon.sync`` has been imported.
A test that replaces ``BotSession`` with a fake never calls them, so this file
uses the real ``TelegramClient`` class and only replaces the network methods.
"""

import asyncio
import inspect
import json
import sys
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from tests.test_telegram import CHAT, TOKEN

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.tl.types import PeerChannel
except Exception:  # pragma: no cover - the suite still runs without the optional package
    TelegramClient = None


def _settings():
    from msr.telegram_queue import Settings
    return Settings(True, 12345, "a" * 32, TOKEN, int(CHAT), 55, 4242, False)


def _message(mid, topic, peer):
    return SimpleNamespace(
        id=mid,
        peer_id=peer,
        reply_to=SimpleNamespace(reply_to_msg_id=topic, reply_to_top_id=topic),
    )


@unittest.skipUnless(TelegramClient is not None, "Telethon is not installed")
class RealClientTests(unittest.TestCase):
    def test_an_unpatched_get_me_is_a_coroutine(self):
        self.assertNotIn("telethon.sync", sys.modules)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        probe = TelegramClient(StringSession(), 1, "b" * 32, receive_updates=False)
        try:
            pending = probe.get_me()
            self.assertTrue(inspect.iscoroutine(pending))
            pending.close()
            probe.disconnect()
        finally:
            if not loop.is_closed():
                loop.close()

    def test_session_awaits_every_call_on_a_worker_thread_and_on_this_thread(self):
        """The upload thread and msr_ctl (this thread, when no uploader is running) both sign in."""
        self.assertNotIn("telethon.sync", sys.modules)
        from msr.telegram_api import BotSession, DestinationRejected, owner_is_member
        peer = PeerChannel(1234567890)
        calls = []
        started_with_token = []
        topics = [55, 1, 55]
        progress = []

        async def start(self, *args, **kwargs):
            calls.append("start")
            started_with_token.append("bot_token" in kwargs and bool(kwargs["bot_token"]))
            return self

        async def get_me(self):
            calls.append("get_me")
            return SimpleNamespace(bot=True, id=99, username="recorder_bot")

        async def get_entity(self, chat):
            calls.append("get_entity")
            return peer

        async def send_message(self, entity, text, **kwargs):
            calls.append("send_message")
            return _message(7, topics.pop(0), peer)

        async def send_file(self, entity, path, **kwargs):
            calls.append("send_file")
            kwargs["progress_callback"](4, 9)
            return _message(8, int(kwargs["reply_to"]), peer)

        async def delete_messages(self, entity, ids):
            calls.append("delete_messages")

        async def disconnect(self):
            calls.append("disconnect")

        patches = {
            "start": start, "get_me": get_me, "get_entity": get_entity,
            "send_message": send_message, "send_file": send_file,
            "delete_messages": delete_messages, "disconnect": disconnect,
        }
        settings = _settings()
        errors = []

        def opener(req, timeout=20):
            if "api.telegram.org" not in req.full_url:
                raise AssertionError("member check left api.telegram.org")

            class Response:
                def read(self):
                    return json.dumps({"ok": True, "result": {"status": "creator"}}).encode()

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    return False

            return Response()

        def on_worker():
            try:
                session = BotSession()
                session.connect(settings, None)
                self.assertEqual(session.username, "recorder_bot")
                owner_is_member(TOKEN, int(CHAT), 4242, opener=opener)
                session.send_text(settings, "saved", "hello")
                session.send_file(settings, "saved", "clip.mp4", "caption",
                                   lambda sent, total: progress.append((sent, total)))
                with self.assertRaises(DestinationRejected):
                    session.send_text(settings, "saved", "wrong topic")
                session.close()
            except Exception as exc:
                errors.append(exc)

        with mock.patch.multiple(TelegramClient, **patches):
            worker = threading.Thread(target=on_worker, name="telegram-upload-test")
            worker.start()
            worker.join(timeout=10)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            # msr_ctl sends the test on the calling thread when the upload thread is not running.
            ctl = BotSession()
            ctl.connect(settings, None)
            self.assertEqual(ctl.username, "recorder_bot")
            ctl.send_text(settings, "saved", "from msr_ctl")
            ctl.close()

        self.assertNotIn("telethon.sync", sys.modules)
        self.assertTrue(started_with_token)
        self.assertTrue(all(started_with_token))
        for name in ("start", "get_me", "get_entity", "send_message", "send_file",
                     "delete_messages", "disconnect"):
            self.assertIn(name, calls, name)
        self.assertEqual(progress, [(4, 9)])
        self.assertGreaterEqual(calls.count("get_me"), 2)

    def test_a_user_session_is_reported_as_not_a_bot(self):
        from msr.telegram_api import AuthRejected, BotSession

        async def start(self, *args, **kwargs):
            return self

        async def get_me(self):
            return SimpleNamespace(bot=False, id=1, username="a_person")

        async def disconnect(self):
            return None

        session = BotSession()
        with mock.patch.multiple(TelegramClient, start=start, get_me=get_me, disconnect=disconnect):
            with self.assertRaises(AuthRejected) as caught:
                session.connect(_settings(), None)
            session.close()
        self.assertIn("user account", str(caught.exception))
        self.assertNotIn("bot token was rejected", str(caught.exception))

    def test_classify_keeps_the_three_sign_in_failures_apart(self):
        from msr.telegram_queue import AuthRejected, classify_failure

        class ApiIdInvalidError(Exception):
            pass

        class AccessTokenInvalidError(Exception):
            pass

        api = classify_failure(ApiIdInvalidError("api_hash=" + ("a" * 32)))
        token = classify_failure(AccessTokenInvalidError(TOKEN))
        user = classify_failure(AuthRejected(
            "Telegram signed in as a user account. MSR only logs in with a bot token."))
        self.assertIn("api id or api hash", str(api))
        self.assertNotIn("a" * 32, str(api))
        self.assertIn("bot token was rejected", str(token))
        self.assertNotIn(TOKEN, str(token))
        self.assertIn("user account", str(user))
        self.assertNotIn("bot token was rejected", str(user))
