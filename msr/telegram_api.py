"""Bot-only Telegram access. Telethon is optional; nothing here runs at import.

MSR logs in with the bot token (MTProto). It never asks for a phone number
or a user session. Uploads go to the configured group topic and nowhere else.
Incoming updates are not collected.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import threading
import urllib.parse
import urllib.request

from msr.telegram_queue import (
    AuthRejected,
    DestinationRejected,
    NotInstalled,
    landed_in_topic,
    scrub,
)

_API = "https://api.telegram.org"
_MEMBER_OK = ("creator", "administrator", "member", "restricted")
_TEST_TEXT = "Multi-Stream Recorder test. Finished recordings will be posted in this topic."


def installed() -> bool:
    """True when Telethon imports. The rest of MSR runs without it."""
    try:
        import telethon  # noqa: F401
        return True
    except Exception:
        return False


def test_text() -> str:
    return _TEST_TEXT


def owner_is_member(token: str, chat_id: int, user_id: int, opener=None) -> None:
    """Confirm ``user_id`` is in ``chat_id`` via the official Bot API getChatMember.

    The token travels only in the request to api.telegram.org and is never put
    in the exception text. Raises DestinationRejected when the user is absent
    or the bot cannot see the group.
    """
    if not token or ":" not in token:
        raise DestinationRejected("Set the bot token from BotFather.")
    url = f"{_API}/bot{token}/getChatMember"
    data = urllib.parse.urlencode({"chat_id": str(chat_id), "user_id": str(user_id)}).encode("ascii")
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        if opener is None:
            from msr.tls import https_context
            with urllib.request.urlopen(req, timeout=20, context=https_context()) as resp:
                body = json.loads(resp.read().decode("utf-8", "replace"))
        else:
            with opener(req, timeout=20) as resp:
                body = json.loads(resp.read().decode("utf-8", "replace"))
    except Exception as exc:
        # The URL contains the token. The message we keep does not.
        raise DestinationRejected(
            "Could not check that your user id is in the group. Make the bot an admin of the group. "
            + type(exc).__name__) from None
    if not isinstance(body, dict) or not body.get("ok"):
        desc = scrub(str((body or {}).get("description") if isinstance(body, dict) else "") or "refused", token)
        raise DestinationRejected("Telegram refused the member check: " + desc[:200])
    status = str((body.get("result") or {}).get("status") or "")
    if status not in _MEMBER_OK:
        raise DestinationRejected(
            f"Your user id is not a member of that group (status: {status or 'unknown'}). "
            "Nothing was uploaded.")


def _reply_ids(message):
    header = getattr(message, "reply_to", None)
    return getattr(header, "reply_to_msg_id", None), getattr(header, "reply_to_top_id", None)


class BotSession:
    """One Telethon client. Bot token only; updates off.

    Telethon's methods are coroutines unless ``telethon.sync`` has patched
    them. MSR does not import that module (it rewrites the library for every
    caller). This session owns an asyncio loop on the thread that uses it and
    runs each call to completion. The upload thread, the test thread and
    ``msr_ctl`` each call in on their own thread; a thread change starts a
    fresh client, because Telethon will not follow a new loop.
    """

    def __init__(self):
        self._client = None
        self._fp = None
        self._loop = None
        self._owner = None
        self.username = ""

    def _prepare_loop(self) -> asyncio.AbstractEventLoop:
        ident = threading.get_ident()
        if self._loop is not None and not self._loop.is_closed() and self._owner == ident:
            asyncio.set_event_loop(self._loop)
            return self._loop
        if self._owner is not None and self._owner != ident:
            # The previous loop belongs to another thread. Leave it; drop the client.
            logging.debug("Telegram: the calling thread changed; signing in again on this thread")
            self._client = None
            self._fp = None
        elif self._loop is not None and not self._loop.is_closed():
            self._loop.close()
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._owner = ident
        return self._loop

    def _run(self, value):
        """Execute a Telethon result. A coroutine is run on this thread's loop; anything else is returned."""
        if not inspect.isawaitable(value):
            return value
        loop = self._prepare_loop()
        return loop.run_until_complete(value)

    def close(self) -> None:
        client = self._client
        self._client = None
        self._fp = None
        own_loop = (self._loop is not None and not self._loop.is_closed()
                    and self._owner == threading.get_ident())
        if client is not None and own_loop:
            try:
                self._run(client.disconnect())
            except Exception:
                logging.debug("Telegram: disconnect", exc_info=True)
        if own_loop:
            try:
                self._loop.close()
            except Exception:
                pass
            self._loop = None

    def _ensure(self, settings, session: str | None):
        if not installed():
            raise NotInstalled("Telethon is not installed. pip install Telethon, then restart MSR.")
        self._prepare_loop()
        fp = settings.credential_fp()
        if self._client is not None and self._fp == fp:
            return self._client
        self.close()
        self._prepare_loop()
        logging.getLogger("telethon").setLevel(logging.WARNING)
        # Do not import telethon.sync. It patches every client in the process.
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        client = TelegramClient(
            StringSession(session or ""),
            settings.api_id,
            settings.api_hash,
            receive_updates=False,
        )
        # Captions are plain text. Channel names contain underscores; the
        # default Markdown parser would treat those as formatting.
        client.parse_mode = None
        try:
            self._run(client.start(bot_token=settings.bot_token))
            me = self._run(client.get_me())
        except Exception:
            try:
                self._run(client.disconnect())
            except Exception:
                pass
            raise
        if not getattr(me, "bot", False):
            try:
                self._run(client.disconnect())
            except Exception:
                pass
            raise AuthRejected("Telegram signed in as a user account. MSR only logs in with a bot token.")
        self._client = client
        self._fp = fp
        return client

    def connect(self, settings, session: str | None) -> str:
        """Sign in if needed. Returns the session string to store. Never a phone login."""
        client = self._ensure(settings, session)
        me = self._run(client.get_me())
        self.username = getattr(me, "username", "") or ""
        return client.session.save()

    def _entity(self, client, settings):
        from telethon import utils
        try:
            entity = self._run(client.get_entity(settings.chat_id))
        except Exception as exc:
            logging.warning("Telegram: could not open the group (%s)", type(exc).__name__)
            raise DestinationRejected(
                "The group id could not be opened. Add the bot to the group as an admin. "
                + type(exc).__name__) from None
        peer = utils.get_peer_id(entity)
        if int(peer) != int(settings.chat_id):
            raise DestinationRejected("That chat is not the configured group. Nothing was sent.")
        return entity

    def _check_landed(self, message, settings, client):
        from telethon import utils
        peer = utils.get_peer_id(getattr(message, "peer_id", None))
        reply_msg, reply_top = _reply_ids(message)
        if landed_in_topic(peer, reply_msg, reply_top, settings.chat_id, settings.topic_id):
            return int(message.id)
        try:
            self._run(client.delete_messages(settings.chat_id, [message.id]))
        except Exception as exc:
            logging.warning("Telegram: a message landed outside the configured topic and could not be deleted (%s)",
                            type(exc).__name__)
        raise DestinationRejected(
            "The message did not land in the configured topic. The local file was kept.")

    def send_text(self, settings, session: str | None, text: str) -> dict:
        client = self._ensure(settings, session)
        entity = self._entity(client, settings)
        message = self._run(client.send_message(
            entity, text, reply_to=int(settings.topic_id), parse_mode=None))
        return {"message_id": self._check_landed(message, settings, client),
                "session": client.session.save()}

    def send_file(self, settings, session: str | None, path: str, caption: str, progress) -> dict:
        client = self._ensure(settings, session)
        entity = self._entity(client, settings)

        def _progress(sent, total):
            try:
                progress(int(sent or 0), int(total or 0))
            except Exception:
                pass

        message = self._run(client.send_file(
            entity,
            path,
            caption=caption,
            reply_to=int(settings.topic_id),
            force_document=True,
            allow_cache=False,
            parse_mode=None,
            progress_callback=_progress,
        ))
        return {"message_id": self._check_landed(message, settings, client),
                "session": client.session.save()}
