"""Twitch go-live push via EventSub WebSocket (opt-in, additive to polling).

Mirrors KickPushListener: one socket, a ``stream.online`` notification for a
recording channel calls ``recorder.check_now(key)``; presence-only channels
just update the presence store. Never starts, stops or kills a recording.
Without a token, client id or websocket-client it logs once and does nothing.
"""

import logging
import threading
import time

from msr import twitch_eventsub as te
from msr.deps import HAS_WEBSOCKET

RECONNECT_BASE = 5
RECONNECT_MAX = 600
FAILURE_LOG_EVERY = 3600
VALIDATE_EVERY = 3600


def _login(key):
    return key.split(":", 1)[1].lower() if key.startswith("twitch:") else None


class TwitchPushListener:
    def __init__(self, recorder, config, presence=None, data_dir=".", api=te,
                 clock=time.monotonic):
        self.recorder = recorder
        self.config = config
        self.presence = presence
        self.data_dir = data_dir
        self.api = api
        self.clock = clock
        self.logger = logging.getLogger()
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self._ws = None
        self._wanted = set()      # twitch:<login> keys with a worker
        self._watch = set()       # presence only
        self._ids = {}            # login -> broadcaster id
        self._subs = {}           # login -> subscription id (current session)
        self._session_id = None
        self._reconnect_url = None
        self._keepalive = te.KEEPALIVE_SECONDS
        self._last_msg = 0.0
        self._last_validate = 0.0
        self._seen = []           # recent message ids (dedupe)
        self._tok = None
        self._auth_failed = False
        self._limit_logged = False
        self._last_fail_log = -FAILURE_LOG_EVERY
        self._healthy = False
        self._ever_connected = False
        self._dropped_at = None
        from msr.recorder import CatchUp
        self._catchup = CatchUp()

    # -- public API (same shape as KickPushListener) --

    def enabled(self):
        return bool(self.config.getboolean("Twitch", "push_enabled", False))

    def client_id(self):
        return (self.config.get("Twitch", "client_id", "") or "").strip() or te.DEFAULT_CLIENT_ID

    def start(self):
        if not self.enabled():
            return
        if not HAS_WEBSOCKET:
            self.logger.info("Twitch push: websocket-client not installed - polling only")
            return
        if not self.client_id():
            self.logger.info("Twitch push: no [Twitch] client_id - polling only")
            return
        tok = self.api.load_token(self.data_dir)
        if not tok or tok.get("client_id") != self.client_id():
            self.logger.info("Twitch push: not authorized yet - polling only "
                             "(run: python -m msr.twitch_eventsub authorize)")
            return
        self._tok = tok
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="twitch-push")
        self._thread.start()

    def stop(self):
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def restart(self):
        """Pick up a new token / the push setting without restarting the session."""
        self.stop()
        t = self._thread
        if t is not None and t.is_alive():
            t.join(5)
        self._thread = None
        self._auth_failed = False
        self._session_id = None
        self._reconnect_url = None
        with self._lock:
            self._subs.clear()
        self._clear_flags()
        self.start()

    def set_channels(self, keys, watch=None):
        new = {k for k in keys if _login(k)}
        with self._lock:
            left = self._wanted - new
            self._wanted = new
            self._watch = ({k for k in (watch or []) if _login(k)} if watch is not None
                           else self._watch) - new
            keep = {_login(k) for k in self._wanted | self._watch}
            gone = [l for l in self._subs if l not in keep]
        for k in left:
            self._set_flag(k, False)
        if self._session_id:
            for login in gone:
                self._unsubscribe(login)
            threading.Thread(target=self._subscribe_all, daemon=True).start()

    def covered(self):
        with self._lock:
            return set(self._subs)

    # -- connection loop --

    def _log_failure(self, what):
        now = self.clock()
        if now - self._last_fail_log >= FAILURE_LOG_EVERY:
            self._last_fail_log = now
            self.logger.warning(f"Twitch push: {what} - polling continues")
        else:
            self.logger.debug(f"Twitch push: {what}")

    def _ensure_token(self):
        tok = self._tok
        if self.api.needs_refresh(tok, time.time()):
            tok = self.api.refresh(tok)
            self.api.save_token(self.data_dir, tok)
            self._tok = tok
        if self.clock() - self._last_validate >= VALIDATE_EVERY:
            self.api.validate(tok)
            self._last_validate = self.clock()
        return tok

    def _run(self):
        import websocket
        backoff = RECONNECT_BASE
        while not self._stop.is_set():
            try:
                self._ensure_token()
            except te.TwitchAuthError as e:
                self._auth_off(str(e))
                return
            except Exception as e:
                self._log_failure(f"token check failed ({e})")
            else:
                url = self._reconnect_url or self.api.ws_url(te.KEEPALIVE_SECONDS)
                carried = bool(self._reconnect_url)
                self._reconnect_url = None
                if not carried:
                    self._session_id = None
                    with self._lock:
                        self._subs.clear()
                self._healthy = False
                try:
                    self._ws = websocket.WebSocketApp(
                        url, on_message=self._on_message,
                        on_error=self._on_socket_error)
                    self._last_msg = self.clock()
                    watchdog = threading.Thread(target=self._watchdog, args=(self._ws,),
                                                daemon=True)
                    watchdog.start()
                    self._ws.run_forever()
                except Exception as e:
                    self._log_failure(f"connection failed ({e})")
                finally:
                    self._ws = None
                    if not self._reconnect_url:
                        if self._healthy and self._dropped_at is None:
                            self._dropped_at = self.clock()
                        self._session_id = None
                        self._clear_flags()
            if self._stop.is_set() or self._auth_failed:
                break
            if self._reconnect_url:
                continue  # server-requested move: go now, subscriptions carry over
            if self._healthy:
                backoff = RECONNECT_BASE
            if self._stop.wait(backoff):
                break
            backoff = min(backoff * 2, RECONNECT_MAX)

    def _on_socket_error(self, ws, e):
        if self._healthy:
            self.logger.debug(f"Twitch push: socket error after connect: {e}")
        else:
            self._log_failure(f"socket error: {e}")

    def _watchdog(self, ws):
        """No keepalive/notification for keepalive+10 s means the socket is dead."""
        while not self._stop.wait(2):
            if self._ws is not ws:
                return
            if self.clock() - self._last_msg > self._keepalive + 10:
                self._log_failure("keepalive missed - reconnecting")
                try:
                    ws.close()
                except Exception:
                    pass
                return

    def _on_message(self, ws, raw):
        self._last_msg = self.clock()
        m = self.api.parse_message(raw)
        t = m["type"]
        mid = m.get("message_id")
        if mid:
            if mid in self._seen:
                return
            self._seen = (self._seen + [mid])[-200:]
        if t == "welcome":
            self._keepalive = int(m.get("keepalive") or te.KEEPALIVE_SECONDS)
            carried = self._session_id is not None
            self._session_id = m.get("session_id")
            self._healthy = True
            if carried:
                self.logger.info("Twitch push: moved to new server, subscriptions kept")
                with self._lock:
                    logins = list(self._subs)
                for login in logins:
                    self._flag_login(login, True)
            else:
                reconnect = self._ever_connected
                self._ever_connected = True
                dropped, self._dropped_at = self._dropped_at, None
                threading.Thread(target=self._subscribe_all, kwargs={
                    "reconnect": reconnect,
                    "gap": (self.clock() - dropped) if (reconnect and dropped) else None},
                    daemon=True, name="twitch-push-subscribe").start()
        elif t == "reconnect":
            self._reconnect_url = m.get("reconnect_url")
            try:
                ws.close()
            except Exception:
                pass
        elif t == "notification" and m.get("sub_type") == "stream.online":
            login = m.get("login") or self._login_for_id(m.get("broadcaster_id"))
            if login:
                self._handle_live(login, m)
        elif t == "revocation":
            login = self._login_for_id(m.get("broadcaster_id"))
            with self._lock:
                if login:
                    self._subs.pop(login, None)
            if login:
                self._flag_login(login, False)
            self.logger.info(f"Twitch push: subscription revoked for {login} ({m.get('status')})")
            if m.get("status") == "authorization_revoked":
                self._auth_off("authorization revoked")

    # -- subscriptions --

    def _login_for_id(self, bid):
        for login, i in self._ids.items():
            if i == bid:
                return login
        return None

    def _priority(self):
        with self._lock:
            wanted = sorted(_login(k) for k in self._wanted)
            watch = sorted(_login(k) for k in self._watch)
        return wanted + [l for l in watch if l not in wanted]

    def _subscribe_all(self, reconnect=False, gap=None):
        session = self._session_id
        if not session:
            return
        try:
            tok = self._ensure_token()
            todo = [l for l in self._priority() if l not in self._subs]
            missing = [l for l in todo if l not in self._ids]
            if missing:
                self._ids.update(self.api.lookup_user_ids(tok, missing))
            for login in todo:
                if self._stop.is_set() or session != self._session_id:
                    return
                if len(self._subs) >= te.MAX_TOTAL_COST:
                    self._log_limit()
                    return
                bid = self._ids.get(login)
                if not bid:
                    continue
                status, sub_id, _, _ = self.api.create_subscription(tok, session, bid)
                if status == "ok":
                    with self._lock:
                        self._subs[login] = sub_id
                    self._flag_login(login, True)
                    (self.logger.debug if reconnect else self.logger.info)(
                        f"Twitch push: listening for {login}")
                elif status == "limit":
                    self._log_limit()
                    return
            if todo and not self._subs:
                self._log_failure("no subscription accepted")
            elif reconnect:
                self._reconnect_summary(gap)
        except te.TwitchAuthError as e:
            self._auth_off(str(e))
        except Exception as e:
            self._log_failure(f"subscribe failed ({e})")

    def _reconnect_summary(self, gap):
        queued = self._catchup.due()
        took = f" in {gap:.0f} s" if gap is not None else ""
        self.logger.info(f"Twitch push: connection dropped, reconnected{took} - re-subscribed "
                         f"{len(self._subs)} channel(s)" + (" (catch-up check queued)" if queued else ""))
        if queued and self.recorder is not None:
            with self._lock:
                wanted = sorted(self._wanted)
            try:
                self.recorder.catch_up_check(wanted, "Twitch push")
            except Exception as e:
                self.logger.debug(f"Twitch push: catch-up check failed: {e}")

    def _unsubscribe(self, login):
        with self._lock:
            sub_id = self._subs.pop(login, None)
        self._flag_login(login, False)
        if sub_id and self._tok:
            try:
                self.api.delete_subscription(self._tok, sub_id)
            except Exception:
                pass

    def _log_limit(self):
        if not self._limit_logged:
            self._limit_logged = True
            self.logger.warning(f"Twitch push: Twitch allows {te.MAX_TOTAL_COST} go-live "
                                "subscriptions per authorization - other Twitch channels "
                                "rely on polling")

    def _auth_off(self, why):
        self._auth_failed = True
        self._stop.set()
        self.logger.warning(f"Twitch push: {why} - turned off, polling continues "
                            "(authorize again to re-enable)")
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        self._clear_flags()

    # -- events --

    def _handle_live(self, login, m):
        key = f"twitch:{login}"
        with self._lock:
            recording = key in self._wanted
        if self.presence is not None:
            try:
                self.presence.update(key, True, None, m.get("started_at"), source="push")
            except Exception:
                pass
        if not recording:
            self.logger.info(f"Twitch push: {login} went LIVE (presence only)")
            return
        self.logger.info(f"Twitch push: {login} went LIVE - waking worker")
        if self.recorder is not None:
            self.recorder.check_now(key)

    # -- worker status flag --

    def _flag_login(self, login, value):
        key = f"twitch:{login}"
        with self._lock:
            if value and key not in self._wanted:
                return
        self._set_flag(key, value)

    def _set_flag(self, key, value):
        if self.recorder is None:
            return
        try:
            self.recorder.runtime[f"twitch_push:{key}"] = bool(value)
        except Exception:
            pass

    def _clear_flags(self):
        with self._lock:
            keys = list(self._wanted)
        for k in keys:
            self._set_flag(k, False)
