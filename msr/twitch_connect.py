"""Tk-free "Connect Twitch" job: device-code flow on a background thread.

Events go to a queue the Options tab drains with after():
("code", uri, user_code, expires_in) / ("connected", login) /
("expired",) / ("error", text) / ("cancelled",).
Only one job runs per process (Options or control): start() raises SignInBusy.
"""

import queue
import threading
import time

from msr import twitch_eventsub as te

_lock = threading.Lock()
_active = None


class SignInBusy(Exception):
    """Another sign-in is running; .job is it (its .info has the code once known)."""

    def __init__(self, job):
        super().__init__("A Twitch sign-in is already in progress.")
        self.job = job


def active():
    """The running sign-in job (Options or control), or None."""
    with _lock:
        return _active if _active is not None and not _active.done else None


def client_id_for(config, api=te):
    """The configured app id, or MSR's public one (shared by Options and control)."""
    try:
        cid = (config.get("Twitch", "client_id", fallback="") or "").strip()
    except Exception:
        cid = ""
    return cid or api.DEFAULT_CLIENT_ID


def status_of(data_dir, client_id, api=te):
    """('none'|'connected'|'expired', login) from the saved token, no network."""
    tok = api.load_token(data_dir)
    if not tok or tok.get("client_id") != client_id:
        return "none", None
    if not tok.get("refresh_token") and api.needs_refresh(tok):
        return "expired", tok.get("login")
    return "connected", tok.get("login")


def check_online(data_dir, client_id, api=te):
    """Validate (refreshing once if needed). Returns the same tuple as status_of."""
    state, login = status_of(data_dir, client_id, api)
    if state != "connected":
        return state, login
    store = te.TokenStore(data_dir, api)
    try:
        tok = store.get_fresh()
        try:
            api.validate(tok)
        except te.TwitchAuthError:
            tok = store.get_fresh(force=True, used=tok.get("refresh_token"))
            api.validate(tok)
        return "connected", tok.get("login") or login
    except te.TwitchAuthError:
        return "expired", login
    except Exception:
        return "connected", login  # offline: trust the file


def disconnect(data_dir, api=te):
    """Revoke at Twitch (best effort), then delete the token file. False if revoking failed."""
    tok = api.load_token(data_dir)
    ok = True
    if tok:
        try:
            api.revoke(tok)
        except Exception:
            ok = False
    api.clear_token(data_dir)
    return ok


class ConnectJob:
    def __init__(self, client_id, data_dir, api=te, sleep=None, clock=time.time, owner=""):
        self.client_id = client_id
        self.owner = owner
        self.info = {}      # user_code, verification_url, expires_at once Twitch answers
        self.done = False
        self.data_dir = data_dir
        self.api = api
        self.clock = clock
        self.events = queue.Queue()
        self._cancel = threading.Event()
        self._sleep = sleep or self._cancel.wait
        self.thread = None

    def start(self):
        global _active
        with _lock:
            if _active is not None and not _active.done:
                raise SignInBusy(_active)
            _active = self
        self.thread = threading.Thread(target=self._run, daemon=True, name="twitch-connect")
        self.thread.start()
        return self

    def cancel(self):
        self._cancel.set()

    def running(self):
        return bool(self.thread and self.thread.is_alive() and not self.done)

    def _run(self):
        global _active
        try:
            self._flow()
        finally:
            with _lock:
                self.done = True
                if _active is self:
                    _active = None

    def _flow(self):
        try:
            d = self.api.device_start(self.client_id)
            expires = int(d.get("expires_in") or 1800)
            code = d.get("user_code") or ""
            self.info = {"user_code": code, "expires_at": round(time.time() + expires),
                         "verification_url": te.safe_verification_uri(d.get("verification_uri"), code)}
            self.events.put(("code", te.safe_verification_uri(d.get("verification_uri"), code),
                             code, expires))
            deadline = self.clock() + expires
            interval = max(1, int(d.get("interval") or 5))
            while self.clock() < deadline:
                self._sleep(interval)
                if self._cancel.is_set():
                    self.events.put(("cancelled",))
                    return
                tok = self.api.device_poll(self.client_id, d["device_code"])
                if tok:
                    info = self.api.validate(tok)
                    tok["user_id"], tok["login"] = info.get("user_id"), info.get("login")
                    self.api.save_token(self.data_dir, tok)
                    self.events.put(("connected", tok["login"]))
                    return
            self.events.put(("expired",))
        except te.TwitchAuthError as e:
            self.events.put(("error", str(e)))
        except Exception as e:
            self.events.put(("error", f"could not reach Twitch ({type(e).__name__})"))
