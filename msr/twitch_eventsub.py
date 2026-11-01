"""Twitch EventSub over WebSocket: OAuth device flow, token store, Helix calls,
message parsing. No threads, no tkinter (the listener is msr/twitch_push.py).

Documented flow (dev.twitch.tv/docs/eventsub, /docs/authentication):

* The user authorizes MSR's registered *public* client once with the Device
  Code Grant (no client secret, no redirect). ``stream.online`` needs no scope.
* Connect ``wss://eventsub.wss.twitch.tv/ws``; the first frame is
  ``session_welcome`` with ``payload.session.id``. Subscriptions must be
  created within ~10 s with ``POST /helix/eventsub/subscriptions`` using the
  user token and the same Client-Id, transport ``{"method": "websocket",
  "session_id": ...}``.
* Limits per (client id, user): 3 sockets, 300 subs per socket and
  ``max_total_cost`` 10. ``stream.online`` for a channel that has not
  authorized MSR costs 1, so only 10 channels get push; the rest poll.
* ``session_keepalive`` arrives when idle; silence longer than
  ``keepalive_timeout_seconds`` means the socket is dead.
* ``session_reconnect`` carries ``reconnect_url``; subscriptions move with it.
* The server closes the socket if the client sends anything but pongs.
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

EVENTSUB_WS_URL = "wss://eventsub.wss.twitch.tv/ws"
HELIX = "https://api.twitch.tv/helix"
OAUTH = "https://id.twitch.tv/oauth2"
DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
TOKEN_FILE = "twitch_token.json"
DEFAULT_CLIENT_ID = "cpdfs62hlro5hyzyzlfogd8gshbnu4"  # public, not a secret
MAX_TOTAL_COST = 10
KEEPALIVE_SECONDS = 30
REFRESH_MARGIN = 600


class TwitchAuthError(Exception):
    """Token missing, revoked or refused: push is off until the user re-authorizes."""


class TwitchHTTPError(Exception):
    def __init__(self, status, message=""):
        super().__init__(f"HTTP {status} {message}".strip())
        self.status = status
        self.message = message


def http_request(method, url, headers=None, form=None, body=None, timeout=15):
    """(status, json) - raises TwitchHTTPError for 4xx/5xx with Twitch's message."""
    data = None
    headers = dict(headers or {})
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read().decode("utf-8", "replace")).get("message", "")
        except Exception:
            msg = ""
        raise TwitchHTTPError(e.code, msg) from None


# -- token store (git-ignored file in the data folder; never logged) --

def token_path(data_dir):
    return os.path.join(data_dir or ".", TOKEN_FILE)


def load_token(data_dir):
    try:
        with open(token_path(data_dir), "r", encoding="utf-8") as f:
            tok = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(tok, dict) or not tok.get("access_token"):
        return None
    return tok


def save_token(data_dir, tok):
    from msr import history
    history.atomic_write(token_path(data_dir), json.dumps(tok, indent=2).encode("utf-8"))


def clear_token(data_dir):
    try:
        os.remove(token_path(data_dir))
    except OSError:
        pass


def _token_record(resp, client_id, now):
    return {"client_id": client_id,
            "access_token": resp["access_token"],
            "refresh_token": resp.get("refresh_token", ""),
            "expires_at": now + int(resp.get("expires_in") or 0),
            "scope": resp.get("scope") or []}


# -- device code flow (public client: no secret) --

def device_start(client_id, request=http_request):
    """-> {device_code, user_code, verification_uri, expires_in, interval}"""
    _, resp = request("POST", f"{OAUTH}/device", form={"client_id": client_id, "scopes": ""})
    return resp


def device_poll(client_id, device_code, request=http_request, now=time.time):
    """One poll. Returns a token record, or None while the user has not approved yet.
    Raises TwitchAuthError when the code expired or was denied."""
    try:
        _, resp = request("POST", f"{OAUTH}/token", form={
            "client_id": client_id, "scopes": "", "device_code": device_code,
            "grant_type": DEVICE_GRANT})
    except TwitchHTTPError as e:
        if e.status == 400 and "authorization_pending" in (e.message or ""):
            return None
        if e.status == 400 and "slow_down" in (e.message or ""):
            return None
        raise TwitchAuthError(f"authorization failed ({e})") from None
    return _token_record(resp, client_id, now())


def refresh(tok, request=http_request, now=time.time):
    """Public-client refresh (one-time-use refresh token, no secret)."""
    if not tok.get("refresh_token"):
        raise TwitchAuthError("no refresh token - authorize again")
    try:
        _, resp = request("POST", f"{OAUTH}/token", form={
            "client_id": tok["client_id"], "grant_type": "refresh_token",
            "refresh_token": tok["refresh_token"]})
    except TwitchHTTPError as e:
        if e.status in (400, 401):
            raise TwitchAuthError("refresh refused - authorize again") from None
        raise
    out = _token_record(resp, tok["client_id"], now())
    out["user_id"] = tok.get("user_id")
    out["login"] = tok.get("login")
    return out


def validate(tok, request=http_request):
    """Twitch requires apps to validate tokens hourly. -> {client_id, login, user_id, expires_in}"""
    try:
        _, resp = request("GET", f"{OAUTH}/validate",
                          headers={"Authorization": f"OAuth {tok['access_token']}"})
    except TwitchHTTPError as e:
        if e.status == 401:
            raise TwitchAuthError("token invalid") from None
        raise
    return resp


def needs_refresh(tok, now=None):
    now = time.time() if now is None else now
    return float(tok.get("expires_at") or 0) - now < REFRESH_MARGIN


# -- Helix --

def _helix_headers(tok):
    return {"Authorization": f"Bearer {tok['access_token']}", "Client-Id": tok["client_id"]}


def lookup_user_ids(tok, logins, request=http_request):
    """login -> numeric id string for up to 100 logins per call (missing logins skipped)."""
    out = {}
    logins = [l.lower() for l in logins if l]
    for i in range(0, len(logins), 100):
        q = urllib.parse.urlencode([("login", l) for l in logins[i:i + 100]])
        try:
            _, resp = request("GET", f"{HELIX}/users?{q}", headers=_helix_headers(tok))
        except TwitchHTTPError as e:
            if e.status == 401:
                raise TwitchAuthError("token refused by Helix") from None
            raise
        for u in resp.get("data") or []:
            if u.get("login") and u.get("id"):
                out[u["login"].lower()] = str(u["id"])
    return out


def create_subscription(tok, session_id, broadcaster_id, sub_type="stream.online",
                        request=http_request):
    """-> ("ok", sub_id, total_cost, max_total_cost) | ("exists"|"limit"|"error", None, None, None).
    Raises TwitchAuthError on 401/403."""
    try:
        _, resp = request("POST", f"{HELIX}/eventsub/subscriptions", headers=_helix_headers(tok),
                          body={"type": sub_type, "version": "1",
                                "condition": {"broadcaster_user_id": str(broadcaster_id)},
                                "transport": {"method": "websocket", "session_id": session_id}})
    except TwitchHTTPError as e:
        if e.status in (401, 403):
            raise TwitchAuthError(f"subscription refused ({e.status})") from None
        if e.status == 409:
            return ("exists", None, None, None)
        if e.status == 429:
            return ("limit", None, None, None)
        return ("error", None, None, None)
    sub = (resp.get("data") or [{}])[0]
    return ("ok", sub.get("id"), resp.get("total_cost"), resp.get("max_total_cost"))


def delete_subscription(tok, sub_id, request=http_request):
    try:
        request("DELETE", f"{HELIX}/eventsub/subscriptions?id={urllib.parse.quote(sub_id)}",
                headers=_helix_headers(tok))
        return True
    except TwitchHTTPError:
        return False


# -- websocket frames --

def ws_url(keepalive=KEEPALIVE_SECONDS):
    return f"{EVENTSUB_WS_URL}?keepalive_timeout_seconds={int(keepalive)}"


def parse_message(raw):
    """One EventSub frame -> dict with ``type`` in
    welcome / keepalive / notification / reconnect / revocation / unknown."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    try:
        msg = json.loads(raw)
    except (TypeError, ValueError):
        return {"type": "unknown"}
    if not isinstance(msg, dict):
        return {"type": "unknown"}
    meta = msg.get("metadata") or {}
    payload = msg.get("payload") or {}
    mtype = meta.get("message_type")
    if mtype in ("session_welcome", "session_reconnect"):
        s = payload.get("session") or {}
        return {"type": "welcome" if mtype == "session_welcome" else "reconnect",
                "session_id": s.get("id"),
                "keepalive": s.get("keepalive_timeout_seconds"),
                "reconnect_url": s.get("reconnect_url"),
                "message_id": meta.get("message_id")}
    if mtype == "session_keepalive":
        return {"type": "keepalive"}
    if mtype in ("notification", "revocation"):
        sub = payload.get("subscription") or {}
        ev = payload.get("event") or {}
        return {"type": mtype, "message_id": meta.get("message_id"),
                "sub_type": sub.get("type") or meta.get("subscription_type"),
                "sub_id": sub.get("id"), "status": sub.get("status"),
                "broadcaster_id": str((sub.get("condition") or {}).get("broadcaster_user_id")
                                      or ev.get("broadcaster_user_id") or ""),
                "login": (ev.get("broadcaster_user_login") or "").lower(),
                "started_at": ev.get("started_at"), "stream_type": ev.get("type")}
    return {"type": "unknown"}


def authorize_console(client_id, data_dir, request=http_request, sleep=time.sleep,
                      now=time.time, out=print):
    """Device flow for a console: show the code, wait for approval, save the token."""
    d = device_start(client_id, request=request)
    out(f"Open {d.get('verification_uri')} and enter code {d.get('user_code')}")
    deadline = now() + int(d.get("expires_in") or 1800)
    interval = max(1, int(d.get("interval") or 5))
    while now() < deadline:
        sleep(interval)
        tok = device_poll(client_id, d["device_code"], request=request, now=now)
        if tok:
            info = validate(tok, request=request)
            tok["user_id"], tok["login"] = info.get("user_id"), info.get("login")
            save_token(data_dir, tok)
            out(f"Authorized as {tok['login']}. Twitch push can now be enabled.")
            return tok
    raise TwitchAuthError("code expired - run authorize again")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Authorize MSR for Twitch EventSub push (device code flow)")
    ap.add_argument("command", choices=["authorize", "forget"])
    ap.add_argument("--client-id", default="")
    ap.add_argument("--data-dir", default=".")
    a = ap.parse_args()
    if a.command == "forget":
        clear_token(a.data_dir)
        print("Twitch token removed.")
    else:
        cid = a.client_id
        if not cid:
            from msr.config import new_parser
            p = new_parser()
            p.read(os.path.join(a.data_dir, "config.ini"), encoding="utf-8")
            cid = (p.get("Twitch", "client_id", fallback="") or "").strip()
        cid = cid or DEFAULT_CLIENT_ID
        if not cid:
            raise SystemExit("No client id: set [Twitch] client_id or pass --client-id")
        authorize_console(cid, a.data_dir)
