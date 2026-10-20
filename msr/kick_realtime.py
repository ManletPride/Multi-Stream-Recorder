"""Kick realtime negotiation + Centrifugo JSON protocol helpers (no I/O state).

Kick retired the hard-coded Pusher app key (error 4001 "App key ... not in
this cluster"). Its web client now does, per connection:

1. ``POST https://web.kick.com/api/v1/realtime/connection`` with
   ``{"client": {"id": <uuid>, "type": "web"}, "capabilities":
   {"accepted_providers": [{"provider": "pusher"}, {"provider": "centrifugo"}]}}``
   -> ``{"data": {"mode": "websocket", "connections": [{"provider":
   "centrifugo", "credentials": {"url": "wss://realtime.<region>.platform.kick.com/connection/websocket"}}]}}``
   (a ``pusher`` entry would carry ``credentials.app_key`` / ``cluster``).
2. Centrifugo: ``POST /api/v1/realtime/auth/connection`` ``{"client_id": <uuid>}``
   -> ``{"data": {"token": <guest JWT, ~45 min>}}``.
3. Websocket JSON protocol (Centrifugo v5/v6, one JSON object per line):
   ``{"id":1,"connect":{"token":..,"name":"js"}}`` -> ``{"id":1,"connect":{.."ttl":2700,"ping":25,"pong":true}}``;
   ``{"id":N,"subscribe":{"channel":"channel.<id>"}}`` -> ``{"id":N,"subscribe":{}}``
   (public channels need no token; ``private-*`` would); server ping is ``{}``
   and must be answered with ``{}``; token refresh is
   ``{"id":N,"refresh":{"token":..}}``.
4. Publications: ``{"push":{"channel":"channel.<id>","pub":{"data":{"event":
   "App\\Events\\StreamerIsLive","data":"<json string>"}}}}``. Event names
   and channel names are the old Pusher ones; the web player still listens to
   ``channel.<id>`` for ``StreamerIsLive`` / ``StopStreamBroadcast``.

A connection allows at most 128 subscriptions (error 106 "limit exceeded").
"""

import json
import uuid

KICK_REALTIME_API = "https://web.kick.com/api"
CONNECTION_PATH = "/v1/realtime/connection"
AUTH_CONNECTION_PATH = "/v1/realtime/auth/connection"
CENTRIFUGO_MAX_SUBS = 120   # server limit is 128; keep headroom
BROWSER_HEADERS = {
    "content-type": "application/json",
    "accept": "application/json",
    "origin": "https://kick.com",
    "referer": "https://kick.com/",
    "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
}


def new_client_id():
    return str(uuid.uuid4())


def connection_request_body(client_id):
    return {"client": {"id": client_id, "type": "web"},
            "capabilities": {"accepted_providers": [{"provider": "pusher"},
                                                    {"provider": "centrifugo"}]}}


def parse_connection_response(resp):
    """Mirror of the web client's selector: first usable websocket connection.

    Returns ``{"provider": "centrifugo", "url": ...}`` or
    ``{"provider": "pusher", "app_key": ..., "cluster": ...}`` or None.
    """
    data = (resp or {}).get("data") if isinstance(resp, dict) else None
    if not isinstance(data, dict) or data.get("mode") != "websocket":
        return None
    for conn in data.get("connections") or []:
        if not isinstance(conn, dict):
            continue
        creds = conn.get("credentials") or {}
        prov = conn.get("provider")
        if prov == "centrifugo" and creds.get("url"):
            url = str(creds["url"])
            if url.startswith("wss://"):
                return {"provider": "centrifugo", "url": url}
        if prov == "pusher" and creds.get("app_key") and creds.get("cluster"):
            return {"provider": "pusher", "app_key": str(creds["app_key"]),
                    "cluster": str(creds["cluster"])}
    return None


def pusher_url(app_key, cluster):
    return (f"wss://ws-{cluster}.pusher.com/app/{app_key}"
            "?protocol=7&client=js&version=8.4.0&flash=false")


def parse_token_response(resp):
    data = (resp or {}).get("data") if isinstance(resp, dict) else None
    tok = data.get("token") if isinstance(data, dict) else None
    return tok if isinstance(tok, str) and tok else None


def http_post_json(path, body, timeout=15):
    """POST like the browser does. urllib first; curl_cffi if Cloudflare 403s."""
    import urllib.request
    url = KICK_REALTIME_API + path
    raw = json.dumps(body).encode()
    try:
        req = urllib.request.Request(url, data=raw, headers=BROWSER_HEADERS, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "replace"))
    except Exception as first:
        from msr.deps import HAS_CURL_CFFI
        if not HAS_CURL_CFFI:
            raise
        from curl_cffi import requests as curl_requests
        r = curl_requests.post(url, data=raw, headers=BROWSER_HEADERS,
                               impersonate="chrome", timeout=timeout)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}") from first
        return r.json()


def negotiate(client_id, post=http_post_json):
    """Return the connection dict from parse_connection_response (raises on error)."""
    conn = parse_connection_response(post(CONNECTION_PATH, connection_request_body(client_id)))
    if conn is None:
        raise RuntimeError("realtime negotiation returned no usable websocket connection")
    if conn["provider"] == "centrifugo":
        tok = parse_token_response(post(AUTH_CONNECTION_PATH, {"client_id": client_id}))
        if not tok:
            raise RuntimeError("realtime auth/connection returned no token")
        conn["token"] = tok
        conn["client_id"] = client_id
    return conn


# ── Centrifugo JSON protocol ──

def cf_connect(cmd_id, token):
    return json.dumps({"id": cmd_id, "connect": {"token": token, "name": "js"}})


def cf_subscribe(cmd_id, channel):
    return json.dumps({"id": cmd_id, "subscribe": {"channel": channel}})


def cf_unsubscribe(cmd_id, channel):
    return json.dumps({"id": cmd_id, "unsubscribe": {"channel": channel}})


def cf_refresh(cmd_id, token):
    return json.dumps({"id": cmd_id, "refresh": {"token": token}})


def parse_centrifugo_message(raw):
    """Split one websocket message into frames.

    Yields dicts:
      ``{"type": "ping"}`` (answer with ``{}``),
      ``{"type": "reply", "id": n, "kind": "connect"|"subscribe"|..., "result": {...}}``,
      ``{"type": "error", "id": n, "code": c, "message": m}``,
      ``{"type": "pub", "channel": ch, "msg": <pusher-style dict>}`` where the
      pusher-style dict is ``{"event", "channel", "data"}`` so
      ``recorder.parse_kick_push_event`` handles both transports,
      ``{"type": "disconnect", "code": c, "reason": r}``.
    """
    out = []
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    for line in str(raw or "").split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            fr = json.loads(line)
        except ValueError:
            continue
        if not isinstance(fr, dict):
            continue
        if not fr:
            out.append({"type": "ping"})
            continue
        if "error" in fr:
            err = fr.get("error") or {}
            out.append({"type": "error", "id": fr.get("id"), "code": err.get("code"),
                        "message": err.get("message", "")})
            continue
        if "id" in fr:
            kind = next((k for k in fr if k != "id"), None)
            out.append({"type": "reply", "id": fr["id"], "kind": kind,
                        "result": fr.get(kind) if kind else None})
            continue
        push = fr.get("push")
        if isinstance(push, dict):
            ch = push.get("channel", "")
            if isinstance(push.get("pub"), dict):
                env = push["pub"].get("data")
                if isinstance(env, str):
                    try:
                        env = json.loads(env)
                    except ValueError:
                        env = None
                if isinstance(env, dict) and env.get("event"):
                    out.append({"type": "pub", "channel": ch,
                                "msg": {"event": env.get("event"), "channel": ch,
                                        "data": env.get("data", "{}")}})
            elif isinstance(push.get("disconnect"), dict):
                d = push["disconnect"]
                out.append({"type": "disconnect", "code": d.get("code"),
                            "reason": d.get("reason", "")})
    return out
