"""Single-use Twitch refresh tokens: one locked store, retry rules, health check.
Fake API (no network), temp data folders; token values never logged."""
import os
import tempfile
import threading
import time
import unittest
import urllib.error

from msr import health, twitch_connect as tc
from msr import twitch_eventsub as te
from msr.twitch_push import TwitchPushListener, VALIDATE_EVERY


class OneTimeApi:
    """Twitch-like: each refresh token works once; the 'file' is shared state."""
    TwitchAuthError = te.TwitchAuthError
    DEFAULT_CLIENT_ID = "cid"

    def __init__(self, expired=True, slow=0.0):
        self.n = 0
        self.file = self._tok(0, expired)
        self.spent = set()
        self.sent = []
        self.slow = slow
        self.validate_401 = 0
        self.helix_401 = 0
        self.net_error = False
        self.lock = threading.Lock()

    def _tok(self, n, expired=False):
        return {"client_id": "cid", "access_token": f"at{n}", "refresh_token": f"rt{n}",
                "expires_at": 0 if expired else time.time() + 14400, "login": "me"}

    def load_token(self, d):
        return dict(self.file) if self.file else None

    def save_token(self, d, t):
        self.file = dict(t)

    def needs_refresh(self, tok, now=None):
        return te.needs_refresh(tok, now)

    def refresh(self, tok):
        if self.net_error:
            raise urllib.error.URLError("offline")
        rt = tok["refresh_token"]
        self.sent.append(rt)
        time.sleep(self.slow)
        with self.lock:
            if rt in self.spent:
                raise te.TwitchAuthError("refresh refused - authorize again")
            self.spent.add(rt)
            self.n += 1
            return self._tok(self.n)

    def validate(self, tok):
        if self.validate_401:
            self.validate_401 -= 1
            raise te.TwitchAuthError("token invalid")
        return {}

    def lookup_user_ids(self, tok, logins):
        if self.helix_401:
            self.helix_401 -= 1
            raise te.TwitchAuthError("token refused by Helix")
        return {l: str(i) for i, l in enumerate(sorted(logins))}

    def create_subscription(self, tok, session, bid):
        return ("ok", "sub-" + bid, 1, 10)


class Cfg:
    def getboolean(self, s, k, fallback=None):
        return True

    def get(self, s, k, fallback=None):
        return "cid" if k == "client_id" else fallback


class Rec:
    runtime = {}

    def check_now(self, key):
        pass


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.d = self.tmp.name

    def listener(self, api):
        lst = TwitchPushListener(Rec(), Cfg(), data_dir=self.d, api=api)
        lst._tok = api.load_token(self.d)
        return lst


class TestStore(Base):
    def test_concurrent_ensure_token_refreshes_once(self):
        api = OneTimeApi(slow=0.05)
        lst = self.listener(api)
        errs = []

        def go():
            try:
                lst._ensure_token()
            except Exception as e:
                errs.append(e)
        ts = [threading.Thread(target=go) for _ in range(5)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(10)
        self.assertEqual(errs, [])
        self.assertEqual(api.sent, ["rt0"])
        self.assertFalse(lst._auth_failed)

    def test_listener_reloads_after_check_online_rotated(self):
        api = OneTimeApi()
        lst = self.listener(api)  # holds rt0 in memory
        self.assertEqual(tc.check_online(self.d, "cid", api), ("connected", "me"))
        api.file["expires_at"] = 0  # due again
        tok = lst._ensure_token()
        self.assertEqual(api.sent, ["rt0", "rt1"])
        self.assertEqual(tok["refresh_token"], "rt2")
        self.assertFalse(lst._auth_failed)

    def test_refused_then_newer_token_in_file_retries_once(self):
        api = OneTimeApi()
        real = api.refresh

        def outside_writer(tok):  # an old MSR rotated the file without the lock
            if tok["refresh_token"] == "rt0" and "rt0" not in api.spent:
                api.spent.add("rt0")
                api.file = dict(api._tok(9), expires_at=0)
                api.sent.append("rt0")
                raise te.TwitchAuthError("refresh refused - authorize again")
            return real(tok)
        api.refresh = outside_writer
        tok = te.TokenStore(self.d, api).get_fresh()
        self.assertEqual(api.sent, ["rt0", "rt9"])
        self.assertEqual(api.file["refresh_token"], tok["refresh_token"])

    def test_refused_with_same_token_turns_push_off_once(self):
        api = OneTimeApi()
        api.spent.add("rt0")
        lst = self.listener(api)
        with self.assertLogs(level="WARNING") as logs:
            lst._stop.wait = lambda s: True
            lst._run()
        self.assertTrue(lst._auth_failed)
        self.assertEqual(api.sent, ["rt0"])
        self.assertEqual(sum("refresh refused" in l for l in logs.output), 1)

    def test_validate_401_refreshes_once(self):
        api = OneTimeApi(expired=False)
        api.validate_401 = 1
        lst = self.listener(api)
        # Force the hourly validate path (clock is time.monotonic; CI runners
        # often have uptime < VALIDATE_EVERY so _last_validate=0 would skip it).
        lst._last_validate = -VALIDATE_EVERY
        tok = lst._ensure_token()
        self.assertEqual((api.sent, tok["refresh_token"]), (["rt0"], "rt1"))

    def test_helix_401_refreshes_once_and_retries(self):
        api = OneTimeApi(expired=False)
        api.helix_401 = 1
        lst = self.listener(api)
        lst._last_validate = lst.clock()
        lst.set_channels(["twitch:bob"])
        lst._session_id = "s"
        lst._subscribe_all()
        self.assertEqual(api.sent, ["rt0"])
        self.assertEqual(sorted(lst.covered()), ["bob"])
        self.assertFalse(lst._auth_failed)

    def test_set_channels_during_welcome_no_second_refresh(self):
        api = OneTimeApi(slow=0.05)
        lst = self.listener(api)
        lst._session_id = "s"
        lst._wanted = {"twitch:bob"}
        a = threading.Thread(target=lst._subscribe_all)
        a.start()
        lst.set_channels(["twitch:bob", "twitch:amy"])
        a.join(10)
        time.sleep(0.3)
        self.assertEqual(api.sent, ["rt0"])
        self.assertFalse(lst._auth_failed)

    def test_check_online_never_sends_same_refresh_token_twice(self):
        api = OneTimeApi()
        api.validate_401 = 1
        self.assertEqual(tc.check_online(self.d, "cid", api)[0], "connected")
        self.assertEqual(len(api.sent), len(set(api.sent)))

    def test_network_errors_never_turn_push_off(self):
        api = OneTimeApi()
        api.net_error = True
        lst = self.listener(api)
        lst._stop.wait = lambda s: True
        lst._run()
        self.assertFalse(lst._auth_failed)

    def test_success_logged_without_token_values(self):
        api = OneTimeApi()
        with self.assertLogs(level="INFO") as logs:
            te.TokenStore(self.d, api).get_fresh()
        text = "\n".join(logs.output)
        self.assertIn("Twitch: sign-in refreshed", text)
        for secret in ("rt0", "rt1", "at1"):
            self.assertNotIn(secret, text)

    def test_cross_process_lock_is_exclusive(self):
        with te.token_lock(self.d):
            got = []
            t = threading.Thread(target=lambda: got.append(self._try_other_handle()))
            t.start()
            t.join(5)
        self.assertEqual(got, [False])

    def _try_other_handle(self):
        fh = open(te._lock_path(self.d), "a+b")
        try:
            if os.name == "nt":
                import msvcrt
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False
        finally:
            fh.close()


class TestHealth(Base):
    def test_expired_access_but_good_refresh_is_fine(self):
        api = OneTimeApi()
        store = te.TokenStore(self.d, api)
        api.validate_401 = 0
        self.assertIsNone(health.twitch_issue(api.file, True, api.validate, te.TwitchAuthError,
                                              fresh=store.get_fresh))
        self.assertEqual(api.sent, ["rt0"])

    def test_refused_refresh_warns(self):
        api = OneTimeApi()
        api.spent.add("rt0")
        issue = health.twitch_issue(api.file, True, api.validate, te.TwitchAuthError,
                                    fresh=te.TokenStore(self.d, api).get_fresh)
        self.assertIn("no longer valid", issue.text if hasattr(issue, "text") else str(issue))

    def test_offline_is_not_reported(self):
        api = OneTimeApi()
        api.net_error = True
        self.assertIsNone(health.twitch_issue(api.file, True, api.validate, te.TwitchAuthError,
                                              fresh=te.TokenStore(self.d, api).get_fresh))


if __name__ == "__main__":
    unittest.main()
