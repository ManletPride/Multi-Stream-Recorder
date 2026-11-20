"""Telegram upload queue: split plan, delete-after-success, secrets. Telegram itself is mocked."""

import json
import os
import stat
import tempfile
import unittest
import urllib.error
from unittest import mock

from msr import control, dpapi, history
from msr.config import Config
from msr.telegram_api import owner_is_member
from msr.telegram_queue import (
    UPLOAD_LIMIT_BYTES,
    AuthRejected,
    DestinationRejected,
    Flood,
    QueueStore,
    TelegramError,
    build_caption,
    config_for_worker,
    deletion_allowed,
    guard_recording,
    landed_in_topic,
    load_session,
    note_ready,
    part_filename,
    plan_spans,
    save_session,
    scrub,
)
from msr.telegram_upload import Uploader
from msr.util import redact_for_log

TOKEN = "123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw"
CHAT = "-1001234567890"
OTHER = "-1009999999999"


class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


class FakeTransport:
    def __init__(self, fail=None):
        self.sent = []
        self.texts = []
        self.fail = list(fail or [])
        self.session = "session-secret"
        self.closed = False

    def connect(self, settings, session):
        return self.session

    def _maybe_fail(self):
        if self.fail:
            raise self.fail.pop(0)

    def send_file(self, settings, session, path, caption, progress):
        self._maybe_fail()
        progress(5, 10)
        progress(10, 10)
        self.sent.append({"path": os.path.basename(path), "caption": caption,
                          "chat": settings.chat_id, "topic": settings.topic_id})
        return {"message_id": len(self.sent), "session": self.session}

    def send_text(self, settings, session, text):
        self._maybe_fail()
        self.texts.append({"text": text, "chat": settings.chat_id, "topic": settings.topic_id})
        return {"message_id": 1, "session": self.session}

    def close(self):
        self.closed = True


class Cfg:
    def __init__(self, **kw):
        self.v = {
            ("Telegram", "enabled"): "true",
            ("Telegram", "api_id"): "12345",
            ("Telegram", "api_hash"): "a" * 32,
            ("Telegram", "bot_token"): TOKEN,
            ("Telegram", "chat_id"): CHAT,
            ("Telegram", "topic_id"): "55",
            ("Telegram", "owner_id"): "4242",
            ("Telegram", "delete_after_upload"): "false",
            ("Advanced", "ffmpeg_path"): "ffmpeg",
        }
        self.v.update(kw)

    def get(self, section, key, fallback=None):
        return self.v.get((section, key), fallback)

    def getboolean(self, section, key, fallback=False):
        raw = self.v.get((section, key))
        if raw is None:
            return fallback
        return str(raw).strip().lower() in ("1", "true", "yes", "on")


def drain(up, n=30):
    states = []
    for _ in range(n):
        state = up.step()
        states.append(state)
        if state in ("idle", "off", "blocked", "missing", "incomplete"):
            break
    return states


class PlanTests(unittest.TestCase):
    def test_a_file_under_the_limit_is_one_part(self):
        self.assertEqual(plan_spans(1000, 30.0, limit=2000), [{"whole": True}])
        self.assertLess(UPLOAD_LIMIT_BYTES, 4000 * 512 * 1024)
        self.assertGreater(UPLOAD_LIMIT_BYTES, 1900 * 1024 * 1024)

    def test_spans_stay_under_the_limit_and_cover_the_file(self):
        size, duration, limit = 5_000_000, 100.0, 1_000_000
        spans = plan_spans(size, duration, limit=limit)
        self.assertGreater(len(spans), 1)
        self.assertFalse(spans[0].get("whole"))
        covered = 0.0
        for span in spans:
            covered += span["duration"]
            estimated = size * span["duration"] / duration
            self.assertLessEqual(estimated, limit)
        self.assertAlmostEqual(covered, duration, delta=0.01)

    def test_unknown_length_is_not_split_by_guessing(self):
        self.assertEqual(plan_spans(5_000_000, None, limit=1_000_000), [])

    def test_part_names_and_caption(self):
        self.assertEqual(part_filename("alice.mp4", 1, 1), "alice.mp4")
        self.assertEqual(part_filename("alice.mp4", 2, 3), "alice.part02of03.mp4")
        text = build_caption("alice", "twitch", "Night stream", "2026-11-20T21:00:00", 2, 3)
        self.assertIn("alice (twitch)", text)
        self.assertIn("Night stream", text)
        self.assertIn("part 2/3", text)
        self.assertNotIn("part ", build_caption("alice", "kick", "", "", 1, 1))


class RuleTests(unittest.TestCase):
    def test_delete_only_when_every_part_is_confirmed_and_the_file_is_unchanged(self):
        job = {"path": "/x", "size": 10, "mtime_ns": 5, "state": "uploading",
               "parts": [{"confirmed": True}, {"confirmed": False}]}
        self.assertFalse(deletion_allowed(job, 10, 5, True)[0])
        job["parts"][1]["confirmed"] = True
        self.assertFalse(deletion_allowed(job, 10, 5, False)[0])
        self.assertTrue(deletion_allowed(job, 10, 5, True)[0])
        self.assertFalse(deletion_allowed(job, 11, 5, True)[0])
        self.assertFalse(deletion_allowed(job, 10, 6, True)[0])
        job["state"] = "error"
        self.assertFalse(deletion_allowed(job, 10, 5, True)[0])

    def test_message_must_land_in_the_configured_topic(self):
        self.assertTrue(landed_in_topic(int(CHAT), 55, 55, int(CHAT), 55))
        self.assertTrue(landed_in_topic(int(CHAT), 55, None, int(CHAT), 55))
        self.assertFalse(landed_in_topic(int(OTHER), 55, 55, int(CHAT), 55))
        self.assertFalse(landed_in_topic(int(CHAT), 1, None, int(CHAT), 55))
        self.assertFalse(landed_in_topic(int(CHAT), None, None, int(CHAT), 55))

    def test_only_a_processed_recording_is_eligible(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "Processed", "kick", "alice", "a.mp4")
            os.makedirs(os.path.dirname(path))
            with open(path, "wb") as fh:
                fh.write(b"x" * 2048)
            self.assertTrue(guard_recording(path, [tmp])[0])
            clip = os.path.join(tmp, "Clips", "kick", "alice", "c.mp4")
            os.makedirs(os.path.dirname(clip))
            with open(clip, "wb") as fh:
                fh.write(b"x" * 2048)
            self.assertFalse(guard_recording(clip, [tmp])[0])
            outside = os.path.join(tmp, "notes.txt")
            with open(outside, "wb") as fh:
                fh.write(b"x" * 2048)
            self.assertFalse(guard_recording(outside, [tmp])[0])


class SecretTests(unittest.TestCase):
    def test_log_redaction_removes_a_bot_token_and_a_bot_url(self):
        url = f"https://api.telegram.org/bot{TOKEN}/getChatMember?chat_id=1"
        text = redact_for_log(f"boom {TOKEN} {url}")
        self.assertNotIn(TOKEN, text)
        self.assertNotIn("AAHdq", text)
        self.assertNotIn("getChatMember", text)

    def test_scrub_removes_a_secret_that_is_not_a_token_shape(self):
        self.assertNotIn("h" * 32, scrub("hash hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh", "h" * 32))

    def test_history_keeps_ids_and_drops_hash_and_token(self):
        raw = (f"[Telegram]\napi_hash = {'b' * 32}\nbot_token = {TOKEN}\n"
               f"chat_id = {CHAT}\nowner_id = 5\n").encode()
        text = history.strip_secrets(raw).decode()
        self.assertNotIn(TOKEN, text)
        self.assertNotIn("b" * 32, text)
        self.assertIn(CHAT, text)
        self.assertIn("owner_id = 5", text)

    def test_worker_config_does_not_carry_the_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(os.path.join(tmp, "config.ini"))
            cfg.config.set("Telegram", "bot_token", TOKEN)
            cfg.config.set("Telegram", "api_hash", "c" * 32)
            cfg.config.set("Telegram", "api_hash_dpapi", "cipher")
            handed = config_for_worker(cfg)
            blob = json.dumps(handed)
            self.assertNotIn(TOKEN, blob)
            self.assertNotIn("c" * 32, blob)
            self.assertNotIn("cipher", blob)
            self.assertIn("enabled", handed["Telegram"])

    def test_chat_id_must_be_a_supergroup(self):
        self.assertEqual(control.validate_setting("Telegram", "chat_id", CHAT), CHAT)
        self.assertEqual(control.validate_setting("Telegram", "chat_id", ""), "")
        for bad in ("12345", "-50", "not-a-number", "-100"):
            with self.assertRaises(control.ControlError):
                control.validate_setting("Telegram", "chat_id", bad)
        with self.assertRaises(control.ControlError):
            control.validate_setting("Telegram", "bot_token", "not-a-token")

    def test_config_get_masks_the_token(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(os.environ, {"MSR_SECRET_DIR": os.path.join(tmp, "keys")}):
            cfg = Config(os.path.join(tmp, "config.ini"))
            app = control.HeadlessApp(cfg, lambda keys: None)
            self.addCleanup(app.close)
            ctl = control.Controller(app)
            reply = ctl.handle("config_set", {"section": "Telegram", "key": "bot_token", "value": TOKEN})
            self.assertTrue(reply["ok"], reply)
            self.assertEqual(reply["result"]["value"], "<set>")
            shown = ctl.handle("config_get", {"section": "Telegram", "key": "bot_token"})
            self.assertEqual(shown["result"]["settings"]["Telegram"]["bot_token"], "<set>")
            self.assertNotIn("bot_token_dpapi", shown["result"]["settings"]["Telegram"])
            self.assertNotIn("bot_token_box", shown["result"]["settings"]["Telegram"])
            with open(cfg.config_file, encoding="utf-8") as fh:
                text = fh.read()
            self.assertNotIn(TOKEN, text)
            if dpapi.available():
                self.assertIn("bot_token_dpapi", text)
            else:
                self.assertIn("bot_token_box", text)

    def test_session_file_round_trip_and_mode(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(os.environ, {"MSR_SECRET_DIR": os.path.join(tmp, "keys")}), \
                mock.patch.object(dpapi, "available", return_value=False):
            save_session(tmp, "session-secret", "fp1")
            path = os.path.join(tmp, "telegram_session.json")
            text = open(path, encoding="utf-8").read()
            self.assertNotIn("session-secret", text)
            self.assertNotIn(TOKEN, text)
            self.assertIn("box", text)
            if os.name != "nt":
                self.assertEqual(stat.S_IMODE(os.stat(path).st_mode) & 0o777, 0o600)
                key = os.path.join(tmp, "keys", "secret.key")
                self.assertEqual(stat.S_IMODE(os.stat(key).st_mode) & 0o777, 0o600)
                self.assertEqual(stat.S_IMODE(os.stat(os.path.dirname(key)).st_mode) & 0o777, 0o700)
            self.assertEqual(load_session(tmp, "fp1"), "session-secret")
            self.assertIsNone(load_session(tmp, "other-bot"))

    def test_plaintext_session_is_kept_when_the_key_is_unusable(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(os.environ, {"MSR_SECRET_DIR": os.path.join(tmp, "keys")}), \
                mock.patch.object(dpapi, "available", return_value=False):
            os.makedirs(os.path.join(tmp, "keys"))
            with open(os.path.join(tmp, "keys", "secret.key"), "wb") as fh:
                fh.write(b"short")
            path = os.path.join(tmp, "telegram_session.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump({"v": 1, "fp": "fp1", "session": "session-secret"}, fh)
            with self.assertLogs(level="WARNING") as logs:
                self.assertEqual(load_session(tmp, "fp1"), "session-secret")
            text = open(path, encoding="utf-8").read()
            self.assertIn("session-secret", text)
            self.assertIn("left unchanged", "".join(logs.output))
            self.assertNotIn("session-secret", "".join(logs.output))

    def test_plaintext_session_is_rewritten_encrypted(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(os.environ, {"MSR_SECRET_DIR": os.path.join(tmp, "keys")}), \
                mock.patch.object(dpapi, "available", return_value=False):
            path = os.path.join(tmp, "telegram_session.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump({"v": 1, "fp": "fp1", "session": "session-secret"}, fh)
            self.assertEqual(load_session(tmp, "fp1"), "session-secret")
            text = open(path, encoding="utf-8").read()
            self.assertNotIn("session-secret", text)

    def test_linux_config_encrypts_the_token_and_the_hash(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(os.environ, {"MSR_SECRET_DIR": os.path.join(tmp, "keys")}), \
                mock.patch.object(dpapi, "available", return_value=False):
            path = os.path.join(tmp, "config.ini")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(f"[Telegram]\napi_hash = {'c' * 32}\nbot_token = {TOKEN}\nchat_id = {CHAT}\n")
            cfg = Config(path)
            from msr.config import read_secret
            self.assertEqual(read_secret(cfg, "Telegram", "bot_token"), TOKEN)
            self.assertEqual(read_secret(cfg, "Telegram", "api_hash"), "c" * 32)
            text = open(path, encoding="utf-8").read()
            self.assertNotIn(TOKEN, text)
            self.assertNotIn("c" * 32, text)
            self.assertIn("bot_token_box", text)
            self.assertIn("api_hash_box", text)
            self.assertIn(CHAT, text)
            # a flipped ciphertext byte does not come back as the token
            cfg.config.set("Telegram", "bot_token_box", "AAAA")
            self.assertEqual(read_secret(cfg, "Telegram", "bot_token"), "")
            if os.name != "nt":
                key = os.path.join(tmp, "keys", "secret.key")
                self.assertEqual(stat.S_IMODE(os.stat(key).st_mode) & 0o777, 0o600)

    def test_session_file_is_ciphertext_when_dpapi_is_on(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(dpapi, "available", return_value=True), \
                    mock.patch.object(dpapi, "encrypt_text", return_value="CIPHERTEXT"), \
                    mock.patch.object(dpapi, "decrypt_text",
                                      return_value='{"v": 1, "fp": "fp1", "session": "session-secret"}'):
                save_session(tmp, "session-secret", "fp1")
                text = open(os.path.join(tmp, "telegram_session.json"), encoding="utf-8").read()
                self.assertNotIn("session-secret", text)
                self.assertIn("CIPHERTEXT", text)
                self.assertEqual(load_session(tmp, "fp1"), "session-secret")

    def test_note_is_not_written_when_uploads_are_off(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "a.mp4")
            with open(path, "wb") as fh:
                fh.write(b"x" * 100)
            self.assertIsNone(note_ready(tmp, False, {"path": path, "channel": "a"}))
            self.assertFalse(os.path.isdir(os.path.join(tmp, "telegram_inbox")))
            note = note_ready(tmp, True, {"path": path, "channel": "alice", "platform": "kick",
                                          "title": "Hello", "recorded_at": "2026-11-20"})
            self.assertTrue(note)
            body = open(note, encoding="utf-8").read()
            self.assertNotIn(TOKEN, body)
            self.assertIn("alice", body)

    def test_gitignore_covers_the_session_and_the_queue(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        text = open(os.path.join(root, ".gitignore"), encoding="utf-8").read()
        for name in ("telegram_session.json", "telegram_queue.json", "telegram_inbox/", "telegram_parts/"):
            self.assertIn(name, text)


class MemberCheckTests(unittest.TestCase):
    def test_creator_passes_and_a_left_member_does_not(self):
        def opener(payload):
            def _open(req, timeout=None):
                self.assertTrue(req.full_url.startswith("https://api.telegram.org/bot"))
                class R:
                    def read(self_):
                        return payload
                    def __enter__(self_):
                        return self_
                    def __exit__(self_, *a):
                        return False
                return R()
            return _open

        owner_is_member(TOKEN, int(CHAT), 5, opener=opener(b'{"ok": true, "result": {"status": "creator"}}'))
        with self.assertRaises(DestinationRejected) as caught:
            owner_is_member(TOKEN, int(CHAT), 5, opener=opener(b'{"ok": true, "result": {"status": "left"}}'))
        self.assertNotIn(TOKEN, str(caught.exception))

    def test_a_transport_error_does_not_include_the_token(self):
        def boom(req, timeout=None):
            raise urllib.error.URLError(req.full_url)
        with self.assertRaises(DestinationRejected) as caught:
            owner_is_member(TOKEN, int(CHAT), 5, opener=boom)
        self.assertNotIn(TOKEN, str(caught.exception))
        self.assertNotIn("api.telegram.org/bot", str(caught.exception))


class UploadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.streams = os.path.join(self.tmp.name, "streams")
        self.clock = Clock()
        self.transport = FakeTransport()
        self._env = mock.patch.dict(os.environ, {"MSR_SECRET_DIR": os.path.join(self.tmp.name, "keys")})
        self._env.start()
        self.addCleanup(self._env.stop)

    def recording(self, payload, name="alice.mp4"):
        folder = os.path.join(self.streams, "Processed", "kick", "alice")
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, name)
        with open(path, "wb") as fh:
            fh.write(payload)
        meta = os.path.splitext(path)[0] + ".meta.json"
        with open(meta, "w", encoding="utf-8") as fh:
            fh.write("{}")
        return path

    def uploader(self, cfg=None, split=None, limit=100_000):
        def _split(src, dest, start, dur):
            data = b"p" * 1500
            with open(dest, "wb") as fh:
                fh.write(data)
            return len(data)
        return Uploader(
            cfg or Cfg(), self.tmp.name, transport=self.transport, membership=lambda s: None,
            installed=lambda: True, clock=self.clock, roots=[self.streams],
            probe=lambda path: 10.0, split=split or _split, limit=limit,
        )

    def test_a_small_file_uploads_once_and_is_kept(self):
        path = self.recording(b"x" * 2048)
        up = self.uploader()
        note_ready(self.tmp.name, True, {"path": path, "channel": "alice", "platform": "kick",
                                         "title": "Live", "recorded_at": "2026-11-20T01:02:03"})
        self.assertIn("idle", drain(up))
        self.assertEqual(len(self.transport.sent), 1)
        self.assertEqual(self.transport.sent[0]["chat"], int(CHAT))
        self.assertEqual(self.transport.sent[0]["topic"], 55)
        self.assertIn("alice (kick)", self.transport.sent[0]["caption"])
        self.assertNotIn("part ", self.transport.sent[0]["caption"])
        self.assertTrue(os.path.isfile(path))
        blob = json.dumps(up.status())
        self.assertNotIn(TOKEN, blob)
        self.assertNotIn("a" * 32, blob)
        self.assertNotIn(path, blob)

    def test_over_the_limit_is_split_and_deleted_only_after_every_part(self):
        path = self.recording(b"x" * 5000)
        cfg = Cfg()
        cfg.v[("Telegram", "delete_after_upload")] = "true"
        calls = {"n": 0}

        def send_file(settings, session, file_path, caption, progress):
            calls["n"] += 1
            if calls["n"] == 2:
                raise TelegramError("connection reset")
            return FakeTransport.send_file(self.transport, settings, session, file_path, caption, progress)

        self.transport.send_file = send_file
        up = self.uploader(cfg, limit=2000)
        note_ready(self.tmp.name, True, {"path": path, "channel": "alice", "platform": "kick", "title": "Live"})
        drain(up)
        self.assertTrue(os.path.isfile(path), "a failed part must keep the local file")
        self.assertGreaterEqual(len(self.transport.sent), 1)
        self.clock.now += 30
        drain(up)
        self.assertFalse(os.path.isfile(path))
        self.assertFalse(os.path.isfile(os.path.splitext(path)[0] + ".meta.json"))
        captions = [row["caption"] for row in self.transport.sent]
        self.assertTrue(any("part " in c for c in captions))
        self.assertTrue(all(row["chat"] == int(CHAT) and row["topic"] == 55 for row in self.transport.sent))

    def test_delete_stays_off_keeps_the_file_after_a_full_upload(self):
        path = self.recording(b"x" * 5000)
        up = self.uploader(limit=2000)
        note_ready(self.tmp.name, True, {"path": path, "channel": "alice", "platform": "kick"})
        drain(up)
        self.assertGreater(len(self.transport.sent), 1)
        self.assertTrue(os.path.isfile(path))

    def test_flood_wait_is_honoured(self):
        path = self.recording(b"x" * 2048)
        self.transport.fail.append(Flood(30))
        up = self.uploader()
        note_ready(self.tmp.name, True, {"path": path, "channel": "alice", "platform": "kick"})
        drain(up)
        self.assertEqual(self.transport.sent, [])
        self.assertTrue(os.path.isfile(path))
        drain(up)
        self.assertEqual(self.transport.sent, [])
        self.clock.now += 31
        drain(up)
        self.assertEqual(len(self.transport.sent), 1)

    def test_a_rejected_token_pauses_the_queue(self):
        path = self.recording(b"x" * 2048)
        self.transport.fail.append(AuthRejected(f"token {TOKEN} rejected"))
        up = self.uploader()
        note_ready(self.tmp.name, True, {"path": path, "channel": "alice", "platform": "kick"})
        drain(up)
        self.assertEqual(self.transport.sent, [])
        again = drain(up)
        self.assertIn("blocked", again)
        self.assertEqual(self.transport.sent, [])
        self.assertTrue(os.path.isfile(path))
        self.assertNotIn(TOKEN, json.dumps(up.status()))

    def test_changing_the_group_after_a_part_stops_the_upload(self):
        path = self.recording(b"x" * 5000)
        cfg = Cfg()
        up = self.uploader(cfg, limit=2000)
        note_ready(self.tmp.name, True, {"path": path, "channel": "alice", "platform": "kick"})
        sent_before = 0
        for _ in range(12):
            state = up.step()
            if len(self.transport.sent) >= 1:
                sent_before = len(self.transport.sent)
                break
            if state in ("idle", "blocked", "error"):
                break
        self.assertGreaterEqual(sent_before, 1)
        cfg.v[("Telegram", "chat_id")] = OTHER
        drain(up)
        self.assertTrue(all(row["chat"] == int(CHAT) for row in self.transport.sent))
        self.assertFalse(any(row["chat"] == int(OTHER) for row in self.transport.sent))
        self.assertTrue(os.path.isfile(path))

    def test_a_file_outside_processed_is_not_uploaded(self):
        path = os.path.join(self.tmp.name, "secret.mp4")
        with open(path, "wb") as fh:
            fh.write(b"x" * 2048)
        up = self.uploader()
        with up._lock:
            up.store.enqueue(path, "alice", "kick", "", "")
            up.store.save()
        drain(up)
        self.assertEqual(self.transport.sent, [])
        self.assertTrue(os.path.isfile(path))

    def test_queue_survives_a_restart_and_does_not_resend_a_confirmed_part(self):
        path = self.recording(b"x" * 5000)
        up = self.uploader(limit=2000)
        note_ready(self.tmp.name, True, {"path": path, "channel": "alice", "platform": "kick"})
        for _ in range(8):
            if self.transport.sent:
                break
            up.step()
        self.assertGreaterEqual(len(self.transport.sent), 1)
        up.store.save()
        restarted = self.uploader(limit=2000)
        restarted.transport = self.transport
        drain(restarted)
        names = [row["path"] for row in self.transport.sent]
        # part 1 was confirmed before the restart, so its filename is not sent again
        self.assertEqual(names.count(names[0]), 1)

    def test_uploads_stay_off_without_a_thread_until_enabled(self):
        cfg = Cfg()
        cfg.v[("Telegram", "enabled")] = "false"
        up = self.uploader(cfg)
        up.sync()
        self.assertIsNone(up._thread)
        cfg.v[("Telegram", "enabled")] = "true"
        up.sync()
        self.assertTrue(up._thread.is_alive())
        up.stop()

    def test_status_when_telethon_is_missing(self):
        up = self.uploader()
        up._installed_fn = lambda: False
        self.assertEqual(up.step(), "missing")
        self.assertIn("Telethon", up.status()["line"])
        self.assertEqual(self.transport.sent, [])


if __name__ == "__main__":
    unittest.main()
