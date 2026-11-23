"""Roster allowlist, log redaction, yt-dlp ffmpeg protocol whitelist."""
import logging
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.deps import HAS_STREAMLINK, HAS_YTDLP
from msr.platforms import (
    FFMPEG_NETWORK_PROTOCOLS,
    build_recording_command_ffmpeg_merge,
    build_recording_command_fishtank,
    build_recording_command_rumble_hls,
    build_recording_command_ytdlp,
)
from msr.util import (
    RedactLogFilter,
    channel_key_to_dirs,
    coerce_channel_records,
    parse_channel_key,
    redact_cmd_for_log,
    redact_for_log,
    validate_channel_name,
)


class ParseChannelKeyTests(unittest.TestCase):
    def test_kick_bare_name(self):
        self.assertEqual(parse_channel_key("asmongold"), ("kick", "asmongold"))

    def test_prefixed_platforms(self):
        self.assertEqual(parse_channel_key("twitch:saruei"), ("twitch", "saruei"))
        self.assertEqual(parse_channel_key("youtube:@OhDough"), ("youtube", "@OhDough"))
        self.assertEqual(parse_channel_key("tiktok:@qvc"), ("tiktok", "@qvc"))

    def test_custom_https(self):
        url = "https://chaturbate.com/alice/"
        self.assertEqual(parse_channel_key(f"custom:{url}"), ("custom", url))

    def test_kick_colon_is_not_a_platform(self):
        self.assertIsNone(parse_channel_key(r"..\..\..\Users\Public:out"))
        self.assertIsNone(parse_channel_key("evil:rec"))

    def test_file_url_custom_rejected(self):
        self.assertIsNone(parse_channel_key("custom:file:///C:/Windows/notepad.exe"))
        self.assertIsNone(parse_channel_key("custom:javascript:alert(1)"))

    def test_path_metachar_in_name(self):
        self.assertIsNone(parse_channel_key("twitch:foo/bar"))
        self.assertIsNone(parse_channel_key(r"kick_name_with\slash"))
        self.assertIsNone(parse_channel_key(".."))

    def test_unknown_prefix(self):
        self.assertIsNone(parse_channel_key("notaplatform:foo"))


class ValidateChannelNameTests(unittest.TestCase):
    def test_kick_rejects_colon(self):
        ok, msg = validate_channel_name(r"..\..\Users\Public:out", "kick", [])
        self.assertFalse(ok)
        self.assertIn("invalid characters", msg.lower())

    def test_kick_rejects_slash(self):
        ok, msg = validate_channel_name("foo/bar", "kick", [])
        self.assertFalse(ok)

    def test_unknown_platform(self):
        ok, msg = validate_channel_name("foo", "notreal", [])
        self.assertFalse(ok)
        self.assertIn("Unknown platform", msg)

    def test_custom_http_ok_shape(self):
        # May still fail if yt-dlp is missing; the URL shape itself is accepted
        # only when HAS_YTDLP. Either way file: is never ok.
        ok, _ = validate_channel_name("file:///tmp/x", "custom", [])
        self.assertFalse(ok)

    def test_empty_name(self):
        ok, msg = validate_channel_name("", "kick", [])
        self.assertFalse(ok)
        self.assertIn("empty", msg.lower())

    def test_duplicate_kick(self):
        ok, msg = validate_channel_name("asmongold", "kick", ["asmongold"])
        self.assertFalse(ok)
        self.assertIn("already", msg.lower())

    def test_url_paste_on_kick(self):
        ok, _ = validate_channel_name("https://kick.com/asmongold", "kick", [])
        self.assertFalse(ok)

    def test_too_long(self):
        ok, _ = validate_channel_name("a" * 101, "kick", [])
        self.assertFalse(ok)

    def test_custom_requires_http(self):
        ok, msg = validate_channel_name("chaturbate.com/alice", "custom", [])
        self.assertFalse(ok)
        self.assertIn("full URL", msg)

    def test_valid_kick_shape(self):
        if not HAS_STREAMLINK:
            self.skipTest("streamlink not installed")
        ok, msg = validate_channel_name("asmongold", "kick", [])
        self.assertTrue(ok, msg)

    def test_valid_custom_https(self):
        if not HAS_YTDLP:
            self.skipTest("yt-dlp not installed")
        ok, msg = validate_channel_name(
            "https://chaturbate.com/alice/", "custom", [],
        )
        self.assertTrue(ok, msg)

    def test_fishtank_alias_ok(self):
        ok, msg = validate_channel_name("director", "fishtank", [])
        self.assertTrue(ok, msg)

    def test_fishtank_raw_id_ok(self):
        ok, msg = validate_channel_name("ben-5", "fishtank", [])
        self.assertTrue(ok, msg)
        ok, msg = validate_channel_name("computer-lab2-5", "fishtank", [])
        self.assertTrue(ok, msg)
        ok, msg = validate_channel_name("director-as-seen-on", "fishtank", [])
        self.assertTrue(ok, msg)

    def test_fishtank_garbage_rejected(self):
        ok, msg = validate_channel_name("notacamera", "fishtank", [])
        self.assertFalse(ok)
        self.assertIn("Unknown fishtank camera", msg)


class CoerceChannelRecordsTests(unittest.TestCase):
    def test_keeps_valid_drops_traversal(self):
        loaded = [
            {"name": "asmongold", "enabled": True},
            {"name": r"..\..\..\Users\Public:out", "enabled": True},
            {"name": "custom:file:///etc/passwd", "enabled": True},
            "twitch:saruei",
            {"name": "custom:https://chaturbate.com/alice/", "enabled": True},
        ]
        records, skipped = coerce_channel_records(loaded)
        names = [r["name"] for r in records]
        self.assertEqual(
            names,
            ["asmongold", "twitch:saruei", "custom:https://chaturbate.com/alice/"],
        )
        self.assertEqual(len(skipped), 2)

    def test_unsafe_key_cannot_traverse_dirs(self):
        self.assertEqual(
            channel_key_to_dirs(r"..\..\..\Users\Public:out"),
            ("unknown", "unknown"),
        )


class RedactTests(unittest.TestCase):
    def test_googlevideo_url_hides_ip_and_signatures(self):
        url = ("https://manifest.googlevideo.com/api/manifest/hls_playlist/expire/1793321105/ei/MZTj/"
               "ip/2600:4040:10fe:9200:b0d6:496f:b899:3cd6/id/MqdMmvG6TdM.1/itag/311/source/yt_live_broadcast/"
               "sparams/expire,ei,ip/sig/AJfQdSswRQ/lsig/APaTxxM/playlist/index.m3u8")
        cmd = ["ffmpeg", "-headers", "User-Agent: X\r\nCookie: a=b\r\n", "-i", url, "out.ts"]
        out = redact_cmd_for_log(cmd)
        self.assertIn("manifest.googlevideo.com \u2026 id=MqdMmvG6TdM itag=311 [redacted]", out)
        for secret in ("2600:", "AJfQ", "APaT", "1793321105", "MZTj", "a=b"):
            self.assertNotIn(secret, out)
        self.assertIn(url, cmd)  # the real argv is untouched

    def test_twitch_m3u8_tokens_and_ip_query(self):
        out = redact_for_log("https://usher.ttvnw.net/api/channel/hls/saruei.m3u8?allow_source=true"
                             "&sig=abc123&token=%7B%22user_ip%22%7D&ip=1.2.3.4&expire=99")
        for secret in ("abc123", "user_ip", "1.2.3.4", "=99"):
            self.assertNotIn(secret, out)
        self.assertIn("allow_source=true", out)

    def test_jwt_query(self):
        text = "Error opening input: https://cdn.example/hls/index.m3u8?jwt=eyJabc.def"
        out = redact_for_log(text)
        self.assertIn("jwt=***", out)
        self.assertNotIn("eyJabc", out)

    def test_authorization_header(self):
        text = "Authorization: Bearer supersecret\r\nX-Other: 1"
        out = redact_for_log(text)
        self.assertIn("Authorization: ***", out)
        self.assertNotIn("supersecret", out)

    def test_cookie_header(self):
        text = "Cookie: sessionid=abc123; other=1"
        out = redact_for_log(text)
        self.assertIn("Cookie: ***", out)
        self.assertNotIn("abc123", out)

    def test_token_and_password_query(self):
        text = "https://cdn.example/x?token=sekrit&password=hunter2&ok=1"
        out = redact_for_log(text)
        self.assertIn("token=***", out)
        self.assertIn("password=***", out)
        self.assertIn("ok=1", out)
        self.assertNotIn("sekrit", out)
        self.assertNotIn("hunter2", out)

    def test_redact_cmd_joins_argv(self):
        cmd = ["ffmpeg", "-i", "https://x/index.m3u8?jwt=eyJabc", "out.ts"]
        out = redact_cmd_for_log(cmd)
        self.assertIn("jwt=***", out)
        self.assertNotIn("eyJabc", out)
        self.assertIn("ffmpeg", out)

    def test_filter_redacts_message_and_channel(self):
        filt = RedactLogFilter()
        rec = logging.LogRecord(
            "t", logging.INFO, __file__, 1,
            "fail: https://x/index.m3u8?jwt=eyJabc", (), None,
        )
        rec.channel = "custom:https://x/index.m3u8?jwt=eyJabc"
        self.assertTrue(filt.filter(rec))
        self.assertNotIn("eyJabc", rec.getMessage())
        self.assertNotIn("eyJabc", rec.channel)
        self.assertIn("jwt=***", rec.getMessage())

    def test_filter_redacts_traceback(self):
        filt = RedactLogFilter()
        try:
            raise ValueError("https://x/?jwt=eyJabc")
        except ValueError:
            rec = logging.LogRecord(
                "t", logging.ERROR, __file__, 1, "boom", (), sys.exc_info(),
            )
        self.assertTrue(filt.filter(rec))
        self.assertIsNotNone(rec.exc_text)
        self.assertNotIn("eyJabc", rec.exc_text)
        self.assertIn("jwt=***", rec.exc_text)


class _Cfg:
    def get(self, section, key, fallback=None):
        if key == "ffmpeg_path":
            return "ffmpeg"
        return fallback if fallback is not None else ""


def _whitelist_protocols(cmd):
    """Protocol names from -protocol_whitelist or ffmpeg_i:-protocol_whitelist."""
    found = []
    for i, arg in enumerate(cmd):
        if arg == "-protocol_whitelist" and i + 1 < len(cmd):
            found.extend(p.strip() for p in cmd[i + 1].split(",") if p.strip())
        if "protocol_whitelist" in arg and ":" in arg:
            found.extend(
                p.strip()
                for p in arg.split("protocol_whitelist", 1)[-1].replace(" ", ",").split(",")
                if p.strip() and p.strip() != "-"
            )
    return found


class NetworkFfmpegWhitelistTests(unittest.TestCase):
    def _assert_no_file(self, cmd):
        self.assertIn(FFMPEG_NETWORK_PROTOCOLS, " ".join(cmd))
        protos = _whitelist_protocols(cmd)
        self.assertTrue(protos, msg=f"no whitelist in {cmd}")
        self.assertNotIn("file", protos)
        self.assertNotIn("file", FFMPEG_NETWORK_PROTOCOLS.split(","))

    def test_constant_omits_file(self):
        self.assertNotIn("file", FFMPEG_NETWORK_PROTOCOLS.split(","))
        self.assertIn("https", FFMPEG_NETWORK_PROTOCOLS.split(","))
        self.assertIn("hls", FFMPEG_NETWORK_PROTOCOLS.split(","))

    def test_ytdlp_ffmpeg_downloader(self):
        cmd = build_recording_command_ytdlp(
            "https://youtube.com/@x/live", "out.ts", _Cfg(), False, False,
        )
        self.assertIn("--downloader-args", cmd)
        self.assertIn("ffmpeg_i:-protocol_whitelist", " ".join(cmd))
        self._assert_no_file(cmd)

    def test_rumble_hls(self):
        cmd = build_recording_command_rumble_hls(
            "https://cdn.rumble.com/live/x.m3u8", "out.ts", _Cfg(), False,
        )
        self._assert_no_file(cmd)

    def test_fishtank(self):
        cmd = build_recording_command_fishtank(
            "https://host/hls/live+id/index.m3u8?jwt=x", "out.ts", _Cfg(), False,
        )
        self._assert_no_file(cmd)

    def test_cmaf_merge(self):
        cmd = build_recording_command_ffmpeg_merge(
            "https://cdn.example/v.m3u8", "https://cdn.example/a.m3u8",
            "out.ts", _Cfg(), False,
        )
        self._assert_no_file(cmd)
        # -protocol_whitelist and -headers are per-input in ffmpeg and must precede both -i args
        i_positions = [idx for idx, arg in enumerate(cmd) if arg == "-i"]
        self.assertEqual(len(i_positions), 2)
        wl_positions = [idx for idx, arg in enumerate(cmd) if arg == "-protocol_whitelist"]
        self.assertEqual(len(wl_positions), 2)
        self.assertLess(wl_positions[0], i_positions[0])
        self.assertGreater(wl_positions[1], i_positions[0])
        self.assertLess(wl_positions[1], i_positions[1])

    def test_cmaf_merge_rejects_non_http_and_crlf_headers(self):
        with self.assertRaises(ValueError):
            build_recording_command_ffmpeg_merge(
                "https://cdn.example/v.m3u8", "file:///etc/passwd",
                "out.ts", _Cfg(), False,
            )
        cmd = build_recording_command_ffmpeg_merge(
            "https://cdn.example/v.m3u8", "https://cdn.example/a.m3u8",
            "out.ts", _Cfg(), False,
            http_headers={"X-Good": "ok", "X-Bad": "val\r\nInjected: yes"},
        )
        hdr = cmd[cmd.index("-headers") + 1]
        self.assertIn("X-Good: ok\r\n", hdr)
        self.assertNotIn("Injected", hdr)

    def test_cmaf_manifest_fallback(self):
        cmd = build_recording_command_ffmpeg_merge(
            None, None, "out.ts", _Cfg(), False,
            manifest_url="https://cdn.example/master.m3u8",
        )
        self._assert_no_file(cmd)


class HardeningQuickFixTests(unittest.TestCase):
    def test_redact_lf_headers_and_basic_auth(self):
        out = redact_for_log("Cookie: session=SECRET123\nUser-Agent: Mozilla\nX-Auth-Token: tok99")
        self.assertNotIn("SECRET123", out)
        self.assertNotIn("tok99", out)
        self.assertIn("Cookie: ***", out)
        self.assertIn("User-Agent: Mozilla", out)

        arg_out = redact_cmd_for_log(["streamlink", "--http-header", "Cookie=topsecret; a=1"])
        self.assertNotIn("topsecret", arg_out)
        self.assertIn("Cookie=***", arg_out)

        auth_url = redact_for_log("https://alice:s3cr3t@cdn.example/live.m3u8")
        self.assertNotIn("s3cr3t", auth_url)
        self.assertNotIn("alice", auth_url)
        self.assertIn("https://***@cdn.example/live.m3u8", auth_url)

        yt = redact_for_log("https://www.youtube.com/@LofiGirl/live")
        self.assertEqual(yt, "https://www.youtube.com/@LofiGirl/live")

    def test_ytdlp_escapes_percent_and_separates_url(self):
        cmd = build_recording_command_ytdlp(
            "https://youtube.com/@x/live", "dir/%(http_headers)s_100%.ts", _Cfg(), False, False,
        )
        self.assertEqual(cmd[cmd.index("-o") + 1], "dir/%%(http_headers)s_100%%.ts")
        self.assertEqual(cmd[-2:], ["--", "https://youtube.com/@x/live"])

    def test_rumble_host_and_html_url_validation(self):
        from msr.platforms import _is_rumble_channel_url, parse_rumble_channel_html
        self.assertFalse(_is_rumble_channel_url("https://evil.example/?u=rumble.com/c/foo"))
        self.assertTrue(_is_rumble_channel_url("https://rumble.com/c/NewsmaxTV"))
        html = (
            '<script type="application/json">'
            '{"items":[{"live":true,"url":"--exec=evil","title":"t","videos":[{"url":"file:///etc/passwd"}]}]}'
            '</script>'
        )
        url, title, hls = parse_rumble_channel_html(html, logging.getLogger("test"))
        self.assertIsNone(url)
        self.assertIsNone(hls)

    def test_fishtank_cookie_domain_and_host_validation(self):
        import base64
        import json
        import tempfile
        import time
        from unittest import mock
        from msr.platforms import FishtankAuth, _is_fishtank_host, resolve_best_fishtank_variant

        self.assertTrue(_is_fishtank_host("streams-f.fishtank.live"))
        self.assertFalse(_is_fishtank_host("fishtank.live.evil.com"))
        self.assertFalse(_is_fishtank_host("user@fishtank.live"))

        hdr = base64.urlsafe_b64encode(b'{"alg":"HS256"}').decode().rstrip("=")
        pay = base64.urlsafe_b64encode(json.dumps({"exp": int(time.time()) + 3600}).encode()).decode().rstrip("=")
        fake_jwt = f"{hdr}.{pay}.sig"
        val = f'["{fake_jwt}","refresh"]'

        with tempfile.TemporaryDirectory() as tmp:
            jar = os.path.join(tmp, "cookies.txt")
            # Cookie from an unrelated Supabase site must NOT be sent to fishtank.live
            with open(jar, "w", encoding="utf-8") as fh:
                fh.write(f".unrelated-app.com\tTRUE\t/\tTRUE\t0\tsb-xyz-auth-token\t{val}\n")
            auth = FishtankAuth(jar, logging.getLogger("test"))
            self.assertIsNone(auth._extract_supabase_jwt())

            # Cookie on fishtank.live (even #HttpOnly_) is accepted
            with open(jar, "a", encoding="utf-8") as fh:
                fh.write(f"#HttpOnly_.fishtank.live\tTRUE\t/\tTRUE\t0\tsb-abc-auth-token\t{val}\n")
            self.assertEqual(auth._extract_supabase_jwt(), fake_jwt)

        # Off-domain / cleartext absolute variant URI is rejected
        playlist = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=5000000\nhttp://evil.example/steal.m3u8\n"
        master = "https://streams-f.fishtank.live/hls/live+dirc-5/index.m3u8?jwt=secret"
        with mock.patch("urllib.request.urlopen") as mopen:
            mopen.return_value.__enter__.return_value.read.return_value = playlist.encode()
            url, _ = resolve_best_fishtank_variant(master, "secret", logging.getLogger("test"))
        self.assertEqual(url, master)

        # Malicious loadBalancer / redirect host in get_live_streams is ignored
        auth2 = FishtankAuth(None, logging.getLogger("test"))
        payload = json.dumps({
            "liveStreams": [{"id": "dirc-5", "name": "Director"}],
            "liveStreamStatus": {"dirc-5": "online"},
            "loadBalancer": {"dirc-5": "fishtank.live.evil.com"},
        })
        fake_resp = mock.MagicMock()
        fake_resp.__enter__.return_value.url = "https://fishtank.live.evil.com/live+dirc-5.jpeg"
        with mock.patch.object(auth2, "_fetch_json", return_value=payload), \
                mock.patch("urllib.request.urlopen", return_value=fake_resp):
            auth2.get_live_streams()
        self.assertEqual(auth2._stream_hosts["dirc-5"], auth2._DEFAULT_STREAM_HOST)
        self.assertEqual(auth2._stream_host, auth2._DEFAULT_STREAM_HOST)

    def test_control_blocks_remote_host_path_and_exe_settings(self):
        import tempfile
        from msr import control
        from msr.config import Config
        from tests.test_control import FakeRecorder, make_data

        with tempfile.TemporaryDirectory() as tmp:
            data, _ = make_data(tmp, ["kickone"])
            cfg = Config(os.path.join(data, "config.ini"))
            app = control.HeadlessApp(cfg, FakeRecorder)
            self.addCleanup(app.close)
            ctl = control.Controller(app)
            for sec, key, val in (
                ("Advanced", "ffmpeg_path", "/bin/sh"),
                ("Clipping", "clips_dir", tmp),
                ("Paths", "channels_file", os.path.join(tmp, "evil.json")),
                ("Paths", "fallback_dir", tmp),
                ("GUI", "window_state_file", "../escape.json"),
            ):
                reply = ctl.handle("config_set", {"section": sec, "key": key, "value": val})
                self.assertFalse(reply["ok"], (sec, key))
                self.assertEqual(reply["code"], "forbidden", (sec, key))

            # Local direct apply_setting still allows Options tab to set ffmpeg_path
            self.assertEqual(control.apply_setting(cfg, "Advanced", "ffmpeg_path", "ffmpeg"), "ffmpeg")
            # window_state_file still rejects path traversal even locally
            with self.assertRaises(control.ControlError):
                control.apply_setting(cfg, "GUI", "window_state_file", "../escape.json")

    def test_twitch_lock_inside_data_dir_and_depsupdate_zip_note(self):
        import tempfile
        from msr import depsupdate, twitch_eventsub as te, update
        from unittest import mock

        with tempfile.TemporaryDirectory() as tmp:
            lp = te._lock_path(tmp)
            self.assertEqual(os.path.dirname(lp), os.path.abspath(tmp))

        row = {"name": "msr", "updatable": True, "status": "update_available", "note": ""}
        with mock.patch.object(update, "source_note", return_value=""):
            depsupdate._annotate_msr(row, "1.0.0")
        self.assertEqual(row["note"], update.ZIP_NOTE)

    def test_reconcile_fallback_skips_symlinks(self):
        import tempfile
        from msr import storage
        if os.name == "nt":
            self.skipTest("POSIX symlink test")
        with tempfile.TemporaryDirectory() as tmp:
            fb = os.path.join(tmp, "fb")
            main = os.path.join(tmp, "main")
            secret = os.path.join(tmp, "outside.txt")
            os.makedirs(fb)
            os.makedirs(main)
            with open(secret, "w", encoding="utf-8") as fh:
                fh.write("secret")
            os.utime(secret, (0, 0))
            link = os.path.join(fb, "cookies.txt")
            os.symlink(secret, link)
            res = storage.reconcile_fallback(fb, main, logging.getLogger("test"), min_age=0)
            self.assertEqual(res.moved, 0)
            self.assertFalse(os.path.exists(os.path.join(main, "cookies.txt")))


class MediumSecurityHardeningTests(unittest.TestCase):
    def test_filecrypt_aad_prevents_cross_field_ciphertext_swap(self):
        import tempfile
        from unittest import mock
        from msr import filecrypt

        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {"MSR_SECRET_DIR": tmp}):
                blob = filecrypt.encrypt_text("bot-token-secret", aad=b"config:telegram.bot_token")
                self.assertEqual(
                    filecrypt.decrypt_text(blob, aad=b"config:telegram.bot_token"),
                    "bot-token-secret",
                )
                with self.assertRaises(filecrypt.CryptoError):
                    filecrypt.decrypt_text(blob, aad=b"config:fishtank.password")
                # Legacy v1 ciphertext (no AAD) still decrypts for seamless migration
                legacy = filecrypt.encrypt_text("legacy-secret")
                self.assertEqual(filecrypt.decrypt_text(legacy, aad=b"config:fishtank.password"), "legacy-secret")

    def test_history_strips_multiline_secrets_and_redacts_custom_urls(self):
        import json
        from msr import history

        ini = b"[Fishtank]\npassword =\n    line1secret\n    line2secret\n[GUI]\ntheme = dark\n"
        stripped = history.strip_secrets(ini).decode("utf-8")
        self.assertNotIn("line1secret", stripped)
        self.assertNotIn("line2secret", stripped)
        self.assertIn("theme = dark", stripped)

        old = json.dumps([{"name": "custom:https://cdn.example/live.m3u8?token=TOPSECRET", "enabled": True}]).encode()
        cur = json.dumps([]).encode()
        summary = history.summarize("channels.json", old, cur)
        self.assertNotIn("TOPSECRET", summary)
        self.assertIn("cdn.example", summary)

    def test_agent_status_redacts_custom_url_tokens_and_errors(self):
        import tempfile
        from msr import agent_status
        from msr.config import Config

        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(os.path.join(tmp, "config.ini"))
            tr = agent_status.Tracker(cfg)
            key = "custom:https://cdn.example/live.m3u8?jwt=SUPERSECRET"
            ev = tr.emit("live", key, source="recording")
            self.assertNotIn("SUPERSECRET", ev["channel"])
            self.assertIn("cdn.example", ev["channel"])
            # wait() still matches when passed the raw channel key
            self.assertEqual(tr.wait({"live"}, channel=key, timeout=0.05, after=0)["seq"], ev["seq"])
            tr._finished(key, False, "failed opening https://x.example/?token=TOK999", path="")
            fin = tr.recent_finished()[0]
            self.assertNotIn("SUPERSECRET", fin["channel"])
            self.assertNotIn("TOK999", fin["error"])

    def test_updater_repo_scope_depsupdate_isolated_and_kick_ws_host(self):
        from msr import depsupdate, kick_realtime as kr, update

        self.assertFalse(update.zipball_url_is_allowed("https://github.com/attacker/evil/archive/refs/tags/v9.zip"))
        self.assertFalse(update.zipball_url_is_allowed("https://api.github.com/repos/attacker/evil/zipball/v9"))
        self.assertTrue(update.zipball_url_is_allowed(
            "https://github.com/ManletPride/Multi-Stream-Recorder/archive/refs/tags/v3.2.0.zip"))

        self.assertIn("--isolated", depsupdate.pip_command("python", "yt-dlp"))

        bad_ws = {"data": {"mode": "websocket", "connections": [
            {"provider": "centrifugo", "credentials": {"url": "wss://evil.example/ws"}},
            {"provider": "pusher", "credentials": {"app_key": "ok", "cluster": "us2.evil.com@"}},
        ]}}
        self.assertIsNone(kr.parse_connection_response(bad_ws))
        with self.assertRaises(ValueError):
            kr.pusher_url("abc", "us2.evil.com@")

    def test_library_destination_and_pending_purge_reject_symlinks(self):
        import tempfile
        from msr import library as lib, library_actions as la, recorder

        if os.name == "nt":
            self.skipTest("POSIX symlink test")
        with tempfile.TemporaryDirectory() as tmp:
            streams = os.path.join(tmp, "streams")
            pending = os.path.join(streams, "PendingDeletion")
            outside = os.path.join(tmp, "outside.mp4")
            os.makedirs(pending)
            with open(outside, "w", encoding="utf-8") as fh:
                fh.write("keep")
            os.utime(outside, (0, 0))
            os.symlink(outside, os.path.join(pending, "link.mp4"))
            deleted = recorder.purge_old_pending_files(streams, max_age_days=1)
            self.assertEqual(deleted, 0)
            self.assertTrue(os.path.exists(outside))

            # Guard.check_destination rejects non-deletable file types (e.g. .exe / .ini)
            guard = la.Guard(lib.Roots(streams_dir=streams), data_dir=os.path.join(streams, "Processed", "data"))
            self.assertIsNotNone(guard.check_destination(os.path.join(streams, "Processed", "kick", "a", "evil.ini")))


class DefenseInDepthSecurityTests(unittest.TestCase):
    def test_config_for_worker_scopes_fishtank_credentials_by_platform(self):
        import json
        import tempfile
        from msr.config import Config
        from msr.telegram_queue import config_for_worker

        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(os.path.join(tmp, "config.ini"))
            cfg.config.set("Fishtank", "email", "user@example.com")
            cfg.config.set("Fishtank", "password", "ft-secret-pw")
            cfg.config.set("Fishtank", "password_box", "box-cipher")

            # Default (no platform) and non-Fishtank platforms never receive [Fishtank]
            for plat in (None, "kick", "twitch", "youtube", "rumble", "tiktok", "custom"):
                handed = config_for_worker(cfg, platform=plat)
                self.assertNotIn("Fishtank", handed, f"platform={plat}")
                blob = json.dumps(handed)
                self.assertNotIn("ft-secret-pw", blob)
                self.assertNotIn("box-cipher", blob)
                self.assertNotIn("user@example.com", blob)

            # Fishtank worker receives [Fishtank]
            ft_handed = config_for_worker(cfg, platform="fishtank")
            self.assertIn("Fishtank", ft_handed)
            self.assertEqual(ft_handed["Fishtank"]["email"], "user@example.com")

    def test_spawn_worker_strips_fishtank_section_for_non_fishtank_channels(self):
        import threading
        from unittest import mock
        from msr.recorder import StreamRecorder

        rec = StreamRecorder.__new__(StreamRecorder)
        rec.wake_events = {}
        rec.should_stop = threading.Event()
        rec.status_queue = object()
        rec.runtime = {}
        rec.log_queue = None
        captured = []

        class FakeProcess:
            def __init__(self, target=None, args=()):
                captured.append(args[0])
                self.daemon = False
                self.pid = 1

            def start(self):
                pass

        shared = {"Paths": {"streams_dir": "/tmp/s"}, "Fishtank": {"email": "a@b.c", "password": "pw"}}
        with mock.patch("msr.recorder.mp.Process", FakeProcess):
            rec._spawn_worker("twitch:streamer", shared)
            rec._spawn_worker("fishtank:director", shared)

        self.assertNotIn("Fishtank", captured[0][1])
        self.assertIn("Fishtank", captured[1][1])
        # Original dict was not mutated in-place
        self.assertIn("Fishtank", shared)

    def test_subprocess_env_filter_strips_fishtank_password(self):
        import subprocess
        import types
        from unittest import mock
        from msr.config import (
            FISHTANK_PASSWORD_ENV,
            clean_subprocess_env,
            install_subprocess_env_filter,
        )

        # Helper strips secret while keeping unrelated variables
        cleaned = clean_subprocess_env({"PATH": "/bin", FISHTANK_PASSWORD_ENV: "topsecret"})
        self.assertEqual(cleaned, {"PATH": "/bin"})

        # Wrapper strips from inherited os.environ and explicit env dicts
        seen = []

        class FakePopen:
            def __init__(self, *args, **kwargs):
                seen.append((args, kwargs))

        fake_mod = types.SimpleNamespace(Popen=FakePopen)
        self.assertTrue(install_subprocess_env_filter(fake_mod))
        self.assertFalse(install_subprocess_env_filter(fake_mod))

        with mock.patch.dict(os.environ, {FISHTANK_PASSWORD_ENV: "env-secret"}):
            fake_mod.Popen(["ffmpeg"])
            self.assertNotIn(FISHTANK_PASSWORD_ENV, seen[-1][1]["env"])
            fake_mod.Popen(["ffmpeg"], env={"A": "1", FISHTANK_PASSWORD_ENV: "explicit-secret"})
            self.assertEqual(seen[-1][1]["env"], {"A": "1"})

        # End-to-end check on real subprocess.run (installed on msr.config import)
        with mock.patch.dict(os.environ, {FISHTANK_PASSWORD_ENV: "live-secret"}):
            r = subprocess.run(
                [sys.executable, "-c",
                 "import os, sys; sys.stdout.write(os.environ.get('MSR_FISHTANK_PASSWORD', 'ABSENT'))"],
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(r.returncode, 0)
            self.assertEqual(r.stdout, "ABSENT")

    def test_git_env_strips_fishtank_password(self):
        from unittest import mock
        from msr import update
        from msr.config import FISHTANK_PASSWORD_ENV

        with mock.patch.dict(os.environ, {FISHTANK_PASSWORD_ENV: "git-secret"}):
            env = update._git_env()
            self.assertNotIn(FISHTANK_PASSWORD_ENV, env)
            self.assertEqual(env["GIT_TERMINAL_PROMPT"], "0")

    def test_security_md_documents_linux_cmdline_caveat(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "SECURITY.md"), encoding="utf-8") as fh:
            text = fh.read()
        self.assertIn("/proc/<pid>/cmdline", text)
        self.assertIn("hidepid", text)
        self.assertIn("MSR_FISHTANK_PASSWORD", text)


if __name__ == "__main__":
    unittest.main()
