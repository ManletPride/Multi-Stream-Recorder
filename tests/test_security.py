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

    def test_cmaf_manifest_fallback(self):
        cmd = build_recording_command_ffmpeg_merge(
            None, None, "out.ts", _Cfg(), False,
            manifest_url="https://cdn.example/master.m3u8",
        )
        self._assert_no_file(cmd)


if __name__ == "__main__":
    unittest.main()
