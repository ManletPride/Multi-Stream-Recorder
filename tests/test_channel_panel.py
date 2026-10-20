"""Pure-logic tests for the left roster panel (msr/channel_panel.py)."""
import unittest

from msr import channel_panel as cp
from msr.util import get_cookie_status_for_channel


def _info(meaningful=(), expired=(), valid=True):
    return {
        "valid": valid,
        "domains": sorted(set(meaningful) | set(expired)),
        "meaningful_domains": sorted(set(meaningful) | set(expired)),
        "expired_domains": sorted(expired),
    }


class TestPlatformDisplay(unittest.TestCase):
    def test_bare_name_is_kick(self):
        self.assertEqual(cp.channel_display("vampireliquor"), ("vampireliquor", "Kick"))

    def test_prefixed(self):
        self.assertEqual(cp.channel_display("twitch:clovernekoo"), ("clovernekoo", "Twitch"))
        self.assertEqual(cp.channel_display("fishtank:counter"), ("counter", "Fishtank"))
        self.assertEqual(cp.channel_display("youtube:@OhDough"), ("@OhDough", "YouTube"))
        self.assertEqual(cp.channel_display("rumble:NewsmaxTV"), ("NewsmaxTV", "Rumble"))
        self.assertEqual(cp.channel_display("tiktok:ghbk52"), ("ghbk52", "TikTok"))

    def test_custom_url(self):
        self.assertEqual(cp.channel_display("custom:https://www.chaturbate.com/alice/"),
                         ("chaturbate.com/alice", "Custom"))

    def test_split_keeps_unknown_prefix_as_name(self):
        self.assertEqual(cp.split_channel_key("foo:bar"), ("kick", "foo:bar"))

    def test_build_key_round_trip(self):
        for key in ("vampireliquor", "twitch:clovernekoo", "fishtank:counter",
                    "youtube:@x", "custom:https://example.com/a"):
            plat, rest = cp.split_channel_key(key)
            self.assertEqual(cp.build_channel_key(rest, plat), key)

    def test_guess_platform(self):
        self.assertEqual(cp.guess_platform_from_text("twitch:foo"), ("twitch", "foo"))
        self.assertEqual(cp.guess_platform_from_text("https://kick.com/bar"), ("kick", "bar"))
        self.assertEqual(cp.guess_platform_from_text("https://www.twitch.tv/baz/"), ("twitch", "baz"))
        self.assertEqual(cp.guess_platform_from_text("https://x.com/live"), ("custom", "https://x.com/live"))
        self.assertEqual(cp.guess_platform_from_text("plain"), (None, "plain"))


class TestLiveState(unittest.TestCase):
    def s(self, status, detail=""):
        return cp.channel_live_state({"status": status, "detail": detail})

    def test_recording(self):
        self.assertEqual(self.s("Recording", "1080p60 6.0 Mbps"), cp.STATE_RECORDING)
        self.assertEqual(self.s("Recording", "remuxing 40%"), cp.STATE_RECORDING)

    def test_live_not_capturing(self):
        self.assertEqual(self.s("Recording", "starting"), cp.STATE_LIVE)
        self.assertEqual(self.s("Recording", "retry (streamlink)"), cp.STATE_LIVE)
        self.assertEqual(self.s("Offline", "reconnecting (40s left)"), cp.STATE_LIVE)
        self.assertEqual(self.s("Offline", "bad stream data, retry in 60s"), cp.STATE_LIVE)
        self.assertEqual(self.s("Live", ""), cp.STATE_LIVE)

    def test_offline(self):
        self.assertEqual(self.s("Offline", "next check ~171s"), cp.STATE_OFFLINE)
        self.assertEqual(self.s("Checking...", ""), cp.STATE_OFFLINE)
        self.assertEqual(self.s("Error", "login required (check cookies)"), cp.STATE_OFFLINE)
        self.assertEqual(self.s("Completed", "1.2 GB"), cp.STATE_OFFLINE)
        self.assertEqual(self.s("Checking...", "not live"), cp.STATE_OFFLINE)
        self.assertEqual(cp.channel_live_state(None), cp.STATE_OFFLINE)


class TestGrouping(unittest.TestCase):
    CH = [
        {"name": "a", "enabled": True},
        {"name": "twitch:b", "enabled": False},
        {"name": "c", "enabled": True},
        {"name": "d", "enabled": True},
        {"name": "fishtank:e", "enabled": False},
    ]

    def test_groups_keep_manual_order(self):
        states = {"d": cp.STATE_RECORDING, "a": cp.STATE_RECORDING, "c": cp.STATE_LIVE}
        g = dict(cp.group_channels(self.CH, states))
        self.assertEqual(g["recording"], ["a", "d"])
        self.assertEqual(g["enabled"], ["c"])
        self.assertEqual(g["disabled"], ["twitch:b", "fishtank:e"])

    def test_disabled_but_recording_goes_on_top(self):
        g = dict(cp.group_channels(self.CH, {"twitch:b": cp.STATE_RECORDING}))
        self.assertEqual(g["recording"], ["twitch:b"])

    def test_filter_and_views(self):
        g = dict(cp.group_channels(self.CH, {}, query="twitch"))
        self.assertEqual(sum(len(v) for v in g.values()), 1)
        g = dict(cp.group_channels(self.CH, {}, query="FISH"))
        self.assertEqual(g["disabled"], ["fishtank:e"])
        g = dict(cp.group_channels(self.CH, {}, view=cp.VIEW_ENABLED))
        self.assertEqual(g["disabled"], [])
        self.assertEqual(g["enabled"], ["a", "c", "d"])
        g = dict(cp.group_channels(self.CH, {"c": cp.STATE_LIVE, "d": cp.STATE_RECORDING},
                                   view=cp.VIEW_LIVE))
        self.assertEqual(g, {"recording": ["d"], "enabled": ["c"], "disabled": []})

    def test_live_view_includes_live_disabled_channels(self):
        g = dict(cp.group_channels(self.CH, {"fishtank:e": cp.STATE_LIVE},
                                   view=cp.VIEW_LIVE))
        self.assertEqual(g, {"recording": [], "enabled": [], "disabled": ["fishtank:e"]})

    def test_coerce_view_migrates_old_checkbox(self):
        self.assertEqual(cp.coerce_view(None, True), cp.VIEW_ENABLED)
        self.assertEqual(cp.coerce_view(None, False), cp.VIEW_ALL)
        self.assertEqual(cp.coerce_view("Live"), cp.VIEW_LIVE)
        self.assertEqual(cp.coerce_view("bogus"), cp.VIEW_ALL)

    def test_move_relative(self):
        ch = [{"name": n} for n in "abcde"]
        self.assertTrue(cp.move_relative(ch, "d", "b"))
        self.assertEqual([c["name"] for c in ch], list("adbce"))
        self.assertTrue(cp.move_relative(ch, "a", "c", after=True))
        self.assertEqual([c["name"] for c in ch], list("dbcae"))
        self.assertFalse(cp.move_relative(ch, "a", "zzz"))
        self.assertFalse(cp.move_relative(ch, "a", "a"))


class TestCookieBadge(unittest.TestCase):
    def badge(self, key, info, status=None, fishtank_login=False):
        return cp.cookie_badge(key, info, status, get_status=get_cookie_status_for_channel,
                               fishtank_login=fishtank_login)

    def test_fine_means_no_badge(self):
        info = _info(meaningful=["kick.com", "twitch.tv", "fishtank.live"])
        for key in ("vampireliquor", "twitch:x", "fishtank:counter"):
            self.assertIsNone(self.badge(key, info))

    def test_missing_cookie_only_badges_login_required_sites(self):
        info = _info(meaningful=["youtube.com"])
        self.assertIsNone(self.badge("vampireliquor", info))
        self.assertIsNone(self.badge("twitch:x", info))
        self.assertIn("Fishtank needs a login", self.badge("fishtank:counter", info))
        self.assertIsNone(self.badge("fishtank:counter", info, fishtank_login=True))

    def test_no_cookies_file(self):
        info = _info(valid=False)
        self.assertIsNone(self.badge("vampireliquor", info))
        self.assertIn("cookies.txt not found", self.badge("fishtank:counter", info))

    def test_expired_any_platform(self):
        info = _info(expired=["kick.com"])
        self.assertEqual(self.badge("vampireliquor", info),
                         "Kick cookie expired \u2014 re-export cookies.txt")
        self.assertIsNone(self.badge("twitch:x", info))

    def test_rejected_from_worker_error(self):
        info = _info(meaningful=["twitch.tv"])
        st = {"status": "Error", "detail": "login required (check cookies)"}
        self.assertIn("Twitch rejected the login", self.badge("twitch:x", info, st))
        st = {"status": "Error", "detail": "403/503 (cookies expired?)"}
        self.assertIsNotNone(self.badge("youtube:@a", info, st))
        st = {"status": "Error", "detail": "could not obtain JWT for stream"}
        self.assertIsNotNone(self.badge("fishtank:a", info, st, fishtank_login=True))

    def test_rumble_cloudflare_403_is_not_a_cookie_problem(self):
        info = _info(meaningful=["rumble.com"])
        st = {"status": "Error", "detail": "403 (Rumble blocked yt-dlp)"}
        self.assertIsNone(self.badge("rumble:x", info, st))

    def test_auth_text_only_counts_on_error_status(self):
        info = _info(meaningful=["twitch.tv"])
        st = {"status": "Offline", "detail": "login required"}
        self.assertIsNone(self.badge("twitch:x", info, st))

    def test_platform_summary(self):
        info = _info(meaningful=["twitch.tv"], expired=["kick.com"])
        s = cp.platform_cookie_summary(info, get_status=get_cookie_status_for_channel,
                                       fishtank_login=True)
        self.assertEqual(s["kick"], "expired")
        self.assertEqual(s["twitch"], "present")
        self.assertEqual(s["youtube"], "missing")
        self.assertEqual(s["fishtank"], "login")


if __name__ == "__main__":
    unittest.main()
