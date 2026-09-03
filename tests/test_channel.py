"""Channel key helpers: watch URLs, cookie domains, on-disk folders."""
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.gui import move_list_items
from msr.platforms import is_fishtank_stream_id, is_known_fishtank_camera
from msr.util import (
    channel_file_stem,
    channel_key_to_dirs,
    channel_watch_url,
    custom_url_folder_names,
    get_cookie_domain_for_channel,
    iter_channel_record_dirs,
    parse_custom_url,
)


class FishtankIdTests(unittest.TestCase):
    def test_stream_id_shape(self):
        self.assertTrue(is_fishtank_stream_id("dirc-5"))
        self.assertTrue(is_fishtank_stream_id("computer-lab2-5"))
        self.assertTrue(is_fishtank_stream_id("cameraman2-5"))
        self.assertFalse(is_fishtank_stream_id("director"))
        self.assertFalse(is_fishtank_stream_id("notacamera"))
        self.assertFalse(is_fishtank_stream_id("foo-"))

    def test_known_alias_or_raw(self):
        self.assertTrue(is_known_fishtank_camera("director"))
        self.assertTrue(is_known_fishtank_camera("ben-5"))
        self.assertFalse(is_known_fishtank_camera("notacamera"))


class ChannelWatchUrlTests(unittest.TestCase):
    def test_kick_bare_name(self):
        self.assertEqual(channel_watch_url("asmongold"), "https://kick.com/asmongold")

    def test_twitch(self):
        self.assertEqual(channel_watch_url("twitch:saruei"), "https://twitch.tv/saruei")

    def test_youtube_handle_without_at(self):
        self.assertEqual(
            channel_watch_url("youtube:OhDough"),
            "https://youtube.com/@OhDough/live",
        )

    def test_youtube_handle_keeps_single_at(self):
        # Previously Open in Browser prepended @ even when the stored name
        # already had one, producing youtube.com/@@handle/live.
        self.assertEqual(
            channel_watch_url("youtube:@OhDough"),
            "https://youtube.com/@OhDough/live",
        )

    def test_youtube_channel_id(self):
        self.assertEqual(
            channel_watch_url("youtube:UCxxxxxxxxxxxxxxxx"),
            "https://youtube.com/channel/UCxxxxxxxxxxxxxxxx/live",
        )

    def test_rumble_not_kick(self):
        self.assertEqual(
            channel_watch_url("rumble:BadlandsMedia"),
            "https://rumble.com/c/BadlandsMedia",
        )

    def test_tiktok_strips_at(self):
        self.assertEqual(
            channel_watch_url("tiktok:@qvc"),
            "https://www.tiktok.com/@qvc/live",
        )
        self.assertEqual(
            channel_watch_url("tiktok:qvc"),
            "https://www.tiktok.com/@qvc/live",
        )

    def test_fishtank_opens_site(self):
        self.assertEqual(
            channel_watch_url("fishtank:director"),
            "https://www.fishtank.live/",
        )

    def test_custom_passthrough(self):
        url = "https://chaturbate.com/alice/"
        self.assertEqual(channel_watch_url(f"custom:{url}"), url)

    def test_empty(self):
        self.assertIsNone(channel_watch_url(""))
        self.assertIsNone(channel_watch_url(None))


class CookieDomainTests(unittest.TestCase):
    def test_rumble_is_not_kick(self):
        self.assertEqual(get_cookie_domain_for_channel("rumble:BadlandsMedia"), "rumble.com")

    def test_kick_bare_name(self):
        self.assertEqual(get_cookie_domain_for_channel("asmongold"), "kick.com")

    def test_tiktok_youtube_twitch_fishtank(self):
        self.assertEqual(get_cookie_domain_for_channel("tiktok:qvc"), "tiktok.com")
        self.assertEqual(get_cookie_domain_for_channel("youtube:@x"), "youtube.com")
        self.assertEqual(get_cookie_domain_for_channel("twitch:saruei"), "twitch.tv")
        self.assertEqual(get_cookie_domain_for_channel("fishtank:director"), "fishtank.live")


class ChannelKeyToDirsTests(unittest.TestCase):
    def test_kick_bare_name(self):
        self.assertEqual(channel_key_to_dirs("asmongold"), ("kick", "asmongold"))

    def test_tiktok_drops_leading_at(self):
        self.assertEqual(channel_key_to_dirs("tiktok:@qvc"), ("tiktok", "qvc"))

    def test_youtube_keeps_at(self):
        self.assertEqual(channel_key_to_dirs("youtube:@OhDough"), ("youtube", "@OhDough"))

    def test_chaturbate_custom_nests_under_site(self):
        platform, rel = channel_key_to_dirs(
            "custom:https://chaturbate.com/kittycaitlin/"
        )
        self.assertEqual(platform, "custom")
        self.assertEqual(rel, os.path.join("chaturbate", "kittycaitlin"))

    def test_chaturbate_custom_other_rooms(self):
        for user in ("kaydenwithpaul", "gigi_ulala"):
            _, rel = channel_key_to_dirs(f"custom:https://chaturbate.com/{user}/")
            self.assertEqual(rel, os.path.join("chaturbate", user))

    def test_custom_m3u8_stays_in_site_bag(self):
        platform, rel = channel_key_to_dirs(
            "custom:https://cdn.example.com/hls/master.m3u8"
        )
        self.assertEqual(platform, "custom")
        self.assertEqual(rel, "example")

    def test_fansly_live_path_nests(self):
        _, rel = channel_key_to_dirs("custom:https://fansly.com/live/YuukoVT")
        self.assertEqual(rel, os.path.join("fansly", "YuukoVT"))

    def test_rumble_custom_skips_c_segment(self):
        _, rel = channel_key_to_dirs("custom:https://rumble.com/c/BadlandsMedia")
        self.assertEqual(rel, os.path.join("rumble", "BadlandsMedia"))

    def test_tiktok_custom_stays_single_folder(self):
        # Legacy layout: custom/<handle>, not custom/tiktok/<handle>
        _, rel = channel_key_to_dirs("custom:https://www.tiktok.com/@qvc/live")
        self.assertEqual(rel, "qvc")

    def test_clip_filename_does_not_add_extra_chaturbate_layer(self):
        # Regression: f"{username_dir}_{ts}.mp4" with username_dir
        # "chaturbate\\mode_bad" created
        # Clips/custom/chaturbate/mode_bad/chaturbate/mode_bad_….mp4
        _, rel = channel_key_to_dirs(
            "custom:https://chaturbate.com/mode_bad/"
        )
        stem = channel_file_stem(rel)
        self.assertEqual(stem, "mode_bad")
        out_dir = os.path.join("Clips", "custom", rel)
        out_file = os.path.join(out_dir, f"{stem}_20260902_205254_15s.mp4")
        self.assertEqual(
            os.path.basename(out_file),
            "mode_bad_20260902_205254_15s.mp4",
        )
        self.assertEqual(
            os.path.normpath(os.path.dirname(out_file)),
            os.path.normpath(os.path.join(
                "Clips", "custom", "chaturbate", "mode_bad",
            )),
        )
        # The old join is what produced the extra layer
        buggy = os.path.join(out_dir, f"{rel}_20260902_205254_15s.mp4")
        self.assertNotEqual(
            os.path.normpath(os.path.dirname(buggy)),
            os.path.normpath(os.path.dirname(out_file)),
        )


class CustomUrlFolderNamesTests(unittest.TestCase):
    def test_file_username_is_handle_not_nested_path(self):
        file_name, rel = custom_url_folder_names(
            "https://chaturbate.com/kittycaitlin/"
        )
        self.assertEqual(file_name, "kittycaitlin")
        self.assertEqual(rel, os.path.join("chaturbate", "kittycaitlin"))
        self.assertNotIn(os.sep, file_name)

    def test_parse_chaturbate(self):
        self.assertEqual(
            parse_custom_url("https://chaturbate.com/tatumwest0/"),
            ("chaturbate", "tatumwest0"),
        )


class IterChannelRecordDirsTests(unittest.TestCase):
    def test_walks_bag_and_nested_user(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            bag = os.path.join(tmp, "custom", "chaturbate")
            nested = os.path.join(bag, "alice")
            os.makedirs(nested)
            open(os.path.join(bag, "old.ts"), "w").close()
            open(os.path.join(nested, "new.ts"), "w").close()
            found = {
                rel.replace("\\", "/")
                for _plat, rel, _path in iter_channel_record_dirs(tmp)
            }
            self.assertIn("chaturbate", found)
            self.assertIn("chaturbate/alice", found)


class MoveListItemsTests(unittest.TestCase):
    def test_move_one_to_top(self):
        seq = ["a", "b", "c", "d"]
        new = move_list_items(seq, [2], to="top")
        self.assertEqual(seq, ["c", "a", "b", "d"])
        self.assertEqual(new, [0])

    def test_move_block_to_top_keeps_relative_order(self):
        seq = ["a", "b", "c", "d", "e"]
        new = move_list_items(seq, [3, 1], to="top")
        self.assertEqual(seq, ["b", "d", "a", "c", "e"])
        self.assertEqual(new, [0, 1])

    def test_move_block_to_bottom(self):
        seq = ["a", "b", "c", "d"]
        new = move_list_items(seq, [0, 2], to="bottom")
        self.assertEqual(seq, ["b", "d", "a", "c"])
        self.assertEqual(new, [2, 3])

    def test_already_at_top_is_stable(self):
        seq = ["a", "b", "c"]
        new = move_list_items(seq, [0], to="top")
        self.assertEqual(seq, ["a", "b", "c"])
        self.assertEqual(new, [0])

    def test_ignores_bad_indices(self):
        seq = ["a", "b"]
        self.assertEqual(move_list_items(seq, [-1, 9, "x"], to="top"), [])
        self.assertEqual(seq, ["a", "b"])


if __name__ == "__main__":
    unittest.main()
