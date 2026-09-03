"""TikTok live-status parsers used when yt-dlp reports offline."""
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.platforms import tiktok_parse_api_live_room, tiktok_room_id_from_html


class TiktokRoomIdFromHtmlTests(unittest.TestCase):
    def test_quoted_room_id(self):
        html = '{"user":{"uniqueId":"aljazeeraenglish","roomId":"7555123499"}}'
        self.assertEqual(tiktok_room_id_from_html(html), "7555123499")

    def test_unquoted_room_id(self):
        html = '{"roomId": 7555123499, "status": 2}'
        self.assertEqual(tiktok_room_id_from_html(html), "7555123499")

    def test_missing(self):
        self.assertIsNone(tiktok_room_id_from_html("<html>captcha</html>"))
        self.assertIsNone(tiktok_room_id_from_html(""))
        self.assertIsNone(tiktok_room_id_from_html(None))


class TiktokParseApiLiveRoomTests(unittest.TestCase):
    def test_live_user_and_room_status_2(self):
        data = {
            "data": {
                "user": {"uniqueId": "aljazeeraenglish", "roomId": "111", "status": 2},
                "liveRoom": {"title": "Al Jazeera English TT Live", "status": 2},
            }
        }
        live, room_id, title = tiktok_parse_api_live_room(data)
        self.assertTrue(live)
        self.assertEqual(room_id, "111")
        self.assertEqual(title, "Al Jazeera English TT Live")

    def test_ended_keeps_room_id_but_not_live(self):
        data = {
            "data": {
                "user": {"roomId": "222", "status": 4},
                "liveRoom": {"title": "press conference", "status": 4},
            }
        }
        live, room_id, title = tiktok_parse_api_live_room(data)
        self.assertFalse(live)
        self.assertEqual(room_id, "222")
        self.assertEqual(title, "press conference")

    def test_live_if_only_live_room_status_is_2(self):
        data = {
            "data": {
                "user": {"roomId": 333, "status": 4},
                "liveRoom": {"status": 2, "title": "still up"},
            }
        }
        live, room_id, title = tiktok_parse_api_live_room(data)
        self.assertTrue(live)
        self.assertEqual(room_id, "333")
        self.assertEqual(title, "still up")

    def test_garbage(self):
        self.assertEqual(tiktok_parse_api_live_room({}), (False, None, None))
        self.assertEqual(tiktok_parse_api_live_room(None), (False, None, None))
