"""Rumble channel-page JSON: live URL + HLS playlist."""
import logging
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.platforms import parse_rumble_channel_html


class ParseRumbleChannelHtmlTests(unittest.TestCase):
    def test_live_item_exposes_page_url_and_hls(self):
        html = '''
        <rum-videos-grid>
        <script type="application/json">
        {"items":[{"live":true,"livestream_status":2,
          "url":"https://rumble.com/v60552h-newsmax2-live.html",
          "title":"NEWSMAX2 LIVE",
          "videos":[{"url":"https://cdn.rumble.com/live/newsmax.m3u8"}]}]}
        </script>
        </rum-videos-grid>
        '''
        live_url, title, hls = parse_rumble_channel_html(html, logging.getLogger("test"))
        self.assertEqual(live_url, "https://rumble.com/v60552h-newsmax2-live.html")
        self.assertEqual(title, "NEWSMAX2 LIVE")
        self.assertEqual(hls, "https://cdn.rumble.com/live/newsmax.m3u8")

    def test_ended_livestream_is_not_live(self):
        html = '''
        <script type="application/json">
        {"items":[{"live":false,"livestream_status":1,
          "url":"https://rumble.com/vended.html",
          "videos":[{"url":"https://cdn.rumble.com/dvr.m3u8"}]}]}
        </script>
        '''
        live_url, title, hls = parse_rumble_channel_html(html, logging.getLogger("test"))
        self.assertIsNone(live_url)
        self.assertIsNone(hls)

    def test_relative_url(self):
        html = '''
        <script type="application/json">
        {"items":[{"live":true,"relative_url":"/vabc12-slug.html","title":"x"}]}
        </script>
        '''
        live_url, title, hls = parse_rumble_channel_html(html, logging.getLogger("test"))
        self.assertEqual(live_url, "https://rumble.com/vabc12-slug.html")
        self.assertIsNone(hls)


if __name__ == "__main__":
    unittest.main()
