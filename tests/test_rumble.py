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


class PollingEfficiencyTests(unittest.TestCase):
    def test_rumble_offline_channel_does_not_rerun_impersonate(self):
        import subprocess
        from unittest import mock
        from msr import platforms

        ok_empty = subprocess.CompletedProcess(["yt-dlp"], 0, "", "")
        with mock.patch.object(platforms, "HAS_CURL_CFFI", True), \
                mock.patch.object(platforms, "HAS_YTDLP", True), \
                mock.patch.object(platforms.subprocess, "run", return_value=ok_empty) as run_spy, \
                mock.patch.object(platforms, "fetch_rumble_page", return_value=("<html></html>", None)):
            live, *_ = platforms.check_stream_ytdlp("https://rumble.com/c/OfflineChan", logging.getLogger("test"))
        self.assertFalse(live)
        self.assertEqual(run_spy.call_count, 1)

    def test_fishtank_get_live_streams_target_id_probes_only_target(self):
        import json
        from unittest import mock
        from msr.platforms import FishtankAuth

        auth = FishtankAuth(None, logging.getLogger("test"))
        payload = json.dumps({
            "liveStreams": [
                {"id": "cam1-6", "name": "Cam 1"},
                {"id": "cam2-6", "name": "Cam 2"},
                {"id": "cam3-6", "name": "Cam 3"},
            ],
            "liveStreamStatus": {"cam1-6": "online", "cam2-6": "online", "cam3-6": "online"},
            "loadBalancer": {
                "cam1-6": "streams-a.fishtank.live",
                "cam2-6": "streams-b.fishtank.live",
                "cam3-6": "streams-c.fishtank.live",
            },
        })
        fake_resp = mock.MagicMock()
        fake_resp.__enter__.return_value.url = "https://streams-b.fishtank.live/live+cam2-6.jpeg"
        with mock.patch.object(auth, "_fetch_json", return_value=payload), \
                mock.patch("urllib.request.urlopen", return_value=fake_resp) as head_spy:
            streams = auth.get_live_streams(target_id="cam2-6")
        self.assertEqual(len(streams), 3)
        self.assertEqual(head_spy.call_count, 1)
        self.assertEqual(auth._stream_hosts.get("cam2-6"), "streams-b.fishtank.live")


if __name__ == "__main__":
    unittest.main()
