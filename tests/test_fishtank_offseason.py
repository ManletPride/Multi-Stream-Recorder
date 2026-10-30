"""Fishtank off-season behaviour: listed-but-offline cameras, an empty catalog, and no login."""

import json
import logging
import os
import sys
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import msr.platforms as platforms  # noqa: E402


class ListLogger(logging.Logger):
    def __init__(self):
        super().__init__("fishtank-test", logging.DEBUG)
        self.records = []

    def handle(self, record):
        self.records.append(record)

    def errors(self):
        return [r for r in self.records if r.levelno >= logging.ERROR]


def make_auth(catalog_json, logger):
    """A real FishtankAuth with the network replaced: the catalog reply is canned and
    every login attempt fails."""
    auth = platforms.FishtankAuth(None, logger)
    auth._fetch_json = lambda url: catalog_json
    return auth


def catalog(ids, online=()):
    return json.dumps({"liveStreams": [{"id": i, "name": i.title()} for i in ids],
                       "liveStreamStatus": {i: "online" for i in online},
                       "loadBalancer": {}})


class ListedOfflineTests(unittest.TestCase):
    def test_listed_camera_not_online_is_offline_without_probe_or_login(self):
        log = ListLogger()
        auth = make_auth(catalog(["bar-6", "dirc-6"]), log)
        with mock.patch("urllib.request.urlopen") as urlopen, \
                mock.patch.object(auth, "get_jwt") as get_jwt:
            live, _name, error = platforms.check_stream_fishtank("bar-6", auth, log)
        self.assertFalse(live)
        self.assertIsNone(error)
        urlopen.assert_not_called()
        get_jwt.assert_not_called()

    def test_director_ids_are_still_probed(self):
        log = ListLogger()
        auth = make_auth(catalog(["bar-6", "dirc-6"]), log)
        err = urllib.error.HTTPError("https://example.invalid", 404, "x", {}, None)
        with mock.patch("urllib.request.urlopen", side_effect=err) as urlopen, \
                mock.patch.object(auth, "get_jwt", return_value="tok"):
            live, _name, error = platforms.check_stream_fishtank("dirc-6", auth, log)
        self.assertFalse(live)
        self.assertIsNone(error)
        urlopen.assert_called_once()

    def test_listed_online_camera_is_live(self):
        log = ListLogger()
        auth = make_auth(catalog(["bar-6"], online=["bar-6"]), log)
        with mock.patch("urllib.request.urlopen", side_effect=OSError("offline test")), \
                mock.patch.object(auth, "get_jwt", return_value="tok"):
            live, _name, error = platforms.check_stream_fishtank("bar-6", auth, log)
        self.assertTrue(live)
        self.assertIsNone(error)


class EmptyCatalogTests(unittest.TestCase):
    def test_empty_catalog_is_offline_with_between_seasons_detail(self):
        log = ListLogger()
        auth = make_auth(catalog([]), log)
        live, _name, error = platforms.check_stream_fishtank("director", auth, log)
        self.assertFalse(live)
        self.assertIsNone(error)
        self.assertEqual(auth.last_detail, "no Fishtank cameras listed (between seasons)")

    def test_unreachable_api_is_still_an_error(self):
        log = ListLogger()
        auth = make_auth(None, log)
        live, _name, error = platforms.check_stream_fishtank("director", auth, log)
        self.assertFalse(live)
        self.assertEqual(error, "failed to reach fishtank API")


class NoLoginTests(unittest.TestCase):
    def test_probe_without_login_is_offline_not_error(self):
        log = ListLogger()
        auth = make_auth(catalog(["bar-6"]), log)
        with mock.patch.object(auth, "get_jwt", return_value=None):
            live, _name, error = platforms.check_stream_fishtank("dirc-6", auth, log)
        self.assertFalse(live)
        self.assertIsNone(error)
        self.assertIn("login", auth.last_detail)

    def test_session_expired_error_is_logged_at_most_once_an_hour(self):
        log = ListLogger()
        auth = platforms.FishtankAuth(None, log)
        clock = [1000.0]
        with mock.patch.object(platforms.time, "monotonic", side_effect=lambda: clock[0]):
            for _ in range(5):
                auth._log_auth_failure("[fishtank] Authentication failed")
                clock[0] += 600
            self.assertEqual(len(log.errors()), 1)
            clock[0] += 3600
            auth._log_auth_failure("[fishtank] Authentication failed")
        self.assertEqual(len(log.errors()), 2)


if __name__ == "__main__":
    unittest.main()
