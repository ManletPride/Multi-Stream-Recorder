"""Update check: certifi added to the trust store, real failure reasons, honest footer. No network."""
import os
import ssl
import sys
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from msr import depsupdate, tls  # noqa: E402


def cert_error():
    return urllib.error.URLError(ssl.SSLCertVerificationError(
        1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get issuer certificate"))


class TlsTests(unittest.TestCase):
    def test_context_verifies_and_loads_certifi(self):
        tls._CTX = None
        loaded = []
        real = ssl.SSLContext.load_verify_locations
        with mock.patch.object(ssl.SSLContext, "load_verify_locations",
                               lambda self, *a, **k: (loaded.append(a), real(self, *a, **k))[1]):
            ctx = tls.https_context()
        tls._CTX = None
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(ctx.check_hostname)
        try:
            import certifi
            self.assertIn((certifi.where(),), loaded)
        except ImportError:
            pass

    def test_default_get_json_passes_the_context(self):
        seen = {}

        def fake(req, timeout=None, context=None):
            seen["ctx"] = context
            raise cert_error()
        with mock.patch("urllib.request.urlopen", fake):
            with self.assertRaises(urllib.error.URLError):
                depsupdate._get_json("https://api.github.com/x")
        self.assertIs(seen["ctx"], tls.https_context())


class WhyTests(unittest.TestCase):
    def test_cert_error_is_named(self):
        self.assertIn("certificate is not trusted", depsupdate._why(cert_error()))

    def test_offline_shows_reason(self):
        w = depsupdate._why(urllib.error.URLError("getaddrinfo failed"))
        self.assertIn("getaddrinfo failed", w)

    def test_msr_row_carries_the_reason(self):
        def opener(req, timeout=None):
            if "api.github.com" in req.full_url:
                raise cert_error()
            raise AssertionError("no PyPI in this test")
        with mock.patch.object(depsupdate, "PACKAGES", ()):
            rs = depsupdate.rows(True, opener=opener)
        self.assertEqual(rs[0]["status"], "unknown")
        self.assertIn("certificate", rs[0]["note"])


class SummaryTests(unittest.TestCase):
    def test_failed_item_is_never_up_to_date(self):
        rs = [{"name": "msr", "label": "Multi-Stream Recorder", "status": "unknown"}, {"name": "yt-dlp", "label": "yt-dlp", "status": "up_to_date"}]
        self.assertEqual(depsupdate.summary_text(rs),
                         "Could not check Multi-Stream Recorder; other items are up to date.")
        rs.append({"name": "streamlink", "label": "streamlink", "status": "update_available"})
        self.assertIn("1 update(s) available for the rest", depsupdate.summary_text(rs))

    def test_all_fine(self):
        self.assertEqual(depsupdate.summary_text([{"name": "a", "label": "a", "status": "up_to_date"}]), "Everything is up to date.")


if __name__ == "__main__":
    unittest.main()
