"""Size-split remux: clips stay enabled and status detail shows remux progress."""
import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.worker import (
    _remux_detail_suffix,
    _strip_remux_suffix,
    _with_remux_suffix,
    channel_status_allows_clip,
    find_active_recording_file,
    wait_until_file_has_data,
)


class ChannelStatusAllowsClipTests(unittest.TestCase):
    def test_recording(self):
        self.assertTrue(channel_status_allows_clip("Recording"))

    def test_recording_case_insensitive(self):
        self.assertTrue(channel_status_allows_clip("RECORDING"))

    def test_remuxing(self):
        self.assertTrue(channel_status_allows_clip("Remuxing..."))

    def test_offline_rejected(self):
        self.assertFalse(channel_status_allows_clip("Offline"))

    def test_completed_rejected(self):
        self.assertFalse(channel_status_allows_clip("Completed"))

    def test_checking_rejected(self):
        self.assertFalse(channel_status_allows_clip("Checking..."))

    def test_remux_failed_rejected(self):
        # "Remux failed" contains "remux" but not "remuxing"
        self.assertFalse(channel_status_allows_clip("Remux failed"))

    def test_empty_rejected(self):
        self.assertFalse(channel_status_allows_clip(""))
        self.assertFalse(channel_status_allows_clip(None))


class RemuxDetailSuffixTests(unittest.TestCase):
    def test_inactive_is_empty(self):
        self.assertEqual(_remux_detail_suffix(None), "")
        self.assertEqual(_remux_detail_suffix({"active": 0}), "")

    def test_active_without_pct(self):
        self.assertEqual(_remux_detail_suffix({"active": 1, "pct": None}), "remuxing…")

    def test_active_with_pct(self):
        self.assertEqual(_remux_detail_suffix({"active": 1, "pct": 67.4}), "remuxing 67%")

    def test_compose_and_strip(self):
        state = {"active": 1, "pct": 50}
        composed = _with_remux_suffix("1920x1080 · 30fps", state)
        self.assertEqual(composed, "1920x1080 · 30fps · remuxing 50%")
        self.assertEqual(_strip_remux_suffix(composed), "1920x1080 · 30fps")
        # Re-compose must not double-append
        again = _with_remux_suffix(composed, {"active": 1, "pct": 80})
        self.assertEqual(again, "1920x1080 · 30fps · remuxing 80%")

    def test_compose_clears_when_idle(self):
        composed = _with_remux_suffix("starting · remuxing 10%", {"active": 0})
        self.assertEqual(composed, "starting")


class WaitUntilFileHasDataTests(unittest.TestCase):
    def test_true_once_file_reaches_min_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "next.ts")

            def _write():
                time.sleep(0.15)
                with open(path, "wb") as f:
                    f.write(b"x" * 70000)

            t = threading.Thread(target=_write)
            t.start()
            self.assertTrue(
                wait_until_file_has_data(path, min_bytes=65536, timeout=2, poll=0.05)
            )
            t.join()

    def test_timeout_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "never.ts")
            self.assertFalse(
                wait_until_file_has_data(path, min_bytes=1, timeout=0.2, poll=0.05)
            )

    def test_stop_event_aborts(self):
        stop = threading.Event()
        stop.set()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "x.ts")
            with open(path, "wb") as f:
                f.write(b"x" * 70000)
            self.assertFalse(
                wait_until_file_has_data(
                    path, stop_event=stop, min_bytes=1, timeout=2, poll=0.05
                )
            )


class FindActiveRecordingFileTests(unittest.TestCase):
    def test_picks_newest_ts_when_two_exist(self):
        # After an 8 GB split the closed file is still in Recorded/ while
        # the new capture grows. Clip Now must cut from the growing one.
        with tempfile.TemporaryDirectory() as tmp:
            channel_dir = os.path.join(tmp, "kick", "kimmee")
            os.makedirs(channel_dir)
            old_ts = os.path.join(channel_dir, "kimmee_old.ts")
            new_ts = os.path.join(channel_dir, "kimmee_new.ts")
            with open(old_ts, "wb") as f:
                f.write(b"old")
            os.utime(old_ts, (1_000_000, 1_000_000))
            with open(new_ts, "wb") as f:
                f.write(b"new")
            os.utime(new_ts, (2_000_000, 2_000_000))

            class _Rec:
                recorded_base = tmp

            found = find_active_recording_file(_Rec(), "kimmee")
            self.assertEqual(found, new_ts)


if __name__ == "__main__":
    unittest.main()
