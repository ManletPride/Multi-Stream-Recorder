"""A preview frame must come from this grab, never from a PNG an earlier grab or stream left behind.

Bug: ffmpeg exits 0 and writes nothing when the live tail has no decodable frame (a 3 s window
without a key frame). create_preview_frame() then saw the old PNG at the same path, called it
"wrote", and never tried its later seeks, so the large preview showed the previous stream's frame
forever (and the strip tile froze on its last good frame)."""
import logging
import os
import subprocess
import tempfile
import unittest
from unittest import mock

from msr import worker

PNG = b"\x89PNG\r\n\x1a\n" + b"x" * 400


class FakeRun:
    """Stands in for worker._hidden_run: each call either writes the output file or writes nothing (rc 0)."""

    def __init__(self, writes):
        self.writes = list(writes)  # one entry per ffmpeg call: bytes to write, or None for "no frame"
        self.cmds = []

    def __call__(self, cmd, timeout):
        self.cmds.append(cmd)
        data = self.writes.pop(0) if self.writes else None
        if data is not None:
            with open(cmd[-1], "wb") as f:
                f.write(data)
        return subprocess.CompletedProcess(cmd, 0, "", "")


class PreviewFrameStaleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.raw = os.path.join(self.tmp.name, "live.ts")
        with open(self.raw, "wb") as f:
            f.write(b"\0" * (128 * 1024))
        self.out = os.path.join(self.tmp.name, "chan_big.png")
        with open(self.out, "wb") as f:
            f.write(PNG.replace(b"x", b"o"))  # the previous stream's frame

    def grab(self, writes):
        fake = FakeRun(writes)
        with mock.patch.object(worker, "_hidden_run", fake), mock.patch.object(worker, "_probe_duration", return_value=None):
            result = worker.create_preview_frame(self.raw, self.out, "ffmpeg", logging.getLogger("t"), width=480, height=270)
        return result, fake

    def test_an_old_png_is_not_a_success_when_ffmpeg_wrote_nothing(self):
        (ok, _msg), fake = self.grab([None, None])  # both -sseof tries decode no frame; the -ss try is skipped (no duration)
        self.assertFalse(ok)
        self.assertEqual(len(fake.cmds), 2)
        self.assertFalse(os.path.exists(self.out), "the old frame must not be left to be shown")

    def test_a_later_seek_is_tried_and_its_frame_is_used(self):
        fresh = PNG.replace(b"x", b"n")
        (ok, _msg), fake = self.grab([None, fresh])  # -3 s: no frame, -6 s: a frame
        self.assertTrue(ok)
        self.assertEqual(len(fake.cmds), 2)
        with open(self.out, "rb") as f:
            self.assertEqual(f.read(), fresh)

    def test_the_first_seek_still_wins_when_it_writes_a_frame(self):
        fresh = PNG.replace(b"x", b"n")
        (ok, _msg), fake = self.grab([fresh])
        self.assertTrue(ok)
        self.assertEqual(len(fake.cmds), 1)
        with open(self.out, "rb") as f:
            self.assertEqual(f.read(), fresh)


if __name__ == "__main__":
    unittest.main()
