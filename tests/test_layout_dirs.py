"""Linux reuses an existing recordings folder whose name differs only by case.

Windows keeps the spelled path (``Processed``). These tests force both behaviours
so a case-insensitive volume and a case-sensitive one each prove their half.
Nothing here writes outside a temporary directory.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

from msr.util import channel_record_dirs, layout_bases, prefer_dir, recorded_scan_bases, reuse_dir, reuse_path


def _sensitive(folder: Path) -> bool:
    low, high = folder / "aa", folder / "Aa"
    low.mkdir()
    try:
        high.mkdir()
    except FileExistsError:
        return False
    try:
        return not os.path.samefile(low, high)
    finally:
        if high.exists() and not os.path.samefile(low, high):
            high.rmdir()
        low.rmdir()


class ReuseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "streams"
        self.root.mkdir()

    def test_windows_spelling_is_not_rewritten(self):
        (self.root / "processed").mkdir()
        got = reuse_dir(str(self.root), "Processed", reuse=False)
        self.assertEqual(got, os.path.join(str(self.root), "Processed"))
        recorded, processed, pending = channel_record_dirs(str(self.root), "kick", "alice", reuse=False)
        self.assertTrue(processed.endswith(os.path.join("Processed", "kick", "alice")))
        self.assertTrue(recorded.endswith(os.path.join("Recorded", "kick", "alice")))
        self.assertTrue(pending.endswith(os.path.join("PendingDeletion", "kick", "alice")))
        # On a case-insensitive volume "Processed" is the same directory as the
        # "processed" we just created, so existence cannot prove a sibling.
        # The listing still has exactly the name we made: reuse did not add one.
        spellings = [p.name for p in self.root.iterdir() if p.name.lower() == "processed"]
        self.assertEqual(spellings, ["processed"])

    def test_linux_reuses_the_existing_tree_and_does_not_create_a_sibling(self):
        if not _sensitive(self.root):
            self.skipTest("this volume cannot hold two spellings of one name")
        old = self.root / "processed" / "Kick" / "Alice"
        old.mkdir(parents=True)
        (old / "old.mp4").write_bytes(b"x" * 50)
        recorded, processed, pending = channel_record_dirs(str(self.root), "kick", "alice", reuse=True)
        self.assertEqual(processed, str(old))
        os.makedirs(recorded, exist_ok=True)
        os.makedirs(processed, exist_ok=True)
        os.makedirs(pending, exist_ok=True)
        self.assertFalse((self.root / "Processed").exists())
        self.assertTrue((self.root / "processed" / "Kick" / "Alice").is_dir())
        self.assertTrue((self.root / "Recorded" / "kick" / "alice").is_dir())
        self.assertTrue((self.root / "PendingDeletion").is_dir())
        # a channel folder that is not there yet keeps today's spelling, under the old top folder
        fresh = reuse_path(str(self.root / "processed"), "twitch", "newname", reuse=True)
        self.assertEqual(fresh, str(self.root / "processed" / "twitch" / "newname"))

    def test_when_both_spellings_exist_the_fuller_one_is_used(self):
        if not _sensitive(self.root):
            self.skipTest("this volume cannot hold two spellings of one name")
        low = self.root / "processed" / "kick" / "a"
        high = self.root / "Processed" / "kick" / "a"
        low.mkdir(parents=True)
        high.mkdir(parents=True)
        for i in range(6):
            (low / f"f{i}.mp4").write_bytes(b"x")
        (high / "only.mp4").write_bytes(b"x")
        self.assertEqual(os.path.basename(prefer_dir([str(low.parent.parent), str(high.parent.parent)], "Processed")),
                         "processed")
        self.assertEqual(reuse_dir(str(self.root), "Processed", reuse=True), str(self.root / "processed"))
        # the small capitalized tree is the one to use when it holds more
        for i in range(20):
            (high / f"more{i}.mp4").write_bytes(b"x")
        self.assertEqual(reuse_dir(str(self.root), "Processed", reuse=True), str(self.root / "Processed"))

    def test_recorded_scan_sees_every_variant_on_linux_only(self):
        (self.root / "Recorded").mkdir()
        one = recorded_scan_bases(str(self.root), os.path.join(str(self.root), "Recorded"))
        self.assertEqual(one, [os.path.join(str(self.root), "Recorded")])
        if not _sensitive(self.root):
            return
        (self.root / "recorded").mkdir()
        if not sys.platform.startswith("linux"):
            # recorded_scan_bases follows the real platform, not the reuse= flag
            self.assertEqual(len(recorded_scan_bases(str(self.root), str(self.root / "Recorded"))), 1)
            return
        found = recorded_scan_bases(str(self.root), str(self.root / "Recorded"))
        self.assertEqual({os.path.basename(p) for p in found}, {"Recorded", "recorded"})

    def test_layout_bases_prefer_what_is_already_there(self):
        if not sys.platform.startswith("linux"):
            bases = layout_bases(str(self.root))
            self.assertEqual(bases[1], os.path.join(str(self.root), "Processed"))
            return
        if not _sensitive(self.root):
            self.skipTest("this volume cannot hold two spellings of one name")
        (self.root / "processed").mkdir()
        (self.root / "recorded").mkdir()
        recorded, processed, pending = layout_bases(str(self.root))
        self.assertEqual(os.path.basename(recorded), "recorded")
        self.assertEqual(os.path.basename(processed), "processed")
        self.assertEqual(os.path.basename(pending), "PendingDeletion")


if __name__ == "__main__":
    unittest.main()
