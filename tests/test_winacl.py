"""msr_control.json is limited to the current user on Windows (fail-soft). Temp folders only."""

import os
import sys
import tempfile
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import control, winacl  # noqa: E402


@unittest.skipUnless(os.name == "nt", "Windows ACLs")
class WinAclTests(unittest.TestCase):
    def test_control_file_has_one_ace_for_this_user(self):
        sid = winacl.current_user_sid()
        self.assertTrue(sid and sid.startswith("S-1-"))
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, control.CONTROL_FILE)
            control._write_private(path, b"{}")
            sddl = winacl.dacl_sddl(path)
            self.assertTrue(sddl.startswith("D:P"), sddl)    # protected: nothing inherited
            self.assertEqual(sddl.count("(A;"), 1, sddl)       # one allow entry ...
            # ... for this user; SDDL writes some SIDs as aliases (built-in Administrator, RID 500 = "LA",
            # the CI runner account)
            names = {sid} | ({"LA"} if sid.endswith("-500") else set())
            self.assertTrue(any(f";;;{n})" in sddl for n in names), (sid, sddl))
            with open(path, "rb") as fh:
                self.assertEqual(fh.read(), b"{}")              # still readable by us
            control._write_private(path, b"{1}")                # rewrite works too
            self.assertTrue(winacl.dacl_sddl(path).startswith("D:P"))

    def test_status_file_is_left_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, control.STATUS_FILE)
            control._write_private(path, b"{}")
            self.assertFalse(winacl.dacl_sddl(path).startswith("D:P"))

    def test_fail_soft(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(winacl, "current_user_sid", side_effect=OSError("no")):
            path = os.path.join(tmp, control.CONTROL_FILE)
            self.assertFalse(winacl.restrict_to_current_user(path + ".missing"))
            control._write_private(path, b"{}")
            with open(path, "rb") as fh:
                self.assertEqual(fh.read(), b"{}")


class NonWindowsTests(unittest.TestCase):
    def test_noop_off_windows(self):
        with mock.patch.object(winacl.os, "name", "posix"):
            self.assertFalse(winacl.restrict_to_current_user("x"))
            self.assertIsNone(winacl.current_user_sid())


if __name__ == "__main__":
    unittest.main()
