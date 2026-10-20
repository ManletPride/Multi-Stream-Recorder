import os
import unittest
from unittest import mock

from msr import wintheme


class _Win:
    def __init__(self, frame):
        self._frame = frame

    def wm_frame(self):
        return self._frame

    def winfo_id(self):
        return 0

    def update_idletasks(self):
        pass


class TestWinTheme(unittest.TestCase):
    def test_frame_hwnd_parses_hex(self):
        self.assertEqual(wintheme.frame_hwnd(_Win("0x1a2b")), 0x1A2B)

    def test_noop_off_windows(self):
        with mock.patch.object(wintheme.os, "name", "posix"):
            self.assertFalse(wintheme.set_title_bar_dark(_Win("0x10"), True))
        self.assertFalse(wintheme.set_title_bar_dark(None, True))

    @unittest.skipUnless(os.name == "nt", "Windows only")
    def test_real_toplevel_does_not_raise(self):
        import tkinter as tk
        try:
            root = tk.Tk()
        except tk.TclError:
            self.skipTest("no display")
        try:
            top = tk.Toplevel(root)
            wintheme.set_title_bar_dark(top, True)
            wintheme.set_title_bar_dark(top, False)
            self.assertIn(top, wintheme.theme_toplevels(root, True))
        finally:
            root.destroy()


if __name__ == "__main__":
    unittest.main()
