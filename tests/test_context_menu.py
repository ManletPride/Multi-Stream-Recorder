"""Right-click menus open on button release under X11, and on press elsewhere."""
import tkinter as tk
import unittest
from unittest.mock import patch

from msr.context_menu import bind_context_menu, context_sequence


class SequenceTests(unittest.TestCase):
    def test_x11_waits_for_the_release(self):
        self.assertEqual(context_sequence("x11"), "<ButtonRelease-3>")

    def test_win32_and_aqua_stay_on_press(self):
        self.assertEqual(context_sequence("win32"), "<Button-3>")
        self.assertEqual(context_sequence("aqua"), "<Button-3>")
        self.assertEqual(context_sequence(""), "<Button-3>")


class BindTests(unittest.TestCase):
    def setUp(self):
        from tests import new_tk_root
        self.root = new_tk_root(self)
        self.addCleanup(self.root.destroy)

    def _bind(self, system):
        widget = tk.Frame(self.root)
        with patch("msr.context_menu._windowingsystem", return_value=system):
            bound = bind_context_menu(widget, lambda _event: None)
        return widget, bound

    def test_helper_binds_button_release_on_x11(self):
        widget, bound = self._bind("x11")
        self.assertEqual(bound, "<ButtonRelease-3>")
        self.assertNotEqual(widget.bind("<ButtonRelease-3>"), "")
        self.assertEqual(widget.bind("<Button-3>"), "")

    def test_helper_binds_button_3_on_win32(self):
        widget, bound = self._bind("win32")
        self.assertEqual(bound, "<Button-3>")
        self.assertNotEqual(widget.bind("<Button-3>"), "")
        self.assertEqual(widget.bind("<ButtonRelease-3>"), "")


if __name__ == "__main__":
    unittest.main()
