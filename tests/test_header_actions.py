"""Status-row buttons ignore Space. The channel list's Space binding still runs.

The window is shown: a key event on a withdrawn widget does not invoke a ttk
button, which would hide a regression.
"""
import tkinter as tk
import unittest
from tkinter import ttk

from tests import new_tk_root


class HeaderActionKeyTests(unittest.TestCase):
    def setUp(self):
        self.root = new_tk_root(self)
        self.root.geometry("420x280+0+0")
        self.root.deiconify()
        self.hits = []
        from msr.gui import _HEADER_ACTION, _header_action_button
        self.tag = _HEADER_ACTION
        self.button = _header_action_button
        self.root.update()

    def tearDown(self):
        self.root.destroy()

    def _space(self, widget):
        widget.focus_force()
        self.root.update()
        self.assertIs(self.root.focus_get(), widget)
        widget.event_generate("<Key-space>")
        widget.event_generate("<KeyRelease-space>")
        self.root.update()

    def test_space_on_dvr_and_clip_now_does_not_run_them(self):
        dvr = self.button(self.root, text="DVR", command=lambda: self.hits.append("dvr"))
        clip = self.button(self.root, text="Clip Now", style="Accent.TButton",
                           command=lambda: self.hits.append("clip"))
        plain = ttk.Button(self.root, text="plain", command=lambda: self.hits.append("plain"))
        for b in (dvr, clip, plain):
            b.pack()
        self.root.update()
        self.assertEqual(str(dvr.cget("takefocus")), "0")
        self.assertEqual(str(clip.cget("takefocus")), "0")
        self.assertIn(self.tag, dvr.bindtags())
        self.assertIn(self.tag, clip.bindtags())
        self._space(dvr)
        self._space(clip)
        self.assertEqual(self.hits, [])
        self._space(plain)
        self.assertEqual(self.hits, ["plain"])          # the same event still activates a normal button
        dvr.invoke()
        clip.event_generate("<Button-1>", x=4, y=4)
        clip.event_generate("<ButtonRelease-1>", x=4, y=4)
        self.root.update()
        self.assertEqual(self.hits, ["plain", "dvr", "clip"])

    def test_channel_list_space_still_runs(self):
        tree = ttk.Treeview(self.root, height=4)
        tree.pack()
        tree.insert("", "end", text="alice")
        tree.bind("<space>", lambda e: (self.hits.append("tree"), "break")[1])
        dvr = self.button(self.root, text="DVR", command=lambda: self.hits.append("dvr"))
        dvr.pack()
        self.root.update()
        self._space(tree)
        self._space(dvr)
        self.assertEqual(self.hits, ["tree"])
