"""Menus and combobox lists snap back after a compositor forces 1100x760."""
import tkinter as tk
from tkinter import ttk
import unittest

from msr.popup_guard import RESIZE_CORRECTIONS, guard_menu, guard_popdown, popup_menu


def _root(case):
    from tests import new_tk_root
    root = new_tk_root(case)
    case.addCleanup(root.destroy)
    return root


class MenuGuardTests(unittest.TestCase):
    def test_a_posted_menu_returns_to_the_pointer_after_a_forced_resize(self):
        root = _root(self)
        if root.tk.call("tk", "windowingsystem") != "x11":
            menu = tk.Menu(root, tearoff=0)
            menu.add_command(label="Only")
            popup_menu(menu, 12, 12)
            self.assertEqual(menu.bind("<Configure>"), "")
            menu.unpost()
            menu.grab_release()
            return
        menu = tk.Menu(root, tearoff=0)
        for i in range(6):
            menu.add_command(label=f"Item {i}")
        self.addCleanup(lambda: (menu.unpost(), menu.grab_release()))
        popup_menu(menu, 100, 80)
        root.update_idletasks()
        root.update()
        req_w, req_h = menu.winfo_reqwidth(), menu.winfo_reqheight()
        good = f"{req_w}x{req_h}+100+80"
        self.assertEqual(menu.winfo_geometry(), good)
        for _ in range(RESIZE_CORRECTIONS):
            menu.tk.call("wm", "geometry", menu, "1100x760+400+300")
            root.update_idletasks()
            root.update()
            self.assertEqual(menu.winfo_geometry(), good)
        menu.tk.call("wm", "geometry", menu, "1100x760+400+300")
        root.update_idletasks()
        root.update()
        self.assertTrue(str(menu.winfo_geometry()).startswith("1100x760"), menu.winfo_geometry())

    def test_a_cascade_keeps_the_place_tk_posted(self):
        root = _root(self)
        if root.tk.call("tk", "windowingsystem") != "x11":
            self.skipTest("cascade guard is X11 only")
        parent = tk.Menu(root, tearoff=0)
        cascade = tk.Menu(parent, tearoff=0)
        guard_menu(cascade)
        for i in range(4):
            cascade.add_command(label=f"Sort {i}")
        self.addCleanup(lambda: (cascade.unpost(), cascade.grab_release()))
        # Tk posts a cascade itself; this is that post, without a remembered pointer.
        cascade.post(220, 90)
        root.update_idletasks()
        root.update()
        good = cascade.winfo_geometry()
        self.assertTrue(good.endswith("+220+90"), good)
        cascade.tk.call("wm", "geometry", cascade, "1100x760+400+300")
        root.update_idletasks()
        root.update()
        self.assertEqual(cascade.winfo_geometry(), good)


class PopdownGuardTests(unittest.TestCase):
    def test_a_combobox_popdown_returns_after_a_forced_resize(self):
        root = _root(self)
        if root.tk.call("tk", "windowingsystem") != "x11":
            self.skipTest("popdown guard is X11 only")
        root.deiconify()
        root.geometry("420x280+60+60")
        combo = ttk.Combobox(root, values=["alpha", "beta", "gamma", "delta", "epsilon"])
        combo.pack(padx=30, pady=40)
        root.update_idletasks()
        root.update()
        guard_popdown(combo)
        combo.tk.call("ttk::combobox::Post", combo._w)
        root.update_idletasks()
        root.update()
        pop = str(combo.tk.call("ttk::combobox::PopdownWindow", combo._w))
        good = str(root.tk.call("winfo", "geometry", pop))
        self.assertFalse(good.startswith("1100x760"), good)
        self.addCleanup(lambda: combo.tk.call("ttk::combobox::Unpost", combo._w))
        for _ in range(RESIZE_CORRECTIONS):
            root.tk.call("wm", "geometry", pop, "1100x760+800+500")
            root.update_idletasks()
            root.update()
            self.assertEqual(str(root.tk.call("winfo", "geometry", pop)), good)
        root.tk.call("wm", "geometry", pop, "1100x760+800+500")
        root.update_idletasks()
        root.update()
        self.assertTrue(str(root.tk.call("winfo", "geometry", pop)).startswith("1100x760"))


if __name__ == "__main__":
    unittest.main()
