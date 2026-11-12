"""Tooltips are one helper: themed, sized, and still withdrawn when they are shown.

The Linux flash was an override-redirect Toplevel mapped at Tk's default size
(~200px here, larger under XWayland) with the default grey background, then
resized. These tests snapshot the window at deiconify, before it is mapped.
"""
import ast
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
MSR = ROOT / "msr"


def _overrideredirect_true_lines(source: str) -> list[int]:
    """Lines that call overrideredirect(True) / wm_overrideredirect(True)."""
    found = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "overrideredirect" and node.func.attr != "wm_overrideredirect":
            continue
        if len(node.args) == 1 and isinstance(node.args[0], ast.Constant) and node.args[0].value is True:
            found.append(node.lineno)
    return found


class TooltipSourceTests(unittest.TestCase):
    def test_no_other_override_redirect_tooltip_copies(self):
        bad = []
        files = list(ROOT.glob("*.py")) + list(MSR.glob("*.py"))
        for path in files:
            if path.name == "tooltip.py":
                continue
            lines = _overrideredirect_true_lines(path.read_text(encoding="utf-8"))
            bad.extend(f"{path.relative_to(ROOT)}:{n}" for n in lines)
        self.assertEqual(bad, [])

    def test_the_helper_itself_marks_the_window_override_redirect(self):
        lines = _overrideredirect_true_lines((MSR / "tooltip.py").read_text(encoding="utf-8"))
        self.assertEqual(lines, [lines[0]] if lines else [])
        self.assertTrue(lines)

    def test_show_path_calls_update_idletasks_and_not_update(self):
        tree = ast.parse((MSR / "tooltip.py").read_text(encoding="utf-8"))
        idle, full = [], []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "update_idletasks":
                    idle.append(node.lineno)
                elif node.func.attr == "update":
                    full.append(node.lineno)
        self.assertTrue(idle)
        self.assertEqual(full, [])

    def test_options_panel_has_no_private_tip_class(self):
        src = (MSR / "options_panel.py").read_text(encoding="utf-8")
        self.assertNotIn("class _Tip", src)
        self.assertNotIn("class _Tooltip", (MSR / "gui.py").read_text(encoding="utf-8"))


class ClampTests(unittest.TestCase):
    def test_origin_stays_on_the_screen(self):
        from msr.tooltip import clamp_tooltip_origin
        self.assertEqual(clamp_tooltip_origin(10, 20, 80, 40, 800, 600), (10, 20))
        self.assertEqual(clamp_tooltip_origin(790, 20, 80, 40, 800, 600), (720, 20))
        self.assertEqual(clamp_tooltip_origin(10, 590, 80, 40, 800, 600), (10, 560))
        self.assertEqual(clamp_tooltip_origin(-30, -8, 80, 40, 800, 600), (0, 0))
        # Wider than the screen: pin to the origin rather than a negative place.
        self.assertEqual(clamp_tooltip_origin(50, 50, 900, 40, 800, 600), (50, 50))


class TooltipWindowTests(unittest.TestCase):
    def setUp(self):
        from tests import new_tk_root
        self.root = new_tk_root(self)
        self.addCleanup(self.root.destroy)
        from msr import themes
        themes.set_current(themes.DEFAULT_THEME)
        self.theme = themes.current()

    def _rgb(self, widget, color):
        return widget.winfo_rgb(color)

    def test_popup_is_themed_and_sized_before_it_is_shown(self):
        import tkinter as tk
        from msr.tooltip import popup
        seen = {}
        real = tk.Toplevel.deiconify

        def snap(win):
            seen["state"] = str(win.state())
            seen["bg"] = str(win.cget("bg"))
            seen["geom"] = str(win.wm_geometry())
            seen["req"] = (win.winfo_reqwidth(), win.winfo_reqheight())
            if win.tk.call("tk", "windowingsystem") == "x11":
                kind = win.wm_attributes("-type")
                seen["type"] = kind[0] if isinstance(kind, tuple) else kind
            return real(win)

        host = tk.Label(self.root, text="host")
        with patch.object(tk.Toplevel, "deiconify", snap):
            tip = popup(host, "Versions of MSR\nand packages", 30, 40, size=8)
        self.addCleanup(tip.destroy)
        self.assertEqual(seen["state"], "withdrawn")
        self.assertEqual(self._rgb(tip, seen["bg"]), self._rgb(tip, self.theme["entry_bg"]))
        self.assertNotEqual(self._rgb(tip, seen["bg"]), self._rgb(tip, "#d9d9d9"))
        width, height = seen["req"]
        self.assertRegex(seen["geom"], rf"^{width}x{height}\+\d+\+\d+$")
        self.assertLess(width, 400, seen)   # the flash was the default window, not this label
        self.assertLess(height, 200, seen)
        if "type" in seen:
            self.assertEqual(seen["type"], "tooltip")
        # Mapped size matches what was committed before deiconify.
        tip.update_idletasks()
        tip.update()
        self.assertEqual((tip.winfo_width(), tip.winfo_height()), (width, height))

    def test_widget_tip_uses_the_same_window_and_hides(self):
        import tkinter as tk
        from tkinter import font as tkfont
        from msr.tooltip import Tooltip
        host = tk.Label(self.root, text="Updates…")
        host.pack()
        self.root.update_idletasks()
        tip = Tooltip(host, "check and install updates")
        tip._show()
        self.assertIsNotNone(tip.tip)
        self.assertEqual(str(tip.tip.state()), "normal")
        family = tkfont.Font(font=tip.tip.winfo_children()[0].cget("font")).actual()["family"]
        if sys.platform == "win32":
            self.assertEqual(family, "Segoe UI")
        else:
            self.assertNotEqual(family, "Segoe UI")
            # cget("family") is the configured name ("sans-serif"); actual() is the face Tk draws.
            expected = tkfont.nametofont("TkDefaultFont").actual()["family"]
            self.assertEqual(family, expected)
        tip._hide()
        self.assertIsNone(tip.tip)

    def test_empty_text_shows_nothing(self):
        import tkinter as tk
        from msr.tooltip import Tooltip, popup
        host = tk.Label(self.root, text="x")
        self.assertIsNone(popup(host, "", 0, 0))
        self.assertIsNone(popup(host, None, 0, 0))
        bound = Tooltip(host, lambda: "")
        bound._show()
        self.assertIsNone(bound.tip)

    def test_a_tip_near_the_corner_is_clamped_before_it_is_shown(self):
        import tkinter as tk
        from msr.tooltip import popup
        seen = {}
        real = tk.Toplevel.deiconify

        def snap(win):
            seen["geom"] = str(win.wm_geometry())
            seen["screen"] = (win.winfo_screenwidth(), win.winfo_screenheight())
            return real(win)

        host = tk.Label(self.root, text="host")
        with patch.object(tk.Toplevel, "deiconify", snap):
            tip = popup(host, "edge", 100000, 100000)
        self.addCleanup(tip.destroy)
        width, height, x, y = _parse_geometry(seen["geom"])
        sw, sh = seen["screen"]
        self.assertLessEqual(x + width, sw)
        self.assertLessEqual(y + height, sh)
        self.assertGreaterEqual(x, 0)
        self.assertGreaterEqual(y, 0)

    def test_a_compositor_resize_snaps_back_and_then_stops(self):
        """Omarchy floating mode maps the tip, then forces about 1100x760.

        Generating that <Configure> must put the requested size back. Further
        resizes stop after RESIZE_CORRECTIONS so the handler cannot loop.
        """
        import tkinter as tk
        from msr import tooltip
        host = tk.Label(self.root, text="host")
        tip = tooltip.popup(host, "Versions of MSR", 40, 50, size=8)
        self.addCleanup(tip.destroy)
        tip.update_idletasks()
        tip.update()
        width, height = tip.winfo_reqwidth(), tip.winfo_reqheight()
        placed = str(tip.wm_geometry())
        self.assertTrue(placed.startswith(f"{width}x{height}+"), placed)

        applied = []
        real = tip.wm_geometry

        def wrapped(value=None):
            if value is not None:
                applied.append(value)
            return real(value)

        tip.wm_geometry = wrapped
        # The size we asked for is not a correction.
        tip.event_generate("<Configure>", width=width, height=height)
        self.assertEqual(applied, [])
        # A child Configure (the label) is not the compositor resizing the tip.
        tip.winfo_children()[0].event_generate("<Configure>", width=1100, height=760)
        self.assertEqual(applied, [])

        # The compositor's size, not a call the handler made.
        real("1100x760+200+180")
        tip.event_generate("<Configure>", width=1100, height=760, x=200, y=180)
        tip.update_idletasks()
        tip.update()
        self.assertGreaterEqual(len(applied), 1)
        self.assertLessEqual(len(applied), tooltip.RESIZE_CORRECTIONS)
        self.assertTrue(all(spec == placed for spec in applied), applied)
        self.assertEqual((tip.winfo_width(), tip.winfo_height()), (width, height))
        self.assertEqual(str(real()), placed)

        spent = len(applied)
        applied.clear()
        for _ in range(10):
            tip.event_generate("<Configure>", width=1100, height=760)
            # The correction is applied after <Configure> returns.
            tip.update()
        self.assertEqual(len(applied), tooltip.RESIZE_CORRECTIONS - spent)
        self.assertTrue(all(spec == placed for spec in applied))


def _parse_geometry(geom: str):
    size, _, place = geom.partition("+")
    width, height = size.split("x")
    x, y = place.split("+")
    return int(width), int(height), int(x), int(y)


if __name__ == "__main__":
    unittest.main()
