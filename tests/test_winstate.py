"""Window size/place memory, safe main-window geometry, and drag-by-background."""
import inspect
import json
import tempfile
import tkinter as tk
import unittest
from pathlib import Path
from tkinter import ttk
from unittest import mock

from msr import dialogs, winstate, wintheme

AREA = (0, 0, 1920, 1040)


class GeometryTests(unittest.TestCase):
    def test_parse_and_format_round_trip_including_negative_places(self):
        self.assertEqual(winstate.parse_geometry("1060x772+2554+654"), (1060, 772, 2554, 654))
        self.assertEqual(winstate.parse_geometry("300x200+-1920+10"), (300, 200, -1920, 10))
        self.assertEqual(winstate.parse_geometry("300x200"), (300, 200, None, None))
        self.assertEqual(winstate.parse_geometry(" 300x200-10-20 "), (300, 200, -10, -20))
        for junk in ("", None, "abc", "300x", "x200+1+1", "300x200+1"):
            self.assertIsNone(winstate.parse_geometry(junk), junk)
        self.assertEqual(winstate.format_geometry(300, 200, -1920, 10), "300x200+-1920+10")  # not -1920: that means "from the right"
        self.assertEqual(winstate.format_geometry(300, 200), "300x200")

    def test_clamp_keeps_a_good_geometry_as_it_is(self):
        self.assertEqual(winstate.clamp_geometry("1060x772+100+50", (1060, 720), AREA), "1060x772+100+50")

    def test_a_window_saved_on_a_monitor_that_is_gone_comes_back_on_screen(self):
        got = winstate.clamp_geometry("1060x772+2554+654", (1060, 720), AREA)
        x, y = winstate.parse_geometry(got)[2:]
        self.assertLessEqual(x, 1920 - winstate.KEEP_VISIBLE)
        self.assertGreaterEqual(x, 0)
        self.assertEqual(y, 654)
        left = winstate.clamp_geometry("800x600+-5000+10", (400, 300), AREA)
        self.assertEqual(winstate.parse_geometry(left)[2], 0 - 800 + winstate.KEEP_VISIBLE)
        self.assertEqual(winstate.parse_geometry(winstate.clamp_geometry("800x600+100+5000", (400, 300), AREA))[3],
                         1040 - winstate.KEEP_VISIBLE)
        self.assertEqual(winstate.parse_geometry(winstate.clamp_geometry("800x600+100+-300", (400, 300), AREA))[3], 0)

    def test_second_monitor_to_the_left_is_respected(self):
        area = (-1920, 0, 0, 1040)
        self.assertEqual(winstate.clamp_geometry("800x600+-1500+100", (400, 300), area), "800x600+-1500+100")

    def test_size_is_kept_between_the_minimum_and_the_monitor(self):
        self.assertEqual(winstate.clamp_geometry("200x100+10+10", (1060, 720), AREA), "1060x720+10+10")
        self.assertEqual(winstate.clamp_geometry("5000x4000+0+0", (1060, 720), AREA), "1920x1040+0+0")
        self.assertEqual(winstate.clamp_geometry("200x100", (400, 300), AREA), "400x300")  # size only stays size only
        self.assertIsNone(winstate.clamp_geometry("nonsense", (400, 300), AREA))


class StateFileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.file = str(Path(self.tmp.name) / "window_state.json")

    def test_merge_keeps_other_keys_and_the_dialog_map(self):
        Path(self.file).write_text(json.dumps({"geometry": "1x1", "dark_mode": True, "dialogs": {"a": "1x1+0+0"}}))
        winstate.merge(self.file, {"geometry": "2x2"})
        winstate.merge(self.file, {}, dialog=("b", "3x3+1+1"))
        data = json.loads(Path(self.file).read_text())
        self.assertEqual(data["geometry"], "2x2")
        self.assertTrue(data["dark_mode"])
        self.assertEqual(data["dialogs"], {"a": "1x1+0+0", "b": "3x3+1+1"})
        self.assertEqual([p.name for p in Path(self.tmp.name).iterdir()], ["window_state.json"])  # no .tmp left

    def test_missing_corrupt_or_odd_files_are_fine(self):
        self.assertEqual(winstate.load(self.file), {})
        Path(self.file).write_text("{broken")
        self.assertEqual(winstate.load(self.file), {})
        Path(self.file).write_text("[1, 2]")
        self.assertEqual(winstate.load(self.file), {})
        winstate.merge(self.file, {"x": 1})  # replaces the broken file
        self.assertEqual(winstate.load(self.file), {"x": 1})
        winstate.merge(str(Path(self.tmp.name) / "no" / "such" / "dir.json"), {"x": 1})  # never raises


class TkCase(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.file = str(Path(self.tmp.name) / "window_state.json")
        for patcher in (
            mock.patch.object(dialogs, "_reveal", lambda win, on_top: None),
            mock.patch.object(dialogs.wintheme, "set_title_bar_dark", mock.Mock(return_value=True)),
            mock.patch.object(dialogs, "work_area", lambda win, x=0, y=0: AREA),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)


class MainWindowTests(TkCase):
    def test_fit_main_geometry(self):
        self.assertEqual(winstate.fit_main_geometry(self.root, "1060x772+2554+654", "1100x760", (1060, 720))[:9], "1060x772+")
        x = winstate.parse_geometry(winstate.fit_main_geometry(self.root, "1060x772+2554+654", "1100x760", (1060, 720)))[2]
        self.assertLessEqual(x, 1920 - winstate.KEEP_VISIBLE)
        self.assertEqual(winstate.fit_main_geometry(self.root, None, "1100x760", (1060, 720)), "1100x760")
        self.assertEqual(winstate.fit_main_geometry(self.root, "junk", "1100x760", (1060, 720)), "1100x760")


class DialogMemoryTests(TkCase):
    def make(self):
        win = tk.Toplevel(self.root)
        win.withdraw()
        ttk.Label(win, text="x" * 40).pack()
        return win

    def test_saved_size_and_place_are_applied_and_used_instead_of_centring(self):
        winstate.merge(self.file, {}, dialog=("restore_channels", "700x500+-3000+40"))
        win = self.make()
        self.assertTrue(winstate.attach(win, self.file, "restore_channels", (300, 200)))
        self.assertEqual(win._msr_geometry, "700x500+-620+40")  # clamped back so 80 px stay visible
        dialogs.center_over(win, self.root)
        self.assertEqual(win._msr_place, "700x500+-620+40")

    def test_a_size_only_memory_still_centres(self):
        winstate.merge(self.file, {}, dialog=("k", "640x400"))
        win = self.make()
        winstate.attach(win, self.file, "k", (300, 200))
        dialogs.center_over(win, self.root)
        self.assertTrue(win._msr_place.startswith("640x400+"))

    def test_no_memory_means_not_applied(self):
        win = self.make()
        self.assertFalse(winstate.attach(win, self.file, "never_saved", (300, 200)))
        self.assertFalse(hasattr(win, "_msr_geometry"))

    def test_place_is_saved_when_the_dialog_closes_and_other_keys_survive(self):
        winstate.merge(self.file, {"dark_mode": True})
        win = self.make()
        winstate.attach(win, self.file, "k", (300, 200))
        win.state = lambda *a: "normal"  # the test window is never shown
        win.winfo_geometry = lambda: "820x610+-1900+30"
        win.event_generate("<Configure>")
        win.update()
        win.destroy()
        data = json.loads(Path(self.file).read_text())
        self.assertEqual(data["dialogs"]["k"], "820x610+-1900+30")
        self.assertTrue(data["dark_mode"])

    def test_nothing_is_saved_for_a_window_that_never_moved_or_was_maximised(self):
        win = self.make()
        winstate.attach(win, self.file, "k", (300, 200))
        win.destroy()
        self.assertFalse(Path(self.file).exists())
        win = self.make()
        winstate.attach(win, self.file, "k", (300, 200))
        win.state = lambda *a: "zoomed"
        win.event_generate("<Configure>")
        win.update()
        win.destroy()
        self.assertFalse(Path(self.file).exists())

    def test_restore_window_remembers_its_place(self):
        from msr import history
        from msr.restore_dialog import RestoreWindow
        f = Path(self.tmp.name) / "channels.json"
        f.write_text("[1]")
        history.snapshot(f)
        f.write_text("[2]")
        winstate.merge(self.file, {}, dialog=("restore_channels", "800x420+100+100"))
        w = RestoreWindow(self.root, f, "Restore", state_file=self.file)
        self.assertEqual(w.win._msr_geometry, "800x420+100+100")


class DragTests(TkCase):
    def test_only_plain_background_counts(self):
        root = self.root
        photo = tk.PhotoImage(width=2, height=2)
        clicked = tk.Label(root, text="x")
        clicked.bind("<Button-1>", lambda e: None)
        cases = {
            "frame": (tk.Frame(root), True),
            "ttk frame": (ttk.Frame(root), True),
            "labelframe": (tk.LabelFrame(root), True),
            "label": (tk.Label(root, text="x"), True),
            "ttk label": (ttk.Label(root, text="x"), True),
            "separator": (ttk.Separator(root), True),
            "toplevel": (tk.Toplevel(root), True),
            "image label": (tk.Label(root, image=photo), False),
            "clickable label": (clicked, False),
            "hand label": (tk.Label(root, text="x", cursor="hand2"), False),
            "button": (tk.Button(root), False),
            "ttk button": (ttk.Button(root), False),
            "entry": (tk.Entry(root), False),
            "combobox": (ttk.Combobox(root), False),
            "canvas": (tk.Canvas(root), False),
            "text": (tk.Text(root), False),
            "listbox": (tk.Listbox(root), False),
            "tree": (ttk.Treeview(root), False),
            "notebook": (ttk.Notebook(root), False),
            "paned": (tk.PanedWindow(root), False),
            "scale": (ttk.Scale(root), False),
            "scrollbar": (ttk.Scrollbar(root), False),
            "check": (ttk.Checkbutton(root), False),
            "menubutton": (ttk.Menubutton(root), False),
        }
        for name, (widget, expected) in cases.items():
            self.assertEqual(wintheme.is_drag_background(widget), expected, name)

    def test_press_on_background_starts_a_caption_drag_and_on_a_control_does_not(self):
        top = tk.Toplevel(self.root)
        top.withdraw()
        frame, button = tk.Frame(top), tk.Button(top)
        wintheme.enable_background_drag(top)
        wintheme.enable_background_drag(top)  # idempotent
        Event = lambda w: type("E", (), {"widget": w, "x_root": -50, "y_root": 20})()
        with mock.patch.object(wintheme, "_begin_caption_drag", return_value=True) as begin:
            self.assertEqual(top._msr_drag_press(Event(frame)), "break")
            begin.assert_called_once_with(top, -50, 20)
            begin.reset_mock()
            self.assertIsNone(top._msr_drag_press(Event(button)))
            begin.assert_not_called()
        with mock.patch.object(wintheme, "_begin_caption_drag", return_value=False):
            self.assertIsNone(top._msr_drag_press(Event(frame)))  # not Windows: the click goes on

    def test_a_maximised_window_is_not_dragged(self):
        top = tk.Toplevel(self.root)
        top.withdraw()
        wintheme.enable_background_drag(top)
        top.state = lambda *a: "zoomed"
        with mock.patch.object(wintheme, "_begin_caption_drag", return_value=True) as begin:
            self.assertIsNone(top._msr_drag_press(type("E", (), {"widget": top, "x_root": 1, "y_root": 1})()))
            begin.assert_not_called()

    def test_every_dialog_and_the_main_window_get_the_drag(self):
        win, _frame = dialogs.new_dialog(self.root, "t")
        self.assertTrue(win._msr_drag)
        gui = (Path(__file__).resolve().parent.parent / "msr" / "gui.py").read_text(encoding="utf-8")
        self.assertIn("_wintheme.enable_background_drag(root)", gui)


class DragMessageTests(unittest.TestCase):
    def test_lparam_packs_the_screen_position_in_16_bit_halves(self):
        self.assertEqual(wintheme.caption_lparam(100, 50), (50 << 16) | 100)
        self.assertEqual(wintheme.caption_lparam(-100, 50) & 0xFFFF, 0xFF9C)  # a monitor to the left
        self.assertEqual(wintheme.caption_lparam(5, -1) >> 16, 0xFFFF)

    def test_it_posts_the_message_and_never_sends_it(self):
        import ast
        import textwrap
        source = textwrap.dedent(inspect.getsource(wintheme._begin_caption_drag))
        called = {n.func.attr for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Call)
                  and isinstance(n.func, ast.Attribute)}
        self.assertIn("PostMessageW", called)
        self.assertNotIn("SendMessageW", called)
        self.assertNotIn("SendMessage", called)
        self.assertIn("WM_NCLBUTTONDOWN", source)
        self.assertEqual(wintheme.HTCAPTION, 2)
        self.assertEqual(wintheme.WM_NCLBUTTONDOWN, 0xA1)

    def test_it_does_nothing_off_windows(self):
        with mock.patch.object(wintheme.os, "name", "posix"):
            self.assertFalse(wintheme._begin_caption_drag(object(), 1, 1))


if __name__ == "__main__":
    unittest.main()