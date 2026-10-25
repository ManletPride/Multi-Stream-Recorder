"""The Theme... window, driven without showing it: nothing takes focus."""
import json
import tempfile
import tkinter as tk
import unittest
from pathlib import Path
from tkinter import ttk
from unittest import mock

from msr import dialogs, theme_dialog, themeui, themes


class ThemeDialogTests(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self._cleanup)
        self.addCleanup(themes.set_current, themes.current())
        self.addCleanup(themes.register_user_themes, [])
        themes.set_current(themes.THEMES[themes.DEFAULT_THEME])
        patcher = mock.patch.object(dialogs.wintheme, "set_title_bar_dark", mock.Mock(return_value=True))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.applied, self.saved = [], []
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = str(Path(self.tmp.name) / "themes")

    def _cleanup(self):
        try:
            self.root.destroy()
        except tk.TclError:
            pass

    def apply(self, theme):
        themes.set_current(theme)
        self.applied.append(theme.name)

    def make(self, **kw):
        opts = dict(apply=self.apply, save=lambda t: self.saved.append(t.name), folder=self.folder)
        opts.update(kw)
        dlg = theme_dialog.ThemeDialog(self.root, **opts)
        self.addCleanup(dlg._close)
        return dlg

    def test_every_theme_can_be_picked_and_previewed(self):
        dlg = self.make()
        for name in themes.THEME_NAMES:
            dlg.pick(name)
            self.assertEqual(themes.current().name, name)
            self.assertGreater(len(dlg.canvas.find_all()), 20, name)  # the preview drew something
            self.assertIn("contrast", dlg.info.cget("text"))
        self.assertEqual(self.applied, list(themes.THEME_NAMES))
        self.assertEqual(self.saved, [])  # nothing is saved until OK

    def test_ok_saves_the_choice_once_and_closes(self):
        dlg = self.make()
        dlg.pick("Midnight Purple")
        dlg.ok()
        dlg.ok()
        self.assertEqual(self.saved, ["Midnight Purple"])
        self.assertFalse(dlg.win.winfo_exists())

    def test_ok_without_a_change_saves_nothing(self):
        dlg = self.make()
        dlg.ok()
        self.assertEqual(self.saved, [])

    def test_cancel_puts_the_old_theme_back(self):
        dlg = self.make()
        dlg.pick("Light")
        dlg.pick("Neutral Dark")
        self.assertEqual(themes.current().name, "Neutral Dark")
        dlg.cancel()
        self.assertEqual(themes.current().name, "PowerShell Blue")
        self.assertEqual(self.applied[-1], "PowerShell Blue")
        self.assertEqual(self.saved, [])

    def test_cancel_without_picking_applies_nothing(self):
        dlg = self.make()
        dlg.cancel()
        self.assertEqual(self.applied, [])

    def test_selecting_in_the_list_applies_live(self):
        dlg = self.make()
        iid = next(i for i, t in dlg.names.items() if t.name == "Paper")
        dlg.tree.selection_set(iid)
        self.root.update()
        self.assertEqual(themes.current().name, "Paper")

    def test_unknown_names_are_ignored(self):
        dlg = self.make()
        dlg.pick("not a theme")
        self.assertEqual(self.applied, [])

    def test_your_themes_group_and_the_reload_button(self):
        folder = Path(self.folder)
        folder.mkdir()
        (folder / "a.json").write_text(json.dumps({"name": "Teal", "colors": {"accent": "#2a9d8f"}}), encoding="utf-8")
        (folder / "bad.json").write_text("{", encoding="utf-8")
        state = {"problems": []}

        def reload():
            found, state["problems"] = themes.load_user_themes(self.folder)
            themes.register_user_themes(found)

        dlg = self.make(reload=reload, problems=lambda: state["problems"])
        self.assertNotIn("Teal", [t.name for t in dlg.names.values()])
        dlg.reload()
        self.assertIn("Teal", [t.name for t in dlg.names.values()])
        self.assertIn("bad.json", dlg.note.cget("text"))  # the ignored file is named, the app keeps running
        dlg.pick("Teal")
        self.assertEqual(themes.current().name, "Teal")
        (folder / "a.json").unlink()  # the chosen file disappears: fall back to the default theme
        dlg.reload()
        self.assertEqual(themes.current().name, themes.DEFAULT_THEME)

    def test_open_folder_makes_the_folder_and_a_sample_and_opens_it(self):
        opened = []
        dlg = self.make(opener=opened.append)
        dlg.open_folder()
        self.assertEqual(opened, [self.folder])
        sample = Path(self.folder) / theme_dialog.EXAMPLE_FILE
        self.assertTrue(sample.is_file())
        self.assertTrue(sample.name.endswith(".txt"))  # not picked up as a theme until renamed
        self.assertEqual(themes.load_user_themes(self.folder), ([], []))
        dlg.open_folder()  # again: keeps the file, still opens

    def test_the_buttons_for_the_folder_only_appear_with_a_folder(self):
        dlg = self.make(folder="")
        self.assertFalse(dlg.folder_btn.winfo_manager())
        dlg2 = self.make()
        self.assertTrue(dlg2.folder_btn.winfo_manager())


class ThemeUiTests(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)

    def test_every_theme_applies_to_ttk_and_the_widgets_use_its_colours(self):
        style = ttk.Style(self.root)
        entry = ttk.Entry(self.root)
        combo = ttk.Combobox(self.root, values=["a", "b"])
        tree = ttk.Treeview(self.root)
        for name in themes.THEME_NAMES:
            t = themes.THEMES[name]
            themeui.apply_ttk(self.root, style, t)
            self.assertEqual(style.lookup("TLabel", "foreground"), t["fg"], name)
            self.assertEqual(style.lookup("TEntry", "fieldbackground"), t["entry_bg"], name)
            self.assertEqual(style.lookup("Treeview", "background"), t["tree_field"], name)
            self.assertEqual(style.lookup("Muted.TLabel", "foreground"), t["muted"], name)
            self.assertEqual(style.lookup("TNotebook.Tab", "foreground"), t["tab_fg"], name)
            self.assertEqual(style.lookup("Vertical.TScrollbar", "background"), t["btn_bg"], name)
            self.root.update_idletasks()
        entry.destroy(), combo.destroy(), tree.destroy()

    def test_menu_colours_come_from_the_theme(self):
        t = themes.THEMES["Light"]
        colours = themeui.menu_colors(t)
        self.assertTrue(colours)
        self.assertTrue(all(themes.is_hex_color(v) for v in colours.values() if isinstance(v, str) and v.startswith("#")))
        menu = tk.Menu(self.root)
        menu.configure(**colours)  # every key is a real Menu option

    def test_popdown_lists_are_recoloured(self):
        style = ttk.Style(self.root)
        combo = ttk.Combobox(self.root, values=["a"])
        combo.pack()
        self.root.update_idletasks()
        t = themes.THEMES["Paper"]
        themeui.apply_ttk(self.root, style, t)
        themeui.recolor_popdowns(self.root, t)
        popdown = self.root.tk.call("ttk::combobox::PopdownWindow", str(combo))
        self.assertEqual(self.root.tk.call(f"{popdown}.f.l", "cget", "-background"), t["entry_bg"])
        self.assertEqual(self.root.tk.call(f"{popdown}.f.l", "cget", "-foreground"), t["entry_fg"])


if __name__ == "__main__":
    unittest.main()
