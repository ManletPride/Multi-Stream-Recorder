"""The real app, hidden, on a temporary data folder: the saved theme is honoured at startup (old dark_mode
values migrate), every theme applies to the live window with no errors, charts redraw in the new colours,
the bottom bar still fits, and the choice is written to config.ini. Never touches real data."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

THEME_SCRIPT = r"""
import gc, json, os, sys, time, tkinter as tk
from tkinter import ttk
sys.path.insert(0, sys.argv[1])
os.chdir(sys.argv[2])
os.environ["MSR_SMOKE_TEST"] = "1"
mode = sys.argv[3]
out = {"errors": []}

def fake_mainloop(self, n=0):
    try:
        from msr import library_window as lw, theme_dialog, themes
        self.update_idletasks(); self.update()
        out["start_theme"] = themes.current().name

        def walk(w):
            for c in w.winfo_children():
                yield c
                yield from walk(c)

        def find(text, cls="TButton"):
            for w in walk(self):
                if w.winfo_class() == cls and str(w.cget("text")) == text:
                    return w
        theme_button = find("Theme...")
        out["theme_button"] = theme_button is not None
        out["dark_mode_checkbox"] = any(w.winfo_class() in ("TCheckbutton", "Checkbutton")
                                        and "dark" in str(w.cget("text")).lower() for w in walk(self))
        out["root_bg"] = self.cget("bg")
        out["style_label_fg"] = ttk.Style(self).lookup("TLabel", "foreground")
        if mode != "all":
            sys.stdout.write(json.dumps(out)); sys.stdout.flush(); os._exit(0)

        # the Library tab, with its charts, on a temporary empty library
        panel = next(o for o in gc.get_objects() if type(o) is lw.LibraryPanel)
        panel.frame.master.select(panel.frame)
        end = time.time() + 30
        while (panel.stats is None or panel.busy) and time.time() < end:
            self.update(); time.sleep(0.02)

        # the bottom bar at the default and the minimum window width
        out["bar"] = {}
        for width in (1100, 1060):
            self.geometry(f"{width}x720"); self.update(); self.update_idletasks()
            bar = theme_button.master
            out["bar"][width] = [bar.winfo_reqwidth(), bar.winfo_width(), theme_button.winfo_x() + theme_button.winfo_width(),
                                 theme_button.winfo_viewable(), self.winfo_width()]

        made = []
        real = theme_dialog.ThemeDialog.__init__
        def spy(dlg, *a, **k):
            real(dlg, *a, **k); made.append(dlg)
        theme_dialog.ThemeDialog.__init__ = spy
        theme_dialog.dialogs.run_modal = lambda win, root, focus=None: None   # never shown, never modal
        theme_button.invoke()
        dlg = made[0]
        out["dialog_themes"] = [t.name for t in dlg.names.values()]
        rgbs = lambda t: {tuple(v >> 8 for v in self.winfo_rgb(c)) for c in t.colors.values()}
        out["per_theme"] = {}
        for name in list(themes.THEME_NAMES):
            info = {}
            try:
                dlg.pick(name)
                self.update_idletasks(); self.update()
                t = themes.THEMES[name]
                info["root_bg"] = self.cget("bg") == t["bg"]
                info["label_fg"] = ttk.Style(self).lookup("TLabel", "foreground") == t["fg"]
                info["title_dark"] = t.dark
                # charts redrew in the new colours
                chart_colors = set()
                for chart in (panel.donut, panel.bars, panel.months):
                    info.setdefault("chart_bg", []).append(chart.cget("bg") == t["bg"])
                    for item in chart.find_all():
                        for opt in ("fill", "outline"):
                            try:
                                col = chart.itemcget(item, opt)
                            except tk.TclError:
                                continue
                            if col:
                                chart_colors.add(col)
                allowed = rgbs(t)
                info["chart_stray"] = sorted(c for c in chart_colors
                                             if tuple(v >> 8 for v in self.winfo_rgb(c)) not in allowed)
                info["chart_items"] = len(chart_colors)
                # a tooltip drawn now uses the theme's tooltip colours
                panel.bars.show_tip(40, 20, "tip")
                tip = [chart_item for chart_item in panel.bars.find_withtag("tip")]
                info["tip_fills"] = sorted({panel.bars.itemcget(i, "fill") for i in tip if panel.bars.type(i) == "rectangle"})
                info["tip_expected"] = t["tip_bg"]
                panel.bars.hide_tip()
                # every classic widget colour is one of the theme's colours
                stray = []
                for w in walk(self):
                    cls = w.winfo_class()
                    if cls in ("Menu", "Toplevel"):
                        continue
                    for opt in ("bg", "fg", "insertbackground", "activebackground", "activeforeground",
                                "disabledforeground", "selectbackground", "selectforeground"):
                        if cls.startswith("T") or cls in ("Frame", "Canvas") and opt not in ("bg",):
                            continue
                        try:
                            col = str(w.cget(opt))
                        except tk.TclError:
                            continue
                        if col and tuple(v >> 8 for v in self.winfo_rgb(col)) not in allowed:
                            if opt in ("activebackground", "activeforeground", "selectbackground", "selectforeground",
                                       "insertbackground", "disabledforeground") and cls in ("Label", "Frame", "Canvas"):
                                continue
                            stray.append(f"{cls} {w} {opt}={col}")
                info["stray_widgets"] = stray[:40]
            except BaseException:
                import traceback
                out["errors"].append(f"{name}: {traceback.format_exc()[-1500:]}")
            out["per_theme"][name] = info
        dlg.pick("Paper")
        dlg.ok()
        out["final_theme"] = themes.current().name
        out["saved"] = open("config.ini", encoding="utf-8").read()
    except BaseException:
        import traceback; traceback.print_exc(file=sys.stderr)
    sys.stdout.write(json.dumps(out)); sys.stdout.flush(); os._exit(0)

tk.Tk.mainloop = fake_mainloop
from msr.config import Config
from msr.gui import main_gui
main_gui(Config(os.path.abspath("config.ini")))
"""


class ThemeSmokeTests(unittest.TestCase):
    def run_app(self, mode="start", state=None, gui_lines="", extra=None):
        try:
            import tkinter
            tkinter.Tk().destroy()
        except Exception as exc:
            self.skipTest(f"no display: {exc}")
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            rec = data / "rec"
            rec.mkdir()
            (data / "config.ini").write_text(
                f"[Paths]\nstreams_dir = {rec}\n[GUI]\nminimize_to_tray = false\nnotifications = false\n{gui_lines}",
                encoding="utf-8")
            (data / "channels.json").write_text("[]", encoding="utf-8")
            if state is not None:
                (data / "window_state.json").write_text(json.dumps(state), encoding="utf-8")
            done = subprocess.run([sys.executable, "-c", THEME_SCRIPT, str(ROOT), str(data), mode],
                                  capture_output=True, text=True, timeout=180, cwd=str(ROOT))
        self.assertEqual(done.returncode, 0, done.stderr[-3000:])
        self.assertNotIn("Traceback", done.stderr)
        return json.loads(done.stdout)

    def test_a_fresh_install_starts_in_powershell_blue_with_a_theme_button(self):
        out = self.run_app()
        self.assertEqual(out["start_theme"], "PowerShell Blue")
        self.assertTrue(out["theme_button"])
        self.assertFalse(out["dark_mode_checkbox"])

    def test_old_dark_mode_values_migrate(self):
        self.assertEqual(self.run_app(state={"dark_mode": True})["start_theme"], "PowerShell Blue")
        self.assertEqual(self.run_app(state={"dark_mode": False})["start_theme"], "Light")
        self.assertEqual(self.run_app(gui_lines="dark_mode = false\n")["start_theme"], "Light")
        self.assertEqual(self.run_app(gui_lines="dark_mode = true\n")["start_theme"], "PowerShell Blue")

    def test_a_saved_theme_wins_over_the_old_switch(self):
        out = self.run_app(state={"dark_mode": False}, gui_lines="theme = Midnight Purple\n")
        self.assertEqual(out["start_theme"], "Midnight Purple")
        out = self.run_app(gui_lines="theme = No Such Theme\ndark_mode = false\n")
        self.assertEqual(out["start_theme"], "Light")

    def test_every_theme_applies_to_the_live_window_and_charts_redraw(self):
        out = self.run_app(mode="all")
        self.assertEqual(out["errors"], [])
        from msr import themes
        self.assertEqual(out["dialog_themes"], list(themes.THEME_NAMES))
        self.assertEqual(len(out["dialog_themes"]), len(set(out["dialog_themes"])))  # no duplicate names
        for name in themes.THEME_NAMES:
            info = out["per_theme"][name]
            self.assertTrue(info["root_bg"] and info["label_fg"], (name, info))
            self.assertTrue(all(info["chart_bg"]), (name, info))
            self.assertEqual(info["chart_stray"], [], name)         # chart colours are all theme colours
            self.assertEqual(info["tip_fills"], [info["tip_expected"]], name)
            self.assertEqual(info["stray_widgets"], [], name)
        self.assertEqual(out["final_theme"], "Paper")
        self.assertIn("theme = Paper", out["saved"])                 # persisted
        self.assertIn("dark_mode = false", out["saved"])             # and the old key stays in step

    def test_the_bottom_bar_fits_at_the_default_and_the_minimum_width(self):
        out = self.run_app(mode="all")
        for width in ("1100", "1060"):
            req, shown, right_edge, viewable, window = out["bar"][width]
            self.assertLessEqual(right_edge, shown, (width, out["bar"]))
            self.assertLessEqual(req, int(width), (width, out["bar"]))


if __name__ == "__main__":
    unittest.main()
