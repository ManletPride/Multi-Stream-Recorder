"""The Theme... window: pick a colour theme, see it applied live, keep it or cancel back.

Built on msr/dialogs.py. The preview is drawn from the highlighted theme's own tokens, and
the whole app already shows it behind this window (msr/themeui.apply_ttk does the work)."""
from __future__ import annotations

import os
import tkinter as tk
from tkinter import ttk
from typing import Callable

from msr import dialogs, themes

EXAMPLE_FILE = "example-theme.json.txt"
EXAMPLE_TEXT = """{
  "name": "My theme",
  "base": "PowerShell Blue",
  "dark": true,
  "colors": {
    "bg": "#101820",
    "accent": "#2a9d8f"
  }
}

Rename this file to my-theme.json (remove the .txt), then press Reload in the Theme window.
"base" is one of the built-in themes; "colors" only needs the colours you want to change
(see msr/themes.py for the names). Colours are #rrggbb. A file with a wrong value is ignored.
"""


def theme_groups() -> list[tuple[str, list[themes.Theme]]]:
    """The list as the window shows it: built-in, dark and light palettes from Omarchy themes, then the user's own.
    Pure (no window), so a test can check that every theme appears once, in themes.THEME_NAMES order."""
    own = [t for k, t in themes.THEMES.items() if k not in themes.THEME_NAMES]
    return [("Built-in", [themes.THEMES[n] for n in themes.GROUP_BUILTIN]),
            ("Dark, from Omarchy palettes", [themes.THEMES[n] for n in themes.GROUP_DARK]),
            ("Light, from Omarchy palettes", [themes.THEMES[n] for n in themes.GROUP_LIGHT]),
            ("Your themes", own)]


def preview(canvas: tk.Canvas, t: themes.Theme) -> None:
    """A miniature of the app in theme t: caption, tabs, roster rows, buttons, log and chart colours."""
    canvas.delete("all")
    w = int(canvas.cget("width"))
    h = int(canvas.cget("height"))
    font, small = ("Segoe UI", 9), ("Segoe UI", 8)
    canvas.configure(bg=t["bg"], highlightthickness=1, highlightbackground=t["border"])
    canvas.create_rectangle(0, 0, w, 22, fill=t["title_bg"], outline="")
    canvas.create_text(8, 11, text="Multi-Stream Recorder", anchor="w", fill=t["title_fg"], font=font)
    x = 8
    for i, label in enumerate(("Status", "Logs", "Library")):
        selected = i == 0
        canvas.create_rectangle(x, 28, x + 58, 48, fill=t["accent"] if selected else t["tab_bg"], outline=t["border"])
        canvas.create_text(x + 29, 38, text=label, fill=t["accent_fg"] if selected else t["tab_fg"], font=small)
        x += 62
    canvas.create_rectangle(8, 52, w - 8, 112, fill=t["tree_field"], outline=t["border"])
    for i, (text, token) in enumerate((("alice   Recording", "rec_fg"), ("bob   Completed", "completed_fg"),
                                       ("carol   Offline", "offline_fg"), ("dave   Error", "error_fg"))):
        canvas.create_text(14, 62 + i * 14, text=text, anchor="w", fill=t[token], font=small)
    canvas.create_rectangle(w - 130, 58, w - 14, 76, fill=t["entry_bg"], outline=t["border"])
    canvas.create_text(w - 124, 67, text="Filter", anchor="w", fill=t["placeholder"], font=small)
    canvas.create_rectangle(w - 130, 82, w - 74, 104, fill=t["btn_bg"], outline=t["border"])
    canvas.create_text(w - 102, 93, text="Button", fill=t["btn_fg"], font=small)
    canvas.create_rectangle(8, 118, 100, 142, fill=t["start_bg"], outline="")
    canvas.create_text(54, 130, text="Start", fill=t["start_fg"], font=font)
    canvas.create_rectangle(108, 118, 200, 142, fill=t["stop_bg"], outline="")
    canvas.create_text(154, 130, text="Stop", fill=t["stop_fg"], font=font)
    canvas.create_rectangle(8, 148, w - 8, 190, fill=t["log_bg"], outline=t["border"])
    for i, (text, token) in enumerate((("12:01 Recording started", "log_fg"), ("12:02 [WARNING] slow", "log_warn"),
                                       ("12:03 [ERROR] failed", "log_error"))):
        canvas.create_text(14, 156 + i * 12, text=text, anchor="w", fill=t[token], font=small)
    base = h - 8
    left = 8
    for i, token in enumerate(("site_kick", "site_twitch", "site_youtube", "site_tiktok", "site_rumble",
                               "site_fishtank", "series_1", "series_2")):
        height = 10 + (i * 7) % 24
        canvas.create_rectangle(left, base - height, left + 22, base, fill=t[token], outline="")
        left += 28
    canvas.create_text(w - 12, h - 14, text=f"{'dark' if t.dark else 'light'}", anchor="e", fill=t["muted"], font=small)


class ThemeDialog:
    """Build with the root and callbacks; open_theme_dialog() runs it modally. Tests drive pick/ok/cancel."""

    def __init__(self, root, *, apply: Callable[[themes.Theme], None], save: Callable[[themes.Theme], None],
                 reload: Callable[[], None] | None = None, folder: str = "", opener=None,
                 problems: Callable[[], list] | None = None):
        self.root, self.apply_cb, self.save_cb = root, apply, save
        self.reload_cb, self.folder, self.opener = reload, folder, opener
        self.problems_cb = problems or (lambda: [])
        self.original = themes.current()
        self.chosen = self.original
        self.done = False
        self.names: dict[str, themes.Theme] = {}
        self.win, frame = dialogs.new_dialog(root, "Theme")
        ttk.Label(frame, wraplength=520, justify="left",
                  text="Pick a theme. The whole window changes as you pick; Cancel puts the old one back.").pack(anchor="w")
        body = ttk.Frame(frame)
        body.pack(fill="both", expand=True, pady=(10, 0))
        left = ttk.Frame(body)
        left.pack(side="left", fill="y")
        self.tree = ttk.Treeview(left, columns=("name",), show="tree", height=13, selectmode="browse")
        self.tree.column("#0", width=200, stretch=False)
        scroll = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.pack(side="left", fill="y")
        scroll.pack(side="left", fill="y")
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._selected())
        right = ttk.Frame(body)
        right.pack(side="left", fill="both", expand=True, padx=(12, 0))
        self.canvas = tk.Canvas(right, width=330, height=244, highlightthickness=1)
        self.canvas.pack(anchor="nw")
        self.info = ttk.Label(right, text="", style="Muted.TLabel", wraplength=330, justify="left")
        self.info.pack(anchor="w", pady=(6, 0))
        self.note = ttk.Label(frame, text="", style="Warn.TLabel", wraplength=520, justify="left")
        self.note.pack(anchor="w", pady=(6, 0))
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=(12, 0))
        self.ok_btn = ttk.Button(row, text="OK", command=self.ok, width=10)
        self.ok_btn.pack(side="right")
        self.cancel_btn = ttk.Button(row, text="Cancel", command=self.cancel, width=10)
        self.cancel_btn.pack(side="right", padx=(0, 6))
        self.folder_btn = ttk.Button(row, text="Open themes folder", command=self.open_folder)
        self.reload_btn = ttk.Button(row, text="Reload", command=self.reload)
        if self.folder:
            self.folder_btn.pack(side="left")
            self.reload_btn.pack(side="left", padx=(6, 0))
        self.win.bind("<Escape>", lambda _e: self.cancel())
        self.win.bind("<Return>", lambda _e: self.ok())
        self.win.protocol("WM_DELETE_WINDOW", self.cancel)
        self.fill()

    # -- list
    def fill(self) -> None:
        self.tree.delete(*self.tree.get_children())
        self.names = {}
        for label, group in theme_groups():
            if not group:
                continue
            parent = self.tree.insert("", "end", text=label, open=True, tags=("group",))
            for t in group:
                iid = self.tree.insert(parent, "end", text=t.name)
                self.names[iid] = t
                if t.name == self.chosen.name:
                    self.tree.selection_set(iid)
                    self.tree.see(iid)
        self.note.configure(text=("Ignored theme files: " + "; ".join(self.problems_cb()[:3])) if self.problems_cb() else "")
        self._show(self.chosen)

    def _show(self, t: themes.Theme) -> None:
        preview(self.canvas, t)
        text, graphic = themes.audit(t)
        kind = "Dark" if t.dark else "Light"
        if t.name in themes.THEME_NAMES:
            line = f"{kind} theme. Text contrast at least {text:.1f}:1, chart colours {graphic:.1f}:1."
            if t.credit:
                line += f"\n{t.credit_line()} Licences and credits: THIRD_PARTY_NOTICES.md."
            self.info.configure(text=line)
        else:
            self.info.configure(text=f"{kind} theme from your themes folder. Text contrast at least {text:.1f}:1"
                                     + ("" if text >= 4.5 else " (below the 4.5:1 guideline)") + ".")

    def _selected(self) -> None:
        picked = self.tree.selection()
        if picked and picked[0] in self.names:
            self.pick(self.names[picked[0]])

    # -- actions
    def pick(self, theme: themes.Theme | str) -> None:
        """Highlight a theme and show it in the whole app right away."""
        found = theme if isinstance(theme, themes.Theme) else themes.find(theme)
        if found is None:
            return
        self.chosen = found
        self.apply_cb(found)
        try:
            self.win.configure(bg=found["bg"])
        except tk.TclError:
            return
        self._show(found)

    def ok(self) -> None:
        if self.done:
            return
        self.done = True
        if self.chosen.name != self.original.name:
            self.save_cb(self.chosen)
        self._close()

    def cancel(self) -> None:
        if self.done:
            return
        self.done = True
        if themes.current().name != self.original.name:
            self.apply_cb(self.original)
        self._close()

    def _close(self) -> None:
        try:
            self.win.destroy()
        except tk.TclError:
            pass

    def reload(self) -> None:
        if self.reload_cb:
            self.reload_cb()
        if themes.find(self.chosen.name) is None:
            self.chosen = themes.THEMES[themes.DEFAULT_THEME]
            self.apply_cb(self.chosen)
        self.fill()

    def open_folder(self) -> None:
        """Make the themes folder (with a how-to example file) and open it."""
        try:
            os.makedirs(self.folder, exist_ok=True)
            example = os.path.join(self.folder, EXAMPLE_FILE)
            if not os.path.exists(example):
                with open(example, "w", encoding="utf-8") as fh:
                    fh.write(EXAMPLE_TEXT)
            if self.opener:
                self.opener(self.folder)
        except OSError as exc:
            dialogs.show_error(self.win, "Theme", f"Could not open the themes folder: {exc}")


def open_theme_dialog(root, **kwargs) -> ThemeDialog:
    dialog = ThemeDialog(root, **kwargs)
    dialogs.run_modal(dialog.win, root, dialog.tree)
    return dialog
