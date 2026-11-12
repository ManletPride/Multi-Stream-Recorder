"""Apply a Theme to Tk: every ttk style, the combobox pop-down lists and the Windows caption.

msr/themes.py holds the colours (no tkinter); this module is the one place that turns them
into ttk styles, so the main window, the dialogs and the tests all get the same look.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from msr import themes

FONT_HEADING = ("Segoe UI", 10, "bold")


def apply_ttk(root: tk.Misc, style: ttk.Style, t: themes.Theme) -> None:
    """(Re)configure every ttk style from the theme. Safe to call again for a live theme change."""
    style.theme_use("clam")
    border = t["border"]
    frame_edge = dict(bordercolor=border, lightcolor=border, darkcolor=border)
    style.configure(".", background=t["bg"], foreground=t["fg"], fieldbackground=t["tree_field"],
                    troughcolor=t["tree_field"], insertcolor=t["fg"], selectbackground=t["select_bg"],
                    selectforeground=t["select_fg"], **frame_edge)
    style.map(".", foreground=[("disabled", t["disabled_fg"])])
    style.configure("TFrame", background=t["bg"])
    style.configure("TLabel", background=t["bg"], foreground=t["fg"])
    for name, token in (("Muted", "muted"), ("Warn", "warn"), ("Error", "error"), ("Ok", "ok"), ("Hint", "hint")):
        style.configure(f"{name}.TLabel", background=t["bg"], foreground=t[token])
    style.configure("TLabelframe", background=t["bg"], **frame_edge)
    style.configure("TLabelframe.Label", background=t["bg"], foreground=t["fg"])
    style.configure("TSeparator", background=border)
    style.configure("TPanedwindow", background=t["bg"])
    style.configure("TSizegrip", background=t["bg"])
    style.configure("TNotebook", background=t["bg"], borderwidth=0, tabmargins=[0, 0, 0, 0], **frame_edge)
    style.configure("TNotebook.Tab", background=t["tab_bg"], foreground=t["tab_fg"], padding=[12, 4], **frame_edge)
    style.map("TNotebook.Tab",
              background=[("selected", t["accent"]), ("!selected", t["tab_bg"])],
              foreground=[("selected", t["accent_fg"]), ("!selected", t["tab_fg"])],
              lightcolor=[("selected", border), ("!selected", border)],
              darkcolor=[("selected", border), ("!selected", border)],
              bordercolor=[("selected", border), ("!selected", border)])
    style.configure("Treeview", background=t["tree_field"], foreground=t["tree_fg"], fieldbackground=t["tree_field"],
                    rowheight=22, **frame_edge)
    style.configure("Treeview.Heading", background=t["tree_heading_bg"], foreground=t["tree_heading_fg"],
                    font=FONT_HEADING, bordercolor=border, lightcolor=t["tree_heading_bg"], darkcolor=border)
    style.map("Treeview", background=[("selected", t["select_bg"])], foreground=[("selected", t["select_fg"])])
    style.configure("TEntry", fieldbackground=t["entry_bg"], foreground=t["entry_fg"], insertcolor=t["entry_fg"],
                    **frame_edge)
    style.map("TEntry", bordercolor=[("focus", t["accent"])],
              fieldbackground=[("disabled", t["panel"]), ("readonly", t["panel"])],
              foreground=[("disabled", t["disabled_fg"])])
    style.configure("TCombobox", fieldbackground=t["entry_bg"], foreground=t["entry_fg"], background=t["btn_bg"],
                    arrowcolor=t["fg"], selectbackground=t["entry_bg"], selectforeground=t["entry_fg"], **frame_edge)
    style.map("TCombobox", fieldbackground=[("readonly", t["entry_bg"]), ("disabled", t["panel"])],
              foreground=[("readonly", t["entry_fg"]), ("disabled", t["disabled_fg"])],
              selectbackground=[("readonly", t["entry_bg"])], selectforeground=[("readonly", t["entry_fg"])],
              bordercolor=[("focus", t["accent"])])
    style.configure("TSpinbox", fieldbackground=t["entry_bg"], foreground=t["entry_fg"], arrowcolor=t["fg"],
                    background=t["btn_bg"], **frame_edge)
    for kind in ("TCheckbutton", "TRadiobutton"):
        style.configure(kind, background=t["bg"], foreground=t["fg"], indicatorcolor=t["entry_bg"],
                        indicatorrelief="flat")
        style.map(kind, background=[("active", t["bg"]), ("pressed", t["bg"])],
                  indicatorcolor=[("selected", t["accent"]), ("pressed", t["accent"])],
                  foreground=[("disabled", t["disabled_fg"])])
    style.configure("TButton", background=t["btn_bg"], foreground=t["btn_fg"], padding=[8, 4], **frame_edge)
    style.map("TButton", background=[("active", t["btn_hover"]), ("pressed", t["btn_hover"])],
              foreground=[("disabled", t["disabled_fg"]), ("active", t["btn_hover_fg"]),
                          ("pressed", t["btn_hover_fg"])])
    # The primary action (Clip Now): accent colours, audited as a contrast pair in msr/themes.py.
    style.configure("Accent.TButton", background=t["accent"], foreground=t["accent_fg"], padding=[8, 4],
                    **frame_edge)
    style.map("Accent.TButton", background=[("active", t["btn_hover"]), ("pressed", t["btn_hover"]),
                                            ("disabled", t["btn_bg"])],
              foreground=[("disabled", t["disabled_fg"]), ("active", t["btn_hover_fg"]),
                          ("pressed", t["btn_hover_fg"])])
    style.configure("TMenubutton", background=t["btn_bg"], foreground=t["btn_fg"], arrowcolor=t["fg"],
                    padding=[8, 4], **frame_edge)
    style.map("TMenubutton", background=[("active", t["btn_hover"]), ("pressed", t["btn_hover"])],
              foreground=[("active", t["btn_hover_fg"]), ("pressed", t["btn_hover_fg"])])
    for orient in ("Vertical", "Horizontal"):
        style.configure(f"{orient}.TScrollbar", background=t["btn_bg"], bordercolor=border, arrowcolor=t["fg"],
                        troughcolor=t["tree_field"], lightcolor=border, darkcolor=border)
        style.map(f"{orient}.TScrollbar", background=[("active", t["btn_hover"]), ("pressed", t["btn_hover"])])
        style.configure(f"{orient}.TProgressbar", background=t["accent"], troughcolor=t["tree_field"],
                        bordercolor=border, lightcolor=t["accent"], darkcolor=t["accent"])
    style.configure("TProgressbar", background=t["accent"], troughcolor=t["tree_field"], bordercolor=border,
                    lightcolor=t["accent"], darkcolor=t["accent"])
    # Pop-down lists of comboboxes created from now on pick these up from the option database.
    for option, token in (("background", "entry_bg"), ("foreground", "entry_fg"), ("selectBackground", "select_bg"),
                          ("selectForeground", "select_fg")):
        root.option_add(f"*TCombobox*Listbox.{option}", t[token])
    recolor_popdowns(root, t)


def recolor_popdown(combo: ttk.Combobox, t: themes.Theme) -> None:
    """A combobox's drop-down list keeps the colours it was created with, so repaint it."""
    try:
        popdown = combo.tk.call("ttk::combobox::PopdownWindow", combo._w)
        combo.tk.call(f"{popdown}.f.l", "configure", "-background", t["entry_bg"], "-foreground", t["entry_fg"],
                      "-selectbackground", t["select_bg"], "-selectforeground", t["select_fg"],
                      "-highlightthickness", 0, "-relief", "flat", "-borderwidth", 0)
        combo.tk.call(popdown, "configure", "-background", t["entry_bg"])
    except tk.TclError:
        pass


def recolor_popdowns(root: tk.Misc, t: themes.Theme) -> int:
    """Repaint the drop-down of every combobox below root (open dialogs included)."""
    count = 0
    stack = [root]
    while stack:
        widget = stack.pop()
        try:
            stack.extend(widget.winfo_children())
            if isinstance(widget, ttk.Combobox):
                recolor_popdown(widget, t)
                count += 1
        except tk.TclError:
            continue
    return count


def menu_colors(t: themes.Theme) -> dict:
    """Options for a tk.Menu (context menus, the More menu): flat, themed, no system border."""
    return dict(bg=t["entry_bg"], fg=t["fg"], activebackground=t["select_bg"], activeforeground=t["select_fg"],
                disabledforeground=t["disabled_fg"], borderwidth=0, activeborderwidth=0, relief="flat")
