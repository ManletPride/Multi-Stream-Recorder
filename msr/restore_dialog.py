"""The Restore previous... window: pick an earlier saved version of channels.json or
config.ini and put it back. Built on msr/dialogs.py (themed, parented, centred)."""

from __future__ import annotations

from pathlib import Path
import tkinter as tk
from tkinter import ttk
from typing import Callable

from msr import dialogs, history, winstate


def when_text(stamp) -> str:
    return stamp.strftime("%Y-%m-%d  %H:%M:%S")


class RestoreWindow:
    """The window itself; open_restore() builds it and runs it modally."""

    def __init__(self, parent, path, title: str, *, can_restore: Callable[[], str | None] | None = None,
                 on_restored: Callable[[str], None] | None = None, note: str = "", state_file=None):
        self.parent, self.path, self.title = parent, path, title
        self.can_restore, self.on_restored, self.note = can_restore, on_restored, note
        self.versions: list[history.Version] = []
        self.win, frame = dialogs.new_dialog(parent, title)
        self.win.resizable(True, True)
        ttk.Label(frame, wraplength=560, justify="left",
                  text="Pick an earlier version and press Restore. The file as it is now is saved to this "
                       "list first, so a restore can be undone. Passwords and e-mail addresses are never "
                       "shown here.").pack(anchor="w")
        box = ttk.Frame(frame)
        box.pack(fill="both", expand=True, pady=(10, 0))
        self.tree = ttk.Treeview(box, columns=("when", "summary"), show="headings", height=9, selectmode="browse")
        self.tree.heading("when", text="Saved")
        self.tree.heading("summary", text="What restoring does")
        self.tree.column("when", width=150, stretch=False)
        self.tree.column("summary", width=440)
        scroll = ttk.Scrollbar(box, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._sync())
        self.tree.bind("<Double-1>", lambda _e: self.restore_selected())
        self.status = ttk.Label(frame, text="", wraplength=560, justify="left")
        self.status.pack(anchor="w", pady=(8, 0))
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=(12, 0))
        self.close_btn = ttk.Button(row, text="Close", command=self.win.destroy)
        self.close_btn.pack(side="right")
        self.restore_btn = ttk.Button(row, text="Restore", command=self.restore_selected)
        self.restore_btn.pack(side="right", padx=(0, 6))
        self.win.bind("<Escape>", lambda _e: self.win.destroy())
        self.win.protocol("WM_DELETE_WINDOW", self.win.destroy)
        self.refresh()
        self.win.update_idletasks()
        if state_file:  # remembers its size and place between sessions
            winstate.attach(self.win, state_file, "restore_" + Path(str(path)).stem,
                            (self.win.winfo_reqwidth() * 2 // 3, self.win.winfo_reqheight() * 2 // 3))

    def refresh(self) -> None:
        self.versions = history.list_versions(self.path)
        try:
            current = Path(self.path).read_bytes()
        except OSError:
            current = None
        self.tree.delete(*self.tree.get_children())
        name = str(self.path)
        for index, version in enumerate(self.versions):
            try:
                summary = history.summarize(name, version.read(), current)
            except OSError:
                summary = "(unreadable)"
            self.tree.insert("", "end", iid=str(index), values=(when_text(version.stamp), summary))
        if self.versions:
            self.tree.selection_set("0")
        self._sync()

    def selected(self) -> history.Version | None:
        picked = self.tree.selection()
        if not picked:
            return None
        try:
            return self.versions[int(picked[0])]
        except (ValueError, IndexError):
            return None

    def _sync(self) -> None:
        self.restore_btn.state(["!disabled"] if self.selected() else ["disabled"])
        if not self.versions:
            self.status.configure(text="No earlier versions yet. A copy is kept each time this file changes.")
        else:
            self.status.configure(text=f"{len(self.versions)} saved version(s), newest first (up to {history.KEEP}).")

    def restore_selected(self) -> bool:
        version = self.selected()
        if version is None:
            return False
        if self.can_restore is not None:
            problem = self.can_restore()
            if problem:
                dialogs.show_warning(self.win, self.title, problem)
                return False
        if not dialogs.ask_yes_no(
                self.win, self.title,
                f"Restore the version saved {when_text(version.stamp)}?\n\n"
                "What is there now is kept in this list first, so you can come back to it."):
            return False
        try:
            history.restore(self.path, version)
        except OSError as exc:
            dialogs.show_error(self.win, self.title, f"Could not restore: {exc}")
            return False
        if self.on_restored is not None:
            self.on_restored(str(self.path))
        self.refresh()
        self.status.configure(text="Restored. " + self.note)
        if self.note:
            dialogs.show_info(self.win, self.title, self.note)
        return True


def open_restore(parent, path, title: str, **kwargs) -> RestoreWindow | None:
    """Open the window over `parent`. With no history yet, just say so."""
    if not history.list_versions(path):
        dialogs.show_info(parent, title,
                          "No earlier versions yet. A copy is kept each time this file changes.")
        return None
    window = RestoreWindow(parent, path, title, **kwargs)
    dialogs.run_modal(window.win, parent, window.tree)
    return window