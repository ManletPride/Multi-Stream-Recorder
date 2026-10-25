"""File management in the Library tab: the Files list, the PendingDeletion card, the Clean up
suggestions, and the Manager that every delete button and menu entry goes through.

Nothing here removes a file by itself. Every path leads to a review window (library_dialog)
that shows a dry run, then to library_actions.ActionJob running in a background thread, with
the Guard asked again before each file. Roots and the recorder's busy state are looked up
fresh each time.
"""

from __future__ import annotations

import os
import time
import tkinter as tk
from dataclasses import dataclass, field
from tkinter import ttk
from typing import Callable

from msr import charts, dialogs
from msr import library as lib
from msr import library_actions as act
from msr import library_dialog as ldialog

FILES_CAP = 5000
CHECK_ON, CHECK_OFF = "\u2611", "\u2610"
# (label, kinds, where): where is "pending" (only PendingDeletion), "live" (not PendingDeletion) or None
SHOW_CHOICES = (("Recordings", ("recording",), "live"), ("Raw and partial files", ("raw", "partial"), "live"),
                ("Clips and screenshots", ("clip", "screenshot"), "live"),
                ("In PendingDeletion", ("media", "partial", "sidecar"), "pending"),
                ("Everything", ("recording", "raw", "partial", "clip", "screenshot", "media"), None))


@dataclass
class ManageContext:
    """What the tab needs from the app (all optional in tests)."""
    data_dir: str = ""
    log_path: str = ""
    manifest_path: str = ""
    get_busy: Callable[[], tuple] = lambda: (set(), set())
    get_purge: Callable[[], tuple] = lambda: (7, True)      # (auto_purge_days, purge_on_startup)
    revealer: Callable[[str], None] | None = None
    opener: Callable[[str], None] | None = None
    deleters: dict = field(default_factory=dict)             # action -> deleter(item); tests inject fakes
    recycle_problem: Callable | None = None
    logger: object = None


def popup(win, menu: tk.Menu, event) -> None:
    """Show a context menu in the app's colours."""
    pal = charts.palette(win)
    menu.configure(bg=pal["menu_bg"], fg=pal["menu_fg"], activebackground=pal["menu_active"],
                   activeforeground=pal["menu_active_fg"])
    try:
        menu.tk_popup(event.x_root, event.y_root)
    finally:
        menu.grab_release()


class Manager:
    def __init__(self, panel, ctx: ManageContext):
        self.panel, self.ctx = panel, ctx
        self.manifest = act.Manifest(ctx.manifest_path or None)
        self.log = act.ActionLog(ctx.log_path or None)
        self.ask = ldialog.ask_action          # tests replace this with a function returning a Plan
        self.last_batch: str | None = None
        self.files = FilesTab(self, panel.tabs)
        self.pending = PendingTab(self, panel.tabs)
        self.cleanup = CleanupTab(self, panel.tabs)
        panel.tabs.add(self.files.frame, text="  Files  ")
        panel.tabs.add(self.pending.frame, text="  PendingDeletion  ")
        panel.tabs.add(self.cleanup.frame, text="  Clean up  ")

    # ------------------------------------------------------------ services
    def guard(self) -> act.Guard:
        return act.Guard(self.panel.get_roots(), data_dir=self.ctx.data_dir, busy=self.ctx.get_busy)

    def log_ok(self) -> bool:
        return self.log.writable()

    def methods(self) -> tuple:
        return (act.ACTION_PENDING, act.ACTION_TRASH, act.ACTION_PERMANENT) if os.name == "nt" \
            else (act.ACTION_PENDING, act.ACTION_PERMANENT)

    def recycle_problem(self, plan) -> str | None:
        sample = plan.items[0].path if plan is not None and plan.items else (self.panel.get_roots().streams_dir or ".")
        total = plan.bytes if plan is not None else 0
        check = self.ctx.recycle_problem or act.recycle_bin_problem
        return check(sample, total)

    def can_act(self) -> str | None:
        panel = self.panel
        if panel.busy:
            return "A scan or another action is still running. Wait for it, or press Cancel."
        if panel.stats is None:
            return "The library has not been scanned yet."
        if panel.stats.cancelled:
            return "The last scan was cancelled and is incomplete. Rescan first."
        if not self.log_ok():
            return "MSR cannot write its action log in the data folder, so it will not change any files."
        return None

    def _refuse(self, reason: str) -> None:
        dialogs.show_info(self.panel.toplevel, "Library", reason)

    # ------------------------------------------------------------ entry points
    def recs_of_channels(self, channels) -> list:
        keys = {(c.platform.lower(), c.name.lower()) for c in channels}
        return [r for r in self.panel.stats.files if (r.platform, r.channel.lower()) in keys]

    def delete_channels(self, channels, method: str | None = None) -> None:
        reason = self.can_act()
        if reason:
            return self._refuse(reason)
        if not channels:
            return
        names = ", ".join(f"{c.name} ({lib.platform_label(c.platform)})" for c in channels[:4])
        if len(channels) > 4:
            names += f" and {len(channels) - 4} more"
        recs = self.recs_of_channels(channels)
        if not recs:
            return self._refuse("There is nothing in the library for the selected channel(s).")
        intro = (f"Delete content of: {names}.\nChoose what to include and how to remove it. You always see "
                 "exactly which files first, and a channel that is recording right now is skipped.")
        plan = self.ask(self.panel.toplevel, "Delete channel content", intro, recs, filters=True, preselect=True,
                        methods=self.methods(), default_method=method or act.ACTION_PENDING)
        if plan is not None:
            self.run_plan(plan)

    def delete_files(self, recs, method: str | None = None, title: str = "Delete files") -> None:
        reason = self.can_act()
        if reason:
            return self._refuse(reason)
        if not recs:
            return self._refuse("Tick or select some files first.")
        plan = self.ask(self.panel.toplevel, title, f"{len(recs):,} file(s) chosen in the Files list.", recs,
                        filters=False, preselect=True, methods=self.methods(),
                        default_method=method or act.ACTION_PENDING)
        if plan is not None:
            self.run_plan(plan)

    def review(self, suggestion: act.Suggestion) -> None:
        reason = self.can_act()
        if reason:
            return self._refuse(reason)
        if not suggestion.count:
            return self._refuse("Nothing found for this suggestion.")
        if suggestion.dirs:
            plan = self.ask(self.panel.toplevel, suggestion.title, suggestion.detail + " Untick any to keep.",
                            dirs=suggestion.dirs, preselect=False)
        else:
            plan = self.ask(self.panel.toplevel, suggestion.title,
                            suggestion.detail + "\nNothing is ticked yet: tick what you want to remove.",
                            suggestion.recs, filters=False, preselect=False, methods=self.methods())
        if plan is not None:
            self.run_plan(plan)

    def empty_pending(self) -> None:
        reason = self.can_act()
        if reason:
            return self._refuse(reason)
        recs = [r for r in act.pending_recs(self.panel.stats) if r.kind in act.DELETABLE_KINDS]
        if not recs:
            return self._refuse("PendingDeletion is empty.")
        plan = self.ask(self.panel.toplevel, "Empty PendingDeletion now",
                        "These files are waiting in PendingDeletion. Emptying it deletes them permanently now, "
                        "instead of waiting for the automatic purge.", recs, filters=False, preselect=True,
                        methods=(act.ACTION_PERMANENT,), default_method=act.ACTION_PERMANENT)
        if plan is not None:
            self.run_plan(plan)

    def restore_pending(self, recs=None) -> None:
        reason = self.can_act()
        if reason:
            return self._refuse(reason)
        recs = recs if recs is not None else [r for r in act.pending_recs(self.panel.stats) if r.kind in act.DELETABLE_KINDS]
        if not recs:
            return self._refuse("There is nothing to restore.")
        plan = self.ask(self.panel.toplevel, "Restore from PendingDeletion",
                        "These files go back to where they came from. Nothing is overwritten.", recs,
                        filters=False, preselect=True, methods=(act.ACTION_RESTORE,), default_method=act.ACTION_RESTORE)
        if plan is not None:
            self.run_plan(plan)

    def undo_last(self) -> None:
        reason = self.can_act()
        if reason:
            return self._refuse(reason)
        if not self.last_batch:
            return
        guard = self.guard()
        plan = act.restore_batch_plan(guard, self.manifest, self.last_batch)
        if not plan.ok:
            self.last_batch = None
            self.panel.show_undo(False)
            return self._refuse("Those files are no longer in PendingDeletion (already purged or moved).")
        self.run_plan(plan)

    # ------------------------------------------------------------ running
    def run_plan(self, plan: act.Plan) -> None:
        reason = self.can_act()
        if reason:
            return self._refuse(reason)
        guard = self.guard()
        job = act.ActionJob(plan, guard, self.panel.out, deleter=self.ctx.deleters.get(plan.action), log=self.log,
                            manifest=self.manifest, logger=self.ctx.logger)
        self.panel.begin_action(job, f"{act.ACTION_LABELS[plan.action]}: {plan.files or plan.folders:,} item(s)...")

    def finished(self, result: act.Result) -> None:
        panel = self.panel
        text = result.summary
        if result.action == act.ACTION_PENDING and result.done:
            text += " Space is freed when PendingDeletion is emptied."
        panel.status.configure(text=text)
        panel._result_text = text  # still visible once the rescan has finished
        if result.action == act.ACTION_RESTORE:
            self.last_batch = None
            panel.show_undo(False)
        elif result.moves:
            self.last_batch = result.batch
            panel.show_undo(True)
        else:
            panel.show_undo(False)
        if result.failed:
            lines = "\n".join(f"{os.path.basename(p)}: {why}" for p, why in result.failed[:6])
            more = f"\n... and {len(result.failed) - 6} more (see the action log)." if len(result.failed) > 6 else ""
            dialogs.show_warning(panel.toplevel, "Library", f"{text}\n\n{lines}{more}")

    # ------------------------------------------------------------ data
    def on_stats(self, stats: lib.LibraryStats) -> None:
        self.files.set_stats(stats)
        self.pending.update(stats)
        self.cleanup.update(stats)

    def refresh_theme(self) -> None:
        pass  # the tabs use ttk widgets, which follow the theme by themselves


# ====================================================================== Files tab
class FilesTab:
    """Every file of a channel (or of the whole library), with ticks, search, month filter,
    Play, Show in Explorer and Delete."""

    COLUMNS = (("check", "", 30, "center"), ("name", "File", 300, "w"), ("channel", "Channel", 150, "w"),
               ("size", "Size", 85, "e"), ("date", "Date", 90, "w"), ("kind", "Type", 100, "w"))

    def __init__(self, mgr: Manager, parent):
        self.mgr = mgr
        self.frame = f = ttk.Frame(parent, padding=8)
        self.recs: list = []
        self.checked: set[str] = set()
        self.rows: dict[str, object] = {}
        self.sort_col, self.sort_desc = "date", True
        self.month: str | None = None
        self.channel_map: dict[str, tuple] = {}

        top = ttk.Frame(f)
        top.pack(fill="x")
        ttk.Label(top, text="Channel:").pack(side="left")
        self.channel_box = ttk.Combobox(top, state="readonly", width=26, values=["All channels"])
        self.channel_box.current(0)
        self.channel_box.pack(side="left", padx=(4, 10))
        ttk.Label(top, text="Show:").pack(side="left")
        self.show_box = ttk.Combobox(top, state="readonly", width=22, values=[c[0] for c in SHOW_CHOICES])
        self.show_box.current(0)
        self.show_box.pack(side="left", padx=(4, 10))
        ttk.Label(top, text="Search:").pack(side="left")
        self.search_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.search_var, width=18).pack(side="left", padx=(4, 10))
        self.month_btn = ttk.Button(top, text="", command=lambda: self.set_month(None))
        for widget in (self.channel_box, self.show_box):
            widget.bind("<<ComboboxSelected>>", lambda _e: self.fill())
        self.search_var.trace_add("write", lambda *_: self.fill())

        bar = ttk.Frame(f)
        bar.pack(fill="x", pady=(6, 4))
        ttk.Button(bar, text="Tick all", command=self.check_all).pack(side="left")
        ttk.Button(bar, text="Untick all", command=self.check_none).pack(side="left", padx=(6, 0))
        ttk.Label(bar, text="  Tick older than").pack(side="left")
        self.days_var = tk.StringVar(value="90")
        ttk.Entry(bar, textvariable=self.days_var, width=5).pack(side="left", padx=4)
        ttk.Label(bar, text="days").pack(side="left")
        ttk.Button(bar, text="Tick", command=self.check_older).pack(side="left", padx=(4, 0))
        self.delete_btn = ttk.Button(bar, text="Delete\u2026", command=lambda: self.delete())
        self.delete_btn.pack(side="right")
        self.reveal_btn = ttk.Button(bar, text="Show in Explorer", command=self.reveal)
        self.reveal_btn.pack(side="right", padx=(0, 6))
        self.play_btn = ttk.Button(bar, text="Play", command=self.play)
        self.play_btn.pack(side="right", padx=(0, 6))
        self.count = ttk.Label(f, text="Scan the library to list files.")
        self.count.pack(anchor="w")

        box = ttk.Frame(f)
        box.pack(fill="both", expand=True, pady=(4, 0))
        self.tree = ttk.Treeview(box, columns=[c[0] for c in self.COLUMNS], show="headings", selectmode="extended",
                                 height=12)
        for cid, text, width, anchor in self.COLUMNS:
            self.tree.heading(cid, text=text, command=(lambda c=cid: self.sort_by(c)) if cid != "check" else "")
            self.tree.column(cid, width=width, anchor=anchor, stretch=(cid == "name"))
        scroll = ttk.Scrollbar(box, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.tree.bind("<Button-1>", self._click)
        self.tree.bind("<space>", lambda _e: (self.toggle_selected(), "break")[1])
        self.tree.bind("<Control-a>", lambda _e: (self.check_all(), "break")[1])
        self.tree.bind("<Delete>", lambda _e: (self.delete(), "break")[1])
        self.tree.bind("<Double-1>", self._double)
        self.tree.bind("<Button-3>", self._menu)
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._sync())
        self.menu = tk.Menu(self.tree, tearoff=0)
        self.menu.add_command(label="Play", command=self.play)
        self.menu.add_command(label="Show in Explorer", command=self.reveal)
        self.menu.add_separator()
        self.menu.add_command(label="Tick / untick selected", command=self.toggle_selected)
        self.menu.add_command(label="Move to PendingDeletion\u2026", command=lambda: self.delete(act.ACTION_PENDING))
        self.menu.add_command(label="Delete\u2026", command=lambda: self.delete())
        self.menu.add_command(label="Delete permanently\u2026", command=lambda: self.delete(act.ACTION_PERMANENT))

    # -- data
    def set_stats(self, stats: lib.LibraryStats) -> None:
        self.recs = list(stats.files)
        labels = {}
        for ch in stats.ranked_channels():
            labels[f"{ch.name}  ({lib.platform_label(ch.platform)})"] = (ch.platform.lower(), ch.name.lower())
        current = self.channel_box.get()
        self.channel_map = labels
        self.channel_box.configure(values=["All channels"] + sorted(labels, key=str.lower))
        if current not in labels:
            self.channel_box.current(0)
        self.checked &= {os.path.normcase(r.path) for r in self.recs}
        self.fill()

    def set_channel(self, platform: str, name: str) -> None:
        for label, key in self.channel_map.items():
            if key == (platform.lower(), name.lower()):
                self.channel_box.set(label)
                break
        self.fill()

    def set_month(self, month: str | None) -> None:
        self.month = month
        if month:
            self.month_btn.configure(text=f"Month {month}  \u2715")
            self.month_btn.pack(side="left")
        else:
            self.month_btn.pack_forget()
        self.fill()

    def shown(self) -> list:
        _label, kinds, where = SHOW_CHOICES[max(self.show_box.current(), 0)]
        picked = self.channel_box.get()
        key = self.channel_map.get(picked)
        rows = act.filter_recs(self.recs, kinds=kinds, text=self.search_var.get(), month=self.month)
        if where == "pending":
            rows = [r for r in rows if r.category == "pending"]
        elif where == "live":
            rows = [r for r in rows if r.category != "pending"]
        if key is not None:
            rows = [r for r in rows if (r.platform, r.channel.lower()) == key]
        keyf = {"name": lambda r: os.path.basename(r.path).lower(), "channel": lambda r: (r.platform, r.channel.lower()),
                "size": lambda r: r.size, "date": lambda r: r.when, "kind": lambda r: r.kind}[self.sort_col]
        rows.sort(key=keyf, reverse=self.sort_desc)
        return rows

    def sort_by(self, column: str) -> None:
        if column == self.sort_col:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_col, self.sort_desc = column, column in ("size", "date")
        self.fill()

    def fill(self) -> None:
        self.tree.delete(*self.tree.get_children())
        self.rows = {}
        rows = self.shown()
        for index, rec in enumerate(rows[:FILES_CAP]):
            iid = f"f{index}"
            self.rows[iid] = rec
            self.tree.insert("", "end", iid=iid, values=self._values(rec))
        for cid, text, *_ in self.COLUMNS:
            if cid != "check":
                arrow = (" \u25bc" if self.sort_desc else " \u25b2") if cid == self.sort_col else ""
                self.tree.heading(cid, text=text + arrow)
        more = f"  (showing the first {FILES_CAP:,}: narrow the filter)" if len(rows) > FILES_CAP else ""
        self.shown_count = len(rows)
        self._sync(more)

    def _values(self, rec) -> list:
        return [CHECK_ON if os.path.normcase(rec.path) in self.checked else CHECK_OFF, os.path.basename(rec.path),
                f"{rec.channel} ({lib.platform_label(rec.platform)})", lib.fmt_size(rec.size),
                lib._day(rec.when) if rec.when else "", act.KIND_LABELS.get(rec.kind, rec.kind)]

    def _sync(self, more: str = "") -> None:
        ticked = [r for r in self.recs if os.path.normcase(r.path) in self.checked]
        size = sum(r.size for r in ticked)
        month = f", month {self.month}" if self.month else ""
        self.count.configure(text=f"{getattr(self, 'shown_count', 0):,} shown{month}; {len(ticked):,} ticked "
                                  f"({lib.fmt_size(size)}){more}")
        have_sel = bool(self.tree.selection())
        for button in (self.play_btn, self.reveal_btn):
            button.state(["!disabled"] if have_sel else ["disabled"])
        self.delete_btn.state(["!disabled"] if (ticked or have_sel) else ["disabled"])

    # -- ticks
    def _toggle(self, iid: str) -> None:
        rec = self.rows.get(iid)
        if rec is None:
            return
        self.checked.symmetric_difference_update({os.path.normcase(rec.path)})
        self.tree.item(iid, values=self._values(rec))
        self._sync()

    def _click(self, event):
        if self.tree.identify_region(event.x, event.y) == "cell" and self.tree.identify_column(event.x) == "#1":
            self._toggle(self.tree.identify_row(event.y))
            return "break"
        return None

    def toggle_selected(self) -> None:
        for iid in self.tree.selection():
            self._toggle(iid)

    def check_all(self) -> None:
        self.checked |= {os.path.normcase(r.path) for r in self.rows.values()}
        self.fill()

    def check_none(self) -> None:
        self.checked.clear()
        self.fill()

    def check_older(self) -> None:
        try:
            days = float(self.days_var.get())
        except ValueError:
            return
        now = time.time()
        self.checked |= {os.path.normcase(r.path) for r in self.rows.values() if now - r.when >= days * 86400}
        self.fill()

    # -- actions
    def selected_recs(self) -> list:
        return [self.rows[i] for i in self.tree.selection() if i in self.rows]

    def target_recs(self) -> list:
        ticked = [r for r in self.recs if os.path.normcase(r.path) in self.checked]
        return ticked or self.selected_recs()

    def delete(self, method: str | None = None) -> None:
        self.mgr.delete_files(self.target_recs(), method, "Delete files")

    def play(self) -> None:
        rec = next(iter(self.selected_recs()), None)
        opener = self.mgr.ctx.opener
        if rec is None or opener is None:
            return
        try:
            opener(rec.path)
        except Exception as exc:
            dialogs.show_error(self.mgr.panel.toplevel, "Play", f"Could not open the file: {exc}")

    def reveal(self) -> None:
        rec = next(iter(self.selected_recs()), None)
        reveal = self.mgr.ctx.revealer
        if rec is None or reveal is None:
            return
        try:
            reveal(rec.path)
        except Exception as exc:
            dialogs.show_error(self.mgr.panel.toplevel, "Show in Explorer", f"Could not open it: {exc}")

    def _double(self, event):
        if self.tree.identify_column(event.x) != "#1":
            self.play()

    def _menu(self, event) -> None:
        iid = self.tree.identify_row(event.y)
        if iid and iid not in self.tree.selection():
            self.tree.selection_set(iid)
        popup(self.frame, self.menu, event)


# ====================================================================== PendingDeletion tab
class PendingTab:
    def __init__(self, mgr: Manager, parent):
        self.mgr = mgr
        self.frame = f = ttk.Frame(parent, padding=10)
        self.info = ttk.Label(f, text="Scan the library first.", justify="left")
        self.info.pack(anchor="w")
        ttk.Label(f, style="Muted.TLabel", wraplength=820, justify="left",
                  text="PendingDeletion is MSR's own waiting room: files MSR moved aside, and files you moved "
                       "here from the Library, wait here until the automatic purge deletes them when MSR starts. "
                       "Empty it now to free the space immediately, or restore what you want back.").pack(anchor="w", pady=(4, 8))
        bar = ttk.Frame(f)
        bar.pack(fill="x")
        self.empty_btn = ttk.Button(bar, text="Empty now\u2026", command=mgr.empty_pending)
        self.restore_btn = ttk.Button(bar, text="Restore all\u2026", command=lambda: mgr.restore_pending())
        self.restore_sel_btn = ttk.Button(bar, text="Restore selected\u2026", command=self.restore_selected)
        self.open_btn = ttk.Button(bar, text="Open folder", command=self.open_folder)
        self.log_btn = ttk.Button(bar, text="Open action log", command=self.open_log)
        for b in (self.empty_btn, self.restore_btn, self.restore_sel_btn, self.open_btn, self.log_btn):
            b.pack(side="left", padx=(0, 6))
        box = ttk.Frame(f)
        box.pack(fill="both", expand=True, pady=(8, 0))
        self.tree = ttk.Treeview(box, columns=("channel", "files", "size", "age"), show="headings",
                                 selectmode="extended", height=10)
        for cid, text, width, anchor in (("channel", "Channel", 260, "w"), ("files", "Files", 70, "e"),
                                         ("size", "Size", 100, "e"), ("age", "Oldest there", 120, "w")):
            self.tree.heading(cid, text=text)
            self.tree.column(cid, width=width, anchor=anchor, stretch=(cid == "channel"))
        scroll = ttk.Scrollbar(box, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.groups: dict[str, list] = {}
        self.tree.bind("<Control-a>", lambda _e: (self.tree.selection_set(self.tree.get_children()), "break")[1])
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._sync())

    def update(self, stats: lib.LibraryStats) -> None:
        days, on_start = self.mgr.ctx.get_purge()
        self.last_info = act.pending_info(stats, days, on_start)
        self.info.configure(text="\n".join(self.last_info.lines()))
        self.tree.delete(*self.tree.get_children())
        self.groups = {}
        for rec in act.pending_recs(stats):
            self.groups.setdefault(f"{rec.platform}/{rec.channel}", []).append(rec)
        now = time.time()
        for index, (label, recs) in enumerate(sorted(self.groups.items(), key=lambda kv: -sum(r.size for r in kv[1]))):
            oldest = min(max(r.mtime, r.ctime) for r in recs)
            self.tree.insert("", "end", iid=f"p{index}", values=(label, f"{len(recs):,}",
                                                                   lib.fmt_size(sum(r.size for r in recs)),
                                                                   f"{(now - oldest) / 86400:.1f} days"))
        self._sync()

    def _sync(self) -> None:
        has = bool(self.groups)
        self.empty_btn.state(["!disabled"] if has else ["disabled"])
        self.restore_btn.state(["!disabled"] if has else ["disabled"])
        self.restore_sel_btn.state(["!disabled"] if self.tree.selection() else ["disabled"])
        self.open_btn.state(["!disabled"] if self.mgr.guard().library.pending_top else ["disabled"])

    def restore_selected(self) -> None:
        labels = [self.tree.item(i, "values")[0] for i in self.tree.selection()]
        recs = [r for label in labels for r in self.groups.get(label, []) if r.kind in act.DELETABLE_KINDS]
        self.mgr.restore_pending(recs)

    def open_folder(self) -> None:
        top = self.mgr.guard().library.pending_top
        opener = self.mgr.ctx.opener
        if top and os.path.isdir(top) and opener:
            opener(top)
        else:
            dialogs.show_info(self.mgr.panel.toplevel, "PendingDeletion", "The PendingDeletion folder does not exist yet.")

    def open_log(self) -> None:
        path, opener = self.mgr.ctx.log_path, self.mgr.ctx.opener
        if path and os.path.isfile(path) and opener:
            opener(path)
        else:
            dialogs.show_info(self.mgr.panel.toplevel, "Action log", "Nothing has been deleted or moved yet, so there is no log.")


# ====================================================================== Clean up tab
class CleanupTab:
    def __init__(self, mgr: Manager, parent):
        self.mgr = mgr
        self.frame = f = ttk.Frame(parent, padding=10)
        self.suggestions: dict[str, act.Suggestion] = {}
        self.stats: lib.LibraryStats | None = None
        ttk.Label(f, style="Muted.TLabel", wraplength=820, justify="left",
                  text="Ideas for freeing space. Pick one and press Review: you get the list with nothing ticked "
                       "and decide yourself. Nothing here deletes anything by itself.").pack(anchor="w")
        opts = ttk.Frame(f)
        opts.pack(fill="x", pady=(8, 4))
        ttk.Label(opts, text="Old means older than").pack(side="left")
        self.days_var = tk.StringVar(value="90")
        ttk.Entry(opts, textvariable=self.days_var, width=5).pack(side="left", padx=4)
        ttk.Label(opts, text="days").pack(side="left")
        ttk.Button(opts, text="Update", command=lambda: self.update(self.stats)).pack(side="left", padx=(6, 18))
        ttk.Label(opts, text="Free up").pack(side="left")
        self.free_var = tk.StringVar(value="100")
        ttk.Entry(opts, textvariable=self.free_var, width=7).pack(side="left", padx=4)
        ttk.Label(opts, text="GB").pack(side="left")
        self.free_btn = ttk.Button(opts, text="Suggest oldest first\u2026", command=self.free_up)
        self.free_btn.pack(side="left", padx=(6, 0))
        box = ttk.Frame(f)
        box.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(box, columns=("what", "count", "size"), show="headings", selectmode="browse", height=9)
        for cid, text, width, anchor in (("what", "Suggestion", 330, "w"), ("count", "Items", 80, "e"), ("size", "Size", 100, "e")):
            self.tree.heading(cid, text=text)
            self.tree.column(cid, width=width, anchor=anchor, stretch=(cid == "what"))
        self.tree.pack(fill="both", expand=True)
        self.detail = ttk.Label(f, text="", wraplength=820, justify="left")
        self.detail.pack(anchor="w", pady=(6, 0))
        self.review_btn = ttk.Button(f, text="Review\u2026", command=self.review)
        self.review_btn.pack(anchor="e", pady=(6, 0))
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._sync())
        self.tree.bind("<Double-1>", lambda _e: self.review())

    def update(self, stats) -> None:
        self.stats = stats
        self.tree.delete(*self.tree.get_children())
        self.suggestions = {}
        if stats is None:
            return
        try:
            days = max(1, int(float(self.days_var.get())))
        except ValueError:
            days = 90
        for sug in act.build_suggestions(stats, older_days=days):
            self.suggestions[sug.key] = sug
            self.tree.insert("", "end", iid=sug.key, values=(sug.title, f"{sug.count:,}",
                                                              lib.fmt_size(sug.bytes) if sug.recs else "-"))
        self._sync()

    def current(self) -> act.Suggestion | None:
        picked = self.tree.selection()
        return self.suggestions.get(picked[0]) if picked else None

    def _sync(self) -> None:
        sug = self.current()
        self.detail.configure(text=sug.detail if sug else "")
        self.review_btn.state(["!disabled"] if sug and sug.count else ["disabled"])

    def review(self) -> None:
        sug = self.current()
        if sug is not None:
            self.mgr.review(sug)

    def free_up(self) -> None:
        if self.stats is None:
            return
        try:
            gigabytes = float(self.free_var.get())
        except ValueError:
            return
        if gigabytes <= 0:
            return
        self.mgr.review(act.free_up_suggestion(self.stats, int(gigabytes * 1024 ** 3)))
