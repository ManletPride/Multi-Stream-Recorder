"""The review-and-confirm window that stands in front of every deletion in the Library tab.

It is the dry run: it lists exactly which files would be touched (untick any), what the action
is, how much it frees (or does not), what is refused and why, and for a permanent delete it asks
for the channel's name (or DELETE) to be typed. Nothing happens until the button is pressed;
the caller then runs the returned Plan in a background thread (library_actions.ActionJob).
"""

from __future__ import annotations

import os
import time
import tkinter as tk
from tkinter import ttk
from typing import NamedTuple

from msr import charts, dialogs
from msr import library as lib
from msr import library_actions as act

ROW_CAP = 3000   # rows shown in the list (the totals always count everything ticked)
KIND_GROUPS = (  # (key, label, kinds)
    ("recordings", "Finished recordings", ("recording",)),
    ("raw", "Raw / partial files", ("raw", "partial")),
    ("clips", "Clips", ("clip",)),
    ("shots", "Screenshots", ("screenshot",)),
    ("pending", "Already in PendingDeletion", ("media",)),
)
DEFAULT_KINDS = {"recordings", "raw"}
MODES = (("all", "Everything ticked above"), ("older", "Older than N days"), ("newest_files", "All but the newest N files"),
         ("newest_gb", "All but the newest N GB"), ("range", "Recorded between two dates"))
METHOD_TEXT = {
    act.ACTION_PENDING: ("Move to PendingDeletion (recommended)",
                         "Reversible. MSR deletes the files itself after the PendingDeletion delay "
                         "(Cleanup settings), but only when MSR starts. Disk space is freed then, not now."),
    act.ACTION_TRASH: ("Move to the Recycle Bin",
                       "Windows only. Frees space when the bin is emptied. Offered only when Windows reports "
                       "a Recycle Bin big enough: it deletes for good what does not fit."),
    act.ACTION_PERMANENT: ("Delete permanently", "Gone for good, space freed now. This cannot be undone."),
    act.ACTION_RESTORE: ("Restore to the original place",
                         "Moves the files back to where they were (or Processed / Recorded if unknown). "
                         "Nothing is overwritten."),
}


class DirRow(NamedTuple):
    path: str
    size: int
    mtime: float
    ctime: float
    category: str
    platform: str
    channel: str
    kind: str
    when: float


def dir_rows(paths) -> list[DirRow]:
    rows = []
    for path in paths:
        rows.append(DirRow(path, 0, 0.0, 0.0, "", os.path.basename(os.path.dirname(path)).lower(),
                           os.path.basename(path), "folder", 0.0))
    return rows


def visible_recs(recs, extra_orphan_sidecars: bool = True) -> list:
    """The rows to list: the info files (.meta.json) that go along with a video are not listed
    separately; lone ones are."""
    stems = {os.path.splitext(r.path)[0].lower() for r in recs if r.kind != "sidecar"}
    out = []
    for r in recs:
        if r.kind == "sidecar" and r.path[:-len(lib.SIDECAR_SUFFIX)].lower() in stems:
            continue
        out.append(r)
    return out


class ActionDialog:
    """Build with the manager and a parent window; run with ask_action() (modal) or, in tests,
    drive the attributes and call confirm()."""

    def __init__(self, mgr, parent, title: str, intro: str, recs=(), dirs=(), *, filters: bool = False,
                 preselect: bool = True, methods=(act.ACTION_PENDING, act.ACTION_PERMANENT),
                 default_method: str = act.ACTION_PENDING, note: str = "", now: float | None = None):
        self.mgr, self.parent = mgr, parent
        self.recs = visible_recs(list(recs)) if not dirs else dir_rows(dirs)
        self.dirs_mode = bool(dirs)
        self.filters, self.preselect = filters and not dirs, preselect
        self.methods = tuple(methods) if not dirs else (act.ACTION_RMDIR,)
        self.now = now
        self.result: act.Plan | None = None
        self.plan: act.Plan | None = None
        self.checked: set[str] = set()
        self.rows: dict[str, object] = {}
        self._refresh_job = None
        self.win, frame = dialogs.new_dialog(parent, title)
        self.win.resizable(True, True)
        ttk.Label(frame, text=intro, wraplength=760, justify="left").pack(anchor="w")

        self.kind_vars: dict[str, tk.BooleanVar] = {}
        self.mode_var = tk.StringVar(value="all")
        self.a_var, self.b_var = tk.StringVar(value="90"), tk.StringVar()
        if self.filters:
            self._build_filters(frame)

        self.method_var = tk.StringVar(value=default_method if default_method in self.methods else self.methods[0])
        self.method_buttons: dict[str, ttk.Radiobutton] = {}
        self.method_notes: dict[str, ttk.Label] = {}
        if not self.dirs_mode:
            box = ttk.LabelFrame(frame, text="What to do", padding=6)
            box.pack(fill="x", pady=(8, 0))
            for method in self.methods:
                label, desc = METHOD_TEXT[method]
                radio = ttk.Radiobutton(box, text=label, value=method, variable=self.method_var,
                                        command=self._schedule_refresh)
                radio.pack(anchor="w")
                note_label = ttk.Label(box, text=desc, wraplength=720, justify="left", style="Muted.TLabel")
                note_label.pack(anchor="w", padx=(22, 0), pady=(0, 3))
                self.method_buttons[method], self.method_notes[method] = radio, note_label

        box = ttk.LabelFrame(frame, text="Preview: nothing has been changed yet. Untick what you want to keep",
                             padding=6)
        box.pack(fill="both", expand=True, pady=(8, 0))
        cols = ("check", "name", "channel", "size", "date", "kind")
        self.tree = ttk.Treeview(box, columns=cols, show="headings", height=7, selectmode="extended")
        for cid, text, width, anchor in (("check", "", 28, "center"), ("name", "File", 270, "w"),
                                         ("channel", "Channel", 130, "w"), ("size", "Size", 80, "e"),
                                         ("date", "Date", 85, "w"), ("kind", "Type", 100, "w")):
            self.tree.heading(cid, text=text)
            self.tree.column(cid, width=width, anchor=anchor, stretch=(cid == "name"))
        scroll = ttk.Scrollbar(box, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.tree.bind("<Button-1>", self._click)
        self.tree.bind("<space>", self._space)
        self.tree.bind("<Control-a>", lambda _e: (self.check_all(), "break")[1])
        side = ttk.Frame(frame)
        side.pack(fill="x", pady=(4, 0))
        ttk.Button(side, text="Tick all", command=self.check_all).pack(side="left")
        ttk.Button(side, text="Untick all", command=self.check_none).pack(side="left", padx=(6, 0))
        self.count_label = ttk.Label(side, text="")
        self.count_label.pack(side="left", padx=(12, 0))

        self.summary = ttk.Label(frame, text="", wraplength=760, justify="left")
        self.summary.pack(anchor="w", pady=(8, 0))
        self.warning = ttk.Label(frame, text=note, wraplength=760, justify="left", style="Error.TLabel")
        self.warning.pack(anchor="w")
        self.confirm_row = ttk.Frame(frame)
        self.confirm_label = ttk.Label(self.confirm_row, text="")
        self.confirm_label.pack(side="left")
        self.confirm_var = tk.StringVar()
        self.confirm_entry = ttk.Entry(self.confirm_row, textvariable=self.confirm_var, width=26)
        self.confirm_entry.pack(side="left", padx=(8, 0))
        self.confirm_var.trace_add("write", lambda *_: self._sync_button())
        row = ttk.Frame(frame)
        row.pack(fill="x", pady=(12, 0))
        self.cancel_btn = ttk.Button(row, text="Cancel", command=self.win.destroy)
        self.cancel_btn.pack(side="right")
        self.go_btn = ttk.Button(row, text="", command=self.confirm)
        self.go_btn.pack(side="right", padx=(0, 6))
        self.win.bind("<Destroy>", self._destroyed, add="+")
        self.win.bind("<Escape>", lambda _e: self.win.destroy())
        self.win.protocol("WM_DELETE_WINDOW", self.win.destroy)
        self._apply_scope()
        self.win.update_idletasks()
        self.win.minsize(min(760, self.win.winfo_reqwidth()), 420)

    def _destroyed(self, event) -> None:
        if event.widget is self.win and self._refresh_job is not None:
            try:
                self.win.after_cancel(self._refresh_job)
            except tk.TclError:
                pass
            self._refresh_job = None

    # ---------------------------------------------------------------- scope (filters)
    def _build_filters(self, frame) -> None:
        box = ttk.LabelFrame(frame, text="What to include", padding=6)
        box.pack(fill="x", pady=(8, 0))
        kinds_row = ttk.Frame(box)
        kinds_row.pack(fill="x")
        for key, label, kinds in KIND_GROUPS:
            count = sum(1 for r in self.recs if r.kind in kinds)
            size = sum(r.size for r in self.recs if r.kind in kinds)
            var = tk.BooleanVar(value=(key in DEFAULT_KINDS) and count > 0)
            self.kind_vars[key] = var
            check = ttk.Checkbutton(kinds_row, text=f"{label} ({count:,}, {lib.fmt_size(size)})", variable=var,
                                    command=self._scope_changed)
            check.pack(side="left", padx=(0, 10))
            if not count:
                check.state(["disabled"])
        mode_row = ttk.Frame(box)
        mode_row.pack(fill="x", pady=(6, 0))
        self.mode_box = ttk.Combobox(mode_row, state="readonly", width=30, values=[m[1] for m in MODES])
        self.mode_box.current(0)
        self.mode_box.pack(side="left")
        self.mode_box.bind("<<ComboboxSelected>>", lambda _e: self._mode_picked())
        self.a_entry = ttk.Entry(mode_row, textvariable=self.a_var, width=12)
        self.b_entry = ttk.Entry(mode_row, textvariable=self.b_var, width=12)
        self.unit_label = ttk.Label(mode_row, text="")
        for var in (self.a_var, self.b_var):
            var.trace_add("write", lambda *_: self._scope_changed())
        self._mode_picked()

    def _mode_picked(self) -> None:
        index = self.mode_box.current() if self.filters else 0
        self.mode_var.set(MODES[max(index, 0)][0])
        mode = self.mode_var.get()
        for widget in (self.a_entry, self.b_entry, self.unit_label):
            widget.pack_forget()
        if mode == "all":
            self._scope_changed()
            return
        defaults = {"older": ("90", "days"), "newest_files": ("20", "files"), "newest_gb": ("50", "GB"),
                    "range": ("", "from (YYYY-MM-DD)  to (YYYY-MM-DD)")}
        if mode != "range" or not self.a_var.get() or self.a_var.get().isdigit():
            self.a_var.set(defaults[mode][0] if mode != "range" else "")
        self.a_entry.pack(side="left", padx=(8, 4))
        if mode == "range":
            self.b_entry.pack(side="left", padx=(4, 4))
        self.unit_label.configure(text=defaults[mode][1])
        self.unit_label.pack(side="left")
        self._scope_changed()

    def scope_kwargs(self) -> dict | None:
        """The filter_recs() arguments for the chosen scope, None when the numbers are not valid."""
        mode = self.mode_var.get()
        try:
            if mode == "older":
                return {"older_than_days": float(self.a_var.get())}
            if mode == "newest_files":
                return {"keep_newest_files": int(float(self.a_var.get()))}
            if mode == "newest_gb":
                return {"keep_newest_bytes": int(float(self.a_var.get()) * 1024 ** 3)}
            if mode == "range":
                first, last = act.parse_day(self.a_var.get()), act.parse_day(self.b_var.get())
                if first is None and last is None:
                    return None
                return {"date_from": first, "date_to": last}
        except ValueError:
            return None
        return {}

    def selected_kinds(self) -> set[str]:
        kinds: set[str] = set()
        for key, _label, group in KIND_GROUPS:
            if self.kind_vars.get(key) is not None and self.kind_vars[key].get():
                kinds.update(group)
        return kinds

    def listed(self) -> list:
        if not self.filters:
            return self.recs
        kinds = self.selected_kinds()
        return [r for r in self.recs if r.kind in kinds]

    def _scope_changed(self) -> None:
        if hasattr(self, "tree"):
            self._apply_scope()

    def _apply_scope(self) -> None:
        listed = self.listed()
        if self.filters:
            kwargs = self.scope_kwargs()
            picked = act.filter_recs(listed, kinds=self.selected_kinds(), now=self.now, **kwargs) if kwargs is not None else []
            self.checked = {os.path.normcase(r.path) for r in picked}
        elif self.preselect:
            self.checked = {os.path.normcase(r.path) for r in listed}
        else:
            self.checked = set()
        self._fill(listed)
        self._schedule_refresh()

    # ---------------------------------------------------------------- list
    def _fill(self, listed) -> None:
        self.tree.delete(*self.tree.get_children())
        self.rows = {}
        shown = sorted(listed, key=lambda r: (r.when, r.path))[:ROW_CAP]
        for index, rec in enumerate(shown):
            iid = f"r{index}"
            self.rows[iid] = rec
            self.tree.insert("", "end", iid=iid, values=self._values(rec))
        self._more = max(0, len(listed) - len(shown))

    def _values(self, rec) -> list:
        mark = "\u2611" if os.path.normcase(rec.path) in self.checked else "\u2610"
        day = lib._day(rec.when) if rec.when else ""
        name = rec.channel if rec.kind == "folder" else os.path.basename(rec.path)
        return [mark, name, "" if rec.kind == "folder" else f"{rec.platform}/{rec.channel}",
                "" if rec.kind == "folder" else lib.fmt_size(rec.size), day,
                act.KIND_LABELS.get(rec.kind, "Empty folder")]

    def _toggle(self, iid: str) -> None:
        rec = self.rows.get(iid)
        if rec is None:
            return
        key = os.path.normcase(rec.path)
        self.checked.symmetric_difference_update({key})
        self.tree.item(iid, values=self._values(rec))
        self._schedule_refresh()

    def _click(self, event):
        if self.tree.identify_region(event.x, event.y) == "cell" and self.tree.identify_column(event.x) == "#1":
            self._toggle(self.tree.identify_row(event.y))
            return "break"
        return None

    def _space(self, _event):
        for iid in self.tree.selection():
            self._toggle(iid)
        return "break"

    def check_all(self) -> None:
        self.checked = {os.path.normcase(r.path) for r in self.listed()}
        self._fill(self.listed())
        self._schedule_refresh()

    def check_none(self) -> None:
        self.checked = set()
        self._fill(self.listed())
        self._schedule_refresh()

    def set_method(self, method: str) -> None:
        self.method_var.set(method)
        self._schedule_refresh()

    # ---------------------------------------------------------------- plan and buttons
    def _schedule_refresh(self) -> None:
        if self._refresh_job is None:
            try:
                self._refresh_job = self.win.after(120, self.refresh)
            except tk.TclError:
                pass

    def chosen(self) -> list:
        return [r for r in self.listed() if os.path.normcase(r.path) in self.checked]

    def build_plan(self) -> act.Plan:
        guard = self.mgr.guard()
        if self.dirs_mode:
            return act.plan_empty_dirs(guard, [r.path for r in self.chosen()])
        return act.build_plan(guard, self.chosen(), self.method_var.get())

    def refresh(self) -> None:
        if self._refresh_job is not None:  # called directly: drop the one still waiting
            try:
                self.win.after_cancel(self._refresh_job)
            except tk.TclError:
                pass
            self._refresh_job = None
        try:
            if not self.win.winfo_exists():
                return
        except tk.TclError:
            return
        plan = self.plan = self.build_plan()
        method = self.method_var.get()
        for key, radio in self.method_buttons.items():
            if key == act.ACTION_TRASH:
                blocked = self.mgr.recycle_problem(plan) if plan.ok else self.mgr.recycle_problem(None)
                radio.state(["disabled"] if blocked else ["!disabled"])
                self.method_notes[key].configure(text=METHOD_TEXT[key][1] + (f"\n\u26a0 {blocked}" if blocked else ""))
        more = f" (list shows the first {ROW_CAP:,})" if getattr(self, "_more", 0) else ""
        self.count_label.configure(text=f"{len(self.checked):,} of {len(self.listed()):,} ticked{more}")
        lines = plan.summary_lines() if plan.ok else ["Nothing is ticked, or everything ticked is refused."]
        self.summary.configure(text="\n".join(lines))
        warns = []
        for label, why in plan.blocked.items():
            warns.append(f"{label}: {why}")
        if plan.skipped:
            reasons: dict[str, int] = {}
            for _path, why in plan.skipped:
                reasons[why] = reasons.get(why, 0) + 1
            warns.append(f"{len(plan.skipped):,} file(s) will be left alone: "
                         + "; ".join(f"{n}\u00d7 {why}" for why, n in list(reasons.items())[:3]))
        if not self.mgr.log_ok():
            warns.append("MSR cannot write its action log in the data folder, so it will not change files.")
        self.warning.configure(text="\n".join(warns))
        if method == act.ACTION_PERMANENT and not self.dirs_mode:
            self.confirm_label.configure(text=f"Type  {plan.confirm_word}  to confirm:")
            if not self.confirm_row.winfo_ismapped():
                self.confirm_row.pack(anchor="w", pady=(6, 0), before=self.go_btn.master)
        else:
            self.confirm_row.pack_forget()
        self._sync_button()

    def _sync_button(self) -> None:
        plan = self.plan
        method = self.method_var.get()
        if self.dirs_mode:
            text = f"Remove {plan.folders if plan else 0} empty folder(s)"
        else:
            verb = {act.ACTION_PENDING: "Move {n} file(s) to PendingDeletion", act.ACTION_TRASH: "Recycle {n} file(s)",
                    act.ACTION_PERMANENT: "Delete {n} file(s) permanently",
                    act.ACTION_RESTORE: "Restore {n} file(s)"}[method]
            text = verb.format(n=f"{plan.files:,}" if plan else 0)
        self.go_btn.configure(text=text, width=max(14, len(text) + 2))
        self.go_btn.state(["!disabled"] if self.allowed() else ["disabled"])

    def allowed(self) -> bool:
        plan = self.plan
        if plan is None or not plan.ok or not self.mgr.log_ok():
            return False
        method = self.method_var.get()
        if method == act.ACTION_TRASH and self.mgr.recycle_problem(plan):
            return False
        if method == act.ACTION_PERMANENT and not self.dirs_mode and self.confirm_var.get().strip() != plan.confirm_word:
            return False
        return True

    def confirm(self) -> bool:
        """Run again against the files as they are right now, then hand the plan to the caller."""
        if self._refresh_job is not None:
            try:
                self.win.after_cancel(self._refresh_job)
            except tk.TclError:
                pass
            self._refresh_job = None
        self.refresh()
        if not self.allowed():
            return False
        self.result = self.build_plan()
        if not self.result.ok:
            self.result = None
            return False
        self.win.destroy()
        return True


def ask_action(mgr, parent, *args, **kwargs) -> act.Plan | None:
    """Open the dialog modally; the confirmed Plan, or None when cancelled."""
    dialog = ActionDialog(mgr, parent, *args, **kwargs)
    dialogs.run_modal(dialog.win, parent, dialog.tree)
    return dialog.result
