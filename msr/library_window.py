"""The Library tab: what is recorded, from which sites and channels, and how it grows.

A panel that lives in the main window's notebook (next to Status and Logs), with the Canvas
charts from msr/charts.py. The scan runs in a background thread (msr/library.py ScanJob) and
talks to the panel only through a queue that the panel polls, so the UI never freezes and the
thread never touches Tk. It starts the first time the tab is opened.

With a ManageContext the tab can also manage files (msr/library_manage.py): delete a channel's
content, drill into single files, empty PendingDeletion, clean-up suggestions. Every one of those
goes through a review window and msr/library_actions.py; without a context the tab is read-only.
"""

from __future__ import annotations

import queue
import time
import tkinter as tk
from tkinter import ttk
from typing import Callable

from msr import charts, dialogs
from msr import library as lib
from msr import library_actions as act
from msr import library_manage as manage

POLL_MS = 100
TOP_CHANNELS = 10
TOP_PLATFORMS = 6


def _dash(n: int, text: str) -> str:
    return text if n else "-"


# (id, heading, width, anchor, sort key, cell text)
COLUMNS = (
    ("name", "Channel", 150, "w", lambda c: c.name.lower(), lambda c: c.name),
    ("site", "Site", 80, "w", lambda c: lib.platform_label(c.platform).lower(),
     lambda c: lib.platform_label(c.platform)),
    ("size", "Recordings", 100, "e", lambda c: c.recordings_bytes, lambda c: _dash(c.recordings_bytes, lib.fmt_size(c.recordings_bytes))),
    ("files", "Files", 65, "e", lambda c: c.rec_files + c.raw_files, lambda c: _dash(c.rec_files + c.raw_files, f"{c.rec_files + c.raw_files:,}")),
    ("raw", "Raw .ts", 75, "e", lambda c: c.raw_bytes, lambda c: _dash(c.raw_bytes, lib.fmt_size(c.raw_bytes))),
    ("pending", "Pending delete", 105, "e", lambda c: c.pending_bytes, lambda c: _dash(c.pending_bytes, lib.fmt_size(c.pending_bytes))),
    ("clips", "Clips + shots", 100, "e", lambda c: c.clip_bytes + c.shot_bytes,
     lambda c: _dash(c.clip_bytes + c.shot_bytes, lib.fmt_size(c.clip_bytes + c.shot_bytes))),
    ("hours", "Hours", 70, "e", lambda c: -1 if c.seconds is None else c.seconds,
     lambda c: "" if c.seconds is None else lib.fmt_hours(c.seconds)),
    ("last", "Last", 90, "w", lambda c: c.last or 0, lambda c: lib._day(c.last) if c.last else "-"),
    ("roster", "In roster", 75, "w", lambda c: {True: 2, False: 1}.get(c.in_roster, 0),
     lambda c: {True: "yes", False: "no"}.get(c.in_roster, "")),
)
_COLUMN = {c[0]: c for c in COLUMNS}


def sorted_channels(channels, column: str, descending: bool, text: str = "", not_in_roster: bool = False,
                    site: str | None = None):
    """Channels filtered by a name/site substring (and optionally 'not in the roster', or one
    site) and sorted by a table column. Ties keep a stable name order."""
    needle = (text or "").strip().lower()
    rows = []
    for ch in channels:
        if site and ch.platform.lower() != site.lower():
            continue
        if needle and needle not in ch.name.lower() and needle not in lib.platform_label(ch.platform).lower():
            continue
        if not_in_roster and ch.in_roster is not False:
            continue
        rows.append(ch)
    rows.sort(key=lambda c: c.name.lower())
    rows.sort(key=_COLUMN.get(column, COLUMNS[2])[4], reverse=descending)
    return rows


def donut_rows(stats: lib.LibraryStats):
    """(label, value, legend text, tooltip, colour) for the biggest sites; the rest are one 'Other' slice."""
    total = stats.recordings_bytes
    if total <= 0:
        return []
    platforms = stats.ranked_platforms()
    shown, rest = platforms[:TOP_PLATFORMS], platforms[TOP_PLATFORMS:]
    rows = []
    for i, (p, b, f) in enumerate(shown):
        label = lib.platform_label(p)
        pct = b * 100 / total
        rows.append((label, b, f"{label}  {lib.fmt_size(b)}  ({pct:.0f}%)",
                     f"{label}: {lib.fmt_size(b)} in {f:,} files ({pct:.1f}% of recordings). Click to list its channels",
                     charts.color_for(p, i), p))
    if rest:
        b = sum(r[1] for r in rest)
        f = sum(r[2] for r in rest)
        rows.append(("Other", b, f"Other sites  {lib.fmt_size(b)}  ({b * 100 / total:.0f}%)",
                     f"{len(rest)} more sites: {lib.fmt_size(b)} in {f:,} files", charts.color_for("other")))
    return rows


def bar_rows(stats: lib.LibraryStats, count: int = TOP_CHANNELS):
    total = stats.recordings_bytes or 1
    rows = []
    for ch in [c for c in stats.ranked_channels() if c.recordings_bytes > 0][:count]:
        label = lib.platform_label(ch.platform)
        tip = (f"{ch.name} ({label}): {lib.fmt_size(ch.recordings_bytes)} in {ch.rec_files + ch.raw_files:,} files, "
               f"{ch.recordings_bytes * 100 / total:.1f}% of recordings")
        if ch.last:
            tip += f"; last {lib._day(ch.last)}"
        if ch.seconds is not None:
            tip += f"; {lib.fmt_hours(ch.seconds)}"
        rows.append((ch.name, ch.recordings_bytes, lib.fmt_size(ch.recordings_bytes), tip + ". Click to select, double-click to open",
                     charts.color_for(ch.platform), f"{ch.platform.lower()}|{ch.name.lower()}"))
    return rows


def month_rows(stats: lib.LibraryStats):
    out = []
    for key in charts.month_axis(stats.months):
        b, f = stats.months.get(key, (0, 0))
        out.append((f"{key[2:4]}-{key[5:]}", b,
                    f"{key}: {lib.fmt_size(b)} in {f:,} recordings. Click to list them" if f else f"{key}: nothing recorded",
                    key if f else None))
    return out


class LibraryPanel:
    """Build with the notebook as `container`, then notebook.add(panel.frame, ...)."""

    def __init__(self, container, get_roots: Callable[[], lib.Roots], get_roster: Callable[[], list[str]] | None = None,
                 cache_path: str | None = None, ffprobe: str | None = None,
                 opener: Callable[[str], None] | None = None, manage_ctx: manage.ManageContext | None = None):
        self.get_roots, self.get_roster = get_roots, get_roster
        self.mgr: manage.Manager | None = None
        self._manage_ctx = manage_ctx
        self.site_filter: str | None = None
        self._rescan_pending = False
        self._result_text = ""
        self.cache_path, self.ffprobe, self.opener = cache_path, ffprobe, opener
        self.stats: lib.LibraryStats | None = None
        self.job: lib.ScanJob | None = None
        self.out: queue.Queue = queue.Queue()
        self._poll_id = None
        self._probe_offered = False
        self.sort_column, self.sort_desc = "size", True
        self.rows: dict[str, lib.ChannelStat] = {}

        frame = self.frame = self.win = ttk.Frame(container, padding=10)  # self.win: the widget used for after()/clipboard

        top = ttk.Frame(frame)
        top.pack(fill="x")
        self.status = ttk.Label(top, text="Not scanned yet.")
        self.status.pack(side="left")
        self.undo_btn = ttk.Button(top, text="Undo", command=lambda: self.mgr and self.mgr.undo_last())

        bar = ttk.Frame(frame)
        bar.pack(fill="x", pady=(8, 6))
        self.rescan_btn = ttk.Button(bar, text="Rescan", command=self.start_scan)
        self.cancel_btn = ttk.Button(bar, text="Cancel", command=self.cancel)
        self.hours_btn = ttk.Button(bar, text="Count hours", command=lambda: self.start_hours(False))
        self.copy_btn = ttk.Button(bar, text="Copy summary", command=self.copy_summary)
        for b in (self.rescan_btn, self.cancel_btn, self.hours_btn, self.copy_btn):
            b.pack(side="left", padx=(0, 6))
        self.progress = ttk.Progressbar(frame, mode="indeterminate", length=200)

        self.headline = ttk.Label(frame, text="", justify="left", anchor="nw")
        self.headline.pack(fill="x", pady=(0, 8))
        frame.bind("<Configure>", lambda e: self.headline.configure(wraplength=max(300, e.width - 24)))

        self.tabs = ttk.Notebook(frame)
        self.tabs.pack(fill="both", expand=True)
        overview = ttk.Frame(self.tabs, padding=8)
        channels = ttk.Frame(self.tabs, padding=8)
        self.tabs.add(overview, text="  Overview  ")
        self.tabs.add(channels, text="  Channels  ")

        overview.columnconfigure(1, weight=1)
        overview.rowconfigure(1, weight=1)
        donut_box = ttk.LabelFrame(overview, text="Share by site", padding=6)
        donut_box.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        self.donut = charts.DonutChart(donut_box, height=190, width=380)
        self.donut.pack(fill="both", expand=True)
        bars_box = ttk.LabelFrame(overview, text="Biggest channels", padding=6)
        bars_box.grid(row=0, column=1, sticky="nsew")
        self.bars = charts.BarChart(bars_box, width=520)
        self.bars.pack(fill="both", expand=True)
        month_box = ttk.LabelFrame(overview, text="Recorded per month (finished files)", padding=6)
        month_box.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        self.months = charts.ColumnChart(month_box, height=150, width=900)
        self.months.pack(fill="both", expand=True)

        self.channels_tab = channels
        self._build_channels_tab(channels)
        self.donut.on_click = self.donut.on_double = self.pick_site
        self.bars.on_click = self.pick_channel
        self.bars.on_double = lambda key: (self.pick_channel(key), self.open_selected_folder())
        self.months.on_click = self.months.on_double = self.pick_month
        if manage_ctx is not None:
            self.mgr = manage.Manager(self, manage_ctx)

        frame.bind("<Destroy>", lambda e: self.shutdown() if e.widget is frame else None, add="+")
        self._sync_buttons()

    def activate(self) -> None:
        """The tab was opened: scan the first time (later: use Rescan)."""
        if self.stats is None and not self.busy:
            self.start_scan()

    @property
    def toplevel(self):
        return self.frame.winfo_toplevel()

    # ------------------------------------------------------------ channels tab
    def _build_channels_tab(self, tab) -> None:
        tool = ttk.Frame(tab)
        tool.pack(fill="x")
        ttk.Label(tool, text="Filter:").pack(side="left")
        self.filter_var = tk.StringVar()
        entry = ttk.Entry(tool, textvariable=self.filter_var, width=24)
        entry.pack(side="left", padx=(4, 12))
        self.filter_var.trace_add("write", lambda *_: self._fill_tree())
        self.orphan_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(tool, text="Only channels not in my roster", variable=self.orphan_var,
                        command=self._fill_tree).pack(side="left")
        self.site_btn = ttk.Button(tool, text="", command=lambda: self.pick_site(None))
        bar = ttk.Frame(tab)
        bar.pack(fill="x", pady=(6, 4))
        self.open_btn = ttk.Button(bar, text="Open folder", command=self.open_selected_folder)
        self.open_btn.pack(side="left")
        self.files_btn = ttk.Button(bar, text="Files\u2026", command=self.show_selected_files)
        self.delete_btn = ttk.Button(bar, text="Delete\u2026", command=lambda: self.delete_selected())
        if self.mgr_enabled:
            self.files_btn.pack(side="left", padx=(6, 0))
            self.delete_btn.pack(side="left", padx=(6, 0))
            hint = ("Click a heading to sort. Select channels and press Delete\u2026 (or right-click, or the Delete key): "
                    "you always get a preview first, and the default is a reversible move to PendingDeletion.")
        else:
            hint = "Click a heading to sort. This list only shows sizes; use Open folder to clean up in Explorer."
        ttk.Label(tab, foreground="gray", justify="left", wraplength=820, text=hint).pack(anchor="w", pady=(0, 4))
        box = ttk.Frame(tab)
        box.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(box, columns=[c[0] for c in COLUMNS], show="headings", selectmode="extended", height=14)
        for cid, heading, width, anchor, _k, _t in COLUMNS:
            self.tree.heading(cid, text=heading, command=lambda c=cid: self.sort_by(c))
            self.tree.column(cid, width=width, anchor=anchor, stretch=(cid == "name"))
        scroll = ttk.Scrollbar(box, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.tree.bind("<Double-1>", lambda _e: self.open_selected_folder())
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._sync_buttons())
        self.tree.bind("<Control-a>", lambda _e: (self.tree.selection_set(self.tree.get_children()), "break")[1])
        self.tree.bind("<Button-3>", self._channel_menu)
        self.menu = tk.Menu(self.tree, tearoff=0)
        self.menu.add_command(label="Open folder", command=self.open_selected_folder)
        self.menu.add_command(label="Copy summary of selection", command=self.copy_selection_summary)
        if self.mgr_enabled:
            self.tree.bind("<Delete>", lambda _e: (self.delete_selected(), "break")[1])
            self.menu.insert_command(1, label="Show files\u2026", command=self.show_selected_files)
            self.menu.add_separator()
            self.menu.add_command(label="Delete content\u2026", command=lambda: self.delete_selected())
            self.menu.add_command(label="Move to PendingDeletion\u2026",
                                  command=lambda: self.delete_selected(act.ACTION_PENDING))
            self.menu.add_command(label="Delete permanently\u2026",
                                  command=lambda: self.delete_selected(act.ACTION_PERMANENT))

    @property
    def mgr_enabled(self) -> bool:
        return self._manage_ctx is not None

    def sort_by(self, column: str) -> None:
        if column == self.sort_column:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_column, self.sort_desc = column, column not in ("name", "site")
        self._fill_tree()

    def _fill_tree(self) -> None:
        self.tree.delete(*self.tree.get_children())
        self.rows = {}
        if self.stats is None:
            return
        for index, ch in enumerate(sorted_channels(self.stats.channels.values(), self.sort_column, self.sort_desc,
                                                   self.filter_var.get(), self.orphan_var.get(), self.site_filter)):
            iid = f"c{index}"
            self.rows[iid] = ch
            self.tree.insert("", "end", iid=iid, values=[c[5](ch) for c in COLUMNS])
        for cid, heading, *_ in COLUMNS:
            arrow = (" \u25bc" if self.sort_desc else " \u25b2") if cid == self.sort_column else ""
            self.tree.heading(cid, text=heading + arrow)
        self._sync_buttons()

    def selected_channel(self) -> lib.ChannelStat | None:
        picked = self.tree.selection()
        return self.rows.get(picked[0]) if picked else None

    def selected_channels(self) -> list[lib.ChannelStat]:
        return [self.rows[i] for i in self.tree.selection() if i in self.rows]

    # -- clicking the charts / the table
    def pick_site(self, platform: str | None) -> None:
        """A donut slice was clicked: list only that site's channels (None: all sites)."""
        self.site_filter = platform
        if platform:
            self.site_btn.configure(text=f"Site: {lib.platform_label(platform)}  \u2715")
            self.site_btn.pack(side="left", padx=(12, 0))
            self.tabs.select(self.channels_tab)
        else:
            self.site_btn.pack_forget()
        self._fill_tree()

    def pick_channel(self, key: str) -> None:
        """A bar was clicked: show the Channels list with that channel selected."""
        platform, _sep, name = key.partition("|")
        self.site_filter = None
        self.site_btn.pack_forget()
        self.filter_var.set("")
        self.orphan_var.set(False)
        self._fill_tree()
        for iid, ch in self.rows.items():
            if ch.platform.lower() == platform and ch.name.lower() == name:
                self.tree.selection_set(iid)
                self.tree.see(iid)
                break
        self.tabs.select(self.channels_tab)

    def pick_month(self, month: str) -> None:
        """A month column was clicked: list that month's files."""
        if self.mgr is not None:
            self.mgr.files.set_month(month)
            self.tabs.select(self.mgr.files.frame)

    def _channel_menu(self, event) -> None:
        iid = self.tree.identify_row(event.y)
        if iid and iid not in self.tree.selection():
            self.tree.selection_set(iid)
        if self.tree.selection():
            manage.popup(self.frame, self.menu, event)

    def show_selected_files(self) -> None:
        channel = self.selected_channel()
        if channel is None or self.mgr is None:
            return
        self.mgr.files.set_channel(channel.platform, channel.name)
        self.tabs.select(self.mgr.files.frame)

    def delete_selected(self, method: str | None = None) -> None:
        if self.mgr is not None and self.tree.selection():
            self.mgr.delete_channels(self.selected_channels(), method)

    def copy_selection_summary(self) -> None:
        picked = self.selected_channels()
        if not picked:
            return
        lines = [f"{c.name} ({lib.platform_label(c.platform)}): {lib.fmt_size(c.recordings_bytes)} in "
                 f"{c.rec_files + c.raw_files:,} files" + (f", last {lib._day(c.last)}" if c.last else "")
                 for c in picked]
        self.win.clipboard_clear()
        self.win.clipboard_append("\n".join(lines))
        self.status.configure(text=f"Copied a summary of {len(picked)} channel(s)")

    # ------------------------------------------------------------ actions
    @property
    def busy(self) -> bool:
        return self.job is not None and self.job.is_alive()

    def _sync_buttons(self) -> None:
        busy = self.busy
        self.rescan_btn.state(["disabled"] if busy else ["!disabled"])
        self.cancel_btn.state(["!disabled"] if busy else ["disabled"])
        can_hours = (not busy) and self.stats is not None and bool(self.stats.videos)
        self.hours_btn.state(["!disabled"] if can_hours else ["disabled"])
        self.copy_btn.state(["!disabled"] if self.stats is not None else ["disabled"])
        channel = self.selected_channel() if hasattr(self, "tree") else None
        self.open_btn.state(["!disabled"] if channel and channel.best_folder() else ["disabled"])
        picked = bool(self.tree.selection()) if hasattr(self, "tree") else False
        for button in (self.files_btn, self.delete_btn):
            button.state(["!disabled"] if picked and not busy and self.stats is not None else ["disabled"])

    def _start(self, job: lib.ScanJob, message: str) -> None:
        self.out = queue.Queue()
        job.out = self.out
        self.job = job
        self.status.configure(text=message)
        self.progress.pack(fill="x", pady=(0, 6), before=self.headline)
        job.start()
        self._sync_buttons()  # after start(): the buttons follow job.is_alive()
        self._schedule_poll()

    def start_scan(self) -> None:
        if self.busy:
            return
        roster = None
        if self.get_roster is not None:
            try:
                roster = list(self.get_roster())
            except Exception:
                roster = None
        job = lib.ScanJob(self.get_roots(), self.out, roster=roster, cache_path=self.cache_path, ffprobe=self.ffprobe,
                          collect_files=self.mgr is not None)
        self._probe_offered = False
        self._start(job, "Scanning...")

    def begin_action(self, job, message: str) -> None:
        """Run a library_actions.ActionJob (delete / move / restore) with progress and Cancel; the
        library is rescanned afterwards so the charts and lists show what is left."""
        if self.busy:
            return
        self._rescan_pending = True
        self._start(job, message)

    def show_undo(self, show: bool) -> None:
        if show:
            self.undo_btn.pack(side="left", padx=(10, 0))
        else:
            self.undo_btn.pack_forget()

    def start_hours(self, probe: bool) -> None:
        if self.busy or self.stats is None:
            return
        job = lib.ScanJob(self.get_roots(), self.out, cache_path=self.cache_path, ffprobe=self.ffprobe,
                          hours_for=self.stats, probe_missing=probe)
        self._start(job, "Counting hours..." if not probe else "Reading lengths with ffprobe...")

    def cancel(self) -> None:
        if self.job is not None:
            self.job.cancel()
            self.status.configure(text="Cancelling...")

    def shutdown(self) -> None:
        """Stop the scan and the polling (the window is closing)."""
        if self.job is not None:
            self.job.cancel()
        if self._poll_id is not None:
            try:
                self.win.after_cancel(self._poll_id)
            except tk.TclError:
                pass
            self._poll_id = None

    def exists(self) -> bool:
        try:
            return bool(self.win.winfo_exists())
        except tk.TclError:
            return False

    def copy_summary(self) -> None:
        if self.stats is None:
            return
        self.win.clipboard_clear()
        self.win.clipboard_append(lib.summary_text(self.stats))
        self.status.configure(text="Summary copied to the clipboard")

    def open_selected_folder(self) -> None:
        channel = self.selected_channel()
        folder = channel.best_folder() if channel else None
        if not folder or self.opener is None:
            return
        try:
            self.opener(folder)
        except Exception as exc:
            dialogs.show_error(self.toplevel, "Open folder", f"Could not open the folder: {exc}")

    def refresh_theme(self) -> None:
        if not self.exists():
            return
        for chart in (self.donut, self.bars, self.months):
            chart.redraw()

    # ------------------------------------------------------------ polling
    def _schedule_poll(self) -> None:
        if self._poll_id is None and self.exists():
            self._poll_id = self.win.after(POLL_MS, self._poll)

    def _poll(self) -> None:
        self._poll_id = None
        if not self.exists():
            return
        finished = False
        try:
            while True:
                msg = self.out.get_nowait()
                kind = msg[0]
                if kind == "progress":
                    _k, files, size, label = msg
                    if label != "done":
                        self.status.configure(text=f"Scanning {label}: {files:,} files, {lib.fmt_size(size)}")
                elif kind == "done":
                    self.stats = msg[1]
                    self.render()
                    note = "cancelled (partial)" if self.stats.cancelled else \
                        f"scanned {self.stats.files_seen:,} files in {self.stats.elapsed:.1f} s"
                    text = note.capitalize() + time.strftime("  \u00b7  %H:%M")
                    if self._result_text:  # what the action just did stays visible after the rescan
                        text, self._result_text = f"{self._result_text}  \u00b7  rescanned", ""
                    self.status.configure(text=text)
                elif kind == "act_progress":
                    self.status.configure(text=f"Working: {msg[1]:,} of {msg[2]:,} done ({lib.fmt_size(msg[3])})")
                elif kind == "act_done":
                    finished = True
                    if self.mgr is not None:
                        self.mgr.finished(msg[1])
                elif kind == "hours_progress":
                    self.status.configure(text=f"Counting hours: {msg[1]:,} of {msg[2]:,} recordings")
                elif kind == "hours_done":
                    self.render()
                    finished = True
                    self._after_hours(msg[2], msg[3])
                elif kind == "error":
                    finished = True
                    self.status.configure(text="Scan failed")
                    dialogs.show_error(self.toplevel, "Library", f"The scan failed: {msg[1]}")
        except queue.Empty:
            pass
        if self.job is not None and not self.job.is_alive() and self.out.empty():
            finished = True
        if finished or (self.job is not None and not self.job.is_alive() and self.out.empty()):
            self.progress.pack_forget()
            self._sync_buttons()
        if self._rescan_pending and not self.busy and self.out.empty():
            self._rescan_pending = False
            keep = self.status.cget("text")
            self.start_scan()
            self._result_text = keep  # (shown again once the rescan has drawn the new numbers)
        if self.busy or not self.out.empty():
            # The busy bar is stepped from this poll (ttk's own animation timer would outlive the
            # widget if the app closes mid-scan).
            self.progress.step(8)
            self._schedule_poll()

    def _after_hours(self, known: int, missing: int) -> None:
        self.status.configure(text=f"Recorded time counted for {known:,} of {known + missing:,} recordings")
        if missing and self.ffprobe and not self._probe_offered and not self.stats.cancelled:
            self._probe_offered = True
            if dialogs.ask_yes_no(
                    self.toplevel, "Count hours",
                    f"{missing:,} recordings have no length saved by MSR.\n\nRead them with ffprobe? It looks at "
                    "one file at a time, gently, and can take a few minutes for a big library. You can cancel "
                    "any time and the results are remembered."):
                self.win.after(50, lambda: self.start_hours(True))

    # ------------------------------------------------------------ drawing
    def render(self) -> None:
        stats = self.stats
        if stats is None:
            return
        self.headline.configure(text="\n".join("\u2022 " + line for line in lib.summary_lines(stats)))
        self.donut.center_text = lib.fmt_size(stats.recordings_bytes) if stats.recordings_bytes else ""
        self.donut.set_data(donut_rows(stats))
        if self.mgr is not None:
            self.mgr.on_stats(stats)
        self.bars.set_data(bar_rows(stats))
        self.months.set_data(month_rows(stats))
        self._fill_tree()

