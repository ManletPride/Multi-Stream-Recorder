"""Library > Recordings: one channel's recordings, newest first, with title search.

Pick a channel in the jump box (type to narrow it, Enter or pick from the list) or from the
Channels tab ("Recordings" button / menu). The list shows date, length, stream title and file;
length and title come from the .meta.json MSR writes next to each recording (no ffprobe).
Listing runs on a worker thread that only posts to a queue; the tab drains it with after().
Read-only: Open folder (Explorer with the file selected), Play, Open in Clip Editor.

Clips: each recording row expands to the clips cut from it in the Clip Editor. The link comes
from the clip's .meta.json (msr.clip_core.write_clip_meta), else from the editor's clip name
pattern; a clip is matched to a recording by file name (stem), so moved recordings still match.
Clip files sitting next to the recordings (cut/trim/vertical/burned, a clip sidecar, or the
editor's name pattern) are folded under their source row; with no source they stay, marked.
"""

from __future__ import annotations

import queue
import threading
import tkinter as tk
from tkinter import ttk
from typing import Callable

import inspect

from msr import dialogs
from msr import library as lib
from msr.recordings_index import (  # noqa: F401  (Tk-free part, re-exported)
    CLIP_EXT, COLUMNS, LIST_CATEGORIES, _COLUMN, _DERIVED_RE, _STAMP_RE, ClipRef, Recording, _clip_source,
    _meta_start, channel_label, clip_cells, filter_sort, fmt_length, fmt_when, fold_clip_files, link_clips,
    list_clips, list_recordings, make_recording, match_channels, read_meta, stamp_from_name)

POLL_MS = 100


# ------------------------------------------------------------------ the tab
class RecordingsTab:
    def __init__(self, parent, panel, *, opener: Callable[[str], None] | None = None,
                 revealer: Callable[[str], None] | None = None, clip_editor: Callable[[str], None] | None = None):
        self.panel = panel
        self.opener, self.revealer, self.clip_editor = opener, revealer, clip_editor
        self.channels: dict[str, lib.ChannelStat] = {}   # label -> channel
        self.current: lib.ChannelStat | None = None
        self.recs: list[Recording] = []
        self.rows: dict[str, object] = {}   # iid -> Recording or ClipRef
        self.parent_of: dict[str, Recording] = {}   # clip iid -> its recording
        self.loose = 0                      # clips whose recording was not found
        self._open: set[str] = set()        # recordings expanded to show clips (by path)
        self.sort_col, self.sort_desc = "date", True
        self.out: queue.Queue = queue.Queue()
        self._gen = 0
        self._cancel: threading.Event | None = None
        self._poll_id = None

        self.frame = f = ttk.Frame(parent, padding=8)
        top = ttk.Frame(f)
        top.pack(fill="x")
        ttk.Label(top, text="Channel:").pack(side="left")
        self.channel_var = tk.StringVar()
        self.channel_box = ttk.Combobox(top, textvariable=self.channel_var, width=22, values=[])  # in characters: DejaVu (Linux) is wider than Segoe UI
        self.channel_box.pack(side="left", padx=(4, 10))
        self.channel_box.bind("<<ComboboxSelected>>", lambda _e: self.jump())
        self.channel_box.bind("<Return>", lambda _e: (self.jump(), "break")[1])
        self.channel_box.bind("<KeyRelease>", self._typed)
        ttk.Label(top, text="Search:").pack(side="left")
        self.search_var = tk.StringVar()
        self.search = ttk.Entry(top, textvariable=self.search_var, width=18)
        self.search.pack(side="left", padx=(4, 10))
        self.search_var.trace_add("write", lambda *_: self.fill())
        self.search.bind("<Escape>", lambda _e: self.search_var.set(""))
        self.clip_btn = ttk.Button(top, text="Clip Editor", command=self.open_clip_editor)
        self.play_btn = ttk.Button(top, text="Play", command=self.play)
        self.reveal_btn = ttk.Button(top, text="Open folder", command=self.reveal)
        for b in (self.clip_btn, self.play_btn, self.reveal_btn):
            b.pack(side="right", padx=(6, 0))
        self.count = ttk.Label(f, text="Pick a channel (type a few letters, Enter).", style="Muted.TLabel")
        self.count.pack(anchor="w", pady=(4, 2))

        box = ttk.Frame(f)
        box.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(box, columns=[c[0] for c in COLUMNS], show=("tree", "headings"), selectmode="browse",
                                 height=14)
        self.tree.column("#0", width=24, minwidth=24, stretch=False)  # expander for clips
        for cid, heading, width, anchor, _k, _t in COLUMNS:
            self.tree.heading(cid, text=heading, command=lambda c=cid: self.sort_by(c))
            self.tree.column(cid, width=width, anchor=anchor, stretch=cid in ("title", "file"))
        scroll = ttk.Scrollbar(box, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.tree.bind("<Double-1>", self._double)
        self.tree.bind("<<TreeviewOpen>>", lambda _e: self._opened(True))
        self.tree.bind("<<TreeviewClose>>", lambda _e: self._opened(False))
        self.tree.bind("<Return>", lambda _e: self.play())
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._sync())
        self.tree.bind("<Button-3>", self._menu)
        self.menu = tk.Menu(self.tree, tearoff=0)   # filled per row in _menu()
        if clip_editor is None:
            self.clip_btn.pack_forget()
        self.frame.bind("<Destroy>", lambda e: self.shutdown() if e.widget is self.frame else None, add="+")
        self._sync()

    # -- channels
    def set_stats(self, stats: lib.LibraryStats) -> None:
        chans = [c for c in stats.ranked_channels() if any(k in c.paths for k in LIST_CATEGORIES)]
        self.channels = {channel_label(c): c for c in chans}
        self.channel_box.configure(values=sorted(self.channels, key=str.lower))
        if self.current is not None:  # rescan: reload the same channel with fresh paths
            key = (self.current.platform.lower(), self.current.name.lower())
            again = next((c for c in chans if (c.platform.lower(), c.name.lower()) == key), None)
            if again is not None:
                self.show_channel(again)

    def _typed(self, event) -> None:
        if event.keysym in ("Return", "Escape", "Up", "Down", "Tab"):
            return
        labels = sorted(self.channels, key=str.lower)
        self.channel_box.configure(values=match_channels(labels, self.channel_var.get()))

    def jump(self) -> None:
        """Open the channel typed or picked in the jump box (best match for partial text)."""
        text = self.channel_var.get()
        label = text if text in self.channels else next(iter(match_channels(sorted(self.channels, key=str.lower), text)), None)
        if label is None:
            self.count.configure(text=f"No channel matches '{text.strip()}'.")
            return
        self.show_channel(self.channels[label])

    def show_channel(self, ch: lib.ChannelStat) -> None:
        self.current = ch
        self.channel_var.set(channel_label(ch))
        self.channel_box.configure(values=sorted(self.channels, key=str.lower))
        self.recs = []
        self.fill()
        self.count.configure(text=f"Listing {ch.name}...")
        if self._cancel is not None:
            self._cancel.set()
        self._gen += 1
        gen, cancel, out = self._gen, threading.Event(), self.out
        self._cancel = cancel
        folders = {k: v for k, v in ch.paths.items() if k in LIST_CATEGORIES}
        clips_dir = ch.paths.get("clips")

        def work():  # worker: no Tk here, only the queue
            try:
                recs = fold_clip_files(list_recordings(folders, cancel))
                loose = link_clips(recs, list_clips(clips_dir, cancel))
                out.put((gen, recs, len(loose), None))
            except Exception as exc:  # pragma: no cover - reported in the tab
                out.put((gen, [], 0, exc))
        threading.Thread(target=work, daemon=True, name="msr-library-recordings").start()
        self._schedule_poll()

    def _schedule_poll(self) -> None:
        if self._poll_id is None:
            self._poll_id = self.frame.after(POLL_MS, self._poll)

    def _poll(self) -> None:
        self._poll_id = None
        try:
            while True:
                gen, recs, loose, err = self.out.get_nowait()
                if gen != self._gen:
                    continue  # an older channel's listing
                self.recs, self.loose = recs, loose
                self.fill()
                if err is not None:
                    self.count.configure(text=f"Could not list the folder: {err}")
                return
        except queue.Empty:
            pass
        self._schedule_poll()

    def shutdown(self) -> None:
        if self._cancel is not None:
            self._cancel.set()
        if self._poll_id is not None:
            try:
                self.frame.after_cancel(self._poll_id)
            except tk.TclError:
                pass
            self._poll_id = None

    # -- list
    def sort_by(self, column: str) -> None:
        if column == self.sort_col:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_col, self.sort_desc = column, column in ("date", "length", "size")
        self.fill()

    def fill(self) -> None:
        keep = self.selected_item()
        self.tree.delete(*self.tree.get_children())
        self.rows, self.parent_of = {}, {}
        shown = filter_sort(self.recs, self.search_var.get(), self.sort_col, self.sort_desc)
        for i, r in enumerate(shown):
            iid = f"r{i}"
            self.rows[iid] = r
            self.tree.insert("", "end", iid=iid, values=[c[5](r) for c in COLUMNS], open=r.path in self._open)
            for j, clip in enumerate(r.clips + r.related):
                cid = f"{iid}c{j}"
                self.rows[cid], self.parent_of[cid] = clip, r
                self.tree.insert(iid, "end", iid=cid, values=clip_cells(clip))
        if keep is not None:
            for iid, item in self.rows.items():
                if item.path == keep.path:
                    self.tree.selection_set(iid)
                    self.tree.see(iid)
                    break
        for cid, heading, *_ in COLUMNS:
            arrow = (" \u25bc" if self.sort_desc else " \u25b2") if cid == self.sort_col else ""
            self.tree.heading(cid, text=heading + arrow)
        if self.current is not None:
            total = sum(r.seconds or 0 for r in shown)
            extra = f", {fmt_length(total)} known length" if total else ""
            of = f" of {len(self.recs):,}" if len(shown) != len(self.recs) else ""
            nclips = sum(len(r.clips) for r in shown)
            extra += f", {nclips:,} linked clips" if nclips else ""
            extra += f" ({self.loose:,} clips match no recording here)" if self.loose else ""
            self.count.configure(text=f"{self.current.name}: {len(shown):,}{of} recordings{extra}")
        self._sync()

    def selected_item(self):
        picked = self.tree.selection()
        return self.rows.get(picked[0]) if picked else None

    def selected(self) -> Recording | None:
        item = self.selected_item()
        return item if isinstance(item, Recording) else None

    def selected_clip(self) -> ClipRef | None:
        item = self.selected_item()
        return item if isinstance(item, ClipRef) else None

    def source_of(self, clip: ClipRef) -> Recording | None:
        return next((r for iid, r in self.parent_of.items() if self.rows.get(iid) is clip), None)

    def _opened(self, is_open: bool) -> None:
        iid = self.tree.focus()
        rec = self.rows.get(iid)
        if isinstance(rec, Recording):
            (self._open.add if is_open else self._open.discard)(rec.path)

    def _double(self, event):
        if self.tree.identify_row(event.y) and self.tree.identify_column(event.x) != "#0":
            self.play()
            return "break"
        return None

    def _sync(self) -> None:
        item = self.selected_item()
        clip = isinstance(item, ClipRef)
        self.clip_btn.configure(text="Open source" if clip else "Clip Editor")
        for b, fn in ((self.reveal_btn, self.revealer), (self.play_btn, self.opener), (self.clip_btn, self.clip_editor)):
            b.state(["!disabled"] if item is not None and fn is not None else ["disabled"])

    # -- actions
    def _run(self, fn, title: str) -> None:
        rec = self.selected_item()
        if rec is None or fn is None:
            return
        try:
            fn(rec.path)
        except Exception as exc:
            dialogs.show_error(self.frame.winfo_toplevel(), title, f"Could not open it: {exc}")

    def reveal(self) -> None:
        self._run(self.revealer, "Open folder")

    def play(self) -> None:
        self._run(self.opener, "Play")

    def open_clip_editor(self) -> None:
        if self.selected_clip() is not None:
            self.open_source()
            return
        rec = self.selected()
        if rec is not None and not rec.path.lower().endswith(CLIP_EXT):
            dialogs.show_info(self.frame.winfo_toplevel(), "Clip Editor", "The Clip Editor opens video files only.")
            return
        self._run(self.clip_editor, "Clip Editor")

    def open_source(self) -> None:
        """A clip's recording in the Clip Editor at the clip's in point (with in/out marked)."""
        clip = self.selected_clip()
        rec = self.source_of(clip) if clip is not None else None
        if rec is None or self.clip_editor is None:
            return
        if not rec.path.lower().endswith(CLIP_EXT):
            self.select_source()
            return
        try:
            if _takes_times(self.clip_editor):
                self.clip_editor(rec.path, clip.t_in, clip.t_out)
            else:
                self.clip_editor(rec.path)
        except Exception as exc:
            dialogs.show_error(self.frame.winfo_toplevel(), "Open source", f"Could not open it: {exc}")

    def select_source(self) -> None:
        picked = self.tree.selection()
        if picked and self.tree.parent(picked[0]):
            parent = self.tree.parent(picked[0])
            self.tree.selection_set(parent)
            self.tree.focus(parent)
            self.tree.see(parent)

    def toggle_clips(self) -> None:
        picked = self.tree.selection()
        if picked and self.tree.get_children(picked[0]):
            iid = picked[0]
            is_open = not self.tree.item(iid, "open")
            self.tree.item(iid, open=is_open)
            (self._open.add if is_open else self._open.discard)(self.rows[iid].path)

    def _copy(self, what: str) -> None:
        item = self.selected_item()
        if item is None:
            return
        self.frame.clipboard_clear()
        self.frame.clipboard_append(getattr(item, "title", item.name) if what == "title" else item.path)

    def _build_menu(self) -> None:
        m = self.menu
        m.delete(0, "end")
        m.add_command(label="Play", command=self.play)
        m.add_command(label="Open folder (select file)", command=self.reveal)
        if self.selected_clip() is not None:
            if self.clip_editor is not None:
                m.add_command(label="Open source in Clip Editor (at in point)", command=self.open_source)
            m.add_command(label="Select source recording", command=self.select_source)
        else:
            if self.clip_editor is not None:
                m.add_command(label="Open in Clip Editor", command=self.open_clip_editor)
            rec = self.selected()
            if rec is not None and (rec.clips or rec.related):
                m.add_command(label=f"Show / hide clips ({len(rec.clips) + len(rec.related)})", command=self.toggle_clips)
            m.add_separator()
            m.add_command(label="Copy title", command=lambda: self._copy("title"))
        m.add_command(label="Copy path", command=lambda: self._copy("path"))

    def _menu(self, event) -> None:
        iid = self.tree.identify_row(event.y)
        if iid:
            self.tree.selection_set(iid)
        if self.selected_item() is not None:
            self._build_menu()
            from msr.library_manage import popup
            popup(self.frame, self.menu, event)


def _takes_times(fn) -> bool:
    """True if the Clip Editor callback accepts (path, start, end)."""
    try:
        inspect.signature(fn).bind("p", 0.0, 1.0)
        return True
    except (TypeError, ValueError):
        return False
