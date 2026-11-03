"""Library > Recordings (msr/library_browser.py): per-channel list, search, sort, actions. Temp trees only."""
import datetime
import json
import os
import tempfile
import time
import tkinter as tk
import unittest
from pathlib import Path
from tkinter import ttk

from msr import library as lib
from msr import library_browser as lb
from msr import library_window as lw


def put(path: Path, size=10, meta=None, mtime=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    if meta is not None:
        path.with_name(path.name.rsplit(".", 1)[0] + ".meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return path


def build(streams: Path):
    put(streams / "Processed/kick/alice/alice_20260101_120000.mp4", 400,
        {"duration_seconds": 3661, "stream_title": "New year chill stream", "recording_started": "2026-01-01 12:00:00"})
    put(streams / "Processed/kick/alice/alice_20260215_083000.mp4", 300, {"duration_seconds": 60, "stream_title": "IRL Tokyo walk"})
    put(streams / "Processed/kick/alice/alice_20251231_235900.mp4", 200)  # no sidecar
    put(streams / "Processed/kick/alice/alice_20260301_100000.mp4.part", 50)  # partial: hidden
    put(streams / "Recorded/kick/alice/alice_20260302_100000.ts", 100)        # raw, still waiting
    put(streams / "Processed/twitch/bob/bob_20260301_000000.mp4", 20, {"stream_title": "bob plays"})


class PureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.streams = Path(self.tmp.name) / "streams"
        build(self.streams)
        self.folders = {"processed": str(self.streams / "Processed/kick/alice"),
                        "recorded": str(self.streams / "Recorded/kick/alice")}

    def test_list_reads_title_length_and_skips_partials(self):
        recs = {r.name: r for r in lb.list_recordings(self.folders)}
        self.assertEqual(set(recs), {"alice_20260101_120000.mp4", "alice_20260215_083000.mp4",
                                     "alice_20251231_235900.mp4", "alice_20260302_100000.ts"})
        first = recs["alice_20260101_120000.mp4"]
        self.assertEqual(first.title, "New year chill stream")
        self.assertEqual(first.seconds, 3661)
        self.assertEqual(lb.fmt_length(first.seconds), "1:01:01")
        self.assertEqual(lb.fmt_when(first.when), "2026-01-01 12:00")
        self.assertEqual(lb.fmt_when(recs["alice_20260215_083000.mp4"].when), "2026-02-15 08:30")  # from the name
        self.assertIsNone(recs["alice_20251231_235900.mp4"].seconds)
        self.assertEqual(recs["alice_20251231_235900.mp4"].title, "")
        self.assertTrue(recs["alice_20260302_100000.ts"].raw)

    def test_default_newest_first_and_sorting(self):
        recs = lb.list_recordings(self.folders)
        names = [r.name for r in lb.filter_sort(recs)]
        self.assertEqual(names[0], "alice_20260302_100000.ts")
        self.assertEqual(names[-1], "alice_20251231_235900.mp4")
        self.assertEqual(lb.filter_sort(recs, column="length", descending=True)[0].seconds, 3661)
        self.assertEqual(lb.filter_sort(recs, column="date", descending=False)[0].name, "alice_20251231_235900.mp4")

    def test_search_title_and_filename_all_words(self):
        recs = lb.list_recordings(self.folders)
        self.assertEqual([r.name for r in lb.filter_sort(recs, "tokyo")], ["alice_20260215_083000.mp4"])
        self.assertEqual([r.name for r in lb.filter_sort(recs, "CHILL new")], ["alice_20260101_120000.mp4"])
        self.assertEqual([r.name for r in lb.filter_sort(recs, "20251231")], ["alice_20251231_235900.mp4"])
        self.assertEqual(lb.filter_sort(recs, "nothing here"), [])

    def test_missing_folder_and_bad_meta(self):
        bad = put(self.streams / "Processed/kick/carol/c_20260101_000000.mp4", 5)
        bad.with_name("c_20260101_000000.meta.json").write_text("{not json", encoding="utf-8")
        recs = lb.list_recordings({"processed": str(bad.parent), "recorded": str(self.streams / "nope")})
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].title, "")

    def test_match_channels_prefix_first(self):
        labels = ["bob  (Twitch)", "alice  (Kick)", "malice  (Kick)"]
        self.assertEqual(lb.match_channels(labels, "ali")[0], "alice  (Kick)")
        self.assertEqual(lb.match_channels(labels, "ali"), ["alice  (Kick)", "malice  (Kick)"])
        self.assertEqual(lb.match_channels(labels, ""), labels)

    def test_stamp_from_name(self):
        ts = lb.stamp_from_name("x_20260215_0830.mp4")
        self.assertEqual(datetime.datetime.fromtimestamp(ts).strftime("%H:%M"), "08:30")
        self.assertIsNone(lb.stamp_from_name("plain.mp4"))


class TabTests(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.streams = Path(self.tmp.name) / "streams"
        build(self.streams)
        self.calls = []
        holder = ttk.Frame(self.root)
        holder.pack()
        self.panel = lw.LibraryPanel(
            holder, get_roots=lambda: lib.Roots(str(self.streams)), cache_path=str(Path(self.tmp.name) / "c.json"),
            opener=lambda p: self.calls.append(("play", p)), revealer=lambda p: self.calls.append(("reveal", p)),
            clip_editor=lambda p: self.calls.append(("clip", p)))
        self.panel.frame.pack(fill="both", expand=True)
        self.addCleanup(self.panel.shutdown)
        self.panel.activate()
        self.pump(lambda: self.panel.stats is not None and not self.panel.busy)
        self.tab = self.panel.recordings

    def pump(self, cond, timeout=20.0):
        end = time.time() + timeout
        while not cond():
            self.root.update()
            time.sleep(0.02)
            if time.time() > end:
                self.fail("timed out")
        self.root.update()

    def test_overview_tabs_unchanged_and_recordings_added(self):
        texts = [self.panel.tabs.tab(t, "text").strip() for t in self.panel.tabs.tabs()]
        self.assertEqual(texts[:3], ["Overview", "Channels", "Recordings"])

    def test_jump_box_lists_channel_and_actions(self):
        self.assertIn("alice  (Kick)", self.tab.channel_box.cget("values"))
        self.tab.channel_var.set("ali")
        self.tab.jump()
        self.pump(lambda: len(self.tab.rows) == 4)
        first = self.tab.rows[self.tab.tree.get_children()[0]]
        self.assertEqual(first.name, "alice_20260302_100000.ts")  # newest first
        self.assertEqual(str(self.tab.reveal_btn.state()), "('disabled',)")
        self.tab.search_var.set("tokyo")
        kids = self.tab.tree.get_children()
        self.assertEqual(len(kids), 1)
        self.assertIn("1 of 4", self.tab.count.cget("text"))
        self.tab.tree.selection_set(kids[0])
        self.root.update()
        self.tab.reveal()
        self.tab.play()
        self.tab.open_clip_editor()
        path = self.tab.rows[kids[0]].path
        self.assertEqual(self.calls, [("reveal", path), ("play", path), ("clip", path)])

    def test_channels_tab_button_opens_recordings(self):
        tree = self.panel.tree
        iid = next(i for i, c in self.panel.rows.items() if c.name == "bob")
        tree.selection_set(iid)
        self.root.update()
        self.panel.show_selected_recordings()
        self.pump(lambda: len(self.tab.rows) == 1)
        self.assertEqual(self.panel.tabs.select(), str(self.tab.frame))
        self.assertEqual(next(iter(self.tab.rows.values())).title, "bob plays")

    def test_sort_by_heading_toggles(self):
        self.tab.channel_var.set("alice  (Kick)")
        self.tab.jump()
        self.pump(lambda: len(self.tab.rows) == 4)
        self.tab.sort_by("date")  # same column: oldest first now
        self.assertEqual(self.tab.rows[self.tab.tree.get_children()[0]].name, "alice_20251231_235900.mp4")

    def test_unknown_channel_message(self):
        self.tab.channel_var.set("zzz")
        self.tab.jump()
        self.assertIn("No channel matches", self.tab.count.cget("text"))


if __name__ == "__main__":
    unittest.main()
