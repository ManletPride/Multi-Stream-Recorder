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

from msr import clip_core as core
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
    put(streams / "Clips/kick/alice/notes.txt", 1)  # so the scan knows alice's clips folder


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
            clip_editor=lambda p, start=None, end=None: self.calls.append(("clip", p) if start is None else ("clip", p, start, end)))
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



def clip_for(streams: Path, rec_name: str, t_in: float, t_out: float, *, sidecar=True, folder="Clips/kick/alice"):
    """A clip as the Clip Editor saves it (name pattern, optionally with its link sidecar)."""
    src = streams / "Processed/kick/alice" / rec_name
    name = core.clip_name(str(src), t_in, t_out)
    path = put(streams / folder / name, 30)
    if sidecar:
        res = core.CutResult(str(path), t_in, t_out, t_in, t_out, t_out - t_in, False)
        core.write_clip_meta(res, str(src))
    return path


class ClipLinkTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.streams = Path(self.tmp.name) / "streams"
        build(self.streams)

    def test_sidecar_round_trip(self):
        clip = clip_for(self.streams, "alice_20260101_120000.mp4", 61.5, 91.25)
        link = core.read_clip_link(str(clip))
        self.assertEqual((link["source_name"], link["in"], link["out"], link["guessed"]),
                         ("alice_20260101_120000.mp4", 61.5, 91.25, False))
        meta = json.loads(Path(core.clip_meta_path(str(clip))).read_text(encoding="utf-8"))
        self.assertEqual(meta["kind"], core.CLIP_META_KIND)
        self.assertTrue(os.path.isabs(meta["source"]))

    def test_old_clip_from_name_only(self):
        clip = clip_for(self.streams, "alice_20260101_120000.mp4", 3725, 3790, sidecar=False)
        link = core.read_clip_link(str(clip))
        self.assertTrue(link["guessed"])
        self.assertEqual((link["stem"], link["in"], link["out"]), ("alice_20260101_120000", 3725.0, 3790.0))
        self.assertIsNone(core.read_clip_link(str(put(self.streams / "Clips/kick/alice/random.mp4"))))

    def test_link_by_name_then_stem_and_loose(self):
        recs = lb.list_recordings({"processed": str(self.streams / "Processed/kick/alice"),
                                   "recorded": str(self.streams / "Recorded/kick/alice")})
        clip_for(self.streams, "alice_20260101_120000.mp4", 10, 20)
        clip_for(self.streams, "alice_20260101_120000.mp4", 5, 8, sidecar=False)
        clip_for(self.streams, "alice_20260302_100000.ts", 1, 3)   # cut live from the raw .ts
        clip_for(self.streams, "gone_20250101_000000.mp4", 1, 3)
        put(self.streams / "Clips/kick/alice/x.part.mp4", 5)
        clips = lb.list_clips(str(self.streams / "Clips/kick/alice"))
        loose = lb.link_clips(recs, clips)
        by = {r.name: r for r in recs}
        self.assertEqual([c.t_in for c in by["alice_20260101_120000.mp4"].clips], [5, 10])
        self.assertEqual(len(by["alice_20260302_100000.ts"].clips), 1)
        self.assertEqual(len(loose), 1)

    def test_moved_recording_still_links_by_name(self):
        clip_for(self.streams, "alice_20260101_120000.mp4", 10, 20)
        moved = self.streams / "Elsewhere" / "alice_20260101_120000.mp4"
        moved.parent.mkdir()
        (self.streams / "Processed/kick/alice/alice_20260101_120000.mp4").rename(moved)
        recs = lb.list_recordings({"processed": str(moved.parent)})
        self.assertEqual(lb.link_clips(recs, lb.list_clips(str(self.streams / "Clips/kick/alice"))), [])
        self.assertEqual(len(recs[0].clips), 1)


class ClipTabTests(TabTests):
    # only the clip tests here (the inherited TabTests ones already run above)
    test_overview_tabs_unchanged_and_recordings_added = test_jump_box_lists_channel_and_actions = None
    test_channels_tab_button_opens_recordings = test_sort_by_heading_toggles = test_unknown_channel_message = None

    def show_alice(self, n=4):
        self.tab.channel_var.set("alice  (Kick)")
        self.tab.jump()
        self.pump(lambda: sum(isinstance(v, lb.Recording) for v in self.tab.rows.values()) == n)

    def test_clips_column_children_and_open_source(self):
        clip_for(self.streams, "alice_20260101_120000.mp4", 61.5, 91.25)
        clip_for(self.streams, "alice_20260101_120000.mp4", 120, 130, sidecar=False)
        self.show_alice()
        rid = next(i for i, r in self.tab.rows.items() if getattr(r, "name", "") == "alice_20260101_120000.mp4")
        self.assertEqual(str(self.tab.tree.set(rid, "clips")), "2")
        kids = self.tab.tree.get_children(rid)
        self.assertEqual(len(kids), 2)
        self.assertIn("2 linked clips", self.tab.count.cget("text"))
        self.tab.tree.selection_set(kids[0])
        self.root.update()
        self.assertEqual(self.tab.clip_btn.cget("text"), "Open source")
        self.tab.play()
        self.tab.reveal()
        self.tab.open_clip_editor()
        clip = self.tab.rows[kids[0]]
        src = self.tab.rows[rid].path
        self.assertEqual(self.calls, [("play", clip.path), ("reveal", clip.path), ("clip", src, 61.5, 91.25)])
        self.tab.select_source()
        self.assertEqual(self.tab.tree.selection(), (rid,))
        self.root.update()
        self.assertEqual(self.tab.clip_btn.cget("text"), "Clip Editor")
        self.tab._build_menu()  # recording menu offers show/hide clips
        labels = [self.tab.menu.entrycget(i, "label") for i in range(self.tab.menu.index("end") + 1)
                  if self.tab.menu.type(i) == "command"]
        self.assertIn("Show / hide clips (2)", labels)
        self.tab.toggle_clips()
        self.tab.search_var.set("")  # refill keeps the row open
        again = next(i for i, r in self.tab.rows.items() if isinstance(r, lb.Recording) and r.path == src)
        self.assertTrue(self.tab.tree.item(again, "open"))

    def test_old_style_editor_callback_gets_path_only(self):
        clip_for(self.streams, "alice_20260101_120000.mp4", 1, 2)
        self.tab.clip_editor = lambda p: self.calls.append(("old", p))
        self.show_alice()
        cid = next(i for i, r in self.tab.rows.items() if isinstance(r, lb.ClipRef))
        self.tab.tree.selection_set(cid)
        self.tab.open_source()
        self.assertEqual(self.calls[-1][0], "old")



class EditorStartAtTests(unittest.TestCase):
    def test_opens_at_in_point_with_marks(self):
        from unittest import mock
        from msr import clip_editor
        try:
            root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        root.withdraw()
        self.addCleanup(root.destroy)
        with tempfile.TemporaryDirectory() as d:
            src = put(Path(d) / "Processed/kick/a/a_20260101_000000.mp4", 2000)
            ed = clip_editor.ClipEditor(root, str(src), clips_base=d, show=False, audio_factory=mock.MagicMock(),
                                        frame_grabber=lambda *_a, **_k: None, length_reader=lambda _p: 600.0,
                                        start_at=61.5, end_at=91.25)
            self.addCleanup(ed.close)
            end = time.time() + 10
            while not ed.marks.length and time.time() < end:
                root.update()
                time.sleep(0.02)
            self.assertEqual(ed.pos, 61.5)
            self.assertEqual((ed.marks.cut_in, ed.marks.cut_out), (61.5, 91.25))


if __name__ == "__main__":
    unittest.main()


def put_clip_files(streams: Path):
    """Clip files saved next to alice's recordings, plus one whose source is gone."""
    here = streams / "Processed/kick/alice"
    put(here / "alice_20260101_120000_cut_0h01m00s-0h01m10s.mp4", 7)
    put(here / "alice_20260101_120000_cut_0h01m00s-0h01m10s_01.mp4", 7)
    put(here / "alice_20260101_120000_trim_vertical.mp4", 7)
    put(here / "alice_20260215_083000_burned.mp4", 7)
    clip_for(streams, "alice_20260215_083000.mp4", 5, 9, folder="Processed/kick/alice")  # editor name + sidecar
    linked = put(here / "highlight.mp4", 7)  # any name, linked only by its sidecar
    core.write_clip_meta(core.CutResult(str(linked), 1, 4, 1, 4, 3, False), str(here / "alice_20251231_235900.mp4"))
    put(here / "gone_20250101_000000_trim.mp4", 7)


class FoldClipFilesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.streams = Path(self.tmp.name) / "streams"
        build(self.streams)
        put_clip_files(self.streams)
        self.recs = lb.list_recordings({"processed": str(self.streams / "Processed/kick/alice"),
                                        "recorded": str(self.streams / "Recorded/kick/alice")})

    def test_clip_files_fold_under_their_source(self):
        top = {r.name: r for r in lb.fold_clip_files(self.recs)}
        self.assertEqual(set(top), {"alice_20260101_120000.mp4", "alice_20260215_083000.mp4",
                                    "alice_20251231_235900.mp4", "alice_20260302_100000.ts",
                                    "gone_20250101_000000_trim.mp4"})
        self.assertEqual(len(top["alice_20260101_120000.mp4"].related), 3)
        self.assertEqual(len(top["alice_20260215_083000.mp4"].related), 2)
        self.assertEqual([c.name for c in top["alice_20251231_235900.mp4"].related], ["highlight.mp4"])
        self.assertTrue(all(not r.clips for r in top.values()))  # the Clips count is not changed

    def test_orphan_stays_marked_and_plain_names_are_left_alone(self):
        put(self.streams / "Processed/kick/alice/alice_cutting_board_20260105_100000.mp4", 7)
        recs = lb.list_recordings({"processed": str(self.streams / "Processed/kick/alice")})
        top = {r.name: r for r in lb.fold_clip_files(recs)}
        self.assertTrue(top["gone_20250101_000000_trim.mp4"].orphan)
        self.assertFalse(top["alice_cutting_board_20260105_100000.mp4"].orphan)
        self.assertFalse(top["alice_20260101_120000.mp4"].orphan)
        cell = lb._COLUMN["title"][5](top["gone_20250101_000000_trim.mp4"])
        self.assertIn("source missing", cell)


class FoldClipTabTests(ClipTabTests):
    locals().update({n: None for n in dir(ClipTabTests) if n.startswith("test_")})  # run only the test below

    def test_recordings_tab_shows_clip_files_only_as_children(self):
        put_clip_files(self.streams)
        clip_for(self.streams, "alice_20260101_120000.mp4", 61.5, 91.25)  # one Clips-folder clip
        self.show_alice(n=5)
        top = {self.tab.rows[i].name: i for i in self.tab.tree.get_children()}
        self.assertEqual(len(top), 5)
        self.assertFalse(any("_cut_" in n or "_burned" in n for n in top))
        rid = top["alice_20260101_120000.mp4"]
        self.assertEqual(str(self.tab.tree.set(rid, "clips")), "1")  # Clips column: Clips-folder links only
        self.assertEqual(len(self.tab.tree.get_children(rid)), 4)
        self.assertIn("source missing", self.tab.tree.set(top["gone_20250101_000000_trim.mp4"], "title"))
        kid = self.tab.tree.get_children(rid)[-1]
        self.tab.tree.selection_set(kid)
        self.root.update()
        self.tab.select_source()
        self.assertEqual(self.tab.tree.selection(), (rid,))
