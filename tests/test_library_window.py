"""The Library tab (msr/library_window.py) on a hidden root with temporary folder trees."""
import os
import tempfile
import threading
import time
import tkinter as tk
import unittest
from pathlib import Path
from unittest import mock

from tkinter import ttk

from msr import dialogs
from msr import library as lib
from msr import library_window as lw
from tests.test_library import put


class Case(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.streams = self.base / "streams"
        r = self.streams
        put(r / "Processed/kick/alice/alice_20260101_120000.mp4", 4000, meta_seconds=3600)
        put(r / "Processed/kick/alice/alice_20260215_120000.mp4", 3000, meta_seconds=7200)
        put(r / "Processed/twitch/bob/bob_20260301_000000.mp4", 2000)
        put(r / "Processed/youtube/@dave/dave_20260320_101010.mp4", 400)
        put(r / "PendingDeletion/twitch/bob/old.mp4", 900)
        put(r / "Clips/twitch/bob/c.mp4", 300)
        self.cache = str(self.base / "data" / "library_cache.json")
        (self.base / "data").mkdir()

    def make(self, activate=True, **kw):
        kw.setdefault("get_roots", lambda: lib.Roots(str(self.streams)))
        kw.setdefault("get_roster", lambda: ["alice", "twitch:bob"])
        kw.setdefault("cache_path", self.cache)
        holder = ttk.Frame(self.root)
        holder.pack()
        win = lw.LibraryPanel(holder, **kw)
        win.frame.pack(fill="both", expand=True)
        self.addCleanup(win.shutdown)
        if activate:
            win.activate()
        return win

    def pump(self, cond, timeout=20.0):
        end = time.time() + timeout
        while not cond():
            self.root.update()
            time.sleep(0.02)
            if time.time() > end:
                self.fail("timed out waiting for the window")
        self.root.update()

    def scanned(self, **kw):
        win = self.make(**kw)
        self.pump(lambda: win.stats is not None and not win.busy)
        return win


class PureTests(unittest.TestCase):
    def stats(self):
        s = lib.LibraryStats()
        for i in range(15):
            ch = lib.ChannelStat("kick" if i % 2 else "twitch", f"chan{i}")
            ch.rec_bytes, ch.rec_files = (15 - i) * 1000, i + 1
            s.channels[(ch.platform, ch.name)] = ch
        s.categories = {"processed": [sum(c.rec_bytes for c in s.channels.values()), 120]}
        for name, b in (("kick", 50000), ("twitch", 30000), ("youtube", 20000), ("tiktok", 10000), ("rumble", 5000),
                        ("fishtank", 4000), ("a", 3000), ("b", 2000)):
            s.platforms[name] = [b, 10]
        s.categories["processed"][0] = 124000
        s.months = {"2026-01": [1000, 2], "2026-03": [5000, 4]}
        return s

    def test_top_channels_and_platform_rows(self):
        s = self.stats()
        bars = lw.bar_rows(s)
        self.assertEqual(len(bars), 10)
        self.assertEqual(bars[0][0], "chan0")
        self.assertIn("chan0 (Twitch)", bars[0][3])
        donut = lw.donut_rows(s)
        self.assertEqual(len(donut), lw.TOP_PLATFORMS + 1)  # six sites + one Other slice
        self.assertEqual(donut[-1][0], "Other")
        self.assertEqual(donut[-1][1], 3000 + 2000)  # the two smallest sites
        self.assertEqual(donut[0][0], "Kick")
        self.assertEqual(lw.donut_rows(lib.LibraryStats()), [])

    def test_month_rows_fill_the_gap(self):
        rows = lw.month_rows(self.stats())
        self.assertEqual([r[0] for r in rows], ["26-01", "26-02", "26-03"])
        self.assertIn("nothing recorded", rows[1][2])

    def test_sorting_and_filtering(self):
        chans = list(self.stats().channels.values())
        by_size = lw.sorted_channels(chans, "size", True)
        self.assertEqual(by_size[0].name, "chan0")
        self.assertEqual(lw.sorted_channels(chans, "name", False)[0].name, "chan0")
        self.assertEqual([c.name for c in lw.sorted_channels(chans, "name", False, "chan1")][:3], ["chan1", "chan10", "chan11"])
        self.assertTrue(all(c.platform == "kick" for c in lw.sorted_channels(chans, "size", True, "kick")))
        chans[0].in_roster = False
        chans[1].in_roster = True
        self.assertEqual(lw.sorted_channels(chans, "size", True, "", True), [chans[0]])  # unknown (None) is not 'not in roster'
        self.assertEqual(len(lw.sorted_channels(chans, "nonsense", True)), 15)  # unknown column falls back to size


class WindowTests(Case):
    def test_scan_fills_text_charts_and_table(self):
        win = self.scanned()
        text = win.headline.cget("text")
        self.assertIn("of recordings in 4 files", text)
        self.assertIn("alice", text)
        self.assertEqual(len(win.tree.get_children()), 3)
        first = win.tree.item(win.tree.get_children()[0], "values")
        self.assertEqual(first[0], "alice")
        self.assertEqual(first[-1], "yes")
        self.assertEqual(len(win.bars.data), 3)
        self.assertEqual(win.donut.center_text, lib.fmt_size(9400))
        self.assertIn("alice (Kick)", win.bars.tooltip_at(5, 6 + 12))
        self.assertIn("scanned", win.status.cget("text").lower())

    def test_buttons_follow_the_state(self):
        gate = threading.Event()

        def slow(roots, progress=None, cancel=None, **kw):
            progress and progress(1, 1, "Processed")
            cancel.wait(10)
            s = lib.LibraryStats()
            s.cancelled = True
            return s
        slow_cancel = mock.patch.object(lib, "scan_library", slow)
        with slow_cancel:
            win = self.make()
            self.root.update()
            self.assertTrue(win.busy)
            self.assertIn("disabled", win.rescan_btn.state())
            self.assertNotIn("disabled", win.cancel_btn.state())
            self.assertIn("disabled", win.hours_btn.state())
            win.cancel()
            self.assertIn("Cancelling", win.status.cget("text"))
            self.pump(lambda: not win.busy and win.stats is not None)
        self.assertTrue(win.stats.cancelled)
        self.assertIn("cancelled", win.status.cget("text").lower())
        self.assertNotIn("disabled", win.rescan_btn.state())
        self.assertIn("disabled", win.cancel_btn.state())
        self.assertFalse(win.progress.winfo_ismapped())

    def test_table_sort_filter_and_roster_toggle(self):
        win = self.scanned(get_roster=lambda: ["alice"])
        names = lambda: [win.tree.item(i, "values")[0] for i in win.tree.get_children()]
        self.assertEqual(names(), ["alice", "bob", "@dave"])  # by recordings size, biggest first
        win.sort_by("name")
        self.assertEqual(names(), ["@dave", "alice", "bob"])
        win.sort_by("name")
        self.assertEqual(names(), ["bob", "alice", "@dave"])
        win.filter_var.set("ali")
        self.assertEqual(names(), ["alice"])
        win.filter_var.set("")
        win.orphan_var.set(True)
        win._fill_tree()
        self.assertEqual(sorted(names()), ["@dave", "bob"])  # on disk but not in channels.json
        self.assertIn("\u25bc", win.tree.heading("name", "text"))  # sorted twice: descending now
        self.assertNotIn("\u25bc", win.tree.heading("size", "text"))

    def test_open_folder_uses_the_channels_folder(self):
        opened = []
        win = self.scanned(opener=opened.append)
        self.assertIn("disabled", win.open_btn.state())
        win.tree.selection_set(win.tree.get_children()[0])
        self.root.update()
        self.assertNotIn("disabled", win.open_btn.state())
        win.open_selected_folder()
        self.assertEqual(len(opened), 1)
        self.assertTrue(os.path.samefile(opened[0], self.streams / "Processed" / "kick" / "alice"))
        with mock.patch.object(dialogs, "show_error") as err:
            win.opener = mock.Mock(side_effect=OSError("no explorer"))
            win.open_selected_folder()
            err.assert_called_once()

    def test_copy_summary_puts_plain_text_on_the_clipboard(self):
        win = self.scanned()
        win.copy_summary()
        try:
            text = win.win.clipboard_get()
        except tk.TclError:
            self.skipTest("no clipboard here")
        self.assertIn("Multi-Stream Recorder library", text)
        self.assertIn("alice", text)
        self.assertNotIn(self.tmp.name, text)

    def test_count_hours_then_offer_ffprobe_for_the_rest(self):
        with mock.patch.object(dialogs, "ask_yes_no", return_value=False) as ask:
            win = self.scanned(ffprobe="ffprobe-not-needed")
            win.start_hours(False)
            self.pump(lambda: not win.busy and win.stats.seconds_total is not None)
            self.pump(lambda: ask.called)
        self.assertAlmostEqual(win.stats.seconds_total, 10800)  # two of four have length notes
        self.assertIn("2 recordings have no length", ask.call_args[0][2].replace(",", ""))
        alice_row = [win.tree.item(i, "values") for i in win.tree.get_children()][0]
        self.assertEqual(alice_row[7], "3.0 h")
        self.assertEqual(alice_row[0], "alice")

    def test_count_hours_without_ffprobe_does_not_ask(self):
        with mock.patch.object(dialogs, "ask_yes_no") as ask:
            win = self.scanned(ffprobe=None)
            win.start_hours(False)
            self.pump(lambda: not win.busy and win.stats.seconds_total is not None)
        ask.assert_not_called()

    def test_probe_pass_uses_the_probe_and_the_cache_file(self):
        with mock.patch.object(lib, "ffprobe_seconds", return_value=60.0), \
                mock.patch.object(dialogs, "ask_yes_no", return_value=True):
            win = self.scanned(ffprobe="ffprobe")
            win.start_hours(False)
            self.pump(lambda: win.stats.seconds_total is not None and not win.busy)
            # the yes answer starts the probe pass
            self.pump(lambda: win.stats.timed_files == 4 and not win.busy, timeout=30)
        self.assertAlmostEqual(win.stats.seconds_total, 10800 + 120)
        self.assertTrue(os.path.exists(self.cache))

    def test_closing_while_scanning_cancels_cleanly(self):
        def slow(roots, progress=None, cancel=None, **kw):
            cancel.wait(10)
            return lib.LibraryStats()
        with mock.patch.object(lib, "scan_library", slow):
            win = self.make()
            self.root.update()
            job = win.job
            win.frame.destroy()  # the window closing destroys the tab
            job.join(5)
            self.root.update()
        self.assertFalse(job.is_alive())
        self.assertFalse(win.exists())

    def test_nothing_is_scanned_until_the_tab_is_opened(self):
        win = self.make(activate=False)
        self.root.update()
        self.assertIsNone(win.job)
        self.assertIsNone(win.stats)
        self.assertEqual(win.status.cget("text"), "Not scanned yet.")
        self.assertIn("disabled", win.copy_btn.state())
        win.activate()
        self.pump(lambda: win.stats is not None and not win.busy)
        first = win.job
        win.activate()  # opening the tab again does not rescan
        self.assertIs(win.job, first)

    def test_scan_errors_are_shown_not_raised(self):
        with mock.patch.object(lib, "scan_library", side_effect=RuntimeError("boom")), \
                mock.patch.object(dialogs, "show_error") as err:
            win = self.make()
            self.pump(lambda: err.called)
        self.assertIn("boom", err.call_args[0][2])
        self.assertEqual(win.status.cget("text"), "Scan failed")

    def test_missing_folder_still_opens(self):
        win = self.scanned(get_roots=lambda: lib.Roots(str(self.base / "nope")))
        self.assertIn("not found: Recordings folder", win.headline.cget("text"))
        self.assertEqual(win.tree.get_children(), ())

    def test_rescan_picks_up_new_files_and_the_window_writes_nothing_in_the_library(self):
        before = sorted(str(p) for p in self.streams.rglob("*"))
        win = self.scanned()
        self.assertEqual(sorted(str(p) for p in self.streams.rglob("*")), before)
        put(self.streams / "Processed/kick/zed/z_20260401_000000.mp4", 777)
        win.start_scan()
        self.pump(lambda: len(win.tree.get_children()) == 4 and not win.busy)
        expected = sorted(before + [str(self.streams / "Processed" / "kick" / "zed"),
                                    str(self.streams / "Processed" / "kick" / "zed" / "z_20260401_000000.mp4")])
        self.assertEqual(sorted(str(p) for p in self.streams.rglob("*")), expected)  # only what the test added

    def test_theme_refresh_redraws(self):
        win = self.scanned()
        with mock.patch.object(dialogs, "is_dark", return_value=False):
            win.refresh_theme()
        win.frame.destroy()
        win.refresh_theme()  # after close: nothing happens, nothing raises


if __name__ == "__main__":
    unittest.main()