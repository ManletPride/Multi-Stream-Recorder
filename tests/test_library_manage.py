"""The Library tab's file management (msr/library_manage.py, library_dialog.py) on a hidden root.
Everything happens in temporary folders; nothing here can reach a real library."""
import json
import os
import time
from pathlib import Path
from unittest import mock

from msr import dialogs
from msr import library as lib
from msr import library_actions as act
from msr import library_dialog as ldialog
from msr import library_manage as manage
from tests.test_library import put
from tests.test_library_window import Case

OLD = time.time() - 40 * 86400


class ManageCase(Case):
    """A library whose files are all 40 days old (nothing is 'recent'), with an action log in a temp data dir."""

    def setUp(self):
        super().setUp()
        for dirpath, _dirs, files in os.walk(self.streams):
            for name in files:
                os.utime(os.path.join(dirpath, name), (OLD, OLD))
        self.data = self.base / "data"
        self.busy: tuple = (set(), set())
        self.opened: list = []
        self.revealed: list = []
        self.infos: list = []
        for name in ("show_info", "show_warning", "show_error"):
            patcher = mock.patch.object(dialogs, name, side_effect=lambda parent, title, msg, _n=name: self.infos.append(msg))
            patcher.start()
            self.addCleanup(patcher.stop)
        self.dialog = None

    def ctx(self, **kw) -> manage.ManageContext:
        kw.setdefault("data_dir", str(self.data))
        kw.setdefault("log_path", str(self.data / "library_actions.log"))
        kw.setdefault("manifest_path", str(self.data / "library_moves.json"))
        kw.setdefault("get_busy", lambda: self.busy)
        kw.setdefault("revealer", self.revealed.append)
        kw.setdefault("opener", self.opened.append)
        return manage.ManageContext(**kw)

    def managed(self, **kw):
        win = self.scanned(manage_ctx=self.ctx(**kw))
        win.mgr.ask = self.confirming_ask(win.mgr)
        return win

    def confirming_ask(self, mgr, tweak=None):
        def ask(parent, title, intro, recs=(), dirs=(), **kw):
            dialog = self.dialog = ldialog.ActionDialog(mgr, parent, title, intro, recs, dirs, **kw)
            self.addCleanup(lambda: dialog.win.winfo_exists() and dialog.win.destroy())
            dialog.refresh()  # (the window does this a moment after opening)
            if tweak:
                tweak(dialog)
            if dialog.method_var.get() == act.ACTION_PERMANENT and dialog.plan and dialog.plan.ok:
                dialog.confirm_var.set(dialog.plan.confirm_word)
            dialog.confirm()
            return dialog.result
        return ask

    def settle(self, win):
        """Wait for the action to finish and the automatic rescan after it."""
        self.pump(lambda: not win.busy and not win._rescan_pending and win.stats is not None and win.out.empty())

    def channel(self, win, name):
        return next(c for c in win.stats.channels.values() if c.name == name)

    def exists(self, *parts):
        return (self.streams.joinpath(*parts)).exists()


class DeleteChannelTests(ManageCase):
    def test_default_is_a_reversible_move_to_pending_deletion(self):
        win = self.managed()
        win.mgr.delete_channels([self.channel(win, "alice")])
        self.assertEqual(self.dialog.method_var.get(), act.ACTION_PENDING)
        self.settle(win)
        self.assertFalse(self.exists("Processed/kick/alice/alice_20260101_120000.mp4"))
        self.assertTrue(self.exists("PendingDeletion/kick/alice/alice_20260101_120000.mp4"))
        self.assertTrue(self.exists("Processed/twitch/bob/bob_20260301_000000.mp4"))  # other channels untouched
        self.assertIn("Moved to PendingDeletion: 4 file(s)", win.status.cget("text"))  # two videos and their two metadata files
        self.assertTrue(win.undo_btn.winfo_ismapped() or win.undo_btn.winfo_manager())
        log = (self.data / "library_actions.log").read_text(encoding="utf-8")
        self.assertIn("alice_20260101_120000.mp4", log)

    def test_pending_move_resets_the_clock_and_undo_restores(self):
        win = self.managed()
        win.mgr.delete_channels([self.channel(win, "alice")])
        self.settle(win)
        moved = self.streams / "PendingDeletion/kick/alice/alice_20260101_120000.mp4"
        self.assertGreater(moved.stat().st_mtime, time.time() - 120)  # the purge clock starts at the move
        win.mgr.undo_last()
        self.settle(win)
        back = self.streams / "Processed/kick/alice/alice_20260101_120000.mp4"
        self.assertTrue(back.exists())
        self.assertAlmostEqual(back.stat().st_mtime, OLD, delta=2)  # original date back
        self.assertFalse(self.exists("PendingDeletion/kick/alice/alice_20260101_120000.mp4"))
        self.assertIsNone(win.mgr.last_batch)

    def test_permanent_delete_needs_the_typed_word(self):
        win = self.managed()
        asked = []

        def tweak(dialog):
            dialog.set_method(act.ACTION_PERMANENT)
            dialog.refresh()
            asked.append((dialog.confirm_word_needed if hasattr(dialog, "confirm_word_needed") else dialog.plan.confirm_word,
                          dialog.allowed()))
        win.mgr.ask = self.confirming_ask(win.mgr, tweak)
        # make the helper type nothing: replace the typing step by clearing the entry again
        with mock.patch.object(ldialog.ActionDialog, "confirm", lambda self_: False):
            win.mgr.delete_channels([self.channel(win, "alice")], act.ACTION_PERMANENT)
        self.assertEqual(asked[0], ("alice", False))  # the channel's name, and not allowed yet
        self.assertTrue(self.exists("Processed/kick/alice/alice_20260101_120000.mp4"))

    def test_permanent_delete_after_typing(self):
        win = self.managed()
        win.mgr.delete_channels([self.channel(win, "alice")], act.ACTION_PERMANENT)
        self.settle(win)
        self.assertFalse(self.exists("Processed/kick/alice/alice_20260101_120000.mp4"))
        self.assertFalse(self.exists("PendingDeletion/kick/alice/alice_20260101_120000.mp4"))
        self.assertIn("Deleted", win.status.cget("text"))
        self.assertFalse(win.undo_btn.winfo_manager())  # nothing to undo

    def test_several_channels_need_the_word_delete(self):
        win = self.managed()
        win.mgr.delete_channels([self.channel(win, "alice"), self.channel(win, "bob")])
        self.assertEqual(self.dialog.plan.confirm_word, "DELETE")
        self.settle(win)

    def test_a_recording_channel_is_blocked(self):
        self.busy = ({("kick", "alice")}, set())
        win = self.managed()
        win.mgr.delete_channels([self.channel(win, "alice")])
        self.assertIsNone(self.dialog.result)
        self.assertTrue(self.exists("Processed/kick/alice/alice_20260101_120000.mp4"))
        self.assertIn("recording", " ".join(self.dialog.plan.blocked.values()).lower() + self.dialog.warning.cget("text").lower())

    def test_no_action_while_busy_or_unscanned(self):
        win = self.make(manage_ctx=self.ctx(), activate=False)
        win.mgr.delete_channels([lib.ChannelStat("kick", "alice")])
        self.assertIn("not been scanned", self.infos[-1])
        win.activate()
        win.mgr.delete_channels([lib.ChannelStat("kick", "alice")])  # scan still running
        self.assertIn("still running", self.infos[-1])
        self.pump(lambda: not win.busy)

    def test_refuses_when_the_log_cannot_be_written(self):
        win = self.managed(log_path=str(self.data))  # a folder: cannot be a log file
        win.mgr.delete_channels([self.channel(win, "alice")])
        self.assertIn("action log", self.infos[-1])
        self.assertTrue(self.exists("Processed/kick/alice/alice_20260101_120000.mp4"))

    def test_filter_older_than_keeps_newer_files(self):
        win = self.managed()
        keep = self.streams / "Processed/kick/alice/alice_20260215_120000.mp4"
        os.utime(keep, None)  # now
        os.utime(keep.with_suffix(".meta.json"), None)
        win.mgr.ask = self.confirming_ask(win.mgr, lambda d: (d.mode_var.set("older"), d.a_var.set("30"), d._apply_scope()))
        win.start_scan()
        self.settle(win)
        win.mgr.delete_channels([self.channel(win, "alice")])
        self.settle(win)
        self.assertTrue(keep.exists())
        self.assertFalse(self.exists("Processed/kick/alice/alice_20260101_120000.mp4"))

    def test_a_cancelled_dialog_changes_nothing(self):
        win = self.managed()
        win.mgr.ask = lambda *a, **k: None
        win.mgr.delete_channels([self.channel(win, "alice")])
        self.assertTrue(self.exists("Processed/kick/alice/alice_20260101_120000.mp4"))
        self.assertFalse((self.data / "library_actions.log").exists() and (self.data / "library_actions.log").read_text())


class PendingTabTests(ManageCase):
    def test_card_shows_size_and_count(self):
        win = self.managed()
        text = win.mgr.pending.info.cget("text")
        self.assertIn("1 file", text)

    def test_empty_now_deletes_permanently(self):
        win = self.managed()
        win.mgr.empty_pending()
        self.assertEqual(self.dialog.plan.confirm_word, "bob")  # one channel
        self.settle(win)
        self.assertFalse(self.exists("PendingDeletion/twitch/bob/old.mp4"))
        self.assertTrue(self.exists("Processed/twitch/bob/bob_20260301_000000.mp4"))

    def test_empty_when_empty(self):
        win = self.managed()
        win.mgr.empty_pending()
        self.settle(win)
        win.mgr.empty_pending()
        self.assertIn("empty", self.infos[-1])

    def test_restore_all_puts_files_back(self):
        win = self.managed()
        win.mgr.restore_pending()
        self.settle(win)
        self.assertFalse(self.exists("PendingDeletion/twitch/bob/old.mp4"))
        self.assertTrue(self.exists("Processed/twitch/bob/old.mp4"))


class TabsAndClicks(ManageCase):
    def test_files_tab_lists_and_filters(self):
        win = self.managed()
        files = win.mgr.files
        self.assertEqual(len(files.tree.get_children()), 4)  # the four recordings
        files.search_var.set("bob_2026")
        self.assertEqual(len(files.tree.get_children()), 1)

    def test_files_tab_delete_the_ticked_files_only(self):
        win = self.managed()
        files = win.mgr.files
        files.search_var.set("alice_20260101")
        files.check_all()
        files.delete()
        self.settle(win)
        self.assertFalse(self.exists("Processed/kick/alice/alice_20260101_120000.mp4"))
        self.assertTrue(self.exists("Processed/kick/alice/alice_20260215_120000.mp4"))

    def test_play_and_reveal_use_the_injected_helpers(self):
        win = self.managed()
        files = win.mgr.files
        files.search_var.set("dave")
        files.tree.selection_set(files.tree.get_children()[0])
        files.play()
        files.reveal()
        self.assertTrue(self.opened[0].endswith("dave_20260320_101010.mp4"))
        self.assertTrue(self.revealed[0].endswith("dave_20260320_101010.mp4"))

    def test_channels_table_multi_select_and_buttons(self):
        win = self.managed()
        self.assertEqual(str(win.tree.cget("selectmode")), "extended")
        win.tree.selection_set(win.tree.get_children()[:2])
        win.root_update = self.root.update()
        self.assertEqual(len(win.selected_channels()), 2)
        self.assertNotIn("disabled", win.delete_btn.state())
        win.tree.selection_remove(win.tree.selection())
        self.root.update()
        self.assertIn("disabled", win.delete_btn.state())

    def test_delete_key_opens_the_review(self):
        win = self.managed()
        asked = []
        win.mgr.ask = lambda *a, **k: asked.append(a[1])
        win.tree.selection_set(win.tree.get_children()[0])
        self.assertTrue(win.tree.bind("<Delete>"))  # (a hidden window has no keyboard focus, so call what it runs)
        win.delete_selected()
        self.assertEqual(asked, ["Delete channel content"])
        self.assertTrue(win.mgr.files.tree.bind("<Delete>"))

    def test_clicking_a_donut_slice_filters_the_channels(self):
        win = self.managed()
        win.pick_site("kick")
        self.assertEqual({c.platform for c in win.rows.values()}, {"kick"})
        self.assertIn("Kick", win.site_btn.cget("text"))
        win.pick_site(None)
        self.assertGreater(len(win.rows), 1)
        self.assertFalse(win.site_btn.winfo_manager())

    def test_clicking_a_bar_selects_the_channel(self):
        win = self.managed()
        win.pick_channel("twitch|bob")
        self.assertEqual([c.name for c in win.selected_channels()], ["bob"])

    def test_clicking_a_month_filters_the_files(self):
        win = self.managed()
        win.pick_month("2026-03")
        names = [win.mgr.files.tree.set(i, "name") for i in win.mgr.files.tree.get_children()]
        self.assertTrue(names and all("2026" in n for n in names))
        self.assertEqual(win.mgr.files.month, "2026-03")

    def test_the_tab_without_a_manager_is_read_only(self):
        win = self.scanned()
        self.assertIsNone(win.mgr)
        self.assertFalse(win.delete_btn.winfo_manager())

    def test_cleanup_tab_has_suggestions_and_nothing_preticked(self):
        win = self.managed()
        tab = win.mgr.cleanup
        self.assertTrue(tab.tree.get_children())
        first = tab.tree.get_children()[0]
        tab.tree.selection_set(first)
        seen = {}
        win.mgr.ask = lambda parent, title, intro, recs=(), dirs=(), **kw: seen.update(preselect=kw.get("preselect"))
        tab.review()
        self.assertFalse(seen.get("preselect"))


class ActionLogTests(ManageCase):
    def test_the_log_lines_are_json_with_sizes(self):
        win = self.managed()
        win.mgr.delete_channels([self.channel(win, "alice")])
        self.settle(win)
        lines = [json.loads(x) for x in (self.data / "library_actions.log").read_text(encoding="utf-8").splitlines() if x.strip()]
        self.assertTrue(lines)
        self.assertTrue(all("action" in x or "path" in x for x in lines))
