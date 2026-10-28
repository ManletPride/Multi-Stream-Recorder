"""The review window (msr/library_dialog.py): scope filters, ticks, method rules and the typed confirmation."""
import calendar
import os
import time
from unittest import mock

from msr import library_actions as act
from msr import library_dialog as ldialog
from tests.test_library_manage import ManageCase

NOW = calendar.timegm((2026, 3, 1, 12, 0, 0))


class DialogCase(ManageCase):
    def open(self, name="alice", **kw):
        win = self.scanned(manage_ctx=self.ctx())
        recs = win.mgr.recs_of_channels([self.channel(win, name)])
        kw.setdefault("filters", True)
        kw.setdefault("methods", win.mgr.methods())
        dialog = ldialog.ActionDialog(win.mgr, win.toplevel, "Delete", "intro", recs, now=NOW, **kw)
        self.addCleanup(lambda: dialog.win.winfo_exists() and dialog.win.destroy())
        dialog.refresh()
        self.win = win
        return dialog

    def names(self, dialog):
        return sorted(os.path.basename(r.path) for r in dialog.chosen())


class ScopeTests(DialogCase):
    def test_default_ticks_recordings_and_raw_but_not_clips_or_pending(self):
        win = self.scanned(manage_ctx=self.ctx())
        put_dir = self.streams / "Clips" / "kick" / "alice"
        put_dir.mkdir(parents=True)
        (put_dir / "clip1.mp4").write_bytes(b"x" * 100)
        os.utime(put_dir / "clip1.mp4", (time.time() - 99999, time.time() - 99999))
        win.start_scan()
        self.pump(lambda: not win.busy and win.out.empty())
        recs = win.mgr.recs_of_channels([self.channel(win, "alice")])
        dialog = ldialog.ActionDialog(win.mgr, win.toplevel, "Delete", "x", recs, filters=True,
                                      methods=win.mgr.methods())
        self.addCleanup(dialog.win.destroy)
        dialog.refresh()
        self.assertEqual(self.names(dialog), ["alice_20260101_120000.mp4", "alice_20260215_120000.mp4"])
        dialog.kind_vars["clips"].set(True)
        dialog._scope_changed()
        self.assertIn("clip1.mp4", self.names(dialog))  # ticking a type includes it, the list says what

    def test_older_than_days(self):
        dialog = self.open()
        dialog.mode_var.set("older")
        dialog.a_var.set("30")
        self.assertEqual(self.names(dialog), ["alice_20260101_120000.mp4"])

    def test_all_but_the_newest_files(self):
        dialog = self.open()
        dialog.mode_var.set("newest_files")
        dialog.a_var.set("1")
        self.assertEqual(self.names(dialog), ["alice_20260101_120000.mp4"])

    def test_all_but_the_newest_gb_keeps_everything_when_small(self):
        dialog = self.open()
        dialog.mode_var.set("newest_gb")
        dialog.a_var.set("1")
        self.assertEqual(self.names(dialog), [])

    def test_date_range(self):
        dialog = self.open()
        dialog.mode_var.set("range")
        dialog.a_var.set("2026-02-01")
        dialog.b_var.set("2026-02-28")
        self.assertEqual(self.names(dialog), ["alice_20260215_120000.mp4"])

    def test_nonsense_numbers_tick_nothing(self):
        dialog = self.open()
        dialog.mode_var.set("older")
        dialog.a_var.set("abc")
        self.assertEqual(self.names(dialog), [])
        dialog.refresh()
        self.assertIn("disabled", dialog.go_btn.state())


class PlanRulesTests(DialogCase):
    def test_untick_removes_from_the_plan_and_the_count_shows(self):
        dialog = self.open()
        self.assertEqual(dialog.plan.files, 4)  # two videos and their info files
        iid = next(iter(dialog.rows))
        dialog._toggle(iid)
        dialog.refresh()
        self.assertEqual(dialog.plan.files, 2)
        self.assertIn("1 of 2", dialog.count_label.cget("text"))
        dialog.check_none()
        dialog.refresh()
        self.assertIn("disabled", dialog.go_btn.state())

    def test_the_safe_move_is_the_default_and_the_button_says_what_it_does(self):
        dialog = self.open()
        self.assertEqual(dialog.method_var.get(), act.ACTION_PENDING)
        self.assertIn("PendingDeletion", dialog.go_btn.cget("text"))
        self.assertTrue(dialog.allowed())

    def test_permanent_needs_the_exact_word(self):
        dialog = self.open()
        dialog.set_method(act.ACTION_PERMANENT)
        dialog.refresh()
        self.assertFalse(dialog.allowed())
        self.assertTrue(dialog.confirm_row.winfo_manager())
        dialog.confirm_var.set("alic")
        self.assertFalse(dialog.allowed())
        dialog.confirm_var.set("alice")
        self.assertTrue(dialog.allowed())
        self.assertFalse(dialog.confirm() and dialog.result is None)
        self.assertEqual(dialog.result.action, act.ACTION_PERMANENT)

    def test_recycle_bin_is_offered_only_when_windows_says_it_fits(self):
        problem = "The Recycle Bin on this drive is too small for 3 GB."
        win = self.scanned(manage_ctx=self.ctx(recycle_problem=lambda path, size: problem))
        recs = win.mgr.recs_of_channels([self.channel(win, "alice")])
        dialog = ldialog.ActionDialog(win.mgr, win.toplevel, "Delete", "x", recs, filters=True,
                                      methods=(act.ACTION_PENDING, act.ACTION_TRASH, act.ACTION_PERMANENT))
        self.addCleanup(dialog.win.destroy)
        dialog.refresh()
        self.assertIn("disabled", dialog.method_buttons[act.ACTION_TRASH].state())
        self.assertIn("too small", dialog.method_notes[act.ACTION_TRASH].cget("text"))
        dialog.set_method(act.ACTION_TRASH)
        dialog.refresh()
        self.assertFalse(dialog.allowed())

    def test_a_recording_channel_cannot_be_confirmed(self):
        self.busy = ({("kick", "alice")}, set())
        dialog = self.open()
        self.assertFalse(dialog.allowed())
        self.assertFalse(dialog.confirm())
        self.assertIsNone(dialog.result)
        self.assertIn("recording", dialog.warning.cget("text").lower())

    def test_a_file_that_changed_after_the_preview_is_left_alone(self):
        dialog = self.open()
        victim = self.streams / "Processed/kick/alice/alice_20260101_120000.mp4"
        victim.write_bytes(b"y" * 5)  # someone wrote to it between preview and confirm
        self.assertTrue(dialog.confirm())
        names = [os.path.basename(i.path) for i in dialog.result.items]
        self.assertNotIn("alice_20260101_120000.mp4", names)

    def test_moving_pending_files_to_pending_is_not_offered_as_a_delete(self):
        dialog = self.open("bob")
        dialog.kind_vars["pending"].set(True)
        dialog._scope_changed()
        dialog.refresh()
        plan = dialog.build_plan()
        self.assertFalse([i for i in plan.items if "PendingDeletion" in i.path])

    def test_escape_closes_without_a_result(self):
        dialog = self.open()
        dialog.win.event_generate("<Escape>")
        dialog.win.destroy()
        self.assertIsNone(dialog.result)

    def test_empty_folder_mode_lists_folders_and_removes_only_those(self):
        (self.streams / "Processed" / "kick" / "ghost").mkdir()
        win = self.scanned(manage_ctx=self.ctx())
        dirs = list(win.stats.empty_dirs)
        self.assertTrue(any(d.endswith("ghost") for d in dirs))
        dialog = ldialog.ActionDialog(win.mgr, win.toplevel, "Empty folders", "x", dirs=dirs, preselect=False)
        self.addCleanup(dialog.win.destroy)
        dialog.refresh()
        self.assertFalse(dialog.allowed())  # nothing ticked
        dialog.check_all()
        dialog.refresh()
        self.assertTrue(dialog.allowed())
        self.assertEqual(dialog.build_plan().action, act.ACTION_RMDIR)
