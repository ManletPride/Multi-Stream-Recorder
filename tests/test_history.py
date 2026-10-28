"""History of channels.json / config.ini: snapshots, limit, restore, masked summaries, dialog."""
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

from msr import history as h
from msr.config import Config


def at(seconds):
    return datetime(2026, 10, 24, 12, 0, 0) + timedelta(seconds=seconds)


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.file = Path(self.tmp.name) / "channels.json"

    def write(self, text):
        self.file.write_text(text, encoding="utf-8")

    def test_snapshot_copies_the_current_file_beside_it(self):
        self.write("[1]")
        made = h.snapshot(self.file, now=at(0))
        self.assertEqual(made.read_text(encoding="utf-8"), "[1]")
        self.assertEqual(made.parent, Path(self.tmp.name) / "msr_history" / "channels.json")
        versions = h.list_versions(self.file)
        self.assertEqual(len(versions), 1)
        self.assertEqual(versions[0].stamp, at(0))

    def test_nothing_to_keep_for_a_missing_or_empty_file_or_a_duplicate(self):
        self.assertIsNone(h.snapshot(self.file))
        self.write("")
        self.assertIsNone(h.snapshot(self.file))
        self.write("[1]")
        self.assertIsNotNone(h.snapshot(self.file, now=at(0)))
        self.assertIsNone(h.snapshot(self.file, now=at(5)))  # identical to the newest copy
        self.assertEqual(len(h.list_versions(self.file)), 1)

    def test_only_the_last_twenty_are_kept_newest_first(self):
        for n in range(30):
            self.write(f"[{n}]")
            h.snapshot(self.file, now=at(n))
        versions = h.list_versions(self.file)
        self.assertEqual(len(versions), 20)
        self.assertEqual(versions[0].read(), b"[29]")
        self.assertEqual(versions[-1].read(), b"[10]")

    def test_a_burst_of_saves_keeps_the_state_from_before_the_burst(self):
        self.write("[0]")
        h.snapshot(self.file, min_interval=45, now=at(0))
        for n in range(1, 6):
            self.write(f"[{n}]")
            h.snapshot(self.file, min_interval=45, now=at(n * 3))
        self.assertEqual([v.read() for v in h.list_versions(self.file)], [b"[0]"])
        self.write("[9]")
        h.snapshot(self.file, min_interval=45, now=at(120))  # a minute later: kept
        self.assertEqual(len(h.list_versions(self.file)), 2)

    def test_two_snapshots_in_the_same_instant_do_not_collide(self):
        self.write("[1]")
        h.snapshot(self.file, now=at(0))
        self.write("[2]")
        h.snapshot(self.file, now=at(0))
        self.assertEqual(len(h.list_versions(self.file)), 2)

    def test_a_history_failure_never_raises(self):
        self.write("[1]")
        with mock.patch.object(h.Path, "mkdir", side_effect=OSError("disk full")):
            self.assertIsNone(h.snapshot(self.file))

    def test_stray_files_in_the_history_folder_are_ignored(self):
        self.write("[1]")
        h.snapshot(self.file, now=at(0))
        folder = h.history_folder(self.file)
        (folder / "notes.txt").write_text("x")
        (folder / "garbage.bak").write_text("x")
        self.assertEqual(len(h.list_versions(self.file)), 1)


class RestoreTests(unittest.TestCase):
    def test_restore_puts_the_old_text_back_and_keeps_the_current_one_in_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Path(tmp) / "channels.json"
            f.write_text("[1]")
            h.snapshot(f, now=at(0))
            f.write_text("[2]")
            old = h.list_versions(f)[0]
            h.restore(f, old)
            self.assertEqual(f.read_text(), "[1]")
            texts = [v.read() for v in h.list_versions(f)]
            self.assertIn(b"[2]", texts)  # the state before the restore can be restored too
            self.assertEqual([p.name for p in Path(tmp).iterdir() if p.is_file()], ["channels.json"])  # no temp left

    def test_atomic_write_leaves_the_old_file_if_the_write_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Path(tmp) / "a.json"
            f.write_text("old")
            with mock.patch.object(h.os, "replace", side_effect=OSError("locked")):
                with self.assertRaises(OSError):
                    h.atomic_write(f, b"new")
            self.assertEqual(f.read_text(), "old")
            self.assertEqual([p.name for p in Path(tmp).iterdir()], ["a.json"])

    def test_restore_keeps_the_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Path(tmp) / "c.json"
            for n in range(25):
                f.write_text(f"[{n}]")
                h.snapshot(f, now=at(n))
            h.restore(f, h.list_versions(f)[5])
            self.assertLessEqual(len(h.list_versions(f)), h.KEEP)


class SummaryTests(unittest.TestCase):
    def test_channel_lists(self):
        old = json.dumps([{"name": "a", "enabled": True}, {"name": "b", "enabled": True}, "c"]).encode()
        new = json.dumps([{"name": "a", "enabled": False}, {"name": "d", "enabled": True}]).encode()
        text = h.summarize("channels.json", old, new)
        self.assertIn("3 channels", text)
        self.assertIn("brings back 2 (b, c)", text)
        self.assertIn("drops 1 (d)", text)
        self.assertIn("1 enabled/disabled differ (a)", text)
        self.assertEqual(h.summarize("channels.json", old, old), "Same as the current file")
        self.assertIn("different order", h.summarize("channels.json", json.dumps(["x", "y"]).encode(),
                                                      json.dumps(["y", "x"]).encode()))

    def test_long_channel_lists_are_shortened(self):
        old = json.dumps([f"n{i}" for i in range(10)]).encode()
        text = h.summarize("channels.json", old, b"[]")
        self.assertIn("n0, n1, n2 +7 more", text)

    def test_config_summary_masks_every_sensitive_value(self):
        old = ("[Fishtank]\nemail = me@example.com\npassword = hunter2\n[Recording]\nfilename_pattern = {a}\n"
               "[Tokens]\napi_key = abc123\n").encode()
        new = ("[Fishtank]\nemail = other@example.com\npassword = swordfish\n[Recording]\nfilename_pattern = {b}\n"
               "[Tokens]\napi_key = zzz999\n").encode()
        text = h.summarize("config.ini", old, new)
        for secret in ("hunter2", "swordfish", "me@example.com", "other@example.com", "abc123", "zzz999"):
            self.assertNotIn(secret, text)
        self.assertIn("Fishtank.password " + h.MASK, text)
        self.assertIn("Recording.filename_pattern: {b} \u2192 {a}", text)

    def test_config_summary_for_a_key_only_in_one_side_and_junk(self):
        text = h.summarize("config.ini", b"[A]\nx = 1\n", b"[A]\nx = 1\ny = 2\n")
        self.assertIn("A.y: 2 \u2192 (none)", text)
        self.assertIn("unreadable", h.summarize("config.ini", b"no section header", b"[A]\nx=1"))
        self.assertIn("1 settings", h.summarize("config.ini", b"[A]\nx = 1\n", None))
        self.assertIn("formatting", h.summarize("config.ini", b"[A]\nx = 1\n", b"[A]\nx=1\n\n"))

    def test_sensitive_key_detection(self):
        for key in ("password", "Fishtank_email", "api_key", "auth_token", "cookies_file", "secret"):
            self.assertTrue(h.is_sensitive(key), key)
        for key in ("streams_dir", "filename_pattern", "poll_interval_minutes"):
            self.assertFalse(h.is_sensitive(key), key)


class ConfigHookTests(unittest.TestCase):
    def test_config_writes_keep_the_previous_file_and_reload_reads_it_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "config.ini")
            cfg = Config(path)  # creates the file (nothing to keep yet)
            self.assertEqual(h.list_versions(path), [])
            cfg.config.set("Timeouts", "poll_interval_minutes", "7")
            cfg.save()
            versions = h.list_versions(path)
            self.assertEqual(len(versions), 1)
            self.assertNotIn(b"poll_interval_minutes = 7", versions[0].read())
            h.restore(path, versions[0])
            cfg.reload()
            self.assertEqual(cfg.get("Timeouts", "poll_interval_minutes"), "3")


class DialogTests(unittest.TestCase):
    def setUp(self):
        import tkinter as tk
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.root.destroy)
        from msr import dialogs
        self.dialogs = dialogs
        for patcher in (
            mock.patch.object(dialogs, "_reveal", lambda win, on_top: None),
            mock.patch.object(dialogs.wintheme, "set_title_bar_dark", mock.Mock(return_value=True)),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.file = Path(self.tmp.name) / "channels.json"
        self.file.write_text(json.dumps(["a", "b"]))
        h.snapshot(self.file, now=at(0))
        self.file.write_text(json.dumps(["a"]))
        h.snapshot(self.file, now=at(60))
        self.file.write_text(json.dumps(["z"]))

    def window(self, **kw):
        from msr.restore_dialog import RestoreWindow
        return RestoreWindow(self.root, self.file, "Restore previous channel list", **kw)

    def test_lists_versions_newest_first_with_a_summary(self):
        w = self.window()
        rows = [w.tree.item(i, "values") for i in w.tree.get_children()]
        self.assertEqual(len(rows), 2)
        self.assertTrue(rows[0][0].startswith("2026-10-24  12:01"))
        self.assertIn("brings back", rows[0][1])
        self.assertTrue(w.win._msr_title_bar)

    def test_restore_asks_first_then_restores_and_reports(self):
        done = []
        w = self.window(on_restored=done.append)
        w.tree.selection_set("1")  # the oldest: ["a", "b"]
        with mock.patch.object(self.dialogs, "ask_yes_no", return_value=False) as ask:
            self.assertFalse(w.restore_selected())
            self.assertIn("kept in this list", ask.call_args.args[2])
        self.assertEqual(json.loads(self.file.read_text()), ["z"])
        with mock.patch.object(self.dialogs, "ask_yes_no", return_value=True):
            self.assertTrue(w.restore_selected())
        self.assertEqual(json.loads(self.file.read_text()), ["a", "b"])
        self.assertEqual(done, [str(self.file)])
        self.assertEqual(len(h.list_versions(self.file)), 3)  # ["z"] is now in the list
        self.assertEqual(len(w.tree.get_children()), 3)

    def test_a_blocked_restore_says_why_and_changes_nothing(self):
        w = self.window(can_restore=lambda: "Stop recording first.")
        with mock.patch.object(self.dialogs, "show_warning") as warn, \
                mock.patch.object(self.dialogs, "ask_yes_no") as ask:
            self.assertFalse(w.restore_selected())
        warn.assert_called_once()
        ask.assert_not_called()
        self.assertEqual(json.loads(self.file.read_text()), ["z"])

    def test_note_is_shown_after_a_restore(self):
        w = self.window(note="Restart to apply.")
        with mock.patch.object(self.dialogs, "ask_yes_no", return_value=True), \
                mock.patch.object(self.dialogs, "show_info") as info:
            w.restore_selected()
        info.assert_called_once()

    def test_open_restore_without_history_just_says_so(self):
        from msr import restore_dialog
        empty = Path(self.tmp.name) / "none.json"
        empty.write_text("[]")
        with mock.patch.object(self.dialogs, "show_info") as info:
            self.assertIsNone(restore_dialog.open_restore(self.root, empty, "Restore"))
        info.assert_called_once()


class WiringTests(unittest.TestCase):
    def test_history_is_git_ignored_and_gui_snapshots_before_saving(self):
        root = Path(__file__).resolve().parent.parent
        self.assertIn("msr_history/", (root / ".gitignore").read_text(encoding="utf-8"))
        gui = (root / "msr" / "gui.py").read_text(encoding="utf-8")
        self.assertLess(gui.index("_history.snapshot(channels_file"), gui.index('with open(channels_file, "w")'))
        self.assertNotIn("config.config.write(f)", gui)


if __name__ == "__main__":
    unittest.main()