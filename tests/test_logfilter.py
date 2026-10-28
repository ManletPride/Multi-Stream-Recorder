"""Log tab search / level filter and "Open log file" (msr/logview.py)."""
import logging
import tempfile
import tkinter as tk
import unittest
from pathlib import Path

from msr import logview as lv
from tests.test_logview import FakeText


def record(level, msg):
    return logging.LogRecord("t", level, __file__, 1, msg, None, None)


class FormatterTests(unittest.TestCase):
    def test_info_lines_keep_the_old_look_and_problems_get_a_tag(self):
        fmt = lv.GuiFormatter("%(asctime)s %(message)s", datefmt="%H:%M:%S")
        info = fmt.format(record(logging.INFO, "recording started"))
        warn = fmt.format(record(logging.WARNING, "kick is slow"))
        err = fmt.format(record(logging.CRITICAL, "disk full"))
        self.assertRegex(info, r"^\d\d:\d\d:\d\d recording started$")
        self.assertRegex(warn, r"^\d\d:\d\d:\d\d \[WARNING\] kick is slow$")
        self.assertRegex(err, r"^\d\d:\d\d:\d\d \[ERROR\] disk full$")
        self.assertEqual([lv.line_level(x) for x in (info, warn, err)], ["INFO", "WARNING", "ERROR"])

    def test_a_tag_typed_in_the_message_is_not_a_level(self):
        self.assertEqual(lv.line_level("12:00:00 saw [ERROR] in a title"), "INFO")
        self.assertEqual(lv.line_level(""), "INFO")

    def test_level_labels_round_trip(self):
        for key, label in lv.LEVEL_CHOICES:
            self.assertEqual(lv.level_key(lv.level_label(key)), key)
            self.assertEqual(lv.level_key(label), key)
        self.assertEqual(lv.level_key("junk"), lv.LEVEL_ALL)


class MatchTests(unittest.TestCase):
    def test_all_words_any_case(self):
        line = "12:00:00 [WARNING] Kick push: subscription limit reached"
        self.assertTrue(lv.matches(line, lv.split_query("KICK limit")))
        self.assertTrue(lv.matches(line, []))
        self.assertFalse(lv.matches(line, lv.split_query("kick twitch")))

    def test_levels(self):
        info, warn, err = "1 a", "1 [WARNING] a", "1 [ERROR] a"
        self.assertEqual([lv.matches(x, [], lv.LEVEL_WARNING) for x in (info, warn, err)], [False, True, True])
        self.assertEqual([lv.matches(x, [], lv.LEVEL_ERROR) for x in (info, warn, err)], [False, False, True])
        self.assertTrue(lv.matches(info, [], lv.LEVEL_ALL))


class FilterTests(unittest.TestCase):
    def make(self):
        text = FakeText(0)
        stats = []
        follower = lv.LogFollower(text, on_stats=lambda s, t: stats.append((s, t)))
        follower.append(["10:00:00 start alice", "10:00:01 [WARNING] slow bob", "10:00:02 start bob",
                         "10:00:03 [ERROR] crash alice"])
        return text, follower, stats

    def shown(self, text):
        return [x for x in text.lines if x]

    def test_filter_shows_matching_lines_and_can_be_removed_without_losing_any(self):
        text, follower, stats = self.make()
        follower.set_filter("bob")
        self.assertEqual(self.shown(text), ["10:00:01 [WARNING] slow bob", "10:00:02 start bob"])
        self.assertEqual(stats[-1], (2, 4))
        follower.set_filter("")
        self.assertEqual(len(self.shown(text)), 4)
        self.assertFalse(follower.filtering)

    def test_level_filter(self):
        text, follower, _ = self.make()
        follower.set_filter("", lv.LEVEL_ERROR)
        self.assertEqual(self.shown(text), ["10:00:03 [ERROR] crash alice"])
        follower.set_filter("alice", lv.LEVEL_WARNING)
        self.assertEqual(self.shown(text), ["10:00:03 [ERROR] crash alice"])

    def test_new_lines_are_filtered_as_they_arrive(self):
        text, follower, stats = self.make()
        follower.set_filter("bob")
        follower.append(["10:00:04 start carol", "10:00:05 stop bob"])
        self.assertEqual(self.shown(text)[-1], "10:00:05 stop bob")
        self.assertNotIn("10:00:04 start carol", text.lines)
        self.assertEqual(stats[-1], (3, 6))
        follower.set_filter("")
        self.assertIn("10:00:04 start carol", text.lines)  # kept although it was hidden

    def test_a_batch_with_no_hits_does_not_touch_the_view(self):
        text, follower, _ = self.make()
        follower.set_filter("bob")
        text.states.clear()
        follower.append(["x", "y"])
        self.assertEqual(text.states, [])
        self.assertEqual(follower.behind, 0)

    def test_filtering_while_scrolled_back_does_not_yank_the_view(self):
        text = FakeText(60)
        follower = lv.LogFollower(text)
        follower.append([f"l{i} bob" for i in range(40)])
        text.scroll_to(5)
        sees = text.sees
        follower.set_filter("bob")
        self.assertEqual(text.sees, sees)

    def test_stays_pinned_to_the_bottom_when_it_was_there(self):
        text, follower, _ = self.make()
        sees = text.sees
        follower.set_filter("alice")
        self.assertEqual(text.sees, sees + 1)

    def test_clear_forgets_everything_and_the_buffer_is_capped(self):
        text, follower, _ = self.make()
        follower.clear()
        follower.set_filter("alice")
        self.assertEqual(self.shown(text), [])
        follower.append([f"m{i}" for i in range(lv.MAX_LINES + 10)])
        self.assertLessEqual(len(follower.buffer), lv.MAX_LINES)
        self.assertGreaterEqual(len(follower.buffer), lv.KEEP_LINES)


class RealTextTests(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)
        self.text = tk.Text(self.root, state="disabled")
        self.text.pack()
        self.follower = lv.LogFollower(self.text)

    def tagged(self, tag):
        ranges = self.text.tag_ranges(tag)
        return [self.text.get(ranges[i], ranges[i + 1]) for i in range(0, len(ranges), 2)]

    def test_hits_and_levels_are_coloured(self):
        self.follower.append(["1 hello Bob", "2 [WARNING] careful", "3 [ERROR] bad bob"])
        self.assertEqual(self.tagged("lvl_warning"), ["2 [WARNING] careful\n"])
        self.assertEqual(self.tagged("lvl_error"), ["3 [ERROR] bad bob\n"])
        self.follower.set_filter("BOB")
        self.assertEqual(self.tagged("find"), ["Bob", "bob"])
        self.follower.append(["4 more bOb"])
        self.assertEqual(self.tagged("find"), ["Bob", "bob", "bOb"])
        self.follower.set_filter("")
        self.assertEqual(self.tagged("find"), [])
        self.assertEqual(self.text.get("1.0", "end-1c").count("\n"), 4)


class OpenLogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.log = Path(self.tmp.name) / "a folder" / "stream_recorder.log"
        self.log.parent.mkdir()
        self.log.write_text("x")

    def test_opens_the_file_with_the_exact_path(self):
        opened = []
        self.assertEqual(lv.open_log_file(str(self.log), opened.append), (True, ""))
        self.assertEqual(opened, [str(self.log)])

    def test_a_missing_file_is_explained_not_opened(self):
        opened = []
        ok, why = lv.open_log_file(str(self.log.with_name("nope.log")), opened.append)
        self.assertFalse(ok)
        self.assertIn("no log file", why)
        self.assertEqual(opened, [])
        self.assertFalse(lv.open_log_file("", opened.append)[0])

    def test_falls_back_when_the_os_has_no_program_for_logs(self):
        def refuse(path):
            raise OSError("no association")
        used = []
        self.assertTrue(lv.open_log_file(str(self.log), refuse, used.append)[0])
        self.assertEqual(used, [str(self.log)])
        ok, why = lv.open_log_file(str(self.log), refuse, lambda p: 1 / 0)
        self.assertFalse(ok)
        self.assertIn("no association", why)


class WiringTests(unittest.TestCase):
    def test_gui_uses_the_formatter_and_the_filter(self):
        gui = (Path(__file__).resolve().parent.parent / "msr" / "gui.py").read_text(encoding="utf-8")
        for needle in ("logview.GuiFormatter", "log_follow.set_filter", "Open log file", "<Control-f>"):
            self.assertIn(needle, gui)


if __name__ == "__main__":
    unittest.main()