"""Log panel follow logic (msr/logview.py), tested against a fake Text so no window is needed."""
import unittest

from msr import logview as lv


class FakeText:
    """Just enough of tk.Text: lines, a viewport of `visible` lines starting at `top`."""

    def __init__(self, lines=0, visible=10):
        self.lines = [f"old {i}" for i in range(lines)]
        self.visible = visible
        self.top = max(0, lines + 1 - visible)  # starts scrolled to the end
        self.state = "disabled"
        self.states: list[str] = []
        self.sees = 0

    def configure(self, state=None, **_kw):
        if state:
            self.state = state
            self.states.append(state)

    def total(self):
        return len(self.lines) + 1  # the empty last line after the final newline

    def yview(self):
        total = self.total()
        return self.top / total, min(1.0, (self.top + self.visible) / total)

    def index(self, where):
        assert where == "end-1c"
        return f"{self.total()}.0"

    def insert(self, where, text):
        assert self.state == "normal", "text must be writable while inserting"
        self.lines.extend(text.rstrip("\n").split("\n"))

    def delete(self, start, end):
        assert self.state == "normal"
        first = int(start.split(".")[0])
        last = self.total() if end == "end" else int(end.split(".")[0])
        del self.lines[first - 1:last - 1]
        self.top = min(self.top, max(0, self.total() - self.visible))

    def see(self, where):
        self.sees += 1
        self.top = max(0, self.total() - self.visible)

    def scroll_to(self, top):
        self.top = top


class AtBottomTests(unittest.TestCase):
    def test_whole_log_visible_or_last_line_visible(self):
        self.assertTrue(lv.at_bottom(0.0, 1.0, 5))
        self.assertTrue(lv.at_bottom(0.4, 1.0, 100))

    def test_scrolled_back_even_one_line_is_not_at_the_bottom(self):
        self.assertFalse(lv.at_bottom(0.5, 0.9, 100))
        self.assertFalse(lv.at_bottom(0.9, 0.95, 20))  # one line up in a short log
        self.assertFalse(lv.at_bottom(0.99, 0.9998, 5000))  # one line up in a long one

    def test_half_a_line_of_rounding_still_counts_as_the_bottom(self):
        self.assertTrue(lv.at_bottom(0.99, 0.99995, 5000))
        self.assertTrue(lv.at_bottom(0.5, 0.999, 1))


class FollowerTests(unittest.TestCase):
    def make(self, lines=30, enabled=True):
        text = FakeText(lines)
        behind: list[int] = []
        follower = lv.LogFollower(text, enabled=lambda: enabled, on_behind=behind.append)
        return text, follower, behind

    def test_follows_new_lines_while_at_the_bottom(self):
        text, follower, behind = self.make()
        follower.append(["a", "b", "c"])
        self.assertTrue(follower.is_at_bottom())
        self.assertEqual(text.sees, 1)
        self.assertEqual(follower.behind, 0)
        self.assertEqual(text.lines[-3:], ["a", "b", "c"])
        self.assertEqual(text.state, "disabled")  # read-only again
        self.assertEqual(text.states[-2:], ["normal", "disabled"])

    def test_leaves_the_view_alone_when_scrolled_back_and_counts_new_lines(self):
        text, follower, behind = self.make()
        text.scroll_to(5)
        follower.append(["x"] * 4)
        self.assertEqual(text.top, 5)
        self.assertEqual(text.sees, 0)
        self.assertEqual(follower.behind, 4)
        follower.append(["y"] * 3)
        self.assertEqual(text.top, 5)
        self.assertEqual(follower.behind, 7)
        self.assertEqual(behind, [4, 7])

    def test_scrolling_to_the_bottom_clears_the_counter(self):
        text, follower, behind = self.make()
        text.scroll_to(5)
        follower.append(["x"] * 4)
        follower.on_scroll(*text.yview())  # still behind: stays
        self.assertEqual(follower.behind, 4)
        text.scroll_to(text.total() - text.visible)
        follower.on_scroll(*text.yview())
        self.assertEqual(follower.behind, 0)
        self.assertEqual(behind[-1], 0)

    def test_jump_goes_to_the_newest_line_and_hides_the_button(self):
        text, follower, behind = self.make()
        text.scroll_to(0)
        follower.append(["x"] * 2)
        follower.jump()
        self.assertTrue(follower.is_at_bottom())
        self.assertEqual(follower.behind, 0)
        self.assertEqual(behind[-1], 0)
        follower.append(["next"])  # and following resumes
        self.assertTrue(follower.is_at_bottom())

    def test_scroll_back_then_return_resumes_following(self):
        text, follower, _ = self.make()
        follower.append(["1"])
        text.scroll_to(3)
        follower.append(["2"])
        self.assertEqual(text.top, 3)
        follower.jump()
        follower.append(["3"])
        self.assertTrue(follower.is_at_bottom())

    def test_autoscroll_checkbox_off_never_follows(self):
        text, follower, behind = self.make(enabled=False)
        follower.append(["a", "b"])
        self.assertEqual(text.sees, 0)
        self.assertEqual(follower.behind, 2)
        self.assertEqual(behind, [2])

    def test_trims_the_oldest_lines_past_the_limit_and_keeps_following(self):
        text, follower, _ = self.make(lines=lv.MAX_LINES - 2)
        follower.append(["n"] * 10)
        self.assertLessEqual(len(text.lines), lv.KEEP_LINES + 1)
        self.assertEqual(text.lines[-1], "n")
        self.assertTrue(follower.is_at_bottom())

    def test_empty_batch_and_clear(self):
        text, follower, behind = self.make()
        follower.append([])
        self.assertEqual(text.states, [])
        text.scroll_to(0)
        follower.append(["x"])
        follower.clear()
        self.assertEqual(text.lines, [])
        self.assertEqual(follower.behind, 0)
        self.assertEqual(behind[-1], 0)

    def test_short_log_that_fits_the_window_is_at_the_bottom(self):
        text, follower, _ = self.make(lines=3)
        self.assertTrue(follower.is_at_bottom())
        follower.append(["a"])
        self.assertEqual(follower.behind, 0)


class WiringTests(unittest.TestCase):
    def test_gui_log_panel_uses_the_follower_not_a_blind_scroll(self):
        from pathlib import Path
        gui = (Path(__file__).resolve().parent.parent / "msr" / "gui.py").read_text(encoding="utf-8")
        self.assertIn("logview.LogFollower(", gui)
        self.assertIn("log_follow.append(batch)", gui)
        self.assertNotIn("log_text.see(tk.END)", gui)


if __name__ == "__main__":
    unittest.main()