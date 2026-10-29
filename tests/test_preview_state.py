"""msr/preview_state.py: what the preview strip remembers between ticks (no Tk, no ffmpeg)."""
import unittest

from msr import preview_state as ps
from msr import themes


class Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class AgeTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.state = ps.PreviewState(clock=self.clock)

    def test_a_channel_with_a_recent_frame_shows_nothing(self):
        self.state.mark_active("a")
        self.state.record_success("a", ps.LADDER[0])
        self.clock.advance(ps.STALE_AFTER)  # exactly the threshold is still fresh
        st = self.state.status("a")
        self.assertEqual((st.kind, st.text), ("fresh", ""))

    def test_past_the_threshold_the_age_is_shown(self):
        self.state.mark_active("a")
        self.state.record_success("a", ps.LADDER[0])
        self.clock.advance(12.4)
        st = self.state.status("a")
        self.assertEqual((st.kind, st.text), ("stale", "12 s"))
        self.clock.advance(60)
        self.assertEqual(self.state.status("a").text, "1 min")

    def test_a_new_frame_makes_it_fresh_again(self):
        self.state.mark_active("a")
        self.state.record_success("a", ps.LADDER[0])
        self.clock.advance(30)
        self.assertEqual(self.state.status("a").kind, "stale")
        self.state.record_success("a", ps.LADDER[0])
        self.assertEqual(self.state.status("a").kind, "fresh")

    def test_a_tile_that_never_got_a_frame_says_so_after_the_threshold(self):
        self.state.mark_active("a")
        self.clock.advance(5)
        self.assertEqual(self.state.status("a").kind, "fresh")
        self.clock.advance(6)
        st = self.state.status("a")
        self.assertEqual((st.kind, st.text), ("stale", "no frame"))

    def test_unknown_channel_is_fresh(self):
        self.assertEqual(self.state.status("nobody").kind, "fresh")

    def test_age_formats(self):
        self.assertEqual([ps.format_age(s) for s in (0, 11.9, 59, 60, 119, 3600, 7300)],
                         ["0 s", "11 s", "59 s", "1 min", "1 min", "1 h", "2 h"])


class EndedTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.state = ps.PreviewState(clock=self.clock)
        self.state.mark_active("a")
        self.state.record_success("a", ps.LADDER[0])

    def test_an_ended_channel_keeps_its_last_frame_for_a_while_then_goes(self):
        self.state.mark_ended("a")
        self.assertEqual(self.state.status("a").text, "ended")
        self.assertTrue(self.state.keep_ended("a"))
        self.clock.advance(ps.ENDED_KEEP - 1)
        self.assertTrue(self.state.keep_ended("a"))
        self.clock.advance(2)
        self.assertFalse(self.state.keep_ended("a"))

    def test_only_the_first_mark_starts_the_clock(self):
        self.state.mark_ended("a")
        self.clock.advance(ps.ENDED_KEEP - 5)
        self.state.mark_ended("a")  # the next tick marks it again
        self.clock.advance(6)
        self.assertFalse(self.state.keep_ended("a"))

    def test_a_channel_that_never_had_a_frame_is_not_kept(self):
        self.state.mark_active("b")
        self.state.mark_ended("b")
        self.assertFalse(self.state.keep_ended("b"))

    def test_recording_again_revives_the_tile(self):
        self.state.mark_ended("a")
        self.state.mark_active("a")
        self.assertNotEqual(self.state.status("a").kind, "ended")
        self.assertFalse(self.state.keep_ended("a"))

    def test_forget_drops_everything(self):
        self.state.forget("a")
        self.assertEqual(self.state.status("a").kind, "fresh")
        self.assertTrue(self.state.due("a"))
        self.assertEqual(self.state.plan("a")[0], ps.LADDER[0])


class LadderTests(unittest.TestCase):
    def test_default_ladder(self):
        self.assertEqual(ps.LADDER, (
            ("sseof", 3.0, True), ("sseof", 6.0, True), ("ss", 3.0, True), ("sseof", 6.0, False)))

    def test_ladder_scales_with_the_offset(self):
        self.assertEqual(ps.build_ladder(2.0)[:2], (("sseof", 2.0, True), ("sseof", 4.0, True)))

    def test_the_last_rung_is_the_old_plain_decode(self):
        self.assertFalse(ps.LADDER[-1][2])
        self.assertTrue(all(r[2] for r in ps.LADDER[:-1]))


class OffsetMemoryTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.state = ps.PreviewState(clock=self.clock)

    def test_a_new_channel_starts_at_three_seconds_and_tries_every_rung_in_order(self):
        self.assertEqual(self.state.plan("a"), list(ps.LADDER))

    def test_the_working_seek_is_remembered_and_tried_first(self):
        self.state.record_success("a", ps.LADDER[1])  # -3 found nothing, -6 did
        self.assertEqual(self.state.plan("a"), list(ps.LADDER[1:]))

    def test_nothing_is_remembered_by_a_failure(self):
        self.state.record_success("a", ps.LADDER[1])
        self.state.record_failure("a")
        self.assertEqual(self.state.plan("a")[0], ps.LADDER[1])

    def test_channels_do_not_share_memory(self):
        self.state.record_success("a", ps.LADDER[1])
        self.assertEqual(self.state.plan("b")[0], ps.LADDER[0])

    def test_a_shorter_seek_is_retried_after_ten_successes_and_the_memory_moves_down_if_it_works(self):
        for _ in range(ps.RETRY_SHORTER_EVERY - 1):
            self.state.record_success("a", ps.LADDER[1])
            self.assertEqual(self.state.plan("a")[0], ps.LADDER[1])  # not yet: 9 good grabs
        self.state.record_success("a", ps.LADDER[1])  # the 10th
        probe = self.state.plan("a")
        self.assertEqual(probe, [ps.LADDER[0], *ps.LADDER[1:]])
        self.state.record_success("a", probe[0])  # the key frames are closer now
        self.assertEqual(self.state.plan("a")[0], ps.LADDER[0])

    def test_when_the_retry_fails_the_longer_seek_stays_and_the_count_starts_over(self):
        for _ in range(ps.RETRY_SHORTER_EVERY):
            self.state.record_success("a", ps.LADDER[1])
        self.assertEqual(self.state.plan("a")[0], ps.LADDER[0])  # the probe
        self.state.record_success("a", ps.LADDER[1])             # it found nothing, -6 worked
        self.assertEqual(self.state.plan("a")[0], ps.LADDER[1])  # not probing again right away

    def test_the_large_pane_grab_does_not_use_up_the_probe(self):
        for _ in range(ps.RETRY_SHORTER_EVERY):
            self.state.record_success("a", ps.LADDER[1])
        self.assertEqual(self.state.plan("a", allow_retry=False)[0], ps.LADDER[1])
        self.assertEqual(self.state.plan("a")[0], ps.LADDER[0])

    def test_the_first_rung_never_probes(self):
        for _ in range(30):
            self.state.record_success("a", ps.LADDER[0])
        self.assertEqual(self.state.plan("a"), list(ps.LADDER))

    def test_a_new_recording_file_starts_the_ladder_over(self):
        self.state.note_source("a", "one.ts")
        self.state.record_success("a", ps.LADDER[2])
        self.state.record_failure("a")
        self.state.note_source("a", "one.ts")  # same file: nothing changes
        self.assertEqual(self.state.plan("a")[0], ps.LADDER[2])
        self.assertFalse(self.state.due("a"))
        self.state.note_source("a", "two.ts")  # new stream, other key-frame spacing
        self.assertEqual(self.state.plan("a")[0], ps.LADDER[0])
        self.assertTrue(self.state.due("a"))


class BackoffTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.state = ps.PreviewState(clock=self.clock)

    def test_the_delays_double_and_are_capped(self):
        self.assertEqual([ps.backoff_seconds(n) for n in range(0, 8)],
                         [0, 4, 8, 16, 32, 60, 60, 60])

    def test_a_failed_channel_is_skipped_until_its_delay_is_over(self):
        self.assertTrue(self.state.due("a"))
        self.assertEqual(self.state.record_failure("a", started=self.clock()), 4.0)
        self.assertFalse(self.state.due("a"))
        self.clock.advance(2.9)
        self.assertFalse(self.state.due("a"))
        self.clock.advance(0.2)  # within the 1 s grace: a 4 s tick still lands on it
        self.assertTrue(self.state.due("a"))

    def test_every_further_failure_waits_longer(self):
        delays = []
        for _ in range(6):
            delays.append(self.state.record_failure("a", started=self.clock()))
            self.clock.advance(delays[-1])
        self.assertEqual(delays, [4, 8, 16, 32, 60, 60])

    def test_the_delay_counts_from_when_the_grab_started(self):
        self.state.record_failure("a", started=self.clock())  # 4 s
        start = self.clock()
        self.clock.advance(3.5)  # the second failing grab itself took a while
        self.assertEqual(self.state.record_failure("a", started=start), 8.0)
        self.clock.advance(3.4)  # 6.9 s after the start
        self.assertFalse(self.state.due("a"))
        self.clock.advance(0.2)  # 7.1 s: within the grace of start + 8 (counted from the end it would wait to 11.5)
        self.assertTrue(self.state.due("a"))

    def test_success_resets_the_backoff(self):
        for _ in range(4):
            self.state.record_failure("a", started=self.clock())
        self.state.record_success("a", ps.LADDER[0])
        self.assertTrue(self.state.due("a"))
        self.assertEqual(self.state.record_failure("a", started=self.clock()), 4.0)

    def test_other_channels_are_not_held_back(self):
        self.state.record_failure("a", started=self.clock())
        self.assertTrue(self.state.due("b"))


class LookTests(unittest.TestCase):
    FRESH = ps.PreviewStatus("fresh", 2.0, "")
    STALE = ps.PreviewStatus("stale", 12.0, "12 s")
    ENDED = ps.PreviewStatus("ended", 30.0, "ended")

    def test_fresh_tile_is_unchanged(self):
        t = themes.current()
        look = ps.tile_look(self.FRESH, t)
        self.assertEqual((look.suffix, look.border, look.thickness, look.name_fg), ("", t["border"], 1, t["fg"]))
        sel = ps.tile_look(self.FRESH, t, selected=True)
        self.assertEqual((sel.border, sel.thickness), (t["accent"], 2))

    def test_a_stale_tile_shows_the_age_and_the_warning_colour(self):
        t = themes.current()
        for selected in (False, True):
            look = ps.tile_look(self.STALE, t, selected)
            self.assertEqual(look.suffix, " \u00b7 12 s")
            self.assertEqual((look.border, look.name_fg), (t["warn"], t["warn"]))
        self.assertEqual(ps.pane_border(self.STALE, t), t["warn"])
        self.assertEqual(ps.pane_border(self.FRESH, t), t["accent"])

    def test_an_ended_tile_says_ended_in_the_muted_colour(self):
        t = themes.current()
        look = ps.tile_look(self.ENDED, t)
        self.assertEqual(look.suffix, " \u00b7 ended")
        self.assertEqual((look.border, look.name_fg), (t["offline_fg"], t["offline_fg"]))
        self.assertEqual(ps.pane_border(self.ENDED, t), t["accent"])

    def test_every_theme_has_the_tokens_the_looks_use(self):
        for name, theme in themes.THEMES.items():
            for status in (self.FRESH, self.STALE, self.ENDED):
                look = ps.tile_look(status, theme, True)
                for color in (look.border, look.name_fg, ps.pane_border(status, theme)):
                    self.assertTrue(themes.is_hex_color(color), (name, status.kind, color))


if __name__ == "__main__":
    unittest.main()
