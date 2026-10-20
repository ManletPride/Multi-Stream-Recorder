"""Reconnect polling schedule after a stream drops (ReconnectWindow)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.worker import ReconnectWindow


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class TestReconnectWindow(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.win = ReconnectWindow(180, 600, clock=self.clock)

    def test_window_arms_at_first_poll_not_at_drop(self):
        self.win.reset()              # recording ended
        self.clock.t += 175           # 3-minute synchronous remux
        phase, remaining, sleep = self.win.next_poll()
        self.assertEqual(phase, "fast")
        self.assertEqual(remaining, 180)
        self.assertEqual(sleep, 15)

    def test_fast_then_extended_then_expired(self):
        self.assertEqual(self.win.next_poll()[0], "fast")
        self.clock.t += 179
        self.assertEqual(self.win.next_poll(), ("fast", 1, 15))
        self.clock.t += 1
        self.assertEqual(self.win.next_poll(), ("extended", 600, 30))
        self.clock.t += 599
        self.assertEqual(self.win.next_poll()[0], "extended")
        self.clock.t += 1
        self.assertEqual(self.win.next_poll(), ("expired", 0, None))

    def test_reset_starts_fresh_window(self):
        self.win.next_poll()
        self.clock.t += 10_000
        self.assertEqual(self.win.next_poll()[0], "expired")
        self.win.reset()
        self.assertEqual(self.win.next_poll()[0], "fast")

    def test_zero_extended_goes_straight_to_normal(self):
        win = ReconnectWindow(180, 0, clock=self.clock)
        win.next_poll()
        self.clock.t += 180
        self.assertEqual(win.next_poll()[0], "expired")


if __name__ == "__main__":
    unittest.main()
