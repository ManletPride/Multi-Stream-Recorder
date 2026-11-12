"""Mouse-wheel notches: X11 Button-4/5 and the Windows delta of 120."""
import tkinter as tk
import unittest

from msr.wheel import (
    SEQUENCES, bind_wheel, unbind_wheel, wheel_delta, wheel_steps, widget_is_inside,
)


class _Ev:
    def __init__(self, num=None, delta=0, state=0):
        self.num = num
        self.delta = delta
        self.state = state


class WheelStepTests(unittest.TestCase):
    def test_x11_buttons_are_one_notch(self):
        self.assertEqual(wheel_steps(_Ev(num=4, delta=0)), 1)
        self.assertEqual(wheel_steps(_Ev(num=5, delta=0)), -1)
        self.assertEqual(wheel_delta(_Ev(num=4, delta=0)), 120)
        self.assertEqual(wheel_delta(_Ev(num=5, delta=0)), -120)

    def test_windows_notches_match_int_delta_over_120(self):
        for delta, steps in ((120, 1), (-120, -1), (240, 2), (-240, -2), (0, 0)):
            ev = _Ev(num=0, delta=delta)
            self.assertEqual(wheel_steps(ev), steps)
            self.assertEqual(wheel_delta(ev), delta)
            # The scroll sites used int(-delta / 120), which is -steps.
            self.assertEqual(-wheel_steps(ev), int(-delta / 120))

    def test_a_fractional_windows_delta_is_kept_for_the_timeline(self):
        # int(delta/120) truncates, but the Clip Editor divides the raw delta.
        ev = _Ev(delta=180)
        self.assertEqual(wheel_steps(ev), 1)
        self.assertEqual(wheel_delta(ev), 180)

    def test_button_number_wins_over_a_stale_delta(self):
        self.assertEqual(wheel_steps(_Ev(num=4, delta=-240)), 1)
        self.assertEqual(wheel_steps(_Ev(num=5, delta=240)), -1)


class BindWheelTests(unittest.TestCase):
    def setUp(self):
        from tests import new_tk_root
        self.root = new_tk_root(self)
        self.addCleanup(self._cleanup)
        self.frame = tk.Frame(self.root)
        self.frame.pack()
        self.child = tk.Frame(self.frame)
        self.child.pack()
        self.root.update_idletasks()

    def _cleanup(self):
        unbind_wheel(self.frame, all=True)
        self.root.destroy()

    def test_bind_wheel_binds_all_three_sequences(self):
        seen = []
        bind_wheel(self.frame, lambda e: seen.append((e.num, int(e.delta))) or "break")
        for seq in SEQUENCES:
            self.assertTrue(self.frame.bind(seq), seq)
        # A withdrawn window does not deliver Button events. Show it, then hide again.
        self.root.deiconify()
        self.root.update()
        self.frame.event_generate("<Button-4>")
        self.frame.event_generate("<Button-5>")
        self.frame.event_generate("<MouseWheel>", delta=240)
        self.root.update()
        self.root.withdraw()
        # MouseWheel's num is Tk's '??' (not a button). Button-4/5 are 4 and 5.
        self.assertEqual(seen, [(4, 0), (5, 0), ("??", 240)])
        self.assertEqual(wheel_steps(type("E", (), {"num": "??", "delta": 240})()), 2)
        unbind_wheel(self.frame)
        for seq in SEQUENCES:
            self.assertEqual(self.frame.bind(seq), "")

    def test_bind_all_and_unbind_all(self):
        bind_wheel(self.frame, lambda e: "break", all=True)
        for seq in SEQUENCES:
            self.assertTrue(self.frame.bind_all(seq), seq)
        unbind_wheel(self.frame, all=True)
        for seq in SEQUENCES:
            self.assertEqual(self.frame.bind_all(seq), "")

    def test_a_child_counts_as_inside_the_frame(self):
        self.assertTrue(widget_is_inside(self.child, self.frame))
        self.assertTrue(widget_is_inside(self.frame, self.frame))
        self.assertFalse(widget_is_inside(self.root, self.frame))
        self.assertFalse(widget_is_inside(None, self.frame))


if __name__ == "__main__":
    unittest.main()
