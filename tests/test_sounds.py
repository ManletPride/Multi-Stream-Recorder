"""Sound effects (msr/sounds.py) with a fake player and lock detection - nothing is played."""
import tkinter as tk
import unittest

from msr import control, sounds
from msr import options_panel as op
from tests.test_options_panel import FakeApi, FakeConfig


class Cfg:
    def __init__(self, **on):
        self.v = {k: "false" for k in ("go_live", "problems", "clip_saved")}
        self.v["quiet_while_locked"] = "true"
        self.v.update({k: ("true" if val else "false") for k, val in on.items()})

    def getboolean(self, s, k, fallback=None):
        v = self.v.get(k) if s == "Sounds" else None
        return fallback if v is None else v == "true"


class Base(unittest.TestCase):
    def make(self, locked=False, **on):
        self.played = []
        self.t = [100.0]
        return sounds.Sounds(Cfg(**on), player=self.played.append, locked=lambda: locked,
                             clock=lambda: self.t[0], platform="win32")


class TestSounds(Base):
    def test_off_by_default(self):
        s = self.make()
        self.assertFalse(s.play("live"))
        self.assertEqual(self.played, [])

    def test_aliases_and_rate_limit(self):
        s = self.make(go_live=True, clip_saved=True)
        self.assertTrue(s.play("live"))
        self.assertFalse(s.play("live"))       # burst coalesced
        self.assertTrue(s.play("clip"))        # other events have their own limit
        self.t[0] += sounds.MIN_GAP + 0.1
        self.assertTrue(s.play("live"))
        self.assertEqual(self.played, ["SystemAsterisk", "SystemDefault", "SystemAsterisk"])

    def test_same_problem_once_per_5_min(self):
        s = self.make(problems=True)
        self.assertTrue(s.play("problem", "a:Error"))
        self.t[0] += 10
        self.assertFalse(s.play("problem", "a:Error"))
        self.assertTrue(s.play("problem", "b:Error"))
        self.t[0] += sounds.PROBLEM_REPEAT
        self.assertTrue(s.play("problem", "a:Error"))

    def test_quiet_while_locked(self):
        s = self.make(locked=True, go_live=True)
        self.assertFalse(s.play("live"))
        s.config.v["quiet_while_locked"] = "false"
        self.t[0] += 10
        self.assertTrue(s.play("live"))

    def test_lock_detection_error_means_unlocked(self):
        def boom():
            raise OSError("no desktop")
        s = sounds.Sounds(Cfg(go_live=True), player=lambda a: None, locked=boom, platform="win32")
        self.assertTrue(s.play("live"))

    def test_test_button_ignores_checkbox_and_lock(self):
        s = self.make(locked=True)
        self.assertTrue(s.test("problem"))
        self.assertEqual(self.played, ["SystemExclamation"])

    def test_player_failure_is_swallowed(self):
        def bad(alias):
            raise RuntimeError("no audio device")
        s = sounds.Sounds(Cfg(go_live=True), player=bad, locked=lambda: False, platform="win32")
        self.assertFalse(s.play("live"))

    def test_non_windows_is_noop(self):
        s = sounds.Sounds(Cfg(go_live=True), platform="linux")
        self.assertFalse(s.play("live"))
        self.assertFalse(s.test("live"))

    def test_session_locked_never_raises(self):
        self.assertIn(sounds.session_locked(), (True, False))

    def test_settings_validate(self):
        self.assertEqual(control.validate_setting("Sounds", "go_live", "on"), "true")
        with self.assertRaises(control.ControlError):
            control.validate_setting("Sounds", "problems", "loud")


class TestOptionsRows(unittest.TestCase):
    def test_test_buttons_call_sound_test(self):
        try:
            root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        root.withdraw()
        self.addCleanup(root.destroy)
        called = []
        p = op.OptionsPanel(root, FakeConfig(), ".", lambda s, k, v: v, api=FakeApi(),
                            environ={}, sound_test=called.append)
        box, _ = p.groups["Sounds"]
        buttons = [w for w in box.winfo_children() if w.winfo_class() == "TButton" and w.cget("text") == "Test"]
        self.assertEqual(len(buttons), 3)
        for b in buttons:
            b.invoke()
        self.assertEqual(called, ["live", "problem", "clip"])
        self.assertIn(("Sounds", "quiet_while_locked"), p.vars)


if __name__ == "__main__":
    unittest.main()
