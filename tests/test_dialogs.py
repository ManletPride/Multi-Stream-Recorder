"""Tests for the themed dialog helpers. Windows are never shown: the reveal step and the
modal wait are replaced, and the root stays withdrawn, so nothing takes focus."""
import tkinter as tk
import unittest
from unittest import mock

from msr import dialogs


class PureTests(unittest.TestCase):
    def test_parse_number(self):
        p = dialogs.parse_number
        self.assertEqual(p("12", integer=True, minvalue=1, maxvalue=30), (12, ""))
        self.assertEqual(p(" 1,5 ", integer=False, minvalue=0.5, maxvalue=60), (1.5, ""))
        self.assertEqual(p("2.5", integer=False, minvalue=None, maxvalue=None), (2.5, ""))
        value, error = p("abc", integer=True, minvalue=1, maxvalue=30)
        self.assertIsNone(value)
        self.assertIn("whole number from 1 to 30", error)
        self.assertIn("from 1 to 30", p("31", integer=True, minvalue=1, maxvalue=30)[1])
        self.assertIn("from 0.5 to 60", p("0.1", integer=False, minvalue=0.5, maxvalue=60)[1])
        self.assertIsNone(p("2.5", integer=True, minvalue=1, maxvalue=30)[0])  # not a whole number
        self.assertIsNone(p("nan", integer=False, minvalue=1, maxvalue=2)[0])
        self.assertIsNone(p("", integer=False, minvalue=None, maxvalue=None)[0])

    def test_centered_position_centres_and_stays_in_the_work_area(self):
        area = (0, 0, 1920, 1040)
        self.assertEqual(dialogs.centered_position((100, 100, 800, 600), (400, 200), area), (300, 300))
        # a parent half off the right edge pulls the dialog back inside
        x, y = dialogs.centered_position((1700, 900, 800, 600), (400, 300), area)
        self.assertEqual((x, y), (1520, 740))
        # second monitor to the left (negative coordinates)
        left = (-1920, 0, 0, 1040)
        self.assertEqual(dialogs.centered_position((-1800, 100, 600, 400), (200, 100), left), (-1600, 250))
        # a dialog larger than the area starts at its top-left corner
        self.assertEqual(dialogs.centered_position((0, 0, 100, 100), (3000, 2000), area), (0, 0))

    def test_dark_is_guessed_from_the_background_when_no_switch_is_registered(self):
        self.assertLess(dialogs._luminance("#0a1628"), 0.5)
        self.assertGreater(dialogs._luminance("#f0f0f0"), 0.5)
        self.assertEqual(dialogs._luminance("#fff"), 1.0)
        self.assertEqual(dialogs._luminance("nonsense"), 1.0)

    def test_visible_owner_ignores_hidden_and_missing_windows(self):
        class Top:
            def __init__(self, viewable=True, state="normal", exists=True):
                self._v, self._s, self._e = viewable, state, exists

            def winfo_toplevel(self):
                return self

            def winfo_exists(self):
                return self._e

            def winfo_viewable(self):
                return self._v

            def state(self):
                return self._s

        shown = Top()
        self.assertIs(dialogs.visible_owner(shown), shown)
        self.assertIsNone(dialogs.visible_owner(Top(viewable=False)))  # tray-minimised
        self.assertIsNone(dialogs.visible_owner(Top(state="withdrawn")))
        self.assertIsNone(dialogs.visible_owner(Top(state="iconic")))
        self.assertIsNone(dialogs.visible_owner(Top(exists=False)))
        self.assertIsNone(dialogs.visible_owner(None))


class TkCase(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self._cleanup)
        self.revealed: list = []
        self.hooks: list = []
        self.dialogs: list = []

        def reveal(win, on_top):
            self.revealed.append((win, on_top))  # not shown: nothing may take focus

        def wait(widget, window=None):
            win = window or widget
            self.dialogs.append(win)
            if self.hooks:
                self.hooks.pop(0)(win)

        for patcher in (
            mock.patch.object(dialogs, "_reveal", reveal),
            mock.patch.object(tk.Misc, "wait_window", wait),
            mock.patch.object(dialogs.wintheme, "set_title_bar_dark", mock.Mock(return_value=True)),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.addCleanup(dialogs.set_dark_getter, None)

    def _cleanup(self):
        try:
            self.root.destroy()
        except tk.TclError:
            pass

    def press(self, key):
        return lambda win: win._msr_buttons[key].invoke()

    def alive(self, win):
        try:
            return bool(win.winfo_exists())
        except tk.TclError:
            return False


class MessageTests(TkCase):
    def test_yes_no_answers(self):
        self.hooks = [self.press("yes")]
        self.assertTrue(dialogs.ask_yes_no(self.root, "Q", "Sure?"))
        self.hooks = [self.press("no")]
        self.assertFalse(dialogs.ask_yes_no(self.root, "Q", "Sure?"))

    def test_escape_and_closing_mean_no_and_enter_follows_the_default(self):
        self.hooks = [lambda w: w._msr_escape()]
        self.assertFalse(dialogs.ask_yes_no(self.root, "Q", "Sure?"))
        self.hooks = [lambda w: w._msr_enter()]
        self.assertTrue(dialogs.ask_yes_no(self.root, "Q", "Sure?"))  # Enter = Yes by default
        self.hooks = [lambda w: w._msr_enter()]
        self.assertFalse(dialogs.ask_yes_no(self.root, "Q", "Sure?", default_no=True))  # Enter = No
        self.hooks = [lambda w: w.tk.call(w.protocol("WM_DELETE_WINDOW"))]  # the X button
        self.assertFalse(dialogs.ask_yes_no(self.root, "Q", "Sure?"))

    def test_ok_box_closes_with_enter_or_escape(self):
        for key in ("_msr_enter", "_msr_escape"):
            seen = []

            def hit(win, key=key):
                getattr(win, key)()
                seen.append(self.alive(win))
            self.hooks = [hit]
            dialogs.show_info(self.root, "t", "m")
            self.assertEqual(seen, [False])

    def test_dialog_is_owned_themed_withdrawn_and_closed_afterwards(self):
        seen = {}

        def look(win):
            seen["themed"] = win._msr_title_bar
            seen["master"] = str(win.master)
            seen["transient"] = win.wm_transient()
            seen["state"] = str(win.state())
            win._msr_buttons["ok"].invoke()
        self.hooks = [look]
        dialogs.show_info(self.root, "About", "Hello")
        self.assertTrue(seen["themed"])
        self.assertEqual(seen["master"], str(self.root))
        self.assertEqual(seen["transient"], "")  # parent is hidden: not made transient to it
        self.assertEqual(seen["state"], "withdrawn")  # the test never shows it
        self.assertEqual(self.revealed[0][1], True)  # raised on top because the parent is hidden
        self.assertFalse(self.alive(self.dialogs[0]))

    def test_visible_parent_owns_the_dialog_and_is_not_forced_on_top(self):
        self.hooks = [self.press("ok")]
        with mock.patch.object(dialogs, "visible_owner", return_value=self.root):
            dialogs.show_warning(self.root, "Careful", "Hot")
        self.assertEqual(self.revealed[0][1], False)

    def test_message_kinds_show_their_glyph(self):
        seen = []

        def look(win):
            labels = [w for w in win.winfo_children()[0].winfo_children()[0].winfo_children() if isinstance(w, tk.Label)]
            seen.append(labels[0].cget("text"))
            win.destroy()
        for fn in (dialogs.show_info, dialogs.show_warning, dialogs.show_error):
            self.hooks = [look]
            fn(self.root, "t", "m")
        self.assertEqual(seen, [dialogs._ICONS[k][0] for k in ("info", "warning", "error")])

    def test_caption_is_themed_before_the_dialog_is_placed_and_shown(self):
        events = []
        dialogs.wintheme.set_title_bar_dark.side_effect = lambda win, dark: events.append("caption") or True
        real = dialogs.centered_position

        def place(*args):
            events.append("place")
            return real(*args)

        def reveal(win, on_top):
            events.append("show")

        self.hooks = [self.press("ok")]
        with mock.patch.object(dialogs, "centered_position", place), mock.patch.object(dialogs, "_reveal", reveal):
            dialogs.show_info(self.root, "t", "m")
        self.assertIn("caption", events)
        self.assertLess(events.index("caption"), events.index("place"))
        self.assertLess(events.index("place"), events.index("show"))

    def test_caption_follows_the_registered_dark_switch(self):
        calls = []
        dialogs.wintheme.set_title_bar_dark.side_effect = lambda win, dark: calls.append(dark) or True
        dialogs.set_dark_getter(lambda: False)
        self.hooks = [self.press("ok")]
        dialogs.show_info(self.root, "t", "m")
        self.assertEqual(set(calls), {False})
        calls.clear()
        dialogs.set_dark_getter(lambda: True)
        self.hooks = [self.press("ok")]
        dialogs.show_info(self.root, "t", "m")
        self.assertEqual(set(calls), {True})


class NumberTests(TkCase):
    def type_then(self, text, key="ok"):
        def act(win):
            win._msr_var.set(text)
            win._msr_buttons[key].invoke()
        return act

    def test_enter_submits_the_typed_number(self):
        def enter(win):
            win._msr_var.set("12")
            win._msr_enter()
        self.hooks = [enter]
        self.assertEqual(dialogs.ask_int(self.root, "t", "Seconds", minvalue=1, maxvalue=30), 12)

    def test_valid_number_is_returned(self):
        self.hooks = [self.type_then("7")]
        self.assertEqual(dialogs.ask_int(self.root, "t", "Seconds", minvalue=1, maxvalue=30), 7)
        self.hooks = [self.type_then("1,5")]
        self.assertEqual(dialogs.ask_float(self.root, "t", "Minutes", minvalue=0.5, maxvalue=60), 1.5)

    def test_bad_input_shows_an_error_and_keeps_the_dialog_open(self):
        seen = {}

        def bad_then_good(win):
            win._msr_var.set("99")
            win._msr_buttons["ok"].invoke()
            seen["open"] = self.alive(win)
            seen["error"] = [w for w in win.winfo_children()[0].winfo_children() if isinstance(w, tk.Label)][0].cget("text")
            win._msr_var.set("5")
            win._msr_buttons["ok"].invoke()
        self.hooks = [bad_then_good]
        self.assertEqual(dialogs.ask_int(self.root, "t", "Seconds", minvalue=1, maxvalue=30), 5)
        self.assertTrue(seen["open"])
        self.assertIn("from 1 to 30", seen["error"])

    def test_cancel_and_escape_return_none(self):
        self.hooks = [self.press("cancel")]
        self.assertIsNone(dialogs.ask_int(self.root, "t", "Seconds", minvalue=1, maxvalue=30))
        self.hooks = [lambda w: w._msr_escape()]
        self.assertIsNone(dialogs.ask_float(self.root, "t", "Minutes"))

    def test_initial_value_is_prefilled(self):
        got = []
        self.hooks = [lambda w: (got.append(w._msr_var.get()), w.destroy())]
        dialogs.ask_int(self.root, "t", "Seconds", minvalue=1, maxvalue=30, initial=45)
        self.assertEqual(got, ["45"])


class AddChannelDialogTests(TkCase):
    def test_add_channel_dialog_is_themed_parented_and_placed_before_it_is_shown(self):
        from msr import channel_panel as cp
        theme = {"bg": "#0a1628", "fg": "#d4d4d4", "entry_bg": "#112240", "entry_fg": "#d4d4d4",
                 "border": "#1a3a5c", "accent": "#1a5fb4", "btn_bg": "#132d5e", "btn_fg": "#d4d4d4"}
        dlg = cp.open_add_channel_dialog(self.root, theme, lambda n, p: (True, ""), dark=True)
        self.addCleanup(dlg.destroy)
        self.assertTrue(dlg._msr_title_bar)
        self.assertEqual(self.revealed[0][0], dlg)
        self.assertTrue(dlg._msr_place.startswith("+"))
        self.assertEqual(str(dlg.state()), "withdrawn")  # nothing was shown by the test
        self.assertEqual(dlg.wm_transient(), "")  # hidden parent: no transient link
        dialogs.wintheme.set_title_bar_dark.assert_called()
        self.assertTrue(all(call.args[1] is True for call in dialogs.wintheme.set_title_bar_dark.call_args_list))


if __name__ == "__main__":
    unittest.main()