"""Options tab settings rows: hints, browse, password, background checks (hidden root, fakes)."""
import time
import tkinter as tk
import unittest

from msr import control
from msr import options_panel as op
from tests.test_options_panel import FakeApi, FakeConfig


class Case(unittest.TestCase):
    def setUp(self):
        try:
            self.root = tk.Tk()
        except tk.TclError as exc:
            self.skipTest(f"no display: {exc}")
        self.root.withdraw()
        self.addCleanup(self.root.destroy)
        self.cfg = FakeConfig()
        self.saved = []
        self.dirs = ["C:\\Rec\\New"]
        self.tested = []

    def apply(self, s, k, v):
        v = control.validate_setting(s, k, v)
        self.cfg.v[(s, k)] = v
        self.saved.append((s, k, v))
        return v

    def make(self, **kw):
        kw.setdefault("ask_dir", lambda initial: self.dirs.pop(0) if self.dirs else "")
        kw.setdefault("ask_file", lambda initial: "C:\\tools\\ffmpeg.exe")
        kw.setdefault("environ", {})
        p = op.OptionsPanel(self.root, self.cfg, ".", self.apply, api=FakeApi(), **kw)
        p.frame.pack()
        return p

    def pump(self, until, timeout=5):
        end = time.monotonic() + timeout
        while time.monotonic() < end and not until():
            self.root.update()
            time.sleep(0.02)

    def test_every_row_is_a_known_setting(self):
        for spec in op.all_rows():
            self.assertTrue(control.known_setting(self.cfg_real(), spec["section"], spec["key"]),
                            (spec["section"], spec["key"]))

    def cfg_real(self):
        class C:
            class config:
                @staticmethod
                def has_option(s, k):
                    return False
        return C

    def test_zero_hints_say_what_zero_means(self):
        hints = {(r["section"], r["key"]): r["hint"] for r in op.all_rows()}
        self.assertEqual(hints[("Recording", "max_record_hours")], "0 = no limit")
        self.assertEqual(hints[("Cleanup", "auto_purge_days")], "0 = never delete")
        self.assertEqual(hints[("Cleanup", "max_log_size_mb")], "0 = never rotate")
        self.assertIn("0 = never split", hints[("Recording", "max_file_size_gb")])
        self.assertIn("0 = no free-space check", hints[("Recording", "min_disk_space_gb")])
        self.assertFalse(any("\u2265" in h for h in hints.values()))

    def test_pattern_preview_updates(self):
        p = self.make()
        v = p.vars[("Recording", "filename_pattern")]
        v.set("{platform}-{username}")
        self.assertEqual(p.hints[("Recording", "filename_pattern")].cget("text"), "\u2192 twitch-channel.mp4")

    def test_browse_dir_saves_and_dirlist_appends(self):
        self.dirs = ["C:\\Clips", "D:\\Old", "E:\\More"]
        p = self.make()
        spec = next(r for r in op.all_rows() if r["key"] == "clips_dir")
        p._browse(spec, p.vars[("Clipping", "clips_dir")])
        self.assertEqual(self.saved[-1], ("Clipping", "clips_dir", "C:\\Clips"))
        spec = next(r for r in op.all_rows() if r["key"] == "extra_dirs")
        p._browse(spec, p.vars[("Library", "extra_dirs")])
        p._browse(spec, p.vars[("Library", "extra_dirs")])
        self.assertEqual(self.saved[-1], ("Library", "extra_dirs", "D:\\Old;E:\\More"))

    def test_refused_change_reverts_with_message(self):
        def refuse(s, k, v):
            raise ValueError("Stop recording before changing the recordings folder.")
        p = op.OptionsPanel(self.root, self.cfg, ".", refuse, api=FakeApi(), environ={},
                            ask_dir=lambda i: "C:\\Elsewhere")
        spec = next(r for r in op.all_rows() if r["key"] == "streams_dir")
        p._browse(spec, p.vars[("Paths", "streams_dir")])
        self.assertIn("Stop recording", p.message.cget("text"))
        self.assertEqual(p.vars[("Paths", "streams_dir")].get(), "")

    def test_password_never_shown(self):
        self.cfg.v[("Fishtank", "password")] = "hunter2"
        p = self.make()
        v = p.vars[("Fishtank", "password")]
        self.assertEqual(v.get(), "")
        self.assertEqual(p.hints[("Fishtank", "password")].cget("text"), "saved - type to replace")
        v.set("n3w-pass")
        spec = next(r for r in op.all_rows() if r["key"] == "password")
        p._commit_field(spec, v)
        self.assertEqual(self.saved[-1], ("Fishtank", "password", "n3w-pass"))
        self.assertEqual(v.get(), "")
        self.assertNotIn("n3w-pass", p.message.cget("text"))

    def test_password_env_wins(self):
        p = self.make(environ={"MSR_FISHTANK_PASSWORD": "x"})
        self.assertIn("MSR_FISHTANK_PASSWORD", p.hints[("Fishtank", "password")].cget("text"))

    def test_fishtank_test_login_runs_in_background(self):
        self.cfg.v[("Fishtank", "email")] = "a@b.c"
        self.cfg.v[("Fishtank", "password")] = "pw"
        p = self.make(fishtank_test=lambda e, pw: self.tested.append((e, pw)) or True)
        p.test_fishtank()
        self.pump(lambda: p.hints[("Fishtank", "password")].cget("text") == "login OK")
        self.assertEqual(self.tested, [("a@b.c", "pw")])

    def test_ffmpeg_check(self):
        p = self.make(ffmpeg_check=lambda path: "7.1.1-full_build" if path == "ffmpeg" else None)
        p.check_ffmpeg("ffmpeg")
        self.pump(lambda: p.hints[("Advanced", "ffmpeg_path")].cget("text").startswith("ffmpeg "))
        self.assertEqual(p.hints[("Advanced", "ffmpeg_path")].cget("text"), "ffmpeg 7.1.1-full_build")
        p.check_ffmpeg("C:\\nope.exe")
        self.pump(lambda: p.hints[("Advanced", "ffmpeg_path")].cget("text") == "does not run")

    def test_quality_hint_follows_format(self):
        p = self.make()
        self.assertIn("lower is better", p.hints[("Clipping", "screenshot_quality")].cget("text"))
        p._commit("Clipping", "screenshot_format", "webp")
        self.assertIn("higher is better", p.hints[("Clipping", "screenshot_quality")].cget("text"))

    def test_advanced_group_starts_collapsed(self):
        p = self.make()
        box, toggle = p.groups["Advanced timing"]
        self.assertEqual(box.winfo_manager(), "")
        toggle()
        self.assertEqual(box.winfo_manager(), "pack")

    def test_bool_row_and_applies_note(self):
        p = self.make()
        p.vars[("Control", "enabled")].set(False)
        p._commit("Control", "enabled", "false")
        self.assertEqual(self.saved[-1], ("Control", "enabled", "false"))
        self.assertIn("restarting MSR", p.message.cget("text"))


if __name__ == "__main__":
    unittest.main()
