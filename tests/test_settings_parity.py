"""config_set and the Options tab share control.apply_setting: same checks, same refusals,
secrets encrypted on save. Temp data folders only."""

import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import control, dpapi  # noqa: E402
from msr.config import Config, read_secret  # noqa: E402
from tests.test_control import SECRET_PASSWORD, FakeRecorder, make_data  # noqa: E402


class SettingsParityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data, self.streams = make_data(self.tmp.name, ["kickone"])
        self.config = Config(os.path.join(self.data, "config.ini"))
        self.app = control.HeadlessApp(self.config, FakeRecorder)
        self.addCleanup(self.app.close)
        self.ctl = control.Controller(self.app)

    def both(self, section, key, value, recording=False):
        """(code from config_set, code from the Options path) for one write."""
        self.app.recorder = FakeRecorder(["kickone"]) if recording else None
        if recording:
            self.app.recorder.is_running = True
        reply = self.ctl.handle("config_set", {"section": section, "key": key, "value": value})
        try:
            control.apply_setting(self.config, section, key, value, recording=recording)
            direct = "ok"
        except control.ControlError as e:
            direct = e.code
        return ("ok" if reply["ok"] else reply["code"]), direct

    def test_same_refusals_both_ways(self):
        cases = [
            (("Timeouts", "poll_interval_minutes", "0.01"), "invalid"),
            (("Timeouts", "poll_interval_minutes", "abc"), "invalid"),
            (("Recording", "split_on_resolution_change", "maybe"), "invalid"),
            (("Clipping", "screenshot_format", "gif"), "invalid"),
            (("Paths", "streams_dir", os.path.join(self.tmp.name, "nope")), "invalid"),
            (("Nope", "thing", "1"), "not_found"),
            (("Fishtank", "password_dpapi", "x"), "invalid"),
            (("Timeouts", "poll_interval_minutes", "2"), "ok"),
        ]
        for args, want in cases:
            self.assertEqual(self.both(*args), (want, want), args)

    def test_recordings_folder_refused_while_recording(self):
        other = os.path.join(self.tmp.name, "other")
        os.makedirs(other)
        self.assertEqual(self.both("Paths", "streams_dir", other, recording=True), ("conflict", "conflict"))
        self.assertEqual(self.both("Paths", "streams_dir", other), ("ok", "ok"))

    def test_poll_interval_goes_live_through_the_recorder_lane(self):
        rec = FakeRecorder(["kickone"])
        rec.is_running = True
        self.app.recorder = rec
        reply = self.ctl.handle("config_set", {"section": "Timeouts", "key": "poll_interval_minutes", "value": "1"})
        self.assertTrue(reply["ok"], reply)
        self.assertEqual(reply["result"]["applies"], "now")
        self.assertIn(("set_poll_interval", 1.0), rec.calls)

    def test_secret_is_stored_encrypted_and_masked(self):
        reply = self.ctl.handle("config_set", {"section": "Fishtank", "key": "password", "value": "n3w-SECRET"})
        self.assertTrue(reply["ok"], reply)
        self.assertEqual(reply["result"]["value"], "<set>")
        with open(self.config.config_file, encoding="utf-8") as fh:
            text = fh.read()
        if dpapi.available():
            self.assertNotIn("n3w-SECRET", text)
            self.assertIn("password_dpapi", text)
        self.assertNotIn(SECRET_PASSWORD, text)
        self.assertEqual(read_secret(Config(self.config.config_file).config, "Fishtank", "password"), "n3w-SECRET")
        got = self.ctl.handle("config_get", {"section": "Fishtank"})["result"]["settings"]["Fishtank"]
        self.assertEqual(got["password"], "<set>")
        self.assertNotIn("password_dpapi", got)

    def test_clearing_a_secret_drops_the_encrypted_copy(self):
        self.ctl.handle("config_set", {"section": "Fishtank", "key": "password", "value": "abc"})
        self.assertTrue(self.ctl.handle("config_set", {"section": "Fishtank", "key": "password", "value": ""})["ok"])
        fresh = Config(self.config.config_file)
        self.assertEqual(read_secret(fresh.config, "Fishtank", "password"), "")
        self.assertFalse(fresh.config.has_option("Fishtank", "password_dpapi"))

    def test_gui_options_and_control_hooks_use_the_shared_function(self):
        with open(os.path.join(ROOT, "msr", "gui.py"), encoding="utf-8") as fh:
            src = fh.read()
        for hook in ("def _options_apply_setting", "def _control_apply_setting"):
            body = src[src.index(hook):]
            body = body[:body.index("\n    def ", 10)]
            self.assertIn("_control.apply_setting(", body, hook)
            self.assertNotIn("config.save()", body, hook)


if __name__ == "__main__":
    unittest.main()


class FeatureCatalogTests(unittest.TestCase):
    def test_gui_feature_map_names_real_cli_commands(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("msr_ctl_features", os.path.join(ROOT, "msr_ctl.py"))
        ctl = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ctl)
        cat = ctl.catalog()
        names = {c["command"] for c in cat["commands"]}
        for feature, cmds in cat["gui_features"].items():
            self.assertTrue(cmds, feature)
            self.assertEqual(set(cmds) - names, set(), feature)
        text = " ".join(cat["gui_features"])
        for word in ("Options", "Twitch", "Sounds", "Health"):
            self.assertIn(word, text)


class SoundAndHealthTests(unittest.TestCase):
    def test_sound_test_uses_the_adapter_and_never_plays_here(self):
        with tempfile.TemporaryDirectory() as tmp:
            data, _ = make_data(tmp, [])
            app = control.HeadlessApp(Config(os.path.join(data, "config.ini")), FakeRecorder)
            self.addCleanup(app.close)
            played = []
            app.sound_test = lambda event: played.append(event) or True
            ctl = control.Controller(app)
            self.assertTrue(ctl.handle("sound_test", {"event": "live"})["result"]["played"])
            self.assertEqual(ctl.handle("sound_test", {"event": "boom"})["code"], "bad_request")
            self.assertEqual(played, ["live"])
            off = control.Controller(control.OfflineApp(Config(os.path.join(data, "config.ini"))))
            self.assertEqual(off.handle("sound_test", {"event": "live"})["code"], "not_running")

    def test_health_includes_the_window_report(self):
        from msr import health
        with tempfile.TemporaryDirectory() as tmp:
            data, _ = make_data(tmp, [])
            app = control.HeadlessApp(Config(os.path.join(data, "config.ini")), FakeRecorder)
            self.addCleanup(app.close)
            app.health_report = lambda: health.Report([health.Issue(health.WARN, "disk", "low space")], 100.0)
            out = control.Controller(app).handle("health")["result"]
            self.assertEqual(out["monitor"]["level"], "warn")
            self.assertEqual(out["monitor"]["issues"][0]["key"], "disk")
