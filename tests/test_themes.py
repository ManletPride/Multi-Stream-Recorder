"""Palettes, contrast, migration and user themes. Pure Python: no window needed."""
import json
import tempfile
import unittest
from pathlib import Path

from msr import themes


class PaletteTests(unittest.TestCase):
    def test_the_list_has_the_default_light_and_high_contrast_themes(self):
        names = list(themes.THEME_NAMES)
        self.assertEqual(names[0], "PowerShell Blue")
        self.assertEqual(themes.DEFAULT_THEME, "PowerShell Blue")
        for expected in ("Neutral Dark", "Light", "High Contrast"):
            self.assertIn(expected, names)
        self.assertEqual(len(names), len({n.lower() for n in names}))  # no duplicate names
        self.assertTrue(any(not themes.THEMES[n].dark for n in names))
        self.assertTrue(any(themes.THEMES[n].dark for n in names))

    def test_every_theme_defines_every_token_as_a_hex_colour(self):
        for name in themes.THEME_NAMES:
            t = themes.THEMES[name]
            self.assertEqual(t.name, name)
            for token in themes.COLOR_TOKENS:
                self.assertTrue(themes.is_hex_color(t.get(token)), f"{name}: {token} = {t.get(token)!r}")
            self.assertEqual(set(t.colors) - set(themes.COLOR_TOKENS), set(), f"{name} has unknown tokens")
            self.assertIsInstance(t.dark, bool)
            self.assertEqual(len(t.series()), 6)

    def test_text_contrast_is_at_least_4_5_in_every_theme(self):
        for name in themes.THEME_NAMES:
            t = themes.THEMES[name]
            for text, background in themes.TEXT_PAIRS:
                ratio = themes.contrast_ratio(t[text], t[background])
                self.assertGreaterEqual(ratio, 4.5, f"{name}: {text} on {background} is {ratio:.2f}:1")

    def test_chart_colours_reach_3_to_1_against_their_panel_in_every_theme(self):
        for name in themes.THEME_NAMES:
            t = themes.THEMES[name]
            for token, background in themes.GRAPHIC_PAIRS:
                ratio = themes.contrast_ratio(t[token], t[background])
                self.assertGreaterEqual(ratio, 3.0, f"{name}: {token} on {background} is {ratio:.2f}:1")
            # the tracks and grid lines sit on the chart panel too
            self.assertGreater(themes.contrast_ratio(t["chart_track"], t["bg"]), 1.05, name)

    def test_the_audit_matches_the_pairs(self):
        text, graphic = themes.audit(themes.THEMES["High Contrast"])
        self.assertGreaterEqual(text, 7.0)  # the high contrast theme earns its name (AAA)
        self.assertGreaterEqual(graphic, 4.5)
        for name in themes.THEME_NAMES:
            a, b = themes.audit(themes.THEMES[name])
            self.assertGreaterEqual(a, 4.5)
            self.assertGreaterEqual(b, 3.0)

    def test_series_colours_differ_from_each_other(self):
        for name in themes.THEME_NAMES:
            self.assertEqual(len(set(themes.THEMES[name].series())), 6, name)

    def test_contrast_maths(self):
        self.assertAlmostEqual(themes.contrast_ratio("#000000", "#ffffff"), 21.0, places=2)
        self.assertAlmostEqual(themes.contrast_ratio("#777777", "#777777"), 1.0, places=3)
        self.assertEqual(themes.contrast_ratio("#123456", "#abcdef"), themes.contrast_ratio("#abcdef", "#123456"))
        self.assertTrue(themes.is_hex_color("#a1B2c3"))
        for bad in ("#abc", "red", "", None, 5, "#12345g", "#1234567"):
            self.assertFalse(themes.is_hex_color(bad), bad)

    def test_theme_objects(self):
        t = themes.THEMES[themes.DEFAULT_THEME]
        self.assertEqual(t["bg"], t.bg)
        self.assertEqual(t.site("Kick"), t["site_kick"])
        self.assertEqual(t.site("brand-new-site", 7), t.series()[1])  # unknown sites cycle the series colours
        self.assertEqual(t.get("nope", "x"), "x")
        with self.assertRaises(AttributeError):
            t.bg = "#000000"
        with self.assertRaises(KeyError):
            t["nope"]
        self.assertEqual(themes.caption_colors(t), (t["title_bg"], t["title_fg"], t["title_border"]))

    def test_find_is_case_insensitive_and_unknown_is_none(self):
        self.assertIs(themes.find("powershell blue"), themes.THEMES["PowerShell Blue"])
        self.assertIsNone(themes.find("no such theme"))
        self.assertIsNone(themes.find(None))
        self.assertIsNone(themes.find(""))

    def test_set_current_notifies_and_falls_back_to_the_default(self):
        self.addCleanup(themes.set_current, themes.current())
        seen = []
        listener = themes.on_change(seen.append)
        self.addCleanup(themes._listeners.remove, listener)
        self.assertIs(themes.set_current("Light"), themes.THEMES["Light"])
        self.assertIs(themes.current(), themes.THEMES["Light"])
        self.assertIs(themes.set_current("garbage"), themes.THEMES[themes.DEFAULT_THEME])
        self.assertEqual([t.name for t in seen], ["Light", "PowerShell Blue"])


class MigrationTests(unittest.TestCase):
    def test_nothing_saved_means_powershell_blue(self):
        self.assertEqual(themes.resolve_saved("", None).name, "PowerShell Blue")
        self.assertEqual(themes.resolve_saved(None, None).name, "PowerShell Blue")

    def test_old_dark_mode_switch_migrates(self):
        self.assertEqual(themes.resolve_saved("", True).name, "PowerShell Blue")
        self.assertEqual(themes.resolve_saved("", False).name, themes.LIGHT_THEME)

    def test_a_saved_theme_wins_over_the_old_switch(self):
        self.assertEqual(themes.resolve_saved("Midnight Purple", False).name, "Midnight Purple")
        self.assertEqual(themes.resolve_saved("light", True).name, "Light")

    def test_an_unknown_saved_name_falls_back_the_same_way(self):
        self.assertEqual(themes.resolve_saved("deleted theme", True).name, "PowerShell Blue")
        self.assertEqual(themes.resolve_saved("deleted theme", False).name, "Light")

    def test_parse_bool_reads_the_old_values(self):
        for text in ("true", "True", "1", "yes", "on", True):
            self.assertIs(themes.parse_bool(text), True)
        for text in ("false", "False", "0", "no", "off", False):
            self.assertIs(themes.parse_bool(text), False)
        self.assertIsNone(themes.parse_bool("maybe"))
        self.assertIs(themes.parse_bool("maybe", True), True)
        self.assertIsNone(themes.parse_bool(None))


class UserThemeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.addCleanup(themes.register_user_themes, [])

    def write(self, name, data, raw=False):
        (self.dir / name).write_text(data if raw else json.dumps(data), encoding="utf-8")

    def test_a_valid_file_becomes_a_theme_built_on_a_base(self):
        self.write("mine.json", {"name": "Mine", "base": "Light", "colors": {"bg": "#101820", "accent": "#2A9D8F"}})
        found, problems = themes.load_user_themes(str(self.dir))
        self.assertEqual(problems, [])
        self.assertEqual([t.name for t in found], ["Mine"])
        t = found[0]
        self.assertEqual(t["bg"], "#101820")
        self.assertEqual(t["accent"], "#2a9d8f")
        self.assertEqual(t["fg"], themes.THEMES["Light"]["fg"])  # the rest comes from the base
        self.assertFalse(t.dark)  # and so does the dark flag unless the file says otherwise
        themes.register_user_themes(found)
        self.assertIs(themes.find("mine"), t)
        self.assertNotIn("Mine", themes.THEME_NAMES)  # built-ins are a fixed list
        themes.register_user_themes([])
        self.assertIsNone(themes.find("Mine"))

    def test_bad_files_are_ignored_and_reported_never_raised(self):
        self.write("notjson.json", "{ nope", raw=True)
        self.write("list.json", [1, 2])
        self.write("noname.json", {"colors": {}})
        self.write("builtin.json", {"name": "Light", "colors": {}})
        self.write("badbase.json", {"name": "X1", "base": "Nope"})
        self.write("badtoken.json", {"name": "X2", "colors": {"nonsense": "#000000"}})
        self.write("badvalue.json", {"name": "X3", "colors": {"bg": "red"}})
        self.write("badvalue2.json", {"name": "X4", "colors": {"bg": "url(http://x)"}})
        self.write("colors.json", {"name": "X5", "colors": [1]})
        self.write("big.json", json.dumps({"name": "Big", "pad": "x" * 30000}), raw=True)
        self.write("good.json", {"name": "Good", "colors": {"bg": "#000000"}})
        self.write("readme.txt", "ignored")
        found, problems = themes.load_user_themes(str(self.dir))
        self.assertEqual([t.name for t in found], ["Good"])
        self.assertEqual(len(problems), 10)

    def test_duplicates_and_a_missing_folder(self):
        self.write("a.json", {"name": "Twin", "colors": {}})
        self.write("b.json", {"name": "twin", "colors": {}})
        found, problems = themes.load_user_themes(str(self.dir))
        self.assertEqual(len(found), 1)
        self.assertEqual(len(problems), 1)
        self.assertEqual(themes.load_user_themes(str(self.dir / "missing")), ([], []))

    def test_only_the_first_twenty_files_count(self):
        for i in range(30):
            self.write(f"t{i:02d}.json", {"name": f"T{i:02d}", "colors": {}})
        found, _ = themes.load_user_themes(str(self.dir))
        self.assertEqual(len(found), themes.MAX_USER_THEMES)

    def test_the_dark_flag_and_name_rules(self):
        theme, why = themes.validate_user_theme({"name": "D", "dark": "false", "base": "PowerShell Blue"})
        self.assertFalse(theme.dark, why)
        self.assertIsNone(themes.validate_user_theme({"name": "x" * 41})[0])
        self.assertIsNone(themes.validate_user_theme({"name": "  "})[0])
        self.assertIsNone(themes.validate_user_theme({"name": "a\nb"})[0])
        self.assertIsNone(themes.validate_user_theme("text")[0])

    def test_a_user_theme_cannot_replace_a_built_in(self):
        theme = themes.Theme("Light", True, dict(themes.THEMES["Light"].colors))
        themes.register_user_themes([theme])
        self.assertFalse(themes.THEMES["Light"].dark)


if __name__ == "__main__":
    unittest.main()
