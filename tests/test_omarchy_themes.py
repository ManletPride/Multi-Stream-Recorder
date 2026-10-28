"""The Omarchy-inspired themes: built from published palettes (msr/theme_palettes.py) by themes.from_palette().
Pure Python, no window. The window itself is covered by test_theme_dialog / test_theme_smoke, which walk
themes.THEME_NAMES and so include these themes."""
import hashlib
import json
import re
import tempfile
import unittest
from pathlib import Path

from msr import theme_dialog, theme_palettes, themes

ROOT = Path(__file__).resolve().parent.parent
NEW = [p["name"] for p in theme_palettes.PALETTES]
ORIGINAL = ("PowerShell Blue", "Neutral Dark", "Light", "High Contrast", "Midnight Purple", "Paper")
# sha256 of (name, dark, sorted colours) taken before the Omarchy themes were added: the old six must not change.
ORIGINAL_DIGEST = {"PowerShell Blue": "f1c0977e9e0e67c2", "Neutral Dark": "f2bbd043daa4a0e5", "Light": "bed62e5ce89af50f",
                   "High Contrast": "d54fbe730de1ff05", "Midnight Purple": "0d1eee940dfcede6", "Paper": "60214a757f6b9f38"}
RED_HUES = (25.0, 330.0)  # a "red" hue is below 25 or above 330 degrees


def digest(theme):
    return hashlib.sha256(json.dumps([theme.name, theme.dark, sorted(theme.colors.items())]).encode()).hexdigest()[:16]


class ListTests(unittest.TestCase):
    def test_the_ten_themes_are_there_and_the_skipped_ones_are_not(self):
        self.assertEqual(sorted(NEW), sorted([
            "Arc Blueberry", "Catppuccin Mocha OLED", "Rose Pine", "Gruvbox Material", "Dracula", "Flexoki Dark",
            "Vesper", "Tokyo Night OLED", "Tokyo Day", "Solarized Light"]))
        for skipped in ("Artzen", "Evergarden", "Modus Operandi", "Milky Matcha", "Ristretto Light"):
            self.assertIsNone(themes.find(skipped), skipped)
        self.assertEqual(len(NEW), 10)

    def test_powershell_blue_is_still_the_default_and_first_and_the_old_six_are_unchanged(self):
        self.assertEqual(themes.DEFAULT_THEME, "PowerShell Blue")
        self.assertEqual(themes.THEME_NAMES[0], "PowerShell Blue")
        self.assertEqual(themes.THEME_NAMES[:6], ORIGINAL)
        self.assertEqual(themes.current().name if themes.current().name in ORIGINAL else "x", themes.current().name)
        for name in ORIGINAL:
            self.assertEqual(digest(themes.THEMES[name]), ORIGINAL_DIGEST[name], f"{name} changed")
            self.assertIsNone(themes.THEMES[name].credit)
            self.assertEqual(themes.THEMES[name].credit_line(), "")

    def test_dark_ones_come_before_light_ones_and_the_window_groups_match(self):
        names = list(themes.THEME_NAMES)
        dark = [n for n in NEW if themes.THEMES[n].dark]
        light = [n for n in NEW if not themes.THEMES[n].dark]
        self.assertEqual(sorted(light), ["Solarized Light", "Tokyo Day"])
        self.assertLess(max(names.index(n) for n in dark), min(names.index(n) for n in light))
        groups = theme_dialog.theme_groups()
        self.assertEqual([label for label, _ in groups],
                         ["Built-in", "Dark, from Omarchy palettes", "Light, from Omarchy palettes", "Your themes"])
        shown = [t.name for _, group in groups for t in group]
        self.assertEqual(shown, list(themes.THEME_NAMES))              # every theme once, in list order
        self.assertTrue(all(themes.THEMES[n].dark for n in themes.GROUP_DARK))
        self.assertFalse(any(themes.THEMES[n].dark for n in themes.GROUP_LIGHT))
        self.assertEqual(set(themes.GROUP_BUILTIN), set(ORIGINAL))

    def test_names_are_plain_ascii_so_config_ini_stays_simple(self):
        for name in NEW:
            self.assertTrue(name.isascii() and name.isprintable() and len(name) <= 40, name)


class ReadabilityTests(unittest.TestCase):
    def test_every_theme_passes_msrs_own_audit(self):
        for name in NEW:
            t = themes.THEMES[name]
            text, graphic = themes.audit(t)
            self.assertGreaterEqual(text, 4.5, name)
            self.assertGreaterEqual(graphic, 3.0, name)
            for a, b in themes.TEXT_PAIRS:
                self.assertGreaterEqual(themes.contrast_ratio(t[a], t[b]), 4.5, f"{name}: {a} on {b}")
            for tok, bg in themes.GRAPHIC_PAIRS:
                self.assertGreaterEqual(themes.contrast_ratio(t[tok], t[bg]), 3.0, f"{name}: {tok} on {bg}")
            self.assertGreater(themes.contrast_ratio(t["chart_track"], t["bg"]), 1.05, name)
            for token in themes.COLOR_TOKENS:
                self.assertTrue(themes.is_hex_color(t.get(token)), f"{name}: {token}")

    def test_start_and_stop_and_ok_and_error_are_visibly_different_and_the_right_hue(self):
        for name in NEW:
            t = themes.THEMES[name]
            self.assertGreaterEqual(themes.color_distance(t["start_bg"], t["stop_bg"]), themes.STATUS_MIN_DISTANCE, name)
            self.assertGreaterEqual(themes.color_distance(t["ok"], t["error"]), themes.STATUS_MIN_DISTANCE, name)
            self.assertGreaterEqual(themes.color_distance(t["ok"], t["warn"]), 15.0, name)
            for token in ("start_bg", "ok", "rec_fg"):
                self.assertTrue(themes.GREEN_HUES[0] <= themes.hue_of(t[token]) <= themes.GREEN_HUES[1],
                                f"{name}: {token} {t[token]} is not green (hue {themes.hue_of(t[token]):.0f})")
            for token in ("stop_bg", "error", "log_error"):
                hue = themes.hue_of(t[token])
                self.assertTrue(hue <= RED_HUES[0] or hue >= RED_HUES[1], f"{name}: {token} {t[token]} is not red (hue {hue:.0f})")

    def test_rose_pine_start_is_green_not_pine_teal(self):
        t = themes.THEMES["Rose Pine"]
        pine = "#31748f"
        self.assertGreater(themes.color_distance(t["start_bg"], pine), 20.0)
        self.assertLess(abs(themes.hue_of(t["start_bg"]) - 140), 5)       # hue-shifted pine, not teal (197 degrees)
        spec = next(p for p in theme_palettes.PALETTES if p["name"] == "Rose Pine")
        self.assertEqual(spec["green"], pine)                             # the published value stays on record
        self.assertTrue(spec["fix_note"])

    def test_chart_series_are_six_clearly_different_colours(self):
        for name in NEW:
            series = themes.THEMES[name].series()
            self.assertEqual(len(set(series)), 6, name)
            fallback = themes._SITES_DARK if themes.THEMES[name].dark else themes._SITES_LIGHT
            self.assertEqual(set(series) & {fallback[f"series_{i}"] for i in range(1, 7)}, set(), f"{name} used the default chart colours")
            for i, a in enumerate(series):
                for b in series[i + 1:]:
                    self.assertGreaterEqual(themes.color_distance(a, b), themes.SERIES_MIN_DISTANCE, f"{name}: {a} vs {b}")

    def test_series_6_is_not_a_near_duplicate_of_series_3(self):
        # the first sketch fell back to a lightened yellow when a palette had no orange (Catppuccin, Flexoki, Rose Pine)
        for name in NEW:
            t = themes.THEMES[name]
            self.assertGreaterEqual(themes.color_distance(t["series_6"], t["series_3"]), themes.SERIES_MIN_DISTANCE, name)
            self.assertGreaterEqual(themes.color_distance(t["series_3"], t["series_5"]), themes.SERIES_MIN_DISTANCE, name)


class PaletteFidelityTests(unittest.TestCase):
    def test_surfaces_accent_and_selection_are_the_published_values(self):
        for spec in theme_palettes.PALETTES:
            t = themes.THEMES[spec["name"]]
            self.assertEqual(t["bg"], spec["bg"].lower(), spec["name"])
            self.assertEqual(t["accent"], spec["accent"].lower(), spec["name"])
            self.assertEqual(t["btn_bg"], spec["selection"].lower(), spec["name"]) if themes.contrast_ratio(
                t["fg"], spec["selection"]) >= 4.5 else None
            self.assertEqual(t.dark, bool(spec["dark"]))

    def test_readability_nudges_keep_the_hue(self):
        for spec in theme_palettes.PALETTES:
            t = themes.THEMES[spec["name"]]
            fixes = spec.get("fix", {})
            for key, token in (("red", "error"), ("green", "ok"), ("yellow", "warn"), ("blue", "link")):
                published = fixes.get(key, spec[key])
                if t[token] == published.lower():
                    continue
                delta = abs(themes.hue_of(t[token]) - themes.hue_of(published))
                self.assertLess(min(delta, 360 - delta), 4.0, f"{spec['name']}: {token} {published} -> {t[token]}")

    def test_only_rose_pine_replaces_a_published_colour(self):
        self.assertEqual([p["name"] for p in theme_palettes.PALETTES if p.get("fix")], ["Rose Pine"])

    def test_building_is_deterministic(self):
        for spec in theme_palettes.PALETTES:
            self.assertEqual(digest(themes.from_palette(spec)), digest(themes.THEMES[spec["name"]]))

    def test_from_palette_does_not_need_orange_or_tints(self):
        spec = dict(name="Bare", dark=True, bg="#101010", fg="#e0e0e0", selection="#404040", accent="#5599ff",
                    red="#ff5555", green="#55ff55", yellow="#ffff55", blue="#5555ff", magenta="#ff55ff", cyan="#55ffff")
        t = themes.from_palette(spec)
        self.assertEqual(len(set(t.series())), 6)
        a, g = themes.audit(t)
        self.assertGreaterEqual(a, 4.5)
        self.assertGreaterEqual(g, 3.0)


class CreditTests(unittest.TestCase):
    def setUp(self):
        self.notices = (ROOT / "THIRD_PARTY_NOTICES.md").read_text(encoding="utf-8")

    def test_every_theme_has_source_licence_and_credit(self):
        for spec in theme_palettes.PALETTES:
            c = spec["credit"]
            for key in ("port", "port_license", "upstream", "upstream_url", "upstream_license", "file"):
                self.assertTrue(isinstance(c.get(key), str) and c[key].strip(), f"{spec['name']}: {key}")
            for key in ("port", "upstream_url"):
                self.assertRegex(c[key], r"^https://github\.com/[\w.-]+/[\w.-]+$", spec["name"])
            t = themes.THEMES[spec["name"]]
            self.assertEqual(t.credit, c)
            line = t.credit_line()
            self.assertIn(c["upstream"], line)
            self.assertIn(c["port"], line)
            self.assertIn(c["port_license"], line)

    def test_the_notices_file_lists_every_theme_its_sources_and_licences(self):
        for spec in theme_palettes.PALETTES:
            c = spec["credit"]
            for needle in (spec["name"], c["port"], c["upstream_url"], c["upstream"], c["port_license"], c["upstream_license"]):
                self.assertIn(needle, self.notices, f"{spec['name']}: {needle}")
        for needle in ("Omarchy", "David Heinemeier Hansson", "MIT"):
            self.assertIn(needle, self.notices, needle)

    def test_no_licence_is_left_unconfirmed(self):
        for needle in ("could not be confirmed", "not checked", "unconfirmed", "Apache-2.0"):
            self.assertNotIn(needle, self.notices, needle)
        for spec in theme_palettes.PALETTES:
            for text in spec["credit"].values():
                self.assertNotIn("could not be confirmed", text)
                self.assertNotIn("not checked", text)

    def test_every_credit_names_an_author_and_a_licence(self):
        for spec in theme_palettes.PALETTES:
            c = spec["credit"]
            self.assertRegex(c["upstream_license"], r"(MIT|GPL)", spec["name"])
            self.assertIn("Copyright (c)", c["upstream_license"], spec["name"])

    def credit_of(self, name):
        return next(p for p in theme_palettes.PALETTES if p["name"] == name)["credit"]

    def test_tokyo_themes_credit_enkia_with_the_full_mit_text(self):
        for name in ("Tokyo Night OLED", "Tokyo Day"):
            c = self.credit_of(name)
            self.assertIn("Enkia", c["upstream"])
            self.assertEqual(c["upstream_url"], "https://github.com/tokyo-night/tokyo-night-vscode-theme")
            self.assertEqual(c["upstream_license"], "MIT, Copyright (c) 2018-present Enkia")
            self.assertIn("Enkia", themes.THEMES[name].credit_line())
        self.assertIn("Tokyo Night: color palette adapted from https://github.com/tokyo-night/tokyo-night-vscode-theme", self.notices)
        self.assertIn("Copyright (c) 2018-present Enkia", self.notices)
        self.assertIn("Alexey Konoplev", self.credit_of("Tokyo Day")["upstream"])

    def test_arc_blueberry_credits_beardedbear_and_vale(self):
        c = self.credit_of("Arc Blueberry")
        self.assertIn("BeardedBear", c["upstream"])
        self.assertEqual(c["upstream_url"], "https://github.com/BeardedBear/bearded-theme")
        self.assertIn("MIT as of 2021 (Copyright (c) 2021 BeardedBear)", c["upstream_license"])
        self.assertIn("GPL-3.0 since 2023-04-14", c["upstream_license"])
        self.assertIn("Only color values are used; no code or images", c["upstream_license"])
        self.assertEqual(c["port"], "https://github.com/vale-c/omarchy-arc-blueberry")
        self.assertEqual(c["port_license"], "MIT, Copyright (c) 2025 Vale")
        line = themes.THEMES["Arc Blueberry"].credit_line()
        self.assertIn("BeardedBear", line)
        self.assertIn("Vale", line)
        self.assertIn("Copyright (c) 2021 BeardedBear", self.notices)
        self.assertIn("Copyright (c) 2025 Vale", self.notices)
        self.assertIn("src/variations/arc.ts", self.notices)

    def test_omarchy_is_credited_once_with_its_licence_text(self):
        self.assertIn("Omarchy theme system and palettes: MIT, Copyright (c) David Heinemeier Hansson", self.notices)
        self.assertIn("https://github.com/omacom/omarchy", self.notices)
        self.assertEqual(self.notices.count("### Omarchy (https://github.com/omacom/omarchy)"), 1)

    def test_every_mit_notice_carries_the_full_permission_text(self):
        holders = ("2018-present Enkia", "2025 Vale", "2021 BeardedBear", "2021 Catppuccin", "2023 Ros\u00e9 Pine",
                   "2020 sainnhe", "2023 Dracula Theme", "2023 Steph Ango", "2023 Rauno Freiberg",
                   "2011 Ethan Schoonover", "2025 Euan", "2026 Alexey Konoplev")
        permission = "Permission is hereby granted, free of charge, to any person obtaining a copy"
        flat = " ".join(self.notices.split())  # the projects wrap their licence text differently
        self.assertGreaterEqual(flat.count(permission), len(holders) + 1)
        for h in holders:
            self.assertIn("Copyright (c) " + h, self.notices, h)
        self.assertGreaterEqual(flat.count("THE SOFTWARE IS PROVIDED \"AS IS\""), len(holders) + 1)

    def test_catppuccin_notice_is_honest_about_the_missing_licence_file(self):
        self.assertIn("no LICENSE file", self.credit_of("Catppuccin Mocha OLED")["port_license"])
        self.assertIn("has no LICENSE file", self.notices)

    def test_the_notices_file_holds_no_local_paths_or_secrets(self):
        for needle in ("C:\\", "E:\\", "config.ini", "cookie", "password"):
            self.assertNotIn(needle, self.notices.replace("`config.ini`", ""), needle)


class SavedNameTests(unittest.TestCase):
    def test_old_saved_names_still_resolve(self):
        for name in ORIGINAL:
            self.assertEqual(themes.resolve_saved(name, None).name, name)
            self.assertEqual(themes.resolve_saved(name.upper(), False).name, name)
        self.assertEqual(themes.resolve_saved("", True).name, "PowerShell Blue")
        self.assertEqual(themes.resolve_saved("", False).name, "Light")
        self.assertEqual(themes.resolve_saved("", None).name, "PowerShell Blue")
        self.assertEqual(themes.resolve_saved("Dark Mode (retired)", True).name, "PowerShell Blue")

    def test_new_names_resolve_case_insensitively(self):
        for name in NEW:
            self.assertEqual(themes.resolve_saved(name, None).name, name)
            self.assertEqual(themes.resolve_saved(name.lower(), None).name, name)

    def test_a_user_theme_can_be_based_on_a_new_theme_but_not_reuse_its_name(self):
        self.addCleanup(themes.register_user_themes, [])
        good, why = themes.validate_user_theme({"name": "Mine", "base": "Dracula", "colors": {"accent": "#ff00aa"}})
        self.assertEqual(why, "")
        self.assertEqual(good["bg"], themes.THEMES["Dracula"]["bg"])
        self.assertIsNone(good.credit)
        bad, why = themes.validate_user_theme({"name": "dracula", "base": "Light"})
        self.assertIsNone(bad)
        self.assertIn("built-in", why)
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "a.json").write_text(json.dumps({"name": "Tokyo Night OLED"}), encoding="utf-8")
            found, problems = themes.load_user_themes(tmp)
        self.assertEqual((found, len(problems)), ([], 1))


if __name__ == "__main__":
    unittest.main()
