"""Architectural tests for the decomposed GUI panels (RosterPanel, StatusPanel,
PreviewStrip, BottomBar) and zero NameError-guard invariant in msr/gui.py.
"""
import ast
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from msr.bottom_bar import BottomBar  # noqa: E402
from msr.preview_strip import PreviewStrip  # noqa: E402
from msr.roster_panel import RosterPanel  # noqa: E402
from msr.status_panel import StatusPanel, display_name, platform_label  # noqa: E402


class GuiDecompositionTests(unittest.TestCase):
    def test_no_name_error_guards_in_gui_modules(self):
        """Decomposed panel modules and gui.py must never rely on `except NameError`."""
        targets = (
            "msr/gui.py",
            "msr/roster_panel.py",
            "msr/status_panel.py",
            "msr/preview_strip.py",
            "msr/bottom_bar.py",
        )
        found = []
        for rel in targets:
            tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler) and node.type is not None:
                    names = []
                    if isinstance(node.type, ast.Name):
                        names.append(node.type.id)
                    elif isinstance(node.type, ast.Tuple):
                        names.extend(
                            elt.id for elt in node.type.elts if isinstance(elt, ast.Name)
                        )
                    if "NameError" in names:
                        found.append(f"{rel}:{node.lineno}")
        self.assertEqual(found, [], f"Unexpected NameError guards: {found}")

    def test_gui_panel_classes_are_extracted_and_clean(self):
        self.assertEqual(PreviewStrip.PREVIEW_MS, 4000)
        self.assertEqual(PreviewStrip.PREVIEW_W, 160)
        self.assertEqual(PreviewStrip.PREVIEW_H, 90)
        self.assertIn("Channel", StatusPanel.VISIBLE_STATUS_COLUMNS)
        self.assertTrue(hasattr(RosterPanel, "apply_theme"))
        self.assertTrue(hasattr(StatusPanel, "apply_theme"))
        self.assertTrue(hasattr(BottomBar, "apply_theme"))

    def test_status_display_and_platform_helpers(self):
        self.assertEqual(platform_label("twitch:alice"), "Twitch")
        self.assertEqual(platform_label("youtube:@bob"), "YouTube")
        self.assertEqual(platform_label("fishtank:director"), "Fishtank")
        self.assertEqual(platform_label("rumble:carol"), "Rumble")
        self.assertEqual(platform_label("tiktok:@dave"), "TikTok")
        self.assertEqual(platform_label("barekick"), "Kick")
        self.assertEqual(display_name("twitch:alice"), "alice")
        self.assertEqual(display_name("barekick"), "barekick")


if __name__ == "__main__":
    unittest.main()
