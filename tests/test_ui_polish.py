"""About text for 3.0 and the accent style of the primary button in every theme."""
import os
import subprocess
import sys
import tkinter as tk
import unittest
from tkinter import ttk
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import about, themes  # noqa: E402

DEPS = SimpleNamespace(HAS_YTDLP=True, YTDLP_VERSION="2026.10.26", HAS_STREAMLINK=True, STREAMLINK_VERSION="8.6.3",
                       HAS_FFMPEG=True, FFMPEG_VERSION="7.1", HAS_DENO=False, DENO_VERSION=None)


class AboutTests(unittest.TestCase):
    def text(self, in_venv=True):
        return about.about_text(
            "3.0.0", tools=about.tool_lines(DEPS),
            packages=about.package_line(lambda d: None if d == "plyer" else "1.0"),
            environment=about.environment_line(in_venv, "3.12.10"),
            streams=r"E:\Streams", clips=r"E:\Streams\Clips", data_dir=r"D:\msr-data")

    def test_content(self):
        t = self.text()
        self.assertTrue(t.startswith("Multi-Stream Recorder v3.0.0"))
        self.assertNotIn("Chaturbate", t)
        for must in ("Kick", "Twitch", "YouTube", "Rumble", "TikTok", "Fishtank.live", "yt-dlp 2026.10.26",
                     "streamlink 8.6.3", "ffmpeg 7.1", "deno: not found", "curl_cffi 1.0", "plyer (missing)",
                     "Updates\u2026 button", "MSR's own environment (.venv)", "msr_ctl.py",
                     r"Settings (config.ini, channels.json, cookies.txt): D:\msr-data", r"Recordings: E:\Streams",
                     "age-restricted or adult sites", "not responsible for how this software is used"):
            self.assertIn(must, t)
        self.assertNotIn("yt-dlp 1.0", t.split("Python packages:")[1])   # tools are not repeated as packages
        self.assertIn("not MSR's .venv", self.text(in_venv=False))
        self.assertNotIn("between seasons", t)   # a fixed text must not claim the show's current state

    def test_no_chaturbate_anywhere_in_gui(self):
        with open(os.path.join(ROOT, "msr", "gui.py"), encoding="utf-8") as fh:
            self.assertNotIn("Chaturbate and similar", fh.read())


class AccentButtonTests(unittest.TestCase):
    def test_accent_style_in_every_theme(self):
        try:
            root = tk.Tk()
        except tk.TclError:
            self.skipTest("no display")
        root.withdraw()
        self.addCleanup(root.destroy)
        from msr import themeui
        style = ttk.Style(root)
        for name, theme in themes.THEMES.items():
            themeui.apply_ttk(root, style, theme)
            self.assertEqual(style.lookup("Accent.TButton", "background").lower(), theme["accent"].lower(), name)
            self.assertEqual(style.lookup("Accent.TButton", "foreground").lower(), theme["accent_fg"].lower(), name)
            self.assertGreaterEqual(themes.contrast_ratio(theme["accent_fg"], theme["accent"]), 4.5, name)


if __name__ == "__main__":
    unittest.main()
