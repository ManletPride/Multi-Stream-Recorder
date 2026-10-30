"""App icon: the assets, window icons on every Toplevel, tray image, taskbar id, shortcuts.
Temp dirs only; no window is shown and no real shortcut is written."""
import os
import struct
import subprocess
import sys
import tempfile
import tkinter as tk
import unittest
import xml.etree.ElementTree as ET

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import appicon  # noqa: E402

ICO_SIZES = [16, 20, 24, 32, 40, 48, 64, 128, 256]


def png_size(path):
    with open(path, "rb") as fh:
        head = fh.read(24)
    assert head[:8] == b"\x89PNG\r\n\x1a\n"
    return struct.unpack(">II", head[16:24])


class AssetTests(unittest.TestCase):
    def test_pngs_have_their_sizes(self):
        for n in (16, 20, 24, 32, 40, 48, 64, 128, 256, 512):
            self.assertEqual(png_size(appicon.png(n)), (n, n))

    def test_ico_has_every_windows_size(self):
        with open(appicon.ICO, "rb") as fh:
            data = fh.read()
        reserved, kind, count = struct.unpack("<HHH", data[:6])
        self.assertEqual((reserved, kind), (0, 1))
        sizes = sorted((data[6 + 16 * i] or 256) for i in range(count))
        self.assertEqual(sizes, ICO_SIZES)

    def test_svgs_are_hand_written_vectors(self):
        for name in ("icon.svg", "icon_medium.svg", "icon_small.svg"):
            tree = ET.parse(os.path.join(appicon.ASSETS, name))
            root = tree.getroot()
            self.assertEqual(root.get("viewBox"), "0 0 256 256")
            tags = {el.tag.split("}")[-1] for el in root.iter()}
            self.assertNotIn("image", tags)          # no embedded bitmap / tracing
            self.assertLessEqual(tags, {"svg", "title", "rect", "g", "path", "circle"})


class WindowIconTests(unittest.TestCase):
    def test_root_and_later_toplevels_get_the_icon(self):
        root = tk.Tk()
        root.withdraw()
        try:
            calls = []
            orig_photo, orig_bitmap = root.iconphoto, root.iconbitmap
            root.iconphoto = lambda default, *imgs: (calls.append(("photo", default, len(imgs))),
                                                     orig_photo(default, *imgs))
            root.iconbitmap = lambda *a, **k: (calls.append(("bitmap", k.get("default"))), orig_bitmap(*a, **k))
            self.assertTrue(appicon.apply(root))
            # default=True / default=: the icon of every Toplevel made later (Clip Editor, dialogs)
            self.assertIn(("photo", True, len(appicon.PHOTO_SIZES)), calls)
            if sys.platform == "win32":
                self.assertIn(("bitmap", appicon.ICO), calls)
            top = tk.Toplevel(root)
            top.withdraw()
            top.destroy()
        finally:
            root.destroy()

    def test_app_id(self):
        self.assertEqual(appicon.set_app_id(), sys.platform == "win32")

    def test_tray_image_shows_the_state_on_the_dot(self):
        rec, idle = appicon.tray_image(True), appicon.tray_image(False)
        self.assertEqual(rec.size, (64, 64))
        x, y = round(81 * 64 / 256), round(104 * 64 / 256)
        r, g, b, _a = rec.getpixel((x, y))
        self.assertGreater(r, 200)
        self.assertLess(g, 80)                        # red while recording
        r, g, b, _a = idle.getpixel((x, y))
        self.assertLess(abs(r - g), 30)               # grey when idle


class ShortcutTests(unittest.TestCase):
    def test_script_quotes_paths(self):
        s = appicon.shortcut_script(r"C:\Users\O'Neil\Desktop\MSR.lnk", r"C:\py\pythonw.exe", '"a b.pyw"',
                                    r"C:\msr", r"C:\msr\assets\icon.ico")
        self.assertIn(r"'C:\Users\O''Neil\Desktop\MSR.lnk'", s)
        self.assertIn(r"IconLocation = 'C:\msr\assets\icon.ico,0'", s)

    def test_new_shortcut_never_replaces_an_existing_one(self):
        calls = []

        def run(cmd, **kw):
            calls.append(cmd)
            if sys.platform == "win32":
                self.assertIn("creationflags", kw)
            return subprocess.CompletedProcess(cmd, 0, "", "")
        with tempfile.TemporaryDirectory() as d:
            old = os.path.join(d, "Multi-Stream Recorder.lnk")
            open(old, "wb").close()
            made = appicon.create_windows_shortcut(old, run=run)
            self.assertEqual(made, os.path.join(d, "Multi-Stream Recorder (2).lnk"))
            self.assertIn("run_msr.pyw", calls[-1][-1])
            appicon.create_windows_shortcut(os.path.join(d, "x.lnk"), data_dir=r"D:\data", run=run)
            self.assertIn(r'--data-dir "D:\data"', calls[-1][-1])
            self.assertIn("run_msr.pyw", calls[-1][-1])   # keeps .venv, not the app directly
            self.assertNotIn("Multi-Stream-Recorder.py", calls[-1][-1])

    def test_desktop_dir_exists(self):
        self.assertTrue(appicon.desktop_dir().endswith("Desktop") or os.path.isdir(appicon.desktop_dir()))

    def test_linux_desktop_entry(self):
        text = appicon.desktop_entry("/usr/bin/python3", "/home/me/msr-data")
        self.assertTrue(text.startswith("[Desktop Entry]"))
        self.assertIn("Icon=" + appicon.png(256), text)
        self.assertIn('--data-dir "/home/me/msr-data"', text)

    def test_headless_paths_do_not_load_the_icon(self):
        for rel in ("msr/control.py", "msr/recorder.py", "msr/worker.py", "Multi-Stream-Recorder.py"):
            with open(os.path.join(ROOT, rel), encoding="utf-8") as fh:
                self.assertNotIn("appicon", fh.read(), rel)


if __name__ == "__main__":
    unittest.main()
