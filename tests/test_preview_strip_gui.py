"""The real window, never shown (fully transparent while it is "mapped"), with a fake recorder and a fake
grab: the preview strip's tiles, their age text and warning border, the large pane, "ended", and the
pause while the window is hidden. Nothing is recorded or written outside a temporary folder."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

SCRIPT = r'''
import json, os, shutil, struct, sys, tempfile, threading, time, zlib
import tkinter as tk
sys.path.insert(0, sys.argv[1])
os.chdir(sys.argv[2])
os.environ["MSR_SMOKE_TEST"] = "1"
raw, png = sys.argv[3], sys.argv[4]
out = {}
real_exit = os._exit
os._exit = lambda code=0: None   # the smoke-test exit timer must not end the run early
tempfile.gettempdir = lambda: sys.argv[2]   # previews are cached under the temp folder

slow = [False]
orig_after = tk.Misc.after
def fast_after(self, ms, func=None, *args):   # 4 s ticks become 1 s, and later (slow) never come
    if func is None:
        return orig_after(self, ms)
    if ms == 4000 and slow[0]:
        ms = 600000
    elif isinstance(ms, int) and ms >= 1000:
        ms //= 4
    return orig_after(self, ms, func, *args)
tk.Misc.after = fast_after

def write_png(path, w=160, h=90):
    rows = b"".join(b"\x00" + b"\x80\x40\x20" * w for _ in range(h))
    def chunk(tag, data):
        c = struct.pack(">I", len(data)) + tag + data
        return c + struct.pack(">I", zlib.crc32(tag + data) & 0xffffffff)
    with open(path, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
                + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b""))
write_png(png)

from msr import preview_state as ps
clock = [1000.0]
Orig = ps.PreviewState
ps.PreviewState = lambda *a, **k: Orig(clock=lambda: clock[0])

import msr.gui as gui
calls = []
mode = {"fail": False}
def fake_grab(raw_file, output_file, ffmpeg_path, logger, width=160, height=90, **kw):
    calls.append(time.time())
    if mode["fail"]:
        return False, "no frame"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    shutil.copyfile(png, output_file)
    if kw.get("report") is not None and kw.get("attempts"):
        kw["report"]["attempt"] = kw["attempts"][0]
    return True, "ok"
gui.create_preview_frame = fake_grab

class FakeRecorder:
    is_running = True
    def __init__(self, enabled, config, **kw):
        self.status_dict = {c: {"status": "Recording", "raw_file": raw} for c in enabled}
        out["recorder"] = self
    def run(self):
        pass
    def __getattr__(self, name):
        return lambda *a, **k: None
gui.StreamRecorder = FakeRecorder
gui._dialogs.ask_yes_no = lambda *a, **k: True
gui._dialogs.show_warning = lambda *a, **k: None
gui._dialogs.show_info = lambda *a, **k: None

def driver(self):
    # Runs on its own thread while the real main loop runs: tkinter hands each call to the main thread,
    # and the grab thread's root.after(0, ...) needs a running main loop, as in the app.
    from msr import themes
    from tkinter import ttk
    def walk(w):
        yield w
        for c in w.winfo_children():
            yield from walk(c)
    def wait_for(cond, timeout=15.0):
        end = time.time() + timeout
        while time.time() < end:
            value = cond()
            if value:
                return value
            time.sleep(0.01)
        return None
    def tile_label():
        for w in walk(self):
            if w.winfo_class() == "Label" and str(w.cget("wraplength")) == "160" and w.cget("text").startswith("alice"):
                return w
    def big_label():
        for w in walk(self):
            if (w.winfo_class() == "Label" and str(w.cget("wraplength")) == "0"
                    and "bold" in str(w.cget("font")) and w.cget("text").startswith("alice")):
                return w
    def tile_text():
        t = tile_label()
        return t.cget("text") if t else None
    try:
        t = themes.current()
        out["tokens"] = {k: t[k] for k in ("warn", "offline_fg", "accent", "border", "fg")}
        self.attributes("-alpha", 0.0)
        self.deiconify(); time.sleep(0.3)
        out["viewable"] = bool(self.winfo_viewable())
        nb = next(w for w in walk(self) if w.winfo_class() == "TNotebook"
                  and any("Status" in str(w.tab(i, "text")) for i in w.tabs()))
        status_tab = next(i for i in nb.tabs() if "Status" in str(nb.tab(i, "text")))
        other_tab = next(i for i in nb.tabs() if i != status_tab)
        nb.select(status_tab)
        start = next(w for w in walk(self) if w.winfo_class() == "Button" and "Start Recording" in str(w.cget("text")))
        start.invoke()
        # 1. the timer: a tile appears and keeps being refreshed
        wait_for(tile_label)
        out["tile_text"] = tile_text()
        n = len(calls)
        time.sleep(3.3)   # ticks are 1 s apart here
        out["periodic_grabs"] = len(calls) - n
        slow[0] = True    # from now on only a "kick" (tab shown, window restored) starts a tick
        time.sleep(1.2)
        # 2. Status tab hidden: no grabs; shown again: a grab at once
        out["tab_hidden_grabs"], out["tab_resume_delays"] = [], []
        for _ in range(3):
            nb.select(other_tab); time.sleep(0.4)
            n = len(calls)
            nb.select(other_tab); time.sleep(0.2)
            out["tab_hidden_grabs"].append(len(calls) - n)
            t0 = time.time()
            nb.select(status_tab)
            ok = wait_for(lambda: len(calls) > n, 3.0)
            out["tab_resume_delays"].append(round(time.time() - t0, 3) if ok else None)
            time.sleep(0.3)
        # 3. window hidden (minimised/tray): no grabs, even if a tab changes; restored: a grab at once
        out["win_hidden_grabs"], out["win_resume_delays"] = [], []
        for _ in range(2):
            self.withdraw(); time.sleep(0.3)
            n = len(calls)
            nb.select(other_tab); time.sleep(0.2); nb.select(status_tab); time.sleep(0.4)
            out["win_hidden_grabs"].append(len(calls) - n)
            t0 = time.time()
            self.deiconify()
            ok = wait_for(lambda: len(calls) > n, 3.0)
            out["win_resume_delays"].append(round(time.time() - t0, 3) if ok else None)
            time.sleep(0.3)
        def tick_now():
            nb.select(other_tab); time.sleep(0.1); nb.select(status_tab); time.sleep(0.5)
        # 4. pin it (double-click)
        tile = tile_label()
        for _ in range(2):   # Tk turns two quick presses into a double-click
            tile.event_generate("<ButtonPress-1>", x=5, y=5)
            tile.event_generate("<ButtonRelease-1>", x=5, y=5)
        big = wait_for(big_label, 3.0)
        out["pinned_text"] = big.cget("text") if big else None
        # 5. grabs fail and time passes: the age and the warning colour
        mode["fail"] = True
        clock[0] += 30
        tick_now()
        tile, big = tile_label(), big_label()
        out["stale_text"] = tile.cget("text")
        out["stale_border"] = tile.master.cget("highlightbackground")
        out["stale_name_fg"] = str(tile.cget("fg"))
        out["big_stale_text"] = big.cget("text") if big else None
        out["big_stale_border"] = big.master.master.cget("highlightbackground") if big else None
        # 6. a good grab clears it
        mode["fail"] = False
        clock[0] += 5
        tick_now()
        tile = tile_label()
        out["recovered_text"] = tile.cget("text") if tile else None
        out["recovered_border"] = tile.master.cget("highlightbackground") if tile else None
        # 7. the stream ends: the last frame stays, marked "ended"
        out["recorder"].status_dict["alice"]["status"] = "Idle"
        tick_now()
        tile = tile_label()
        out["ended_text"] = tile.cget("text") if tile else None
        out["ended_border"] = tile.master.cget("highlightbackground") if tile else None
        out["has_image_when_ended"] = bool(tile and str(tile.master.winfo_children()[0].cget("image")))
        # 8. ... and goes after the keep time
        clock[0] += 100
        tick_now()
        out["gone"] = tile_label() is None
        # 9. recording again brings a live tile back
        out["recorder"].status_dict["alice"]["status"] = "Recording"
        tick_now()
        out["back_text"] = tile_text()
    except BaseException:
        import traceback; traceback.print_exc(file=sys.stderr)
    out.pop("recorder", None)
    sys.stdout.write(json.dumps(out)); sys.stdout.flush(); real_exit(0)

orig_mainloop = tk.Tk.mainloop
def mainloop_with_driver(self, n=0):
    threading.Thread(target=driver, args=(self,), daemon=True).start()
    return orig_mainloop(self, n)
tk.Tk.mainloop = mainloop_with_driver
from msr.config import Config
from msr.gui import main_gui
main_gui(Config(os.path.abspath("config.ini")))
'''


class PreviewStripGuiTests(unittest.TestCase):
    def run_script(self):
        try:
            import tkinter
            probe = tkinter.Tk()
            probe.destroy()
        except Exception as exc:  # no display (headless CI)
            self.skipTest(f"no display: {exc}")
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            (data / "rec").mkdir()
            raw = data / "alice.ts"
            raw.write_bytes(b"\0" * (128 * 1024))
            (data / "config.ini").write_text(
                f"[Paths]\nstreams_dir = {data / 'rec'}\n[GUI]\nminimize_to_tray = false\nnotifications = false\n",
                encoding="utf-8")
            (data / "channels.json").write_text(json.dumps([{"name": "alice", "enabled": True}]), encoding="utf-8")
            done = subprocess.run(
                [sys.executable, "-c", SCRIPT, str(ROOT), str(data), str(raw), str(data / "frame.png")],
                capture_output=True, text=True, timeout=120, cwd=str(ROOT))
        self.assertEqual(done.returncode, 0, done.stderr[-3000:])
        self.assertNotIn("Traceback", done.stderr)
        return json.loads(done.stdout)

    def test_strip_ages_ends_and_pauses_while_hidden(self):
        out = self.run_script()
        tok = out["tokens"]
        self.assertTrue(out["viewable"], out)
        self.assertEqual(out["tile_text"], "alice", out)
        # the timer keeps refreshing (1 s ticks here), then only a kick can start a tick
        self.assertGreaterEqual(out["periodic_grabs"], 2, out)
        # Status tab hidden: no grabs; shown again: a grab at once
        self.assertEqual(out["tab_hidden_grabs"], [0, 0, 0], out)
        self.assertTrue(all(d is not None and d < 1.0 for d in out["tab_resume_delays"]), out)
        # window hidden (minimised or in the tray): no grabs even when a tab changes; restored: a grab at once
        self.assertEqual(out["win_hidden_grabs"], [0, 0], out)
        self.assertTrue(all(d is not None and d < 1.0 for d in out["win_resume_delays"]), out)
        # pinned pane follows
        self.assertEqual(out["pinned_text"], "alice", out)
        # stale: age in the tile name and the pane title, warning colour on both borders
        self.assertEqual(out["stale_text"], "alice \u00b7 30 s", out)
        self.assertEqual(out["stale_border"], tok["warn"], out)
        self.assertEqual(out["stale_name_fg"], tok["warn"], out)
        self.assertEqual(out["big_stale_text"], "alice \u00b7 30 s", out)
        self.assertEqual(out["big_stale_border"], tok["warn"], out)
        # a good grab clears it
        self.assertEqual(out["recovered_text"], "alice", out)
        self.assertNotEqual(out["recovered_border"], tok["warn"], out)
        # ended: last frame stays, marked, muted; later the tile goes; recording again revives it
        self.assertEqual(out["ended_text"], "alice \u00b7 ended", out)
        self.assertEqual(out["ended_border"], tok["offline_fg"], out)
        self.assertTrue(out["has_image_when_ended"], out)
        self.assertTrue(out["gone"], out)
        self.assertEqual(out["back_text"], "alice", out)


if __name__ == "__main__":
    unittest.main()
