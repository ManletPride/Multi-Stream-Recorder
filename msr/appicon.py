"""The app icon (assets/icon.*): taskbar identity, window icons for every Toplevel, the tray
image, and desktop shortcuts. Nothing here runs in headless mode."""

from __future__ import annotations

import os
import subprocess
import sys

from msr.nowindow import no_window_kwargs

APP_ID = "ManletPride.MultiStreamRecorder"
APP_NAME = "Multi-Stream Recorder"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS = os.path.join(ROOT, "assets")
ICO = os.path.join(ASSETS, "icon.ico")
PHOTO_SIZES = (16, 24, 32, 48, 64, 128, 256)


def png(size: int) -> str:
    return os.path.join(ASSETS, f"icon_{size}.png")


def set_app_id(app_id: str = APP_ID) -> bool:
    """Windows: give this process its own taskbar identity (own group and icon, not python's).
    Call before the first window. False elsewhere or on failure."""
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        return ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(str(app_id)) == 0
    except Exception:
        return False


def apply(root) -> bool:
    """Window icons: iconbitmap(default=.ico) on Windows and iconphoto(True, PNGs) everywhere, so
    every Toplevel opened later (Clip Editor, dialogs) gets it too."""
    ok = False
    if sys.platform == "win32" and os.path.isfile(ICO):
        try:
            root.iconbitmap(default=ICO)
            ok = True
        except Exception:
            pass
    try:
        import tkinter as tk
        photos = [tk.PhotoImage(master=root, file=png(n)) for n in PHOTO_SIZES if os.path.isfile(png(n))]
        if photos:
            root.iconphoto(True, *photos)
            root._msr_icon_photos = photos   # keep them alive
            ok = True
    except Exception:
        pass
    return ok


def tray_image(recording: bool = False, size: int = 64):
    """The app icon for the tray; idle shows the record dot grey. None without Pillow/the file."""
    try:
        from PIL import Image, ImageDraw
        img = Image.open(png(64)).convert("RGBA")
    except Exception:
        return None
    if not recording:
        d = ImageDraw.Draw(img)
        cx, cy, r = 81 * 64 / 256, 104 * 64 / 256, 3.6   # the dot in the 64 px icon
        d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(150, 156, 170, 255))
    return img.resize((size, size)) if size != 64 else img


# ------------------------------------------------------------------ shortcuts
def pythonw() -> str:
    """The base Python's pythonw (not .venv's): run_msr.pyw then picks or rebuilds .venv itself,
    so the shortcut keeps working when .venv is recreated."""
    from msr.venv import base_python
    base = base_python()
    cand = os.path.join(os.path.dirname(base), "pythonw.exe")
    return cand if os.path.isfile(cand) else base


def _ps_quote(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def shortcut_script(lnk: str, target: str, args: str, workdir: str, icon: str) -> str:
    """PowerShell that writes a .lnk (WScript.Shell, part of Windows)."""
    return ("$s = (New-Object -ComObject WScript.Shell).CreateShortcut(" + _ps_quote(lnk) + "); "
            f"$s.TargetPath = {_ps_quote(target)}; $s.Arguments = {_ps_quote(args)}; "
            f"$s.WorkingDirectory = {_ps_quote(workdir)}; $s.IconLocation = {_ps_quote(icon + ',0')}; "
            f"$s.Description = {_ps_quote(APP_NAME)}; $s.Save()")


def desktop_dir() -> str:
    if sys.platform == "win32":
        try:
            r = subprocess.run(["powershell", "-NoProfile", "-Command", "[Environment]::GetFolderPath('Desktop')"],
                               capture_output=True, text=True, timeout=20, **no_window_kwargs())
            path = (r.stdout or "").strip()
            if path and os.path.isdir(path):
                return path
        except (OSError, subprocess.SubprocessError):
            pass
    return os.path.join(os.path.expanduser("~"), "Desktop")


def create_windows_shortcut(lnk: str | None = None, data_dir: str | None = None, run=subprocess.run) -> str:
    """A new shortcut: pythonw + run_msr.pyw with the app icon. Never overwrites an existing one
    (adds ' (2)'). Returns its path."""
    lnk = lnk or os.path.join(desktop_dir(), f"{APP_NAME}.lnk")
    stem, ext = os.path.splitext(lnk)
    n = 1
    while os.path.exists(lnk):
        n += 1
        lnk = f"{stem} ({n}){ext}"
    args = f'"{os.path.join(ROOT, "run_msr.pyw")}"'
    script = shortcut_script(lnk, pythonw(), args, ROOT, ICO)
    if data_dir:   # run_msr.pyw reads MSR_DATA; a shortcut cannot set env, so go direct
        args = f'"{os.path.join(ROOT, "Multi-Stream-Recorder.py")}" --data-dir "{data_dir}"'
        script = shortcut_script(lnk, pythonw(), args, ROOT, ICO)
    r = run(["powershell", "-NoProfile", "-NonInteractive", "-Command", script], capture_output=True, text=True,
            timeout=30, **no_window_kwargs())
    if r.returncode != 0:
        raise OSError((r.stderr or "PowerShell failed").strip()[:300])
    return lnk


def set_icon_script(lnk: str) -> str:
    """PowerShell that points an EXISTING shortcut's icon at assets/icon.ico (for the README)."""
    return ("$s = (New-Object -ComObject WScript.Shell).CreateShortcut(" + _ps_quote(lnk) + "); "
            f"$s.IconLocation = {_ps_quote(ICO + ',0')}; $s.Save()")


def desktop_entry(python: str = "python3", data_dir: str | None = None) -> str:
    """A freedesktop .desktop file for Linux menus/docks."""
    cmd = f'{python} "{os.path.join(ROOT, "run_msr.pyw")}"'
    if data_dir:
        cmd += f' --data-dir "{data_dir}"'
    return ("[Desktop Entry]\nType=Application\nName=Multi-Stream Recorder\n"
            "Comment=Record live streams from Kick, Twitch, YouTube and more\n"
            f"Exec={cmd}\nPath={ROOT}\nIcon={png(256)}\nTerminal=false\nCategories=AudioVideo;Video;Recorder;\n"
            "StartupWMClass=Multi-Stream Recorder\n")
