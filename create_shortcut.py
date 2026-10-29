r"""Create a desktop shortcut for Multi-Stream Recorder with its own icon.

Windows:  python create_shortcut.py [--data-dir D:\msr-data] [--path C:\...\Name.lnk]
          (a NEW shortcut: pythonw + run_msr.pyw, icon assets\icon.ico; existing ones are left alone)
          python create_shortcut.py --set-icon "C:\Users\me\Desktop\Old shortcut.lnk"
          (only changes that shortcut's icon)
Linux:    python3 create_shortcut.py --linux > ~/.local/share/applications/multi-stream-recorder.desktop
"""
import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from msr import appicon  # noqa: E402
from msr.nowindow import no_window_kwargs  # noqa: E402


def main(argv=None):
    p = argparse.ArgumentParser(description="Desktop shortcut with the Multi-Stream Recorder icon")
    p.add_argument("--data-dir", help="data folder the shortcut starts MSR with")
    p.add_argument("--path", help="where to write the .lnk (default: Desktop\\Multi-Stream Recorder.lnk)")
    p.add_argument("--set-icon", metavar="LNK", help="only set the icon of an existing shortcut")
    p.add_argument("--linux", action="store_true", help="print a .desktop file")
    a = p.parse_args(argv)
    if a.linux or sys.platform != "win32":
        print(appicon.desktop_entry(sys.executable, a.data_dir), end="")
        return 0
    if a.set_icon:
        r = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command",
                            appicon.set_icon_script(os.path.abspath(a.set_icon))],
                           capture_output=True, text=True, **no_window_kwargs())
        print("Icon set." if r.returncode == 0 else (r.stderr or "failed").strip())
        return r.returncode
    print("Created", appicon.create_windows_shortcut(a.path, a.data_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
