r"""Double-click launcher: starts Multi-Stream Recorder with no console.

Data folder: %MSR_DATA% if set, else MSR_HOME, else this repo folder.
Uses .venv\Scripts\pythonw.exe next to this file if present, otherwise the
pythonw.exe of whichever Python opened this file.
"""
import os
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
venv_pyw = os.path.join(here, ".venv", "Scripts", "pythonw.exe")
pyw = venv_pyw if os.path.isfile(venv_pyw) else os.path.join(
    os.path.dirname(sys.executable), "pythonw.exe")
if not os.path.isfile(pyw):
    pyw = sys.executable
cmd = [pyw, os.path.join(here, "Multi-Stream-Recorder.py")]
data = (os.environ.get("MSR_DATA") or "").strip().strip('"')
if data:
    cmd += ["--data-dir", data]
subprocess.Popen(cmd, cwd=here, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
