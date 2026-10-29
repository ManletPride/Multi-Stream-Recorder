r"""Double-click launcher: starts Multi-Stream Recorder with no console.

Data folder: %MSR_DATA% if set, else MSR_HOME, else this repo folder.
Uses .venv\Scripts\pythonw.exe next to this file if present, otherwise the
pythonw.exe of whichever Python opened this file.
"""
import os
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
try:
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
except Exception as exc:
    # pythonw has no console: without this a failed launch leaves no trace at all.
    data_dir = (os.environ.get("MSR_DATA") or "").strip().strip('"')
    where = data_dir if data_dir and os.path.isdir(data_dir) else here
    try:
        sys.path.insert(0, here)
        from msr.startup_error import report_crash
        report_crash(exc, directory=where)
    except Exception:
        import traceback
        with open(os.path.join(where, "msr_startup_error.log"), "a", encoding="utf-8") as f:
            f.write(traceback.format_exc() + "\n")
