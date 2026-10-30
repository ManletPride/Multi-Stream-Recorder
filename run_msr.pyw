r"""Double-click launcher: starts Multi-Stream Recorder with no console.

Data folder: --data-dir if given, else %MSR_DATA%, else MSR_HOME, else this repo folder.
MSR runs in its own environment, .venv next to this file: the first launch creates it and
installs requirements.txt there (a small progress window shows this), later launches use it at
once. If that is not possible MSR runs on the Python that opened this file and says why.
Opt out: MSR_NO_VENV=1 or [Advanced] use_venv = false (see README).
Extra arguments are passed on to Multi-Stream-Recorder.py.
"""
import os
import subprocess
import sys

here = os.path.dirname(os.path.abspath(__file__))
data = ""
try:
    sys.path.insert(0, here)
    from msr import venv as _venv
    data, data_args = _venv.launch_data_dir(sys.argv[1:])
    pyw, notice = _venv.prepare(here, data or None)
    if not pyw:
        pyw = os.path.join(os.path.dirname(sys.executable), "pythonw.exe" if os.name == "nt" else "python3")
        if not os.path.isfile(pyw):
            pyw = sys.executable
    cmd = [pyw, os.path.join(here, "Multi-Stream-Recorder.py")]
    cmd += data_args + sys.argv[1:]
    env = dict(os.environ)
    env.pop(_venv.NOTICE_ENV, None)
    if notice:
        env[_venv.NOTICE_ENV] = notice
    subprocess.Popen(cmd, cwd=here, env=env, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
except Exception as exc:
    # pythonw has no console: without this a failed launch leaves no trace at all.
    where = data if data and os.path.isdir(data) else here
    try:
        from msr.startup_error import report_crash
        report_crash(exc, directory=where)
    except Exception:
        import traceback
        with open(os.path.join(where, "msr_startup_error.log"), "a", encoding="utf-8") as f:
            f.write(traceback.format_exc() + "\n")
