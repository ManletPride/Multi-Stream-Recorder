"""MSR's own virtual environment (.venv inside the MSR folder). Stdlib only: the launchers import
this from whatever Python opened them, before any third-party package exists.

First launch: create .venv with the launching Python, pip-install requirements.txt into it (hidden,
with a small progress window), then start MSR with .venv's pythonw. Later launches only stat a few
files and read a marker. Updates from the Updates dialog / msr_ctl.py deps then land in .venv
only, so nothing else on the machine that uses the same Python is affected.

If anything fails (offline, no venv module, no write access) MSR runs on the launching Python,
says why, and tries again next launch. A broken .venv (missing interpreter, half-finished
install, base Python uninstalled, MSR folder moved) is rebuilt.

Opt out: MSR_NO_VENV=1, or [Advanced] use_venv = false in config.ini.
"""

from __future__ import annotations

import configparser
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time

from msr.nowindow import no_window_kwargs

VENV_NAME = ".venv"
MARKER = "msr_venv.json"
ERROR_FILE = "msr_venv_error.txt"
OPT_OUT_ENV = "MSR_NO_VENV"
NOTICE_ENV = "MSR_VENV_NOTICE"
PIP_TIMEOUT = 1800
LAUNCH_NOTICE = ""   # set by the entry point from NOTICE_ENV; the GUI shows it once


def launch_data_dir(argv: list[str], environ=None) -> tuple[str, list[str]]:
    """The data folder run_msr.pyw uses and the --data-dir args it adds: a --data-dir on the
    command line (shortcuts cannot set env vars) wins, else MSR_DATA (passed on), else MSR_HOME."""
    environ = os.environ if environ is None else environ
    for i, a in enumerate(argv):
        if a == "--data-dir" and i + 1 < len(argv):
            return argv[i + 1].strip().strip('"'), []
        if a.startswith("--data-dir="):
            return a.split("=", 1)[1].strip().strip('"'), []
    data = (environ.get("MSR_DATA") or "").strip().strip('"')
    if data:
        return data, ["--data-dir", data]
    return (environ.get("MSR_HOME") or "").strip().strip('"'), []


def is_windows() -> bool:
    return os.name == "nt"


def in_venv() -> bool:
    """True when this interpreter is a virtual environment (any venv, not the system Python)."""
    return sys.prefix != getattr(sys, "base_prefix", sys.prefix)


def venv_dir(root: str) -> str:
    return os.path.join(root, VENV_NAME)


def venv_python(root: str, gui: bool = False) -> str:
    d = venv_dir(root)
    if is_windows():
        return os.path.join(d, "Scripts", "pythonw.exe" if gui else "python.exe")
    return os.path.join(d, "bin", "python")


def running_in(root: str) -> bool:
    return in_venv() and _same(sys.prefix, venv_dir(root))


def _same(a: str, b: str) -> bool:
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def base_python() -> str:
    """A console python of the launching interpreter (not pythonw, not a venv shim)."""
    exe = getattr(sys, "_base_executable", None) or sys.executable
    if is_windows() and os.path.basename(exe).lower() == "pythonw.exe":
        cand = os.path.join(os.path.dirname(exe), "python.exe")
        if os.path.isfile(cand):
            exe = cand
    return exe


def requirements_hash(root: str) -> str:
    try:
        with open(os.path.join(root, "requirements.txt"), "rb") as fh:
            return hashlib.sha256(fh.read()).hexdigest()[:16]
    except OSError:
        return ""


def opted_out(data_dir: str | None = None, root: str | None = None) -> str:
    """Why the venv is not used ("" = use it)."""
    if (os.environ.get(OPT_OUT_ENV) or "").strip().lower() in ("1", "true", "yes", "on"):
        return f"{OPT_OUT_ENV} is set"
    for folder in (data_dir, root):
        if not folder:
            continue
        path = os.path.join(folder, "config.ini")
        if os.path.isfile(path):
            cp = configparser.RawConfigParser(strict=False)
            try:
                cp.read(path, encoding="utf-8")
            except (configparser.Error, OSError, UnicodeDecodeError):
                return ""
            if cp.get("Advanced", "use_venv", fallback="true").strip().lower() in ("0", "false", "no", "off"):
                return "[Advanced] use_venv = false in config.ini"
            return ""
    return ""


def _pyvenv_home(root: str) -> str | None:
    try:
        with open(os.path.join(venv_dir(root), "pyvenv.cfg"), encoding="utf-8") as fh:
            for line in fh:
                k, _, v = line.partition("=")
                if k.strip().lower() == "home":
                    return v.strip()
    except OSError:
        return None
    return None


def read_marker(root: str) -> dict:
    try:
        with open(os.path.join(venv_dir(root), MARKER), encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def check(root: str) -> tuple[str, str]:
    """(state, reason). state: ready | missing | rebuild | install (requirements changed)."""
    d = venv_dir(root)
    if not os.path.isdir(d):
        return "missing", "no .venv yet"
    if not os.path.isfile(venv_python(root)) or not os.path.isfile(venv_python(root, gui=True)):
        return "rebuild", ".venv has no Python (damaged)"
    home = _pyvenv_home(root)
    if not home or not os.path.isdir(home):
        return "rebuild", "the Python that .venv was made from is gone (Python upgraded or removed)"
    m = read_marker(root)
    if not m.get("complete"):
        return "rebuild", "the last setup did not finish"
    if m.get("root") and not _same(m["root"], root):
        return "rebuild", "the MSR folder was moved"
    if m.get("requirements") != requirements_hash(root):
        return "install", "requirements.txt changed"
    return "ready", ""


def _looks_like_venv(d: str) -> bool:
    return os.path.isfile(os.path.join(d, "pyvenv.cfg")) or os.path.isfile(os.path.join(d, MARKER))


def _run(cmd, timeout, run=subprocess.run):
    # errors="replace": pip / venv output in another code page must not crash the setup
    return run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
               stdin=subprocess.DEVNULL, **no_window_kwargs())


def _explain(text: str, step: str) -> str:
    low = (text or "").lower()
    if "no module named venv" in low or "ensurepip" in low:
        return ("This Python cannot make virtual environments (the venv/ensurepip module is missing; "
                "on Debian/Ubuntu: sudo apt install python3-venv).")
    if any(s in low for s in ("permission denied", "access is denied", "winerror 5", "winerror 32")):
        return "No permission to write the .venv folder inside the MSR folder."
    if any(s in low for s in ("could not fetch", "newconnectionerror", "getaddrinfo failed", "read timed out",
                              "failed to establish a new connection", "no matching distribution",
                              "temporary failure in name resolution")):
        return "Could not download packages (offline, or PyPI not reachable)."
    lines = [ln for ln in (text or "").strip().splitlines() if ln.strip()]
    return f"{step} failed: " + (lines[-1][:300] if lines else "unknown error")


def ensure(root: str, progress=None, run=subprocess.run, python: str | None = None) -> tuple[bool, str]:
    """Make .venv ready. (ok, message). progress(text) gets short status lines."""
    say = progress or (lambda text: None)
    state, reason = check(root)
    if state == "ready":
        return True, ""
    d = venv_dir(root)
    try:
        if state in ("rebuild",) and os.path.isdir(d):
            if running_in(root):
                return False, f".venv needs rebuilding ({reason}) but MSR was started from it"
            if not _looks_like_venv(d):
                return False, f"{d} is not a virtual environment; MSR leaves it alone"
            say(f"Rebuilding MSR's environment ({reason})\u2026")
            shutil.rmtree(d)
        if not os.path.isdir(d):
            say("Creating MSR's own Python environment\u2026")
            r = _run([python or base_python(), "-m", "venv", d], 300, run)
            if r.returncode != 0 or not os.path.isfile(venv_python(root)):
                return False, _explain((r.stderr or "") + (r.stdout or ""), "Creating .venv")
        say("Installing yt-dlp, streamlink and the other packages (first time only, about a minute)\u2026")
        r = _run([venv_python(root), "-m", "pip", "install", "--disable-pip-version-check", "--no-input",
                  "--progress-bar", "off", "--retries", "1", "--timeout", "20",
                  "-r", os.path.join(root, "requirements.txt")], PIP_TIMEOUT, run)
        if r.returncode != 0:
            return False, _explain((r.stderr or "") + (r.stdout or ""), "Installing packages")
        with open(os.path.join(d, MARKER), "w", encoding="utf-8") as fh:
            json.dump({"complete": True, "root": os.path.abspath(root), "python": python or base_python(),
                       "version": "%d.%d" % sys.version_info[:2], "requirements": requirements_hash(root),
                       "created": time.strftime("%Y-%m-%d %H:%M:%S")}, fh, indent=1)
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "Setting up .venv took too long and was stopped."
    except OSError as e:
        return False, _explain(str(e), "Setting up .venv")
    except Exception as e:  # the launcher must always get an answer and start MSR
        return False, f"Setting up .venv failed: {type(e).__name__}: {e}"


def write_error(root: str, message: str) -> None:
    try:
        path = os.path.join(root, ERROR_FILE)
        if message:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(time.strftime("%Y-%m-%d %H:%M:%S ") + message + "\n")
        elif os.path.isfile(path):
            os.remove(path)
    except OSError:
        pass


def notice_text(message: str) -> str:
    return (f"MSR could not set up its own Python environment (.venv): {message} "
            "It runs on your normal Python for now and tries again next launch. "
            "Package updates from the Updates dialog are off until .venv works.")


def take_launch_notice() -> str:
    """Read (and clear, so a relaunch does not repeat it) the launcher's notice."""
    global LAUNCH_NOTICE
    LAUNCH_NOTICE = (os.environ.pop(NOTICE_ENV, "") or "").strip()
    return LAUNCH_NOTICE


def describe() -> str:
    if in_venv():
        return f"Python {sys.version.split()[0]} in {sys.prefix}"
    return f"Python {sys.version.split()[0]} at {sys.executable} (not a venv: package updates off)"


# ------------------------------------------------------------------ launcher
def _progress_window():
    """A small themed window for first-time setup; (say, pump, close) or None without a display."""
    try:
        import tkinter as tk
        from tkinter import ttk
        from msr import themes
        theme = themes.current()
        root = tk.Tk()
        try:
            from msr import appicon
            appicon.set_app_id()
            appicon.apply(root)
        except Exception:
            pass
        root.title("Multi-Stream Recorder \u2014 first-time setup")
        root.configure(bg=theme["bg"])
        root.resizable(False, False)
        label = tk.Label(root, text="Preparing\u2026", bg=theme["bg"], fg=theme["fg"], wraplength=420,
                         justify="left", padx=16, pady=12)
        label.pack(fill="x")
        bar = ttk.Progressbar(root, mode="indeterminate", length=420)
        bar.pack(padx=16, pady=(0, 16))
        bar.start(12)
        root.update_idletasks()
        w, h = root.winfo_reqwidth(), root.winfo_reqheight()
        root.geometry(f"+{(root.winfo_screenwidth() - w) // 2}+{(root.winfo_screenheight() - h) // 3}")
        root.protocol("WM_DELETE_WINDOW", lambda: None)   # setup finishes on its own
        return root, label
    except Exception:
        return None


def prepare(root: str, data_dir: str | None = None, show_window: bool = True) -> tuple[str | None, str]:
    """For a launcher: (python to run MSR with, or None for the current one; notice text)."""
    why = opted_out(data_dir, root)
    if why:
        return None, ""
    if running_in(root):
        return None, ""
    state, _ = check(root)
    if state == "ready":
        return venv_python(root, gui=True), ""
    result = {}
    win = _progress_window() if show_window else None
    if win is None:
        ok, msg = ensure(root)
    else:
        import queue
        import threading
        tkroot, label = win
        q = queue.Queue()
        def work():
            try:
                result["r"] = ensure(root, progress=q.put)
            except BaseException as e:  # else the window waits forever (it cannot be closed)
                result["r"] = (False, f"{type(e).__name__}: {e}")
        threading.Thread(target=work, daemon=True).start()

        def pump():
            try:
                while True:
                    label.configure(text=q.get_nowait())
            except queue.Empty:
                pass
            if "r" in result:
                tkroot.destroy()
            else:
                tkroot.after(100, pump)
        tkroot.after(100, pump)
        tkroot.mainloop()
        ok, msg = result.get("r", (False, "setup window closed"))
    write_error(root, "" if ok else msg)
    if ok:
        return venv_python(root, gui=True), ""
    return None, notice_text(msg)
