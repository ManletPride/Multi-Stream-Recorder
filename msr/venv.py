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
import queue
import re
import threading
import shutil
import subprocess
import sys
import time

from msr.nowindow import no_window_kwargs

VENV_NAME = ".venv"
MARKER = "msr_venv.json"
ERROR_FILE = "msr_venv_error.txt"
SETUP_LOG = "msr_venv_setup.log"   # full output of the last setup (git-ignored: *.log)
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


def _process_images() -> list[tuple[int, str]]:
    """(pid, executable path) of the processes this user can see; [] if unknown."""
    out = []
    if is_windows():
        import ctypes
        from ctypes import wintypes

        class Entry(ctypes.Structure):
            _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD), ("th32ProcessID", wintypes.DWORD),
                        ("th32DefaultHeapID", ctypes.c_size_t), ("th32ModuleID", wintypes.DWORD),
                        ("cntThreads", wintypes.DWORD), ("th32ParentProcessID", wintypes.DWORD),
                        ("pcPriClassBase", ctypes.c_long), ("dwFlags", wintypes.DWORD),
                        ("szExeFile", wintypes.WCHAR * 260)]
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        k32.OpenProcess.restype = wintypes.HANDLE
        snap = k32.CreateToolhelp32Snapshot(0x2, 0)   # TH32CS_SNAPPROCESS
        if not snap or snap == wintypes.HANDLE(-1).value:
            return []
        try:
            e = Entry()
            e.dwSize = ctypes.sizeof(Entry)
            ok = k32.Process32FirstW(snap, ctypes.byref(e))
            while ok:
                h = k32.OpenProcess(0x1000, False, e.th32ProcessID)   # PROCESS_QUERY_LIMITED_INFORMATION
                if h:
                    buf, size = ctypes.create_unicode_buffer(1024), wintypes.DWORD(1024)
                    if k32.QueryFullProcessImageNameW(h, 0, buf, ctypes.byref(size)):
                        out.append((int(e.th32ProcessID), buf.value))
                    k32.CloseHandle(h)
                ok = k32.Process32NextW(snap, ctypes.byref(e))
        finally:
            k32.CloseHandle(snap)
        return out
    for pid in os.listdir("/proc") if os.path.isdir("/proc") else []:
        if pid.isdigit():
            try:
                out.append((int(pid), os.readlink(f"/proc/{pid}/exe")))
            except OSError:
                pass
    return out


def in_use(root: str, images=None) -> list[int]:
    """PIDs of other processes running from this .venv (another MSR, msr_ctl): its packages
    must not be changed under them."""
    prefix = os.path.normcase(os.path.abspath(venv_dir(root))) + os.sep
    try:
        found = images() if images is not None else _process_images()
    except Exception:
        return []
    return [pid for pid, exe in found
            if pid != os.getpid() and os.path.normcase(os.path.abspath(exe)).startswith(prefix)]


def _looks_like_venv(d: str) -> bool:
    return os.path.isfile(os.path.join(d, "pyvenv.cfg")) or os.path.isfile(os.path.join(d, MARKER))


def _run(cmd, timeout, run=subprocess.run):
    # errors="replace": pip / venv output in another code page must not crash the setup
    return run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
               stdin=subprocess.DEVNULL, **no_window_kwargs())


def setup_log_path(root: str) -> str:
    return os.path.join(root, SETUP_LOG)


def count_requirements(path: str) -> int:
    """Packages named in requirements.txt (comments, blanks and options skipped)."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return sum(1 for ln in fh if ln.split("#", 1)[0].strip() and not ln.lstrip().startswith("-"))
    except OSError:
        return 0


_NAME = r"([A-Za-z0-9][A-Za-z0-9._\-]*)"
_COLLECT = re.compile(r"^Collecting " + _NAME)
_REQ_OK = re.compile(r"^Requirement already satisfied: " + _NAME)
_DOWNLOAD = re.compile(r"^\s*Downloading (?:https?://\S*/)?([^\s/]+?)(?:-(\d[^-\s]*))?(?:-[^\s]*)?\.(?:whl|tar\.gz|zip)(?:\.metadata)?(?:\s*\(([^)]+)\))?")
_CACHED = re.compile(r"^\s*Using cached (?:https?://\S*/)?([^\s/]+?)(?:-(\d[^-\s]*))?(?:-[^\s]*)?\.(?:whl|tar\.gz|zip)")
_BUILDING = re.compile(r"^\s*Building wheel for " + _NAME)
_INSTALLING = re.compile(r"^Installing collected packages: (.+)$")
_DONE = re.compile(r"^Successfully installed (.+)$")


class PipProgress:
    """Turns pip's output lines into one friendly status line. Tk-free (tested on its own)."""

    def __init__(self, top_level: int = 0):
        self.top_level = top_level   # packages in requirements.txt
        self.found = 0               # packages pip has looked up so far (dependencies included)
        self.total = 0               # known once pip starts installing

    def feed(self, line: str) -> str | None:
        """New status text for this line, or None (status unchanged)."""
        text = (line or "").rstrip()
        m = _COLLECT.match(text) or _REQ_OK.match(text)
        if m:
            self.found += 1
            if self.top_level and self.found <= self.top_level:
                return f"Looking up package {self.found} of {self.top_level}: {m.group(1)}"
            return f"Looking up {m.group(1)} (dependency, {self.found} packages so far)"
        m = _DOWNLOAD.match(text)
        if m:
            name, ver, size = m.group(1).replace("_", "-"), m.group(2), m.group(3)
            return f"Downloading {name}" + (f" {ver}" if ver else "") + (f" ({size})" if size else "")
        m = _CACHED.match(text)
        if m:
            return f"Using the cached download of {m.group(1).replace('_', '-')}"
        m = _BUILDING.match(text)
        if m:
            return f"Building {m.group(1)} (can take a moment)"
        m = _INSTALLING.match(text)
        if m:
            names = [n.strip() for n in m.group(1).split(",") if n.strip()]
            self.total = len(names)
            return f"Installing {len(names)} packages (the longest step): " + ", ".join(names[:6]) + (", \u2026" if len(names) > 6 else "")
        m = _DONE.match(text)
        if m:
            return f"Installed {len(m.group(1).split())} packages"
        if text.startswith("ERROR:"):
            return text[:200]
        return None


def _stream(cmd, timeout, on_line, popen=subprocess.Popen):
    """Run cmd hidden with stdout+stderr merged; on_line(line) for each line as it arrives.
    Returns a CompletedProcess (stdout = everything). Kills the process after `timeout` s."""
    proc = popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, text=True,
                 encoding="utf-8", errors="replace", bufsize=1, **no_window_kwargs())
    timed_out = []

    def kill():
        timed_out.append(True)
        try:
            proc.kill()
        except OSError:
            pass
    timer = threading.Timer(timeout, kill)
    timer.daemon = True
    timer.start()
    lines = []
    try:
        for line in proc.stdout:
            line = line.rstrip("\r\n")
            lines.append(line)
            on_line(line)
        rc = proc.wait()
    finally:
        timer.cancel()
        try:
            proc.stdout.close()
        except (OSError, AttributeError):
            pass
    if timed_out:
        raise subprocess.TimeoutExpired(cmd, timeout)
    return subprocess.CompletedProcess(cmd, rc, "\n".join(lines) + "\n", "")


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


def ensure(root: str, progress=None, run=subprocess.run, python: str | None = None, log=None,
           popen=None) -> tuple[bool, str]:
    """Make .venv ready. (ok, message). progress(text) gets short status lines; log(line), when
    given, gets every output line live (pip is then streamed; popen is injectable for tests).
    The full output also goes to msr_venv_setup.log in the MSR folder."""
    say = progress or (lambda text: None)
    logfile = None

    def out(line: str) -> None:
        if logfile is not None:
            try:
                logfile.write(line + "\n")
                logfile.flush()
            except OSError:
                pass
        if log is not None:
            log(line)
    state, reason = check(root)
    if state == "ready":
        return True, ""
    d = venv_dir(root)
    try:
        try:
            logfile = open(setup_log_path(root), "w", encoding="utf-8")
            logfile.write(time.strftime("MSR first-time setup %Y-%m-%d %H:%M:%S\n"))
        except OSError:
            logfile = None
        if state in ("rebuild",) and os.path.isdir(d):
            if running_in(root):
                return False, f".venv needs rebuilding ({reason}) but MSR was started from it"
            if not _looks_like_venv(d):
                return False, f"{d} is not a virtual environment; MSR leaves it alone"
            say(f"Rebuilding MSR's environment ({reason})\u2026")
            shutil.rmtree(d)
        if not os.path.isdir(d):
            say("Creating MSR's own Python environment (.venv)\u2026")
            out(f"> python -m venv {d}")
            r = _run([python or base_python(), "-m", "venv", d], 300, run)
            for line in ((r.stdout or "") + (r.stderr or "")).splitlines():
                out(line)
            if r.returncode != 0 or not os.path.isfile(venv_python(root)):
                return False, _explain((r.stderr or "") + (r.stdout or ""), "Creating .venv")
        say("Installing yt-dlp, streamlink and the other packages (first time only, about a minute)\u2026")
        reqs = os.path.join(root, "requirements.txt")
        cmd = [venv_python(root), "-m", "pip", "install", "--disable-pip-version-check", "--no-input",
               "--progress-bar", "off", "--retries", "1", "--timeout", "20", "-r", reqs]
        out("> pip install -r requirements.txt")
        if log is not None and (popen is not None or run is subprocess.run):
            tracker = PipProgress(count_requirements(reqs))

            def line_seen(line: str) -> None:
                out(line)
                status = tracker.feed(line)
                if status:
                    say(status)
            r = _stream(cmd, PIP_TIMEOUT, line_seen, popen or subprocess.Popen)
        else:
            r = _run(cmd, PIP_TIMEOUT, run)
            for line in ((r.stdout or "") + (r.stderr or "")).splitlines():
                out(line)
        if r.returncode != 0:
            return False, _explain((r.stderr or "") + (r.stdout or ""), "Installing packages")
        with open(os.path.join(d, MARKER), "w", encoding="utf-8") as fh:
            json.dump({"complete": True, "root": os.path.abspath(root), "python": python or base_python(),
                       "version": "%d.%d" % sys.version_info[:2], "requirements": requirements_hash(root),
                       "created": time.strftime("%Y-%m-%d %H:%M:%S")}, fh, indent=1)
        say("Done. Starting Multi-Stream Recorder\u2026")
        out("Setup finished.")
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "Setting up .venv took too long and was stopped."
    except OSError as e:
        return False, _explain(str(e), "Setting up .venv")
    except Exception as e:  # the launcher must always get an answer and start MSR
        return False, f"Setting up .venv failed: {type(e).__name__}: {e}"
    finally:
        if logfile is not None:
            try:
                logfile.close()
            except OSError:
                pass


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
LOG_LINES_KEPT = 500   # the window's log keeps the last N lines (the file has all)


def drain(q, on_status, on_line, limit: int = 200) -> int:
    """Hand queued ("status", text) / ("line", text) items to the window, at most `limit` per
    tick so a burst of pip output cannot freeze it. Returns how many were handled."""
    n = 0
    while n < limit:
        try:
            kind, text = q.get_nowait()
        except queue.Empty:
            break
        (on_status if kind == "status" else on_line)(text)
        n += 1
    return n


def fmt_elapsed(seconds: float) -> str:
    s = max(0, int(seconds))
    return f"{s // 60}:{s % 60:02d}"


class SetupWindow:
    """First-time setup window: status line, elapsed time, a small live log, and on failure a
    message naming the full log file plus a Continue button."""

    def __init__(self, tk, ttk, theme, show: bool = True):
        self.tk = tk
        self.started = time.time()
        root = self.root = tk.Tk()
        root.withdraw()   # built hidden: the caption is themed before it is ever shown (no white flash)
        try:  # MSR's ttk styles (clam): themed scrollbar, progress bar and button
            from msr import themeui
            themeui.apply_ttk(root, ttk.Style(root), theme)
        except Exception:
            pass
        try:
            from msr import appicon
            appicon.set_app_id()
            appicon.apply(root)
        except Exception:
            pass
        root.title("Multi-Stream Recorder \u2014 first-time setup")
        root.configure(bg=theme["bg"])
        root.resizable(True, True)
        bg, fg = theme["bg"], theme["fg"]
        self.status = tk.Label(root, text="Preparing\u2026", bg=bg, fg=fg, wraplength=520, justify="left",
                               anchor="w", padx=12, pady=8)
        self.status.pack(fill="x")
        self.detail = tk.Label(root, text="", bg=bg, fg=theme.get("muted", fg), anchor="w", padx=12)
        self.detail.pack(fill="x")
        self.bar = ttk.Progressbar(root, mode="indeterminate", length=520)
        self.bar.pack(fill="x", padx=12, pady=(4, 6))
        self.bar.start(12)
        self.footer = tk.Frame(root, bg=bg)   # holds Continue on failure (packed before the log grows)
        self.footer.pack(side="bottom", fill="x")
        box = tk.Frame(root, bg=bg)
        box.pack(fill="both", expand=True, padx=12, pady=(0, 10))
        self.log = tk.Text(box, height=6, width=80, wrap="none", bg=theme["log_bg"], fg=theme["log_fg"],
                           insertbackground=theme["log_fg"], relief="flat", borderwidth=0,
                           font=("Consolas", 9), state="disabled")
        scroll = ttk.Scrollbar(box, orient="vertical", command=self.log.yview)
        self.log.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")   # first, so a narrow window never hides it
        self.log.pack(side="left", fill="both", expand=True)
        self.button = ttk.Button(self.footer, text="Continue", command=root.destroy)
        root.update_idletasks()
        w, h = root.winfo_reqwidth(), root.winfo_reqheight()
        root.minsize(min(w, 420), max(160, h - self.log.winfo_reqheight() + 3 * 15))   # ~3 log lines
        root.geometry(f"+{(root.winfo_screenwidth() - w) // 2}+{(root.winfo_screenheight() - h) // 3}")
        root.protocol("WM_DELETE_WINDOW", lambda: None)   # setup finishes on its own
        self.themed = False
        try:  # same caption as every MSR window (dark mode + theme caption colours, re-applied on <Map>)
            from msr import dialogs
            dialogs.apply_title_bar(root, getattr(theme, "dark", True))
            self.themed = True
        except Exception:
            pass
        if show:
            root.deiconify()
        self.lines = 0

    def set_status(self, text: str) -> None:
        self.status.configure(text=text)

    def add_line(self, text: str) -> None:
        at_end = self.log.yview()[1] >= 0.999   # follow new lines only while at the bottom
        self.log.configure(state="normal")
        self.log.insert("end", text + "\n")
        self.lines += 1
        if self.lines > LOG_LINES_KEPT:
            self.log.delete("1.0", "2.0")
            self.lines -= 1
        self.log.configure(state="disabled")
        if at_end:
            self.log.see("end")

    def tick(self) -> None:
        self.detail.configure(text=f"Elapsed {fmt_elapsed(time.time() - self.started)}  \u00b7  "
                                   "only on the first start; later starts are instant")

    def failed(self, message: str, log_path: str) -> None:
        """Stop the bar, keep the log, say what failed and where the full log is; wait for Continue."""
        self.bar.stop()
        self.bar.pack_forget()
        self.set_status(f"Setup did not finish: {message}\n\nFull log: {log_path}\n"
                        "Multi-Stream Recorder will start on your normal Python and try again next time.")
        self.button.pack(side="right", padx=12, pady=(0, 10))
        self.root.protocol("WM_DELETE_WINDOW", self.root.destroy)
        self.button.focus_set()
        self.root.bind("<Return>", lambda _e: self.root.destroy())


def _progress_window(show: bool = True):
    """The themed setup window, or None without a display."""
    try:
        import tkinter as tk
        from tkinter import ttk
        from msr import themes
        return SetupWindow(tk, ttk, themes.current(), show=show)
    except Exception:
        return None


def run_with_window(win, root: str, ensure_fn=None) -> tuple[bool, str]:
    """ensure() on a worker thread; its status and log lines reach the window through a queue
    drained by after() (the worker never touches Tk)."""
    ensure_fn = ensure_fn or ensure
    q: queue.Queue = queue.Queue()
    result = {}

    def work():
        try:
            result["r"] = ensure_fn(root, progress=lambda t: q.put(("status", t)), log=lambda t: q.put(("line", t)))
        except BaseException as e:  # else the window waits forever (it cannot be closed)
            result["r"] = (False, f"{type(e).__name__}: {e}")
    threading.Thread(target=work, daemon=True).start()

    def pump():
        drain(q, win.set_status, win.add_line)
        win.tick()
        if "r" in result and q.empty():
            ok, msg = result["r"]
            if ok:
                win.root.destroy()
            else:
                win.failed(msg, setup_log_path(root))   # stays until Continue
            return
        win.root.after(100, pump)
    win.root.after(100, pump)
    win.root.mainloop()
    return result.get("r", (False, "setup window closed"))


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
    busy = in_use(root) if state in ("install", "rebuild") else []
    if busy and state == "install":   # keep the old packages until no other copy uses them
        return venv_python(root, gui=True), ""
    if busy:
        return None, notice_text(f".venv needs rebuilding but another Multi-Stream Recorder (PID {busy[0]}) "
                                 "is running from it; close it and start MSR again.")
    result = {}
    win = _progress_window() if show_window else None
    if win is None:
        ok, msg = ensure(root)
    else:
        ok, msg = run_with_window(win, root)
    write_error(root, "" if ok else msg)
    if ok:
        return venv_python(root, gui=True), ""
    return None, notice_text(msg)
