"""Make a failed start visible when there is no console (``pythonw`` / ``run_msr.pyw``).

A start that dies under pythonw used to leave nothing: no window, no traceback. Now the
problem is appended to ``msr_startup_error.log`` (current folder, i.e. the data folder once it
is active; git-ignored) and, in the GUI, shown in a themed dialog.

Standard library only at import time: this must work when ``msr.gui`` itself fails to import.
No secrets: the traceback has source lines but no variable values, and the message of an
exception that may quote a config value (``configparser``, decode errors) is withheld.
"""
import configparser
import datetime
import os
import platform
import sys
import traceback

LOG_NAME = "msr_startup_error.log"
MAX_LOG_BYTES = 64 * 1024
_QUOTES_INPUT = (configparser.Error, UnicodeError)


def error_log_path(directory=None):
    return os.path.join(os.path.abspath(directory or os.getcwd()), LOG_NAME)


def _redact(text):
    try:
        from msr.util import redact_for_log
        return redact_for_log(text)
    except Exception:
        return text


def safe_exception_text(exc, limit=5):
    """Traceback text for ``exc`` (and its causes) that is safe to keep on disk."""
    parts = []
    seen = set()
    while exc is not None and id(exc) not in seen and len(parts) < limit:
        seen.add(id(exc))
        frames = _redact("".join(traceback.format_tb(exc.__traceback__)))
        if isinstance(exc, _QUOTES_INPUT):
            message = "(details withheld: the message can quote a value from config.ini)"
        else:
            message = _redact(str(exc))
        parts.append(f"{frames}{type(exc).__name__}: {message}\n")
        exc = exc.__cause__ or exc.__context__
    return "\n--- caused by ---\n".join(parts)


def short_message(exc):
    """One line for a dialog: the exception type and, where it is safe, its message."""
    if isinstance(exc, _QUOTES_INPUT):
        return f"{type(exc).__name__} (see the log file for where)"
    text = _redact(str(exc)).strip().splitlines()[0] if str(exc).strip() else ""
    return f"{type(exc).__name__}: {text[:300]}" if text else type(exc).__name__


def write_error_log(text, directory=None):
    """Append ``text`` with a header to msr_startup_error.log; returns the path or None."""
    path = error_log_path(directory)
    try:
        from msr import __version__ as version
    except Exception:
        version = "?"
    header = (f"=== {datetime.datetime.now():%Y-%m-%d %H:%M:%S} | MSR {version} | "
              f"Python {platform.python_version()} | {sys.platform} ===\n")
    try:
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                old = f.read()
        except OSError:
            old = ""
        body = old + header + text.rstrip() + "\n\n"
        if len(body) > MAX_LOG_BYTES:
            body = body[-MAX_LOG_BYTES // 2:]
            body = body[body.find("=== "):] if "=== " in body else body
        with open(path, "w", encoding="utf-8") as f:
            f.write(body)
        return path
    except OSError:
        return None


NO_DIALOGS_ENV = "MSR_NO_DIALOGS"


def dialogs_allowed() -> bool:
    """False under tests/CI (MSR_NO_DIALOGS=1, MSR_SMOKE_TEST) and without a display: a modal box
    nobody can see would block the start forever. The message still goes to stderr and the log."""
    if os.environ.get(NO_DIALOGS_ENV, "").strip() in ("1", "true", "yes") or os.environ.get("MSR_SMOKE_TEST"):
        return False
    if sys.platform.startswith("linux") and not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
        return False
    return True


def show_dialog(title, message):
    """Show ``message`` in the themed error dialog on a hidden root. False if Tk is unavailable
    or dialogs are off (see dialogs_allowed)."""
    if not dialogs_allowed():
        return False
    try:
        import tkinter as tk
        from msr import dialogs
        root = tk.Tk()
        try:
            root.withdraw()
            dialogs.show_error(root, title, message)
        finally:
            root.destroy()
        return True
    except Exception:
        return False


def _stderr(text):
    try:
        if sys.stderr:
            print(text, file=sys.stderr)
    except Exception:
        pass


def report_failure(title, message, *, headless=False, directory=None, show=None):
    """Log ``message``, print it to stderr, and (GUI only) show it. Returns the log path."""
    path = write_error_log(message, directory)
    _stderr(f"{title}\n{message}")
    if not headless:
        where = f"\n\nDetails: {path}" if path else ""
        (show or show_dialog)(title, message + where)
    return path


def report_crash(exc, *, headless=False, directory=None, show=None):
    """An uncaught exception ended the start. Log it and tell the user; never raises."""
    try:
        path = write_error_log(safe_exception_text(exc), directory)
        _stderr(f"Multi-Stream Recorder could not start: {short_message(exc)}")
        if not headless:
            where = f"\n\nDetails were saved to:\n{path}" if path else ""
            (show or show_dialog)("Multi-Stream Recorder - Cannot start",
                                  f"Multi-Stream Recorder could not start.\n\n{short_message(exc)}{where}")
        return path
    except Exception:
        return None
