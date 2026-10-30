"""Test package setup.

Tk objects left over by the GUI tests (in this one test process) must be freed on the main
thread: when Python's cyclic garbage collector happened to run on a background thread of a
later test (recorder, cleanup and control threads), Tcl aborted the whole run with
"Tcl_AsyncDelete: async handler deleted by the wrong thread". So automatic collection is off
while the tests run, and every test ends with a collection on the main thread instead.
"""
import gc
import os
import shutil
import threading
import unittest

os.environ.setdefault("MSR_NO_UPDATE_CHECK", "1")  # headless runs in tests never ask GitHub
os.environ.setdefault("MSR_NO_DIALOGS", "1")       # a startup error box would block a test run
HAS_FFMPEG = shutil.which("ffmpeg") is not None    # the app refuses to start without it


def new_tk_root(testcase):
    """A hidden Tk root, or skip the test when there is no display (Linux CI without Xvfb)."""
    import tkinter as tk
    try:
        root = tk.Tk()
    except tk.TclError as exc:
        testcase.skipTest(f"no display: {exc}")
    root.withdraw()
    return root

gc.disable()
_run = unittest.TestCase.run


def _run_then_collect(self, *args, **kwargs):
    try:
        return _run(self, *args, **kwargs)
    finally:
        if threading.current_thread() is threading.main_thread():
            gc.collect()


unittest.TestCase.run = _run_then_collect