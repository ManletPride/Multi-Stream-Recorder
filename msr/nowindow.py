"""Hide console windows for every child process on Windows.

Under pythonw there is no console, so each ffmpeg/ffprobe/streamlink/yt-dlp call would open (and
close) its own console window. Two layers, both from this stdlib-only module:

* ``no_window_kwargs()``: the explicit helper. Every launch in ``msr/`` passes ``**no_window_kwargs()``
  (``tests/test_no_console.py`` fails when a new one does not).
* Importing this module also makes CREATE_NO_WINDOW the default for any ``subprocess.Popen`` that
  was missed, including ones that pass other creationflags (a priority class used to switch the
  default off, which is how the background remux opened a window per file).
"""
import subprocess
import sys

CREATE_NO_WINDOW = 0x08000000
DETACHED_PROCESS = 0x00000008
CREATE_NEW_CONSOLE = 0x00000010


def is_windows():
    return sys.platform == "win32"


def no_window_flags(creationflags=0):
    """``creationflags`` plus CREATE_NO_WINDOW, unless the caller asked for its own console.

    DETACHED_PROCESS has no console to hide, and CREATE_NEW_CONSOLE is a visible console on
    purpose: both are left exactly as given.
    """
    creationflags = int(creationflags or 0)
    if creationflags & (DETACHED_PROCESS | CREATE_NEW_CONSOLE):
        return creationflags
    return creationflags | CREATE_NO_WINDOW


def no_window_kwargs(creationflags=0):
    """Popen/run keyword arguments for a child that must not show a console window.

    Windows: ``{"creationflags": ...}``. Elsewhere: ``{}`` (``creationflags`` is rejected there).
    Use as ``subprocess.run(cmd, capture_output=True, **no_window_kwargs())``; pass extra flags
    such as a priority class as ``no_window_kwargs(BELOW_NORMAL_PRIORITY_CLASS)``.
    """
    if not is_windows():
        return {}
    return {"creationflags": no_window_flags(creationflags)}


def install(subprocess_module=subprocess, windows=None):
    """Make CREATE_NO_WINDOW part of every Popen of ``subprocess_module`` (once). Returns True if patched."""
    if windows is None:
        windows = is_windows()
    popen = subprocess_module.Popen
    if not windows or getattr(popen, "_msr_no_window", False):
        return False

    class _NoWindowPopen(popen):
        _msr_no_window = True

        def __init__(self, *args, **kwargs):
            kwargs["creationflags"] = no_window_flags(kwargs.get("creationflags", 0))
            super().__init__(*args, **kwargs)

    subprocess_module.Popen = _NoWindowPopen
    return True


install()
