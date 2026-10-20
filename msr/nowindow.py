"""Hide console windows for every child process on Windows.

Under pythonw there is no console, so each ffmpeg/streamlink/yt-dlp call
would flash its own window. Importing this module makes CREATE_NO_WINDOW
the default for all subprocess calls that don't set creationflags.
"""
import subprocess
import sys

if sys.platform == "win32" and not getattr(subprocess.Popen, "_msr_no_window", False):
    _Popen = subprocess.Popen

    class _NoWindowPopen(_Popen):
        _msr_no_window = True

        def __init__(self, *args, **kwargs):
            if not kwargs.get("creationflags"):
                kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
            super().__init__(*args, **kwargs)

    subprocess.Popen = _NoWindowPopen
