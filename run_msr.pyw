r"""Double-click launcher: starts Multi-Stream Recorder with no console.
Code comes from this repo, settings and recordings from E:\streams."""
import os, subprocess, sys
here = os.path.dirname(os.path.abspath(__file__))
pyw = os.path.join(os.path.dirname(sys.executable), "pythonw.exe")
subprocess.Popen([pyw, os.path.join(here, "Multi-Stream-Recorder.py"), "--data-dir", r"E:\streams"],
                 cwd=here, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
