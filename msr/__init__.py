"""Multi-Stream Recorder internals.

The user-facing entry point remains ``Multi-Stream-Recorder.py`` in the
project root.  Worker processes import ``msr.worker.record_worker`` so
Windows ``spawn`` does not re-execute the GUI.

Conventions and clip/GUI gotchas for future sessions: see ``AGENTS.md``.
"""

__version__ = "2.0.1"

