"""Optional sound effects ([Sounds] in config.ini, all off by default).

Windows system sounds via winsound (async, nothing bundled); a no-op elsewhere.
Muted while the session is locked when quiet_while_locked is on. Rate-limited
per event; a problem key (channel + detail) sounds at most once per 5 min.
"""

import sys
import threading
import time

EVENTS = {
    # event: (config key, Windows sound alias)
    "live": ("go_live", "SystemAsterisk"),
    "problem": ("problems", "SystemExclamation"),
    "clip": ("clip_saved", "SystemDefault"),
}
MIN_GAP = 4.0          # seconds between two sounds of the same event (bursts coalesce)
PROBLEM_REPEAT = 300.0  # same problem again only after 5 min


def _winsound_player(alias):
    import winsound
    winsound.PlaySound(alias, winsound.SND_ALIAS | winsound.SND_ASYNC | winsound.SND_NODEFAULT)


def session_locked():
    """True while the Windows lock screen is up. Any error: False (fail-safe)."""
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        user32 = ctypes.windll.user32
        user32.OpenInputDesktop.restype = ctypes.c_void_p
        h = user32.OpenInputDesktop(0, False, 0x0100)  # DESKTOP_SWITCHDESKTOP
        if not h:
            return True  # the input desktop is the secure (lock) desktop
        user32.CloseDesktop(ctypes.c_void_p(h))
        return False
    except Exception:
        return False


class Sounds:
    def __init__(self, config, player=None, locked=session_locked, clock=time.monotonic,
                 platform=sys.platform):
        self.config = config
        self.player = player or (_winsound_player if platform == "win32" else None)
        self.locked = locked
        self.clock = clock
        self._lock = threading.Lock()
        self._last = {}
        self._problems = {}

    def enabled(self, event):
        key = EVENTS[event][0]
        try:
            return bool(self.config.getboolean("Sounds", key, False))
        except Exception:
            return False

    def play(self, event, key=""):
        """Play *event* if enabled, not rate-limited and not locked. Returns True if played."""
        if event not in EVENTS or self.player is None or not self.enabled(event):
            return False
        now = self.clock()
        with self._lock:
            if event == "problem" and key:
                last = self._problems.get(key)
                if last is not None and now - last < PROBLEM_REPEAT:
                    return False
                self._problems[key] = now
            last = self._last.get(event)
            if last is not None and now - last < MIN_GAP:
                return False
            self._last[event] = now
        try:
            if self.config.getboolean("Sounds", "quiet_while_locked", True) and self.locked():
                return False
        except Exception:
            pass
        return self._play(event)

    def test(self, event):
        """The Options tab's Test button: ignores the checkbox, lock and rate limit."""
        if event not in EVENTS or self.player is None:
            return False
        return self._play(event)

    def _play(self, event):
        try:
            self.player(EVENTS[event][1])
            return True
        except Exception:
            return False
