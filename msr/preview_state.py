"""Tk-free bookkeeping for the live preview strip.

The window (``msr/gui.py``) and the grab (``msr/worker.py: create_preview_frame``) stay thin; what
needs to be remembered between ticks lives here so it can be tested without a display or ffmpeg:

* when each channel last produced a fresh frame (``last_ok``, ``time.monotonic()``) and what to show
  for it: nothing, an age such as "12 s", "no frame", or "ended";
* which seek in the ``LADDER`` worked for each channel, so a stream whose key frames are far apart
  (a 4 s gap: the old fixed ``-sseof -3`` found nothing every tick) does not pay for the failing
  seek again, with an occasional retry of a shorter one in case the key-frame spacing changed;
* a back-off when every seek fails, so a stream with no usable video is not hammered every tick.

Methods may be called from the grab thread and the GUI thread; one lock guards the table.
"""
import threading
import time
from typing import NamedTuple

STALE_AFTER = 10.0          # seconds without a fresh frame before the age is shown
ENDED_KEEP = 90.0           # seconds the last frame of a channel that stopped recording stays on screen
RETRY_SHORTER_EVERY = 10    # successful grabs on a longer seek before one shorter seek is retried
BACKOFF_FIRST = 4.0         # seconds to wait after the first failed grab; doubles per failure
BACKOFF_MAX = 60.0
DUE_GRACE = 1.0             # a 4 s GUI tick lands a little before a 4 s back-off ends; do not skip it


def build_ladder(offset=3.0):
    """Seeks to try, shortest first: (mode, seconds, fast).

    ``sseof`` seeks that far before the end of the growing file; ``ss`` seeks to (duration - seconds)
    after an ffprobe. *fast* decodes key frames only (``-skip_frame nokey``): much cheaper, but the
    picture can be up to one key-frame interval older. The last rung is the old plain decode, a safety
    net for a stream whose key frames are not flagged.
    """
    o = float(offset)
    return (
        ("sseof", o, True),
        ("sseof", o * 2, True),
        ("ss", o, True),
        ("sseof", o * 2, False),
    )


LADDER = build_ladder(3.0)


def backoff_seconds(failures):
    """4, 8, 16, 32, 60, 60, ... seconds after that many failed grabs in a row (0 for none)."""
    if failures <= 0:
        return 0.0
    return min(BACKOFF_MAX, BACKOFF_FIRST * (2 ** (failures - 1)))


def format_age(seconds):
    """'12 s', '3 min', '2 h'."""
    s = max(0, int(seconds))
    if s < 60:
        return f"{s} s"
    if s < 3600:
        return f"{s // 60} min"
    return f"{s // 3600} h"


class PreviewStatus(NamedTuple):
    kind: str    # "fresh", "stale" or "ended"
    age: float   # seconds since the last fresh frame (since the tile appeared if there never was one)
    text: str    # "" when fresh, else "12 s" / "no frame" / "ended"


class TileLook(NamedTuple):
    suffix: str      # appended to the channel name, "" or " \u00b7 12 s"
    border: str
    thickness: int
    name_fg: str


def tile_look(status, tokens, selected=False):
    """Colours and label for one tile. *tokens* is a theme dict; only theme tokens are used."""
    border = tokens["accent"] if selected else tokens["border"]
    thickness = 2 if selected else 1
    name_fg = tokens["fg"]
    suffix = ""
    if status.kind == "stale":
        border, thickness, name_fg = tokens["warn"], 2, tokens["warn"]
        suffix = f" \u00b7 {status.text}"
    elif status.kind == "ended":
        border, name_fg = tokens["offline_fg"], tokens["offline_fg"]
        suffix = " \u00b7 ended"
    return TileLook(suffix, border, thickness, name_fg)


def pane_border(status, tokens):
    """Border of the large pane: the accent, or the warning colour once its frame is stale."""
    return tokens["warn"] if status.kind == "stale" else tokens["accent"]


class _Channel:
    __slots__ = ("first_seen", "last_ok", "rung", "ok_since_probe", "failures",
                 "retry_at", "ended_at", "source")

    def __init__(self, now):
        self.first_seen = now
        self.last_ok = None
        self.rung = 0
        self.ok_since_probe = 0
        self.failures = 0
        self.retry_at = 0.0
        self.ended_at = None
        self.source = None


class PreviewState:
    def __init__(self, clock=time.monotonic, ladder=LADDER):
        self.clock = clock
        self.ladder = tuple(ladder)
        self._lock = threading.Lock()
        self._channels = {}

    def _get(self, ch):
        c = self._channels.get(ch)
        if c is None:
            c = self._channels[ch] = _Channel(self.clock())
        return c

    # -- lifecycle -------------------------------------------------------------------------
    def mark_active(self, ch):
        """The channel is recording (again): a tile that was marked ended is live again."""
        with self._lock:
            self._get(ch).ended_at = None

    def mark_ended(self, ch):
        """The channel stopped recording. Only the first call counts."""
        with self._lock:
            c = self._channels.get(ch)
            if c is not None and c.ended_at is None:
                c.ended_at = self.clock()

    def keep_ended(self, ch):
        """True while an ended channel's last frame should stay on screen (it must have had one)."""
        with self._lock:
            c = self._channels.get(ch)
            if c is None or c.last_ok is None or c.ended_at is None:
                return False
            return self.clock() - c.ended_at < ENDED_KEEP

    def forget(self, ch):
        with self._lock:
            self._channels.pop(ch, None)

    def note_source(self, ch, raw_file):
        """A new recording file means new key-frame spacing: start the seek ladder and back-off over."""
        with self._lock:
            c = self._get(ch)
            if c.source is not None and c.source != raw_file:
                c.rung, c.ok_since_probe, c.failures, c.retry_at = 0, 0, 0, 0.0
            c.source = raw_file

    # -- grabbing --------------------------------------------------------------------------
    def due(self, ch):
        """False while the channel is backing off after failed grabs."""
        with self._lock:
            c = self._channels.get(ch)
            return c is None or self.clock() >= c.retry_at - DUE_GRACE

    def plan(self, ch, allow_retry=True):
        """Seeks to try this time, in order: the remembered one, then the longer ones.

        Every RETRY_SHORTER_EVERY successful grabs on a rung above the first, the next shorter rung is
        tried first once (it moves the memory down if it works). Pass ``allow_retry=False`` for a
        second grab of the same tick (the large pane) so it does not use up the probe.
        """
        with self._lock:
            c = self._get(ch)
            order = list(range(c.rung, len(self.ladder)))
            if allow_retry and c.rung > 0 and c.ok_since_probe >= RETRY_SHORTER_EVERY:
                c.ok_since_probe = 0
                order.insert(0, c.rung - 1)
            return [self.ladder[k] for k in order]

    def record_success(self, ch, rung=None):
        """A frame was written by *rung* (a ladder entry): remember it and reset the back-off."""
        with self._lock:
            c = self._get(ch)
            if rung in self.ladder:
                c.rung = self.ladder.index(rung)
            c.ok_since_probe = c.ok_since_probe + 1 if c.rung > 0 else 0
            c.last_ok = self.clock()
            c.failures = 0
            c.retry_at = 0.0

    def record_failure(self, ch, started=None):
        """Every rung failed. Returns the seconds to wait before this channel is grabbed again."""
        with self._lock:
            c = self._get(ch)
            c.failures += 1
            delay = backoff_seconds(c.failures)
            c.retry_at = (self.clock() if started is None else started) + delay
            return delay

    # -- display ---------------------------------------------------------------------------
    def status(self, ch):
        with self._lock:
            c = self._channels.get(ch)
            if c is None:
                return PreviewStatus("fresh", 0.0, "")
            now = self.clock()
            ref = c.last_ok if c.last_ok is not None else c.first_seen
            age = max(0.0, now - ref)
            if c.ended_at is not None:
                return PreviewStatus("ended", age, "ended")
            if age > STALE_AFTER:
                return PreviewStatus("stale", age, format_age(age) if c.last_ok is not None else "no frame")
            return PreviewStatus("fresh", age, "")
