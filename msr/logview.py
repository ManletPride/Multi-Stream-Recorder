"""Log panel behaviour: follow new lines only while the reader is at the bottom.

Auto-scrolling on every line makes it impossible to scroll back (the view is yanked
down twice a second). ``LogFollower`` checks whether the view was at the bottom
*before* a batch is added: if so it stays pinned, otherwise it leaves the view alone and
counts the lines the reader has not seen so the GUI can offer a "New lines" button.
Pure logic around a Text-like widget, so it is unit tested with a fake.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Callable

MAX_LINES = 5000  # trim the oldest lines above this ...
KEEP_LINES = 4000  # ... down to this


# --- levels and filtering --------------------------------------------------------------
LEVEL_ALL, LEVEL_WARNING, LEVEL_ERROR = "all", "warning", "error"
LEVEL_CHOICES = (  # (key, label shown in the drop-down)
    (LEVEL_ALL, "All levels"),
    (LEVEL_WARNING, "Warnings and errors"),
    (LEVEL_ERROR, "Errors only"),
)
_LEVEL_RANK = {"INFO": 0, "WARNING": 1, "ERROR": 2}
_TAG_RE = re.compile(r"^\S+ \[(WARNING|ERROR)\] ")


class GuiFormatter(logging.Formatter):
    """The Log tab's format ("HH:MM:SS message"), with a [WARNING] / [ERROR] tag after the
    time for those levels only, so plain INFO lines look exactly as before."""

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        tag = "ERROR" if record.levelno >= logging.ERROR else "WARNING" if record.levelno >= logging.WARNING else ""
        if not tag:
            return text
        head, sep, rest = text.partition(" ")
        return f"{head} [{tag}] {rest}" if sep else f"[{tag}] {text}"


def line_level(line: str) -> str:
    """INFO / WARNING / ERROR of one formatted log message (no tag = INFO)."""
    match = _TAG_RE.match(line)
    return match.group(1) if match else "INFO"


def level_label(key: str) -> str:
    return dict(LEVEL_CHOICES).get(key, LEVEL_CHOICES[0][1])


def level_key(label: str) -> str:
    return {v: k for k, v in LEVEL_CHOICES}.get(label, LEVEL_ALL)


def matches(line: str, words: list[str], level: str = LEVEL_ALL) -> bool:
    """Every word must appear (case-insensitive) and the line must be at least `level`."""
    if level == LEVEL_WARNING and _LEVEL_RANK[line_level(line)] < 1:
        return False
    if level == LEVEL_ERROR and _LEVEL_RANK[line_level(line)] < 2:
        return False
    low = line.lower()
    return all(w in low for w in words)


def split_query(query: str) -> list[str]:
    return [w for w in (query or "").lower().split() if w]


def open_log_file(path: str, opener: Callable[[str], None],
                  fallback: Callable[[str], None] | None = None) -> tuple[bool, str]:
    """Open the log in the OS viewer. Returns (ok, message for the user when not ok).
    `fallback` is tried when the OS has no program for .log files."""
    if not path or not os.path.isfile(path):
        return False, "There is no log file yet. It is created when the app starts logging."
    try:
        opener(path)
        return True, ""
    except Exception as first:  # no association, no desktop, ...
        if fallback is not None:
            try:
                fallback(path)
                return True, ""
            except Exception:
                pass
        return False, f"Could not open the log file: {first}"


def at_bottom(first: float, last: float, lines: int) -> bool:
    """True when the last line is on screen. `first`/`last` are Text.yview() fractions,
    `lines` the number of lines in the widget; the slack is half a line (at most 1%)."""
    if last >= 1.0:
        return True
    slack = min(0.01, 0.5 / max(1, lines))
    return last >= 1.0 - slack


class LogFollower:
    """Appends batches of lines to a read-only Text and decides whether to follow them.

    enabled     -> the Auto-scroll checkbox (False: never follow, just count new lines)
    on_behind   -> called with the number of unseen lines (0 = hide the button)
    """

    def __init__(self, text, enabled: Callable[[], bool] = lambda: True,
                 on_behind: Callable[[int], None] | None = None,
                 on_stats: Callable[[int, int], None] | None = None):
        self.text = text
        self.enabled = enabled
        self.on_behind = on_behind
        self.on_stats = on_stats  # (messages shown, messages kept) while a filter is on
        self.behind = 0
        self.buffer: list[str] = []  # every message, so a filter can be changed or removed
        self.words: list[str] = []
        self.level = LEVEL_ALL
        # Real Text widgets get colours for warnings/errors and the search hits; the unit
        # tests' fake has no tags.
        self._tags = hasattr(text, "tag_configure") and hasattr(text, "search")

    @property
    def filtering(self) -> bool:
        return bool(self.words) or self.level != LEVEL_ALL

    def _visible(self, messages: list[str]) -> list[str]:
        if not self.filtering:
            return messages
        return [m for m in messages if matches(m, self.words, self.level)]

    def _insert(self, messages: list[str]) -> None:
        start = self.text.index("end-1c") if self._tags else None
        for message in messages:
            tag = {"WARNING": "lvl_warning", "ERROR": "lvl_error"}.get(line_level(message)) if self._tags else None
            if tag:
                self.text.insert("end", message + "\n", tag)
            else:
                self.text.insert("end", message + "\n")
        if self._tags and self.words:
            self._highlight(start)

    def _highlight(self, start) -> None:
        self.text.tag_remove("find", start, "end")
        for word in self.words:
            pos = start
            while True:
                pos = self.text.search(word, pos, stopindex="end", nocase=True)
                if not pos:
                    break
                stop = f"{pos}+{len(word)}c"
                self.text.tag_add("find", pos, stop)
                pos = stop

    def _report(self) -> None:
        if self.on_stats is not None:
            shown = sum(1 for m in self.buffer if matches(m, self.words, self.level)) if self.filtering else len(self.buffer)
            self.on_stats(shown, len(self.buffer))

    def set_filter(self, query: str = "", level: str = LEVEL_ALL) -> None:
        """Show only messages containing every word of `query` at or above `level`.
        Re-renders from the kept messages; stays at the bottom if you were there."""
        words = split_query(query)
        if words == self.words and level == self.level:
            return
        self.words, self.level = words, level
        pinned = self.is_at_bottom()
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self._insert(self._visible(self.buffer))
        self.text.configure(state="disabled")
        if pinned and self.enabled():
            self.text.see("end")
        self._set_behind(0)
        self._report()

    def _line_count(self) -> int:
        try:
            return int(str(self.text.index("end-1c")).split(".")[0])
        except (ValueError, IndexError):
            return 1

    def is_at_bottom(self) -> bool:
        first, last = self.text.yview()
        return at_bottom(first, last, self._line_count())

    def _set_behind(self, count: int) -> None:
        if count != self.behind or count == 0:
            self.behind = count
            if self.on_behind is not None:
                self.on_behind(count)

    def append(self, messages: list[str]) -> None:
        if not messages:
            return
        pinned = self.is_at_bottom()  # decided before the new lines move the view
        self.buffer.extend(messages)
        if len(self.buffer) > MAX_LINES:
            del self.buffer[:len(self.buffer) - KEEP_LINES]
        messages = self._visible(messages)
        if not messages:
            self._report()
            return
        self.text.configure(state="normal")
        self._insert(messages)
        lines = self._line_count()
        if lines > MAX_LINES:
            self.text.delete("1.0", f"{lines - KEEP_LINES}.0")
        self.text.configure(state="disabled")
        if pinned and self.enabled():
            self.text.see("end")
            self._set_behind(0)
        else:
            self._set_behind(self.behind + len(messages))
        self._report()

    def on_scroll(self, first, last) -> None:
        """Wire to the Text's yscrollcommand: reaching the bottom clears the counter."""
        if self.behind and at_bottom(float(first), float(last), self._line_count()):
            self._set_behind(0)

    def jump(self) -> None:
        """Scroll to the newest line (the New lines button, or ticking Auto-scroll)."""
        self.text.see("end")
        self._set_behind(0)

    def clear(self) -> None:
        self.buffer.clear()
        self._report()
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.configure(state="disabled")
        self._set_behind(0)