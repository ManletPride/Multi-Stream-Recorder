"""Log panel behaviour: follow new lines only while the reader is at the bottom.

Auto-scrolling on every line makes it impossible to scroll back (the view is yanked
down twice a second). ``LogFollower`` checks whether the view was at the bottom
*before* a batch is added: if so it stays pinned, otherwise it leaves the view alone and
counts the lines the reader has not seen so the GUI can offer a "New lines" button.
Pure logic around a Text-like widget, so it is unit tested with a fake.
"""

from __future__ import annotations

from typing import Callable

MAX_LINES = 5000  # trim the oldest lines above this ...
KEEP_LINES = 4000  # ... down to this


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
                 on_behind: Callable[[int], None] | None = None):
        self.text = text
        self.enabled = enabled
        self.on_behind = on_behind
        self.behind = 0

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
        self.text.configure(state="normal")
        self.text.insert("end", "".join(m + "\n" for m in messages))
        lines = self._line_count()
        if lines > MAX_LINES:
            self.text.delete("1.0", f"{lines - KEEP_LINES}.0")
        self.text.configure(state="disabled")
        if pinned and self.enabled():
            self.text.see("end")
            self._set_behind(0)
        else:
            self._set_behind(self.behind + len(messages))

    def on_scroll(self, first, last) -> None:
        """Wire to the Text's yscrollcommand: reaching the bottom clears the counter."""
        if self.behind and at_bottom(float(first), float(last), self._line_count()):
            self._set_behind(0)

    def jump(self) -> None:
        """Scroll to the newest line (the New lines button, or ticking Auto-scroll)."""
        self.text.see("end")
        self._set_behind(0)

    def clear(self) -> None:
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.configure(state="disabled")
        self._set_behind(0)