r"""msr_tui.py - read-only terminal status viewer for Multi-Stream Recorder.

One screen: channel table, last log lines, and a header with version, disk free and whether
MSR is running. Works in any terminal, also over SSH. It only reads msr_status.json and asks a
running copy for log lines (read-only ``logs`` command); it never changes anything.

    python -m pip install -r requirements-tui.txt
    python msr_tui.py [--data-dir PATH] [--interval 2] [--theme "PowerShell Blue"]

Data folder: --data-dir, else %MSR_HOME%, else %MSR_DATA%, else the current folder.
Keys: r refresh, q quit. Built with Textual by Textualize (MIT).
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from msr import themes, tui_data  # noqa: E402

try:
    from rich.text import Text
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.widgets import DataTable, Footer, Static
except ImportError:  # optional dependency: the GUI never needs it
    App = None

PIP_HINT = ("The terminal viewer needs Textual (optional, not used by the GUI).\n"
            "Install it with:\n    python -m pip install -r requirements-tui.txt\n"
            "or:\n    python -m pip install textual")

COLUMNS = ("", "Channel", "Platform", "Status", "Duration", "Size", "Last live")


def build_css(theme) -> str:
    """Textual CSS from an MSR theme (colours come from msr/themes.py, none hard-coded)."""
    return f"""
    Screen {{ background: {theme.bg}; color: {theme.fg}; }}
    #header {{ height: auto; padding: 0 1; background: {theme.title_bg}; color: {theme.title_fg}; }}
    #table {{ height: 1fr; background: {theme.listbox_bg}; color: {theme.listbox_fg}; }}
    #table > .datatable--header {{ background: {theme.tree_heading_bg}; color: {theme.fg}; text-style: bold; }}
    #table > .datatable--cursor {{ background: {theme.select_bg}; color: {theme.select_fg}; }}
    #logtitle {{ height: 1; padding: 0 1; color: {theme.muted}; background: {theme.bg}; }}
    #log {{ height: {tui_data.LOG_LINES + 2}; padding: 0 1; background: {theme.log_bg}; color: {theme.log_fg};
            border: round {theme.border}; }}
    Footer {{ background: {theme.btn_bg}; color: {theme.btn_fg}; }}
    """


def header_text(snap, data_dir, theme) -> "Text":
    t = Text()
    t.append(f"Multi-Stream Recorder {snap.version}", style="bold")
    if snap.running:
        t.append("   ● running", style=f"bold {theme.ok}")
        t.append(f" ({snap.mode}, status {tui_data.format_age(snap.age)} old)")
    else:
        t.append("   ● not running", style=f"bold {theme.error}")
        t.append(f" ({snap.reason})")
    t.append(f"   recording {snap.recording}")
    t.append("   disk free ")
    t.append(snap.disk, style=f"bold {theme.warn}" if snap.disk_low else "")
    if snap.disk_low:
        t.append(" LOW", style=f"bold {theme.warn}")
    t.append(f"\ndata {data_dir}   control {'on' if snap.control else 'off'}")
    return t


def log_text(lines, theme) -> "Text":
    t = Text(no_wrap=True, overflow="ellipsis")
    if not lines:
        t.append("(no log lines: MSR not running or agent control off)", style=theme.muted)
    for i, line in enumerate(lines):
        low = line.lower()
        style = theme.log_error if (" error" in low or "critical" in low) else (
            theme.log_warn if "warning" in low else "")
        t.append(("\n" if i else "") + line, style=style)
    return t


if App is not None:
    class StatusApp(App):
        """Read-only viewer. Future control keys (c/s/l) would go through msr.control.call."""

        TITLE = "MSR status"
        BINDINGS = [Binding("r", "refresh", "Refresh"), Binding("q", "quit", "Quit")]

        def __init__(self, data_dir, interval=2.0, theme_name=None, fetch_logs=tui_data.fetch_logs):
            self.msr_theme = themes.find(theme_name) or themes.THEMES[themes.DEFAULT_THEME]
            self.CSS = build_css(self.msr_theme)
            super().__init__()
            self.data_dir = data_dir
            self.interval = max(0.5, float(interval))
            self._fetch_logs = fetch_logs
            self.snapshot = None
            self.log_lines = []

        def compose(self) -> ComposeResult:
            yield Static(id="header")
            yield DataTable(id="table", cursor_type="none", zebra_stripes=False)
            yield Static("Log", id="logtitle")
            yield Static(id="log")
            yield Footer()

        def on_mount(self):
            self.query_one(DataTable).add_columns(*COLUMNS)
            self.action_refresh()
            self.set_interval(self.interval, self.action_refresh)

        def action_refresh(self):
            self.show(tui_data.read_snapshot(self.data_dir, theme=self.msr_theme))
            if self.snapshot.control:
                # socket call off the UI thread; a hung copy cannot freeze the screen
                self.run_worker(self._load_logs, thread=True, exclusive=True, group="logs")
            else:
                self.show_logs([])

        def _load_logs(self):
            lines = self._fetch_logs(self.data_dir)
            self.call_from_thread(self.show_logs, lines)

        def show(self, snap):
            self.snapshot = snap
            self.query_one("#header", Static).update(header_text(snap, self.data_dir, self.msr_theme))
            table = self.query_one(DataTable)
            row = table.cursor_row
            table.clear()
            for ch in snap.channels:
                table.add_row(Text("●", style=ch.color), ch.name, ch.platform,
                              Text(ch.status, style=ch.color), ch.duration, ch.size, ch.last_live)
            if not snap.channels:
                table.add_row("", Text("(no channels)", style=self.msr_theme.muted), "", "", "", "", "")
            if 0 <= row < table.row_count:
                table.move_cursor(row=row)

        def show_logs(self, lines):
            self.log_lines = list(lines)
            self.query_one("#log", Static).update(log_text(self.log_lines, self.msr_theme))


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Read-only terminal status viewer for Multi-Stream Recorder.")
    p.add_argument("--data-dir", help="MSR data folder (default: MSR_HOME, MSR_DATA, current folder)")
    p.add_argument("--interval", type=float, default=2.0, help="refresh every N seconds (default 2)")
    p.add_argument("--theme", default=None, help=f'MSR theme name (default "{themes.DEFAULT_THEME}")')
    args = p.parse_args(argv)
    if App is None:
        print(PIP_HINT, file=sys.stderr)
        return 1
    StatusApp(tui_data.resolve_data_dir(args.data_dir), args.interval, args.theme).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
