r"""msr_ctl.py - control Multi-Stream Recorder from a script, an agent or a terminal.

Talks to the copy running for the data folder (GUI or --headless) over its local control
channel (127.0.0.1 + the token in msr_control.json). With no copy running, roster, settings,
history and library commands work on the files directly (atomic writes, same history copies
as the app); session commands then exit with code 3.

Data folder: --data-dir, else %MSR_HOME%, else %MSR_DATA%, else the current folder.

Exit codes: 0 ok, 1 refused/failed, 2 usage, 3 not running (or running but not answering),
4 wrong token, 5 needs confirmation (--yes / --confirm WORD), 6 timed out.

Examples:
  python msr_ctl.py status --json
  python msr_ctl.py add twitch:somename
  python msr_ctl.py disable "#3" kickname
  python msr_ctl.py start | stop | quit
  python msr_ctl.py record-start kickname      (one channel, mid-session)
  python msr_ctl.py config set Timeouts.poll_interval_minutes 1
  python msr_ctl.py library plan pending --channel somename --older-than-days 30
  python msr_ctl.py library run pending --channel somename --older-than-days 30 --yes
"""

import argparse
import json
import os
import socket
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import msr.nowindow  # noqa: F401,E402  (hide child console windows, as everywhere else)
from msr import control  # noqa: E402

EXIT_OK, EXIT_FAILED, EXIT_USAGE, EXIT_NOT_RUNNING, EXIT_AUTH, EXIT_CONFIRM, EXIT_TIMEOUT = range(7)
LIVE_ONLY = {"start", "stop", "quit", "record_start", "record_stop", "check", "clip", "screenshot",
             "update_install"}
CODE_EXIT = {"auth": EXIT_AUTH, "confirm_required": EXIT_CONFIRM, "not_running": EXIT_NOT_RUNNING}


def data_dir_from(args):
    for value in (args.data_dir, os.environ.get("MSR_HOME"), os.environ.get("MSR_DATA")):
        value = (value or "").strip().strip('"')
        if value:
            return os.path.abspath(os.path.expandvars(os.path.expanduser(value)))
    return os.getcwd()


def _filters(p):
    p.add_argument("--channel", help="channel folder name (platform folder via --platform)")
    p.add_argument("--platform")
    p.add_argument("--category", choices=["processed", "recorded", "pending", "clips"])
    p.add_argument("--kinds", help="comma list: recording,raw,partial,clip,screenshot,sidecar,media")
    p.add_argument("--text", default="")
    p.add_argument("--month", help="YYYY-MM")
    p.add_argument("--older-than-days", type=float)
    p.add_argument("--from", dest="date_from", help="YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", help="YYYY-MM-DD")
    p.add_argument("--keep-newest-files", type=int)
    p.add_argument("--keep-newest-bytes", type=int)
    p.add_argument("--path", action="append", dest="paths", help="exact file (repeatable)")


def _filter_args(a):
    out = {"channel": a.channel, "platform": a.platform, "category": a.category, "text": a.text,
           "month": a.month, "older_than_days": a.older_than_days, "from": a.date_from, "to": a.date_to,
           "keep_newest_files": a.keep_newest_files, "keep_newest_bytes": a.keep_newest_bytes,
           "paths": a.paths, "kinds": [k.strip() for k in a.kinds.split(",")] if a.kinds else None}
    return {k: v for k, v in out.items() if v not in (None, "", [])}


def build_parser():
    ap = argparse.ArgumentParser(prog="msr_ctl.py", description="Control Multi-Stream Recorder.",
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog=__doc__.split("Examples:", 1)[1].join(["examples:", ""]))
    ap.add_argument("--data-dir", help="folder with config.ini / channels.json / msr_control.json")
    ap.add_argument("--json", action="store_true", help="print the full JSON reply")
    ap.add_argument("--timeout", type=float, default=600.0, help="seconds to wait for a reply")
    sub = ap.add_subparsers(dest="command", required=True, metavar="command")

    for name, text in (("status", "live status (or the files, when nothing runs)"), ("channels", "the roster"),
                       ("ping", "is a copy running?"), ("help", "list the protocol commands"),
                       ("start", "Start Recording"), ("stop", "Stop Recording"), ("quit", "quit the app cleanly"),
                       ("reload-channels", "re-read channels.json")):
        sub.add_parser(name, help=text)

    p = sub.add_parser("add", help="add channel(s): name, platform:name or a URL")
    p.add_argument("names", nargs="+")
    p.add_argument("--platform")
    p.add_argument("--disabled", action="store_true", help="add unticked")
    for name, text in (("remove", "remove from the roster (recordings stay)"), ("enable", "tick"),
                       ("disable", "untick"), ("record-start", "start recording now (ticks it)"),
                       ("record-stop", "stop recording this channel")):
        sub.add_parser(name, help=text).add_argument("channels", nargs="+", help="#N, key or shown name")
    p = sub.add_parser("move", help="reorder")
    p.add_argument("channels", nargs="+")
    p.add_argument("--to", required=True, choices=["top", "bottom", "up", "down"])
    sub.add_parser("sort", help="sort the roster").add_argument("by", choices=["enabled", "platform", "name"])
    p = sub.add_parser("edit", help="rename / change platform")
    p.add_argument("channel")
    p.add_argument("--name", dest="new_name")
    p.add_argument("--platform")
    sub.add_parser("check", help="Check Now (all, or the named channels)").add_argument("channels", nargs="*")
    p = sub.add_parser("clip", help="Clip Now")
    p.add_argument("channel")
    p.add_argument("--seconds", type=int)
    sub.add_parser("screenshot", help="Screenshot Now").add_argument("channel")

    p = sub.add_parser("config", help="settings")
    csub = p.add_subparsers(dest="action", required=True)
    g = csub.add_parser("get")
    g.add_argument("name", nargs="?", help="Section or Section.key")
    s = csub.add_parser("set")
    s.add_argument("name", help="Section.key")
    s.add_argument("value", nargs="?")
    s.add_argument("--stdin", action="store_true", help="read the value from stdin (keeps it out of argv)")
    csub.add_parser("reload")

    p = sub.add_parser("logs", help="last log lines (redacted)")
    p.add_argument("-n", "--lines", type=int, default=100)
    p.add_argument("--filter", default="")

    p = sub.add_parser("update", help="check / install an update")
    usub = p.add_subparsers(dest="action", required=True)
    usub.add_parser("check")
    usub.add_parser("install").add_argument("--yes", action="store_true")

    p = sub.add_parser("library", help="sizes, files, clean-up")
    lsub = p.add_subparsers(dest="action", required=True)
    lsub.add_parser("summary")
    f = lsub.add_parser("files")
    _filters(f)
    f.add_argument("--limit", type=int, default=500)
    for name in ("plan", "run"):
        f = lsub.add_parser(name, help="dry run" if name == "plan" else "do it (needs --yes / --confirm)")
        f.add_argument("method", choices=["pending", "trash", "permanent", "restore", "rmdir"])
        _filters(f)
        f.add_argument("--no-sidecars", action="store_true")
        if name == "run":
            f.add_argument("--yes", action="store_true")
            f.add_argument("--confirm", help="the word library plan shows for a permanent delete")
    lsub.add_parser("pending")
    lsub.add_parser("batches")
    r = lsub.add_parser("restore", help="undo one move to PendingDeletion")
    r.add_argument("batch")
    r.add_argument("--yes", action="store_true")

    p = sub.add_parser("history", help="Restore previous channels.json / config.ini")
    hsub = p.add_subparsers(dest="action", required=True)
    hsub.add_parser("list").add_argument("file", choices=["channels", "config"])
    r = hsub.add_parser("restore")
    r.add_argument("file", choices=["channels", "config"])
    r.add_argument("index", type=int)
    r.add_argument("--yes", action="store_true")

    p = sub.add_parser("call", help="send any protocol command with JSON args")
    p.add_argument("cmd")
    p.add_argument("args", nargs="?", default="{}")
    return ap


def to_request(a):
    """(protocol command, args) for parsed CLI arguments."""
    c = a.command
    simple = {"status": "status", "channels": "channels", "ping": "ping", "help": "help", "start": "start",
              "stop": "stop", "quit": "quit", "reload-channels": "channels_reload"}
    if c in simple:
        return simple[c], {}
    if c == "add":
        return "channel_add", {"names": a.names, "platform": a.platform, "disabled": a.disabled}
    if c in ("remove", "enable", "disable"):
        return "channel_" + c, {"channels": a.channels}
    if c in ("record-start", "record-stop"):
        return c.replace("-", "_"), {"channels": a.channels}
    if c == "move":
        return "channel_move", {"channels": a.channels, "to": a.to}
    if c == "sort":
        return "channel_sort", {"by": a.by}
    if c == "edit":
        return "channel_edit", {k: v for k, v in {"channel": a.channel, "new_name": a.new_name,
                                                  "platform": a.platform}.items() if v is not None}
    if c == "check":
        return "check", ({"channels": a.channels} if a.channels else {})
    if c == "clip":
        return "clip", {"channel": a.channel, "seconds": a.seconds}
    if c == "screenshot":
        return "screenshot", {"channel": a.channel}
    if c == "config":
        if a.action == "reload":
            return "config_reload", {}
        section, _, key = (a.name or "").partition(".")
        if a.action == "get":
            return "config_get", {k: v for k, v in {"section": section, "key": key.lower()}.items() if v}
        value = sys.stdin.readline().rstrip("\r\n") if a.stdin else a.value
        if value is None or not key:
            raise SystemExit("config set needs Section.key and a value (or --stdin)")
        return "config_set", {"section": section, "key": key, "value": value}
    if c == "logs":
        return "logs", {"lines": a.lines, "filter": a.filter}
    if c == "update":
        return ("update_check", {}) if a.action == "check" else ("update_install", {"confirm": a.yes})
    if c == "library":
        if a.action in ("summary", "pending", "batches"):
            return "library_" + a.action, {}
        if a.action == "files":
            return "library_files", dict(_filter_args(a), limit=a.limit)
        if a.action == "restore":
            return "library_restore", {"batch": a.batch, "confirm": a.yes}
        args = dict(_filter_args(a), action=a.method, no_sidecars=a.no_sidecars)
        if a.action == "plan":
            return "library_plan", args
        args["confirm"] = a.confirm if a.confirm else a.yes
        return "library_execute", args
    if c == "history":
        if a.action == "list":
            return "history_list", {"file": a.file}
        return "history_restore", {"file": a.file, "index": a.index, "confirm": a.yes}
    if c == "call":
        return a.cmd, json.loads(a.args)
    raise SystemExit(f"unknown command {c}")


def run_offline(data_dir, cmd, args):
    """No copy answers: run the command on the files (refused if a copy holds the lock)."""
    from msr.config import Config
    from msr.status import is_running
    config_path = os.path.join(data_dir, "config.ini")
    if not os.path.isfile(config_path):
        return {"ok": False, "code": "not_running",
                "error": f"No running copy and no config.ini in {data_dir} (use --data-dir)."}, EXIT_NOT_RUNNING
    cwd = os.getcwd()
    os.chdir(data_dir)  # relative paths in config.ini are relative to the data folder, as in the app
    try:
        config = Config(config_path)
        if is_running(config.get("Paths", "streams_dir")):
            return {"ok": False, "code": "not_running",
                    "error": "A copy is running for this recordings folder but its control channel does not "
                             "answer (an older version, or [Control] enabled = false). Nothing was changed."}, \
                EXIT_NOT_RUNNING
        if cmd in LIVE_ONLY:
            return {"ok": False, "code": "not_running", "error": "Multi-Stream Recorder is not running."}, \
                EXIT_NOT_RUNNING
        reply = control.Controller(control.OfflineApp(config)).handle(cmd, args)
        if cmd == "status" and reply.get("ok"):
            reply["result"]["app"]["running"] = False
        return reply, None
    finally:
        os.chdir(cwd)


def _channel_lines(res):
    out = []
    for row in res["channels"]:
        mark = "x" if row["enabled"] else (" " if row["enabled"] is False else "-")
        num = f"#{row['index']}" if row["index"] else "  "
        extra = " ".join(str(x) for x in (row["size"], row["duration"], row["file"]) if x)
        out.append(f"{num:>4} [{mark}] {row['name']:<32} {row['status']:<14} {extra}".rstrip())
    return out


def human(cmd, reply):
    if not reply.get("ok"):
        return f"error ({reply.get('code')}): {reply.get('error')}"
    res = reply.get("result")
    if cmd in ("status", "channels"):
        lines = []
        if cmd == "status":
            app, ses = res["app"], res["session"]
            if app.get("mode") == "offline":
                lines.append("MSR is not running - changes are saved to the files; start MSR to record.")
                lines.extend(_channel_lines(res))
                return "\n".join(lines)
            lines.append(f"MSR {app['version']} {app['mode']} pid {app['pid']} - "
                         + ("recording session running" if ses["running"] else "not recording")
                         + (f", {ses['recording']} recording" if ses["running"] else ""))
            if ses.get("storage", {}).get("notice"):
                lines.append("storage: " + ses["storage"]["notice"])
            if not ses["running"] and app.get("mode") == "headless":
                lines.append("Recording is stopped: 'start' (or add/enable a channel) starts it.")
        lines.extend(_channel_lines(res))
        return "\n".join(lines)
    if cmd == "logs":
        return "\n".join(res["lines"])
    return json.dumps(res, indent=2)


def main(argv=None):
    ap = build_parser()
    a = ap.parse_args(argv)
    try:
        cmd, args = to_request(a)
    except ValueError as e:
        print(f"bad JSON args: {e}", file=sys.stderr)
        return EXIT_USAGE
    data_dir = data_dir_from(a)
    exit_code = None
    try:
        reply = control.call(data_dir, cmd, args, timeout=a.timeout)
    except control.NotRunning:
        reply, exit_code = run_offline(data_dir, cmd, args)
    except (socket.timeout, TimeoutError):
        reply, exit_code = {"ok": False, "code": "timeout", "error": "No reply in time."}, EXIT_TIMEOUT
    if exit_code is None:
        exit_code = EXIT_OK if reply.get("ok") else CODE_EXIT.get(reply.get("code"), EXIT_FAILED)
    out = json.dumps(reply, indent=2) if a.json else human(cmd, reply)
    stream = sys.stdout if reply.get("ok") else sys.stderr
    try:
        stream.reconfigure(errors="replace")
    except Exception:
        pass
    print(out, file=stream)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
