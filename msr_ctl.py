r"""msr_ctl.py - control Multi-Stream Recorder from a script, an agent or a terminal.

Talks to the copy running for the data folder (GUI or --headless) over its local control
channel (127.0.0.1 + the token in msr_control.json). With no copy running, roster, settings,
history and library commands work on the files directly (atomic writes, same history copies
as the app); session commands then exit with code 3.

Data folder: --data-dir, else %MSR_HOME%, else %MSR_DATA%, else the current folder.

Exit codes: 0 ok, 1 refused/failed, 2 usage, 3 not running (or running but not answering),
4 wrong token, 5 needs confirmation (--yes / --confirm WORD), 6 timed out, 7 API version
mismatch (use the msr_ctl.py of the running copy), 130 Ctrl+C.

`help --json` prints every command with its arguments, safety level and an example.
Agents: see docs/AGENT_GUIDE.md.

Examples:
  python msr_ctl.py --json status
  python msr_ctl.py add twitch:somename
  python msr_ctl.py disable "#3" kickname
  python msr_ctl.py start | stop | quit
  python msr_ctl.py record-start kickname      (one channel, mid-session)
  python msr_ctl.py health
  python msr_ctl.py recordings --since 24h
  python msr_ctl.py wait --event finished --channel kickname --timeout 3600
  python msr_ctl.py library suggest --free 200
  python msr_ctl.py config set Timeouts.poll_interval_minutes 1
  python msr_ctl.py library plan pending --channel somename --older-than-days 30
  python msr_ctl.py library run pending --channel somename --older-than-days 30 --yes
"""

import argparse
import json
import os
import socket
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import msr.nowindow  # noqa: F401,E402  (hide child console windows, as everywhere else)
from msr import control  # noqa: E402
from msr.nowindow import no_window_kwargs  # noqa: E402
from msr.agent_status import EVENT_KINDS  # noqa: E402

EXIT_OK, EXIT_FAILED, EXIT_USAGE, EXIT_NOT_RUNNING, EXIT_AUTH, EXIT_CONFIRM, EXIT_TIMEOUT = range(7)
EXIT_API = 7  # the running copy speaks another control api_version
EXIT_INTERRUPTED = 130  # Ctrl+C
EXIT_CODES = {EXIT_OK: "ok", EXIT_FAILED: "refused or failed", EXIT_USAGE: "usage error",
              EXIT_NOT_RUNNING: "not running (or running but not answering)", EXIT_AUTH: "wrong token",
              EXIT_CONFIRM: "needs confirmation (--yes / --confirm WORD)", EXIT_TIMEOUT: "timed out",
              EXIT_API: "control API version mismatch", EXIT_INTERRUPTED: "interrupted (Ctrl+C)"}

# One row per CLI command (the parser's leaves): safety level and examples. help --json joins this
# with the parser itself (arguments, options) and derives each example's protocol command through
# to_request, so the catalog cannot drift; tests/test_agent_status.py checks both directions.
# Safety: read = changes nothing; write = safe, reversible edit; session = starts/stops recording or
# the app; destructive = removes or replaces something and needs --yes / --confirm.
CATALOG = {
    "status": ("read", ["--json status"]),
    "channels": ("read", ["channels"]),
    "ping": ("read", ["ping"]),
    "help": ("read", ["help --json"]),
    "health": ("read", ["health"]),
    "start": ("session", ["start"]),
    "stop": ("session", ["stop"]),
    "quit": ("session", ["quit"]),
    "reload-channels": ("write", ["reload-channels"]),
    "add": ("write", ["add kick:knut twitch:someone"]),
    "remove": ("write", ["remove twitch:someone"]),
    "enable": ("write", ["enable knut", "enable --all"]),
    "disable": ("write", ["disable knut", "disable --all"]),
    "record-start": ("session", ["record-start knut"]),
    "record-stop": ("session", ["record-stop knut"]),
    "recordings": ("read", ["recordings --since 24h", "recordings pending", "recordings retry"]),
    "wait": ("read", ["wait --event finished --channel knut --timeout 600",
                      "wait --event offline --channel knut --timeout 3600"]),
    "events": ("read", ["events --after 0"]),
    "move": ("write", ["move knut --to top"]),
    "sort": ("write", ["sort platform"]),
    "edit": ("write", ["edit twitch:old --name new"]),
    "check": ("read", ["check knut"]),
    "clip": ("write", ["clip knut --seconds 30", "clip knut --from=-1:00 --to end",
                       "clip --file D:/streams/Processed/kick/knut/knut.mp4 --from 1:02:03 --to 1:02:48 --exact"]),
    "screenshot": ("write", ["screenshot knut"]),
    "config get": ("read", ["config get Timeouts"]),
    "config set": ("write", ["config set Timeouts.poll_interval_minutes 1"]),
    "config reload": ("write", ["config reload"]),
    "logs": ("read", ["logs -n 50 --filter error"]),
    "update check": ("read", ["update check"]),
    "deps": ("write", ["deps list", "deps check", "deps update yt-dlp streamlink", "deps update --all"]),
    "update install": ("destructive", ["update install --yes"]),
    "library summary": ("read", ["library summary"]),
    "library files": ("read", ["library files --channel knut --older-than-days 30"]),
    "library plan": ("read", ["library plan pending --channel knut --older-than-days 30"]),
    "library run": ("destructive", ["library run pending --channel knut --older-than-days 30 --yes"]),
    "library pending": ("read", ["library pending"]),
    "library batches": ("read", ["library batches"]),
    "library restore": ("destructive", ["library restore --last --yes"]),
    "library suggest": ("read", ["library suggest --free 200 --older-days 90"]),
    "library log": ("read", ["library log -n 20"]),
    "history list": ("read", ["history list channels"]),
    "history restore": ("destructive", ["history restore channels 2 --yes"]),
    "call": ("depends on the command", ["call ping"]),
}
SAFETY_NOTES = {"deps": "deps list / check only read; deps update pip-installs (old version restored on failure)",
                "recordings": "recordings retry runs the cleanup remux (write; originals kept until verified)"}
LIVE_ONLY = {"start", "stop", "quit", "record_start", "record_stop", "check", "clip", "screenshot",
             "update_install", "wait", "events"}
CODE_EXIT = {"auth": EXIT_AUTH, "confirm_required": EXIT_CONFIRM, "not_running": EXIT_NOT_RUNNING,
             "timeout": EXIT_TIMEOUT}


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
                       ("ping", "is a copy running?"),
                       ("start", "Start Recording"), ("stop", "Stop Recording"), ("quit", "quit the app cleanly"),
                       ("reload-channels", "re-read channels.json"),
                       ("health", "tools, disk space, cookies, Fishtank login, problems")):
        sub.add_parser(name, help=text)

    sub.add_parser("help", help="every command: arguments, safety, examples (local; --json for agents)") \
        .add_argument("--json", dest="help_json", action="store_true", help="machine-readable catalog")

    p = sub.add_parser("add", help="add channel(s): name, platform:name or a URL")
    p.add_argument("names", nargs="+")
    p.add_argument("--platform")
    p.add_argument("--disabled", action="store_true", help="add unticked")
    for name, text in (("remove", "remove from the roster (recordings stay)"),
                       ("record-start", "start recording now (ticks it)"),
                       ("record-stop", "stop recording this channel")):
        sub.add_parser(name, help=text).add_argument("channels", nargs="+", help="#N, key or shown name")
    for name, text in (("enable", "tick"), ("disable", "untick")):
        p = sub.add_parser(name, help=text)
        p.add_argument("channels", nargs="*", help="#N, key or shown name")
        p.add_argument("--all", action="store_true", help="every channel in the roster")

    p = sub.add_parser("recordings", help="finished recordings; 'pending' = waiting files; 'retry' = run cleanup")
    p.add_argument("action", nargs="?", default="list", choices=["list", "pending", "retry"])
    p.add_argument("--since", help="24h, 7d, 2026-10-29 or '2026-10-29 14:00'")
    p.add_argument("--channel")
    p.add_argument("--limit", type=int, default=50)
    p = sub.add_parser("wait", help="block until a channel goes live, a recording finishes or an error")
    p.add_argument("--event", action="append", choices=list(EVENT_KINDS),
                   help="repeatable; default: any. A 'finished' wait for a channel that was stopped "
                        "starts at that stop, so its finish is never missed")
    p.add_argument("--channel")
    p.add_argument("--timeout", "--for", dest="wait_timeout", type=float, default=300.0,
                   help="seconds (max 3600); exit code 6 when nothing happened")
    p.add_argument("--after", type=int, help="event seq to continue from (no event is missed between waits)")
    sub.add_parser("events", help="recent events (" + "/".join(EVENT_KINDS) + ")").add_argument("--after", type=int, default=0)
    p = sub.add_parser("move", help="reorder")
    p.add_argument("channels", nargs="+")
    p.add_argument("--to", required=True, choices=["top", "bottom", "up", "down"])
    sub.add_parser("sort", help="sort the roster").add_argument("by", choices=["enabled", "platform", "name"])
    p = sub.add_parser("edit", help="rename / change platform")
    p.add_argument("channel")
    p.add_argument("--name", dest="new_name")
    p.add_argument("--platform")
    sub.add_parser("check", help="Check Now (all, or the named channels)").add_argument("channels", nargs="*")
    p = sub.add_parser("clip", help="Clip Now (last N seconds), or cut --from/--to like the Clip Editor")
    p.add_argument("channel", nargs="?")
    p.add_argument("--seconds", type=int)
    p.add_argument("--from", dest="from_", help="start: seconds, M:SS, H:MM:SS; --from=-T counts back from the end")
    p.add_argument("--to", help="end (default: end = the live edge)")
    p.add_argument("--file", help="a recording file (finished .mp4 or a .ts) instead of the channel")
    p.add_argument("--exact", action="store_true", help="re-encode for frame-exact edges (default: keyframe)")
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

    p = sub.add_parser("deps", help="dependencies: list versions, check for updates, pip-update packages")
    p.add_argument("action", choices=["list", "check", "update"])
    p.add_argument("names", nargs="*", help="update: package names (yt-dlp, streamlink, pillow, ...)")
    p.add_argument("--all", action="store_true", help="update: every package with an update")
    p.add_argument("--system", action="store_true",
                   help="update: allow pip outside MSR's .venv (changes the Python other programs use)")
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
    r.add_argument("batch", nargs="?")
    r.add_argument("--last", action="store_true", help="the newest batch")
    r.add_argument("--yes", action="store_true")
    g = lsub.add_parser("suggest", help="clean-up suggestions (read only)")
    g.add_argument("--free", type=float, dest="free_gb", help="GB to free: oldest recordings first")
    g.add_argument("--older-days", type=int, default=90)
    g.add_argument("--limit", type=int, default=50)
    g = lsub.add_parser("log", help="library_actions.log")
    g.add_argument("-n", "--lines", type=int, default=50)

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
    if c in ("enable", "disable"):
        if a.all == bool(a.channels):
            raise SystemExit(f"{c}: name channels or use --all (not both)")
        return "channel_" + c, ({"all": True} if a.all else {"channels": a.channels})
    if c == "remove":
        return "channel_" + c, {"channels": a.channels}
    if c == "health":
        return "health", {}
    if c == "recordings":
        if a.action == "list":
            return "recordings", {k: v for k, v in {"since": a.since, "channel": a.channel,
                                                    "limit": a.limit}.items() if v is not None}
        return "recordings_" + a.action, {}
    if c == "wait":
        args = {"events": a.event or list(EVENT_KINDS), "timeout": a.wait_timeout}
        if a.channel:
            args["channel"] = a.channel
        if a.after is not None:
            args["after"] = a.after
        return "wait", args
    if c == "events":
        return "events", {"after": a.after}
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
        if a.from_ is not None or a.to is not None or a.file:
            return "clip", {k: v for k, v in {"channel": a.channel, "file": a.file, "from": a.from_, "to": a.to,
                                              "exact": a.exact or None}.items() if v is not None}
        if not a.channel:
            raise SystemExit("clip: give a channel (or --file with --from/--to)")
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
    if c == "deps":
        if a.action != "update":
            if a.names or a.all:
                raise SystemExit(f"deps {a.action} takes no names")
            return "deps", {"action": a.action}
        if a.all == bool(a.names):
            raise SystemExit("deps update: name packages or use --all (not both)")
        req = {"action": "update", "all": True} if a.all else {"action": "update", "names": a.names}
        if a.system:
            req["system"] = True
        return "deps", req
    if c == "update":
        return ("update_check", {}) if a.action == "check" else ("update_install", {"confirm": a.yes})
    if c == "library":
        if a.action in ("summary", "pending", "batches"):
            return "library_" + a.action, {}
        if a.action == "files":
            return "library_files", dict(_filter_args(a), limit=a.limit)
        if a.action == "restore":
            if bool(a.batch) == a.last:
                raise SystemExit("library restore: give a batch id or --last")
            return "library_restore", {"batch": a.batch, "last": a.last, "confirm": a.yes}
        if a.action == "suggest":
            return "library_suggest", {"free_gb": a.free_gb, "older_days": a.older_days, "limit": a.limit}
        if a.action == "log":
            return "library_log", {"lines": a.lines}
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


def _leaves(parser, prefix=()):
    """(path, parser) for every runnable command of the CLI, from the parser itself."""
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            helps = {c.dest: c.help for c in action._choices_actions}
            for name, sub in action.choices.items():
                path = prefix + (name,)
                inner = any(isinstance(x, argparse._SubParsersAction) for x in sub._actions)
                if inner:
                    yield from _leaves(sub, path)
                else:
                    yield " ".join(path), sub, helps.get(name) or ""


def _arg_info(action):
    out = {"name": action.dest, "flags": list(action.option_strings) or None,
           "required": bool(action.required) or (not action.option_strings and action.nargs not in ("?", "*")),
           "help": action.help or ""}
    if action.choices:
        out["choices"] = list(action.choices)
    if action.nargs is not None:
        out["nargs"] = action.nargs
    if action.default not in (None, argparse.SUPPRESS, False) and action.option_strings:
        out["default"] = action.default
    if isinstance(action, (argparse._StoreTrueAction,)):
        out["flag_only"] = True
    return out


def catalog():
    """The machine-readable command catalog (help --json)."""
    ap = build_parser()
    commands = []
    for path, sub, text in _leaves(ap):
        safety, examples = CATALOG[path]
        protocol = []
        for ex in examples:
            cmd, _args = to_request(ap.parse_args(ex.split()))
            if cmd not in protocol:
                protocol.append(cmd)
        commands.append({
            "command": path, "help": text, "safety": safety, "safety_note": SAFETY_NOTES.get(path, ""),
            "arguments": [_arg_info(x) for x in sub._actions if not isinstance(x, argparse._HelpAction)],
            "examples": ["python msr_ctl.py " + ex for ex in examples],
            "protocol_commands": protocol,
        })
    return {
        "api_version": control.API_VERSION,
        "usage": "python msr_ctl.py [--data-dir DIR] [--json] [--timeout S] <command> ...",
        "global_options": [_arg_info(x) for x in ap._actions
                           if not isinstance(x, (argparse._HelpAction, argparse._SubParsersAction))],
        "exit_codes": {str(k): v for k, v in sorted(EXIT_CODES.items())},
        "events": list(EVENT_KINDS),
        "safety_levels": {"read": "changes nothing", "write": "safe, reversible edit",
                          "session": "starts or stops recording, or the app",
                          "destructive": "removes or replaces something; needs --yes or --confirm WORD"},
        "commands": commands,
    }


def _help_text(cat):
    lines = [f"msr_ctl.py commands (control API v{cat['api_version']}):"]
    for c in cat["commands"]:
        lines.append(f"  {c['command']:<18} [{c['safety']}] {c['help']}")
        lines.append(f"  {'':<18} e.g. {c['examples'][0]}")
    lines.append("exit codes: " + ", ".join(f"{k} {v}" for k, v in cat["exit_codes"].items()))
    return "\n".join(lines)


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
        if cmd in LIVE_ONLY and not (cmd == "clip" and args.get("file")):
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
    if cmd in ("logs", "library_log"):
        return "\n".join(res["lines"])
    if cmd == "deps":
        if "items" in res:
            lines = [f"{'item':<24} {'installed':<30} {'latest':<14} status"]
            for r in res["items"]:
                lines.append(f"{r['label']:<24} {str(r.get('installed') or '-'):<30} {str(r.get('latest') or '-'):<14} "
                             f"{r['status']}" + (f"  ({r['note']})" if r.get("note") else ""))
            return "\n".join(lines)
        out = [f"{r['name']}: " + (f"{r.get('old') or '-'} -> {r.get('new')}" if r.get("ok") else
               f"FAILED - {r.get('error')}" + (" (previous version restored)" if r.get("rolled_back") else ""))
               for r in res["results"]]
        return "\n".join(out + [res["note"]]) if out else "Nothing to update."
    if cmd == "health":
        lines = ["OK" if res.get("ok") else "Problems:"] + [f"  - {p}" for p in res.get("problems", [])]
        for side, d in (res.get("disk") or {}).items():
            if d:
                lines.append(f"disk {side}: {d['free_gb']} GB free (min {d['min_gb']:g}) {d['path']}")
        ck = res.get("cookies") or {}
        lines.append("cookies: " + ("none" if not ck.get("found") else
                                    f"{ck.get('cookies')} in {ck.get('file')}, earliest login expiry "
                                    f"{ck.get('earliest_auth_expiry') or '-'}"
                                    + (" (EXPIRED)" if ck.get("auth_expired") else "")))
        lines.append(f"fishtank login set: {res.get('fishtank', {}).get('login_set')}")
        for name, t in (res.get("tools") or {}).items():
            lines.append(f"{name}: {(t.get('version') or 'yes') if t.get('available') else 'missing'}")
        return "\n".join(lines)
    if cmd == "recordings":
        return "\n".join(f"{r['finished_at']}  {r['channel']:<28} {r['size'] / 1e6:>9.1f} MB  {r['path']}"
                         for r in res["recordings"]) or "No finished recordings in that range."
    if cmd == "wait":
        return f"#{res['seq']} {res['at']} {res['event']} {res['channel']}" + (
            f" {res['path']}" if res.get("path") else "") + (f" {res['error']}" if res.get("error") else "")
    return json.dumps(res, indent=2)


def main(argv=None):
    ap = build_parser()
    a = ap.parse_args(argv)
    if a.command == "help":  # local: works with nothing running
        cat = catalog()
        print(json.dumps(cat, indent=2) if (a.json or a.help_json) else _help_text(cat))
        return EXIT_OK
    try:
        cmd, args = to_request(a)
    except ValueError as e:
        print(f"bad JSON args: {e}", file=sys.stderr)
        return EXIT_USAGE
    data_dir = data_dir_from(a)
    if cmd == "deps":
        handed = _deps_in_venv(argv)
        if handed is not None:
            return handed
    exit_code = None
    try:
        timeout = a.timeout
        if cmd == "wait":
            timeout = max(timeout, float(args.get("timeout", 300)) + 30)  # outlives the server's wait
            _warn_if_nothing_to_finish(data_dir, args)
        reply = control.call(data_dir, cmd, args, timeout=timeout)
    except control.NotRunning:
        reply, exit_code = run_offline(data_dir, cmd, args)
    except control.ApiMismatch as e:
        reply, exit_code = {"ok": False, "code": "api_mismatch", "error": str(e)}, EXIT_API
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


def _deps_in_venv(argv, run=subprocess.run):
    """deps from a Python that is not MSR's: run the same command with .venv's Python (versions
    and pip updates belong to the environment MSR records with). None = handle it here."""
    from msr import venv
    root = os.path.dirname(os.path.abspath(__file__))
    if venv.in_venv() or os.environ.get("MSR_CTL_VENV_HANDOFF") or venv.check(root)[0] != "ready":
        return None
    env = dict(os.environ, MSR_CTL_VENV_HANDOFF="1")
    argv = list(sys.argv[1:] if argv is None else argv)
    r = run([venv.venv_python(root), os.path.abspath(__file__)] + argv, capture_output=True, text=True,
            env=env, stdin=subprocess.DEVNULL, **no_window_kwargs())
    if r.stdout:
        print(r.stdout, end="")
    if r.stderr:
        print(r.stderr, end="", file=sys.stderr)
    return r.returncode


def _warn_if_nothing_to_finish(data_dir, args):
    """wait --event finished on a channel that is not recording would just run into the timeout."""
    if "finished" not in (args.get("events") or []):
        return
    try:
        rows = control.call(data_dir, "status", {}, timeout=10)["result"]["channels"]
    except Exception:
        return
    want = str(args.get("channel") or "").lower()
    picked = [r for r in rows if not want or want in (r["name"].lower(), f"#{r['index']}", str(r["index"]))]
    # A stopped channel whose recording is still being remuxed ("finishing"), or that finished
    # since the stop (the wait returns that at once), is fine: no warning.
    if picked and not any(r["recording"] or r.get("finishing") or r.get("finished_since_stop")
                          for r in picked):
        who = picked[0]["name"] if want else "no channel"
        print(f"warning: {who} is not recording now, so a 'finished' event comes only after a new "
              "recording starts and ends.", file=sys.stderr)


def run(argv=None):
    try:
        return main(argv)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(run())
