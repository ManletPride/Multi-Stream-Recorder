"""A scripted fake ffmpeg + ffprobe for testing remux/probe flows without real media.

    fake = FakeFfmpeg(tmpdir, "ok")                    # every remux succeeds
    fake = FakeFfmpeg(tmpdir, "fail", "ok")            # the 1st remux fails, the 2nd and later succeed
    fake = FakeFfmpeg(tmpdir, {"mode": "truncate", "fraction": 0.4})
    write_media(raw, size=8 * MB, duration=600)        # a stand-in .ts: a header line + padding
    with fake.installed():                             # msr finds the fake ffprobe next to the fake ffmpeg
        worker.remux_to_mp4(raw, mp4, fake.ffmpeg, logger, timeout=30)
    fake.calls()                                       # every invocation: {"tool": "ffmpeg", "argv": [...]}

A "media file" is ``FAKEMEDIA duration=<seconds>`` on its first line, then padding up to ``size``
bytes. ffprobe (and ``ffmpeg -i x -hide_banner``) read the duration from that line; a file without
the line is "unreadable" like an MP4 whose moov atom was never written.

Modes, one consumed per remux/concat call (the last repeats; ``"mode"`` plus options as a dict):

====================  =========================================================================
ok                    output = same duration, size ``size`` (default: the input's), exit 0
truncate              exit 0 but only ``fraction`` (default 0.5) of the duration and size
fail                  exit ``code`` (default 1), ``stderr`` text, ``partial`` (default 0.3) of the
                      bytes left behind without a header, like an ffmpeg killed mid-write
no_moov               exit 0 with an unreadable output (no header)
no_output             exit 0 and no output file at all
hang                  writes a partial file, then prints nothing and sleeps ``seconds`` (3600)
====================  =========================================================================

Runs anywhere Python runs: a ``#!/bin/sh`` launcher on POSIX, a ``.cmd`` launcher on Windows, both
calling this file. Stdlib only; the ``msr`` package is not imported by the fake itself.
"""
import contextlib
import json
import os
import re
import stat
import sys
import time

MB = 1024 * 1024
HEADER = b"FAKEMEDIA duration="


def write_media(path, size, duration):
    """Create a stand-in media file of ``size`` bytes that probes to ``duration`` seconds."""
    head = HEADER + f"{float(duration):.3f}\n".encode()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as f:
        f.write(head)
        f.write(b"\0" * max(0, size - len(head)))
    return path


def read_duration(path):
    """Seconds from a media file's header, or None if it has none (unreadable / not media)."""
    try:
        with open(path, "rb") as f:
            line = f.readline(200)
    except OSError:
        return None
    if not line.startswith(HEADER):
        return None
    try:
        return float(line[len(HEADER):].strip())
    except ValueError:
        return None


class FakeFfmpeg:
    def __init__(self, directory, *modes):
        self.directory = os.path.abspath(directory)
        os.makedirs(self.directory, exist_ok=True)
        self._scenario = os.path.join(self.directory, "scenario.json")
        self._state = os.path.join(self.directory, "state.json")
        self._log = os.path.join(self.directory, "calls.jsonl")
        self.set_modes(*(modes or ("ok",)))
        win = os.name == "nt"
        for tool in ("ffmpeg", "ffprobe"):
            path = os.path.join(self.directory, tool + (".cmd" if win else ""))
            with open(path, "w", newline="\n" if not win else "\r\n") as f:
                if win:
                    f.write(f'@"{sys.executable}" "{__file__}" {tool} "{self.directory}" %*\n')
                else:
                    f.write(f'#!/bin/sh\nexec "{sys.executable}" "{__file__}" {tool} "{self.directory}" "$@"\n')
            if not win:
                os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)
            setattr(self, tool, path)

    def set_modes(self, *modes):
        with open(self._scenario, "w") as f:
            json.dump([m if isinstance(m, dict) else {"mode": m} for m in modes], f)
        with open(self._state, "w") as f:
            json.dump({"n": 0}, f)

    def calls(self, tool=None):
        try:
            with open(self._log) as f:
                rows = [json.loads(line) for line in f if line.strip()]
        except OSError:
            return []
        return [r for r in rows if tool is None or r["tool"] == tool]

    def remuxes(self):
        """The ffmpeg calls that wrote an output (not the ``-i x -hide_banner`` probes)."""
        return [c for c in self.calls("ffmpeg") if _output_of(c["argv"])]

    @contextlib.contextmanager
    def installed(self):
        """Point msr's ffprobe lookup at the fake ffprobe (a ``.cmd`` has no ``ffprobe.exe`` sibling)."""
        from unittest import mock
        import msr.util
        import msr.worker
        real = msr.util.ffprobe_from_ffmpeg
        fake_probe = lambda p: self.ffprobe if os.path.dirname(os.path.abspath(p)) == self.directory else real(p)  # noqa: E731
        with mock.patch.object(msr.worker, "ffprobe_from_ffmpeg", fake_probe), \
                mock.patch.object(msr.util, "ffprobe_from_ffmpeg", fake_probe):
            yield self


# ── the fake program itself ─────────────────────────────────────────────────────

def _output_of(argv):
    if "-i" not in argv:
        return None
    if argv[-1] == "-y" and len(argv) >= 2:
        return argv[-2]
    last = argv[-1]
    if last.startswith("-") or last == argv[argv.index("-i") + 1]:
        return None
    return last


def _inputs_of(argv):
    src = argv[argv.index("-i") + 1]
    if "concat" in argv:
        with open(src, encoding="utf-8") as f:
            return [m.group(1).replace("'\\''", "'") for m in (re.match(r"file '(.*)'$", ln.strip()) for ln in f) if m]
    return [src]


def _next_mode(directory):
    with open(os.path.join(directory, "scenario.json")) as f:
        modes = json.load(f)
    state_path = os.path.join(directory, "state.json")
    with open(state_path) as f:
        n = json.load(f)["n"]
    with open(state_path, "w") as f:
        json.dump({"n": n + 1}, f)
    return modes[min(n, len(modes) - 1)]


def _write_partial(out, size):
    with open(out, "wb") as f:
        f.write(b"\0" * size)


def _ffmpeg(directory, argv):
    sources = None
    if "-i" in argv:
        try:
            sources = _inputs_of(argv)
        except OSError:
            sources = None
    out = _output_of(argv)
    if out is None:                                    # `ffmpeg -i x -hide_banner`: the info probe
        dur = read_duration(sources[0]) if sources else None
        if dur is not None:
            h, rem = divmod(dur, 3600)
            m, s = divmod(rem, 60)
            sys.stderr.write(f"Input #0, mpegts, from '{sources[0]}':\n  Duration: {int(h):02d}:{int(m):02d}:{s:05.2f},"
                             " start: 0.000000, bitrate: 1000 kb/s\n")
        else:
            sys.stderr.write("Invalid data found when processing input\n")
        sys.stderr.write("At least one output file must be specified\n")
        return 1
    spec = _next_mode(directory)
    mode = spec.get("mode", "ok")
    in_size = sum(os.path.getsize(p) for p in sources if os.path.exists(p)) if sources else 0
    in_dur = sum(read_duration(p) or 0 for p in sources) if sources else 0
    if mode in ("ok", "truncate"):
        frac = float(spec.get("fraction", 0.5)) if mode == "truncate" else 1.0
        size = int(spec["size"]) if "size" in spec else int(in_size * frac)
        for i in range(3):                             # what `-progress pipe:1` prints
            sys.stdout.write(f"out_time_us={int(in_dur * frac * 1e6 * (i + 1) / 3)}\nprogress=continue\n")
        sys.stdout.write("progress=end\n")
        sys.stdout.flush()
        write_media(out, size, in_dur * frac)
        return 0
    if mode == "no_moov":
        _write_partial(out, int(in_size * float(spec.get("partial", 0.95))))
        return 0
    if mode == "no_output":
        return 0
    if mode == "hang":
        _write_partial(out, int(in_size * float(spec.get("partial", 0.3))))
        time.sleep(float(spec.get("seconds", 3600)))
        return 0
    if mode == "fail":
        part = float(spec.get("partial", 0.3))
        if part:
            _write_partial(out, int(in_size * part))
        sys.stderr.write(spec.get("stderr", "fake ffmpeg: simulated failure") + "\n")
        return int(spec.get("code", 1))
    sys.stderr.write(f"fake ffmpeg: unknown mode {mode!r}\n")
    return 2


def _ffprobe(directory, argv):
    path = argv[-1]
    dur = read_duration(path)
    if dur is None:
        sys.stderr.write(f"{path}: Invalid data found when processing input\n")
        return 1
    joined = " ".join(argv)
    if "-show_streams" in argv:
        streams = [{"codec_name": "aac", "channels": 2}] if "a" in argv else [{"codec_name": "h264"}]
        sys.stdout.write(json.dumps({"streams": streams, "format": {"duration": f"{dur:.6f}"}}))
    elif "format=duration" in joined:
        sys.stdout.write(f"duration={dur:.6f}\nstart_time=0.000000\n")
    elif "stream=duration" in joined:
        sys.stdout.write(f"{dur:.6f}\n")
    else:
        sys.stdout.write(f"{dur:.6f}\n")
    return 0


def main(argv):
    tool, directory, args = argv[1], argv[2], argv[3:]
    with open(os.path.join(directory, "calls.jsonl"), "a") as f:
        f.write(json.dumps({"tool": tool, "argv": args}) + "\n")
    code = (_ffmpeg if tool == "ffmpeg" else _ffprobe)(directory, args)
    sys.stdout.flush()
    sys.stderr.flush()
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv))
