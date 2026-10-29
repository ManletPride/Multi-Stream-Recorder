"""The About text (Tk-free so it can be tested). The GUI shows it in a themed info dialog."""

from __future__ import annotations

import os

SITES = ("Records live streams from Kick, Twitch, YouTube, Rumble, TikTok, Fishtank.live "
         "and any site yt-dlp supports (or a direct .m3u8 link).")
DISCLAIMER = ("Use responsibly: you are responsible for following each site's terms of service and "
              "your local laws, including when accessing age-restricted or adult sites. The author is "
              "not responsible for how this software is used. cookies.txt is your login; never share it.")


def tool_lines(deps_module) -> list[str]:
    d = deps_module
    return [
        f"yt-dlp {d.YTDLP_VERSION}" if d.HAS_YTDLP else "yt-dlp: not found",
        f"streamlink {d.STREAMLINK_VERSION}" if d.HAS_STREAMLINK else "streamlink: not found",
        f"ffmpeg {d.FFMPEG_VERSION}" if d.HAS_FFMPEG else "ffmpeg: not found",
        f"deno {d.DENO_VERSION}" if d.HAS_DENO else "deno: not found (recommended for YouTube)",
    ]


def package_line(version_of=None) -> str:
    from msr import depsupdate
    version_of = version_of or depsupdate.installed_version
    parts = []
    for dist, label, kind in depsupdate.PACKAGES:
        if kind == "tool":
            continue                       # yt-dlp / streamlink are listed with the tools
        v = version_of(dist)
        parts.append(f"{label} {v}" if v else f"{label} (missing)")
    return ", ".join(parts)


def environment_line(in_venv: bool, python_version: str) -> str:
    if in_venv:
        return f"Python {python_version} in MSR's own environment (.venv)."
    return f"Python {python_version}, not MSR's .venv (package updates are off)."


def about_text(version: str, *, tools: list[str], packages: str, environment: str,
               streams: str, clips: str, data_dir: str) -> str:
    return "\n".join([
        f"Multi-Stream Recorder v{version}",
        "",
        SITES,
        "",
        "Tools: " + " \u00b7 ".join(tools),
        "Python packages: " + packages,
        environment,
        "Check and update them with the Updates\u2026 button.",
        "",
        f"Recordings: {streams}",
        f"Clips & screenshots: {clips}",
        f"Settings (config.ini, channels.json, cookies.txt): {data_dir}",
        "To move the recordings or clips, change streams_dir / clips_dir in config.ini, "
        "then fully quit (Ctrl+Q) and relaunch.",
        "",
        "Scripts and AI agents can control MSR with msr_ctl.py (see docs/AGENT_GUIDE.md).",
        "",
        DISCLAIMER,
    ])


def about_for(config, version: str) -> str:
    """The text for this running copy."""
    import sys
    from msr import deps, venv
    streams = config.get("Paths", "streams_dir")
    clips = (config.get("Clipping", "clips_dir", fallback="") or "").strip() or os.path.join(streams, "Clips")
    data_dir = os.path.dirname(os.path.abspath(getattr(config, "config_file", "config.ini")))
    return about_text(version, tools=tool_lines(deps), packages=package_line(),
                      environment=environment_line(venv.in_venv(), sys.version.split()[0]),
                      streams=streams, clips=clips, data_dir=data_dir)
