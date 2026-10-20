"""Data-directory resolution: keep runtime files out of the code folder.

MSR historically resolved config.ini, channels.json, window_state.json and
kick_channel_ids.json relative to the current working directory. To run the
code from a git checkout while every runtime/secret file stays elsewhere,
pass ``--data-dir`` or set ``MSR_HOME``. The process then chdirs there before
reading config, so all of those relative paths land in the data dir. Worker
processes inherit both the working directory and ``MSR_HOME``.

Without either, nothing changes (the current directory is used as before).
"""
import os

ENV_VAR = "MSR_HOME"


def resolve_data_dir(cli_value=None, environ=None):
    """Return the absolute data dir, or None to keep the current behaviour.

    ``--data-dir`` wins over ``MSR_HOME``.  Blank values are ignored.
    """
    environ = os.environ if environ is None else environ
    value = (cli_value or "").strip() or (environ.get(ENV_VAR) or "").strip()
    if not value:
        return None
    value = os.path.expandvars(os.path.expanduser(value.strip('"')))
    return os.path.abspath(value)


def activate_data_dir(path, environ=None):
    """chdir into ``path`` and export it as MSR_HOME for child processes.

    Raises FileNotFoundError if the folder does not exist: silently creating
    an empty one would start MSR with a fresh default config and no channels.
    """
    environ = os.environ if environ is None else environ
    if not os.path.isdir(path):
        raise FileNotFoundError(f"MSR data dir does not exist: {path}")
    os.chdir(path)
    environ[ENV_VAR] = path
    return path
