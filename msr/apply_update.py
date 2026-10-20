#!/usr/bin/env python3
"""Standalone helper: wait for MSR to exit, copy a staged update, relaunch.

Copied to a temp file before MSR quits so this process does not run from the
``msr/`` tree it is about to replace. Stdlib only — do not import ``msr``.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time

ERROR_FILE = "msr_update_error.txt"


def pid_alive(pid):
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        handle = ctypes.windll.kernel32.OpenProcess(0x100000, False, int(pid))
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def wait_for_pid(pid, timeout=120):
    deadline = time.monotonic() + timeout
    while pid_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.4)
    return not pid_alive(pid)


def copy_staged_tree(src, dst):
    """Copy *src* over *dst*, skipping bytecode caches."""
    os.makedirs(dst, exist_ok=True)
    for dirpath, dirnames, filenames in os.walk(src):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        rel = os.path.relpath(dirpath, src)
        dest_dir = dst if rel == os.curdir else os.path.join(dst, rel)
        os.makedirs(dest_dir, exist_ok=True)
        for name in filenames:
            if name.endswith((".pyc", ".pyo")):
                continue
            shutil.copy2(os.path.join(dirpath, name), os.path.join(dest_dir, name))


def apply_staged_update(staging, target):
    launcher_src = os.path.join(staging, "Multi-Stream-Recorder.py")
    msr_src = os.path.join(staging, "msr")
    if not os.path.isfile(launcher_src) or not os.path.isdir(msr_src):
        raise RuntimeError("staging folder is missing Multi-Stream-Recorder.py or msr/")
    shutil.copy2(launcher_src, os.path.join(target, "Multi-Stream-Recorder.py"))
    for extra in ("requirements.txt", "channels_fishtank.json"):
        src = os.path.join(staging, extra)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(target, extra))
    shutil.rmtree(os.path.join(target, "msr", "__pycache__"), ignore_errors=True)
    copy_staged_tree(msr_src, os.path.join(target, "msr"))


def relaunch(python_exe, launcher, cwd):
    kwargs = {"cwd": cwd, "close_fds": True}
    if os.name == "nt":
        flags = 0
        flags |= getattr(subprocess, "DETACHED_PROCESS", 0)
        flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
        kwargs["creationflags"] = flags
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen([python_exe, launcher], **kwargs)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Apply a staged MSR update")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--staging", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--launcher", required=True)
    args = parser.parse_args(argv)
    err_path = os.path.join(args.target, ERROR_FILE)
    try:
        if os.path.isfile(err_path):
            os.remove(err_path)
        if not wait_for_pid(args.pid):
            raise RuntimeError(f"MSR PID {args.pid} is still running")
        time.sleep(0.5)
        apply_staged_update(args.staging, args.target)
        relaunch(args.python, args.launcher, args.target)
    except Exception as exc:
        try:
            with open(err_path, "w", encoding="utf-8") as fh:
                fh.write(str(exc))
        except Exception:
            pass
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
