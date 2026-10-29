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


BACKUP_DIR = "msr_update_backup"     # the previous program files, kept until the next update
NOTICE_FILE = "msr_update_notice.txt"
ROOT_FILES = ("Multi-Stream-Recorder.py", "requirements.txt", "channels_fishtank.json")


def _program_files(root):
    """Relative paths of the program files under *root* (launcher, extras, ``msr/``)."""
    out = [name for name in ROOT_FILES if os.path.isfile(os.path.join(root, name))]
    msr_dir = os.path.join(root, "msr")
    for dirpath, dirnames, filenames in os.walk(msr_dir):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for name in filenames:
            if not name.endswith((".pyc", ".pyo")):
                out.append(os.path.relpath(os.path.join(dirpath, name), root))
    return out


def _same_bytes(a, b):
    try:
        if os.path.getsize(a) != os.path.getsize(b):
            return False
        with open(a, "rb") as fa, open(b, "rb") as fb:
            return fa.read() == fb.read()
    except OSError:
        return False


def _requirement_lines(path):
    """The requirements in a file, ignoring line endings, blank lines and comments."""
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            return sorted({line.split("#", 1)[0].strip() for line in fh} - {""})
    except OSError:
        return None


def check_staging(staging):
    """Refuse a staged tree that is incomplete or has a broken Python file."""
    if not os.path.isfile(os.path.join(staging, "Multi-Stream-Recorder.py")) or \
            not os.path.isfile(os.path.join(staging, "msr", "__init__.py")):
        raise RuntimeError("staging folder is missing Multi-Stream-Recorder.py or msr/")
    for rel in _program_files(staging):
        if rel.endswith(".py"):
            path = os.path.join(staging, rel)
            try:
                with open(path, "rb") as fh:
                    compile(fh.read(), path, "exec")
            except (SyntaxError, ValueError) as exc:
                raise RuntimeError(f"the downloaded update is damaged ({rel}: {exc})")


def _copy_files(src_root, dst_root, rels):
    for rel in rels:
        dest = os.path.join(dst_root, rel)
        os.makedirs(os.path.dirname(dest) or dst_root, exist_ok=True)
        shutil.copy2(os.path.join(src_root, rel), dest)


def apply_staged_update(staging, target):
    """Copy the staged program files over *target*, all or nothing.

    The current program files are copied to ``msr_update_backup/`` first. Every copied file
    is compared byte for byte afterwards; on any error the backup is put back and files the
    update added are removed, so a failed update never leaves a half-updated install.
    User files (config, channels, cookies, recordings) are never touched.
    """
    check_staging(staging)
    new_files = _program_files(staging)
    old_files = _program_files(target)
    backup = os.path.join(target, BACKUP_DIR)
    shutil.rmtree(backup, ignore_errors=True)
    _copy_files(target, backup, old_files)
    for rel in old_files:
        if not _same_bytes(os.path.join(target, rel), os.path.join(backup, rel)):
            raise RuntimeError(f"could not back up {rel}; nothing was changed")
    old_requirements = os.path.join(backup, "requirements.txt")
    try:
        shutil.rmtree(os.path.join(target, "msr", "__pycache__"), ignore_errors=True)
        _copy_files(staging, target, new_files)
        bad = [rel for rel in new_files
               if not _same_bytes(os.path.join(staging, rel), os.path.join(target, rel))]
        if bad:
            raise RuntimeError(f"copied files differ from the update: {', '.join(bad[:3])}")
    except Exception as exc:
        rollback(target, backup, old_files, new_files)
        raise RuntimeError(f"the update could not be applied and was rolled back ({exc})") from exc
    new_requirements = os.path.join(target, "requirements.txt")
    if os.path.isfile(new_requirements) and \
            _requirement_lines(old_requirements) != _requirement_lines(new_requirements):
        _write_text(os.path.join(target, NOTICE_FILE),
                    "This update changed requirements.txt. If MSR reports a missing module, run:\n"
                    "  pip install -r requirements.txt\n"
                    f"in {target}")


def rollback(target, backup, old_files, new_files):
    """Put the backed-up program files back and remove files only the update brought."""
    for rel in set(new_files) - set(old_files):
        try:
            os.remove(os.path.join(target, rel))
        except OSError:
            pass
    shutil.rmtree(os.path.join(target, "msr", "__pycache__"), ignore_errors=True)
    _copy_files(backup, target, old_files)


def _write_text(path, text):
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
    except OSError:
        pass


def relaunch(python_exe, launcher, cwd, args=()):
    kwargs = {"cwd": cwd, "close_fds": True}
    if os.name == "nt":
        flags = 0
        flags |= getattr(subprocess, "DETACHED_PROCESS", 0)
        flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
        kwargs["creationflags"] = flags
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen([python_exe, launcher, *args], **kwargs)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Apply a staged MSR update")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--staging", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--launcher", required=True)
    parser.add_argument("--arg", action="append", default=[], help="passed to the relaunched MSR")
    args = parser.parse_args(argv)
    err_path = os.path.join(args.target, ERROR_FILE)
    try:
        if os.path.isfile(err_path):
            os.remove(err_path)
        if not wait_for_pid(args.pid):
            raise RuntimeError(f"MSR PID {args.pid} is still running")
        time.sleep(0.5)
        apply_staged_update(args.staging, args.target)
        relaunch(args.python, args.launcher, args.target, args.arg)
    except Exception as exc:
        _write_text(err_path, str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
