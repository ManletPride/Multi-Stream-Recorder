"""Whole-disk write and NIC download rates for the Status header meters.

Uses ``psutil`` counters (the same optional dependency as process cleanup).
The GUI shows machine-wide rates; MSR-only totals belong in the tooltip.
"""
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass

from msr.deps import HAS_PSUTIL, psutil

# Ignore a sample if the previous one is this old (sleep / debugger pause).
MAX_SAMPLE_GAP_S = 5.0

# Write-busy fraction of wall time. Windows write_time is milliseconds spent
# in write I/O; Linux busy_time (when present) is preferred over write_time.
BUSY_WARN = 0.60
BUSY_HOT = 0.85

_MBPS_RE = re.compile(r"([\d.]+)\s*Mbps", re.I)
_POSIX_NVME_PART = re.compile(r"^(nvme\d+n\d+)p\d+$")
_POSIX_SD_PART = re.compile(r"^([a-z]+)(\d+)$")

# Drive letter -> PhysicalDriveN, so CreateFile is not repeated every tick.
_win_drive_key_cache: dict[str, str] = {}


@dataclass(frozen=True)
class IoSnapshot:
    ready: bool
    disk_label: str
    disk_write_bps: float | None
    disk_busy_frac: float | None
    net_recv_bps: float | None  # bytes/sec
    msr_write_bps: float | None
    msr_stream_mbps: float | None


def volume_label(path: str, partitions=None) -> str:
    """Short name for the volume that holds *path* (``E:`` or a mountpoint)."""
    if not path:
        return "Disk"
    if os.name == "nt":
        drive = os.path.splitdrive(os.path.abspath(path))[0]
        return (drive or "?").rstrip("\\") or "?"
    try:
        abspath = os.path.abspath(path)
    except OSError:
        return path
    best_mp = None
    best_len = -1
    for part in partitions if partitions is not None else _safe_partitions():
        mp = getattr(part, "mountpoint", "") or ""
        if not mp:
            continue
        if abspath == mp or abspath.startswith(mp.rstrip("/") + "/"):
            if len(mp) > best_len:
                best_mp = mp
                best_len = len(mp)
    if best_mp and best_mp != "/":
        return best_mp
    if best_mp == "/":
        return "/"
    return abspath


def disk_key_candidates(device_basename: str) -> list[str]:
    """psutil per-disk keys to try for a partition device name (``sda1``)."""
    if not device_basename:
        return []
    names = [device_basename]
    nvme = _POSIX_NVME_PART.match(device_basename)
    if nvme:
        names.insert(0, nvme.group(1))
    else:
        sd = _POSIX_SD_PART.match(device_basename)
        if sd:
            names.insert(0, sd.group(1))
    return names


def _posix_path(path: str) -> str:
    """Normalize *path* with forward slashes (host os.path.abspath is Windows-wrong)."""
    p = (path or "").replace("\\", "/")
    if os.name != "nt":
        try:
            p = os.path.abspath(path).replace("\\", "/")
        except OSError:
            pass
    return p


def posix_disk_counter_key(path: str, partitions, counter_keys) -> str | None:
    """Match *path* to a ``disk_io_counters(perdisk=True)`` key on POSIX."""
    abspath = _posix_path(path)
    if not abspath:
        return None
    best = None
    best_len = -1
    for part in partitions:
        mp = (getattr(part, "mountpoint", "") or "").replace("\\", "/")
        if not mp:
            continue
        if abspath == mp or abspath.startswith(mp.rstrip("/") + "/"):
            if len(mp) > best_len:
                best = part
                best_len = len(mp)
    if best is None:
        return None
    dev = os.path.basename((getattr(best, "device", "") or "").replace("\\", "/"))
    keys = set(counter_keys)
    for cand in disk_key_candidates(dev):
        if cand in keys:
            return cand
    return None


def windows_physical_drive_key(device_number: int) -> str:
    return f"PhysicalDrive{int(device_number)}"


def _windows_physical_drive_for_letter(letter: str) -> str | None:
    """Map ``E:`` to ``PhysicalDriveN`` via ``IOCTL_STORAGE_GET_DEVICE_NUMBER``."""
    letter = (letter or "").strip().rstrip("\\/:").upper()
    if len(letter) != 1 or not letter.isalpha():
        return None
    if letter in _win_drive_key_cache:
        return _win_drive_key_cache[letter]
    try:
        import ctypes
        import ctypes.wintypes
    except Exception:
        return None

    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    OPEN_EXISTING = 3
    IOCTL_STORAGE_GET_DEVICE_NUMBER = 0x2D1080

    class STORAGE_DEVICE_NUMBER(ctypes.Structure):
        _fields_ = [
            ("DeviceType", ctypes.wintypes.DWORD),
            ("DeviceNumber", ctypes.wintypes.DWORD),
            ("PartitionNumber", ctypes.wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    path = "\\\\.\\{}:".format(letter)
    handle = kernel32.CreateFileW(
        path, 0, FILE_SHARE_READ | FILE_SHARE_WRITE, None, OPEN_EXISTING, 0, None
    )
    invalid = {
        0,
        -1,
        0xFFFFFFFF,
        0xFFFFFFFFFFFFFFFF,
        int(ctypes.c_void_p(-1).value or 0),
    }
    try:
        if handle is None or int(handle) in invalid:
            return None
        sdn = STORAGE_DEVICE_NUMBER()
        returned = ctypes.wintypes.DWORD()
        ok = kernel32.DeviceIoControl(
            ctypes.wintypes.HANDLE(handle),
            IOCTL_STORAGE_GET_DEVICE_NUMBER,
            None,
            0,
            ctypes.byref(sdn),
            ctypes.sizeof(sdn),
            ctypes.byref(returned),
            None,
        )
        if not ok:
            return None
        key = windows_physical_drive_key(sdn.DeviceNumber)
        _win_drive_key_cache[letter] = key
        return key
    except Exception:
        return None
    finally:
        try:
            kernel32.CloseHandle(ctypes.wintypes.HANDLE(handle))
        except Exception:
            pass


def disk_counter_key(path: str, counters=None) -> str | None:
    """``perdisk=True`` key for the disk that holds *path*, or None."""
    if not path or not HAS_PSUTIL or psutil is None:
        return None
    if counters is None:
        try:
            counters = psutil.disk_io_counters(perdisk=True) or {}
        except Exception:
            return None
    if os.name == "nt":
        drive = os.path.splitdrive(os.path.abspath(path))[0]
        letter = (drive or "").rstrip("\\")
        key = _windows_physical_drive_for_letter(letter)
        if key and key in counters:
            return key
        return None
    try:
        parts = psutil.disk_partitions(all=True)
    except Exception:
        parts = []
    return posix_disk_counter_key(path, parts, counters.keys())


def _safe_partitions():
    if not HAS_PSUTIL or psutil is None:
        return []
    try:
        return psutil.disk_partitions(all=False)
    except Exception:
        return []


def _counter_write(io) -> tuple[int, float | None]:
    """Return (write_bytes, write_busy_ms_or_none) from a psutil sdiskio."""
    write_bytes = int(getattr(io, "write_bytes", 0) or 0)
    busy_ms = None
    busy_time = getattr(io, "busy_time", None)
    if busy_time is not None:
        busy_ms = float(busy_time)
    else:
        write_time = getattr(io, "write_time", None)
        if write_time is not None:
            busy_ms = float(write_time)
    return write_bytes, busy_ms


def read_disk_write(path: str) -> tuple[str, int, float | None] | None:
    """``(label, write_bytes, busy_ms)`` for the volume under *path*.

    Falls back to the machine-wide counter (label ``All disks``) when the
    specific drive cannot be resolved.
    """
    if not HAS_PSUTIL or psutil is None:
        return None
    label = volume_label(path)
    try:
        perdisk = psutil.disk_io_counters(perdisk=True) or {}
        key = disk_counter_key(path, counters=perdisk)
        if key and key in perdisk:
            write_bytes, busy_ms = _counter_write(perdisk[key])
            return label, write_bytes, busy_ms
        total = psutil.disk_io_counters()
        if total is None:
            return None
        write_bytes, busy_ms = _counter_write(total)
        return "All disks", write_bytes, busy_ms
    except Exception:
        return None


def read_net_recv_bytes() -> int | None:
    if not HAS_PSUTIL or psutil is None:
        return None
    try:
        io = psutil.net_io_counters()
        if io is None:
            return None
        return int(getattr(io, "bytes_recv", 0) or 0)
    except Exception:
        return None


def delta_rate(prev, curr, dt: float, max_gap: float = MAX_SAMPLE_GAP_S):
    """Bytes/sec (or the same unit as the counters) between two samples.

    Returns None when the pair is not usable (first sample, sleep gap,
    or a counter that went backwards).
    """
    if prev is None or curr is None:
        return None
    if dt <= 0 or dt > max_gap:
        return None
    try:
        prev_f = float(prev)
        curr_f = float(curr)
    except (TypeError, ValueError):
        return None
    if curr_f < prev_f:
        return None
    return (curr_f - prev_f) / dt


def msr_write_bps(prev_sizes: dict, curr_sizes: dict, dt: float) -> float | None:
    """Recording-file growth rate. New or split files do not count as negative."""
    if dt <= 0 or dt > MAX_SAMPLE_GAP_S:
        return None
    if not curr_sizes and not prev_sizes:
        return 0.0
    delta = 0
    for path, size in (curr_sizes or {}).items():
        prev = (prev_sizes or {}).get(path)
        if prev is None:
            continue
        if size >= prev:
            delta += size - prev
    return delta / dt


def stream_mbps_from_detail(detail: str):
    """Parse ``3.2Mbps`` out of a status-row detail string."""
    if not detail:
        return None
    m = _MBPS_RE.search(detail)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def sum_stream_mbps(details) -> float:
    total = 0.0
    for detail in details or ():
        v = stream_mbps_from_detail(detail)
        if v is not None:
            total += v
    return total


def meter_severity(busy_frac) -> str:
    """``ok``, ``warn``, or ``hot`` from a 0–1 write-busy fraction."""
    if busy_frac is None:
        return "ok"
    if busy_frac >= BUSY_HOT:
        return "hot"
    if busy_frac >= BUSY_WARN:
        return "warn"
    return "ok"


def format_mb_s(bps: float) -> str:
    mb = max(0.0, float(bps)) / 1_000_000.0
    if mb < 0.05:
        return "0 MB/s"
    if mb < 100:
        return f"{mb:.1f} MB/s"
    return f"{mb:.0f} MB/s"


def format_mbps(bytes_per_sec: float) -> str:
    mbps = max(0.0, float(bytes_per_sec)) * 8.0 / 1_000_000.0
    if mbps < 0.05:
        return "0 Mbps"
    if mbps < 100:
        return f"{mbps:.1f} Mbps"
    return f"{mbps:.0f} Mbps"


def format_header(snap: IoSnapshot) -> str:
    disk = format_mb_s(snap.disk_write_bps) if snap.disk_write_bps is not None else "—"
    net = format_mbps(snap.net_recv_bps) if snap.net_recv_bps is not None else "—"
    return f"Disk {disk:<9}   ↓ {net:>9}"


def format_tooltip(snap: IoSnapshot) -> str:
    if not snap.ready:
        return "Sampling disk and network…"
    disk_s = format_mb_s(snap.disk_write_bps or 0.0)
    net_s = format_mbps(snap.net_recv_bps or 0.0)
    lines = [f"{snap.disk_label}  {disk_s} write"]
    if snap.disk_busy_frac is not None:
        lines[0] += f"  ·  {snap.disk_busy_frac * 100:.0f}% busy"
    lines.append(f"NIC  {net_s} down")
    lines.append("(whole disk / whole NIC)")
    lines.append("")
    lines.append("This app")
    msr_w = format_mb_s(snap.msr_write_bps or 0.0)
    msr_n = snap.msr_stream_mbps
    if msr_n is None:
        msr_n_s = "—"
    elif msr_n < 0.05:
        msr_n_s = "0 Mbps"
    elif msr_n < 100:
        msr_n_s = f"{msr_n:.1f} Mbps"
    else:
        msr_n_s = f"{msr_n:.0f} Mbps"
    lines.append(f"  recordings  {msr_w}")
    lines.append(f"  streams     {msr_n_s}")
    return "\n".join(lines)


class IoSampler:
    """Pair consecutive OS / file-size samples into rates."""

    def __init__(self, path: str = ""):
        self.path = path or ""
        self._t = None
        self._write_bytes = None
        self._busy_ms = None
        self._recv_bytes = None
        self._msr_sizes = None
        # key -> (size, monotonic t of last size change) so 5s worker
        # size_bytes updates still yield a correct bytes/sec, not a 1s spike.
        self._msr_meta = {}
        self._msr_last_rate = None
        self._msr_last_change_t = None
        self.last = IoSnapshot(
            ready=False,
            disk_label=volume_label(self.path),
            disk_write_bps=None,
            disk_busy_frac=None,
            net_recv_bps=None,
            msr_write_bps=None,
            msr_stream_mbps=None,
        )

    def sample(self, msr_sizes=None, msr_stream_mbps: float = 0.0) -> IoSnapshot:
        now = time.monotonic()
        disk = read_disk_write(self.path)
        recv = read_net_recv_bytes()
        sizes = dict(msr_sizes or {})

        dt = None if self._t is None else (now - self._t)
        write_bps = None
        busy_frac = None
        recv_bps = None
        msr_bps = None
        label = volume_label(self.path)

        if disk is not None:
            label, write_bytes, busy_ms = disk
            write_bps = delta_rate(self._write_bytes, write_bytes, dt or -1)
            if busy_ms is not None and self._busy_ms is not None and dt and 0 < dt <= MAX_SAMPLE_GAP_S:
                busy_delta_s = (busy_ms - self._busy_ms) / 1000.0
                if busy_delta_s >= 0:
                    busy_frac = min(1.0, busy_delta_s / dt)
            self._write_bytes = write_bytes
            self._busy_ms = busy_ms
        else:
            self._write_bytes = None
            self._busy_ms = None

        recv_bps = delta_rate(self._recv_bytes, recv, dt or -1)
        self._recv_bytes = recv

        msr_bps = self._msr_rate_from_status(sizes, now)
        self._msr_sizes = sizes
        self._t = now

        ready = write_bps is not None or recv_bps is not None
        self.last = IoSnapshot(
            ready=ready,
            disk_label=label,
            disk_write_bps=write_bps,
            disk_busy_frac=busy_frac,
            net_recv_bps=recv_bps,
            msr_write_bps=msr_bps,
            msr_stream_mbps=float(msr_stream_mbps or 0.0),
        )
        return self.last

    def _msr_rate_from_status(self, sizes, now):
        """Bytes/sec from worker ``size_bytes``, which may only move every few seconds."""
        prev_meta = self._msr_meta
        changed_delta = 0
        oldest_t = None
        new_meta = {}
        for key, size in sizes.items():
            try:
                size = int(size)
            except (TypeError, ValueError):
                continue
            prev = prev_meta.get(key)
            if prev is not None:
                psize, pt = prev
                if size > psize:
                    changed_delta += size - psize
                    oldest_t = pt if oldest_t is None else min(oldest_t, pt)
                    new_meta[key] = (size, now)
                else:
                    new_meta[key] = prev
            else:
                new_meta[key] = (size, now)
        self._msr_meta = new_meta

        if changed_delta and oldest_t is not None and now > oldest_t:
            rate = changed_delta / (now - oldest_t)
            self._msr_last_rate = rate
            self._msr_last_change_t = now
            return rate
        if (
            self._msr_last_rate is not None
            and self._msr_last_change_t is not None
            and (now - self._msr_last_change_t) <= (MAX_SAMPLE_GAP_S + 1.0)
        ):
            return self._msr_last_rate
        if prev_meta or sizes:
            return 0.0
        return None
