"""Disk / NIC meter: rates, formatters, busy color, file-growth delta."""
import os
import sys
import unittest
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msr.iometer import (
    BUSY_HOT,
    BUSY_WARN,
    POSIX_DISK_WINDOW_S,
    IoSampler,
    IoSnapshot,
    delta_rate,
    disk_key_candidates,
    displayed_posix_disk_bps,
    format_header,
    format_mb_s,
    format_mbps,
    format_tooltip,
    meter_severity,
    msr_write_bps,
    posix_disk_counter_key,
    posix_disk_window,
    stream_mbps_from_detail,
    sum_stream_mbps,
    volume_label,
    windows_physical_drive_key,
)


# Measured on an ntfs3 volume while three streams were recording: write_bytes
# stayed put, then the page cache flushed about every 15 s. Mean over 45 s
# was 1.8 MB/s. Each flush also looks ~90% busy if you only look at that second.
_WRITEBACK_BURSTS = {15: 21_000_000, 30: 10_700_000, 45: 49_200_000}
_WRITEBACK_BUSY_MS = 900.0


def _writeback_rows(seconds=45):
    write = 0
    busy = 0.0
    rows = []
    for t in range(0, seconds + 1):
        if t in _WRITEBACK_BURSTS:
            write += _WRITEBACK_BURSTS[t]
            busy += _WRITEBACK_BUSY_MS
        rows.append((float(t), write, busy))
    return rows


def _disk_header(snap):
    """The disk half of the header, without the NIC figure."""
    return format_header(snap).split("\u2193")[0]


def _replay(os_name, rows, size_at=None):
    """Run IoSampler over synthetic ``(t, write_bytes, busy_ms)`` rows."""
    ticks = [row[0] for row in rows]
    disks = [("sdb", row[1], row[2]) for row in rows]
    with mock.patch("msr.iometer.os.name", os_name), \
            mock.patch("msr.iometer._monotonic", side_effect=_clock(*ticks)), \
            mock.patch("msr.iometer.read_disk_write", side_effect=disks), \
            mock.patch("msr.iometer.read_net_recv_bytes", side_effect=[0] * len(rows)):
        sampler = IoSampler("/data/streams")
        snaps = []
        for i, row in enumerate(rows):
            sizes = None if size_at is None else size_at(i, row[0])
            snaps.append(sampler.sample(msr_sizes=sizes))
        return snaps


def _clock(*ticks):
    """Fake monotonic: yields ticks, then repeats the last one (Python 3.12 calls it more often)."""
    it = iter(ticks)
    last = [ticks[0]]
    def now():
        last[0] = next(it, last[0])
        return last[0]
    return now

class FormatTests(unittest.TestCase):
    def test_mb_s_buckets(self):
        self.assertEqual(format_mb_s(0), "0 MB/s")
        self.assertEqual(format_mb_s(40_000), "0 MB/s")
        self.assertEqual(format_mb_s(3_200_000), "3.2 MB/s")
        self.assertEqual(format_mb_s(18_200_000), "18.2 MB/s")
        self.assertEqual(format_mb_s(180_000_000), "180 MB/s")

    def test_mbps_from_bytes(self):
        # 12_500_000 B/s * 8 = 100 Mbps
        self.assertEqual(format_mbps(0), "0 Mbps")
        self.assertEqual(format_mbps(12_500_000), "100 Mbps")
        self.assertEqual(format_mbps(400_000), "3.2 Mbps")

    def test_header_not_ready_em_dashes(self):
        snap = IoSnapshot(
            ready=False, disk_label="E:", disk_write_bps=None,
            disk_busy_frac=None, net_recv_bps=None,
            msr_write_bps=None, msr_stream_mbps=None,
        )
        text = format_header(snap)
        self.assertIn("Disk", text)
        self.assertIn("—", text)
        self.assertIn("↓", text)

    def test_header_zero_is_not_dash(self):
        snap = IoSnapshot(
            ready=True, disk_label="E:", disk_write_bps=0.0,
            disk_busy_frac=0.0, net_recv_bps=0.0,
            msr_write_bps=0.0, msr_stream_mbps=0.0,
        )
        text = format_header(snap)
        self.assertIn("0 MB/s", text)
        self.assertIn("0 Mbps", text)
        self.assertNotIn("—", text)

    def test_tooltip_lists_msr_totals(self):
        snap = IoSnapshot(
            ready=True, disk_label="E:", disk_write_bps=18_200_000,
            disk_busy_frac=0.72, net_recv_bps=12_500_000,
            msr_write_bps=12_100_000, msr_stream_mbps=97.0,
        )
        tip = format_tooltip(snap)
        self.assertIn("E:", tip)
        self.assertIn("72% busy", tip)
        self.assertIn("This app", tip)
        self.assertIn("whole disk", tip)
        self.assertIn("97.0 Mbps", tip)
        self.assertIn("12.1 MB/s", tip)


class DeltaRateTests(unittest.TestCase):
    def test_bytes_per_sec(self):
        self.assertAlmostEqual(delta_rate(1000, 3000, 2.0), 1000.0)

    def test_rejects_first_sample(self):
        self.assertIsNone(delta_rate(None, 1000, 1.0))

    def test_rejects_sleep_gap(self):
        self.assertIsNone(delta_rate(1000, 5000, 9.0))

    def test_rejects_backwards_counter(self):
        self.assertIsNone(delta_rate(5000, 1000, 1.0))

    def test_rejects_zero_dt(self):
        self.assertIsNone(delta_rate(1000, 2000, 0.0))


class MsrWriteTests(unittest.TestCase):
    def test_growth_of_known_files(self):
        prev = {"a.ts": 1000, "b.ts": 5000}
        curr = {"a.ts": 3000, "b.ts": 5000}
        self.assertAlmostEqual(msr_write_bps(prev, curr, 2.0), 1000.0)

    def test_split_does_not_go_negative(self):
        prev = {"a.ts": 8_000_000_000}
        curr = {"a.ts": 1000}  # new segment reused name, or shrink
        self.assertAlmostEqual(msr_write_bps(prev, curr, 1.0), 0.0)

    def test_new_file_ignored_until_second_sample(self):
        prev = {"a.ts": 1000}
        curr = {"a.ts": 1000, "b.ts": 50_000}
        self.assertAlmostEqual(msr_write_bps(prev, curr, 1.0), 0.0)

    def test_empty_is_zero_not_none(self):
        self.assertEqual(msr_write_bps({}, {}, 1.0), 0.0)


class StreamMbpsTests(unittest.TestCase):
    def test_status_detail_no_space(self):
        self.assertAlmostEqual(
            stream_mbps_from_detail("1920x1080 · 30fps · 3.2Mbps"), 3.2,
        )

    def test_status_detail_with_space(self):
        self.assertAlmostEqual(
            stream_mbps_from_detail("1920x1080 · 60fps · 7.1 Mbps"), 7.1,
        )

    def test_missing(self):
        self.assertIsNone(stream_mbps_from_detail("starting"))
        self.assertIsNone(stream_mbps_from_detail(""))
        self.assertIsNone(stream_mbps_from_detail(None))

    def test_sum_recording_rows(self):
        total = sum_stream_mbps([
            "1920x1080 · 30fps · 3.2Mbps",
            "starting",
            "1920x1080 · 60fps · 7.1Mbps",
        ])
        self.assertAlmostEqual(total, 10.3)


class SeverityTests(unittest.TestCase):
    def test_thresholds(self):
        self.assertEqual(meter_severity(None), "ok")
        self.assertEqual(meter_severity(0.0), "ok")
        self.assertEqual(meter_severity(BUSY_WARN - 0.01), "ok")
        self.assertEqual(meter_severity(BUSY_WARN), "warn")
        self.assertEqual(meter_severity(BUSY_HOT), "hot")


class DiskKeyTests(unittest.TestCase):
    def test_windows_physical_drive_key(self):
        self.assertEqual(windows_physical_drive_key(1), "PhysicalDrive1")

    def test_sda_partition_prefers_parent_disk(self):
        self.assertEqual(disk_key_candidates("sda1"), ["sda", "sda1"])

    def test_nvme_partition(self):
        self.assertEqual(
            disk_key_candidates("nvme0n1p2"), ["nvme0n1", "nvme0n1p2"],
        )

    def test_posix_longest_mountpoint(self):
        parts = [
            SimpleNamespace(device="/dev/sda1", mountpoint="/"),
            SimpleNamespace(device="/dev/sdb1", mountpoint="/data"),
        ]
        keys = ["sda", "sda1", "sdb", "sdb1"]
        self.assertEqual(
            posix_disk_counter_key("/data/streams/foo.ts", parts, keys),
            "sdb",
        )
        self.assertEqual(
            posix_disk_counter_key("/home/user", parts, keys),
            "sda",
        )

    def test_volume_label_windows_drive(self):
        if os.name != "nt":
            self.skipTest("Windows drive letter")
        self.assertEqual(volume_label(r"E:\Streams"), "E:")


class SamplerTests(unittest.TestCase):
    @mock.patch("msr.iometer.read_net_recv_bytes")
    @mock.patch("msr.iometer.read_disk_write")
    @mock.patch("msr.iometer._monotonic")
    def test_second_sample_yields_rates(self, mono, disk, net):
        mono.side_effect = _clock(1.0, 2.0)
        disk.side_effect = [
            ("E:", 1_000_000, 10.0),
            ("E:", 3_000_000, 110.0),
        ]
        net.side_effect = [1_000_000, 2_000_000]
        sampler = IoSampler(r"E:\Streams")
        first = sampler.sample()
        self.assertFalse(first.ready)
        second = sampler.sample(
            msr_sizes={"a.ts": 5_000_000},
            msr_stream_mbps=10.3,
        )
        # First MSR sizes have no previous pair — write bps 0 after dt valid
        # with empty prev: msr_write_bps(None, {...}, 1) -> 0.0 (no known files)
        self.assertTrue(second.ready)
        self.assertAlmostEqual(second.disk_write_bps, 2_000_000.0)
        self.assertAlmostEqual(second.net_recv_bps, 1_000_000.0)
        self.assertAlmostEqual(second.disk_busy_frac, 0.1)
        self.assertAlmostEqual(second.msr_stream_mbps, 10.3)

    @mock.patch("msr.iometer.read_net_recv_bytes")
    @mock.patch("msr.iometer.read_disk_write")
    @mock.patch("msr.iometer._monotonic")
    def test_sleep_gap_resets(self, mono, disk, net):
        mono.side_effect = _clock(1.0, 10.0)
        disk.side_effect = [
            ("E:", 1_000_000, 0.0),
            ("E:", 9_000_000, 0.0),
        ]
        net.side_effect = [1_000_000, 9_000_000]
        sampler = IoSampler(r"E:\Streams")
        sampler.sample()
        after_sleep = sampler.sample()
        self.assertFalse(after_sleep.ready)
        self.assertIsNone(after_sleep.disk_write_bps)
        self.assertIsNone(after_sleep.net_recv_bps)

    @mock.patch("msr.iometer.read_net_recv_bytes")
    @mock.patch("msr.iometer.read_disk_write")
    @mock.patch("msr.iometer._monotonic")
    def test_msr_growth_on_third_tick(self, mono, disk, net):
        mono.side_effect = _clock(1.0, 2.0, 3.0)
        disk.side_effect = [
            ("E:", 0, 0.0),
            ("E:", 0, 0.0),
            ("E:", 0, 0.0),
        ]
        net.side_effect = [0, 0, 0]
        sampler = IoSampler(r"E:\Streams")
        sampler.sample(msr_sizes={"a.ts": 1_000_000})
        sampler.sample(msr_sizes={"a.ts": 1_000_000})
        third = sampler.sample(msr_sizes={"a.ts": 3_000_000})
        # Growth is spread over time since the size last changed (t=1 → t=3).
        self.assertAlmostEqual(third.msr_write_bps, 1_000_000.0)

    @mock.patch("msr.iometer.read_net_recv_bytes")
    @mock.patch("msr.iometer.read_disk_write")
    @mock.patch("msr.iometer._monotonic")
    def test_sparse_worker_size_bytes_not_a_one_second_spike(self, mono, disk, net):
        # Worker status only moves every 5s; GUI samples every 1s.
        mono.side_effect = _clock(0.0, 1.0, 2.0, 3.0, 4.0, 5.0)
        disk.side_effect = [("E:", 0, 0.0)] * 6
        net.side_effect = [0] * 6
        sampler = IoSampler(r"E:\Streams")
        sampler.sample(msr_sizes={"ch": 1_000_000})
        for _ in range(4):
            sampler.sample(msr_sizes={"ch": 1_000_000})
        last = sampler.sample(msr_sizes={"ch": 6_000_000})
        self.assertAlmostEqual(last.msr_write_bps, 1_000_000.0)


_WINDOWS_TOOLTIP = (
    "E:  18.2 MB/s write  ·  72% busy\n"
    "NIC  100 Mbps down\n"
    "(whole disk / whole NIC)\n"
    "\n"
    "This app\n"
    "  recordings  12.1 MB/s\n"
    "  streams     97.0 Mbps"
)


class WritebackWindowTests(unittest.TestCase):
    """Linux page-cache flushes are bursts. The header averages them.

    The same counter series on Windows stays a one-second delta.
    """

    def test_window_constant_covers_a_writeback_interval(self):
        # dirty_expire_centisecs is often 30 s and writeback is often 15 s.
        self.assertGreaterEqual(POSIX_DISK_WINDOW_S, 45.0)
        self.assertLessEqual(POSIX_DISK_WINDOW_S, 60.0)

    def test_bursty_counters_average_oldest_to_newest(self):
        rows = _writeback_rows(45)
        rate, busy = posix_disk_window(rows)
        total = sum(_WRITEBACK_BURSTS.values())
        self.assertAlmostEqual(rate, total / 45.0)
        self.assertAlmostEqual(rate, 1_797_777.777, delta=1.0)
        # Three 900 ms flushes spread over 45 s, not a hot one-second spike.
        self.assertAlmostEqual(busy, (3 * _WRITEBACK_BUSY_MS) / 1000.0 / 45.0)
        self.assertEqual(meter_severity(busy), "ok")
        self.assertEqual(format_mb_s(rate), "1.8 MB/s")

    def test_quiet_second_inside_the_window_is_not_zero(self):
        # t=16 is the second after the 21 MB flush. A one-second delta is 0.
        rows = _writeback_rows(16)
        rate, busy = posix_disk_window(rows)
        self.assertAlmostEqual(rate, 21_000_000 / 16.0)
        self.assertGreater(rate, 50_000)  # format_mb_s would not round this to 0
        self.assertAlmostEqual(busy, _WRITEBACK_BUSY_MS / 1000.0 / 16.0)
        self.assertEqual(meter_severity(busy), "ok")

    def test_flush_after_quiet_history_does_not_warn(self):
        rows = _writeback_rows(15)
        _rate, busy = posix_disk_window(rows)
        self.assertAlmostEqual(busy, 0.9 / 15.0)
        self.assertLess(busy, BUSY_WARN)
        self.assertEqual(meter_severity(busy), "ok")

    def test_sustained_busy_still_warns(self):
        rows = [(float(t), t * 1_000_000, t * _WRITEBACK_BUSY_MS) for t in range(0, 11)]
        _rate, busy = posix_disk_window(rows)
        self.assertAlmostEqual(busy, 0.9)
        self.assertEqual(meter_severity(busy), "hot")

    def test_sleep_gap_drops_samples_before_it(self):
        rows = [
            (0.0, 0, 0.0),
            (1.0, 0, 0.0),
            (10.0, 5_000_000, 0.0),  # 9 s gap
            (11.0, 6_000_000, 0.0),
        ]
        rate, _busy = posix_disk_window(rows)
        self.assertAlmostEqual(rate, 1_000_000.0)

    def test_backwards_counter_restarts_the_window(self):
        rows = [(0.0, 100.0, 0.0), (1.0, 50.0, 0.0), (2.0, 80.0, 0.0)]
        rate, _busy = posix_disk_window(rows)
        self.assertAlmostEqual(rate, 30.0)
        self.assertIsNone(posix_disk_window([(0.0, 100.0, 0.0), (1.0, 50.0, 0.0)])[0])

    def test_sample_older_than_the_window_is_the_baseline_only(self):
        # The burst at t=1 is outside a 10 s window ending at t=21, so it is
        # not part of the delta. The bytes after the oldest kept sample are.
        rows = [
            (0.0, 0, 0.0),
            (1.0, 100_000_000, 0.0),
            (20.0, 100_000_000, 0.0),
            (21.0, 121_000_000, 0.0),
        ]
        rate, _busy = posix_disk_window(rows, window_s=10, max_gap=30)
        self.assertAlmostEqual(rate, 21_000_000.0)

    def test_bursts_that_aged_out_of_the_window_read_as_zero(self):
        rows = _writeback_rows(45)
        write, busy = rows[-1][1], rows[-1][2]
        for t in range(46, 91):
            rows.append((float(t), write, busy))
        rate, busy_frac = posix_disk_window(rows)
        self.assertAlmostEqual(rate, 0.0)
        self.assertAlmostEqual(busy_frac, 0.0)

    def test_header_floor_uses_the_larger_of_device_and_file_growth(self):
        self.assertIsNone(displayed_posix_disk_bps(None, 1_800_000))
        self.assertEqual(displayed_posix_disk_bps(0.0, None), 0.0)
        self.assertEqual(displayed_posix_disk_bps(0.0, 0.0), 0.0)
        self.assertEqual(displayed_posix_disk_bps(0.0, 1_800_000), 1_800_000)
        self.assertEqual(displayed_posix_disk_bps(5_000_000, 1_800_000), 5_000_000)

    def test_posix_sampler_shows_the_window_not_the_flush(self):
        snaps = _replay("posix", _writeback_rows(45))
        self.assertIsNone(snaps[0].disk_write_bps)
        self.assertFalse(snaps[0].ready)
        # Quiet second after the first flush: still the running average.
        self.assertAlmostEqual(snaps[16].disk_write_bps, 21_000_000 / 16.0)
        self.assertNotIn("0 MB/s", _disk_header(snaps[16]))
        self.assertEqual(meter_severity(snaps[15].disk_busy_frac), "ok")
        total = sum(_WRITEBACK_BURSTS.values())
        self.assertAlmostEqual(snaps[45].disk_write_bps, total / 45.0)
        self.assertIn("1.8 MB/s", _disk_header(snaps[45]))
        self.assertEqual(meter_severity(snaps[45].disk_busy_frac), "ok")

    def test_posix_header_stays_up_while_files_grow_and_the_disk_is_quiet(self):
        # No flush yet: the device counter is flat, the recording is not.
        rows = [(float(t), 1_000, 0.0) for t in range(0, 6)]

        def growing(_i, t):
            return {"a.ts": 1_000_000 + int(t) * 1_800_000}

        snaps = _replay("posix", rows, size_at=growing)
        self.assertAlmostEqual(snaps[1].msr_write_bps, 1_800_000.0)
        self.assertAlmostEqual(snaps[1].disk_write_bps, 1_800_000.0)
        self.assertIn("1.8 MB/s", _disk_header(snaps[1]))
        self.assertNotIn("0 MB/s", _disk_header(snaps[1]))
        # A busier disk still wins over the file-growth floor.
        fast = [(float(t), t * 5_000_000, 0.0) for t in range(0, 6)]

        def slower(_i, t):
            return {"a.ts": 1_000_000 + int(t) * 1_000_000}

        fast_snaps = _replay("posix", fast, size_at=slower)
        self.assertAlmostEqual(fast_snaps[5].disk_write_bps, 5_000_000.0)
        self.assertAlmostEqual(fast_snaps[5].msr_write_bps, 1_000_000.0)

    def test_windows_path_is_the_one_second_delta(self):
        snaps = _replay("nt", _writeback_rows(45))
        self.assertIsNone(snaps[0].disk_write_bps)
        self.assertFalse(snaps[0].ready)
        # The flush second is the whole burst, and the next second is 0.
        self.assertAlmostEqual(snaps[15].disk_write_bps, 21_000_000.0)
        self.assertAlmostEqual(snaps[15].disk_busy_frac, 0.9)
        self.assertEqual(meter_severity(snaps[15].disk_busy_frac), "hot")
        self.assertIn("21.0 MB/s", _disk_header(snaps[15]))
        self.assertAlmostEqual(snaps[16].disk_write_bps, 0.0)
        self.assertAlmostEqual(snaps[16].disk_busy_frac, 0.0)
        self.assertIn("0 MB/s", _disk_header(snaps[16]))
        self.assertEqual(meter_severity(snaps[16].disk_busy_frac), "ok")
        self.assertAlmostEqual(snaps[30].disk_write_bps, 10_700_000.0)
        self.assertAlmostEqual(snaps[44].disk_write_bps, 0.0)
        self.assertAlmostEqual(snaps[45].disk_write_bps, 49_200_000.0)
        self.assertIn("49.2 MB/s", _disk_header(snaps[45]))
        # File growth does not lift a quiet Windows sample.
        rows = [(float(t), 1_000, 0.0) for t in range(0, 4)]

        def growing(_i, t):
            return {"a.ts": 1_000_000 + int(t) * 1_800_000}

        quiet = _replay("nt", rows, size_at=growing)
        self.assertAlmostEqual(quiet[2].msr_write_bps, 1_800_000.0)
        self.assertAlmostEqual(quiet[2].disk_write_bps, 0.0)
        self.assertIn("0 MB/s", _disk_header(quiet[2]))

    def test_windows_tooltip_text_is_unchanged(self):
        snap = IoSnapshot(
            ready=True, disk_label="E:", disk_write_bps=18_200_000,
            disk_busy_frac=0.72, net_recv_bps=12_500_000,
            msr_write_bps=12_100_000, msr_stream_mbps=97.0,
        )
        with mock.patch("msr.iometer.os.name", "nt"):
            tip = format_tooltip(snap)
        self.assertEqual(tip, _WINDOWS_TOOLTIP)
        self.assertNotIn("average", tip.lower())

    def test_posix_tooltip_says_the_disk_rate_is_averaged(self):
        snap = IoSnapshot(
            ready=True, disk_label="E:", disk_write_bps=18_200_000,
            disk_busy_frac=0.72, net_recv_bps=12_500_000,
            msr_write_bps=12_100_000, msr_stream_mbps=97.0,
        )
        with mock.patch("msr.iometer.os.name", "posix"):
            tip = format_tooltip(snap)
        self.assertIn(
            f"Linux: disk rate is a {int(POSIX_DISK_WINDOW_S)} s average "
            "(page-cache write-back).",
            tip,
        )
        self.assertIn("(whole disk / whole NIC)", tip)
        self.assertIn("This app", tip)
        self.assertIn("12.1 MB/s", tip)


class PartitionCacheTests(unittest.TestCase):
    def test_cached_partitions_avoids_repeated_psutil_calls(self):
        from msr import iometer as im
        fake_psutil = mock.Mock()
        fake_psutil.disk_partitions.return_value = []
        im._partitions_cache.clear()
        with mock.patch.object(im, "HAS_PSUTIL", True), mock.patch.object(im, "psutil", fake_psutil):
            for _ in range(5):
                im._cached_partitions(False)
                im._cached_partitions(True)
        self.assertEqual(fake_psutil.disk_partitions.call_count, 2)
        im._partitions_cache.clear()


if __name__ == "__main__":
    unittest.main()
