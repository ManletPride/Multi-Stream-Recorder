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
    IoSampler,
    IoSnapshot,
    delta_rate,
    disk_key_candidates,
    format_header,
    format_mb_s,
    format_mbps,
    format_tooltip,
    meter_severity,
    msr_write_bps,
    posix_disk_counter_key,
    stream_mbps_from_detail,
    sum_stream_mbps,
    volume_label,
    windows_physical_drive_key,
)


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
    @mock.patch("msr.iometer.time.monotonic")
    def test_second_sample_yields_rates(self, mono, disk, net):
        mono.side_effect = [1.0, 2.0]
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
    @mock.patch("msr.iometer.time.monotonic")
    def test_sleep_gap_resets(self, mono, disk, net):
        mono.side_effect = [1.0, 10.0]
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
    @mock.patch("msr.iometer.time.monotonic")
    def test_msr_growth_on_third_tick(self, mono, disk, net):
        mono.side_effect = [1.0, 2.0, 3.0]
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
    @mock.patch("msr.iometer.time.monotonic")
    def test_sparse_worker_size_bytes_not_a_one_second_spike(self, mono, disk, net):
        # Worker status only moves every 5s; GUI samples every 1s.
        mono.side_effect = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        disk.side_effect = [("E:", 0, 0.0)] * 6
        net.side_effect = [0] * 6
        sampler = IoSampler(r"E:\Streams")
        sampler.sample(msr_sizes={"ch": 1_000_000})
        for _ in range(4):
            sampler.sample(msr_sizes={"ch": 1_000_000})
        last = sampler.sample(msr_sizes={"ch": 6_000_000})
        self.assertAlmostEqual(last.msr_write_bps, 1_000_000.0)


if __name__ == "__main__":
    unittest.main()
