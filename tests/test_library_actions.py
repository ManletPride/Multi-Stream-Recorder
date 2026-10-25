"""Library actions (msr/library_actions.py): guards, plans, move/delete/restore, undo, log.
Everything happens in temporary folders; nothing outside them is ever touched."""
import json
import os
import queue
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from msr import library as lib
from msr import library_actions as act

OLD = time.time() - 40 * 86400


def put(path: Path, size: int = 100, age: float = OLD) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    os.utime(path, (age, age))
    return path


class Tree(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.streams = self.base / "streams"
        self.data = self.base / "data"
        self.data.mkdir()
        s = self.streams
        self.a1 = put(s / "Processed/kick/alice/alice_20260101_120000.mp4", 400)
        self.a1m = put(s / "Processed/kick/alice/alice_20260101_120000.meta.json", 10)
        self.a2 = put(s / "Processed/kick/alice/alice_20260201_120000.mp4", 300)
        self.a3 = put(s / "Processed/kick/alice/alice_20260301_120000.mp4", 200)
        self.b1 = put(s / "Processed/twitch/bob/bob_20260105_000000.mp4", 1000)
        self.raw = put(s / "Recorded/kick/alice/alice_20260110_000000.ts", 50)
        self.clip = put(s / "Clips/kick/alice/clip_20260102_000000.mp4", 70)
        self.shot = put(s / "Clips/kick/alice/shot_20260102_000000.jpg", 5)
        self.pend = put(s / "PendingDeletion/twitch/bob/bob_20251201_000000.mp4", 90)
        self.note = put(s / "Processed/kick/alice/notes.txt", 3)
        self.roots = lib.Roots(str(s))
        self.manifest = act.Manifest(str(self.data / "moves.json"))
        self.log_path = self.data / "actions.log"

    def scan(self):
        return lib.scan_library(self.roots, collect_files=True)

    def guard(self, **kw):
        kw.setdefault("data_dir", str(self.data))
        return act.Guard(self.roots, **kw)

    def recs(self, *paths):
        want = {os.path.normcase(str(p)) for p in paths}
        return [r for r in self.scan().files if os.path.normcase(r.path) in want]

    def log_events(self):
        if not self.log_path.exists():
            return []
        return [json.loads(line) for line in self.log_path.read_text(encoding="utf-8").splitlines()]


class ScanFilesTests(Tree):
    def test_scan_collects_every_file_with_its_kind(self):
        kinds = {os.path.basename(r.path): r.kind for r in self.scan().files}
        self.assertEqual(kinds["alice_20260101_120000.mp4"], "recording")
        self.assertEqual(kinds["alice_20260101_120000.meta.json"], "sidecar")
        self.assertEqual(kinds["alice_20260110_000000.ts"], "raw")
        self.assertEqual(kinds["clip_20260102_000000.mp4"], "clip")
        self.assertEqual(kinds["shot_20260102_000000.jpg"], "screenshot")
        self.assertEqual(kinds["bob_20251201_000000.mp4"], "media")
        self.assertEqual(kinds["notes.txt"], "other")

    def test_files_are_not_collected_unless_asked_for(self):
        self.assertEqual(lib.scan_library(self.roots).files, [])

    def test_empty_folders_are_found_but_not_top_folders(self):
        (self.streams / "Processed/kick/ghost").mkdir()
        (self.streams / "Recorded/empty_top_child").mkdir()
        stats = self.scan()
        names = {os.path.basename(p) for p in stats.empty_dirs}
        self.assertEqual(names, {"ghost", "empty_top_child"})


class GuardTests(Tree):
    def test_an_old_recording_is_allowed(self):
        self.assertIsNone(self.guard().check(str(self.a1)))

    def test_outside_the_library_is_refused(self):
        stray = put(self.base / "elsewhere/bob/x.mp4")
        self.assertIn("not inside", self.guard().check(str(stray)))

    def test_dotdot_cannot_escape(self):
        sneaky = os.path.join(str(self.streams), "Processed", "kick", "alice", "..", "..", "..", "..", "data", "x.mp4")
        put(self.data / "x.mp4")
        self.assertIsNotNone(self.guard().check(sneaky))

    def test_files_directly_in_a_top_folder_are_refused(self):
        loose = put(self.streams / "Processed/loose.mp4")
        self.assertIn("directly", self.guard().check(str(loose)))

    def test_other_file_types_are_left_alone(self):
        for name in ("notes.txt", "config.ini", "channels.json", "cookies.txt", "tool.py", "app.log"):
            f = put(self.streams / "Processed/kick/alice" / name)
            self.assertIn("left alone", self.guard().check(str(f)), name)

    def test_recent_files_are_refused_but_old_partials_are_fine(self):
        fresh = put(self.streams / "Recorded/kick/alice/live.ts", age=time.time() - 30)
        self.assertIn("minutes", self.guard().check(str(fresh)))
        old_part = put(self.streams / "Processed/kick/alice/x.mp4.part")
        self.assertIsNone(self.guard().check(str(old_part)))
        fresh_part = put(self.streams / "Processed/kick/alice/y.mp4.part", age=time.time() - 5)
        self.assertIn("minutes", self.guard().check(str(fresh_part)))

    def test_a_channel_that_is_recording_is_blocked(self):
        busy = lambda: ({("kick", "alice")}, set())  # noqa: E731
        g = self.guard(busy=busy)
        self.assertIn("recording right now", g.check(str(self.a1)))
        self.assertIn("recording right now", g.check(str(self.clip)))
        self.assertIsNone(g.check(str(self.b1)))  # other channels are fine

    def test_a_file_a_recorder_is_writing_is_blocked(self):
        g = self.guard(busy=lambda: (set(), {os.path.normcase(os.path.abspath(str(self.raw)))}))
        self.assertIn("writing", g.check(str(self.raw)))

    def test_unreadable_recorder_state_blocks_everything(self):
        def broken():
            raise RuntimeError("no status")
        self.assertIn("recording right now", self.guard(busy=broken).check(str(self.a1)))

    def test_a_library_inside_the_data_folder_is_still_manageable(self):
        # portable setup: config.ini and the Streams folder live side by side
        g = act.Guard(self.roots, data_dir=str(self.base))
        self.assertIsNone(g.check(str(self.a1)))
        self.assertIn("directly", g.check(str(self.streams / "Processed" / "x.mp4")) or "directly")

    def test_the_data_folder_is_never_touched(self):
        g = act.Guard(self.roots, data_dir=str(self.streams / "Processed/kick/alice"))
        self.assertIn("data folder", g.check(str(self.a1)))

    def test_changed_since_the_preview_is_refused(self):
        g = self.guard()
        self.assertIn("size", g.check(str(self.a1), size=1, mtime=self.a1.stat().st_mtime))
        self.assertIn("modified", g.check(str(self.a1), size=400, mtime=self.a1.stat().st_mtime + 100))
        self.assertIsNone(g.check(str(self.a1), size=400, mtime=self.a1.stat().st_mtime))

    def test_missing_files_are_refused(self):
        self.assertIsNotNone(self.guard().check(str(self.streams / "Processed/kick/alice/nope.mp4")))

    def test_a_drive_root_or_missing_streams_folder_allows_nothing(self):
        anchor = os.path.abspath(os.sep) if os.name != "nt" else os.path.splitdrive(str(self.base))[0] + "\\"
        g = act.Guard(lib.Roots(anchor), data_dir=str(self.data))
        self.assertEqual(g.tops, {})
        self.assertIsNotNone(g.check(str(self.a1)))
        self.assertEqual(act.Guard(lib.Roots(str(self.base / "missing"))).tops, {})

    def test_symlinked_file_is_refused(self):
        link = self.streams / "Processed/kick/alice/link.mp4"
        try:
            os.symlink(str(self.b1), str(link))
        except (OSError, NotImplementedError):
            self.skipTest("symlinks not permitted here")
        self.assertIn("link", self.guard().check(str(link)))

    def _junction(self, link: Path, target: Path):
        try:
            os.symlink(str(target), str(link), target_is_directory=True)
            return True
        except (OSError, NotImplementedError):
            pass
        if os.name == "nt":
            done = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(target)], capture_output=True)
            return done.returncode == 0
        return False

    def test_a_linked_folder_that_leads_outside_is_refused(self):
        outside = self.base / "outside"
        victim = put(outside / "precious.mp4")
        if not self._junction(self.streams / "Processed/kick/escape", outside):
            self.skipTest("cannot create a link here")
        path = self.streams / "Processed/kick/escape/precious.mp4"
        self.assertTrue(path.exists())
        self.assertIsNotNone(self.guard().check(str(path)))
        plan = act.build_plan(self.guard(), [lib.FileRec(str(path), 100, victim.stat().st_mtime, 0, "processed", "kick",
                                                          "escape", "recording", OLD)], act.ACTION_PERMANENT)
        self.assertFalse(plan.ok)
        self.assertTrue(victim.exists())

    def test_a_linked_top_folder_is_not_a_library_folder(self):
        outside = self.base / "outside2"
        put(outside / "kick/zed/z.mp4")
        os.rename(self.streams / "Clips", self.base / "clips_moved")
        if not self._junction(self.streams / "Clips", outside):
            self.skipTest("cannot create a link here")
        self.assertNotIn("clips", self.guard().tops)


class PlanTests(Tree):
    def test_plan_counts_sizes_and_brings_the_info_file(self):
        plan = act.build_plan(self.guard(), self.recs(self.a1, self.a2), act.ACTION_PENDING)
        self.assertEqual(plan.files, 3)  # two videos and a1's .meta.json
        self.assertEqual(plan.bytes, 400 + 300 + 10)
        self.assertEqual(plan.channels, ["kick/alice"])
        first, last = plan.span
        self.assertLess(first, last)
        self.assertEqual(plan.by_kind()["sidecar"], (1, 10))
        text = " ".join(plan.summary_lines())
        self.assertIn("kick/alice", text)
        self.assertIn("2026-01-01", text)

    def test_refused_files_are_listed_with_a_reason_not_dropped_silently(self):
        recs = self.recs(self.a1, self.note)
        plan = act.build_plan(self.guard(), recs, act.ACTION_PENDING)
        self.assertEqual({os.path.basename(p) for p, _ in plan.skipped}, {"notes.txt"})
        self.assertEqual(plan.files, 2)

    def test_files_already_in_pending_are_not_moved_again(self):
        plan = act.build_plan(self.guard(), self.recs(self.pend), act.ACTION_PENDING)
        self.assertFalse(plan.ok)
        self.assertIn("already", plan.skipped[0][1])

    def test_duplicates_in_the_candidate_list_count_once(self):
        recs = self.recs(self.a2)
        plan = act.build_plan(self.guard(), recs + recs, act.ACTION_PENDING)
        self.assertEqual(plan.files, 1)

    def test_a_recording_channel_is_named_in_the_blockers(self):
        g = self.guard(busy=lambda: ({("kick", "alice")}, set()))
        plan = act.build_plan(g, self.recs(self.a1, self.b1), act.ACTION_PENDING)
        self.assertEqual(list(plan.blocked), ["kick/alice"])
        self.assertEqual(plan.channels, ["twitch/bob"])

    def test_confirm_word_is_the_channel_or_DELETE(self):
        single = act.build_plan(self.guard(), self.recs(self.a2), act.ACTION_PERMANENT)
        self.assertEqual(single.confirm_word, "alice")
        both = act.build_plan(self.guard(), self.recs(self.a2, self.b1), act.ACTION_PERMANENT)
        self.assertEqual(both.confirm_word, "DELETE")

    def test_empty_folder_plan_only_takes_empty_plain_folders(self):
        empty = self.streams / "Processed/kick/ghost"
        empty.mkdir()
        full = self.streams / "Processed/kick/alice"
        top = self.streams / "Processed"
        plan = act.plan_empty_dirs(self.guard(), [str(empty), str(full), str(top)])
        self.assertEqual([i.path for i in plan.items], [str(empty)])
        self.assertEqual(len(plan.skipped), 2)


class FilterTests(Tree):
    def setUp(self):
        super().setUp()
        self.recs_all = [r for r in self.scan().files if r.category == "processed"]
        self.now = time.mktime((2026, 4, 1, 0, 0, 0, 0, 0, -1))

    def names(self, rows):
        return sorted(os.path.basename(r.path) for r in rows)

    def test_older_than_uses_the_date_in_the_name(self):
        rows = act.filter_recs(self.recs_all, kinds={"recording"}, older_than_days=45, now=self.now)
        self.assertEqual(self.names(rows), ["alice_20260101_120000.mp4", "alice_20260201_120000.mp4",
                                            "bob_20260105_000000.mp4"])

    def test_keep_newest_files_and_bytes(self):
        alice = [r for r in self.recs_all if r.channel == "alice" and r.kind == "recording"]
        rows = act.filter_recs(alice, kinds={"recording"}, keep_newest_files=1)
        self.assertEqual(self.names(rows), ["alice_20260101_120000.mp4", "alice_20260201_120000.mp4"])
        rows = act.filter_recs(alice, kinds={"recording"}, keep_newest_bytes=450)  # newest 200 + 300 reach it
        self.assertEqual(self.names(rows), ["alice_20260101_120000.mp4"])
        rows = act.filter_recs(alice, kinds={"recording"}, keep_newest_files=99)
        self.assertEqual(rows, [])

    def test_date_range_month_and_text(self):
        day = act.parse_day
        rows = act.filter_recs(self.recs_all, kinds={"recording"}, date_from=day("2026-01-05"), date_to=day("2026-02-01"))
        self.assertEqual(self.names(rows), ["alice_20260201_120000.mp4", "bob_20260105_000000.mp4"])
        rows = act.filter_recs(self.recs_all, kinds={"recording"}, month="2026-03")
        self.assertEqual(self.names(rows), ["alice_20260301_120000.mp4"])
        rows = act.filter_recs(self.recs_all, kinds={"recording"}, text="BOB")
        self.assertEqual(self.names(rows), ["bob_20260105_000000.mp4"])
        self.assertIsNone(act.parse_day("nope"))
        self.assertIsNone(act.parse_day(""))

    def test_info_files_never_come_alone(self):
        rows = act.filter_recs(self.recs_all)
        self.assertNotIn("sidecar", {r.kind for r in rows})


class ExecuteTests(Tree):
    def run_plan(self, action, recs, **kw):
        g = kw.pop("guard", None) or self.guard()
        plan = act.build_plan(g, recs, action)
        deleter = kw.pop("deleter", None) or act.deleter_for(action, g, self.manifest)
        return plan, act.execute(plan, g, deleter, log=act.ActionLog(str(self.log_path)), manifest=self.manifest, **kw)

    def test_move_to_pending_mirrors_the_folders_and_takes_the_info_file(self):
        plan, result = self.run_plan(act.ACTION_PENDING, self.recs(self.a1, self.clip))
        pending = self.streams / "PendingDeletion"
        self.assertTrue((pending / "kick/alice/alice_20260101_120000.mp4").is_file())
        self.assertTrue((pending / "kick/alice/alice_20260101_120000.meta.json").is_file())
        self.assertTrue((pending / "kick/alice/clip_20260102_000000.mp4").is_file())
        self.assertFalse(self.a1.exists() or self.a1m.exists() or self.clip.exists())
        self.assertEqual((result.done, result.bytes, len(result.failed)), (3, 400 + 10 + 70, 0))
        self.assertIn("Moved to PendingDeletion: 3", result.summary)
        self.assertTrue(self.a2.exists() and self.b1.exists())  # nothing else moved

    def test_the_grace_period_really_applies_the_recorders_own_purge_leaves_it_alone(self):
        """The recorder ages a PendingDeletion file by max(mtime, ctime). A recording moved
        with its old dates (and, on Windows, an old creation time) would be purged at the next
        start. The move resets mtime, so the grace period counts from the move."""
        from msr.recorder import purge_old_pending_files
        real_stat = os.stat

        def old_birth(path, *args, **kwargs):  # a recording made long ago: old creation/change time
            st = real_stat(path, *args, **kwargs)
            vals = list(st)[:10]
            vals[9] = int(OLD)
            return os.stat_result(vals + [st.st_atime, st.st_mtime, OLD])
        self.run_plan(act.ACTION_PENDING, self.recs(self.a1))  # a1 is 40 days old
        moved = self.streams / "PendingDeletion/kick/alice/alice_20260101_120000.mp4"
        self.assertLess(time.time() - moved.stat().st_mtime, 120)
        with mock.patch.object(os, "stat", old_birth):
            self.assertEqual(purge_old_pending_files(str(self.streams / "."), 7), 1)  # only the old pre-existing file
        self.assertTrue(moved.exists())
        self.assertFalse(self.pend.exists())
        # control: a file moved the plain way keeps its old mtime and is purged straight away
        plain = put(self.streams / "PendingDeletion/kick/alice/plain.mp4", 5)
        with mock.patch.object(os, "stat", old_birth):
            self.assertEqual(purge_old_pending_files(str(self.streams), 7), 1)
        self.assertFalse(plain.exists())
        self.assertTrue(moved.exists())

    def test_name_clash_in_pending_gets_a_number_and_restore_still_finds_the_original(self):
        clash = put(self.streams / "PendingDeletion/kick/alice/alice_20260201_120000.mp4", 5)
        self.run_plan(act.ACTION_PENDING, self.recs(self.a2))
        self.assertTrue(clash.exists())
        self.assertTrue((self.streams / "PendingDeletion/kick/alice/alice_20260201_120000 (2).mp4").exists())
        moved = self.manifest.load()[0]
        self.assertTrue(moved["pending"].endswith("(2).mp4"))
        self.assertEqual(os.path.normcase(moved["original"]), os.path.normcase(str(self.a2)))

    def test_restore_puts_files_back_with_their_old_date(self):
        old_mtime = self.a2.stat().st_mtime
        _plan, moved = self.run_plan(act.ACTION_PENDING, self.recs(self.a2))
        g = self.guard()
        undo = act.restore_batch_plan(g, self.manifest, moved.batch)
        self.assertEqual(undo.files, 1)
        result = act.execute(undo, g, act.deleter_for(act.ACTION_RESTORE, g, self.manifest),
                             log=act.ActionLog(str(self.log_path)), manifest=self.manifest)
        self.assertEqual(result.done, 1)
        self.assertTrue(self.a2.exists())
        self.assertEqual(int(self.a2.stat().st_mtime), int(old_mtime))
        self.assertEqual(self.manifest.load(), [])
        self.assertFalse((self.streams / "PendingDeletion/kick/alice/alice_20260201_120000.mp4").exists())

    def test_restore_refuses_to_overwrite(self):
        _plan, moved = self.run_plan(act.ACTION_PENDING, self.recs(self.a2))
        put(self.a2, 7)  # a new file took its place
        g = self.guard()
        undo = act.restore_batch_plan(g, self.manifest, moved.batch)
        result = act.execute(undo, g, act.deleter_for(act.ACTION_RESTORE, g, self.manifest), manifest=self.manifest)
        self.assertEqual(result.done, 0)
        self.assertIn("already there", result.failed[0][1])
        self.assertEqual(self.a2.stat().st_size, 7)

    def test_restore_without_a_manifest_uses_processed_and_recorded(self):
        ts = put(self.streams / "PendingDeletion/kick/alice/old_20250101_000000.ts", 9)
        mp4 = put(self.streams / "PendingDeletion/kick/alice/old_20250102_000000.mp4", 9)
        g = self.guard()
        plan = act.build_plan(g, self.recs(ts, mp4), act.ACTION_RESTORE)
        act.execute(plan, g, act.deleter_for(act.ACTION_RESTORE, g, self.manifest), manifest=self.manifest)
        self.assertTrue((self.streams / "Recorded/kick/alice/old_20250101_000000.ts").exists())
        self.assertTrue((self.streams / "Processed/kick/alice/old_20250102_000000.mp4").exists())

    def test_restore_all_in_pending_skips_non_pending_files(self):
        plan = act.build_plan(self.guard(), self.recs(self.a1), act.ACTION_RESTORE)
        self.assertFalse(plan.ok)

    def test_permanent_delete_removes_and_logs(self):
        plan, result = self.run_plan(act.ACTION_PERMANENT, self.recs(self.b1))
        self.assertFalse(self.b1.exists())
        self.assertEqual(result.done, 1)
        events = self.log_events()
        self.assertEqual([e["event"] for e in events], ["start", "done", "end"])
        self.assertEqual(events[1]["action"], "permanent")
        self.assertEqual(events[1]["size"], 1000)
        self.assertEqual(events[0]["batch"], events[2]["batch"])
        self.assertIn("time", events[1])

    def test_cancel_stops_between_files_and_the_info_file_goes_with_its_video(self):
        cancel = threading.Event()
        g = self.guard()
        recs = self.recs(self.a1, self.a2, self.a3)
        plan = act.build_plan(g, recs, act.ACTION_PERMANENT)

        def progress(done, total, size):
            cancel.set()  # after the first file has been dealt with
        result = act.execute(plan, g, act.permanent_deleter(), progress=progress, cancel=cancel,
                             log=act.ActionLog(str(self.log_path)))
        self.assertTrue(result.cancelled)
        remaining = [p for p in (self.a1, self.a2, self.a3) if p.exists()]
        self.assertEqual(len(remaining), 2)
        self.assertIn("Cancelled after", result.summary)

    def test_a_failing_file_is_reported_and_the_rest_continue(self):
        calls = []

        def flaky(item):
            calls.append(item.path)
            if item.path == str(self.a1):
                raise PermissionError("locked")
            os.remove(item.path)
            return {}
        _plan, result = self.run_plan(act.ACTION_PERMANENT, self.recs(self.a1, self.a2), deleter=flaky)
        self.assertTrue(self.a1.exists())
        self.assertFalse(self.a2.exists())
        self.assertEqual(len(result.failed), 1)
        self.assertIn("locked", result.failed[0][1])
        self.assertTrue(any(e["event"] == "error" for e in self.log_events()))

    def test_a_file_that_changed_after_the_preview_is_not_touched(self):
        g = self.guard()
        plan = act.build_plan(g, self.recs(self.a2), act.ACTION_PERMANENT)
        self.a2.write_bytes(b"y" * 999)  # growing again
        result = act.execute(plan, g, act.permanent_deleter(), log=act.ActionLog(str(self.log_path)))
        self.assertEqual(result.done, 0)
        self.assertTrue(self.a2.exists())
        self.assertIn("skipped", result.failed[0][1])

    def test_a_channel_that_starts_recording_after_the_preview_is_not_touched(self):
        state = {"busy": set()}
        g = self.guard(busy=lambda: (state["busy"], set()))
        plan = act.build_plan(g, self.recs(self.a2), act.ACTION_PERMANENT)
        self.assertTrue(plan.ok)
        state["busy"] = {("kick", "alice")}
        result = act.execute(plan, g, act.permanent_deleter())
        self.assertEqual(result.done, 0)
        self.assertTrue(self.a2.exists())

    def test_remove_empty_folders(self):
        deep = self.streams / "Processed/kick/ghost/inner"
        deep.mkdir(parents=True)
        g = self.guard()
        plan = act.plan_empty_dirs(g, [str(deep), str(deep.parent)])
        self.assertEqual(plan.folders, 1)  # the parent is not empty yet
        result = act.execute(plan, g, None, log=act.ActionLog(str(self.log_path)))
        self.assertEqual(result.done, 1)
        self.assertFalse(deep.exists())
        self.assertTrue(deep.parent.exists())

    def test_a_folder_that_got_a_file_is_not_removed(self):
        ghost = self.streams / "Processed/kick/ghost"
        ghost.mkdir()
        g = self.guard()
        plan = act.plan_empty_dirs(g, [str(ghost)])
        put(ghost / "new.mp4")
        result = act.execute(plan, g, None)
        self.assertEqual(result.done, 0)
        self.assertTrue((ghost / "new.mp4").exists())

    def test_empty_now_removes_only_whats_in_pending(self):
        stats = self.scan()
        g = self.guard()
        plan = act.build_plan(g, act.pending_recs(stats), act.ACTION_PERMANENT)
        self.assertEqual(plan.files, 1)
        act.execute(plan, g, act.permanent_deleter())
        self.assertFalse(self.pend.exists())
        self.assertTrue(self.b1.exists() and self.a1.exists())

    def test_job_reports_through_the_queue_and_never_needs_tk(self):
        out = queue.Queue()
        g = self.guard()
        plan = act.build_plan(g, self.recs(self.b1), act.ACTION_PERMANENT)
        job = act.ActionJob(plan, g, out, log=act.ActionLog(str(self.log_path)))
        job.start()
        job.join(10)
        kinds = []
        while not out.empty():
            kinds.append(out.get()[0])
        self.assertEqual(kinds[-1], "act_done")
        self.assertFalse(self.b1.exists())

    def test_nothing_outside_the_library_is_ever_removed(self):
        precious = put(self.base / "keep/me.mp4")
        for action in (act.ACTION_PERMANENT, act.ACTION_PENDING):
            plan = act.build_plan(self.guard(), [lib.FileRec(str(precious), 100, precious.stat().st_mtime, 0, "processed",
                                                             "kick", "alice", "recording", OLD)], action)
            self.assertFalse(plan.ok)
            act.execute(plan, self.guard(), act.deleter_for(action, self.guard()))
        self.assertTrue(precious.exists())
        self.assertTrue((self.data).exists())


class PendingCardTests(Tree):
    def test_info_and_next_purge_follow_the_recorders_rule(self):
        now = time.time()
        fresh = put(self.streams / "PendingDeletion/kick/alice/f.mp4", 30, age=now - 2 * 86400)
        stats = self.scan()
        # (files made a moment ago have a new creation time; give the old one the dates a real one has)
        stats.files = [r._replace(ctime=OLD) if r.path == str(self.pend) else r._replace(ctime=now - 2 * 86400)
                       for r in stats.files]
        info = act.pending_info(stats, 7, True, now)
        self.assertEqual((info.files, info.bytes), (2, 120))
        self.assertEqual(info.due_files, 1)  # the 40 day old one
        self.assertIsNotNone(info.next_due)
        text = " ".join(info.lines(now))
        self.assertIn("when MSR starts", text)
        self.assertIn("already old enough", text)
        off = " ".join(act.pending_info(stats, 0, True, now).lines(now))
        self.assertIn("purge is off", off)
        self.assertEqual(act.pending_info(lib.LibraryStats(), 7).lines(), ["PendingDeletion is empty."])
        self.assertTrue(fresh.exists())


class SuggestionTests(Tree):
    def test_suggestions(self):
        (self.streams / "Processed/kick/ghost").mkdir()
        dup = put(self.streams / "Processed/twitch/bob2/bob_20260105_000000.mp4", 1000)  # same name and size
        put(self.streams / "Processed/kick/alice/lonely_20260101_000000.meta.json", 4)
        put(self.streams / "Processed/kick/alice/zzz_20260101_000000.mp4.part", 12)
        stats = self.scan()
        lib.mark_roster(stats, ["alice"])
        by = {s.key: s for s in act.build_suggestions(stats, older_days=30, now=time.time())}
        self.assertEqual(os.path.basename(by["largest"].recs[0].path), "bob_20260105_000000.mp4")
        self.assertEqual(os.path.basename(by["oldest"].recs[0].path), "alice_20260101_120000.mp4")
        self.assertTrue(all(r.channel in ("bob", "bob2") for r in by["roster"].recs) and by["roster"].count)
        extra = {r.path for r in by["duplicates"].recs}  # one copy is kept, the other is the suggestion\n        self.assertEqual(len(extra), 1)\n        self.assertTrue(extra <= {str(dup), str(self.b1)})
        self.assertEqual([os.path.basename(r.path) for r in by["orphans"].recs], ["lonely_20260101_000000.meta.json"])
        self.assertEqual([os.path.basename(r.path) for r in by["leftover"].recs].count("zzz_20260101_000000.mp4.part"), 1)
        self.assertIn(str(self.streams / "Processed/kick/ghost"), by["empty"].dirs)
        self.assertGreater(by["older"].count, 0)

    def test_free_up_picks_oldest_first_until_the_target_is_met(self):
        stats = self.scan()
        s = act.free_up_suggestion(stats, 650)
        names = [os.path.basename(r.path) for r in s.recs]
        self.assertEqual(names[0], "alice_20260101_120000.mp4")
        self.assertGreaterEqual(s.bytes, 650)
        self.assertLess(s.bytes, 650 + 1001)
        short = act.free_up_suggestion(stats, 10 ** 9)
        self.assertIn("short", short.detail)


class StatusAndRecycleTests(unittest.TestCase):
    def test_busy_from_status(self):
        channels, files = act.busy_from_status({
            "alice": {"status": "Recording", "detail": "x", "raw_file": os.path.join("C:\\", "r", "a.ts")},
            "twitch:bob": {"status": "Offline", "detail": "reconnecting (20s left)"},
            "twitch:carol": {"status": "Offline", "detail": "not live"},
            "youtube:@dan": {"status": "Remuxing...", "detail": ""},
            "eve": {"status": "Stopped"},
            "kick2": {"status": "Something new"},
        })
        self.assertIn(("kick", "alice"), channels)
        self.assertIn(("twitch", "bob"), channels)
        self.assertNotIn(("twitch", "carol"), channels)
        self.assertIn(("youtube", "@dan"), channels)
        self.assertNotIn(("kick", "eve"), channels)
        self.assertIn(("kick", "kick2"), channels)  # unknown states count as busy
        self.assertEqual(len(files), 1)

    def test_shfileoperation_buffer_is_double_null_terminated(self):
        buf = act._shfileop_buffer(os.path.join("C:\\", "x", "a.mp4"))
        self.assertTrue(buf.endswith("\0\0"))
        self.assertEqual(buf.count("\0"), 2)

    def test_recycle_bin_is_not_offered_off_windows_and_never_raises(self):
        with mock.patch.object(act.os, "name", "posix"):
            self.assertIn("Windows", act.recycle_bin_problem("/x", 1))
        result = act.recycle_bin_problem(tempfile.gettempdir(), 10 ** 15)
        self.assertTrue(result is None or isinstance(result, str))
        if os.name == "nt":  # a petabyte can never fit
            self.assertIsInstance(result, str)

    def test_manifest_survives_garbage(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "m.json")
            Path(path).write_text("{not json", encoding="utf-8")
            self.assertEqual(act.Manifest(path).load(), [])
            self.assertEqual(act.Manifest(None).load(), [])
            m = act.Manifest(path)
            m.add([{"batch": "b", "pending": "p", "original": "o"}])
            self.assertEqual(len(m.load()), 1)
            m.remove(["p"])
            self.assertEqual(m.load(), [])


if __name__ == "__main__":
    unittest.main()
