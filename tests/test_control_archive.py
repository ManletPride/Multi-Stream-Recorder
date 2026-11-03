"""recordings_browse / recordings_clips: Library > Recordings over control (no Tk). Temp trees."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from msr import clip_core as core, control  # noqa: E402
from msr.config import Config  # noqa: E402
from tests.test_control import make_data  # noqa: E402


def put(path: Path, size=10, meta=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    if meta is not None:
        path.with_name(path.name.rsplit(".", 1)[0] + ".meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return path


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data, streams = make_data(self.tmp.name, ["alice", "twitch:bob"])
        s = self.streams = Path(streams)
        self.new = put(s / "Processed/kick/alice/alice_20260101_120000.mp4", 400,
                       {"duration_seconds": 3661, "stream_title": "New year chill stream",
                        "recording_started": "2026-01-01 12:00:00"})
        self.walk = put(s / "Processed/kick/alice/alice_20260215_083000.mp4", 300,
                        {"duration_seconds": 60, "stream_title": "IRL Tokyo walk"})
        put(s / "Processed/kick/alice/alice_20260301_100000.mp4.part", 50)
        put(s / "Recorded/kick/alice/alice_20260302_100000.ts", 100)
        put(s / "Processed/twitch/bob/bob_20260301_000000.mp4", 20, {"stream_title": "bob plays"})
        # a clip with a sidecar, and an older one linked by its name only
        clip_dir = s / "Clips/kick/alice"
        self.clip = put(clip_dir / core.clip_name(str(self.new), 60, 90), 7)
        core.write_clip_meta(core.CutResult(str(self.clip), 60, 90, 60, 90, 30, False), str(self.new))
        self.named = put(clip_dir / "alice_20260215_083000_clip_0-00-10_to_0-00-20_20260216_101010.mp4", 5)
        self.gone = put(clip_dir / "alice_20250101_000000_clip_0-00-01_to_0-00-02_20250102_101010.mp4", 3)
        self.ctl = control.Controller(control.OfflineApp(Config(os.path.join(self.data, "config.ini"))))

    def ok(self, cmd, args=None):
        reply = self.ctl.handle(cmd, args or {})
        self.assertTrue(reply["ok"], reply)
        return reply["result"]

    def test_channels_without_a_channel(self):
        chans = {c["channel"]: c["files"] for c in self.ok("recordings_browse")["channels"]}
        self.assertEqual(set(chans), {"kick:alice", "twitch:bob"})
        self.assertGreaterEqual(chans["kick:alice"], 3)

    def test_list_has_date_length_title_size_path_and_clips(self):
        res = self.ok("recordings_browse", {"channel": "alice"})
        self.assertEqual(res["channel"], "kick:alice")
        rows = {r["file"]: r for r in res["recordings"]}
        self.assertEqual(set(rows), {"alice_20260101_120000.mp4", "alice_20260215_083000.mp4",
                                     "alice_20260302_100000.ts"})
        first = rows["alice_20260101_120000.mp4"]
        self.assertEqual((first["length"], first["title"], first["size"]), ("1:01:01", "New year chill stream", 400))
        self.assertEqual(first["date"], "2026-01-01T12:00:00")
        self.assertEqual(os.path.normcase(first["path"]), os.path.normcase(str(self.new)))
        self.assertEqual([c["file"] for c in first["clips"]], [self.clip.name])
        self.assertEqual((first["clips"][0]["in_seconds"], first["clips"][0]["out_seconds"]), (60, 90))
        self.assertFalse(first["clips"][0]["times_from_name"])
        named = rows["alice_20260215_083000.mp4"]["clips"][0]
        self.assertTrue(named["times_from_name"])
        self.assertTrue(rows["alice_20260302_100000.ts"]["raw"])
        self.assertEqual(res["unlinked_clips"], 1)

    def test_search_sort_and_limit(self):
        res = self.ok("recordings_browse", {"channel": "kick:alice", "text": "tokyo"})
        self.assertEqual([r["file"] for r in res["recordings"]], ["alice_20260215_083000.mp4"])
        res = self.ok("recordings_browse", {"channel": "#1", "sort": "length", "order": "desc", "limit": 1})
        self.assertEqual((res["recordings"][0]["length_seconds"], res["truncated"]), (3661, 2))
        res = self.ok("recordings_browse", {"channel": "alice", "sort": "date", "order": "asc", "clips": False})
        self.assertEqual(res["recordings"][0]["file"], "alice_20260101_120000.mp4")
        self.assertNotIn("clips", res["recordings"][0])
        self.assertEqual(self.ctl.handle("recordings_browse", {"channel": "alice", "sort": "x"})["code"], "bad_request")
        self.assertEqual(self.ctl.handle("recordings_browse", {"channel": "nobody"})["code"], "not_found")

    def test_clip_to_source_and_back(self):
        res = self.ok("recordings_clips", {"path": str(self.clip)})
        self.assertEqual(res["kind"], "clip")
        self.assertEqual(res["source"]["file"], self.new.name)
        res = self.ok("recordings_clips", {"path": str(self.new)})
        self.assertEqual((res["kind"], [c["file"] for c in res["recording"]["clips"]]), ("recording", [self.clip.name]))
        res = self.ok("recordings_clips", {"path": str(self.gone)})
        self.assertIsNone(res["source"])
        self.assertEqual(self.ctl.handle("recordings_clips", {"path": str(self.streams / "nope.mp4")})["code"],
                         "not_found")

    def test_no_tkinter_needed(self):
        import subprocess
        from msr.nowindow import no_window_kwargs
        code = ("import sys; sys.modules['tkinter'] = None; import msr.recordings_index, msr.control; "
                "print('ok')")
        out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True,
                             **no_window_kwargs())
        self.assertEqual(out.stdout.strip(), "ok", out.stderr)


if __name__ == "__main__":
    unittest.main()
