"""Release notes live in one directory, under one filename.

They were at the repo root through v2.0.2 (``RELEASE_NOTES_v1.8.0.md`` and the
v2.0.x files). Commit 0c6aa57 moved them to ``docs/release_notes/``. The 3.1.4
release branch (5be4fab) created both ``RELEASE_NOTES_3.1.4.md`` at the root
and ``docs/release_notes/RELEASE_NOTES_v3.1.4.md``. The root copy was dropped
before that branch merged. This test fails if a notes file shows up outside
that directory, or under a name without the ``v``.
"""
import os
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP_DIRS = {".git", ".venv", "venv", "__pycache__"}
HOME = "docs/release_notes"


def _note_paths(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name.startswith("RELEASE_NOTES") and name.endswith(".md"):
                yield Path(dirpath) / name


def misplaced_release_notes(root: Path) -> list[str]:
    """Paths that are not ``docs/release_notes/RELEASE_NOTES_vX.Y.Z.md``."""
    bad = []
    for path in _note_paths(root):
        rel = path.relative_to(root).as_posix()
        parent, name = rel.rsplit("/", 1) if "/" in rel else ("", rel)
        versioned = name.startswith("RELEASE_NOTES_v") and name.endswith(".md")
        if parent == HOME and versioned:
            continue
        where = f"{HOME}/RELEASE_NOTES_vX.Y.Z.md"
        bad.append(
            f"{rel}: release notes belong in {where}. "
            "The repo root copy was removed in 0c6aa57 and written again "
            "as RELEASE_NOTES_3.1.4.md on the 3.1.4 branch (5be4fab)."
        )
    return bad


class ReleaseNotesHomeTests(unittest.TestCase):
    def test_every_notes_file_is_in_docs(self):
        self.assertEqual(misplaced_release_notes(ROOT), [])

    def test_the_3_1_4_root_name_is_rejected(self):
        """The file the 3.1.4 branch added next to the copy under docs/."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "RELEASE_NOTES_3.1.4.md").write_text("notes\n", encoding="utf-8")
            docs = root / HOME
            docs.mkdir(parents=True)
            (docs / "RELEASE_NOTES_v3.1.4.md").write_text("notes\n", encoding="utf-8")
            (docs / "RELEASE_NOTES_3.1.4.md").write_text("also wrong\n", encoding="utf-8")
            found = misplaced_release_notes(root)
        self.assertEqual(len(found), 2, found)
        self.assertTrue(any(item.startswith("RELEASE_NOTES_3.1.4.md:") for item in found), found)
        self.assertTrue(any("docs/release_notes/RELEASE_NOTES_3.1.4.md:" in item for item in found), found)
        self.assertTrue(all("docs/release_notes/RELEASE_NOTES_vX.Y.Z.md" in item for item in found))


if __name__ == "__main__":
    unittest.main()
