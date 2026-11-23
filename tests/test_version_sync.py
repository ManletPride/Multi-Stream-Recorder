"""The app version has one home: ``__version__`` in ``msr/__init__.py``.

Release commits from 3.1.0 through 3.1.5 updated that line, the README and the
CHANGELOG, and left AGENTS.md saying 3.0.0. This test fails when those copies
disagree, and when AGENTS.md grows its own version number again.
"""
import re
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VERSION_ASSIGN = re.compile(r'^__version__\s*=\s*["\']([^"\']+)["\']', re.M)
README_LINE = re.compile(r"This README describes \*\*v(\d+\.\d+\.\d+)\*\*")
CHANGELOG_HEADING = re.compile(r"^## v(\d+\.\d+\.\d+) ", re.M)
# A version presented as the current one. Historical mentions in other docs are fine.
AGENTS_CURRENT = re.compile(
    r"__version__` is `(\d+\.\d+\.\d+)`|currently `(\d+\.\d+\.\d+)`"
)


def version_in_init(text: str) -> str | None:
    found = VERSION_ASSIGN.findall(text)
    if len(found) != 1:
        return None
    return found[0]


def agents_version_claims(text: str) -> list[str]:
    """Lines where AGENTS.md states a current version. There should be none."""
    bad = []
    for i, line in enumerate(text.splitlines(), 1):
        match = AGENTS_CURRENT.search(line)
        if match:
            claimed = next(g for g in match.groups() if g)
            bad.append(
                f"AGENTS.md:{i}: states a version ({claimed}). "
                "Do not copy it here. The only copy is __version__ in msr/__init__.py. "
                "README, CHANGELOG and docs/release_notes are checked by "
                "tests/test_version_sync.py."
            )
    return bad


def bundle_problems(root: Path) -> list[str]:
    """README, the latest CHANGELOG heading and the release-notes file vs ``__version__``."""
    init_text = (root / "msr" / "__init__.py").read_text(encoding="utf-8")
    version = version_in_init(init_text)
    problems = []
    if version is None:
        problems.append(
            "msr/__init__.py: expected exactly one __version__ = \"X.Y.Z\" assignment"
        )
        return problems
    for path in (root / "msr").glob("*.py"):
        if path.name == "__init__.py":
            continue
        if VERSION_ASSIGN.search(path.read_text(encoding="utf-8")):
            problems.append(
                f"{path.name}: assigns __version__. Use msr/__init__.py; everything else imports it."
            )
    readme = (root / "README.md").read_text(encoding="utf-8")
    described = README_LINE.search(readme)
    if described is None:
        problems.append(
            'README.md: must contain "This README describes **v'
            + version
            + '**." (the line release commits update).'
        )
    elif described.group(1) != version:
        problems.append(
            f"README.md: describes v{described.group(1)} but msr/__init__.py __version__ is {version}."
        )
    changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    heading = CHANGELOG_HEADING.search(changelog)
    if heading is None or heading.group(1) != version:
        got = heading.group(1) if heading else "none"
        problems.append(
            f"CHANGELOG.md: first released heading is v{got}; "
            f"it must be '## v{version} ' to match msr/__init__.py."
        )
    notes_name = f"RELEASE_NOTES_v{version}.md"
    notes = root / "docs" / "release_notes" / notes_name
    if not notes.is_file():
        problems.append(
            f"docs/release_notes/{notes_name} is missing. "
            "That filename is the release-notes copy of __version__ "
            "(see tests/test_release_notes_home.py for where the file lives)."
        )
    else:
        body = notes.read_text(encoding="utf-8")
        if re.search(rf"(?<![\w.])v{re.escape(version)}(?![\w.])", body) is None:
            problems.append(
                f"docs/release_notes/{notes_name}: does not name v{version}. "
                "The release-notes file must match msr/__init__.py __version__ "
                "(a file left over from another version, or a copy that still "
                "says the previous one, fails here)."
            )
    agents = root / "AGENTS.md"
    if agents.is_file():
        problems.extend(agents_version_claims(agents.read_text(encoding="utf-8")))
    return problems


# The two sentences AGENTS.md carried from the 3.0.0 prep note through every
# later release (still present at v3.1.5, commit aacebea).
STALE_AGENTS = textwrap.dedent("""\
    **Ship status (2026-10-29):** `release/3.0` prepares **v3.0.0** (Clip Editor). `__version__` is `3.0.0`; release date TBD.
    | `msr/__init__.py` | `__version__` (currently `3.0.0`; the only version string) |
""")


class VersionSyncTests(unittest.TestCase):
    def test_repo_copies_match_init(self):
        self.assertEqual(bundle_problems(ROOT), [])

    def test_stale_agents_sentences_are_rejected(self):
        """The wording that survived six releases (3.1.0 through 3.1.5)."""
        found = agents_version_claims(STALE_AGENTS)
        self.assertEqual(len(found), 2, found)
        self.assertTrue(all("msr/__init__.py" in item for item in found), found)

    def test_a_readme_left_behind_is_named(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "msr").mkdir()
            (root / "msr" / "__init__.py").write_text('__version__ = "3.1.5"\n', encoding="utf-8")
            (root / "README.md").write_text(
                "This README describes **v3.0.0**. Full history.\n", encoding="utf-8")
            (root / "CHANGELOG.md").write_text("## Unreleased\n\n## v3.1.5 (2026-11-16)\n", encoding="utf-8")
            notes = root / "docs" / "release_notes"
            notes.mkdir(parents=True)
            (notes / "RELEASE_NOTES_v3.1.5.md").write_text(
                "# Multi-Stream Recorder v3.1.5\n", encoding="utf-8")
            problems = bundle_problems(root)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("README.md: describes v3.0.0", problems[0])
        self.assertIn("3.1.5", problems[0])

    def test_release_notes_for_another_version_are_named(self):
        """A notes file named for a different version, or one that still says it."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "msr").mkdir()
            (root / "msr" / "__init__.py").write_text('__version__ = "3.3.0"\n', encoding="utf-8")
            (root / "README.md").write_text(
                "This README describes **v3.3.0**.\n", encoding="utf-8")
            (root / "CHANGELOG.md").write_text("## v3.3.0 (2026-11-23)\n", encoding="utf-8")
            notes = root / "docs" / "release_notes"
            notes.mkdir(parents=True)
            (notes / "RELEASE_NOTES_v3.2.0.md").write_text(
                "# Multi-Stream Recorder v3.2.0\n", encoding="utf-8")
            missing = bundle_problems(root)
            (notes / "RELEASE_NOTES_v3.3.0.md").write_text(
                "# Multi-Stream Recorder v3.2.0\n", encoding="utf-8")
            stale = bundle_problems(root)
        self.assertEqual(len(missing), 1, missing)
        self.assertIn("RELEASE_NOTES_v3.3.0.md is missing", missing[0])
        self.assertEqual(len(stale), 1, stale)
        self.assertIn("does not name v3.3.0", stale[0])
        self.assertIn("RELEASE_NOTES_v3.3.0.md", stale[0])


if __name__ == "__main__":
    unittest.main()
