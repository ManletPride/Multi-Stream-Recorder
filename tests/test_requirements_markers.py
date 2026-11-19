"""A Linux-only pip dependency must not install on Windows.

PR #7 added PyGObject so the tray can dock on Hyprland. The follow-up commit
on that branch ("Install PyGObject on Linux only") put
``sys_platform == "linux"`` on the line, because a source build of PyGObject
has nothing to do on Windows. ``tests/test_tray_linux.py`` checks that one
package by name. This check also fails when the comment says "Linux only"
and the marker is missing, which is how the next such package would be added.
"""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _requirement(line: str):
    try:
        from packaging.requirements import Requirement
    except ImportError:
        from pip._vendor.packaging.requirements import Requirement
    return Requirement(line)


def marker_problems(text: str) -> list[str]:
    """Lines that are Linux-only but still install on Windows or macOS."""
    problems = []
    comments: list[str] = []
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line:
            comments = []
            continue
        if line.startswith("#"):
            comments.append(line)
            continue
        try:
            req = _requirement(line)
        except Exception:
            comments = []
            continue
        blob = " ".join(comments).lower()
        linux_only = "linux only" in blob or req.name.lower() == "pygobject"
        if linux_only:
            marker = req.marker
            skips_others = (
                marker is not None
                and bool(marker.evaluate({"sys_platform": "linux"}))
                and not marker.evaluate({"sys_platform": "win32"})
                and not marker.evaluate({"sys_platform": "darwin"})
            )
            if not skips_others:
                problems.append(
                    f"requirements.txt:{lineno}: {req.name} is Linux-only and would "
                    "install on Windows. Add `; sys_platform == \"linux\"`, as on the "
                    "PyGObject line (PR #7, \"Install PyGObject on Linux only\")."
                )
        comments = []
    return problems


class RequirementsMarkerTests(unittest.TestCase):
    def test_current_requirements_skip_windows(self):
        text = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        self.assertEqual(marker_problems(text), [])

    def test_pygobject_without_a_marker_is_rejected(self):
        found = marker_problems("PyGObject>=3.50\n")
        self.assertEqual(len(found), 1, found)
        self.assertIn("sys_platform == \"linux\"", found[0])
        self.assertIn("requirements.txt:1", found[0])

    def test_a_linux_only_comment_without_a_marker_is_rejected(self):
        text = "# Linux only. Windows and macOS skip this.\npycairo>=1.20.0\n"
        found = marker_problems(text)
        self.assertEqual(len(found), 1, found)
        self.assertIn("pycairo", found[0])

    def test_a_marker_that_installs_on_windows_is_rejected(self):
        found = marker_problems('PyGObject>=3.50; sys_platform == "win32"\n')
        self.assertEqual(len(found), 1, found)


if __name__ == "__main__":
    unittest.main()
