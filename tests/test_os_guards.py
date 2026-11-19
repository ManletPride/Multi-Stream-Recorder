"""Tests must not treat one OS's PATH as the other's.

PR #4 (cc7fbee) fixed a Linux CI failure: ``os.pathsep.join([r"C:\\other", ...])``
splits the drive letter on POSIX, where the separator is ``:``. The production
function already used ``os.pathsep``. The test had assumed ``;``.

A drive-letter entry joined or split on ``os.pathsep`` has to sit in a function
that also branches on ``os.name`` or ``sys.platform``. ``tests/test_deps_path.py``
does. The old test did not.
"""
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTS = ROOT / "tests"


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ""


def _is_pathsep(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "pathsep"
        and isinstance(node.value, ast.Name)
        and node.value.id == "os"
    )


def _drive_letter(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and len(node.value) >= 3
        and node.value[0].isalpha()
        and node.value[1:3] == ":\\"
    )


def _mentions_platform(fn: ast.AST) -> bool:
    for node in ast.walk(fn):
        if isinstance(node, ast.Attribute) and node.attr in {"name", "platform"}:
            owner = node.value
            if isinstance(owner, ast.Name) and owner.id in {"os", "sys"}:
                return True
    return False


def _joins_drive_on_pathsep(fn: ast.AST) -> list[int]:
    """Lines that put a drive-letter constant through ``os.pathsep`` join or split."""
    lines = []
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call) or _call_name(node.func) not in {"join", "split"}:
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        pieces = []
        if _is_pathsep(func.value):
            pieces.extend(node.args)
        elif _call_name(func) == "join" and node.args and _is_pathsep(node.args[0]):
            pieces.extend(node.args[1:])
        else:
            continue
        for piece in pieces:
            seq = piece.elts if isinstance(piece, (ast.List, ast.Tuple)) else [piece]
            if any(_drive_letter(item) for item in seq):
                lines.append(node.lineno)
    return lines


def drive_letter_pathsep_problems(source: str, filename: str = "<test>") -> list[str]:
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return [f"{filename}:{exc.lineno}: does not parse ({exc.msg})"]
    problems = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        hits = _joins_drive_on_pathsep(node)
        if hits and not _mentions_platform(node):
            for line in hits:
                problems.append(
                    f"{filename}:{line}: {node.name} joins a drive-letter path on "
                    "os.pathsep without checking os.name or sys.platform. "
                    "On Linux that splits C:\\other into 'C' and '\\other' "
                    "(PR #4). See tests/test_deps_path.py: use a native path off Windows."
                )
    return problems


OLD_PATH_TEST = '''
def test_prepends_and_dedupes(self):
    other = r"C:\\other"
    env = {"PATH": os.pathsep.join([r"C:\\other", scripts])}
    self.assertIn(r"C:\\other", env["PATH"].split(os.pathsep))
'''


class DriveLetterPathTests(unittest.TestCase):
    def test_the_old_ubuntu_failure_is_rejected(self):
        found = drive_letter_pathsep_problems(OLD_PATH_TEST, "tests/test_deps_path.py")
        self.assertEqual(len(found), 1, found)
        self.assertIn("PR #4", found[0])
        self.assertIn("test_deps_path.py", found[0])

    def test_a_platform_branch_is_accepted(self):
        src = '''
def test_prepends_and_dedupes(self):
    other = r"C:\\\\other" if os.name == "nt" else "other"
    env = {"PATH": os.pathsep.join([other, scripts])}
'''
        self.assertEqual(drive_letter_pathsep_problems(src), [])

    def test_current_tests_do_not_join_a_drive_letter_blindly(self):
        problems = []
        for path in sorted(TESTS.glob("test_*.py")):
            problems.extend(drive_letter_pathsep_problems(
                path.read_text(encoding="utf-8"), path.name))
        self.assertEqual(problems, [])


if __name__ == "__main__":
    unittest.main()
