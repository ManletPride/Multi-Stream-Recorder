"""Tests that failed because they read a live clock, the kernel, or today's date.

Each one was green until CI timing disagreed with the author's machine:

* PR #19: ``_proc_state`` straight after SIGSTOP still saw the process running.
* 2b636de: ``@mock.patch("msr.iometer.time.monotonic")`` with ``side_effect = [1.0, 2.0]``
  raised StopIteration once another thread called ``time.monotonic``.
* dba84a5: the Twitch validate-401 test left ``_last_validate`` at 0, and a fresh
  runner's ``time.monotonic()`` was still under ``VALIDATE_EVERY``, so the refresh
  never ran.
* 86e7514: the Health bar fixture pinned yt-dlp at ``2026.10.30``. Fourteen days later
  the line was a stale-version warning instead of "Health OK".
"""
import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTS = ROOT / "tests"
DATE_VERSION = re.compile(r"^\d{4}\.\d{1,2}\.\d{1,2}$")


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _functions(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


def direct_proc_state_calls(source: str, filename: str = "tests/test_clip_audio.py") -> list[str]:
    """``_proc_state`` outside ``_wait_proc_state``. SIGSTOP is not visible yet."""
    tree = ast.parse(source)
    problems = []
    for fn in _functions(tree):
        if fn.name == "_wait_proc_state":
            continue
        for node in ast.walk(fn):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node is not fn:
                continue
            if isinstance(node, ast.Call) and _call_name(node.func) == "_proc_state":
                problems.append(
                    f"{filename}:{node.lineno}: {fn.name} reads _proc_state immediately. "
                    "SIGSTOP and SIGCONT show up in /proc a moment later (PR #19, "
                    "'R' not found in ('T', 't')). Poll with _wait_proc_state."
                )
    return problems


def _is_time_module_patch(call: ast.Call) -> bool:
    func = call.func
    if isinstance(func, ast.Attribute) and func.attr == "patch":
        if call.args and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str):
            target = call.args[0].value
            return target.endswith("time.monotonic") or target.endswith("time.time")
    if isinstance(func, ast.Attribute) and func.attr == "object" and len(call.args) >= 2:
        owner, attr = call.args[0], call.args[1]
        if (
            isinstance(attr, ast.Constant)
            and attr.value in {"monotonic", "time"}
            and isinstance(owner, ast.Attribute)
            and owner.attr == "time"
        ):
            return True
    return False


def _side_effect_is_finite(node: ast.AST) -> bool:
    return isinstance(node, (ast.List, ast.Tuple))


def finite_time_patches(source: str, filename: str) -> list[str]:
    """A finite ``side_effect`` on ``time.monotonic`` / ``time.time``."""
    tree = ast.parse(source)
    problems = []
    seen = set()

    def report(line: int, where: str) -> None:
        if line in seen:
            return
        seen.add(line)
        problems.append(
            f"{filename}:{line}: {where} patches time.monotonic or time.time with a "
            "finite side_effect. Other threads exhaust the list and the test dies "
            "with StopIteration (2b636de). Patch the module's own clock "
            "(msr.iometer._monotonic) or use an inexhaustible callable "
            "(tests/test_iometer.py _clock)."
        )

    for fn in _functions(tree):
        patches = []
        for dec in fn.decorator_list:
            if isinstance(dec, ast.Call):
                patches.append(dec)
        for node in ast.walk(fn):
            if isinstance(node, ast.With):
                for item in node.items:
                    if isinstance(item.context_expr, ast.Call):
                        patches.append(item.context_expr)
        time_patches = [call for call in patches if _is_time_module_patch(call)]
        if not time_patches:
            continue
        for call in time_patches:
            for kw in call.keywords:
                if kw.arg == "side_effect" and _side_effect_is_finite(kw.value):
                    report(call.lineno, fn.name)
        # The iometer tests assigned the list after a string patch() decorator.
        string_patch = any(
            isinstance(call.func, ast.Attribute) and call.func.attr == "patch" for call in time_patches
        )
        if not string_patch:
            continue
        for node in ast.walk(fn):
            if isinstance(node, ast.Assign) and _side_effect_is_finite(node.value):
                for target in node.targets:
                    if isinstance(target, ast.Attribute) and target.attr == "side_effect":
                        report(node.lineno, fn.name)
    return problems


def validate_uptime_problems(source: str, filename: str = "tests/test_twitch_token_refresh.py") -> list[str]:
    tree = ast.parse(source)
    for fn in _functions(tree):
        if fn.name != "test_validate_401_refreshes_once":
            continue
        assigned = None
        called = None
        for node in ast.walk(fn):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Attribute) and target.attr == "_last_validate":
                        assigned = node.lineno
            if isinstance(node, ast.Call) and _call_name(node.func) == "_ensure_token":
                called = node.lineno
        if called is None:
            return []
        if assigned is None or assigned > called:
            return [
                f"{filename}:{fn.lineno}: test_validate_401_refreshes_once calls "
                "_ensure_token before setting _last_validate. On a freshly booted "
                "runner time.monotonic() is under VALIDATE_EVERY, so the default 0 "
                "skips the refresh (dba84a5). Set lst._last_validate = -VALIDATE_EVERY."
            ]
    return []


def pinned_health_ytdlp(source: str, filename: str = "tests/test_health_checks.py") -> list[str]:
    """A date-shaped yt-dlp literal in the Health bar fixture, compared with today."""
    tree = ast.parse(source)
    problems = []
    for cls in ast.walk(tree):
        if not isinstance(cls, ast.ClassDef) or cls.name != "ControllerTests":
            continue
        for fn in cls.body:
            if not isinstance(fn, ast.FunctionDef) or fn.name != "setUp":
                continue
            for node in ast.walk(fn):
                if not isinstance(node, ast.Dict):
                    continue
                for key, value in zip(node.keys, node.values):
                    if (
                        isinstance(key, ast.Constant)
                        and key.value == "yt-dlp"
                        and isinstance(value, ast.Constant)
                        and isinstance(value.value, str)
                        and DATE_VERSION.match(value.value)
                    ):
                        problems.append(
                            f"{filename}:{value.lineno}: ControllerTests pins yt-dlp at "
                            f"{value.value}. Health treats that as a release date, so "
                            "fourteen days later the bar is a stale warning instead of "
                            "Health OK (86e7514). Build the version from datetime.date.today()."
                        )
    return problems


OLD_PAUSE = """
def test_pause_stops_the_process_and_close_reaps_it(self):
    sink.pause()
    self.assertIn(_proc_state(pid), ("T", "t"))
    sink.resume()
    self.assertNotIn(_proc_state(pid), ("T", "t"))
"""

OLD_IOMETER = """
@mock.patch("msr.iometer.time.monotonic")
def test_msr_growth_on_third_tick(self, mono, disk, net):
    mono.side_effect = [1.0, 2.0]
    third = sampler.sample(msr_sizes={"a.ts": 3_000_000})
"""

OLD_VALIDATE = """
def test_validate_401_refreshes_once(self):
    api = OneTimeApi(expired=False)
    api.validate_401 = 1
    lst = self.listener(api)
    tok = lst._ensure_token()
"""

OLD_HEALTH = """
class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.versions = {"ffmpeg": "7", "streamlink": "8.6.2", "yt-dlp": "2026.10.30"}
"""


class TimingGuardTests(unittest.TestCase):
    def test_the_pause_test_polled_too_soon(self):
        found = direct_proc_state_calls(OLD_PAUSE)
        self.assertEqual(len(found), 2, found)
        self.assertIn("PR #19", found[0])

    def test_a_finite_monotonic_patch_is_rejected(self):
        found = finite_time_patches(OLD_IOMETER, "tests/test_iometer.py")
        self.assertEqual(len(found), 1, found)
        self.assertIn("msr.iometer._monotonic", found[0])
        allowed = '''
def test_session_expired_error_is_logged_at_most_once_an_hour(self):
    with mock.patch.object(platforms.time, "monotonic", side_effect=lambda: clock[0]):
        auth._log_auth_failure("x")
'''
        self.assertEqual(finite_time_patches(allowed, "tests/test_fishtank_offseason.py"), [])

    def test_validate_401_must_not_depend_on_uptime(self):
        found = validate_uptime_problems(OLD_VALIDATE)
        self.assertEqual(len(found), 1, found)
        self.assertIn("dba84a5", found[0])
        fixed = OLD_VALIDATE.replace(
            "lst = self.listener(api)\n    tok",
            "lst = self.listener(api)\n    lst._last_validate = -VALIDATE_EVERY\n    tok",
        )
        self.assertEqual(validate_uptime_problems(fixed), [])

    def test_a_pinned_health_release_date_is_rejected(self):
        found = pinned_health_ytdlp(OLD_HEALTH)
        self.assertEqual(len(found), 1, found)
        self.assertIn("2026.10.30", found[0])

    def test_the_suite_as_it_stands(self):
        problems = []
        for path in sorted(TESTS.glob("test_*.py")):
            text = path.read_text(encoding="utf-8")
            name = path.name
            problems.extend(direct_proc_state_calls(text, name))
            problems.extend(finite_time_patches(text, name))
            problems.extend(validate_uptime_problems(text, name))
            problems.extend(pinned_health_ytdlp(text, name))
        self.assertEqual(problems, [])


if __name__ == "__main__":
    unittest.main()
