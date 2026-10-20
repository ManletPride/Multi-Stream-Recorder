"""deps.ensure_interpreter_scripts_on_path: venv tools found without activation."""
import os
import tempfile
import unittest

from msr.deps import ensure_interpreter_scripts_on_path, interpreter_script_dirs


class InterpreterPathTests(unittest.TestCase):
    def _fake_python(self, root, sub=None):
        d = os.path.join(root, sub) if sub else root
        os.makedirs(d, exist_ok=True)
        exe = os.path.join(d, "python.exe")
        open(exe, "w").close()
        return exe

    def test_system_install_adds_scripts_dir(self):
        with tempfile.TemporaryDirectory() as root:
            exe = self._fake_python(root)
            os.makedirs(os.path.join(root, "Scripts"))
            dirs = interpreter_script_dirs(exe)
            self.assertEqual(dirs, [root, os.path.join(root, "Scripts")])

    def test_prepends_and_dedupes(self):
        with tempfile.TemporaryDirectory() as root:
            exe = self._fake_python(root, "Scripts")
            scripts = os.path.dirname(exe)
            # Drive-letter paths are one PATH entry only where ';' is the
            # separator. On POSIX, ':' would split "C:\other" into "C" and
            # "\other", which is the same split the OS itself would do.
            other = r"C:\other" if os.name == "nt" else os.path.join(root, "other")
            env = {"PATH": os.pathsep.join([other, scripts])}
            added = ensure_interpreter_scripts_on_path(env, exe)
            self.assertEqual(added, [])
            self.assertEqual(env["PATH"].split(os.pathsep), [scripts, other])

    def test_empty_path(self):
        with tempfile.TemporaryDirectory() as root:
            exe = self._fake_python(root)
            env = {}
            added = ensure_interpreter_scripts_on_path(env, exe)
            self.assertEqual(added, [root])
            self.assertEqual(env["PATH"], root)

    def test_missing_dirs_skipped(self):
        exe = os.path.join(tempfile.gettempdir(), "no_such_dir_msr", "python.exe")
        self.assertEqual(interpreter_script_dirs(exe), [])


if __name__ == "__main__":
    unittest.main()
