"""No colour belongs in the code: every colour comes from msr/themes.py (the only file with hex values).
This test reads the source, so a new hard-coded colour fails here instead of showing up as a
dark-on-dark label in some theme."""
import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HEX = re.compile(r"#[0-9a-fA-F]{3,8}\b")
NAMED = {"white", "black", "gray", "grey", "red", "green", "blue", "yellow", "orange", "purple", "pink", "brown",
         "cyan", "magenta", "gold", "silver", "navy", "teal", "lime", "maroon", "crimson", "darkgray", "lightgray",
         "darkgrey", "lightgrey", "dimgray", "systembuttonface", "systemwindow", "systemwindowtext"}
COLOR_KWARGS = {"fg", "bg", "foreground", "background", "fill", "outline", "activebackground", "activeforeground",
                "selectbackground", "selectforeground", "insertbackground", "highlightbackground", "highlightcolor",
                "disabledforeground", "disabledbackground", "troughcolor", "fieldbackground", "bordercolor",
                "lightcolor", "darkcolor", "arrowcolor", "readonlybackground", "selectcolor"}
PACK_WORDS = {"both", "x", "y", "none"}  # pack(fill="both") is a layout word, not a colour
# file name -> text prefixes that may contain a colour; keep this tiny. The one entry is the how-to
# sample written next to the user's themes folder (documentation, never applied as a colour).
ALLOWED: dict[str, tuple[str, ...]] = {"theme_dialog.py": ('{\n  "name": "My theme"',)}


def literals(path: Path):
    """(line, text) for every colour-looking literal in a module."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    seen = set()
    for line, text in _raw(tree):
        if (line, text) not in seen:
            seen.add((line, text))
            yield line, text


def _raw(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            text = node.value
            if HEX.search(text) and not text.strip().startswith("#!"):
                yield node.lineno, text
            elif text.strip().lower() in NAMED:
                yield node.lineno, text
        elif isinstance(node, ast.keyword) and node.arg in COLOR_KWARGS:
            value = node.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str) and value.value \
                    and value.value not in PACK_WORDS:
                yield value.lineno, value.value


def scan(source_dir: Path) -> list[str]:
    found = []
    for path in sorted(source_dir.glob("*.py")):
        if path.name == "themes.py":
            continue
        allowed = ALLOWED.get(path.name, ())
        for line, text in literals(path):
            if not text.startswith(allowed):
                found.append(f"{path.name}:{line}: {text!r}")
    return found


class ColourLiteralGuardTests(unittest.TestCase):
    def test_no_colour_literals_outside_themes_py(self):
        self.assertEqual(scan(ROOT / "msr"), [], "hard-coded colour: add a token to msr/themes.py and use it")

    def test_the_scanner_finds_what_it_should(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            (folder / "bad.py").write_text(
                'import tkinter as tk\nw = tk.Label(fg="red")\nx = "#a1b2c3"\ny = {"k": "white"}\nz = "plain text"\n'
                'ok = tk.Label(fg=theme["fg"])\nh = "#!shebang"\n', encoding="utf-8")
            (folder / "themes.py").write_text('X = "#112233"\n', encoding="utf-8")  # the one allowed home
            found = scan(folder)
        self.assertEqual(len(found), 3, found)
        self.assertTrue(all(item.startswith("bad.py:") for item in found))

    def test_themes_py_is_the_only_home_for_hex_values(self):
        self.assertTrue(HEX.search((ROOT / "msr" / "themes.py").read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
