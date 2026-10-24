"""Guards for window chrome: no native popup without ``parent=``, and no Toplevel that
skips the themed title bar. Parses the sources (ast), so no window is ever opened."""
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NATIVE_MODULES = {"messagebox", "simpledialog", "filedialog", "colorchooser"}
NATIVE_FUNCTIONS = {
    "showinfo", "showwarning", "showerror", "askyesno", "askokcancel", "askquestion", "askretrycancel",
    "askyesnocancel", "askstring", "askinteger", "askfloat", "askopenfilename", "askopenfilenames",
    "asksaveasfilename", "askdirectory", "askcolor",
}
# A function that builds a Toplevel must also do one of these (themed caption / tooltip).
THEMED = {"apply_title_bar", "new_dialog", "present", "set_title_bar_dark", "overrideredirect", "wm_overrideredirect"}


def _call_name(node: ast.Call) -> str:
    func = node.func
    return func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")


def _module_of(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Attribute):
        owner = func.value
        return owner.attr if isinstance(owner, ast.Attribute) else getattr(owner, "id", "")
    return ""


def unparented_native_calls(source: str) -> list[int]:
    """Line numbers of messagebox/simpledialog/filedialog/colorchooser calls with no parent=."""
    bad = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node)
        native = _module_of(node) in NATIVE_MODULES or (isinstance(node.func, ast.Name) and name in NATIVE_FUNCTIONS)
        if native and not any(k.arg == "parent" for k in node.keywords):
            bad.append(node.lineno)
    return bad


def unthemed_toplevels(source: str) -> list[int]:
    """Line numbers of Toplevel(...) calls whose innermost function never themes the caption."""
    bad: list[int] = []

    def visit(node, scope_calls: set[str] | None, stack) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                names = {_call_name(n) for n in ast.walk(child) if isinstance(n, ast.Call)}
                visit(child, names, stack + [child])
                continue
            if isinstance(child, ast.Call) and _call_name(child) == "Toplevel":
                if scope_calls is None or not (scope_calls & THEMED):
                    bad.append(child.lineno)
            visit(child, scope_calls, stack)

    visit(ast.parse(source), None, [])
    return bad


def _sources():
    files = list(ROOT.glob("*.py")) + list((ROOT / "msr").glob("*.py"))
    return [(p, p.read_text(encoding="utf-8")) for p in files]


class CheckerSelfTests(unittest.TestCase):
    def test_native_popups_need_parent(self):
        self.assertEqual(unparented_native_calls("messagebox.showinfo('a', 'b')"), [1])
        self.assertEqual(unparented_native_calls("tkinter.messagebox.askyesno('a', 'b')"), [1])
        self.assertEqual(unparented_native_calls("x = simpledialog.askfloat('a', 'b', minvalue=1)"), [1])
        self.assertEqual(unparented_native_calls("showerror('a', 'b')"), [1])
        self.assertEqual(unparented_native_calls("filedialog.askdirectory(title='x')"), [1])
        self.assertEqual(unparented_native_calls("messagebox.showinfo('a', 'b', parent=root)"), [])
        self.assertEqual(unparented_native_calls("dialogs.show_info(root, 'a', 'b')"), [])

    def test_toplevels_need_the_themed_caption(self):
        bare = "def f(root):\n    win = tk.Toplevel(root)\n    win.title('x')\n"
        self.assertEqual(unthemed_toplevels(bare), [2])
        themed = "def f(root):\n    win = tk.Toplevel(root)\n    set_title_bar_dark(win)\n"
        self.assertEqual(unthemed_toplevels(themed), [])
        tip = "def f(w):\n    tw = tk.Toplevel(w)\n    tw.wm_overrideredirect(True)\n"
        self.assertEqual(unthemed_toplevels(tip), [])
        module_level = "win = tk.Toplevel(root)\n"
        self.assertEqual(unthemed_toplevels(module_level), [1])

    def test_a_themed_call_elsewhere_does_not_excuse_an_inner_function(self):
        src = (
            "def main():\n"
            "    set_title_bar_dark(root)\n"
            "    def dialog():\n"
            "        win = tk.Toplevel(root)\n"
            "    def themed():\n"
            "        win = tk.Toplevel(root)\n"
            "        apply_title_bar(win)\n"
        )
        self.assertEqual(unthemed_toplevels(src), [4])


class RepoGuards(unittest.TestCase):
    def test_every_native_popup_has_parent(self):
        problems = [f"{p.name}:{n}" for p, src in _sources() for n in unparented_native_calls(src)]
        self.assertEqual(problems, [], "messagebox/simpledialog/filedialog call without parent=")

    def test_every_toplevel_is_themed_or_a_tooltip(self):
        problems = [f"{p.name}:{n}" for p, src in _sources() for n in unthemed_toplevels(src)]
        self.assertEqual(problems, [], "Toplevel without the themed title bar")

    def test_gui_uses_the_themed_dialogs_not_native_ones(self):
        gui = (ROOT / "msr" / "gui.py").read_text(encoding="utf-8")
        self.assertNotIn("messagebox", gui)
        self.assertNotIn("simpledialog", gui)


if __name__ == "__main__":
    unittest.main()