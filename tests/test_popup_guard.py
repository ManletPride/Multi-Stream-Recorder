"""One override-redirect window, and no geometry fight with the compositor.

PR #7 built the tooltip, then bound ``<Configure>`` and put ``wm_geometry``
back after Hyprland resized it. That loses the race (the menu is sometimes
still 1100x760, sometimes the right size at the far left). PR #8 deleted
that and gave the window class ``TkTooltip``, which a user float rule can
skip. A second override-redirect window would be class ``Toplevel`` and get
resized with the dialogs.
"""
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MSR = ROOT / "msr"


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ""


def _functions(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


def _overrideredirect_true(fn: ast.AST) -> list[int]:
    lines = []
    for node in ast.walk(fn):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node is not fn:
            continue
        if not isinstance(node, ast.Call) or _call_name(node.func) != "wm_overrideredirect":
            continue
        if node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value is True:
            lines.append(node.lineno)
    return lines


def _toplevel_uses_popup_class(fn: ast.AST) -> bool:
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call) or _call_name(node.func) != "Toplevel":
            continue
        for kw in node.keywords:
            if kw.arg != "class_":
                continue
            value = kw.value
            if isinstance(value, ast.Name) and value.id == "POPUP_WM_CLASS":
                return True
    return False


def _binds_configure(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _call_name(node.func) not in {"bind", "bind_all"}:
            continue
        if node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == "<Configure>":
            return True
    return False


def _calls_wm_geometry(tree: ast.AST) -> bool:
    return any(isinstance(node, ast.Call) and _call_name(node.func) == "wm_geometry" for node in ast.walk(tree))


def popup_problems(source: str, filename: str) -> list[str]:
    tree = ast.parse(source)
    problems = []
    for fn in _functions(tree):
        for line in _overrideredirect_true(fn):
            if filename != "tooltip.py" or fn.name != "popup":
                problems.append(
                    f"{filename}:{line}: {fn.name} calls wm_overrideredirect(True). "
                    "The only override-redirect window is msr.tooltip.popup "
                    "(class TkTooltip). Another one is resized by Hyprland (PR #8)."
                )
            elif not _toplevel_uses_popup_class(fn):
                problems.append(
                    f"{filename}:{line}: popup() must create the Toplevel with "
                    "class_=POPUP_WM_CLASS. A plain Toplevel is class Toplevel, "
                    "which a Hyprland float rule would also apply to dialogs (PR #8)."
                )
    if _binds_configure(tree) and _calls_wm_geometry(tree):
        problems.append(
            f"{filename}: binds <Configure> and calls wm_geometry. "
            "Putting the size back loses the race with Hyprland "
            "(PR #7, removed in PR #8). Match window class TkTooltip instead "
            "(msr.tooltip.POPUP_WM_CLASS)."
        )
    return problems


def scan_tree(folder: Path) -> list[str]:
    found = []
    for path in sorted(folder.glob("*.py")):
        found.extend(popup_problems(path.read_text(encoding="utf-8"), path.name))
    return found


# The shape of msr/tooltip.py at 5dbf45e (PR #7): geometry put back from <Configure>.
PR7_TOOLTIP = """
def popup(parent):
    tw = tk.Toplevel(parent)
    tw.wm_overrideredirect(True)
    def on_configure(_event):
        tw.wm_geometry(wanted)
    tw.bind("<Configure>", on_configure, add="+")
    tw.wm_geometry(f"{width}x{height}+{x}+{y}")
"""


class PopupGuardTests(unittest.TestCase):
    def test_the_hyprland_geometry_fight_is_rejected(self):
        found = popup_problems(PR7_TOOLTIP, "tooltip.py")
        self.assertTrue(any("class_=POPUP_WM_CLASS" in item for item in found), found)
        self.assertTrue(any("wm_geometry" in item for item in found), found)

    def test_a_second_override_redirect_window_is_rejected(self):
        src = """
def toast(parent):
    tw = tk.Toplevel(parent, class_=POPUP_WM_CLASS)
    tw.wm_overrideredirect(True)
"""
        found = popup_problems(src, "gui.py")
        self.assertEqual(len(found), 1, found)
        self.assertIn("msr.tooltip.popup", found[0])

    def test_the_tree_has_one_tooltip(self):
        self.assertEqual(scan_tree(MSR), [])


if __name__ == "__main__":
    unittest.main()
