"""Every function must only use names that exist.

This test exists because the same failure has now happened three times, each time reaching
production and each time invisible:

  · `message` left behind in the websocket's status log after its signature changed — the socket
    crash-looped on every connect while the menu simply said "reconnecting".
  · `replacing` used in the add-account handler but never assigned, because a patch wrote its
    second half and not its first.
  · `lang` lost from the same handler during the folder rescope — adding any account raised
    NameError, and since python-telegram-bot logs handler exceptions rather than surfacing them,
    the chat just stayed silent.

None of them were caught by anything. Import succeeds, because a NameError inside a function body
only fires when that line runs, and the lines in question run on a screen no unit test opens.

A full type checker would find these and more, but this costs nothing to run and targets the exact
shape that keeps escaping: a name read in a function that is neither assigned there, nor a
parameter, nor visible at module level.
"""

from __future__ import annotations

import ast
import builtins
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "mexc_copy_bot"
sys.path.insert(0, str(SRC.parent))


def module_level_names(tree: ast.Module) -> set[str]:
    """Names visible everywhere in the file.

    Only top-level statements count. Walking the whole tree instead would collect assignments made
    inside functions, and then a variable defined in one method would excuse its use in another —
    which is exactly the mistake this test was written to catch, so the first version of it found
    nothing at all.
    """
    names = set(dir(builtins))

    def collect(body):
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(node.name)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    names.add((alias.asname or alias.name).split(".")[0])
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    for inner in ast.walk(target):
                        if isinstance(inner, ast.Name):
                            names.add(inner.id)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names.add(node.target.id)
            elif isinstance(node, (ast.If, ast.Try)):
                # Imports guarded by a conditional or a try still land at module level.
                collect(node.body)
                collect(getattr(node, "orelse", []))
                collect(getattr(node, "handlers", []))
                collect(getattr(node, "finalbody", []))
            elif isinstance(node, ast.ExceptHandler):
                collect(node.body)

    collect(tree.body)
    return names


def bound_in(fn: ast.AST) -> set[str]:
    """Everything visible inside this function: its parameters and anything it assigns.

    Nested functions get their enclosing scope's names too, which is why the walk collects from
    the whole subtree rather than one level.
    """
    bound: set[str] = {"self", "cls"}
    for node in ast.walk(fn):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            args = node.args
            for group in (args.posonlyargs, args.args, args.kwonlyargs):
                bound.update(a.arg for a in group)
            if args.vararg:
                bound.add(args.vararg.arg)
            if args.kwarg:
                bound.add(args.kwarg.arg)
            if not isinstance(node, ast.Lambda):
                bound.add(node.name)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            bound.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                bound.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            bound.update(node.names)
    return bound


FUNCTION = (ast.FunctionDef, ast.AsyncFunctionDef)


def outermost_functions(tree: ast.Module) -> list[ast.AST]:
    """Functions not nested inside another function.

    A closure reads its enclosing scope, so checking one on its own reports every captured
    variable as undefined. Checking only the outermost function avoids that without losing
    coverage: its own walk already includes everything the nested ones do.
    """
    nested = set()
    for node in ast.walk(tree):
        if isinstance(node, FUNCTION):
            for inner in ast.walk(node):
                if inner is not node and isinstance(inner, FUNCTION):
                    nested.add(id(inner))
    return [n for n in ast.walk(tree) if isinstance(n, FUNCTION) and id(n) not in nested]


def undefined_names(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    module_names = module_level_names(tree)
    found: list[str] = []

    for fn in outermost_functions(tree):
        bound = bound_in(fn) | module_names
        for node in ast.walk(fn):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                if node.id not in bound:
                    found.append(f"{path.name}:{node.lineno} {fn.name}() uses {node.id!r}")
    return found


def test_no_function_reads_a_name_that_does_not_exist():
    problems: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        problems.extend(undefined_names(path))
    assert not problems, "undefined names:\n  " + "\n  ".join(sorted(set(problems)))
