"""Scope guard for the "docstring-only" pull-request label.

The label skips a library's test suites on a pull request that changes only
docstrings, and keeps the library's docstring doctests. Skipping is safe only
when no code changed, so this guard runs whenever the label is present and
fails unless every changed file is a modified Python file whose code is
identical before and after once docstrings are removed.

For each changed file, it parses the merge-base and head versions with `ast`,
removes the docstring from the module and from every class and function, and
compares `ast.dump` of the two trees. Comments aren't part of the AST, so a
comment-only edit passes too. The guard fails on any of the following:

  - A file that isn't `.py`.
  - An added, deleted, renamed, copied, or type-changed file.
  - A file that doesn't parse on either side.
  - Any AST difference after docstrings are removed.

It can't see a test that asserts on `__doc__` text. The postmerge build is the
backstop for that.

Usage:
  python ci/ray_ci/doc/check_docstring_only.py [--base REF]

Without --base, it fetches the pull request's base branch and diffs against
its merge-base with HEAD.
"""

import argparse
import ast
import os
import subprocess
import sys
from typing import List, Optional, Tuple

_DOCSTRING_OWNERS = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


class _DocstringStripper(ast.NodeTransformer):
    def generic_visit(self, node: ast.AST) -> ast.AST:
        super().generic_visit(node)
        if isinstance(node, _DOCSTRING_OWNERS) and _has_docstring(node.body):
            node.body = node.body[1:] or [ast.Pass()]
        return node


def _has_docstring(body: List[ast.stmt]) -> bool:
    return (
        bool(body)
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    )


def code_fingerprint(source: str, filename: str) -> str:
    """Return `ast.dump` of `source` with every docstring removed."""
    tree = ast.parse(source, filename=filename)
    return ast.dump(_DocstringStripper().visit(tree))


def check_file(path: str, old: str, new: str) -> Optional[str]:
    """Return why `path` isn't docstring-only, or None if it is."""
    try:
        before = code_fingerprint(old, path)
        after = code_fingerprint(new, path)
    except SyntaxError as e:
        return f"doesn't parse ({e.msg}, line {e.lineno})"
    if before != after:
        return "changes code, not only docstrings or comments"
    return None


def check_changes(
    changes: List[Tuple[str, str]], read_old, read_new
) -> List[Tuple[str, str]]:
    """Return (path, reason) for every change that isn't docstring-only.

    `changes` holds (status, path) pairs from `git diff --name-status`.
    `read_old` and `read_new` return a path's contents at the base and head.
    """
    failures = []
    for status, path in changes:
        if status != "M":
            failures.append((path, f"git status {status}; only modified files pass"))
        elif not path.endswith(".py"):
            failures.append((path, "isn't a Python file"))
        else:
            reason = check_file(path, read_old(path), read_new(path))
            if reason:
                failures.append((path, reason))
    return failures


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True)


def _resolve_base() -> str:
    base_branch = os.environ.get("BUILDKITE_PULL_REQUEST_BASE_BRANCH", "master")
    # Same fetch the docs example opt-in uses: a clone with a restricted
    # refspec may never create a local origin/<base>.
    subprocess.check_call(["git", "fetch", "-q", "origin", base_branch])
    return _git("merge-base", "FETCH_HEAD", "HEAD").strip()


def _parse_name_status(output: str) -> List[Tuple[str, str]]:
    changes = []
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.split("\t")
        # Renames and copies carry a similarity score, such as R100, and two
        # paths. Report them by the new path.
        changes.append((fields[0][0], fields[-1]))
    return changes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", help="Diff HEAD against this ref.")
    args = parser.parse_args()

    base = args.base or _resolve_base()
    changes = _parse_name_status(
        _git("diff", "--name-status", "--find-renames", base, "HEAD")
    )
    if not changes:
        print("docstring-only guard: no changed files; failing closed.")
        return 1

    failures = check_changes(
        changes,
        read_old=lambda path: _git("show", f"{base}:{path}"),
        read_new=lambda path: _git("show", f"HEAD:{path}"),
    )
    if failures:
        print(
            "docstring-only guard: this PR changes more than docstrings, so the "
            '"docstring-only" label can\'t skip its tests. Remove the label and '
            "push a new commit, because a rebuild replays the old label set."
        )
        for path, reason in failures:
            print(f"  {path}: {reason}")
        return 1

    print(f"docstring-only guard: {len(changes)} file(s), docstrings only.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
