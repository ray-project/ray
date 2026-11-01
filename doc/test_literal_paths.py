"""Fail when a config entry names a doc path that no longer exists.

A few config surfaces name documentation files by literal path, and in each of
them a path that stops existing is silently ignored instead of raising an error:

- An `exclude` list in a BUILD file under doc/. A missing path in a glob's
  `exclude` excludes nothing, so a moved page silently rejoins the target it
  was excluded from. (A missing path in `srcs`, `main`, or `files` already
  fails the Bazel build, so those aren't checked here.)
- .github/CODEOWNERS. A missing path matches no file, so the owning team stops
  being requested on the moved page.
- The .buildkite/*.rules.txt files. A missing path matches no changed file, so
  the moved file falls through to a later, usually broader, rule.

Renaming or moving a page, or converting it from .rst to .md, changes its path
without touching any of these, so this check runs on every pull request. It
only checks entries under doc/ that contain no wildcard. A glob pattern that
matches nothing is left alone, because a pattern is often written ahead of the
files it covers.

If a path is absent on purpose, for example a rule written ahead of the file it
routes, add it to ALLOWLIST below with a comment explaining why.
"""

import ast
import fnmatch
import os
import subprocess
import sys
from typing import Iterator, List, Tuple

# Entries that may name a path that doesn't exist yet. Keep each one commented.
ALLOWLIST = set()

_WILDCARD = set("*?[")


def _repo_root() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "--show-toplevel"], text=True
    ).strip()


def _is_literal(path: str) -> bool:
    return not (_WILDCARD & set(path))


def _build_excludes(root: str) -> Iterator[Tuple[str, int, str]]:
    """Yield (BUILD file, line, repo-relative path) for each literal exclude."""
    out = subprocess.check_output(
        [
            "git",
            "ls-files",
            "doc/BUILD",
            "doc/BUILD.bazel",
            "doc/**/BUILD",
            "doc/**/BUILD.bazel",
        ],
        cwd=root,
        text=True,
    )
    for build in out.split():
        with open(os.path.join(root, build)) as f:
            tree = ast.parse(f.read(), filename=build)
        base = os.path.dirname(build)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            for kw in node.keywords:
                if kw.arg != "exclude" or not isinstance(kw.value, ast.List):
                    continue
                for elt in kw.value.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        if _is_literal(elt.value):
                            yield build, elt.lineno, os.path.join(base, elt.value)


def _codeowners(root: str) -> Iterator[Tuple[str, int, str]]:
    name = ".github/CODEOWNERS"
    with open(os.path.join(root, name)) as f:
        for lineno, line in enumerate(f, 1):
            fields = line.split("#", 1)[0].split()
            if fields and fields[0].startswith("/doc/") and _is_literal(fields[0]):
                yield name, lineno, fields[0].lstrip("/")


def _rules(root: str) -> Iterator[Tuple[str, int, str]]:
    rules_dir = os.path.join(root, ".buildkite")
    for fname in sorted(os.listdir(rules_dir)):
        if not fnmatch.fnmatch(fname, "*.rules.txt"):
            continue
        name = f".buildkite/{fname}"
        with open(os.path.join(rules_dir, fname)) as f:
            for lineno, line in enumerate(f, 1):
                entry = line.split("#", 1)[0].strip()
                if entry.startswith("doc/") and _is_literal(entry):
                    yield name, lineno, entry


def find_missing(root: str) -> List[Tuple[str, int, str]]:
    missing = []
    for source in (_build_excludes, _codeowners, _rules):
        for where, lineno, path in source(root):
            if path.rstrip("/") in ALLOWLIST:
                continue
            if not os.path.exists(os.path.join(root, path)):
                missing.append((where, lineno, path))
    return missing


def main() -> int:
    missing = find_missing(_repo_root())
    if not missing:
        print(
            "Every literal doc path in BUILD excludes, CODEOWNERS, and rules files exists."
        )
        return 0
    print(
        "These entries name doc paths that don't exist. A rename, move, or "
        ".rst-to-.md conversion probably left them behind, and each one is "
        "now silently ignored. Point each entry at the file's new path, or "
        "delete it if the file is gone. If a path is absent on purpose, add "
        "it to ALLOWLIST in doc/test_literal_paths.py with a comment.",
        file=sys.stderr,
    )
    for where, lineno, path in missing:
        print(f"  {where}:{lineno}: {path}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
