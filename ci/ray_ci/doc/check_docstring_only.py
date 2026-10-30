"""Scope guard for the "docstring-only" pull-request label.

The label skips a library's test suites on a pull request that changes only
docstrings, and keeps the library's docstring doctests. Skipping is safe only
when no code changed, so this guard runs whenever the label is present and
fails unless every changed file is a modified Python or Cython file whose code
is identical before and after once docstrings are removed.

For a Python file, `.py` or `.pyi`, it parses the merge-base and head versions
with `ast`, removes the docstring from the module and from every class and
function, and compares `ast.dump` of the two trees. Comments aren't part of the
AST, so a comment-only edit passes too.

`ast` can't parse Cython, so for a `.pyx`, `.pxi`, or `.pxd` file it compares
the two versions' token streams from `tokenize` instead. `tokenize` reads
`cdef` and the other Cython keywords as ordinary names, and the comparison
keeps every one. It drops comments, blank lines, and line breaks inside
brackets, and removes a
docstring only where Python allows one: a string that is the first statement
of the file or of the indented body of a `def`, `class`, `cdef` or `cpdef`
function, or `cdef class`. Two Cython constructs look like comments or
docstrings but are code, and the guard keeps both:

  - A `# cython:` or `# distutils:` comment sets compiler directives or build
    options, such as `embedsignature`.
  - A string in a `cdef extern from` block is verbatim C or C++ that Cython
    compiles into the module.

The token comparison is stricter than the AST one. It treats replacing a
docstring-only body with `pass` as a code change, and it doesn't remove a
docstring written on the same line as its `def`. The guard fails on any of
the following:

  - A file that isn't Python or Cython.
  - An added, deleted, renamed, copied, or type-changed file.
  - A file that doesn't parse or tokenize on either side.
  - Any difference after docstrings are removed.

It can't see a test that asserts on `__doc__` text. The postmerge build is the
backstop for that.

Usage:
  python ci/ray_ci/doc/check_docstring_only.py [--base REF]

Without --base, it fetches the pull request's base branch and diffs against
its merge-base with HEAD.
"""

import argparse
import ast
import io
import os
import re
import subprocess
import sys
import tokenize
from typing import Iterator, List, Optional, Tuple

_DOCSTRING_OWNERS = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)

_PYTHON_SUFFIXES = (".py", ".pyi")
_CYTHON_SUFFIXES = (".pyx", ".pxi", ".pxd")

# Comments that set Cython compiler directives or distutils build options. They
# change the compiled module, so the Cython comparison keeps them.
_BUILD_COMMENT = re.compile(r"#\s*(cython|distutils)\s*:")
# A string prefix that makes a literal something other than a docstring: an
# f-string is code, and Python doesn't treat a bytes literal as a docstring.
_NOT_DOCSTRING_PREFIX = re.compile(r"[a-zA-Z]*[fFbB]")
_BLOCK_TOKENS = (tokenize.INDENT, tokenize.DEDENT, tokenize.ENDMARKER)


class _DocstringStripper(ast.NodeTransformer):
    def generic_visit(self, node: ast.AST) -> ast.AST:
        super().generic_visit(node)
        if isinstance(node, _DOCSTRING_OWNERS) and _has_docstring(node.body):
            node.body = node.body[1:]
            # A module body can be empty, so an empty file and a docstring-only
            # file compare equal. A class or function body can't.
            if not node.body and not isinstance(node, ast.Module):
                node.body = [ast.Pass()]
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


def _cython_lines(source: str) -> Iterator[List[tokenize.TokenInfo]]:
    """Yield the significant tokens of `source`, one list per logical line.

    INDENT, DEDENT, ENDMARKER, and each build comment come as one-token lists.
    Other comments and non-logical line breaks are dropped.
    """
    line = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.COMMENT:
            if _BUILD_COMMENT.match(tok.string):
                yield [tok]
        elif tok.type == tokenize.NEWLINE or tok.type in _BLOCK_TOKENS:
            if line:
                yield line
                line = []
            if tok.type != tokenize.NEWLINE:
                yield [tok]
        elif tok.type != tokenize.NL:
            line.append(tok)
    if line:
        yield line


def _is_docstring(line: List[tokenize.TokenInfo]) -> bool:
    return all(
        tok.type == tokenize.STRING and not _NOT_DOCSTRING_PREFIX.match(tok.string)
        for tok in line
    )


def _owns_docstring(line: List[tokenize.TokenInfo]) -> bool:
    """Whether `line` is a header whose indented body can open with a docstring."""
    words = [tok.string for tok in line]
    if words[-1] != ":":
        return False
    if words[0] in ("def", "class", "async"):
        return True
    # A cdef or cpdef function has a parameter list and a cdef class has the
    # class keyword. A `cdef:` block and a `cdef extern from` block have
    # neither, and a string in an extern block is verbatim C code.
    return words[0] in ("cdef", "cpdef") and ("class" in words or "(" in words)


def cython_fingerprint(source: str) -> str:
    """Return the tokens of `source` with comments and docstrings removed."""
    kept = []
    at_file_start = True
    after_header = False
    in_docstring_slot = False
    for line in _cython_lines(source):
        first = line[0]
        if first.type == tokenize.COMMENT:
            # A build comment leaves the docstring slot open: it sits above a
            # module docstring.
            kept.append(first.string)
            continue
        if first.type in _BLOCK_TOKENS:
            in_docstring_slot = first.type == tokenize.INDENT and after_header
            after_header = False
            kept.append(tokenize.tok_name[first.type])
            continue
        if (at_file_start or in_docstring_slot) and _is_docstring(line):
            at_file_start = in_docstring_slot = False
            continue
        at_file_start = in_docstring_slot = False
        after_header = _owns_docstring(line)
        kept.append(" ".join(tok.string for tok in line))
    return "\n".join(kept)


def check_file(path: str, old: str, new: str) -> Optional[str]:
    """Return why `path` isn't docstring-only, or None if it is."""
    try:
        if path.endswith(_CYTHON_SUFFIXES):
            before = cython_fingerprint(old)
            after = cython_fingerprint(new)
        else:
            before = code_fingerprint(old, path)
            after = code_fingerprint(new, path)
    except SyntaxError as e:
        return f"doesn't parse ({e.msg}, line {e.lineno})"
    except tokenize.TokenError as e:
        return f"doesn't tokenize ({e.args[0]})"
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
        elif not path.endswith(_PYTHON_SUFFIXES + _CYTHON_SUFFIXES):
            failures.append((path, "isn't a Python or Cython file"))
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
