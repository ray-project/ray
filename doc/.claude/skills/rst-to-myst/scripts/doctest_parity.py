#!/usr/bin/env python3
"""Prove a MyST conversion keeps doctest coverage, using pytest-sphinx's own parser.

Ray's doctest targets (the `doctest` macro in bazel/python.bzl) run pytest with Ray's
pytest-sphinx fork. That plugin yields ONE pytest item per file, so
`pytest --collect-only` counts files, not blocks. What executes is the list of
doctest.Example objects `docstring2examples` builds per file: one per testcode block
(paired with its testoutput), minus blocks whose `:skipif:` is true. testsetup and
testcleanup are parsed but never run. doctest sections are parsed too, and never
run by the plugin.

The macro also passes --doctest-glob='*.md', so every .md gets a second runner that
no .rst ever had: pytest's stock doctest parser, which reads the raw file and runs
every `>>>` prompt in it, in any fence. It takes expected output up to the next blank
line, so a closing fence directly under an output line becomes part of that output
and fails the test.

Parity mode (default): for each converted .md, fetch the .rst it replaced from a git
ref and assert that both parse to identical sections (directive, body, :options:,
:skipif:) and identical examples. It also runs the stock parser over every .md
and fails any example whose expected output holds a bare fence line. Exits 1 on
either.

    python3 $SKILL/scripts/doctest_parity.py doc/source/ray-core/actors.md [...]
    python3 $SKILL/scripts/doctest_parity.py --base upstream/master <files>

Census mode: count examples for every .md/.rst under a directory, at the working tree.
Use it for a before/after baseline of a whole doctest target.

    python3 $SKILL/scripts/doctest_parity.py --census doc/source/ray-core
    python3 $SKILL/scripts/doctest_parity.py --selftest

Run from the repo root. Needs pytest and Ray's plugin, no Bazel or Ray install:
    uv venv dt && uv pip install -p dt/bin/python pytest \
        "pytest-sphinx @ git+https://github.com/ray-project/pytest-sphinx"
"""
import argparse
import doctest
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

try:
    import pytest_sphinx as ps
except ImportError:
    ps = None  # --selftest needs only the stock parser

# A bare fence line, as a closing ``` or ::: is. An output line like ```python isn't.
FENCE_RE = re.compile(r"\s*(`{3,}|:{3,})\s*")


def stock_fence_leaks(text):
    """Return (examples, lines) for the stock doctest parser that --doctest-glob
    runs on a raw .md: how many examples it builds, and the 1-based line of each
    `>>>` whose expected output swallowed a fence. A `+SKIP` example's output is
    never compared, so its leak can't fail and isn't reported."""
    examples = doctest.DocTestParser().get_examples(text)
    return len(examples), [
        e.lineno + 1
        for e in examples
        if not e.options.get(doctest.SKIP)
        and any(FENCE_RE.fullmatch(l) for l in e.want.splitlines())
    ]


def parse(text, suffix):
    if ps is None:
        sys.exit(
            "pytest_sphinx not importable; see the install line in this script's docstring"
        )
    syntax = ps._FILE_EXTENSION_TO_SYNTAX[suffix]
    secs = [
        (s.directive.name.lower(), s.body, s.skipif_expr, s.options)
        for s in ps.get_sections(text, syntax)
    ]
    examples = [
        (e.source, e.want, e.options)
        for e in ps.docstring2examples(text, syntax=syntax)
    ]
    return secs, examples


def census(root):
    total = files = 0
    for p in sorted(q for ext in ("md", "rst") for q in Path(root).rglob(f"*.{ext}")):
        try:
            secs, ex = parse(p.read_text(), p.suffix)
        except Exception as e:  # a collection error fails the whole target
            print(f"ERROR  {p}: {type(e).__name__}: {e}")
            continue
        if secs:
            kinds = dict(Counter(s[0] for s in secs))
            print(f"{len(ex):4d}  {p}  {kinds}")
            total += len(ex)
            files += bool(ex)
    print(f"TOTAL examples={total} files_with_examples={files}")
    print("Count only pages a doctest_each() call in doc/BUILD.bazel lists.")


def parity(paths, base):
    bad = 0
    for md in map(Path, paths):
        try:
            n, leaks = stock_fence_leaks(md.read_text())
        except ValueError as e:  # the stock collector errors on the whole file too
            n, leaks = 0, None
            print(f"ERROR {md}: stock doctest parser: {e}")
            bad += 1
        if leaks:
            bad += 1
            print(
                f"FENCE {md}: closing fence read as expected output of the `>>>` on "
                f"line(s) {leaks}. Put a blank line before the closing fence"
            )
        elif n:
            print(
                f"INFO  {md}: stock parser runs {n} `>>>` example(s). Nothing ran "
                f"them as .rst, so run the target locally"
            )
        rst = md.with_suffix(".rst")
        try:
            before = subprocess.run(
                ["git", "show", f"{base}:{rst}"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        except subprocess.CalledProcessError:
            print(f"SKIP  {md}: no {rst} at {base}")
            continue
        (sa, ea), (sb, eb) = parse(before, ".rst"), parse(md.read_text(), ".md")
        ok = sa == sb and ea == eb
        bad += not ok
        print(
            f"{'OK  ' if ok else 'DIFF'}  {md}  examples {len(ea)} -> {len(eb)}, "
            f"sections {len(sa)} -> {len(sb)}"
        )
        if not ok:
            for i, (a, b) in enumerate(zip(sa, sb)):
                if a != b:
                    print(f"      first differing section #{i}: {a[0]} vs {b[0]}")
                    break
    return bad


SELFTEST = [  # (name, markdown, expected leak lines)
    ("fence swallowed", "```{doctest}\n>>> 1\n1\n```\n", [2]),
    ("blank line ends output", "```{doctest}\n>>> 1\n1\n\n```\n", []),
    ("colon fence swallowed", ":::{note}\n>>> 1\n1\n:::\n", [2]),
    (
        "indented fence in list item",
        "1. Run:\n\n   ```python\n   >>> 1\n   1\n   ```\n",
        [4],
    ),
    ("opening fence isn't bare", ">>> print('```python')\n```python\n\n", []),
    ("inline backticks in output", ">>> s\n'``` x'\n", []),
    (
        "skipped example never compares",
        "```{doctest}\n>>> f()  # doctest: +SKIP\n1\n```\n",
        [],
    ),
]


def selftest():
    bad = 0
    for name, text, want in SELFTEST:
        got = stock_fence_leaks(text)[1]
        if got != want:
            bad += 1
            print(f"FAIL {name}: want {want}, got {got}")
    print(f"{len(SELFTEST) - bad}/{len(SELFTEST)} cases pass")
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("files", nargs="*", help="converted .md paths (parity mode)")
    ap.add_argument(
        "--base", default="upstream/master", help="git ref holding the .rst"
    )
    ap.add_argument("--census", metavar="DIR", help="count examples under DIR instead")
    ap.add_argument("--selftest", action="store_true", help="run the built-in cases")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(1 if selftest() else 0)
    if a.census:
        census(a.census)
    elif a.files:
        sys.exit(1 if parity(a.files, a.base) else 0)
    else:
        ap.error("pass converted .md files, or --census DIR")


if __name__ == "__main__":
    main()
