"""Keep the per-file prose doctest lists in doc/BUILD.bazel complete and live.

Doc prose is tested by `doctest_each()` over explicit lists of pages, not
directory globs, so that every target tests something. Each library (Core,
Data, RLlib, Serve, Train, and Tune) has its own list, and prose that no library
owns is in the team:none list. An explicit list can silently miss a page: a
runnable block added to an unlisted page runs nowhere, not even post-merge.
This check fails when:

- A page under doc/source has a runnable example but no doctest target names
  it. Add the page to its library's list in doc/BUILD.bazel, or to the team:none
  list if no library owns it. Or add it to UNTESTED below with a reason.
- A page a doctest target names has no runnable example. Its target would pass
  without testing anything, so remove it from doc/BUILD.bazel.

The doc_example rule in .buildkite/test.rules.txt routes all prose under
doc/source/, so these lists are the only place a page needs to be named.

A runnable example is a `>>>` prompt, or a `testcode` directive that isn't
marked `:skipif: True`. Like pytest-sphinx, the directive syntax follows the
file extension: a MyST fence in `.md` and `.. testcode::` in `.rst`. An rST
directive in a `.md` page isn't collected. This mirrors what pytest and
pytest-sphinx collect for these pages without needing either installed, so it
runs in the lint image on prose-only pull requests.
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Iterable, Set

# Pages with a runnable example that deliberately have no doctest target.
UNTESTED = {
    # These pages teach doctest syntax. Their `>>>` samples illustrate the
    # format and aren't meant to run.
    "doc/source/ray-contribute/getting-involved.md",
    "doc/source/ray-contribute/writing-code-snippets.md",
}

_PROMPT_RE = re.compile(r"^\s*>>>( |$)", re.MULTILINE)
_DIRECTIVE_RE = {
    ".md": re.compile(r"^\s*```\{testcode\}"),
    ".rst": re.compile(r"^\s*\.\. testcode::"),
}
_OPTION_RE = re.compile(r"^\s*:[\w-]+:")
_SKIPIF_TRUE_RE = re.compile(r"^\s*:skipif:\s*True\s*$")
# buildifier normalizes spacing in doc/BUILD.bazel but keeps end-of-line
# comments, so a call's closing parenthesis and its `gpu` argument can carry one.
_BUILD_CALL_RE = re.compile(
    r"^doctest(?:_each)?\(\n.*?^\)[ \t]*(?:#[^\n]*)?$", re.MULTILINE | re.DOTALL
)
_COMMENT_RE = re.compile(r"#.*")
_BUILD_PAGE_RE = re.compile(r'"(source/[^"*]+\.(?:md|rst))"')


def has_runnable_example(text: str, suffix: str) -> bool:
    if _PROMPT_RE.search(text):
        return True
    directive_re = _DIRECTIVE_RE[suffix]
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if not directive_re.match(line):
            continue
        skipped = False
        for option in lines[i + 1 :]:
            if not _OPTION_RE.match(option):
                break
            if _SKIPIF_TRUE_RE.match(option):
                skipped = True
        if not skipped:
            return True
    return False


def doc_pages(doc_dir: Path) -> Iterable[Path]:
    for suffix in ("*.md", "*.rst"):
        yield from (doc_dir / "source").rglob(suffix)


def _build_pages(call: str) -> Set[str]:
    """Pages a doctest call names, ignoring commented-out entries."""
    return set(_BUILD_PAGE_RE.findall(_COMMENT_RE.sub("", call)))


def listed_pages(doc_dir: Path) -> Set[str]:
    """Pages named by a doctest call in doc/BUILD.bazel."""
    build = (doc_dir / "BUILD.bazel").read_text(encoding="utf-8")
    pages = set()
    for call in _BUILD_CALL_RE.findall(build):
        pages |= {"doc/" + page for page in _build_pages(call)}
    return pages


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--doc-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__))),
        help="Path to the doc/ directory (default: this file's directory).",
    )
    args = parser.parse_args()
    doc_dir = Path(args.doc_dir).resolve()
    repo_root = doc_dir.parent

    listed = listed_pages(doc_dir)
    runnable = {
        str(page.relative_to(repo_root))
        for page in doc_pages(doc_dir)
        if has_runnable_example(
            page.read_text(encoding="utf-8", errors="replace"), page.suffix
        )
    }

    missing = sorted(runnable - listed - UNTESTED)
    vacuous = sorted(listed - runnable)
    stale_untested = sorted(UNTESTED - runnable)

    for header, pages in (
        (
            "Pages with a runnable example but no doctest target. Add each to "
            "its library's list in doc/BUILD.bazel, or to the team:none list if "
            "no library owns it. Or add it to UNTESTED in "
            "doc/test_prose_doctest_targets.py with a reason:",
            missing,
        ),
        (
            "Pages named by a doctest target with no runnable example. Their "
            "targets would pass without testing anything. Remove them from "
            "doc/BUILD.bazel:",
            vacuous,
        ),
        (
            "Pages in UNTESTED with no runnable example. Remove them from "
            "UNTESTED in doc/test_prose_doctest_targets.py:",
            stale_untested,
        ),
    ):
        if pages:
            print(header, file=sys.stderr)
            for page in pages:
                print(f"  - {page}", file=sys.stderr)

    if missing or vacuous or stale_untested:
        return 1
    print(f"{len(listed)} listed pages; every runnable page is accounted for.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
