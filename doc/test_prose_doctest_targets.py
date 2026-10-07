"""Keep the per-file prose doctest lists in doc/BUILD.bazel complete and live.

Core, RLlib, Serve, Train, and Tune prose is tested by `doctest_each()` over an
explicit list of pages, not a directory glob, so that every target tests
something. An explicit list can silently miss a page: a runnable block added
to an unlisted page runs nowhere, not even post-merge. This check fails when:

- A page under one of those library directories has a runnable example but no
  doctest target names it. Add the page to the library's list in
  doc/BUILD.bazel and to the matching doc_example rule in
  .buildkite/test.rules.txt, or add it to UNTESTED below with a reason.
- A page a doctest target names has no runnable example. Its target would pass
  without testing anything, so remove it from doc/BUILD.bazel and
  .buildkite/test.rules.txt.
- The doc_example rule in .buildkite/test.rules.txt and the CPU doctest lists
  in doc/BUILD.bazel name different pages. A listed page that the rule doesn't
  route never starts the "docs-example-test" opt-in step. GPU pages are left out
  of the rule, because the opt-in never runs GPU tests.

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

LIBRARY_DIRS = ("core", "rllib", "serve", "train", "tune")

# Pages with a runnable example that deliberately have no doctest target.
UNTESTED = {
    # The `doc_code/` snippet for this page is tested instead.
    "doc/source/core/tasks/nested-tasks.md",
    # CI does not have Horovod installed.
    "doc/source/train/horovod.md",
    # These were excluded from their library's doctest target before the
    # per-file split, with no reason recorded.
    "doc/source/core/handling-dependencies.md",
    "doc/source/serve/production-guide/fault-tolerance.md",
}

_PROMPT_RE = re.compile(r"^\s*>>>( |$)", re.MULTILINE)
_DIRECTIVE_RE = {
    ".md": re.compile(r"^\s*```\{testcode\}"),
    ".rst": re.compile(r"^\s*\.\. testcode::"),
}
_OPTION_RE = re.compile(r"^\s*:[\w-]+:")
_SKIPIF_TRUE_RE = re.compile(r"^\s*:skipif:\s*True\s*$")
_BUILD_CALL_RE = re.compile(r"^doctest(?:_each)?\(\n.*?^\)$", re.MULTILINE | re.DOTALL)
_GPU_RE = re.compile(r"^\s*gpu = True,$", re.MULTILINE)
_RULES_PAGE_RE = re.compile(
    r"^(doc/source/(?:%s)/[^*\s]+\.(?:md|rst))$" % "|".join(LIBRARY_DIRS),
    re.MULTILINE,
)
_BUILD_PAGE_RE = re.compile(
    r'"(source/(?:%s)/[^"*]+\.(?:md|rst))"' % "|".join(LIBRARY_DIRS)
)


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


def library_pages(doc_dir: Path) -> Iterable[Path]:
    for lib in LIBRARY_DIRS:
        for suffix in ("*.md", "*.rst"):
            yield from (doc_dir / "source" / lib).rglob(suffix)


def listed_pages(doc_dir: Path) -> Set[str]:
    build = (doc_dir / "BUILD.bazel").read_text(encoding="utf-8")
    return {"doc/" + page for page in _BUILD_PAGE_RE.findall(build)}


def gpu_pages(doc_dir: Path) -> Set[str]:
    """Pages named by a `gpu = True` doctest call in doc/BUILD.bazel."""
    build = (doc_dir / "BUILD.bazel").read_text(encoding="utf-8")
    pages = set()
    for call in _BUILD_CALL_RE.findall(build):
        if _GPU_RE.search(call):
            pages |= {"doc/" + page for page in _BUILD_PAGE_RE.findall(call)}
    return pages


def routed_pages(repo_root: Path) -> Set[str]:
    """Library prose pages listed by name in .buildkite/test.rules.txt."""
    rules = (repo_root / ".buildkite" / "test.rules.txt").read_text(encoding="utf-8")
    return set(_RULES_PAGE_RE.findall(rules))


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
        for page in library_pages(doc_dir)
        if has_runnable_example(
            page.read_text(encoding="utf-8", errors="replace"), page.suffix
        )
    }

    missing = sorted(runnable - listed - UNTESTED)
    vacuous = sorted(listed - runnable)
    stale_untested = sorted(UNTESTED - runnable)
    cpu_listed = listed - gpu_pages(doc_dir)
    routed = routed_pages(repo_root)
    unrouted = sorted(cpu_listed - routed)
    routed_unlisted = sorted(routed - cpu_listed)

    for header, pages in (
        (
            "Pages with a runnable example but no doctest target. Add each to "
            "its library's list in doc/BUILD.bazel and to the doc_example rule "
            "in .buildkite/test.rules.txt, or to UNTESTED in "
            "doc/test_prose_doctest_targets.py with a reason:",
            missing,
        ),
        (
            "Pages named by a doctest target with no runnable example. Their "
            "targets would pass without testing anything. Remove them from "
            "doc/BUILD.bazel and .buildkite/test.rules.txt:",
            vacuous,
        ),
        (
            "Pages in UNTESTED with no runnable example. Remove them from "
            "UNTESTED in doc/test_prose_doctest_targets.py:",
            stale_untested,
        ),
        (
            "Pages with a CPU doctest target that the doc_example rule in "
            ".buildkite/test.rules.txt doesn't route. Add them to the rule:",
            unrouted,
        ),
        (
            "Pages the doc_example rule in .buildkite/test.rules.txt routes "
            "with no CPU doctest target. Remove them from the rule:",
            routed_unlisted,
        ),
    ):
        if pages:
            print(header, file=sys.stderr)
            for page in pages:
                print(f"  - {page}", file=sys.stderr)

    if missing or vacuous or stale_untested or unrouted or routed_unlisted:
        return 1
    print(f"{len(listed)} listed pages; every runnable page is accounted for.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
