"""Print the doc example test targets that a change feeds directly.

Prints one `<team> <label>` pair per line to stdout, and a report of what
runs and what doesn't to stderr. See doc_example_targets.py for the matching rules.

In CI, the changed files are the pull request's diff against its base branch.
Locally, pass files explicitly, or pass --base to diff against a ref:

  bazel run //ci/ray_ci/doc:cmd_doc_example_targets -- \\
      doc/source/data/doc_code/loading_data.py
  bazel run //ci/ray_ci/doc:cmd_doc_example_targets -- --base upstream/master
"""

import argparse
import os
import subprocess
import sys
from typing import List

from ci.ray_ci.doc.doc_example_targets import format_report, parse_query_xml, select


def _changed_files(base: str, workspace: str) -> List[str]:
    output = subprocess.check_output(
        ["git", "diff", "--name-only", f"{base}...HEAD"], cwd=workspace, text=True
    )
    return [line.strip() for line in output.splitlines() if line.strip()]


def _query_doc_tests(workspace: str) -> str:
    return subprocess.check_output(
        ["bazel", "query", "tests(//doc/...)", "--output=xml"],
        cwd=workspace,
        text=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("files", nargs="*", help="Changed files, repo-relative.")
    parser.add_argument(
        "--base",
        help="Diff HEAD against this ref instead of taking files. Defaults to "
        "origin/$BUILDKITE_PULL_REQUEST_BASE_BRANCH in CI.",
    )
    args = parser.parse_args()

    workspace = os.environ.get("BUILD_WORKSPACE_DIRECTORY", os.getcwd())
    files = args.files
    if not files:
        base = args.base
        if not base and os.environ.get("BUILDKITE_PULL_REQUEST_BASE_BRANCH"):
            base = "origin/" + os.environ["BUILDKITE_PULL_REQUEST_BASE_BRANCH"]
        if not base:
            print(
                "No changed files and no base ref; nothing to select.", file=sys.stderr
            )
            return 0
        files = _changed_files(base, workspace)

    selection = select(files, parse_query_xml(_query_doc_tests(workspace)))
    report = format_report(selection)
    if report:
        print(report, file=sys.stderr)

    for team, labels in sorted(selection.runnable.items()):
        for label in labels:
            print(team, label)
    return 0


if __name__ == "__main__":
    sys.exit(main())
