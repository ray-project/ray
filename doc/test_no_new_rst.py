"""Fail PRs that add new .rst files under doc/source/.

Uses --diff-filter=A so renames (status R) are skipped. Listed paths in
ALLOWLIST below are exempt -- add a path here only with a comment that
explains why MyST won't work.
"""

import argparse
import os
import subprocess
import sys
from typing import List


# Paths under doc/source/ that are exempt from the no-new-.rst rule.
ALLOWLIST = {
    # "doc/source/example.rst",  # Reason: ...
}


def _list_added_files(base: str, commit: str, remote: str = "origin") -> List[str]:
    subprocess.check_call(["git", "fetch", "-q", remote, base])
    out = subprocess.check_output(
        [
            "git",
            "diff",
            "--name-only",
            "--diff-filter=A",
            f"{remote}/{base}...{commit}",
            "--",
        ],
        text=True,
    )
    return [line for line in out.splitlines() if line]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base",
        default=os.environ.get("BUILDKITE_PULL_REQUEST_BASE_BRANCH") or "master",
        help="Base branch to diff against "
        "(default: $BUILDKITE_PULL_REQUEST_BASE_BRANCH or 'master').",
    )
    parser.add_argument(
        "--remote",
        default="origin",
        help="Git remote to fetch from (default: 'origin').",
    )
    parser.add_argument(
        "--commit",
        default=os.environ.get("BUILDKITE_COMMIT", "HEAD"),
        help="Commit to diff (default: $BUILDKITE_COMMIT or 'HEAD').",
    )
    args = parser.parse_args()

    if os.environ.get("BUILDKITE_PULL_REQUEST", "true") == "false":
        print("Not a pull request build; skipping.")
        sys.exit(0)

    added = _list_added_files(args.base, args.commit, remote=args.remote)
    new_rst = sorted(
        f
        for f in added
        if f.startswith("doc/source/") and f.endswith(".rst") and f not in ALLOWLIST
    )
    if new_rst:
        print(
            "New .rst files under doc/source/ are not allowed; "
            "please use MyST Markdown (.md) instead. If an exception is "
            "needed, add the path to ALLOWLIST in doc/test_no_new_rst.py "
            "with a comment explaining why.",
            file=sys.stderr,
        )
        print(
            f"Newly added .rst files (vs {args.remote}/{args.base}):", file=sys.stderr
        )
        for path in new_rst:
            print(f"  - {path}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
