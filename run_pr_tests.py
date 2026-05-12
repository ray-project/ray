"""Run unit tests for the files changed in the current PR branch.

Detects test files modified vs. a base branch (default: ``master``) and
runs them with pytest. Useful for fast local iteration on a focused
change without re-running the entire suite.

Usage:
    python run_pr_tests.py                  # diff vs. master
    python run_pr_tests.py --base origin/master
    python run_pr_tests.py -- -k testInitializeSDK   # pass-through pytest args
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent


def changed_files(base: str) -> list[Path]:
    """Return paths (relative to repo root) changed vs. ``base``.

    Includes both committed diffs and the current working tree, so the
    script stays useful while you're still editing.
    """
    cmd = ["git", "diff", "--name-only", f"{base}...HEAD"]
    committed = subprocess.check_output(cmd, cwd=REPO_ROOT, text=True).splitlines()
    working = subprocess.check_output(
        ["git", "diff", "--name-only", "HEAD"], cwd=REPO_ROOT, text=True
    ).splitlines()
    untracked = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=REPO_ROOT,
        text=True,
    ).splitlines()
    seen: set[str] = set()
    out: list[Path] = []
    for rel in (*committed, *working, *untracked):
        if rel and rel not in seen:
            seen.add(rel)
            out.append(REPO_ROOT / rel)
    return out


def pick_test_files(paths: list[Path]) -> list[Path]:
    """Keep only existing pytest files (``test_*.py``)."""
    return [
        p
        for p in paths
        if p.exists()
        and p.is_file()
        and p.name.startswith("test_")
        and p.suffix == ".py"
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base",
        default="master",
        help="Base ref to diff against (default: master).",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Extra args forwarded to pytest (use `--` to separate).",
    )
    args = parser.parse_args()

    files = pick_test_files(changed_files(args.base))
    if not files:
        print(f"No changed test files found vs. {args.base}. Nothing to run.")
        return 0

    print(f"Changed test files vs. {args.base}:")
    for f in files:
        print(f"  - {f.relative_to(REPO_ROOT)}")
    print()

    extra = [a for a in args.pytest_args if a != "--"]
    import pytest

    return pytest.main([*[str(f) for f in files], "-v", *extra])


if __name__ == "__main__":
    sys.exit(main())
