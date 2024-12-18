#!/usr/bin/env python3
"""This script checks whether header file inclusion for ray core C++ code is correct.
"""

import subprocess

# All ray core C++ code is under certain folder.
_RAY_CORE_CPP_DIR = "src/ray"


def get_merge_base_develop() -> str:
    """Return merge-base develop commit of current HEAD"""
    current_branch = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"]
    ).strip()
    return subprocess.check_output(
        ["git", "merge-base", current_branch, "origin/master"]
    ).strip()


def diff_files_with_develop() -> str:
    """Return files that are changed between merge-base develop and HEAD."""
    return (
        subprocess.check_output(
            ["git", "diff", "--name-only", "HEAD", get_merge_base_develop()]
        )
        .strip()
        .decode()
        .split("\n")
    )


def check_ray_core_inclusion(fname: str):
    """Check whether ray core file has incorrect inclusion `src/ray/...`, which doesn't
    build on windows env.

    raise:
        ImportError: if incorrect inclusion is detected.
    """
    # Exclude protobuf, which requires absolution path for compilation.
    cmd = f"grep 'src/ray' {fname} | grep -v 'pb'"
    grep_res = subprocess.check_output(cmd, shell=True, text=True)
    if grep_res:
        raise ImportError(f"{fname} has invalid header file inclusion: {grep_res}")


def main():
    changed_files = diff_files_with_develop()
    for cur_file in changed_files:
        if not cur_file.startswith(_RAY_CORE_CPP_DIR):
            continue
        check_ray_core_inclusion(cur_file)


if __name__ == "__main__":
    main()
