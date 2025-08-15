#!/usr/bin/env python3
"""This script checks whether header file inclusion for ray core C++ code is correct.
"""

import re
import sys


def check_ray_core_inclusion(fname: str):
    """Check whether ray core file has incorrect inclusion `src/ray/...`, which doesn't
    build on windows env.

    If invalid inclusion is detected, error message will be printed, and exit with
    non-zero code directly.
    """
    # Exclude protobuf, which requires absolution path for compilation.
    pattern = re.compile(r"^#include(?=.*src/ray)(?!.*pb).*$", re.MULTILINE)

    with open(fname, "r") as file:
        content = file.read()
        match = pattern.search(content)
        if match:
            print(f"{fname} has invalid header file inclusion: {match.group(0)}")
            sys.exit(1)


def main():
    changed_files = sys.argv[1:]
    for cur_file in changed_files:
        check_ray_core_inclusion(cur_file)


if __name__ == "__main__":
    main()
