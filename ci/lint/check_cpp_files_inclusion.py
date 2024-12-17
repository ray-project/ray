#!/usr/bin/env python3
"""This script checks whether header file inclusion for ray core C++ code is correct.
"""

import sys
import re


def check_ray_core_inclusion(fname: str):
    """Check whether ray core file has incorrect inclusion `src/ray/...`, which doesn't
    build on windows env.

    raise:
        ImportError: if incorrect inclusion is detected.
    """
    # Exclude protobuf, which requires absolution path for compilation.
    pattern = re.compile(r"^(?=.*src/ray)(?!.*pb)")

    with open(fname, "r") as file:
        for line in file:
            if pattern.search(line):
                raise ImportError(f"{fname} has invalid header file inclusion: {line}")


def main():
    changed_files = sys.argv[1:]
    for cur_file in changed_files:
        check_ray_core_inclusion(cur_file)
    exit(0)


if __name__ == "__main__":
    main()
