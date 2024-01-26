#!/usr/bin/env python3
from ray_release.test import Test, TestState
from ci.ray_ci.utils import logger
from typing import List
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile
import json
import sys
import os

def main():
    """
    Filter tests based on test targets and test state.
    Read list of test targets from file path.
    Modify the file path to filter tests based on test state.

    Args:
        test_targets_file_path (str): Path to test targets file.
        test_state (TestState): Test state.
    """
    # Initialize global config
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

    # Process arguments
    test_targets_file_path = None
    if len(sys.argv) == 2:
        test_targets_file_path = sys.argv[1]
    elif len(sys.argv) == 3:
        test_targets_file_path = sys.argv[1]
        test_state = sys.argv[2]
    else:
        print("Invalid number of arguments.")

    # Sanity check
    if test_targets_file_path is None:
        print("Invalid test targets file path.")

    # Read test targets from file
    test_targets = []
    with open(test_targets_file_path, "r") as f:
        test_targets.append(f.read().splitlines())
    # Obtain all existing flaky tests
    tests = Test.get_tests(["d"])
    flaky_tests = Test.filter_tests_by_state(tests, TestState.FLAKY)

    # Eliminate flaky test from list of test targets
    print(flaky_tests)
    print("\n\n\n")
    


if __name__ == "__main__":
    main()