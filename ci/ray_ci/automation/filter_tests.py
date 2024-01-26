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
        raise ValueError("Invalid number of arguments.")

    # Sanity check
    if test_targets_file_path is None:
        raise ValueError("Invalid test targets file path.")

    # Read test targets from file
    test_targets = []
    with open(test_targets_file_path, "r") as f:
        test_targets = f.read().splitlines()

    TEST_PREFIXES = ["darwin:"]

    # Obtain all existing flaky tests
    flaky_test_names = obtain_existing_flaky_test_names(prefix_on=False)
    # Eliminate flaky test from list of test targets
    non_flaky_test_targets = [test for test in test_targets if test not in flaky_test_names]

    # Write non-flaky test targets to file
    with open(test_targets_file_path, "w") as f:
        f.write("\n".join(non_flaky_test_targets))

def obtain_existing_flaky_test_names(prefix_on: bool = False):
    """
    Obtain all existing flaky test names.

    Args:
        prefix_on (bool): Whether to include prefix in test name.

    Returns:
        List[str]: List of test names.
    """
    TEST_PREFIXES = ["darwin:"]

    # Obtain all existing flaky tests
    tests = Test.get_tests(TEST_PREFIXES)
    flaky_tests = Test.filter_tests_by_state(tests, TestState.FLAKY)
    flaky_test_names = [test.get_name() for test in flaky_tests]
    if prefix_on:
        return flaky_test_names
    else:
        no_prefix_flaky_test_names = [test.get_name().removeprefix(prefix) for test in flaky_tests for prefix in TEST_PREFIXES if test.get_name().startswith(prefix)]
        return no_prefix_flaky_test_names

if __name__ == "__main__":
    main()