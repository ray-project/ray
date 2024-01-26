#!/usr/bin/env python3
from ray_release.test import Test, TestState
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile
import sys


def main():
    """
    Filter tests based on test targets and test state.
    Read list of test targets from file path.
    Modify the file path to filter tests based on test state.
    """
    # Initialize global config
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

    # Process arguments
    test_targets_file_path = None
    if len(sys.argv) == 3:
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

    # Obtain all existing flaky tests
    flaky_test_names = obtain_existing_test_names_by_state(
        prefix_on=False, test_state=test_state
    )
    # Eliminate flaky test from list of test targets
    non_flaky_test_targets = [
        test for test in test_targets if test not in flaky_test_names
    ]

    # Write non-flaky test targets to file
    with open(test_targets_file_path, "w") as f:
        f.write("\n".join(non_flaky_test_targets))


def obtain_existing_test_names_by_state(
    prefix_on: bool = False, test_state: str = "flaky"
):
    """
    Obtain all existing flaky test names.

    Args:
        prefix_on: Whether to include prefix in test name.

    Returns:
        List[str]: List of test names.
    """
    TEST_PREFIXES = ["darwin:"]

    # Convert test_state string into TestState enum
    test_state_enum = next(
        (state for state in TestState if state.value == test_state), None
    )
    if test_state_enum is None:
        raise ValueError("Invalid test state.")

    # Obtain all existing tests
    tests = Test.get_tests(TEST_PREFIXES)

    # Filter tests by test state
    filtered_tests = Test.filter_tests_by_state(tests, test_state_enum)
    filtered_test_names = [test.get_name() for test in filtered_tests]
    if prefix_on:
        return filtered_test_names
    else:
        no_prefix_filtered_test_names = [
            test.replace(prefix, '')
            for test in filtered_test_names
            for prefix in TEST_PREFIXES
            if test.startswith(prefix)
        ]
        return no_prefix_filtered_test_names


if __name__ == "__main__":
    main()
