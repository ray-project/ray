#!/usr/bin/env python3
from ci.ray_ci.utils import query_all_test_names_by_state
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile
import sys


def main():
    """
    Filter tests based on test targets and test state.
    Read list of test targets from file path.
    Write back into the same file path with flaky tests removed.

    Args:
        test_targets_file_path: Path to file containing list of test targets.
        test_state: Test state to filter by.
            Use string representation from ray_release.test.TestState class.
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
    flaky_test_names = query_all_test_names_by_state(
        test_state=test_state, prefix_on=False
    )
    # Eliminate flaky test from list of test targets
    non_flaky_test_targets = [
        test for test in test_targets if test not in flaky_test_names
    ]

    # Write non-flaky test targets to file
    with open(test_targets_file_path, "w") as f:
        f.write("\n".join(non_flaky_test_targets))


if __name__ == "__main__":
    main()
