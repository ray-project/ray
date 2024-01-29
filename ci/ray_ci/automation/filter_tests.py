import sys

from ci.ray_ci.utils import filter_out_flaky_tests
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile


def main():
    """
    Filter tests based on test targets and test state.
    Read list of test targets from file path.
    Write back into the same file path with tests of specified state removed.

    Args:
        test_state: Test state to filter by.
            Use string representation from ray_release.test.TestState class.
    """
    # Process arguments
    if len(sys.argv) != 2:
        raise ValueError("Invalid number of arguments.")

    prefix = sys.argv[1]

    test_targets_input = sys.stdin

    # Initialize global config
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

    filter_out_flaky_tests(sys.stdin, sys.stdout, prefix)


if __name__ == "__main__":
    main()
