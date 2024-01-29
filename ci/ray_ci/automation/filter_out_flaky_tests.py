import sys

from ci.ray_ci.utils import filter_out_flaky_tests
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile


def main():
    """
    Filter out flaky tests.
    Input (stdin) as a list of test names.
    Output (stdout) as a list of test names without flaky tests.

    Args:
        prefix: Prefix to query tests with.
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
