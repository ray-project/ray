import sys

from ci.ray_ci.utils import filter_out_flaky_tests
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile


def main():
    """
    Filter flaky tests.
    Input (stdin) as a list of test names.
    Output (stdout) as a list of test names without flaky tests.

    Args:
        prefix: Prefix to query tests with.
        select_flaky: If true, only print flaky tests. If false, only print non-flaky tests.
    """
    # Process arguments
    if len(sys.argv) != 3:
        raise ValueError("Invalid number of arguments.")

    prefix = sys.argv[1]
    select_flaky = sys.argv[2]

    # Initialize global config
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

    filter_out_flaky_tests(sys.stdin, sys.stdout, prefix, select_flaky)


if __name__ == "__main__":
    main()
