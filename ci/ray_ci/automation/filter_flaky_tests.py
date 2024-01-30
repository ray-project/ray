import sys
import click

from ci.ray_ci.utils import filter_flaky_tests
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile


@click.command()
@click.argument("prefix", required=True)
@click.argument(
    "filter_option", required=True, type=click.Choice(["only-flaky", "non-flaky"])
)
def main(prefix: str, filter_option: str) -> None:
    """
    Filter flaky tests.
    Input (stdin) as a list of test names.
    Output (stdout) as a list of test names without flaky tests.

    Args:
        prefix: Prefix to query tests with.
        filter_option: Options on what test to filter: "only-flaky" or "non-flaky".
    """
    # Initialize global config
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

    filter_flaky_tests(sys.stdin, sys.stdout, prefix, filter_option)


if __name__ == "__main__":
    main()
