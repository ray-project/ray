import sys
import click

from ci.ray_ci.utils import filter_tests
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile


@click.command()
@click.option(
    "--prefix", required=True, type=click.Choice(["darwin:", "linux:", "windows:"])
)
@click.option("--state_filter", required=True, type=click.Choice(["flaky", "-flaky"]))
def main(prefix: str, state_filter: str) -> None:
    """
    Filter flaky tests.
    Input (stdin) as a list of test names.
    Output (stdout) as a list of test names without flaky tests.

    Args:
        prefix: Prefix to query tests with.
        state_filter: Options on what test to filter: "flaky" or "-flaky".
    """
    # Initialize global config
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

    filter_tests(sys.stdin, sys.stdout, prefix, state_filter)


if __name__ == "__main__":
    main()
