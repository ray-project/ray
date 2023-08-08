import logging
import os
import subprocess
from typing import List, Optional
from math import ceil

import yaml
import click

# Gets the path of product/tools/docker (i.e. the parent of 'common')
bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")


@click.command()
@click.argument("targets", required=True, type=str)
@click.argument("team", required=True, type=str)
@click.option(
    "--concurrency",
    default=3,
    type=int,
    help=("Number of concurrent test jobs to run."),
)
@click.option(
    "--shard",
    default=0,
    type=int,
    help=("Index of the concurrent shard to run."),
)
def main(targets: str, team: str, concurrency: int, shard: int) -> None:
    if not bazel_workspace_dir:
        raise Exception("Please use `bazelisk run //ci/ray_ci`")

    os.chdir(bazel_workspace_dir)

    test_targets = _get_test_targets(targets, team, concurrency, shard)
    if not test_targets:
        logging.info("No tests to run")
        return
    logging.info(f"Running tests: {test_targets}")
    _run_tests(test_targets)


def _run_tests(test_targets: List[str]) -> None:
    """
    Run tests
    """
    bazel_options = (
        subprocess.check_output([f"{bazel_workspace_dir}/ci/run/bazel_export_options"])
        .decode("utf-8")
        .split()
    )
    subprocess.check_call(
        [
            "bazel",
            "test",
            "--config=ci",
        ]
        + bazel_options
        + test_targets
    )


def _get_test_targets(
    targets: str,
    team: str,
    concurrency: int,
    shard: int,
    yaml_dir: Optional[str] = None,
) -> List[str]:
    """
    Get test targets to run for a particular shard
    """
    if not yaml_dir:
        yaml_dir = os.path.join(bazel_workspace_dir, "ci/ray_ci")

    return _chunk_into_n(
        _get_all_test_targets(targets, team, yaml_dir=yaml_dir),
        concurrency,
    )[shard]


def _chunk_into_n(list: List[str], n: int):
    size = ceil(len(list) / n)
    return [list[x * size : x * size + size] for x in range(n)]


def _get_all_test_targets(targets: str, team: str, yaml_dir: str) -> List[str]:
    """
    Get all test targets that are not flaky
    """

    test_targets = (
        subprocess.check_output(
            [
                "bazel",
                "query",
                f"attr(tags, team:{team}, tests({targets})) intersect ("
                # TODO(can): Remove this once we have a better way
                # to filter out test size
                f"attr(size, small, tests({targets})) union "
                f"attr(size, medium, tests({targets}))"
                ")",
            ]
        )
        .decode("utf-8")
        .split("\n")
    )
    with open(f"{yaml_dir}/{team}.tests.yml", "rb") as f:
        flaky_tests = yaml.safe_load(f)["flaky_tests"]

    return [test for test in test_targets if test and test not in flaky_tests]
