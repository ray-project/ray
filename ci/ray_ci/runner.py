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
@click.argument("targets", required=True, type=str, nargs=-1)
@click.argument("team", required=True, type=str, nargs=1)
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
@click.option(
    "--size",
    default="small,medium,large",
    type=str,
    help=("Size of tests to run."),
)
def main(
    targets: List[str], team: str, concurrency: int, shard: int, size: str
) -> None:
    if not bazel_workspace_dir:
        raise Exception("Please use `bazelisk run //ci/ray_ci`")
    os.chdir(bazel_workspace_dir)

    test_targets = _get_test_targets(targets, team, concurrency, shard, size)
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
        ["bazel", "test", "--config=ci"] + bazel_options + test_targets
    )


def _get_test_targets(
    targets: str,
    team: str,
    concurrency: int,
    shard: int,
    size: str,
    yaml_dir: Optional[str] = None,
) -> List[str]:
    """
    Get test targets to run for a particular shard
    """
    if not yaml_dir:
        yaml_dir = os.path.join(bazel_workspace_dir, "ci/ray_ci")

    return _chunk_into_n(
        _get_all_test_targets(targets, team, size, yaml_dir=yaml_dir),
        concurrency,
    )[shard]


def _chunk_into_n(list: List[str], n: int):
    size = ceil(len(list) / n)
    return [list[x * size : x * size + size] for x in range(n)]


def _get_all_test_query(targets: List[str], team: str, size: str) -> str:
    test_query = " union ".join([f"tests({target})" for target in targets])
    team_query = f"attr(tags, team:{team}, {test_query})"
    size_query = " union ".join(
        [f"attr(size, {s}, {test_query})" for s in size.split(",")]
    )
    except_query = " union ".join(
        [
            f"attr(tags, {t}, {test_query})"
            for t in ["debug_tests", "asan_tests", "ray_ha"]
        ]
    )

    return f"({team_query} intersect ({size_query})) except ({except_query})"


def _get_all_test_targets(
    targets: str, team: str, size: str, yaml_dir: str
) -> List[str]:
    """
    Get all test targets that are not flaky
    """

    test_targets = (
        subprocess.check_output(
            ["bazel", "query", _get_all_test_query(targets, team, size)],
        )
        .decode("utf-8")
        .split("\n")
    )
    with open(f"{yaml_dir}/{team}.tests.yml", "rb") as f:
        flaky_tests = yaml.safe_load(f)["flaky_tests"]

    return [test for test in test_targets if test and test not in flaky_tests]
