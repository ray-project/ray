import os
import sys
from typing import List, Optional

import yaml
import click

from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.utils import shard_tests, docker_login

# Gets the path of product/tools/docker (i.e. the parent of 'common')
bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")


@click.command()
@click.argument("targets", required=True, type=str, nargs=-1)
@click.argument("team", required=True, type=str, nargs=1)
@click.option(
    "--workers",
    default=1,
    type=int,
    help=("Number of concurrent test jobs to run."),
)
@click.option(
    "--worker-id",
    default=0,
    type=int,
    help=("Index of the concurrent shard to run."),
)
@click.option(
    "--parallelism-per-worker",
    default=1,
    type=int,
    help=("Number of concurrent test jobs to run per worker."),
)
@click.option(
    "--except-tags",
    default="",
    type=str,
    help=("Except tests with the given tags."),
)
@click.option(
    "--run-flaky-tests",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Run flaky tests."),
)
@click.option(
    "--test-env",
    multiple=True,
    type=str,
    help="Environment variables to set for the test.",
)
@click.option(
    "--build-name",
    type=str,
    help="Name of the build used to run tests",
)
def main(
    targets: List[str],
    team: str,
    workers: int,
    worker_id: int,
    parallelism_per_worker: int,
    except_tags: str,
    run_flaky_tests: bool,
    test_env: List[str],
    build_name: Optional[str],
) -> None:
    if not bazel_workspace_dir:
        raise Exception("Please use `bazelisk run //ci/ray_ci`")
    os.chdir(bazel_workspace_dir)
    docker_login(_DOCKER_ECR_REPO.split("/")[0])

    if not build_name:
        build_name = f"{team}build"
    container = TesterContainer(build_name)
    if run_flaky_tests:
        test_targets = _get_flaky_test_targets(team)
    else:
        test_targets = _get_test_targets(
            container,
            targets,
            team,
            workers,
            worker_id,
            except_tags,
        )
    success = container.run_tests(test_targets, test_env, parallelism_per_worker)
    sys.exit(0 if success else 1)


def _get_test_targets(
    container: TesterContainer,
    targets: str,
    team: str,
    workers: int,
    worker_id: int,
    except_tags: Optional[str] = "",
    yaml_dir: Optional[str] = None,
) -> List[str]:
    """
    Get test targets to run for a particular shard
    """
    return shard_tests(
        _get_all_test_targets(container, targets, team, except_tags, yaml_dir=yaml_dir),
        workers,
        worker_id,
    )


def _get_all_test_query(targets: List[str], team: str, except_tags: str) -> str:
    """
    Get all test targets that are owned by a particular team, except those that
    have the given tags
    """
    test_query = " union ".join([f"tests({target})" for target in targets])
    team_query = f"attr(tags, 'team:{team}\\\\b', {test_query})"
    if not except_tags:
        # return all tests owned by the team if no except_tags are given
        return team_query

    # otherwise exclude tests with the given tags
    except_query = " union ".join(
        [f"attr(tags, {t}, {test_query})" for t in except_tags.split(",")]
    )
    return f"{team_query} except ({except_query})"


def _get_all_test_targets(
    container: TesterContainer,
    targets: str,
    team: str,
    except_tags: Optional[str] = "",
    yaml_dir: Optional[str] = None,
) -> List[str]:
    """
    Get all test targets that are not flaky
    """

    test_targets = (
        container.run_script_with_output(
            [
                f'bazel query "{_get_all_test_query(targets, team, except_tags)}"',
            ]
        )
        .decode("utf-8")
        .split("\n")
    )
    flaky_tests = _get_flaky_test_targets(team, yaml_dir)

    return [test for test in test_targets if test and test not in flaky_tests]


def _get_flaky_test_targets(team: str, yaml_dir: Optional[str] = None) -> List[str]:
    """
    Get all test targets that are flaky
    """
    if not yaml_dir:
        yaml_dir = os.path.join(bazel_workspace_dir, "ci/ray_ci")

    with open(f"{yaml_dir}/{team}.tests.yml", "rb") as f:
        flaky_tests = yaml.safe_load(f)["flaky_tests"]

    return flaky_tests
