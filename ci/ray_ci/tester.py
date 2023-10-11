import os
import sys
from typing import List, Optional

import yaml
import click

from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.utils import docker_login

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
    "--only-tags",
    default="",
    type=str,
    help=("Only include tests with the given tags."),
)
@click.option(
    "--run-flaky-tests",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Run flaky tests."),
)
@click.option(
    "--skip-ray-installation",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Skip ray installation."),
)
@click.option(
    "--test-env",
    multiple=True,
    type=str,
    help="Environment variables to set for the test.",
)
@click.option(
    "--test-arg",
    type=str,
    help=("Arguments to pass to the test."),
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
    only_tags: str,
    run_flaky_tests: bool,
    skip_ray_installation: bool,
    test_env: List[str],
    test_arg: Optional[str],
    build_name: Optional[str],
) -> None:
    if not bazel_workspace_dir:
        raise Exception("Please use `bazelisk run //ci/ray_ci`")
    os.chdir(bazel_workspace_dir)
    docker_login(_DOCKER_ECR_REPO.split("/")[0])

    container = _get_container(
        team,
        workers,
        worker_id,
        parallelism_per_worker,
        build_name,
        skip_ray_installation,
    )
    test_targets = _get_test_targets(
        container,
        targets,
        team,
        except_tags=except_tags,
        only_tags=only_tags,
        get_flaky_tests=run_flaky_tests,
    )
    success = container.run_tests(test_targets, test_env, test_arg)
    sys.exit(0 if success else 1)


def _get_container(
    team: str,
    workers: int,
    worker_id: int,
    parallelism_per_worker: int,
    build_name: Optional[str] = None,
    skip_ray_installation: bool = False,
) -> TesterContainer:
    shard_count = workers * parallelism_per_worker
    shard_start = worker_id * parallelism_per_worker
    shard_end = (worker_id + 1) * parallelism_per_worker

    return TesterContainer(
        build_name or f"{team}build",
        shard_count=shard_count,
        shard_ids=list(range(shard_start, shard_end)),
        skip_ray_installation=skip_ray_installation,
    )


def _get_all_test_query(
    targets: List[str],
    team: str,
    except_tags: Optional[str] = None,
    only_tags: Optional[str] = None,
) -> str:
    """
    Get all test targets that are owned by a particular team, except those that
    have the given tags
    """
    test_query = " union ".join([f"tests({target})" for target in targets])
    query = f"attr(tags, 'team:{team}\\\\b', {test_query})"

    if only_tags:
        only_query = " union ".join(
            [f"attr(tags, {t}, {test_query})" for t in only_tags.split(",")]
        )
        query = f"{query} intersect ({only_query})"

    if except_tags:
        except_query = " union ".join(
            [f"attr(tags, {t}, {test_query})" for t in except_tags.split(",")]
        )
        query = f"{query} except ({except_query})"

    return query


def _get_test_targets(
    container: TesterContainer,
    targets: str,
    team: str,
    except_tags: Optional[str] = "",
    only_tags: Optional[str] = "",
    yaml_dir: Optional[str] = None,
    get_flaky_tests: bool = False,
) -> List[str]:
    """
    Get test targets that are owned by a particular team
    """
    query = _get_all_test_query(targets, team, except_tags, only_tags)
    test_targets = set(
        container.run_script_with_output(
            [
                f'bazel query "{query}"',
            ]
        )
        .decode("utf-8")
        .strip()
        .split("\n")
    )
    flaky_tests = set(_get_flaky_test_targets(team, yaml_dir))

    if get_flaky_tests:
        return list(flaky_tests.intersection(test_targets))
    return list(test_targets.difference(flaky_tests))


def _get_flaky_test_targets(team: str, yaml_dir: Optional[str] = None) -> List[str]:
    """
    Get all test targets that are flaky
    """
    if not yaml_dir:
        yaml_dir = os.path.join(bazel_workspace_dir, "ci/ray_ci")

    with open(f"{yaml_dir}/{team}.tests.yml", "rb") as f:
        flaky_tests = yaml.safe_load(f)["flaky_tests"]

    return flaky_tests
