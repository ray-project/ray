import os
import sys
from typing import List, Tuple, Optional

import yaml
import click

from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.builder_container import (
    BuilderContainer,
    DEFAULT_BUILD_TYPE,
    DEFAULT_PYTHON_VERSION,
    DEFAULT_ARCHITECTURE,
)
from ci.ray_ci.linux_tester_container import LinuxTesterContainer
from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.utils import docker_login

CUDA_COPYRIGHT = """
==========
== CUDA ==
==========

CUDA Version 11.8.0

Container image Copyright (c) 2016-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.

This container image and its contents are governed by the NVIDIA Deep Learning Container License.
By pulling and using the container, you accept the terms and conditions of this license:
https://developer.nvidia.com/ngc/nvidia-deep-learning-container-license

A copy of this license is made available in this container at /NGC-DL-CONTAINER-LICENSE for your convenience.
"""  # noqa: E501

DEFAULT_EXCEPT_TAGS = {"manual"}

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
    "--build-only",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Build ray only, skip running tests."),
)
@click.option(
    "--gpus",
    default=0,
    type=int,
    help=("Number of GPUs to use for the test."),
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
@click.option(
    "--build-type",
    type=click.Choice(
        [
            # python build types
            "optimized",
            "debug",
            "asan",
            "wheel",
            "wheel-aarch64",
            # cpp build types
            "clang",
            "asan-clang",
            "ubsan",
            "tsan-clang",
            # java build types
            "java",
        ]
    ),
    default="optimized",
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
    build_only: bool,
    gpus: int,
    test_env: Tuple[str],
    test_arg: Optional[str],
    build_name: Optional[str],
    build_type: Optional[str],
) -> None:
    if not bazel_workspace_dir:
        raise Exception("Please use `bazelisk run //ci/ray_ci`")
    os.chdir(bazel_workspace_dir)
    docker_login(_DOCKER_ECR_REPO.split("/")[0])

    if build_type == "wheel" or build_type == "wheel-aarch64":
        # for wheel testing, we first build the wheel and then use it for running tests
        architecture = DEFAULT_ARCHITECTURE if build_type == "wheel" else "aarch64"
        BuilderContainer(DEFAULT_PYTHON_VERSION, DEFAULT_BUILD_TYPE, architecture).run()
    container = _get_container(
        team,
        workers,
        worker_id,
        parallelism_per_worker,
        gpus,
        test_env=list(test_env),
        build_name=build_name,
        build_type=build_type,
        skip_ray_installation=skip_ray_installation,
    )
    if build_only:
        sys.exit(0)
    test_targets = _get_test_targets(
        container,
        targets,
        team,
        except_tags=_add_default_except_tags(except_tags),
        only_tags=only_tags,
        get_flaky_tests=run_flaky_tests,
    )
    success = container.run_tests(test_targets, test_arg)
    sys.exit(0 if success else 42)


def _add_default_except_tags(except_tags: str) -> str:
    final_except_tags = set(DEFAULT_EXCEPT_TAGS)
    if except_tags:
        final_except_tags.update(except_tags.split(","))
    return ",".join(final_except_tags)


def _get_container(
    team: str,
    workers: int,
    worker_id: int,
    parallelism_per_worker: int,
    gpus: int,
    test_env: Optional[List[str]] = None,
    build_name: Optional[str] = None,
    build_type: Optional[str] = None,
    skip_ray_installation: bool = False,
) -> TesterContainer:
    shard_count = workers * parallelism_per_worker
    shard_start = worker_id * parallelism_per_worker
    shard_end = (worker_id + 1) * parallelism_per_worker

    return LinuxTesterContainer(
        build_name or f"{team}build",
        test_envs=test_env,
        shard_count=shard_count,
        shard_ids=list(range(shard_start, shard_end)),
        gpus=gpus,
        skip_ray_installation=skip_ray_installation,
        build_type=build_type,
    )


def _get_tag_matcher(tag: str) -> str:
    """
    Return a regular expression that matches the given bazel tag. This is required for
    an exact tag match because bazel query uses regex to match tags.

    The word boundary is escaped twice because it is used in a python string and then
    used again as a string in bazel query.
    """
    return f"\\\\b{tag}\\\\b"


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
    query = f"attr(tags, '{_get_tag_matcher(f'team:{team}')}', {test_query})"

    if only_tags:
        only_query = " union ".join(
            [
                f"attr(tags, '{_get_tag_matcher(t)}', {test_query})"
                for t in only_tags.split(",")
            ]
        )
        query = f"{query} intersect ({only_query})"

    if except_tags:
        except_query = " union ".join(
            [
                f"attr(tags, '{_get_tag_matcher(t)}', {test_query})"
                for t in except_tags.split(",")
            ]
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
        # CUDA image comes with a license header that we need to remove
        .replace(CUDA_COPYRIGHT, "")
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
