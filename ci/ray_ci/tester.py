import os
import sys
from typing import List, Set, Tuple, Optional

import yaml
import click

from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.builder_container import (
    BuilderContainer,
    DEFAULT_BUILD_TYPE,
    DEFAULT_PYTHON_VERSION,
    DEFAULT_ARCHITECTURE,
    PYTHON_VERSIONS,
)
from ci.ray_ci.linux_tester_container import LinuxTesterContainer
from ci.ray_ci.windows_tester_container import WindowsTesterContainer
from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.utils import docker_login, ci_init
from ray_release.test import Test, TestState

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
    default="1",
    type=str,
    help=("Number of concurrent test jobs to run."),
)
@click.option(
    "--worker-id",
    default="0",
    type=str,
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
    "--run-high-impact-tests",
    is_flag=True,
    show_default=True,
    default=False,
    help=(
        "Run only high impact tests. "
        "High impact tests are tests that often catch regressions in the past."
    ),
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
    "--network",
    type=str,
    help="Network to use for the test.",
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
    "--python-version",
    type=click.Choice(list(PYTHON_VERSIONS.keys())),
    help=("Python version to build the wheel with"),
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
            # do not build ray
            "skip",
        ]
    ),
    default="optimized",
)
@click.option(
    "--bisect-run-test-target",
    type=str,
    help="Test target to run in bisection mode",
)
@click.option(
    "--operating-system",
    default="linux",
    type=click.Choice(["linux", "windows"]),
    help=("Operating system to run tests on"),
)
@click.option(
    "--tmp-filesystem",
    type=str,
    help=("Filesystem to use for /tmp"),
)
def main(
    targets: List[str],
    team: str,
    workers: str,
    worker_id: str,
    parallelism_per_worker: int,
    operating_system: str,
    except_tags: str,
    only_tags: str,
    run_flaky_tests: bool,
    run_high_impact_tests: bool,
    skip_ray_installation: bool,
    build_only: bool,
    gpus: int,
    network: Optional[str],
    test_env: Tuple[str],
    test_arg: Optional[str],
    python_version: Optional[str],
    build_name: Optional[str],
    build_type: Optional[str],
    bisect_run_test_target: Optional[str],
    tmp_filesystem: Optional[str],
) -> None:
    if not bazel_workspace_dir:
        raise Exception("Please use `bazelisk run //ci/ray_ci`")
    os.chdir(bazel_workspace_dir)
    ci_init()
    docker_login(_DOCKER_ECR_REPO.split("/")[0])

    if build_type == "wheel" or build_type == "wheel-aarch64":
        # for wheel testing, we first build the wheel and then use it for running tests
        architecture = DEFAULT_ARCHITECTURE if build_type == "wheel" else "aarch64"
        BuilderContainer(DEFAULT_PYTHON_VERSION, DEFAULT_BUILD_TYPE, architecture).run()
    bisect_run_test_target = bisect_run_test_target or os.environ.get(
        "RAYCI_BISECT_TEST_TARGET"
    )
    container = _get_container(
        team,
        operating_system,
        int(workers) if workers else 1,
        int(worker_id) if worker_id else 0,
        parallelism_per_worker,
        gpus,
        network=network,
        tmp_filesystem=tmp_filesystem,
        test_env=list(test_env),
        python_version=python_version,
        build_name=build_name,
        build_type=build_type,
        skip_ray_installation=skip_ray_installation,
    )
    if build_only:
        sys.exit(0)
    if bisect_run_test_target:
        test_targets = [bisect_run_test_target]
    else:
        test_targets = _get_test_targets(
            container,
            targets,
            team,
            operating_system,
            except_tags=_add_default_except_tags(except_tags),
            only_tags=only_tags,
            get_flaky_tests=run_flaky_tests,
            get_high_impact_tests=run_high_impact_tests
            or os.environ.get("RAYCI_MICROCHECK_RUN") == "1",
        )
    success = container.run_tests(
        team,
        test_targets,
        test_arg,
        is_bisect_run=bisect_run_test_target is not None,
        run_flaky_tests=run_flaky_tests,
    )
    sys.exit(0 if success else 43)


def _add_default_except_tags(except_tags: str) -> str:
    final_except_tags = set(DEFAULT_EXCEPT_TAGS)
    if except_tags:
        final_except_tags.update(except_tags.split(","))
    return ",".join(final_except_tags)


def _get_container(
    team: str,
    operating_system: str,
    workers: int,
    worker_id: int,
    parallelism_per_worker: int,
    gpus: int,
    network: Optional[str],
    tmp_filesystem: Optional[str] = None,
    test_env: Optional[List[str]] = None,
    python_version: Optional[str] = None,
    build_name: Optional[str] = None,
    build_type: Optional[str] = None,
    skip_ray_installation: bool = False,
) -> TesterContainer:
    shard_count = workers * parallelism_per_worker
    shard_start = worker_id * parallelism_per_worker
    shard_end = (worker_id + 1) * parallelism_per_worker
    if not build_name:
        build_name = (
            f"{team}build-py{python_version}" if python_version else f"{team}build"
        )

    if operating_system == "linux":
        return LinuxTesterContainer(
            build_name,
            test_envs=test_env,
            shard_count=shard_count,
            shard_ids=list(range(shard_start, shard_end)),
            gpus=gpus,
            network=network,
            skip_ray_installation=skip_ray_installation,
            build_type=build_type,
            tmp_filesystem=tmp_filesystem,
        )

    if operating_system == "windows":
        return WindowsTesterContainer(
            build_name,
            network=network,
            test_envs=test_env,
            shard_count=shard_count,
            shard_ids=list(range(shard_start, shard_end)),
            skip_ray_installation=skip_ray_installation,
        )

    assert False, f"Unsupported operating system: {operating_system}"


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
    operating_system: str,
    except_tags: Optional[str] = "",
    only_tags: Optional[str] = "",
    yaml_dir: Optional[str] = None,
    get_flaky_tests: bool = False,
    get_high_impact_tests: bool = False,
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
        .strip()
        .split(os.linesep)
    )
    flaky_tests = set(_get_flaky_test_targets(team, operating_system, yaml_dir))

    if get_flaky_tests:
        # run flaky test cases, so we include flaky tests in the list of targets
        # provided by users
        final_targets = flaky_tests.intersection(test_targets)
    else:
        # normal case, we want to exclude flaky tests from the list of targets provided
        # by users
        final_targets = test_targets.difference(flaky_tests)

    if get_high_impact_tests:
        # run high impact test cases, so we include only high impact tests in the list
        # of targets provided by users
        prefix = f"{operating_system}:"
        # TODO(can): we should also move the logic of _get_new_tests into the
        # gen_microcheck_tests function; this is currently blocked by the fact that
        # we need a container to run _get_new_tests
        high_impact_tests = Test.gen_microcheck_tests(
            prefix=prefix,
            bazel_workspace_dir=bazel_workspace_dir,
            team=team,
        ).union(_get_new_tests(prefix, container))
        final_targets = high_impact_tests.intersection(final_targets)

    return list(final_targets)


def _get_new_tests(prefix: str, container: TesterContainer) -> Set[str]:
    """
    Get all local test targets that are not in database
    """
    local_test_targets = set(
        container.run_script_with_output(['bazel query "tests(//...)"'])
        .strip()
        .split(os.linesep)
    )
    db_test_targets = {test.get_target() for test in Test.gen_from_s3(prefix=prefix)}

    return local_test_targets.difference(db_test_targets)


def _get_flaky_test_targets(
    team: str, operating_system: str, yaml_dir: Optional[str] = None
) -> List[str]:
    """
    Get all test targets that are flaky
    """
    if not yaml_dir:
        yaml_dir = os.path.join(bazel_workspace_dir, "ci/ray_ci")

    yaml_flaky_tests = set()
    yaml_flaky_file = os.path.join(yaml_dir, f"{team}.tests.yml")
    if os.path.exists(yaml_flaky_file):
        with open(yaml_flaky_file, "rb") as f:
            # load flaky tests from yaml
            yaml_flaky_tests = set(yaml.safe_load(f)["flaky_tests"])

    # load flaky tests from DB
    s3_flaky_tests = {
        # remove "linux:" prefix for linux tests to be consistent with the
        # interface supported in the yaml file
        test.get_name().lstrip("linux:")
        for test in Test.gen_from_s3(prefix=f"{operating_system}:")
        if test.get_oncall() == team and test.get_state() == TestState.FLAKY
    }
    all_flaky_tests = sorted(yaml_flaky_tests.union(s3_flaky_tests))

    # linux tests are prefixed with "//"
    if operating_system == "linux":
        return [test for test in all_flaky_tests if test.startswith("//")]

    # and other os tests are prefixed with "os:"
    os_prefix = f"{operating_system}:"
    return [
        test.lstrip(os_prefix) for test in all_flaky_tests if test.startswith(os_prefix)
    ]
