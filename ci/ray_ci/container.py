import os
import subprocess
import sys

from typing import List

import ci.ray_ci.bazel_sharding as bazel_sharding

DOCKER_ECR = "029272617770.dkr.ecr.us-west-2.amazonaws.com"
DOCKER_REPO = (
    "rayci_temp_pr"
    if os.environ.get("BUILDKITE_PIPELINE_SLUG") == "premerge"
    else "rayci_temp_branch"
)
DOCKER_TAG = f"{os.environ.get('RAYCI_BUILD_ID')}-forge"


def run_tests(
    test_targets: List[str],
    parallelism,
) -> bool:
    """
    Run tests parallelly in docker.  Return whether all tests pass.
    """
    chunks = [shard_tests(test_targets, parallelism, i) for i in range(parallelism)]
    runs = [_run_tests_in_docker(chunk) for chunk in chunks]
    exits = [run.wait() for run in runs]
    return all(exit == 0 for exit in exits)


def setup_test_environment(team: str) -> None:
    env = os.environ.copy()
    env["DOCKER_BUILDKIT"] = "1"
    subprocess.check_call(
        [
            "docker",
            "build",
            "--build-arg",
            f"BASE_IMAGE={_get_docker_image()}",
            "--build-arg",
            f"TEST_ENVIRONMENT_SCRIPT=ci/ray_ci/{team}.tests.env.sh",
            "-t",
            _get_docker_image(),
            "-f",
            "/ray/ci/ray_ci/tests.env.Dockerfile",
            "/ray",
        ],
        env=env,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )


def _run_tests_in_docker(test_targets: List[str]) -> subprocess.Popen:
    commands = (
        [
            "cleanup() { ./ci/build/upload_build_info.sh; }",
            "trap cleanup EXIT",
        ]
        if os.environ.get("BUILDKITE_BRANCH") == "master"
        else []
    )
    commands.append(
        "bazel test --config=ci $(./ci/run/bazel_export_options) "
        f"{' '.join(test_targets)}",
    )
    return subprocess.Popen(_docker_run_bash_script("\n".join(commands)))


def run_script_in_docker(script: str) -> bytes:
    """
    Run command in docker
    """
    return subprocess.check_output(_docker_run_bash_script(script))


def _docker_run_bash_script(script: str) -> str:
    return _get_docker_run_command() + ["/bin/bash", "-ice", script]


def shard_tests(test_targets: List[str], shard_count: int, shard_id: int) -> List[str]:
    """
    Shard tests into N shards and return the shard corresponding to shard_id
    """
    return bazel_sharding.main(test_targets, index=shard_id, count=shard_count)


def _get_docker_run_command() -> List[str]:
    return [
        "docker",
        "run",
        "-i",
        "--rm",
        "--volume",
        "/tmp/artifacts:/artifact-mount",
        "--env",
        "BUILDKITE_BUILD_URL",
        "--env",
        "BUILDKITE_BRANCH",
        "--env",
        "BUILDKITE_COMMIT",
        "--env",
        "BUILDKITE_JOB_ID",
        "--env",
        "BUILDKITE_LABEL",
        "--cap-add",
        "SYS_PTRACE",
        "--cap-add",
        "SYS_ADMIN",
        "--cap-add",
        "NET_ADMIN",
        "--workdir",
        "/ray",
        "--shm-size=9.69gb",
        _get_docker_image(),
    ]


def _get_docker_image() -> str:
    """
    Get docker image for a particular commit
    """
    return f"{DOCKER_ECR}/{DOCKER_REPO}:{DOCKER_TAG}"
