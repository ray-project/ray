import os
import subprocess
import sys

from typing import List

import ci.ray_ci.bazel_sharding as bazel_sharding

_DOCKER_ECR_REPO = os.environ.get(
    "RAYCI_WORK_REPO",
    "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp",
)
_RAYCI_BUILD_ID = os.environ.get("RAYCI_BUILD_ID", "unknown")


def run_tests(
    team: str,
    test_targets: List[str],
    parallelism: int,
    test_env: List[str],
) -> bool:
    """
    Run tests parallelly in docker.  Return whether all tests pass.
    """
    chunks = [shard_tests(test_targets, parallelism, i) for i in range(parallelism)]
    runs = [_run_tests_in_docker(chunk, team, test_env) for chunk in chunks]
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
            f"BASE_IMAGE={_get_docker_image(team)}",
            "-t",
            _get_docker_image(team),
            "-f",
            "/ray/ci/ray_ci/tests.env.Dockerfile",
            "/ray",
        ],
        env=env,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )


def _run_tests_in_docker(
    test_targets: List[str], 
    team: str, 
    test_env: List[str],
) -> subprocess.Popen:
    commands = []
    if os.environ.get("BUILDKITE_BRANCH", "") == "master":
        commands.extend(
            [
                "cleanup() { ./ci/build/upload_build_info.sh; }",
                "trap cleanup EXIT",
            ]
        )
    commands.append(
        "bazel test --config=ci "
        "$(./ci/run/bazel_export_options) ",
    )
    for env in test_env:
        commands.append(f"{' '.join(f'--test_env={env}')}")
    commands.append(
        f"{' '.join(test_targets)}",
    )
    return subprocess.Popen(_docker_run_bash_script("\n".join(commands), team))


def run_script_in_docker(script: str, team: str) -> bytes:
    """
    Run command in docker
    """
    return subprocess.check_output(_docker_run_bash_script(script, team))


def _docker_run_bash_script(script: str, team: str) -> str:
    return _get_docker_run_command(team) + ["/bin/bash", "-ice", script]


def shard_tests(test_targets: List[str], shard_count: int, shard_id: int) -> List[str]:
    """
    Shard tests into N shards and return the shard corresponding to shard_id
    """
    return bazel_sharding.main(test_targets, index=shard_id, count=shard_count)


def _get_docker_run_command(team) -> List[str]:
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
        "BUILDKITE_PIPELINE_ID",
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
        "/rayci",
        "--shm-size=2.5gb",
        _get_docker_image(team),
    ]


def _get_docker_image(team: str) -> str:
    """
    Get docker image for a particular commit
    """
    return f"{_DOCKER_ECR_REPO}:{_RAYCI_BUILD_ID}-{team}build"
