import os
import subprocess

from typing import List

from ci.ray_ci.utils import chunk_into_n

DOCKER_ECR = "029272617770.dkr.ecr.us-west-2.amazonaws.com"
DOCKER_REPO = "ci_base_images"
DOCKER_TAG = f"oss-ci-build_{os.environ.get('BUILDKITE_COMMIT')}"


def run_tests(
    test_targets: List[str],
    pre_run_commands: List[str],
    parallelism,
) -> bool:
    """
    Run tests parallelly in docker. Return whether all tests pass.
    """
    chunks = chunk_into_n(test_targets, parallelism)
    # Run tests in parallel. Currently, the logs are also printed in parallel.
    # TODO(can): We can use a queue to print the logs in order.
    runs = [_run_tests_in_docker(chunk, pre_run_commands) for chunk in chunks]
    exits = [run.wait() for run in runs]
    return all(exit == 0 for exit in exits)


def _run_tests_in_docker(
    test_targets: List[str],
    pre_test_commands: List[str],
) -> subprocess.Popen:
    bazel_command = ["bazel", "test", "--config=ci"] + test_targets
    script = "\n".join(pre_test_commands + [" ".join(bazel_command)])
    return subprocess.Popen(_docker_run_bash_script(script))


def run_command(script: str) -> bytes:
    """
    Run command in docker
    """
    return subprocess.check_output(_docker_run_bash_script(script))


def _docker_run_bash_script(script: str) -> str:
    return _get_docker_run_command() + ["/bin/bash", "-ice", script]


def docker_login() -> None:
    """
    Login to docker with AWS credentials
    """
    subprocess.run(["pip", "install", "awscli"])
    password = subprocess.check_output(
        ["aws", "ecr", "get-login-password", "--region", "us-west-2"],
    )
    subprocess.run(
        [
            "docker",
            "login",
            "--username",
            "AWS",
            "--password-stdin",
            DOCKER_ECR,
        ],
        stdin=password,
    )


def _get_docker_run_command() -> List[str]:
    return [
        "docker",
        "run",
        "-i",
        "--rm",
        "--workdir",
        "/ray",
        "--shm-size=2.5gb",
        _get_docker_image(),
    ]


def _get_docker_image() -> str:
    """
    Get docker image for a particular commit
    """
    return f"{DOCKER_ECR}/{DOCKER_REPO}:{DOCKER_TAG}"
