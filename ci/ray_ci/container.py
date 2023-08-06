import os
import subprocess

from typing import List

from ci.ray_ci.utils import chunk_into_n

DOCKER_ECR = "029272617770.dkr.ecr.us-west-2.amazonaws.com"
DOCKER_REPO = "rayci_temp_pr"
DOCKER_TAG = f"{os.environ.get('RAYCI_BUILD_ID')}-forge"


def run_tests(
    team: str,
    test_targets: List[str],
    parallelism,
) -> bool:
    """
    Run tests parallelly in docker.  Return whether all tests pass.
    """
    chunks = chunk_into_n(test_targets, parallelism)
    runs = [_run_tests_in_docker(chunk) for chunk in chunks]
    exits = [run.wait() for run in runs]
    return all(exit == 0 for exit in exits)


def setup_test_environment(team: str) -> None:
    env = os.environ.copy()
    env["DOCKER_BUILDKIT"] = "1"
    subprocess.run(
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
    )


def _run_tests_in_docker(test_targets: List[str]) -> subprocess.Popen:
    command = f"bazel test --config=ci {' '.join(test_targets)}"
    return subprocess.Popen(_docker_run_bash_script(command))


def run_command(script: str) -> bytes:
    """
    Run command in docker
    """
    return subprocess.check_output(_docker_run_bash_script(script))


def _docker_run_bash_script(script: str) -> str:
    return _get_docker_run_command() + ["/bin/bash", "-ice", script]


def _get_docker_run_command() -> List[str]:
    return [
        "docker",
        "run",
        "-i",
        "--rm",
        "--workdir",
        "/tmp/ray",
        "--shm-size=2.5gb",
        _get_docker_image(),
    ]


def _get_docker_image() -> str:
    """
    Get docker image for a particular commit
    """
    return f"{DOCKER_ECR}/{DOCKER_REPO}:{DOCKER_TAG}"
