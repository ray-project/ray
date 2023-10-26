import os
import subprocess
import sys

from typing import List, Optional

_DOCKER_ECR_REPO = os.environ.get(
    "RAYCI_WORK_REPO",
    "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp",
)
_DOCKER_ENV = [
    "BUILDKITE_BUILD_URL",
    "BUILDKITE_BRANCH",
    "BUILDKITE_COMMIT",
    "BUILDKITE_JOB_ID",
    "BUILDKITE_LABEL",
    "BUILDKITE_PIPELINE_ID",
]
_DOCKER_CAP_ADD = [
    "SYS_PTRACE",
    "SYS_ADMIN",
    "NET_ADMIN",
]
_RAYCI_BUILD_ID = os.environ.get("RAYCI_BUILD_ID", "unknown")


class Container:
    """
    A wrapper for running commands in ray ci docker container
    """

    def __init__(self, docker_tag: str, volumes: Optional[List[str]] = None) -> None:
        self.docker_tag = docker_tag
        self.volumes = volumes or []

    def run_script_with_output(self, script: List[str]) -> bytes:
        """
        Run a script in container and returns output
        """
        return subprocess.check_output(self._get_run_command(script))

    def run_script(self, script: List[str]) -> None:
        """
        Run a script in container
        """
        return subprocess.check_call(
            self._get_run_command(script),
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

    def install_ray(self, build_type: Optional[str] = None) -> None:
        env = os.environ.copy()
        env["DOCKER_BUILDKIT"] = "1"
        subprocess.check_call(
            [
                "docker",
                "build",
                "--pull",
                "--build-arg",
                f"BASE_IMAGE={self._get_docker_image()}",
                "--build-arg",
                f"BUILD_TYPE={build_type or ''}",
                "-t",
                self._get_docker_image(),
                "-f",
                "/ray/ci/ray_ci/tests.env.Dockerfile",
                "/ray",
            ],
            env=env,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

    def _get_run_command(
        self,
        script: List[str],
        gpu_ids: Optional[List[int]] = None,
    ) -> List[str]:
        command = [
            "docker",
            "run",
            "-i",
            "--rm",
            "--volume",
            "/tmp/artifacts:/artifact-mount",
        ]
        for volume in self.volumes:
            command += ["--volume", volume]
        for env in _DOCKER_ENV:
            command += ["--env", env]
        for cap in _DOCKER_CAP_ADD:
            command += ["--cap-add", cap]
        if gpu_ids:
            command += ["--gpus", f'"device={",".join(map(str, gpu_ids))}"']
        command += [
            "--workdir",
            "/rayci",
            "--shm-size=2.5gb",
            self._get_docker_image(),
            "/bin/bash",
            "-iecuo",
            "pipefail",
            "--",
            "\n".join(script),
        ]

        return command

    def _get_docker_image(self) -> str:
        """
        Get docker image for a particular commit
        """
        return f"{_DOCKER_ECR_REPO}:{_RAYCI_BUILD_ID}-{self.docker_tag}"
