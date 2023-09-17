import os
import subprocess

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

    def run_script(self, script: List[str]) -> bytes:
        """
        Run a script in container
        """
        return subprocess.check_output(self._get_run_command(script))

    def _get_run_command(self, script: List[str]) -> List[str]:
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
