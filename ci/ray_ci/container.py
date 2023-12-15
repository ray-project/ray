import abc
import os
import subprocess
import sys

from typing import List, Optional

_DOCKER_ECR_REPO = os.environ.get(
    "RAYCI_WORK_REPO",
    "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp",
)
_DOCKER_GCP_REGISTRY = os.environ.get(
    "RAYCI_GCP_REGISTRY",
    "us-west1-docker.pkg.dev/anyscale-oss-ci",
)
_DOCKER_ENV = [
    "BUILDKITE_BUILD_URL",
    "BUILDKITE_BRANCH",
    "BUILDKITE_COMMIT",
    "BUILDKITE_JOB_ID",
    "BUILDKITE_LABEL",
    "BUILDKITE_PIPELINE_ID",
]
_RAYCI_BUILD_ID = os.environ.get("RAYCI_BUILD_ID", "unknown")


class Container(abc.ABC):
    """
    A wrapper for running commands in ray ci docker container
    """

    def __init__(self, docker_tag: str, envs: Optional[List[str]] = None) -> None:
        self.docker_tag = docker_tag
        self.envs = envs or []
        self.envs += _DOCKER_ENV

    def run_script_with_output(self, script: List[str]) -> bytes:
        """
        Run a script in container and returns output
        """
        return subprocess.check_output(self.get_run_command(script))

    def run_script(self, script: List[str]) -> None:
        """
        Run a script in container
        """
        return subprocess.check_call(
            self.get_run_command(script),
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

    def _get_docker_image(self) -> str:
        """
        Get docker image for a particular commit
        """
        return f"{_DOCKER_ECR_REPO}:{_RAYCI_BUILD_ID}-{self.docker_tag}"

    @abc.abstractmethod
    def install_ray(self, build_type: Optional[str] = None) -> None:
        """
        Build and install ray in container
        :param build_type: opt, asan, tsan, etc.
        """
        pass

    def get_run_command(
        self,
        script: List[str],
        gpu_ids: Optional[List[int]] = None,
    ) -> List[str]:
        """
        Get docker run command
        :param script: script to run in container
        :param gpu_ids: ids of gpus on the host machine
        """
        command = ["docker", "run", "-i", "--rm"]
        for env in self.envs:
            command += ["--env", env]
        return (
            command
            + self.get_run_command_extra_args(gpu_ids)
            + [self._get_docker_image()]
            + self.get_run_command_shell()
            + ["\n".join(script)]
        )

    @abc.abstractmethod
    def get_run_command_shell(self) -> List[str]:
        pass

    @abc.abstractmethod
    def get_run_command_extra_args(
        self,
        gpu_ids: Optional[List[int]] = None,
    ) -> List[str]:
        pass
