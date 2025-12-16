import abc
import os
import re
import subprocess
import sys
from typing import List, Optional, Tuple

# Regex pattern to match CUDA copyright header with any version
_CUDA_COPYRIGHT_PATTERN = r"""==========
== CUDA ==
==========

CUDA Version \d+\.\d+(?:\.\d+)?

Container image Copyright \(c\) 2016-2023, NVIDIA CORPORATION & AFFILIATES\. All rights reserved\.

This container image and its contents are governed by the NVIDIA Deep Learning Container License\.
By pulling and using the container, you accept the terms and conditions of this license:
https://developer\.nvidia\.com/ngc/nvidia-deep-learning-container-license

A copy of this license is made available in this container at /NGC-DL-CONTAINER-LICENSE for your convenience\.
"""

_AZURE_REGISTRY_NAME = "rayreleasetest"
_DOCKER_ECR_REPO = os.environ.get(
    "RAYCI_WORK_REPO",
    "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp",
)
_DOCKER_GCP_REGISTRY = os.environ.get(
    "RAYCI_GCP_REGISTRY",
    "us-west1-docker.pkg.dev/anyscale-oss-ci",
)
_DOCKER_AZURE_REGISTRY = os.environ.get(
    "RAYCI_AZURE_REGISTRY",
    "rayreleasetest.azurecr.io",
)
_DOCKER_ENV = [
    "BUILDKITE",
    "BUILDKITE_BUILD_URL",
    "BUILDKITE_BRANCH",
    "BUILDKITE_COMMIT",
    "BUILDKITE_JOB_ID",
    "BUILDKITE_LABEL",
    "BUILDKITE_PIPELINE_ID",
    "BUILDKITE_PULL_REQUEST",
    "BUILDKITE_BAZEL_CACHE_URL",
    "BUILDKITE_CACHE_READONLY",
]
_RAYCI_BUILD_ID = os.environ.get("RAYCI_BUILD_ID", "")


def get_docker_image(
    docker_tag: str,
    build_id: Optional[str] = None,
    docker_repo: Optional[str] = None,
) -> str:
    """Get rayci image for a particular tag.

    Args:
        docker_tag: The tag for the docker image.
        build_id: The build ID to prepend to the tag (for CI-built images).
        docker_repo: The docker repository. Defaults to RAYCI_WORK_REPO.
            If a non-default repo is provided, the image is treated as external
            and the build_id prefix is not added.
    """
    if docker_repo is not None and docker_repo != _DOCKER_ECR_REPO:
        return f"{docker_repo}:{docker_tag}"

    docker_repo = _DOCKER_ECR_REPO
    if not build_id:
        build_id = _RAYCI_BUILD_ID
    if build_id:
        return f"{docker_repo}:{build_id}-{docker_tag}"
    return f"{docker_repo}:{docker_tag}"


class Container(abc.ABC):
    """
    A wrapper for running commands in ray ci docker container
    """

    def __init__(
        self,
        docker_tag: str,
        docker_repo: Optional[str] = None,
        volumes: Optional[List[str]] = None,
        envs: Optional[List[str]] = None,
    ) -> None:
        self.docker_tag = docker_tag
        self.docker_repo = docker_repo
        self.volumes = volumes or []
        self.envs = envs or []
        self.envs += _DOCKER_ENV

    def run_script_with_output(self, script: List[str]) -> str:
        """
        Run a script in container and returns output
        """
        # CUDA image comes with a license header that we need to remove
        output = subprocess.check_output(self.get_run_command(script)).decode("utf-8")
        # Use regex to remove CUDA copyright header with any version
        return re.sub(_CUDA_COPYRIGHT_PATTERN, "", output, flags=re.MULTILINE)

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
        """Get docker image for a particular commit."""
        return get_docker_image(self.docker_tag, docker_repo=self.docker_repo)

    @abc.abstractmethod
    def install_ray(
        self, build_type: Optional[str] = None, mask: Optional[str] = None
    ) -> None:
        """
        Build and install ray in container
        :param build_type: opt, asan, tsan, etc.
        :param mask: a string that sends into the build to mask components.
        """
        pass

    def get_run_command(
        self,
        script: List[str],
        network: Optional[str] = None,
        gpu_ids: Optional[List[int]] = None,
        volumes: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Get docker run command
        :param script: script to run in container
        :param gpu_ids: ids of gpus on the host machine
        """
        artifact_mount_host, artifact_mount_container = self.get_artifact_mount()
        command = [
            "docker",
            "run",
            "-i",
            "--rm",
            "--volume",
            f"{artifact_mount_host}:{artifact_mount_container}",
        ]
        for env in self.envs:
            command += ["--env", env]
        if network:
            command += ["--network", network]
        for volume in (volumes or []) + self.volumes:
            command += ["--volume", volume]
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

    @abc.abstractmethod
    def get_artifact_mount(self) -> Tuple[str, str]:
        """
        Get artifact mount path on host and container
        """
        pass
