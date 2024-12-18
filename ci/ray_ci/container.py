import abc
import os
import subprocess
import sys

from typing import List, Tuple, Optional

_CUDA_COPYRIGHT = """
==========
== CUDA ==
==========

CUDA Version 12.1.1

Container image Copyright (c) 2016-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.

This container image and its contents are governed by the NVIDIA Deep Learning Container License.
By pulling and using the container, you accept the terms and conditions of this license:
https://developer.nvidia.com/ngc/nvidia-deep-learning-container-license

A copy of this license is made available in this container at /NGC-DL-CONTAINER-LICENSE for your convenience.
"""  # noqa: E501
_DOCKER_ECR_REPO = os.environ.get(
    "RAYCI_WORK_REPO",
    "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp",
)
_DOCKER_GCP_REGISTRY = os.environ.get(
    "RAYCI_GCP_REGISTRY",
    "us-west1-docker.pkg.dev/anyscale-oss-ci",
)
_DOCKER_ENV = [
    "BUILDKITE",
    "BUILDKITE_BUILD_URL",
    "BUILDKITE_BRANCH",
    "BUILDKITE_COMMIT",
    "BUILDKITE_JOB_ID",
    "BUILDKITE_LABEL",
    "BUILDKITE_BAZEL_CACHE_URL",
    "BUILDKITE_PIPELINE_ID",
    "BUILDKITE_PULL_REQUEST",
]
_RAYCI_BUILD_ID = os.environ.get("RAYCI_BUILD_ID", "unknown")


class Container(abc.ABC):
    """
    A wrapper for running commands in ray ci docker container
    """

    def __init__(
        self,
        docker_tag: str,
        volumes: Optional[List[str]] = None,
        envs: Optional[List[str]] = None,
    ) -> None:
        self.docker_tag = docker_tag
        self.volumes = volumes or []
        self.envs = envs or []
        self.envs += _DOCKER_ENV

    def run_script_with_output(self, script: List[str]) -> str:
        """
        Run a script in container and returns output
        """
        # CUDA image comes with a license header that we need to remove
        return (
            subprocess.check_output(self.get_run_command(script))
            .decode("utf-8")
            .replace(_CUDA_COPYRIGHT, "")
        )

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
