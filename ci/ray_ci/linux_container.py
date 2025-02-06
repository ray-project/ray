import os
import subprocess
import sys
from typing import List, Tuple, Optional

from ci.ray_ci.container import Container

_DOCKER_CAP_ADD = [
    "SYS_PTRACE",
    "SYS_ADMIN",
    "NET_ADMIN",
]


class LinuxContainer(Container):
    def __init__(
        self,
        docker_tag: str,
        volumes: Optional[List[str]] = None,
        envs: Optional[List[str]] = None,
        tmp_filesystem: Optional[str] = None,
    ) -> None:
        super().__init__(docker_tag, volumes, envs)

        if tmp_filesystem is not None:
            if tmp_filesystem != "tmpfs":
                raise ValueError("Only tmpfs is supported for tmp filesystem")
        self.tmp_filesystem = tmp_filesystem

    def install_ray(self, build_type: Optional[str] = None) -> List[str]:
        cache_readonly = os.environ.get("BUILDKITE_CACHE_READONLY", "")

        env = os.environ.copy()
        env["DOCKER_BUILDKIT"] = "1"
        subprocess.check_call(
            [
                "docker",
                "build",
                "--pull",
                "--progress=plain",
                "--build-arg",
                f"BASE_IMAGE={self._get_docker_image()}",
                "--build-arg",
                f"BUILD_TYPE={build_type or ''}",
                "--build-arg",
                f"BUILDKITE_CACHE_READONLY={cache_readonly}",
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

    def get_run_command_shell(self) -> List[str]:
        return ["/bin/bash", "-iecuo", "pipefail", "--"]

    def get_run_command_extra_args(
        self,
        gpu_ids: Optional[List[int]] = None,
    ) -> List[str]:
        extra_args = [
            "--add-host",
            "rayci.localhost:host-gateway",
        ]
        if self.tmp_filesystem:
            extra_args += [
                "--mount",
                f"type={self.tmp_filesystem},destination=/tmp",
            ]
        for cap in _DOCKER_CAP_ADD:
            extra_args += ["--cap-add", cap]
        if gpu_ids:
            extra_args += ["--gpus", f'"device={",".join(map(str, gpu_ids))}"']
        extra_args += [
            "--workdir",
            "/rayci",
            "--shm-size=2.5gb",
        ]

        return extra_args

    def get_artifact_mount(self) -> Tuple[str, str]:
        return ("/tmp/artifacts", "/artifact-mount")
