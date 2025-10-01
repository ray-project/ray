import os
import subprocess
import sys
from typing import List, Optional, Tuple

from ci.ray_ci.container import Container, get_docker_image

_DOCKER_CAP_ADD = [
    "SYS_PTRACE",
    "SYS_ADMIN",
    "NET_ADMIN",
]

DEFAULT_PYTHON_VERSION = "3.9"


class LinuxContainer(Container):
    def __init__(
        self,
        docker_tag: str,
        volumes: Optional[List[str]] = None,
        envs: Optional[List[str]] = None,
        python_version: Optional[str] = None,
        tmp_filesystem: Optional[str] = None,
        privileged: bool = False,
    ) -> None:
        super().__init__(docker_tag, volumes, envs)

        if tmp_filesystem is not None:
            if tmp_filesystem != "tmpfs":
                raise ValueError("Only tmpfs is supported for tmp filesystem")

        self.python_version = python_version or DEFAULT_PYTHON_VERSION
        self.tmp_filesystem = tmp_filesystem
        self.privileged = privileged

    def install_ray(
        self, build_type: Optional[str] = None, mask: Optional[str] = None
    ) -> List[str]:
        cache_readonly = os.environ.get("BUILDKITE_CACHE_READONLY", "")

        env = os.environ.copy()
        env["DOCKER_BUILDKIT"] = "1"
        build_cmd = [
            "docker",
            "build",
            "--pull",
            "--progress=plain",
            "-t",
            self._get_docker_image(),
            "--build-arg",
            f"BASE_IMAGE={self._get_docker_image()}",
            "--build-arg",
            f"BUILDKITE_CACHE_READONLY={cache_readonly}",
            "--build-arg",
            f"BUILD_TYPE={build_type or ''}",
        ]

        if not build_type or build_type == "optimized":
            python_version = self.python_version
            ray_core_image = get_docker_image(f"ray-core-py{python_version}")
            build_cmd += ["--build-arg", f"RAY_CORE_IMAGE={ray_core_image}"]
            ray_dashboard_image = get_docker_image("ray-dashboard")
            build_cmd += ["--build-arg", f"RAY_DASHBOARD_IMAGE={ray_dashboard_image}"]
            if mask:
                build_cmd += ["--build-arg", "RAY_INSTALL_MASK=" + mask]
        else:
            if mask:
                raise ValueError(
                    "install mask is not supported for build type: " + build_type
                )

        build_cmd += ["-f", "ci/ray_ci/tests.env.Dockerfile", "/ray"]
        subprocess.check_call(
            build_cmd,
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
        if self.privileged:
            extra_args += ["--privileged"]
        else:
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
