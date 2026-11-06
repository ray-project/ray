import os
import platform
import subprocess
import sys
from typing import List, Optional, Tuple

from ci.ray_ci.configs import DEFAULT_ARCHITECTURE, DEFAULT_PYTHON_VERSION
from ci.ray_ci.container import Container, get_docker_image

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
        python_version: Optional[str] = None,
        tmp_filesystem: Optional[str] = None,
        architecture: Optional[str] = None,
        privileged: bool = False,
    ) -> None:
        super().__init__(docker_tag, volumes, envs)

        if tmp_filesystem is not None:
            if tmp_filesystem != "tmpfs":
                raise ValueError("Only tmpfs is supported for tmp filesystem")

        self.python_version = python_version or DEFAULT_PYTHON_VERSION
        self.tmp_filesystem = tmp_filesystem
        self.privileged = privileged

        if architecture is None:
            architecture = platform.machine()
            if architecture.lower() == "amd64":
                architecture = "x86_64"
            if architecture == "arm64":
                architecture = "aarch64"
        self.architecture = architecture

    def install_ray(
        self, build_type: Optional[str] = None, mask: Optional[str] = None
    ) -> List[str]:
        cache_readonly = os.environ.get("BUILDKITE_CACHE_READONLY", "")
        # The step's own index, forwarded into the build: docker builds inherit
        # nothing from the step environment, and the mirror-hosted index
        # (ci/pypi_proxy_profile.sh) is one HTTPS URL reachable from inside builds.
        # Unset outside CI, and then the Dockerfile falls back to PyPI.
        image_index_url = os.environ.get("RAYCI_IMAGE_PIP_INDEX_URL", "")

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
            f"BUILD_TYPE={build_type or ''}",
            "--build-arg",
            f"BUILDKITE_CACHE_READONLY={cache_readonly}",
            "--build-arg",
            f"RAYCI_IMAGE_PIP_INDEX_URL={image_index_url}",
        ]

        if not build_type or build_type in (
            "optimized",
            "wheel",
            "wheel-aarch64",
        ):
            for base_tag, arg_name in [
                (f"ray-core-py{self.python_version}", "RAY_CORE_IMAGE"),
                ("ray-dashboard", "RAY_DASHBOARD_IMAGE"),
            ]:
                if self.architecture != DEFAULT_ARCHITECTURE:
                    base_tag += f"-{self.architecture}"
                build_cmd += ["--build-arg", f"{arg_name}={get_docker_image(base_tag)}"]

        if build_type in ("wheel", "wheel-aarch64"):
            base_tag = f"ray-wheel-py{self.python_version}"
            if self.architecture != DEFAULT_ARCHITECTURE:
                base_tag += f"-{self.architecture}"
            build_cmd += [
                "--build-arg",
                f"RAY_WHEEL_IMAGE={get_docker_image(base_tag)}",
            ]

        if mask:
            build_cmd += ["--build-arg", "RAY_INSTALL_MASK=" + mask]

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
