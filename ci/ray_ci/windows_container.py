import os
import subprocess
import sys
from typing import List, Tuple, Optional

from ci.ray_ci.container import Container


WORKDIR = "C:\\rayci"


class WindowsContainer(Container):
    def install_ray(self, build_type: Optional[str] = None) -> List[str]:
        assert build_type is None, f"Windows does not support build types {build_type}"
        bazel_cache = os.environ.get("BUILDKITE_BAZEL_CACHE_URL", "")
        pipeline_id = os.environ.get("BUILDKITE_PIPELINE_ID", "")
        cache_readonly = os.environ.get("BUILDKITE_CACHE_READONLY", "")
        subprocess.check_call(
            [
                "docker",
                "build",
                "--build-arg",
                f"BASE_IMAGE={self._get_docker_image()}",
                "--build-arg",
                f"BUILDKITE_BAZEL_CACHE_URL={bazel_cache}",
                "--build-arg",
                f"BUILDKITE_PIPELINE_ID={pipeline_id}",
                "--build-arg",
                f"BUILDKITE_CACHE_READONLY={cache_readonly}",
                "-t",
                self._get_docker_image(),
                "-f",
                "C:\\workdir\\ci\\ray_ci\\windows\\tests.env.Dockerfile",
                "C:\\workdir",
            ],
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

    def get_run_command_shell(self) -> List[str]:
        return ["bash", "-c"]

    def get_run_command_extra_args(
        self,
        gpu_ids: Optional[List[int]] = None,
    ) -> List[str]:
        assert not gpu_ids, "Windows does not support gpu ids"
        return ["--workdir", WORKDIR]

    def get_artifact_mount(self) -> Tuple[str, str]:
        return ("C:\\tmp\\artifacts", "C:\\artifact-mount")
