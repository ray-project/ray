import subprocess
import sys
from typing import List, Optional

from ci.ray_ci.container import Container


class WindowsContainer(Container):
    def install_ray(self, build_type: Optional[str] = None) -> List[str]:
        assert build_type is None, f"Windows does not support build types {build_type}"
        subprocess.check_call(
            [
                "docker",
                "build",
                "--build-arg",
                f"BASE_IMAGE={self._get_docker_image()}",
                "-t",
                self._get_docker_image(),
                "-f",
                "c:\\workdir\\ci\\ray_ci\\windows\\tests.env.Dockerfile",
                "c:\\workdir",
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
        assert gpu_ids is None, "Windows does not support gpu ids"
        return []
