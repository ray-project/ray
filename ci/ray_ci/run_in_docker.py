"""Run arbitrary commands inside CI build images.

This is a lightweight alternative to test_in_docker for running non-test
commands (e.g. linters, type checkers) inside a CI build image.  Unlike
test_in_docker, it skips install_ray() and instead bind-mounts the checkout
directory so that the current source code is available inside the container.

Usage:
    bazel run //ci/ray_ci:run_in_docker -- \\
        --build-name data9build-py3.10 \\
        "pip install pyrefly && pyrefly check \\$(cat ci/lint/pyrefly-allowlist.txt)"
"""

import os
from typing import List, Optional

import click

from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.linux_container import LinuxContainer
from ci.ray_ci.utils import ci_init, ecr_docker_login


class _RunContainer(LinuxContainer):
    """A container that bind-mounts the checkout and uses /ray as WORKDIR.

    The raw build images (e.g. data9build) only contain dependency files, not
    the full source tree.  We mount ``RAYCI_CHECKOUT_DIR`` at ``/ray`` so that
    the current source code is available, and override ``--workdir`` from the
    default ``/rayci`` (which only exists after ``install_ray()``) to ``/ray``.
    """

    def __init__(self, docker_tag: str, **kwargs) -> None:
        checkout_dir = os.environ.get("RAYCI_CHECKOUT_DIR")
        volumes = kwargs.pop("volumes", None) or []
        if checkout_dir:
            volumes.append(f"{checkout_dir}:/ray")
        super().__init__(docker_tag, volumes=volumes, **kwargs)

    def get_run_command_extra_args(
        self,
        gpu_ids: Optional[List[int]] = None,
    ) -> List[str]:
        args = super().get_run_command_extra_args(gpu_ids)
        try:
            idx = args.index("--workdir")
            args[idx + 1] = "/ray"
        except ValueError:
            pass
        return args


@click.command()
@click.argument("commands", required=True, nargs=-1)
@click.option("--build-name", required=True, help="Name of the Docker build image")
def main(commands, build_name):
    """Run commands in a CI build image."""
    ci_init()
    ecr_docker_login(_DOCKER_ECR_REPO.split("/")[0])
    container = _RunContainer(build_name)
    container.run_script(list(commands))


if __name__ == "__main__":
    main()
