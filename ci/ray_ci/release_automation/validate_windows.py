from ci.ray_ci.container import _DOCKER_ECR_REPO
from ci.ray_ci.utils import docker_login, logger, chunk_into_n
from ci.ray_ci.windows_container import WindowsContainer
from typing import Optional, List, Tuple
import os
import subprocess
import random
import string
import click
bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")

@click.command()
@click.option(
    "--test-env",
    multiple=True,
    type=str,
    help="Environment variables to set for the test.",
)
@click.option(
    "--build-name",
    type=str,
    help="Name of the build used to run tests",
)
def main(
    build_name: str,
    test_env: Tuple[str],
) -> None:
    if not bazel_workspace_dir:
        raise Exception("Please use `bazelisk run //ci/ray_ci`")
    os.environ["RAYCI_BUILD_ID"] = "6e2156af"
    os.environ["BUILDKITE_BAZEL_CACHE_URL"] = "https://bazel-cache-dev.s3.us-west-2.amazonaws.com"
    os.chdir(bazel_workspace_dir)
    docker_login(_DOCKER_ECR_REPO.split("/")[0])

    container = _get_container(build_name, list(test_env))
    container.run_sanity_check()

class WindowsValidateContainer(WindowsContainer):
    def __init__(
        self,
        docker_tag: str,
        test_envs: Optional[List[str]] = None,
    ) -> None:
        WindowsContainer.__init__(self, docker_tag, test_envs)
        self.install_ray()

    def run_sanity_check(self):
        logger.info("Run sanity check in container")
        commands = [
            f'cleanup() {{ chmod -R a+r "{self.bazel_log_dir}"; }}',
            "trap cleanup EXIT",
        ]

        commands.append(
            "powershell ci/pipeline/fix-windows-container-networking.ps1"
        )
        commands.append(
            ".buildkite/release-automation/verifiy-windows-wheels.sh"
        )
        return subprocess.Popen(
            self.get_run_command(
                commands
            )
        )


def _get_container(
    build_name: str,
    test_envs: List[str],
):
    return WindowsValidateContainer(build_name, test_envs)