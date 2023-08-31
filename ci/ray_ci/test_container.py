import os
import subprocess
import sys
from typing import List

from ci.ray_ci.utils import shard_tests
from ci.ray_ci.container import Container


class TestContainer(Container):
    """
    A wrapper for running tests in ray ci docker container
    """

    def __init__(self, team: str) -> None:
        super().__init__(f"{team}build")

    def run_tests(self, test_targets: List[str], parallelism) -> bool:
        """
        Run tests parallelly in docker.  Return whether all tests pass.
        """
        chunks = [shard_tests(test_targets, parallelism, i) for i in range(parallelism)]
        runs = [self._run_tests_in_docker(chunk) for chunk in chunks]
        exits = [run.wait() for run in runs]
        return all(exit == 0 for exit in exits)

    def setup_test_environment(self) -> None:
        """
        Build the docker image for running tests
        """
        env = os.environ.copy()
        env["DOCKER_BUILDKIT"] = "1"
        subprocess.check_call(
            [
                "docker",
                "build",
                "--build-arg",
                f"BASE_IMAGE={self._get_docker_image()}",
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

    def _run_tests_in_docker(self, test_targets: List[str]) -> subprocess.Popen:
        commands = []
        if os.environ.get("BUILDKITE_BRANCH", "") == "master":
            commands.extend(
                [
                    "cleanup() { ./ci/build/upload_build_info.sh; }",
                    "trap cleanup EXIT",
                ]
            )
        commands.append(
            "bazel test --config=ci $(./ci/run/bazel_export_options) "
            f"{' '.join(test_targets)}",
        )
        return subprocess.Popen(self._get_run_command("\n".join(commands)))
