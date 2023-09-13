import os
import subprocess
import sys
from typing import List

from ci.ray_ci.utils import shard_tests
from ci.ray_ci.container import Container
from ci.ray_ci.utils import logger


class TesterContainer(Container):
    """
    A wrapper for running tests in ray ci docker container
    """

    def run_tests(
        self,
        test_targets: List[str],
        test_envs: List[str],
        parallelism: int,
    ) -> bool:
        """
        Run tests parallelly in docker.  Return whether all tests pass.
        """
        chunks = [shard_tests(test_targets, parallelism, i) for i in range(parallelism)]
        runs = [
            self._run_tests_in_docker(chunk, test_envs) for chunk in chunks if chunk
        ]
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

    def _run_tests_in_docker(
        self,
        test_targets: List[str],
        test_envs: List[str],
    ) -> subprocess.Popen:
        logger.info("Running tests: %s", test_targets)
        commands = []
        if os.environ.get("BUILDKITE_BRANCH", "") == "master":
            commands.extend(
                [
                    "cleanup() { ./ci/build/upload_build_info.sh; }",
                    "trap cleanup EXIT",
                ]
            )
        test_cmd = "bazel test --config=ci $(./ci/run/bazel_export_options) "
        for env in test_envs:
            test_cmd += f"--test_env {env} "
        test_cmd += f"{' '.join(test_targets)}"
        commands.append(test_cmd)
        return subprocess.Popen(self._get_run_command(commands))
