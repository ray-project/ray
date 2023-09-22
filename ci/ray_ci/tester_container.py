import os
import subprocess
from typing import List

from ci.ray_ci.utils import shard_tests
from ci.ray_ci.container import Container
from ci.ray_ci.utils import logger


class TesterContainer(Container):
    """
    A wrapper for running tests in ray ci docker container
    """

    def __init__(self, docker_tag: str) -> None:
        super().__init__(docker_tag)
        self.install_ray()

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
