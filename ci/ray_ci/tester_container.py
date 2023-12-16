import os
import platform
import subprocess
from typing import List, Optional

from ci.ray_ci.utils import shard_tests, chunk_into_n
from ci.ray_ci.utils import logger
from ci.ray_ci.container import Container


class TesterContainer(Container):
    """
    A wrapper for running tests in ray ci docker container
    """

    def __init__(
        self,
        shard_count: int = 1,
        gpus: int = 0,
        test_envs: Optional[List[str]] = None,
        shard_ids: Optional[List[int]] = None,
        skip_ray_installation: bool = False,
        build_type: Optional[str] = None,
    ) -> None:
        """
        :param gpu: Number of gpus to use in the container. If 0, used all gpus.
        :param shard_count: The number of shards to split the tests into. This can be
        used to run tests in a distributed fashion.
        :param shard_ids: The list of shard ids to run. If none, run no shards.
        """
        self.shard_count = shard_count
        self.shard_ids = shard_ids or []
        self.test_envs = test_envs or []
        self.build_type = build_type
        self.gpus = gpus
        assert (
            self.gpus == 0 or self.gpus >= self.shard_count
        ), f"Not enough gpus ({self.gpus} provided) for {self.shard_count} shards"

        if not skip_ray_installation:
            self.install_ray(build_type)

    def run_tests(
        self,
        test_targets: List[str],
        test_arg: Optional[str] = None,
    ) -> bool:
        """
        Run tests parallelly in docker.  Return whether all tests pass.
        """
        # shard tests and remove empty chunks
        chunks = list(
            filter(
                len,
                [
                    shard_tests(test_targets, self.shard_count, i)
                    for i in self.shard_ids
                ],
            )
        )
        if not chunks:
            # no tests to run
            return True

        # divide gpus evenly among chunks
        gpu_ids = chunk_into_n(list(range(self.gpus)), len(chunks))
        runs = [
            self._run_tests_in_docker(chunks[i], gpu_ids[i], self.test_envs, test_arg)
            for i in range(len(chunks))
        ]
        exits = [run.wait() for run in runs]
        return all(exit == 0 for exit in exits)

    def _run_tests_in_docker(
        self,
        test_targets: List[str],
        gpu_ids: List[int],
        test_envs: List[str],
        test_arg: Optional[str] = None,
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
        if platform.system() == "Windows":
            # allow window tests to access aws services
            commands.append(
                "powershell ci/pipeline/fix-windows-container-networking.ps1"
            )
        if self.build_type == "ubsan":
            # clang currently runs into problems with ubsan builds, this will revert to
            # using GCC instead.
            commands.append("unset CC CXX")
        # note that we run tests serially within each docker, since we already use
        # multiple dockers to shard tests
        test_cmd = "bazel test --jobs=1 --config=ci $(./ci/run/bazel_export_options) "
        if self.build_type == "debug":
            test_cmd += "--config=ci-debug "
        if self.build_type == "asan":
            test_cmd += "--config=asan --config=asan-buildkite "
        if self.build_type == "clang":
            test_cmd += "--config=llvm "
        if self.build_type == "asan-clang":
            test_cmd += "--config=asan-clang "
        if self.build_type == "ubsan":
            test_cmd += "--config=ubsan "
        if self.build_type == "tsan-clang":
            test_cmd += "--config=tsan-clang "
        for env in test_envs:
            test_cmd += f"--test_env {env} "
        if test_arg:
            test_cmd += f"--test_arg {test_arg} "
        test_cmd += f"{' '.join(test_targets)}"
        commands.append(test_cmd)
        return subprocess.Popen(self.get_run_command(commands, gpu_ids))
