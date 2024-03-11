import json
import os
import platform
import random
import shutil
import string
import subprocess
from typing import List, Tuple, Optional
from os import path, listdir

from ci.ray_ci.utils import shard_tests, chunk_into_n
from ci.ray_ci.utils import logger
from ci.ray_ci.container import Container
from ray_release.test import TestResult, Test


PIPELINE_POSTMERGE = "0189e759-8c96-4302-b6b5-b4274406bf89"
PIPELINE_MACOS_POSTMERGE = "018e0f94-ccb6-45c2-b072-1e624fe9a404"


class TesterContainer(Container):
    """
    A wrapper for running tests in ray ci docker container
    """

    def __init__(
        self,
        shard_count: int = 1,
        gpus: int = 0,
        bazel_log_dir: str = "/tmp",
        network: Optional[str] = None,
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
        self.bazel_log_dir = bazel_log_dir
        self.shard_count = shard_count
        self.shard_ids = shard_ids or []
        self.test_envs = test_envs or []
        self.build_type = build_type
        self.network = network
        self.gpus = gpus
        assert (
            self.gpus == 0 or self.gpus >= self.shard_count
        ), f"Not enough gpus ({self.gpus} provided) for {self.shard_count} shards"

        if not skip_ray_installation:
            self.install_ray(build_type)

    def _create_bazel_log_mount(self, tmp_dir: Optional[str] = None) -> Tuple[str, str]:
        """
        Create a temporary directory in the current container to store bazel event logs
        produced by the test runs. We do this by using the artifact mount directory from
        the host machine as a shared directory between all containers.
        """
        tmp_dir = tmp_dir or "".join(
            random.choice(string.ascii_lowercase) for _ in range(5)
        )
        artifact_host, artifact_container = self.get_artifact_mount()
        bazel_log_dir_host = os.path.join(artifact_host, tmp_dir)
        bazel_log_dir_container = os.path.join(artifact_container, tmp_dir)
        os.mkdir(bazel_log_dir_container)
        return (bazel_log_dir_host, bazel_log_dir_container)

    def run_tests(
        self,
        team: str,
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
        bazel_log_dir_host, bazel_log_dir_container = self._create_bazel_log_mount()
        runs = [
            self._run_tests_in_docker(
                chunks[i], gpu_ids[i], bazel_log_dir_host, self.test_envs, test_arg
            )
            for i in range(len(chunks))
        ]
        exits = [run.wait() for run in runs]
        self._persist_test_results(team, bazel_log_dir_container)
        self._cleanup_bazel_log_mount(bazel_log_dir_container)

        return all(exit == 0 for exit in exits)

    def _persist_test_results(self, team: str, bazel_log_dir: str) -> None:
        if os.environ.get("BUILDKITE_BRANCH") != "master":
            logger.info("Skip upload test results. We only upload on master branch.")
            return
        if os.environ.get("BUILDKITE_PIPELINE_ID") != PIPELINE_POSTMERGE:
            logger.info(
                "Skip upload test results. We only upload on postmerge pipeline."
            )
            return
        self._upload_build_info(bazel_log_dir)
        TesterContainer.upload_test_results(team, bazel_log_dir)

    def _upload_build_info(self, bazel_log_dir) -> None:
        logger.info("Uploading bazel test logs")
        subprocess.check_call(
            [
                "bash",
                "ci/build/upload_build_info.sh",
                bazel_log_dir,
            ]
        )

    @classmethod
    def upload_test_results(cls, team: str, bazel_log_dir: str) -> None:
        for test, result in cls.get_test_and_results(team, bazel_log_dir):
            logger.info(f"Test {test.get_name()} run status is {result.status}")
            test.update_from_s3()
            test.persist_to_s3()
            test.persist_test_result_to_s3(result)

    @classmethod
    def get_test_and_results(
        cls, team, bazel_log_dir: str
    ) -> List[Tuple[Test, TestResult]]:
        bazel_logs = []
        # Find all bazel logs
        for file in listdir(bazel_log_dir):
            log = path.join(bazel_log_dir, file)
            if path.isfile(log) and file.startswith("bazel_log"):
                bazel_logs.append(log)

        tests = {}
        # Parse bazel logs and print test results
        for file in bazel_logs:
            with open(file, "rb") as f:
                for line in f:
                    event = json.loads(line.decode("utf-8"))
                    if "testResult" not in event:
                        continue
                    test = Test.from_bazel_event(event, team)
                    test_result = TestResult.from_bazel_event(event)
                    # Obtain only the final test result for a given test in case
                    # the test is retried.
                    tests[test.get_name()] = (test, test_result)

        return list(tests.values())

    def _cleanup_bazel_log_mount(self, bazel_log_dir: str) -> None:
        shutil.rmtree(bazel_log_dir)

    def _run_tests_in_docker(
        self,
        test_targets: List[str],
        gpu_ids: List[int],
        bazel_log_dir_host: str,
        test_envs: List[str],
        test_arg: Optional[str] = None,
    ) -> subprocess.Popen:
        logger.info("Running tests: %s", test_targets)
        commands = [
            f'cleanup() {{ chmod -R a+r "{self.bazel_log_dir}"; }}',
            "trap cleanup EXIT",
        ]
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
        return subprocess.Popen(
            self.get_run_command(
                commands,
                network=self.network,
                gpu_ids=gpu_ids,
                volumes=[f"{bazel_log_dir_host}:{self.bazel_log_dir}"],
            )
        )
