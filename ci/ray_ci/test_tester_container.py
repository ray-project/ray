import sys
import pytest
from unittest import mock
from typing import List

from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.utils import chunk_into_n


class MockPopen:
    """
    Mock subprocess.Popen. This process returns 0 if both test_targets and
    commands are not empty; otherwise return 1.
    """

    def __init__(self, test_targets: List[str]):
        self.test_targets = test_targets

    def wait(self) -> int:
        return 0 if self.test_targets else 1


def test_run_tests_in_docker() -> None:
    def _mock_popen(input: List[str]) -> None:
        input_str = " ".join(input)
        assert (
            "bazel test --config=ci $(./ci/run/bazel_export_options) "
            "--test_env v=k t1 t2" in input_str
        )

    with mock.patch("subprocess.Popen", side_effect=_mock_popen):
        container = TesterContainer("team")
        container._run_tests_in_docker(["t1", "t2"], ["v=k"])


def test_run_script_in_docker() -> None:
    def _mock_check_output(input: List[str]) -> None:
        input_str = " ".join(input)
        assert "/bin/bash -iecuo pipefail -- run command" in input_str

    with mock.patch("subprocess.check_output", side_effect=_mock_check_output):
        container = TesterContainer("team")
        container.run_script(["run command"])


def test_run_tests() -> None:
    def _mock_run_tests_in_docker(
        test_targets: List[str],
        test_envs: List[str],
    ) -> MockPopen:
        return MockPopen(test_targets)

    def _mock_shard_tests(tests: List[str], workers: int, worker_id: int) -> List[str]:
        return chunk_into_n(tests, workers)[worker_id]

    with mock.patch(
        "ci.ray_ci.tester_container.TesterContainer._run_tests_in_docker",
        side_effect=_mock_run_tests_in_docker,
    ), mock.patch(
        "ci.ray_ci.tester_container.shard_tests", side_effect=_mock_shard_tests
    ):
        container = TesterContainer("team")
        # test_targets are not empty
        assert container.run_tests(["t1", "t2"], [], 2)
        # test_targets is empty after chunking
        assert not container.run_tests(["t1"], [], 2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
