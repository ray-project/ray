import sys
import pytest
from unittest import mock
from typing import List, Optional

from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.utils import chunk_into_n


class MockPopen:
    """
    Mock subprocess.Popen. This process returns 1 if test targets is empty or contains
    bad_test; otherwise return 0.
    """

    def __init__(self, test_targets: List[str]):
        self.test_targets = test_targets

    def wait(self) -> int:
        return 1 if "bad_test" in self.test_targets or not self.test_targets else 0


def test_run_tests_in_docker() -> None:
    def _mock_popen(input: List[str]) -> None:
        input_str = " ".join(input)
        assert (
            "bazel test --config=ci $(./ci/run/bazel_export_options) "
            "--test_env v=k --test_arg flag t1 t2" in input_str
        )

    with mock.patch("subprocess.Popen", side_effect=_mock_popen), mock.patch(
        "ci.ray_ci.tester_container.TesterContainer.install_ray",
        return_value=None,
    ):
        container = TesterContainer("team")
        container._run_tests_in_docker(["t1", "t2"], ["v=k"], "flag")


def test_run_script_in_docker() -> None:
    def _mock_check_output(input: List[str]) -> None:
        input_str = " ".join(input)
        assert "/bin/bash -iecuo pipefail -- run command" in input_str

    with mock.patch(
        "subprocess.check_output", side_effect=_mock_check_output
    ), mock.patch(
        "ci.ray_ci.tester_container.TesterContainer.install_ray",
        return_value=None,
    ):
        container = TesterContainer("team")
        container.run_script_with_output(["run command"])


def test_skip_ray_installation() -> None:
    install_ray_called = []

    def _mock_install_ray() -> None:
        install_ray_called.append(True)

    with mock.patch(
        "ci.ray_ci.tester_container.TesterContainer.install_ray",
        side_effect=_mock_install_ray,
    ):
        assert len(install_ray_called) == 0
        TesterContainer("team", skip_ray_installation=False)
        assert len(install_ray_called) == 1
        TesterContainer("team", skip_ray_installation=True)
        assert len(install_ray_called) == 1


def test_run_tests() -> None:
    def _mock_run_tests_in_docker(
        test_targets: List[str],
        test_envs: List[str],
        test_arg: Optional[str] = None,
    ) -> MockPopen:
        return MockPopen(test_targets)

    def _mock_shard_tests(tests: List[str], workers: int, worker_id: int) -> List[str]:
        return chunk_into_n(tests, workers)[worker_id]

    with mock.patch(
        "ci.ray_ci.tester_container.TesterContainer._run_tests_in_docker",
        side_effect=_mock_run_tests_in_docker,
    ), mock.patch(
        "ci.ray_ci.tester_container.shard_tests", side_effect=_mock_shard_tests
    ), mock.patch(
        "ci.ray_ci.tester_container.TesterContainer.install_ray",
        return_value=None,
    ):
        container = TesterContainer("team", shard_count=2, shard_ids=[0, 1])
        # test_targets are not empty
        assert container.run_tests(["t1", "t2"], [])
        # test_targets is empty after chunking, but not creating popen
        assert container.run_tests(["t1"], [])
        assert container.run_tests([], [])
        # test targets contain bad_test
        assert not container.run_tests(["bad_test"], [])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
