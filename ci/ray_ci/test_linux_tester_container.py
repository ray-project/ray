import json
import os
import platform
import sys
import pytest
import tempfile
from unittest import mock
from typing import List, Optional

from ci.ray_ci.linux_tester_container import LinuxTesterContainer
from ci.ray_ci.utils import chunk_into_n
from ci.ray_ci.container import _DOCKER_ECR_REPO, _RAYCI_BUILD_ID


class MockPopen:
    """
    Mock subprocess.Popen. This process returns 1 if test targets is empty or contains
    bad_test; otherwise return 0.
    """

    def __init__(self, test_targets: List[str]):
        self.test_targets = test_targets

    def wait(self) -> int:
        return 1 if "bad_test" in self.test_targets or not self.test_targets else 0


def test_enough_gpus() -> None:
    # not enough gpus
    try:
        LinuxTesterContainer("team", shard_count=2, gpus=1, skip_ray_installation=True)
    except AssertionError:
        pass
    else:
        assert False, "Should raise an AssertionError"

    # not enough gpus
    try:
        LinuxTesterContainer("team", shard_count=1, gpus=1, skip_ray_installation=True)
    except AssertionError:
        assert False, "Should not raise an AssertionError"


def test_run_tests_in_docker() -> None:
    inputs = []

    def _mock_popen(input: List[str]) -> None:
        inputs.append(" ".join(input))

    with mock.patch("subprocess.Popen", side_effect=_mock_popen), mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer.install_ray",
        return_value=None,
    ):
        LinuxTesterContainer(
            "team", network="host", build_type="debug", test_envs=["ENV_01", "ENV_02"]
        )._run_tests_in_docker(["t1", "t2"], [0, 1], "/tmp", ["v=k"], "flag")
        input_str = inputs[-1]
        assert "--env ENV_01 --env ENV_02 --env BUILDKITE" in input_str
        assert "--network host" in input_str
        assert '--gpus "device=0,1"' in input_str
        assert "--volume /tmp:/tmp/bazel_event_logs" in input_str
        assert (
            "bazel test --jobs=1 --config=ci $(./ci/run/bazel_export_options) "
            "--config=ci-debug --test_env v=k --test_arg flag t1 t2" in input_str
        )

        LinuxTesterContainer("team")._run_tests_in_docker(
            ["t1", "t2"], [], "/tmp", ["v=k"]
        )
        input_str = inputs[-1]
        assert "--env BUILDKITE_BUILD_URL" in input_str
        assert "--gpus" not in input_str


def test_run_script_in_docker() -> None:
    def _mock_check_output(input: List[str]) -> None:
        input_str = " ".join(input)
        assert "/bin/bash -iecuo pipefail -- run command" in input_str

    with mock.patch(
        "subprocess.check_output", side_effect=_mock_check_output
    ), mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer.install_ray",
        return_value=None,
    ):
        container = LinuxTesterContainer("team")
        container.run_script_with_output(["run command"])


def test_skip_ray_installation() -> None:
    install_ray_called = []

    def _mock_install_ray(build_type: Optional[str]) -> None:
        install_ray_called.append(True)

    with mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer.install_ray",
        side_effect=_mock_install_ray,
    ):
        assert len(install_ray_called) == 0
        LinuxTesterContainer("team", skip_ray_installation=False)
        assert len(install_ray_called) == 1
        LinuxTesterContainer("team", skip_ray_installation=True)
        assert len(install_ray_called) == 1


def test_ray_installation() -> None:
    install_ray_cmds = []

    def _mock_subprocess(inputs: List[str], env, stdout, stderr) -> None:
        install_ray_cmds.append(inputs)

    with mock.patch("subprocess.check_call", side_effect=_mock_subprocess):
        LinuxTesterContainer("team", build_type="debug")
        docker_image = f"{_DOCKER_ECR_REPO}:{_RAYCI_BUILD_ID}-team"
        assert install_ray_cmds[-1] == [
            "docker",
            "build",
            "--pull",
            "--build-arg",
            f"BASE_IMAGE={docker_image}",
            "--build-arg",
            "BUILD_TYPE=debug",
            "-t",
            docker_image,
            "-f",
            "/ray/ci/ray_ci/tests.env.Dockerfile",
            "/ray",
        ]


def test_run_tests() -> None:
    def _mock_run_tests_in_docker(
        test_targets: List[str],
        gpu_ids: List[int],
        bazel_log_dir: str,
        test_envs: List[str],
        test_arg: Optional[str] = None,
    ) -> MockPopen:
        return MockPopen(test_targets)

    def _mock_shard_tests(tests: List[str], workers: int, worker_id: int) -> List[str]:
        return chunk_into_n(tests, workers)[worker_id]

    with tempfile.TemporaryDirectory() as tmpdir, mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer.get_artifact_mount",
        return_value=("/tmp/artifacts", tmpdir),
    ), mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer._persist_test_results",
        return_value=None,
    ), mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer._run_tests_in_docker",
        side_effect=_mock_run_tests_in_docker,
    ), mock.patch(
        "ci.ray_ci.tester_container.shard_tests", side_effect=_mock_shard_tests
    ), mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer.install_ray",
        return_value=None,
    ):
        container = LinuxTesterContainer("team", shard_count=2, shard_ids=[0, 1])
        # test_targets are not empty
        assert container.run_tests("manu", ["t1", "t2"], [])
        # test_targets is empty after chunking, but not creating popen
        assert container.run_tests("manu", ["t1"], [])
        assert container.run_tests("manu", [], [])
        # test targets contain bad_test
        assert not container.run_tests("manu", ["bad_test"], [])


def test_create_bazel_log_mount() -> None:
    with tempfile.TemporaryDirectory() as tmpdir, mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer.get_artifact_mount",
        return_value=("/tmp/artifacts", tmpdir),
    ):
        container = LinuxTesterContainer("team", skip_ray_installation=True)
        assert container._create_bazel_log_mount("w00t") == (
            "/tmp/artifacts/w00t",
            os.path.join(tmpdir, "w00t"),
        )


def test_get_test_results() -> None:
    _BAZEL_LOGS = [
        json.dumps(log)
        for log in [
            {
                "id": {"testResult": {"label": "//ray/ci:test"}},
                "testResult": {"status": "FAILED"},
            },
            {
                "id": {"testResult": {"label": "//ray/ci:reef"}},
                "testResult": {"status": "FAILED"},
            },
            {
                "id": {"testResult": {"label": "//ray/ci:test"}},
                "testResult": {"status": "FAILED"},
            },
            {
                "id": {"testResult": {"label": "//ray/ci:test"}},
                "testResult": {"status": "PASSED"},
            },
        ]
    ]

    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "bazel_log"), "w") as f:
            f.write("\n".join(_BAZEL_LOGS))
        container = LinuxTesterContainer("docker_tag", skip_ray_installation=True)
        results = container._get_test_and_results("manu", tmp)
        results.sort(key=lambda x: x[0].get_name())

        test, result = results[0]
        assert test.get_name() == f"{platform.system().lower()}://ray/ci:reef"
        assert test.get_oncall() == "manu"
        assert result.is_failing()

        test, result = results[1]
        assert test.get_name() == f"{platform.system().lower()}://ray/ci:test"
        assert test.get_oncall() == "manu"
        assert result.is_passing()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
