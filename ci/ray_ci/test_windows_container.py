import sys
import pytest
from unittest import mock
from typing import List

from ci.ray_ci.windows_container import WindowsContainer
from ci.ray_ci.container import _DOCKER_ENV


def test_install_ray() -> None:
    install_ray_cmds = []

    def _mock_subprocess(inputs: List[str], stdout, stderr) -> None:
        install_ray_cmds.append(inputs)

    with mock.patch(
        "subprocess.check_call", side_effect=_mock_subprocess
    ), mock.patch.dict("os.environ", {"BUILDKITE_BAZEL_CACHE_URL": "http://hi.com"}):
        WindowsContainer("hi").install_ray()
        image = (
            "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:unknown-hi"
        )
        assert install_ray_cmds[-1] == [
            "docker",
            "build",
            "--build-arg",
            f"BASE_IMAGE={image}",
            "--build-arg",
            "BUILDKITE_BAZEL_CACHE_URL=http://hi.com",
            "-t",
            image,
            "-f",
            "c:\\workdir\\ci\\ray_ci\\windows\\tests.env.Dockerfile",
            "c:\\workdir",
        ]


def test_get_run_command() -> None:
    container = WindowsContainer("test")
    envs = []
    for env in _DOCKER_ENV:
        envs.extend(["--env", env])

    assert container.get_run_command(["hi", "hello"]) == [
        "docker",
        "run",
        "-i",
        "--rm",
    ] + envs + [
        "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:unknown-test",
        "bash",
        "-c",
        "hi\nhello",
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
