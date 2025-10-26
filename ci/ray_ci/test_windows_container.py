import sys
from typing import List
from unittest import mock

import pytest

from ci.ray_ci.container import _DOCKER_ENV
from ci.ray_ci.windows_container import WindowsContainer


def test_install_ray() -> None:
    install_ray_cmds = []

    def _mock_subprocess(inputs: List[str], stdout, stderr) -> None:
        install_ray_cmds.append(inputs)

    with mock.patch(
        "subprocess.check_call", side_effect=_mock_subprocess
    ), mock.patch.dict(
        "os.environ",
        {
            "BUILDKITE_BAZEL_CACHE_URL": "http://hi.com",
            "BUILDKITE_CACHE_READONLY": "true",
            "BUILDKITE_PIPELINE_ID": "woot",
        },
    ):
        WindowsContainer("hi").install_ray()
        image = "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:hi"
        assert install_ray_cmds[-1] == [
            "docker",
            "build",
            "--build-arg",
            f"BASE_IMAGE={image}",
            "--build-arg",
            "BUILDKITE_BAZEL_CACHE_URL=http://hi.com",
            "--build-arg",
            "BUILDKITE_PIPELINE_ID=woot",
            "--build-arg",
            "BUILDKITE_CACHE_READONLY=true",
            "-t",
            image,
            "-f",
            "C:\\workdir\\ci\\ray_ci\\windows\\tests.env.Dockerfile",
            "C:\\workdir",
        ]


def test_get_run_command() -> None:
    container = WindowsContainer("test", volumes=["/hi:/hello"])
    envs = []
    for env in _DOCKER_ENV:
        envs.extend(["--env", env])

    artifact_mount_host, artifact_mount_container = container.get_artifact_mount()
    assert container.get_run_command(["hi", "hello"]) == [
        "docker",
        "run",
        "-i",
        "--rm",
        "--volume",
        f"{artifact_mount_host}:{artifact_mount_container}",
    ] + envs + [
        "--volume",
        "/hi:/hello",
        "--workdir",
        "C:\\rayci",
        "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:test",
        "bash",
        "-c",
        "hi\nhello",
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
