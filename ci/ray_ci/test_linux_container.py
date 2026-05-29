import sys

import pytest

from ci.ray_ci.linux_container import LinuxContainer


def test_get_docker_image() -> None:
    assert (
        LinuxContainer("test")._get_docker_image()
        == "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:test"
    )


def test_get_run_command() -> None:
    command = " ".join(LinuxContainer("test").get_run_command(["hi", "hello"]))
    assert "-env BUILDKITE_JOB_ID" in command
    assert "--cap-add SYS_PTRACE" in command
    assert "/bin/bash -iecuo pipefail -- hi\nhello" in command


def test_get_run_command_tmpfs() -> None:
    container = LinuxContainer("test", tmp_filesystem="tmpfs")
    command = " ".join(container.get_run_command(["hi", "hello"]))
    assert "--mount type=tmpfs,destination=/tmp" in command


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
