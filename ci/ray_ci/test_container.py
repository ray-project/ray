import sys
import pytest

from ci.ray_ci.container import Container


def test_get_docker_image() -> None:
    assert (
        Container("test")._get_docker_image()
        == "029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:unknown-test"
    )


def test_get_run_command() -> None:
    command = " ".join(Container("test")._get_run_command(["hi", "hello"]))
    assert "-env BUILDKITE_JOB_ID" in command
    assert "--cap-add SYS_PTRACE" in command
    assert "/bin/bash -iecuo pipefail -- hi\nhello" in command


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
