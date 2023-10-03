import sys
import pytest
from unittest import mock
from typing import List

from ci.ray_ci.utils import chunk_into_n, docker_login


def test_chunk_into_n() -> None:
    assert chunk_into_n([1, 2, 3, 4, 5], 2) == [[1, 2, 3], [4, 5]]
    assert chunk_into_n([1, 2], 3) == [[1], [2], []]
    assert chunk_into_n([1, 2], 1) == [[1, 2]]


def test_docker_login() -> None:
    def _mock_subprocess_run(
        cmd: List[str],
        stdin=None,
        stdout=None,
        stderr=None,
        check=True,
    ) -> None:
        assert cmd == ["pip", "install", "awscli"] or stdin.read() == b"password"

    with mock.patch("subprocess.check_output", return_value=b"password"):
        with mock.patch("subprocess.run", side_effect=_mock_subprocess_run):
            docker_login("docker_ecr")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
