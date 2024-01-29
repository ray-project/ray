import base64
import sys
import pytest
from unittest import mock
from typing import List

from ray_release.test import Test, TestState
from ci.ray_ci.utils import (
    chunk_into_n,
    docker_login,
    query_all_test_names_by_state,
    omit_tests_by_state,
)


def test_chunk_into_n() -> None:
    assert chunk_into_n([1, 2, 3, 4, 5], 2) == [[1, 2, 3], [4, 5]]
    assert chunk_into_n([1, 2], 3) == [[1], [2], []]
    assert chunk_into_n([1, 2], 1) == [[1, 2]]


@mock.patch("boto3.client")
def test_docker_login(mock_client) -> None:
    def _mock_subprocess_run(
        cmd: List[str],
        stdin=None,
        stdout=None,
        stderr=None,
        check=True,
    ) -> None:
        assert stdin.read() == b"password"

    mock_client.return_value.get_authorization_token.return_value = {
        "authorizationData": [
            {"authorizationToken": base64.b64encode(b"AWS:password")},
        ],
    }

    with mock.patch("subprocess.run", side_effect=_mock_subprocess_run):
        docker_login("docker_ecr")


@mock.patch("ray_release.test.Test.get_tests")
def test_query_test_names_by_state(mock_get_tests):
    mock_get_tests.side_effect = (
        [
            Test(
                {
                    "name": "darwin://test_1",
                    "team": "core",
                    "state": TestState.FLAKY,
                }
            ),
            Test(
                {
                    "name": "darwin://test_2",
                    "team": "ci",
                    "state": TestState.FLAKY,
                }
            ),
        ],
    )
    flaky_test_names = query_all_test_names_by_state(
        test_state="flaky", prefix="darwin:", prefix_on=False
    )
    assert flaky_test_names == ["//test_1", "//test_2"]


@mock.patch("ray_release.test.Test.get_tests")
def test_omit_tests_by_state(mock_get_tests):
    mock_get_tests.side_effect = (
        [
            Test(
                {
                    "name": "darwin://test_1",
                    "team": "core",
                    "state": TestState.FLAKY,
                }
            ),
            Test(
                {
                    "name": "darwin://test_2",
                    "team": "ci",
                    "state": TestState.FLAKY,
                }
            ),
        ],
    )

    test_targets = ["//test_1", "//test_2", "//test_3"]
    filtered_test_targets = omit_tests_by_state(test_targets, "flaky")
    assert filtered_test_targets == ["test_3"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
