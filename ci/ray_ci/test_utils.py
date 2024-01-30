import base64
import io
import sys
import pytest
from unittest import mock
from typing import List

from ray_release.test import Test
from ci.ray_ci.utils import (
    chunk_into_n,
    docker_login,
    get_flaky_test_names,
    filter_flaky_tests,
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


def _make_test(name: str, state: str, team: str) -> Test:
    return Test(
        {
            "name": name,
            "state": state,
            "team": team,
        }
    )


@mock.patch("ray_release.test.Test.gen_from_s3")
def test_get_flaky_test_names(mock_gen_from_s3):
    mock_gen_from_s3.side_effect = (
        [
            _make_test("darwin://test_1", "flaky", "core"),
            _make_test("darwin://test_2", "flaky", "ci"),
            _make_test("darwin://test_3", "passing", "core"),
        ],
        [
            _make_test("linux://test_1", "flaky", "core"),
            _make_test("linux://test_2", "passing", "ci"),
        ],
    )
    flaky_test_names = get_flaky_test_names(
        prefix="darwin:",
    )
    assert flaky_test_names == ["//test_1", "//test_2"]
    flaky_test_names = get_flaky_test_names(
        prefix="linux:",
    )
    assert flaky_test_names == ["//test_1"]


@pytest.mark.parametrize(
    "filter_option, expected_value",
    [
        (
            "non-flaky",
            "//test_3\n//test_4\n",
        ),
        (
            "only-flaky",
            "//test_1\n//test_2\n",
        ),
        (
            "wrong-option",  # invalid filter option
            "",
        ),
    ],
)
@mock.patch("ray_release.test.Test.gen_from_s3")
def test_filter_flaky_tests(mock_gen_from_s3, filter_option, expected_value):
    # Setup test input/output
    mock_gen_from_s3.side_effect = (
        [
            _make_test("darwin://test_1", "flaky", "core"),
            _make_test("darwin://test_2", "flaky", "ci"),
            _make_test("darwin://test_3", "passing", "core"),
            _make_test("darwin://test_4", "passing", "ci"),
        ],
    )
    test_targets = ["//test_1", "//test_2", "//test_3", "//test_4"]
    output = io.StringIO()

    # Test invalid filter option
    if filter_option not in ["only-flaky", "non-flaky"]:
        with pytest.raises(
            ValueError, match="must be one of 'only-flaky' or 'non-flaky'"
        ):
            filter_flaky_tests(
                io.StringIO("\n".join(test_targets)), output, "darwin:", filter_option
            )
        return

    # Test valid filter options
    filter_flaky_tests(
        io.StringIO("\n".join(test_targets)), output, "darwin:", filter_option
    )
    assert output.getvalue() == expected_value


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
