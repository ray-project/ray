import os
import subprocess
import sys
import tempfile
from unittest import mock

import pytest

from ci.ray_ci.automation.pypi_lib import (
    _get_pypi_token,
    _get_pypi_url,
    upload_wheels_to_pypi,
)


@pytest.mark.parametrize(
    "pypi_env, expected_url",
    [
        ("test", "https://test.pypi.org/legacy/"),
        ("prod", "https://upload.pypi.org/legacy/"),
    ],
)
def test_get_pypi_url(pypi_env, expected_url):
    assert _get_pypi_url(pypi_env) == expected_url


def test_get_pypi_url_fail():
    with pytest.raises(ValueError):
        _get_pypi_url("non-test")


@pytest.mark.parametrize(
    "pypi_env, expected_token",
    [
        ("test", "test_token"),
        ("prod", "prod_token"),
    ],
)
@mock.patch("boto3.client")
def test_get_pypi_token(mock_boto3_client, pypi_env, expected_token):
    mock_boto3_client.return_value.get_secret_value.return_value = {
        "SecretString": expected_token
    }
    assert _get_pypi_token(pypi_env) == expected_token


@mock.patch("boto3.client")
def test_get_pypi_token_fail(mock_boto3_client):
    mock_boto3_client.return_value.get_secret_value.return_value = {
        "SecretString": "test_token"
    }
    with pytest.raises(ValueError):
        _get_pypi_token("non-test")


@mock.patch("ci.ray_ci.automation.pypi_lib._get_pypi_token")
@mock.patch("ci.ray_ci.automation.pypi_lib._get_pypi_url")
@mock.patch("ci.ray_ci.automation.pypi_lib._call_subprocess")
def test_upload_wheels_to_pypi(mock_subprocess, mock_get_pypi_url, mock_get_pypi_token):
    pypi_env = "test"
    wheels = [
        "ray_cpp-2.9.3-cp310-cp310-macosx_12_0_arm64.whl",
        "ray_cpp-2.9.3-cp311-cp311-macosx_12_0_arm64.whl",
    ]
    mock_get_pypi_token.return_value = "test_token"
    mock_get_pypi_url.return_value = "test_pypi_url"

    with tempfile.TemporaryDirectory() as tmp_dir:
        for wheel in wheels:
            with open(os.path.join(tmp_dir, wheel), "w") as f:
                f.write("")
        wheel_paths = [os.path.join(tmp_dir, wheel) for wheel in wheels]
        upload_wheels_to_pypi(pypi_env, tmp_dir)

        mock_get_pypi_token.assert_called_once_with(pypi_env)
        mock_get_pypi_url.assert_called_once_with(pypi_env)
        assert mock_subprocess.call_count == len(wheels)
        for i, call_args in enumerate(mock_subprocess.call_args_list):
            command = call_args[0][0]
            assert command[:-1] == [
                sys.executable,
                "-m",
                "twine",
                "upload",
                "--repository-url",
                "test_pypi_url",
                "--username",
                "__token__",
            ]
            assert command[-1] in wheel_paths

            add_env = call_args[1]["add_env"]
            assert add_env["TWINE_PASSWORD"] == "test_token"


@mock.patch("ci.ray_ci.automation.pypi_lib._get_pypi_token")
@mock.patch("ci.ray_ci.automation.pypi_lib._get_pypi_url")
@mock.patch("ci.ray_ci.automation.pypi_lib._call_subprocess")
def test_upload_wheels_to_pypi_fail_twine_upload(
    mock_subprocess, mock_get_pypi_url, mock_get_pypi_token
):
    pypi_env = "test"
    wheels = [
        "ray_cpp-2.9.3-cp310-cp310-macosx_12_0_arm64.whl",
        "ray_cpp-2.9.3-cp311-cp311-macosx_12_0_arm64.whl",
    ]
    mock_get_pypi_token.return_value = "test_token"
    mock_get_pypi_url.return_value = "test_pypi_url"
    mock_subprocess.side_effect = subprocess.CalledProcessError(1, "twine")

    with tempfile.TemporaryDirectory() as tmp_dir:
        for wheel in wheels:
            with open(os.path.join(tmp_dir, wheel), "w") as f:
                f.write("")
        with pytest.raises(subprocess.CalledProcessError):
            upload_wheels_to_pypi(pypi_env, tmp_dir)


@mock.patch("ci.ray_ci.automation.pypi_lib._get_pypi_token")
@mock.patch("ci.ray_ci.automation.pypi_lib._get_pypi_url")
def test_upload_wheels_to_pypi_fail_get_pypi(mock_get_pypi_url, mock_get_pypi_token):
    pypi_env = "test"
    wheels = [
        "ray_cpp-2.9.3-cp310-cp310-macosx_12_0_arm64.whl",
        "ray_cpp-2.9.3-cp311-cp311-macosx_12_0_arm64.whl",
    ]
    mock_get_pypi_token.side_effect = ValueError("Invalid pypi_env: test")
    mock_get_pypi_url.side_effect = ValueError("Invalid pypi_env: test")

    with tempfile.TemporaryDirectory() as tmp_dir:
        for wheel in wheels:
            with open(os.path.join(tmp_dir, wheel), "w") as f:
                f.write("")
        with pytest.raises(ValueError, match="Invalid pypi_env: test"):
            upload_wheels_to_pypi(pypi_env, tmp_dir)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
