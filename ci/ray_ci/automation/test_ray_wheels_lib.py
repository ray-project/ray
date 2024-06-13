from unittest import mock
import sys
import tempfile
import os
from botocore.exceptions import ClientError
import pytest

from ci.ray_ci.automation.ray_wheels_lib import (
    _get_wheel_names,
    download_wheel_from_s3,
    download_ray_wheels_from_s3,
    _check_downloaded_wheels,
    PYTHON_VERSIONS,
    FULL_PLATFORMS,
    PARTIAL_PLATFORMS,
    RAY_TYPES,
)

SAMPLE_WHEELS = [
    "ray-1.0.0-cp39-cp39-manylinux2014_x86_64",
    "ray-1.0.0-cp39-cp39-manylinux2014_aarch64",
    "ray-1.0.0-cp39-cp39-macosx_10_15_x86_64",
    "ray-1.0.0-cp39-cp39-macosx_11_0_arm64",
    "ray-1.0.0-cp39-cp39-win_amd64",
]


def test_get_wheel_names():
    ray_version = "1.0.0"
    wheel_names = _get_wheel_names(ray_version, full_platform=True)

    assert len(wheel_names) == len(PYTHON_VERSIONS) * len(FULL_PLATFORMS) * len(
        RAY_TYPES
    )

    for wheel_name in wheel_names:
        assert len(wheel_name.split("-")) == 5
        (
            ray_type,
            ray_version,
            python_version,
            python_version2,
            platform,
        ) = wheel_name.split("-")
        platform = platform.split(".")[0]  # Remove the .whl suffix

        assert ray_type in RAY_TYPES
        assert ray_version == ray_version
        assert f"{python_version}-{python_version2}" in PYTHON_VERSIONS
        assert platform in FULL_PLATFORMS


def test_get_wheel_names_partial_platform():
    ray_version = "1.1.0"
    wheel_names = _get_wheel_names(ray_version, full_platform=False)

    assert len(wheel_names) == len(PYTHON_VERSIONS) * len(PARTIAL_PLATFORMS) * len(
        RAY_TYPES
    )

    for wheel_name in wheel_names:
        assert len(wheel_name.split("-")) == 5
        (
            ray_type,
            ray_version,
            python_version,
            python_version2,
            platform,
        ) = wheel_name.split("-")
        platform = platform.split(".")[0]  # Remove the .whl suffix

        assert ray_type in RAY_TYPES
        assert ray_version == ray_version
        assert f"{python_version}-{python_version2}" in PYTHON_VERSIONS
        assert platform in PARTIAL_PLATFORMS


def test_check_downloaded_wheels():
    with tempfile.TemporaryDirectory() as tmp_dir:
        wheels = [
            "ray-1.0.0-cp39-cp39-manylinux2014_x86_64",
            "ray-1.0.0-cp39-cp39-manylinux2014_aarch64",
            "ray-1.0.0-cp39-cp39-macosx_10_15_x86_64",
            "ray-1.0.0-cp39-cp39-macosx_11_0_arm64",
            "ray-1.0.0-cp39-cp39-win_amd64",
        ]

        for wheel in wheels:
            with open(os.path.join(tmp_dir, wheel + ".whl"), "w") as f:
                f.write("")

        _check_downloaded_wheels(tmp_dir, wheels)


def test_check_downloaded_wheels_fail():
    with tempfile.TemporaryDirectory() as tmp_dir:
        wheels = [
            "ray-1.0.0-cp39-cp39-manylinux2014_x86_64",
            "ray-1.0.0-cp39-cp39-manylinux2014_aarch64",
            "ray-1.0.0-cp39-cp39-macosx_10_15_x86_64",
            "ray-1.0.0-cp39-cp39-macosx_11_0_arm64",
            "ray-1.0.0-cp39-cp39-win_amd64",
        ]

        for wheel in wheels[:3]:
            with open(os.path.join(tmp_dir, wheel + ".whl"), "w") as f:
                f.write("")

        with pytest.raises(AssertionError):
            _check_downloaded_wheels(tmp_dir, wheels)


@mock.patch("boto3.client")
def test_download_wheel_from_s3(mock_boto3_client):
    with tempfile.TemporaryDirectory() as tmp_dir:
        keys = [
            "releases/1.0.0/1234567/ray-1.0.0-cp39-cp39-manylinux2014_x86_64.whl",
            "releases/1.0.0/1234567/ray-1.0.0-cp39-cp39-manylinux2014_aarch64.whl",
            "releases/1.0.0/1234567/ray-1.0.0-cp39-cp39-macosx_10_15_x86_64.whl",
            "releases/1.0.0/1234567/ray-1.0.0-cp39-cp39-macosx_11_0_arm64.whl",
            "releases/1.0.0/1234567/ray-1.0.0-cp39-cp39-win_amd64.whl",
        ]
        for key in keys:
            download_wheel_from_s3(key=key, directory_path=tmp_dir)
            mock_boto3_client.return_value.download_file.assert_called_with(
                "ray-wheels", key, f"{tmp_dir}/{key.split('/')[-1]}"
            )


@mock.patch("boto3.client")
def test_download_wheel_from_s3_fail(mock_boto3_client):
    mock_boto3_client.return_value.download_file.side_effect = ClientError(
        {
            "Error": {
                "Code": "404",
                "Message": "Not Found",
            }
        },
        "download_file",
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        keys = [
            "releases/1.0.0/1234567/ray-1.0.0-cp39-cp39-manylinux2014_x86_64.whl",
            "releases/1.0.0/1234567/ray-1.0.0-cp39-cp39-manylinux2014_aarch64.whl",
        ]
        for key in keys:
            with pytest.raises(ClientError, match="Not Found"):
                download_wheel_from_s3(key=key, directory_path=tmp_dir)


@mock.patch("ci.ray_ci.automation.ray_wheels_lib.download_wheel_from_s3")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._check_downloaded_wheels")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._get_wheel_names")
def test_download_ray_wheels_from_s3(
    mock_get_wheel_names, mock_check_wheels, mock_download_wheel
):
    commit_hash = "1234567"
    ray_version = "1.0.0"

    mock_get_wheel_names.return_value = SAMPLE_WHEELS

    with tempfile.TemporaryDirectory() as tmp_dir:
        download_ray_wheels_from_s3(
            commit_hash=commit_hash,
            ray_version=ray_version,
            directory_path=tmp_dir,
        )

        mock_get_wheel_names.assert_called_with(
            ray_version=ray_version, full_platform=True
        )
        assert mock_download_wheel.call_count == len(SAMPLE_WHEELS)
        for i, call_args in enumerate(mock_download_wheel.call_args_list):
            assert (
                call_args[0][0]
                == f"releases/{ray_version}/{commit_hash}/{SAMPLE_WHEELS[i]}.whl"
            )
            assert call_args[0][1] == tmp_dir

        mock_check_wheels.assert_called_with(tmp_dir, SAMPLE_WHEELS)


@mock.patch("ci.ray_ci.automation.ray_wheels_lib.download_wheel_from_s3")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._check_downloaded_wheels")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._get_wheel_names")
def test_download_ray_wheels_from_s3_partial_platform(
    mock_get_wheel_names, mock_check_wheels, mock_download_wheel
):
    commit_hash = "1234567"
    ray_version = "1.1.0"

    mock_get_wheel_names.return_value = SAMPLE_WHEELS

    with tempfile.TemporaryDirectory() as tmp_dir:
        download_ray_wheels_from_s3(
            commit_hash=commit_hash,
            ray_version=ray_version,
            directory_path=tmp_dir,
        )

        mock_get_wheel_names.assert_called_with(
            ray_version=ray_version, full_platform=False
        )
        assert mock_download_wheel.call_count == len(SAMPLE_WHEELS)
        for i, call_args in enumerate(mock_download_wheel.call_args_list):
            assert (
                call_args[0][0]
                == f"releases/{ray_version}/{commit_hash}/{SAMPLE_WHEELS[i]}.whl"
            )
            assert call_args[0][1] == tmp_dir

        mock_check_wheels.assert_called_with(tmp_dir, SAMPLE_WHEELS)


@mock.patch("ci.ray_ci.automation.ray_wheels_lib.download_wheel_from_s3")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._check_downloaded_wheels")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._get_wheel_names")
def test_download_ray_wheels_from_s3_fail_check_wheels(
    mock_get_wheel_names, mock_check_wheels, mock_download_wheel
):
    commit_hash = "1234567"
    ray_version = "1.0.0"

    mock_get_wheel_names.return_value = SAMPLE_WHEELS
    mock_check_wheels.side_effect = AssertionError()

    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(AssertionError):
            download_ray_wheels_from_s3(
                commit_hash=commit_hash, ray_version=ray_version, directory_path=tmp_dir
            )
    assert mock_download_wheel.call_count == len(SAMPLE_WHEELS)


@mock.patch("ci.ray_ci.automation.ray_wheels_lib.download_wheel_from_s3")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._check_downloaded_wheels")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._get_wheel_names")
def test_download_ray_wheels_from_s3_fail_download(
    mock_get_wheel_names, mock_check_wheels, mock_download_wheel
):
    commit_hash = "1234567"
    ray_version = "1.0.0"

    mock_get_wheel_names.return_value = SAMPLE_WHEELS
    mock_download_wheel.side_effect = ClientError(
        {
            "Error": {
                "Code": "404",
                "Message": "Not Found",
            }
        },
        "download_file",
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(ClientError):
            download_ray_wheels_from_s3(
                commit_hash=commit_hash, ray_version=ray_version, directory_path=tmp_dir
            )
    assert mock_check_wheels.call_count == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
