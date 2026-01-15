import os
import sys
import tempfile
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from ci.ray_ci.automation.ray_wheels_lib import (
    ALL_PLATFORMS,
    PYTHON_VERSIONS,
    RAY_TYPES,
    _check_downloaded_wheels,
    _get_wheel_names,
    add_build_tag_to_wheel,
    add_build_tag_to_wheels,
    download_ray_wheels_from_s3,
    download_wheel_from_s3,
)

_SAMPLE_WHEELS = [
    "ray-1.0.0-cp312-cp312-manylinux2014_x86_64",
    "ray-1.0.0-cp312-cp312-manylinux2014_aarch64",
    "ray-1.0.0-cp312-cp312-macosx_12_0_arm64",
    "ray-1.0.0-cp312-cp312-win_amd64",
]


def test_get_wheel_names():
    ray_version = "2.50.0"
    wheel_names = _get_wheel_names(ray_version)

    assert (
        len(wheel_names)
        == len(PYTHON_VERSIONS) * len(ALL_PLATFORMS) * len(RAY_TYPES) - 2
    )  # Except for the win_amd64 wheel for cp313 on ray and ray-cpp

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
        assert platform in ALL_PLATFORMS


def test_check_downloaded_wheels():
    with tempfile.TemporaryDirectory() as tmp_dir:
        wheels = [
            "ray-1.0.0-cp312-cp312-manylinux2014_x86_64",
            "ray-1.0.0-cp312-cp312-manylinux2014_aarch64",
            "ray-1.0.0-cp312-cp312-macosx_12_0_arm64",
            "ray-1.0.0-cp312-cp312-win_amd64",
        ]

        for wheel in wheels:
            with open(os.path.join(tmp_dir, wheel + ".whl"), "w") as f:
                f.write("")

        _check_downloaded_wheels(tmp_dir, wheels)


def test_check_downloaded_wheels_fail():
    with tempfile.TemporaryDirectory() as tmp_dir:
        wheels = [
            "ray-1.0.0-cp312-cp312-manylinux2014_x86_64",
            "ray-1.0.0-cp312-cp312-manylinux2014_aarch64",
            "ray-1.0.0-cp312-cp312-macosx_12_0_arm64",
            "ray-1.0.0-cp312-cp312-win_amd64",
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
            "releases/1.0.0/1234567/ray-1.0.0-cp312-cp312-manylinux2014_x86_64.whl",
            "releases/1.0.0/1234567/ray-1.0.0-cp312-cp312-manylinux2014_aarch64.whl",
            "releases/1.0.0/1234567/ray-1.0.0-cp312-cp312-macosx_12_0_arm64.whl",
            "releases/1.0.0/1234567/ray-1.0.0-cp312-cp312-win_amd64.whl",
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
            "releases/1.0.0/1234567/ray-1.0.0-cp312-cp312-manylinux2014_x86_64.whl",
            "releases/1.0.0/1234567/ray-1.0.0-cp312-cp312-manylinux2014_aarch64.whl",
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

    mock_get_wheel_names.return_value = _SAMPLE_WHEELS

    with tempfile.TemporaryDirectory() as tmp_dir:
        download_ray_wheels_from_s3(
            commit_hash=commit_hash,
            ray_version=ray_version,
            directory_path=tmp_dir,
        )

        mock_get_wheel_names.assert_called_with(ray_version=ray_version)
        assert mock_download_wheel.call_count == len(_SAMPLE_WHEELS)
        for i, call_args in enumerate(mock_download_wheel.call_args_list):
            assert (
                call_args[0][0]
                == f"releases/{ray_version}/{commit_hash}/{_SAMPLE_WHEELS[i]}.whl"
            )
            assert call_args[0][1] == tmp_dir

        mock_check_wheels.assert_called_with(tmp_dir, _SAMPLE_WHEELS)


@mock.patch("ci.ray_ci.automation.ray_wheels_lib.download_wheel_from_s3")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._check_downloaded_wheels")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._get_wheel_names")
def test_download_ray_wheels_from_s3_with_branch(
    mock_get_wheel_names, mock_check_wheels, mock_download_wheel
):
    commit_hash = "1234567"
    ray_version = "1.0.0"

    mock_get_wheel_names.return_value = _SAMPLE_WHEELS

    with tempfile.TemporaryDirectory() as tmp_dir:
        download_ray_wheels_from_s3(
            commit_hash=commit_hash,
            ray_version=ray_version,
            directory_path=tmp_dir,
            branch="custom_branch",
        )

        mock_get_wheel_names.assert_called_with(ray_version=ray_version)
        assert mock_download_wheel.call_count == len(_SAMPLE_WHEELS)
        for i, call_args in enumerate(mock_download_wheel.call_args_list):
            assert (
                call_args[0][0]
                == f"custom_branch/{commit_hash}/{_SAMPLE_WHEELS[i]}.whl"
            )
            assert call_args[0][1] == tmp_dir

        mock_check_wheels.assert_called_with(tmp_dir, _SAMPLE_WHEELS)


@mock.patch("ci.ray_ci.automation.ray_wheels_lib.download_wheel_from_s3")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._check_downloaded_wheels")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._get_wheel_names")
def test_download_ray_wheels_from_s3_partial_platform(
    mock_get_wheel_names, mock_check_wheels, mock_download_wheel
):
    commit_hash = "1234567"
    ray_version = "1.1.0"

    mock_get_wheel_names.return_value = _SAMPLE_WHEELS

    with tempfile.TemporaryDirectory() as tmp_dir:
        download_ray_wheels_from_s3(
            commit_hash=commit_hash,
            ray_version=ray_version,
            directory_path=tmp_dir,
        )

        mock_get_wheel_names.assert_called_with(ray_version=ray_version)
        assert mock_download_wheel.call_count == len(_SAMPLE_WHEELS)
        for i, call_args in enumerate(mock_download_wheel.call_args_list):
            assert (
                call_args[0][0]
                == f"releases/{ray_version}/{commit_hash}/{_SAMPLE_WHEELS[i]}.whl"
            )
            assert call_args[0][1] == tmp_dir

        mock_check_wheels.assert_called_with(tmp_dir, _SAMPLE_WHEELS)


@mock.patch("ci.ray_ci.automation.ray_wheels_lib.download_wheel_from_s3")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._check_downloaded_wheels")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._get_wheel_names")
def test_download_ray_wheels_from_s3_fail_check_wheels(
    mock_get_wheel_names, mock_check_wheels, mock_download_wheel
):
    commit_hash = "1234567"
    ray_version = "1.0.0"

    mock_get_wheel_names.return_value = _SAMPLE_WHEELS
    mock_check_wheels.side_effect = AssertionError()

    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(AssertionError):
            download_ray_wheels_from_s3(
                commit_hash=commit_hash, ray_version=ray_version, directory_path=tmp_dir
            )
    assert mock_download_wheel.call_count == len(_SAMPLE_WHEELS)


@mock.patch("ci.ray_ci.automation.ray_wheels_lib.download_wheel_from_s3")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._check_downloaded_wheels")
@mock.patch("ci.ray_ci.automation.ray_wheels_lib._get_wheel_names")
def test_download_ray_wheels_from_s3_fail_download(
    mock_get_wheel_names, mock_check_wheels, mock_download_wheel
):
    commit_hash = "1234567"
    ray_version = "1.0.0"

    mock_get_wheel_names.return_value = _SAMPLE_WHEELS
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


def test_add_build_tag_to_wheel():
    with tempfile.TemporaryDirectory() as tmp_dir:
        wheel_name = "ray-1.0.0-cp312-cp312-manylinux2014_x86_64.whl"
        wheel_path = os.path.join(tmp_dir, wheel_name)
        with open(wheel_path, "w") as f:
            f.write("")
        add_build_tag_to_wheel(wheel_path=wheel_path, build_tag="123")
        expected_wheel_name = "ray-1.0.0-123-cp312-cp312-manylinux2014_x86_64.whl"
        expected_wheel_path = os.path.join(tmp_dir, expected_wheel_name)
        assert os.path.exists(expected_wheel_path)


def test_add_build_tag_to_wheels():
    with tempfile.TemporaryDirectory() as tmp_dir:
        wheels = [
            "ray-1.0.0-cp312-cp312-manylinux2014_x86_64.whl",
            "ray-1.0.0-cp312-cp312-manylinux2014_aarch64.whl",
        ]
        for wheel in wheels:
            with open(os.path.join(tmp_dir, wheel), "w") as f:
                f.write("")
        add_build_tag_to_wheels(directory_path=tmp_dir, build_tag="123")
        assert os.path.exists(
            os.path.join(tmp_dir, "ray-1.0.0-123-cp312-cp312-manylinux2014_x86_64.whl")
        )
        assert os.path.exists(
            os.path.join(tmp_dir, "ray-1.0.0-123-cp312-cp312-manylinux2014_aarch64.whl")
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
