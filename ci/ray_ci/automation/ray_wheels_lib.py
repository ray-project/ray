import boto3
from typing import List, Tuple
import os
from botocore.exceptions import ClientError

from ci.ray_ci.utils import logger

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")

PYTHON_VERSIONS = ["cp38-cp38", "cp39-cp39", "cp310-cp310", "cp311-cp311"]
PLATFORMS = [
    "manylinux2014_x86_64",
    "manylinux2014_aarch64",
    "macosx_10_15_x86_64",
    "macosx_11_0_arm64",
    "win_amd64",
]
RAY_TYPES = ["ray", "ray_cpp"]


def _check_downloaded_wheels(
    directory_path: str, wheels: List[str]
) -> Tuple[bool, str]:
    """
    Check if the wheels are downloaded as expected.

    Args:
        directory_path: The directory where the wheels are downloaded and stored in.
        wheels: The list of wheel names to check.

    Returns:
        A tuple of (bool, str):
            If the wheels are downloaded properly, the first element
            is True and the second element is the message.
            If the wheels are not downloaded properly, the first element is
            False and the second element is the error message.
    """

    if not os.path.exists(directory_path):
        return False, f"{directory_path} does not exist"

    for wheel in wheels:
        wheel_path = os.path.join(directory_path, wheel + ".whl")
        if not os.path.exists(wheel_path):
            return False, f"{wheel_path} was not downloaded"

    # Check if number of *.whl files in the directory is equal to the number of wheels
    num_whl_files_in_dir = len(
        [f for f in os.listdir(directory_path) if f.endswith(".whl")]
    )
    if num_whl_files_in_dir != len(wheels):
        return False, (
            f"Expected {len(wheels)} *.whl files in {directory_path},"
            f"but found {num_whl_files_in_dir} instead"
        )
    return True, "Wheels are downloaded properly"


def _get_wheel_names(ray_version: str) -> List[str]:
    """List all wheel names for the given ray version."""
    wheel_names = []
    for python_version in PYTHON_VERSIONS:
        for platform in PLATFORMS:
            for ray_type in RAY_TYPES:
                wheel_name = f"{ray_type}-{ray_version}-{python_version}-{platform}"
                wheel_names.append(wheel_name)
    return wheel_names


def download_wheel_from_s3(key: str, directory_path: str) -> Tuple[bool, str]:
    """
    Download a Ray wheel from S3 to the given directory.

    Args:
        key: The key of the wheel in S3.
        directory_path: The directory to download the wheel to.

    Returns:
        A tuple of (bool, str): If the download is successful, the first element is True
        and the second element is the message. If the download fails, the first element
        is False and the second element is the error message.

    """
    bucket = "ray-wheels"
    try:
        s3_client = boto3.client("s3", region_name="us-west-2")
    except ClientError as client_err:
        return False, client_err.response["Error"]["Message"]
    wheel_name = key.split("/")[-1]  # Split key to get the wheel file name

    logger.info(
        f"Downloading {bucket}/{key} to {directory_path}/{wheel_name} ........."
    )
    try:
        s3_client.download_file(bucket, key, os.path.join(directory_path, wheel_name))
    except ClientError as client_err:
        return False, client_err.response["Error"]["Message"]
    return (
        True,
        f"Downloaded {bucket}/{key} to {directory_path}/{wheel_name} successfully",
    )


def download_ray_wheels_from_s3(
    commit_hash: str, ray_version: str, directory_path: str
) -> Tuple[bool, str]:
    """
    Download Ray wheels from S3 to the given directory.

    Args:
        commit_hash: The commit hash of the green commit.
        ray_version: The version of Ray.
        directory_path: The directory to download the wheels to.

    Returns:
        A tuple of (bool, str): If the download is successful, the first element is True
        and the second element is the message. If the download fails, the first element
        is False and the second element is the error message.
    """
    full_directory_path = os.path.join(bazel_workspace_dir, directory_path)

    wheels = _get_wheel_names(ray_version)
    for wheel in wheels:
        s3_key = f"releases/{ray_version}/{commit_hash}/{wheel}.whl"
        result, msg = download_wheel_from_s3(s3_key, full_directory_path)
        if not result:
            return False, msg

    result, msg = _check_downloaded_wheels(full_directory_path, wheels)
    if not result:
        return False, msg

    return True, f"Downloaded wheels to {full_directory_path} successfully"
