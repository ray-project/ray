import os
from typing import List, Optional

import boto3

from ci.ray_ci.utils import logger

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")

PYTHON_VERSIONS = [
    "cp310-cp310",
    "cp311-cp311",
    "cp312-cp312",
    "cp313-cp313",
]
ALL_PLATFORMS = [
    "manylinux2014_x86_64",
    "manylinux2014_aarch64",
    "macosx_12_0_arm64",
    "win_amd64",
]
RAY_TYPES = ["ray", "ray_cpp"]


def _check_downloaded_wheels(directory_path: str, wheels: List[str]) -> None:
    """
    Check if the wheels are downloaded as expected.

    Args:
        directory_path: The directory where the wheels are downloaded and stored in.
        wheels: The list of wheel names to check.
    """
    assert os.path.exists(directory_path), f"{directory_path} does not exist"

    for wheel in wheels:
        wheel_path = os.path.join(directory_path, wheel + ".whl")
        assert os.path.exists(wheel_path), f"{wheel_path} does not exist"

    # Check if number of *.whl files in the directory is equal to the number of wheels
    num_whl_files_in_dir = len(
        [f for f in os.listdir(directory_path) if f.endswith(".whl")]
    )
    assert num_whl_files_in_dir == len(wheels), (
        f"Expected {len(wheels)} *.whl files in {directory_path},"
        f"but found {num_whl_files_in_dir} instead"
    )


def _get_wheel_names(ray_version: str) -> List[str]:
    """List all wheel names for the given ray version."""
    wheel_names = []
    for python_version in PYTHON_VERSIONS:
        for platform in ALL_PLATFORMS:
            for ray_type in RAY_TYPES:
                if python_version == "cp313-cp313" and platform == "win_amd64":
                    continue
                wheel_name = f"{ray_type}-{ray_version}-{python_version}-{platform}"
                wheel_names.append(wheel_name)
    return wheel_names


def download_wheel_from_s3(key: str, directory_path: str) -> None:
    """
    Download a Ray wheel from S3 to the given directory.

    Args:
        key: The key of the wheel in S3.
        directory_path: The directory to download the wheel to.
    """
    bucket = "ray-wheels"
    s3_client = boto3.client("s3", region_name="us-west-2")
    wheel_name = key.split("/")[-1]  # Split key to get the wheel file name

    logger.info(
        f"Downloading {bucket}/{key} to {directory_path}/{wheel_name} ........."
    )
    s3_client.download_file(bucket, key, os.path.join(directory_path, wheel_name))


def download_ray_wheels_from_s3(
    commit_hash: str,
    ray_version: str,
    directory_path: str,
    branch: Optional[str] = None,
) -> None:
    """
    Download Ray wheels from S3 to the given directory.

    Args:
        commit_hash: The commit hash of the green commit.
        ray_version: The version of Ray.
        directory_path: The directory to download the wheels to.
    """
    full_directory_path = os.path.join(bazel_workspace_dir, directory_path)
    wheels = _get_wheel_names(ray_version=ray_version)
    if not branch:
        branch = f"releases/{ray_version}"
    for wheel in wheels:
        s3_key = f"{branch}/{commit_hash}/{wheel}.whl"
        download_wheel_from_s3(s3_key, full_directory_path)

    _check_downloaded_wheels(full_directory_path, wheels)


def add_build_tag_to_wheel(wheel_path: str, build_tag: str) -> None:
    """
    Add build tag to the wheel.
    """
    wheel_name = os.path.basename(wheel_path)
    directory_path = os.path.dirname(wheel_path)
    (
        ray_type,
        ray_version,
        python_version,
        python_version_duplicate,
        platform,
    ) = wheel_name.split("-")
    new_wheel_name = f"{ray_type}-{ray_version}-{build_tag}-{python_version}-{python_version_duplicate}-{platform}"
    new_wheel_path = os.path.join(directory_path, new_wheel_name)
    os.rename(wheel_path, new_wheel_path)


def add_build_tag_to_wheels(directory_path: str, build_tag: str) -> None:
    """
    Add build tag to all wheels in the given directory.
    """
    for wheel in os.listdir(directory_path):
        wheel_path = os.path.join(directory_path, wheel)
        add_build_tag_to_wheel(wheel_path=wheel_path, build_tag=build_tag)
