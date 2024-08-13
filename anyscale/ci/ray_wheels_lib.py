from typing import List

import requests

PYTHON_VERSIONS = ["cp39-cp39", "cp310-cp310", "cp311-cp311", "cp312-cp312"]


def _get_wheel_names(ray_version: str) -> List[str]:
    """List all wheel names for the given ray version that are need on Runtime."""
    wheel_names = []
    for python_version in PYTHON_VERSIONS:
        wheel_name = f"ray-{ray_version}-{python_version}-manylinux2014_x86_64"
        wheel_names.append(wheel_name)
    return wheel_names


def check_wheels_exist_on_s3(
    ray_wheels_s3_url: str, branch: str, commit_hash: str, ray_version: str
) -> bool:
    """
    Check if all wheels exist on S3.
    Args:
        branch: The branch where wheels are built.
        commit_hash: The commit hash of the green commit.
        ray_version: The version of Ray.
    """
    url_prefix = f"{ray_wheels_s3_url}/{branch}/{commit_hash}/"
    wheels = _get_wheel_names(ray_version=ray_version)
    for wheel in wheels:
        url = url_prefix + wheel + ".whl"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"{wheel} does not exist on S3.")
            return False
    return True
