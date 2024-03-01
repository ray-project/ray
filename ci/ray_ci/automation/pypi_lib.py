import subprocess
import os
from typing import List

from ray_release.aws import get_secret_token

AWS_SECRET_TEST_PYPI = "ray_ci_test_pypi_token"
AWS_SECRET_PYPI = "ray_ci_pypi_token"

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")


def _get_pypi_url(pypi_env: str) -> str:
    if pypi_env not in ["test", "prod"]:
        raise ValueError(f"Invalid pypi_env: {pypi_env}")

    if pypi_env == "test":
        return "https://test.pypi.org/legacy/"
    return "https://upload.pypi.org/legacy/"


def _get_pypi_token(pypi_env: str) -> str:
    if pypi_env not in ["test", "prod"]:
        raise ValueError(f"Invalid pypi_env: {pypi_env}")

    if pypi_env == "test":
        return get_secret_token(AWS_SECRET_TEST_PYPI)
    return get_secret_token(AWS_SECRET_PYPI)


def _call_subprocess(command: List[str]):
    subprocess.run(
        command,
        check=True,
    )


def upload_wheels_to_pypi(
    pypi_env: str, directory_path: str, wheels: List[str]
) -> None:
    directory_path = os.path.join(bazel_workspace_dir, directory_path)
    pypi_url = _get_pypi_url(pypi_env)
    pypi_token = _get_pypi_token(pypi_env)

    for wheel in wheels:
        if not os.path.exists(os.path.join(directory_path, wheel)):
            raise FileNotFoundError(f"{wheel} not found in {directory_path}")
        wheel_path = os.path.join(directory_path, wheel)
        cmd = [
            "twine",
            "upload",
            "--repository-url",
            pypi_url,
            "--username",
            "__token__",
            "--password",
            pypi_token,
            wheel_path,
        ]
        _call_subprocess(cmd)
