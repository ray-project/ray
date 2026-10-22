import os
import subprocess
import sys
from typing import Dict, List

from ray_release.aws import get_secret_token

AWS_SECRET_TEST_PYPI = "ray_ci_test_pypi_token"
AWS_SECRET_PYPI = "ray_ci_pypi_token"

bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")


def _check_pypi_env(pypi_env: str) -> None:
    if pypi_env not in ["test", "prod"]:
        raise ValueError(f"Invalid pypi_env: {pypi_env}")


def _get_pypi_url(pypi_env: str) -> str:
    _check_pypi_env(pypi_env)

    if pypi_env == "test":
        return "https://test.pypi.org/legacy/"
    return "https://upload.pypi.org/legacy/"


def _get_pypi_token(pypi_env: str) -> str:
    _check_pypi_env(pypi_env)

    if pypi_env == "test":
        return get_secret_token(AWS_SECRET_TEST_PYPI)
    return get_secret_token(AWS_SECRET_PYPI)


def _call_subprocess(command: List[str], add_env: Dict[str, str]):
    env = os.environ.copy()
    env.update(add_env)
    subprocess.run(command, env=env, check=True)


def upload_wheels_to_pypi(pypi_env: str, directory_path: str) -> None:
    directory_path = os.path.join(bazel_workspace_dir, directory_path)
    pypi_url = _get_pypi_url(pypi_env)
    pypi_token = _get_pypi_token(pypi_env)
    wheels = sorted(os.listdir(directory_path))
    for wheel in wheels:
        wheel_path = os.path.join(directory_path, wheel)
        cmd = [
            sys.executable,
            "-m",
            "twine",
            "upload",
            # A file that is already on the index must not fail the whole
            # upload: twine uploads one file per call and `check=True` aborts
            # the loop, so a single rejected file strands every wheel after it
            # in sort order and a retry then dies on the first file it already
            # uploaded. This was hit on the 2.58.1 release, where 9 of 24
            # wheels never reached TestPyPI and the retry could not recover.
            "--skip-existing",
            "--repository-url",
            pypi_url,
            "--username",
            "__token__",
            wheel_path,
        ]
        _call_subprocess(cmd, add_env={"TWINE_PASSWORD": pypi_token})
