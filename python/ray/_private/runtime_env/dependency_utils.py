"""Util functions to manage dependency requirements."""

import logging
import os
import tempfile
from contextlib import asynccontextmanager
from typing import List, Optional, Tuple

from ray._private.runtime_env import virtualenv_utils
from ray._private.runtime_env.utils import check_output_cmd

INTERNAL_PIP_FILENAME = "ray_runtime_env_internal_pip_requirements.txt"
MAX_INTERNAL_PIP_FILENAME_TRIES = 100


def gen_requirements_txt(requirements_file: str, pip_packages: List[str]):
    """Dump [pip_packages] to the given [requirements_file] for later env setup."""
    with open(requirements_file, "w") as file:
        for line in pip_packages:
            file.write(line + "\n")


@asynccontextmanager
async def check_ray(python: str, cwd: str, logger: logging.Logger):
    """A context manager to check ray is not overwritten.

    Currently, we only check ray version and path. It works for virtualenv,
        - ray is in Python's site-packages.
        - ray is overwritten during yield.
        - ray is in virtualenv's site-packages.
    """

    async def _get_ray_version_and_path() -> Tuple[str, str]:
        with tempfile.TemporaryDirectory(
            prefix="check_ray_version_tempfile"
        ) as tmp_dir:
            ray_version_path = os.path.join(tmp_dir, "ray_version.txt")
            check_ray_cmd = [
                python,
                "-c",
                """
import ray
with open(r"{ray_version_path}", "wt") as f:
    f.write(ray.__version__)
    f.write(" ")
    f.write(ray.__path__[0])
                """.format(
                    ray_version_path=ray_version_path
                ),
            ]
            if virtualenv_utils._WIN32:
                env = os.environ.copy()
            else:
                env = {}
            output = await check_output_cmd(
                check_ray_cmd, logger=logger, cwd=cwd, env=env
            )
            logger.info(f"try to write ray version information in: {ray_version_path}")
            with open(ray_version_path, "rt") as f:
                output = f.read()
            # print after import ray may have [0m endings, so we strip them by *_
            ray_version, ray_path, *_ = [s.strip() for s in output.split()]
        return ray_version, ray_path

    version, path = await _get_ray_version_and_path()
    yield
    actual_version, actual_path = await _get_ray_version_and_path()
    if actual_version != version or actual_path != path:
        raise RuntimeError(
            "Changing the ray version is not allowed: \n"
            f"  current version: {actual_version}, "
            f"current path: {actual_path}\n"
            f"  expect version: {version}, "
            f"expect path: {path}\n"
            "Please ensure the dependencies in the runtime_env pip field "
            "do not install a different version of Ray."
        )


def get_requirements_file(target_dir: str, pip_list: Optional[List[str]]) -> str:
    """Returns the path to the requirements file to use for this runtime env.

    If pip_list is not None, we will check if the internal pip filename is in any of
    the entries of pip_list. If so, we will append numbers to the end of the
    filename until we find one that doesn't conflict. This prevents infinite
    recursion if the user specifies the internal pip filename in their pip list.

    Args:
        target_dir: The directory to store the requirements file in.
        pip_list: A list of pip requirements specified by the user.

    Returns:
        The path to the requirements file to use for this runtime env.
    """

    def filename_in_pip_list(filename: str) -> bool:
        for pip_entry in pip_list:
            if filename in pip_entry:
                return True
        return False

    filename = INTERNAL_PIP_FILENAME
    if pip_list is not None:
        i = 1
        while filename_in_pip_list(filename) and i < MAX_INTERNAL_PIP_FILENAME_TRIES:
            filename = f"{INTERNAL_PIP_FILENAME}.{i}"
            i += 1
        if i == MAX_INTERNAL_PIP_FILENAME_TRIES:
            raise RuntimeError(
                "Could not find a valid filename for the internal "
                "pip requirements file. Please specify a different "
                "pip list in your runtime env."
            )
    return os.path.join(target_dir, filename)
