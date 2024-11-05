"""Util functions to manage dependency requirements."""

from typing import List, Tuple
import os
import tempfile
import logging
from contextlib import asynccontextmanager
from ray._private.runtime_env import virtualenv_utils
from ray._private.runtime_env.utils import check_output_cmd


def dump_requirements_txt(requirements_file: str, pip_packages: List[str]):
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
