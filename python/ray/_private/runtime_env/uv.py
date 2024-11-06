"""Util class to install packages via uv.
"""

# TODO(hjiang): Implement `UvPlugin`, which is the counterpart for `PipPlugin`.

from typing import Dict, List, Optional
from asyncio import get_running_loop
import os
from ray._private.runtime_env import virtualenv_utils
from ray._private.runtime_env import dependency_utils
from ray._private.runtime_env.utils import check_output_cmd
import shutil
import logging
import sys

default_logger = logging.getLogger(__name__)


class UvProcessor:
    def __init__(
        self,
        target_dir: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        logger: Optional[logging.Logger] = default_logger,
    ):
        try:
            import virtualenv  # noqa: F401 ensure virtualenv exists.
        except ImportError:
            raise RuntimeError(
                f"Please install virtualenv "
                f"`{sys.executable} -m pip install virtualenv`"
                f"to enable uv runtime env."
            )

        logger.debug("Setting up uv for runtime_env: %s", runtime_env)
        self._target_dir = target_dir
        self._runtime_env = runtime_env
        self._logger = logger

        self._uv_config = self._runtime_env.uv_config()
        self._uv_env = os.environ.copy()
        self._uv_env.update(self._runtime_env.env_vars())

    # TODO(hjiang): Check `uv` existence before installation, so we don't blindly
    # install.
    async def _install_uv(
        self, path: str, cwd: str, pip_env: dict, logger: logging.Logger
    ):
        """Before package install, make sure `uv` is installed."""
        virtualenv_path = virtualenv_utils.get_virtualenv_path(path)
        python = virtualenv_utils.get_virtualenv_python(path)

        uv_install_cmd = [
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-cache-dir",
            "uv",
        ]
        logger.info("Installing package uv to %s", virtualenv_path)
        await check_output_cmd(uv_install_cmd, logger=logger, cwd=cwd, env=pip_env)

    async def _install_uv_packages(
        self,
        path: str,
        uv_packages: List[str],
        cwd: str,
        pip_env: Dict,
        logger: logging.Logger,
    ):
        virtualenv_path = virtualenv_utils.get_virtualenv_path(path)
        python = virtualenv_utils.get_virtualenv_python(path)
        # TODO(fyrestone): Support -i, --no-deps, --no-cache-dir, ...
        requirements_file = dependency_utils.get_requirements_file(path, uv_packages)

        # Install uv, which acts as the default package manager.
        await self._install_uv(path, cwd, pip_env, logger)

        # Avoid blocking the event loop.
        loop = get_running_loop()
        await loop.run_in_executor(
            None, dependency_utils.gen_requirements_txt, requirements_file, uv_packages
        )

        # Install all dependencies.
        #
        # Difference with pip:
        # 1. `--disable-pip-version-check` has no effect for uv.
        # 2. `--no-cache-dir` for `pip` maps to `--no-cache` for uv.
        pip_install_cmd = [
            python,
            "-m",
            "uv",
            "pip",
            "install",
            "--no-cache",
            "-r",
            requirements_file,
        ]
        logger.info("Installing python requirements to %s", virtualenv_path)
        await check_output_cmd(pip_install_cmd, logger=logger, cwd=cwd, env=pip_env)

    async def _run(self):
        path = self._target_dir
        logger = self._logger
        uv_packages = self._uv_config["packages"]
        # We create an empty directory for exec cmd so that the cmd will
        # run more stable. e.g. if cwd has ray, then checking ray will
        # look up ray in cwd instead of site packages.
        exec_cwd = os.path.join(path, "exec_cwd")
        os.makedirs(exec_cwd, exist_ok=True)
        try:
            await virtualenv_utils.create_or_get_virtualenv(path, exec_cwd, logger)
            python = virtualenv_utils.get_virtualenv_python(path)
            async with dependency_utils.check_ray(python, exec_cwd, logger):
                # Install packages with uv.
                await self._install_uv_packages(
                    path,
                    uv_packages,
                    exec_cwd,
                    self._uv_env,
                    logger,
                )
        except Exception:
            logger.info("Delete incomplete virtualenv: %s", path)
            shutil.rmtree(path, ignore_errors=True)
            logger.exception("Failed to install uv packages.")
            raise

    def __await__(self):
        return self._run().__await__()
