"""Utils to detect runtime environment."""

import logging
import os
import sys
from typing import List

from ray._private.runtime_env.utils import check_output_cmd

_WIN32 = os.name == "nt"


def is_in_virtualenv() -> bool:
    # virtualenv <= 16.7.9 sets the real_prefix,
    # virtualenv > 16.7.9 & venv set the base_prefix.
    # So, we check both of them here.
    # https://github.com/pypa/virtualenv/issues/1622#issuecomment-586186094
    return hasattr(sys, "real_prefix") or (
        hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
    )


def get_virtualenv_path(target_dir: str) -> str:
    """Get virtual environment path."""
    return os.path.join(target_dir, "virtualenv")


def get_virtualenv_python(target_dir: str) -> str:
    virtualenv_path = get_virtualenv_path(target_dir)
    if _WIN32:
        return os.path.join(virtualenv_path, "Scripts", "python.exe")
    else:
        return os.path.join(virtualenv_path, "bin", "python")


def get_virtualenv_activate_command(target_dir: str) -> List[str]:
    """Get the command to activate virtual environment."""
    virtualenv_path = get_virtualenv_path(target_dir)
    if _WIN32:
        cmd = [os.path.join(virtualenv_path, "Scripts", "activate.bat")]
    else:
        cmd = ["source", os.path.join(virtualenv_path, "bin/activate")]
    return cmd + ["1>&2", "&&"]


async def create_or_get_virtualenv(path: str, cwd: str, logger: logging.Logger):
    """Create or get a virtualenv from path."""
    python = sys.executable
    virtualenv_path = os.path.join(path, "virtualenv")
    virtualenv_app_data_path = os.path.join(path, "virtualenv_app_data")

    if _WIN32:
        current_python_dir = sys.prefix
        env = os.environ.copy()
    else:
        current_python_dir = os.path.abspath(
            os.path.join(os.path.dirname(python), "..")
        )
        env = {}

    if is_in_virtualenv():
        # virtualenv-clone homepage:
        # https://github.com/edwardgeorge/virtualenv-clone
        # virtualenv-clone Usage:
        # virtualenv-clone /path/to/existing/venv /path/to/cloned/ven
        # or
        # python -m clonevirtualenv /path/to/existing/venv /path/to/cloned/ven
        clonevirtualenv = os.path.join(os.path.dirname(__file__), "_clonevirtualenv.py")
        create_venv_cmd = [
            python,
            clonevirtualenv,
            current_python_dir,
            virtualenv_path,
        ]
        logger.info("Cloning virtualenv %s to %s", current_python_dir, virtualenv_path)
    else:
        # virtualenv options:
        # https://virtualenv.pypa.io/en/latest/cli_interface.html
        #
        # --app-data
        # --reset-app-data
        #   Set an empty separated app data folder for current virtualenv.
        #
        # --no-periodic-update
        #   Disable the periodic (once every 14 days) update of the embedded
        #   wheels.
        #
        # --system-site-packages
        #   Inherit site packages.
        #
        # --no-download
        #   Never download the latest pip/setuptools/wheel from PyPI.
        create_venv_cmd = [
            python,
            "-m",
            "virtualenv",
            "--app-data",
            virtualenv_app_data_path,
            "--reset-app-data",
            "--no-periodic-update",
            "--system-site-packages",
            "--no-download",
            virtualenv_path,
        ]
        logger.info(
            "Creating virtualenv at %s, current python dir %s",
            virtualenv_path,
            virtualenv_path,
        )
    await check_output_cmd(create_venv_cmd, logger=logger, cwd=cwd, env=env)
