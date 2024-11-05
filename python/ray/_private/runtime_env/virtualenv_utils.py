"""Utils to detect runtime environment."""

import sys
from ray._private.runtime_env.utils import check_output_cmd
import logging
import os

_WIN32 = os.name == "nt"


def is_in_virtualenv() -> bool:
    # virtualenv <= 16.7.9 sets the real_prefix,
    # virtualenv > 16.7.9 & venv set the base_prefix.
    # So, we check both of them here.
    # https://github.com/pypa/virtualenv/issues/1622#issuecomment-586186094
    return hasattr(sys, "real_prefix") or (
        hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
    )


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
        #   Set an empty seperated app data folder for current virtualenv.
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
