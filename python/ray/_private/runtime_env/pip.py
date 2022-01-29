import contextlib
import os
import sys
import json
import logging
import hashlib
import shutil

from typing import Optional, List, Dict, Tuple

from ray._private.runtime_env.conda_utils import exec_cmd_stream_to_logger
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import Protocol, parse_uri
from ray._private.runtime_env.utils import RuntimeEnv
from ray._private.utils import try_to_create_directory

default_logger = logging.getLogger(__name__)


def _get_pip_hash(pip_list: List[str]) -> str:
    serialized_pip_spec = json.dumps(pip_list, sort_keys=True)
    hash = hashlib.sha1(serialized_pip_spec.encode("utf-8")).hexdigest()
    return hash


def _get_path_from_hash(pip_resources_dir: str, hash: str) -> str:
    """Generate a path from the hash of a pip spec.

    Example output:
        /tmp/ray/session_2021-11-03_16-33-59_356303_41018/runtime_resources
            /pip/ray-9a7972c3a75f55e976e620484f58410c920db091
    """
    return os.path.join(pip_resources_dir, hash)


def get_uri(runtime_env: Dict) -> Optional[str]:
    """Return `"pip://<hashed_dependencies>"`, or None if no GC required."""
    pip = runtime_env.get("pip")
    if pip is not None:
        if isinstance(pip, list):
            uri = "pip://" + _get_pip_hash(pip_list=pip)
        else:
            raise TypeError("pip field received by RuntimeEnvAgent must be "
                            f"list, not {type(pip).__name__}.")
    else:
        uri = None
    return uri


class PipProcessor:
    def __init__(self,
                 pip_resources_dir: str,
                 runtime_env: RuntimeEnv,
                 context: RuntimeEnvContext,
                 logger: Optional[logging.Logger] = default_logger):
        try:
            import virtualenv  # noqa: F401 ensure virtualenv exits.
        except ImportError:
            raise RuntimeError(f"Please install virtualenv "
                               f"`{sys.executable} -m pip install virtualenv`"
                               f"to enable pip runtime env.")
        logger.debug("Setting up pip for runtime_env: %s", runtime_env)
        self._pip_resources_dir = pip_resources_dir
        self._runtime_env = runtime_env
        self._context = context
        self._logger = logger

    @staticmethod
    def _is_in_virtualenv() -> bool:
        return (hasattr(sys, "real_prefix")
                or (hasattr(sys, "base_prefix")
                    and sys.base_prefix != sys.prefix))

    @staticmethod
    def _get_current_python() -> str:
        return sys.executable

    @staticmethod
    def _get_virtualenv_python(virtualenv_path: str) -> str:
        return os.path.join(virtualenv_path, "bin/python")

    @staticmethod
    @contextlib.contextmanager
    def _check_ray(python: str, cwd: str, logger: logging.Logger):
        """A context manager to check ray is not overwritten.

        Currently, we only check ray version and path. It works for virtualenv,
          - ray is in Python's site-packages.
          - ray is overwritten during yield.
          - ray is in virtualenv's site-packages.
        """

        def _get_ray_version_and_path() -> Tuple[str, str]:
            check_ray_cmd = [
                python, "-c",
                "import ray; print(ray.__version__, ray.__path__[0])"
            ]
            exit_code, output = exec_cmd_stream_to_logger(
                check_ray_cmd, logger, cwd=cwd, env={})
            if exit_code != 0:
                raise RuntimeError("Get ray version and path failed.")
            # print after import ray may have [0m endings, so we strip them by *_
            ray_version, ray_path, *_ = [s.strip() for s in output.split()]
            return ray_version, ray_path

        version, path = _get_ray_version_and_path()
        yield
        actual_version, actual_path = _get_ray_version_and_path()
        if actual_version != version or actual_path != path:
            raise RuntimeError("Change ray version is not allowed: \n"
                               f"  current version: {actual_version}, "
                               f"current path: {actual_path}\n"
                               f"  expect version: {version}, "
                               f"expect path: {path}")

    @classmethod
    def _create_or_get_virtualenv(cls, path: str, cwd: str,
                                  logger: logging.Logger) -> str:
        """Create or get a virtualenv from path.

        Returns:
            A valid virtualenv path.
        """
        if cls._is_in_virtualenv():
            # TODO(fyrestone): Handle create virtualenv from virtualenv.
            #
            # Currently, create a virtualenv from virtualenv will get an
            # unexpected result. The new created virtualenv only inherits
            # the site packages from the real Python, not current
            # virtualenv.
            #
            # It's possible to copy the virtualenv to create a new
            # virtualenv, but copying the entire Python is very slow.
            #
            # So, we decide to raise an exception until creating virtualenv
            # from virtualenv is fully supported.
            raise RuntimeError("Can't create virtualenv from virtualenv.")
        python = cls._get_current_python()
        virtualenv_path = os.path.join(path, "virtualenv")
        virtualenv_app_data_path = os.path.join(path, "virtualenv_app_data")
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
            python, "-m", "virtualenv", "--app-data", virtualenv_app_data_path,
            "--reset-app-data", "--no-periodic-update",
            "--system-site-packages", "--no-download", virtualenv_path
        ]
        logger.info("Creating virtualenv at %s", virtualenv_path)
        exit_code, output = exec_cmd_stream_to_logger(
            create_venv_cmd, logger, cwd=cwd, env={})
        if exit_code != 0:
            raise RuntimeError(
                f"Failed to create virtualenv {virtualenv_path}:\n{output}")
        return virtualenv_path

    @classmethod
    def _install_pip_packages(cls, virtualenv_path: str,
                              pip_packages: List[str], cwd: str,
                              logger: logging.Logger):
        python = cls._get_virtualenv_python(virtualenv_path)
        # TODO(fyrestone): Support -i, --no-deps, --no-cache-dir, ...
        pip_install_cmd = [
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
        ] + pip_packages
        logger.info("Installing python requirements to %s", virtualenv_path)
        exit_code, output = exec_cmd_stream_to_logger(
            pip_install_cmd, logger, cwd=cwd, env={})
        if exit_code != 0:
            raise RuntimeError(
                f"Failed to install python requirements to {virtualenv_path}:\n{output}"
            )

    def run(self):
        logger = self._logger
        pip_packages = self._runtime_env.pip_packages()
        path = _get_path_from_hash(self._pip_resources_dir,
                                   _get_pip_hash(pip_packages))
        # We create an empty directory for exec cmd so that the cmd will
        # run more stable. e.g. if cwd has ray, then checking ray will
        # look up ray in cwd instead of site packages.
        exec_cwd = os.path.join(path, "exec_cwd")
        os.makedirs(exec_cwd, exist_ok=True)
        try:
            virtualenv_path = self._create_or_get_virtualenv(
                path, exec_cwd, logger)
            python = self._get_virtualenv_python(virtualenv_path)
            with self._check_ray(python, exec_cwd, logger):
                self._install_pip_packages(virtualenv_path, pip_packages,
                                           exec_cwd, logger)
            # TODO(fyrestone): pip check.
            self._context.py_executable = python
        except Exception:
            logger.info("Delete incomplete virtualenv: %s", path)
            shutil.rmtree(path, ignore_errors=True)
            logger.exception("Failed to install pip packages.")
            raise


class PipManager:
    def __init__(self, resources_dir: str):
        self._pip_resources_dir = os.path.join(resources_dir, "pip")
        try_to_create_directory(self._pip_resources_dir)

    def delete_uri(self,
                   uri: str,
                   logger: Optional[logging.Logger] = default_logger) -> bool:
        logger.info("Got request to delete URI %s", uri)
        protocol, hash = parse_uri(uri)
        if protocol != Protocol.PIP:
            raise ValueError("PipManager can only delete URIs with protocol "
                             f"pip. Received protocol {protocol}, URI {uri}")

        pip_env_path = _get_path_from_hash(self._pip_resources_dir, hash)
        try:
            shutil.rmtree(pip_env_path)
            successful = True
        except OSError:
            successful = False
            logger.warning("Error when deleting pip env %s.", pip_env_path)
        return successful

    def setup(self,
              runtime_env: RuntimeEnv,
              context: RuntimeEnvContext,
              logger: Optional[logging.Logger] = default_logger):
        if not runtime_env.has_pip():
            return

        pip_processor = PipProcessor(self._pip_resources_dir, runtime_env,
                                     context, logger)
        pip_processor.run()
