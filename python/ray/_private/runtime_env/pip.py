import contextlib
import os
import sys
import json
import logging
import hashlib
import shutil

from filelock import FileLock
from typing import Optional, List, Dict, Tuple

from ray._private.runtime_env.conda_utils import exec_cmd_stream_to_logger
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import Protocol, parse_uri
from ray._private.runtime_env.utils import RuntimeEnv
from ray._private.utils import get_directory_size_bytes, try_to_create_directory

default_logger = logging.getLogger(__name__)


def _get_pip_hash(pip_list: List[str]) -> str:
    serialized_pip_spec = json.dumps(pip_list, sort_keys=True)
    hash = hashlib.sha1(serialized_pip_spec.encode("utf-8")).hexdigest()
    return hash


def get_uri(runtime_env: Dict) -> Optional[str]:
    """Return `"pip://<hashed_dependencies>"`, or None if no GC required."""
    pip = runtime_env.get("pip")
    if pip is not None:
        if isinstance(pip, list):
            uri = "pip://" + _get_pip_hash(pip_list=pip)
        else:
            raise TypeError(
                "pip field received by RuntimeEnvAgent must be "
                f"list, not {type(pip).__name__}."
            )
    else:
        uri = None
    return uri


class _PathHelper:
    @staticmethod
    def get_virtualenv_path(target_dir: str) -> str:
        return os.path.join(target_dir, "virtualenv")

    @classmethod
    def get_virtualenv_python(cls, target_dir: str) -> str:
        virtualenv_path = cls.get_virtualenv_path(target_dir)
        return os.path.join(virtualenv_path, "bin/python")

    @staticmethod
    def get_requirements_file(target_dir: str) -> str:
        return os.path.join(target_dir, "requirements.txt")


class PipProcessor:
    def __init__(
        self,
        target_dir: str,
        runtime_env: RuntimeEnv,
        logger: Optional[logging.Logger] = default_logger,
    ):
        try:
            import virtualenv  # noqa: F401 ensure virtualenv exists.
        except ImportError:
            raise RuntimeError(
                f"Please install virtualenv "
                f"`{sys.executable} -m pip install virtualenv`"
                f"to enable pip runtime env."
            )
        logger.debug("Setting up pip for runtime_env: %s", runtime_env)
        self._target_dir = target_dir
        self._runtime_env = runtime_env
        self._logger = logger

    @staticmethod
    def _is_in_virtualenv() -> bool:
        # virtualenv <= 16.7.9 sets the real_prefix,
        # virtualenv > 16.7.9 & venv set the base_prefix.
        # So, we check both of them here.
        # https://github.com/pypa/virtualenv/issues/1622#issuecomment-586186094
        return hasattr(sys, "real_prefix") or (
            hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
        )

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
                python,
                "-c",
                "import ray; print(ray.__version__, ray.__path__[0])",
            ]
            exit_code, output = exec_cmd_stream_to_logger(
                check_ray_cmd, logger, cwd=cwd, env={}
            )
            if exit_code != 0:
                raise RuntimeError("Get ray version and path failed.")
            # print after import ray may have [0m endings, so we strip them by *_
            ray_version, ray_path, *_ = [s.strip() for s in output.split()]
            return ray_version, ray_path

        version, path = _get_ray_version_and_path()
        yield
        actual_version, actual_path = _get_ray_version_and_path()
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

    @classmethod
    def _create_or_get_virtualenv(cls, path: str, cwd: str, logger: logging.Logger):
        """Create or get a virtualenv from path."""

        python = sys.executable
        virtualenv_path = os.path.join(path, "virtualenv")
        virtualenv_app_data_path = os.path.join(path, "virtualenv_app_data")
        current_python_dir = os.path.abspath(
            os.path.join(os.path.dirname(python), "..")
        )

        if cls._is_in_virtualenv():
            # virtualenv-clone homepage:
            # https://github.com/edwardgeorge/virtualenv-clone
            # virtualenv-clone Usage:
            # virtualenv-clone /path/to/existing/venv /path/to/cloned/ven
            create_venv_cmd = [
                python,
                "-m",
                "clonevirtualenv",
                current_python_dir,
                virtualenv_path,
            ]
            logger.info(
                "Colning virtualenv %s to %s", current_python_dir, virtualenv_path
            )
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
                "Creating virtualenv at %s," " current python dir %s",
                virtualenv_path,
                current_python_dir,
            )
        exit_code, output = exec_cmd_stream_to_logger(
            create_venv_cmd, logger, cwd=cwd, env={}
        )
        if exit_code != 0:
            raise RuntimeError(
                f"Failed to create virtualenv {virtualenv_path}:\n{output}"
            )

    @classmethod
    def _install_pip_packages(
        cls,
        path: str,
        pip_packages: List[str],
        cwd: str,
        logger: logging.Logger,
    ):
        virtualenv_path = _PathHelper.get_virtualenv_path(path)
        python = _PathHelper.get_virtualenv_python(path)
        # TODO(fyrestone): Support -i, --no-deps, --no-cache-dir, ...
        pip_requirements_file = _PathHelper.get_requirements_file(path)
        with open(pip_requirements_file, "w") as file:
            for line in pip_packages:
                file.write(line + "\n")
        pip_install_cmd = [
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "-r",
            pip_requirements_file,
        ]
        logger.info("Installing python requirements to %s", virtualenv_path)
        exit_code, output = exec_cmd_stream_to_logger(
            pip_install_cmd, logger, cwd=cwd, env={}
        )
        if exit_code != 0:
            raise RuntimeError(
                f"Failed to install python requirements to {virtualenv_path}:\n{output}"
            )

    def run(self):
        path = self._target_dir
        logger = self._logger
        pip_packages = self._runtime_env.pip_packages()
        # We create an empty directory for exec cmd so that the cmd will
        # run more stable. e.g. if cwd has ray, then checking ray will
        # look up ray in cwd instead of site packages.
        exec_cwd = os.path.join(path, "exec_cwd")
        os.makedirs(exec_cwd, exist_ok=True)
        try:
            self._create_or_get_virtualenv(path, exec_cwd, logger)
            python = _PathHelper.get_virtualenv_python(path)
            with self._check_ray(python, exec_cwd, logger):
                self._install_pip_packages(path, pip_packages, exec_cwd, logger)
            # TODO(fyrestone): pip check.
        except Exception:
            logger.info("Delete incomplete virtualenv: %s", path)
            shutil.rmtree(path, ignore_errors=True)
            logger.exception("Failed to install pip packages.")
            raise


class PipManager:
    def __init__(self, resources_dir: str):
        self._pip_resources_dir = os.path.join(resources_dir, "pip")
        try_to_create_directory(self._pip_resources_dir)
        # Concurrent pip installs are unsafe.  This lock prevents concurrent
        # installs (and deletions).
        self._installs_and_deletions_file_lock = os.path.join(
            self._pip_resources_dir, "ray-pip-installs-and-deletions.lock"
        )

    def _get_path_from_hash(self, hash: str) -> str:
        """Generate a path from the hash of a pip spec.

        Example output:
            /tmp/ray/session_2021-11-03_16-33-59_356303_41018/runtime_resources
                /pip/ray-9a7972c3a75f55e976e620484f58410c920db091
        """
        return os.path.join(self._pip_resources_dir, hash)

    def get_uri(self, runtime_env: RuntimeEnv) -> Optional[str]:
        """Return the pip URI from the RuntimeEnv if it exists, else None."""
        pip_uri = runtime_env.pip_uri()
        if pip_uri != "":
            return pip_uri
        return None

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info("Got request to delete pip URI %s", uri)
        protocol, hash = parse_uri(uri)
        if protocol != Protocol.PIP:
            raise ValueError(
                "PipManager can only delete URIs with protocol "
                f"pip. Received protocol {protocol}, URI {uri}"
            )

        pip_env_path = self._get_path_from_hash(hash)
        local_dir_size = get_directory_size_bytes(pip_env_path)
        try:
            with FileLock(self._installs_and_deletions_file_lock):
                shutil.rmtree(pip_env_path)
        except OSError as e:
            logger.warning(f"Error when deleting pip env {pip_env_path}: {str(e)}")
            return 0

        return local_dir_size

    def create(
        self,
        uri: str,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        if not runtime_env.has_pip():
            return 0

        protocol, hash = parse_uri(uri)
        target_dir = self._get_path_from_hash(hash)

        with FileLock(self._installs_and_deletions_file_lock):
            pip_processor = PipProcessor(target_dir, runtime_env, logger)
            pip_processor.run()

        return get_directory_size_bytes(target_dir)

    def modify_context(
        self,
        uri: str,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.has_pip():
            return
        # Update py_executable.
        protocol, hash = parse_uri(uri)
        target_dir = self._get_path_from_hash(hash)
        virtualenv_python = _PathHelper.get_virtualenv_python(target_dir)

        if not os.path.exists(virtualenv_python):
            raise ValueError(
                f"Local directory {target_dir} for URI {uri} does "
                "not exist on the cluster. Something may have gone wrong while "
                "installing the runtime_env `pip` packages."
            )
        context.py_executable = virtualenv_python
