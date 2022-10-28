import asyncio
import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
from typing import Dict, List, Optional, Tuple

from ray._private.async_compat import asynccontextmanager, create_task, get_running_loop
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import Protocol, parse_uri
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.utils import check_output_cmd
from ray._private.utils import get_directory_size_bytes, try_to_create_directory

default_logger = logging.getLogger(__name__)

_WIN32 = os.name == "nt"


def _get_pip_hash(pip_dict: Dict) -> str:
    serialized_pip_spec = json.dumps(pip_dict, sort_keys=True)
    hash = hashlib.sha1(serialized_pip_spec.encode("utf-8")).hexdigest()
    return hash


def get_uri(runtime_env: Dict) -> Optional[str]:
    """Return `"pip://<hashed_dependencies>"`, or None if no GC required."""
    pip = runtime_env.get("pip")
    if pip is not None:
        if isinstance(pip, dict):
            uri = "pip://" + _get_pip_hash(pip_dict=pip)
        elif isinstance(pip, list):
            uri = "pip://" + _get_pip_hash(pip_dict=dict(packages=pip))
        else:
            raise TypeError(
                "pip field received by RuntimeEnvAgent must be "
                f"list or dict, not {type(pip).__name__}."
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
        if _WIN32:
            return os.path.join(virtualenv_path, "Scripts", "python.exe")
        else:
            return os.path.join(virtualenv_path, "bin", "python")

    @classmethod
    def get_virtualenv_activate_command(cls, target_dir: str) -> List[str]:
        virtualenv_path = cls.get_virtualenv_path(target_dir)
        if _WIN32:
            cmd = [os.path.join(virtualenv_path, "Scripts", "activate.bat")]

        else:
            cmd = ["source", os.path.join(virtualenv_path, "bin/activate")]
        return cmd + ["1>&2", "&&"]

    @staticmethod
    def get_requirements_file(target_dir: str) -> str:
        return os.path.join(target_dir, "requirements.txt")


class PipProcessor:
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
                f"to enable pip runtime env."
            )
        logger.debug("Setting up pip for runtime_env: %s", runtime_env)
        self._target_dir = target_dir
        self._runtime_env = runtime_env
        self._logger = logger

        self._pip_config = self._runtime_env.pip_config()
        self._pip_env = os.environ.copy()
        self._pip_env.update(self._runtime_env.env_vars())

    @staticmethod
    def _is_in_virtualenv() -> bool:
        # virtualenv <= 16.7.9 sets the real_prefix,
        # virtualenv > 16.7.9 & venv set the base_prefix.
        # So, we check both of them here.
        # https://github.com/pypa/virtualenv/issues/1622#issuecomment-586186094
        return hasattr(sys, "real_prefix") or (
            hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
        )

    @classmethod
    async def _ensure_pip_version(
        cls,
        path: str,
        pip_version: Optional[str],
        cwd: str,
        pip_env: Dict,
        logger: logging.Logger,
    ):
        """Run the pip command to reinstall pip to the specified version."""
        if not pip_version:
            return

        python = _PathHelper.get_virtualenv_python(path)
        # Ensure pip version.
        pip_reinstall_cmd = [
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            f"pip{pip_version}",
        ]
        logger.info("Installing pip with version %s", pip_version)

        await check_output_cmd(pip_reinstall_cmd, logger=logger, cwd=cwd, env=pip_env)

    async def _pip_check(
        self,
        path: str,
        pip_check: bool,
        cwd: str,
        pip_env: Dict,
        logger: logging.Logger,
    ):
        """Run the pip check command to check python dependency conflicts.
        If exists conflicts, the exit code of pip check command will be non-zero.
        """
        if not pip_check:
            logger.info("Skip pip check.")
            return
        python = _PathHelper.get_virtualenv_python(path)

        await check_output_cmd(
            [python, "-m", "pip", "check", "--disable-pip-version-check"],
            logger=logger,
            cwd=cwd,
            env=pip_env,
        )

        logger.info("Pip check on %s successfully.", path)

    @staticmethod
    @asynccontextmanager
    async def _check_ray(python: str, cwd: str, logger: logging.Logger):
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
                if _WIN32:
                    env = os.environ.copy()
                else:
                    env = {}
                output = await check_output_cmd(
                    check_ray_cmd, logger=logger, cwd=cwd, env=env
                )
                logger.info(
                    f"try to write ray version information in: {ray_version_path}"
                )
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

    @classmethod
    async def _create_or_get_virtualenv(
        cls, path: str, cwd: str, logger: logging.Logger
    ):
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

        if cls._is_in_virtualenv():
            # virtualenv-clone homepage:
            # https://github.com/edwardgeorge/virtualenv-clone
            # virtualenv-clone Usage:
            # virtualenv-clone /path/to/existing/venv /path/to/cloned/ven
            # or
            # python -m clonevirtualenv /path/to/existing/venv /path/to/cloned/ven
            clonevirtualenv = os.path.join(
                os.path.dirname(__file__), "_clonevirtualenv.py"
            )
            create_venv_cmd = [
                python,
                clonevirtualenv,
                current_python_dir,
                virtualenv_path,
            ]
            logger.info(
                "Cloning virtualenv %s to %s", current_python_dir, virtualenv_path
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
                "Creating virtualenv at %s, current python dir %s",
                virtualenv_path,
                virtualenv_path,
            )
        await check_output_cmd(create_venv_cmd, logger=logger, cwd=cwd, env=env)

    @classmethod
    async def _install_pip_packages(
        cls,
        path: str,
        pip_packages: List[str],
        cwd: str,
        pip_env: Dict,
        logger: logging.Logger,
    ):
        virtualenv_path = _PathHelper.get_virtualenv_path(path)
        python = _PathHelper.get_virtualenv_python(path)
        # TODO(fyrestone): Support -i, --no-deps, --no-cache-dir, ...
        pip_requirements_file = _PathHelper.get_requirements_file(path)

        def _gen_requirements_txt():
            with open(pip_requirements_file, "w") as file:
                for line in pip_packages:
                    file.write(line + "\n")

        # Avoid blocking the event loop.
        loop = get_running_loop()
        await loop.run_in_executor(None, _gen_requirements_txt)

        # pip options
        #
        # --disable-pip-version-check
        #   Don't periodically check PyPI to determine whether a new version
        #   of pip is available for download.
        #
        # --no-cache-dir
        #   Disable the cache, the pip runtime env is a one-time installation,
        #   and we don't need to handle the pip cache broken.
        pip_install_cmd = [
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-cache-dir",
            "-r",
            pip_requirements_file,
        ]
        logger.info("Installing python requirements to %s", virtualenv_path)

        await check_output_cmd(pip_install_cmd, logger=logger, cwd=cwd, env=pip_env)

    async def _run(self):
        path = self._target_dir
        logger = self._logger
        pip_packages = self._pip_config["packages"]
        # We create an empty directory for exec cmd so that the cmd will
        # run more stable. e.g. if cwd has ray, then checking ray will
        # look up ray in cwd instead of site packages.
        exec_cwd = os.path.join(path, "exec_cwd")
        os.makedirs(exec_cwd, exist_ok=True)
        try:
            await self._create_or_get_virtualenv(path, exec_cwd, logger)
            python = _PathHelper.get_virtualenv_python(path)
            async with self._check_ray(python, exec_cwd, logger):
                # Ensure pip version.
                await self._ensure_pip_version(
                    path,
                    self._pip_config.get("pip_version", None),
                    exec_cwd,
                    self._pip_env,
                    logger,
                )
                # Install pip packages.
                await self._install_pip_packages(
                    path,
                    pip_packages,
                    exec_cwd,
                    self._pip_env,
                    logger,
                )
                # Check python environment for conflicts.
                await self._pip_check(
                    path,
                    self._pip_config.get("pip_check", False),
                    exec_cwd,
                    self._pip_env,
                    logger,
                )
        except Exception:
            logger.info("Delete incomplete virtualenv: %s", path)
            shutil.rmtree(path, ignore_errors=True)
            logger.exception("Failed to install pip packages.")
            raise

    def __await__(self):
        return self._run().__await__()


class PipPlugin(RuntimeEnvPlugin):
    name = "pip"

    def __init__(self, resources_dir: str):
        self._pip_resources_dir = os.path.join(resources_dir, "pip")
        self._creating_task = {}
        # Maps a URI to a lock that is used to prevent multiple concurrent
        # installs of the same virtualenv, see #24513
        self._create_locks: Dict[str, asyncio.Lock] = {}
        try_to_create_directory(self._pip_resources_dir)

    def _get_path_from_hash(self, hash: str) -> str:
        """Generate a path from the hash of a pip spec.

        Example output:
            /tmp/ray/session_2021-11-03_16-33-59_356303_41018/runtime_resources
                /pip/ray-9a7972c3a75f55e976e620484f58410c920db091
        """
        return os.path.join(self._pip_resources_dir, hash)

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        """Return the pip URI from the RuntimeEnv if it exists, else return []."""
        pip_uri = runtime_env.pip_uri()
        if pip_uri:
            return [pip_uri]
        return []

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info("Got request to delete pip URI %s", uri)
        protocol, hash = parse_uri(uri)
        if protocol != Protocol.PIP:
            raise ValueError(
                "PipPlugin can only delete URIs with protocol "
                f"pip. Received protocol {protocol}, URI {uri}"
            )

        # Cancel running create task.
        task = self._creating_task.pop(hash, None)
        if task is not None:
            task.cancel()

        pip_env_path = self._get_path_from_hash(hash)
        local_dir_size = get_directory_size_bytes(pip_env_path)
        del self._create_locks[uri]
        try:
            shutil.rmtree(pip_env_path)
        except OSError as e:
            logger.warning(f"Error when deleting pip env {pip_env_path}: {str(e)}")
            return 0

        return local_dir_size

    async def create(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        if not runtime_env.has_pip():
            return 0

        protocol, hash = parse_uri(uri)
        target_dir = self._get_path_from_hash(hash)

        async def _create_for_hash():
            await PipProcessor(
                target_dir,
                runtime_env,
                logger,
            )

            loop = get_running_loop()
            return await loop.run_in_executor(
                None, get_directory_size_bytes, target_dir
            )

        if uri not in self._create_locks:
            # async lock to prevent the same virtualenv being concurrently installed
            self._create_locks[uri] = asyncio.Lock()

        async with self._create_locks[uri]:
            self._creating_task[hash] = task = create_task(_create_for_hash())
            task.add_done_callback(lambda _: self._creating_task.pop(hash, None))
            return await task

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ):
        if not runtime_env.has_pip():
            return
        # PipPlugin only uses a single URI.
        uri = uris[0]
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
        context.command_prefix += _PathHelper.get_virtualenv_activate_command(
            target_dir
        )
