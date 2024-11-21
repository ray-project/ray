"""Util class to install packages via uv.
"""

from typing import Dict, List, Optional
import os
import hashlib
from ray._private.runtime_env import virtualenv_utils
from ray._private.runtime_env import dependency_utils
from ray._private.runtime_env.utils import check_output_cmd
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.packaging import Protocol, parse_uri
from asyncio import create_task, get_running_loop
import shutil
import logging
import json
import asyncio
import sys
from ray._private.utils import try_to_create_directory, get_directory_size_bytes

default_logger = logging.getLogger(__name__)


def _get_uv_hash(uv_dict: Dict) -> str:
    """Get a deterministic hash value for `uv` related runtime envs."""
    serialized_uv_spec = json.dumps(uv_dict, sort_keys=True)
    hash_val = hashlib.sha1(serialized_uv_spec.encode("utf-8")).hexdigest()
    return hash_val


def get_uri(runtime_env: Dict) -> Optional[str]:
    """Return `"uv://<hashed_dependencies>"`, or None if no GC required."""
    uv = runtime_env.get("uv")
    if uv is not None:
        if isinstance(uv, dict):
            uri = "uv://" + _get_uv_hash(uv_dict=uv)
        elif isinstance(uv, list):
            uri = "uv://" + _get_uv_hash(uv_dict=dict(packages=uv))
        else:
            raise TypeError(
                "uv field received by RuntimeEnvAgent must be "
                f"list or dict, not {type(uv).__name__}."
            )
    else:
        uri = None
    return uri


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
        # An empty directory is created to execute cmd.
        self._exec_cwd = os.path.join(self._target_dir, "exec_cwd")
        self._runtime_env = runtime_env
        self._logger = logger

        self._uv_config = self._runtime_env.uv_config()
        self._uv_env = os.environ.copy()
        self._uv_env.update(self._runtime_env.env_vars())

    async def _install_uv(
        self, path: str, cwd: str, pip_env: dict, logger: logging.Logger
    ):
        """Before package install, make sure the required version `uv` (if specifieds)
        is installed.
        """
        virtualenv_path = virtualenv_utils.get_virtualenv_path(path)
        python = virtualenv_utils.get_virtualenv_python(path)

        def _get_uv_exec_to_install() -> str:
            """Get `uv` executable with version to install."""
            uv_version = self._uv_config.get("uv_version", None)
            if uv_version:
                return f"uv{uv_version}"
            # Use default version.
            return "uv"

        uv_install_cmd = [
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-cache-dir",
            _get_uv_exec_to_install(),
        ]
        logger.info("Installing package uv to %s", virtualenv_path)
        await check_output_cmd(uv_install_cmd, logger=logger, cwd=cwd, env=pip_env)

    async def _check_uv_existence(
        self, path: str, cwd: str, env: dict, logger: logging.Logger
    ) -> bool:
        """Check and return the existence of `uv` in virtual env."""
        python = virtualenv_utils.get_virtualenv_python(path)

        check_existence_cmd = [
            python,
            "-m",
            "uv",
            "version",
        ]

        try:
            # If `uv` doesn't exist, exception will be thrown.
            await check_output_cmd(check_existence_cmd, logger=logger, cwd=cwd, env=env)
            return True
        except Exception:
            return False

    async def _uv_check(sef, python: str, cwd: str, logger: logging.Logger) -> None:
        """Check virtual env dependency compatibility.
        If any incompatibility detected, exception will be thrown.

        param:
            python: the path for python executable within virtual environment.
        """
        cmd = [python, "-m", "uv", "pip", "check"]
        await check_output_cmd(
            cmd,
            logger=logger,
            cwd=cwd,
        )

    async def _install_uv_packages(
        self,
        path: str,
        uv_packages: List[str],
        cwd: str,
        pip_env: Dict,
        logger: logging.Logger,
    ):
        """Install required python packages via `uv`."""
        virtualenv_path = virtualenv_utils.get_virtualenv_path(path)
        python = virtualenv_utils.get_virtualenv_python(path)
        # TODO(fyrestone): Support -i, --no-deps, --no-cache-dir, ...
        requirements_file = dependency_utils.get_requirements_file(path, uv_packages)

        # Check existence for `uv` and see if we could skip `uv` installation.
        uv_exists = await self._check_uv_existence(python, cwd, pip_env, logger)

        # Install uv, which acts as the default package manager.
        if (not uv_exists) or (self._uv_config.get("uv_version", None) is not None):
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

        # Check python environment for conflicts.
        if self._uv_config.get("uv_check", False):
            await self._uv_check(python, cwd, logger)

    async def _run(self):
        path = self._target_dir
        logger = self._logger
        uv_packages = self._uv_config["packages"]
        # We create an empty directory for exec cmd so that the cmd will
        # run more stable. e.g. if cwd has ray, then checking ray will
        # look up ray in cwd instead of site packages.
        os.makedirs(self._exec_cwd, exist_ok=True)
        try:
            await virtualenv_utils.create_or_get_virtualenv(
                path, self._exec_cwd, logger
            )
            python = virtualenv_utils.get_virtualenv_python(path)
            async with dependency_utils.check_ray(python, self._exec_cwd, logger):
                # Install packages with uv.
                await self._install_uv_packages(
                    path,
                    uv_packages,
                    self._exec_cwd,
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


class UvPlugin(RuntimeEnvPlugin):
    name = "uv"

    def __init__(self, resources_dir: str):
        self._uv_resource_dir = os.path.join(resources_dir, "uv")
        self._creating_task = {}
        # Maps a URI to a lock that is used to prevent multiple concurrent
        # installs of the same virtualenv, see #24513
        self._create_locks: Dict[str, asyncio.Lock] = {}
        # Key: created hashes. Value: size of the uv dir.
        self._created_hash_bytes: Dict[str, int] = {}
        try_to_create_directory(self._uv_resource_dir)

    def _get_path_from_hash(self, hash_val: str) -> str:
        """Generate a path from the hash of a uv spec.

        Example output:
            /tmp/ray/session_2021-11-03_16-33-59_356303_41018/runtime_resources
                /uv/ray-9a7972c3a75f55e976e620484f58410c920db091
        """
        return os.path.join(self._uv_resource_dir, hash_val)

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        """Return the uv URI from the RuntimeEnv if it exists, else return []."""
        uv_uri = runtime_env.uv_uri()
        if uv_uri:
            return [uv_uri]
        return []

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info("Got request to delete uv URI %s", uri)
        protocol, hash_val = parse_uri(uri)
        if protocol != Protocol.UV:
            raise ValueError(
                "UvPlugin can only delete URIs with protocol "
                f"uv. Received protocol {protocol}, URI {uri}"
            )

        # Cancel running create task.
        task = self._creating_task.pop(hash_val, None)
        if task is not None:
            task.cancel()

        del self._created_hash_bytes[hash_val]

        uv_env_path = self._get_path_from_hash(hash_val)
        local_dir_size = get_directory_size_bytes(uv_env_path)
        del self._create_locks[uri]
        try:
            shutil.rmtree(uv_env_path)
        except OSError as e:
            logger.warning(f"Error when deleting uv env {uv_env_path}: {str(e)}")
            return 0

        return local_dir_size

    async def create(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: "RuntimeEnvContext",  # noqa: F821
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        if not runtime_env.has_uv():
            return 0

        protocol, hash_val = parse_uri(uri)
        target_dir = self._get_path_from_hash(hash_val)

        async def _create_for_hash():
            await UvProcessor(
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
            if hash_val in self._created_hash_bytes:
                return self._created_hash_bytes[hash_val]
            self._creating_task[hash_val] = task = create_task(_create_for_hash())
            task.add_done_callback(lambda _: self._creating_task.pop(hash_val, None))
            uv_dir_bytes = await task
            self._created_hash_bytes[hash_val] = uv_dir_bytes
            return uv_dir_bytes

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: "RuntimeEnvContext",  # noqa: F821
        logger: logging.Logger = default_logger,
    ):
        if not runtime_env.has_uv():
            return
        # UvPlugin only uses a single URI.
        uri = uris[0]
        # Update py_executable.
        protocol, hash_val = parse_uri(uri)
        target_dir = self._get_path_from_hash(hash_val)
        virtualenv_python = virtualenv_utils.get_virtualenv_python(target_dir)

        if not os.path.exists(virtualenv_python):
            raise ValueError(
                f"Local directory {target_dir} for URI {uri} does "
                "not exist on the cluster. Something may have gone wrong while "
                "installing the runtime_env `uv` packages."
            )
        context.py_executable = virtualenv_python
        context.command_prefix += virtualenv_utils.get_virtualenv_activate_command(
            target_dir
        )
