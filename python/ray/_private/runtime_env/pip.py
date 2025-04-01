import asyncio
import hashlib
import json
import logging
import os
import shutil
import sys
import shlex
from typing import Dict, List, Optional
from asyncio import create_task, get_running_loop

from ray._private.runtime_env import virtualenv_utils
from ray._private.runtime_env import dependency_utils
from ray._private.runtime_env.packaging import Protocol, parse_uri
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.utils import check_output_cmd
from ray._private.utils import get_directory_size_bytes, try_to_create_directory

default_logger = logging.getLogger(__name__)


def _is_pip_cmd(s):
    return s.strip().startswith("pip ")


def _get_pip_hash(pip_dict: Dict) -> str:
    serialized_pip_spec = json.dumps(pip_dict, sort_keys=True)
    hash_val = hashlib.sha1(serialized_pip_spec.encode("utf-8")).hexdigest()
    return hash_val


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

        python = virtualenv_utils.get_virtualenv_python(path)
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
        python = virtualenv_utils.get_virtualenv_python(path)

        await check_output_cmd(
            [python, "-m", "pip", "check", "--disable-pip-version-check"],
            logger=logger,
            cwd=cwd,
            env=pip_env,
        )

        logger.info("Pip check on %s successfully.", path)

    @classmethod
    async def _install_pip_packages(
        cls,
        path: str,
        pip_packages: List[str],
        cwd: str,
        pip_env: Dict,
        logger: logging.Logger,
    ):
        virtualenv_path = virtualenv_utils.get_virtualenv_path(path)
        python = virtualenv_utils.get_virtualenv_python(path)
        # TODO(fyrestone): Support -i, --no-deps, --no-cache-dir, ...
        pip_requirements_file = dependency_utils.get_requirements_file(
            path, pip_packages
        )

        # Avoid blocking the event loop.
        loop = get_running_loop()
        await loop.run_in_executor(
            None,
            dependency_utils.gen_requirements_txt,
            pip_requirements_file,
            pip_packages,
        )

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

    @classmethod
    async def _run_pip_cmd_requirements(
        cls,
        path: str,
        pip_commands: List[str],
        cwd: str,
        pip_env: Dict,
        logger: logging.Logger,
    ):
        virtualenv_path = virtualenv_utils.get_virtualenv_path(path)
        python = virtualenv_utils.get_virtualenv_python(path)
        logger.info("execute pip command to %s", virtualenv_path)
        for cmd_str in pip_commands:
            cmd = shlex.split(cmd_str)
            if cmd[0] != "pip":
                raise ValueError(f"Unexpected Python requirement: {cmd_str}")
            await check_output_cmd(
                [python, "-m"] + cmd, logger=logger, cwd=cwd, env=pip_env
            )

    async def _run(self):
        path = self._target_dir
        logger = self._logger
        pip_dependencies = self._pip_config["packages"]
        # We create an empty directory for exec cmd so that the cmd will
        # run more stable. e.g. if cwd has ray, then checking ray will
        # look up ray in cwd instead of site packages.
        exec_cwd = os.path.join(path, "exec_cwd")
        os.makedirs(exec_cwd, exist_ok=True)
        pip_packages = []
        pip_commands = []

        for pip_dep in pip_dependencies:
            if _is_pip_cmd(pip_dep):
                pip_commands.append(pip_dep)
            else:
                pip_packages.append(pip_dep)
        try:
            await virtualenv_utils.create_or_get_virtualenv(path, exec_cwd, logger)
            python = virtualenv_utils.get_virtualenv_python(path)
            async with dependency_utils.check_ray(python, exec_cwd, logger):
                # Ensure pip version.
                await self._ensure_pip_version(
                    path,
                    self._pip_config.get("pip_version", None),
                    exec_cwd,
                    self._pip_env,
                    logger,
                )
                # Install pip packages.
                if len(pip_packages) > 0:
                    await self._install_pip_packages(
                        path,
                        pip_packages,
                        exec_cwd,
                        self._pip_env,
                        logger,
                    )
                # NOTE(Jacky): If there are both pip packages and pip commands in the `pip packages` field,
                # we will first write all pip packages into `requirements.txt` for installation.
                # Then pip commands will be executed in the order of pip commands.
                # Therefore, previously installed packages may be overwritten
                # so that we do not guarantee that the packages installed by pip_packages or pip_command
                # will exist in the ray virtual environment.
                # Users need to determine whether there are pip package dependency conflicts
                # or pip package dependencies are overwritten.
                if len(pip_commands) > 0:
                    await self._run_pip_cmd_requirements(
                        path,
                        pip_commands,
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
        # Key: created hashes. Value: size of the pip dir.
        self._created_hash_bytes: Dict[str, int] = {}
        try_to_create_directory(self._pip_resources_dir)

    def _get_path_from_hash(self, hash_val: str) -> str:
        """Generate a path from the hash of a pip spec.

        Example output:
            /tmp/ray/session_2021-11-03_16-33-59_356303_41018/runtime_resources
                /pip/ray-9a7972c3a75f55e976e620484f58410c920db091
        """
        return os.path.join(self._pip_resources_dir, hash_val)

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
        protocol, hash_val = parse_uri(uri)
        if protocol != Protocol.PIP:
            raise ValueError(
                "PipPlugin can only delete URIs with protocol "
                f"pip. Received protocol {protocol}, URI {uri}"
            )

        # Cancel running create task.
        task = self._creating_task.pop(hash_val, None)
        if task is not None:
            task.cancel()

        del self._created_hash_bytes[hash_val]

        pip_env_path = self._get_path_from_hash(hash_val)
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
        context: "RuntimeEnvContext",  # noqa: F821
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        if not runtime_env.has_pip():
            return 0

        protocol, hash_val = parse_uri(uri)
        target_dir = self._get_path_from_hash(hash_val)

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
            if hash_val in self._created_hash_bytes:
                return self._created_hash_bytes[hash_val]
            self._creating_task[hash_val] = task = create_task(_create_for_hash())
            task.add_done_callback(lambda _: self._creating_task.pop(hash_val, None))
            pip_dir_bytes = await task
            self._created_hash_bytes[hash_val] = pip_dir_bytes
            return pip_dir_bytes

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: "RuntimeEnvContext",  # noqa: F821
        logger: logging.Logger = default_logger,
    ):
        if not runtime_env.has_pip():
            return
        # PipPlugin only uses a single URI.
        uri = uris[0]
        # Update py_executable.
        protocol, hash_val = parse_uri(uri)
        target_dir = self._get_path_from_hash(hash_val)
        virtualenv_python = virtualenv_utils.get_virtualenv_python(target_dir)

        if not os.path.exists(virtualenv_python):
            raise ValueError(
                f"Local directory {target_dir} for URI {uri} does "
                "not exist on the cluster. Something may have gone wrong while "
                "installing the runtime_env `pip` packages."
            )
        context.py_executable = virtualenv_python
        context.command_prefix += virtualenv_utils.get_virtualenv_activate_command(
            target_dir
        )
