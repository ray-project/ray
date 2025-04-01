import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

import ray._private.ray_constants as ray_constants
import ray._private.runtime_env.agent.runtime_env_consts as runtime_env_consts
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    Protocol,
    delete_package,
    download_and_unpack_package,
    get_local_dir_from_uri,
    get_uri_for_directory,
    get_uri_for_package,
    parse_uri,
    upload_package_if_needed,
    upload_package_to_gcs,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import get_directory_size_bytes, try_to_create_directory
from ray.exceptions import RuntimeEnvSetupError

default_logger = logging.getLogger(__name__)

_WIN32 = os.name == "nt"


def upload_working_dir_if_needed(
    runtime_env: Dict[str, Any],
    scratch_dir: Optional[str] = os.getcwd(),
    logger: Optional[logging.Logger] = default_logger,
    upload_fn=None,
) -> Dict[str, Any]:
    """Uploads the working_dir and replaces it with a URI.

    If the working_dir is already a URI, this is a no-op.
    """
    working_dir = runtime_env.get("working_dir")
    if working_dir is None:
        return runtime_env

    if not isinstance(working_dir, str) and not isinstance(working_dir, Path):
        raise TypeError(
            "working_dir must be a string or Path (either a local path "
            f"or remote URI), got {type(working_dir)}."
        )

    if isinstance(working_dir, Path):
        working_dir = str(working_dir)

    # working_dir is already a URI -- just pass it through.
    try:
        protocol, path = parse_uri(working_dir)
    except ValueError:
        protocol, path = None, None

    if protocol is not None:
        if protocol in Protocol.remote_protocols() and not path.endswith(".zip"):
            raise ValueError("Only .zip files supported for remote URIs.")
        return runtime_env

    excludes = runtime_env.get("excludes", None)
    try:
        working_dir_uri = get_uri_for_directory(working_dir, excludes=excludes)
    except ValueError:  # working_dir is not a directory
        package_path = Path(working_dir)
        if not package_path.exists() or package_path.suffix != ".zip":
            raise ValueError(
                f"directory {package_path} must be an existing "
                "directory or a zip package"
            )

        pkg_uri = get_uri_for_package(package_path)
        try:
            upload_package_to_gcs(pkg_uri, package_path.read_bytes())
        except Exception as e:
            raise RuntimeEnvSetupError(
                f"Failed to upload package {package_path} to the Ray cluster: {e}"
            ) from e
        runtime_env["working_dir"] = pkg_uri
        return runtime_env
    if upload_fn is None:
        try:
            upload_package_if_needed(
                working_dir_uri,
                scratch_dir,
                working_dir,
                include_parent_dir=False,
                excludes=excludes,
                logger=logger,
            )
        except Exception as e:
            raise RuntimeEnvSetupError(
                f"Failed to upload working_dir {working_dir} to the Ray cluster: {e}"
            ) from e
    else:
        upload_fn(working_dir, excludes=excludes)

    runtime_env["working_dir"] = working_dir_uri
    return runtime_env


def set_pythonpath_in_context(python_path: str, context: RuntimeEnvContext):
    """Insert the path as the first entry in PYTHONPATH in the runtime env.

    This is compatible with users providing their own PYTHONPATH in env_vars,
    and is also compatible with the existing PYTHONPATH in the cluster.

    The import priority is as follows:
    this python_path arg > env_vars PYTHONPATH > existing cluster env PYTHONPATH.
    """
    if "PYTHONPATH" in context.env_vars:
        python_path += os.pathsep + context.env_vars["PYTHONPATH"]
    if "PYTHONPATH" in os.environ:
        python_path += os.pathsep + os.environ["PYTHONPATH"]
    context.env_vars["PYTHONPATH"] = python_path


class WorkingDirPlugin(RuntimeEnvPlugin):

    name = "working_dir"

    # Note working_dir is not following the priority order of other plugins. Instead
    # it's specially treated to happen before all other plugins.
    priority = 5
    working_dir_placeholder = "$WORKING_DIR_PLACEHOLDER"

    def __init__(
        self, resources_dir: str, gcs_aio_client: "GcsAioClient"  # noqa: F821
    ):
        self._resources_dir = os.path.join(resources_dir, "working_dir_files")
        self._working_dirs = os.path.join(resources_dir, "working_dirs")
        self._gcs_aio_client = gcs_aio_client
        try_to_create_directory(self._resources_dir)

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info("Got request to delete working dir URI %s", uri)
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        local_dir_size = get_directory_size_bytes(local_dir)

        deleted = delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning(f"Tried to delete nonexistent URI: {uri}.")
            return 0

        return local_dir_size

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        working_dir_uri = runtime_env.working_dir()
        if working_dir_uri != "":
            return [working_dir_uri]
        return []

    async def create(
        self,
        uri: Optional[str],
        runtime_env: dict,
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        local_dir = await download_and_unpack_package(
            uri,
            self._resources_dir,
            self._gcs_aio_client,
            logger=logger,
            overwrite=True,
        )
        return get_directory_size_bytes(local_dir)

    def modify_context(
        self,
        uris: List[str],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not uris:
            return

        # WorkingDirPlugin uses a single URI.
        uri = uris[0]
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        if not local_dir.exists():
            raise ValueError(
                f"Local directory {local_dir} for URI {uri} does "
                "not exist on the cluster. Something may have gone wrong while "
                "downloading or unpacking the working_dir."
            )
        # Use placeholder here and will replace it by `pre_worker_startup`.
        context.working_dir = WorkingDirPlugin.working_dir_placeholder
        working_dir = context.working_dir
        context.symlink_paths_to_working_dir.append(str(local_dir))
        context.env_vars[runtime_env_consts.RAY_WORKING_DIR] = working_dir

        if not _WIN32:
            context.command_prefix += [
                "cd",
                str(working_dir),
                "&&",
            ]
        else:
            # Include '/d' incase temp folder is on different drive than Ray install.
            context.command_prefix += [
                "cd",
                "/d",
                str(working_dir),
                "&&",
            ]
        set_pythonpath_in_context(python_path=str(working_dir), context=context)

    async def pre_worker_startup(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        worker_id: str,
        job_id: str,
        logger: logging.Logger = default_logger,
    ) -> None:
        if not runtime_env.working_dir():
            return
        logger.info(f"Creating working dir for worker {worker_id}, job id {job_id}")
        working_dir = os.path.join(self._working_dirs, worker_id)
        os.makedirs(working_dir, exist_ok=True)
        context.working_dir = working_dir
        # Replace the placeholder with the real working dir.
        for i, prefix in enumerate(context.command_prefix):
            context.command_prefix[i] = prefix.replace(
                WorkingDirPlugin.working_dir_placeholder, working_dir
            )
        for k, v in context.env_vars.items():
            context.env_vars[k] = v.replace(
                WorkingDirPlugin.working_dir_placeholder, working_dir
            )
        # Add symbol links to the working dir.
        # Deduplicate, as linking duplicate file will raise FileExistsError
        linked_dir = set()
        for symlink_dir in context.symlink_paths_to_working_dir:
            for name in os.listdir(symlink_dir):
                src_path = os.path.join(symlink_dir, name)
                link_path = os.path.join(working_dir, name)
                if src_path not in linked_dir:
                    if os.path.isfile(src_path) and src_path.endswith("jar"):
                        # Hard link jar files only
                        logger.info(
                            f"Creating hardlink from {src_path} to {link_path}."
                        )
                        os.link(src_path, link_path)
                    else:
                        logger.info(f"Creating symlink from {src_path} to {link_path}.")
                        os.symlink(src_path, link_path)
                    linked_dir.add(src_path)

    async def post_worker_exit(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        worker_id: str,
        logger: logging.Logger = default_logger,
    ) -> None:
        if not runtime_env.working_dir():
            return
        # TODO(Jacky): Cache working dirs for debugging.
        if runtime_env_consts.DISABLE_WORKING_DIR_GC:
            logger.info(
                f"Working directory GC has been disabled. The dir {worker_id} "
                "won't be deleted."
            )
            return
        logger.info(f"Deleting working dir for worker {worker_id}")
        working_dir = os.path.join(self._working_dirs, worker_id)
        # TODO(Jacky): Use async method to remove, such as aiofiles.os.removedirs
        shutil.rmtree(working_dir)

    @contextmanager
    def with_working_dir_env(self, uri):
        """
        If uri is not None, add the local working directory to the environment variable
        as "RAY_RUNTIME_ENV_CREATE_WORKING_DIR". This is useful for other plugins to
        create their environment with reference to the working directory. For example
        `pip -r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/requirements.txt`

        The environment variable is removed after the context manager exits.
        """
        if uri is None:
            yield
        else:
            local_dir = get_local_dir_from_uri(uri, self._resources_dir)
            if not local_dir.exists():
                raise ValueError(
                    f"Local directory {local_dir} for URI {uri} does "
                    "not exist on the cluster. Something may have gone wrong while "
                    "downloading or unpacking the working_dir."
                )
            key = ray_constants.RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR
            prev = os.environ.get(key)
            # Windows backslash paths are weird. When it's passed to the env var, and
            # when Pip expands it, the backslashes are interpreted as escape characters
            # and messes up the whole path. So we convert it to forward slashes.
            # This works at least for all Python applications, including pip.
            os.environ[key] = local_dir.as_posix()
            try:
                yield
            finally:
                if prev is None:
                    del os.environ[key]
                else:
                    os.environ[key] = prev
