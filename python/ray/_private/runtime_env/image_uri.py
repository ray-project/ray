import asyncio
import logging
import os
import shlex
import tempfile
from typing import List, Optional

from ray._private.authentication.authentication_token_setup import (
    _get_default_token_path,
)
from ray._private.authentication.authentication_utils import is_token_auth_enabled
from ray._private.runtime_env.context import (
    RuntimeEnvContext,
    expand_run_option,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

default_logger = logging.getLogger(__name__)


async def _create_impl(image_uri: str, logger: logging.Logger):
    # Pull image if it doesn't exist
    # Also get path to `default_worker.py` inside the image.
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chmod(tmpdir, 0o777)
        result_file = os.path.join(tmpdir, "worker_path.txt")
        get_worker_path_script = """
import ray._private.workers.default_worker as dw
with open('/shared/worker_path.txt', 'w') as f:
    f.write(dw.__file__)
"""
        cmd = [
            "podman",
            "run",
            "--rm",
            "-v",
            f"{tmpdir}:/shared:Z",
            image_uri,
            "python",
            "-c",
            get_worker_path_script,
        ]

        logger.info("Pulling image %s", image_uri)

        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            raise RuntimeError(
                f"Podman command failed: cmd={cmd}, returncode={process.returncode}, stdout={stdout.decode()}, stderr={stderr.decode()}"
            )

        if not os.path.exists(result_file):
            raise FileNotFoundError(
                f"Worker path file not created when getting worker path for image {image_uri}"
            )

        with open(result_file, "r") as f:
            worker_path = f.read().strip()

        if not worker_path.endswith(".py"):
            raise ValueError(
                f"Invalid worker path inferred in image {image_uri}: {worker_path}"
            )

        logger.info(f"Inferred worker path in image {image_uri}: {worker_path}")
        return worker_path


def _modify_context_impl(
    image_uri: str,
    worker_path: str,
    run_options: Optional[List[str]],
    context: RuntimeEnvContext,
    logger: logging.Logger,
    ray_tmp_dir: str,
):
    context.override_worker_entrypoint = worker_path

    container_driver = "podman"
    container_command = [
        container_driver,
        "run",
        "-v",
        ray_tmp_dir + ":" + ray_tmp_dir,
        "--cgroup-manager=cgroupfs",
        "--network=host",
        "--pid=host",
        "--ipc=host",
        # NOTE(zcin): Mounted volumes in rootless containers are
        # owned by the user `root`. The user on host (which will
        # usually be `ray` if this is being run in a ray docker
        # image) who started the container is mapped using user
        # namespaces to the user `root` in a rootless container. In
        # order for the Ray Python worker to access the mounted ray
        # tmp dir, we need to use keep-id mode which maps the user
        # as itself (instead of as `root`) into the container.
        # https://www.redhat.com/sysadmin/rootless-podman-user-namespace-modes
        "--userns=keep-id",
    ]

    # Ray-owned values for the container. The job's env_vars and the host's
    # RAY_* vars are added when the worker is exec'd; see
    # RuntimeEnvContext._exec_container_worker.
    ray_container_env = dict()

    # A token kept only in a file has no value in podman's environment to pass
    # by name, so mount the file read-only. A token in RAY_AUTH_TOKEN reaches
    # the container by name with the other RAY_* vars.
    if (
        is_token_auth_enabled()
        and "RAY_AUTH_TOKEN" not in context.env_vars
        and "RAY_AUTH_TOKEN" not in os.environ
    ):
        # Ignore the job's env_vars: a job-chosen path would mount any
        # host file the Ray user can read into the container.
        token_path = os.path.abspath(
            os.environ.get("RAY_AUTH_TOKEN_PATH") or _get_default_token_path()
        )
        if os.path.isfile(token_path):
            container_command.extend(["-v", f"{token_path}:{token_path}:ro"])
            ray_container_env["RAY_AUTH_TOKEN_PATH"] = token_path

    run_options = list(run_options or [])
    # Expanded at exec time, but checked here so a malformed item fails
    # runtime_env setup instead of every worker start.
    for run_option in run_options:
        expand_run_option(run_option, {})
    # TODO(chenk008): add resource limit

    # Example:
    # podman run -v /tmp/ray:/tmp/ray
    # --cgroup-manager=cgroupfs --network=host --pid=host --ipc=host
    # --userns=keep-id rayproject/ray:nightly-py39
    logger.info(
        "Starting worker in container with prefix %s %s",
        shlex.join(container_command + run_options),
        image_uri,
    )

    context.container = {
        "command": container_command,
        "run_options": run_options,
        "image": image_uri,
        "entrypoint": "python",
        "env_vars": ray_container_env,
    }


class ImageURIPlugin(RuntimeEnvPlugin):
    """Starts worker in a container of a custom image."""

    name = "image_uri"

    @staticmethod
    def get_compatible_keys():
        return {"image_uri", "config", "env_vars"}

    def __init__(self, ray_tmp_dir: str):
        self._ray_tmp_dir = ray_tmp_dir

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        if not runtime_env.image_uri():
            return

        self.worker_path = await _create_impl(runtime_env.image_uri(), logger)

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.image_uri():
            return

        _modify_context_impl(
            runtime_env.image_uri(),
            self.worker_path,
            [],
            context,
            logger,
            self._ray_tmp_dir,
        )


class ContainerPlugin(RuntimeEnvPlugin):
    """Starts worker in container."""

    name = "container"

    def __init__(self, ray_tmp_dir: str):
        self._ray_tmp_dir = ray_tmp_dir

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        if not runtime_env.has_py_container() or not runtime_env.py_container_image():
            return

        self.worker_path = await _create_impl(runtime_env.py_container_image(), logger)

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.has_py_container() or not runtime_env.py_container_image():
            return

        if runtime_env.py_container_worker_path():
            logger.warning(
                "You are using `container.worker_path`, but the path to "
                "`default_worker.py` is now automatically detected from the image. "
                "`container.worker_path` is deprecated and will be removed in future "
                "versions."
            )

        _modify_context_impl(
            runtime_env.py_container_image(),
            runtime_env.py_container_worker_path() or self.worker_path,
            runtime_env.py_container_run_options(),
            context,
            logger,
            self._ray_tmp_dir,
        )
