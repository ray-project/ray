import logging
import os
from typing import List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.utils import check_output_cmd

default_logger = logging.getLogger(__name__)


async def _create_impl(image_uri: str, logger: logging.Logger):
    # Pull image if it doesn't exist
    # Also get path to `default_worker.py` inside the image.
    pull_image_cmd = [
        "podman",
        "run",
        "--rm",
        image_uri,
        "python",
        "-c",
        (
            "import ray._private.workers.default_worker as default_worker; "
            "print(default_worker.__file__)"
        ),
    ]
    logger.info("Pulling image %s", image_uri)
    worker_path = await check_output_cmd(pull_image_cmd, logger=logger)
    return worker_path.strip()


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

    # Environment variables to set in container
    env_vars = dict()

    # Propagate all host environment variables that have the prefix "RAY_"
    # This should include RAY_RAYLET_PID
    for env_var_name, env_var_value in os.environ.items():
        if env_var_name.startswith("RAY_"):
            env_vars[env_var_name] = env_var_value

    # Support for runtime_env['env_vars']
    env_vars.update(context.env_vars)

    # Set environment variables
    for env_var_name, env_var_value in env_vars.items():
        container_command.append("--env")
        container_command.append(f"{env_var_name}='{env_var_value}'")

    # The RAY_JOB_ID environment variable is needed for the default worker.
    # It won't be set at the time setup() is called, but it will be set
    # when worker command is executed, so we use RAY_JOB_ID=$RAY_JOB_ID
    # for the container start command
    container_command.append("--env")
    container_command.append("RAY_JOB_ID=$RAY_JOB_ID")

    if run_options:
        container_command.extend(run_options)
    # TODO(chenk008): add resource limit
    container_command.append("--entrypoint")
    container_command.append("python")
    container_command.append(image_uri)

    # Example:
    # podman run -v /tmp/ray:/tmp/ray
    # --cgroup-manager=cgroupfs --network=host --pid=host --ipc=host
    # --userns=keep-id --env RAY_RAYLET_PID=23478 --env RAY_JOB_ID=$RAY_JOB_ID
    # --entrypoint python rayproject/ray:nightly-py39
    container_command_str = " ".join(container_command)
    logger.info(f"Starting worker in container with prefix {container_command_str}")

    context.py_executable = container_command_str


class ImageURIPlugin(RuntimeEnvPlugin):
    """Starts worker in a container of a custom image."""

    name = "image_uri"

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
