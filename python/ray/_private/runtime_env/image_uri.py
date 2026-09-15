import asyncio
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

default_logger = logging.getLogger(__name__)


def validate_worker_resource_limits_support() -> None:
    """Fail early for the Podman configuration known not to support cgroup limits."""
    if sys.platform != "linux":
        raise RuntimeError(
            "Per-worker container CPU and memory limits are supported only on Linux."
        )
    if os.geteuid() != 0 and not Path("/sys/fs/cgroup/cgroup.controllers").exists():
        raise RuntimeError(
            "Per-worker container CPU and memory limits require cgroup v2 when "
            "Podman runs rootless. Use cgroup v2 or run Podman as root."
        )


def apply_worker_resource_limits(
    context: RuntimeEnvContext,
    cpu_period_us: int,
    cpu_quota_us: int,
    memory_bytes: int,
) -> None:
    """Add one worker's normalized cgroup limits to its Podman command."""
    if (cpu_period_us == 0) != (cpu_quota_us == 0):
        raise ValueError("CPU period and quota must either both be set or both be zero")
    if cpu_period_us < 0 or cpu_quota_us < 0 or memory_bytes < 0:
        raise ValueError("Worker resource limits cannot be negative")
    if cpu_period_us > 0 and not 1000 <= cpu_period_us <= 1_000_000:
        raise ValueError("CPU period must be between 1000 and 1000000 microseconds")
    if 0 < cpu_quota_us < 1000:
        raise ValueError("CPU quota must be at least 1000 microseconds")
    if cpu_period_us == 0 and memory_bytes == 0:
        return

    marker = " --entrypoint python "
    command_prefix, separator, command_suffix = context.py_executable.rpartition(marker)
    if not separator or not command_prefix.startswith("podman run "):
        raise RuntimeError(
            "Cannot apply per-worker resource limits: the image_uri runtime "
            "environment did not produce the expected Podman worker command."
        )

    options = []
    if cpu_period_us > 0:
        options.extend(
            [
                f"--cpu-period={cpu_period_us}",
                f"--cpu-quota={cpu_quota_us}",
            ]
        )
    if memory_bytes > 0:
        options.append(f"--memory={memory_bytes}b")

    context.py_executable = (
        f"{command_prefix} {' '.join(options)}{marker}{command_suffix}"
    )


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
