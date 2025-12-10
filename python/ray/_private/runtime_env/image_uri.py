import asyncio
import logging
import os
import tempfile
import shlex
from typing import List, Optional
import ray
import ray._private.runtime_env.constants as runtime_env_constants
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

from ray._private.utils import (
    get_ray_site_packages_path,
    get_pyenv_path,
    try_parse_default_mount_points,
    try_parse_container_run_options,
    try_update_runtime_env_vars,
    get_ray_whl_dir,
    get_dependencies_installer_path,
    try_generate_entrypoint_args,
    parse_allocated_resource,
)

default_logger = logging.getLogger(__name__)


async def _create_impl(image_uri: str, logger: logging.Logger):
    # Pull image if it doesn't exist
    # Also get path to `default_worker.py` inside the image.
    custom_pull_cmd = os.getenv("RAY_PODMAN_PULL_CMD", "")
    if custom_pull_cmd:
        logger.info("Using custom pull command: %s", custom_pull_cmd)
        cmd = ["sh", "-c", custom_pull_cmd]
    else:
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

    output_filter = os.getenv("RAY_PODMAN_OUTPUT_FILTER", "")
    if output_filter:
        worker_path = await _apply_output_filter(logger, worker_path, output_filter)
        worker_path = worker_path.strip()
    logger.info(f"Inferred worker path in image after filter {image_uri}: {worker_path}")
    return worker_path

async def _apply_output_filter(logger, worker_path, output_filter):
    safe_worker_path = shlex.quote(worker_path)
    filter_cmd = ["sh", "-c", f"printf '%s' {safe_worker_path} | {output_filter}"]
    filtered_path = await check_output_cmd(filter_cmd, logger=logger)
    worker_path = filtered_path
    return worker_path

def _modify_container_context_impl(
    runtime_env: "RuntimeEnv",  # noqa: F821
    context: RuntimeEnvContext,
    ray_tmp_dir: str,
    worker_path: str,
    logger: Optional[logging.Logger] = default_logger,
):
    if not runtime_env.py_container_image():
        return
    context.override_worker_entrypoint = worker_path

    logger.info(f"container setup for {runtime_env}")
    container_option = runtime_env.get("container")

    # Use the user's python executable if py_executable is not None.
    py_executable = container_option.get("py_executable")
    container_install_ray = runtime_env.py_container_install_ray()
    container_isolate_pip_installation = (
        runtime_env.py_container_isolate_pip_installation()
    )
    container_dependencies_installer_path = (
        runtime_env_constants.RAY_PODMAN_DEPENDENCIES_INSTALLER_PATH
    )

    container_driver = "podman"
    container_command = [
        container_driver,
        "run",
        "--rm",
        "-v",
        ray_tmp_dir + ":" + ray_tmp_dir,
        "--cgroup-manager=cgroupfs",
        "--network=host",
        "--pid=host",
        "--ipc=host",
        "--env-host",
        "--cgroups=no-conmon",  # ANT-INTERNAL
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

    # Note(Jacky): If the same target_path is present in the container mount,
    # the image will not start due to the duplicate mount target path error,
    # so we create a reverse dict mapping,
    # which can modify the latest source_path.
    container_to_host_mount_dict = {}

    # we mount default mount path to container
    container_to_host_mount_dict = try_parse_default_mount_points(
        container_to_host_mount_dict
    )

    # `install_ray` and `isolate_pip_installation` are mutually exclusive
    if container_install_ray and container_isolate_pip_installation:
        raise ValueError(
            "`install_ray` and `isolate_pip_installation` can't both be True, "
            "please check your runtime_env field."
        )

    # Add reousrces isolation if needed
    if runtime_env.get_serialized_allocated_instances():
        container_command.extend(
            parse_allocated_resource(runtime_env.get_serialized_allocated_instances())
        )

    pip_packages = runtime_env.pip_config().get("packages", [])
    container_pip_packages = runtime_env.py_container_pip_list()
    # NOTE(Jacky): When `install_ray` is True or runtime_env field has container pip packages,
    # generate `entrypoint_args` to install dependencies before starting the worker.
    # These arguments will:
    # 1. Use the Python interpreter inside the container to install the specified version of `ant-ray` if needed.
    # 2. Install additional user-specified Python packages via `pip_packages`.
    # Example command structure:
    #   python /tmp/scripts/dependencies_installer.py --ray-version=2.0.0 --packages "{base64_pip_packages}"
    # The generated `entrypoint_args` are prefixed to the container's entrypoint, ensuring dependencies are installed
    # before the worker process starts.
    if container_install_ray or container_pip_packages:
        entrypoint_args = try_generate_entrypoint_args(
            container_install_ray,
            pip_packages,
            container_pip_packages,
            container_dependencies_installer_path,
            container_isolate_pip_installation,
            context,
        )
        context.container["entrypoint_prefix"] = entrypoint_args
    # we need 'sudo' and 'admin', mount logs
    if container_option.get("sudo"):
        container_command = ["sudo", "-E"] + container_command

    container_command.append("-u")
    # we set the user in the container, default is 'admin'
    user = container_option.get("user")
    if user:
        container_command.append(user)
    else:
        container_command.append("admin")
    container_command.append("-w")
    if context.working_dir:
        container_command.append(context.working_dir)
    else:
        container_command.append(os.getcwd())
    container_command.append("--cap-add=AUDIT_WRITE")

    redirected_pyenv_folder = None
    if container_install_ray or container_pip_packages:
        container_to_host_mount_dict[container_dependencies_installer_path] = (
            get_dependencies_installer_path()
        )
        if runtime_env_constants.RAY_PODMAN_UES_WHL_PACKAGE:
            container_to_host_mount_dict[get_ray_whl_dir()] = get_ray_whl_dir()

    if not container_install_ray:
        # mount ray package site path
        host_site_packages_path = get_ray_site_packages_path()

        # If the user specifies a `py_executable` in the container
        # and it starts with the ${PYENV_ROOT} environment variable (indicating a PYENV-managed executable),
        # we define `redirected_pyenv_folder` as `ray/.pyenv`.
        # This ensures that all .pyenv-related paths are redirected
        # to avoid overwriting the container's internal PYENV environment
        # (which defaults to `/home/admin/.pyenv`).
        if py_executable and py_executable.startswith(get_pyenv_path()):
            redirected_pyenv_folder = "ray/.pyenv"

        host_pyenv_path = get_pyenv_path()
        container_pyenv_path = host_pyenv_path
        if redirected_pyenv_folder:
            container_pyenv_path = host_pyenv_path.replace(
                ".pyenv", redirected_pyenv_folder
            )
            context.container[redirected_pyenv_folder] = redirected_pyenv_folder
        container_to_host_mount_dict[container_pyenv_path] = host_pyenv_path
        container_to_host_mount_dict[host_site_packages_path] = host_site_packages_path

    # For loop `run options` and append each item to the command line of podman
    run_options = container_option.get("run_options")
    if run_options:
        (
            container_command,
            container_to_host_mount_dict,
        ) = try_parse_container_run_options(
            run_options, container_command, container_to_host_mount_dict
        )

    for (
        target_path,
        source_path,
    ) in container_to_host_mount_dict.items():
        container_command.append("-v")
        container_command.append(f"{source_path}:{target_path}")
    if container_option.get("native_libraries"):
        container_native_libraries = container_option["native_libraries"]
        context.native_libraries["code_search_path"].append(container_native_libraries)

    # Environment variables to set in container
    context.env_vars = try_update_runtime_env_vars(
        context.env_vars, redirected_pyenv_folder
    )

    # Append the container_placeholder as a placeholder to the command.
    # This placeholder will be replaced in the `try_update_container_command`
    # function with all required environment variables (e.g., `--env RAY_RAYLET_PID=xxx`).
    # Environment variables like `LD_LIBRARY_PATH` need to be updated before the
    # `try_update_container_command` function, so direct inclusion of `--env` flags here
    # is avoided to ensure proper ordering and dynamic updates.
    container_command.append(runtime_env_constants.CONTAINER_ENV_PLACEHOLDER)
    container_command.append("--entrypoint")
    # Some docker image use conda to run python, it depend on ~/.bashrc.
    # So we need to use bash as container entrypoint.
    container_command.append("bash")

    # If podman integrate nydus, we use nydus image as rootfs
    if runtime_env_constants.RAY_PODMAN_USE_NYDUS:
        container_command.append("--rootfs")
        container_command.append(runtime_env.py_container_image() + ":O")
    else:
        container_command.append(runtime_env.py_container_image())
    container_command.extend(["-l", "-c"])
    context.container["container_command"] = container_command

    if py_executable:
        context.py_executable = py_executable
    # Example:
    # sudo -E podman run -v /tmp/ray:/tmp/ray
    # --cgroup-manager=cgroupfs --network=host --pid=host --ipc=host --env-host --cgroups=no-conmon
    # --userns=keep-id -v /home/admin/ray-pack:/home/admin/ray-pack --env RAY_RAYLET_PID=23478 --env RAY_JOB_ID=$RAY_JOB_ID
    # --entrypoint python rayproject/ray:nightly-py39
    logger.info(
        "start worker in container with prefix: {}".format(" ".join(container_command))
    )


def _modify_context_impl(
    image_uri: str,
    worker_path: str,
    run_options: Optional[List[str]],
    context: RuntimeEnvContext,
    logger: logging.Logger,
    ray_tmp_dir: str,
):
    context.override_worker_entrypoint = worker_path
    custom_container_cmd = os.getenv("RAY_PODMAN_CONTAINER_CMD", "")
    if custom_container_cmd:
        custom_container_cmd_str = custom_container_cmd.format(
            ray_tmp_dir=ray_tmp_dir, image_uri=image_uri
        )
        logger.info(
            f"Starting worker in container with prefix {custom_container_cmd_str}"
        )
        context.py_executable = custom_container_cmd_str
        return

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

    extra_env_keys = os.getenv("RAY_PODMAN_EXTRA_ENV_KEYS", "")
    if extra_env_keys:
        for key in (k.strip() for k in extra_env_keys.split(",")):
            if key and key in os.environ:
                env_vars[key] = os.environ[key]

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

        _modify_container_context_impl(
            runtime_env=runtime_env,
            context=context,
            ray_tmp_dir=self._ray_tmp_dir,
            worker_path=runtime_env.py_container_worker_path(),
            logger=logger,
        )
