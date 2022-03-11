import os
import logging

from typing import Optional

from ray._private.runtime_env.context import RuntimeEnvContext

default_logger = logging.getLogger(__name__)


class ContainerManager:
    def __init__(self, tmp_dir: str):
        # _ray_tmp_dir will be mounted into container, so the worker process
        # can connect to raylet.
        self._ray_tmp_dir = tmp_dir

    async def setup(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.has_py_container() or not runtime_env.py_container_image():
            return

        container_driver = "podman"
        container_command = [
            container_driver,
            "run",
            "-v",
            self._ray_tmp_dir + ":" + self._ray_tmp_dir,
            "--cgroup-manager=cgroupfs",
            "--network=host",
            "--pid=host",
            "--ipc=host",
            "--env-host",
        ]
        container_command.append("--env")
        container_command.append("RAY_RAYLET_PID=" + os.getenv("RAY_RAYLET_PID"))
        if runtime_env.py_container_run_options():
            container_command.extend(runtime_env.py_container_run_options())
        # TODO(chenk008): add resource limit
        container_command.append("--entrypoint")
        container_command.append("python")
        container_command.append(runtime_env.py_container_image())
        context.py_executable = " ".join(container_command)
        logger.info(
            "start worker in container with prefix: {}".format(context.py_executable)
        )
