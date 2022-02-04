import json
import os
import logging

from typing import Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.utils import RuntimeEnv

default_logger = logging.getLogger(__name__)


# NOTE(chenk008): it is moved from setup_worker. And it will be used
# to setup resource limit.
def parse_allocated_resource(allocated_instances_serialized_json):
    container_resource_args = []
    allocated_resource = json.loads(allocated_instances_serialized_json)
    if "CPU" in allocated_resource.keys():
        cpu_resource = allocated_resource["CPU"]
        if isinstance(cpu_resource, list):
            # cpuset: because we may split one cpu core into some pieces,
            # we need set cpuset.cpu_exclusive=0 and set cpuset-cpus
            cpu_ids = []
            cpu_shares = 0
            for idx, val in enumerate(cpu_resource):
                if val > 0:
                    cpu_ids.append(idx)
                    cpu_shares += val
            container_resource_args.append(
                "--cpu-shares=" + str(int(cpu_shares / 10000 * 1024))
            )
            container_resource_args.append(
                "--cpuset-cpus=" + ",".join(str(e) for e in cpu_ids)
            )
        else:
            # cpushare
            container_resource_args.append(
                "--cpu-shares=" + str(int(cpu_resource / 10000 * 1024))
            )
    if "memory" in allocated_resource.keys():
        container_resource_args.append(
            "--memory=" + str(int(allocated_resource["memory"] / 10000))
        )
    return container_resource_args


class ContainerManager:
    def __init__(self, tmp_dir: str):
        # _ray_tmp_dir will be mounted into container, so the worker process
        # can connect to raylet.
        self._ray_tmp_dir = tmp_dir

    def setup(
        self,
        runtime_env: RuntimeEnv,
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
