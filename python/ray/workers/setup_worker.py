import argparse
import json
import logging
import os

from ray._private.utils import import_attr

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description=(
        "Set up the environment for a Ray worker and launch the worker."))

parser.add_argument(
    "--worker-setup-hook",
    type=str,
    help="the module path to a Python function to run to set up the "
    "environment for a worker and launch the worker.")

parser.add_argument(
    "--serialized-runtime-env",
    type=str,
    help="the serialized parsed runtime env dict")

parser.add_argument(
    "--allocated-instances-serialized-json",
    type=str,
    help="the worker allocated resource")


def get_tmp_dir(remaining_args):
    for arg in remaining_args:
        if arg.startswith("--temp-dir="):
            return arg[11:]
    return None


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
            container_resource_args.append("--cpu-shares=" +
                                           str(int(cpu_shares / 10000 * 1024)))
            container_resource_args.append("--cpuset-cpus=" + ",".join(
                str(e) for e in cpu_ids))
        else:
            # cpushare
            container_resource_args.append(
                "--cpu-shares=" + str(int(cpu_resource / 10000 * 1024)))
    if "memory" in allocated_resource.keys():
        container_resource_args.append(
            "--memory=" + str(int(allocated_resource["memory"] / 10000)))
    return container_resource_args


def start_worker_in_container(container_option, args, remaining_args):
    worker_setup_hook = args.worker_setup_hook
    last_period_idx = worker_setup_hook.rfind(".")
    module_name = worker_setup_hook[:last_period_idx]
    # python -m ray.workers.setup_runtime_env --session-dir=
    # default_worker.py --node-ip-address= ...
    entrypoint_args = ["-m"]
    entrypoint_args.append(module_name)
    # replace default_worker.py path
    if container_option.get("worker_path"):
        remaining_args[1] = container_option.get("worker_path")
    entrypoint_args.extend(remaining_args)
    # setup_runtime_env will install conda,pip according to
    # serialized-runtime-env, so add this argument
    entrypoint_args.append("--serialized-runtime-env")
    entrypoint_args.append(args.serialized_runtime_env or "{}")
    # now we will start a container, add argument worker-shim-pid
    entrypoint_args.append("--worker-shim-pid={}".format(os.getpid()))

    tmp_dir = get_tmp_dir(remaining_args)
    if not tmp_dir:
        logger.error(
            "failed to get tmp_dir, the args: {}".format(remaining_args))

    container_driver = "podman"
    # todo add cgroup config
    # todo flag "--rm"
    container_command = [
        container_driver, "run", "-v", tmp_dir + ":" + tmp_dir,
        "--cgroup-manager=cgroupfs", "--network=host", "--pid=host",
        "--ipc=host", "--env-host"
    ]
    container_command.append("--env")
    container_command.append("RAY_RAYLET_PID=" + str(os.getppid()))
    if container_option.get("run_options"):
        container_command.extend(container_option.get("run_options"))
    container_command.extend(
        parse_allocated_resource(args.allocated_instances_serialized_json))

    container_command.append("--entrypoint")
    container_command.append("python")
    container_command.append(container_option.get("image"))
    container_command.extend(entrypoint_args)
    logger.warning("start worker in container: {}".format(container_command))
    os.execvp(container_driver, container_command)


if __name__ == "__main__":
    args, remaining_args = parser.parse_known_args()
    runtime_env: dict = json.loads(args.serialized_runtime_env or "{}")
    container_option = runtime_env.get("container")
    if container_option and container_option.get("image"):
        start_worker_in_container(container_option, args, remaining_args)
    else:
        remaining_args.append("--serialized-runtime-env")
        remaining_args.append(args.serialized_runtime_env or "{}")
        setup = import_attr(args.worker_setup_hook)
        setup(remaining_args)
