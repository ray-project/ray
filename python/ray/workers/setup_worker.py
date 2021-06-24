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

args, remaining_args = parser.parse_known_args()
# add worker-shim-pid argument
remaining_args.append("--worker-shim-pid={}".format(os.getpid()))


def get_tmp_dir(remaining_args):
    for arg in remaining_args:
        if arg.startswith("--temp-dir="):
            return arg[11:]
    return None


runtime_env: dict = json.loads(args.serialized_runtime_env or "{}")
container_option = runtime_env.get("container_option")
container_image_option = container_option.get("image")
if container_option and container_image_option:
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

    tmp_dir = get_tmp_dir(remaining_args)
    if not tmp_dir:
        logger.error(
            "failed to get tmp_dir, the args: {}".format(remaining_args))

    container_driver = "podman"
    # todo add cgroup config
    # todo add container options
    # todo RAYLET_PID
    # todo flag "--rm"
    # todo --log-opt ???
    container_command = [
        container_driver, "run", "--log-level=debug", "-v", tmp_dir + ":" + tmp_dir,
        "--cgroup-manager=cgroupfs", "--network=host", "--pid=host",
        "--ipc=host", "--env-host", "--entrypoint", "python"
    ]
    container_command.append(container_image_option)
    container_command.extend(entrypoint_args)
    logger.warning("start worker in container: {}".format(container_command))
    os.execvp(container_driver, container_command)
else:
    setup = import_attr(args.worker_setup_hook)

    setup(remaining_args)
