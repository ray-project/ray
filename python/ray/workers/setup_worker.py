import argparse
import json
import os

from ray._private.utils import import_attr
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

runtime_env: dict = json.loads(args.serialized_runtime_env or "{}")
container_option = runtime_env.get("container_option")
container_image_option = container_option.get("image")
if container_option and container_image_option:
    worker_setup_hook = args.worker_setup_hook
    last_period_idx = worker_setup_hook.rfind(".")
    module_name = worker_setup_hook[:last_period_idx]
    #python -m ray.workers.setup_runtime_env --session-dir=
    # default_worker.py --node-ip-address= ...
    entrypoint_args = ["-m"]
    entrypoint_args.append(module_name)
    entrypoint_args.append(remaining_args)
    # setup_runtime_env will install conda,pip according to
    # serialized-runtime-env, so add this argument
    entrypoint_args.append("--serialized-runtime-env")
    entrypoint_args.append(args.serialized_runtime_env or "{}")

    tmp_dir = get_tmp_dir(remaining_args)

    #todo add cgroup config
    #todo add container options
    #todo RAYLET_PID
    #todo flag "--rm"
    #todo --log-opt ???
    container_command = ["podman", "-v", tmp_dir, ":",
                         tmp_dir, "--cgroup-manager=cgroupfs",
                         "--network=host", "--pid=host", "--ipc=host",
                         "--env-host", "--entrypoint", "python",
                         container_image_option, entrypoint_args]
    container_command_str = " ".join(container_command)
    os.execvp(container_command_str)
else:
    setup = import_attr(args.worker_setup_hook)

    setup(remaining_args)

def get_tmp_dir(remaining_args):
    found_tmp_dir_arg = False
    for arg in remaining_args:
        if found_tmp_dir_arg:
            return arg
        if arg == "--temp-dir":
            found_tmp_dir_arg = True
    return None

