import os
import argparse
import json

from ray._private.conda import get_conda_activate_commands
from ray._private.runtime_env import RuntimeEnvDict
parser = argparse.ArgumentParser()

parser.add_argument(
    "--serialized-runtime-env",
    type=str,
    help="the serialized parsed runtime env dict")


def setup(input_args):
    # remaining_args contains the arguments to the original worker command,
    # minus the python executable, e.g. default_worker.py --node-ip-address=...
    args, remaining_args = parser.parse_known_args(args=input_args)

    commands = []
    runtime_env: RuntimeEnvDict = json.loads(args.serialized_runtime_env
                                             or "{}")
    if runtime_env.get("conda_env_name"):
        commands += get_conda_activate_commands(runtime_env["conda_env_name"])

    commands += [" ".join(["exec python"] + remaining_args)]
    command_separator = " && "
    command_str = command_separator.join(commands)

    os.execvp("bash", ["bash", "-c", command_str])
