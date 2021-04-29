import os
import argparse

from ray._private.conda import get_conda_activate_commands

parser = argparse.ArgumentParser()

parser.add_argument(
    "--conda-env-name",
    type=str,
    help="the name of an existing conda env to activate")


def setup(input_args):
    # remaining_args contains the arguments to the original worker command,
    # minus the python executable, e.g. default_worker.py --node-ip-address=...
    args, remaining_args = parser.parse_known_args(args=input_args)

    commands = []

    if args.conda_env_name:
        commands += get_conda_activate_commands(args.conda_env_name)

    commands += [" ".join(["exec python"] + remaining_args)]
    command_separator = " && "
    command_str = command_separator.join(commands)

    os.execvp("bash", ["bash", "-c", command_str])
