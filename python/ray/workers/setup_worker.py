import argparse
import os

from ray._private.conda import (get_conda_activate_commands,
                                get_or_create_conda_env)

parser = argparse.ArgumentParser(
    description=(
        "Set up the environment for a Ray worker and launch the worker."))

parser.add_argument(
    "--conda-env-name",
    type=str,
    help="the name of an existing conda env to activate")

parser.add_argument(
    "--conda-yaml-path",
    type=str,
    help="the path to a conda environment yaml to install")

# remaining_args contains the arguments to the original worker command,
# minus the python executable, e.g. default_worker.py --node-ip-address=...
args, remaining_args = parser.parse_known_args()

# conda_env_path = "/fake/path/environment.yml"
commands = []

if args.conda_env_name:
    commands += get_conda_activate_commands(args.conda_env_name)
elif args.conda_yaml_path:
    conda_env_name = get_or_create_conda_env(args.conda_yaml_path)
    commands += get_conda_activate_commands(conda_env_name)

commands += [" ".join(["exec python"] + remaining_args)]
command_separator = " && "
command_str = command_separator.join(commands)

os.execvp("bash", ["bash", "-c", command_str])
