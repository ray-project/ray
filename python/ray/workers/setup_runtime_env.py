import os
import sys
import argparse
import json
import logging
import yaml

from filelock import FileLock

from ray._private.conda import (get_conda_activate_commands,
                                get_or_create_conda_env)
from ray._private.runtime_env import RuntimeEnvDict
from ray._private.utils import try_to_create_directory
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()

parser.add_argument(
    "--serialized-runtime-env",
    type=str,
    help="the serialized parsed runtime env dict")

# The worker is not set up yet, so we can't get session_dir from the worker.
parser.add_argument(
    "--session-dir", type=str, help="the directory for the current session")


def setup(input_args):
    # remaining_args contains the arguments to the original worker command,
    # minus the python executable, e.g. default_worker.py --node-ip-address=...
    args, remaining_args = parser.parse_known_args(args=input_args)

    commands = []
    runtime_env: RuntimeEnvDict = json.loads(args.serialized_runtime_env
                                             or "{}")

    py_executable: str = sys.executable

    if runtime_env.get("conda"):
        py_executable = "python"
        if isinstance(runtime_env["conda"], str):
            commands += get_conda_activate_commands(runtime_env["conda"])
        elif isinstance(runtime_env["conda"], dict):
            # Locking to avoid multiple processes installing concurrently
            with FileLock(
                    os.path.join(args.session_dir, "ray-conda-install.lock")):
                conda_dir = os.path.join(args.session_dir, "runtime_resources",
                                         "conda")
                try_to_create_directory(conda_dir)
                conda_yaml_path = os.path.join(conda_dir, "environment.yml")
                with open(conda_yaml_path, "w") as file:
                    yaml.dump(runtime_env["conda"], file)
                conda_env_name = get_or_create_conda_env(
                    conda_yaml_path, conda_dir)
                if os.path.exists(conda_yaml_path):
                    os.remove(conda_yaml_path)
                # print("sleeping")
                # time.sleep(30)
                logger.error(conda_env_name)
                commands += get_conda_activate_commands(conda_env_name)

    commands += [" ".join([f"exec {py_executable}"] + remaining_args)]
    command_separator = " && "
    command_str = command_separator.join(commands)
    os.execvp("bash", ["bash", "-c", command_str])
