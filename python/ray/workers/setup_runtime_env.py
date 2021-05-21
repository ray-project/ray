import os
import sys
import argparse
import json
import logging
import yaml
import hashlib

from filelock import FileLock

from ray._private.conda import (get_conda_activate_commands,
                                get_or_create_conda_env)
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
    runtime_env: dict = json.loads(args.serialized_runtime_env or "{}")

    py_executable: str = sys.executable

    if runtime_env.get("conda"):
        py_executable = "python"
        if isinstance(runtime_env["conda"], str):
            commands += get_conda_activate_commands(runtime_env["conda"])
        elif isinstance(runtime_env["conda"], dict):
            # Locking to avoid multiple processes installing concurrently
            conda_hash = hashlib.sha1(
                json.dumps(runtime_env["conda"],
                           sort_keys=True).encode("utf-8")).hexdigest()
            conda_hash_str = f"conda-generated-{conda_hash}"
            file_lock_name = f"ray-{conda_hash_str}.lock"
            with FileLock(os.path.join(args.session_dir, file_lock_name)):
                conda_dir = os.path.join(args.session_dir, "runtime_resources",
                                         "conda")
                try_to_create_directory(conda_dir)
                conda_yaml_path = os.path.join(conda_dir, "environment.yml")
                with open(conda_yaml_path, "w") as file:
                    # Sort keys because we hash based on the file contents,
                    # and we don't want the hash to depend on the order
                    # of the dependencies.
                    yaml.dump(runtime_env["conda"], file, sort_keys=True)
                conda_env_name = get_or_create_conda_env(
                    conda_yaml_path, conda_dir)
            commands += get_conda_activate_commands(conda_env_name)
    elif runtime_env.get("pip"):
        # Install pip requirements into an empty conda env.
        py_executable = "python"
        requirements_txt = runtime_env["pip"]
        pip_hash = hashlib.sha1(requirements_txt.encode("utf-8")).hexdigest()
        pip_hash_str = f"pip-generated-{pip_hash}"

        conda_dir = os.path.join(args.session_dir, "runtime_resources",
                                 "conda")
        requirements_txt_path = os.path.join(
            conda_dir, f"requirements-{pip_hash_str}.txt")

        py_version = ".".join(map(str, sys.version_info[:3]))  # E.g. 3.6.13
        conda_dict = {
            "name": pip_hash_str,
            "dependencies": [
                f"python={py_version}", "pip", {
                    "pip": [f"-r {requirements_txt_path}"]
                }
            ]
        }

        file_lock_name = f"ray-{pip_hash_str}.lock"
        with FileLock(os.path.join(args.session_dir, file_lock_name)):
            try_to_create_directory(conda_dir)
            conda_yaml_path = os.path.join(conda_dir,
                                           f"env-{pip_hash_str}.yml")
            with open(conda_yaml_path, "w") as file:
                yaml.dump(conda_dict, file, sort_keys=True)

            with open(requirements_txt_path, "w") as file:
                file.write(requirements_txt)

            conda_env_name = get_or_create_conda_env(conda_yaml_path,
                                                     conda_dir)

        commands += get_conda_activate_commands(conda_env_name)

    commands += [" ".join([f"exec {py_executable}"] + remaining_args)]
    command_separator = " && "
    command_str = command_separator.join(commands)
    os.execvp("bash", ["bash", "-c", command_str])
