import os
import sys
import argparse
import json
import logging
import yaml
import hashlib
import subprocess

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
            with FileLock(
                    os.path.join(args.session_dir, "ray-conda-install.lock")):
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
                if os.path.exists(conda_yaml_path):
                    os.remove(conda_yaml_path)
            commands += get_conda_activate_commands(conda_env_name)
    elif runtime_env.get("pip"):
        # Install pip requirements into an empty conda env.
        py_executable = "python"
        requirements_txt = runtime_env["pip"]

        pip_contents_hash = "pip-%s" % hashlib.sha1(
            requirements_txt.encode("utf-8")).hexdigest()
        # E.g. 3.6.13
        py_version = ".".join(map(str, sys.version_info[:3]))
        conda_dict = {
            "name": pip_contents_hash,
            "dependencies": [f"python={py_version}", "pip"]
        }
        with FileLock(
                os.path.join(args.session_dir, "ray-conda-install.lock")):
            conda_dir = os.path.join(args.session_dir, "runtime_resources",
                                     "conda")
            try_to_create_directory(conda_dir)
            conda_yaml_path = os.path.join(conda_dir,
                                           f"env-{pip_contents_hash}.yml")
            with open(conda_yaml_path, "w") as file:
                yaml.dump(conda_dict, file, sort_keys=True)
            conda_env_name = get_or_create_conda_env(conda_yaml_path,
                                                     conda_dir)
            if os.path.exists(conda_yaml_path):
                os.remove(conda_yaml_path)
            requirements_txt_path = os.path.join(
                conda_dir, f"requirements-{pip_contents_hash}.txt")
            with open(requirements_txt_path, "w") as file:
                file.write(requirements_txt)

            # In a subprocess, activate the conda env and install pip packages.

            pip_install_commands = []
            pip_install_commands += get_conda_activate_commands(conda_env_name)
            pip_install_commands += [f"pip install -r {requirements_txt_path}"]
            pip_install_command_str = " && ".join(pip_install_commands)

            subprocess.run(pip_install_command_str, shell=True, check=True)
            logger.info("Successfully installed pip packages.")

        commands += get_conda_activate_commands(conda_env_name)

    commands += [" ".join([f"exec {py_executable}"] + remaining_args)]
    command_separator = " && "
    command_str = command_separator.join(commands)
    os.execvp("bash", ["bash", "-c", command_str])
