import os
import sys
import argparse
import json
import logging
import yaml
import hashlib

from filelock import FileLock
from typing import Optional
from pathlib import Path

import ray
from ray._private.conda import (get_conda_activate_commands,
                                get_or_create_conda_env)
from ray._private.utils import try_to_create_directory
from ray.test_utils import get_wheel_filename, get_master_wheel_url
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
            py_version = ".".join(map(str,
                                      sys.version_info[:3]))  # like 3.6.10
            conda_dict = inject_ray_and_python(runtime_env["conda"],
                                               current_ray_pip_specifier(),
                                               py_version)
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
                    yaml.dump(conda_dict, file, sort_keys=True)
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
            "dependencies": ["pip", {
                "pip": [f"-r {requirements_txt_path}"]
            }]
        }

        conda_dict = inject_ray_and_python(conda_dict,
                                           current_ray_pip_specifier(),
                                           py_version)

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


def current_ray_pip_specifier() -> Optional[str]:
    """The pip requirement specifier for the running version of Ray.

    Returns:
        A string which can be passed to `pip install` to install the
        currently running Ray version, or None if running on a version
        built from source locally (likely if you are developing Ray).

    Examples:
        Returns "ray[all]==1.4.0" if running the stable release
        Returns "https://s3-us-west-2.amazonaws.com/ray-wheels/master/[..].whl"
            if running the nightly or a specific commit
    """
    if os.environ.get("RAY_CI_POST_WHEEL_TESTS"):
        # Running in Buildkite CI after the wheel has been built.
        # Wheels are at in the ray/.whl directory, and the present file is
        # at ray/python/ray/workers.  Use relative paths to allow for
        # testing locally if needed.
        return os.path.join(
            Path(__file__).resolve().parents[3], ".whl", get_wheel_filename())
    elif ray.__commit__ == "{{RAY_COMMIT_SHA}}":
        # Running on a version built from source locally.
        return None
    elif "dev" in ray.__version__:
        # Running on a nightly wheel.
        return get_master_wheel_url()
    else:
        return f"ray[all]=={ray.__version__}"


def inject_ray_and_python(conda_dict, ray_pip_specifier: Optional[str],
                          py_version: str) -> None:
    if conda_dict.get("dependencies") is None:
        conda_dict["dependencies"] = []

    # Inject Python dependency.
    deps = conda_dict["dependencies"]

    # Add current python dependency.  If the user has already included a
    # python version dependency, conda will raise a readable error if the two
    # are incompatible, e.g:
    #   ResolvePackageNotFound: - python[version='3.5.*,>=3.6']
    deps.append(f"python={py_version}")

    if "pip" not in deps:
        deps.append("pip")

    # Insert Ray dependency. If the user has already included Ray, conda
    # will raise an error only if the two are incompatible.

    if ray_pip_specifier is not None:
        found_pip_dict = False
        for dep in deps:
            if isinstance(dep, dict) and dep.get("pip"):
                dep["pip"].append(ray_pip_specifier)
                found_pip_dict = True
                break
        if not found_pip_dict:
            deps.append({"pip": [ray_pip_specifier]})
    else:
        logger.warning("Current Ray version could not be inserted "
                       "into conda's pip dependencies, most likely "
                       "because you are using a version of Ray "
                       "built from source.  If so, you can try "
                       "building a wheel and including the wheel "
                       "as a dependency.")

    return conda_dict
