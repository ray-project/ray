import os
import sys
import argparse
import json
import logging
import yaml
import hashlib

from filelock import FileLock
from typing import Optional, List, Dict, Any
from pathlib import Path

import ray
from ray._private.conda import (get_conda_activate_commands,
                                get_or_create_conda_env)
from ray._private.utils import try_to_create_directory
from ray.test_utils import (get_wheel_filename, get_master_wheel_url,
                            get_release_wheel_url)
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

    if runtime_env.get("conda") or runtime_env.get("pip"):
        conda_dict = get_conda_dict(runtime_env, args.session_dir)
        py_executable = "python"
        if isinstance(runtime_env.get("conda"), str):
            conda_env_name = runtime_env["conda"]
        else:
            assert conda_dict is not None
            py_version = ".".join(map(str,
                                      sys.version_info[:3]))  # like 3.6.10
            ray_pip = current_ray_pip_specifier()
            if ray_pip and not runtime_env.get("_skip_inject_ray"):
                extra_pip_dependencies = [ray_pip, "ray[default]"]
            else:
                extra_pip_dependencies = []
            conda_dict = inject_dependencies(conda_dict, py_version,
                                             extra_pip_dependencies)
            # Locking to avoid multiple processes installing concurrently
            conda_hash = hashlib.sha1(
                json.dumps(conda_dict,
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

    commands += [" ".join([f"exec {py_executable}"] + remaining_args)]
    command_separator = " && "
    command_str = command_separator.join(commands)

    if runtime_env.get("env_vars"):
        env_vars = runtime_env["env_vars"]
        os.environ.update(env_vars)

    os.execvp("bash", ["bash", "-c", command_str])


def get_conda_dict(runtime_env, session_dir) -> Optional[Dict[Any, Any]]:
    """ Construct a conda dependencies dict from a runtime env.

        This function does not inject Ray or Python into the conda dict.
        If the runtime env does not specify pip or conda, or if it specifies
        the name of a preinstalled conda environment, this function returns
        None.  If pip is specified, a conda dict is created containing the
        pip dependencies.  If conda is already given as a dict, this function
        is the identity function.
    """
    if runtime_env.get("conda"):
        if isinstance(runtime_env["conda"], dict):
            return runtime_env["conda"]
        else:
            return None
    if runtime_env.get("pip"):
        requirements_txt = runtime_env["pip"]
        pip_hash = hashlib.sha1(requirements_txt.encode("utf-8")).hexdigest()
        pip_hash_str = f"pip-generated-{pip_hash}"

        conda_dir = os.path.join(session_dir, "runtime_resources", "conda")
        requirements_txt_path = os.path.join(
            conda_dir, f"requirements-{pip_hash_str}.txt")
        conda_dict = {
            "name": pip_hash_str,
            "dependencies": ["pip", {
                "pip": [f"-r {requirements_txt_path}"]
            }]
        }
        file_lock_name = f"ray-{pip_hash_str}.lock"
        with FileLock(os.path.join(session_dir, file_lock_name)):
            try_to_create_directory(conda_dir)
            with open(requirements_txt_path, "w") as file:
                file.write(requirements_txt)
        return conda_dict
    return None


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
        logger.warning(
            "Current Ray version could not be detected, most likely "
            "because you are using a version of Ray "
            "built from source.  If you wish to use runtime_env, "
            "you can try building a wheel and including the wheel "
            "explicitly as a pip dependency.")
        return None
    elif "dev" in ray.__version__:
        # Running on a nightly wheel.
        return get_master_wheel_url()
    else:
        return get_release_wheel_url()


def inject_dependencies(
        conda_dict: Dict[Any, Any],
        py_version: str,
        pip_dependencies: Optional[List[str]] = None) -> Dict[Any, Any]:
    """Add Ray, Python and (optionally) extra pip dependencies to a conda dict.

    Args:
        conda_dict (dict): A dict representing the JSON-serialized conda
            environment YAML file.  This dict will be modified and returned.
        py_version (str): A string representing a Python version to inject
            into the conda dependencies, e.g. "3.7.7"
        pip_dependencies (List[str]): A list of pip dependencies that
            will be prepended to the list of pip dependencies in
            the conda dict.  If the conda dict does not already have a "pip"
            field, one will be created.
    Returns:
        The modified dict.  (Note: the input argument conda_dict is modified
        and returned.)
    """
    if pip_dependencies is None:
        pip_dependencies = []
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

    # Insert pip dependencies.
    found_pip_dict = False
    for dep in deps:
        if isinstance(dep, dict) and dep.get("pip") and isinstance(
                dep["pip"], list):
            dep["pip"] = pip_dependencies + dep["pip"]
            found_pip_dict = True
            break
    if not found_pip_dict:
        deps.append({"pip": pip_dependencies})

    return conda_dict
