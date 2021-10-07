import os
import sys
import logging
import yaml
import hashlib
import subprocess
import runpy
import shutil

from filelock import FileLock
from typing import Optional, List, Dict, Any
from pathlib import Path

import ray
from ray._private.runtime_env import RuntimeEnvContext
from ray._private.runtime_env.conda_utils import (get_conda_activate_commands,
                                                  get_or_create_conda_env)
from ray._private.utils import (get_wheel_filename, get_master_wheel_url,
                                get_release_wheel_url, try_to_create_directory)

default_logger = logging.getLogger(__name__)


def _resolve_current_ray_path():
    # When ray is built from source with pip install -e,
    # ray.__file__ returns .../python/ray/__init__.py.
    # When ray is installed from a prebuilt binary, it returns
    # .../site-packages/ray/__init__.py
    return os.path.split(os.path.split(ray.__file__)[0])[0]


def _resolve_install_from_source_ray_dependencies():
    """Find the ray dependencies when Ray is install from source"""
    ray_source_python_path = _resolve_current_ray_path()
    setup_py_path = os.path.join(ray_source_python_path, "setup.py")
    ray_install_requires = runpy.run_path(setup_py_path)[
        "setup_spec"].install_requires
    return ray_install_requires


def _inject_ray_to_conda_site(
        conda_path, logger: Optional[logging.Logger] = default_logger):
    """Write the current Ray site package directory to a new site"""
    python_binary = os.path.join(conda_path, "bin/python")
    site_packages_path = subprocess.check_output(
        [python_binary, "-c",
         "import site; print(site.getsitepackages()[0])"]).decode().strip()

    ray_path = _resolve_current_ray_path()
    logger.warning(f"Injecting {ray_path} to environment {conda_path} "
                   "because _inject_current_ray flag is on.")

    maybe_ray_dir = os.path.join(site_packages_path, "ray")
    if os.path.isdir(maybe_ray_dir):
        logger.warning(f"Replacing existing ray installation with {ray_path}")
        shutil.rmtree(maybe_ray_dir)

    # See usage of *.pth file at
    # https://docs.python.org/3/library/site.html
    with open(os.path.join(site_packages_path, "ray.pth"), "w") as f:
        f.write(ray_path)


def _current_py_version():
    return ".".join(map(str, sys.version_info[:3]))  # like 3.6.10


def get_conda_dict(runtime_env, runtime_env_dir) -> Optional[Dict[Any, Any]]:
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

        conda_dir = os.path.join(runtime_env_dir, "conda")
        requirements_txt_path = os.path.join(
            conda_dir, f"requirements-{pip_hash_str}.txt")
        conda_dict = {
            "name": pip_hash_str,
            "dependencies": ["pip", {
                "pip": [f"-r {requirements_txt_path}"]
            }]
        }
        file_lock_name = f"ray-{pip_hash_str}.lock"
        with FileLock(os.path.join(runtime_env_dir, file_lock_name)):
            try_to_create_directory(conda_dir)
            with open(requirements_txt_path, "w") as file:
                file.write(requirements_txt)
        return conda_dict
    return None


def current_ray_pip_specifier(
        logger: Optional[logging.Logger] = default_logger) -> Optional[str]:
    """The pip requirement specifier for the running version of Ray.

    Returns:
        A string which can be passed to `pip install` to install the
        currently running Ray version, or None if running on a version
        built from source locally (likely if you are developing Ray).

    Examples:
        Returns "https://s3-us-west-2.amazonaws.com/ray-wheels/[..].whl"
            if running a stable release, a nightly or a specific commit
    """
    if os.environ.get("RAY_CI_POST_WHEEL_TESTS"):
        # Running in Buildkite CI after the wheel has been built.
        # Wheels are at in the ray/.whl directory, but use relative path to
        # allow for testing locally if needed.
        return os.path.join(
            Path(ray.__file__).resolve().parents[2], ".whl",
            get_wheel_filename())
    elif ray.__commit__ == "{{RAY_COMMIT_SHA}}":
        # Running on a version built from source locally.
        if os.environ.get("RAY_RUNTIME_ENV_LOCAL_DEV_MODE") != "1":
            logger.warning(
                "Current Ray version could not be detected, most likely "
                "because you have manually built Ray from source.  To use "
                "runtime_env in this case, set the environment variable "
                "RAY_RUNTIME_ENV_LOCAL_DEV_MODE=1.")
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


class CondaManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = resources_dir

    def setup(self,
              runtime_env: dict,
              context: RuntimeEnvContext,
              logger: Optional[logging.Logger] = default_logger):
        if not runtime_env.get("conda") and not runtime_env.get("pip"):
            return

        logger.debug(f"Setting up conda or pip for runtime_env: {runtime_env}")
        conda_dict = get_conda_dict(runtime_env, self._resources_dir)
        if isinstance(runtime_env.get("conda"), str):
            conda_env_name = runtime_env["conda"]
        else:
            assert conda_dict is not None
            ray_pip = current_ray_pip_specifier(logger=logger)
            if ray_pip:
                extra_pip_dependencies = [ray_pip, "ray[default]"]
            elif runtime_env.get("_inject_current_ray"):
                extra_pip_dependencies = (
                    _resolve_install_from_source_ray_dependencies())
            else:
                extra_pip_dependencies = []
            conda_dict = inject_dependencies(conda_dict, _current_py_version(),
                                             extra_pip_dependencies)
            logger.info(f"Setting up conda environment with {runtime_env}")
            # It is not safe for multiple processes to install conda envs
            # concurrently, even if the envs are different, so use a global
            # lock for all conda installs.
            # See https://github.com/ray-project/ray/issues/17086
            file_lock_name = "ray-conda-install.lock"
            with FileLock(os.path.join(self._resources_dir, file_lock_name)):
                conda_dir = os.path.join(self._resources_dir, "conda")
                try_to_create_directory(conda_dir)
                conda_yaml_path = os.path.join(conda_dir, "environment.yml")
                with open(conda_yaml_path, "w") as file:
                    # Sort keys because we hash based on the file contents,
                    # and we don't want the hash to depend on the order
                    # of the dependencies.
                    yaml.dump(conda_dict, file, sort_keys=True)
                conda_env_name = get_or_create_conda_env(
                    conda_yaml_path, conda_dir, logger=logger)

            if runtime_env.get("_inject_current_ray"):
                conda_path = os.path.join(conda_dir, conda_env_name)
                _inject_ray_to_conda_site(conda_path, logger=logger)

        context.py_executable = "python"
        context.command_prefix += get_conda_activate_commands(conda_env_name)
        logger.info(
            f"Finished setting up runtime environment at {conda_env_name}")
