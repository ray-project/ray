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


# allocated-instances-serialized-json
def parse_allocated_resource(allocated_instances_serialized_json):
    container_resource_args = []
    allocated_resource = json.loads(allocated_instances_serialized_json)
    if "CPU" in allocated_resource.keys():
        cpu_resource = allocated_resource["CPU"]
        if isinstance(cpu_resource, list):
            # cpuset: because we may split one cpu core into some pieces,
            # we need set cpuset.cpu_exclusive=0 and set cpuset-cpus
            cpu_ids = []
            cpu_shares = 0
            for idx, val in enumerate(cpu_resource):
                if val > 0:
                    cpu_ids.append(idx)
                    cpu_shares += val
            container_resource_args.append("--cpu-shares=" +
                                           str(int(cpu_shares / 10000 * 1024)))
            container_resource_args.append("--cpuset-cpus=" + ",".join(
                str(e) for e in cpu_ids))
        else:
            # cpushare
            container_resource_args.append(
                "--cpu-shares=" + str(int(cpu_resource / 10000 * 1024)))
    if "memory" in allocated_resource.keys():
        container_resource_args.append(
            "--memory=" + str(int(allocated_resource["memory"] / 10000)))
    return container_resource_args


def get_tmp_dir(remaining_args):
    for arg in remaining_args:
        if arg.startswith("--temp-dir="):
            return arg[11:]
    return None


class ContainerManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = resources_dir

    def setup(self,
              runtime_env: dict,
              context: RuntimeEnvContext,
              logger: Optional[logging.Logger] = default_logger):
        container_option = runtime_env.get("container")
        if not container_option and not container_option.get("image"):
            return

        # python -m ray.workers.default_worker --session-dir=
        # default_worker.py --node-ip-address= ...
        entrypoint_args = ["-m"]
        entrypoint_args.append(module_name)
        # replace default_worker.py path
        if container_option.get("worker_path"):
            entrypoint_args.extend(container_option.get("worker_path"))
        else:
            entrypoint_args.extend(remaining_args)
        # now we will start a container, add argument worker-shim-pid

        tmp_dir = get_tmp_dir(remaining_args)
        if not tmp_dir:
            logger.error(
                "failed to get tmp_dir, the args: {}".format(remaining_args))

        container_driver = "podman"
        # todo add cgroup config
        # todo flag "--rm"
        container_command = [
            container_driver, "run", "-v", tmp_dir + ":" + tmp_dir,
            "--cgroup-manager=cgroupfs", "--network=host", "--pid=host",
            "--ipc=host", "--env-host"
        ]
        container_command.append("--env")
        container_command.append("RAY_RAYLET_PID=" + os.getenv("RAY_RAYLET_PID"))
        if container_option.get("run_options"):
            container_command.extend(container_option.get("run_options"))
        # TODO(chenk008): add resource limit

        container_command.append("--entrypoint")
        container_command.append("bash")
        container_command.append(container_option.get("image"))
        logger.warning("start worker in container: {}".format(" ".join(container_command)))