import hashlib
import json
import logging
import os
import platform
import runpy
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from filelock import FileLock

import ray
from ray._private.runtime_env.conda_utils import (
    create_conda_env_if_needed,
    delete_conda_env,
    get_conda_activate_commands,
)
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import Protocol, parse_uri
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import (
    get_directory_size_bytes,
    get_master_wheel_url,
    get_or_create_event_loop,
    get_release_wheel_url,
    get_wheel_filename,
    try_to_create_directory,
)

default_logger = logging.getLogger(__name__)

_WIN32 = os.name == "nt"


def _resolve_current_ray_path() -> str:
    # When ray is built from source with pip install -e,
    # ray.__file__ returns .../python/ray/__init__.py and this function returns
    # ".../python".
    # When ray is installed from a prebuilt binary, ray.__file__ returns
    # .../site-packages/ray/__init__.py and this function returns
    # ".../site-packages".
    return os.path.split(os.path.split(ray.__file__)[0])[0]


def _get_ray_setup_spec():
    """Find the Ray setup_spec from the currently running Ray.

    This function works even when Ray is built from source with pip install -e.
    """
    ray_source_python_path = _resolve_current_ray_path()
    setup_py_path = os.path.join(ray_source_python_path, "setup.py")
    return runpy.run_path(setup_py_path)["setup_spec"]


def _resolve_install_from_source_ray_dependencies():
    """Find the Ray dependencies when Ray is installed from source."""
    deps = (
        _get_ray_setup_spec().install_requires + _get_ray_setup_spec().extras["default"]
    )
    # Remove duplicates
    return list(set(deps))


def _inject_ray_to_conda_site(
    conda_path, logger: Optional[logging.Logger] = default_logger
):
    """Write the current Ray site package directory to a new site"""
    if _WIN32:
        python_binary = os.path.join(conda_path, "python")
    else:
        python_binary = os.path.join(conda_path, "bin/python")
    site_packages_path = (
        subprocess.check_output(
            [
                python_binary,
                "-c",
                "import sysconfig; print(sysconfig.get_paths()['purelib'])",
            ]
        )
        .decode()
        .strip()
    )

    ray_path = _resolve_current_ray_path()
    logger.warning(
        f"Injecting {ray_path} to environment site-packages {site_packages_path} "
        "because _inject_current_ray flag is on."
    )

    maybe_ray_dir = os.path.join(site_packages_path, "ray")
    if os.path.isdir(maybe_ray_dir):
        logger.warning(f"Replacing existing ray installation with {ray_path}")
        shutil.rmtree(maybe_ray_dir)

    # See usage of *.pth file at
    # https://docs.python.org/3/library/site.html
    with open(os.path.join(site_packages_path, "ray_shared.pth"), "w") as f:
        f.write(ray_path)


def _current_py_version():
    return ".".join(map(str, sys.version_info[:3]))  # like 3.6.10


def _is_m1_mac():
    return sys.platform == "darwin" and platform.machine() == "arm64"


def current_ray_pip_specifier(
    logger: Optional[logging.Logger] = default_logger,
) -> Optional[str]:
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
            Path(ray.__file__).resolve().parents[2], ".whl", get_wheel_filename()
        )
    elif ray.__commit__ == "{{RAY_COMMIT_SHA}}":
        # Running on a version built from source locally.
        if os.environ.get("RAY_RUNTIME_ENV_LOCAL_DEV_MODE") != "1":
            logger.warning(
                "Current Ray version could not be detected, most likely "
                "because you have manually built Ray from source.  To use "
                "runtime_env in this case, set the environment variable "
                "RAY_RUNTIME_ENV_LOCAL_DEV_MODE=1."
            )
        return None
    elif "dev" in ray.__version__:
        # Running on a nightly wheel.
        if _is_m1_mac():
            raise ValueError("Nightly wheels are not available for M1 Macs.")
        return get_master_wheel_url()
    else:
        if _is_m1_mac():
            # M1 Mac release wheels are currently not uploaded to AWS S3; they
            # are only available on PyPI.  So unfortunately, this codepath is
            # not end-to-end testable prior to the release going live on PyPI.
            return f"ray=={ray.__version__}"
        else:
            return get_release_wheel_url()


def inject_dependencies(
    conda_dict: Dict[Any, Any],
    py_version: str,
    pip_dependencies: Optional[List[str]] = None,
) -> Dict[Any, Any]:
    """Add Ray, Python and (optionally) extra pip dependencies to a conda dict.

    Args:
        conda_dict: A dict representing the JSON-serialized conda
            environment YAML file.  This dict will be modified and returned.
        py_version: A string representing a Python version to inject
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
        if isinstance(dep, dict) and dep.get("pip") and isinstance(dep["pip"], list):
            dep["pip"] = pip_dependencies + dep["pip"]
            found_pip_dict = True
            break
    if not found_pip_dict:
        deps.append({"pip": pip_dependencies})

    return conda_dict


def _get_conda_env_hash(conda_dict: Dict) -> str:
    # Set `sort_keys=True` so that different orderings yield the same hash.
    serialized_conda_spec = json.dumps(conda_dict, sort_keys=True)
    hash = hashlib.sha1(serialized_conda_spec.encode("utf-8")).hexdigest()
    return hash


def get_uri(runtime_env: Dict) -> Optional[str]:
    """Return `"conda://<hashed_dependencies>"`, or None if no GC required."""
    conda = runtime_env.get("conda")
    if conda is not None:
        if isinstance(conda, str):
            # User-preinstalled conda env.  We don't garbage collect these, so
            # we don't track them with URIs.
            uri = None
        elif isinstance(conda, dict):
            uri = "conda://" + _get_conda_env_hash(conda_dict=conda)
        else:
            raise TypeError(
                "conda field received by RuntimeEnvAgent must be "
                f"str or dict, not {type(conda).__name__}."
            )
    else:
        uri = None
    return uri


def _get_conda_dict_with_ray_inserted(
    runtime_env: "RuntimeEnv",  # noqa: F821
    logger: Optional[logging.Logger] = default_logger,
) -> Dict[str, Any]:
    """Returns the conda spec with the Ray and `python` dependency inserted."""
    conda_dict = json.loads(runtime_env.conda_config())
    assert conda_dict is not None

    ray_pip = current_ray_pip_specifier(logger=logger)
    if ray_pip:
        extra_pip_dependencies = [ray_pip, "ray[default]"]
    elif runtime_env.get_extension("_inject_current_ray"):
        extra_pip_dependencies = _resolve_install_from_source_ray_dependencies()
    else:
        extra_pip_dependencies = []
    conda_dict = inject_dependencies(
        conda_dict, _current_py_version(), extra_pip_dependencies
    )
    return conda_dict


class CondaPlugin(RuntimeEnvPlugin):

    name = "conda"

    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "conda")
        try_to_create_directory(self._resources_dir)

        # It is not safe for multiple processes to install conda envs
        # concurrently, even if the envs are different, so use a global
        # lock for all conda installs and deletions.
        # See https://github.com/ray-project/ray/issues/17086
        self._installs_and_deletions_file_lock = os.path.join(
            self._resources_dir, "ray-conda-installs-and-deletions.lock"
        )

    def _get_path_from_hash(self, hash: str) -> str:
        """Generate a path from the hash of a conda or pip spec.

        The output path also functions as the name of the conda environment
        when using the `--prefix` option to `conda create` and `conda remove`.

        Example output:
            /tmp/ray/session_2021-11-03_16-33-59_356303_41018/runtime_resources
                /conda/ray-9a7972c3a75f55e976e620484f58410c920db091
        """
        return os.path.join(self._resources_dir, hash)

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        """Return the conda URI from the RuntimeEnv if it exists, else return []."""
        conda_uri = runtime_env.conda_uri()
        if conda_uri:
            return [conda_uri]
        return []

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info(f"Got request to delete URI {uri}")
        protocol, hash = parse_uri(uri)
        if protocol != Protocol.CONDA:
            raise ValueError(
                "CondaPlugin can only delete URIs with protocol "
                f"conda.  Received protocol {protocol}, URI {uri}"
            )

        conda_env_path = self._get_path_from_hash(hash)
        local_dir_size = get_directory_size_bytes(conda_env_path)

        with FileLock(self._installs_and_deletions_file_lock):
            successful = delete_conda_env(prefix=conda_env_path, logger=logger)
        if not successful:
            logger.warning(f"Error when deleting conda env {conda_env_path}. ")
            return 0

        return local_dir_size

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        if uri is None:
            # The "conda" field is the name of an existing conda env, so no
            # need to create one.
            # TODO(architkulkarni): Try "conda activate" here to see if the
            # env exists, and raise an exception if it doesn't.
            return 0

        # Currently create method is still a sync process, to avoid blocking
        # the loop, need to run this function in another thread.
        # TODO(Catch-Bull): Refactor method create into an async process, and
        # make this method running in current loop.
        def _create():
            logger.debug(
                "Setting up conda for runtime_env: " f"{runtime_env.serialize()}"
            )
            protocol, hash = parse_uri(uri)
            conda_env_name = self._get_path_from_hash(hash)

            conda_dict = _get_conda_dict_with_ray_inserted(runtime_env, logger=logger)

            logger.info(f"Setting up conda environment with {runtime_env}")
            with FileLock(self._installs_and_deletions_file_lock):
                try:
                    conda_yaml_file = os.path.join(
                        self._resources_dir, "environment.yml"
                    )
                    with open(conda_yaml_file, "w") as file:
                        yaml.dump(conda_dict, file)
                    create_conda_env_if_needed(
                        conda_yaml_file, prefix=conda_env_name, logger=logger
                    )
                finally:
                    os.remove(conda_yaml_file)

                if runtime_env.get_extension("_inject_current_ray"):
                    _inject_ray_to_conda_site(conda_path=conda_env_name, logger=logger)
            logger.info(f"Finished creating conda environment at {conda_env_name}")
            return get_directory_size_bytes(conda_env_name)

        loop = get_or_create_event_loop()
        return await loop.run_in_executor(None, _create)

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.has_conda():
            return

        if runtime_env.conda_env_name():
            conda_env_name = runtime_env.conda_env_name()
        else:
            protocol, hash = parse_uri(runtime_env.conda_uri())
            conda_env_name = self._get_path_from_hash(hash)
        context.py_executable = "python"
        context.command_prefix += get_conda_activate_commands(conda_env_name)
