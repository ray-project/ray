import hashlib
import logging
import inspect

from filelock import FileLock
from pathlib import Path
from zipfile import ZipFile
from ray.job_config import JobConfig
from enum import Enum

from ray.experimental.internal_kv import (_internal_kv_put, _internal_kv_get,
                                          _internal_kv_exists)

from typing import List, Tuple
from types import ModuleType
from urllib.parse import urlparse
import os
import sys

from ray._private.utils import get_conda_env_dir

# We need to setup this variable before
# using this module
PKG_DIR = None

logger = logging.getLogger(__name__)

FILE_SIZE_WARNING = 10 * 1024 * 1024  # 10MB
FILE_SIZE_LIMIT = 50 * 1024 * 1024  # 50MB


class RuntimeEnvDict:
    """Parses and validates the runtime env dictionary from the user.

    Attributes:
        working_dir (Path): Specifies the working directory of the worker.
            This can either be a local directory or zip file.
            Examples:
                "."  # cwd
                "local_project.zip"  # archive is unpacked into directory
        py_modules (List[Path]): Similar to working_dir, but specifies python
            modules to add to the `sys.path`.
            Examples:
                ["/path/to/other_module", "/other_path/local_project.zip"]
        conda (dict | str): Either the conda YAML config or the name of a
            local conda env (e.g., "pytorch_p36"). The Ray dependency will be
            automatically injected into the conda env to ensure compatibility
            with the cluster Ray. The conda name may be mangled automatically
            to avoid conflicts between runtime envs.
            Examples:
                {"channels": ["defaults"], "dependencies": ["codecov"]}
                "pytorch_p36"   # Found on DLAMIs
        docker (dict): Require a given (Docker) container image. The Ray
            dependency will be automatically installed into the docker image
            to ensure compatibility with the cluster Ray. The `run_options`
            dict spec is here: https://docs.docker.com/engine/reference/run/
            Examples:
                {"image": "anyscale/ray-ml:nightly-py38-cpu", **run_options}
        env_vars (dict): Environment variables to set.
            Examples:
                {"OMP_NUM_THREADS": "32", "TF_WARNINGS": "none"}
    """

    def __init__(self, runtime_env_json: dict):
        if "conda" in runtime_env_json:
            self.conda = runtime_env_json["conda"]
        else:
            self.conda = None
        if "working_dir" in runtime_env_json:
            self.working_dir = runtime_env_json["working_dir"]
        else:
            self.working_dir = None
        # TODO(ekl) we should have better schema validation here.
        # TODO(ekl) support env_vars, docker, py_modules

    def to_worker_env_vars(self, override_environment_variables: dict) -> dict:
        """Given existing worker env vars, return an updated dict.

        This sets any necessary env vars to setup the runtime env.
        TODO(ekl): env vars is probably not the right long term impl.
        """
        if override_environment_variables is None:
            override_environment_variables = {}
        if self.conda:
            conda_env_dir = get_conda_env_dir(self.conda)
            override_environment_variables.update(PYTHONHOME=conda_env_dir)
        if self.working_dir:
            override_environment_variables.update(
                RAY_RUNTIME_ENV_FILES=self.working_dir)
        return override_environment_variables


class Protocol(Enum):
    """A enum for supported backend storage."""

    # For docstring
    def __new__(cls, value, doc=None):
        self = object.__new__(cls)
        self._value_ = value
        if doc is not None:
            self.__doc__ = doc
        return self

    GCS = "gcs", "For packages created and managed by the system."
    PIN_GCS = "pingcs", "For packages created and managed by the users."


def _xor_bytes(left: bytes, right: bytes) -> bytes:
    if left and right:
        return bytes(a ^ b for (a, b) in zip(left, right))
    return left or right


def _zip_module(path: Path, relative_path: Path, zip_handler: ZipFile) -> None:
    """Go through all files and zip them into a zip file"""
    for from_file_name in path.glob("**/*"):
        file_size = from_file_name.stat().st_size
        if file_size >= FILE_SIZE_LIMIT:
            raise RuntimeError(f"File {from_file_name} is too big, "
                               "which currently is not allowd ")
        if file_size >= FILE_SIZE_WARNING:
            logger.warning(
                f"File {from_file_name} is too big ({file_size} bytes). "
                "Consider exclude this file in working directory.")
        to_file_name = from_file_name.relative_to(relative_path)
        zip_handler.write(from_file_name, to_file_name)


def _hash_modules(path: Path) -> bytes:
    """Helper function to create hash of a directory.

    It'll go through all the files in the directory and xor
    hash(file_name, file_content) to create a hash value.
    """
    hash_val = None
    BUF_SIZE = 4096 * 1024
    for from_file_name in path.glob("**/*"):
        md5 = hashlib.md5()
        md5.update(str(from_file_name).encode())
        if not Path(from_file_name).is_dir():
            with open(from_file_name, mode="rb") as f:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)
        hash_val = _xor_bytes(hash_val, md5.digest())
    return hash_val


def _get_local_path(pkg_uri: str) -> str:
    assert PKG_DIR, "Please set PKG_DIR in the module first."
    (_, pkg_name) = _parse_uri(pkg_uri)
    return os.path.join(PKG_DIR, pkg_name)


def _parse_uri(pkg_uri: str) -> Tuple[Protocol, str]:
    uri = urlparse(pkg_uri)
    protocol = Protocol(uri.scheme)
    return (protocol, uri.netloc)


# TODO(yic): Fix this later to handle big directories in better way
def get_project_package_name(working_dir: str, modules: List[str]) -> str:
    """Get the name of the package by working dir and modules.

    This function will generate the name of the package by the working
    directory and modules. It'll go through all the files in working_dir
    and modules and hash the contents of these files to get the hash value
    of this package. The final package name is: _ray_pkg_<HASH_VAL>.zip
    Right now, only the modules given will be included. The dependencies
    are not included automatically.

    Examples:

    .. code-block:: python
        >>> import any_module
        >>> get_project_package_name("/working_dir", [any_module])
        .... _ray_pkg_af2734982a741.zip

 e.g., _ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip
    Args:
        working_dir (str): The working directory.
        modules (list[module]): The python module.

    Returns:
        Package name as a string.
    """
    RAY_PKG_PREFIX = "_ray_pkg_"
    hash_val = None
    if working_dir:
        assert isinstance(working_dir, str)
        assert Path(working_dir).exists()
        hash_val = _xor_bytes(hash_val, _hash_modules(Path(working_dir)))
    for module in modules or []:
        assert inspect.ismodule(module)
        hash_val = _xor_bytes(hash_val,
                              _hash_modules(Path(module.__file__).parent))
    return RAY_PKG_PREFIX + hash_val.hex() + ".zip" if hash_val else None


def create_project_package(working_dir: str, modules: List[ModuleType],
                           output_path: str) -> None:
    """Create a pckage that will be used by workers.

    This function is used to create a package file based on working directory
    and python local modules.

    Args:
        working_dir (str): The working directory.
        modules (list[module]): The python modules to be included.
        output_path (str): The path of file to be created.
    """
    pkg_file = Path(output_path)
    with ZipFile(pkg_file, "w") as zip_handler:
        if working_dir:
            # put all files in /path/working_dir into zip
            working_path = Path(working_dir)
            _zip_module(working_path, working_path, zip_handler)
        for module in modules or []:
            logger.info(module.__file__)
            # we only take care of modules with path like this for now:
            #    /path/module_name/__init__.py
            # module_path should be: /path/module_name
            module_path = Path(module.__file__).parent
            _zip_module(module_path, module_path.parent, zip_handler)


def fetch_package(pkg_uri: str, pkg_file: Path) -> int:
    """Fetch a package from a given uri.

    This function is used to fetch a pacakge from the given uri to local
    filesystem.

    Args:
        pkg_uri (str): The uri of the package to download.
        pkg_file (pathlib.Path): The path in local filesystem to download the
            package.

    Returns:
        The number of bytes downloaded.
    """
    (protocol, pkg_name) = _parse_uri(pkg_uri)
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        code = _internal_kv_get(pkg_uri)
        code = code or b""
        pkg_file.write_bytes(code)
        return len(code)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def _store_package_in_gcs(gcs_key: str, data: bytes) -> int:
    _internal_kv_put(gcs_key, data)
    return len(data)


def push_package(pkg_uri: str, pkg_path: str) -> int:
    """Push a package to uri.

    This function is to push a local file to remote uri. Right now, only GCS
    is supported.

    Args:
        pkg_uri (str): The uri of the package to upload to.
        pkg_path (str): Path of the local file.

    Returns:
        The number of bytes uploaded.
    """
    (protocol, pkg_name) = _parse_uri(pkg_uri)
    data = Path(pkg_path).read_bytes()
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        return _store_package_in_gcs(pkg_uri, data)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def package_exists(pkg_uri: str) -> bool:
    """Check whether the package with given uri exists or not.

    Args:
        pkg_uri (str): The uri of the package

    Return:
        True for package existing and False for not.
    """
    (protocol, pkg_name) = _parse_uri(pkg_uri)
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        return _internal_kv_exists(pkg_uri)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def rewrite_working_dir_uri(job_config: JobConfig) -> None:
    """Rewrite the working dir uri field in job_config.

    This function is used to update the runtime field in job_config. The
    runtime field will be generated based on the hash of required files and
    modules.

    Args:
        job_config (JobConfig): The job config.
    """
    # For now, we only support local directory and packages
    working_dir = job_config.runtime_env.get("working_dir")
    required_modules = job_config.runtime_env.get("local_modules")

    if (not job_config.runtime_env.get("working_dir_uri")) and (
            working_dir or required_modules):
        pkg_name = get_project_package_name(working_dir, required_modules)
        job_config.runtime_env[
            "working_dir_uri"] = Protocol.GCS.value + "://" + pkg_name


def upload_runtime_env_package_if_needed(job_config: JobConfig) -> None:
    """Upload runtime env if it's not there.

    It'll check whether the runtime environment exists in the cluster or not.
    If it doesn't exist, a package will be created based on the working
    directory and modules defined in job config. The package will be
    uploaded to the cluster after this.

    Args:
        job_config (JobConfig): The job config of driver.
    """
    pkg_uris = job_config.get_runtime_env_uris()
    for pkg_uri in pkg_uris:
        if not package_exists(pkg_uri):
            file_path = _get_local_path(pkg_uri)
            pkg_file = Path(file_path)
            working_dir = job_config.runtime_env.get("working_dir")
            required_modules = job_config.runtime_env.get("local_modules")
            logger.info(f"{pkg_uri} doesn't exist. Create new package with"
                        f" {working_dir} and {required_modules}")
            if not pkg_file.exists():
                create_project_package(working_dir, required_modules,
                                       file_path)
            # Push the data to remote storage
            pkg_size = push_package(pkg_uri, pkg_file)
            logger.info(f"{pkg_uri} has been pushed with {pkg_size} bytes")


def ensure_runtime_env_setup(pkg_uris: List[str]) -> None:
    """Make sure all required packages are downloaded it local.

    Necessary packages required to run the job will be downloaded
    into local file system if it doesn't exist.

    Args:
        pkg_uri list(str): Package of the working dir for the runtime env.
    """
    for pkg_uri in pkg_uris:
        pkg_file = Path(_get_local_path(pkg_uri))
        # For each node, the package will only be downloaded one time
        # Locking to avoid multiple process download concurrently
        lock = FileLock(str(pkg_file) + ".lock")
        with lock:
            # TODO(yic): checksum calculation is required
            if pkg_file.exists():
                logger.debug(
                    f"{pkg_uri} has existed locally, skip downloading")
            else:
                pkg_size = fetch_package(pkg_uri, pkg_file)
                logger.debug(f"Downloaded {pkg_size} bytes into {pkg_file}")
        sys.path.insert(0, str(pkg_file))
