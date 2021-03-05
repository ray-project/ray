import hashlib
import logging
import inspect

from filelock import FileLock
from pathlib import Path
from zipfile import ZipFile
from ray.job_config import JobConfig
from enum import Enum
from ray.experimental import internal_kv
from ray.core.generated.common_pb2 import RuntimeEnv
from urllib.parse import urlparse
import os
import sys

PKG_DIR = "/tmp/ray/runtime_resources"
Path(PKG_DIR).mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)

FILE_SIZE_WARNING = 10 * 1024 * 1024  # 10MB
FILE_SIZE_LIMIT = 50 * 1024 * 1024  # 50MB


class Protocol(Enum):
    """A enum for supported backend storage."""

    # For docstring
    def __new__(cls, value, doc=None):
        self = object.__new__(cls)
        self._value_ = value
        if doc is not None:
            self.__doc__ = doc
        return self

    GCS = "gcs", "For package created and managed by system."
    PIN_GCS = "pingcs", "For package created and managed by users."


def _xor_bytes(left, right):
    if left and right:
        return bytes(a ^ b for (a, b) in zip(left, right))
    return left or right


def _zip_module(path, relative_path, zip_handler):
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


def _hash_modules(path):
    """
    Helper function to create hash of a directory.
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


def _get_local_path(pkg_uri) -> str:
    (_, pkg_name) = _parse_uri(pkg_uri)
    return os.path.join(PKG_DIR, pkg_name)


def _parse_uri(pkg_uri: str) -> (Protocol, str):
    uri = urlparse(pkg_uri)
    protocol = Protocol(uri.scheme)
    return (protocol, uri.netloc)


# TODO(yic): Fix this later to handle big directories in better way
def get_project_package_name(working_dir, modules) -> str:
    """This function will generate the name of the package by the working
    directory and modules. It'll go through all the files in working_dir
    and modules and hash the contents of these files to get the hash value
    of this package. The final package name is: _ray_pkg_<HASH_VAL>.zip

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


def create_project_package(pkg_file: Path, working_dir: str, modules):
    """This function is used to create a package file based on working directory
    and python local modules.

    Args:
        pkg_file (pathlib.Path): The path of file to be created.
        working_dir (str): The working directory.
        modules (list[module]): The python modules to be included.

    Returns:
        None
    """
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


def fetch_package(pkg_uri: str, pkg_file: Path):
    """This function is used to fetch a pacakge from the given uri to local
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
        code = internal_kv._internal_kv_get(pkg_uri)
        code = code or b""
        pkg_file.write_bytes(code)
        return len(code)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def _store_package_in_gcs(gcs_key: str, data: bytes):
    internal_kv._internal_kv_put(gcs_key, data)
    return len(data)


def push_package(pkg_uri: str, pkg_file: Path):
    """This function is to push a local file to remote uri. Right now, only GCS
    is supported.

    Args:
        pkg_uri (str): The uri of the package to upload to.
        pkg_file (Path): Path of the local file.

    Returns:
        The number of bytes uploaded.
    """
    (protocol, pkg_name) = _parse_uri(pkg_uri)
    data = pkg_file.read_bytes()
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        _store_package_in_gcs(pkg_uri, data)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def package_exists(pkg_uri: str):
    """Check whether the package with given uri exists or not.

    Args:
        pkg_uri (str): The uri of the package

    Return:
        True for package existing and False for not.
    """
    (protocol, pkg_name) = _parse_uri(pkg_uri)
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        return internal_kv._internal_kv_exists(pkg_uri)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def update_runtime_env(job_config: JobConfig):
    """This function is used to update the runtime field in job_config. The
    runtime field will be generated based on the hash of required files and
    modules.

    Args:
        job_config (JobConfig): The job config.

    Return:
        None
    """
    # For now, we only support local directory and packages
    working_dir = job_config.runtime_env.get("working_dir")
    required_modules = job_config.runtime_env.get("local_modules")

    if (not job_config.runtime_env.get("working_dir_uri")) and (
            working_dir or required_modules):
        pkg_name = get_project_package_name(working_dir, required_modules)
        job_config.runtime_env[
            "working_dir_uri"] = Protocol.GCS.value + "://" + pkg_name


def driver_runtime_init(job_config: JobConfig):
    """Init runtime env for driver. It'll check whether the runtime
    environment exists in the cluster or not. If it doesn't exist, a package
    will be created based on the working directory and modules defined in job
    config. The package will be uploaded to the cluster after this.

    Args:
        job_config (JobConfig): The job config of driver.
    """
    pkg_uri = job_config.get_package_uri()
    if not pkg_uri:
        return
    if not package_exists(pkg_uri):
        pkg_file = Path(_get_local_path(pkg_uri))
        working_dir = job_config.runtime_env.get("working_dir")
        required_modules = job_config.runtime_env.get("local_modules")
        logger.info(f"{pkg_uri} doesn't exist. Create new package with"
                    f" {working_dir} and {required_modules}")
        if not pkg_file.exists():
            create_project_package(pkg_file, working_dir, required_modules)
        # Push the data to remote storage
        pkg_size = push_package(pkg_uri, pkg_file)
        logger.info(f"{pkg_uri} has been pushed with {pkg_size} bytes")


def worker_runtime_init(runtime_env: RuntimeEnv):
    """Init runtime env for worker. Necessary packages required to run the job will
    be downloaded into local file system if it doesn't exist.

    Args:
        runtime_env (RuntimeEnv): Runtime environment of the job
    """
    pkg_uri = runtime_env.working_dir_uri
    if not pkg_uri:
        return
    pkg_file = Path(_get_local_path(pkg_uri))
    # For each node, the package will only be downloaded one time
    # Locking to avoid multiple process download concurrently
    lock = FileLock(str(pkg_file) + ".lock")
    with lock:
        # TODO(yic): checksum calculation is required
        if pkg_file.exists():
            logger.info(f"{pkg_uri} has existed locally, skip downloading")
        else:
            pkg_size = fetch_package(pkg_uri, pkg_file)
            logger.info(f"Downloaded {pkg_size} bytes into {pkg_file}")
    sys.path.insert(0, str(pkg_file))
