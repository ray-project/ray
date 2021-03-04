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


class Protocol(Enum):
    # gcs is for package created by system and will be managed by
    # ray cluster automatically through reference counting
    GCS = "gcs"
    # pin_gcs is for package created mannually. User needs to
    # maintain the lifetime of this object
    PIN_GCS = "pingcs"


def _xor_bytes(left, right):
    if left and right:
        return bytes(a ^ b for (a, b) in zip(left, right))
    return left or right


def _zip_module(path, relative_path, zip_handler):
    for from_file_name in path.glob("**/*"):
        to_file_name = from_file_name.relative_to(relative_path)
        zip_handler.write(from_file_name, to_file_name)


def _hash_modules(path):
    """

    """
    hash_val = None
    BUF_SIZE = 4096 * 1024
    for from_file_name in path.glob("**/*"):
        md5 = hashlib.md5()
        if Path(from_file_name).is_dir():
            md5.update(str(from_file_name).encode())
        else:
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


# TODO(yic): Fix this later to handle big directories in better way
def get_project_package_name(working_dir, modules) -> str:
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


def _parse_uri(pkg_uri: str) -> (Protocol, str):
    uri = urlparse(pkg_uri)
    protocol = Protocol(uri.scheme)
    return (protocol, uri.netloc)


def fetch_package(pkg_uri: str, pkg_file: Path):
    (protocol, pkg_name) = _parse_uri(pkg_uri)
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        code = internal_kv._internal_kv_get(pkg_uri)
        code = code or b""
        pkg_file.write_bytes(code)
        return len(code)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def push_package(pkg_uri: str, pkg_file: Path):
    (protocol, pkg_name) = _parse_uri(pkg_uri)
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        data = pkg_file.read_bytes()
        internal_kv._internal_kv_put(pkg_uri, data)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")
    return len(data)


def package_exists(pkg_uri: str):
    (protocol, pkg_name) = _parse_uri(pkg_uri)
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        return internal_kv._internal_kv_exists(pkg_uri)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def update_runtime_env(job_config: JobConfig):
    # For now, we only support local directory and packages
    working_dir = job_config.runtime_env.get("working_dir")
    required_modules = job_config.runtime_env.get("local_modules")

    if (not job_config.runtime_env.get("working_dir_uri")) and (
            working_dir or required_modules):
        pkg_name = get_project_package_name(working_dir, required_modules)
        job_config.runtime_env[
            "working_dir_uri"] = Protocol.GCS.value + "://" + pkg_name


def driver_runtime_init(job_config: JobConfig):
    pkg_uri = job_config.get_pacakge_uri()
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
