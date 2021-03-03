import hashlib
from pathlib import Path
from zipfile import ZipFile

from enum import Enum
from ray.experimental import internal_kv

PKG_DIR='/tmp'

class Protocol(Enum):
    GCS = "gcs://"
    PIN_GCS = "pin_gcs://"
    # These two are not supported
    S3 = "s3://"
    GITHUB = "github://"

def _xor_bytes(left, right):
    if left and right:
        return bytes(a ^ b for (a, b) in zip(left, right))
    return left or right

def _zip_module(path, zip_handler):
    for from_file_name in path.glob("**/*"):
        # We include pyc for performance
        if from_file_name.match("*.py") or from_file_name.match("*.pyc"):
            to_file_name = from_file_name.relative_to(path.parent)
            zip_handler.write(from_file_name, to_file_name)

def _hash_modules(path):
    hash_val = None
    BUF_SIZE = 4096 * 1024
    for from_file_name in path.glob("**/*"):
        md5 = hashlib.md5()
        # We include pyc for performance
        if from_file_name.match("*.py") or from_file_name.match("*.pyc"):
            with open(from_file_name, mode="rb") as f:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)
        hash_val = _xor_bytes(hash_val, md5.digest())
    return hash_val

def get_local_path(pkg_name) -> str:
    return PKG_DIR + "/" + pkg_name

def get_project_package_name(dirs, modules) -> str:
    RAY_PKG_PREFIX = "_ray_pkg_"
    hash_val = None
    for directory in dirs or []:
        hash_val = _xor_bytes(hash_val, _hash_modules(Path(directory)))
    for module in modules or []:
        hash_val = _xor_bytes(hash_val, _hash_modules(Path(module.__file__).parent))
    return RAY_PKG_PREFIX + hash_val.hex() + ".zip" if hash_val else None

def create_project_package(pkg_file: Path, dirs, modules):
    with ZipFile(pkg_file, "w") as zip_handler:
        for directory in dirs or []:
            _zip_module(Path(directory), zip_handler)
        for module in modules or []:
            _zip_module(Path(module.__file__).parent, zip_handler)

def parse_uri(pkg_uri: str) -> (str, str):
    for p in Protocol:
        if pkg_uri.startswith(p.value):
            return (p, pkg_uri[len(p.value):])
    return (None, pkg_uri)

def fetch_package(protocol: Protocol, pkg_uri: str, pkg_file: Path):
    if protocol == Protocol.GCS or protocol == Protocol.PIN_GCS:
        code = internal_kv._internal_kv_get(pkg_uri)
        code = code or b""
        pkg_file.write_bytes(code)
        return len(code)
    else:
        raise NotImplementedError(f"Protocol {protocol.value} is not supported")

def push_package(protocol: Protocol, pkg_uri: str, pkg_file: Path):
    if protocol == Protocol.GCS or protocol == Protocol.PIN_GCS:
        data = pkg_file.read_bytes()
        internal_kv._internal_kv_put(pkg_uri, data)
        return len(data)
    else:
        raise NotImplementedError(f"Protocol {protocol.value} is not supported")

def package_exists(protocol: Protocol, pkg_uri: str):
    if protocol == Protocol.GCS or protocol == Protocol.PIN_GCS:
        return internal_kv._internal_kv_exists(pkg_uri)
    else:
        raise NotImplementedError(f"Protocol {protocol.value} is not supported")
