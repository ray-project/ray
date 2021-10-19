from enum import Enum
from filelock import FileLock
import hashlib
import logging
import os
from pathlib import Path
import shutil
from typing import Callable, List, Optional, Tuple
from urllib.parse import urlparse
from zipfile import ZipFile

from ray.experimental.internal_kv import (_internal_kv_put, _internal_kv_get,
                                          _internal_kv_exists)
from ray._private.thirdparty.pathspec import PathSpec

default_logger = logging.getLogger(__name__)

FILE_SIZE_WARNING = 10 * 1024 * 1024  # 10MiB
# NOTE(edoakes): we should be able to support up to 512 MiB based on the GCS'
# limit, but for some reason that causes failures when downloading.
GCS_STORAGE_MAX_SIZE = 100 * 1024 * 1024  # 100MiB
RAY_PKG_PREFIX = "_ray_pkg_"


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


def _dir_travel(
        path: Path,
        excludes: List[Callable],
        handler: Callable,
        logger: Optional[logging.Logger] = default_logger,
):
    e = _get_gitignore(path)
    if e is not None:
        excludes.append(e)
    skip = any(e(path) for e in excludes)
    if not skip:
        try:
            handler(path)
        except Exception as e:
            logger.error(f"Issue with path: {path}")
            raise e
        if path.is_dir():
            for sub_path in path.iterdir():
                _dir_travel(sub_path, excludes, handler, logger=logger)
    if e is not None:
        excludes.pop()


def _zip_module(root: Path,
                relative_path: Path,
                excludes: Optional[Callable],
                zip_handler: ZipFile,
                logger: Optional[logging.Logger] = default_logger) -> None:
    """Go through all files and zip them into a zip file"""

    def handler(path: Path):
        # Pack this path if it's an empty directory or it's a file.
        if path.is_dir() and next(path.iterdir(),
                                  None) is None or path.is_file():
            file_size = path.stat().st_size
            if file_size >= FILE_SIZE_WARNING:
                logger.warning(
                    f"File {path} is very large ({file_size} bytes). "
                    "Consider excluding this file from the working directory.")
            to_path = path.relative_to(relative_path)
            zip_handler.write(path, to_path)

    excludes = [] if excludes is None else [excludes]
    _dir_travel(root, excludes, handler, logger=logger)


def _hash_directory(
        root: Path,
        relative_path: Path,
        excludes: Optional[Callable],
        logger: Optional[logging.Logger] = default_logger,
) -> bytes:
    """Helper function to create hash of a directory.

    It'll go through all the files in the directory and xor
    hash(file_name, file_content) to create a hash value.
    """
    hash_val = b"0"
    BUF_SIZE = 4096 * 1024

    def handler(path: Path):
        md5 = hashlib.md5()
        md5.update(str(path.relative_to(relative_path)).encode())
        if not path.is_dir():
            with path.open("rb") as f:
                data = f.read(BUF_SIZE)
                while len(data) != 0:
                    md5.update(data)
                    data = f.read(BUF_SIZE)
        nonlocal hash_val
        hash_val = _xor_bytes(hash_val, md5.digest())

    excludes = [] if excludes is None else [excludes]
    _dir_travel(root, excludes, handler, logger=logger)
    return hash_val


def _parse_uri(pkg_uri: str) -> Tuple[Protocol, str]:
    uri = urlparse(pkg_uri)
    protocol = Protocol(uri.scheme)
    return (protocol, uri.netloc)


def _get_excludes(path: Path, excludes: List[str]) -> Callable:
    path = path.absolute()
    pathspec = PathSpec.from_lines("gitwildmatch", excludes)

    def match(p: Path):
        path_str = str(p.absolute().relative_to(path))
        return pathspec.match_file(path_str)

    return match


def _get_gitignore(path: Path) -> Optional[Callable]:
    path = path.absolute()
    ignore_file = path / ".gitignore"
    if ignore_file.is_file():
        with ignore_file.open("r") as f:
            pathspec = PathSpec.from_lines("gitwildmatch", f.readlines())

        def match(p: Path):
            path_str = str(p.absolute().relative_to(path))
            return pathspec.match_file(path_str)

        return match
    else:
        return None


def _store_package_in_gcs(gcs_key: str, data: bytes) -> int:
    if len(data) >= GCS_STORAGE_MAX_SIZE:
        raise RuntimeError(
            "working_dir package exceeds the maximum size of 100MiB. You "
            "can exclude large files using the 'excludes' option to the "
            "runtime_env.")

    _internal_kv_put(gcs_key, data)
    return len(data)


def _get_local_path(base_directory: str, pkg_uri: str) -> str:
    _, pkg_name = _parse_uri(pkg_uri)
    return os.path.join(base_directory, pkg_name)


def _create_package_from_directory(
        directory: str,
        excludes: List[str],
        output_path: str,
        logger: Optional[logging.Logger] = default_logger) -> None:
    """Create a package that will be used by workers.

    This function is used to create a package file based on working
    directory and python local modules.

    Args:
        directory (str): The working directory.
        excludes (List(str)): The directories or file to be excluded.
        output_path (str): The path of file to be created.
    """
    pkg_file = Path(output_path).absolute()
    with ZipFile(pkg_file, "w") as zip_handler:
        # Put all files in /path/directory into the zip file.
        working_path = Path(directory).absolute()
        _zip_module(
            working_path,
            working_path,
            _get_excludes(working_path, excludes),
            zip_handler,
            logger=logger)


def _push_package(pkg_uri: str, pkg_path: str) -> int:
    """Push a package to a given URI.

    This function is to push a local file to remote URI. Right now, only
    storing in the GCS is supported.

    Args:
        pkg_uri (str): The URI of the package to upload to.
        pkg_path (str): Path of the local file.

    Returns:
        The number of bytes uploaded.
    """
    protocol, pkg_name = _parse_uri(pkg_uri)
    data = Path(pkg_path).read_bytes()
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        return _store_package_in_gcs(pkg_uri, data)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def _package_exists(pkg_uri: str) -> bool:
    """Check whether the package with given URI exists or not.

    Args:
        pkg_uri (str): The uri of the package

    Return:
        True for package existing and False for not.
    """
    protocol, pkg_name = _parse_uri(pkg_uri)
    if protocol in (Protocol.GCS, Protocol.PIN_GCS):
        return _internal_kv_exists(pkg_uri)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def get_uri_for_directory(directory: str,
                          excludes: Optional[List[str]] = None) -> str:
    """Get the name of the package by working dir.

    This function will generate the name of the package by the directory.
    It'll go through all the files in the directory and hash the contents
    of the files to get the hash value of the package.
    The final package name is: _ray_pkg_<HASH_VAL>.zip,
    e.g., _ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip

    Examples:

    .. code-block:: python
        >>> import any_module
        >>> get_directory_package_name("/my_directory")
        .... _ray_pkg_af2734982a741.zip

    Args:
        directory (str): The directory.
        excludes (list[str]): The dir or files that should be excluded.

    Returns:
        Package name as a string.
    """
    if excludes is None:
        excludes = []

    directory = Path(directory).absolute()
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"directory {directory} must be an existing"
                         " directory")

    hash_val = b"0"
    hash_val = _xor_bytes(
        hash_val,
        _hash_directory(directory, directory, _get_excludes(
            directory, excludes)))

    return "{protocol}://{pkg_name}.zip".format(
        protocol=Protocol.GCS.value, pkg_name=RAY_PKG_PREFIX + hash_val.hex())


def upload_package_if_needed(pkg_uri: str,
                             base_directory: str,
                             directory: str,
                             excludes: Optional[List[str]] = None,
                             logger: Optional[logging.Logger] = default_logger
                             ) -> Tuple[bool, bool]:
    """XXX: TODO

    It'll check whether the runtime environment exists in the cluster or
    not. If it doesn't, a package will be created based on the working
    directory and modules defined in job config. The package will be
    uploaded to the cluster after this.

    Args:
        job_config (JobConfig): The job config of driver.
    """
    if excludes is None:
        excludes = []

    if logger is None:
        logger = default_logger

    created, uploaded = False, False
    if not _package_exists(pkg_uri):
        pkg_file = Path(_get_local_path(base_directory, pkg_uri))
        if not pkg_file.exists():
            created = True
            logger.info(f"Creating a new package for directory {directory}.")
            _create_package_from_directory(
                directory, excludes, pkg_file, logger=logger)
        # Push the data to remote storage
        pkg_size = _push_package(pkg_uri, pkg_file)
        logger.info(f"{pkg_uri} has been pushed with {pkg_size} bytes.")
        uploaded = True

    return created, uploaded


def download_and_unpack_package(
        pkg_uri: str,
        base_directory: str,
        logger: Optional[logging.Logger] = default_logger,
) -> Optional[str]:
    """XXX"""
    pkg_file = Path(_get_local_path(base_directory, pkg_uri))
    with FileLock(str(pkg_file) + ".lock"):
        if logger is None:
            logger = default_logger

        logger.debug(f"Fetching package for uri: {pkg_uri}")

        local_dir = pkg_file.with_suffix("")
        assert local_dir != pkg_file, "Invalid pkg_file!"
        if local_dir.exists():
            assert local_dir.is_dir(), f"{local_dir} is not a directory"
        else:
            protocol, pkg_name = _parse_uri(pkg_uri)
            if protocol in (Protocol.GCS, Protocol.PIN_GCS):
                code = _internal_kv_get(pkg_uri)
                if code is None:
                    raise IOError("Fetch uri failed")
                code = code or b""
                pkg_file.write_bytes(code)
            else:
                raise NotImplementedError(
                    f"Protocol {protocol} is not supported")

            os.mkdir(local_dir)
            logger.debug(f"Unpacking {pkg_file} to {local_dir}")
            with ZipFile(str(pkg_file), "r") as zip_ref:
                zip_ref.extractall(local_dir)
            pkg_file.unlink()

        return str(local_dir)


def delete_package(pkg_uri: str, base_directory: str) -> bool:
    """Deletes a specific URI from the local filesystem.

    Args:
        pkg_uri (str): URI to delete.

    Returns:
        True if the URI was successfully deleted, else False.
    """

    deleted = False
    path = Path(_get_local_path(base_directory, pkg_uri))
    with FileLock(str(path) + ".lock"):
        path = path.with_suffix("")
        if path.exists():
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(str(path))
            else:
                path.unlink()
            deleted = True

    return deleted
