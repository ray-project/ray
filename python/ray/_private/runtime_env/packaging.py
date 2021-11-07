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

# If an individual file is beyond this size, print a warning.
FILE_SIZE_WARNING = 10 * 1024 * 1024  # 10MiB
# NOTE(edoakes): we should be able to support up to 512 MiB based on the GCS'
# limit, but for some reason that causes failures when downloading.
GCS_STORAGE_MAX_SIZE = 100 * 1024 * 1024  # 100MiB
RAY_PKG_PREFIX = "_ray_pkg_"


def _mib_string(num_bytes: float) -> str:
    size_mib = float(num_bytes / 1024**2)
    return f"{size_mib:.2f}MiB"


class Protocol(Enum):
    """A enum for supported storage backends."""

    # For docstring
    def __new__(cls, value, doc=None):
        self = object.__new__(cls)
        self._value_ = value
        if doc is not None:
            self.__doc__ = doc
        return self

    GCS = "gcs", "For packages dynamically uploaded and managed by the GCS."
    S3 = "s3", "Remote s3 path, assumes everything packed in one zip file."
    CONDA = "conda", "For conda environments installed locally on each node."


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
    """Travels the path recursively, calling the handler on each subpath.

    Respects excludes, which will be called to check if this path is skipped.
    """
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
    hash_val = b"0" * 8
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


def parse_uri(pkg_uri: str) -> Tuple[Protocol, str]:
    """
    Parse resource uri into protocol and package name based on its format.
    Note that the output of this function is not for handling actual IO, it's
    only for setting up local directory folders by using package name as path.
    For GCS URIs, netloc is the package name.
        urlparse("gcs://_ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip")
            -> ParseResult(
                scheme='gcs',
                netloc='_ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip'
            )
            -> ("gcs", "_ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip")
    For S3 URIs, the bucket and path will have '/' replaced with '_'.
        urlparse("s3://bucket/dir/file.zip")
            -> ParseResult(
                scheme='s3',
                netloc='bucket',
                path='/dir/file.zip'
            )
            -> ("s3", "s3_bucket_dir_file.zip")
    """
    uri = urlparse(pkg_uri)
    protocol = Protocol(uri.scheme)
    if protocol == Protocol.S3:
        return (protocol, f"s3_{uri.netloc}_" + "_".join(uri.path.split("/")))
    else:
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


def _store_package_in_gcs(
        pkg_uri: str,
        data: bytes,
        logger: Optional[logging.Logger] = default_logger) -> int:
    file_size = len(data)
    size_str = _mib_string(file_size)
    if len(data) >= GCS_STORAGE_MAX_SIZE:
        raise RuntimeError(
            f"Package size ({size_str}) exceeds the maximum size of "
            f"{_mib_string(GCS_STORAGE_MAX_SIZE)}. You can exclude large "
            "files using the 'excludes' option to the runtime_env.")

    logger.info(f"Pushing file package '{pkg_uri}' ({size_str}) to "
                "Ray cluster...")
    _internal_kv_put(pkg_uri, data)
    logger.info(f"Successfully pushed file package '{pkg_uri}'.")
    return len(data)


def _get_local_path(base_directory: str, pkg_uri: str) -> str:
    _, pkg_name = parse_uri(pkg_uri)
    return os.path.join(base_directory, pkg_name)


def _zip_directory(directory: str,
                   excludes: List[str],
                   output_path: str,
                   include_parent_dir: bool = False,
                   logger: Optional[logging.Logger] = default_logger) -> None:
    """Zip the target directory and write it to the output_path.

        directory (str): The directory to zip.
        excludes (List(str)): The directories or file to be excluded.
        output_path (str): The output path for the zip file.
        include_parent_dir: If true, includes the top-level directory as a
            directory inside the zip file.
    """
    pkg_file = Path(output_path).absolute()
    with ZipFile(pkg_file, "w") as zip_handler:
        # Put all files in the directory into the zip file.
        dir_path = Path(directory).absolute()

        def handler(path: Path):
            # Pack this path if it's an empty directory or it's a file.
            if path.is_dir() and next(path.iterdir(),
                                      None) is None or path.is_file():
                file_size = path.stat().st_size
                if file_size >= FILE_SIZE_WARNING:
                    logger.warning(
                        f"File {path} is very large "
                        f"({_mib_string(file_size)}). Consider adding this "
                        "file to the 'excludes' list to skip uploading it: "
                        "`ray.init(..., "
                        f"runtime_env={{'excludes': ['{path}']}})`")
                to_path = path.relative_to(dir_path)
                if include_parent_dir:
                    to_path = dir_path.name / to_path
                zip_handler.write(path, to_path)

        excludes = [_get_excludes(dir_path, excludes)]
        _dir_travel(dir_path, excludes, handler, logger=logger)


def package_exists(pkg_uri: str) -> bool:
    """Check whether the package with given URI exists or not.

    Args:
        pkg_uri (str): The uri of the package

    Return:
        True for package existing and False for not.
    """
    protocol, pkg_name = parse_uri(pkg_uri)
    if protocol == Protocol.GCS:
        return _internal_kv_exists(pkg_uri)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def get_uri_for_directory(directory: str,
                          excludes: Optional[List[str]] = None) -> str:
    """Get a content-addressable URI from a directory's contents.

    This function will generate the name of the package by the directory.
    It'll go through all the files in the directory and hash the contents
    of the files to get the hash value of the package.
    The final package name is: _ray_pkg_<HASH_VAL>.zip of this package.
    e.g., _ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip

    Examples:

    .. code-block:: python
        >>> get_uri_for_directory("/my_directory")
        .... _ray_pkg_af2734982a741.zip

    Args:
        directory (str): The directory.
        excludes (list[str]): The dir or files that should be excluded.

    Returns:
        URI (str)

    Raises:
        ValueError if the directory doesn't exist.
    """
    if excludes is None:
        excludes = []

    directory = Path(directory).absolute()
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"directory {directory} must be an existing"
                         " directory")

    hash_val = _hash_directory(directory, directory,
                               _get_excludes(directory, excludes))

    return "{protocol}://{pkg_name}.zip".format(
        protocol=Protocol.GCS.value, pkg_name=RAY_PKG_PREFIX + hash_val.hex())


def upload_package_to_gcs(pkg_uri: str, pkg_bytes: bytes):
    protocol, pkg_name = parse_uri(pkg_uri)
    if protocol == Protocol.GCS:
        _store_package_in_gcs(pkg_uri, pkg_bytes)
    elif protocol == Protocol.S3:
        raise RuntimeError("push_package should not be called with s3 path.")
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def create_package(directory: str,
                   target_path: Path,
                   include_parent_dir: bool = False,
                   excludes: Optional[List[str]] = None,
                   logger: Optional[logging.Logger] = default_logger):
    if excludes is None:
        excludes = []

    if logger is None:
        logger = default_logger

    if not target_path.exists():
        logger.info(
            f"Creating a file package for local directory '{directory}'.")
        _zip_directory(
            directory,
            excludes,
            target_path,
            include_parent_dir=include_parent_dir,
            logger=logger)


def upload_package_if_needed(
        pkg_uri: str,
        base_directory: str,
        directory: str,
        include_parent_dir: bool = False,
        excludes: Optional[List[str]] = None,
        logger: Optional[logging.Logger] = default_logger) -> bool:
    """Upload the contents of the directory under the given URI.

    This will first create a temporary zip file under the passed
    base_directory.

    If the package already exists in storage, this is a no-op.

    Args:
        pkg_uri: URI of the package to upload.
        base_directory: Directory where package files are stored.
        directory: Directory to be uploaded.
        include_parent_dir: If true, includes the top-level directory as a
            directory inside the zip file.
        excludes: List specifying files to exclude.
    """
    if excludes is None:
        excludes = []

    if logger is None:
        logger = default_logger

    if package_exists(pkg_uri):
        return False

    package_file = Path(_get_local_path(base_directory, pkg_uri))
    create_package(
        directory,
        package_file,
        include_parent_dir=include_parent_dir,
        excludes=excludes)

    upload_package_to_gcs(pkg_uri, package_file.read_bytes())

    # Remove the local file to avoid accumulating temporary zip files.
    package_file.unlink()

    return True


def download_and_unpack_package(
        pkg_uri: str,
        base_directory: str,
        logger: Optional[logging.Logger] = default_logger,
) -> str:
    """Download the package corresponding to this URI and unpack it.

    Will be written to a directory named {base_directory}/{uri}.
    """
    pkg_file = Path(_get_local_path(base_directory, pkg_uri))
    with FileLock(str(pkg_file) + ".lock"):
        if logger is None:
            logger = default_logger

        logger.debug(f"Fetching package for URI: {pkg_uri}")

        local_dir = pkg_file.with_suffix("")
        assert local_dir != pkg_file, "Invalid pkg_file!"
        if local_dir.exists():
            assert local_dir.is_dir(), f"{local_dir} is not a directory"
        else:
            protocol, pkg_name = parse_uri(pkg_uri)
            if protocol == Protocol.GCS:
                # Download package from the GCS.
                code = _internal_kv_get(pkg_uri)
                if code is None:
                    raise IOError(f"Failed to fetch URI {pkg_uri} from GCS.")
                code = code or b""
                pkg_file.write_bytes(code)
            elif protocol == Protocol.S3:
                # Download package from S3.
                try:
                    from smart_open import open
                    import boto3
                except ImportError:
                    raise ImportError(
                        "You must `pip install smart_open` and "
                        "`pip install boto3` to fetch URIs in s3 "
                        "bucket.")

                tp = {"client": boto3.client("s3")}
                with open(pkg_uri, "rb", transport_params=tp) as package_zip:
                    with open(pkg_file, "wb") as fin:
                        fin.write(package_zip.read())
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
