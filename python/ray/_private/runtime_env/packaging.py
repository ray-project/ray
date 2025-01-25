from typing import Callable, List, Optional, Tuple
import time
import asyncio
import hashlib
import logging
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
from zipfile import ZipFile

from filelock import FileLock
from ray.util.annotations import DeveloperAPI

from ray._private.ray_constants import (
    RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT,
    RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_ENV_VAR,
    RAY_RUNTIME_ENV_IGNORE_GITIGNORE,
    GRPC_CPP_MAX_MESSAGE_SIZE,
)
from ray._private.runtime_env.conda_utils import exec_cmd_stream_to_logger
from ray._private.runtime_env.protocol import Protocol
from ray._private.thirdparty.pathspec import PathSpec
from ray.experimental.internal_kv import (
    _internal_kv_exists,
    _internal_kv_put,
    _pin_runtime_env_uri,
)

default_logger = logging.getLogger(__name__)

# If an individual file is beyond this size, print a warning.
FILE_SIZE_WARNING = 10 * 1024 * 1024  # 10MiB
# The size is bounded by the max gRPC message size.
# Keep in sync with max_grpc_message_size in ray_config_def.h.
GCS_STORAGE_MAX_SIZE = int(
    os.environ.get("RAY_max_grpc_message_size", GRPC_CPP_MAX_MESSAGE_SIZE)
)
RAY_PKG_PREFIX = "_ray_pkg_"

RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR = (
    "RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING"
)
RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING_ENV_VAR = (
    "RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING"
)

# The name of the hidden top-level directory that appears when files are
# zipped on MacOS.
MAC_OS_ZIP_HIDDEN_DIR_NAME = "__MACOSX"


def _mib_string(num_bytes: float) -> str:
    size_mib = float(num_bytes / 1024**2)
    return f"{size_mib:.2f}MiB"


class _AsyncFileLock:
    """Asyncio version used to prevent blocking event loop."""

    def __init__(self, lock_file: str):
        self.file = FileLock(lock_file)

    async def __aenter__(self):
        while True:
            try:
                self.file.acquire(timeout=0)
                return
            except TimeoutError:
                await asyncio.sleep(0.1)

    async def __aexit__(self, exc_type, exc, tb):
        self.file.release()


def _xor_bytes(left: bytes, right: bytes) -> bytes:
    if left and right:
        return bytes(a ^ b for (a, b) in zip(left, right))
    return left or right


def _dir_travel(
    path: Path,
    root: Path,
    excludes: List[Callable],
    includes: Optional[List[Callable]] = None,
    handler: Callable = None,
    logger: Optional[logging.Logger] = default_logger,
):
    """
    Recursively traverses the directory at path, calling the handler on each subpath.
    Supports both `includes` and `excludes`:
    - `excludes`: A list of callables to check if a path should be skipped.
    - `includes`: A list of callables to check if a path should be processed.
                  If `includes` is None, all paths not in `excludes` will be processed.
    Note:
    The priority of excludes is higher than that of includes. Therefore,
    when both are assigned values simultaneously,
    the system will first check excludes, and
    then filter the remaining files using includes.
    If the values of includes and excludes are the same,
    includes will not take effect.

    Args:
        path: The path to start traversal.
        root: The root path to start traversal.
        excludes: A list of functions to exclude paths.
        includes: A list of functions to include paths.
        handler: A function to handle each path.
        logger: Logger for error reporting.
    """

    def _collect_paths(path: Path, excludes: List[Callable]) -> List[Path]:
        """
        Collect all paths in the directory tree, respecting the excludes rules.
        """
        paths = []
        e = _get_gitignore(path)  # Example: Get gitignore rules
        if e is not None:
            excludes.append(e)

        # Check if the path is excluded
        skip = any(e(path) for e in excludes)
        if not skip:
            # Add the current path to the list
            paths.append(path)

        # Recursively collect paths for subdirectories
        if path.is_dir():
            for sub_path in path.iterdir():
                paths.extend(_collect_paths(sub_path, excludes))

        if e is not None:
            excludes.pop()

        return paths

    def _apply_includes_and_handle(
        paths: List[Path],
        includes: Optional[List[Callable]],
        handler: Callable,
        root: Path,
    ):
        """
        Apply includes rules and call the handler on each included path.
        Ensure that paths are processed in order from parent to child.
        """
        # Sort paths by depth (parent directories first)
        paths.sort(key=lambda p: len(p.parts))

        # Set to track paths that need to be processed (to avoid duplicates)
        processed_paths = set()

        for path in paths:
            # Check if the path is included (if includes is specified)
            include = True
            if includes is not None:
                include = any(i(path) for i in includes)

            if include:
                # Add the path and all its parent directories to the processed set
                current = path
                while current != root and current not in processed_paths:
                    processed_paths.add(current)
                    current = current.parent

                # Add the root directory itself
                processed_paths.add(root)

        # Process all collected paths in order
        for path in sorted(processed_paths, key=lambda p: len(p.parts)):
            try:
                # pass path and root as hash_value
                handler(path, root)
            except Exception as e:
                logger.error(f"Issue with path: {path}")
                raise e

    # Phase 1: Collect all paths
    all_paths = _collect_paths(path, excludes)

    # Phase 2: Apply includes and handle paths
    _apply_includes_and_handle(all_paths, includes, handler, root)


def _hash_file_content_or_directory_name(
    filepath: Path,
    relative_path: Path,
    logger: Optional[logging.Logger] = default_logger,
) -> bytes:
    """Helper function to create hash of a single file or directory.

    This function hashes the path of the file or directory,
    and if it's a file, then it hashes its content too.
    """

    BUF_SIZE = 4096 * 1024

    sha1 = hashlib.sha1()
    sha1.update(str(filepath.relative_to(relative_path)).encode())
    if not filepath.is_dir():
        try:
            f = filepath.open("rb")
        except Exception as e:
            logger.debug(
                f"Skipping contents of file {filepath} when calculating package hash "
                f"because the file couldn't be opened: {e}"
            )
        else:
            try:
                data = f.read(BUF_SIZE)
                while len(data) != 0:
                    sha1.update(data)
                    data = f.read(BUF_SIZE)
            finally:
                f.close()

    return sha1.digest()


def _hash_file(
    filepath: Path,
    relative_path: Path,
    logger: Optional[logging.Logger] = default_logger,
) -> bytes:
    """Helper function to create hash of a single file.

    It hashes the path of the file and its content to create a hash value.
    """
    file_hash = _hash_file_content_or_directory_name(
        filepath, relative_path, logger=logger
    )
    return _xor_bytes(file_hash, b"0" * 8)


def _hash_directory(
    root: Path,
    relative_path: Path,
    excludes: Optional[Callable] = None,
    includes: Optional[Callable] = None,
    logger: Optional[logging.Logger] = default_logger,
) -> bytes:
    """Helper function to create hash of a directory.

    It'll go through all the files in the directory and xor
    hash(file_name, file_content) to create a hash value.
    """
    hash_val = b"0" * 8

    def handler(path: Path, root: Path):
        file_hash = _hash_file_content_or_directory_name(path, root, logger=logger)
        nonlocal hash_val
        hash_val = _xor_bytes(hash_val, file_hash)

    excludes = [] if excludes is None else [excludes]
    includes = None if includes is None else [includes]
    _dir_travel(root, relative_path, excludes, includes, handler, logger=logger)
    return hash_val


def parse_uri(pkg_uri: str) -> Tuple[Protocol, str]:
    """
    Parse package uri into protocol and package name based on its format.
    Note that the output of this function is not for handling actual IO, it's
    only for setting up local directory folders by using package name as path.

    >>> parse_uri("https://test.com/file.zip")
    (<Protocol.HTTPS: 'https'>, 'https_test_com_file.zip')

    >>> parse_uri("https://test.com/file.whl")
    (<Protocol.HTTPS: 'https'>, 'file.whl')

    """
    uri = urlparse(pkg_uri)
    try:
        protocol = Protocol(uri.scheme)
    except ValueError as e:
        raise ValueError(
            f'Invalid protocol for runtime_env URI "{pkg_uri}". '
            f"Supported protocols: {Protocol._member_names_}. Original error: {e}"
        )

    if protocol in Protocol.remote_protocols():
        if uri.path.endswith(".whl"):
            # Don't modify the .whl filename. See
            # https://peps.python.org/pep-0427/#file-name-convention
            # for more information.
            package_name = uri.path.split("/")[-1]
        else:
            package_name = f"{protocol.value}_{uri.netloc}{uri.path}"

            disallowed_chars = ["/", ":", "@", "+", " "]
            for disallowed_char in disallowed_chars:
                package_name = package_name.replace(disallowed_char, "_")

            # Remove all periods except the last, which is part of the
            # file extension
            package_name = package_name.replace(".", "_", package_name.count(".") - 1)
    else:
        package_name = uri.netloc

    return (protocol, package_name)


def is_zip_uri(uri: str) -> bool:
    try:
        protocol, path = parse_uri(uri)
    except ValueError:
        return False

    return Path(path).suffix == ".zip"


def is_whl_uri(uri: str) -> bool:
    try:
        _, path = parse_uri(uri)
    except ValueError:
        return False

    return Path(path).suffix == ".whl"


def is_jar_uri(uri: str) -> bool:
    try:
        _, path = parse_uri(uri)
    except ValueError:
        return False

    return Path(path).suffix == ".jar"


def _get_matching_paths(path: Path, pattern_paths: List[str]) -> Callable:
    path = path.absolute()
    pathspec = PathSpec.from_lines("gitwildmatch", pattern_paths)

    def match(p: Path):
        path_str = str(p.absolute().relative_to(path))
        return pathspec.match_file(path_str)

    return match


def _get_gitignore(path: Path) -> Optional[Callable]:
    """Returns a function that returns True if the path should be excluded.

    Returns None if there is no .gitignore file in the path, or if the
    RAY_RUNTIME_ENV_IGNORE_GITIGNORE environment variable is set to 1.

    Args:
        path: The path to the directory to check for a .gitignore file.

    Returns:
        A function that returns True if the path should be excluded.
    """
    ignore_gitignore = os.environ.get(RAY_RUNTIME_ENV_IGNORE_GITIGNORE, "0") == "1"
    if ignore_gitignore:
        return None

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


def pin_runtime_env_uri(uri: str, *, expiration_s: Optional[int] = None) -> None:
    """Pin a reference to a runtime_env URI in the GCS on a timeout.

    This is used to avoid premature eviction in edge conditions for job
    reference counting. See https://github.com/ray-project/ray/pull/24719.

    Packages are uploaded to GCS in order to be downloaded by a runtime env plugin
    (e.g. working_dir, py_modules) after the job starts.

    This function adds a temporary reference to the package in the GCS to prevent
    it from being deleted before the job starts. (See #23423 for the bug where
    this happened.)

    If this reference didn't have an expiration, then if the script exited
    (e.g. via Ctrl-C) before the job started, the reference would never be
    removed, so the package would never be deleted.
    """

    if expiration_s is None:
        expiration_s = int(
            os.environ.get(
                RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_ENV_VAR,
                RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT,
            )
        )
    elif not isinstance(expiration_s, int):
        raise ValueError(f"expiration_s must be an int, got {type(expiration_s)}.")

    if expiration_s < 0:
        raise ValueError(f"expiration_s must be >= 0, got {expiration_s}.")
    elif expiration_s > 0:
        _pin_runtime_env_uri(uri, expiration_s=expiration_s)


def _store_package_in_gcs(
    pkg_uri: str,
    data: bytes,
    logger: Optional[logging.Logger] = default_logger,
) -> int:
    """Stores package data in the Global Control Store (GCS).

    Args:
        pkg_uri: The GCS key to store the data in.
        data: The serialized package's bytes to store in the GCS.
        logger (Optional[logging.Logger]): The logger used by this function.

    Return:
        int: Size of data

    Raises:
        RuntimeError: If the upload to the GCS fails.
        ValueError: If the data's size exceeds GCS_STORAGE_MAX_SIZE.
    """

    file_size = len(data)
    size_str = _mib_string(file_size)
    if len(data) >= GCS_STORAGE_MAX_SIZE:
        raise ValueError(
            f"Package size ({size_str}) exceeds the maximum size of "
            f"{_mib_string(GCS_STORAGE_MAX_SIZE)}. You can exclude large "
            "files using the 'excludes' option to the runtime_env or provide "
            "a remote URI of a zip file using protocols such as 's3://', "
            "'https://' and so on, refer to "
            "https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#api-reference."  # noqa
        )

    logger.info(f"Pushing file package '{pkg_uri}' ({size_str}) to Ray cluster...")
    try:
        if os.environ.get(RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR):
            raise RuntimeError(
                "Simulating failure to upload package for testing purposes."
            )
        _internal_kv_put(pkg_uri, data)
    except Exception as e:
        raise RuntimeError(
            "Failed to store package in the GCS.\n"
            f"  - GCS URI: {pkg_uri}\n"
            f"  - Package data ({size_str}): {data[:15]}...\n"
        ) from e
    logger.info(f"Successfully pushed file package '{pkg_uri}'.")
    return len(data)


def _get_local_path(base_directory: str, pkg_uri: str) -> str:
    _, pkg_name = parse_uri(pkg_uri)
    return os.path.join(base_directory, pkg_name)


def _zip_files(
    path_str: str,
    excludes: List[str],
    includes: List[str],
    output_path: str,
    include_parent_dir: bool = False,
    logger: Optional[logging.Logger] = default_logger,
) -> None:
    """Zip the target file or directory and write it to the output_path.

    path_str: The file or directory to zip.
    excludes (List(str)): The directories or file to be excluded.
    output_path: The output path for the zip file.
    include_parent_dir: If true, includes the top-level directory as a
        directory inside the zip file.
    """
    pkg_file = Path(output_path).absolute()
    with ZipFile(pkg_file, "w", strict_timestamps=False) as zip_handler:
        # Put all files in the directory into the zip file.
        file_path = Path(path_str).absolute()
        dir_path = file_path
        if file_path.is_file():
            dir_path = file_path.parent

        def handler(path: Path, root: Path):
            # Pack this path if it's an empty directory or it's a file.
            if path.is_dir() and next(path.iterdir(), None) is None or path.is_file():
                file_size = path.stat().st_size
                if file_size >= FILE_SIZE_WARNING:
                    logger.warning(
                        f"File {path} is very large "
                        f"({_mib_string(file_size)}). Consider adding this "
                        "file to the 'excludes' list to skip uploading it: "
                        "`ray.init(..., "
                        f"runtime_env={{'excludes': ['{path}']}})`"
                    )
                to_path = path.relative_to(dir_path)
                if include_parent_dir:
                    to_path = dir_path.name / to_path
                zip_handler.write(path, to_path)

        excludes = (
            []
            if excludes is None or len(excludes) == 0
            else [_get_matching_paths(file_path, excludes)]
        )
        includes = (
            []
            if includes is None or len(includes) == 0
            else [_get_matching_paths(file_path, includes)]
        )
        _dir_travel(file_path, file_path, excludes, includes, handler, logger=logger)


def package_exists(pkg_uri: str) -> bool:
    """Check whether the package with given URI exists or not.

    Args:
        pkg_uri: The uri of the package

    Return:
        True for package existing and False for not.
    """
    protocol, pkg_name = parse_uri(pkg_uri)
    if protocol == Protocol.GCS:
        return _internal_kv_exists(pkg_uri)
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def get_uri_for_package(package: Path) -> str:
    """Get a content-addressable URI from a package's contents."""

    if package.suffix == ".whl":
        # Wheel file names include the Python package name, version
        # and tags, so it is already effectively content-addressed.
        return "{protocol}://{whl_filename}".format(
            protocol=Protocol.GCS.value, whl_filename=package.name
        )
    else:
        hash_val = hashlib.sha1(package.read_bytes()).hexdigest()
        return "{protocol}://{pkg_name}.zip".format(
            protocol=Protocol.GCS.value, pkg_name=RAY_PKG_PREFIX + hash_val
        )


def get_uri_for_file(file: str) -> str:
    """Get a content-addressable URI from a file's content.

    This function generates the name of the package by the file.
    The final package name is _ray_pkg_<HASH_VAL>.zip of this package,
    where HASH_VAL is the hash value of the file.
    For example: _ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip

    Examples:

        >>> get_uri_for_file("/my_file.py")  # doctest: +SKIP
        _ray_pkg_af2734982a741.zip

    Args:
        file: The file.

    Returns:
        URI (str)

    Raises:
        ValueError if the file doesn't exist.
    """
    filepath = Path(file).absolute()
    if not filepath.exists() or not filepath.is_file():
        raise ValueError(f"File {filepath} must be an existing file")

    hash_val = _hash_file(filepath, filepath.parent)

    return "{protocol}://{pkg_name}.zip".format(
        protocol=Protocol.GCS.value, pkg_name=RAY_PKG_PREFIX + hash_val.hex()
    )


def get_uri_for_directory(
    directory: str,
    excludes: Optional[List[str]] = None,
    includes: Optional[List[str]] = None,
) -> str:
    """Get a content-addressable URI from a directory's contents.

    This function generates the name of the package by the directory.
    It'll go through all the files in the directory and hash the contents
    of the files to get the hash value of the package.
    The final package name is _ray_pkg_<HASH_VAL>.zip of this package.
    For example: _ray_pkg_029f88d5ecc55e1e4d64fc6e388fd103.zip

    Examples:

        >>> get_uri_for_directory("/my_directory")  # doctest: +SKIP
        _ray_pkg_af2734982a741.zip

    Args:
        directory: The directory.
        excludes (list[str]): The dir or files that should be excluded.
        includes (list[str]): The dir or files that should be included.

    Returns:
        URI (str)

    Raises:
        ValueError if the directory doesn't exist.
    """
    directory = Path(directory).absolute()
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"directory {directory} must be an existing directory")

    excludes_matching_paths_call = (
        None if excludes is None else _get_matching_paths(directory, excludes)
    )
    includes_matching_paths_call = (
        None if includes is None else _get_matching_paths(directory, includes)
    )

    hash_val = _hash_directory(
        directory, directory, excludes_matching_paths_call, includes_matching_paths_call
    )

    return "{protocol}://{pkg_name}.zip".format(
        protocol=Protocol.GCS.value, pkg_name=RAY_PKG_PREFIX + hash_val.hex()
    )


def upload_package_to_gcs(pkg_uri: str, pkg_bytes: bytes) -> None:
    """Upload a local package to GCS.

    Args:
        pkg_uri: The URI of the package, e.g. gcs://my_package.zip
        pkg_bytes: The data to be uploaded.

    Raises:
        RuntimeError: If the upload fails.
        ValueError: If the pkg_uri is a remote path or if the data's
            size exceeds GCS_STORAGE_MAX_SIZE.
        NotImplementedError: If the protocol of the URI is not supported.

    """
    protocol, pkg_name = parse_uri(pkg_uri)
    if protocol == Protocol.GCS:
        _store_package_in_gcs(pkg_uri, pkg_bytes)
    elif protocol in Protocol.remote_protocols():
        raise ValueError(
            "upload_package_to_gcs should not be called with a remote path."
        )
    else:
        raise NotImplementedError(f"Protocol {protocol} is not supported")


def create_package(
    module_path: str,
    target_path: Path,
    include_parent_dir: bool = False,
    excludes: Optional[List[str]] = None,
    includes: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = default_logger,
):
    if excludes is None:
        excludes = []

    if includes is None:
        includes = []

    if logger is None:
        logger = default_logger

    if not target_path.exists():
        logger.info(f"Creating a file package for local module '{module_path}'.")
        _zip_files(
            module_path,
            excludes,
            includes,
            str(target_path),
            include_parent_dir=include_parent_dir,
            logger=logger,
        )


def upload_package_if_needed(
    pkg_uri: str,
    base_directory: str,
    module_path: str,
    include_parent_dir: bool = False,
    excludes: Optional[List[str]] = None,
    includes: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = default_logger,
) -> bool:
    """Upload the contents of the directory under the given URI.

    This will first create a temporary zip file under the passed
    base_directory.

    If the package already exists in storage, this is a no-op.

    Args:
        pkg_uri: URI of the package to upload.
        base_directory: Directory where package files are stored.
        module_path: The module to be uploaded, either a single .py file or a directory.
        include_parent_dir: If true, includes the top-level directory as a
            directory inside the zip file.
        excludes: List specifying files to exclude.
        includes: List specifying files to include.

    Raises:
        RuntimeError: If the upload fails.
        ValueError: If the pkg_uri is a remote path or if the data's
            size exceeds GCS_STORAGE_MAX_SIZE.
        NotImplementedError: If the protocol of the URI is not supported.
    """
    if excludes is None:
        excludes = []

    if logger is None:
        logger = default_logger

    pin_runtime_env_uri(pkg_uri)

    if package_exists(pkg_uri):
        return False

    package_file = Path(_get_local_path(base_directory, pkg_uri))
    # Make the temporary zip file name unique so that it doesn't conflict with
    # concurrent upload_package_if_needed calls with the same pkg_uri.
    # See https://github.com/ray-project/ray/issues/47471.
    package_file = package_file.with_name(
        f"{time.time_ns()}_{os.getpid()}_{package_file.name}"
    )

    create_package(
        module_path,
        package_file,
        include_parent_dir=include_parent_dir,
        excludes=excludes,
        includes=includes,
    )
    package_file_bytes = package_file.read_bytes()
    # Remove the local file to avoid accumulating temporary zip files.
    package_file.unlink()

    upload_package_to_gcs(pkg_uri, package_file_bytes)

    return True


def get_local_dir_from_uri(uri: str, base_directory: str) -> Path:
    """Return the local directory corresponding to this URI."""
    pkg_file = Path(_get_local_path(base_directory, uri))
    local_dir = pkg_file.with_suffix("")
    return local_dir


@DeveloperAPI
async def download_and_unpack_package(
    pkg_uri: str,
    base_directory: str,
    gcs_aio_client: Optional["GcsAioClient"] = None,  # noqa: F821
    logger: Optional[logging.Logger] = default_logger,
    overwrite: bool = False,
) -> str:
    """Download the package corresponding to this URI and unpack it if zipped.

    Will be written to a file or directory named {base_directory}/{uri}.
    Returns the path to this file or directory.

    Args:
        pkg_uri: URI of the package to download.
        base_directory: Directory to use as the parent directory of the target
            directory for the unpacked files.
        gcs_aio_client: Client to use for downloading from the GCS.
        logger: The logger to use.
        overwrite: If True, overwrite the existing package.

    Returns:
        Path to the local directory containing the unpacked package files.

    Raises:
        IOError: If the download fails.
        ImportError: If smart_open is not installed and a remote URI is used.
        NotImplementedError: If the protocol of the URI is not supported.
        ValueError: If the GCS client is not provided when downloading from GCS,
                    or if package URI is invalid.

    """
    pkg_file = Path(_get_local_path(base_directory, pkg_uri))
    if pkg_file.suffix == "":
        raise ValueError(
            f"Invalid package URI: {pkg_uri}."
            "URI must have a file extension and the URI must be valid."
        )

    async with _AsyncFileLock(str(pkg_file) + ".lock"):
        if logger is None:
            logger = default_logger

        logger.debug(f"Fetching package for URI: {pkg_uri}")

        local_dir = get_local_dir_from_uri(pkg_uri, base_directory)
        assert local_dir != pkg_file, "Invalid pkg_file!"

        download_package: bool = True
        if local_dir.exists() and not overwrite:
            download_package = False
            assert local_dir.is_dir(), f"{local_dir} is not a directory"
        elif local_dir.exists():
            logger.info(f"Removing {local_dir} with pkg_file {pkg_file}")
            shutil.rmtree(local_dir)

        if download_package:
            protocol, _ = parse_uri(pkg_uri)
            logger.info(
                f"Downloading package from {pkg_uri} to {pkg_file} "
                f"with protocol {protocol}"
            )
            if protocol == Protocol.GCS:
                if gcs_aio_client is None:
                    raise ValueError(
                        "GCS client must be provided to download from GCS."
                    )

                # Download package from the GCS.
                code = await gcs_aio_client.internal_kv_get(
                    pkg_uri.encode(), namespace=None, timeout=None
                )
                if os.environ.get(RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING_ENV_VAR):
                    code = None
                if code is None:
                    raise IOError(
                        f"Failed to download runtime_env file package {pkg_uri} "
                        "from the GCS to the Ray worker node. The package may "
                        "have prematurely been deleted from the GCS due to a "
                        "long upload time or a problem with Ray. Try setting the "
                        "environment variable "
                        f"{RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_ENV_VAR} "
                        " to a value larger than the upload time in seconds "
                        "(the default is "
                        f"{RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT}). "
                        "If this fails, try re-running "
                        "after making any change to a file in the file package."
                    )
                code = code or b""
                pkg_file.write_bytes(code)

                if is_zip_uri(pkg_uri):
                    unzip_package(
                        package_path=pkg_file,
                        target_dir=local_dir,
                        remove_top_level_directory=False,
                        unlink_zip=True,
                        logger=logger,
                    )
                else:
                    return str(pkg_file)
            elif protocol in Protocol.remote_protocols():
                protocol.download_remote_uri(source_uri=pkg_uri, dest_file=pkg_file)

                if pkg_file.suffix in [".zip", ".jar"]:
                    unzip_package(
                        package_path=pkg_file,
                        target_dir=local_dir,
                        remove_top_level_directory=True,
                        unlink_zip=True,
                        logger=logger,
                    )
                elif pkg_file.suffix == ".whl":
                    return str(pkg_file)
                else:
                    raise NotImplementedError(
                        f"Package format {pkg_file.suffix} is ",
                        "not supported for remote protocols",
                    )
            else:
                raise NotImplementedError(f"Protocol {protocol} is not supported")

        return str(local_dir)


def get_top_level_dir_from_compressed_package(package_path: str):
    """
    If compressed package at package_path contains a single top-level
    directory, returns the name of the top-level directory. Otherwise,
    returns None.

    Ignores a second top-level directory if it is named __MACOSX.
    """

    package_zip = ZipFile(package_path, "r")
    top_level_directory = None

    def is_top_level_file(file_name):
        return "/" not in file_name

    def base_dir_name(file_name):
        return file_name.split("/")[0]

    for file_name in package_zip.namelist():
        if top_level_directory is None:
            # Cache the top_level_directory name when checking
            # the first file in the zipped package
            if is_top_level_file(file_name):
                return None
            else:
                # Top-level directory, or non-top-level file or directory
                dir_name = base_dir_name(file_name)
                if dir_name == MAC_OS_ZIP_HIDDEN_DIR_NAME:
                    continue
                top_level_directory = dir_name
        else:
            # Confirm that all other files
            # belong to the same top_level_directory
            if is_top_level_file(file_name) or base_dir_name(file_name) not in [
                top_level_directory,
                MAC_OS_ZIP_HIDDEN_DIR_NAME,
            ]:
                return None

    return top_level_directory


def remove_dir_from_filepaths(base_dir: str, rdir: str):
    """
    base_dir: String path of the directory containing rdir
    rdir: String path of directory relative to base_dir whose contents should
          be moved to its base_dir, its parent directory

    Removes rdir from the filepaths of all files and directories inside it.
    In other words, moves all the files inside rdir to the directory that
    contains rdir. Assumes base_dir's contents and rdir's contents have no
    name conflicts.
    """

    # Move rdir to a temporary directory, so its contents can be moved to
    # base_dir without any name conflicts
    with TemporaryDirectory() as tmp_dir:
        # shutil.move() is used instead of os.rename() in case rdir and tmp_dir
        # are located on separate file systems
        shutil.move(os.path.join(base_dir, rdir), os.path.join(tmp_dir, rdir))

        # Shift children out of rdir and into base_dir
        rdir_children = os.listdir(os.path.join(tmp_dir, rdir))
        for child in rdir_children:
            shutil.move(
                os.path.join(tmp_dir, rdir, child), os.path.join(base_dir, child)
            )


def unzip_package(
    package_path: str,
    target_dir: str,
    remove_top_level_directory: bool,
    unlink_zip: bool,
    logger: Optional[logging.Logger] = default_logger,
) -> None:
    """
    Unzip the compressed package contained at package_path to target_dir.

    If remove_top_level_directory is True and the top level consists of a
    a single directory (or possibly also a second hidden directory named
    __MACOSX at the top level arising from macOS's zip command), the function
    will automatically remove the top-level directory and store the contents
    directly in target_dir.

    Otherwise, if remove_top_level_directory is False or if the top level
    consists of multiple files or directories (not counting __MACOS),
    the zip contents will be stored in target_dir.

    Args:
        package_path: String path of the compressed package to unzip.
        target_dir: String path of the directory to store the unzipped contents.
        remove_top_level_directory: Whether to remove the top-level directory
            from the zip contents.
        unlink_zip: Whether to unlink the zip file stored at package_path.
        logger: Optional logger to use for logging.

    """
    try:
        os.mkdir(target_dir)
    except FileExistsError:
        logger.info(f"Directory at {target_dir} already exists")

    logger.debug(f"Unpacking {package_path} to {target_dir}")

    with ZipFile(str(package_path), "r") as zip_ref:
        zip_ref.extractall(target_dir)
    if remove_top_level_directory:
        top_level_directory = get_top_level_dir_from_compressed_package(package_path)
        if top_level_directory is not None:
            # Remove __MACOSX directory if it exists
            macos_dir = os.path.join(target_dir, MAC_OS_ZIP_HIDDEN_DIR_NAME)
            if os.path.isdir(macos_dir):
                shutil.rmtree(macos_dir)

            remove_dir_from_filepaths(target_dir, top_level_directory)

    if unlink_zip:
        Path(package_path).unlink()


def delete_package(pkg_uri: str, base_directory: str) -> Tuple[bool, int]:
    """Deletes a specific URI from the local filesystem.

    Args:
        pkg_uri: URI to delete.

    Returns:
        bool: True if the URI was successfully deleted, else False.
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


async def install_wheel_package(
    wheel_uri: str,
    target_dir: str,
    logger: Optional[logging.Logger] = default_logger,
) -> None:
    """Install packages in the wheel URI, and then delete the local wheel file."""

    pip_install_cmd = [
        "pip",
        "install",
        wheel_uri,
        f"--target={target_dir}",
    ]

    logger.info("Running py_modules wheel install command: %s", str(pip_install_cmd))
    try:
        # TODO(architkulkarni): Use `await check_output_cmd` or similar.
        exit_code, output = exec_cmd_stream_to_logger(pip_install_cmd, logger)
    finally:
        if Path(wheel_uri).exists():
            Path(wheel_uri).unlink()

        if exit_code != 0:
            if Path(target_dir).exists():
                Path(target_dir).unlink()
            raise RuntimeError(
                f"Failed to install py_modules wheel {wheel_uri}"
                f"to {target_dir}:\n{output}"
            )
