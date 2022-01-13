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
    CONDA = "conda", "For conda environments installed locally on each node."
    PIP = "pip", "For pip environments installed locally on each node."
    HTTPS = "https", ("Remote https path, "
                      "assumes everything packed in one zip file.")
    S3 = "s3", "Remote s3 path, assumes everything packed in one zip file."
    GS = "gs", ("Remote google storage path, "
                "assumes everything packed in one zip file.")

    @classmethod
    def remote_protocols(cls):
        # Returns a lit of protocols that support remote storage
        # These protocols should only be used with paths that end in ".zip"
        return [cls.HTTPS, cls.S3, cls.GS]


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
    For HTTPS URIs, the netloc will have '.' replaced with '_', and
    the path will have '/' replaced with '_'. The package name will be the
    adjusted path with 'https_' prepended.
        urlparse(
            "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
        )
            -> ParseResult(
                scheme='https',
                netloc='github.com',
                path='/shrekris-anyscale/test_repo/archive/HEAD.zip'
            )
            -> ("https",
            "github_com_shrekris-anyscale_test_repo_archive_HEAD.zip")
    For S3 URIs, the bucket and path will have '/' replaced with '_'. The
    package name will be the adjusted path with 's3_' prepended.
        urlparse("s3://bucket/dir/file.zip")
            -> ParseResult(
                scheme='s3',
                netloc='bucket',
                path='/dir/file.zip'
            )
            -> ("s3", "bucket_dir_file.zip")
    For GS URIs, the path will have '/' replaced with '_'. The package name
    will be the adjusted path with 'gs_' prepended.
        urlparse("gs://public-runtime-env-test/test_module.zip")
            -> ParseResult(
                scheme='gs',
                netloc='public-runtime-env-test',
                path='/test_module.zip'
            )
            -> ("gs",
            "gs_public-runtime-env-test_test_module.zip")
    """
    uri = urlparse(pkg_uri)
    protocol = Protocol(uri.scheme)
    if protocol == Protocol.S3 or protocol == Protocol.GS:
        return (protocol,
                f"{protocol.value}_{uri.netloc}{uri.path.replace('/', '_')}")
    elif protocol == Protocol.HTTPS:
        return (
            protocol,
            f"https_{uri.netloc.replace('.', '_')}{uri.path.replace('/', '_')}"
        )
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


def get_uri_for_package(package: Path) -> str:
    """Get a content-addressable URI from a package's contents.
    """

    hash_val = hashlib.md5(package.read_bytes()).hexdigest()
    return "{protocol}://{pkg_name}.zip".format(
        protocol=Protocol.GCS.value, pkg_name=RAY_PKG_PREFIX + hash_val)


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
    elif protocol in Protocol.remote_protocols():
        raise RuntimeError(
            "push_package should not be called with remote path.")
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
                unzip_package(
                    package_path=pkg_file,
                    target_dir=local_dir,
                    remove_top_level_directory=False,
                    unlink_zip=True,
                    logger=logger)
            elif protocol in Protocol.remote_protocols():
                # Download package from remote URI
                tp = None

                if protocol == Protocol.S3:
                    try:
                        from smart_open import open
                        import boto3
                    except ImportError:
                        raise ImportError(
                            "You must `pip install smart_open` and "
                            "`pip install boto3` to fetch URIs in s3 "
                            "bucket.")
                    tp = {"client": boto3.client("s3")}
                elif protocol == Protocol.GS:
                    try:
                        from smart_open import open
                        from google.cloud import storage  # noqa: F401
                    except ImportError:
                        raise ImportError(
                            "You must `pip install smart_open` and "
                            "`pip install google-cloud-storage` "
                            "to fetch URIs in Google Cloud Storage bucket.")
                else:
                    try:
                        from smart_open import open
                    except ImportError:
                        raise ImportError(
                            "You must `pip install smart_open` "
                            f"to fetch {protocol.value.upper()} URIs.")

                with open(pkg_uri, "rb", transport_params=tp) as package_zip:
                    with open(pkg_file, "wb") as fin:
                        fin.write(package_zip.read())

                unzip_package(
                    package_path=pkg_file,
                    target_dir=local_dir,
                    remove_top_level_directory=True,
                    unlink_zip=True,
                    logger=logger)
            else:
                raise NotImplementedError(
                    f"Protocol {protocol} is not supported")

        return str(local_dir)


def get_top_level_dir_from_compressed_package(package_path: str):
    """
    If compressed package at package_path contains a single top-level
    directory, returns the name of the top-level directory. Otherwise,
    returns None.
    """

    package_zip = ZipFile(package_path, "r")
    top_level_directory = None

    for file_name in package_zip.namelist():
        if top_level_directory is None:
            # Cache the top_level_directory name when checking
            # the first file in the zipped package
            if "/" in file_name:
                top_level_directory = file_name.split("/")[0]
            else:
                return None
        else:
            # Confirm that all other files
            # belong to the same top_level_directory
            if "/" not in file_name or \
                    file_name.split("/")[0] != top_level_directory:
                return None

    return top_level_directory


def extract_file_and_remove_top_level_dir(base_dir: str, fname: str,
                                          zip_ref: ZipFile):
    """
    Extracts fname file from zip_ref zip file, removes the top level directory
    from fname's file path, and stores fname in the base_dir.
    """

    fname_without_top_level_dir = "/".join(fname.split("/")[1:])

    # If this condition is false, it means there was no top-level directory,
    # so we do nothing
    if fname_without_top_level_dir:
        zip_ref.extract(fname, base_dir)
        os.rename(
            os.path.join(base_dir, fname),
            os.path.join(base_dir, fname_without_top_level_dir))


def unzip_package(package_path: str,
                  target_dir: str,
                  remove_top_level_directory: bool,
                  unlink_zip: bool,
                  logger: Optional[logging.Logger] = default_logger):
    """
    Unzip the compressed package contained at package_path and store the
    contents in target_dir. If remove_top_level_directory is True, the function
    will automatically remove the top_level_directory and store the contents
    directly in target_dir. If unlink_zip is True, the function will unlink the
    zip file stored at package_path.
    """
    try:
        os.mkdir(target_dir)
    except FileExistsError:
        logger.info(f"Directory at {target_dir} already exists")

    logger.debug(f"Unpacking {package_path} to {target_dir}")

    if remove_top_level_directory:
        top_level_directory = get_top_level_dir_from_compressed_package(
            package_path)
        if top_level_directory is None:
            raise ValueError("The package at package_path must contain "
                             "a single top level directory. Make sure there "
                             "are no hidden files at the same level as the "
                             "top level directory.")
        with ZipFile(str(package_path), "r") as zip_ref:
            for fname in zip_ref.namelist():
                extract_file_and_remove_top_level_dir(
                    base_dir=target_dir, fname=fname, zip_ref=zip_ref)

            # Remove now-empty top_level_directory and any empty subdirectories
            # left over from extract_file_and_remove_top_level_dir operations
            leftover_top_level_directory = os.path.join(
                target_dir, top_level_directory)
            if os.path.isdir(leftover_top_level_directory):
                shutil.rmtree(leftover_top_level_directory)
    else:
        with ZipFile(str(package_path), "r") as zip_ref:
            zip_ref.extractall(target_dir)

    if unlink_zip:
        Path(package_path).unlink()


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
