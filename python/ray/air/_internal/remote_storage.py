import fnmatch
import os
import urllib.parse
from pathlib import Path
from pkg_resources import packaging
import shutil
from typing import List, Optional, Tuple

from ray.air._internal.filelock import TempFileLock

try:
    import fsspec

except ImportError:
    fsspec = None

try:
    import pyarrow
    import pyarrow.fs

    # TODO(krfricke): Remove this once gcsfs > 2022.3.0 is released
    # (and make sure to pin)
    class _CustomGCSHandler(pyarrow.fs.FSSpecHandler):
        """Custom FSSpecHandler that avoids a bug in gcsfs <= 2022.3.0."""

        def create_dir(self, path, recursive):
            try:
                # GCSFS doesn't expose `create_parents` argument,
                # so it is omitted here
                self.fs.mkdir(path)
            except FileExistsError:
                pass

except (ImportError, ModuleNotFoundError):
    pyarrow = None
    _CustomGCSHandler = None

from ray import logger


def _pyarrow_fs_copy_files(
    source, destination, source_filesystem=None, destination_filesystem=None, **kwargs
):
    if isinstance(source_filesystem, pyarrow.fs.S3FileSystem) or isinstance(
        destination_filesystem, pyarrow.fs.S3FileSystem
    ):
        # Workaround multi-threading issue with pyarrow
        # https://github.com/apache/arrow/issues/32372
        kwargs.setdefault("use_threads", False)

    return pyarrow.fs.copy_files(
        source,
        destination,
        source_filesystem=source_filesystem,
        destination_filesystem=destination_filesystem,
        **kwargs,
    )


def _assert_pyarrow_installed():
    if pyarrow is None:
        raise RuntimeError(
            "Uploading, downloading, and deleting from cloud storage "
            "requires pyarrow to be installed. Install with: "
            "`pip install pyarrow`. Subsequent calls to cloud operations "
            "will be ignored."
        )


def fs_hint(uri: str) -> str:
    """Return a hint how to install required filesystem package"""
    if pyarrow is None:
        return "Please make sure PyArrow is installed: `pip install pyarrow`."
    if fsspec is None:
        return "Try installing fsspec: `pip install fsspec`."

    from fsspec.registry import known_implementations

    protocol = urllib.parse.urlparse(uri).scheme

    if protocol in known_implementations:
        return known_implementations[protocol]["err"]

    return "Make sure to install and register your fsspec-compatible filesystem."


def is_non_local_path_uri(uri: str) -> bool:
    """Check if target URI points to a non-local location"""
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme == "file" or not parsed.scheme:
        return False

    if bool(get_fs_and_path(uri)[0]):
        return True

    return False


# Cache fs objects
_cached_fs = {}


def get_fs_and_path(
    uri: str,
) -> Tuple[Optional["pyarrow.fs.FileSystem"], Optional[str]]:
    if not pyarrow:
        return None, None

    parsed = urllib.parse.urlparse(uri)
    # for uri="hdfs://48bb8ca83706:8020/test":
    # netloc="48bb8ca83706:8020/"
    # netloc's information is taken into account from the pyarrow client.
    # so path should not include netloc.
    # On the other hand, for uri="s3://my_bucket/test":
    # netloc="my_bucket/" and path="test/"
    # netloc's information is not part of the pyarrow client.
    # so path should include netloc information hence the concatenation.
    if uri.startswith("hdfs://"):
        path = parsed.path
    else:
        path = parsed.netloc + parsed.path

    cache_key = (parsed.scheme, parsed.netloc)

    if cache_key in _cached_fs:
        fs = _cached_fs[cache_key]
        return fs, path

    # In case of hdfs filesystem, if uri does not have the netloc part below will
    # fail with hdfs access error.  For example 'hdfs:///user_folder/...' will
    # fail, while only 'hdfs://namenode_server/user_foler/...' will work
    # we consider the two cases of uri: short_hdfs_uri or other_uri,
    # other_uri includes long hdfs uri and other filesystem uri, like s3 or gcp
    # filesystem. Two cases of imported module of fsspec: yes or no. So we need
    # to handle 4 cases:
    # (uri,             fsspec)
    # (short_hdfs_uri,  yes) --> use fsspec
    # (short_hdfs_uri,  no) --> return None and avoid init pyarrow
    # (other_uri,       yes) --> try pyarrow, if throw use fsspec
    # (other_uri,       no) --> try pyarrow, if throw return None
    short_hdfs_uri = parsed.scheme == "hdfs" and parsed.netloc == ""
    try:
        if short_hdfs_uri and not fsspec:
            return None, None
        if not short_hdfs_uri:
            fs, path = pyarrow.fs.FileSystem.from_uri(uri)
            _cached_fs[cache_key] = fs
            return fs, path
    except (pyarrow.lib.ArrowInvalid, pyarrow.lib.ArrowNotImplementedError):
        # Raised when URI not recognized
        if not fsspec:
            # Only return if fsspec is not installed
            return None, None

    # Else, try to resolve protocol via fsspec
    try:
        fsspec_fs = fsspec.filesystem(parsed.scheme)
    except ValueError:
        # Raised when protocol not known
        return None, None

    fsspec_handler = pyarrow.fs.FSSpecHandler
    if parsed.scheme in ["gs", "gcs"]:

        # TODO(amogkam): Remove after https://github.com/fsspec/gcsfs/issues/498 is
        #  resolved.
        try:
            import gcsfs

            # For minimal install that only needs python3-setuptools
            if packaging.version.parse(gcsfs.__version__) > packaging.version.parse(
                "2022.7.1"
            ):
                raise RuntimeError(
                    "`gcsfs` versions greater than '2022.7.1' are not "
                    f"compatible with pyarrow. You have gcsfs version "
                    f"{gcsfs.__version__}. Please downgrade your gcsfs "
                    f"version. See more details in "
                    f"https://github.com/fsspec/gcsfs/issues/498."
                )
        except ImportError:
            pass

        # GS doesn't support `create_parents` arg in `create_dir()`
        fsspec_handler = _CustomGCSHandler

    fs = pyarrow.fs.PyFileSystem(fsspec_handler(fsspec_fs))
    _cached_fs[cache_key] = fs

    return fs, path


def delete_at_uri(uri: str):
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not clear URI contents: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    try:
        fs.delete_dir(bucket_path)
    except Exception as e:
        logger.warning(f"Caught exception when clearing URI `{uri}`: {e}")


def read_file_from_uri(uri: str) -> bytes:
    _assert_pyarrow_installed()

    fs, file_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not download from URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    with fs.open_input_file(file_path) as file:
        return file.read()


def download_from_uri(uri: str, local_path: str, filelock: bool = True):
    """Downloads a directory or file at a URI to a local path.

    If the URI points to a directory, then the full directory contents are
    copied, and `local_path` is the downloaded directory.
    If the download fails, the `local_path` contents are
    cleaned up before raising, if the directory did not previously exist.
    NOTE: This creates `local_path`'s parent directories if they do not
    already exist. If the download fails, this does NOT clean up all the parent
    directories that were created.

    Args:
        uri: The URI to download from.
        local_path: The local path to download to.
        filelock: Whether to require a file lock before downloading, useful for
            multiple downloads to the same directory that may be happening in parallel.

    Raises:
        ValueError: if the URI scheme is not supported.
        FileNotFoundError: if the URI doesn't exist.
    """
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not download from URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    _local_path = Path(local_path)
    exists_before = _local_path.exists()
    if is_directory(uri):
        _local_path.mkdir(parents=True, exist_ok=True)
    try:
        if filelock:
            with TempFileLock(f"{os.path.normpath(local_path)}.lock"):
                _pyarrow_fs_copy_files(bucket_path, local_path, source_filesystem=fs)
        else:
            _pyarrow_fs_copy_files(bucket_path, local_path, source_filesystem=fs)
    except Exception as e:
        # Clean up the directory if downloading was unsuccessful.
        if not exists_before:
            shutil.rmtree(local_path, ignore_errors=True)
        raise e


def upload_to_uri(
    local_path: str, uri: str, exclude: Optional[List[str]] = None
) -> None:
    """Uploads a local directory or file to a URI.

    NOTE: This will create all necessary directories at the URI destination.

    Args:
        local_path: The local path to upload.
        uri: The URI to upload to.
        exclude: A list of filename matches to exclude from upload. This includes
            all files under subdirectories as well.
            Ex: ["*.png"] to exclude all .png images.

    Raises:
        ValueError: if the URI scheme is not supported.
    """
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not upload to URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    if not exclude:
        _ensure_directory(bucket_path, fs=fs)
        _pyarrow_fs_copy_files(local_path, bucket_path, destination_filesystem=fs)
    else:
        # Walk the filetree and upload
        _upload_to_uri_with_exclude(
            local_path=local_path, fs=fs, bucket_path=bucket_path, exclude=exclude
        )


def _upload_to_uri_with_exclude(
    local_path: str, fs: "pyarrow.fs", bucket_path: str, exclude: Optional[List[str]]
) -> None:
    def _should_exclude(candidate: str) -> bool:
        for excl in exclude:
            if fnmatch.fnmatch(candidate, excl):
                return True
        return False

    for root, dirs, files in os.walk(local_path):
        rel_root = os.path.relpath(root, local_path)
        for file in files:
            candidate = os.path.join(rel_root, file)

            if _should_exclude(candidate):
                continue

            full_source_path = os.path.normpath(os.path.join(local_path, candidate))
            full_target_path = os.path.normpath(os.path.join(bucket_path, candidate))

            _ensure_directory(str(Path(full_target_path).parent), fs=fs)
            _pyarrow_fs_copy_files(
                full_source_path, full_target_path, destination_filesystem=fs
            )


def list_at_uri(uri: str) -> List[str]:
    """Returns the list of filenames at a URI (similar to os.listdir).

    If the URI doesn't exist, returns an empty list.
    """
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not list at URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    if not is_non_local_path_uri(uri):
        # Make sure local paths get expanded fully
        bucket_path = os.path.abspath(os.path.expanduser(bucket_path))

    selector = pyarrow.fs.FileSelector(
        bucket_path, allow_not_found=True, recursive=False
    )
    return [
        os.path.relpath(file_info.path.lstrip("/"), start=bucket_path.lstrip("/"))
        for file_info in fs.get_file_info(selector)
    ]


def is_directory(uri: str) -> bool:
    """Checks if a remote URI is a directory or a file.

    Returns:
        bool: True if the URI is a directory. False if it is a file.

    Raises:
        FileNotFoundError: if the URI doesn't exist.
    """
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    file_info = fs.get_file_info(bucket_path)
    return not file_info.is_file


def _ensure_directory(uri: str, fs: Optional["pyarrow.fs.FileSystem"] = None):
    """Create directory at remote URI.

    Some external filesystems require directories to already exist, or at least
    the `netloc` to be created (e.g. PyArrows ``mock://`` filesystem).

    Generally this should be done before and outside of Ray applications. This
    utility is thus primarily used in testing, e.g. of ``mock://` URIs.

    If a ``fs`` is passed, the ``uri`` is expected to be a path on this filesystem.
    Otherwise, the fs and the uri are automatically detected.
    """
    if fs:
        path = uri
    else:
        fs, path = get_fs_and_path(uri)
    try:
        fs.create_dir(path)
    except Exception:
        pass
