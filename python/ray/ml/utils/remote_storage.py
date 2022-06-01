import urllib.parse
from filelock import FileLock
from typing import Optional, Tuple

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

# We keep these constants for legacy compatibility with Tune's sync client
# After Tune fully moved to using pyarrow.fs we can remove these.
S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
HDFS_PREFIX = "hdfs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX, HDFS_PREFIX)


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
    # Keep manual check for prefixes for backwards compatibility with the
    # TrialCheckpoint class. Remove once fully deprecated.
    # Deprecated: Remove in Ray > 1.13
    if any(uri.startswith(p) for p in ALLOWED_REMOTE_PREFIXES):
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
    path = parsed.netloc + parsed.path

    cache_key = (parsed.scheme, parsed.netloc)

    if cache_key in _cached_fs:
        fs = _cached_fs[cache_key]
        return fs, path

    try:
        fs, path = pyarrow.fs.FileSystem.from_uri(uri)
        _cached_fs[cache_key] = fs
        return fs, path
    except pyarrow.lib.ArrowInvalid:
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


def download_from_uri(uri: str, local_path: str):
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not download from URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    with FileLock(f"{local_path}.lock"):
        pyarrow.fs.copy_files(bucket_path, local_path, source_filesystem=fs)


def upload_to_uri(local_path: str, uri: str):
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not upload to URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    pyarrow.fs.copy_files(local_path, bucket_path, destination_filesystem=fs)


def _ensure_directory(uri: str):
    """Create directory at remote URI.

    Some external filesystems require directories to already exist, or at least
    the `netloc` to be created (e.g. PyArrows ``mock://`` filesystem).

    Generally this should be done before and outside of Ray applications. This
    utility is thus primarily used in testing, e.g. of ``mock://` URIs.
    """
    fs, path = get_fs_and_path(uri)
    try:
        fs.create_dir(path)
    except Exception:
        pass
