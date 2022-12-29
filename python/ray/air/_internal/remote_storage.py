import fnmatch
import os
import pathlib
import sys
import urllib.parse
from pkg_resources import packaging
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


def _is_local_path(uri: str) -> bool:
    """Check if the path points to the local filesystem."""
    if len(uri) >= 1 and uri[0] == "/":
        return True

    if sys.platform == "win32":
        if len(uri) >= 1 and uri[0] == "\\":
            return True
        if (
            len(uri) >= 3
            and uri[1] == ":"
            and (uri[2] == "/" or uri[2] == "\\")
            and uri[0].isalpha()
        ):
            return True
    return False


def get_fs_and_path(
    uri: str,
) -> Tuple[Optional["pyarrow.fs.FileSystem"], Optional[str]]:
    if not pyarrow:
        return None, None

    if _is_local_path(uri):
        # Append protocol such that the downstream operations work
        # properly on Linux and Windows.
        uri = "file://" + pathlib.Path(uri).as_posix()

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
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not download from URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    if filelock:
        with TempFileLock(f"{os.path.normpath(local_path)}.lock"):
            pyarrow.fs.copy_files(bucket_path, local_path, source_filesystem=fs)
    else:
        pyarrow.fs.copy_files(bucket_path, local_path, source_filesystem=fs)


def upload_to_uri(
    local_path: str, uri: str, exclude: Optional[List[str]] = None
) -> None:
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not upload to URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    if not exclude:
        pyarrow.fs.copy_files(local_path, bucket_path, destination_filesystem=fs)
        return

    # Else, walk and upload
    return _upload_to_uri_with_exclude(
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

            pyarrow.fs.copy_files(
                full_source_path, full_target_path, destination_filesystem=fs
            )


def list_at_uri(uri: str) -> List[str]:
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not upload to URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    selector = pyarrow.fs.FileSelector(
        bucket_path, allow_not_found=True, recursive=False
    )
    return [
        os.path.relpath(file_info.path.lstrip("/"), start=bucket_path.lstrip("/"))
        for file_info in fs.get_file_info(selector)
    ]


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
