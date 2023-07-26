import fnmatch
import logging
import os
from pathlib import Path
import shutil
from typing import List, Optional

try:
    import fsspec
    from fsspec.implementations.local import LocalFileSystem

except ImportError:
    fsspec = None
    LocalFileSystem = object

try:
    import pyarrow
    import pyarrow.fs

except (ImportError, ModuleNotFoundError) as e:
    raise RuntimeError(
        "pyarrow is a required dependency of Ray Train and Ray Tune. "
        "Please install with: `pip install pyarrow`"
    ) from e

from ray.air._internal.filelock import TempFileLock
from ray.air._internal.uri_utils import is_uri


logger = logging.getLogger(__file__)


class _ExcludingLocalFilesystem(LocalFileSystem):
    """LocalFileSystem wrapper to exclude files according to patterns.

    Args:
        exclude: List of patterns that are applied to files returned by
            ``self.find()``. If a file path matches this pattern, it will
            be excluded.

    """

    def __init__(self, exclude: List[str], **kwargs):
        super().__init__(**kwargs)
        self._exclude = exclude

    @property
    def fsid(self):
        return "_excluding_local"

    def _should_exclude(self, name: str) -> bool:
        """Return True if `name` matches any of the `self._exclude` patterns."""
        alt = None
        if os.path.isdir(name):
            # If this is a directory, also test it with trailing slash
            alt = os.path.join(name, "")
        for excl in self._exclude:
            if fnmatch.fnmatch(name, excl):
                return True
            if alt and fnmatch.fnmatch(alt, excl):
                return True
        return False

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        """Call parent find() and exclude from result."""
        names = super().find(
            path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs
        )
        if detail:
            return {
                name: out
                for name, out in names.items()
                if not self._should_exclude(name)
            }
        else:
            return [name for name in names if not self._should_exclude(name)]


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


# TODO(justinvyu): Add unit tests for all these utils.


def _delete_fs_path(fs: pyarrow.fs.FileSystem, fs_path: str):
    assert not is_uri(fs_path), fs_path

    try:
        fs.delete_dir(fs_path)
    except Exception:
        logger.exception(f"Caught exception when deleting path at ({fs}, {fs_path}):")


def _download_from_fs_path(
    fs: pyarrow.fs.FileSystem,
    fs_path: str,
    local_path: str,
    filelock: bool = True,
):
    """Downloads a directory or file from (fs, fs_path) to a local path.

    If fs_path points to a directory:
    - The full directory contents are downloaded directly into `local_path`,
      rather than to a subdirectory of `local_path`.

    If fs_path points to a file:
    - The file is downloaded to `local_path`, which is expected to be a file path.

    If the download fails, the `local_path` contents are
    cleaned up before raising, if the directory did not previously exist.

    NOTE: This method creates `local_path`'s parent directories if they do not
    already exist. If the download fails, this does NOT clean up all the parent
    directories that were created.

    Args:
        fs: The filesystem to download from.
        fs_path: The filesystem path (either a directory or a file) to download.
        local_path: The local path to download to.
        filelock: Whether to require a file lock before downloading, useful for
            multiple downloads to the same directory that may be happening in parallel.

    Raises:
        FileNotFoundError: if (fs, fs_path) doesn't exist.
    """
    assert not is_uri(fs_path), fs_path

    _local_path = Path(local_path).resolve()
    exists_before = _local_path.exists()
    if _is_directory(fs=fs, fs_path=fs_path):
        _local_path.mkdir(parents=True, exist_ok=True)
    else:
        _local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if filelock:
            with TempFileLock(f"{os.path.normpath(local_path)}.lock"):
                _pyarrow_fs_copy_files(fs_path, local_path, source_filesystem=fs)
        else:
            _pyarrow_fs_copy_files(fs_path, local_path, source_filesystem=fs)
    except Exception as e:
        # Clean up the directory if downloading was unsuccessful.
        if not exists_before:
            shutil.rmtree(local_path, ignore_errors=True)
        raise e


def _upload_to_fs_path(
    local_path: str,
    fs: pyarrow.fs.FileSystem,
    fs_path: str,
    exclude: Optional[List[str]] = None,
) -> None:
    """Uploads a local directory or file to (fs, fs_path).

    NOTE: This will create all necessary parent directories at the destination.

    Args:
        local_path: The local path to upload.
        fs: The filesystem to upload to.
        fs_path: The filesystem path where the dir/file will be uploaded to.
        exclude: A list of filename matches to exclude from upload. This includes
            all files under subdirectories as well.
            Ex: ["*.png"] to exclude all .png images.
    """
    assert not is_uri(fs_path), fs_path

    if not exclude:
        # TODO(justinvyu): uploading a single file doesn't work
        # (since we always create a directory at fs_path)
        _create_directory(fs=fs, fs_path=fs_path)
        _pyarrow_fs_copy_files(local_path, fs_path, destination_filesystem=fs)
    elif fsspec:
        # If fsspec is available, prefer it because it's more efficient than
        # calling pyarrow.fs.copy_files multiple times
        _upload_to_uri_with_exclude_fsspec(
            local_path=local_path, fs=fs, fs_path=fs_path, exclude=exclude
        )
    else:
        # Walk the filetree and upload
        _upload_to_uri_with_exclude_pyarrow(
            local_path=local_path, fs=fs, fs_path=fs_path, exclude=exclude
        )


def _upload_to_uri_with_exclude_fsspec(
    local_path: str, fs: "pyarrow.fs", fs_path: str, exclude: Optional[List[str]]
) -> None:
    local_fs = _ExcludingLocalFilesystem(exclude=exclude)
    handler = pyarrow.fs.FSSpecHandler(local_fs)
    source_fs = pyarrow.fs.PyFileSystem(handler)

    _create_directory(fs=fs, fs_path=fs_path)
    _pyarrow_fs_copy_files(
        local_path, fs_path, source_filesystem=source_fs, destination_filesystem=fs
    )


def _upload_to_uri_with_exclude_pyarrow(
    local_path: str, fs: "pyarrow.fs", fs_path: str, exclude: Optional[List[str]]
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
            full_target_path = os.path.normpath(os.path.join(fs_path, candidate))

            _create_directory(fs=fs, fs_path=str(Path(full_target_path).parent))
            _pyarrow_fs_copy_files(
                full_source_path, full_target_path, destination_filesystem=fs
            )


def _list_at_fs_path(fs: pyarrow.fs.FileSystem, fs_path: str) -> List[str]:
    """Returns the list of filenames at (fs, fs_path), similar to os.listdir.

    If the path doesn't exist, returns an empty list.
    """
    assert not is_uri(fs_path), fs_path

    selector = pyarrow.fs.FileSelector(fs_path, allow_not_found=True, recursive=False)
    return [
        os.path.relpath(file_info.path.lstrip("/"), start=fs_path.lstrip("/"))
        for file_info in fs.get_file_info(selector)
    ]


def _is_directory(fs: pyarrow.fs.FileSystem, fs_path: str) -> bool:
    """Checks if (fs, fs_path) is a directory or a file.

    Raises:
        FileNotFoundError: if (fs, fs_path) doesn't exist.
    """
    assert not is_uri(fs_path), fs_path
    file_info = fs.get_file_info(fs_path)
    return not file_info.is_file


def _create_directory(fs: pyarrow.fs.FileSystem, fs_path: str) -> None:
    """Create directory at (fs, fs_path).

    Some external filesystems require directories to already exist, or at least
    the `netloc` to be created (e.g. PyArrows ``mock://`` filesystem).

    Generally this should be done before and outside of Ray applications. This
    utility is thus primarily used in testing, e.g. of ``mock://` URIs.
    """
    try:
        fs.create_dir(fs_path)
    except Exception:
        logger.exception(
            f"Caught exception when creating directory at ({fs}, {fs_path}):"
        )
