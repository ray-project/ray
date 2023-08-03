import dataclasses
import fnmatch
import logging
import os
from pathlib import Path
import shutil
from typing import Callable, Dict, List, Optional, Tuple

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
from ray.air._internal.uri_utils import URI, is_uri
from ray.tune.syncer import Syncer, SyncConfig, _BackgroundSyncer
from ray.tune.result import _get_defaults_results_dir


logger = logging.getLogger(__file__)


def _use_storage_context() -> bool:
    # Whether to enable the new simple persistence mode.
    return bool(int(os.environ.get("RAY_AIR_NEW_PERSISTENCE_MODE", "0")))


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
        return

    if not fsspec:
        # TODO(justinvyu): Make fsspec a hard requirement of Tune/Train.
        raise RuntimeError("fsspec is required to upload with exclude patterns.")

    _upload_to_uri_with_exclude_fsspec(
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


def _exists_at_fs_path(fs: pyarrow.fs.FileSystem, fs_path: str) -> bool:
    """Returns True if (fs, fs_path) exists."""
    assert not is_uri(fs_path), fs_path

    valid = fs.get_file_info([fs_path])[0]
    return valid.type != pyarrow.fs.FileType.NotFound


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


class _FilesystemSyncer(_BackgroundSyncer):
    """Syncer between local filesystem and a `storage_filesystem`."""

    def __init__(self, storage_filesystem: Optional["pyarrow.fs.FileSystem"], **kwargs):
        self.storage_filesystem = storage_filesystem
        super().__init__(**kwargs)

    def _sync_up_command(
        self, local_path: str, uri: str, exclude: Optional[List] = None
    ) -> Tuple[Callable, Dict]:
        # TODO(justinvyu): Defer this cleanup up as part of the
        # external-facing Syncer deprecation.
        fs_path = uri
        return (
            _upload_to_fs_path,
            dict(
                local_path=local_path,
                fs=self.storage_filesystem,
                fs_path=fs_path,
                exclude=exclude,
            ),
        )

    def _sync_down_command(self, uri: str, local_path: str) -> Tuple[Callable, Dict]:
        fs_path = uri
        return (
            _download_from_fs_path,
            dict(
                fs=self.storage_filesystem,
                fs_path=fs_path,
                local_path=local_path,
            ),
        )

    def _delete_command(self, uri: str) -> Tuple[Callable, Dict]:
        fs_path = uri
        return _delete_fs_path, dict(fs=self.storage_filesystem, fs_path=fs_path)


class StorageContext:
    """Shared context that holds all paths and storage utilities, passed along from
    the driver to workers.

    The properties of this context may not all be set at once, depending on where
    the context lives.
    For example, on the driver, the storage context is initialized, only knowing
    the experiment path. On the Trainable actor, the trial_dir_name is accessible.

    Example with storage_path="mock:///bucket/path":

        >>> from ray.train._internal.storage import StorageContext
        >>> import os
        >>> os.environ["RAY_AIR_LOCAL_CACHE_DIR"] = "/tmp/ray_results"
        >>> storage = StorageContext(
        ...     storage_path="mock:///bucket/path",
        ...     sync_config=SyncConfig(),
        ...     experiment_dir_name="exp_name",
        ... )
        >>> storage.storage_filesystem   # Auto-resolved  # doctest: +ELLIPSIS
        <pyarrow._fs._MockFileSystem object...
        >>> storage.experiment_path
        'mock:///bucket/path/exp_name'
        >>> storage.experiment_fs_path
        'bucket/path/exp_name'
        >>> storage.experiment_local_path
        '/tmp/ray_results/exp_name'
        >>> storage.trial_dir_name = "trial_dir"
        >>> storage.trial_fs_path
        'bucket/path/exp_name/trial_dir'
        >>> storage.trial_local_path
        '/tmp/ray_results/exp_name/trial_dir'
        >>> storage.current_checkpoint_index = 1
        >>> storage.checkpoint_fs_path
        'bucket/path/exp_name/trial_dir/checkpoint_000001'

    Example with storage_path=None:

        >>> from ray.train._internal.storage import StorageContext
        >>> import os
        >>> os.environ["RAY_AIR_LOCAL_CACHE_DIR"] = "/tmp/ray_results"
        >>> storage = StorageContext(
        ...     storage_path=None,
        ...     sync_config=SyncConfig(),
        ...     experiment_dir_name="exp_name",
        ... )
        >>> storage.storage_path  # Auto-resolved
        '/tmp/ray_results'
        >>> storage.storage_local_path
        '/tmp/ray_results'
        >>> storage.experiment_path
        '/tmp/ray_results/exp_name'
        >>> storage.experiment_local_path
        '/tmp/ray_results/exp_name'
        >>> storage.experiment_fs_path
        '/tmp/ray_results/exp_name'
        >>> storage.syncer is None
        True
        >>> storage.storage_filesystem   # Auto-resolved  # doctest: +ELLIPSIS
        <pyarrow._fs.LocalFileSystem object...


    Internal Usage Examples:
    - To copy files to the trial directory on the storage filesystem:

        pyarrow.fs.copy_files(
            local_dir,
            os.path.join(storage.trial_fs_path, "subdir"),
            destination_filesystem=storage.filesystem
        )
    """

    def __init__(
        self,
        storage_path: Optional[str],
        sync_config: SyncConfig,
        experiment_dir_name: str,
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
        trial_dir_name: Optional[str] = None,
        current_checkpoint_index: Optional[int] = None,
    ):
        storage_path_provided = storage_path is not None

        self.storage_local_path = _get_defaults_results_dir()
        # If `storage_path=None`, then set it to the local path.
        # Invariant: (`storage_filesystem`, `storage_path`) is the location where
        # *all* results can be accessed.
        self.storage_path = storage_path or self.storage_local_path
        self.experiment_dir_name = experiment_dir_name
        self.trial_dir_name = trial_dir_name
        self.current_checkpoint_index = current_checkpoint_index
        self.sync_config = dataclasses.replace(sync_config)

        if storage_filesystem:
            # Custom pyarrow filesystem
            self.storage_filesystem = storage_filesystem
            if is_uri(self.storage_path):
                raise ValueError(
                    "If you specify a custom `storage_filesystem`, the corresponding "
                    "`storage_path` must be a *path* on that filesystem, not a URI.\n"
                    "For example: "
                    "(storage_filesystem=CustomS3FileSystem(), "
                    "storage_path='s3://bucket/path') should be changed to "
                    "(storage_filesystem=CustomS3FileSystem(), "
                    "storage_path='bucket/path')\n"
                    "This is what you provided: "
                    f"(storage_filesystem={storage_filesystem}, "
                    f"storage_path={storage_path})\n"
                    "Note that this may depend on the custom filesystem you use."
                )
            self.storage_fs_path = self.storage_path
        else:
            (
                self.storage_filesystem,
                self.storage_fs_path,
            ) = pyarrow.fs.FileSystem.from_uri(self.storage_path)

        # Only initialize a syncer if a `storage_path` was provided.
        self.syncer: Optional[Syncer] = (
            _FilesystemSyncer(
                storage_filesystem=self.storage_filesystem,
                sync_period=self.sync_config.sync_period,
                sync_timeout=self.sync_config.sync_timeout,
            )
            if storage_path_provided
            else None
        )

        self._create_validation_file()
        self._check_validation_file()

    def __str__(self):
        attrs = [
            "storage_path",
            "storage_local_path",
            "storage_filesystem",
            "storage_fs_path",
            "experiment_dir_name",
            "trial_dir_name",
            "current_checkpoint_index",
        ]
        attr_str = "\n".join([f"  {attr}={getattr(self, attr)}" for attr in attrs])
        return f"StorageContext<\n{attr_str}\n>"

    def _create_validation_file(self):
        """On the creation of a storage context, create a validation file at the
        storage path to verify that the storage path can be written to.
        This validation file is also used to check whether the storage path is
        accessible by all nodes in the cluster."""
        valid_file = os.path.join(self.experiment_fs_path, ".validate_storage_marker")
        self.storage_filesystem.create_dir(self.experiment_fs_path)
        with self.storage_filesystem.open_output_stream(valid_file):
            pass

    def _check_validation_file(self):
        """Checks that the validation file exists at the storage path."""
        valid_file = os.path.join(self.experiment_fs_path, ".validate_storage_marker")
        if not _exists_at_fs_path(fs=self.storage_filesystem, fs_path=valid_file):
            raise RuntimeError(
                f"Unable to set up cluster storage at storage_path={self.storage_path}"
                "\nCheck that all nodes in the cluster have read/write access "
                "to the configured storage path."
            )

    @property
    def experiment_path(self) -> str:
        """The path the experiment directory, where the format matches the
        original `storage_path` format specified by the user.

        Ex: If the user passed in storage_path="s3://bucket/path?param=1", then
        this property returns "s3://bucket/path/exp_name?param=1".
        """
        return str(URI(self.storage_path) / self.experiment_dir_name)

    @property
    def experiment_fs_path(self) -> str:
        """The path on the `storage_filesystem` to the experiment directory.

        NOTE: This does not have a URI prefix anymore, since it has been stripped
        by pyarrow.fs.FileSystem.from_uri already. The URI scheme information is
        kept in `storage_filesystem` instead.
        """
        return os.path.join(self.storage_fs_path, self.experiment_dir_name)

    @property
    def experiment_local_path(self) -> str:
        """The local filesystem path to the experiment directory.

        This local "cache" path refers to location where files are dumped before
        syncing them to the `storage_path` on the `storage_filesystem`.
        """
        return os.path.join(self.storage_local_path, self.experiment_dir_name)

    @property
    def trial_local_path(self) -> str:
        """The local filesystem path to the trial directory.

        Raises a ValueError if `trial_dir_name` is not set beforehand.
        """
        if self.trial_dir_name is None:
            raise RuntimeError(
                "Should not access `trial_local_path` without setting `trial_dir_name`"
            )
        return os.path.join(self.experiment_local_path, self.trial_dir_name)

    @property
    def trial_fs_path(self) -> str:
        """The trial directory path on the `storage_filesystem`.

        Raises a ValueError if `trial_dir_name` is not set beforehand.
        """
        if self.trial_dir_name is None:
            raise RuntimeError(
                "Should not access `trial_fs_path` without setting `trial_dir_name`"
            )
        return os.path.join(self.experiment_fs_path, self.trial_dir_name)

    @property
    def checkpoint_fs_path(self) -> str:
        """The trial directory path on the `storage_filesystem`.

        Raises a ValueError if `current_checkpoint_index` is not set beforehand.
        """
        from ray.tune.trainable.util import TrainableUtil

        if self.current_checkpoint_index is None:
            raise RuntimeError(
                "Should not access `checkpoint_fs_path` without setting "
                "`current_checkpoint_index`"
            )
        checkpoint_dir_name = TrainableUtil._make_checkpoint_dir_name(
            self.current_checkpoint_index
        )
        return os.path.join(self.trial_fs_path, checkpoint_dir_name)


_storage_context: Optional[StorageContext] = None


def init_shared_storage_context(storage_context: StorageContext):
    """StorageContext can be made a global singleton by calling this method.

    This singleton is only created on the initialization of remote Trainable actors.
    On the driver, there is no global singleton, since each trial has its own
    trial_dir_name."""
    global _storage_context
    _storage_context = storage_context


def get_storage_context() -> StorageContext:
    assert _storage_context, (
        "You must first call `init_shared_storage_context` in order to access a "
        "global shared copy of StorageContext."
    )
    return _storage_context
