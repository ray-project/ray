import dataclasses
import fnmatch
import logging
import os
from pathlib import Path
import shutil
from typing import Callable, Dict, List, Optional, Tuple, Type, Union, TYPE_CHECKING

try:
    import fsspec  # noqa
    from fsspec.implementations.local import LocalFileSystem

except (ImportError, ModuleNotFoundError) as e:
    raise RuntimeError(
        "fsspec is a required dependency of Ray Train and Ray Tune. "
        "Please install with: `pip install fsspec`"
    ) from e

try:
    import pyarrow
    import pyarrow.fs

except (ImportError, ModuleNotFoundError) as e:
    raise RuntimeError(
        "pyarrow is a required dependency of Ray Train and Ray Tune. "
        "Please install with: `pip install pyarrow`"
    ) from e


from ray._private.storage import _get_storage_uri
from ray.air._internal.filelock import TempFileLock
from ray.train._internal.syncer import Syncer, SyncConfig, _BackgroundSyncer
from ray.train.constants import _get_defaults_results_dir

if TYPE_CHECKING:
    from ray.train._checkpoint import Checkpoint


logger = logging.getLogger(__name__)


_VALIDATE_STORAGE_MARKER_FILENAME = ".validate_storage_marker"


def _use_storage_context() -> bool:
    # Whether to enable the new simple persistence mode.
    from ray.train.constants import RAY_AIR_NEW_PERSISTENCE_MODE

    return bool(int(os.environ.get(RAY_AIR_NEW_PERSISTENCE_MODE, "1")))


class _ExcludingLocalFilesystem(LocalFileSystem):
    """LocalFileSystem wrapper to exclude files according to patterns.

    Args:
        root_path: Root path to strip when matching with the exclude pattern.
            Ex: root_path="/tmp/a/b/c", exclude=["*a*"], will exclude
            /tmp/a/b/c/_a_.txt but not ALL of /tmp/a/*.
        exclude: List of patterns that are applied to files returned by
            ``self.find()``. If a file path matches this pattern, it will
            be excluded.

    """

    def __init__(self, root_path: Path, exclude: List[str], **kwargs):
        super().__init__(**kwargs)
        self._exclude = exclude
        self._root_path = root_path

    @property
    def fsid(self):
        return "_excluding_local"

    def _should_exclude(self, path: str) -> bool:
        """Return True if `path` (relative to `root_path`) matches any of the
        `self._exclude` patterns."""
        path = Path(path)
        relative_path = path.relative_to(self._root_path).as_posix()
        alt = os.path.join(relative_path, "") if path.is_dir() else None

        for excl in self._exclude:
            if fnmatch.fnmatch(relative_path, excl):
                return True
            if alt and fnmatch.fnmatch(alt, excl):
                return True
        return False

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        """Call parent find() and exclude from result."""
        paths = super().find(
            path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs
        )
        if detail:
            return {
                path: out
                for path, out in paths.items()
                if not self._should_exclude(path)
            }
        else:
            return [path for path in paths if not self._should_exclude(path)]


def _pyarrow_fs_copy_files(
    source, destination, source_filesystem=None, destination_filesystem=None, **kwargs
):
    if isinstance(destination_filesystem, pyarrow.fs.S3FileSystem):
        # Workaround multi-threading issue with pyarrow. Note that use_threads=True
        # is safe for download, just not for uploads, see:
        # https://github.com/apache/arrow/issues/32372
        kwargs.setdefault("use_threads", False)

    # Use a large chunk size to speed up large checkpoint transfers.
    kwargs.setdefault("chunk_size", 64 * 1024 * 1024)

    return pyarrow.fs.copy_files(
        source,
        destination,
        source_filesystem=source_filesystem,
        destination_filesystem=destination_filesystem,
        **kwargs,
    )


# TODO(justinvyu): Add unit tests for all these utils.


def _delete_fs_path(fs: pyarrow.fs.FileSystem, fs_path: str):

    is_dir = _is_directory(fs, fs_path)

    try:
        if is_dir:
            fs.delete_dir(fs_path)
        else:
            fs.delete_file(fs_path)
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
        # Clean up the directory if downloading was unsuccessful
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
            This pattern will match with the relative paths of all files under
            `local_path`.
            Ex: ["*.png"] to exclude all .png images.
    """

    if not exclude:
        # TODO(justinvyu): uploading a single file doesn't work
        # (since we always create a directory at fs_path)
        _create_directory(fs=fs, fs_path=fs_path)
        _pyarrow_fs_copy_files(local_path, fs_path, destination_filesystem=fs)
        return

    _upload_to_uri_with_exclude_fsspec(
        local_path=local_path, fs=fs, fs_path=fs_path, exclude=exclude
    )


def _upload_to_uri_with_exclude_fsspec(
    local_path: str, fs: "pyarrow.fs", fs_path: str, exclude: Optional[List[str]]
) -> None:
    local_fs = _ExcludingLocalFilesystem(root_path=local_path, exclude=exclude)
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

    selector = pyarrow.fs.FileSelector(fs_path, allow_not_found=True, recursive=False)
    return [
        os.path.relpath(file_info.path.lstrip("/"), start=fs_path.lstrip("/"))
        for file_info in fs.get_file_info(selector)
    ]


def _exists_at_fs_path(fs: pyarrow.fs.FileSystem, fs_path: str) -> bool:
    """Returns True if (fs, fs_path) exists."""

    valid = fs.get_file_info(fs_path)
    return valid.type != pyarrow.fs.FileType.NotFound


def _is_directory(fs: pyarrow.fs.FileSystem, fs_path: str) -> bool:
    """Checks if (fs, fs_path) is a directory or a file.

    Raises:
        FileNotFoundError: if (fs, fs_path) doesn't exist.
    """

    file_info = fs.get_file_info(fs_path)
    if file_info.type == pyarrow.fs.FileType.NotFound:
        raise FileNotFoundError(f"Path not found: ({fs}, {fs_path})")

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


def get_fs_and_path(
    storage_path: Union[str, os.PathLike],
    storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
) -> Tuple[pyarrow.fs.FileSystem, str]:
    """Returns the fs and path from a storage path and an optional custom fs.

    Args:
        storage_path: A storage path or URI. (ex: s3://bucket/path or /tmp/ray_results)
        storage_filesystem: A custom filesystem to use. If not provided,
            this will be auto-resolved by pyarrow. If provided, the storage_path
            is assumed to be prefix-stripped already, and must be a valid path
            on the filesystem.
    """
    storage_path = str(storage_path)

    if storage_filesystem:
        return storage_filesystem, storage_path

    return pyarrow.fs.FileSystem.from_uri(storage_path)


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

    There are 2 types of paths:
    1. *_fs_path: A path on the `storage_filesystem`. This is a regular path
        which has been prefix-stripped by pyarrow.fs.FileSystem.from_uri and
        can be joined with `os.path.join`.
    2. *_local_path: The path on the local filesystem where results are saved to
       before persisting to storage.

    Example with storage_path="mock:///bucket/path?param=1":

        >>> from ray.train._internal.storage import StorageContext
        >>> import os
        >>> os.environ["RAY_AIR_LOCAL_CACHE_DIR"] = "/tmp/ray_results"
        >>> storage = StorageContext(
        ...     storage_path="mock://netloc/bucket/path?param=1",
        ...     experiment_dir_name="exp_name",
        ... )
        >>> storage.storage_filesystem   # Auto-resolved  # doctest: +ELLIPSIS
        <pyarrow._fs._MockFileSystem object...
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
        ...     experiment_dir_name="exp_name",
        ... )
        >>> storage.storage_path  # Auto-resolved
        '/tmp/ray_results'
        >>> storage.storage_local_path
        '/tmp/ray_results'
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
        storage_path: Optional[Union[str, os.PathLike]],
        experiment_dir_name: str,
        sync_config: Optional[SyncConfig] = None,
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
        trial_dir_name: Optional[str] = None,
        current_checkpoint_index: int = 0,
    ):
        self.custom_fs_provided = storage_filesystem is not None

        self.storage_local_path = _get_defaults_results_dir()

        # If no remote path is set, try to get Ray Storage URI
        ray_storage_uri: Optional[str] = _get_storage_uri()
        if ray_storage_uri and storage_path is None:
            logger.info(
                "Using configured Ray Storage URI as the `storage_path`: "
                f"{ray_storage_uri}"
            )

        # If `storage_path=None`, then set it to the local path.
        # Invariant: (`storage_filesystem`, `storage_path`) is the location where
        # *all* results can be accessed.
        self.storage_path = storage_path or ray_storage_uri or self.storage_local_path
        self.experiment_dir_name = experiment_dir_name
        self.trial_dir_name = trial_dir_name
        self.current_checkpoint_index = current_checkpoint_index
        self.sync_config = (
            dataclasses.replace(sync_config) if sync_config else SyncConfig()
        )

        self.storage_filesystem, self.storage_fs_path = get_fs_and_path(
            self.storage_path, storage_filesystem
        )
        self.storage_fs_path = Path(self.storage_fs_path).as_posix()

        # Syncing is always needed if a custom `storage_filesystem` is provided.
        # Otherwise, syncing is only needed if storage_local_path
        # and storage_fs_path point to different locations.
        syncing_needed = (
            self.custom_fs_provided or self.storage_fs_path != self.storage_local_path
        )
        self.syncer: Optional[Syncer] = (
            _FilesystemSyncer(
                storage_filesystem=self.storage_filesystem,
                sync_period=self.sync_config.sync_period,
                sync_timeout=self.sync_config.sync_timeout,
            )
            if syncing_needed
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
        valid_file = os.path.join(
            self.experiment_fs_path, _VALIDATE_STORAGE_MARKER_FILENAME
        )
        self.storage_filesystem.create_dir(self.experiment_fs_path)
        with self.storage_filesystem.open_output_stream(valid_file):
            pass

    def _check_validation_file(self):
        """Checks that the validation file exists at the storage path."""
        valid_file = os.path.join(
            self.experiment_fs_path, _VALIDATE_STORAGE_MARKER_FILENAME
        )
        if not _exists_at_fs_path(fs=self.storage_filesystem, fs_path=valid_file):
            raise RuntimeError(
                f"Unable to set up cluster storage at storage_path={self.storage_path}"
                "\nCheck that all nodes in the cluster have read/write access "
                "to the configured storage path."
            )

    def persist_current_checkpoint(self, checkpoint: "Checkpoint") -> "Checkpoint":
        """Persists a given checkpoint to the current checkpoint path on the filesystem.

        "Current" is defined by the `current_checkpoint_index` attribute of the
        storage context.

        This method copies the checkpoint files to the storage location.
        It's up to the user to delete the original checkpoint files if desired.

        For example, the original directory is typically a local temp directory.

        Args:
            checkpoint: The checkpoint to persist to (fs, checkpoint_fs_path).

        Returns:
            Checkpoint: A Checkpoint pointing to the persisted checkpoint location.
        """
        # TODO(justinvyu): Fix this cyclical import.
        from ray.train._checkpoint import Checkpoint

        logger.debug(
            "Copying checkpoint files to storage path:\n"
            "({source_fs}, {source}) -> ({dest_fs}, {destination})".format(
                source=checkpoint.path,
                destination=self.checkpoint_fs_path,
                source_fs=checkpoint.filesystem,
                dest_fs=self.storage_filesystem,
            )
        )

        # Raise an error if the storage path is not accessible when
        # attempting to upload a checkpoint from a remote worker.
        # Ex: If storage_path is a local path, then a validation marker
        # will only exist on the head node but not the worker nodes.
        self._check_validation_file()

        self.storage_filesystem.create_dir(self.checkpoint_fs_path)
        _pyarrow_fs_copy_files(
            source=checkpoint.path,
            destination=self.checkpoint_fs_path,
            source_filesystem=checkpoint.filesystem,
            destination_filesystem=self.storage_filesystem,
        )

        persisted_checkpoint = Checkpoint(
            filesystem=self.storage_filesystem,
            path=self.checkpoint_fs_path,
        )
        logger.info(f"Checkpoint successfully created at: {persisted_checkpoint}")
        return persisted_checkpoint

    def persist_artifacts(self, force: bool = False) -> None:
        """Persists all artifacts within `trial_local_dir` to storage.

        This method possibly launches a background task to sync the trial dir,
        depending on the `sync_period` + `sync_artifacts_on_checkpoint`
        settings of `SyncConfig`.

        `(local_fs, trial_local_path) -> (storage_filesystem, trial_fs_path)`

        Args:
            force: If True, wait for a previous sync to finish, launch a new one,
                and wait for that one to finish. By the end of a `force=True` call, the
                latest version of the trial artifacts will be persisted.
        """
        if not self.sync_config.sync_artifacts:
            return

        # Skip if we don't need to sync (e.g., storage_path == storage_local_path, and
        # all trial artifacts are already in the right place)
        if not self.syncer:
            return

        if force:
            self.syncer.wait()
            self.syncer.sync_up(
                local_dir=self.trial_local_path, remote_dir=self.trial_fs_path
            )
            self.syncer.wait()
        else:
            self.syncer.sync_up_if_needed(
                local_dir=self.trial_local_path, remote_dir=self.trial_fs_path
            )

    @property
    def experiment_fs_path(self) -> str:
        """The path on the `storage_filesystem` to the experiment directory.

        NOTE: This does not have a URI prefix anymore, since it has been stripped
        by pyarrow.fs.FileSystem.from_uri already. The URI scheme information is
        kept in `storage_filesystem` instead.
        """
        return Path(self.storage_fs_path, self.experiment_dir_name).as_posix()

    @property
    def experiment_local_path(self) -> str:
        """The local filesystem path to the experiment directory.

        This local "cache" path refers to location where files are dumped before
        syncing them to the `storage_path` on the `storage_filesystem`.
        """
        return Path(self.storage_local_path, self.experiment_dir_name).as_posix()

    @property
    def trial_local_path(self) -> str:
        """The local filesystem path to the trial directory.

        Raises a ValueError if `trial_dir_name` is not set beforehand.
        """
        if self.trial_dir_name is None:
            raise RuntimeError(
                "Should not access `trial_local_path` without setting `trial_dir_name`"
            )
        return Path(self.experiment_local_path, self.trial_dir_name).as_posix()

    @property
    def trial_fs_path(self) -> str:
        """The trial directory path on the `storage_filesystem`.

        Raises a ValueError if `trial_dir_name` is not set beforehand.
        """
        if self.trial_dir_name is None:
            raise RuntimeError(
                "Should not access `trial_fs_path` without setting `trial_dir_name`"
            )
        return Path(self.experiment_fs_path, self.trial_dir_name).as_posix()

    @property
    def checkpoint_fs_path(self) -> str:
        """The current checkpoint directory path on the `storage_filesystem`.

        "Current" refers to the checkpoint that is currently being created/persisted.
        The user of this class is responsible for setting the `current_checkpoint_index`
        (e.g., incrementing when needed).
        """
        return Path(self.trial_fs_path, self.checkpoint_dir_name).as_posix()

    @property
    def checkpoint_dir_name(self) -> str:
        """The current checkpoint directory name, based on the checkpoint index."""
        return StorageContext._make_checkpoint_dir_name(self.current_checkpoint_index)

    @staticmethod
    def get_experiment_dir_name(run_obj: Union[str, Callable, Type]) -> str:
        from ray.tune.experiment import Experiment
        from ray.tune.utils import date_str

        run_identifier = Experiment.get_trainable_name(run_obj)

        if bool(int(os.environ.get("TUNE_DISABLE_DATED_SUBDIR", 0))):
            dir_name = run_identifier
        else:
            dir_name = "{}_{}".format(run_identifier, date_str())
        return dir_name

    @staticmethod
    def _make_checkpoint_dir_name(index: int):
        """Get the name of the checkpoint directory, given an index."""
        return f"checkpoint_{index:06d}"
