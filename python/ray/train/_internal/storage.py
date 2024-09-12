# Try import ray[train] core requirements (defined in setup.py)
# isort: off
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
# isort: on

import fnmatch
import logging
import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Type, Union

from ray.air._internal.filelock import TempFileLock
from ray.train._internal.syncer import SyncConfig, Syncer, _BackgroundSyncer
from ray.train.constants import _get_ray_train_session_dir
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.train._checkpoint import Checkpoint


logger = logging.getLogger(__name__)


_VALIDATE_STORAGE_MARKER_FILENAME = ".validate_storage_marker"


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
        match_candidates = [relative_path]
        if path.is_dir():
            # Everything is in posix path format ('/')
            match_candidates.append(relative_path + "/")

        for excl in self._exclude:
            if any(fnmatch.fnmatch(candidate, excl) for candidate in match_candidates):
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


def _list_at_fs_path(
    fs: pyarrow.fs.FileSystem,
    fs_path: str,
    file_filter: Optional[Callable[[pyarrow.fs.FileInfo], bool]] = None,
) -> List[str]:
    """Returns the list of filenames at (fs, fs_path), similar to os.listdir.

    If the path doesn't exist, returns an empty list.
    """
    if file_filter is None:
        file_filter = lambda x: True  # noqa: E731

    selector = pyarrow.fs.FileSelector(fs_path, allow_not_found=True, recursive=False)
    return [
        os.path.relpath(file_info.path.lstrip("/"), start=fs_path.lstrip("/"))
        for file_info in fs.get_file_info(selector)
        if file_filter(file_info)
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


@DeveloperAPI
class StorageContext:
    """Shared context that holds the source of truth for all paths and
    storage utilities, passed along from the driver to workers.

    This object defines a few types of paths:
    1. *_fs_path: A path on the `storage_filesystem`. This is a regular path
        which has been prefix-stripped by pyarrow.fs.FileSystem.from_uri and
        can be joined with `Path(...).as_posix()`.
    2. *_driver_staging_path: The temporary staging directory on the local filesystem
        where driver artifacts are saved to before persisting them to storage.
    3. trial_working_directory: The local filesystem path that the remote
        actors' working directories are moved to by default.
        This is separated from the driver staging path so that driver syncing
        does not implicitly upload the trial working directory, for trials on the
        driver node.

    Example with storage_path="mock:///bucket/path?param=1":

        >>> import ray
        >>> from ray.train._internal.storage import StorageContext
        >>> import os
        >>> _ = ray.init()
        >>> storage = StorageContext(
        ...     storage_path="mock://netloc/bucket/path?param=1",
        ...     experiment_dir_name="exp_name",
        ... )
        >>> storage.storage_filesystem   # Auto-resolved  # doctest: +ELLIPSIS
        <pyarrow._fs._MockFileSystem object...
        >>> storage.experiment_fs_path
        'bucket/path/exp_name'
        >>> storage.experiment_driver_staging_path  # doctest: +ELLIPSIS
        '/tmp/ray/session_.../artifacts/.../exp_name/driver_artifacts'
        >>> storage.trial_dir_name = "trial_dir"
        >>> storage.trial_fs_path
        'bucket/path/exp_name/trial_dir'
        >>> storage.trial_driver_staging_path  # doctest: +ELLIPSIS
        '/tmp/ray/session_.../artifacts/.../exp_name/driver_artifacts/trial_dir'
        >>> storage.trial_working_directory   # doctest: +ELLIPSIS
        '/tmp/ray/session_.../artifacts/.../exp_name/working_dirs/trial_dir'
        >>> storage.current_checkpoint_index = 1
        >>> storage.checkpoint_fs_path
        'bucket/path/exp_name/trial_dir/checkpoint_000001'
        >>> ray.shutdown()

    Example with storage_path="/tmp/ray_results":

        >>> from ray.train._internal.storage import StorageContext
        >>> storage = StorageContext(
        ...     storage_path="/tmp/ray_results",
        ...     experiment_dir_name="exp_name",
        ... )
        >>> storage.storage_fs_path
        '/tmp/ray_results'
        >>> storage.experiment_fs_path
        '/tmp/ray_results/exp_name'
        >>> storage.storage_filesystem   # Auto-resolved  # doctest: +ELLIPSIS
        <pyarrow._fs.LocalFileSystem object...

    Internal Usage Examples:
    - To copy files to the trial directory on the storage filesystem:

        pyarrow.fs.copy_files(
            local_dir,
            Path(storage.trial_fs_path, "subdir").as_posix(),
            destination_filesystem=storage.filesystem
        )

    .. warning::
        This is an experimental developer API and is subject to change
        without notice between versions.
    """

    def __init__(
        self,
        storage_path: Union[str, os.PathLike],
        experiment_dir_name: str,
        sync_config: Optional[SyncConfig] = None,
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
        trial_dir_name: Optional[str] = None,
        current_checkpoint_index: int = -1,
    ):
        from ray.tune.utils import date_str

        self.custom_fs_provided = storage_filesystem is not None

        # Invariant: (`storage_filesystem`, `storage_path`) is the location where
        # *all* results can be accessed.
        self.experiment_dir_name = experiment_dir_name
        self.trial_dir_name = trial_dir_name
        self.current_checkpoint_index = current_checkpoint_index
        self.sync_config = sync_config or SyncConfig()

        self.storage_filesystem, self.storage_fs_path = get_fs_and_path(
            storage_path, storage_filesystem
        )
        self.storage_fs_path = Path(self.storage_fs_path).as_posix()

        self.syncer: Syncer = _FilesystemSyncer(
            storage_filesystem=self.storage_filesystem,
            sync_period=self.sync_config.sync_period,
            sync_timeout=self.sync_config.sync_timeout,
        )

        self._create_validation_file()
        self._check_validation_file()

        # Timestamp is used to create a unique session directory for the current
        # training job. This is used to avoid conflicts when multiple training jobs
        # run with the same name in the same cluster.
        # This is set ONCE at the creation of the storage context, on the driver.
        self._timestamp = date_str()

    def __str__(self):
        return (
            "StorageContext<\n"
            f"  storage_filesystem='{self.storage_filesystem.type_name}',\n"
            f"  storage_fs_path='{self.storage_fs_path}',\n"
            f"  experiment_dir_name='{self.experiment_dir_name}',\n"
            f"  trial_dir_name='{self.trial_dir_name}',\n"
            f"  current_checkpoint_index={self.current_checkpoint_index},\n"
            ">"
        )

    def _create_validation_file(self):
        """On the creation of a storage context, create a validation file at the
        storage path to verify that the storage path can be written to.
        This validation file is also used to check whether the storage path is
        accessible by all nodes in the cluster."""
        valid_file = Path(
            self.experiment_fs_path, _VALIDATE_STORAGE_MARKER_FILENAME
        ).as_posix()
        self.storage_filesystem.create_dir(self.experiment_fs_path)
        with self.storage_filesystem.open_output_stream(valid_file):
            pass

    def _check_validation_file(self):
        """Checks that the validation file exists at the storage path."""
        valid_file = Path(
            self.experiment_fs_path, _VALIDATE_STORAGE_MARKER_FILENAME
        ).as_posix()
        if not _exists_at_fs_path(fs=self.storage_filesystem, fs_path=valid_file):
            raise RuntimeError(
                f"Unable to set up cluster storage with the following settings:\n{self}"
                "\nCheck that all nodes in the cluster have read/write access "
                "to the configured storage path. `RunConfig(storage_path)` should be "
                "set to a cloud storage URI or a shared filesystem path accessible "
                "by all nodes in your cluster ('s3://bucket' or '/mnt/nfs'). "
                "A local path on the head node is not accessible by worker nodes. "
                "See: https://docs.ray.io/en/latest/train/user-guides/persistent-storage.html"  # noqa: E501
            )

    def _update_checkpoint_index(self, metrics: Dict):
        # Per default, increase by 1. This can be overwritten to customize checkpoint
        # directories.
        self.current_checkpoint_index += 1

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

        `(local_fs, trial_working_dir) -> (storage_filesystem, trial_fs_path)`

        Args:
            force: If True, wait for a previous sync to finish, launch a new one,
                and wait for that one to finish. By the end of a `force=True` call, the
                latest version of the trial artifacts will be persisted.
        """
        if not self.sync_config.sync_artifacts:
            return

        # Skip if there are no artifacts to sync
        is_empty = not any(os.scandir(self.trial_working_directory))
        if is_empty:
            return

        if force:
            self.syncer.wait()
            self.syncer.sync_up(
                local_dir=self.trial_working_directory, remote_dir=self.trial_fs_path
            )
            self.syncer.wait()
        else:
            self.syncer.sync_up_if_needed(
                local_dir=self.trial_working_directory, remote_dir=self.trial_fs_path
            )

    @property
    def experiment_fs_path(self) -> str:
        """The path on the `storage_filesystem` to the experiment directory.

        NOTE: This does not have a URI prefix anymore, since it has been stripped
        by pyarrow.fs.FileSystem.from_uri already. The URI scheme information is
        kept in `storage_filesystem` instead.
        """
        return Path(self.storage_fs_path, self.experiment_dir_name).as_posix()

    def _get_session_path(self) -> str:
        """The Ray Train/Tune session local directory used to stage files
        before persisting to the storage filesystem."""
        return Path(
            _get_ray_train_session_dir(), self._timestamp, self.experiment_dir_name
        ).as_posix()

    @property
    def experiment_driver_staging_path(self) -> str:
        """The local filesystem path of the experiment directory on the driver node.

        The driver is the node where `Trainer.fit`/`Tuner.fit` is being called.

        This path is of the form:
        `/tmp/ray/session_<session_id>/artifacts/<ray-train-job-timestamp>/
        <experiment_dir_name>/driver_artifacts`

        This should be used as the temporary staging location for files *on the driver*
        before syncing them to `experiment_fs_path`.
        For example, the search algorithm should dump its state to this directory.
        See `trial_driver_staging_path` for writing trial-specific artifacts.

        The directory is synced to
        `{storage_path}/{experiment_dir_name}` periodically.
        See `_ExperimentCheckpointManager.checkpoint` for where that happens.
        """
        return Path(self._get_session_path(), "driver_artifacts").as_posix()

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
    def trial_driver_staging_path(self) -> str:
        """The local filesystem path of the trial directory on the driver.

        The driver is the node where `Trainer.fit`/`Tuner.fit` is being called.

        This path is of the form:
        `/tmp/ray/session_<session_id>/artifacts/<ray-train-job-timestamp>/
        <experiment_dir_name>/driver_artifacts/<trial_dir_name>`

        This should be used as the temporary location for files on the driver
        before persisting them to `trial_fs_path`.

        For example, callbacks (e.g., JsonLoggerCallback) should write trial-specific
        logfiles within this directory.
        """
        if self.trial_dir_name is None:
            raise RuntimeError(
                "Should not access `trial_driver_staging_path` "
                "without setting `trial_dir_name`"
            )
        return Path(self.experiment_driver_staging_path, self.trial_dir_name).as_posix()

    @property
    def trial_working_directory(self) -> str:
        """The local filesystem path to trial working directory.

        This path is of the form:
        `/tmp/ray/session_<session_id>/artifacts/<ray-train-job-timestamp>/
        <experiment_dir_name>/working_dirs/<trial_dir_name>`

        Ray Train/Tune moves the remote actor's working directory to this path
        by default, unless disabled by `RAY_CHDIR_TO_TRIAL_DIR` environment variable.

        Writing files to this directory allows users to persist training artifacts
        if `SyncConfig(sync_artifacts=True)` is set.
        """
        if self.trial_dir_name is None:
            raise RuntimeError(
                "Cannot access `trial_working_directory` without "
                "setting `trial_dir_name`"
            )
        return Path(
            self._get_session_path(), "working_dirs", self.trial_dir_name
        ).as_posix()

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
