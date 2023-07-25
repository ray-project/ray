import dataclasses
import os
from typing import Optional

import pyarrow.fs

from ray.air._internal.uri_utils import is_uri
from ray.tune.syncer import Syncer, SyncConfig, _DefaultSyncer
from ray.tune.result import _get_defaults_results_dir
from ray.tune.trainable.util import TrainableUtil


def _use_storage_context() -> bool:
    # Whether to enable the new simple persistence mode.
    return bool(int(os.environ.get("RAY_AIR_NEW_PERSISTENCE_MODE", "0")))


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
        >>> storage.experiment_fs_path
        'bucket/path/exp_name'
        >>> storage.experiment_cache_path
        '/tmp/ray_results/exp_name'
        >>> storage.trial_dir_name = "trial_dir"
        >>> storage.trial_fs_path
        'bucket/path/exp_name/trial_dir'
        >>> storage.current_checkpoint_id = 1
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
        >>> storage.storage_cache_path
        '/tmp/ray_results'
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
        current_checkpoint_id: Optional[int] = None,
    ):
        self.storage_cache_path = _get_defaults_results_dir()
        # If `storage_path=None`, then set it to the default cache path.
        # Invariant: (`storage_filesystem`, `storage_path`) is the location where
        # *all* results can be accessed.
        self.storage_path = storage_path or self.storage_cache_path
        self.experiment_dir_name = experiment_dir_name
        self.trial_dir_name = trial_dir_name
        self.current_checkpoint_id = current_checkpoint_id
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

        # Only initialize a syncer if the storage path is different from the cache path.
        self.syncer: Optional[Syncer] = (
            None
            if self.storage_path == self.storage_cache_path
            else _DefaultSyncer(
                sync_period=self.sync_config.sync_period,
                sync_timeout=self.sync_config.sync_timeout,
                storage_filesystem=self.storage_filesystem,
            )
        )

        # TODO(justinvyu): This is needed for the legacy Trainable syncing code.
        # Remove this after syncer argument is removed from SyncConfig.
        self.sync_config.syncer = self.syncer

        self._create_validation_file()
        self._check_validation_file()

    def __str__(self):
        attrs = [
            "storage_path",
            "storage_cache_path",
            "storage_filesystem",
            "storage_fs_path",
            "experiment_dir_name",
            "trial_dir_name",
            "current_checkpoint_id",
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
        valid = self.storage_filesystem.get_file_info([valid_file])[0]
        if valid.type == pyarrow.fs.FileType.NotFound:
            raise RuntimeError(
                f"Unable to set up cluster storage at storage_path={self.storage_path}"
                "\nCheck that all nodes in the cluster have read/write access "
                "to the configured storage path."
            )

    @property
    def experiment_fs_path(self) -> str:
        """The path on the `storage_filesystem` to the experiment directory."""
        return os.path.join(self.storage_fs_path, self.experiment_dir_name)

    @property
    def experiment_cache_path(self) -> str:
        """The local filesystem path to the experiment directory.

        This local "cache" path refers to location where files are dumped before
        syncing them to the `storage_path` on the `storage_filesystem`.
        """
        return os.path.join(self.storage_cache_path, self.experiment_dir_name)

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

        Raises a ValueError if `current_checkpoint_id` is not set beforehand.
        """
        if self.current_checkpoint_id is None:
            raise RuntimeError(
                "Should not access `checkpoint_fs_path` without setting "
                "`current_checkpoint_id`"
            )
        checkpoint_dir_name = TrainableUtil._make_checkpoint_dir_name(
            self.current_checkpoint_id
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
