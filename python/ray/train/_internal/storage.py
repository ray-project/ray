import dataclasses
import os
from typing import Optional

import pyarrow.fs

from ray.air._internal.uri_utils import is_uri
from ray.air._internal.remote_storage import get_fs_and_path
from ray.tune.syncer import Syncer, SyncConfig, _DefaultSyncer
from ray.tune.result import _get_defaults_results_dir


def _use_storage_context() -> bool:
    # Whether to enable the new simple persistence mode.
    return bool(int(os.environ.get("RAY_AIR_NEW_PERSISTENCE_MODE", "1")))


class StorageContext:
    """Shared context that holds all paths and storage utilities, passed along from
    the driver to workers.

    Example:
        storage_path = "s3://bucket/results"
        storage_filesystem = S3FileSystem (auto-resolved)
        storage_cache_path = "~/ray_results"
        storage_fs_path = "bucket/results"

        experiment_dir_name = "exp_name"
        # experiment_path = "s3://bucket/results/exp_name"
        experiment_fs_path = "bucket/results/exp_name"  # Use this path with pyarrow
        experiment_cache_path = "~/ray_results/exp_name"

        # Only set on workers.
        trial_dir_name = "trial_dir"
        # trial_path = "s3://bucket/results/exp_name/trial_dir"
        trial_fs_path = "bucket/results/exp_name/trial_dir"
        # trial_cache_path = "~/ray_results/exp_name/trial_dir"

    Internal Usage Examples:
        construct_checkpoint_fs_path("checkpoint_000001")
        -> "bucket/results/exp_name/trial_dir/checkpoint_000001"

        pyarrow.fs.copy_files(
            local_dir,
            os.path.join(storage.trial_fs_path, "subdir"),
            destination_filesystem=storage.filesystem
        )
    """

    def __init__(
        self,
        storage_path: str,
        sync_config: SyncConfig,
        experiment_dir_name: str,
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
        trial_dir_name: Optional[str] = None,
    ):
        self.storage_path: str = storage_path
        self.storage_cache_path: str = _get_defaults_results_dir()
        self.experiment_dir_name: str = experiment_dir_name
        self.trial_dir_name: Optional[str] = trial_dir_name
        self.sync_config: SyncConfig = dataclasses.replace(sync_config)

        if storage_filesystem:
            # Custom pyarrow filesystem
            self.storage_filesystem = storage_filesystem
            if is_uri(self.storage_path):
                raise ValueError("TODO")
            self.storage_fs_path = self.storage_path
        else:
            (
                self.storage_filesystem,
                self.storage_fs_path,
            ) = get_fs_and_path(self.storage_path)

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
        ]
        attr_str = "\n".join([f"  {attr}={getattr(self, attr)}" for attr in attrs])
        return f"StorageContext<\n{attr_str}\n>"

    def _create_validation_file(self):
        valid_file = os.path.join(self.experiment_fs_path, ".validate_storage_marker")
        self.storage_filesystem.create_dir(self.experiment_fs_path)
        with self.storage_filesystem.open_output_stream(valid_file):
            pass

    def _check_validation_file(self):
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
        return os.path.join(self.storage_fs_path, self.experiment_dir_name)

    @property
    def experiment_cache_path(self) -> str:
        return os.path.join(self.storage_cache_path, self.experiment_dir_name)

    @property
    def trial_fs_path(self) -> str:
        if self.trial_dir_name is None:
            raise RuntimeError(
                "Should not access `trial_fs_path` without setting `trial_dir_name`"
            )
        return os.path.join(self.experiment_fs_path, self.trial_dir_name)

    def construct_checkpoint_fs_path(self, checkpoint_dir_name: str) -> str:
        return os.path.join(self.trial_fs_path, checkpoint_dir_name)


_storage_context: Optional[StorageContext] = None


def init_shared_storage_context(storage_context: StorageContext):
    global _storage_context
    _storage_context = storage_context


def get_storage_context() -> StorageContext:
    assert _storage_context, (
        "You must first call `init_shared_storage_context` in order to access a "
        "global shared copy of StorageContext."
    )
    return _storage_context
