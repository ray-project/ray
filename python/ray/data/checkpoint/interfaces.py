import os
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Optional, Tuple

import pyarrow

from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data.datasource import PathPartitionFilter


@PublicAPI(stability="alpha")
class CheckpointBackend(Enum):
    """Supported backends for storing and reading checkpoint files.

    Currently, only one type of backend is supported:

    * Batch-based backends: CLOUD_OBJECT_STORAGE and FILE_STORAGE.

    Their differences are as follows:

    1. Writing checkpoints: Batch-based backends write a checkpoint file
       for each block.
    2. Loading checkpoints and filtering input data: Batch-based backends
       load all checkpoint data into memory prior to dataset execution.
       The checkpoint data is then passed to each read task to perform filtering.
    """

    CLOUD_OBJECT_STORAGE = "CLOUD_OBJECT_STORAGE"
    """
    Batch-based checkpoint backend that uses cloud object storage, such as
    AWS S3, Google Cloud Storage, etc.
    """

    FILE_STORAGE = "FILE_STORAGE"
    """
    Batch based checkpoint backend that uses file system storage.
    Note, when using this backend, the checkpoint path must be a network-mounted
    file system (e.g. `/mnt/cluster_storage/`).
    """


@PublicAPI(stability="beta")
class CheckpointConfig:
    """Configuration for checkpointing.

    Args:
        id_column: Name of the ID column in the input dataset.
            ID values must be unique across all rows in the dataset and must persist
            during all operators.
        checkpoint_path: Path to store the checkpoint data. It can be a path to a cloud
            object storage (e.g. `s3://bucket/path`) or a file system path.
            If the latter, the path must be a network-mounted file system (e.g.
            `/mnt/cluster_storage/`) that is accessible to the entire cluster.
            If not set, defaults to `RAY_DATA_CHECKPOINT_PATH_BUCKET/ray_data_checkpoint`.
        delete_checkpoint_on_success: If true, automatically delete checkpoint
            data when the dataset execution succeeds. Only supported for
            batch-based backend currently.
        override_filesystem: Override the :class:`pyarrow.fs.FileSystem` object used to
            read/write checkpoint data. Use this when you want to use custom credentials.
        override_backend: Override the :class:`CheckpointBackend` object used to
            access the checkpoint backend storage.
        filter_num_threads: Number of threads used to filter checkpointed rows.
        write_num_threads: Number of threads used to write checkpoint files for
            completed rows.
        checkpoint_path_partition_filter: Filter for checkpoint files to load during
            restoration when reading from `checkpoint_path`.
    """

    DEFAULT_CHECKPOINT_PATH_BUCKET_ENV_VAR = "RAY_DATA_CHECKPOINT_PATH_BUCKET"
    DEFAULT_CHECKPOINT_PATH_DIR = "ray_data_checkpoint"

    def __init__(
        self,
        id_column: Optional[str] = None,
        checkpoint_path: Optional[str] = None,
        *,
        delete_checkpoint_on_success: bool = True,
        override_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        override_backend: Optional[CheckpointBackend] = None,
        filter_num_threads: int = 3,
        write_num_threads: int = 3,
        checkpoint_path_partition_filter: Optional["PathPartitionFilter"] = None,
    ):
        self.id_column: Optional[str] = id_column

        if not isinstance(self.id_column, str) or len(self.id_column) == 0:
            raise InvalidCheckpointingConfig(
                "Checkpoint ID column must be a non-empty string, "
                f"but got {self.id_column}"
            )

        if override_backend is not None:
            warnings.warn(
                "`override_backend` is deprecated and will be removed in August 2025.",
                FutureWarning,
                stacklevel=2,
            )

        self.checkpoint_path: str = (
            checkpoint_path or self._get_default_checkpoint_path()
        )
        inferred_backend, inferred_fs = self._infer_backend_and_fs(
            self.checkpoint_path,
            override_filesystem,
            override_backend,
        )
        self.filesystem: "pyarrow.fs.FileSystem" = inferred_fs
        self.backend: CheckpointBackend = inferred_backend
        self.delete_checkpoint_on_success: bool = delete_checkpoint_on_success
        self.filter_num_threads: int = filter_num_threads
        self.write_num_threads: int = write_num_threads
        self.checkpoint_path_partition_filter = checkpoint_path_partition_filter

    def _get_default_checkpoint_path(self) -> str:
        artifact_storage = os.environ.get(self.DEFAULT_CHECKPOINT_PATH_BUCKET_ENV_VAR)
        if artifact_storage is None:
            raise InvalidCheckpointingConfig(
                f"`{self.DEFAULT_CHECKPOINT_PATH_BUCKET_ENV_VAR}` env var is not set, "
                "please explicitly set `CheckpointConfig.checkpoint_path`."
            )
        return f"{artifact_storage}/{self.DEFAULT_CHECKPOINT_PATH_DIR}"

    def _infer_backend_and_fs(
        self,
        checkpoint_path: str,
        override_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        override_backend: Optional[CheckpointBackend] = None,
    ) -> Tuple[CheckpointBackend, "pyarrow.fs.FileSystem"]:
        try:
            if override_filesystem is not None:
                assert isinstance(override_filesystem, pyarrow.fs.FileSystem), (
                    "override_filesystem must be an instance of "
                    f"`pyarrow.fs.FileSystem`, but got {type(override_filesystem)}"
                )
                fs = override_filesystem
            else:
                fs, _ = pyarrow.fs.FileSystem.from_uri(checkpoint_path)

            if override_backend is not None:
                assert isinstance(override_backend, CheckpointBackend), (
                    "override_backend must be an instance of `CheckpointBackend`, "
                    f"but got {type(override_backend)}"
                )
                backend = override_backend
            else:
                if isinstance(fs, pyarrow.fs.LocalFileSystem):
                    backend = CheckpointBackend.FILE_STORAGE
                else:
                    backend = CheckpointBackend.CLOUD_OBJECT_STORAGE

            return backend, fs
        except Exception as e:
            raise InvalidCheckpointingConfig(
                f"Invalid checkpoint path: {checkpoint_path}. "
            ) from e


@DeveloperAPI
class InvalidCheckpointingConfig(Exception):
    """Exception which indicates that the checkpointing
    configuration is invalid."""

    pass


@DeveloperAPI
class InvalidCheckpointingOperators(Exception):
    """Exception which indicates that the DAG is not eligible for checkpointing,
    due to one or more incompatible operators."""

    pass
