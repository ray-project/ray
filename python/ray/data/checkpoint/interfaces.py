import inspect
import os
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Optional, Tuple, Type

import pyarrow

from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data.checkpoint.checkpoint_filter import (
        CheckpointFilter,
        CheckpointManager,
    )
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


def _validate_checkpoint_cls(cls: type, base_cls: type, param_name: str) -> None:
    """Validate that ``cls`` is a concrete subclass of ``base_cls``.

    Args:
        cls: The user-provided class to validate.
        base_cls: The required base class.
        param_name: The ``CheckpointConfig`` parameter name, for error messages.

    Raises:
        InvalidCheckpointingConfig: if ``cls`` is not a subclass of
            ``base_cls``, or is abstract (so instantiation would fail later
            inside a remote worker instead of at config construction).
    """
    if not (isinstance(cls, type) and issubclass(cls, base_cls)):
        raise InvalidCheckpointingConfig(
            f"`{param_name}` must be a subclass of `{base_cls.__name__}`, "
            f"but got {cls}"
        )
    if inspect.isabstract(cls):
        raise InvalidCheckpointingConfig(
            f"`{param_name}` must be a concrete class, but {cls} is abstract "
            "(it does not implement all abstract methods of "
            f"`{base_cls.__name__}`)"
        )


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
        generated_id_column: Name of the ID column to generate a row ID for each row.
            Use this when you don't have an `id_column` in the input dataset.
            Currently, only Parquet files based data sources are supported for
            auto-generated row IDs feature.
        delete_checkpoint_on_success: If true, automatically delete checkpoint
            data when the dataset execution succeeds. Only supported for
            batch-based backend currently.
        override_filesystem: Override the :class:`pyarrow.fs.FileSystem` object used to
            read/write checkpoint data. Use this when you want to use custom credentials.
        override_backend: Override the :class:`CheckpointBackend` object used to
            access the checkpoint backend storage.
        write_num_threads: Number of threads used to write checkpoint files for
            completed rows.
        checkpoint_path_partition_filter: Filter for checkpoint files to load during
            restoration when reading from `checkpoint_path`.
        checkpoint_filter_cls: Override the :class:`~ray.data.checkpoint.CheckpointFilter`
            subclass used to filter out already-checkpointed rows during
            restoration. The class is instantiated once per checkpoint filter
            actor with ``(checkpoint_config, checkpoint_ref)``, where
            ``checkpoint_ref`` is the ``ObjectRef`` returned by the
            checkpoint manager's ``load_checkpoint`` (by default, a sorted
            NumPy array of checkpointed IDs). Defaults to
            :class:`~ray.data.checkpoint.NumpyArrayBasedCheckpointFilter`.
        checkpoint_manager_cls: Override the
            :class:`~ray.data.checkpoint.CheckpointManager` subclass used to
            load checkpoint data during restoration. The class is instantiated
            on the driver with ``(checkpoint_config=..., data_context=...)``
            and its ``load_checkpoint`` must return an ``(ObjectRef, int)``
            tuple: the ref is passed opaquely to ``checkpoint_filter_cls``,
            and the int (size in bytes) feeds the per-actor memory
            reservation. Typically customized together with
            ``checkpoint_filter_cls``. Defaults to
            :class:`~ray.data.checkpoint.IdColumnCheckpointManager`.
    """

    DEFAULT_CHECKPOINT_PATH_BUCKET_ENV_VAR = "RAY_DATA_CHECKPOINT_PATH_BUCKET"
    DEFAULT_CHECKPOINT_PATH_DIR = "ray_data_checkpoint"
    CHECKPOINT_ACTOR_POOL_MIN_SIZE = 1
    CHECKPOINT_ACTOR_POOL_MAX_SIZE = 10
    CHECKPOINT_ACTOR_MEMORY_BYTES = 1 * 1024**3

    def __init__(
        self,
        id_column: Optional[str] = None,
        checkpoint_path: Optional[str] = None,
        *,
        generated_id_column: Optional[str] = None,
        delete_checkpoint_on_success: bool = True,
        override_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        override_backend: Optional[CheckpointBackend] = None,
        write_num_threads: int = 3,
        checkpoint_path_partition_filter: Optional["PathPartitionFilter"] = None,
        checkpoint_filter_cls: Optional[Type["CheckpointFilter"]] = None,
        checkpoint_manager_cls: Optional[Type["CheckpointManager"]] = None,
    ):
        self.generated_id_column: Optional[str] = generated_id_column

        # Validate that we don't have both `id_column` and `generated_id_column`
        # explicitly specified
        if id_column is not None and generated_id_column is not None:
            raise InvalidCheckpointingConfig(
                "Cannot specify both `id_column` and `generated_id_column`. "
                "Use `id_column` when you have an existing ID column in your dataset, "
                "or use `generated_id_column` when you want to generate row IDs "
                "automatically."
            )

        # If no `id_column` is provided, use the generated row ID column
        elif id_column is None and generated_id_column is None:
            raise InvalidCheckpointingConfig(
                "Either `id_column` or `generated_id_column` must be provided. "
                "Use `id_column` when you have an existing ID column in your dataset, "
                "or use `generated_id_column` when you want to generate row IDs "
                "automatically."
            )
        elif id_column is None:
            # Use generated_id_column as the id_column
            id_column = generated_id_column

        self.id_column: Optional[str] = id_column

        if not isinstance(self.id_column, str) or len(self.id_column) == 0:
            raise InvalidCheckpointingConfig(
                "Checkpoint ID column must be a non-empty string, "
                f"but got {self.id_column}"
            )

        if checkpoint_filter_cls is not None:
            from ray.data.checkpoint.checkpoint_filter import CheckpointFilter

            _validate_checkpoint_cls(
                checkpoint_filter_cls, CheckpointFilter, "checkpoint_filter_cls"
            )

        if checkpoint_manager_cls is not None:
            from ray.data.checkpoint.checkpoint_filter import CheckpointManager

            _validate_checkpoint_cls(
                checkpoint_manager_cls, CheckpointManager, "checkpoint_manager_cls"
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
        self.write_num_threads: int = write_num_threads
        self.checkpoint_path_partition_filter = checkpoint_path_partition_filter
        self.checkpoint_filter_cls = checkpoint_filter_cls
        self.checkpoint_manager_cls = checkpoint_manager_cls
        self.checkpoint_actor_pool_min_size = self.CHECKPOINT_ACTOR_POOL_MIN_SIZE
        self.checkpoint_actor_pool_max_size = self.CHECKPOINT_ACTOR_POOL_MAX_SIZE
        self.checkpoint_actor_memory_bytes = self.CHECKPOINT_ACTOR_MEMORY_BYTES

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

    @property
    def has_generated_id_column(self) -> bool:
        """Whether this config uses auto-generated row IDs."""
        return self.generated_id_column is not None


def is_generated_id_checkpoint(config: Optional["CheckpointConfig"]) -> bool:
    """Whether ``config`` uses auto-generated row IDs (None-safe)."""
    return getattr(config, "has_generated_id_column", False)


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
