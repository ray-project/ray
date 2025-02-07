import abc
import os
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional

import pyarrow
import pyarrow.compute as pc

from ray.data._internal.execution.operators.map_transformer import Row
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.datasource.path_util import _unwrap_protocol


class CheckpointBackend(Enum):
    """Supported backends for storing and reading checkpoint files.

    Currently, there are two types of backends:
        * Batch-based backends: CLOUD_OBJECT_STORAGE and FILE_STORAGE.
        * Row-based backends: CLOUD_OBJECT_STORAGE_ROW and FILE_STORAGE_ROW.

    Their differences are as follows:
    1. Writing checkpoints:
       * Batch-based backends write a checkpoint file for each block.
       * Row-based backends write a checkpoint file for each individual row.
    2. Loading checkpoints and filtering input data:
       * Batch-based backends load all checkpoint data into memory prior to
         dataset execution. The checkpoint data is then passed to each
         read task to perform filtering.
       * Row-based backends do not preload any data at the execution start-up.
         Instead, during the read tasks, each row is filtered based on whether it
         already exists in the backend.

    Overall, batch-based backends are recommended due to their lower runtime
    overheads. However, they may introduce a delay in job start-up due to the
    checkpoint loading process.
    """

    # TODO(haochen): Deprecate row-based backends when we make sure the
    # checkpoint loading overhead of the batch-based backends is acceptable
    # for all workloads.

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

    CLOUD_OBJECT_STORAGE_ROW = "CLOUD_OBJECT_STORAGE_ROW"
    """
    Batch-based checkpoint backend that uses cloud object storage, such as
    AWS S3, Google Cloud Storage, etc.
    It's more recommended to use the batch-based version.
    """

    FILE_STORAGE_ROW = "FILE_STORAGE_ROW"
    """
    Batch based checkpoint backend that uses file system storage.
    Note, when using this backend, the checkpoint path must be a network-mounted
    file system (e.g. `/mnt/cluster_storage/`).
    It's more recommended to use the batch-based version.
    """


@dataclass
class CheckpointConfig:
    """Configuration for row-level checkpointing.

    Attribute:
        backend: The storage backend to use for checkpointing.
        id_column: Name of the ID column in the input dataset.
            ID values must the unique across all rows in the dataset and must persist
            during all operators.
        delete_checkpoint_on_success: If true, automatically delete checkpoint
            data when the dataset execution succeeds. Only supported for
            batch-based backend currently.
        fs: Optional filesystem object used to read/write checkpoint files.
            If not specified, constructs a Pyarrow FileSystem when necessary.
        checkpoint_path: Optional path where checkpoint files are written.
            If not specified, use default paths configured for each backend.
        filter_num_threads: Number of threads used to filter checkpointed rows.
            Only used for row-based backends.
        write_num_threads: Number of threads used to write checkpoint files for
            completed rows.
    """

    backend: CheckpointBackend

    id_column: str

    delete_checkpoint_on_success: bool = True

    fs: Optional[pyarrow.fs.FileSystem] = None

    checkpoint_path: Optional[str] = None

    filter_num_threads: int = 3

    write_num_threads: int = 3

    def is_row_based(self):
        """Whether the checkpoint backend is row-based."""
        return self.backend in [
            CheckpointBackend.FILE_STORAGE_ROW,
            CheckpointBackend.CLOUD_OBJECT_STORAGE_ROW,
        ]

    def is_batch_based(self):
        """Whether the checkpoint backend is batch-based."""
        return self.backend in [
            CheckpointBackend.FILE_STORAGE,
            CheckpointBackend.CLOUD_OBJECT_STORAGE,
        ]

    def id_column_name(self) -> Optional[str]:
        """Returns the name of the ID column, if `id_col` is a string."""
        return self.id_column if isinstance(self.id_column, str) else None

    def id_column_function(self) -> Optional[Callable[[Row], str]]:
        """Returns the ID generation function, if `id_col` is a function."""
        return self.id_column if callable(self.id_column) else None

    def __post_init__(self):
        if not isinstance(self.backend, CheckpointBackend):
            raise InvalidCheckpointingConfig(
                f"Checkpoint backend is invalid {self.backend}, "
                f"available options: {[backend.value for backend in CheckpointBackend]}"
            )
        if (
            self.id_column is None
            or not isinstance(self.id_column, str)
            or len(self.id_column) == 0
        ):
            raise InvalidCheckpointingConfig(
                "Checkpoint ID column must be as an non-empty string, "
                f"but got {self.id_column}"
            )


class InvalidCheckpointingConfig(Exception):
    """Exception which indicates that the checkpointing
    configuration is invalid."""

    pass


class InvalidCheckpointingOperators(Exception):
    """Exception which indicates that the DAG is not
    eligible for row-based checkpointing, due to
    one or more incompatible operators."""

    pass


class CheckpointIO(abc.ABC):
    """Base class for checkpoint IO operations."""

    def get_checkpoint_path(self, config: CheckpointConfig) -> str:
        checkpoint_path = config.checkpoint_path
        if checkpoint_path:
            return checkpoint_path
        else:
            default_checkpoint_path = self._get_default_checkpoint_path()
            if default_checkpoint_path is None:
                raise ValueError("CheckpointConfig.checkpoint_path must be set")
            return default_checkpoint_path

    @abc.abstractmethod
    def _get_default_checkpoint_path(self) -> Optional[str]:
        """Returns the default path where checkpoint files are written,
        or `None` if the path cannot be inferred."""
        ...


class CloudObjectStorageCheckpointIO(CheckpointIO):
    """CheckpointIO for cloud object storage backends."""

    def get_checkpoint_path(self, config: CheckpointConfig) -> str:
        return _unwrap_protocol(super().get_checkpoint_path(config))

    def _get_default_checkpoint_path(self) -> Optional[str]:
        artifact_storage = os.environ.get("ANYSCALE_ARTIFACT_STORAGE")
        if artifact_storage is None:
            return None
        return f"{artifact_storage}/ray_data_checkpoint"


class FileStorageCheckpointIO(CheckpointIO):
    """CheckpointIO for FILE_STORAGE backends."""

    def _get_default_checkpoint_path(self) -> Optional[str]:
        return "/mnt/cluster_storage/ray_data_checkpoint"


class CheckpointFilter(CheckpointIO, abc.ABC):
    """Abstract class which defines the interface for filtering checkpointed rows
    based on varying backends.
    """

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.checkpoint_path = self.get_checkpoint_path(config)
        self.id_column = self.ckpt_config.id_column
        self.fs = self.ckpt_config.fs
        self.filter_num_threads = self.ckpt_config.filter_num_threads


class RowBasedCheckpointFilter(CheckpointFilter):
    """CheckpointFiter for row-based backends."""

    @staticmethod
    def create(config: CheckpointConfig) -> "RowBasedCheckpointFilter":
        """Factory method to create a `RowBasedCheckpointFilter` based on the
        provided `CheckpointConfig`."""
        assert config.is_row_based()
        backend = config.backend
        if backend == CheckpointBackend.CLOUD_OBJECT_STORAGE_ROW:
            from ray.anyscale.data.checkpoint.checkpoint_cloud_object_storage_row import (
                RowBasedCloudObjectStorageCheckpointFilter,
            )

            return RowBasedCloudObjectStorageCheckpointFilter(config)
        if backend == CheckpointBackend.FILE_STORAGE_ROW:
            from ray.anyscale.data.checkpoint.checkpoint_file_storage_row import (
                RowBasedFileStorageCheckpointFilter,
            )

            return RowBasedFileStorageCheckpointFilter(config)

        raise NotImplementedError(f"Backend {backend} not implemented")

    @abc.abstractmethod
    def filter_rows_for_block(self, block: Block) -> Block:
        """For the given block, filter out rows that have already
        been checkpointed, and return the resulting block.

        Subclasses must implement this method.

        Args:
            block: The input block to filter.
        Returns:
            A new block with rows that have not been checkpointed.
        """
        ...

    def filter_rows_for_batch(self, batch: DataBatch) -> DataBatch:
        """For the given batch, filter out rows that have already
        been checkpointed, and return the resulting batch.

        Note that this method calls `filter_rows_for_block()` under the hood,
        so it is preferred to call that method directly if you already have a block."""
        arrow_block = BlockAccessor.batch_to_block(batch)
        filtered_block = self.filter_rows_for_block(arrow_block)
        filtered_batch = BlockAccessor.for_block(filtered_block).to_batch_format(None)
        return filtered_batch


class BatchBasedCheckpointFilter(CheckpointFilter):
    """CheckpointFilter for batch-based backends."""

    @staticmethod
    def create(config: CheckpointConfig) -> "BatchBasedCheckpointFilter":
        """Factory method to create a `BatchBasedCheckpointFilter` based on the
        provided `CheckpointConfig`."""
        assert config.is_batch_based()
        backend = config.backend

        if backend == CheckpointBackend.CLOUD_OBJECT_STORAGE:
            from ray.anyscale.data.checkpoint.checkpoint_cloud_object_storage import (
                CloudObjectStorageCheckpointFilter,
            )

            return CloudObjectStorageCheckpointFilter(config)
        if backend == CheckpointBackend.FILE_STORAGE:
            from ray.anyscale.data.checkpoint.checkpoint_file_storage import (
                FileStorageCheckpointFilter,
            )

            return FileStorageCheckpointFilter(config)

        raise NotImplementedError(f"Backend {backend} not implemented")

    @abc.abstractmethod
    def load_checkpoint(self) -> Block:
        """Load checkpointed ids as a block."""
        ...

    @abc.abstractmethod
    def delete_checkpoint(self):
        """Delete the checkpoint data."""
        ...

    def filter_rows_for_block(
        self,
        block: Block,
        checkpointed_ids: Block,
    ) -> Block:
        """For the given block, filter out rows that have already
        been checkpointed, and return the resulting block.

        Subclasses must implement this method.

        Args:
            block: The input block to filter.
            checkpointed_ids: A block containing IDs of all rows that have
                been checkpointed.
        Returns:
            A new block with rows that have not been checkpointed.
        """

        if len(checkpointed_ids) == 0:
            return block

        assert isinstance(block, pyarrow.Table)
        assert isinstance(checkpointed_ids, pyarrow.Table)

        mask = pc.is_in(
            block[self.id_column], value_set=checkpointed_ids[self.id_column]
        )
        mask = pc.invert(mask)
        return block.filter(mask)

    def filter_rows_for_batch(
        self,
        batch: DataBatch,
        checkpointed_ids: Block,
    ) -> DataBatch:
        """For the given batch, filter out rows that have already
        been checkpointed, and return the resulting batch.

        Note that this method calls `filter_rows_for_block()` under the hood,
        so it is preferred to call that method directly if you already have a block."""
        arrow_block = BlockAccessor.batch_to_block(batch)
        filtered_block = self.filter_rows_for_block(arrow_block, checkpointed_ids)
        filtered_batch = BlockAccessor.for_block(filtered_block).to_batch_format(None)
        return filtered_batch


class CheckpointWriter(CheckpointIO):
    """Abstract class which defines the interface for writing row-level
    checkpoints based on varying backends.

    Subclasses must implement `.write_block_checkpoint()`."""

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.checkpoint_path = self.get_checkpoint_path(config)
        self.id_col = self.ckpt_config.id_column
        self.fs = self.ckpt_config.fs
        self.write_num_threads = self.ckpt_config.write_num_threads

    def write_block_checkpoint(self, block: BlockAccessor):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        raise NotImplementedError()

    @staticmethod
    def create(config: CheckpointConfig) -> "CheckpointWriter":
        """Factory method to create a `CheckpointWriter` based on the
        provided `CheckpointConfig`."""
        backend = config.backend

        if backend == CheckpointBackend.CLOUD_OBJECT_STORAGE:
            from ray.anyscale.data.checkpoint.checkpoint_cloud_object_storage import (
                CloudObjectStorageCheckpointWriter,
            )

            return CloudObjectStorageCheckpointWriter(config)
        if backend == CheckpointBackend.FILE_STORAGE:
            from ray.anyscale.data.checkpoint.checkpoint_file_storage import (
                FileStorageCheckpointWriter,
            )

            return FileStorageCheckpointWriter(config)

        if backend == CheckpointBackend.CLOUD_OBJECT_STORAGE_ROW:
            from ray.anyscale.data.checkpoint.checkpoint_cloud_object_storage_row import (
                RowBasedCloudObjectStorageCheckpointWriter,
            )

            return RowBasedCloudObjectStorageCheckpointWriter(config)
        if backend == CheckpointBackend.FILE_STORAGE_ROW:
            from ray.anyscale.data.checkpoint.checkpoint_file_storage_row import (
                RowBasedFileStorageCheckpointWriter,
            )

            return RowBasedFileStorageCheckpointWriter(config)

        raise NotImplementedError(f"Backend {backend} not implemented")
