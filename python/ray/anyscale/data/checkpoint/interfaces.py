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

    Currently, there are two types of backends: batch-based and row-based.
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

    # AWS S3, batched.
    S3_BATCH = "S3_BATCH"

    # Disk/Filesystem, batched.
    # Note: if the job is running on multiple nodes, the
    # path must be a network-mounted filesystem (e.g. `/mnt/cluster_storage/`)
    DISK_BATCH = "DISK_BATCH"

    # AWS S3, row based. It is recommended to use the batched version above.
    S3_ROW = "S3_ROW"

    # Disk/Filesystem, row based. It is recommended to use the batched version above.
    # Note: if the job is running on multiple nodes, the
    # path must be a network-mounted filesystem (e.g. `/mnt/cluster_storage/`)
    DISK_ROW = "DISK_ROW"


@dataclass
class CheckpointConfig:
    """Configuration for row-level checkpointing."""

    # These parameters MUST be specified by the user prior to using checkpointing.
    enabled: bool = False
    backend: Optional[CheckpointBackend] = None
    id_col: Optional[str] = None

    # Optional filesystem object used to read/write checkpoint files.
    # If not specified, constructs a Pyarrow FileSystem when necessary.
    fs: Optional[pyarrow.fs.FileSystem] = None

    # Optional function which takes an input row and generates an
    # ID value "on the fly", to be used in the `id_col` column.
    # This function MUST deterministically generate a ID value from the row.
    generate_id_column_fn: Optional[Callable[[Row], str]] = None

    # Optional path where checkpoint files are written.
    # If not specified, use default paths configured for each backend.
    output_path: Optional[str] = None

    # Number of threads used to filter checkpointed rows.
    filter_num_threads: int = 3
    # Number of threads used to write checkpoint files for completed rows.
    write_num_threads: int = 3

    _skip_ckpt: bool = False

    def is_row_based(self):
        """Whether the checkpoint backend is row-based."""
        return self.backend in [
            CheckpointBackend.DISK_ROW,
            CheckpointBackend.S3_ROW,
        ]

    def is_batch_based(self):
        """Whether the checkpoint backend is batch-based."""
        return self.backend in [
            CheckpointBackend.DISK_BATCH,
            CheckpointBackend.S3_BATCH,
        ]


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

    def get_output_path(self, config: CheckpointConfig) -> str:
        output_path = config.output_path
        if output_path:
            return output_path
        else:
            default_output_path = self._get_default_ckpt_output_path()
            if default_output_path is None:
                raise ValueError("CheckpointConfig.output_path must be set")
            return default_output_path

    @abc.abstractmethod
    def _get_default_ckpt_output_path(self) -> Optional[str]:
        """Returns the default path where checkpoint files are written,
        or `None` if the path cannot be inferred."""
        ...


class S3CheckpointIO(CheckpointIO):
    """CheckpointIO for S3 backends."""

    def get_output_path(self, config: CheckpointConfig) -> str:
        return _unwrap_protocol(super().get_output_path(config))

    def _get_default_ckpt_output_path(self) -> Optional[str]:
        artifact_storage = os.environ.get("ANYSCALE_ARTIFACT_STORAGE")
        if artifact_storage is None:
            return None
        return f"{artifact_storage}/ray_data_checkpoint"


class DiskCheckpointIO(CheckpointIO):
    """CheckpointIO for disk backends."""

    def _get_default_ckpt_output_path(self) -> Optional[str]:
        return "/mnt/cluster_storage/ray_data_checkpoint"


class CheckpointFilter(CheckpointIO, abc.ABC):
    """Abstract class which defines the interface for filtering checkpointed rows
    based on varying backends.

    Subclasses must implement `.filter_rows_for_block()`."""

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.output_path = self.get_output_path(config)
        self.id_col = self.ckpt_config.id_col
        self.fs = self.ckpt_config.fs
        self.filter_num_threads = self.ckpt_config.filter_num_threads
        self.generate_id_column_fn = self.ckpt_config.generate_id_column_fn

        if self.id_col is None:
            raise InvalidCheckpointingConfig("Checkpoint ID column must be specified")

    def generate_id_column_for_block(self, block: Block) -> Block:
        """For the given block, add a new column containing a deterministically
        generated ID for each row, and return the resulting block.

        This is useful for generating IDs 'on the fly' if the input dataset
        does not already have an ID column."""
        if not self.generate_id_column_fn:
            return block

        block_accessor = BlockAccessor.for_block(block)
        ids = []
        for row in block_accessor.iter_rows(False):
            ids.append(self.generate_id_column_fn(row))
        arrow_block = block_accessor.to_arrow()
        id_block = arrow_block.append_column(self.id_col, [ids])
        return id_block

    def generate_id_column_for_batch(self, batch: DataBatch) -> DataBatch:
        """For the given block, add a new column containing a deterministically
        generated ID for each row, and return the resulting block.

        Note that this method calls `generate_id_column_for_block()` under the hood,
        so it is preferred to call that method directly if you already have a block."""
        arrow_block = BlockAccessor.batch_to_block(batch)
        id_block = self.generate_id_column_for_block(arrow_block)
        id_batch = BlockAccessor.for_block(id_block).to_batch_format(None)
        return id_batch


class RowBasedCheckpointFilter(CheckpointFilter):
    """CheckpointFiter for row-based backends."""

    @staticmethod
    def create(config: CheckpointConfig) -> "RowBasedCheckpointFilter":
        """Factory method to create a `RowBasedCheckpointFilter` based on the
        provided `CheckpointConfig`."""
        assert config.is_row_based()
        backend = config.backend
        if backend == CheckpointBackend.S3_ROW:
            from ray.anyscale.data.checkpoint.checkpoint_s3_row import (
                RowBasedS3CheckpointFilter,
            )

            return RowBasedS3CheckpointFilter(config)
        if backend == CheckpointBackend.DISK_ROW:
            from ray.anyscale.data.checkpoint.checkpoint_disk_row import (
                RowBasedDiskCheckpointFilter,
            )

            return RowBasedDiskCheckpointFilter(config)

        raise InvalidCheckpointingConfig(f"Backend {backend} not implemented")

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

        if backend == CheckpointBackend.S3_BATCH:
            from ray.anyscale.data.checkpoint.checkpoint_s3 import S3CheckpointFilter

            return S3CheckpointFilter(config)
        if backend == CheckpointBackend.DISK_BATCH:
            from ray.anyscale.data.checkpoint.checkpoint_disk import (
                DiskCheckpointFilter,
            )

            return DiskCheckpointFilter(config)

        raise InvalidCheckpointingConfig(f"Backend {backend} not implemented")

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

        mask = pc.is_in(block[self.id_col], value_set=checkpointed_ids[self.id_col])
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
        self.output_path = self.get_output_path(config)
        self.id_col = self.ckpt_config.id_col
        if self.id_col is None:
            raise InvalidCheckpointingConfig("Checkpoint ID column must be specified")

        self.fs = self.ckpt_config.fs
        self.write_num_threads = self.ckpt_config.write_num_threads

    def write_block_checkpoint(self, block: BlockAccessor):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.output_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        raise NotImplementedError()

    @staticmethod
    def create(config: CheckpointConfig) -> "CheckpointWriter":
        """Factory method to create a `CheckpointWriter` based on the
        provided `CheckpointConfig`."""
        backend = config.backend

        if backend == CheckpointBackend.S3_BATCH:
            from ray.anyscale.data.checkpoint.checkpoint_s3 import S3CheckpointWriter

            return S3CheckpointWriter(config)
        if backend == CheckpointBackend.DISK_BATCH:
            from ray.anyscale.data.checkpoint.checkpoint_disk import (
                DiskCheckpointWriter,
            )

            return DiskCheckpointWriter(config)

        if backend == CheckpointBackend.S3_ROW:
            from ray.anyscale.data.checkpoint.checkpoint_s3_row import (
                RowBasedS3CheckpointWriter,
            )

            return RowBasedS3CheckpointWriter(config)
        if backend == CheckpointBackend.DISK_ROW:
            from ray.anyscale.data.checkpoint.checkpoint_disk_row import (
                RowBasedDiskCheckpointWriter,
            )

            return RowBasedDiskCheckpointWriter(config)

        raise InvalidCheckpointingConfig(f"Backend {backend} not implemented")
