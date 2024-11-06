from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable, Optional

from ray.data._internal.execution.operators.map_transformer import Row
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import DataContext

if TYPE_CHECKING:
    import pyarrow


class CheckpointBackend(Enum):
    """Supported backends for storing and reading checkpoint files."""

    # AWS S3
    S3 = "S3"

    # Disk/Filesystem
    # Note: if the job is running on multiple nodes, the
    # path must be a network-mounted filesystem (e.g. `/mnt/cluster_storage/`)
    DISK = "DISK"


@dataclass
class CheckpointConfig:
    """Configuration for row-level checkpointing."""

    # These parameters MUST be specified by the user prior to using checkpointing.
    enabled: bool = False
    backend: Optional[CheckpointBackend] = None
    id_col: Optional[str] = None

    # Optional filesystem object used to read/write checkpoint files.
    # If not specified, constructs a Pyarrow FileSystem when necessary.
    fs: Optional["pyarrow.fs.FileSystem"] = None

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


class InvalidCheckpointingConfig(Exception):
    """Exception which indicates that the checkpointing
    configuration is invalid."""

    pass


class InvalidCheckpointingOperators(Exception):
    """Exception which indicates that the DAG is not
    eligible for row-based checkpointing, due to
    one or more incompatible operators."""

    pass


class CheckpointFilter:
    """Abstract class which defines the interface for filtering checkpointed rows
    based on varying backends.

    Subclasses must implement `.filter_rows_for_block()`."""

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config

        # TODO(scottjlee-ckpt): append job ID or other unique ID to checkpoint bucket?
        self.output_path = self.ckpt_config.output_path
        self.id_col = self.ckpt_config.id_col
        self.fs = self.ckpt_config.fs
        self.filter_num_threads = self.ckpt_config.filter_num_threads
        self.generate_id_column_fn = self.ckpt_config.generate_id_column_fn

        if self.id_col is None:
            raise InvalidCheckpointingConfig("Checkpoint ID column must be specified")

    def _get_default_ckpt_output_path(self) -> Optional[str]:
        """Returns the default path where checkpoint files are written,
        or `None` if the path cannot be inferred.
        Subclasses of `CheckpointFilter` must implement this method."""
        raise NotImplementedError()

    def filter_rows_for_block(self, block: Block) -> Block:
        """For the given block, filter out rows that have already
        been checkpointed, and return the resulting block.

        Subclasses of `CheckpointFilter` must implement this method."""
        raise NotImplementedError()

    def filter_rows_for_batch(self, batch: DataBatch) -> DataBatch:
        """For the given batch, filter out rows that have already
        been checkpointed, and return the resulting batch.

        Note that this method calls `filter_rows_for_block()` under the hood,
        so it is preferred to call that method directly if you already have a block."""
        arrow_block = BlockAccessor.batch_to_block(batch)
        filtered_block = self.filter_rows_for_block(arrow_block)
        filtered_batch = BlockAccessor.for_block(filtered_block).to_batch_format(None)
        return filtered_batch

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

    @staticmethod
    def create_checkpoint_filter(
        config: Optional[CheckpointConfig] = None,
    ) -> "CheckpointFilter":
        """Factory method to create a `CheckpointFilter` based on the
        provided `CheckpointConfig`; if no config is provided, use the current
        configuration in `DataContext.get_current().checkpoint_config`."""
        if config is None:
            config = DataContext.get_current().checkpoint_config

        backend = config.backend

        if backend == CheckpointBackend.S3:
            from ray.anyscale.data.checkpoint.checkpoint_s3_row import (
                RowBasedS3CheckpointFilter,
            )

            return RowBasedS3CheckpointFilter(config)
        if backend == CheckpointBackend.DISK:
            from ray.anyscale.data.checkpoint.checkpoint_disk_row import (
                RowBasedDiskCheckpointFilter,
            )

            return RowBasedDiskCheckpointFilter(config)

        raise InvalidCheckpointingConfig(f"Backend {backend} not implemented")


class CheckpointWriter:
    """Abstract class which defines the interface for writing row-level
    checkpoints based on varying backends.

    Subclasses must implement `.write_block_checkpoint()`."""

    def __init__(self, config: CheckpointConfig):
        if config.output_path is None:
            config.output_path = self._get_default_ckpt_output_path()

        self.ckpt_config = config
        self.output_path = self.ckpt_config.output_path
        self.id_col = self.ckpt_config.id_col
        if self.id_col is None:
            raise InvalidCheckpointingConfig("Checkpoint ID column must be specified")

        self.fs = self.ckpt_config.fs
        self.write_num_threads = self.ckpt_config.write_num_threads

    def _get_default_ckpt_output_path(self) -> Optional[str]:
        """Returns the default path where checkpoint files are written,
        or `None` if the path cannot be inferred.
        Subclasses of `CheckpointWriter` must implement this method."""
        raise NotImplementedError()

    def write_block_checkpoint(self, block: BlockAccessor):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.output_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        raise NotImplementedError()

    @staticmethod
    def create_checkpoint_writer(
        config: Optional[CheckpointConfig] = None,
    ) -> "CheckpointWriter":
        """Factory method to create a `CheckpointWriter` based on the
        provided `CheckpointConfig`; if no config is provided, use the current
        configuration in `DataContext.get_current().checkpoint_config`."""
        if config is None:
            config = DataContext.get_current().checkpoint_config

        backend = config.backend
        if backend == CheckpointBackend.S3:
            from ray.anyscale.data.checkpoint.checkpoint_s3_row import (
                RowBasedS3CheckpointWriter,
            )

            return RowBasedS3CheckpointWriter(config)
        if backend == CheckpointBackend.DISK:
            from ray.anyscale.data.checkpoint.checkpoint_disk_row import (
                RowBasedDiskCheckpointWriter,
            )

            return RowBasedDiskCheckpointWriter(config)

        raise InvalidCheckpointingConfig(f"Backend {backend} not implemented")
