import logging
import os
import uuid
from abc import abstractmethod

from pyarrow import parquet as pq

from ray.data._internal.util import call_with_retry
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointBackend, CheckpointConfig
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)


class CheckpointWriter:
    """Abstract class which defines the interface for writing row-level
    checkpoints based on varying backends.

    Subclasses must implement `.write_block_checkpoint()`."""

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.checkpoint_path_unwrapped = _unwrap_protocol(
            self.ckpt_config.checkpoint_path
        )
        self.id_col = self.ckpt_config.id_column
        self.filesystem = self.ckpt_config.filesystem
        self.write_num_threads = self.ckpt_config.write_num_threads

    @abstractmethod
    def write_block_checkpoint(self, block: BlockAccessor):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        ...

    @staticmethod
    def create(config: CheckpointConfig) -> "CheckpointWriter":
        """Factory method to create a `CheckpointWriter` based on the
        provided `CheckpointConfig`."""
        backend = config.backend

        if backend in [
            CheckpointBackend.CLOUD_OBJECT_STORAGE,
            CheckpointBackend.FILE_STORAGE,
        ]:
            return BatchBasedCheckpointWriter(config)
        if backend == CheckpointBackend.ICEBERG:
            return IcebergCheckpointWriter(config)
        raise NotImplementedError(f"Backend {backend} not implemented")


class IcebergCheckpointWriter(CheckpointWriter):
    """CheckpointWriter that writes to an Iceberg table."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)
        from ray.data._internal.datasource.iceberg_datasink import IcebergDatasink
        from pyiceberg.catalog import load_catalog
        from pyiceberg.exceptions import NoSuchTableError
        import pyarrow as pa

        # Try to load or create the table
        catalog_kwargs = self.ckpt_config.catalog_kwargs.copy()
        catalog_name = catalog_kwargs.pop("name", "default")
        catalog = load_catalog(catalog_name, **catalog_kwargs)

        table_identifier = self.ckpt_config.checkpoint_path
        # on_write_start needs to be called to initialize the table and schema
        # We'll use a simple schema for the checkpoint table
        schema = pa.schema([(self.id_col, pa.int64())])

        try:
            catalog.load_table(table_identifier)
        except NoSuchTableError:
            # Create the table if it doesn't exist
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, LongType

            iceberg_schema = Schema(
                NestedField(
                    field_id=1, name=self.id_col, field_type=LongType(), required=True
                )
            )
            catalog.create_table(table_identifier, schema=iceberg_schema)

        self.datasink = IcebergDatasink(
            table_identifier=table_identifier,
            catalog_kwargs=self.ckpt_config.catalog_kwargs,
        )
        self.datasink.on_write_start(schema)

    def write_block_checkpoint(self, block: BlockAccessor):
        if block.num_rows() == 0:
            return

        # Ensure table is loaded (needed when running on worker after unpickling)
        if self.datasink._table is None:
            self.datasink._reload_table()

        checkpoint_ids_block = block.select(columns=[self.id_col])
        # Use TaskContext for worker-side write
        from ray.data._internal.execution.interfaces.task_context import TaskContext

        ctx = TaskContext(task_idx=0, op_name="checkpoint_write")
        write_result = self.datasink.write([checkpoint_ids_block], ctx)

        # Commit on the worker side for simplicity in checkpointing
        # Normally on_write_complete runs on the driver, but here each worker
        # can commit its own checkpoint IDs to the Iceberg table.
        from ray.data.datasource.datasink import WriteResult

        self.datasink.on_write_complete(
            WriteResult(
                num_rows=block.num_rows(),
                size_bytes=block.size_bytes(),
                write_returns=[write_result],
            )
        )


class BatchBasedCheckpointWriter(CheckpointWriter):
    """CheckpointWriter for batch-based backends."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        self.filesystem.create_dir(self.checkpoint_path_unwrapped, recursive=True)

    def write_block_checkpoint(self, block: BlockAccessor):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        if block.num_rows() == 0:
            return

        file_name = f"{uuid.uuid4()}.parquet"
        ckpt_file_path = os.path.join(self.checkpoint_path_unwrapped, file_name)

        checkpoint_ids_block = block.select(columns=[self.id_col])
        # `pyarrow.parquet.write_parquet` requires a PyArrow table. It errors if the block is
        # a pandas DataFrame.
        checkpoint_ids_table = BlockAccessor.for_block(checkpoint_ids_block).to_arrow()

        def _write():
            pq.write_table(
                checkpoint_ids_table,
                ckpt_file_path,
                filesystem=self.filesystem,
            )

        try:
            return call_with_retry(
                _write,
                description=f"Write checkpoint file: {file_name}",
                match=DataContext.get_current().retried_io_errors,
            )
        except Exception:
            logger.exception(f"Checkpoint write failed: {file_name}")
            raise
