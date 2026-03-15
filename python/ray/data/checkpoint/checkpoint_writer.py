import logging
import os
import pickle
import uuid
from abc import abstractmethod
from typing import Any

from pyarrow import parquet as pq

from ray._common.retry import call_with_retry
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
    def write_block_checkpoint(self, block: BlockAccessor, **kwargs):
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
        raise NotImplementedError(f"Backend {backend} not implemented")


class BatchBasedCheckpointWriter(CheckpointWriter):
    """CheckpointWriter for batch-based backends."""

    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        self.filesystem.create_dir(self.checkpoint_path_unwrapped, recursive=True)

    def write_block_checkpoint(
        self, block: BlockAccessor, write_result: Any = None, **kwargs
    ):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Subclasses of `CheckpointWriter` must implement this method."""
        if block.num_rows() == 0:
            return

        file_id = str(uuid.uuid4())
        file_name = f"{file_id}.parquet"
        ckpt_file_path = os.path.join(self.checkpoint_path_unwrapped, file_name)
        meta_bytes = None
        meta_path = None
        meta_tmp_path = None
        if write_result is not None:
            # Serialize first so pickling errors don't occur after the parquet file
            # has already been persisted.
            #
            # Write metadata before parquet to avoid a "parquet-only" partial state:
            # the restore path reads checkpoint IDs from parquet and may skip rows,
            # while Iceberg write-result recovery reads .meta.pkl. If we crash after
            # parquet but before metadata, we can skip rows without recovering the
            # corresponding IcebergWriteResult, leading to silent data loss.
            meta_bytes = pickle.dumps(write_result)
            meta_name = f"{file_id}.meta.pkl"
            meta_path = os.path.join(self.checkpoint_path_unwrapped, meta_name)
            meta_tmp_path = os.path.join(
                self.checkpoint_path_unwrapped, f".{meta_name}.tmp"
            )

        checkpoint_ids_block = block.select(columns=[self.id_col])
        # `pyarrow.parquet.write_parquet` requires a PyArrow table. It errors if the block is
        # a pandas DataFrame.
        checkpoint_ids_table = BlockAccessor.for_block(checkpoint_ids_block).to_arrow()

        try:
            if meta_bytes is not None:

                def _write_meta():
                    with self.filesystem.open_output_stream(meta_tmp_path) as f:
                        f.write(meta_bytes)
                    # Use a temp file + move to make metadata creation atomic.
                    # If move isn't atomic for this filesystem, the loader also
                    # validates metadata/parquet pairing before loading metadata.
                    try:
                        self.filesystem.move(meta_tmp_path, meta_path)
                    except Exception:
                        try:
                            self.filesystem.delete_file(meta_path)
                        except Exception:
                            pass
                        self.filesystem.move(meta_tmp_path, meta_path)

                call_with_retry(
                    _write_meta,
                    description=f"Write checkpoint metadata file: {file_id}.meta.pkl",
                    match=DataContext.get_current().retried_io_errors,
                )

            def _write_parquet():
                pq.write_table(
                    checkpoint_ids_table,
                    ckpt_file_path,
                    filesystem=self.filesystem,
                )

            return call_with_retry(
                _write_parquet,
                description=f"Write checkpoint file: {file_name}",
                match=DataContext.get_current().retried_io_errors,
            )
        except Exception:
            logger.exception(f"Checkpoint write failed: {file_name}")
            raise
