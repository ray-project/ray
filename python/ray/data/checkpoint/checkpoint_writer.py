import logging
import os
import uuid
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pyarrow import parquet as pq
from pyarrow.fs import FileType

from ray.data._internal.util import call_with_retry
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointBackend, CheckpointConfig
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)

# Metadata keys for storing data file location in checkpoint parquet files.
# The directory and filename prefix are stored separately to avoid unnecessary
# join/split round-tripping, and to make partitioned write handling explicit.
DATA_FILE_DIR_METADATA_KEY = b"ray.data.checkpoint.data_file_dir"
DATA_FILE_PREFIX_METADATA_KEY = b"ray.data.checkpoint.data_file_prefix"

# Suffix for pending checkpoint files (2-phase commit)
PENDING_CHECKPOINT_SUFFIX = ".pending"


@dataclass
class PendingCheckpoint:
    """Represents a pending checkpoint file for 2-phase commit.

    Attributes:
        pending_path: Path to the pending checkpoint file.
        committed_path: Path where the checkpoint will be after commit.
    """

    pending_path: str
    committed_path: str


class CheckpointWriter:
    """Abstract class which defines the interface for writing row-level
    checkpoints based on varying backends.

    Subclasses must implement `.write_block_checkpoint()`.

    For 2-phase commit support, subclasses should also implement:
    - `.write_pending_checkpoint()`: Write checkpoint as pending file
    - `.commit_checkpoint()`: Rename pending to committed
    """

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

        This is used for non-file datasinks (SQL, MongoDB, etc.) where there's
        no predictable file path to store in checkpoint metadata. For file-based
        datasinks that need 2-phase commit with data file path tracking, use
        `write_pending_checkpoint()` and `commit_checkpoint()` instead.

        Args:
            block: The block accessor containing the data to checkpoint.

        Subclasses of `CheckpointWriter` must implement this method."""
        ...

    def write_pending_checkpoint(
        self,
        id_column_data: "pyarrow.Array",
        data_file_dir: str,
        data_file_prefix: str,
        checkpoint_id: str,
    ) -> Optional[PendingCheckpoint]:
        """Write a pending checkpoint for 2-phase commit.

        This is called BEFORE the data file is written. The checkpoint is
        written as a pending file with the expected data file location in metadata.

        Note: This method intentionally receives only the ID column data, not
        the full block. The expected data file path must be computable without
        block content (from write_uuid and task_idx only) to enable 2-phase
        commit - we need to know the path BEFORE writing to support rollback.

        Args:
            id_column_data: PyArrow array containing the ID column values.
            data_file_dir: The directory where data files will be written.
                May include a protocol prefix (e.g., s3://bucket/path).
            data_file_prefix: The filename prefix for data files (without
                extension). Used for prefix matching during recovery.
            checkpoint_id: Deterministic identifier for the checkpoint file,
                derived from write_uuid and task_idx. Must be the same on retry
                to ensure idempotent writes.

        Returns:
            PendingCheckpoint object for later commit, or None if empty.
        """
        raise NotImplementedError(
            "2-phase commit not implemented for this checkpoint writer"
        )

    def commit_checkpoint(self, pending: PendingCheckpoint) -> None:
        """Commit a pending checkpoint by renaming it to committed.

        This is called AFTER the data file is successfully written.

        Args:
            pending: The PendingCheckpoint to commit.
        """
        raise NotImplementedError(
            "2-phase commit not implemented for this checkpoint writer"
        )

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

    def _prepare_checkpoint_table_from_block(self, block: BlockAccessor):
        """Prepare the checkpoint table from a block.

        Args:
            block: The block accessor containing the data to checkpoint.

        Returns:
            PyArrow table with checkpoint IDs.
        """
        checkpoint_ids_block = block.select(columns=[self.id_col])
        # `pyarrow.parquet.write_parquet` requires a PyArrow table.
        return BlockAccessor.for_block(checkpoint_ids_block).to_arrow()

    def _prepare_checkpoint_table_from_id_column(
        self,
        id_column_data: "pyarrow.Array",
        data_file_dir: Optional[str] = None,
        data_file_prefix: Optional[str] = None,
    ):
        """Prepare the checkpoint table from ID column data with optional data file location metadata.

        Args:
            id_column_data: PyArrow array containing the ID column values.
            data_file_dir: Optional directory where data files are written.
            data_file_prefix: Optional filename prefix for data files.

        Returns:
            PyArrow table with checkpoint IDs and metadata.
        """
        import pyarrow as pa

        checkpoint_ids_table = pa.table({self.id_col: id_column_data})
        return self._add_data_file_path_metadata(
            checkpoint_ids_table, data_file_dir, data_file_prefix
        )

    def _add_data_file_path_metadata(
        self, table, data_file_dir: Optional[str], data_file_prefix: Optional[str]
    ):
        """Add data file directory and prefix to table schema metadata."""
        if data_file_dir is not None and data_file_prefix is not None:
            existing_metadata = table.schema.metadata or {}
            new_metadata = {
                **existing_metadata,
                DATA_FILE_DIR_METADATA_KEY: data_file_dir.encode("utf-8"),
                DATA_FILE_PREFIX_METADATA_KEY: data_file_prefix.encode("utf-8"),
            }
            table = table.replace_schema_metadata(new_metadata)
        return table

    def write_block_checkpoint(self, block: BlockAccessor) -> None:
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        This is used for non-file datasinks (SQL, MongoDB, etc.) where there's
        no predictable file path to store in checkpoint metadata.

        Args:
            block: The block accessor containing the data to checkpoint.
        """
        if block.num_rows() == 0:
            return

        file_name = f"{uuid.uuid4()}.parquet"
        ckpt_file_path = os.path.join(self.checkpoint_path_unwrapped, file_name)

        checkpoint_ids_table = self._prepare_checkpoint_table_from_block(block)

        def _write():
            pq.write_table(
                checkpoint_ids_table,
                ckpt_file_path,
                filesystem=self.filesystem,
            )

        try:
            call_with_retry(
                _write,
                description=f"Write checkpoint file: {file_name}",
                match=DataContext.get_current().retried_io_errors,
            )
        except Exception:
            logger.exception(f"Checkpoint write failed: {file_name}")
            raise

    def write_pending_checkpoint(
        self,
        id_column_data: "pyarrow.Array",
        data_file_dir: str,
        data_file_prefix: str,
        checkpoint_id: str,
    ) -> Optional[PendingCheckpoint]:
        if len(id_column_data) == 0:
            return None
        pending_file_name = f"{checkpoint_id}{PENDING_CHECKPOINT_SUFFIX}.parquet"
        committed_file_name = f"{checkpoint_id}.parquet"

        pending_path = os.path.join(self.checkpoint_path_unwrapped, pending_file_name)
        committed_path = os.path.join(
            self.checkpoint_path_unwrapped, committed_file_name
        )

        checkpoint_ids_table = self._prepare_checkpoint_table_from_id_column(
            id_column_data, data_file_dir, data_file_prefix
        )

        def _write():
            pq.write_table(
                checkpoint_ids_table,
                pending_path,
                filesystem=self.filesystem,
            )

        call_with_retry(
            _write,
            description=f"Write pending checkpoint file: {pending_file_name}",
            match=DataContext.get_current().retried_io_errors,
        )
        return PendingCheckpoint(
            pending_path=pending_path,
            committed_path=committed_path,
        )

    def commit_checkpoint(self, pending: PendingCheckpoint) -> None:
        """Commit a pending checkpoint by renaming it to committed.

        This is called AFTER the data file is successfully written.

        This operation is idempotent: if the committed file already exists
        (and pending doesn't), it's considered already committed. This handles
        the case where a retry happens after successful commit (e.g., network
        timeout after move succeeded but before acknowledgment).

        Args:
            pending: The PendingCheckpoint to commit.
        """

        def _rename():
            # Check if already committed (idempotent)
            committed_info = self.filesystem.get_file_info(pending.committed_path)
            pending_info = self.filesystem.get_file_info(pending.pending_path)

            committed_exists = committed_info.type != FileType.NotFound
            pending_exists = pending_info.type != FileType.NotFound

            if committed_exists:
                # Already committed. Clean up pending file if it exists.
                if pending_exists:
                    self.filesystem.delete_file(pending.pending_path)
                return

            if not pending_exists:
                raise FileNotFoundError(
                    f"Neither pending ({pending.pending_path}) nor committed "
                    f"({pending.committed_path}) checkpoint exists"
                )

            # Normal case: move pending to committed
            self.filesystem.move(pending.pending_path, pending.committed_path)

        call_with_retry(
            _rename,
            description=f"Commit checkpoint: {pending.pending_path}",
            match=DataContext.get_current().retried_io_errors,
        )
