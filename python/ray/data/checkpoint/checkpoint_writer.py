import logging
import os
import uuid
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pyarrow import parquet as pq
from pyarrow.fs import FileType

if TYPE_CHECKING:
    import pyarrow

from ray.data._internal.utils.util import call_with_retry
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointBackend, CheckpointConfig
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)

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
        checkpoint_id: str,
    ) -> Optional[PendingCheckpoint]:
        """Write a pending checkpoint for 2-phase commit.

        This is called BEFORE the data file is written. The checkpoint filename
        is deterministic (based on checkpoint_id), enabling idempotent writes
        on retry. For file-based datasinks, the checkpoint filename matches
        the data file prefix, enabling recovery to match pending checkpoints
        to data files via prefix trie.

        Args:
            id_column_data: PyArrow array containing the ID column values.
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

    def _prepare_checkpoint_table_from_id_column(self, id_column_data: "pyarrow.Array"):
        """Prepare the checkpoint table from ID column data.

        Args:
            id_column_data: PyArrow array containing the ID column values.

        Returns:
            PyArrow table with checkpoint IDs.
        """
        import pyarrow as pa

        return pa.table({self.id_col: id_column_data})

    def write_block_checkpoint(self, block: BlockAccessor):
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
        id_column_data,
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
            id_column_data
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
