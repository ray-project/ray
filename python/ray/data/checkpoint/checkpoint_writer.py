import logging
import os
import uuid
from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional

from pyarrow import parquet as pq

from ray.data._internal.util import call_with_retry
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointBackend, CheckpointConfig
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)

# Metadata key for storing data file path in checkpoint parquet files
DATA_FILE_PATH_METADATA_KEY = b"ray.data.checkpoint.data_file_path"

# Suffix for pending checkpoint files (2-phase commit)
PENDING_CHECKPOINT_SUFFIX = ".pending"


@dataclass
class PendingCheckpoint:
    """Represents a pending checkpoint file for 2-phase commit.

    Attributes:
        pending_path: Path to the pending checkpoint file.
        committed_path: Path where the checkpoint will be after commit.
        data_file_path: Path to the corresponding data file.
    """

    pending_path: str
    committed_path: str
    data_file_path: str


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
    def write_block_checkpoint(
        self,
        block: BlockAccessor,
        data_file_path: Optional[str] = None,
    ):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Args:
            block: The block accessor containing the data to checkpoint.
            data_file_path: Optional path to the data file that was written.
                This is stored in checkpoint metadata for 2-phase commit support,
                enabling mapping between data files and checkpoint files.

        Subclasses of `CheckpointWriter` must implement this method."""
        ...

    def write_pending_checkpoint(
        self,
        block: BlockAccessor,
        expected_data_file_path: str,
    ) -> Optional[PendingCheckpoint]:
        """Write a pending checkpoint for 2-phase commit.

        This is called BEFORE the data file is written. The checkpoint is
        written as a pending file with the expected data file path in metadata.

        Args:
            block: The block accessor containing the data to checkpoint.
            expected_data_file_path: The expected path where data will be written.

        Returns:
            PendingCheckpoint object for later commit, or None if block is empty.
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

    def _prepare_checkpoint_table(
        self,
        block: BlockAccessor,
        data_file_path: Optional[str] = None,
    ):
        """Prepare the checkpoint table with optional data file path metadata.

        Args:
            block: The block accessor containing the data to checkpoint.
            data_file_path: Optional path to the data file.

        Returns:
            PyArrow table with checkpoint IDs and metadata.
        """
        checkpoint_ids_block = block.select(columns=[self.id_col])
        # `pyarrow.parquet.write_parquet` requires a PyArrow table.
        checkpoint_ids_table = BlockAccessor.for_block(checkpoint_ids_block).to_arrow()

        # Add data file path to schema metadata for checkpoint-to-data mapping
        if data_file_path is not None:
            existing_metadata = checkpoint_ids_table.schema.metadata or {}
            new_metadata = {
                **existing_metadata,
                DATA_FILE_PATH_METADATA_KEY: data_file_path.encode("utf-8"),
            }
            checkpoint_ids_table = checkpoint_ids_table.replace_schema_metadata(
                new_metadata
            )

        return checkpoint_ids_table

    def write_block_checkpoint(
        self,
        block: BlockAccessor,
        data_file_path: Optional[str] = None,
    ):
        """Write a checkpoint for all rows in a single block to the checkpoint
        output directory given by `self.checkpoint_path`.

        Args:
            block: The block accessor containing the data to checkpoint.
            data_file_path: Optional path to the data file that was written.
                This is stored in checkpoint metadata for 2-phase commit support.
        """
        if block.num_rows() == 0:
            return

        file_name = f"{uuid.uuid4()}.parquet"
        ckpt_file_path = os.path.join(self.checkpoint_path_unwrapped, file_name)

        checkpoint_ids_table = self._prepare_checkpoint_table(block, data_file_path)

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

    def write_pending_checkpoint(
        self,
        block: BlockAccessor,
        expected_data_file_path: str,
    ) -> Optional[PendingCheckpoint]:
        """Write a pending checkpoint for 2-phase commit.

        This is called BEFORE the data file is written. The checkpoint is
        written as a pending file with the expected data file path in metadata.

        Args:
            block: The block accessor containing the data to checkpoint.
            expected_data_file_path: The expected path where data will be written.

        Returns:
            PendingCheckpoint object for later commit, or None if block is empty.
        """
        if block.num_rows() == 0:
            return None

        # Generate checkpoint filename with pending suffix
        checkpoint_id = uuid.uuid4().hex
        pending_file_name = f"{checkpoint_id}{PENDING_CHECKPOINT_SUFFIX}.parquet"
        committed_file_name = f"{checkpoint_id}.parquet"

        pending_path = os.path.join(self.checkpoint_path_unwrapped, pending_file_name)
        committed_path = os.path.join(
            self.checkpoint_path_unwrapped, committed_file_name
        )

        checkpoint_ids_table = self._prepare_checkpoint_table(
            block, expected_data_file_path
        )

        def _write():
            pq.write_table(
                checkpoint_ids_table,
                pending_path,
                filesystem=self.filesystem,
            )

        try:
            call_with_retry(
                _write,
                description=f"Write pending checkpoint file: {pending_file_name}",
                match=DataContext.get_current().retried_io_errors,
            )
            logger.debug(
                f"Written pending checkpoint: {pending_file_name} "
                f"for data file: {expected_data_file_path}"
            )
            return PendingCheckpoint(
                pending_path=pending_path,
                committed_path=committed_path,
                data_file_path=expected_data_file_path,
            )
        except Exception:
            logger.exception(f"Pending checkpoint write failed: {pending_file_name}")
            raise

    def commit_checkpoint(self, pending: PendingCheckpoint) -> None:
        """Commit a pending checkpoint by renaming it to committed.

        This is called AFTER the data file is successfully written.

        Args:
            pending: The PendingCheckpoint to commit.
        """

        def _rename():
            self.filesystem.move(pending.pending_path, pending.committed_path)

        try:
            call_with_retry(
                _rename,
                description=f"Commit checkpoint: {pending.pending_path}",
                match=DataContext.get_current().retried_io_errors,
            )
            logger.debug(
                f"Committed checkpoint: {pending.committed_path} "
                f"for data file: {pending.data_file_path}"
            )
        except Exception:
            logger.exception(f"Checkpoint commit failed: {pending.pending_path}")
            raise
