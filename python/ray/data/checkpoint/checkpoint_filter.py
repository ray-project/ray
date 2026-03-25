import abc
import logging
import os
import posixpath
import sys
import time
from abc import abstractmethod
from typing import List, Optional, Tuple

import numpy as np
import pyarrow
from pyarrow.fs import FileSelector, FileType

import ray
from ray._common.retry import call_with_retry
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.arrow_ops.transform_pyarrow import combine_chunks
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data.block import Block, BlockMetadata, Schema
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_writer import PENDING_CHECKPOINT_SUFFIX
from ray.data.checkpoint.util import build_pending_checkpoint_trie
from ray.data.datasource.path_util import _unwrap_protocol
from ray.types import ObjectRef

logger = logging.getLogger(__name__)

# Retry configuration for checkpoint recovery operations.
# These can be overridden via environment variables for testing or tuning.
CHECKPOINT_RECOVERY_MAX_ATTEMPTS = int(
    os.environ.get("RAY_DATA_CHECKPOINT_RECOVERY_MAX_ATTEMPTS", "3")
)
CHECKPOINT_RECOVERY_MAX_BACKOFF_S = int(
    os.environ.get("RAY_DATA_CHECKPOINT_RECOVERY_MAX_BACKOFF_S", "8")
)


def _numpy_size(array: np.ndarray) -> int:
    """Calculate the size of a numpy ndarray."""
    total_size = array.nbytes
    if array.dtype == object:
        sample_count = 10**4

        if len(array) <= sample_count:
            for item in array.flat:
                total_size += sys.getsizeof(item)
        else:
            sample_total_size = 0
            for item in array[:sample_count].flat:
                sample_total_size += sys.getsizeof(item)
            total_size += sample_total_size / sample_count * len(array)
    return total_size


@ray.remote(num_cpus=0)
def _clean_pending_checkpoints_task(
    checkpoint_path_unwrapped: str,
    checkpoint_filesystem: pyarrow.fs.FileSystem,
    data_file_dir_unwrapped: str,
    data_file_filesystem: pyarrow.fs.FileSystem,
) -> int:
    """Delete data files that have matching pending checkpoint files, then
    delete the pending checkpoints.

    This runs as a remote task to avoid blocking the driver during potentially
    slow filesystem operations (especially on cloud storage like S3).

    Algorithm:
    1. List all files in checkpoint dir, find those ending with .pending.parquet
    2. Build a PrefixTrie from their basenames (strip .pending.parquet)
    3. List all data files in data_file_dir (recursively for partitions)
    4. For each data file, if trie.has_prefix_of(basename) -> delete it
    5. Delete all the pending checkpoint files
    6. Return count of pending checkpoints cleaned

    Args:
        checkpoint_path_unwrapped: The unwrapped checkpoint path.
        checkpoint_filesystem: The filesystem for checkpoint files.
        data_file_dir_unwrapped: The unwrapped directory where data files are
            written (protocol prefix already stripped).
        data_file_filesystem: The filesystem for data files. May differ from
            checkpoint_filesystem (e.g., checkpoints on local disk, data on S3).

    Returns:
        Number of pending checkpoints cleaned.
    """

    def _clean() -> int:
        # 1. List all files in checkpoint dir, find pending ones
        ckpt_files = checkpoint_filesystem.get_file_info(
            FileSelector(checkpoint_path_unwrapped, recursive=False)
        )
        pending_suffix = f"{PENDING_CHECKPOINT_SUFFIX}.parquet"
        pending_file_paths = [
            f
            for f in ckpt_files
            if f.type == FileType.File and f.path.endswith(pending_suffix)
        ]

        if not pending_file_paths:
            return 0

        # 2. Build prefix trie from pending checkpoint basenames
        trie = build_pending_checkpoint_trie(pending_file_paths, pending_suffix)

        # 3. List all data files (recursively for partitions)
        data_files = data_file_filesystem.get_file_info(
            FileSelector(data_file_dir_unwrapped, recursive=True, allow_not_found=True)
        )

        # 4. Delete data files matching a pending checkpoint prefix
        for f in data_files:
            if f.type != FileType.File:
                continue
            basename = posixpath.basename(f.path)
            if trie.has_prefix_of(basename):
                data_file_filesystem.delete_file(f.path)

        # 5. Delete all pending checkpoint files
        for f in pending_file_paths:
            checkpoint_filesystem.delete_file(f.path)

        return len(pending_file_paths)

    return call_with_retry(
        _clean,
        description="clean pending checkpoints",
        max_attempts=CHECKPOINT_RECOVERY_MAX_ATTEMPTS,
        max_backoff_s=CHECKPOINT_RECOVERY_MAX_BACKOFF_S,
    )


@ray.remote(num_returns=2)
def convert_checkpointed_ids(
    checkpointed_ids_arrow: Block, id_column: str
) -> Tuple[np.ndarray, int]:
    """Convert checkpointed IDs from pyarrow.Table to np.ndarray.

    Args:
        checkpointed_ids_arrow: A pyarrow.Table containing the checkpointed
            IDs, loaded from the checkpoint parquet files.
        id_column: The id column of `checkpoint_ids_array`.

    Returns:
        A tuple of:
        - The checkpointed IDs of type numpy.ndarray, which can be passed
          directly to each checkpoint filter actor.
        - The size (bytes) of the ndarray, which can be used to determine
          the `ray_remote_args` of each checkpoint filter actor.
    """
    checkpointed_ids_ndarray = np.array([])

    try:
        if checkpointed_ids_arrow.num_rows != 0:
            combined = combine_chunks(checkpointed_ids_arrow)
            ckpt_chunks = combined[id_column].chunks

            arrays = []
            for chunk in ckpt_chunks:
                arrays.append(transform_pyarrow.to_numpy(chunk, zero_copy_only=False))
            checkpointed_ids_ndarray = np.concatenate(arrays)
    except Exception as e:
        raise RuntimeError(f"Failed to get numpy-typed checkpointed IDs: {e}")

    checkpoint_size = _numpy_size(checkpointed_ids_ndarray)
    return checkpointed_ids_ndarray, checkpoint_size


class CheckpointManager:
    """Manage checkpoint data."""

    def __init__(self, checkpoint_config: CheckpointConfig):
        """Initialize the CheckpointLoader.

        Args:
            checkpoint_config: the checkpoint config.
        """
        self.checkpoint_path = checkpoint_config.checkpoint_path
        self.filesystem = checkpoint_config.filesystem
        self.id_column = checkpoint_config.id_column
        self.checkpoint_path_partition_filter = (
            checkpoint_config.checkpoint_path_partition_filter
        )
        self.checkpoint_path_unwrapped = _unwrap_protocol(
            checkpoint_config.checkpoint_path
        )

    def load_checkpoint(
        self,
        data_file_dir: Optional[str] = None,
        data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> Tuple[Optional[ObjectRef[np.ndarray]], int]:
        """Loading checkpoint data.

        This method first cleans up any pending checkpoints from incomplete
        2-phase commits, then loads the committed checkpoint data.

        Args:
            data_file_dir: Optional directory where data files are written.
                If provided, pending checkpoints will be used to find and
                delete matching data files before loading.
            data_file_filesystem: Optional filesystem for data files. If not
                provided, defaults to the checkpoint filesystem. Should be
                provided when data files are on a different filesystem than
                checkpoints.

        Returns:
            ObjectRef: The ref of checkpointed IDs array. None if no checkpoint was loaded.
            int: the size of the checkpointed IDs array.
        """
        start_t = time.time()

        # Clean up pending checkpoints before loading (runs as a Ray task)
        if data_file_dir is not None:
            self._clean_pending_checkpoints(data_file_dir, data_file_filesystem)

        # Load the checkpoint data
        checkpoint_ds: ray.data.Dataset = ray.data.read_parquet(
            self.checkpoint_path,
            filesystem=self.filesystem,
            partition_filter=self.checkpoint_path_partition_filter,
        )

        # Manually disable checkpointing for loading the checkpoint metadata
        # to avoid recursively restoring checkpoints.
        # TODO: Clean way to do this would be to introduce per Op config
        # [https://github.com/ray-project/ray/issues/54520]
        checkpoint_ds.context.checkpoint_config = None

        # Pre-process data pipeline
        checkpoint_ds: ray.data.Dataset = self._preprocess_data_pipeline(checkpoint_ds)

        # Repartition to 1 block.
        checkpoint_ds = checkpoint_ds.repartition(num_blocks=1)

        # Get the block reference
        ref_bundles: List[RefBundle] = list(checkpoint_ds.iter_internal_ref_bundles())

        assert len(ref_bundles) == 1

        # If there are no valid files under the checkpoint_path, return None, 0.
        if ref_bundles[0].num_rows() == 0:
            return None, 0

        ref_bundle: RefBundle = ref_bundles[0]
        schema: Schema = ref_bundle.schema
        assert len(ref_bundle.blocks) == 1
        block_ref: ObjectRef[Block] = ref_bundle.blocks[0][0]
        metadata: BlockMetadata = ref_bundle.blocks[0][1]
        # Validate the loaded checkpoint
        self._validate_loaded_checkpoint(schema, metadata)

        # convert arrow-typed ids to numpy-typed ids.
        # Note: the convert is very time-consuming.
        # Get the object ref the checkpointed IDs, because we do not want the IDs
        # to occupy the memory of the head node.
        checkpointed_ids_ref, checkpoint_size_ref = convert_checkpointed_ids.remote(
            block_ref, self.id_column
        )

        checkpoint_size = ray.get(checkpoint_size_ref)

        logger.info(
            "Checkpoint loaded for %s in %.2f seconds. SizeBytes = %d, Schema = %s",
            type(self).__name__,
            time.time() - start_t,
            checkpoint_size,
            schema.to_string(),
        )
        return checkpointed_ids_ref, checkpoint_size

    def _clean_pending_checkpoints(
        self,
        data_file_dir: Optional[str],
        data_file_filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> None:
        """Clean up pending checkpoints from incomplete 2-phase commits.

        Finds pending checkpoint files, builds a prefix trie from their basenames,
        deletes matching data files, then deletes the pending checkpoints.

        Runs as a Ray task to avoid blocking the driver during potentially
        slow filesystem operations (especially on cloud storage like S3).

        Args:
            data_file_dir: The directory where data files are written.
            data_file_filesystem: The filesystem for data files. If not
                provided, defaults to the checkpoint filesystem.
        """
        if not data_file_dir:
            return
        if data_file_filesystem is None:
            data_file_filesystem = self.filesystem
        try:
            cleaned_count = ray.get(
                _clean_pending_checkpoints_task.remote(
                    self.checkpoint_path_unwrapped,
                    self.filesystem,
                    _unwrap_protocol(data_file_dir),
                    data_file_filesystem,
                )
            )
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} pending checkpoint(s)")
        except ray.exceptions.RayTaskError:
            logger.exception("Failed to clean up pending checkpoints")
            raise

    @abc.abstractmethod
    def _preprocess_data_pipeline(
        self, checkpoint_ds: ray.data.Dataset
    ) -> ray.data.Dataset:
        """Pre-process the checkpoint dataset. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement this method")

    def _validate_loaded_checkpoint(
        self, schema: Schema, metadata: BlockMetadata
    ) -> None:
        """Validate the loaded checkpoint. Subclasses can override for custom validation."""
        pass


class IdColumnCheckpointManager(CheckpointManager):
    """Loader for regular ID columns."""

    def _preprocess_data_pipeline(
        self, checkpoint_ds: ray.data.Dataset
    ) -> ray.data.Dataset:
        """In the pre-process data pipeline,
            - Sort by the IDs, as `filter_rows_for_block` will perform binary search on the
              checkpointed IDs during restore.

        Args:
            checkpoint_ds: The checkpoint dataset to pre-process

        Returns:
            The pre-processed checkpoint dataset
        """
        # Sort by the ID column.
        return checkpoint_ds.sort(self.id_column)


class CheckpointFilter(abc.ABC):
    """Abstract class which defines the interface for filtering checkpointed rows
    based on varying backends.
    """

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.id_column = self.ckpt_config.id_column

    @abstractmethod
    def filter_rows_for_block(self, block: Block) -> Block:
        """For the given block, filter out rows that have already
        been checkpointed, and return the resulting block.

        Args:
            block: The input block to filter.
        Returns:
            A new block with rows that have not been checkpointed.
        """
        raise NotImplementedError


class NumpyArrayBasedCheckpointFilter(CheckpointFilter):
    """CheckpointFilter for batch-based backends.

    This filter will first fetch the checkpointed IDs (as NumPy arrays) from the object store.
    For each input block, it filters the block and returns the filtered block.
    """

    def __init__(
        self,
        checkpoint_config: CheckpointConfig,
        checkpoint_ref: ObjectRef[np.ndarray],
    ):
        super().__init__(checkpoint_config)
        self.checkpointed_ids = ray.get(checkpoint_ref)
        assert isinstance(self.checkpointed_ids, np.ndarray)

    def filter_rows_for_block(
        self,
        block: Block,
    ) -> Block:
        """Filter IDs in memory using NumPy's binary search."""

        if self.checkpointed_ids.shape[0] == 0 or len(block) == 0:
            return block

        assert isinstance(block, pyarrow.Table)

        # The checkpointed_ids block is sorted (see load_checkpoint).
        # We'll use binary search to filter out processed rows.

        # Convert the block's ID column to a numpy array for fast processing.
        combined = combine_chunks(block)
        block_ids = transform_pyarrow.to_numpy(
            combined[self.id_column], zero_copy_only=False
        )

        # Start with a mask of all True (keep all rows).
        mask = np.ones(len(block_ids), dtype=bool)
        # Use binary search to find where block_ids would be in ckpt_ids.
        sorted_indices = np.searchsorted(self.checkpointed_ids, block_ids)
        # Only consider indices that are within bounds.
        valid_indices = sorted_indices < len(self.checkpointed_ids)
        # For valid indices, check for exact matches.
        potential_matches = sorted_indices[valid_indices]
        matched = self.checkpointed_ids[potential_matches] == block_ids[valid_indices]
        # Mark matched IDs as False (filter out these rows).
        mask[valid_indices] = ~matched

        # Convert the final mask to a PyArrow array and filter the block.
        mask_array = pyarrow.array(mask)
        filtered_block = block.filter(mask_array)
        return filtered_block
