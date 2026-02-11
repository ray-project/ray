import abc
import logging
import os
import posixpath
import time
from typing import Dict, List, Optional, Tuple

import numpy
import pyarrow
from pyarrow import parquet as pq
from pyarrow.fs import FileSelector, FileType

import ray
from ray._common.retry import call_with_retry
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data.block import Block, BlockAccessor, BlockMetadata, DataBatch, Schema
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_writer import (
    DATA_FILE_DIR_METADATA_KEY,
    DATA_FILE_PREFIX_METADATA_KEY,
    PENDING_CHECKPOINT_SUFFIX,
)
from ray.data.datasource import PathPartitionFilter
from ray.data.datasource.path_util import (
    _resolve_single_path_with_fallback,
    _unwrap_protocol,
)
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


class CheckpointFilter(abc.ABC):
    """Abstract class which defines the interface for filtering checkpointed rows
    based on varying backends.
    """

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.checkpoint_path = self.ckpt_config.checkpoint_path
        self.checkpoint_path_unwrapped = _unwrap_protocol(
            self.ckpt_config.checkpoint_path
        )
        self.id_column = self.ckpt_config.id_column
        self.filesystem = self.ckpt_config.filesystem
        self.filter_num_threads = self.ckpt_config.filter_num_threads


@ray.remote(num_cpus=0)
def _clean_pending_checkpoints_task(
    checkpoint_path_unwrapped: str,
    filesystem: pyarrow.fs.FileSystem,
) -> int:
    """Ray task to recover pending checkpoints from incomplete 2-phase commits.

    This runs as a remote task to avoid blocking the driver during potentially
    slow filesystem operations (especially on cloud storage like S3).

    Args:
        checkpoint_path_unwrapped: The unwrapped checkpoint path.
        filesystem: The filesystem to use.

    Returns:
        Number of pending checkpoints recovered.
    """

    def impl() -> int:
        # List all files in the checkpoint directory
        file_infos = filesystem.get_file_info(
            FileSelector(checkpoint_path_unwrapped, recursive=False)
        )

        pending_suffix = f"{PENDING_CHECKPOINT_SUFFIX}.parquet"
        recovered_count = 0
        # Cache for file info to avoid redundant filesystem calls.
        # Maps directory path to (filesystem, file_info_list) tuples since
        # data files may be on a different filesystem than checkpoints.
        # TODO: Replace with a FileIndexer abstraction that encapsulates
        # filesystem resolution, directory listing, and caching.
        data_file_info_cache: Dict[
            str, Tuple[pyarrow.fs.FileSystem, List[pyarrow.fs.FileInfo]]
        ] = {}

        for file_info in file_infos:
            if file_info.type != FileType.File:
                continue

            file_path = file_info.path
            if not file_path.endswith(pending_suffix):
                continue

            logger.debug(f"Found pending checkpoint file: {file_path}")

            # Read metadata from pending checkpoint to get data file location
            data_file_info = _get_data_file_info_from_checkpoint(file_path, filesystem)

            if data_file_info is not None:
                data_file_dir, data_file_prefix = data_file_info
                # Note: _delete_matching_data_files resolves the filesystem from
                # data_file_dir itself, which may be different from the checkpoint
                # filesystem (e.g., checkpoint on local disk, output on S3).
                deleted_files = _delete_matching_data_files(
                    data_file_dir, data_file_prefix, data_file_info_cache
                )
                if deleted_files:
                    logger.debug(
                        f"Deleted {len(deleted_files)} data file(s) for "
                        f"dir={data_file_dir}, prefix={data_file_prefix}: "
                        f"{deleted_files}"
                    )

            # Delete the pending checkpoint file
            filesystem.delete_file(file_path)
            recovered_count += 1
            logger.debug(
                f"Deleted pending checkpoint (recovered_count={recovered_count}): "
                f"data_file_info={data_file_info}, checkpoint_file={file_path}"
            )

        return recovered_count

    return call_with_retry(
        impl,
        description="recover pending checkpoints",
        max_attempts=CHECKPOINT_RECOVERY_MAX_ATTEMPTS,
        max_backoff_s=CHECKPOINT_RECOVERY_MAX_BACKOFF_S,
    )


def _get_data_file_info_from_checkpoint(
    checkpoint_path: str,
    filesystem: pyarrow.fs.FileSystem,
) -> Optional[Tuple[str, str]]:
    """Read the data file directory and prefix from checkpoint file metadata.

    Args:
        checkpoint_path: Path to the checkpoint parquet file.
        filesystem: The filesystem to use for reading the file.

    Returns:
        A tuple of (data_file_dir, data_file_prefix), or None if not found.
    """
    # Open the file through the filesystem to support cloud storage (S3, GCS, etc.)
    # where checkpoint_path is an unwrapped path without protocol prefix.
    with filesystem.open_input_file(checkpoint_path) as f:
        schema = pq.read_schema(f)
    metadata = schema.metadata

    if (
        metadata
        and DATA_FILE_DIR_METADATA_KEY in metadata
        and DATA_FILE_PREFIX_METADATA_KEY in metadata
    ):
        data_file_dir = metadata[DATA_FILE_DIR_METADATA_KEY].decode("utf-8")
        data_file_prefix = metadata[DATA_FILE_PREFIX_METADATA_KEY].decode("utf-8")
        return (data_file_dir, data_file_prefix)
    return None


def _delete_matching_data_files(
    data_file_dir: str,
    data_file_prefix: str,
    data_file_info_cache: Dict[
        str, Tuple[pyarrow.fs.FileSystem, List[pyarrow.fs.FileInfo]]
    ],
) -> List[str]:
    """Delete all data files matching a filename prefix in the given directory.

    This function handles both non-partitioned and partitioned writes:
    - Non-partitioned: files are in base directory (e.g., output/file-0.parquet)
    - Partitioned: files are in subdirectories (e.g., output/col=val/file-0.parquet)

    For partitioned writes, we need to recursively search subdirectories to find
    all files matching the prefix. The prefix is just the base filename, so files
    in any partition subdirectory with matching names will be deleted.

    The filesystem is resolved from data_file_dir itself, not passed in.
    This is important because the data files may be on a different filesystem
    than the checkpoint files (e.g., checkpoint on local disk, output on S3).

    Args:
        data_file_dir: Directory containing data files. May include a protocol
            prefix (e.g., s3://bucket/path).
        data_file_prefix: Filename prefix for data files. Any file whose
            basename starts with this prefix will be deleted (searched
            recursively).
        data_file_info_cache: Cache mapping directory paths to tuples of
            (filesystem, file_info_list) to avoid redundant filesystem resolution
            and listing calls.

    Returns:
        List of deleted file paths.
    """
    # Resolve the filesystem from data_file_dir itself.
    # This handles cross-filesystem scenarios where checkpoint is on local disk
    # but output is on S3/GCS/etc.
    filesystem, dir_path = _resolve_single_path_with_fallback(
        data_file_dir, filesystem=None
    )

    # Use cache if available, otherwise fetch from filesystem.
    # Use recursive=True to find files in partition subdirectories
    # (e.g., output/col=val/file-0.parquet when using partition_cols).
    if dir_path in data_file_info_cache:
        cached_filesystem, file_infos = data_file_info_cache[dir_path]
        # Use the cached filesystem for operations
        filesystem = cached_filesystem
    else:
        file_infos = filesystem.get_file_info(FileSelector(dir_path, recursive=True))
        data_file_info_cache[dir_path] = (filesystem, file_infos)

    deleted_files: List[str] = []
    for file_info in file_infos:
        if file_info.type != FileType.File:
            continue
        # Use basename to match files regardless of which subdirectory they're in.
        # This handles partitioned writes where the same base filename appears
        # in multiple partition directories (e.g., output/a=1/file.parquet,
        # output/a=2/file.parquet).
        filename = posixpath.basename(file_info.path)
        if filename.startswith(data_file_prefix):
            filesystem.delete_file(file_info.path)
            deleted_files.append(file_info.path)

    return deleted_files


@ray.remote(max_retries=-1)
def _combine_chunks(ckpt_block: pyarrow.Table) -> pyarrow.Table:
    """Combine chunks for the checkpoint block.

    Args:
        ckpt_block: The checkpoint block to combine chunks for

    Returns:
        The combined checkpoint block
    """
    from ray.data._internal.arrow_ops.transform_pyarrow import combine_chunks

    combined_ckpt_block = combine_chunks(ckpt_block)
    logger.debug(
        "Checkpoint block stats for id column checkpoint: Combined block: type=%s, %d rows, %d bytes",
        combined_ckpt_block.schema.to_string(),
        combined_ckpt_block.num_rows,
        combined_ckpt_block.nbytes,
    )

    return combined_ckpt_block


class CheckpointLoader:
    """Loading checkpoint data."""

    def __init__(
        self,
        checkpoint_path: str,
        filesystem: pyarrow.fs.FileSystem,
        id_column: str,
        checkpoint_path_partition_filter: Optional[PathPartitionFilter] = None,
    ):
        """Initialize the CheckpointLoader.

        Args:
            checkpoint_path: The path to the checkpoint
            filesystem: The filesystem to use
            id_column: The name of the ID column
            checkpoint_path_partition_filter: Filter for checkpoint files to load during
                restoration when reading from `checkpoint_path`.
        """
        self.checkpoint_path = checkpoint_path
        self.filesystem = filesystem
        self.id_column = id_column
        self.checkpoint_path_partition_filter = checkpoint_path_partition_filter

    def load_checkpoint(self) -> ObjectRef[Block]:
        """Loading checkpoint data.

        Returns:
            ObjectRef[Block]: ObjectRef to the checkpointed IDs block.
        """
        start_t = time.time()

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
        ref_bundle: RefBundle = ref_bundles[0]
        schema: Schema = ref_bundle.schema
        assert len(ref_bundle.blocks) == 1
        block_ref: ObjectRef[Block] = ref_bundle.blocks[0][0]
        metadata: BlockMetadata = ref_bundle.blocks[0][1]

        # Post-process the block
        checkpoint_block_ref: ObjectRef[Block] = self._postprocess_block(block_ref)

        # Validate the loaded checkpoint
        self._validate_loaded_checkpoint(schema, metadata)

        logger.info(
            "Checkpoint loaded for %s in %.2f seconds. SizeBytes = %d, Schema = %s",
            type(self).__name__,
            time.time() - start_t,
            metadata.size_bytes,
            schema.to_string(),
        )

        return checkpoint_block_ref

    @abc.abstractmethod
    def _preprocess_data_pipeline(
        self, checkpoint_ds: ray.data.Dataset
    ) -> ray.data.Dataset:
        """Pre-process the checkpoint dataset. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement this method")

    def _postprocess_block(self, block_ref: ObjectRef[Block]) -> ObjectRef[Block]:
        """Combine the block so it has fewer chunks."""
        return _combine_chunks.remote(block_ref)

    def _validate_loaded_checkpoint(
        self, schema: Schema, metadata: BlockMetadata
    ) -> None:
        """Validate the loaded checkpoint. Subclasses can override for custom validation."""
        pass


class IdColumnCheckpointLoader(CheckpointLoader):
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


class BatchBasedCheckpointFilter(CheckpointFilter):
    """CheckpointFilter for batch-based backends."""

    def load_checkpoint(self) -> ObjectRef[Block]:
        """Load checkpointed ids as a sorted block.

        This method first cleans up any pending checkpoints from incomplete
        2-phase commits, then loads the committed checkpoint data.

        Returns:
            ObjectRef[Block]: ObjectRef to the checkpointed IDs block.
        """
        # Clean up pending checkpoints before loading (runs as a Ray task)
        self._clean_pending_checkpoints()

        loader = IdColumnCheckpointLoader(
            checkpoint_path=self.checkpoint_path,
            filesystem=self.filesystem,
            id_column=self.id_column,
            checkpoint_path_partition_filter=self.ckpt_config.checkpoint_path_partition_filter,
        )
        return loader.load_checkpoint()

    def _clean_pending_checkpoints(self) -> None:
        """Clean up pending checkpoints from incomplete 2-phase commits.

        Runs as a Ray task to avoid blocking the driver during potentially
        slow filesystem operations (especially on cloud storage like S3).
        """
        try:
            cleaned_count = ray.get(
                _clean_pending_checkpoints_task.remote(
                    self.checkpoint_path_unwrapped,
                    self.filesystem,
                )
            )
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} pending checkpoint(s)")
        except ray.exceptions.RayTaskError:
            logger.exception("Failed to clean up pending checkpoints")
            raise

    def delete_checkpoint(self) -> None:
        self.filesystem.delete_dir(self.checkpoint_path_unwrapped)

    def filter_rows_for_block(
        self,
        block: Block,
        checkpointed_ids: Block,
    ) -> Block:
        """For the given block, filter out rows that have already
        been checkpointed, and return the resulting block.

        Args:
            block: The input block to filter.
            checkpointed_ids: A block containing IDs of all rows that have
                been checkpointed.
        Returns:
            A new block with rows that have not been checkpointed.
        """

        if len(checkpointed_ids) == 0 or len(block) == 0:
            return block

        assert isinstance(block, pyarrow.Table)
        assert isinstance(checkpointed_ids, pyarrow.Table)

        # The checkpointed_ids block is sorted (see load_checkpoint).
        # We'll use binary search to filter out processed rows.
        # And we process a single chunk at a time, otherwise `to_numpy` below
        # will copy the data from shared memory to worker's heap memory.

        import concurrent.futures

        # Get all chunks of the checkpointed ID column.
        ckpt_chunks = checkpointed_ids[self.id_column].chunks
        # Convert the block's ID column to a numpy array for fast processing.
        block_ids = block[self.id_column].to_numpy()

        def filter_with_ckpt_chunk(ckpt_chunk: pyarrow.ChunkedArray) -> numpy.ndarray:
            # Convert checkpoint chunk to numpy for fast search.
            # Use internal helper function for consistency and robustness (handles null-typed arrays, etc.)
            ckpt_ids = transform_pyarrow.to_numpy(ckpt_chunk, zero_copy_only=False)
            # Start with a mask of all True (keep all rows).
            mask = numpy.ones(len(block_ids), dtype=bool)
            # Use binary search to find where block_ids would be in ckpt_ids.
            sorted_indices = numpy.searchsorted(ckpt_ids, block_ids)
            # Only consider indices that are within bounds.
            valid_indices = sorted_indices < len(ckpt_ids)
            # For valid indices, check for exact matches.
            potential_matches = sorted_indices[valid_indices]
            matched = ckpt_ids[potential_matches] == block_ids[valid_indices]
            # Mark matched IDs as False (filter out these rows).
            mask[valid_indices] = ~matched
            # Delete the chunk to free memory.
            del ckpt_chunk
            return mask

        # Use ThreadPoolExecutor to process each checkpoint chunk in parallel.
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.filter_num_threads or None
        ) as executor:
            masks = list(executor.map(filter_with_ckpt_chunk, ckpt_chunks))

        # Combine all masks using logical AND (row must not be in any checkpoint chunk).
        final_mask = numpy.logical_and.reduce(masks)
        # Convert the final mask to a PyArrow array and filter the block.
        mask_array = pyarrow.array(final_mask)
        filtered_block = block.filter(mask_array)
        return filtered_block

    def filter_rows_for_batch(
        self,
        batch: DataBatch,
        checkpointed_ids: Block,
    ) -> DataBatch:
        """For the given batch, filter out rows that have already
        been checkpointed, and return the resulting batch.

        Note that this method calls `filter_rows_for_block()` under the hood,
        so it is preferred to call that method directly if you already have a block.
        """
        arrow_block = BlockAccessor.batch_to_block(batch)
        filtered_block = self.filter_rows_for_block(arrow_block, checkpointed_ids)
        filtered_batch = BlockAccessor.for_block(filtered_block).to_batch_format(None)
        return filtered_batch
