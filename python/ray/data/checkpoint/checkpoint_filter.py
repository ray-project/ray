import abc
import logging
import sys
import time
from typing import List, Optional, Tuple

import numpy
import pyarrow

import ray
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data.block import Block, BlockMetadata, Schema
from ray.data.checkpoint import CheckpointConfig
from ray.data.datasource import PathPartitionFilter
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


@ray.remote
class CheckpointHolder:
    """Holds checkpointed IDs in numpy ndarray format"""

    def __init__(self):
        self.checkpointed_ids_ndarray: numpy.ndarray = numpy.array([])
        self.checkpointed_ids_size: Optional[int] = None

    def get_checkpointed_ids_ndarray(
        self, checkpointed_ids_arrow: Block, id_column: str
    ) -> numpy.ndarray:
        """Convert checkpointed IDs from pyarrow.Table to numpy.ndarray."""
        if checkpointed_ids_arrow.num_rows != 0:
            self.checkpointed_ids_ndarray = checkpointed_ids_arrow[id_column].to_numpy(
                zero_copy_only=False
            )
        return self.checkpointed_ids_ndarray

    def numpy_size(self, array: numpy.ndarray) -> int:
        """Calculate the size of a numpy ndarray."""
        total_size = array.nbytes
        if array.dtype == object:
            for item in array.flat:
                total_size += sys.getsizeof(item)
        return total_size

    def get_checkpointed_ids_size(self) -> int:
        if not self.checkpointed_ids_size:
            self.checkpointed_ids_size = self.numpy_size(self.checkpointed_ids_ndarray)
        return self.checkpointed_ids_size


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

    def load_checkpoint(self) -> Tuple[Optional[ObjectRef[numpy.ndarray]], int]:
        """Loading checkpoint data.

        Returns:
            ObjectRef: The ref of checkpointed IDs array. None if no checkpoint was loaded.
            int: the size of the checkpointed IDs array.
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
        # Use an actor to hold the checkpointed IDs, because we do not want the IDs
        # to occupy the memory of the head node.
        checkpoint_holder = CheckpointHolder.remote()
        checkpointed_ids_ref = checkpoint_holder.get_checkpointed_ids_ndarray.remote(
            block_ref, self.id_column
        )
        ray.wait([checkpointed_ids_ref])
        checkpoint_size = ray.get(checkpoint_holder.get_checkpointed_ids_size.remote())
        logger.info(
            "Checkpoint loaded for %s in %.2f seconds. SizeBytes = %d, Schema = %s",
            type(self).__name__,
            time.time() - start_t,
            checkpoint_size,
            schema.to_string(),
        )
        return checkpointed_ids_ref, checkpoint_size

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


class CheckpointFilter(abc.ABC):
    """Abstract class which defines the interface for filtering checkpointed rows
    based on varying backends.
    """

    def __init__(self, config: CheckpointConfig):
        self.ckpt_config = config
        self.id_column = self.ckpt_config.id_column
        self.checkpointed_ids = None


class BatchBasedCheckpointFilter(CheckpointFilter):
    """CheckpointFilter for batch-based backends.

    This filter will first retrieve the checkpointed IDs from the object store.
    For each input block, it filters the block and returns the filtered block.
    """

    def __init__(
        self,
        checkpoint_config: CheckpointConfig,
        checkpoint_ref: ObjectRef[numpy.ndarray],
    ):
        super().__init__(checkpoint_config)
        self.checkpointed_ids = ray.get(checkpoint_ref)
        assert isinstance(self.checkpointed_ids, numpy.ndarray)

    def filter_rows_for_block(
        self,
        block: Block,
    ) -> Block:
        """For the given block, filter out rows that have already
        been checkpointed, and return the resulting block.

        Args:
            block: The input block to filter.
        Returns:
            A new block with rows that have not been checkpointed.
        """

        if self.checkpointed_ids.shape[0] == 0 or len(block) == 0:
            return block

        assert isinstance(block, pyarrow.Table)

        # The checkpointed_ids block is sorted (see load_checkpoint).
        # We'll use binary search to filter out processed rows.

        # Convert the block's ID column to a numpy array for fast processing.
        block_ids = block[self.id_column].to_numpy()

        # Start with a mask of all True (keep all rows).
        mask = numpy.ones(len(block_ids), dtype=bool)
        # Use binary search to find where block_ids would be in ckpt_ids.
        sorted_indices = numpy.searchsorted(self.checkpointed_ids, block_ids)
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
