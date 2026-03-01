import abc
import itertools
import logging
import time
from typing import Iterable, List, Optional

import pyarrow
import pyarrow.compute as pc
import pyarrow.dataset as ds

import ray
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data.block import Block, BlockAccessor, BlockMetadata, DataBatch, Schema
from ray.data.checkpoint import CheckpointConfig
from ray.data.datasource import PathPartitionFilter
from ray.data.datasource.path_util import _unwrap_protocol
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


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
        """Pre-process the checkpoint dataset.

        No sorting is needed since filtering uses hash-based anti-join
        (pc.is_in) which does not require sorted data.

        Args:
            checkpoint_ds: The checkpoint dataset to pre-process

        Returns:
            The pre-processed checkpoint dataset
        """
        return checkpoint_ds


class BatchBasedCheckpointFilter(CheckpointFilter):
    """CheckpointFilter for batch-based backends."""

    def load_checkpoint(self) -> ObjectRef[Block]:
        """Load checkpointed ids as a sorted block.

        Returns:
            ObjectRef[Block]: ObjectRef to the checkpointed IDs block.
        """
        loader = IdColumnCheckpointLoader(
            checkpoint_path=self.checkpoint_path,
            filesystem=self.filesystem,
            id_column=self.id_column,
            checkpoint_path_partition_filter=self.ckpt_config.checkpoint_path_partition_filter,
        )
        return loader.load_checkpoint()

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

        # Hash-based anti-join: pc.is_in builds a hash set from the checkpoint
        # IDs for O(1) average-case membership checks. It handles ChunkedArrays
        # natively and runs entirely in C++.
        membership = pc.is_in(
            block[self.id_column], value_set=checkpointed_ids[self.id_column]
        )
        # Keep only rows NOT in the checkpoint (anti-join).
        return block.filter(pc.invert(membership))

    def filter_rows_for_blocks(
        self,
        blocks: Iterable[Block],
        checkpointed_ids: Block,
    ) -> Iterable[Block]:
        """Filter out checkpointed rows from multiple blocks.

        Feeds all blocks through a single Arrow Dataset Scanner so that the
        MemoTable (hash table) is built once at bind time and reused across
        all batches — avoiding redundant hash table rebuilds per block.

        Args:
            blocks: An iterable of input blocks to filter.
            checkpointed_ids: A block containing IDs of all rows that have
                been checkpointed.
        Yields:
            Filtered blocks with only non-checkpointed rows.
        """
        if len(checkpointed_ids) == 0:
            yield from blocks
            return

        # Build anti-join expression. When bound by the Scanner, the MemoTable
        # (hash table) is built once and reused for all batches.
        anti_join_expr = ~pc.field(self.id_column).isin(
            checkpointed_ids[self.id_column]
        )

        # Peek at the first non-empty block to get the schema.
        schema = None
        buffered_blocks = []
        for block in blocks:
            if isinstance(block, pyarrow.Table) and block.num_rows > 0:
                schema = block.schema
                buffered_blocks.append(block)
                break

        if schema is None:
            return

        # Chain buffered block with remaining blocks, convert to record batches,
        # and feed through a SINGLE scanner so the MemoTable is built once.
        all_blocks = itertools.chain(buffered_blocks, blocks)
        batch_iter = self._blocks_to_batches(all_blocks)
        reader = pyarrow.RecordBatchReader.from_batches(schema, batch_iter)
        scanner = ds.Scanner.from_batches(reader, filter=anti_join_expr)

        for batch in scanner.to_batches():
            filtered_block = pyarrow.Table.from_batches([batch], schema=schema)
            if filtered_block.num_rows > 0:
                yield filtered_block

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

    @staticmethod
    def _blocks_to_batches(
        blocks: Iterable[Block],
    ) -> Iterable[pyarrow.RecordBatch]:
        """Convert an iterable of blocks (pyarrow.Table) to record batches."""
        for block in blocks:
            if isinstance(block, pyarrow.Table) and block.num_rows > 0:
                yield from block.to_batches()
