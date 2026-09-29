import dataclasses
from contextlib import contextmanager
from typing import Iterator, Optional

from ray.data._internal.block_batching.interfaces import Batch
from ray.data._internal.block_batching.iter_batches import BatchIterator
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import Block, BlockAccessor
from ray.data.checkpoint.data_iterator_checkpointer import (
    BatchMetadataWithRowIDs,
    RowIDBasedDataIteratorCheckpointer,
)


class CheckpointingBatchIterator(BatchIterator):
    def __init__(
        self,
        ref_bundles_iter: Iterator[RefBundle],
        *,
        checkpointer: Optional[RowIDBasedDataIteratorCheckpointer] = None,
        **kwargs,
    ):
        super().__init__(ref_bundles_iter, **kwargs)
        self._checkpointer = checkpointer

    def _update_batch_with_checkpoint_metadata(self, batch: Batch) -> Batch:
        """Update the batch with checkpoint metadata.

        Args:
            batch: The batch to update with checkpoint metadata.

        Returns:
            An updated batch with the row IDs as metadata.
        """
        assert self._checkpointer

        block_accessor = BlockAccessor.for_block(batch.data)
        assert isinstance(block_accessor, TableBlockAccessor)
        row_ids = block_accessor.select(columns=[self._checkpointer._id_column])

        # Carry over the existing metadata fields (e.g., stage timings).
        metadata_fields = {
            f.name: getattr(batch.metadata, f.name)
            for f in dataclasses.fields(batch.metadata)
        }
        batch = dataclasses.replace(
            batch,
            metadata=BatchMetadataWithRowIDs(**metadata_fields, row_ids=row_ids),
        )
        return batch

    def _blocks_to_batches(self, blocks: Iterator[Block]) -> Iterator[Batch]:
        for batch in super()._blocks_to_batches(blocks):
            if self._checkpointer:
                yield self._update_batch_with_checkpoint_metadata(batch)
            else:
                yield batch

    def before_epoch_start(self):
        super().before_epoch_start()

        if self._checkpointer:
            self._checkpointer.start_epoch()

    def after_epoch_end(self):
        super().after_epoch_end()

        if self._checkpointer:
            self._checkpointer.end_epoch()

    @contextmanager
    def yield_batch_context(self, batch: Batch):
        if self._checkpointer:
            self._checkpointer.record_yielded_batch(batch)

        with super().yield_batch_context(batch):
            yield
