"""The batch iterator of the push-based ``streaming_split``."""

from typing import Any, Iterator, cast

from ray.data._internal.block_batching.interfaces import ResolvedBlock
from ray.data.checkpoint.iterator import CheckpointingBatchIterator


class _MaterializedBatchIterator(CheckpointingBatchIterator):
    """BatchIterator over already-materialized blocks: skips the ref-level
    prefetch/resolve stages, inherits everything else unchanged.

    It extends ``CheckpointingBatchIterator`` so that a split with a
    checkpointer records the row IDs of each batch it yields, like the
    pull-based split. Without a checkpointer, it behaves like a plain
    ``BatchIterator``.
    """

    def __init__(self, blocks: Iterator[ResolvedBlock], **kwargs):
        # BatchIterator stores its input and hands it to _pipeline, which
        # this class overrides to consume blocks instead of RefBundles.
        super().__init__(cast(Any, blocks), **kwargs)

    def _pipeline(self, ref_bundles: Iterator[Any]):
        batch_iter = self._blocks_to_batches(ref_bundles)
        batch_iter = self._format_batches(batch_iter)
        if self._preserve_order:
            batch_iter = self._restore_original_batch_order(batch_iter)
        batch_iter = self._finalize_batches(batch_iter)
        yield from batch_iter
