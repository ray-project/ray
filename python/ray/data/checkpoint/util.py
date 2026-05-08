import logging
import posixpath
from typing import Iterable, List

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.checkpoint.interfaces import (
    CheckpointConfig,
)

logger = logging.getLogger(__name__)


# Checkpoint keyword argument name
CHECKPOINTED_IDS_KWARG_NAME = "checkpointed_ids"


class PrefixTrie:
    """Trie for efficient prefix matching of filenames during recovery."""

    def __init__(self) -> None:
        self.children: dict[str, "PrefixTrie"] = {}
        self.is_end: bool = False

    def insert(self, word: str) -> None:
        node = self
        for ch in word:
            if ch not in node.children:
                node.children[ch] = PrefixTrie()
            node = node.children[ch]
        node.is_end = True

    def has_prefix_of(self, word: str) -> bool:
        """Return True if any inserted word is a prefix of `word`."""
        node = self
        for ch in word:
            if node.is_end:
                return True
            if ch not in node.children:
                return False
            node = node.children[ch]
        return node.is_end


def build_pending_checkpoint_trie(file_paths: List, pending_suffix: str) -> PrefixTrie:
    """Build a PrefixTrie from pending checkpoint file paths.

    Strips the given pending suffix to get the data file prefix.
    """
    trie = PrefixTrie()
    for f in file_paths:
        basename = posixpath.basename(f.path)
        if basename.endswith(pending_suffix):
            prefix = basename[: -len(pending_suffix)]
            trie.insert(prefix)
    return trie


def filter_checkpointed_rows_for_blocks(
    blocks: Iterable[Block],
    task_context: TaskContext,
    checkpoint_config: CheckpointConfig,
) -> Iterable[Block]:
    """For each block, filter rows that have already been checkpointed
    and yield the resulting block."""
    from ray.data.checkpoint.checkpoint_filter import (
        BatchBasedCheckpointFilter,
    )

    # Use .get() so that tasks where load_checkpoint was None (no prior checkpoint
    # exists) and the kwarg was never registered do not raise KeyError.
    checkpointed_ids = task_context.kwargs.get(CHECKPOINTED_IDS_KWARG_NAME)

    # Build the filter once per task invocation, not once per block.
    ckpt_filter = BatchBasedCheckpointFilter(checkpoint_config)

    for block in blocks:
        filtered_block = ckpt_filter.filter_rows_for_block(
            block=block,
            checkpointed_ids=checkpointed_ids,
        )
        ba = BlockAccessor.for_block(filtered_block)
        if ba.num_rows() > 0:
            yield filtered_block


def filter_checkpointed_rows_for_batches(
    batches: Iterable[DataBatch],
    task_context: TaskContext,
    checkpoint_config: CheckpointConfig,
) -> Iterable[DataBatch]:
    """For each batch, filter rows that have already been checkpointed
    and yield the resulting batches."""
    from ray.data.checkpoint.checkpoint_filter import (
        BatchBasedCheckpointFilter,
    )

    # Use .get() so that tasks where load_checkpoint was None do not raise KeyError.
    checkpointed_ids = task_context.kwargs.get(CHECKPOINTED_IDS_KWARG_NAME)

    # Build the filter once per task invocation, not once per batch.
    ckpt_filter = BatchBasedCheckpointFilter(checkpoint_config)

    for batch in batches:
        filtered_batch = ckpt_filter.filter_rows_for_batch(
            batch=batch,
            checkpointed_ids=checkpointed_ids,
        )
        yield filtered_batch
