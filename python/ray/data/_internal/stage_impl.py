import itertools
from typing import Any, Dict, Optional, TYPE_CHECKING

import ray
from ray.data._internal.fast_repartition import fast_repartition
from ray.data._internal.plan import AllToAllStage
from ray.data._internal.shuffle_and_partition import (
    PushBasedShufflePartitionOp,
    SimpleShufflePartitionOp,
)
from ray.data._internal.split import _calculate_blocks_rows, _split_at_indices
from ray.data._internal.block_list import BlockList
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.sort import sort_impl
from ray.data.context import DatasetContext
from ray.data.block import (
    _validate_key_fn,
    Block,
    KeyFn,
    BlockMetadata,
    BlockAccessor,
    BlockExecStats,
)

if TYPE_CHECKING:
    from ray.data import Dataset


class RepartitionStage(AllToAllStage):
    """Implementation of `Dataset.repartition()`."""

    def __init__(self, num_blocks: int, shuffle: bool):
        if shuffle:

            def do_shuffle(
                block_list,
                ctx: TaskContext,
                clear_input_blocks: bool,
                block_udf,
                remote_args,
            ):
                if clear_input_blocks:
                    blocks = block_list.copy()
                    block_list.clear()
                else:
                    blocks = block_list
                context = DatasetContext.get_current()
                if context.use_push_based_shuffle:
                    shuffle_op_cls = PushBasedShufflePartitionOp
                else:
                    shuffle_op_cls = SimpleShufflePartitionOp
                shuffle_op = shuffle_op_cls(block_udf, random_shuffle=False)
                return shuffle_op.execute(
                    blocks,
                    num_blocks,
                    clear_input_blocks,
                    map_ray_remote_args=remote_args,
                    reduce_ray_remote_args=remote_args,
                )

            super().__init__(
                "repartition", num_blocks, do_shuffle, supports_block_udf=True
            )

        else:

            def do_fast_repartition(block_list, clear_input_blocks: bool, *_):
                if clear_input_blocks:
                    blocks = block_list.copy()
                    block_list.clear()
                else:
                    blocks = block_list
                return fast_repartition(blocks, num_blocks)

            super().__init__("repartition", num_blocks, do_fast_repartition)


class RandomizeBlocksStage(AllToAllStage):
    """Implementation of `Dataset.randomize_blocks()`."""

    def __init__(self, seed: Optional[int]):
        self._seed = seed

        super().__init__("randomize_block_order", None, self.do_randomize)

    def do_randomize(self, block_list, *_):
        num_blocks = block_list.initial_num_blocks()
        if num_blocks == 0:
            return block_list, {}
        randomized_block_list = block_list.randomize_block_order(self._seed)
        return randomized_block_list, {}


class RandomShuffleStage(AllToAllStage):
    """Implementation of `Dataset.random_shuffle()`."""

    def __init__(
        self,
        seed: Optional[int],
        output_num_blocks: Optional[int],
        remote_args: Optional[Dict[str, Any]] = None,
    ):
        def do_shuffle(
            block_list,
            ctx: TaskContext,
            clear_input_blocks: bool,
            block_udf,
            remote_args,
        ):
            num_blocks = block_list.executed_num_blocks()  # Blocking.
            if num_blocks == 0:
                return block_list, {}
            if clear_input_blocks:
                blocks = block_list.copy()
                block_list.clear()
            else:
                blocks = block_list
            context = DatasetContext.get_current()
            if context.use_push_based_shuffle:
                if output_num_blocks is not None:
                    raise NotImplementedError(
                        "Push-based shuffle doesn't support setting num_blocks yet."
                    )
                shuffle_op_cls = PushBasedShufflePartitionOp
            else:
                shuffle_op_cls = SimpleShufflePartitionOp
            random_shuffle_op = shuffle_op_cls(
                block_udf, random_shuffle=True, random_seed=seed
            )
            return random_shuffle_op.execute(
                blocks,
                output_num_blocks or num_blocks,
                clear_input_blocks,
                map_ray_remote_args=remote_args,
                reduce_ray_remote_args=remote_args,
            )

        super().__init__(
            "random_shuffle",
            output_num_blocks,
            do_shuffle,
            supports_block_udf=True,
            remote_args=remote_args,
        )


class ZipStage(AllToAllStage):
    """Implementation of `Dataset.zip()`."""

    def __init__(self, other: "Dataset"):
        def do_zip_all(block_list: BlockList, clear_input_blocks: bool, *_):
            # Repartition other to align with this dataset, and then zip together the
            # blocks in parallel.
            # TODO(Clark): Port this to a streaming zip, e.g. push block pairs through
            # an actor that buffers and zips.
            blocks_with_metadata = block_list.get_blocks_with_metadata()
            # Get the split indices that will align other with self.
            block_rows = _calculate_blocks_rows(blocks_with_metadata)
            indices = list(itertools.accumulate(block_rows))
            indices.pop(-1)

            # Execute other to a block list.
            other_block_list = other._plan.execute()
            other_blocks_with_metadata = other_block_list.get_blocks_with_metadata()
            other_block_rows = _calculate_blocks_rows(other_blocks_with_metadata)
            # Check that each dataset has the same number of rows.
            # TODO(Clark): Support different number of rows via user-directed
            # dropping/padding.
            if sum(block_rows) != sum(other_block_rows):
                raise ValueError("Cannot zip datasets of different number of rows.")

            # Split other at the alignment indices, such that for every block in
            # block_list, we have a list of blocks from other that has the same
            # cumulative number of rows as that block.
            # NOTE: _split_at_indices has a no-op fastpath if the blocks are already
            # aligned.
            aligned_other_blocks_with_metadata = _split_at_indices(
                other_blocks_with_metadata,
                indices,
                other_block_list._owned_by_consumer,
                other_block_rows,
            )
            del other_blocks_with_metadata

            blocks = [b for b, _ in blocks_with_metadata]
            other_blocks = aligned_other_blocks_with_metadata[0]
            del blocks_with_metadata, aligned_other_blocks_with_metadata
            if clear_input_blocks:
                block_list.clear()
                other_block_list.clear()

            do_zip = cached_remote_fn(_do_zip, num_returns=2)

            out_blocks = []
            out_metadata = []
            for block, other_blocks in zip(blocks, other_blocks):
                # For each block in block_list, zip it together with 1 or more blocks
                # from other. We're guaranteed to have that block and other_blocks have
                # the same number of rows.
                res, meta = do_zip.remote(block, *other_blocks)
                out_blocks.append(res)
                out_metadata.append(meta)

            # Early release memory.
            del blocks, other_blocks

            # TODO(ekl) it might be nice to have a progress bar here.
            out_metadata = ray.get(out_metadata)
            blocks = BlockList(
                out_blocks,
                out_metadata,
                owned_by_consumer=block_list._owned_by_consumer,
            )
            return blocks, {}

        super().__init__("zip", None, do_zip_all)


def _do_zip(block: Block, *other_blocks: Block) -> (Block, BlockMetadata):
    # Zips together block with other_blocks.
    stats = BlockExecStats.builder()
    # Concatenate other blocks.
    # TODO(Clark): Extend BlockAccessor.zip() to work with N other blocks,
    # so we don't need to do this concatenation.
    builder = DelegatingBlockBuilder()
    for other_block in other_blocks:
        builder.add_block(other_block)
    other_block = builder.build()
    # Zip block and other blocks.
    result = BlockAccessor.for_block(block).zip(other_block)
    br = BlockAccessor.for_block(result)
    return result, br.get_metadata(input_files=[], exec_stats=stats.build())


class SortStage(AllToAllStage):
    """Implementation of `Dataset.sort()`."""

    def __init__(self, ds: "Dataset", key: Optional[KeyFn], descending: bool):
        def do_sort(block_list, clear_input_blocks: bool, *_):
            # Handle empty dataset.
            if block_list.initial_num_blocks() == 0:
                return block_list, {}
            if clear_input_blocks:
                blocks = block_list.copy()
                block_list.clear()
            else:
                blocks = block_list
            if isinstance(key, list):
                if not key:
                    raise ValueError("`key` must be a list of non-zero length")
                for subkey in key:
                    _validate_key_fn(ds, subkey)
            else:
                _validate_key_fn(ds, key)
            return sort_impl(blocks, clear_input_blocks, key, descending)

        super().__init__("sort", None, do_sort)
