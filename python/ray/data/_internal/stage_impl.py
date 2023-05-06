import itertools
from typing import Any, Dict, Tuple, List, Optional, TYPE_CHECKING

import ray
from ray.data._internal.fast_repartition import fast_repartition
from ray.data._internal.plan import AllToAllStage
from ray.data._internal.shuffle_and_partition import (
    PushBasedShufflePartitionOp,
    SimpleShufflePartitionOp,
)
from ray.data._internal.split import (
    _split_at_index,
    _split_at_indices,
)
from ray.data._internal.block_list import BlockList
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.sort import sort_impl
from ray.data.context import DataContext
from ray.data.block import (
    _validate_key_fn,
    Block,
    BlockPartition,
    BlockMetadata,
    BlockAccessor,
    BlockExecStats,
)

if TYPE_CHECKING:
    from ray.data import Datastream


class RepartitionStage(AllToAllStage):
    """Implementation of `Datastream.repartition()`."""

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
                context = DataContext.get_current()
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
                    ctx=ctx,
                )

            super().__init__(
                "Repartition",
                num_blocks,
                do_shuffle,
                supports_block_udf=True,
                sub_stage_names=["ShuffleMap", "ShuffleReduce"],
            )

        else:

            def do_fast_repartition(
                block_list,
                ctx: TaskContext,
                clear_input_blocks: bool,
                *_,
            ):
                if clear_input_blocks:
                    blocks = block_list.copy()
                    block_list.clear()
                else:
                    blocks = block_list
                return fast_repartition(blocks, num_blocks, ctx)

            super().__init__(
                "Repartition",
                num_blocks,
                do_fast_repartition,
                sub_stage_names=["Repartition"],
            )


class RandomizeBlocksStage(AllToAllStage):
    """Implementation of `Datastream.randomize_blocks()`."""

    def __init__(self, seed: Optional[int]):
        self._seed = seed

        super().__init__("RandomizeBlockOrder", None, self.do_randomize)

    def do_randomize(self, block_list, *_):
        num_blocks = block_list.initial_num_blocks()
        if num_blocks == 0:
            return block_list, {}
        randomized_block_list = block_list.randomize_block_order(self._seed)
        return randomized_block_list, {}


class RandomShuffleStage(AllToAllStage):
    """Implementation of `Datastream.random_shuffle()`."""

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
            context = DataContext.get_current()
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
                ctx=ctx,
            )

        super().__init__(
            "RandomShuffle",
            output_num_blocks,
            do_shuffle,
            supports_block_udf=True,
            remote_args=remote_args,
            sub_stage_names=["ShuffleMap", "ShuffleReduce"],
        )


class ZipStage(AllToAllStage):
    """Implementation of `Datastream.zip()`."""

    def __init__(self, other: "Datastream"):
        def do_zip_all(block_list: BlockList, clear_input_blocks: bool, *_):
            # Repartition other to align with the base datastream, and then zip together
            # the blocks in parallel.
            # TODO(Clark): Port this to a streaming zip, e.g. push block pairs through
            # an actor that buffers and zips.
            base_block_list = block_list
            base_blocks_with_metadata = block_list.get_blocks_with_metadata()
            base_block_rows, base_block_bytes = _calculate_blocks_rows_and_bytes(
                base_blocks_with_metadata
            )
            # Execute other to a block list.
            # NOTE: Require to preserve order when executing the other side,
            # because streaming execution does not preserve order by default.
            other_block_list = other._plan.execute(preserve_order=True)
            other_blocks_with_metadata = other_block_list.get_blocks_with_metadata()
            other_block_rows, other_block_bytes = _calculate_blocks_rows_and_bytes(
                other_blocks_with_metadata
            )
            inverted = False
            if sum(other_block_bytes) > sum(base_block_bytes):
                # Make sure that other is the smaller datastream, so we minimize
                # splitting work when aligning other with base.
                # TODO(Clark): Improve this heuristic for minimizing splitting work,
                # e.g. by generating the splitting plans for each route (via
                # _generate_per_block_split_indices) and choosing the plan that splits
                # the least cumulative bytes.
                base_block_list, other_block_list = other_block_list, base_block_list
                base_blocks_with_metadata, other_blocks_with_metadata = (
                    other_blocks_with_metadata,
                    base_blocks_with_metadata,
                )
                base_block_rows, other_block_rows = other_block_rows, base_block_rows
                inverted = True
            # Get the split indices that will align other with base.
            indices = list(itertools.accumulate(base_block_rows))
            indices.pop(-1)

            # Check that each datastream has the same number of rows.
            # TODO(Clark): Support different number of rows via user-directed
            # dropping/padding.
            total_base_rows = sum(base_block_rows)
            total_other_rows = sum(other_block_rows)
            if total_base_rows != total_other_rows:
                raise ValueError(
                    "Cannot zip datastreams of different number of rows: "
                    f"{total_base_rows}, {total_other_rows}"
                )

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

            base_blocks = [b for b, _ in base_blocks_with_metadata]
            other_blocks = aligned_other_blocks_with_metadata[0]
            del base_blocks_with_metadata, aligned_other_blocks_with_metadata
            if clear_input_blocks:
                base_block_list.clear()
                other_block_list.clear()

            do_zip = cached_remote_fn(_do_zip, num_returns=2)

            out_blocks = []
            out_metadata = []
            for base_block, other_blocks in zip(base_blocks, other_blocks):
                # For each block in base, zip it together with 1 or more blocks from
                # other. We're guaranteed to have that base_block has the same number of
                # rows as other_blocks has cumulatively.
                res, meta = do_zip.remote(base_block, *other_blocks, inverted=inverted)
                out_blocks.append(res)
                out_metadata.append(meta)

            # Early release memory.
            del base_blocks, other_blocks

            # TODO(ekl) it might be nice to have a progress bar here.
            out_metadata = ray.get(out_metadata)
            blocks = BlockList(
                out_blocks,
                out_metadata,
                owned_by_consumer=base_block_list._owned_by_consumer,
            )
            return blocks, {}

        super().__init__("Zip", None, do_zip_all)


def _calculate_blocks_rows_and_bytes(
    blocks_with_metadata: BlockPartition,
) -> Tuple[List[int], List[int]]:
    """Calculate the number of rows and size in bytes for a list of blocks with
    metadata.
    """
    get_num_rows_and_bytes = cached_remote_fn(_get_num_rows_and_bytes)
    block_rows = []
    block_bytes = []
    for block, metadata in blocks_with_metadata:
        if metadata.num_rows is None or metadata.size_bytes is None:
            # Need to fetch number of rows or size in bytes, so just fetch both.
            num_rows, size_bytes = ray.get(get_num_rows_and_bytes.remote(block))
            # Cache on the block metadata.
            metadata.num_rows = num_rows
            metadata.size_bytes = size_bytes
        block_rows.append(metadata.num_rows)
        block_bytes.append(metadata.size_bytes)
    return block_rows, block_bytes


def _get_num_rows_and_bytes(block: Block) -> Tuple[int, int]:
    block = BlockAccessor.for_block(block)
    return block.num_rows(), block.size_bytes()


def _do_zip(
    block: Block, *other_blocks: Block, inverted: bool = False
) -> Tuple[Block, BlockMetadata]:
    # Zips together block with other_blocks.
    stats = BlockExecStats.builder()
    # Concatenate other blocks.
    # TODO(Clark): Extend BlockAccessor.zip() to work with N other blocks,
    # so we don't need to do this concatenation.
    builder = DelegatingBlockBuilder()
    for other_block in other_blocks:
        builder.add_block(other_block)
    other_block = builder.build()
    if inverted:
        # Swap blocks if ordering was inverted during block alignment splitting.
        block, other_block = other_block, block
    # Zip block and other blocks.
    result = BlockAccessor.for_block(block).zip(other_block)
    br = BlockAccessor.for_block(result)
    return result, br.get_metadata(input_files=[], exec_stats=stats.build())


class SortStage(AllToAllStage):
    """Implementation of `Datastream.sort()`."""

    def __init__(self, ds: "Datastream", key: Optional[str], descending: bool):
        def do_sort(
            block_list,
            ctx: TaskContext,
            clear_input_blocks: bool,
            *_,
        ):
            # Handle empty datastream.
            if block_list.initial_num_blocks() == 0:
                return block_list, {}
            if clear_input_blocks:
                blocks = block_list.copy()
                block_list.clear()
            else:
                blocks = block_list
            schema = ds.schema(fetch_if_missing=True)
            if isinstance(key, list):
                if not key:
                    raise ValueError("`key` must be a list of non-zero length")
                for subkey in key:
                    _validate_key_fn(schema, subkey)
            else:
                _validate_key_fn(schema, key)
            return sort_impl(blocks, clear_input_blocks, key, descending, ctx)

        super().__init__(
            "Sort",
            None,
            do_sort,
            sub_stage_names=["SortSample", "ShuffleMap", "ShuffleReduce"],
        )


class LimitStage(AllToAllStage):
    """Implementation of `Datastream.limit()`."""

    def __init__(self, limit: int):
        self._limit = limit
        super().__init__(
            "Limit",
            None,
            self._do_limit,
        )

    @property
    def limit(self) -> int:
        return self._limit

    def _do_limit(
        self,
        input_block_list: BlockList,
        clear_input_blocks: bool,
        *_,
    ):
        if clear_input_blocks:
            block_list = input_block_list.copy()
            input_block_list.clear()
        else:
            block_list = input_block_list
        block_list = block_list.truncate_by_rows(self._limit)
        blocks, metadata, _, _ = _split_at_index(block_list, self._limit)
        return (
            BlockList(
                blocks,
                metadata,
                owned_by_consumer=block_list._owned_by_consumer,
            ),
            {},
        )
