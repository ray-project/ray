from typing import Optional, TYPE_CHECKING

import ray
from ray.data._internal.fast_repartition import fast_repartition
from ray.data._internal.plan import AllToAllStage
from ray.data._internal.shuffle_and_partition import (
    PushBasedShufflePartitionOp,
    SimpleShufflePartitionOp,
)
from ray.data._internal.block_list import BlockList
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
                block_list, clear_input_blocks: bool, block_udf, remote_args
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

    def __init__(self, seed: Optional[int], output_num_blocks: Optional[int]):
        def do_shuffle(block_list, clear_input_blocks: bool, block_udf, remote_args):
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
            "random_shuffle", output_num_blocks, do_shuffle, supports_block_udf=True
        )


class ZipStage(AllToAllStage):
    """Implementation of `Dataset.zip()`."""

    def __init__(self, other: "Dataset"):
        def do_zip_all(block_list, clear_input_blocks: bool, *_):
            blocks1 = block_list.get_blocks()
            blocks2 = other.get_internal_block_refs()

            if clear_input_blocks:
                block_list.clear()

            if len(blocks1) != len(blocks2):
                # TODO(ekl) consider supporting if num_rows are equal.
                raise ValueError(
                    "Cannot zip dataset of different num blocks: {} vs {}".format(
                        len(blocks1), len(blocks2)
                    )
                )

            def do_zip(block1: Block, block2: Block) -> (Block, BlockMetadata):
                stats = BlockExecStats.builder()
                b1 = BlockAccessor.for_block(block1)
                result = b1.zip(block2)
                br = BlockAccessor.for_block(result)
                return result, br.get_metadata(input_files=[], exec_stats=stats.build())

            do_zip_fn = cached_remote_fn(do_zip, num_returns=2)

            blocks = []
            metadata = []
            for b1, b2 in zip(blocks1, blocks2):
                res, meta = do_zip_fn.remote(b1, b2)
                blocks.append(res)
                metadata.append(meta)

            # Early release memory.
            del blocks1, blocks2

            # TODO(ekl) it might be nice to have a progress bar here.
            metadata = ray.get(metadata)
            blocks = BlockList(
                blocks, metadata, owned_by_consumer=block_list._owned_by_consumer
            )
            return blocks, {}

        super().__init__("zip", None, do_zip_all)


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
