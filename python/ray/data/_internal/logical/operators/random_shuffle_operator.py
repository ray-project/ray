from typing import Any, Dict, Optional
from functools import partial

from ray.data._internal.block_list import BlockList
from ray.data._internal.compute import BlockTransform
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.all_to_all_operator import AbstractAllToAll
from ray.data._internal.shuffle_and_partition import (
    PushBasedShufflePartitionOp,
    SimpleShufflePartitionOp,
)
from ray.data.context import DatasetContext


class RandomShuffle(AbstractAllToAll):
    """Logical operator for random_shuffle."""

    def __init__(
        self,
        input_op: LogicalOperator,
        seed: Optional[int] = None,
        output_num_blocks: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        fn = partial(shuffle, seed, output_num_blocks)
        super().__init__(
            "RandomShuffle",
            input_op,
            fn,
            num_blocks=output_num_blocks,
            supports_block_udf=True,
            ray_remote_args=ray_remote_args,
        )
        self._seed = seed


def shuffle(
    seed: Optional[int],
    output_num_blocks: Optional[int],
    block_list: BlockList,
    clear_input_blocks: bool,
    block_udf: BlockTransform,
    remote_args: Dict[str, Any],
):
    """Shuffle the given block list randomly with seed."""
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
    random_shuffle_op = shuffle_op_cls(block_udf, random_shuffle=True, random_seed=seed)
    return random_shuffle_op.execute(
        blocks,
        output_num_blocks or num_blocks,
        clear_input_blocks,
        map_ray_remote_args=remote_args,
        reduce_ray_remote_args=remote_args,
    )
