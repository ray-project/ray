from typing import Optional
from functools import partial

from ray.data._internal.block_list import BlockList
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.all_to_all_operator import AbstractAllToAll


class RandomizeBlocks(AbstractAllToAll):
    """Logical operator for randomize_block_order."""

    def __init__(
        self,
        input_op: LogicalOperator,
        seed: Optional[int] = None,
    ):
        fn = partial(randomize, seed)
        super().__init__(
            "RandomizeBlocks",
            input_op,
            fn,
        )
        self._seed = seed


def randomize(seed: Optional[int], block_list: BlockList, *_):
    """Randomize order of the given block list with seed."""
    num_blocks = block_list.initial_num_blocks()
    if num_blocks == 0:
        return block_list, {}
    randomized_block_list = block_list.randomize_block_order(seed)
    return randomized_block_list, {}
