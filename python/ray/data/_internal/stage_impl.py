from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from ray.data._internal.fast_repartition import fast_repartition
from ray.data._internal.plan import AllToAllStage
from ray.data._internal.shuffle_and_partition import (
    PushBasedShufflePartitionOp,
    SimpleShufflePartitionOp,
)


class RepartitionStage(AllToAllStage):
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
    def __init__(self, seed: Optional[int]):
        self._seed = seed

        super().__init__("randomize_block_order", None, self.do_randomize)

    def do_randomize(self, block_list, *_):
        num_blocks = block_list.initial_num_blocks()
        if num_blocks == 0:
            return block_list, {}
        randomized_block_list = block_list.randomize_block_order(self._seed)
        return randomized_block_list, {}
