from typing import Any, Dict, List, Optional, Tuple, Union

from ray.data._internal.block_list import BlockList
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import Block, BlockMetadata


class ShuffleOp:
    """
    A generic shuffle operator. Callers should first implement the `map` and
    `reduce` static methods then choose a plan to execute the shuffle by
    inheriting from the appropriate class. A SimpleShufflePlan is provided
    below. Any custom arguments for map and reduce tasks should be specified by
    setting `ShuffleOp._map_args` and `ShuffleOp._reduce_args`.
    """

    def __init__(self, map_args: List[Any] = None, reduce_args: List[Any] = None):
        self._map_args = map_args or []
        self._reduce_args = reduce_args or []
        assert isinstance(self._map_args, list)
        assert isinstance(self._reduce_args, list)

    @staticmethod
    def map(
        idx: int, block: Block, output_num_blocks: int, *map_args: List[Any]
    ) -> List[Union[BlockMetadata, Block]]:
        """
        Map function to be run on each input block.

        Returns list of [BlockMetadata, O1, O2, O3, ...output_num_blocks].
        """
        raise NotImplementedError

    @staticmethod
    def reduce(
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> (Block, BlockMetadata):
        """
        Reduce function to be run for each output block.

        Args:
            mapper_outputs: List of blocks to reduce.
            partial_reduce: A flag passed by the shuffle operator that
                indicates whether we should partially or fully reduce the
                mapper outputs.

        Returns:
            The reduced block and its metadata.
        """
        raise NotImplementedError


class SimpleShufflePlan(ShuffleOp):
    def execute(
        self,
        input_blocks: BlockList,
        output_num_blocks: int,
        clear_input_blocks: bool,
        *,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        shuffle_col=None,
    ) -> Tuple[BlockList, Dict[str, List[BlockMetadata]]]:
        input_blocks_list = input_blocks.get_blocks()
        input_num_blocks = len(input_blocks_list)

        if map_ray_remote_args is None:
            map_ray_remote_args = {}
        if reduce_ray_remote_args is None:
            reduce_ray_remote_args = {}
        if "scheduling_strategy" not in reduce_ray_remote_args:
            reduce_ray_remote_args = reduce_ray_remote_args.copy()
            reduce_ray_remote_args["scheduling_strategy"] = "SPREAD"

        shuffle_map = cached_remote_fn(self.map)
        shuffle_reduce = cached_remote_fn(self.reduce)

        map_bar = ProgressBar("Shuffle Map", total=input_num_blocks)

        shuffle_map_out = [
            shuffle_map.options(
                **map_ray_remote_args,
                num_returns=1 + output_num_blocks,
            ).remote(i, block, output_num_blocks, *self._map_args, shuffle_col)
            for i, block in enumerate(input_blocks_list)
        ]

        # The first item returned is the BlockMetadata.
        shuffle_map_metadata = []
        for i, refs in enumerate(shuffle_map_out):
            shuffle_map_metadata.append(refs[-1])
            shuffle_map_out[i] = refs[:-1]

        in_blocks_owned_by_consumer = input_blocks._owned_by_consumer

        # Eagerly delete the input block references in order to eagerly release
        # the blocks' memory.
        del input_blocks_list
        if clear_input_blocks:
            input_blocks.clear()
        shuffle_map_metadata = map_bar.fetch_until_complete(shuffle_map_metadata)
        map_bar.close()

        reduce_bar = ProgressBar("Shuffle Reduce", total=output_num_blocks)
        shuffle_reduce_out = [
            shuffle_reduce.options(**reduce_ray_remote_args, num_returns=2,).remote(
                *self._reduce_args,
                *[shuffle_map_out[i][j] for i in range(input_num_blocks)],
            )
            for j in range(output_num_blocks)
        ]
        # Eagerly delete the map block references in order to eagerly release
        # the blocks' memory.
        del shuffle_map_out
        new_blocks, new_metadata = zip(*shuffle_reduce_out)
        new_metadata = reduce_bar.fetch_until_complete(list(new_metadata))
        reduce_bar.close()

        stats = {
            "map": shuffle_map_metadata,
            "reduce": new_metadata,
        }

        return (
            BlockList(
                list(new_blocks),
                list(new_metadata),
                owned_by_consumer=in_blocks_owned_by_consumer,
            ),
            stats,
        )
