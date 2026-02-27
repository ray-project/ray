"""Shuffle spec that runs a fused MapTransformer (upstream map + shuffle) in map tasks.

Used when MapOperator is fused with AllToAllOperator via MapTransformer.fuse().
The fused transformer = upstream_map.fuse(shuffle_map_transformer).
"""

from typing import TYPE_CHECKING, List, Optional, Union

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadataWithSchema

if TYPE_CHECKING:
    pass


def _map_with_fused_transformer(
    idx: int,
    block: Block,
    output_num_blocks: int,
    map_transformer: MapTransformer,
    target_shuffle_max_block_size: int,
    random_shuffle: bool,
    random_seed: Optional[int],
) -> List[Union[Block, BlockMetadataWithSchema]]:
    """Run fused MapTransformer in a shuffle map task. Used by MapTransformerShuffleTaskSpec.

    When MapOp+AllToAll fuses (e.g. ReadRange->MapBatches->Shuffle), input is ReadTasks.
    The fused transformer's first step (do_read) expects ReadTasks and resolves them.
    Pass block as-is - do not resolve ReadTasks here.
    """
    import logging

    from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR
    from ray.data.datasource.datasource import ReadTask

    logger = logging.getLogger(__name__)

    # Block size warning (matches ShuffleTaskSpec). Skip for ReadTask - the fused
    # transformer's do_read will produce blocks; we can't inspect size yet.
    if not isinstance(block, ReadTask):
        accessor = BlockAccessor.for_block(block)
        if (
            accessor.size_bytes()
            > MAX_SAFE_BLOCK_SIZE_FACTOR * target_shuffle_max_block_size
        ):
            logger.warning(
                "Input block to map task has size "
                f"{accessor.size_bytes() // (1024 * 1024)}MiB, which exceeds "
                "DataContext.get_current().target_shuffle_max_block_size="
                f"{target_shuffle_max_block_size // (1024 * 1024)}MiB. "
                "This can lead to out-of-memory errors and can happen "
                "when map tasks are fused to the shuffle operation. "
                "To prevent fusion, call Dataset.materialize() on the "
                "dataset before shuffling."
            )

    ctx = TaskContext(
        task_idx=idx,
        op_name="shuffle_map",
        kwargs={
            "output_num_blocks": output_num_blocks,
            "random_shuffle": random_shuffle,
            "random_seed": random_seed,
            "target_shuffle_max_block_size": target_shuffle_max_block_size,
        },
    )
    output_blocks = list(map_transformer.apply_transform([block], ctx))

    # Build metadata from first block for scheduler (used for stats/schema)
    stats = BlockExecStats.builder()
    if output_blocks:
        first_accessor = BlockAccessor.for_block(output_blocks[0])
        meta = first_accessor.get_metadata(exec_stats=stats.build())
        meta_with_schema = BlockMetadataWithSchema(
            metadata=meta, schema=first_accessor.schema()
        )
    else:
        # Empty output (e.g. empty ReadTask). Use block for metadata if it's a Block.
        if isinstance(block, ReadTask):
            meta = block.metadata
            schema = block.schema
        else:
            accessor = BlockAccessor.for_block(block)
            meta = accessor.get_metadata(exec_stats=stats.build())
            schema = accessor.schema()
        meta_with_schema = BlockMetadataWithSchema(metadata=meta, schema=schema)

    return output_blocks + [meta_with_schema]


class MapTransformerShuffleTaskSpec(ExchangeTaskSpec):
    """ShuffleTaskSpec that runs a fused MapTransformer in map tasks.

    The map_transformer = upstream_map.fuse(shuffle_map_transformer).
    Used when MapOperator fuses with AllToAllOperator via MapTransformer.fuse().
    """

    def __init__(
        self,
        map_transformer: MapTransformer,
        target_shuffle_max_block_size: int,
        random_shuffle: bool = False,
        random_seed: Optional[int] = None,
    ):
        self._map_transformer = map_transformer
        super().__init__(
            map_args=[
                map_transformer,
                target_shuffle_max_block_size,
                random_shuffle,
                random_seed,
            ],
            reduce_args=[random_shuffle, random_seed],
        )

    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        map_transformer: MapTransformer,
        target_shuffle_max_block_size: int,
        random_shuffle: bool,
        random_seed: Optional[int],
    ) -> List[Union[Block, BlockMetadataWithSchema]]:
        return _map_with_fused_transformer(
            idx,
            block,
            output_num_blocks,
            map_transformer,
            target_shuffle_max_block_size,
            random_shuffle,
            random_seed,
        )

    @staticmethod
    def reduce(
        random_shuffle: bool,
        random_seed: Optional[int],
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> tuple:
        return ShuffleTaskSpec.reduce(
            random_shuffle, random_seed, *mapper_outputs, partial_reduce=partial_reduce
        )
