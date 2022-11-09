import logging
import uuid
from typing import Iterator, List, Tuple, TYPE_CHECKING

from ray.data._internal.block_list import BlockList
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block, BlockMetadata
from ray.data.context import DatasetContext
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.data.plan import Stage


logger = logging.getLogger(__name__)

BlockTuple = Tuple[ObjectRef[Block], BlockMetadata]


class AbstractExecutor:
    def execute(
        self, blocks: BlockList, stats: DatasetStats, stages: List["Stage"]
    ) -> (Iterator[BlockTuple], DatasetStats):
        raise NotImplementedError

    def legacy_execute_to_block_list(
        self, blocks: BlockList, stats: DatasetStats, stages: List["Stage"]
    ) -> (BlockList, DatasetStats):
        stats, block_iter = self.execute(blocks, stages)
        blocks, metadata = zip(*block_iter)
        block_list = BlockList(blocks, metadata)
        return block_list, stats


class BulkSyncExecutor(AbstractExecutor):
    def __init__(self, clear_input_blocks: bool, run_by_consumer: bool):
        self._clear_input_blocks = clear_input_blocks
        self._run_by_consumer = run_by_consumer

    def execute(
        self, blocks: BlockList, stats: DatasetStats, stages: List["Stage"]
    ) -> (Iterator[BlockTuple], DatasetStats):
        context = DatasetContext.get_current()
        for stage_idx, stage in enumerate(stages):
            stats_builder = stats.child_builder(stage.name)
            blocks, stage_info = stage(
                blocks, self._clear_input_blocks, self._run_by_consumer
            )
            if stage_info:
                stats = stats_builder.build_multistage(stage_info)
            else:
                stats = stats_builder.build(blocks)
            stats.dataset_uuid = uuid.uuid4().hex
            if context.enable_auto_log_stats:
                logger.info(stats.summary_string(include_parent=False))
        return blocks.iter_blocks_with_metadata(), stats
