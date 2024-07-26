from typing import List, Optional, Tuple, TypeVar, Union

from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data.block import Block, BlockMetadata

T = TypeVar("T")


class RepartitionByColumnTaskSpec(ExchangeTaskSpec):
    """Example ExchangeTaskSpec"""

    SPLIT_SUB_PROGRESS_BAR_NAME = "Split blocks by column"
    MERGE_SUB_PROGRESS_BAR_NAME = "Merge blocks by column"

    def __init__(
        self,
        keys: Union[str, List[str]],
        concurrency: Optional[int],
    ):
        super().__init__(
            map_args=[keys, concurrency],
            reduce_args=[keys],
        )

    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        keys: Union[str, List[str]],
    ) -> List[Union[BlockMetadata, Block]]:
        pass

    @staticmethod
    def reduce(
        keys: Union[str, List[str]],
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> Tuple[Block, BlockMetadata]:
        pass
