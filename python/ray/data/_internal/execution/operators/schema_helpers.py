from typing import TYPE_CHECKING

import ray
from ray.data.block import Block, BlockAccessor

if TYPE_CHECKING:
    import pyarrow as pa


@ray.remote(num_cpus=0)
def _get_arrow_schema_from_block(block: Block) -> "pa.Schema":
    """Extract a block's Arrow schema from a 1-row sample.

    This runs as a lightweight remote metadata task to avoid pulling block data
    to the driver and does not consume a CPU slot.
    """
    accessor = BlockAccessor.for_block(block)
    sample_block = accessor.slice(0, 1)
    sample_accessor = BlockAccessor.for_block(sample_block)
    return sample_accessor.to_arrow().schema
