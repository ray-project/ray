from typing import List

import ray
from ray.data.block import Block, BlockAccessor
from ray.data._internal.execution.interfaces import RefBundle


def make_ref_bundles(simple_data: List[Block]) -> List[RefBundle]:
    """Create ref bundles from a list of block data.

    One bundle is created for each input block.
    """
    output = []
    for block in simple_data:
        output.append(
            RefBundle(
                [
                    (
                        ray.put(block),
                        BlockAccessor.for_block(block).get_metadata([], None),
                    )
                ],
                owns_blocks=True,
            )
        )
    return output
