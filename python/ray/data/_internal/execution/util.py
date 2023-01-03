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


def merge_ref_bundles(x: RefBundle, y: RefBundle) -> RefBundle:
    """Merge two ref bundles into a single bundle of multiple blocks."""
    if x is None:
        return y
    elif y is None:
        return x
    else:
        return RefBundle(
            x.blocks + y.blocks,
            x.owns_blocks and y.owns_blocks,
        )
