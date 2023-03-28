from typing import List, TYPE_CHECKING

import ray
from ray.data.block import Block, BlockAccessor

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


def make_ref_bundles(simple_data: List[Block]) -> List["RefBundle"]:
    """Create ref bundles from a list of block data.

    One bundle is created for each input block.
    """
    from ray.data._internal.execution.interfaces import RefBundle

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


def memory_string(num_bytes: int) -> str:
    """Return a human-readable memory string for the given amount of bytes."""
    if num_bytes >= 1024 * 1024 * 1024:
        mem = str(round(num_bytes / (1024 * 1024 * 1024), 2)) + " GiB"
    else:
        mem = str(round(num_bytes / (1024 * 1024), 2)) + " MiB"
    return mem


def locality_string(locality_hits: int, locality_misses) -> str:
    """Return a human-readable string for object locality stats."""
    if not locality_misses:
        return "[all objects local]"
    return f"[{locality_hits}/{locality_hits + locality_misses} objects local]"
