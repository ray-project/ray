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
    if num_bytes > 1024 * 1024 * 1024:
        mem = str(round(num_bytes / (1024 * 1024 * 1024), 2)) + " GiB"
    else:
        mem = str(round(num_bytes / (1024 * 1024), 2)) + " MiB"
    return mem


def locality_string(locality_hits: int, locality_misses) -> str:
    """Return a human-readable string for object locality stats."""
    try:
        p = round((locality_hits / (locality_hits + locality_misses)) * 100, 1)
    except ZeroDivisionError:
        p = 100.0
    return f"{p}% locality"
