from typing import List, Any

import ray
from ray.data.block import BlockAccessor
from ray.data._internal.execution.interfaces import RefBundle


def _make_ref_bundles(simple_data: List[List[Any]]) -> List[RefBundle]:
    output = []
    for block in simple_data:
        output.append(
            RefBundle(
                [
                    (
                        ray.put(block),
                        BlockAccessor.for_block(block).get_metadata([], None),
                    )
                ]
            )
        )
    return output
