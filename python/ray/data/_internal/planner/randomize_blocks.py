from typing import TYPE_CHECKING, List, Optional

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.transform_fn import (
    AllToAllTransformFnResult,
)
from ray.data._internal.logical.operators.all_to_all_operator import RandomizeBlocks

if TYPE_CHECKING:

    from ray.data.block import Schema


def generate_randomize_blocks_fn(
    op: RandomizeBlocks,
) -> AllToAllTransformFn:
    """Generate function to randomize order of blocks."""

    def fn(
        refs: List[RefBundle],
        schema: Optional["Schema"],
        context: TaskContext,
    ) -> AllToAllTransformFnResult:
        import random

        nonlocal op
        blocks_with_metadata = []
        for ref_bundle in refs:
            blocks_with_metadata.extend(ref_bundle.blocks)

        if len(blocks_with_metadata) == 0:
            return refs, {op._name: []}, schema
        else:
            if op._seed is not None:
                random.seed(op._seed)
            input_owned = all(b.owns_blocks for b in refs)
            random.shuffle(blocks_with_metadata)
            output = []
            stats_list = []
            for block, meta in blocks_with_metadata:
                stats_list.append(meta.to_stats())
                output.append(
                    RefBundle(
                        [
                            (
                                block,
                                meta,
                            )
                        ],
                        owns_blocks=input_owned,
                    )
                )
            return output, {op._name: stats_list}, schema

    return fn
