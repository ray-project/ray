from typing import List

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.transform_fn import (
    AllToAllTransformFnResult,
)
from ray.data._internal.logical.operators.all_to_all_operator import RandomizeBlocks


def generate_randomize_blocks_fn(
    op: RandomizeBlocks,
) -> AllToAllTransformFn:
    """Generate function to randomize order of blocks."""

    def fn(
        refs: List[RefBundle],
        context: TaskContext,
    ) -> AllToAllTransformFnResult:
        import random

        nonlocal op
        blocks_with_metadata = []
        index_to_schema = [None] * len(refs)
        for i, ref_bundle in enumerate(refs):
            index_to_schema[i] = ref_bundle.schema
            blocks_with_metadata.extend(
                (block, meta, i) for block, meta in ref_bundle.blocks
            )

        if len(blocks_with_metadata) == 0:
            return refs, {op._name: []}
        else:
            if op._seed is not None:
                random.seed(op._seed)
            input_owned = all(b.owns_blocks for b in refs)
            random.shuffle(blocks_with_metadata)
            output = []
            stats_list = []
            for block, meta, i in blocks_with_metadata:
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
                        schema=index_to_schema[i],
                    )
                )
            return output, {op._name: stats_list}

    return fn
