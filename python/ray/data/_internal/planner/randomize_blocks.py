from typing import List

import numpy as np

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.transform_fn import (
    AllToAllTransformFnResult,
)
from ray.data._internal.logical.operators import RandomizeBlocks
from ray.data._internal.random_config import get_single_integer_random_seed
from ray.data.context import DataContext


def generate_randomize_blocks_fn(
    op: RandomizeBlocks,
    data_context: DataContext,
) -> AllToAllTransformFn:
    """Generate function to randomize order of blocks."""

    seed = get_single_integer_random_seed(op.seed_config, data_context)

    def fn(
        refs: List[RefBundle],
        context: TaskContext,
    ) -> AllToAllTransformFnResult:

        nonlocal op
        blocks_with_metadata = []
        index_to_schema = [None] * len(refs)
        for i, ref_bundle in enumerate(refs):
            index_to_schema[i] = ref_bundle.schema
            blocks_with_metadata.extend(
                (block, meta, i) for block, meta in ref_bundle.blocks
            )

        if len(blocks_with_metadata) == 0:
            return refs, {op.name: []}
        else:
            rng = np.random.default_rng(seed)
            input_owned = all(b.owns_blocks for b in refs)
            rng.shuffle(blocks_with_metadata)
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
            return output, {op.name: stats_list}

    return fn
