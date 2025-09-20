from contextlib import contextmanager
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
from ray.data._internal.logical.operators.all_to_all_operator import RandomizeBlocks
from ray.data.context import DataContext


@contextmanager
def random_state_context(data_context: DataContext, op: RandomizeBlocks):

    always_reset = data_context.always_reset_random_state_for_random_ops

    if "random_state" in data_context._kv_configs and not always_reset:
        # Reuse the random state if it exists
        random_state = data_context._kv_configs["random_state"]
    else:
        random_state = np.random.default_rng(op._seed)

    yield random_state

    if always_reset:
        data_context._kv_configs.pop("random_state", None)
    else:
        data_context._kv_configs["random_state"] = random_state


def generate_randomize_blocks_fn(
    op: RandomizeBlocks,
    data_context: DataContext,
) -> AllToAllTransformFn:
    """Generate function to randomize order of blocks."""

    def fn(
        refs: List[RefBundle],
        context: TaskContext,
    ) -> AllToAllTransformFnResult:

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

            input_owned = all(b.owns_blocks for b in refs)

            nonlocal data_context
            with random_state_context(data_context, op) as random_state:
                random_state.shuffle(blocks_with_metadata)

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
