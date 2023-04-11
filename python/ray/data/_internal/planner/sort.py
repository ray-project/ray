from functools import partial
from typing import List, Optional, Tuple, Union
from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    RefBundle,
    TaskContext,
)
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.pull_based_shuffle_task_scheduler import (
    PullBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKeyT, SortTaskSpec
from ray.data._internal.stats import StatsDict
from ray.data.block import _validate_key_fn
from ray.data.context import DatasetContext


def generate_sort_fn(
    key: SortKeyT,
    descending: bool,
) -> AllToAllTransformFn:
    """Generate function to sort blocks by the specified key column or key function."""
    import pyarrow as pa

    def fn(
        key: SortKeyT,
        descending: bool,
        refs: List[RefBundle],
        ctx: TaskContext,
    ) -> Tuple[List[RefBundle], StatsDict]:
        blocks = []
        block_schemas: List[Optional[Union[type, pa.lib.Schema]]] = []
        valid_block_schema: Optional[Union[type, pa.lib.Schema]] = None
        for ref_bundle in refs:
            for block, block_metadata in ref_bundle.blocks:
                blocks.append(block)
                if block_metadata.schema is not None and valid_block_schema is None:
                    valid_block_schema = block_metadata.schema
                block_schemas.append(block_metadata.schema)
        if len(blocks) == 0:
            return (blocks, {})
        if isinstance(valid_block_schema, pa.lib.Schema):
            unified_schema = unify_schemas(block_schemas)
        else:  # Covers both simple-type and None cases.
            if isinstance(valid_block_schema, type):
                assert all([b == valid_block_schema for b in block_schemas])
            unified_schema = valid_block_schema
        _validate_key_fn(unified_schema, key)

        if isinstance(key, str):
            key = [(key, "descending" if descending else "ascending")]
        if isinstance(key, list):
            descending = key[0][1] == "descending"

        num_mappers = len(blocks)
        # Use same number of output partitions.
        num_outputs = num_mappers

        # Sample boundaries for sort key.
        boundaries = SortTaskSpec.sample_boundaries(blocks, key, num_outputs)
        if descending:
            boundaries.reverse()
        sort_spec = SortTaskSpec(boundaries=boundaries, key=key, descending=descending)

        if DatasetContext.get_current().use_push_based_shuffle:
            scheduler = PushBasedShuffleTaskScheduler(sort_spec)
        else:
            scheduler = PullBasedShuffleTaskScheduler(sort_spec)

        return scheduler.execute(refs, num_outputs)

    # NOTE: use partial function to pass parameters to avoid error like
    # "UnboundLocalError: local variable ... referenced before assignment",
    # because `key` and `descending` variables are reassigned in `fn()`.
    return partial(fn, key, descending)
