from typing import List

import ray
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_arrow_operator import FromArrowRefs
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import get_table_block_metadata


def _plan_from_arrow_refs_op(op: FromArrowRefs) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for `FromArrowRefs`.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        get_metadata = cached_remote_fn(get_table_block_metadata)
        metadata = ray.get([get_metadata.remote(t) for t in op._tables])
        ref_bundles: List[RefBundle] = [
            RefBundle([(table_ref, block_metadata)], owns_blocks=True)
            for table_ref, block_metadata in zip(op._tables, metadata)
        ]
        return ref_bundles

    return InputDataBuffer(input_data_factory=get_input_data)
