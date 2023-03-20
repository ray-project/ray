from typing import List

import ray
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_arrow_operator import FromArrowRefs
from ray import ObjectRef
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import _get_metadata


get_metadata = cached_remote_fn(_get_metadata)


def _plan_from_arrow_refs_op(op: FromArrowRefs) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for `FromArrowRefs`.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        ref_bundles: List[RefBundle] = []
        for idx, table_ref in enumerate(op._tables):
            if not isinstance(table_ref, ObjectRef):
                op._tables[idx] = ray.put(table_ref)
                table_ref = op._tables[idx]
            block_metadata = get_metadata.remote(table_ref)
            ref_bundles.append(
                RefBundle(
                    [table_ref, ray.get(block_metadata)],
                    owns_blocks=True,
                )
            )
        return ref_bundles

    return InputDataBuffer(input_data_factory=get_input_data)
