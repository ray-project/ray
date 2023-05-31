from typing import List

import ray
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.from_numpy_operator import FromNumpyRefs


def _plan_from_numpy_refs_op(op: FromNumpyRefs) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for FromNumpyRefs.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        from ray.data._internal.remote_fn import cached_remote_fn
        from ray.data._internal.util import ndarray_to_block

        ndarray_to_block_remote = cached_remote_fn(ndarray_to_block, num_returns=2)

        ctx = ray.data.DataContext.get_current()
        res = [ndarray_to_block_remote.remote(arr_ref, ctx) for arr_ref in op._ndarrays]
        blocks, metadata = map(list, zip(*res))
        metadata = ray.get(metadata)
        ref_bundles: List[RefBundle] = [
            RefBundle([(block, block_metadata)], owns_blocks=True)
            for block, block_metadata in zip(blocks, metadata)
        ]
        return ref_bundles

    return InputDataBuffer(input_data_factory=get_input_data)
