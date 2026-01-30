import gc
import weakref
from collections import deque

import pandas as pd
import pytest

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    MapTransformer,
)
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_udf_map_op import (
    _generate_transform_fn_for_map_batches,
)
from ray.data.block import DataBatch


def _create_tracking_udf(input_intermediates: list):
    """Create a UDF that creates new DataFrames and tracks them via weakrefs."""

    def udf(batch: DataBatch) -> DataBatch:
        new_batch = pd.DataFrame({"id": batch["id"] * 2})
        input_intermediates.append(weakref.ref(batch))
        return new_batch

    return udf


def _create_chained_transformer(udf, n):
    """Create a MapTransformer with chained batch transforms that track intermediates."""
    transform_fns = [
        BatchMapTransformFn(
            _generate_transform_fn_for_map_batches(udf),
            batch_size=1,
            output_block_size_option=OutputBlockSizeOption.of(target_max_block_size=1),
        )
        for _ in range(n)
    ]
    return MapTransformer(transform_fns)


def test_chained_transforms_release_intermediates_between_batches():
    """Test that chained transforms release intermediate refs when moving to next batch.

    This test uses `_generate_transform_fn_for_map_batches` to wrap UDFs,
    which is the same code path used in production by `map_batches`.
    """
    NUM_BATCHES = 1
    NUM_CHAINED_TRANSFORMS = 5

    input_intermediates: deque = deque()

    def udf(batch: DataBatch) -> DataBatch:
        # Append received batch into a list
        #
        # NOTE: Every of the chained UDFs will be appending into this list in
        #       order, meaning that in 1 iteration N refs will be added, where
        #       N is the number of chained UDFs
        print(f">>> [DBG] Batch: {batch=}")

        input_intermediates.append(weakref.ref(batch))

        return pd.DataFrame({"id": batch["id"] * 2})

    transformer = _create_chained_transformer(udf, NUM_CHAINED_TRANSFORMS)
    ctx = TaskContext(task_idx=0, op_name="test")

    # Use a generator instead of a list to avoid list_iterator holding references
    def make_input_blocks():
        for i in range(NUM_BATCHES):
            yield pd.DataFrame({"id": [i + 1]})

    result_iter = transformer.apply_transform(make_input_blocks(), ctx)

    for i in range(NUM_BATCHES):
        # Consume batch
        result = next(result_iter)
        assert result is not None

        pd.testing.assert_frame_equal(result, pd.DataFrame({"id": [(i + 1) * 2 ** NUM_CHAINED_TRANSFORMS]}))

        # Trigger GC
        gc.collect()

        print(f">>> {input_intermediates=}")

        # Extract current set of intermediate input refs
        cur_intermediates = [input_intermediates.popleft() for _ in range(NUM_CHAINED_TRANSFORMS)]
        assert len(input_intermediates) == 0

        alive_after_first = sum(
            1 for ref in cur_intermediates if ref() is not None
        )

        if alive_after_first > 0:
            print(">>> Found captured intermediate references!")

            _trace_back_refs(cur_intermediates, "After first batch")

            pytest.fail(f"Expected 0 intermediates alive after first batch, found {alive_after_first}")


def _trace_back_refs(intermediates: list, label: str = ""):
    """Debug utility to show which intermediates are alive and what holds them.

    Args:
        intermediates: List of weakrefs to track
        label: Optional label for the debug output
    """
    if label:
        print(f"\n{label}:")
    for i, ref in enumerate(intermediates):
        obj = ref()
        print(f"  intermediate[{i}]: {'ALIVE' if obj is not None else 'dead'}")
        if obj is not None:
            referrers = gc.get_referrers(obj)
            for r in referrers:
                if isinstance(r, list):
                    print(f"    -> list (len={len(r)}, id={id(r)})")
                    # Find what holds this list - 2 levels up
                    list_referrers = gc.get_referrers(r)
                    for lr in list_referrers:
                        if hasattr(lr, "gi_frame") and lr.gi_frame:
                            print(
                                f"       held by generator: {lr.__name__} at "
                                f"{lr.gi_frame.f_code.co_filename.split('/')[-1]}:"
                                f"{lr.gi_frame.f_lineno}"
                            )
                        elif hasattr(lr, "__class__") and not isinstance(
                            lr, (dict, list, tuple)
                        ):
                            print(f"       held by {type(lr).__name__}")
                elif isinstance(r, dict):
                    # Skip frame dicts
                    pass
                elif hasattr(r, "gi_frame"):
                    frame = r.gi_frame
                    if frame:
                        print(
                            f"    -> generator: {r.__name__} at "
                            f"{frame.f_code.co_filename.split('/')[-1]}:{frame.f_lineno}"
                        )
                else:
                    print(f"    -> {type(r).__name__}")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
