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
from ray.data.block import BlockAccessor, DataBatch


def _create_chained_transformer(udf, n):
    """Create a MapTransformer with chained batch transforms that track intermediates."""
    transform_fns = [
        BatchMapTransformFn(
            _generate_transform_fn_for_map_batches(udf),
            batch_format="pandas",
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

        # apply_transform returns Arrow blocks, convert to pandas to test the correctness of the result
        result_df = BlockAccessor.for_block(result).to_pandas()
        expected_df = pd.DataFrame(
            {"id": [(i + 1) * 2**NUM_CHAINED_TRANSFORMS]}
        ).astype(result_df.dtypes.to_dict())
        pd.testing.assert_frame_equal(result_df, expected_df)

        # Trigger GC
        gc.collect()

        # Extract current set of intermediate input refs
        cur_intermediates = [
            input_intermediates.popleft() for _ in range(NUM_CHAINED_TRANSFORMS)
        ]
        assert len(input_intermediates) == 0

        alive_after_first = sum(1 for ref in cur_intermediates if ref() is not None)

        if alive_after_first > 0:
            print(">>> Found captured intermediate references!")

            _trace_back_refs(cur_intermediates, "After first batch")

            pytest.fail(
                f"Expected 0 intermediates alive after first batch, found {alive_after_first}"
            )


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


def _make_udf_batch_fn(udf, batch_size=8, batch_format="numpy"):
    return BatchMapTransformFn(
        _generate_transform_fn_for_map_batches(udf),
        is_udf=True,
        batch_size=batch_size,
        batch_format=batch_format,
        output_block_size_option=OutputBlockSizeOption.of(target_max_block_size=1),
    )


def test_combine_merges_compatible_pair():
    fn1 = _make_udf_batch_fn(lambda b: b)
    fn2 = _make_udf_batch_fn(lambda b: b)
    result = MapTransformer._combine_transformations([fn1], [fn2])
    assert len(result) == 1
    assert isinstance(result[0], BatchMapTransformFn)
    assert len(result[0]._component_reprs) == 2


@pytest.mark.parametrize(
    "kw_up,kw_down",
    [
        # mismatched batch_format
        ({"batch_format": "numpy"}, {"batch_format": "pandas"}),
        # mismatched batch_size
        ({"batch_size": 4}, {"batch_size": 8}),
        # auto batch_size (upstream)
        ({"batch_size": "auto"}, {"batch_size": 8}),
        # auto batch_size (downstream)
        ({"batch_size": 8}, {"batch_size": "auto"}),
    ],
)
def test_combine_no_merge_mismatched(kw_up, kw_down):
    defaults = {"batch_size": 8, "batch_format": "numpy"}
    fn1 = _make_udf_batch_fn(lambda b: b, **{**defaults, **kw_up})
    fn2 = _make_udf_batch_fn(lambda b: b, **{**defaults, **kw_down})
    result = MapTransformer._combine_transformations([fn1], [fn2])
    assert len(result) == 2


def test_combine_three_compatible_fns_fully_merge():
    fns = [_make_udf_batch_fn(lambda b: b) for _ in range(3)]
    result = MapTransformer._combine_transformations(fns[:2], fns[2:])
    assert len(result) == 1
    assert len(result[0]._component_reprs) == 3


def test_combine_partial_merge():
    fn_numpy_1 = _make_udf_batch_fn(lambda b: b, batch_format="numpy")
    fn_numpy_2 = _make_udf_batch_fn(lambda b: b, batch_format="numpy")
    fn_pandas = _make_udf_batch_fn(lambda b: b, batch_format="pandas")
    result = MapTransformer._combine_transformations(
        [fn_numpy_1, fn_numpy_2], [fn_pandas]
    )
    assert len(result) == 2
    assert len(result[0]._component_reprs) == 2
    assert getattr(result[1], "_component_reprs", None) is None


def test_is_udf_false_prevents_merge():
    fn_udf = _make_udf_batch_fn(lambda b: b)
    fn_internal = BatchMapTransformFn(
        lambda batches, ctx: (b for b in batches),
        is_udf=False,
        batch_size=8,
        batch_format="numpy",
        output_block_size_option=OutputBlockSizeOption.of(target_max_block_size=1),
    )
    assert len(MapTransformer._combine_transformations([fn_udf], [fn_internal])) == 2
    assert len(MapTransformer._combine_transformations([fn_internal], [fn_udf])) == 2


def test_merge_eliminates_batch_blocks_calls():
    # batch_blocks is imported as a local name in map_transformer; patch there.
    import ray.data._internal.execution.operators.map_transformer as mt_mod
    from ray.data._internal.block_batching.block_batching import (
        batch_blocks as real_batch_blocks,
    )

    call_count = []

    def counting_batch_blocks(*args, **kwargs):
        call_count.append(1)
        return real_batch_blocks(*args, **kwargs)

    fns = [_make_udf_batch_fn(lambda b: b) for _ in range(3)]
    transformer = MapTransformer(fns)
    assert len(transformer.get_transform_fns()) == 1, "3 compatible fns should merge to 1"

    ctx = TaskContext(task_idx=0, op_name="test")
    input_block = pd.DataFrame({"x": list(range(8))})

    original = mt_mod.batch_blocks
    mt_mod.batch_blocks = counting_batch_blocks
    try:
        list(transformer.apply_transform([input_block], ctx))
    finally:
        mt_mod.batch_blocks = original

    assert len(call_count) == 1, (
        f"Expected 1 batch_blocks call for 3 merged fns, got {len(call_count)}. "
        "Without merge, 3 separate fns would call batch_blocks 3 times."
    )


def test_merge_correctness_matches_reference():
    import numpy as np

    def double(batch):
        return {k: v * 2 for k, v in batch.items()}

    def add_one(batch):
        return {k: v + 1 for k, v in batch.items()}

    merged_transformer = MapTransformer(
        [_make_udf_batch_fn(double), _make_udf_batch_fn(add_one)]
    )
    assert len(merged_transformer.get_transform_fns()) == 1

    ref_transformer = MapTransformer([_make_udf_batch_fn(lambda b: add_one(double(b)))])

    ctx = TaskContext(task_idx=0, op_name="test")
    input_block = pd.DataFrame({"x": list(range(16))})

    def collect(t):
        return pd.concat(
            [BlockAccessor.for_block(b).to_pandas() for b in t.apply_transform([input_block], ctx)]
        ).reset_index(drop=True)

    pd.testing.assert_frame_equal(collect(merged_transformer), collect(ref_transformer))


def test_merged_repr_shows_components():
    fn1 = _make_udf_batch_fn(lambda b: b)
    fn2 = _make_udf_batch_fn(lambda b: b)
    result = MapTransformer._combine_transformations([fn1], [fn2])
    r = repr(result[0])
    assert "MergedBatchMapTransformFn" in r
    assert "->" in r


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
