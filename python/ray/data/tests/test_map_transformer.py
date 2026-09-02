import gc
import time
import weakref
from collections import deque

import pandas as pd
import pytest

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    MapTransformer,
    TransformClock,
)
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_udf_map_op import (
    _generate_transform_fn_for_map_batches,
)
from ray.data.block import BlockAccessor, DataBatch


def _create_chained_transformer(udf, n, *, is_udf=False):
    """Create a MapTransformer with chained batch transforms that track intermediates.

    ``is_udf`` mirrors what the planner sets for real ``map_batches`` stages; it
    gates UDF timing, so tests that assert on ``udf_time_s`` must pass True.
    """
    transform_fns = [
        BatchMapTransformFn(
            _generate_transform_fn_for_map_batches(udf),
            is_udf=is_udf,
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

    result_iter = transformer.apply_transform(
        make_input_blocks(), ctx, clock=TransformClock()
    )

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


def test_chained_transforms_dont_double_count_udf_time():
    """Chained UDF stages must not each re-count their upstream stages' time.

    Timing every stage and summing reported n(n+1)/2x the real time, which could
    exceed the wall time of the chain that produced it.
    """
    NUM_CHAINED_TRANSFORMS = 3
    NUM_BATCHES = 2
    SLEEP_S = 0.05

    num_calls = 0

    def udf(batch: DataBatch) -> DataBatch:
        nonlocal num_calls
        num_calls += 1
        time.sleep(SLEEP_S)
        return pd.DataFrame({"id": batch["id"]})

    # is_udf=True is what gates timing; without it no stage is timed at all.
    transformer = _create_chained_transformer(udf, NUM_CHAINED_TRANSFORMS, is_udf=True)
    ctx = TaskContext(task_idx=0, op_name="test")

    def make_input_blocks():
        for i in range(NUM_BATCHES):
            yield pd.DataFrame({"id": [i]})

    scope = TransformClock()
    start_s = time.perf_counter()
    blocks = list(transformer.apply_transform(make_input_blocks(), ctx, clock=scope))
    wall_s = time.perf_counter() - start_s

    # Assert on rows rather than block count, which depends on block shaping.
    assert sum(BlockAccessor.for_block(b).num_rows() for b in blocks) == NUM_BATCHES
    assert num_calls > 0

    # The whole transform chain, not just the sleeps: for each stage it covers
    # turning input blocks into batches, the UDF body, and building output
    # blocks. The sleeps are therefore a floor on it, never an equality.
    reported_s = scope.drain().total_s
    # Measured, not assumed: block shaping decides how many batches each stage sees.
    slept_s = num_calls * SLEEP_S

    # Can't exceed the wall time of the chain that produced it. The headroom
    # absorbs timing noise; double counting shows up at ~2x for 3 stages.
    assert (
        reported_s <= wall_s * 1.05
    ), f"reported UDF time {reported_s:.4f}s exceeds chain wall time {wall_s:.4f}s"
    # ...and the stages' time must still be measured, not dropped.
    assert reported_s >= slept_s * 0.9, (
        f"reported UDF time {reported_s:.4f}s is below the {slept_s:.4f}s slept "
        f"across {num_calls} UDF calls"
    )


def test_chained_transforms_total_is_independent_of_distribution():
    """The total must be right however unevenly the time is spread.

    The test above gives every stage the same sleep, so a bug that shifted time
    from one stage to another would leave the total intact and go unnoticed.
    Uneven sleeps remove that cover: the subtraction has to land on the right
    stage for the sum to come out.
    """
    SLEEPS_S = [0.05, 0.1, 0.15]
    NUM_BATCHES = 2

    calls_per_stage = [0] * len(SLEEPS_S)

    def make_udf(stage_idx: int) -> "callable":
        def udf(batch: DataBatch) -> DataBatch:
            calls_per_stage[stage_idx] += 1
            time.sleep(SLEEPS_S[stage_idx])
            return pd.DataFrame({"id": batch["id"]})

        return udf

    transform_fns = [
        BatchMapTransformFn(
            _generate_transform_fn_for_map_batches(make_udf(i)),
            is_udf=True,
            batch_format="pandas",
            batch_size=1,
            output_block_size_option=OutputBlockSizeOption.of(target_max_block_size=1),
        )
        for i in range(len(SLEEPS_S))
    ]
    transformer = MapTransformer(transform_fns)
    ctx = TaskContext(task_idx=0, op_name="test")

    def make_input_blocks():
        for i in range(NUM_BATCHES):
            yield pd.DataFrame({"id": [i]})

    scope = TransformClock()
    start_s = time.perf_counter()
    blocks = list(transformer.apply_transform(make_input_blocks(), ctx, clock=scope))
    wall_s = time.perf_counter() - start_s

    assert sum(BlockAccessor.for_block(b).num_rows() for b in blocks) == NUM_BATCHES
    assert all(n > 0 for n in calls_per_stage), calls_per_stage

    reported_s = scope.drain().total_s
    slept_s = sum(n * s for n, s in zip(calls_per_stage, SLEEPS_S))

    # Summing the stages' inclusive times would give 0.05 + 0.15 + 0.30 = 0.50s
    # against 0.30s of real sleeping -- 1.7x. The subtraction has to cancel that
    # exactly, not approximately.
    assert (
        reported_s <= wall_s * 1.05
    ), f"reported UDF time {reported_s:.4f}s exceeds chain wall time {wall_s:.4f}s"
    assert reported_s >= slept_s * 0.9, (
        f"reported UDF time {reported_s:.4f}s is below the {slept_s:.4f}s slept "
        f"across stages {calls_per_stage}"
    )


def test_every_output_block_is_timed():
    """Draining must not stop the chain from measuring the next block.

    `_map_task` drains once per output block, so a drain that rebound its
    totals instead of clearing them left the steps writing to a list nobody
    read again: a task's first block reported its time and every block after it
    reported zero.
    """
    NUM_BLOCKS = 3
    SLEEP_S = 0.05

    def udf(batch: DataBatch) -> DataBatch:
        time.sleep(SLEEP_S)
        return pd.DataFrame({"id": batch["id"]})

    transformer = _create_chained_transformer(udf, 1, is_udf=True)
    ctx = TaskContext(task_idx=0, op_name="test")

    def make_input_blocks():
        for i in range(NUM_BLOCKS):
            yield pd.DataFrame({"id": [i]})

    clock = TransformClock()
    # `_create_chained_transformer` shapes one batch per output block, so each
    # `next()` here is one output block, drained the way `_map_task` drains it.
    out = iter(transformer.apply_transform(make_input_blocks(), ctx, clock=clock))
    reported_s = []
    for _ in range(NUM_BLOCKS):
        next(out)
        reported_s.append(clock.drain().total_s)

    assert all(s >= SLEEP_S * 0.9 for s in reported_s), (
        f"per-block UDF times {[round(s, 4) for s in reported_s]} do not all "
        f"cover the {SLEEP_S}s slept while producing each block"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
