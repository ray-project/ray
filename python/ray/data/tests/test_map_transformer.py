import gc
import time
import weakref
from collections import deque

import pandas as pd
import pyarrow as pa
import pytest

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    BlockMapTransformFn,
    MapTransformer,
    TransformClock,
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


def test_chained_transforms_dont_double_count_block_transform_time():
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

    transformer = _create_chained_transformer(udf, NUM_CHAINED_TRANSFORMS)
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
    ), f"reported block transform time {reported_s:.4f}s exceeds chain wall time {wall_s:.4f}s"
    # ...and the stages' time must still be measured, not dropped.
    assert reported_s >= slept_s * 0.9, (
        f"reported block transform time {reported_s:.4f}s is below the {slept_s:.4f}s slept "
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
    ), f"reported block transform time {reported_s:.4f}s exceeds chain wall time {wall_s:.4f}s"
    assert reported_s >= slept_s * 0.9, (
        f"reported block transform time {reported_s:.4f}s is below the {slept_s:.4f}s slept "
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

    transformer = _create_chained_transformer(udf, 1)
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
        f"per-block transform times {[round(s, 4) for s in reported_s]} do not all "
        f"cover the {SLEEP_S}s slept while producing each block"
    )


def _failing_block_transformer(tables_before_error):
    """A one-stage transformer whose function yields ``tables_before_error``
    one-row tables, then raises ``ValueError("bad input")``. Its output blocks
    are 1 MiB, so the tables share one buffered block."""

    def fn(blocks, ctx):
        for i in range(tables_before_error):
            yield pa.table({"x": [i]})
        raise ValueError("bad input")

    return MapTransformer(
        [
            BlockMapTransformFn(
                fn,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=1024 * 1024
                ),
            )
        ]
    )


def _outputs(transformer, **retry_settings):
    """The transformer's output blocks for one one-row input block, in a task
    whose ``TaskContext`` carries ``retry_settings`` (how ``_map_task`` and Ray
    rerun it).

    A generator, so the task context is current while the blocks are drawn, as
    ``_map_task`` sets it.
    """
    ctx = TaskContext(task_idx=0, op_name="test", **retry_settings)
    with TaskContext.current(ctx):
        yield from transformer.apply_transform(
            iter([pa.table({"x": [0]})]), ctx, clock=TransformClock()
        )


@pytest.fixture
def skip_errors(monkeypatch):
    """Let the job skip an errored block (``max_errored_blocks=1``); returns the
    patched ``DataContext``."""
    from ray.data.context import DataContext

    context = DataContext.get_current()
    monkeypatch.setattr(context, "max_errored_blocks", 1)
    return context


def test_a_skipped_error_emits_the_buffered_rows_first(skip_errors):
    """When the job skips errors, rows a transform produced before raising come
    out as one last block, then the error is raised, so the task keeps them."""
    out = _outputs(_failing_block_transformer(tables_before_error=2))

    assert BlockAccessor.for_block(next(out)).to_arrow()["x"].to_pylist() == [0, 1]
    with pytest.raises(ValueError, match="bad input"):
        next(out)


def test_an_error_that_fails_the_job_emits_no_partial_block():
    """With ``max_errored_blocks=0``, the default, the job fails anyway, so the
    transform raises without emitting its buffered rows."""
    out = _outputs(_failing_block_transformer(tables_before_error=2))

    with pytest.raises(ValueError, match="bad input"):
        next(out)


def test_a_transform_failing_before_any_output_emits_no_block(skip_errors):
    """A transform that raises before producing a row raises at once, without
    an empty block."""
    out = _outputs(_failing_block_transformer(tables_before_error=0))

    with pytest.raises(ValueError, match="bad input"):
        next(out)


def test_a_transform_that_may_rerun_raises_without_a_partial_block(skip_errors):
    """When ``_map_task`` may rerun the transform (``retried_map_errors``), the
    buffered rows are not emitted: the rerun skips as many blocks as were
    emitted, so a partial block would hide rows of the rerun's first block."""
    # ``None`` patterns retry any error, as ``retried_map_errors=True`` sets them.
    out = _outputs(
        _failing_block_transformer(tables_before_error=2),
        map_retries_left=1,
        map_retried_errors=None,
    )

    with pytest.raises(ValueError, match="bad input"):
        next(out)


def test_an_error_no_retry_pattern_matches_emits_the_buffered_rows_first(
    skip_errors,
):
    """``_map_task`` retries only errors that match ``retried_map_errors``, so an
    error that matches none won't rerun the transform, and its buffered rows come
    out first."""
    out = _outputs(
        _failing_block_transformer(tables_before_error=2),
        map_retries_left=1,
        map_retried_errors=["connection reset"],
    )

    assert BlockAccessor.for_block(next(out)).to_arrow()["x"].to_pylist() == [0, 1]
    with pytest.raises(ValueError, match="bad input"):
        next(out)


def test_an_error_a_retry_pattern_matches_raises_without_a_partial_block(
    skip_errors,
):
    """An error that matches a ``retried_map_errors`` pattern may rerun the
    transform, so its buffered rows are not emitted."""
    out = _outputs(
        _failing_block_transformer(tables_before_error=2),
        map_retries_left=1,
        map_retried_errors=["bad input"],
    )

    with pytest.raises(ValueError, match="bad input"):
        next(out)


def test_an_error_ray_reruns_the_task_on_raises_without_a_partial_block(
    skip_errors,
):
    """Ray reruns a task on an error its ``retry_exceptions`` names, so the
    buffered rows are not emitted, as for ``retried_map_errors``."""
    out = _outputs(
        _failing_block_transformer(tables_before_error=2),
        ray_retried_exceptions=[ValueError],
    )

    with pytest.raises(ValueError, match="bad input"):
        next(out)


def test_an_error_ray_does_not_rerun_the_task_on_emits_the_buffered_rows_first(
    skip_errors,
):
    """An error that ``retry_exceptions`` does not name won't rerun the task,
    so its buffered rows come out first."""
    out = _outputs(
        _failing_block_transformer(tables_before_error=2),
        ray_retried_exceptions=[KeyError],
    )

    assert BlockAccessor.for_block(next(out)).to_arrow()["x"].to_pylist() == [0, 1]
    with pytest.raises(ValueError, match="bad input"):
        next(out)


def _failing_system_error_transformer() -> MapTransformer:
    """A transformer that yields two one-row tables into one buffered block,
    then raises a ``RaySystemError``."""
    from ray.exceptions import RaySystemError

    def fn(blocks, ctx):
        yield pa.table({"x": [0]})
        yield pa.table({"x": [1]})
        raise RaySystemError("lost an object")

    return MapTransformer(
        [
            BlockMapTransformFn(
                fn,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=1024 * 1024
                ),
            )
        ]
    )


@pytest.mark.parametrize(
    "remote_args, emits_rows",
    [
        # Ray Data's default: unlimited retries, ``RaySystemError`` among them.
        ({}, False),
        # With ``max_retries=0`` Ray reruns on nothing.
        ({"max_retries": 0}, True),
    ],
)
def test_a_system_error_emits_the_buffered_rows_only_when_ray_wont_rerun(
    skip_errors, remote_args, emits_rows
):
    """Ray reruns a task on a ``RaySystemError`` only while its retries allow, so
    with ``max_retries=0`` the buffered rows come out before the error."""
    from ray.data._internal.execution.operators.task_pool_map_operator import (
        _ray_retried_exceptions,
    )
    from ray.exceptions import RaySystemError

    out = _outputs(
        _failing_system_error_transformer(),
        ray_retried_exceptions=_ray_retried_exceptions(remote_args),
    )

    if emits_rows:
        assert BlockAccessor.for_block(next(out)).to_arrow()["x"].to_pylist() == [
            0,
            1,
        ]
    with pytest.raises(RaySystemError, match="lost an object"):
        next(out)


def test_the_last_map_attempt_emits_the_buffered_rows_first(skip_errors):
    """On its last attempt ``_map_task`` won't rerun the transform even for an
    error a pattern matches, so the buffered rows come out first."""
    out = _outputs(
        _failing_block_transformer(tables_before_error=2),
        map_retries_left=0,
        map_retried_errors=["bad input"],
    )

    assert BlockAccessor.for_block(next(out)).to_arrow()["x"].to_pylist() == [0, 1]
    with pytest.raises(ValueError, match="bad input"):
        next(out)


def test_actor_retry_settings_do_not_stop_a_task_pool_map_from_emitting(
    skip_errors, monkeypatch
):
    """``actor_task_retry_on_errors`` applies to actor tasks only, through their
    ``retry_exceptions``. A task that Ray won't rerun still emits its buffered
    rows when the setting is on."""
    monkeypatch.setattr(skip_errors, "actor_task_retry_on_errors", True)
    out = _outputs(_failing_block_transformer(tables_before_error=2))

    assert BlockAccessor.for_block(next(out)).to_arrow()["x"].to_pylist() == [0, 1]
    with pytest.raises(ValueError, match="bad input"):
        next(out)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
