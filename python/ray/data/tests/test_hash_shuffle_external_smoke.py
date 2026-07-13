"""Smoke test for the external-shuffle operator pair
(``ExternalHashShuffleMapOp`` + ``ExternalHashShuffleReduceOp``).

Wires the operators directly — bypassing the Ray Data planner — to verify
the simplest end-to-end story: feed N input blocks, hash-partition into K
partitions, reduce each with ``_concat_reduce``, then check row count and
partition count. Catches wiring bugs (RefBundle shape, sentinel metadata,
callback ordering, ShuffleManager lifecycle) that the planner-driven
tests in ``test_hash_shuffle_external_repartition.py`` don't isolate.
"""

import os
import time

import pyarrow as pa
import pytest

import ray
from ray.data._internal.arrow_ops.transform_pyarrow import hash_partition
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionOptions,
    RefBundle,
)
from ray.data._internal.execution.operators.hash_shuffle_v2 import (
    _concat_reduce,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator_external import (  # noqa: E501
    ExternalHashShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator_external import (  # noqa: E501
    ExternalHashShuffleReduceOp,
)
from ray.data._internal.stats import Timer
from ray.data.block import BlockAccessor
from ray.data.context import DataContext


# --- helpers -----------------------------------------------------------------


def _make_partition_fn(key_columns, num_partitions):
    """Concrete PartitionFn over ``hash_partition``."""

    def _partition(block: pa.Table):
        return hash_partition(
            block, hash_cols=key_columns, num_partitions=num_partitions
        )

    return _partition


def _build_input_bundles(num_blocks: int, rows_per_block: int) -> list:
    """Create ``num_blocks`` Plasma'd Arrow tables, return them as input
    RefBundles. Distinct ``id`` values so we can hash-partition them
    meaningfully."""
    bundles = []
    next_id = 0
    for _ in range(num_blocks):
        ids = list(range(next_id, next_id + rows_per_block))
        vals = [f"val_{i}" for i in ids]
        table = pa.table({"id": ids, "val": vals})
        next_id += rows_per_block
        ref = ray.put(table)
        meta = BlockAccessor.for_block(table).get_metadata()
        bundles.append(
            RefBundle(
                (BlockEntry(ref=ref, metadata=meta),),
                schema=table.schema,
                owns_blocks=False,
            )
        )
    return bundles


def _drain_op(op, *, timeout_s: float = 30.0) -> list:
    """Pump the operator until execution finishes; collect output bundles.

    Mirrors the dispatch in
    ``streaming_executor_state.process_completed_tasks``: ``ray.wait`` on
    every active task's waitable, then call ``on_data_ready`` on
    ``DataOpTask`` (streaming gen) and ``on_task_finished`` on
    ``MetadataOpTask`` (single ref). No backpressure or input-queue
    ordering — that's the executor's job.
    """
    from ray.data._internal.execution.interfaces.physical_operator import (
        DataOpTask,
        MetadataOpTask,
    )
    from ray.data._internal.execution.metadata_fetcher import InlineMetadataFetcher

    metadata_fetcher = InlineMetadataFetcher()

    bundles = []
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        while op.has_next():
            bundles.append(op.get_next())
        active_tasks = op.get_active_tasks()
        if not active_tasks:
            if op.has_execution_finished():
                break
            time.sleep(0.05)
            continue
        ref_to_task = {t.get_waitable(): t for t in active_tasks}
        ready, _ = ray.wait(
            list(ref_to_task),
            num_returns=len(ref_to_task),
            fetch_local=False,
            timeout=0.1,
        )
        for ref in ready:
            task = ref_to_task[ref]
            if isinstance(task, DataOpTask):
                task.on_data_ready(None, metadata_fetcher)
            else:
                assert isinstance(task, MetadataOpTask)
                task.on_task_finished()
        if op.has_execution_finished():
            break
    while op.has_next():
        bundles.append(op.get_next())
    return bundles


def _total_rows(bundles) -> int:
    return sum(b.num_rows() or 0 for b in bundles)


# --- tests -------------------------------------------------------------------


@pytest.fixture(scope="module")
def ray_init_shutdown():
    if not ray.is_initialized():
        ray.init(num_cpus=4, include_dashboard=False, ignore_reinit_error=True)
    yield
    # Leave Ray up; multiple tests in this file reuse it.


@pytest.mark.parametrize("num_blocks,rows,num_parts", [(4, 250, 4), (8, 100, 3)])
def test_external_repartition_smoke(ray_init_shutdown, num_blocks, rows, num_parts):
    """End-to-end: map → reduce, verify total row count preserved."""
    ctx = DataContext.get_current()
    input_bundles = _build_input_bundles(num_blocks, rows)
    expected_total_rows = num_blocks * rows

    # Feed bundles into a stub upstream — no real planner-driven input dep.
    from ray.data._internal.execution.operators.input_data_buffer import (
        InputDataBuffer,
    )

    upstream = InputDataBuffer(ctx, input_bundles)
    # PhysicalOperator.start() requires a BlockRefCounter (executor-wide
    # in the real path; a fresh instance is fine for the hand-driven test).
    block_ref_counter = BlockRefCounter()
    upstream.start(ExecutionOptions(), block_ref_counter)

    map_op = ExternalHashShuffleMapOp(
        upstream,
        ctx,
        num_partitions=num_parts,
        partition_fn=_make_partition_fn(["id"], num_parts),
        pool_budget_bytes=4 * 1024 * 1024,
        fsync_on_close=False,  # don't pay fsync cost on a smoke test
        name="ExternalHashShuffleMap-smoke",
    )
    reduce_op = ExternalHashShuffleReduceOp(
        map_op,
        ctx,
        num_partitions=num_parts,
        reduce_fn=_concat_reduce,
        # default target_max_block_size = None ⇒ partition = block
        name="ExternalHashShuffleReduce-smoke",
    )

    map_op.start(ExecutionOptions(), block_ref_counter)
    reduce_op.start(ExecutionOptions(), block_ref_counter)

    try:
        # Drive map by piping every upstream bundle in.
        while upstream.has_next():
            map_op.add_input(upstream.get_next(), input_index=0)
        map_op.all_inputs_done()

        # Drain map → feed reduce. The map op emits one partition wrapper
        # bundle per partition_id (not per mapper).
        map_output = _drain_op(map_op)
        assert len(map_output) == num_parts, (
            f"expected {num_parts} partition wrappers from map, "
            f"got {len(map_output)}"
        )
        for bundle in map_output:
            reduce_op.add_input(bundle, input_index=0)
        reduce_op.all_inputs_done()

        # Drain reduce + check invariants.
        reduce_output = _drain_op(reduce_op, timeout_s=60.0)
        got_rows = _total_rows(reduce_output)
        assert got_rows == expected_total_rows, (
            f"row count mismatch: got {got_rows}, expected "
            f"{expected_total_rows}"
        )
        assert len({id(bundle) for bundle in reduce_output}) >= 1
    finally:
        reduce_op.shutdown(Timer(), force=True)
        map_op.shutdown(Timer(), force=True)
        upstream.shutdown(Timer(), force=True)


def test_external_cleanup_shuffle_dir(ray_init_shutdown, tmp_path):
    """``_cleanup_shuffle_dir`` ``rmtree``s the given ``base_dir``."""
    from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_runtime import (  # noqa: E501
        _cleanup_shuffle_dir,
    )

    base_dir = str(tmp_path / "shuffle_external_cleanup")
    os.makedirs(base_dir, exist_ok=True)
    with open(os.path.join(base_dir, "map_0.shf"), "w") as f:
        f.write("x" * 1024)
    assert os.path.isdir(base_dir)

    ray.get(_cleanup_shuffle_dir.remote(base_dir))

    assert not os.path.exists(base_dir), (
        f"base_dir {base_dir} still present after _cleanup_shuffle_dir "
        "— cleanup task did not fire"
    )
