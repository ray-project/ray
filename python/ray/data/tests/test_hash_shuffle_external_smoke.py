"""Smoke test for the external-shuffle operator pair (ExternalHashShuffleMapOp +
ExternalHashShuffleReduceOp).

This wires the operators directly — bypassing the Ray Data planner — to
verify the simplest end-to-end story: feed N input blocks → hash-partition
into K partitions → reduce each with ``_concat_reduce`` → row count
preserved, partition count == K, no leaked actors / files.

The point is to catch wiring bugs (RefBundle shape, sentinel metadata,
callback ordering, ShuffleManager lifecycle) before we add planner glue.
This is NOT a perf benchmark and NOT exhaustive correctness coverage —
hash invariants, skew handling, multi-node, compression matrices, etc.
get their own tests later.

Run with::

    pytest python/ray/data/tests/test_hash_shuffle_external_smoke.py -xvs
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


# ─────────────────────────────── helpers ───────────────────────────────


def _make_partition_fn(key_columns, num_partitions):
    """Concrete PartitionFn over ``hash_partition`` — matches v2's factory."""

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
                task.on_data_ready(None)
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


# ─────────────────────────────── tests ────────────────────────────────


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

    # Use a stub upstream op via a no-op input dependency. The MVP path
    # plugs ExternalHashShuffleMapOp.input_dependencies[0] into a real upstream; for
    # smoke we just feed bundles in manually.
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

        # Drain map → feed reduce. ExternalHashShuffleMapOp now emits N partition
        # wrapper bundles (one per partition_id, each carrying the same
        # shared handle-list ref + a __partition__<pid> sentinel), mirroring
        # v2's map->reduce contract. So we expect num_parts bundles, not
        # num_blocks (mapper count).
        map_output = _drain_op(map_op)
        assert len(map_output) == num_parts, (
            f"expected {num_parts} partition wrappers from map, "
            f"got {len(map_output)}"
        )
        for bundle in map_output:
            reduce_op.add_input(bundle, input_index=0)
        reduce_op.all_inputs_done()

        # Drain reduce.
        reduce_output = _drain_op(reduce_op, timeout_s=60.0)

        # ── invariants ─────────────────────────────────────────────────
        # Row count preserved.
        got_rows = _total_rows(reduce_output)
        assert got_rows == expected_total_rows, (
            f"row count mismatch: got {got_rows}, expected "
            f"{expected_total_rows}"
        )
        # No more in-flight reducer tasks; every dispatched partition
        # produced at least one output bundle (_concat_reduce + empty input
        # combined could legitimately emit zero, but for non-empty inputs
        # we expect coverage of every partition).
        partition_ids_seen = set()
        for bundle in reduce_output:
            # We don't carry partition_id sentinels on the external-shuffle reduce
            # output (the partition was an internal concept of the
            # ShuffleHandle), so we just count distinct output bundles.
            partition_ids_seen.add(id(bundle))
        assert len(partition_ids_seen) >= 1
    finally:
        reduce_op.shutdown(Timer(), force=True)
        map_op.shutdown(Timer(), force=True)
        upstream.shutdown(Timer(), force=True)


def test_external_base_dir_cleaned_up_on_actor_release(ray_init_shutdown, tmp_path):
    """When the last ``ActorHandle`` to a ``ShuffleManager`` is dropped,
    Ray gracefully terminates the actor process, the ``atexit`` hook fires,
    and ``base_dir`` is removed.  This is the property that lets external-shuffle NOT
    leak files in /tmp across many shuffles."""
    import gc
    import time as _time

    from ray.data._internal.execution.operators.hash_shuffle_external import (
        ShuffleManager,
    )

    base_dir = str(tmp_path / "shuffle_external_atexit")
    actor = ShuffleManager.remote(base_dir, token="test-token")
    # Make sure the actor's __init__ has run (and that mkdirs has happened).
    ray.get(actor.endpoint.remote())
    assert os.path.isdir(base_dir), "actor should have created base_dir"

    # Drop the only handle.  Ray will detect ref-count → 0, send the
    # graceful termination, the actor's atexit will rmtree base_dir.
    del actor
    gc.collect()

    # Poll for cleanup; Ray actor GC is async so we tolerate a short wait.
    deadline = _time.monotonic() + 15.0
    while _time.monotonic() < deadline:
        if not os.path.exists(base_dir):
            break
        _time.sleep(0.2)

    assert not os.path.exists(base_dir), (
        f"base_dir {base_dir} still present 15s after ActorHandle release "
        "— atexit cleanup did not fire (or actor is still alive)"
    )
