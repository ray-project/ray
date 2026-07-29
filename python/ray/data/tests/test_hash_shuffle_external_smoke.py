"""Smoke test for the external-shuffle operator pair
(``ExternalHashShuffleMapOp`` + ``ExternalHashShuffleReduceOp``).

Wires the operators directly — bypassing the Ray Data planner — to verify
the simplest end-to-end story: feed N input blocks, hash-partition into K
partitions, reduce each with ``_concat_reduce``, then check row count and
partition count. Catches wiring bugs (RefBundle shape, sentinel metadata,
callback ordering, ShuffleManager lifecycle) that the planner-driven
tests in ``test_hash_shuffle_external_repartition.py`` don't isolate.
"""

import pytest

import ray
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.operators.hash_shuffle_v2 import (
    _concat_reduce,
    _make_hash_partition_fn,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator_external import (  # noqa: E501
    ExternalHashShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator_external import (  # noqa: E501
    ExternalHashShuffleReduceOp,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.stats import Timer
from ray.data.context import DataContext
from ray.data.tests.util import run_op_tasks_sync


# --- helpers -----------------------------------------------------------------


def _run_and_collect(op) -> list:
    """Pump ``op`` to completion, collect output bundles."""
    run_op_tasks_sync(op)
    bundles = []
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
    input_bundles = make_ref_bundles(
        [list(range(i * rows, (i + 1) * rows)) for i in range(num_blocks)]
    )
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
        partition_fn=_make_hash_partition_fn(["id"], num_parts),
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
        map_output = _run_and_collect(map_op)
        assert len(map_output) == num_parts, (
            f"expected {num_parts} partition wrappers from map, "
            f"got {len(map_output)}"
        )
        for bundle in map_output:
            reduce_op.add_input(bundle, input_index=0)
        reduce_op.all_inputs_done()

        # Drain reduce + check invariants.
        reduce_output = _run_and_collect(reduce_op)
        got_rows = _total_rows(reduce_output)
        assert got_rows == expected_total_rows, (
            f"row count mismatch: got {got_rows}, expected " f"{expected_total_rows}"
        )
        assert len({id(bundle) for bundle in reduce_output}) >= 1
    finally:
        reduce_op.shutdown(Timer(), force=True)
        map_op.shutdown(Timer(), force=True)
        upstream.shutdown(Timer(), force=True)
