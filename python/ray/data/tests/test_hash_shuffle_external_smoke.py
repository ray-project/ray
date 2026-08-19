"""Smoke test for the external-shuffle operator pair
(``ExternalHashShuffleMapOp`` + ``ExternalHashShuffleReduceOp``).

Wires the operators directly — bypassing the Ray Data planner — to verify
the simplest end-to-end story: feed N input blocks, hash-partition into K
partitions, reduce each with ``_concat_reduce``, then check row count and
partition count. Catches wiring bugs (RefBundle shape, sentinel metadata,
callback ordering, ShuffleFileServer lifecycle, empty-partition gating)
that the planner-driven tests in
``test_hash_shuffle_external_repartition.py`` don't isolate.
"""

from typing import List, Optional, Sequence, cast

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionOptions,
    RefBundle,
)
from ray.data._internal.execution.operators.hash_shuffle_v2 import (
    _concat_reduce,
    _make_hash_partition_fn,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_map_operator import (  # noqa: E501
    ExternalHashShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_reduce_operator import (  # noqa: E501
    ExternalHashShuffleReduceOp,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.stats import Timer
from ray.data.block import BlockAccessor
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


def _arrow_ref_bundles(tables: Sequence[pa.Table]) -> List[RefBundle]:
    """One RefBundle per Arrow table (preserves null-typed columns)."""
    bundles = []
    for table in tables:
        bundles.append(
            RefBundle(
                (
                    BlockEntry(
                        # pyrefly: ignore[bad-argument-type]
                        ray.put(table),
                        BlockAccessor.for_block(table).get_metadata(),
                    ),
                ),
                owns_blocks=True,
                schema=table.schema,
            )
        )
    return bundles


def _drive_external_shuffle(
    input_bundles: List[RefBundle],
    *,
    key_columns: List[str],
    num_partitions: int,
    fused_output_map_transformer: Optional[MapTransformer] = None,
):
    """Wire map→reduce, drive to completion, return (map_out, reduce_out, reduce_op).

    Caller must shut down ``reduce_op`` (and its upstream map / InputDataBuffer)
    via the returned reduce op's input chain, or use the try/finally in tests.
    Returns ``(map_output, reduce_output, reduce_op, map_op, upstream)``.
    """
    ctx = DataContext.get_current()
    upstream = InputDataBuffer(ctx, input_bundles)
    block_ref_counter = BlockRefCounter()
    upstream.start(ExecutionOptions(), block_ref_counter)

    map_op = ExternalHashShuffleMapOp(
        upstream,
        ctx,
        num_partitions=num_partitions,
        partition_fn=_make_hash_partition_fn(key_columns, num_partitions),
        name="ExternalHashShuffleMap-smoke",
    )
    reduce_op = ExternalHashShuffleReduceOp(
        map_op,
        ctx,
        num_partitions=num_partitions,
        reduce_fn=_concat_reduce,
        name="ExternalHashShuffleReduce-smoke",
        fused_output_map_transformer=fused_output_map_transformer,
    )

    map_op.start(ExecutionOptions(), block_ref_counter)
    reduce_op.start(ExecutionOptions(), block_ref_counter)

    while upstream.has_next():
        map_op.add_input(upstream.get_next(), input_index=0)
    map_op.all_inputs_done()

    map_output = _run_and_collect(map_op)
    assert len(map_output) == num_partitions, (
        f"expected {num_partitions} partition wrappers from map, "
        f"got {len(map_output)}"
    )
    return map_output, reduce_op, map_op, upstream


def _shutdown_ops(*ops) -> None:
    for op in ops:
        op.shutdown(Timer(), force=True)


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
    input_bundles = make_ref_bundles(
        [list(range(i * rows, (i + 1) * rows)) for i in range(num_blocks)]
    )
    expected_total_rows = num_blocks * rows

    map_output, reduce_op, map_op, upstream = _drive_external_shuffle(
        input_bundles, key_columns=["id"], num_partitions=num_parts
    )
    try:
        for bundle in map_output:
            reduce_op.add_input(bundle, input_index=0)
        reduce_op.all_inputs_done()

        reduce_output = _run_and_collect(reduce_op)
        got_rows = _total_rows(reduce_output)
        assert (
            got_rows == expected_total_rows
        ), f"row count mismatch: got {got_rows}, expected {expected_total_rows}"
        assert len({id(bundle) for bundle in reduce_output}) >= 1
    finally:
        _shutdown_ops(reduce_op, map_op, upstream)


def test_external_more_partitions_than_keys_empty_fast_path(ray_init_shutdown):
    """Few distinct keys into many partitions: empty wrappers skip remote reduce.

    Mirrors v2 ``test_more_partitions_than_keys_emits_empty_blocks`` at the
    operator layer: map still emits N wrappers; empty ones
    (``num_rows==0``) take the reduce empty fast path (no remote task);
    outputs keep schema and total row count.
    """
    # 3 keys → 20 partitions ⇒ many empty wrappers.
    num_parts = 20
    rows = [{"k": i % 3, "v": i} for i in range(60)]
    table = pa.Table.from_pylist(rows)
    map_output, reduce_op, map_op, upstream = _drive_external_shuffle(
        _arrow_ref_bundles([table]),
        key_columns=["k"],
        num_partitions=num_parts,
    )
    try:
        empty_wrappers = [b for b in map_output if (b.num_rows() or 0) == 0]
        non_empty_wrappers = [b for b in map_output if (b.num_rows() or 0) > 0]
        assert len(empty_wrappers) >= 17
        assert len(non_empty_wrappers) <= 3

        for bundle in map_output:
            reduce_op.add_input(bundle, input_index=0)

        # Empty fast path queues output immediately; only non-empty need tasks.
        assert len(reduce_op.get_active_tasks()) == len(non_empty_wrappers)
        assert reduce_op.has_next()

        reduce_op.all_inputs_done()
        reduce_output = _run_and_collect(reduce_op)

        assert _total_rows(reduce_output) == 60
        assert len(reduce_output) == num_parts

        schemas = [b.schema for b in reduce_output if b.schema is not None]
        assert schemas
        assert all(s.equals(schemas[0]) for s in schemas)

        empty_out = [b for b in reduce_output if (b.num_rows() or 0) == 0]
        assert len(empty_out) == len(empty_wrappers)
        for bundle in empty_out:
            block = cast(pa.Table, ray.get(bundle.block_refs[0]))
            assert block.num_rows == 0
            assert block.schema.equals(schemas[0])
    finally:
        _shutdown_ops(reduce_op, map_op, upstream)


def test_external_null_typed_rows_not_gated_as_empty(ray_init_shutdown):
    """Null-typed rows have ``tbl.nbytes == 0`` but must not take empty fast path.

    Gating on ``size_bytes`` would drop them; gating on ``num_rows`` keeps them.
    """
    table = pa.table({"k": pa.nulls(10)})
    assert table.num_rows == 10
    assert table.nbytes == 0

    num_parts = 4
    map_output, reduce_op, map_op, upstream = _drive_external_shuffle(
        _arrow_ref_bundles([table]),
        key_columns=["k"],
        num_partitions=num_parts,
    )
    try:
        # All-null keys land in one partition; that wrapper has rows, nbytes may
        # still be 0 on metadata.
        non_empty = [b for b in map_output if (b.num_rows() or 0) > 0]
        assert len(non_empty) == 1
        assert non_empty[0].num_rows() == 10
        # The bug was gating on bytes: wrapper can report size_bytes==0.
        assert sum((m.size_bytes or 0) for m in non_empty[0].metadata) == 0

        for bundle in map_output:
            reduce_op.add_input(bundle, input_index=0)

        # Non-empty (by rows) must submit a remote reduce task, not fast-path.
        assert len(reduce_op.get_active_tasks()) == 1

        reduce_op.all_inputs_done()
        reduce_output = _run_and_collect(reduce_op)
        assert _total_rows(reduce_output) == 10
        assert len(reduce_output) == num_parts
    finally:
        _shutdown_ops(reduce_op, map_op, upstream)


def test_external_fused_map_runs_on_empty_partitions(ray_init_shutdown):
    """With a fused downstream map, empty partitions must not take the fast path.

    The operator empty fast path is skipped so the fused map still runs (e.g.
    Write). Reduce then hits the ``not sources`` fallback and yields
    ``schema.empty_table()`` into the fused transform.
    """

    def _mark(blocks, ctx):
        # Marker column proves the fused map ran on every reduce stream,
        # including empty partitions (remote task; no driver-side side effects).
        for block in blocks:
            n = block.num_rows
            yield block.append_column(
                "fused",
                pa.array([True] * n, type=pa.bool_()),
            )

    fused = MapTransformer([BlockMapTransformFn(_mark)])

    num_parts = 12
    rows = [{"k": i % 2, "v": i} for i in range(20)]
    table = pa.Table.from_pylist(rows)
    map_output, reduce_op, map_op, upstream = _drive_external_shuffle(
        _arrow_ref_bundles([table]),
        key_columns=["k"],
        num_partitions=num_parts,
        fused_output_map_transformer=fused,
    )
    try:
        empty_wrappers = [b for b in map_output if (b.num_rows() or 0) == 0]
        assert len(empty_wrappers) >= 10

        for bundle in map_output:
            reduce_op.add_input(bundle, input_index=0)

        # Fusion forces every partition — including empty — onto a remote task.
        assert len(reduce_op.get_active_tasks()) == num_parts
        assert not reduce_op.has_next()

        reduce_op.all_inputs_done()
        reduce_output = _run_and_collect(reduce_op)

        assert _total_rows(reduce_output) == 20
        assert len(reduce_output) == num_parts
        empty_out = 0
        for bundle in reduce_output:
            block = cast(pa.Table, ray.get(bundle.block_refs[0]))
            assert "fused" in block.column_names
            if block.num_rows == 0:
                empty_out += 1
        assert empty_out == len(empty_wrappers)
    finally:
        _shutdown_ops(reduce_op, map_op, upstream)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
