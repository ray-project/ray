from typing import List, Optional
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.hash_shuffle_v2 import (
    _concat_reduce,
    _make_hash_partition_fn,
)
from ray.data._internal.execution.operators.shuffle_operators._shuffle_tasks import (
    _encode_partition_ipc,
    _partition_blocks_to_shards,
    _read_partition_ipc,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
    extract_partition_id,
    make_partition_sentinel,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.block import BlockMetadata
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_shard_bundle(ref: ray.ObjectRef, num_rows: int, size_bytes: int) -> RefBundle:
    """Build a single-block bundle representing one mapper's shard for one
    partition — what ShuffleMapOp pushes into a partition staging queue."""
    meta = BlockMetadata(
        num_rows=num_rows, size_bytes=size_bytes, exec_stats=None, input_files=None
    )
    return RefBundle(
        [BlockEntry(ref=ref, metadata=meta)], schema=None, owns_blocks=False
    )


def _make_table(num_rows: int, *, offset: int = 0) -> pa.Table:
    return pa.table(
        {
            "id": list(range(offset, offset + num_rows)),
            "val": [float(i) for i in range(offset, offset + num_rows)],
        }
    )


def _empty_table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "val": pa.array([], type=pa.float64()),
        }
    )


def _make_map_op(
    *,
    num_partitions: int = 4,
    pre_map_merge_threshold: int = ShuffleMapOp._DEFAULT_PRE_MAP_MERGE_THRESHOLD,  # noqa: SLF001
) -> ShuffleMapOp:
    logical_mock = MagicMock(LogicalOperator)
    logical_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None, size_bytes=None, exec_stats=None, input_files=None
    )
    logical_mock.estimated_num_outputs.return_value = 4
    input_op_mock = MagicMock(PhysicalOperator)
    input_op_mock._output_dependencies = []
    input_op_mock._logical_operators = [logical_mock]
    input_op_mock.num_output_splits.return_value = 1
    return ShuffleMapOp(
        input_op_mock,
        DataContext(),
        num_partitions=num_partitions,
        partition_fn=_make_hash_partition_fn(["id"], num_partitions),
        pre_map_merge_threshold=pre_map_merge_threshold,
    )


def _make_reduce_op(
    *,
    num_partitions: int = 4,
    streaming_reduce: bool = True,
    disallow_block_splitting: bool = False,
    map_op: Optional[ShuffleMapOp] = None,
) -> ShuffleReduceOp:
    ctx = DataContext()
    if map_op is None:
        map_op = _make_map_op(num_partitions=num_partitions)
    return ShuffleReduceOp(
        map_op,
        ctx,
        num_partitions=num_partitions,
        reduce_fn=_concat_reduce,
        streaming_reduce=streaming_reduce,
        disallow_block_splitting=disallow_block_splitting,
    )


# ===========================================================================
# Pure helpers
# ===========================================================================


def test_partition_blocks_to_shards_combines_chunked_columns():
    """Regression: hash_partition is sensitive to per-column chunking; the
    operator must defragment via combine_chunks before calling partition_fn
    (a missing call here cut map throughput in half)."""
    chunked_arr = pa.chunked_array([pa.array([1, 2]), pa.array([3, 4])])
    chunked_block = pa.table({"id": chunked_arr, "val": chunked_arr})
    assert chunked_block["id"].num_chunks == 2

    seen_chunk_counts: List[int] = []

    def spy_partition(block: pa.Table):
        seen_chunk_counts.append(max(c.num_chunks for c in block.columns))
        return {0: block}

    _partition_blocks_to_shards((chunked_block,), spy_partition)
    assert seen_chunk_counts == [1]


@pytest.mark.parametrize(
    "table_fn,expected_rows",
    [
        (lambda: _make_table(10), 10),
        (lambda: _make_table(5, offset=100), 5),
        (lambda: _empty_table(), 0),
    ],
)
def test_ipc_encode_decode_roundtrip(table_fn, expected_rows):
    ipc_opts = pa.ipc.IpcWriteOptions(compression=pa.Codec("zstd"))
    buf = _encode_partition_ipc(table_fn(), ipc_opts)
    decoded = _read_partition_ipc(buf)
    if expected_rows == 0:
        assert decoded is None
    else:
        assert decoded is not None
        assert decoded.num_rows == expected_rows


def test_ipc_encode_does_not_mutate_upstream_schema_metadata():
    """Upstream tables that share a schema must remain untouched after encode."""
    original_meta = {b"foo": b"bar"}
    schema = pa.schema(
        [("id", pa.int64()), ("val", pa.float64())], metadata=original_meta
    )
    table = pa.table({"id": [1, 2, 3], "val": [1.0, 2.0, 3.0]}, schema=schema)
    _encode_partition_ipc(table, pa.ipc.IpcWriteOptions(compression=pa.Codec("zstd")))
    assert table.schema.metadata == original_meta


# ===========================================================================
# ShuffleReduceOp: contract flags
# ===========================================================================


@pytest.mark.parametrize(
    "streaming_reduce,disallow_block_splitting,expected_streaming_reduce",
    [
        (True, False, True),
        (True, True, False),
        (False, False, False),
    ],
)
def test_contract_flags(
    streaming_reduce, disallow_block_splitting, expected_streaming_reduce
):
    op = _make_reduce_op(
        streaming_reduce=streaming_reduce,
        disallow_block_splitting=disallow_block_splitting,
    )
    assert op._streaming_reduce is expected_streaming_reduce


# ===========================================================================
# Partition-id sentinel encoded into BlockMetadata.input_files
# ===========================================================================


@pytest.mark.parametrize("partition_id", [0, 1, 7, 199])
def test_partition_sentinel_roundtrip(partition_id):
    """`make_partition_sentinel` and `extract_partition_id` are inverses."""
    files = make_partition_sentinel(partition_id)
    meta = BlockMetadata(
        num_rows=10, size_bytes=100, exec_stats=None, input_files=files
    )
    bundle = RefBundle(
        [BlockEntry(ref=ray.ObjectRef(b"\x00" * 28), metadata=meta)],
        schema=None,
        owns_blocks=False,
    )
    assert extract_partition_id(bundle) == partition_id


def test_extract_partition_id_raises_when_missing():
    meta = BlockMetadata(num_rows=10, size_bytes=100, exec_stats=None, input_files=None)
    bundle = RefBundle(
        [BlockEntry(ref=ray.ObjectRef(b"\x00" * 28), metadata=meta)],
        schema=None,
        owns_blocks=False,
    )
    with pytest.raises(ValueError, match="missing a partition_id sentinel"):
        extract_partition_id(bundle)


# ===========================================================================
# ShuffleMapOp: pre-map merge buffer
# ===========================================================================


@pytest.mark.parametrize(
    "threshold,bundles,finalize,expected_submitted_block_counts",
    [
        # Pre-map merge disabled: each input bundle becomes one task,
        # regardless of how many blocks it carries.
        (0, [(3, 10)], False, [3]),
        (0, [(2, 10), (1, 10), (4, 10)], False, [2, 1, 4]),
        (1000, [(1, 200), (1, 300), (1, 600)], False, [3]),
        (10_000, [(2, 100)], True, [2]),
    ],
)
def test_pre_map_merge_buffer(
    monkeypatch, threshold, bundles, finalize, expected_submitted_block_counts
):
    monkeypatch.setattr(RefBundle, "get_preferred_object_locations", lambda self: {})
    op = _make_map_op(pre_map_merge_threshold=threshold)
    submitted: List[int] = []
    monkeypatch.setattr(
        op,
        "_submit_shuffle_map_task",
        lambda block_refs, _bundles, **kw: submitted.append(len(block_refs)),
    )
    for num_blocks, size_bytes in bundles:
        meta = BlockMetadata(
            num_rows=10, size_bytes=size_bytes, exec_stats=None, input_files=None
        )
        blocks_list = [
            BlockEntry(ref=ray.ObjectRef(bytes([i % 256]) * 28), metadata=meta)
            for i in range(num_blocks)
        ]
        op._add_input_inner(
            RefBundle(blocks_list, schema=None, owns_blocks=False),
            input_index=0,
        )
    if finalize:
        op.all_inputs_done()
    assert submitted == expected_submitted_block_counts


@pytest.mark.parametrize("threshold", [0, 1024])
def test_map_op_short_circuits_empty_bundles(monkeypatch, threshold):
    """Empty input bundles must not reach _shuffle_map_task; passing an empty
    *blocks tuple there hits IndexError on the empty-input fast path."""
    monkeypatch.setattr(RefBundle, "get_preferred_object_locations", lambda self: {})
    op = _make_map_op(pre_map_merge_threshold=threshold)
    submitted: List[int] = []
    monkeypatch.setattr(
        op,
        "_submit_shuffle_map_task",
        lambda block_refs, _bundles, **kw: submitted.append(len(block_refs)),
    )
    empty_bundle = RefBundle([], schema=None, owns_blocks=False)
    op._add_input_inner(empty_bundle, input_index=0)
    op.all_inputs_done()
    assert submitted == []
    # Internal buffers should be empty — no stale bundle/byte entries.
    assert not op._merge_buffer_refs_by_node
    assert not op._merge_buffer_bundles_by_node
    assert not op._merge_buffer_bytes_by_node


# ===========================================================================
# ShuffleMapOp: per-partition staging queues exposed via mixin
# ===========================================================================


def test_map_op_exposes_staging_queues_via_mixin():
    """`_output_queues` must include every per-partition staging queue
    plus the final emit queue, so the pre-barrier shard bytes show up
    in the `Queued blocks (X)` column of the progress bar."""
    num_partitions = 4
    op = _make_map_op(num_partitions=num_partitions)
    qs = op._output_queues
    # N staging + 1 final emit queue.
    assert len(qs) == num_partitions + 1
    assert all(not q.has_next() for q in qs)


def test_map_op_staging_queue_bytes_visible_to_mixin():
    """A shard pushed into a staging queue must immediately appear in
    `internal_output_queue_num_bytes` so the bytes are reflected in the
    `Queued blocks (X)` progress column."""
    op = _make_map_op(num_partitions=4)
    assert op.internal_output_queue_num_bytes() == 0
    op._partition_staging[1].add(
        _make_shard_bundle(ray.ObjectRef(b"\x00" * 28), num_rows=5, size_bytes=500)
    )
    assert op.internal_output_queue_num_bytes() == 500


def test_map_op_emits_partition_bundles_after_barrier():
    """Once all map tasks are done AND all_inputs_done has fired,
    ShuffleMapOp drains each non-empty staging queue into one merged
    bundle (per partition) carrying the partition_id sentinel on the
    first block.  Empty partitions are skipped."""
    num_partitions = 4
    op = _make_map_op(num_partitions=num_partitions, pre_map_merge_threshold=0)

    # Simulate map-task completion: shards land in staging queues.
    ref_a = ray.ObjectRef(b"\x01" * 28)
    ref_b = ray.ObjectRef(b"\x02" * 28)
    op._partition_staging[0].add(_make_shard_bundle(ref_a, num_rows=5, size_bytes=50))
    op._partition_staging[2].add(_make_shard_bundle(ref_a, num_rows=3, size_bytes=30))
    op._partition_staging[2].add(_make_shard_bundle(ref_b, num_rows=4, size_bytes=40))
    op._partition_bytes[0] = 50
    op._partition_bytes[2] = 70
    # Partitions 1 and 3 → no shards, should be skipped.

    op._inputs_complete = True
    op._maybe_emit_partition_bundles()

    assert op._partition_bundles_emitted is True
    bundles: List[RefBundle] = []
    while op._output_queue.has_next():
        bundles.append(op._output_queue.get_next())
    assert len(bundles) == 2  # two non-empty partitions
    assert extract_partition_id(bundles[0]) == 0
    assert extract_partition_id(bundles[1]) == 2
    assert len(bundles[1].block_refs) == 2  # partition 2 received from 2 mappers
    # Staging drained after emit.
    assert all(not q.has_next() for q in op._partition_staging.values())


def test_map_op_emit_is_idempotent():
    """Calling _maybe_emit_partition_bundles twice doesn't re-emit."""
    op = _make_map_op(num_partitions=4)
    op._inputs_complete = True
    op._partition_staging[1].add(
        _make_shard_bundle(ray.ObjectRef(b"\x00" * 28), num_rows=10, size_bytes=100)
    )
    op._partition_bytes[1] = 100

    op._maybe_emit_partition_bundles()
    op._maybe_emit_partition_bundles()
    bundles: List[RefBundle] = []
    while op._output_queue.has_next():
        bundles.append(op._output_queue.get_next())
    assert len(bundles) == 1


def test_map_op_waits_for_barrier_before_emitting():
    """Don't emit while map tasks are still pending OR before
    all_inputs_done has fired."""
    op = _make_map_op(num_partitions=4)
    op._partition_staging[0].add(
        _make_shard_bundle(ray.ObjectRef(b"\x00" * 28), num_rows=10, size_bytes=100)
    )
    op._partition_bytes[0] = 100
    # Tasks still pending → no emit.
    op._shuffle_map_tasks[0] = MagicMock()
    op._inputs_complete = True
    op._maybe_emit_partition_bundles()
    assert not op._output_queue.has_next()
    # No tasks but inputs not complete → no emit.
    op._shuffle_map_tasks.clear()
    op._inputs_complete = False
    op._maybe_emit_partition_bundles()
    assert not op._output_queue.has_next()
    # Both conditions met → emit.
    op._inputs_complete = True
    op._maybe_emit_partition_bundles()
    assert op._output_queue.has_next()


# ===========================================================================
# ShuffleReduceOp: 1-input-bundle → 1-reducer-task contract
# ===========================================================================


def test_reduce_op_submits_one_task_per_partition_bundle(monkeypatch):
    """Every non-empty input bundle becomes exactly one reducer task,
    sized by the per-bundle byte total (memory = 2 × bundle size).

    Each input bundle mirrors what `ShuffleMapOp._maybe_emit_partition_bundles`
    produces: `num_shards` shard refs for one partition, with the partition_id
    sentinel stamped onto the FIRST block's `BlockMetadata.input_files`.
    """
    op = _make_reduce_op(num_partitions=4)

    submitted: List[int] = []
    submitted_options: List[dict] = []

    def _capture_options(**kw):
        submitted_options.append(kw)
        return MagicMock(remote=lambda *a, **kwa: submitted.append(kwa["partition_id"]))

    monkeypatch.setattr(
        "ray.data._internal.execution.operators.shuffle_operators."
        "shuffle_reduce_operator._shuffle_reduce_task.options",
        _capture_options,
    )
    monkeypatch.setattr(
        "ray.data._internal.execution.operators.shuffle_operators."
        "shuffle_reduce_operator.DataOpTask",
        MagicMock(),
    )

    for partition_id in (0, 1, 2, 3):
        blocks = [
            BlockEntry(
                ref=ray.ObjectRef(bytes([(partition_id * 31 + i) % 256]) * 28),
                metadata=BlockMetadata(
                    num_rows=10,
                    size_bytes=100,
                    exec_stats=None,
                    input_files=(
                        make_partition_sentinel(partition_id) if i == 0 else None
                    ),
                ),
            )
            for i in range(3)
        ]
        op._add_input_inner(
            RefBundle(blocks, schema=None, owns_blocks=False),
            input_index=0,
        )

    assert sorted(submitted) == [0, 1, 2, 3]
    assert op._num_reduce_tasks_submitted == 4
    # Each bundle is 3 shards × 100 bytes = 300 bytes;
    # memory ask = 2 × 300 = 600 bytes (per-task peak USS headroom;
    # relies on @ray.remote(max_calls=1) keeping the worker heap clean
    # between tasks); CPUs = 1.0.
    for opt in submitted_options:
        assert opt["num_cpus"] == 1.0
        assert opt["memory"] == 600
        assert opt["scheduling_strategy"] == "SPREAD"


def test_reduce_op_short_circuits_empty_input_bundle(monkeypatch):
    """Defensive: ShuffleMapOp skips empty partitions, but a future
    regression that sent us an empty bundle must not submit a no-op task."""
    op = _make_reduce_op(num_partitions=4)
    submitted: List[int] = []
    monkeypatch.setattr(
        "ray.data._internal.execution.operators.shuffle_operators."
        "shuffle_reduce_operator._shuffle_reduce_task.options",
        lambda **kw: MagicMock(
            remote=lambda *a, **kwa: submitted.append(kwa.get("partition_id"))
        ),
    )
    empty_bundle = RefBundle([], schema=None, owns_blocks=False)
    op._add_input_inner(empty_bundle, input_index=0)
    assert submitted == []
    assert op._num_reduce_tasks_submitted == 0


# ===========================================================================
# ShuffleReduceOp: framework resource declarations
# ===========================================================================


def test_reduce_op_declares_per_task_memory_from_upstream(monkeypatch):
    """Once upstream's per-partition byte snapshot is populated,
    `incremental_resource_usage` declares `memory = 2 × avg` — the
    decompressed accumulator plus a margin for plasma + per-node
    baseline.  Relies on @ray.remote(max_calls=1) on
    _shuffle_reduce_task to keep the worker heap baseline clean."""
    op = _make_reduce_op(num_partitions=4)
    monkeypatch.setattr(
        op.input_dependencies[0],
        "get_partition_bytes",
        lambda: {0: 1024**3, 1: 1024**3, 2: 1024**3, 3: 1024**3},  # 1 GB each
    )
    inc = op.incremental_resource_usage()
    assert inc.cpu == 1.0
    assert inc.memory == 2 * 1024**3


def test_reduce_op_inc_resources_empty_until_upstream_reports(monkeypatch):
    """Before any partition bytes are visible, memory ask is 0 — we
    don't have anything to size against yet."""
    op = _make_reduce_op(num_partitions=4)
    monkeypatch.setattr(
        op.input_dependencies[0],
        "get_partition_bytes",
        lambda: {},
    )
    inc = op.incremental_resource_usage()
    assert inc.cpu == 1.0
    assert inc.memory == 0


def test_reduce_op_does_not_declare_per_task_plasma():
    """Regression: declaring `object_store_memory` per task deadlocks
    the scheduler under upstream plasma pressure, because the
    framework's budget arithmetic treats plasma as additive — it can't
    see that running a reducer FREES input plasma.  We intentionally
    return zero for object_store_memory; plasma usage is tracked via
    `_metrics.on_output_queued` instead.
    """
    op = _make_reduce_op(num_partitions=4)
    inc = op.incremental_resource_usage()
    assert inc.object_store_memory == 0


# ===========================================================================
# End-to-end (real Ray cluster)
# ===========================================================================


@pytest.mark.parametrize("num_partitions", [1, 4, 8])
def test_e2e_repartition_keys_preserves_rows(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
    num_partitions,
):
    """Round-trip: repartition(num_partitions, keys=...) preserves all
    rows and key totals when routed through the V2 two-op DAG."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(1000, override_num_blocks=10)
    out = ds.repartition(num_partitions, keys=["id"])
    assert out.count() == 1000
    assert out.sum("id") == sum(range(1000))


def test_e2e_partition_equals_block_contract(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """V2 must produce exactly num_partitions output blocks (partition =
    block contract).  Guarded by disallow_block_splitting=True +
    streaming_reduce=False."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(1000, override_num_blocks=20)
    out = ds.repartition(8, keys=["id"])
    assert out._logical_plan.initial_num_blocks() == 8


def test_e2e_same_key_lands_in_same_block(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """All rows sharing a key must end up in the same output block."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 25, "v": row["id"]}
    )
    out = ds.repartition(5, keys=["k"])

    seen_keys_per_block = []
    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref, _ in ref_bundle.block_refs:
            block = ray.get(block_ref)
            seen_keys_per_block.append(set(block["k"].to_pylist()))

    all_keys: List[int] = [k for block in seen_keys_per_block for k in block]
    assert len(all_keys) == len(
        set(all_keys)
    ), f"Same key landed in multiple blocks: {seen_keys_per_block}"


def test_e2e_repartition_with_sort_produces_sorted_partitions(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """repartition(sort=True, keys=...) selects the sort reduce; each
    output block should be sorted by the key columns."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(200, override_num_blocks=4)
    out = ds.repartition(4, keys=["id"], sort=True)

    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            block = ray.get(block_ref)
            ids = block["id"].to_pylist()
            assert ids == sorted(ids)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
