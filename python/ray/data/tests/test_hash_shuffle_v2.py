"""Unit + e2e tests for the V2 actorless hash shuffle.

V2 is a two-op physical DAG:
* :class:`ShuffleMapOp` — hash-partitions input blocks into G ZSTD-compressed
  IPC groups, emits one RefBundle per map task containing G blocks.
* :class:`ShuffleReduceOp` — groups bundles by block position, launches one
  reducer task per group with concurrency capped at one per worker node.

Focused on regressions and non-obvious behaviour; pyarrow-internal behaviour
(concat, sort, hash_partition) and trivial Python (deque FIFO) are exercised
by the e2e block below.
"""

from typing import List, Optional
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import (
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


def _make_input_op_mock(num_blocks=None, size_bytes=None):
    """Minimal PhysicalOperator mock that satisfies ShuffleMapOp init."""
    logical_mock = MagicMock(LogicalOperator)
    logical_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=size_bytes,
        exec_stats=None,
        input_files=None,
    )
    logical_mock.estimated_num_outputs.return_value = num_blocks

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_mock]
    op_mock.num_output_splits.return_value = 1
    return op_mock


def _make_bundle(num_blocks: int = 1, size_bytes: int = 100) -> RefBundle:
    meta = BlockMetadata(
        num_rows=10, size_bytes=size_bytes, exec_stats=None, input_files=None
    )
    blocks = [(ray.ObjectRef(bytes([i % 256]) * 28), meta) for i in range(num_blocks)]
    return RefBundle(blocks, schema=None, owns_blocks=False)


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


def _ipc_opts() -> pa.ipc.IpcWriteOptions:
    return pa.ipc.IpcWriteOptions(compression=pa.Codec("zstd"))


def _make_map_op(
    *,
    num_partitions: int = 4,
    pre_map_merge_threshold: int = ShuffleMapOp._DEFAULT_PRE_MAP_MERGE_THRESHOLD,  # noqa: SLF001
) -> ShuffleMapOp:
    ctx = DataContext()
    return ShuffleMapOp(
        _make_input_op_mock(num_blocks=4),
        ctx,
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
    buf = _encode_partition_ipc(table_fn(), _ipc_opts())
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
    _encode_partition_ipc(table, _ipc_opts())
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
# ShuffleReduceOp: concurrency limit
# ===========================================================================


@pytest.mark.parametrize(
    "num_nodes,override,expected_limit",
    [
        (32, None, 32),
        (32, 12, 12),
    ],
)
def test_reduce_concurrency_limit(num_nodes, override, expected_limit):
    op = _make_reduce_op()
    op._num_nodes = num_nodes
    if override is not None:
        op._data_context.set_config("map_reduce_max_concurrent_reducers", override)
    assert op._get_reduce_concurrency_limit() == expected_limit


# ===========================================================================
# ShuffleMapOp: pre-map merge buffer
# ===========================================================================


@pytest.mark.parametrize(
    "threshold,bundles,finalize,expected_submitted_block_counts",
    [
        # Pre-map merge disabled: each input bundle becomes one task,
        # regardless of how many blocks it carries.  (Earlier code looped
        # per-block and shared the same bundle reference across N tasks,
        # which caused double-free + N× inflated input metrics — see
        # shuffle_map_operator._add_input_inner.)
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
        op._add_input_inner(
            _make_bundle(num_blocks=num_blocks, size_bytes=size_bytes),
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
# ShuffleReduceOp: bundle routing by block position
# ===========================================================================


def test_reduce_op_groups_bundles_by_block_position():
    """ShuffleReduceOp gathers bundle.blocks[pid] into _partition_buffers[pid]."""
    op = _make_reduce_op(num_partitions=4)
    refs_a = [ray.ObjectRef(bytes([i]) * 28) for i in range(4)]
    refs_b = [ray.ObjectRef(bytes([i + 100]) * 28) for i in range(4)]
    metas = [
        BlockMetadata(num_rows=10, size_bytes=100, exec_stats=None, input_files=None)
        for _ in range(4)
    ]

    bundle_a = RefBundle(list(zip(refs_a, metas)), schema=None, owns_blocks=False)
    bundle_b = RefBundle(list(zip(refs_b, metas)), schema=None, owns_blocks=False)

    op._add_input_inner(bundle_a, input_index=0)
    op._add_input_inner(bundle_b, input_index=0)

    for pid in range(4):
        assert op._partition_buffers[pid] == [refs_a[pid], refs_b[pid]]


def test_reduce_op_rejects_bundle_with_wrong_block_count():
    """Bundles must have exactly num_partitions blocks (one per partition)."""
    op = _make_reduce_op(num_partitions=4)
    bundle = _make_bundle(num_blocks=3)  # wrong: should be 4
    with pytest.raises(ValueError, match="expected 4 blocks"):
        op._add_input_inner(bundle, input_index=0)


def test_reduce_op_skips_empty_partitions(monkeypatch):
    """Partitions where every mapper produced zero rows should be skipped
    without scheduling a no-op reduce task.  ShuffleMapOp emits one ref per
    partition per bundle (with ``None`` for empty slots), so the skip must
    be driven by the upstream byte snapshot, not by ``_partition_buffers``
    emptiness.
    """
    num_partitions = 4
    op = _make_reduce_op(num_partitions=num_partitions)

    # Populate buffers as ShuffleMapOp would (one ref per partition, even
    # for empty partitions).
    refs = [ray.ObjectRef(bytes([i]) * 28) for i in range(num_partitions)]
    metas = [
        BlockMetadata(num_rows=0, size_bytes=0, exec_stats=None, input_files=None)
        for _ in range(num_partitions)
    ]
    op._add_input_inner(
        RefBundle(list(zip(refs, metas)), schema=None, owns_blocks=False),
        input_index=0,
    )

    # Mark inputs complete and snapshot an "all empty" byte map.
    op._inputs_complete = True
    op._partition_bytes = {}  # all 4 partitions empty
    op._reduce_phase_initialized = True
    op._num_nodes = 1

    # If the skip is working we should never reach _compute_reduce_resources.
    def _should_not_run(_partition_id):  # pragma: no cover
        raise AssertionError("empty partition should have been skipped")

    monkeypatch.setattr(op, "_compute_reduce_resources", _should_not_run)

    op._try_reduce()

    # All partitions should be drained from the pending set and buffers.
    assert op._pending_partition_ids == set()
    assert op._partition_buffers == {}


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
        for block_ref, _ in ref_bundle.blocks:
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
        for block_ref, _ in ref_bundle.blocks:
            block = ray.get(block_ref)
            ids = block["id"].to_pylist()
            assert ids == sorted(ids)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
