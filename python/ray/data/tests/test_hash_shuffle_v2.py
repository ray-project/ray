import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
    make_partition_sentinel,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    _encode_partition_ipc,
    _get_shard_batch,
    _ipc_write_options,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.block import BlockMetadata
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.data.tests.conftest import noop_counter
from ray.data.tests.util import run_op_tasks_sync
from ray.exceptions import GetTimeoutError
from ray.tests.conftest import *  # noqa: F401, F403


def _keys_per_block(ds, columns):
    """Return, for each output block, the set of distinct key tuples it holds.

    Used to assert the hash-shuffle co-location guarantee: a key must appear in
    exactly one block.
    """
    per_block = []
    for ref_bundle in ds.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            block = ray.get(block_ref)
            cols = [block[c].to_pylist() for c in columns]
            per_block.append(set(zip(*cols)))
    return per_block


def _assert_keys_colocated(per_block):
    """Every key tuple appears in at most one block."""
    all_keys = [k for block in per_block for k in block]
    assert len(all_keys) == len(
        set(all_keys)
    ), f"A key landed in more than one block: {per_block}"


@pytest.mark.parametrize("num_partitions", [1, 4, 8])
def test_repartition_keys_preserves_rows(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
    num_partitions,
):
    """No rows are lost or duplicated; key totals are preserved."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(1000, override_num_blocks=10)
    out = ds.repartition(num_partitions, keys=["id"])
    assert out.count() == 1000
    assert out.sum("id") == sum(range(1000))


def test_repartition_block_number_matched(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """All-non-empty partitions => exactly num_partitions output blocks."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    # 1000 distinct keys over 8 buckets => all 8 partitions are non-empty.
    ds = ray.data.range(1000, override_num_blocks=20)
    out = ds.repartition(8, keys=["id"]).materialize()
    assert out.num_blocks() == 8


def test_same_key_lands_in_same_block(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """All rows sharing a key should end up in one block."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 25, "v": row["id"]}
    )
    out = ds.repartition(5, keys=["k"])

    _assert_keys_colocated(_keys_per_block(out, ["k"]))
    assert out.count() == 500


def test_multi_column_keys(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Composite keys hash on all columns: every distinct (a, b) tuple lands in
    exactly one block."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"a": row["id"] % 5, "b": row["id"] % 7, "v": row["id"]}
    )
    out = ds.repartition(4, keys=["a", "b"])

    _assert_keys_colocated(_keys_per_block(out, ["a", "b"]))
    assert out.count() == 500


def test_more_partitions_than_keys_emits_empty_blocks(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Requesting more partitions than there are distinct keys emits the extra
    partitions as empty (0-row) blocks that still carry the dataset schema."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    # 3 distinct keys into 50 partitions => at most 3 non-empty, >=47 empty.
    ds = ray.data.range(600, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 3, "v": row["id"]}
    )
    out = ds.repartition(50, keys=["k"]).materialize()

    assert out.count() == 600
    assert out.num_blocks() == 50

    rows_per_block = []
    schemas = []
    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            block = ray.get(block_ref)
            rows_per_block.append(block.num_rows)
            schemas.append(block.schema)

    assert rows_per_block.count(0) >= 47
    assert all(schema.equals(schemas[0]) for schema in schemas)

    _assert_keys_colocated(_keys_per_block(out, ["k"]))


def test_repartition_empty_dataset(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Empty dataset should still output N blocks"""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(100, override_num_blocks=4).filter(lambda row: False)
    out = ds.repartition(4, keys=["id"]).materialize()
    assert out.count() == 0
    assert out.num_blocks() == 4
    rows_per_block = [
        ray.get(block_ref).num_rows
        for ref_bundle in out.iter_internal_ref_bundles()
        for block_ref in ref_bundle.block_refs
    ]
    assert rows_per_block == [0, 0, 0, 0]


def test_repartition_with_sort_produces_sorted_partitions(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Check that rows are sorted in every partition."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(200, override_num_blocks=4)
    out = ds.repartition(4, keys=["id"], sort=True)

    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            ids = ray.get(block_ref)["id"].to_pylist()
            assert ids == sorted(ids)


def test_get_shard_batch_no_timeout(ray_start_regular_shared_2_cpus):
    """timeout_s <= 0 fetches in a single blocking ray.get."""
    refs = [ray.put(i) for i in range(4)]
    out = _get_shard_batch(
        refs,
        partition_id=0,
        batch_index=0,
        num_batches=1,
        timeout_s=0,
    )
    assert out == [0, 1, 2, 3]


def test_get_shard_batch_returns_ready_values(ray_start_regular_shared_2_cpus):
    """A timeout that is never hit returns the values unchanged."""
    refs = [ray.put(i) for i in range(3)]
    out = _get_shard_batch(
        refs,
        partition_id=1,
        batch_index=0,
        num_batches=1,
        timeout_s=30.0,
    )
    assert out == [0, 1, 2]


def test_get_shard_batch_warns_then_raises_on_stall(
    ray_start_regular_shared_2_cpus, propagate_logs, caplog
):
    """A stalled fetch warns partway through, then raises at the timeout."""

    @ray.remote
    def _never_ready():
        import time

        time.sleep(1000)

    ref = _never_ready.remote()
    with caplog.at_level(
        "WARNING",
        logger="ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks",
    ):
        with pytest.raises(GetTimeoutError):
            _get_shard_batch(
                [ref],
                partition_id=7,
                batch_index=0,
                num_batches=1,
                timeout_s=0.3,
            )
    assert [r.levelname for r in caplog.records].count("WARNING") == 1
    assert [r.levelname for r in caplog.records].count("ERROR") == 1
    assert "partition 7" in caplog.records[0].message
    ray.cancel(ref, force=True)


# --- Multi-input reduce -------------------------------------------------------
# TODO: move these multi-input ShuffleReduceOp tests (and the _get_shard_batch
# shuffle_tasks tests above) into a dedicated operator/task-level test file --
# they aren't specific to hash-shuffle-v2.
def _ipc_shard_bundle(partition_id, table):
    """One partition's shard as a ShuffleMapOp emits it: an IPC-encoded buffer
    stamped with the partition id."""
    from ray.data._internal.execution.interfaces import BlockEntry, RefBundle

    buf = _encode_partition_ipc(table, _ipc_write_options("none"))
    meta = BlockMetadata(
        num_rows=table.num_rows,
        size_bytes=table.nbytes,
        exec_stats=None,
        input_files=make_partition_sentinel(partition_id),
    )
    return RefBundle(
        (
            BlockEntry(
                ref=ray.put(buf),  # pyrefly: ignore[bad-argument-type]
                metadata=meta,
            ),
        ),
        schema=table.schema,
        owns_blocks=True,
    )


def _make_multi_input_reduce_op(reduce_fn, num_inputs=2, num_partitions=2):
    ctx = DataContext.get_current()
    maps = [
        ShuffleMapOp(
            InputDataBuffer(ctx, make_ref_bundles([[0]])),
            ctx,
            num_partitions=num_partitions,
            partition_fn=lambda t: {},
        )
        for _ in range(num_inputs)
    ]
    return ShuffleReduceOp(
        maps,
        ctx,
        num_partitions=num_partitions,
        reduce_fn=reduce_fn,
        disallow_block_splitting=True,
    )


def _drain_reduce_op(op, feed):
    """Run `op` over `feed` (bundle, input_index) pairs and return output tables."""
    op.start(ExecutionOptions(), noop_counter())
    for bundle, input_index in feed:
        op.add_input(bundle, input_index)
    op.all_inputs_done()
    run_op_tasks_sync(op)
    tables = []
    while op.has_next():
        for ref in op.get_next().block_refs:
            tables.append(ray.get(ref))
    return tables


def _concat_inputs_reduce_fn():
    def _reduce(partition_id, tables_by_input):
        tables = [t for shards in tables_by_input for t in shards]
        if tables:
            yield pa.concat_tables(tables)

    return _reduce


def test_reduce_op_combines_all_inputs(ray_start_regular_shared_2_cpus):
    """Both inputs' shards for a partition reach the reducer, in input order."""
    op = _make_multi_input_reduce_op(_concat_inputs_reduce_fn(), num_inputs=2)
    feed = [
        (_ipc_shard_bundle(0, pa.table({"src": ["L"], "v": [1]})), 0),
        (_ipc_shard_bundle(0, pa.table({"src": ["R"], "v": [2]})), 1),
    ]
    out = pa.concat_tables(_drain_reduce_op(op, feed))
    assert sorted(out.column("src").to_pylist()) == ["L", "R"]
    assert sorted(out.column("v").to_pylist()) == [1, 2]


def test_reduce_op_runs_when_an_input_is_missing(ray_start_regular_shared_2_cpus):
    """A partition that never receives one input (a block-less side) is still
    reduced -- the reducer sees an empty shard list for the missing input rather
    than the op hanging on a never-paired partition."""
    op = _make_multi_input_reduce_op(_concat_inputs_reduce_fn(), num_inputs=2)
    # Only input 0 delivers partition 0; input 1 never does.
    feed = [(_ipc_shard_bundle(0, pa.table({"src": ["L"], "v": [1]})), 0)]
    out = pa.concat_tables(_drain_reduce_op(op, feed))
    assert out.column("src").to_pylist() == ["L"]
    assert op.has_completed()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
