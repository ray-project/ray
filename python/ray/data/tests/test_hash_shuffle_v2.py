import gc
import glob
import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import ray
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionOptions,
    RefBundle,
)
from ray.data._internal.execution.operators.hash_shuffle_v2 import (
    _SHUFFLE_MAP_RUNTIME_ENV,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_reduce_operator import (  # noqa: E501
    DiskHashShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_runtime import (  # noqa: E501
    handle_from_block,
    handle_to_block,
    teardown_disk_shuffle,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_tasks import (  # noqa: E501
    DiskShuffleWriteFn,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
    make_partition_sentinel,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    SHUFFLE_PEAK_MEMORY_MULTIPLIER,
    _encode_partition_ipc,
    _get_shard_batch,
    _ipc_write_options,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.planner.plan_all_to_all_op import (
    plan_disk_hash_shuffle_map,
    plan_disk_hash_shuffle_reduce,
)
from ray.data.aggregate import (
    AbsMax,
    AggregateFnV2,
    AsList,
    Mean,
    Quantile,
    Std,
    Sum,
    TopKUnique,
    ValueCounter,
)
from ray.data.block import BlockAccessor, BlockMetadata
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


@pytest.fixture(autouse=True)
def data_context_shuffle_v2(restore_data_context):
    ctx = restore_data_context
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2


@pytest.fixture(params=[False, True], ids=["object_store", "disk"])
def use_disk(request, data_context_shuffle_v2):
    """Run an API-level test on both shuffle v2 transports."""
    DataContext.get_current().use_disk_based_hash_shuffle = request.param
    return request.param


@pytest.fixture(autouse=True)
def _assert_no_leftover_shuffle_dirs():
    """No ``$TMPDIR/ray_shuffle_disk_*`` dir may outlive a test: the disk
    reduce's shutdown removes them eagerly and waits up to 5s."""
    pattern = os.path.join(tempfile.gettempdir(), "ray_shuffle_disk_*")
    pre_existing = set(glob.glob(pattern))
    yield
    gc.collect()
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        leftover = set(glob.glob(pattern)) - pre_existing
        if not leftover:
            return
        time.sleep(0.1)
    raise AssertionError(f"leftover shuffle dirs after test: {leftover}")


def _num_reduce_tasks(ds) -> int:
    """Number of shuffle reduce tasks a materialized dataset ran: the reduce
    stage's ``num_tasks_submitted``, found by walking the stats tree."""
    stats = ds._raw_stats()
    while "ShuffleReduce" not in stats.base_name:
        (stats,) = stats.parents
    return stats.extra_metrics["num_tasks_submitted"]


def _handle_bundle(handle) -> RefBundle:
    """What the disk-shuffle writer emits per task: one handle block."""
    block = handle_to_block(handle)
    return RefBundle(
        (BlockEntry(ray.put(block), BlockAccessor.for_block(block).get_metadata()),),
        schema=block.schema,
        owns_blocks=True,
    )


@pytest.mark.parametrize(
    "num_rows,num_partitions",
    [(0, 4), (12, 1), (12, 4), (11, 4), (10, 6), (3, 5)],
)
def test_slice_partition(num_rows, num_partitions):
    from ray.data._internal.execution.operators.hash_shuffle_v2 import (
        _make_slice_partition_fn,
    )
    from ray.data.extensions import ArrowTensorArray

    tensors = np.arange(48).reshape(12, 2, 2)
    table = pa.table(
        {
            "id": range(12),
            "tensor": ArrowTensorArray.from_numpy(tensors),
            "null": pa.nulls(12),
        }
    ).slice(0, num_rows)
    # Exercise slicing on chunked inputs as well.
    table = pa.concat_tables([table.slice(0, 5), table.slice(5)])
    partition_fn = _make_slice_partition_fn(num_partitions)
    partitions = partition_fn(table)
    num_slices = min(num_rows, num_partitions)
    assert set(partitions) == set(range(num_slices))
    slice_size, remainder = divmod(num_rows, num_slices) if num_slices else (0, 0)
    slice_ranges = []
    row_start = 0
    for partition_id in range(num_slices):
        current_slice_size = slice_size + (1 if partition_id < remainder else 0)
        slice_ranges.append((row_start, row_start + current_slice_size))
        row_start += current_slice_size
    for partition_id, shard in partitions.items():
        row_start, row_end = slice_ranges[partition_id]
        assert shard.schema == table.schema
        assert shard["id"].to_pylist() == list(range(row_start, row_end))
        assert shard["null"].null_count == shard.num_rows
        np.testing.assert_array_equal(
            shard["tensor"].combine_chunks().to_numpy(),
            tensors[row_start:row_end],
        )


@pytest.mark.parametrize("input_batch_bytes", [0, 1024 * 1024])
@pytest.mark.parametrize(
    "num_rows,num_partitions", [(0, 4), (11, 1), (11, 4), (10, 6), (3, 5)]
)
def test_slice_repartition(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
    input_batch_bytes,
    num_rows,
    num_partitions,
):
    DataContext.get_current().shuffle_input_batch_bytes = input_batch_bytes
    tables = [
        pa.table({"id": list(range(start, start + num_rows))}) for start in (0, 100)
    ]
    out = (
        ray.data.from_arrow(tables)
        .repartition(num_partitions, shuffle=True)
        .materialize()
    )
    blocks = [
        ray.get(block_ref)
        for ref_bundle in out.iter_internal_ref_bundles()
        for block_ref in ref_bundle.block_refs
    ]
    assert len(blocks) == num_partitions
    assert all(block.schema == tables[0].schema for block in blocks)
    rows_by_partition = [block["id"].to_pylist() for block in blocks]
    assert sorted(row for rows in rows_by_partition for row in rows) == (
        list(range(num_rows)) + list(range(100, 100 + num_rows))
    )
    # Each input block's contiguous slices are assigned in partition order.
    num_slices = min(num_rows, num_partitions)
    slice_size, remainder = divmod(num_rows, num_slices) if num_slices else (0, 0)
    expected_shards = []
    row_start = 0
    for partition_id in range(num_slices):
        current_slice_size = slice_size + (1 if partition_id < remainder else 0)
        expected_shards.append(list(range(row_start, row_start + current_slice_size)))
        row_start += current_slice_size
    expected_shards.extend([[] for _ in range(num_partitions - num_slices)])
    expected_shards.sort()
    for start in (0, 100):
        shards = [
            [row - start for row in rows if start <= row < start + num_rows]
            for rows in rows_by_partition
        ]
        assert sorted(shards) == expected_shards
    prefix = "Disk" if use_disk else ""
    assert f"{prefix}SliceShuffleMap(partitions={num_partitions})" in out.stats()
    assert f"{prefix}SliceShuffleReduce(partitions={num_partitions})" in out.stats()


def test_slice_repartition_chunked_extensions(
    ray_start_regular_shared_2_cpus, use_disk
):
    from ray.data.extensions import ArrowPythonObjectArray, ArrowTensorArray

    tensors = np.arange(48).reshape(12, 2, 2)
    table = pa.table(
        {
            "id": range(12),
            "tensor": ArrowTensorArray.from_numpy(tensors),
            "object": ArrowPythonObjectArray.from_objects(
                [{"id": i} for i in range(12)]
            ),
        }
    )
    table = pa.concat_tables([table.slice(0, 5), table.slice(5)])
    assert all(column.num_chunks == 2 for column in table.columns)

    out = ray.data.from_arrow(table).repartition(4, shuffle=True).materialize()
    blocks = [
        ray.get(block_ref)
        for ref_bundle in out.iter_internal_ref_bundles()
        for block_ref in ref_bundle.block_refs
    ]
    assert len(blocks) == 4
    all_ids = []
    for block in blocks:
        assert block.schema == table.schema
        ids = block["id"].to_numpy()
        all_ids.extend(ids.tolist())
        np.testing.assert_array_equal(
            block["tensor"].combine_chunks().to_numpy(), tensors[ids]
        )
        assert block["object"].to_pylist() == [{"id": i} for i in ids]
    assert sorted(all_ids) == list(range(12))


@pytest.mark.parametrize("num_partitions", [1, 4, 8])
def test_repartition_keys_preserves_rows(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
    num_partitions,
):
    """No rows are lost or duplicated; key totals are preserved."""

    ds = ray.data.range(1000, override_num_blocks=10)
    out = ds.repartition(num_partitions, keys=["id"]).materialize()
    assert out.count() == 1000
    assert out.sum("id") == sum(range(1000))


def test_repartition_block_number_matched(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
):
    """All-non-empty partitions => exactly num_partitions output blocks."""

    # 1000 distinct keys over 8 buckets => all 8 partitions are non-empty.
    ds = ray.data.range(1000, override_num_blocks=20)
    out = ds.repartition(8, keys=["id"]).materialize()
    assert out.num_blocks() == 8


def test_same_key_lands_in_same_block(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
):
    """All rows sharing a key should end up in one block."""

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 25, "v": row["id"]}
    )
    out = ds.repartition(5, keys=["k"])

    _assert_keys_colocated(_keys_per_block(out, ["k"]))
    assert out.count() == 500


def test_multi_column_keys(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
):
    """Composite keys hash on all columns: every distinct (a, b) tuple lands in
    exactly one block."""

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"a": row["id"] % 5, "b": row["id"] % 7, "v": row["id"]}
    )
    out = ds.repartition(4, keys=["a", "b"])

    _assert_keys_colocated(_keys_per_block(out, ["a", "b"]))
    assert out.count() == 500


def test_more_partitions_than_keys_emits_empty_blocks(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
):
    """Requesting more partitions than there are distinct keys emits the extra
    partitions as empty (0-row) blocks that still carry the dataset schema."""

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
    # Empty partitions skip the reduce task.
    assert _num_reduce_tasks(out) == 50 - rows_per_block.count(0)

    _assert_keys_colocated(_keys_per_block(out, ["k"]))


def test_fused_map_runs_on_empty_partitions(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
):
    """A map fused into the reduce must run on every partition, including the
    empty ones (think of a Write), so no partition takes the empty fast path:
    one reduce task per partition. ``map_batches`` itself passes empty blocks
    through untouched, so the column is checked on the non-empty blocks."""
    # 3 distinct keys into 20 partitions => at least 17 empty partitions.
    out = (
        ray.data.range(600, override_num_blocks=10)
        .map(lambda row: {"k": row["id"] % 3, "v": row["id"]})
        .repartition(20, keys=["k"])
        .map_batches(
            lambda t: t.append_column("fused", pa.array([True] * t.num_rows)),
            batch_format="pyarrow",
        )
        .materialize()
    )

    assert out.count() == 600
    blocks = [
        ray.get(block_ref)
        for ref_bundle in out.iter_internal_ref_bundles()
        for block_ref in ref_bundle.block_refs
    ]
    assert all("fused" in block.column_names for block in blocks if block.num_rows)
    assert _num_reduce_tasks(out) == 20


@pytest.mark.parametrize("num_partitions", [1, 8])
def test_repartition_preserves_null_typed_rows(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
    num_partitions,
):
    """A null-typed column has rows with ``tbl.nbytes == 0``. Empty-gating by
    bytes would drop the partition; gating by ``num_rows`` must keep them."""
    table = pa.table({"k": pa.nulls(10)})
    assert table.num_rows == 10
    assert table.nbytes == 0

    out = ray.data.from_arrow(table).repartition(num_partitions, keys=["k"])
    out = out.materialize()
    assert out.count() == 10
    assert out.num_blocks() == num_partitions


def test_repartition_empty_dataset(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
):
    """Empty dataset should still output N blocks"""

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
    disable_fallback_to_object_extension,
    use_disk,
):
    """Check that rows are sorted in every partition."""

    ds = ray.data.range(200, override_num_blocks=4)
    out = ds.repartition(4, keys=["id"], sort=True)

    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            ids = ray.get(block_ref)["id"].to_pylist()
            assert ids == sorted(ids)


def test_sort_reduce_uses_higher_multiplier(ray_start_regular_shared_2_cpus, use_disk):
    """Sorted reduces request 3x their input (sort_by materializes a sorted
    copy on top of the concatenated shards); plain concat reduces keep the
    2x default. ``isinstance`` doubles as the flag routing check."""
    reduce_cls = DiskHashShuffleReduceOp if use_disk else ShuffleReduceOp

    sorted_dag = get_execution_plan(
        ray.data.range(10).repartition(2, keys=["id"], sort=True)._logical_plan
    )[0].dag
    assert isinstance(sorted_dag, reduce_cls)
    assert sorted_dag._peak_memory_multiplier == 3
    # The multiplier drives the reduce task's memory request.
    if use_disk:
        sorted_dag.input_dependencies[0]._partition_bytes_by_input[0][0] = 100
    else:
        sorted_dag.input_dependencies[0]._partition_bytes[0] = 100
    assert sorted_dag.incremental_resource_usage().memory == 300

    plain_dag = get_execution_plan(
        ray.data.range(10).repartition(2, keys=["id"])._logical_plan
    )[0].dag
    assert isinstance(plain_dag, reduce_cls)
    assert plain_dag._peak_memory_multiplier == SHUFFLE_PEAK_MEMORY_MULTIPLIER


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


@pytest.mark.parametrize(
    "build_map_op,batch_bytes,expected_num_tasks",
    [
        (ShuffleMapOp, 0, 2),
        (ShuffleMapOp, 10**9, 1),
        (plan_disk_hash_shuffle_map, 0, 2),
        (plan_disk_hash_shuffle_map, 10**9, 2),
    ],
)
def test_shuffle_input_batch_bytes_controls_map_task_batching(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    build_map_op,
    batch_bytes,
    expected_num_tasks,
):
    restore_data_context.shuffle_input_batch_bytes = batch_bytes
    op = build_map_op(
        InputDataBuffer(restore_data_context, []),
        restore_data_context,
        num_partitions=2,
        partition_fn=lambda table: {},
    )
    op.start(ExecutionOptions(), noop_counter())

    for bundle in make_ref_bundles([[0], [1]]):
        op.add_input(bundle, 0)
    op.all_inputs_done()

    try:
        assert len(op.get_active_tasks()) == expected_num_tasks
    finally:
        # Drain the tasks so none outlives the test. With no reduce op in the
        # loop, nothing else removes the disk writer's files.
        run_op_tasks_sync(op)
        if build_map_op is plan_disk_hash_shuffle_map:
            file_servers = set()
            while op.has_next():
                handle = handle_from_block(ray.get(op.get_next().block_refs[0]))
                file_servers.add((handle["shuffle_id"], handle["node_id"]))
            teardown_disk_shuffle(file_servers)


def test_shuffle_map_task_uses_operator_name():
    ctx = DataContext.get_current()
    name = "JoinShuffleMapLeft(keys=('id',), parts=2)"
    op = ShuffleMapOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        partition_fn=lambda table: {},
        name=name,
    )
    op.start(ExecutionOptions(), noop_counter())

    with patch(
        "ray.data._internal.execution.operators.shuffle_operators."
        "shuffle_map_operator._shuffle_map_task"
    ) as shuffle_map_task:
        shuffle_map_task.options.return_value.remote.return_value = [
            MagicMock(),
            MagicMock(),
            MagicMock(),
        ]
        op._submit_shuffle_map_task([], [])

    assert shuffle_map_task.options.call_args.kwargs["name"] == name


def test_shuffle_reduce_task_uses_operator_name():
    ctx = DataContext.get_current()
    map_op = ShuffleMapOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=1,
        partition_fn=lambda table: {},
    )
    name = "JoinShuffleReduce(num_partitions=1)"
    op = ShuffleReduceOp(
        map_op,
        ctx,
        num_partitions=1,
        reduce_fn=lambda partition_id, tables_by_input: iter(()),
        reduce_ray_remote_args={"name": "caller-supplied-name"},
        name=name,
    )
    op.start(ExecutionOptions(), noop_counter())

    with patch(
        "ray.data._internal.execution.operators.shuffle_operators."
        "shuffle_reduce_operator._shuffle_reduce_task"
    ) as shuffle_reduce_task:
        shuffle_reduce_task.options.return_value.remote.return_value = MagicMock()
        op._submit_reduce_task(0, [RefBundle((), schema=None, owns_blocks=True)])

    assert shuffle_reduce_task.options.call_args.kwargs["name"] == name


@pytest.mark.parametrize(
    "map_op_cls,task_path,returns_multiple_refs",
    [
        pytest.param(
            ShuffleMapOp,
            "ray.data._internal.execution.operators.shuffle_operators."
            "shuffle_map_operator._shuffle_map_task",
            True,
            id="object_store",
        ),
        # The disk writer is a plain map op; see
        # test_disk_shuffle_map_is_plain_map_operator.
    ],
)
def test_shuffle_map_task_uses_context_label_selector(
    restore_data_context, map_op_cls, task_path, returns_multiple_refs
):
    restore_data_context.execution_options.label_selector = {
        "subcluster": "train",
        "zone": "us-west",
    }
    op = map_op_cls(
        InputDataBuffer(restore_data_context, []),
        restore_data_context,
        num_partitions=1,
        partition_fn=lambda table: {},
    )
    op.start(ExecutionOptions(), noop_counter())

    with patch(task_path) as shuffle_map_task:
        shuffle_map_task.options.return_value.remote.return_value = (
            [MagicMock(), MagicMock()] if returns_multiple_refs else MagicMock()
        )
        op._submit_shuffle_map_task([], [])

    assert shuffle_map_task.options.call_args.kwargs["label_selector"] == {
        "subcluster": "train",
        "zone": "us-west",
    }


@pytest.mark.parametrize(
    "remote_args, expected_options",
    [
        (
            {
                "num_cpus": 2,
                "num_gpus": 1,
                "resources": {"bar": 1},
                "runtime_env": {"env_vars": {"CUSTOM": "x"}},
            },
            {
                "num_cpus": 2,
                "num_gpus": 1,
                "resources": {"bar": 1},
                "runtime_env": {"env_vars": {"POLARS_MAX_THREADS": "1", "CUSTOM": "x"}},
            },
        ),
        # None means "default" to ray.remote and must not override the operator's ask.
        ({"num_cpus": None, "num_gpus": None, "memory": None}, {"num_cpus": 1}),
    ],
)
def test_disk_shuffle_map_applies_ray_remote_args(
    restore_data_context, remote_args, expected_options
):
    map_op = plan_disk_hash_shuffle_map(
        InputDataBuffer(restore_data_context, []),
        restore_data_context,
        num_partitions=1,
        partition_fn=lambda table: {},
        ray_remote_args=remote_args,
    )

    kwargs = map_op._get_dynamic_ray_remote_args()
    assert {k: kwargs[k] for k in expected_options} == expected_options
    assert ("num_gpus" in kwargs) == ("num_gpus" in expected_options)
    assert "memory" not in kwargs


def test_disk_regroup_orders_handles_by_map_id(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()
    writer = plan_disk_hash_shuffle_map(
        InputDataBuffer(ctx, []), ctx, num_partitions=1, partition_fn=lambda t: {}
    )
    reduce_op = plan_disk_hash_shuffle_reduce(
        writer, ctx, num_partitions=1, reduce_fn=lambda *args: []
    )
    regroup_op = reduce_op.input_dependencies[0]
    regroup_op.start(ExecutionOptions(), BlockRefCounter())

    # Writers finish last-first; the published handles must still be in map
    # task order so every reducer concatenates shards the same way.
    for map_id in (2, 0, 1):
        regroup_op.add_input(
            _handle_bundle({"map_id": map_id, "num_rows": np.array([1])}), 0
        )
    regroup_op.all_inputs_done()

    handle_refs = ray.get(regroup_op.get_next().blocks[0].ref)
    assert [handle_from_block(ray.get(ref))["map_id"] for ref in handle_refs] == [
        0,
        1,
        2,
    ]


def test_disk_shuffle_reduce_op_emits_partitions_in_order(restore_data_context):
    writer = plan_disk_hash_shuffle_map(
        InputDataBuffer(restore_data_context, []),
        restore_data_context,
        num_partitions=2,
        partition_fn=lambda table: {},
    )
    op = plan_disk_hash_shuffle_reduce(
        writer,
        restore_data_context,
        num_partitions=2,
        reduce_fn=lambda partition_id, tables_by_input: iter(()),
        preserve_partition_order=True,
    )
    op.start(ExecutionOptions(), noop_counter())
    with patch(
        "ray.data._internal.execution.operators.shuffle_operators."
        "disk_shuffle_reduce_operator._disk_shuffle_reduce_task"
    ) as reduce_task:
        reduce_task.options.return_value.remote.return_value = MagicMock()
        for partition_id in range(2):
            op._submit_reduce_task(
                partition_id, RefBundle((), schema=None, owns_blocks=True)
            )

    # Partition 1 finishes first; its output must wait behind partition 0's.
    outputs = [RefBundle((), schema=None, owns_blocks=True) for _ in range(2)]
    for partition_id in (1, 0):
        op._handle_reduce_output_ready(partition_id, outputs[partition_id])
        assert op.has_next() == (partition_id == 0)
        op._handle_reduce_done(partition_id, None, None, None, False)

    assert [op.get_next() for _ in range(2)] == outputs
    assert not op.has_next()


def test_disk_shuffle_map_is_plain_map_operator(restore_data_context):
    """The disk-shuffle map stage is a task-pool map feeding the reduce, and
    its tasks pick up the execution options' label selector like any map."""
    from ray.data._internal.execution.operators.task_pool_map_operator import (
        TaskPoolMapOperator,
    )

    restore_data_context.execution_options.label_selector = {
        "subcluster": "train",
        "zone": "us-west",
    }
    map_op = plan_disk_hash_shuffle_map(
        InputDataBuffer(restore_data_context, []),
        restore_data_context,
        num_partitions=1,
        partition_fn=lambda table: {},
    )
    assert isinstance(map_op, TaskPoolMapOperator)
    assert not map_op.isolate_workers
    assert not map_op.supports_fusion()
    remote_args = map_op._get_dynamic_ray_remote_args()
    assert remote_args["label_selector"] == {
        "subcluster": "train",
        "zone": "us-west",
    }
    assert remote_args["runtime_env"] == _SHUFFLE_MAP_RUNTIME_ENV


@pytest.mark.parametrize(
    "map_op_cls,reduce_op_cls,task_path",
    [
        pytest.param(
            ShuffleMapOp,
            ShuffleReduceOp,
            "ray.data._internal.execution.operators.shuffle_operators."
            "shuffle_reduce_operator._shuffle_reduce_task",
            id="object_store",
        ),
        pytest.param(
            plan_disk_hash_shuffle_map,
            plan_disk_hash_shuffle_reduce,
            "ray.data._internal.execution.operators.shuffle_operators."
            "disk_shuffle_reduce_operator._disk_shuffle_reduce_task",
            id="disk",
        ),
    ],
)
def test_shuffle_reduce_task_merges_label_selector(
    restore_data_context, map_op_cls, reduce_op_cls, task_path
):
    restore_data_context.execution_options.label_selector = {
        "subcluster": "train",
        "zone": "us-west",
    }
    map_op = map_op_cls(
        InputDataBuffer(restore_data_context, []),
        restore_data_context,
        num_partitions=1,
        partition_fn=lambda table: {},
    )
    op = reduce_op_cls(
        map_op,
        restore_data_context,
        num_partitions=1,
        reduce_fn=lambda partition_id, tables_by_input: iter(()),
        reduce_ray_remote_args={
            "num_cpus": 2,
            "num_gpus": 1,
            "memory": None,
            "label_selector": {"subcluster": "validation", "node": "worker-1"},
        },
    )
    op.start(ExecutionOptions(), noop_counter())

    bundle = RefBundle((), schema=None, owns_blocks=True)
    with patch(task_path) as shuffle_reduce_task:
        shuffle_reduce_task.options.return_value.remote.return_value = MagicMock()
        if isinstance(op, DiskHashShuffleReduceOp):
            # The regroup hands the disk reduce one bundle per partition.
            op._submit_reduce_task(0, bundle)
        else:
            op._submit_reduce_task(0, [bundle])

    kwargs = shuffle_reduce_task.options.call_args.kwargs
    assert kwargs["label_selector"] == {
        "subcluster": "validation",
        "zone": "us-west",
        "node": "worker-1",
    }
    assert "memory" not in kwargs  # None must not override the estimate
    # Estimates and logical usage must advertise the same ask as the task.
    for usage in (op.incremental_resource_usage(), op.current_logical_usage()):
        assert (usage.cpu, usage.gpu) == (2, 1)


# --- Multi-input reduce -------------------------------------------------------
# TODO: move these multi-input ShuffleReduceOp tests (and the _get_shard_batch
# shuffle_tasks tests above) into a dedicated operator/task-level test file --
# they aren't specific to shuffle-v2.
def test_disk_flag_off_keeps_object_store_path(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    """With the flag off the planner must not build the disk-shuffle stage.
    Row count alone can't tell the two paths apart and the shuffle dir is
    cleaned up eagerly, so make constructing the writer's transform fail."""
    DataContext.get_current().use_disk_based_hash_shuffle = False
    with patch.object(
        DiskShuffleWriteFn,
        "__init__",
        side_effect=AssertionError("disk-shuffle stage built with flag=False"),
    ):
        assert ray.data.range(200).repartition(4, keys=["id"]).count() == 200


def test_disk_regroup_emits_one_bundle_per_partition(
    ray_start_regular_shared_2_cpus,
):
    """The writer emits one handle block per task. The regroup tallies rows
    per partition from them and, once all inputs are done, emits one bundle
    per partition. The reduce emits an empty partition without a task and
    submits one task per remaining bundle as it arrives."""
    ctx = DataContext.get_current()
    writer = plan_disk_hash_shuffle_map(
        InputDataBuffer(ctx, []), ctx, num_partitions=3, partition_fn=lambda t: {}
    )
    reduce_op = plan_disk_hash_shuffle_reduce(
        writer, ctx, num_partitions=3, reduce_fn=lambda *args: []
    )
    regroup_op = reduce_op.input_dependencies[0]
    regroup_op.start(ExecutionOptions(), BlockRefCounter())
    reduce_op.start(ExecutionOptions(), BlockRefCounter())
    schema = pa.schema([("id", pa.int64())])
    for rows in ([5, 0, 3], [2, 0, 0]):
        regroup_op.add_input(
            _handle_bundle({"num_rows": np.array(rows), "schema": schema}), 0
        )
    assert not regroup_op.has_next()

    regroup_op.all_inputs_done()
    bundles = []
    while regroup_op.has_next():
        bundles.append(regroup_op.get_next())
    assert [b.num_rows() for b in bundles] == [7, 0, 3]

    with patch.object(reduce_op, "_submit_reduce_task") as submit:
        for bundle in bundles:
            reduce_op.add_input(bundle, 0)
    # Partition 1 is empty: one typed empty block queued, no task.
    assert [b.num_rows() for b in reduce_op._output_queue] == [0]
    assert [c.args[0] for c in submit.call_args_list] == [0, 2]


def test_disk_shuffle_resource_eligibility_by_phase(ray_start_regular_shared_2_cpus):
    """The regroup is a synchronization barrier, like ``ShuffleMapOp``: during
    the map phase the writer is the only operator eligible for resources, and
    once the regroup completes the reduce and its tail take over. Without the
    barrier, the idle reduce and the Write after it would hold a share of the
    pool for the whole map phase."""
    ctx = DataContext.get_current()
    writer = plan_disk_hash_shuffle_map(
        InputDataBuffer(ctx, []), ctx, num_partitions=2, partition_fn=lambda t: {}
    )
    reduce_op = plan_disk_hash_shuffle_reduce(
        writer, ctx, num_partitions=2, reduce_fn=lambda *args: []
    )
    regroup_op = reduce_op.input_dependencies[0]
    tail = MapOperator.create(MagicMock(), reduce_op, ctx, name="Write")
    topo = build_streaming_topology(tail, ExecutionOptions(), noop_counter())
    allocator = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(),
        ctx,
        BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
    ).op_resource_allocator

    # Map phase.
    assert regroup_op.is_synchronization_barrier
    assert allocator._get_eligible_ops() == [writer]

    # Phase boundary: the writer has nothing more to emit, the regroup has
    # ingested the last handle and handed its bundles to the reduce. Both
    # partitions are empty, so the reduce emits typed empty blocks on the
    # driver and launches no task.
    regroup_op.add_input(
        _handle_bundle(
            {"num_rows": np.array([0, 0]), "schema": pa.schema([("id", pa.int64())])}
        ),
        0,
    )
    writer.mark_execution_finished()
    regroup_op.all_inputs_done()
    while regroup_op.has_next():
        reduce_op.add_input(regroup_op.get_next(), 0)
    assert [b.num_rows() for b in reduce_op._output_queue] == [0, 0]

    # Reduce phase.
    assert allocator._get_eligible_ops() == [reduce_op, tail]


def _ipc_shard_bundle(partition_id, table):
    """One partition's shard as a ShuffleMapOp emits it: an IPC-encoded buffer
    stamped with the partition id."""
    from ray.data._internal.execution.interfaces import BlockEntry

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


def _drain_reduce_op_bundles(op, feed):
    """Run `op` over `feed` (bundle, input_index) pairs and return each output
    bundle's tables."""
    op.start(ExecutionOptions(), noop_counter())
    for bundle, input_index in feed:
        op.add_input(bundle, input_index)
    op.all_inputs_done()
    run_op_tasks_sync(op)
    bundles = []
    while op.has_next():
        bundles.append([ray.get(ref) for ref in op.get_next().block_refs])
    return bundles


def _drain_reduce_op(op, feed):
    """Run `op` over `feed` (bundle, input_index) pairs and return output tables."""
    tables = []
    for bundle_tables in _drain_reduce_op_bundles(op, feed):
        tables.extend(bundle_tables)
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


def _yield_each_shard_reduce_fn(fail_partition=None):
    """Yields one output block per shard, so a partition can yield several."""

    def _reduce(partition_id, tables_by_input):
        yield from tables_by_input[0]
        if partition_id == fail_partition:
            raise ValueError(f"reduce of partition {partition_id} failed")

    return _reduce


def _run_whole_partition_reduce_op(reduce_fn, feed):
    """Runs a one-input reduce op in whole-partition mode over ``feed``. Returns
    a (block count, sorted ``v`` values) pair per output bundle, sorted."""
    op = _make_multi_input_reduce_op(reduce_fn, num_inputs=1)
    op.set_emit_whole_partitions()
    bundles = _drain_reduce_op_bundles(op, [(bundle, 0) for bundle in feed])
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    outputs = []
    for tables in bundles:
        values = pa.concat_tables(tables).column("v").to_pylist()
        outputs.append((len(tables), sorted(values)))
    return sorted(outputs)


def test_reduce_op_emits_whole_partitions(ray_start_regular_shared_2_cpus):
    """Each partition's output blocks come out together as one bundle."""
    shards = [_ipc_shard_bundle(0, pa.table({"v": [v]})) for v in range(3)]
    feed = [
        RefBundle.merge_ref_bundles(shards),
        _ipc_shard_bundle(1, pa.table({"v": [9]})),
    ]
    outputs = _run_whole_partition_reduce_op(_yield_each_shard_reduce_fn(), feed)
    assert outputs == [(1, [9]), (3, [0, 1, 2])]


def test_reduce_op_emits_whole_partition_after_retry(
    ray_start_regular_shared_2_cpus, tmp_path
):
    """A reduce task whose worker dies after yielding a block is retried, and
    its partition still comes out once, as one bundle."""
    crashed = tmp_path / "crashed"

    def _reduce(partition_id, tables_by_input):
        for table in tables_by_input[0]:
            yield table
            if not crashed.exists():
                crashed.touch()
                os._exit(1)

    shards = [_ipc_shard_bundle(0, pa.table({"v": [v]})) for v in range(3)]
    outputs = _run_whole_partition_reduce_op(
        _reduce, [RefBundle.merge_ref_bundles(shards)]
    )
    assert crashed.exists()
    assert outputs == [(3, [0, 1, 2])]


def test_reduce_op_drops_failed_partition(ray_start_regular_shared_2_cpus):
    """Blocks held for a partition whose reduce task fails aren't emitted."""
    op = _make_multi_input_reduce_op(
        _yield_each_shard_reduce_fn(fail_partition=0), num_inputs=1
    )
    op.set_emit_whole_partitions()
    feed = [(_ipc_shard_bundle(0, pa.table({"v": [1]})), 0)]
    with pytest.raises(Exception, match="reduce of partition 0 failed"):
        _drain_reduce_op(op, feed)

    assert not op.has_next()
    assert op.metrics.obj_store_mem_internal_outqueue == 0


_V2_OP_CLASSES = [
    (ShuffleMapOp, ShuffleReduceOp),
    (plan_disk_hash_shuffle_map, plan_disk_hash_shuffle_reduce),
]


def _make_map_op(map_op_cls, upstream_total_rows=None, block_transformer=None):
    ctx = DataContext.get_current()
    upstream = InputDataBuffer(ctx, [])
    upstream.num_output_rows_total = lambda: upstream_total_rows
    return map_op_cls(
        upstream,
        ctx,
        num_partitions=2,
        partition_fn=lambda table: {},
        block_transformer=block_transformer,
    )


@pytest.mark.parametrize("map_op_cls,reduce_op_cls", [(ShuffleMapOp, ShuffleReduceOp)])
def test_reduce_num_output_rows_total_unknown_with_block_transformer(
    map_op_cls, reduce_op_cls
):
    map_op = _make_map_op(
        map_op_cls, upstream_total_rows=100, block_transformer=lambda table: table
    )
    reduce_op = reduce_op_cls(
        map_op, map_op.data_context, num_partitions=2, reduce_fn=lambda *args: []
    )

    assert reduce_op.num_output_rows_total() is None


@pytest.mark.parametrize("map_op_cls,reduce_op_cls", _V2_OP_CLASSES)
def test_reduce_num_output_rows_total_borrows_map_total(map_op_cls, reduce_op_cls):
    map_op = _make_map_op(map_op_cls, upstream_total_rows=100)
    reduce_op = reduce_op_cls(
        map_op, map_op.data_context, num_partitions=2, reduce_fn=lambda *args: []
    )

    assert reduce_op.num_output_rows_total() == 100


@pytest.mark.parametrize("map_op_cls,reduce_op_cls", _V2_OP_CLASSES)
def test_reduce_num_output_rows_total_unknown_when_row_count_not_preserved(
    map_op_cls, reduce_op_cls
):
    map_op = _make_map_op(map_op_cls, upstream_total_rows=100)
    reduce_op = reduce_op_cls(
        map_op,
        map_op.data_context,
        num_partitions=2,
        reduce_fn=lambda *args: [],
        preserves_row_count=False,
    )

    assert reduce_op.num_output_rows_total() is None


def test_reduce_op_none_target_emits_blocks_as_is(ray_start_regular_shared_2_cpus):
    """With block splitting disallowed (target_max_block_size=None), the reduce
    task must emit reduce_fn's blocks as-is instead of coalescing them into a
    single block."""

    def _one_row_blocks_reduce(partition_id, tables_by_input):
        tables = [t for shards in tables_by_input for t in shards]
        combined = pa.concat_tables(tables)
        for i in range(combined.num_rows):
            yield combined.slice(i, 1)

    op = _make_multi_input_reduce_op(_one_row_blocks_reduce, num_inputs=2)
    feed = [
        (_ipc_shard_bundle(0, pa.table({"v": [1, 2]})), 0),
        (_ipc_shard_bundle(0, pa.table({"v": [3, 4]})), 1),
    ]
    tables = _drain_reduce_op(op, feed)
    assert [t.num_rows for t in tables] == [1, 1, 1, 1]
    assert sorted(v for t in tables for v in t.column("v").to_pylist()) == [1, 2, 3, 4]


def test_shuffle_ops_report_per_task_exec_stats(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    use_disk,
):
    """Shuffle map and reduce tasks build their exec stats worker-side, where
    the task index isn't known; the operators are responsible for stamping it.
    Without the stamp (or with the old zero-width reduce spans), shuffle
    operators silently vanish from per-task stats consumers."""

    ds = ray.data.range(1000, override_num_blocks=4).groupby("id").count().materialize()

    stats = (
        ds._current_executor.get_stats() if ds._current_executor else ds._raw_stats()
    )
    block_stats_by_op = {}

    def visit(s):
        for name, block_stats in s.metadata.items():
            block_stats_by_op.setdefault(name, []).extend(block_stats)
        for parent in s.parents:
            visit(parent)

    visit(stats)

    for phase in ("HashAggregateMap", "HashAggregateReduce"):
        blocks = [
            bs for name, bss in block_stats_by_op.items() if phase in name for bs in bss
        ]
        assert blocks, (phase, sorted(block_stats_by_op))
        for bs in blocks:
            assert bs.exec_stats is not None, phase
            assert bs.exec_stats.task_idx is not None, phase
        assert (
            sum(bs.exec_stats.end_time_s - bs.exec_stats.start_time_s for bs in blocks)
            > 0
        ), phase


# --- Adaptive map-side partial aggregation -----------------------


def _partial_agg_test_aggs():
    from ray.data.aggregate import (
        Count,
        Max,
        Mean,
        Min,
        MissingValuePercentage,
        Sum,
        ZeroPercentage,
    )

    return (
        Sum("v"),
        Sum("v", ignore_nulls=False, alias_name="sum_v_nulls"),
        Count(),
        Count("v", ignore_nulls=True, alias_name="count_v_valid"),
        Count("v", alias_name="count_v_all"),
        Mean("f"),
        Mean("f", ignore_nulls=False, alias_name="mean_f_nulls"),
        Min("v"),
        Max("f"),
        Max("b"),
        ZeroPercentage("v"),
        ZeroPercentage("v", ignore_nulls=False, alias_name="zero_v_all"),
        MissingValuePercentage("f"),
    )


def _partial_agg_test_table(num_rows: int, num_keys: int) -> pa.Table:
    rng = np.random.default_rng(42)
    v = rng.integers(-3, 4, num_rows).astype(np.int32)
    f = rng.normal(size=num_rows)
    f[::7] = np.nan
    return pa.table(
        {
            "k": pa.array(np.arange(num_rows) % num_keys),
            # int32 values exercise casting raw state to the int64 partial sums.
            "v": pa.array(v, mask=(np.arange(num_rows) % 11 == 0)),
            "f": pa.array(f, mask=(np.arange(num_rows) % 13 == 0)),
            "b": pa.array(v > 0),
            "unused": pa.array(np.zeros(num_rows)),
        }
    )


def _make_partial_agg_transformer(mode, sample_rows=100, threshold=0.8):
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggPolicy,
        _make_aggregating_transformer,
    )

    return _make_aggregating_transformer(
        ("k",),
        _partial_agg_test_aggs(),
        policy=PartialAggPolicy(
            mode=mode, sample_rows=sample_rows, unique_ratio_threshold=threshold
        ),
    )


def _is_partial(table: pa.Table) -> bool:
    return any(name.startswith("__agg") for name in table.schema.names)


def test_partial_agg_estimate_skips_high_cardinality():
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
    )

    table = _partial_agg_test_table(num_rows=1000, num_keys=1000)
    out = _make_partial_agg_transformer(PartialAggMode.ESTIMATE)(table)

    assert not _is_partial(out)
    # Pruned to keys + the columns the aggregations read.
    assert out.schema.names == ["k", "v", "f", "b"]
    assert out.num_rows == 1000


def test_partial_agg_estimate_keeps_low_cardinality():
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
    )

    table = _partial_agg_test_table(num_rows=1000, num_keys=10)
    out = _make_partial_agg_transformer(PartialAggMode.ESTIMATE)(table)

    assert _is_partial(out)
    assert out.num_rows == 10


def test_partial_agg_estimate_skips_sampling_small_blocks():
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
    )

    table = _partial_agg_test_table(num_rows=50, num_keys=50)
    out = _make_partial_agg_transformer(PartialAggMode.ESTIMATE, sample_rows=100)(table)

    assert _is_partial(out)


def test_partial_agg_estimate_samples_whole_block():
    """Unique keys up front, then a few repeated keys: the first rows alone
    look unique, but the block as a whole collapses well, so it aggregates."""
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
    )

    head = _partial_agg_test_table(num_rows=100, num_keys=100)
    tail = _partial_agg_test_table(num_rows=5000, num_keys=10)
    table = pa.concat_tables([head, tail])
    out = _make_partial_agg_transformer(PartialAggMode.ESTIMATE, sample_rows=100)(table)

    assert _is_partial(out)


@pytest.mark.parametrize(
    "rows_per_key,expected",
    [(1, 100_000), (2, 50_000), (5, 20_000), (20_000, 5)],
)
def test_estimate_distinct_keys(rows_per_key, expected):
    """A sample of a block with a few rows per key sees most keys once; the
    estimate should still recover the block's distinct count."""
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        _estimate_distinct_keys,
    )

    table = pa.table({"k": np.arange(100_000) // rows_per_key})
    estimate = _estimate_distinct_keys(table, ["k"], sample_rows=5_000)

    assert estimate == pytest.approx(expected, rel=0.1)


@pytest.mark.parametrize("heavy_keys", [1, 10, 100])
def test_estimate_distinct_keys_unique_plus_heavy_keys(heavy_keys):
    """90% unique keys plus 10% of rows over a few heavy keys (e.g. null):
    the heavy keys must not drag the estimate for the unique ones down. Each
    heavy key holds at least 200 rows, so it shows up several times in the
    sample."""
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        _estimate_distinct_keys,
    )

    num_rows = 200_000
    keys = np.arange(num_rows)
    heavy = np.random.default_rng(0).random(num_rows) < 0.1
    keys[heavy] = -1 - (np.arange(heavy.sum()) % heavy_keys)
    table = pa.table({"k": keys})
    expected = len(np.unique(keys))

    estimate = _estimate_distinct_keys(table, ["k"], sample_rows=10_000)

    assert estimate == pytest.approx(expected, rel=0.1)


def test_estimate_distinct_keys_key_named_count_all():
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        _estimate_distinct_keys,
    )

    table = pa.table({"count_all": pa.array([str(i) for i in range(1_000)])})

    estimate = _estimate_distinct_keys(table, ["count_all"], sample_rows=100)

    assert estimate == pytest.approx(1_000, rel=0.1)


@pytest.mark.parametrize("mode,expect_partial", [("always", True), ("never", False)])
def test_partial_agg_forced_modes(mode, expect_partial):
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
    )

    table = _partial_agg_test_table(num_rows=1000, num_keys=10)
    out = _make_partial_agg_transformer(PartialAggMode(mode))(table)

    assert _is_partial(out) == expect_partial


def test_partial_agg_never_skips_without_raw_to_partial():
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
    )
    from ray.data.aggregate import Sum

    original = Sum._arrow_agg_spec

    def _spec_without_raw_to_partial(self):
        return original(self)._replace(raw_to_partial=None)

    table = _partial_agg_test_table(num_rows=1000, num_keys=1000)
    with patch.object(Sum, "_arrow_agg_spec", _spec_without_raw_to_partial):
        out = _make_partial_agg_transformer(PartialAggMode.NEVER)(table)

    assert _is_partial(out)


def _reduce_shards_table(shards) -> pa.Table:
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        _make_aggregating_reduce_fn,
    )

    reduce_fn = _make_aggregating_reduce_fn(("k",), _partial_agg_test_aggs())
    (out,) = list(reduce_fn(0, [shards]))
    return out


def _reduce_shards(shards):
    out = _reduce_shards_table(shards)
    return out.to_pandas().sort_values("k").reset_index(drop=True)


@pytest.mark.parametrize(
    "layout",
    [
        ("never", "never", "never"),
        ("always", "never", "never"),
        ("never", "always", "never"),
        ("always", "always", "never"),
    ],
)
def test_partial_agg_reduce_merges_raw_and_partial_shards(layout):
    """Any mix of raw and pre-aggregated shards reduces to the same result as
    the classic all-partial plan."""
    import pandas as pd

    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
    )

    table = _partial_agg_test_table(num_rows=900, num_keys=40)
    chunks = [table.slice(i * 300, 300) for i in range(3)]

    def shards_for(modes):
        return [
            _make_partial_agg_transformer(PartialAggMode(mode))(chunk)
            for mode, chunk in zip(modes, chunks)
        ]

    expected = _reduce_shards(shards_for(("always", "always", "always")))
    actual = _reduce_shards(shards_for(layout))

    pd.testing.assert_frame_equal(actual, expected, check_exact=False)


def test_partial_agg_reduce_mixed_shards_dictionary_keys():
    """Raw and pre-aggregated shards with dictionary-encoded keys still
    concatenate and group together in the reducer."""
    import pandas as pd

    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
    )

    table = _partial_agg_test_table(num_rows=900, num_keys=40)
    keys = pc.cast(table["k"], pa.string()).combine_chunks().dictionary_encode()
    table = table.set_column(0, "k", keys)
    chunks = [table.slice(i * 300, 300) for i in range(3)]

    def reduce_layout(modes):
        shards = [
            _make_partial_agg_transformer(PartialAggMode(mode))(chunk)
            for mode, chunk in zip(modes, chunks)
        ]
        out = _reduce_shards_table(shards)
        assert pa.types.is_dictionary(out.schema.field("k").type)
        df = out.to_pandas()
        df["k"] = df["k"].astype(str)
        return df.sort_values("k").reset_index(drop=True)

    expected = reduce_layout(("always", "always", "always"))
    actual = reduce_layout(("always", "never", "never"))
    pd.testing.assert_frame_equal(actual, expected, check_exact=False)


def test_choose_partial_agg_policy(restore_data_context):
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
    )
    from ray.data._internal.planner.plan_all_to_all_op import (
        _choose_partial_agg_policy,
    )

    ctx = restore_data_context

    # Estimating is the default.
    assert ctx.partial_aggregation_mode == "estimate"
    ctx.adaptive_partial_aggregation_sample_rows = 123
    ctx.adaptive_partial_aggregation_unique_ratio_threshold = 0.5
    policy = _choose_partial_agg_policy(ctx)
    assert policy.mode == PartialAggMode.ESTIMATE
    assert policy.sample_rows == 123
    assert policy.unique_ratio_threshold == 0.5

    ctx.partial_aggregation_mode = "never"
    assert _choose_partial_agg_policy(ctx).mode == PartialAggMode.NEVER

    ctx.partial_aggregation_mode = "always"
    assert _choose_partial_agg_policy(ctx).mode == PartialAggMode.ALWAYS

    ctx.partial_aggregation_mode = "bogus"
    with pytest.raises(ValueError):
        _choose_partial_agg_policy(ctx)


@pytest.mark.parametrize("use_disk", [False, True], ids=["object_store", "disk"])
@pytest.mark.parametrize("mode", ["estimate", "always", "never"])
def test_adaptive_partial_agg_e2e(
    ray_start_regular_shared_2_cpus, restore_data_context, use_disk, mode
):
    """One map task sees unique keys (skips), the other few keys (aggregates);
    the result matches pandas either way."""
    import pandas as pd

    from ray.data.aggregate import Count, Max, Mean, Min, Sum

    ctx = restore_data_context
    ctx.use_disk_based_hash_shuffle = use_disk
    ctx.shuffle_input_batch_bytes = 0  # one map task per input block
    ctx.partial_aggregation_mode = mode
    ctx.adaptive_partial_aggregation_sample_rows = 10

    high = pd.DataFrame({"k": np.arange(200), "v": np.arange(200) % 7})
    low = pd.DataFrame({"k": np.arange(200) % 5, "v": np.arange(200) % 3})
    ds = ray.data.from_pandas([high, low])

    actual = (
        ds.groupby("k", num_partitions=4)
        .aggregate(Sum("v"), Count(), Mean("v"), Min("v"), Max("v"))
        .to_pandas()
        .sort_values("k")
        .reset_index(drop=True)
    )

    df = pd.concat([high, low])
    expected = (
        df.groupby("k")["v"]
        .agg(["sum", "count", "mean", "min", "max"])
        .reset_index()
        .rename(
            columns={
                "sum": "sum(v)",
                "count": "count()",
                "mean": "mean(v)",
                "min": "min(v)",
                "max": "max(v)",
            }
        )
    )
    pd.testing.assert_frame_equal(
        actual, expected, check_exact=False, check_dtype=False
    )


@pytest.mark.parametrize("use_disk", [False, True], ids=["object_store", "disk"])
@pytest.mark.parametrize("mode", ["estimate", "always", "never"])
def test_adaptive_partial_agg_e2e_more_aggs(
    ray_start_regular_shared_2_cpus, restore_data_context, use_disk, mode
):
    """Std, AbsMax, Quantile and a Python aggregation with ``raw_to_partial``,
    with one map task skipping and one aggregating, match pandas."""
    import pandas as pd

    ctx = restore_data_context
    ctx.use_disk_based_hash_shuffle = use_disk
    ctx.shuffle_input_batch_bytes = 0  # one map task per input block
    ctx.partial_aggregation_mode = mode
    ctx.adaptive_partial_aggregation_sample_rows = 10

    # Defined here so workers unpickle it by value (the test module isn't
    # importable on them).
    class SumOfSquares(AggregateFnV2):
        def __init__(self, on: str):
            super().__init__(f"sum_sq({on})", lambda: 0, on=on, ignore_nulls=True)

        def aggregate_block(self, block):
            column = BlockAccessor.for_block(block).to_arrow()[self._target_col_name]
            return pc.sum(pc.multiply(column, column)).as_py()

        def combine(self, current_accumulator, new):
            return current_accumulator + new

        def raw_to_partial(self, block):
            column = block[self._target_col_name]
            return pc.multiply(column, column)

    high = pd.DataFrame({"k": np.arange(200), "v": (np.arange(200) % 7) - 3})
    low = pd.DataFrame({"k": np.arange(200) % 5, "v": (np.arange(200) % 3) - 1})
    actual = (
        ray.data.from_pandas([high, low])
        .groupby("k", num_partitions=4)
        .aggregate(
            Std("v"),
            AbsMax("v"),
            Quantile("v", q=0.25),
            SumOfSquares("v"),
        )
        .to_pandas()
        .sort_values("k")
        .reset_index(drop=True)
    )

    grouped = pd.concat([high, low]).groupby("k")["v"]
    expected = pd.DataFrame(
        {
            "k": sorted(grouped.groups),
            "std(v)": grouped.std().values,
            "abs_max(v)": grouped.apply(lambda s: s.abs().max()).values,
            "quantile(v)": grouped.quantile(0.25).values,
            "sum_sq(v)": grouped.apply(lambda s: (s * s).sum()).values,
        }
    )
    pd.testing.assert_frame_equal(
        actual, expected, check_exact=False, check_dtype=False
    )


# --- Aggregations vectorized with list / value-count / Std state -------------


def _vectorized_vs_python_table(kind: str, seed: int, num_keys: int) -> pa.Table:
    rng = np.random.default_rng(seed)
    n = 300
    null = rng.random(n) < 0.1
    if kind == "float":
        values = pa.array(rng.normal(size=n) * 100 + 1e6, mask=null)
    elif kind == "int32":
        values = pa.array(rng.integers(-5, 6, n).astype(np.int32), mask=null)
    elif kind == "bool":
        values = pa.array(rng.random(n) < 0.3, mask=null)
    elif kind == "string":
        values = pa.array([f"s{x}" for x in rng.integers(0, 6, n)], mask=null)
    else:
        assert kind == "dictionary"
        values = pa.array([f"s{x}" for x in rng.integers(0, 6, n)]).dictionary_encode()
    keys = pa.array(rng.integers(0, num_keys, n), mask=rng.random(n) < 0.05)
    return pa.table({"k": keys, "v": values})


def _aggregate_shards(aggs, tables, modes):
    """Map each table with the given partial-agg mode, then reduce them all;
    ``modes=None`` runs the Python engine instead."""
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
        PartialAggPolicy,
        _fallback_aggregating_reduce_fn,
        _fallback_aggregating_transformer,
        _make_aggregating_reduce_fn,
        _make_aggregating_transformer,
    )

    if modes is None:
        transform = _fallback_aggregating_transformer(("k",), aggs)
        reduce_fn = _fallback_aggregating_reduce_fn(("k",), aggs)
        shards = [transform(t) for t in tables]
    else:
        reduce_fn = _make_aggregating_reduce_fn(("k",), aggs)
        shards = [
            _make_aggregating_transformer(
                ("k",), aggs, policy=PartialAggPolicy(mode=PartialAggMode(mode))
            )(t)
            for t, mode in zip(tables, modes)
        ]
    return pa.concat_tables(list(reduce_fn(0, [shards])))


def _comparable(value, sort_lists: bool):
    if isinstance(value, float):
        return None if np.isnan(value) else pytest.approx(value, rel=1e-9)
    if isinstance(value, dict):  # ValueCounter: order of values is unspecified
        return sorted(zip(map(repr, value["values"]), value["counts"]))
    if isinstance(value, list) and sort_lists:  # AsList: order is unspecified
        return sorted(map(repr, value))
    return value


def _rows_by_key(table: pa.Table, sort_lists: bool = False) -> dict:
    return {
        repr(row.pop("k")): {
            name: _comparable(value, sort_lists) for name, value in row.items()
        }
        for row in table.to_pylist()
    }


_VECTORIZED_CASES = {
    "float": lambda ignore_nulls: (
        Std("v", ignore_nulls=ignore_nulls),
        Std("v", ddof=0, ignore_nulls=ignore_nulls, alias_name="std0"),
        AbsMax("v", ignore_nulls=ignore_nulls),
        Quantile("v", q=0.3, ignore_nulls=ignore_nulls),
        Mean("v", ignore_nulls=ignore_nulls),
    ),
    "int32": lambda ignore_nulls: (
        Std("v", ignore_nulls=ignore_nulls),
        AbsMax("v", ignore_nulls=ignore_nulls),
        Quantile("v", q=0.75, ignore_nulls=ignore_nulls),
        TopKUnique("v", k=3, ignore_nulls=ignore_nulls),
        ValueCounter("v"),
        Sum("v", ignore_nulls=ignore_nulls),
    ),
    "bool": lambda ignore_nulls: (Std("v", ddof=0, ignore_nulls=ignore_nulls),),
    "string": lambda ignore_nulls: (
        TopKUnique("v", k=2, ignore_nulls=ignore_nulls),
        ValueCounter("v"),
    ),
    "dictionary": lambda ignore_nulls: (
        TopKUnique("v", k=2, ignore_nulls=ignore_nulls),
        ValueCounter("v"),
    ),
}


@pytest.mark.parametrize("kind", list(_VECTORIZED_CASES))
@pytest.mark.parametrize("ignore_nulls", [True, False])
@pytest.mark.parametrize("num_keys", [3, 300])
@pytest.mark.parametrize(
    "modes",
    [("always",) * 3, ("never",) * 3, ("always", "never", "never")],
)
def test_vectorized_aggs_match_python_engine(kind, ignore_nulls, num_keys, modes):
    """Std, AbsMax, Quantile, TopKUnique and ValueCounter give the Python
    engine's results for any mix of pre-aggregated and raw shards."""
    tables = [_vectorized_vs_python_table(kind, seed, num_keys) for seed in range(3)]
    aggs = _VECTORIZED_CASES[kind](ignore_nulls)
    expected = _rows_by_key(_aggregate_shards(aggs, tables, modes=None))
    assert _rows_by_key(_aggregate_shards(aggs, tables, modes)) == expected


@pytest.mark.parametrize("kind", ["int32", "string"])
@pytest.mark.parametrize("ignore_nulls", [True, False])
@pytest.mark.parametrize(
    "modes",
    [("always",) * 3, ("never",) * 3, ("always", "never", "never")],
)
def test_vectorized_as_list_matches_python_engine(kind, ignore_nulls, modes):
    tables = [_vectorized_vs_python_table(kind, seed, 20) for seed in range(3)]
    aggs = (AsList("v", ignore_nulls=ignore_nulls),)
    expected = _rows_by_key(_aggregate_shards(aggs, tables, None), sort_lists=True)
    actual = _rows_by_key(_aggregate_shards(aggs, tables, modes), sort_lists=True)
    assert actual == expected


def test_vectorized_aggs_nan_semantics():
    """NaN is a value: it makes Std NaN, as with Mean; Quantile skips it;
    TopKUnique counts it as null; ValueCounter counts it on its own. AbsMax
    skips it unless a group is all NaN, as with Max."""
    from ray.data.aggregate import Max

    nan = float("nan")
    table = pa.table({"k": [0, 0, 0, 0, 1], "v": [1.0, nan, -3.0, None, nan]})
    aggs = (
        AbsMax("v"),
        Max("v"),
        Std("v"),
        Mean("v"),
        Quantile("v"),
        TopKUnique("v", k=3),
        ValueCounter("v"),
    )
    for modes in [("always",), ("never",)]:
        row, all_nan = sorted(
            _aggregate_shards(aggs, [table], modes).to_pylist(), key=lambda r: r["k"]
        )
        assert row["abs_max(v)"] == 3.0 and row["max(v)"] == 1.0
        assert np.isnan(all_nan["abs_max(v)"]) and np.isnan(all_nan["max(v)"])
        assert np.isnan(row["std(v)"]) and np.isnan(row["mean(v)"])
        assert row["quantile(v)"] == -1.0
        assert row["topk_unique(v)"] == [None, -3.0, 1.0]
        counts = row["value_counter(v)"]
        assert sorted(zip(map(repr, counts["values"]), counts["counts"])) == [
            ("-3.0", 1),
            ("1.0", 1),
            ("None", 1),
            ("nan", 1),
        ]


def test_vectorized_std_is_numerically_stable():
    """Chan's merge keeps precision when the mean dwarfs the spread, where
    sum / sum-of-squares would cancel catastrophically."""
    rng = np.random.default_rng(0)
    values = 1e9 + rng.normal(size=3000)
    tables = [
        pa.table({"k": np.zeros(1000, dtype=np.int64), "v": values[i::3]})
        for i in range(3)
    ]
    for modes in [("always",) * 3, ("always", "never", "never")]:
        (row,) = _aggregate_shards((Std("v"),), tables, modes).to_pylist()
        assert row["std(v)"] == pytest.approx(np.std(values, ddof=1), rel=1e-6)


def test_unsupported_value_types_use_python_engine():
    """Arrow can't group by list values, so TopKUnique over a list column
    ships raw rows and the reducer runs the Python engine."""
    table = pa.table({"k": [0, 0, 0, 1], "v": [[1, 2], [1, 2], [3], [4]]})
    aggs = (TopKUnique("v", k=1),)
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
        PartialAggPolicy,
        _make_aggregating_transformer,
    )

    shard = _make_aggregating_transformer(
        ("k",), aggs, policy=PartialAggPolicy(mode=PartialAggMode.ALWAYS)
    )(table)
    assert shard.column_names == ["k", "v"]  # raw rows
    expected = _aggregate_shards(aggs, [table], modes=None).to_pylist()
    assert _aggregate_shards(aggs, [table], ("always",)).to_pylist() == expected
    assert sorted(expected, key=lambda r: r["k"]) == [
        {"k": 0, "topk_unique(v)": [[1, 2]]},
        {"k": 1, "topk_unique(v)": [[4]]},
    ]


@pytest.mark.parametrize("modes", [("always", "always"), ("never", "always")])
def test_all_null_block_with_unsupported_type(modes):
    """An all-null block has Arrow type null, which says nothing about the
    column's real type; another block's list values need the Python engine,
    so the null block must not be pre-aggregated either."""
    tables = [
        pa.table({"k": [0, 1], "v": pa.nulls(2)}),
        pa.table({"k": [0, 0], "v": [[1, 2], [1, 2]]}),
    ]
    aggs = (TopKUnique("v", k=1),)
    expected = _aggregate_shards(aggs, tables, modes=None).to_pylist()
    got = _aggregate_shards(aggs, tables, modes).to_pylist()
    assert sorted(got, key=lambda r: r["k"]) == sorted(expected, key=lambda r: r["k"])


@pytest.mark.parametrize(
    "modes",
    [
        ("always", "always"),
        ("never", "always"),
        ("always", "never"),
        ("never", "never"),
    ],
)
@pytest.mark.parametrize(
    "second,expected",
    [
        (
            {"k": [0, 0], "v": [-3, 2]},
            {
                0: (3, 3.5355339059327378, -0.5, [-3, 2, None], [-3, 2]),
                1: (None, None, None, [None], [None]),
            },
        ),
        (
            {"k": [0], "v": pa.nulls(1)},
            {
                0: (None, None, None, [None, None], [None]),
                1: (None, None, None, [None], [None]),
            },
        ),
    ],
    ids=["with_numeric_block", "all_null"],
)
def test_null_typed_block(modes, second, expected):
    """A block whose aggregated column is all null has Arrow type null. Each
    aggregation must handle it alone and next to numeric shards."""
    tables = [pa.table({"k": [0, 1], "v": pa.nulls(2)}), pa.table(second)]
    aggs = (
        AbsMax("v"),
        Std("v"),
        Quantile("v"),
        AsList("v"),
        TopKUnique("v", k=2),
    )
    rows = _aggregate_shards(aggs, tables, modes).to_pylist()
    got = {
        r["k"]: (
            r["abs_max(v)"],
            r["std(v)"],
            r["quantile(v)"],
            sorted(r["list(v)"], key=lambda x: (x is None, x)),
            r["topk_unique(v)"],
        )
        for r in rows
    }
    assert got == expected


@pytest.mark.parametrize("modes", [("always", "never"), ("never", "never")])
def test_large_list_state(modes):
    """List state past int32 offsets is a LargeList; raw rows' state must not
    be cast down to a regular list when merged with it."""
    import ray.data._internal.arrow_aggregation as arrow_aggregation

    def _large_offsets(lengths):
        offsets = np.zeros(len(lengths) + 1, dtype=np.int64)
        np.cumsum(lengths, out=offsets[1:])
        return pa.array(offsets), pa.LargeListArray

    tables = [_vectorized_vs_python_table("int32", seed, 5) for seed in range(2)]
    aggs = (AsList("v"), Quantile("v"), TopKUnique("v", k=2), ValueCounter("v"))
    expected = _rows_by_key(_aggregate_shards(aggs, tables, modes), sort_lists=True)
    with patch.object(arrow_aggregation, "_list_offsets", _large_offsets):
        large = _aggregate_shards(aggs, tables, modes)
    assert pa.types.is_large_list(large.schema.field("list(v)").type)
    assert _rows_by_key(large, sort_lists=True) == expected


class _SumOfSquares(AggregateFnV2):
    """A Python-engine aggregation."""

    def __init__(self, on: str):
        super().__init__(f"sum_sq({on})", lambda: 0, on=on, ignore_nulls=True)

    def aggregate_block(self, block):
        column = BlockAccessor.for_block(block).to_arrow()[self._target_col_name]
        return pc.sum(pc.multiply(column, column)).as_py()

    def combine(self, current_accumulator, new):
        return current_accumulator + new


class _SumOfSquaresWithRawToPartial(_SumOfSquares):
    def raw_to_partial(self, block):
        column = block[self._target_col_name]
        return pc.multiply(column, column)


@pytest.mark.parametrize(
    "modes",
    [("never",) * 3, ("always", "never", "never"), ("always",) * 3],
)
def test_python_raw_to_partial_hook(modes):
    """A Python-engine aggregation implementing ``raw_to_partial`` lets map
    tasks skip partial aggregation, with the same result."""
    rng = np.random.default_rng(0)
    tables = [
        pa.table({"k": rng.integers(0, 50, 200), "v": rng.integers(-5, 5, 200)})
        for _ in range(3)
    ]
    expected = _rows_by_key(
        _aggregate_shards((_SumOfSquares("v"),), tables, ("always",) * 3)
    )
    result = _aggregate_shards((_SumOfSquaresWithRawToPartial("v"),), tables, modes)
    assert "__ray_raw_rows" not in result.column_names
    assert _rows_by_key(result) == expected


def test_python_engine_without_raw_to_partial_always_pre_aggregates():
    from ray.data._internal.execution.operators.hash_aggregate_v2 import (
        PartialAggMode,
        PartialAggPolicy,
        _make_aggregating_transformer,
    )

    table = pa.table({"k": np.arange(100), "v": np.arange(100)})
    shard = _make_aggregating_transformer(
        ("k",),
        (_SumOfSquares("v"),),
        policy=PartialAggPolicy(mode=PartialAggMode.NEVER),
    )(table)
    assert shard.column_names == ["k", "sum_sq(v)"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
