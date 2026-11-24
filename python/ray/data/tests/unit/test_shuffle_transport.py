from dataclasses import dataclass
from typing import Optional

import pyarrow as pa

from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    _encode_partition_ipc,
    _read_partition_ipc,
    _slice_ipc,
    assign_empty_placements,
    pack_partition_ipc,
)
from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator
from ray.data._internal.logical.operators.map_operator import MapRows
from ray.data._internal.planner.plan_all_to_all_op import _select_shuffle_v2_op_classes
from ray.data._internal.planner.shuffle_decision import (
    estimate_shuffle_input_bytes,
    resolve_num_partitions,
    resolve_shuffle_transport,
)
from ray.data.block import BlockMetadata
from ray.data.context import DataContext


@dataclass(frozen=True, repr=False, eq=False)
class _SizedOp(LogicalOperator):
    nbytes: Optional[int] = None

    def __post_init__(self):
        object.__setattr__(self, "_name", "sized")

    def infer_metadata(self):
        return BlockMetadata(
            num_rows=1,
            size_bytes=self.nbytes,
            input_files=None,
            exec_stats=None,
        )


def _with_inputs(op, deps):
    object.__setattr__(op, "_input_dependencies", list(deps))
    return op


def _table(values):
    return pa.table({"v": values})


def _ipc(values):
    return _encode_partition_ipc(_table(values), pa.ipc.IpcWriteOptions())


def _decode(bundle, offset, length) -> pa.Table:
    table = _read_partition_ipc(_slice_ipc(bundle, offset, length))
    assert table is not None
    return table


def test_new_context_stays_on_auto_until_the_bool_is_assigned(monkeypatch):
    monkeypatch.delenv("RAY_DATA_ENABLE_DISK_SHUFFLE", raising=False)
    monkeypatch.delenv("RAY_DATA_ENABLE_EXTERNAL_SHUFFLE", raising=False)

    ctx = DataContext()
    assert ctx.shuffle_transport == "auto"
    assert ctx.use_disk_based_hash_shuffle is False

    ctx.use_disk_based_hash_shuffle = False
    assert ctx.shuffle_transport == "object_store"

    ctx.use_disk_based_hash_shuffle = True
    assert ctx.shuffle_transport == "disk"
    assert ctx.use_disk_based_hash_shuffle is True

    ctx.shuffle_transport = "auto"
    assert ctx.use_disk_based_hash_shuffle is False


def test_constructor_transport_overrides_env_and_bool(monkeypatch):
    monkeypatch.setenv("RAY_DATA_ENABLE_DISK_SHUFFLE", "1")
    ctx = DataContext(shuffle_transport="object_store")
    assert ctx.shuffle_transport == "object_store"
    assert ctx.use_disk_based_hash_shuffle is False

    # The import-time default is False in this process. Patch it so a
    # constructor False is a real override of an env-selected default.
    monkeypatch.setattr("ray.data.context.DEFAULT_ENABLE_DISK_SHUFFLE", True)
    ctx = DataContext(use_disk_based_hash_shuffle=False)
    assert ctx.shuffle_transport == "object_store"
    assert ctx.use_disk_based_hash_shuffle is False

    ctx = DataContext(shuffle_transport="disk", use_disk_based_hash_shuffle=False)
    assert ctx.shuffle_transport == "disk"
    assert ctx.use_disk_based_hash_shuffle is True


def test_disk_shuffle_env_pins_transport(monkeypatch):
    monkeypatch.setenv("RAY_DATA_ENABLE_DISK_SHUFFLE", "0")
    assert DataContext().shuffle_transport == "object_store"

    monkeypatch.setenv("RAY_DATA_ENABLE_DISK_SHUFFLE", "1")
    ctx = DataContext()
    assert ctx.shuffle_transport == "disk"
    assert ctx.use_disk_based_hash_shuffle is True


def test_auto_uses_disk_only_when_estimate_meets_budget():
    ctx = DataContext()
    ctx.shuffle_transport = "auto"
    gib = 1024**3

    assert (
        resolve_shuffle_transport(ctx, 10 * gib, object_store_bytes=2 * gib) == "disk"
    )
    assert (
        resolve_shuffle_transport(ctx, 100 * 1024**2, object_store_bytes=2 * gib)
        == "object_store"
    )
    assert (
        resolve_shuffle_transport(ctx, None, object_store_bytes=2 * gib)
        == "object_store"
    )
    assert (
        resolve_shuffle_transport(ctx, 10 * gib, object_store_bytes=None)
        == "object_store"
    )

    # The estimate meets the budget exactly: half of 2 GiB is 1 GiB.
    assert resolve_shuffle_transport(ctx, gib, object_store_bytes=2 * gib) == "disk"

    ctx.shuffle_transport = "object_store"
    assert (
        resolve_shuffle_transport(ctx, 10 * gib, object_store_bytes=2 * gib)
        == "object_store"
    )
    ctx.shuffle_transport = "disk"
    assert resolve_shuffle_transport(ctx, 1, object_store_bytes=2 * gib) == "disk"


def test_sort_auto_stays_on_object_store():
    ctx = DataContext()
    ctx.shuffle_transport = "auto"
    _, _, prefix = _select_shuffle_v2_op_classes(ctx, 10 * 1024**3, allow_auto=False)
    assert prefix == ""

    ctx.shuffle_transport = "disk"
    _, _, prefix = _select_shuffle_v2_op_classes(ctx, 1, allow_auto=False)
    assert prefix == "Disk"


def test_partition_count_respects_requests_and_keyless_aggregation():
    ctx = DataContext()
    ctx.default_hash_shuffle_parallelism = 200
    ctx.shuffle_target_partition_bytes = 1024**3
    ctx.shuffle_max_partitions = 1024
    gib = 1024**3

    assert resolve_num_partitions(ctx, 10 * gib, requested=7) == 7
    assert resolve_num_partitions(ctx, 10 * gib, requested=None) == 10
    assert resolve_num_partitions(ctx, None, requested=None) == 200
    assert resolve_num_partitions(ctx, 5000 * gib, requested=None) == 1024
    assert resolve_num_partitions(ctx, 5000 * gib, requested=None, keyless=True) == 1
    assert resolve_num_partitions(ctx, 0, requested=None) == 1


def test_udf_ancestor_makes_the_byte_estimate_unknown():
    source = _SizedOp(100)
    mapped = MapRows(fn=lambda row: row, input_dependencies=[source])
    assert estimate_shuffle_input_bytes(_with_inputs(_SizedOp(None), [mapped])) is None

    left = _SizedOp(10)
    right = _SizedOp(5)
    assert (
        estimate_shuffle_input_bytes(_with_inputs(_SizedOp(None), [left, right])) == 15
    )
    missing = _SizedOp(None)
    assert (
        estimate_shuffle_input_bytes(_with_inputs(_SizedOp(None), [left, missing]))
        is None
    )


def test_small_shards_pack_and_round_trip():
    shards = [_ipc([i]) for i in range(200)]
    bundles, placements = pack_partition_ipc(
        shards, min_bytes=1024 * 1024, max_partitions=8
    )
    assert len(bundles) == 25
    assert all(placement is not None for placement in placements)
    for partition_id, placement in enumerate(placements):
        assert placement is not None
        bundle_index, offset, length = placement
        table = _decode(bundles[bundle_index], offset, length)
        assert table.column("v").to_pylist() == [partition_id]


def test_shard_at_the_floor_is_its_own_object():
    floor = 64
    big = b"x" * floor
    small = b"y" * 8
    bundles, placements = pack_partition_ipc(
        [small, big, small], min_bytes=floor, max_partitions=8
    )
    assert len(bundles) == 3
    assert placements[1] == (1, 0, floor)


def test_empty_partition_shares_one_schema_buffer():
    present = _ipc([1, 2])
    schema = _decode(present, 0, -1).schema
    empty = _encode_partition_ipc(
        pa.Table.from_arrays([pa.array([], type=schema.field("v").type)], names=["v"]),
        pa.ipc.IpcWriteOptions(),
    )
    bundles, placements = pack_partition_ipc(
        [present, None, None], min_bytes=1024 * 1024, max_partitions=8
    )
    bundles, placements = assign_empty_placements(bundles, placements, empty)
    assert placements[1][0] == placements[2][0]
    assert placements[1][0] != placements[0][0]
    empty_index, offset, length = placements[1]
    empty_table = _decode(bundles[empty_index], offset, length)
    assert empty_table.num_rows == 0
    assert empty_table.schema == schema
    kept = _decode(bundles[placements[0][0]], placements[0][1], placements[0][2])
    assert kept.column("v").to_pylist() == [1, 2]


def test_reducer_reads_one_partition_out_of_a_shared_bundle():
    shards = [_ipc([1]), _ipc([2, 2]), _ipc([3])]
    bundles, placements = pack_partition_ipc(
        shards, min_bytes=1024 * 1024, max_partitions=8
    )
    assert len(bundles) == 1
    placement = placements[1]
    assert placement is not None
    bundle_index, offset, length = placement
    table = _decode(bundles[bundle_index], offset, length)
    assert table.column("v").to_pylist() == [2, 2]
