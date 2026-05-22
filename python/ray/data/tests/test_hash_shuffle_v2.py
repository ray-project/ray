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

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple
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
    _MAX_RETURN_GROUPS,
    _encode_group_ipc,
    _partition_blocks_to_shards,
    _read_ipc_group,
    compute_num_groups,
    compute_shard_group_size,
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
    shard_group_size = compute_shard_group_size(num_partitions)
    num_groups = compute_num_groups(num_partitions, shard_group_size)
    return ShuffleMapOp(
        _make_input_op_mock(num_blocks=4),
        ctx,
        num_partitions=num_partitions,
        shard_group_size=shard_group_size,
        num_groups=num_groups,
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
    shard_group_size = compute_shard_group_size(num_partitions)
    num_groups = compute_num_groups(num_partitions, shard_group_size)
    if map_op is None:
        map_op = _make_map_op(num_partitions=num_partitions)
    return ShuffleReduceOp(
        map_op,
        ctx,
        num_partitions=num_partitions,
        shard_group_size=shard_group_size,
        num_groups=num_groups,
        reduce_fn=_concat_reduce,
        streaming_reduce=streaming_reduce,
        disallow_block_splitting=disallow_block_splitting,
    )


# ===========================================================================
# Pure helpers
# ===========================================================================


def test_partition_blocks_to_shards_combines_chunked_columns():
    """Regression: ``hash_partition`` is sensitive to per-column chunking; the
    operator must defragment via ``combine_chunks`` before calling partition_fn
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


@dataclass
class IpcRoundtripCase:
    name: str
    pids: List[int]
    pid_tables_fn: Callable[[], Dict[int, pa.Table]]
    expected_decoded_pids: List[int]


@pytest.mark.parametrize(
    "tc",
    [
        IpcRoundtripCase(
            name="single_pid",
            pids=[3],
            pid_tables_fn=lambda: {3: _make_table(10)},
            expected_decoded_pids=[3],
        ),
        IpcRoundtripCase(
            name="multi_pid_in_order",
            pids=[1, 2, 7],
            pid_tables_fn=lambda: {
                p: _make_table(5, offset=p * 100) for p in [1, 2, 7]
            },
            expected_decoded_pids=[1, 2, 7],
        ),
        IpcRoundtripCase(
            name="empty_pid_dropped",
            pids=[1, 2],
            pid_tables_fn=lambda: {1: _make_table(4), 2: _empty_table()},
            expected_decoded_pids=[1],
        ),
    ],
    ids=lambda tc: tc.name,
)
def test_ipc_encode_decode_roundtrip(tc: IpcRoundtripCase):
    buf = _encode_group_ipc(tc.pids, tc.pid_tables_fn(), _ipc_opts())
    out = _read_ipc_group(buf)
    assert [pid for pid, _ in out] == tc.expected_decoded_pids


def test_ipc_decoded_schema_has_no_pids_metadata():
    """``__pids__`` is an internal routing tag; must not leak downstream."""
    buf = _encode_group_ipc([0], {0: _make_table(3)}, _ipc_opts())
    _, decoded = _read_ipc_group(buf)[0]
    meta = decoded.schema.metadata
    assert meta is None or b"__pids__" not in meta


def test_ipc_encode_does_not_mutate_upstream_schema_metadata():
    """Upstream tables that share a schema must remain untouched after encode."""
    original_meta = {b"foo": b"bar"}
    schema = pa.schema(
        [("id", pa.int64()), ("val", pa.float64())], metadata=original_meta
    )
    table = pa.table({"id": [1, 2, 3], "val": [1.0, 2.0, 3.0]}, schema=schema)
    _encode_group_ipc([0], {0: table}, _ipc_opts())
    assert table.schema.metadata == original_meta


# ===========================================================================
# Shard grouping config
# ===========================================================================


@dataclass
class ShardGroupCase:
    name: str
    num_partitions: int
    expected_shard_group_size: int
    expected_num_groups: int


@pytest.mark.parametrize(
    "tc",
    [
        ShardGroupCase("few_one_per_group", 50, 1, 50),
        ShardGroupCase(
            "at_cap_one_per_group", _MAX_RETURN_GROUPS, 1, _MAX_RETURN_GROUPS
        ),
        ShardGroupCase(
            "above_cap_packs_multiple",
            _MAX_RETURN_GROUPS * 3,
            3,
            _MAX_RETURN_GROUPS,
        ),
    ],
    ids=lambda tc: tc.name,
)
def test_shard_grouping(tc: ShardGroupCase):
    sgs = compute_shard_group_size(tc.num_partitions)
    ng = compute_num_groups(tc.num_partitions, sgs)
    assert sgs == tc.expected_shard_group_size
    assert ng == tc.expected_num_groups
    assert ng <= _MAX_RETURN_GROUPS


# ===========================================================================
# ShuffleReduceOp: contract flags
# ===========================================================================


@dataclass
class ContractCase:
    name: str
    streaming_reduce: bool
    disallow_block_splitting: bool
    expected_streaming_reduce: bool


@pytest.mark.parametrize(
    "tc",
    [
        ContractCase("streaming_on_disallow_off", True, False, True),
        ContractCase("disallow_forces_streaming_off", True, True, False),
        ContractCase("streaming_off_passthrough", False, False, False),
    ],
    ids=lambda tc: tc.name,
)
def test_contract_flags(tc: ContractCase):
    op = _make_reduce_op(
        streaming_reduce=tc.streaming_reduce,
        disallow_block_splitting=tc.disallow_block_splitting,
    )
    assert op._streaming_reduce is tc.expected_streaming_reduce


# ===========================================================================
# ShuffleReduceOp: concurrency limit
# ===========================================================================


@dataclass
class ReduceConcurrencyCase:
    name: str
    num_nodes: int
    override: Optional[int] = None
    expected_limit: int = 0


@pytest.mark.parametrize(
    "tc",
    [
        ReduceConcurrencyCase(
            name="default_is_one_per_node", num_nodes=32, expected_limit=32
        ),
        ReduceConcurrencyCase(
            name="override_wins", num_nodes=32, override=12, expected_limit=12
        ),
    ],
    ids=lambda tc: tc.name,
)
def test_reduce_concurrency_limit(tc: ReduceConcurrencyCase):
    op = _make_reduce_op()
    op._num_nodes = tc.num_nodes
    if tc.override is not None:
        op._data_context.set_config("map_reduce_max_concurrent_reducers", tc.override)
    assert op._get_reduce_concurrency_limit() == tc.expected_limit


# ===========================================================================
# ShuffleMapOp: pre-map merge buffer
# ===========================================================================


@dataclass
class MergeBufferCase:
    name: str
    threshold: int
    bundles: List[Tuple[int, int]] = field(default_factory=list)
    finalize: bool = False
    expected_submitted_block_counts: List[int] = field(default_factory=list)


@pytest.mark.parametrize(
    "tc",
    [
        MergeBufferCase(
            name="threshold_zero_one_task_per_block",
            threshold=0,
            bundles=[(3, 10)],
            expected_submitted_block_counts=[1, 1, 1],
        ),
        MergeBufferCase(
            name="below_threshold_then_crossover_flushes_merged",
            threshold=1000,
            bundles=[(1, 200), (1, 300), (1, 600)],
            expected_submitted_block_counts=[3],
        ),
        MergeBufferCase(
            name="all_inputs_done_drains_remaining",
            threshold=10_000,
            bundles=[(2, 100)],
            finalize=True,
            expected_submitted_block_counts=[2],
        ),
    ],
    ids=lambda tc: tc.name,
)
def test_pre_map_merge_buffer(monkeypatch, tc: MergeBufferCase):
    monkeypatch.setattr(RefBundle, "get_preferred_object_locations", lambda self: {})
    op = _make_map_op(pre_map_merge_threshold=tc.threshold)
    submitted: List[int] = []
    monkeypatch.setattr(
        op,
        "_submit_shuffle_map_task",
        lambda block_refs, bundles, **kw: submitted.append(len(block_refs)),
    )
    for num_blocks, size_bytes in tc.bundles:
        op._add_input_inner(
            _make_bundle(num_blocks=num_blocks, size_bytes=size_bytes),
            input_index=0,
        )
    if tc.finalize:
        op.all_inputs_done()
    assert submitted == tc.expected_submitted_block_counts


# ===========================================================================
# ShuffleReduceOp: bundle routing by block position
# ===========================================================================


def test_reduce_op_groups_bundles_by_block_position():
    """ShuffleReduceOp gathers ``bundle.blocks[g]`` into ``_group_buffers[g]``."""
    op = _make_reduce_op(num_partitions=4)
    # num_groups == 4 for num_partitions=4
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

    for g in range(4):
        assert op._group_buffers[g] == [refs_a[g], refs_b[g]]


def test_reduce_op_rejects_bundle_with_wrong_block_count():
    """Bundles must have exactly num_groups blocks (one per group)."""
    op = _make_reduce_op(num_partitions=4)
    bundle = _make_bundle(num_blocks=3)  # wrong: should be 4
    with pytest.raises(ValueError, match="expected 4 blocks"):
        op._add_input_inner(bundle, input_index=0)


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
    """Round-trip: ``repartition(num_partitions, keys=...)`` preserves all
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
    """V2 must produce exactly ``num_partitions`` output blocks (partition =
    block contract).  Guarded by ``disallow_block_splitting=True`` +
    ``streaming_reduce=False``."""
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
    """``repartition(sort=True, keys=...)`` selects the sort reduce; each
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
