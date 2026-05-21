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
from ray.data._internal.execution.operators.base_shuffle_operator import (
    _MAX_RETURN_GROUPS,
    BaseShuffleOperator,
    _encode_group_ipc,
    _partition_blocks_to_shards,
    _read_ipc_group,
)
from ray.data._internal.execution.operators.hash_shuffle_v2 import (
    HashShuffleOperatorV2,
    _concat_reduce,
    _make_hash_partition_fn,
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
    """Minimal PhysicalOperator mock that satisfies BaseShuffleOperator init."""
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


def _make_base_op(
    *,
    num_partitions: int = 4,
    streaming_reduce: bool = True,
    disallow_block_splitting: bool = False,
    pre_map_merge_threshold: int = BaseShuffleOperator._DEFAULT_PRE_MAP_MERGE_THRESHOLD,  # noqa: SLF001
) -> BaseShuffleOperator:
    ctx = DataContext()
    return BaseShuffleOperator(
        _make_input_op_mock(num_blocks=4),
        ctx,
        num_partitions=num_partitions,
        partition_fn=_make_hash_partition_fn(["id"], num_partitions),
        reduce_fn=_concat_reduce,
        streaming_reduce=streaming_reduce,
        disallow_block_splitting=disallow_block_splitting,
        pre_map_merge_threshold=pre_map_merge_threshold,
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
# Operator construction
# ===========================================================================


@dataclass
class ConstructorCase:
    name: str
    # Inputs
    num_partitions: Optional[int]
    sort: bool = False
    estimated_input_blocks: Optional[int] = None
    default_parallelism: int = 200
    # Expected outputs
    expected_num_partitions: int = 0


@pytest.mark.parametrize(
    "tc",
    [
        ConstructorCase(
            name="explicit_num_partitions_used",
            num_partitions=16,
            expected_num_partitions=16,
        ),
        ConstructorCase(
            name="fallback_to_estimated_input_blocks",
            num_partitions=None,
            estimated_input_blocks=12,
            expected_num_partitions=12,
        ),
        ConstructorCase(
            name="fallback_to_context_default_parallelism",
            num_partitions=None,
            estimated_input_blocks=None,
            default_parallelism=300,
            expected_num_partitions=300,
        ),
        ConstructorCase(
            name="sort_false_default",
            num_partitions=4,
            sort=False,
            expected_num_partitions=4,
        ),
        ConstructorCase(
            name="sort_true_swaps_reduce_fn",
            num_partitions=4,
            sort=True,
            expected_num_partitions=4,
        ),
    ],
    ids=lambda tc: tc.name,
)
def test_hash_shuffle_v2_constructor(tc: ConstructorCase):
    ctx = DataContext()
    ctx._shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.default_hash_shuffle_parallelism = tc.default_parallelism
    op = HashShuffleOperatorV2(
        _make_input_op_mock(num_blocks=tc.estimated_input_blocks),
        ctx,
        key_columns=("id",),
        num_partitions=tc.num_partitions,
        sort=tc.sort,
    )
    assert op._num_partitions == tc.expected_num_partitions
    # ``sort=False`` must use the module-level singleton; ``sort=True`` must not.
    if not tc.sort:
        assert op._reduce_fn is _concat_reduce
    else:
        assert op._reduce_fn is not _concat_reduce


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
    op = _make_base_op(
        streaming_reduce=tc.streaming_reduce,
        disallow_block_splitting=tc.disallow_block_splitting,
    )
    assert op._streaming_reduce is tc.expected_streaming_reduce


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
    op = _make_base_op(num_partitions=tc.num_partitions)
    assert op._shard_group_size == tc.expected_shard_group_size
    assert op._num_groups == tc.expected_num_groups
    assert op._num_groups <= _MAX_RETURN_GROUPS


# ===========================================================================
# Operator internals (mocked)
# ===========================================================================


@dataclass
class MergeBufferCase:
    name: str
    threshold: int
    # Sequence of (num_blocks, size_bytes) bundles to add before checking.
    bundles: List[Tuple[int, int]] = field(default_factory=list)
    # Whether to call all_inputs_done() at the end.
    finalize: bool = False
    # Expected sequence of per-call block counts seen by _submit_shuffle_map_task.
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
    # ``get_preferred_object_locations`` requires a live raylet; we don't
    # need real placement info, just an empty dict so the operator falls
    # back to the "unknown" node bucket.
    monkeypatch.setattr(RefBundle, "get_preferred_object_locations", lambda self: {})
    op = _make_base_op(pre_map_merge_threshold=tc.threshold)
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
    rows and key totals when routed through ``HashShuffleOperatorV2``."""
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
    """V2 must produce exactly ``num_partitions`` output blocks.

    This is the partition = block contract — guarded by
    ``disallow_block_splitting=True`` + ``streaming_reduce=False`` in V2.
    """
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
    """All rows sharing a key must end up in the same output block under
    hash-shuffle semantics."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    # Small key cardinality so each block ends up with multiple keys.
    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 25, "v": row["id"]}
    )
    out = ds.repartition(5, keys=["k"])

    # Each block should contain a disjoint subset of keys.
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
