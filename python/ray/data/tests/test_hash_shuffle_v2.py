from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import ray
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa: F401, F403
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


# ---------------------------------------------------------------------------
# Join on the V2 shuffle (two ShuffleMapOps -> one binary ShuffleReduceOp)
# ---------------------------------------------------------------------------


def test_make_join_reduce_fn_inner_join():
    """The binary join reduce fn joins co-partitioned left/right shards."""
    from ray.data._internal.execution.operators.join import _make_join_reduce_fn
    from ray.data._internal.logical.operators import JoinType

    reduce_fn = _make_join_reduce_fn(
        join_type=JoinType.INNER,
        left_key_col_names=("id",),
        right_key_col_names=("id",),
    )

    left_shards = [
        pa.table({"id": [1, 2], "double": [2, 4]}),
        pa.table({"id": [3], "double": [6]}),
    ]
    right_shards = [pa.table({"id": [2, 3, 4], "square": [4, 9, 16]})]

    (out,) = list(reduce_fn(0, [left_shards, right_shards]))
    out = out.sort_by("id")
    assert out.column("id").to_pylist() == [2, 3]
    assert out.column("double").to_pylist() == [4, 6]
    assert out.column("square").to_pylist() == [4, 9]


def test_make_join_reduce_fn_left_outer_empty_right():
    """An empty right side still yields left rows for an outer join.

    Each map task emits a schema-only shard for partitions it has no rows for,
    so the right side arrives as empty-but-typed tables; the reducer must still
    emit the (null-filled) left rows.
    """
    from ray.data._internal.execution.operators.join import _make_join_reduce_fn
    from ray.data._internal.logical.operators import JoinType

    reduce_fn = _make_join_reduce_fn(
        join_type=JoinType.LEFT_OUTER,
        left_key_col_names=("id",),
        right_key_col_names=("id",),
    )

    left_shards = [pa.table({"id": [1, 2], "double": [2, 4]})]
    # Right contributed no rows to this partition: schema-only shard.
    right_schema = pa.schema([("id", pa.int64()), ("square", pa.int64())])
    right_shards = [right_schema.empty_table()]

    (out,) = list(reduce_fn(0, [left_shards, right_shards]))
    out = out.sort_by("id")
    assert out.column("id").to_pylist() == [1, 2]
    assert out.column("double").to_pylist() == [2, 4]
    assert out.column("square").to_pylist() == [None, None]


def _make_input_op_mock():
    """Minimal PhysicalOperator mock accepted as a ShuffleMapOp upstream."""
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.logical.interfaces import LogicalOperator
    from ray.data.block import BlockMetadata

    logical_mock = MagicMock(LogicalOperator)
    logical_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None, size_bytes=None, exec_stats=None, input_files=None
    )
    logical_mock.estimated_num_outputs.return_value = None

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_mock]
    op_mock.num_output_splits.return_value = 1
    return op_mock


def test_join_routes_to_shuffle_reduce_op_when_enabled():
    """With join_use_shuffle_v2, the planner returns a binary ShuffleReduceOp
    fed by two ShuffleMapOps."""
    from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
        ShuffleMapOp,
    )
    from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
        ShuffleReduceOp,
    )
    from ray.data._internal.logical.interfaces import LogicalOperator
    from ray.data._internal.logical.operators.join_operator import Join
    from ray.data._internal.planner.planner import plan_join_op

    ctx = DataContext()
    ctx.join_use_shuffle_v2 = True

    logical_op = Join(
        left_input_op=MagicMock(LogicalOperator),
        right_input_op=MagicMock(LogicalOperator),
        join_type="inner",
        left_key_columns=("id",),
        right_key_columns=("id",),
        num_partitions=8,
    )

    op = plan_join_op(logical_op, [_make_input_op_mock(), _make_input_op_mock()], ctx)

    assert isinstance(op, ShuffleReduceOp)
    assert op._num_inputs == 2
    assert op._num_partitions == 8
    assert len(op.input_dependencies) == 2
    assert all(isinstance(dep, ShuffleMapOp) for dep in op.input_dependencies)


@pytest.mark.parametrize("num_partitions", [1, 4, 16])
def test_join_v2_inner_matches_pandas(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    num_partitions,
):
    """V2 inner join over multi-block inputs matches a pandas reference."""
    import pandas as pd

    ctx = DataContext.get_current()
    ctx.join_use_shuffle_v2 = True

    left = ray.data.range(100, override_num_blocks=8).map(
        lambda row: {"id": row["id"], "double": row["id"] * 2}
    )
    right = ray.data.range(100, override_num_blocks=8).map(
        lambda row: {"id": row["id"] + 50, "square": row["id"] ** 2}
    )

    joined = left.join(
        right, join_type="inner", num_partitions=num_partitions, on=("id",)
    )
    joined_pd = pd.DataFrame(joined.take_all()).sort_values("id").reset_index(drop=True)

    left_pd = left.to_pandas()
    right_pd = right.to_pandas()
    expected = (
        left_pd.merge(right_pd, on="id", how="inner")
        .sort_values("id")
        .reset_index(drop=True)
    )
    expected = expected.astype(joined_pd.dtypes.to_dict())
    pd.testing.assert_frame_equal(expected, joined_pd)


@pytest.mark.parametrize(
    "num_rows_left,num_rows_right",
    [(32, 32), (1, 32), (32, 1)],
)
def test_join_v2_left_outer_degenerate_sides(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    num_rows_left,
    num_rows_right,
):
    """Left outer join is correct even when one side is mostly/entirely empty
    per partition -- exercises the empty-side path of the binary reducer."""
    import pandas as pd

    ctx = DataContext.get_current()
    ctx.join_use_shuffle_v2 = True

    left = ray.data.range(num_rows_left).map(
        lambda row: {"id": row["id"], "double": row["id"] * 2}
    )
    right = ray.data.range(num_rows_right).map(
        lambda row: {"id": row["id"], "square": row["id"] ** 2}
    )

    joined = left.join(right, join_type="left_outer", num_partitions=8, on=("id",))
    joined_pd = pd.DataFrame(joined.take_all()).sort_values("id").reset_index(drop=True)

    left_pd = left.to_pandas()
    right_pd = right.to_pandas()
    expected = (
        left_pd.merge(right_pd, on="id", how="left")
        .sort_values("id")
        .reset_index(drop=True)
    )
    # Every left row is preserved.
    assert len(joined_pd) == num_rows_left
    assert joined_pd["id"].tolist() == expected["id"].tolist()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
