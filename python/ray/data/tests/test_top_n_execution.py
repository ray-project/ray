import random
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.top_n_operator import (
    TopNMergeOperator,
    _batch_candidates,
)
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.logical.rules.top_n_rewrite import (
    TOP_N_REWRITE_ENABLED_CONFIG_KEY,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403


def test_top_n_planner_routes_to_local_map_and_final_merge(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    ds = ray.data.range(10, override_num_blocks=2).sort("id").limit(3)

    dag = get_execution_plan(ds._logical_plan)[0].dag

    assert isinstance(dag, TopNMergeOperator)
    assert dag.name == "TopNMerge(limit=3)"
    local_map = dag.input_dependencies[0]
    assert isinstance(local_map, MapOperator)
    assert local_map.name == "LocalTopN(limit=3)"
    assert not local_map.supports_fusion()


@pytest.mark.parametrize("limit", [0, 1, 7, 10, 20])
def test_top_n_shuffle_v2_end_to_end(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    limit,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)

    rows = [{"a": i % 4, "b": i} for i in range(10)]
    random.Random(0).shuffle(rows)
    ds = (
        ray.data.from_items(rows, override_num_blocks=4)
        .sort(["a", "b"], descending=[False, True])
        .limit(limit)
    )
    if limit > 0:
        assert isinstance(
            get_execution_plan(ds._logical_plan)[0].dag, TopNMergeOperator
        )
    result = ds.take_all()

    assert result == sorted(rows, key=lambda row: (row["a"], -row["b"]))[:limit]


def test_top_n_shuffle_v2_handles_non_adjacent_sort_and_large_limit(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)

    ds = (
        ray.data.range(5, override_num_blocks=2)
        .sort("id", descending=True)
        .map(lambda row: {"id": row["id"]})
        .limit(10)
    )
    result = ds.take_all()

    assert result == [{"id": value} for value in range(4, -1, -1)]
    physical_dag = get_execution_plan(ds._logical_plan)[0].dag
    assert isinstance(physical_dag.input_dependencies[0], TopNMergeOperator)


def test_top_n_shuffle_v2_handles_empty_input(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)

    result = ray.data.range(0, override_num_blocks=2).sort("id").limit(3).take_all()

    assert result == []


def test_top_n_shuffle_v2_preserves_typed_empty_arrow_schema(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    schema = pa.schema([("key", pa.int64()), ("value", pa.string())])
    ds = ray.data.from_arrow([schema.empty_table()]).sort("key").limit(3)

    result = ds.materialize()

    assert result.take_all() == []
    assert result.schema().base_schema == schema


def test_top_n_shuffle_v2_preserves_typed_empty_pandas_schema(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    frame = pd.DataFrame(
        {
            "key": pd.Series(dtype="int64"),
            "value": pd.Series(dtype="string"),
        }
    )
    expected_schema = BlockAccessor.for_block(frame).schema()
    ds = ray.data.from_pandas(frame).sort("key").limit(3)

    result = ds.materialize()

    assert result.take_all() == []
    assert result.schema().base_schema == expected_schema


def test_top_n_shuffle_v2_preserves_invalid_key_error(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)

    with pytest.raises(ValueError, match="there's no such column"):
        ray.data.range(5).sort("missing").limit(3).take_all()


def test_top_n_merges_candidates_in_batches(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    ds = ray.data.range(100, override_num_blocks=20).sort("id", descending=True)

    ref_batches = []

    def record_batches(entries):
        batches, merge_bytes = _batch_candidates(entries)
        ref_batches.extend(batches)
        return batches, merge_bytes

    # Force one candidate block per batch so the merge runs 20 rounds.
    with (
        mock.patch(
            "ray.data._internal.execution.operators.top_n_operator._MERGE_BATCH_BYTES",
            1,
        ),
        mock.patch(
            "ray.data._internal.execution.operators.top_n_operator._batch_candidates",
            side_effect=record_batches,
        ),
    ):
        result = ds.limit(3).take_all()

    assert [len(batch) for batch in ref_batches] == [1] * 20
    assert result == [{"id": 99}, {"id": 98}, {"id": 97}]


def test_top_n_shuffle_v2_arrow_nulls_last(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    blocks = [
        pa.table({"key": [3, None], "row_id": [0, 1]}),
        pa.table({"key": [1, 2], "row_id": [2, 3]}),
    ]

    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    result = ray.data.from_arrow(blocks).sort("key").limit(3).take_all()

    assert result == [
        {"key": 1, "row_id": 2},
        {"key": 2, "row_id": 3},
        {"key": 3, "row_id": 0},
    ]


@pytest.mark.parametrize("descending", [False, True])
def test_top_n_shuffle_v2_matches_global_sort_with_arrow_nan(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    descending,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    blocks = [
        pa.table({"key": [2.0, float("nan"), 1.0], "row_id": [0, 1, 2]}),
        pa.table({"key": [float("nan"), 4.0, 3.0], "row_id": [3, 4, 5]}),
    ]

    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, False)
    baseline = (
        ray.data.from_arrow(blocks)
        .sort("key", descending=descending)
        .limit(5)
        .take_all()
    )
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    top_n = (
        ray.data.from_arrow(blocks)
        .sort("key", descending=descending)
        .limit(5)
        .take_all()
    )

    assert [row["row_id"] for row in top_n] == [row["row_id"] for row in baseline]


def test_top_n_shuffle_v2_pandas_block(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    frame = pd.DataFrame({"key": [3, 1, 2], "value": ["c", "a", "b"]})

    result = ray.data.from_pandas(frame).sort("key").limit(2).take_all()

    assert result == [
        {"key": 1, "value": "a"},
        {"key": 2, "value": "b"},
    ]


def test_top_n_shuffle_v2_allows_any_rows_at_boundary_tie(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    rows = [
        {"key": 0, "row_id": 0},
        {"key": 1, "row_id": 1},
        {"key": 1, "row_id": 2},
        {"key": 1, "row_id": 3},
        {"key": 2, "row_id": 4},
    ]

    result = (
        ray.data.from_items(rows, override_num_blocks=3).sort("key").limit(3).take_all()
    )

    assert [row["key"] for row in result] == [0, 1, 1]
    assert result[0] == {"key": 0, "row_id": 0}
    assert {row["row_id"] for row in result[1:]} <= {1, 2, 3}


def test_top_n_shuffle_v2_promotes_compatible_block_schemas(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    null_block = pa.table(
        {
            "key": pa.array([0], type=pa.int64()),
            "value": pa.array([None], type=pa.null()),
        }
    )
    int_block = pa.table(
        {
            "key": pa.array([2, 1], type=pa.int64()),
            "value": pa.array([20, 10], type=pa.int64()),
        }
    )

    result = (
        ray.data.from_arrow([null_block, int_block]).sort("key").limit(3).take_all()
    )

    assert result == [
        {"key": 0, "value": None},
        {"key": 1, "value": 10},
        {"key": 2, "value": 20},
    ]


def test_top_n_shuffle_v2_normalizes_mixed_arrow_and_pandas_blocks(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    arrow_ds = ray.data.from_arrow(pa.table({"key": [3, 1]}))
    pandas_ds = ray.data.from_pandas(pd.DataFrame({"key": [4, 2]}))

    result = arrow_ds.union(pandas_ds).sort("key").limit(3).take_all()

    assert result == [{"key": 1}, {"key": 2}, {"key": 3}]


def test_top_n_shuffle_v2_mixed_direction_secondary_nulls(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    blocks = [
        pa.table({"a": [2, 1], "b": [None, 0]}),
        pa.table({"a": [2, 1], "b": [1, None]}),
    ]

    result = (
        ray.data.from_arrow(blocks)
        .sort(["a", "b"], descending=[True, False])
        .limit(3)
        .take_all()
    )

    assert result == [
        {"a": 2, "b": 1},
        {"a": 2, "b": None},
        {"a": 1, "b": 0},
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
