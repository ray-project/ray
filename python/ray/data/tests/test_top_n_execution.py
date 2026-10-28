import asyncio
import math
import random
from typing import Any, Iterator, List
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionResources, RefBundle
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.top_n_operator import (
    TopNMergeOperator,
)
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.logical.rules.top_n_rewrite import (
    TOP_N_REWRITE_ENABLED_CONFIG_KEY,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.exceptions import RayTaskError
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
    local_map = dag.input_dependencies[0]

    assert isinstance(dag, TopNMergeOperator)
    assert dag.name == "TopNMerge(limit=3)"

    assert isinstance(local_map, MapOperator)
    assert local_map.name == "LocalTopN(limit=3)"
    assert not local_map.supports_fusion()


@pytest.mark.parametrize(
    "rows_per_block,expected_task_blocks",
    [
        ([2, 2, 2, 2, 2], [2, 2, 1]),
        ([6, 1, 1, 1], [1, 3]),
        ([0, 2, 0, 2, 1], [4, 1]),
    ],
)
def test_top_n_bundles_small_blocks_and_flushes_remainder(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    rows_per_block,
    expected_task_blocks,
) -> None:
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    blocks = []
    offset = 0
    for num_rows in rows_per_block:
        blocks.append(
            pa.table(
                {"id": pa.array(range(offset, offset + num_rows), type=pa.int64())}
            )
        )
        offset += num_rows
    submissions = []
    original_submit = MapOperator._submit_data_task

    def record_submit(
        operator: MapOperator, gen: Any, inputs: RefBundle, **kwargs: Any
    ) -> None:
        if operator.name == "LocalTopN(limit=3)":
            submissions.append(len(inputs.blocks))
        return original_submit(operator, gen, inputs, **kwargs)

    with (
        mock.patch("ray.data._internal.planner.top_n._LOCAL_TOP_N_MIN_BYTES", 32),
        mock.patch.object(MapOperator, "_submit_data_task", record_submit),
    ):
        result = (
            ray.data.from_arrow(blocks).sort("id", descending=True).limit(3).take_all()
        )

    assert result == [{"id": i} for i in range(offset - 1, offset - 4, -1)]
    assert submissions == expected_task_blocks


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
    result = ds.take_all()

    # A zero limit intentionally bypasses the Top-N rewrite.
    if limit > 0:
        assert isinstance(
            get_execution_plan(ds._logical_plan)[0].dag, TopNMergeOperator
        )

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
    physical_dag = get_execution_plan(ds._logical_plan)[0].dag

    assert result == [{"id": value} for value in range(4, -1, -1)]

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


@pytest.mark.parametrize("cpu_limit", [1, 2])
def test_top_n_merges_candidates_in_batches_under_memory_pressure(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    cpu_limit: int,
) -> None:
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    # Constrain the executor's logical budget, not the physical object store.
    ctx.execution_options.resource_limits = ExecutionResources(
        cpu=cpu_limit, object_store_memory=1024
    )
    ds = ray.data.range(100, override_num_blocks=20).sort("id", descending=True)

    submissions = []
    original_submit = TopNMergeOperator._submit_merge_task

    def record_submit(operator: TopNMergeOperator, batch: List[RefBundle]) -> None:
        submissions.append((operator._inputs_complete, len(batch)))
        original_submit(operator, batch)

    # Force one candidate block per batch so the merge runs 20 rounds.
    with (
        mock.patch(
            "ray.data._internal.execution.operators.top_n_operator._MERGE_BATCH_BYTES",
            1,
        ),
        mock.patch.object(TopNMergeOperator, "_submit_merge_task", record_submit),
    ):
        result = ds.limit(3).take_all()

    assert [size for _, size in submissions] == [1] * 20
    assert any(not complete for complete, _ in submissions)

    assert result == [{"id": 99}, {"id": 98}, {"id": 97}]


@pytest.mark.parametrize("failure", [None, "upstream", "downstream"])
def test_top_n_partial_batch_unblocks_waiting_upstream(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    failure,
) -> None:
    class Gate:
        def __init__(self) -> None:
            self._opened = asyncio.Event()

        async def wait(self) -> None:
            await asyncio.wait_for(self._opened.wait(), timeout=30)

        async def open(self) -> None:
            self._opened.set()

    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    # Flush the first row before waiting instead of buffering it with the next row.
    ctx.target_max_block_size = 1
    ctx.target_min_block_size = 1
    ctx.set_config(TOP_N_REWRITE_ENABLED_CONFIG_KEY, True)
    gate: Any = ray.remote(Gate).options(num_cpus=0).remote()
    submissions = []
    operators = []
    original_submit = TopNMergeOperator._submit_merge_task

    def produce(batch: pa.Table) -> Iterator[pa.Table]:
        yield pa.table({"id": [2]})
        # Waiting for upstream completion before submitting a merge would deadlock.
        ray.get(gate.wait.remote())
        if failure == "upstream":
            raise RuntimeError("upstream failed after first candidate")
        yield pa.table({"id": [1]})

    def fail_downstream(batch: pa.Table) -> pa.Table:
        raise RuntimeError("downstream failed after final merge")

    def record_submit(operator: TopNMergeOperator, batch: List[RefBundle]) -> None:
        submissions.append((operator._inputs_complete, len(batch)))
        operators.append(operator)
        gate.open.remote()
        original_submit(operator, batch)

    try:
        with mock.patch.object(TopNMergeOperator, "_submit_merge_task", record_submit):
            ds = (
                ray.data.range(1, override_num_blocks=1)
                .map_batches(produce, batch_size=None, batch_format="pyarrow")
                .sort("id")
                .limit(1)
            )
            if failure == "downstream":
                ds = ds.map_batches(fail_downstream, batch_format="pyarrow")
            if failure is None:
                result = ds.take_all()
                assert result == [{"id": 1}]
            else:
                with pytest.raises(RayTaskError, match=f"{failure} failed"):
                    ds.take_all()

        assert submissions[0] == (False, 1)

        for operator in operators:
            assert not operator.get_active_tasks()
            assert not operator.has_next()

            assert operator._merge.output is None
            assert not operator._merge.inputs

            assert operator.metrics.obj_store_mem_pending_task_inputs == 0
    finally:
        ray.kill(gate)


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

    assert len(top_n) == len(baseline) == 5
    assert top_n[:4] == baseline[:4]

    # NaNs tie at the cutoff; the two execution paths may select different rows.
    assert math.isnan(top_n[-1]["key"])
    assert top_n[-1]["row_id"] in {1, 3}


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
    tied_row_ids = {row["row_id"] for row in result[1:]}

    assert [row["key"] for row in result] == [0, 1, 1]
    assert result[0] == {"key": 0, "row_id": 0}

    # Any boundary-tied rows are valid, but the same input row cannot appear twice.
    assert tied_row_ids <= {1, 2, 3}
    assert len(tied_row_ids) == 2


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
