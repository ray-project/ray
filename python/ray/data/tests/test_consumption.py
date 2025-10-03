import logging
import math
import os
import random
import sys
import time
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data import Schema
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.datasource.csv_datasink import CSVDatasink
from ray.data._internal.datasource.csv_datasource import CSVDatasource
from ray.data._internal.datasource.range_datasource import RangeDatasource
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.data.dataset import Dataset, MaterializedDataset
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.conftest import (
    CoreExecutionMetrics,
    assert_core_execution_metrics_equals,
    get_initial_core_execution_metrics_snapshot,
)
from ray.data.tests.util import column_udf, extract_values
from ray.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_schema(ray_start_regular):
    last_snapshot = get_initial_core_execution_metrics_snapshot()

    ds2 = ray.data.range(10, override_num_blocks=10)
    ds3 = ds2.repartition(5)
    ds3 = ds3.materialize()
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(
            task_count={
                "ReadRange": 10,
                "reduce": 5,
            }
        ),
        last_snapshot,
    )

    ds4 = ds3.map(lambda x: {"a": "hi", "b": 1.0}).limit(5).repartition(1)
    ds4 = ds4.materialize()
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(
            task_count={
                "Map(<lambda>)": lambda count: count <= 5,
                "slice_fn": 1,
                "reduce": 1,
            }
        ),
        last_snapshot,
    )

    assert str(ds2) == "Dataset(num_rows=10, schema={id: int64})"
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(task_count={}), last_snapshot
    )

    assert (
        str(ds3) == "MaterializedDataset(num_blocks=5, num_rows=10, schema={id: int64})"
    )
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(task_count={}), last_snapshot
    )
    assert (
        str(ds4) == "MaterializedDataset(num_blocks=1, num_rows=5, "
        "schema={a: string, b: double})"
    )
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(task_count={}), last_snapshot
    )


def test_schema_no_execution(ray_start_regular):
    last_snapshot = get_initial_core_execution_metrics_snapshot()
    ds = ray.data.range(100, override_num_blocks=10)
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(task_count={}),
        last_snapshot,
    )
    # We do not kick off the read task by default.
    assert not ds._plan.has_started_execution
    schema = ds.schema()
    assert schema.names == ["id"]

    # Fetching the schema does not trigger execution, since
    # the schema is known beforehand for RangeDatasource.
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(task_count={}), last_snapshot
    )
    # Fetching the schema should not trigger execution of extra read tasks.
    assert not ds._plan.has_started_execution


def test_schema_cached(ray_start_regular):
    def check_schema_cached(ds, expected_task_count, last_snapshot):
        schema = ds.schema()
        last_snapshot = assert_core_execution_metrics_equals(
            CoreExecutionMetrics(expected_task_count), last_snapshot
        )
        assert schema.names == ["a"]
        cached_schema = ds.schema(fetch_if_missing=False)
        assert cached_schema is not None
        assert schema == cached_schema
        last_snapshot = assert_core_execution_metrics_equals(
            CoreExecutionMetrics({}), last_snapshot
        )
        return last_snapshot

    last_snapshot = get_initial_core_execution_metrics_snapshot()
    ds = ray.data.from_items([{"a": i} for i in range(100)], override_num_blocks=10)
    last_snapshot = check_schema_cached(ds, {}, last_snapshot)

    # Add a map_batches operator so that we are forced to compute the schema.
    ds = ds.map_batches(lambda x: x)
    last_snapshot = check_schema_cached(
        ds,
        {
            "MapBatches(<lambda>)": lambda count: count <= 5,
            "slice_fn": 1,
        },
        last_snapshot,
    )


def test_count(ray_start_regular):
    ds = ray.data.range(100, override_num_blocks=10)
    # We do not kick off the read task by default.
    assert not ds._plan.has_started_execution
    assert ds.count() == 100
    # Getting number of rows should not trigger execution of any read tasks
    # for ray.data.range(), as the number of rows is known beforehand.
    assert not ds._plan.has_started_execution

    assert_core_execution_metrics_equals(CoreExecutionMetrics(task_count={}))


def test_count_edge_case(ray_start_regular):
    # Test this edge case: https://github.com/ray-project/ray/issues/44509.
    ds = ray.data.range(10)
    ds.count()

    actual_count = ds.filter(lambda row: row["id"] % 2 == 0).count()

    assert actual_count == 5


def test_count_after_caching_after_execution(ray_start_regular):
    SCALE_FACTOR = 5
    FILE_ROW_COUNT = 150
    DS_ROW_COUNT = FILE_ROW_COUNT * SCALE_FACTOR
    paths = ["example://iris.csv"] * SCALE_FACTOR
    ds = ray.data.read_csv(paths)
    # Row count should be unknown before execution.
    assert "num_rows=?" in str(ds)
    # After iterating over bundles and completing execution, row count should be known.
    list(ds.iter_internal_ref_bundles())
    assert f"num_rows={DS_ROW_COUNT}" in str(ds)
    assert ds.count() == DS_ROW_COUNT
    assert ds._plan._snapshot_metadata_schema.metadata.num_rows == DS_ROW_COUNT


def test_limit_execution(ray_start_regular):
    last_snapshot = get_initial_core_execution_metrics_snapshot()
    override_num_blocks = 20
    ds = ray.data.range(100, override_num_blocks=override_num_blocks)

    # Add some delay to the output to prevent all tasks from finishing
    # immediately.
    def delay(row):
        time.sleep(0.1)
        return row

    ds = ds.map(delay)
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(task_count={}),
        last_snapshot=last_snapshot,
    )

    # During lazy execution, we should not execute too many more tasks than is
    # needed to produce the requested number of rows.
    for i in [1, 11]:
        assert extract_values("id", ds.limit(i).take(200)) == list(range(i))
        last_snapshot = assert_core_execution_metrics_equals(
            CoreExecutionMetrics(
                task_count={
                    "ReadRange->Map(delay)": lambda count: count
                    < override_num_blocks / 2,
                    "slice_fn": lambda count: count <= 1,
                }
            ),
            last_snapshot=last_snapshot,
        )

    # .materialize().limit() should only trigger execution once.
    ds = ray.data.range(100, override_num_blocks=20).materialize()
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(
            task_count={
                "ReadRange": 20,
            }
        ),
        last_snapshot=last_snapshot,
    )
    for i in [1, 10]:
        assert extract_values("id", ds.limit(i).take(200)) == list(range(i))
        assert_core_execution_metrics_equals(
            CoreExecutionMetrics(task_count={"slice_fn": lambda count: count <= 1}),
            last_snapshot=last_snapshot,
        )


def test_avoid_placement_group_capture(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def run():
        ds = ray.data.range(5)
        assert sorted(
            extract_values("id", ds.map(column_udf("id", lambda x: x + 1)).take())
        ) == [1, 2, 3, 4, 5]
        assert ds.count() == 5
        assert sorted(extract_values("id", ds.iter_rows())) == [0, 1, 2, 3, 4]

    pg = ray.util.placement_group([{"CPU": 1}])
    ray.get(
        run.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_capture_child_tasks=True
            )
        ).remote()
    )


@pytest.fixture
def remove_named_placement_groups():
    yield
    for info in ray.util.placement_group_table().values():
        if info["name"]:
            pg = ray.util.get_placement_group(info["name"])
            ray.util.remove_placement_group(pg)


def test_ray_remote_args_fn(shutdown_only, remove_named_placement_groups):
    ray.init()

    pg = ray.util.placement_group([{"CPU": 1}], name="test_pg")

    def ray_remote_args_fn():
        scheduling_strategy = PlacementGroupSchedulingStrategy(placement_group=pg)
        return {"scheduling_strategy": scheduling_strategy}

    class ActorClass:
        def __call__(self, batch):
            assert ray.util.get_current_placement_group() == pg
            return batch

    ray.data.range(1).map_batches(
        ActorClass, concurrency=1, ray_remote_args_fn=ray_remote_args_fn
    ).take_all()


def test_dataset_lineage_serialization(shutdown_only):
    ray.init()
    ds = ray.data.range(10)
    ds = ds.map(column_udf("id", lambda x: x + 1))
    ds = ds.map(column_udf("id", lambda x: x + 1))
    ds = ds.random_shuffle()
    uuid = ds._get_uuid()
    plan_uuid = ds._plan._dataset_uuid

    serialized_ds = ds.serialize_lineage()

    ray.shutdown()
    ray.init()

    ds = Dataset.deserialize_lineage(serialized_ds)
    # Check Dataset state.
    assert ds._get_uuid() == uuid
    assert ds._plan._dataset_uuid == plan_uuid
    # Check Dataset content.
    assert ds.count() == 10
    assert sorted(extract_values("id", ds.take())) == list(range(2, 12))


def test_dataset_lineage_serialization_unsupported(shutdown_only):
    ray.init()
    # In-memory data sources not supported.
    ds = ray.data.from_items(list(range(10)))
    ds = ds.map(column_udf("item", lambda x: x + 1))
    ds = ds.map(column_udf("item", lambda x: x + 1))

    with pytest.raises(ValueError):
        ds.serialize_lineage()

    # In-memory data source unions not supported.
    ds = ray.data.from_items(list(range(10)))
    ds1 = ray.data.from_items(list(range(10, 20)))
    ds2 = ds.union(ds1)

    with pytest.raises(ValueError):
        ds2.serialize_lineage()

    # Lazy read unions supported.
    ds = ray.data.range(10)
    ds1 = ray.data.range(20)
    ds2 = ds.union(ds1)

    serialized_ds = ds2.serialize_lineage()
    ds3 = Dataset.deserialize_lineage(serialized_ds)
    assert set(extract_values("id", ds3.take(30))) == set(
        list(range(10)) + list(range(20))
    )

    # Zips not supported.
    ds = ray.data.from_items(list(range(10)))
    ds1 = ray.data.from_items(list(range(10, 20)))
    ds2 = ds.zip(ds1)

    with pytest.raises(ValueError):
        ds2.serialize_lineage()


def test_basic(ray_start_regular_shared):
    ds = ray.data.range(5)
    assert sorted(
        extract_values("id", ds.map(column_udf("id", lambda x: x + 1)).take())
    ) == [1, 2, 3, 4, 5]
    assert ds.count() == 5
    assert sorted(extract_values("id", ds.iter_rows())) == [0, 1, 2, 3, 4]


def test_range(ray_start_regular_shared):
    ds = ray.data.range(10, override_num_blocks=10)
    assert ds._plan.initial_num_blocks() == 10
    assert ds.count() == 10
    assert ds.take() == [{"id": i} for i in range(10)]

    ds = ray.data.range(10, override_num_blocks=2)
    assert ds._plan.initial_num_blocks() == 2
    assert ds.count() == 10
    assert ds.take() == [{"id": i} for i in range(10)]


def test_empty_dataset(ray_start_regular_shared):
    ds = ray.data.range(0)
    assert ds.count() == 0
    assert ds.size_bytes() is None
    assert ds.schema() is None

    ds = ray.data.range(1)
    ds = ds.filter(lambda x: x["id"] > 1)
    ds = ds.materialize()
    assert (
        str(ds)
        == "MaterializedDataset(num_blocks=1, num_rows=0, schema=Unknown schema)"
    )

    # Test map on empty dataset.
    ds = ray.data.from_items([])
    ds = ds.map(lambda x: x)
    ds = ds.materialize()
    assert ds.count() == 0

    # Test filter on empty dataset.
    ds = ray.data.from_items([])
    ds = ds.filter(lambda: True)
    ds = ds.materialize()
    assert ds.count() == 0


def test_cache_dataset(ray_start_regular_shared):
    @ray.remote
    class Counter:
        def __init__(self):
            self.i = 0

        def inc(self):
            print("INC")
            self.i += 1
            return self.i

    c = Counter.remote()

    def inc(x):
        ray.get(c.inc.remote())
        return x

    ds = ray.data.range(1)
    ds = ds.map(inc)
    assert not isinstance(ds, MaterializedDataset)
    ds2 = ds.materialize()
    assert isinstance(ds2, MaterializedDataset)
    assert not isinstance(ds, MaterializedDataset)

    # Tests standard iteration uses the materialized blocks.
    for _ in range(10):
        ds2.take_all()

    assert ray.get(c.inc.remote()) == 2

    # Tests streaming iteration uses the materialized blocks.
    for _ in range(10):
        list(ds2.streaming_split(1)[0].iter_batches())

    assert ray.get(c.inc.remote()) == 3


def test_columns(ray_start_regular_shared):
    ds = ray.data.range(1)
    assert ds.columns() == ds.schema().names
    assert ds.columns() == ["id"]

    ds = ds.map(lambda x: x)
    assert ds.columns(fetch_if_missing=False) is None


def test_schema_repr(ray_start_regular_shared):
    ds = ray.data.from_items([{"text": "spam", "number": 0}])
    # fmt: off
    expected_repr = (
        "Column  Type\n"
        "------  ----\n"
        "text    string\n"
        "number  int64"
    )
    # fmt:on
    assert repr(ds.schema()) == expected_repr

    ds = ray.data.from_items([{"long_column_name": "spam"}])
    # fmt: off
    expected_repr = (
        "Column            Type\n"
        "------            ----\n"
        "long_column_name  string"
    )
    # fmt: on
    assert repr(ds.schema()) == expected_repr


def _check_none_computed(ds):
    # In streaming executor, ds.take() will not invoke partial execution
    # in LazyBlocklist.
    assert not ds._plan.has_started_execution


def test_lazy_loading_exponential_rampup(ray_start_regular_shared):
    ds = ray.data.range(100, override_num_blocks=20)
    _check_none_computed(ds)
    assert extract_values("id", ds.take(10)) == list(range(10))
    _check_none_computed(ds)
    assert extract_values("id", ds.take(20)) == list(range(20))
    _check_none_computed(ds)
    assert extract_values("id", ds.take(30)) == list(range(30))
    _check_none_computed(ds)
    assert extract_values("id", ds.take(50)) == list(range(50))
    _check_none_computed(ds)
    assert extract_values("id", ds.take(100)) == list(range(100))
    _check_none_computed(ds)


def test_dataset_repr(ray_start_regular_shared):
    ds = ray.data.range(10, override_num_blocks=10)
    assert repr(ds) == "Dataset(num_rows=10, schema={id: int64})"
    ds = ds.map_batches(lambda x: x)
    assert repr(ds) == (
        "MapBatches(<lambda>)\n+- Dataset(num_rows=10, schema={id: int64})"
    )
    ds = ds.filter(lambda x: x["id"] > 0)
    assert repr(ds) == (
        "Filter(<lambda>)\n"
        "+- MapBatches(<lambda>)\n"
        "   +- Dataset(num_rows=10, schema={id: int64})"
    )
    ds = ds.random_shuffle()
    assert repr(ds) == (
        "RandomShuffle\n"
        "+- Filter(<lambda>)\n"
        "   +- MapBatches(<lambda>)\n"
        "      +- Dataset(num_rows=10, schema={id: int64})"
    )
    ds = ds.materialize()
    assert (
        repr(ds) == "MaterializedDataset(num_blocks=10, num_rows=9, schema={id: int64})"
    )
    ds = ds.map_batches(lambda x: x)

    assert repr(ds) == (
        "MapBatches(<lambda>)\n+- Dataset(num_rows=9, schema={id: int64})"
    )
    ds1, ds2 = ds.split(2)
    assert (
        repr(ds1) == f"MaterializedDataset(num_blocks=5, num_rows={ds1.count()}, "
        "schema={id: int64})"
    )
    assert (
        repr(ds2) == f"MaterializedDataset(num_blocks=5, num_rows={ds2.count()}, "
        "schema={id: int64})"
    )

    # TODO(scottjlee): include all of the input datasets to union()
    # in the repr output, instead of only the resulting unioned dataset.
    # TODO(@bveeramani): Handle schemas for n-ary operators like `Union`.
    # ds3 = ds1.union(ds2)
    # assert repr(ds3) == ("Union\n+- Dataset(num_rows=9, schema={id: int64})")
    # ds = ds.zip(ds3)
    # assert repr(ds) == (
    #     "Zip\n"
    #     "+- MapBatches(<lambda>)\n"
    #     "+- Union\n"
    #     "   +- Dataset(num_rows=9, schema={id: int64})"
    # )

    def my_dummy_fn(x):
        return x

    ds = ray.data.range(10, override_num_blocks=10)
    ds = ds.map_batches(my_dummy_fn)
    assert repr(ds) == (
        "MapBatches(my_dummy_fn)\n+- Dataset(num_rows=10, schema={id: int64})"
    )


def test_dataset_explain(ray_start_regular_shared, capsys):
    ds = ray.data.range(10, override_num_blocks=10)
    ds = ds.map(lambda x: x)

    ds.explain()
    captured = capsys.readouterr()
    assert captured.out.rstrip() == (
        "-------- Logical Plan --------\n"
        "Map(<lambda>)\n"
        "+- ReadRange\n"
        "-------- Physical Plan --------\n"
        "TaskPoolMapOperator[ReadRange->Map(<lambda>)]\n"
        "+- InputDataBuffer[Input]"
    )

    ds = ds.filter(lambda x: x["id"] > 0)
    ds.explain()
    captured = capsys.readouterr()
    assert captured.out.rstrip() == (
        "-------- Logical Plan --------\n"
        "Filter(<lambda>)\n"
        "+- Map(<lambda>)\n"
        "   +- ReadRange\n"
        "-------- Physical Plan --------\n"
        "TaskPoolMapOperator[ReadRange->Map(<lambda>)->Filter(<lambda>)]\n"
        "+- InputDataBuffer[Input]"
    )
    ds = ds.random_shuffle().map(lambda x: x)
    ds.explain()
    captured = capsys.readouterr()
    assert captured.out.rstrip() == (
        "-------- Logical Plan --------\n"
        "Map(<lambda>)\n"
        "+- RandomShuffle\n"
        "   +- Filter(<lambda>)\n"
        "      +- Map(<lambda>)\n"
        "         +- ReadRange\n"
        "-------- Physical Plan --------\n"
        "TaskPoolMapOperator[Map(<lambda>)]\n"
        "+- AllToAllOperator[ReadRange->Map(<lambda>)->Filter(<lambda>)->RandomShuffle]\n"
        "   +- InputDataBuffer[Input]"
    )


@pytest.mark.parametrize("lazy", [False, True])
def test_limit(ray_start_regular_shared, lazy):
    ds = ray.data.range(100, override_num_blocks=20)
    if not lazy:
        ds = ds.materialize()
    for i in range(100):
        assert extract_values("id", ds.limit(i).take(200)) == list(range(i))


# NOTE: We test outside the power-of-2 range in order to ensure that we're not reading
# redundant files due to exponential ramp-up.
@pytest.mark.parametrize("limit", [10, 20, 30, 60])
def test_limit_no_redundant_read(
    ray_start_regular_shared,
    limit,
):
    # Test that dataset truncation eliminates redundant reads.
    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def increment(self):
            self.count += 1

        def get(self):
            return self.count

        def reset(self):
            self.count = 0

    class CountingRangeDatasource(Datasource):
        def __init__(self):
            self.counter = Counter.remote()

        def prepare_read(self, parallelism, n):
            def range_(i):
                ray.get(self.counter.increment.remote())
                return [
                    pd.DataFrame({"id": range(parallelism * i, parallelism * i + n)})
                ]

            return [
                ReadTask(
                    lambda i=i: range_(i),
                    BlockMetadata(
                        num_rows=n,
                        size_bytes=sum(
                            sys.getsizeof(i)
                            for i in range(parallelism * i, parallelism * i + n)
                        ),
                        input_files=None,
                        exec_stats=None,
                    ),
                    schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
                )
                for i in range(parallelism)
            ]

    source = CountingRangeDatasource()

    total_rows = 1000
    override_num_blocks = 100
    ds = ray.data.read_datasource(
        source,
        override_num_blocks=override_num_blocks,
        n=total_rows // override_num_blocks,
    )
    # Apply multiple limit ops.
    # Once the smallest limit is reached, the entire dataset should stop execution.
    ds = ds.limit(total_rows)
    ds = ds.limit(limit)
    ds = ds.limit(total_rows)
    # Check content.
    assert len(ds.take(limit)) == limit
    # Check number of read tasks launched.
    # min_read_tasks is the minimum number of read tasks needed for the limit.
    # We may launch more tasks than this number, in order to to maximize throughput.
    # But the actual number of read tasks should be less than the parallelism.
    count = ray.get(source.counter.get.remote())
    min_read_tasks = limit // (total_rows // override_num_blocks)
    assert min_read_tasks <= count < override_num_blocks


def test_limit_no_num_row_info(ray_start_regular_shared):
    # Test that datasources with no number-of-rows metadata available are still able to
    # be truncated, falling back to kicking off all read tasks.
    class DumbOnesDatasource(Datasource):
        def prepare_read(self, parallelism, n):
            return parallelism * [
                ReadTask(
                    lambda: [pd.DataFrame({"id": [1] * n})],
                    BlockMetadata(
                        num_rows=None,
                        size_bytes=sys.getsizeof(1) * n,
                        input_files=None,
                        exec_stats=None,
                    ),
                    schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
                )
            ]

    ds = ray.data.read_datasource(DumbOnesDatasource(), override_num_blocks=10, n=10)
    for i in range(1, 100):
        assert extract_values("id", ds.limit(i).take(100)) == [1] * i


def test_convert_types(ray_start_regular_shared):
    plain_ds = ray.data.range(1)
    arrow_ds = plain_ds.map(lambda x: {"a": x["id"]})
    assert arrow_ds.take() == [{"a": 0}]
    assert "dict" in str(arrow_ds.map(lambda x: {"out": str(type(x))}).take()[0])

    arrow_ds = ray.data.range(1)
    assert arrow_ds.map(lambda x: {"out": "plain_{}".format(x["id"])}).take() == [
        {"out": "plain_0"}
    ]
    assert arrow_ds.map(lambda x: {"a": (x["id"],)}).take() == [{"a": [0]}]


@pytest.mark.parametrize(
    "input_blocks",
    [
        [pd.DataFrame({"column": ["spam"]}), pd.DataFrame({"column": ["ham", "eggs"]})],
        [
            pa.Table.from_pydict({"column": ["spam"]}),
            pa.Table.from_pydict({"column": ["ham", "eggs"]}),
        ],
    ],
)
def test_from_blocks(input_blocks, ray_start_regular_shared):
    ds = ray.data.from_blocks(input_blocks)

    bundles = ds.iter_internal_ref_bundles()
    output_blocks = ray.get(_ref_bundles_iterator_to_block_refs_list(bundles))
    assert len(input_blocks) == len(output_blocks)
    assert all(
        input_block.equals(output_block)
        for input_block, output_block in zip(input_blocks, output_blocks)
    )


def test_from_items(ray_start_regular_shared):
    ds = ray.data.from_items(["hello", "world"])
    assert extract_values("item", ds.take()) == ["hello", "world"]
    assert isinstance(next(iter(ds.iter_batches(batch_format=None))), pa.Table)


@pytest.mark.parametrize("parallelism", list(range(1, 21)))
def test_from_items_parallelism(ray_start_regular_shared, parallelism):
    # Test that specifying parallelism yields the expected number of blocks.
    n = 20
    records = [{"a": i} for i in range(n)]
    ds = ray.data.from_items(records, override_num_blocks=parallelism)
    out = ds.take_all()
    assert out == records
    assert ds._plan.initial_num_blocks() == parallelism


def test_from_items_parallelism_truncated(ray_start_regular_shared):
    # Test that specifying parallelism greater than the number of items is truncated to
    # the number of items.
    n = 10
    parallelism = 20
    records = [{"a": i} for i in range(n)]
    ds = ray.data.from_items(records, override_num_blocks=parallelism)
    out = ds.take_all()
    assert out == records
    assert ds._plan.initial_num_blocks() == n


def test_take_batch(ray_start_regular_shared):
    ds = ray.data.range(10, override_num_blocks=2)
    assert ds.take_batch(3)["id"].tolist() == [0, 1, 2]
    assert ds.take_batch(6)["id"].tolist() == [0, 1, 2, 3, 4, 5]
    assert ds.take_batch(100)["id"].tolist() == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert isinstance(ds.take_batch(3, batch_format="pandas"), pd.DataFrame)
    assert isinstance(ds.take_batch(3, batch_format="numpy"), dict)

    ds = ray.data.range_tensor(10, override_num_blocks=2)
    assert np.all(ds.take_batch(3)["data"] == np.array([[0], [1], [2]]))
    assert isinstance(ds.take_batch(3, batch_format="pandas"), pd.DataFrame)
    assert isinstance(ds.take_batch(3, batch_format="numpy"), dict)

    with pytest.raises(ValueError):
        ray.data.range(0).take_batch()


def test_take_all(ray_start_regular_shared):
    assert extract_values("id", ray.data.range(5).take_all()) == [0, 1, 2, 3, 4]

    with pytest.raises(ValueError):
        assert ray.data.range(5).take_all(4)


def test_iter_rows(ray_start_regular_shared):
    # Test simple rows.
    n = 10
    ds = ray.data.range(n)
    for row, k in zip(ds.iter_rows(), range(n)):
        assert row == {"id": k}

    # Test tabular rows.
    t1 = pa.Table.from_pydict({"one": [1, 2, 3], "two": [2, 3, 4]})
    t2 = pa.Table.from_pydict({"one": [4, 5, 6], "two": [5, 6, 7]})
    t3 = pa.Table.from_pydict({"one": [7, 8, 9], "two": [8, 9, 10]})
    t4 = pa.Table.from_pydict({"one": [10, 11, 12], "two": [11, 12, 13]})
    ts = [t1, t2, t3, t4]
    t = pa.concat_tables(ts)
    ds = ray.data.from_arrow(ts)

    def to_pylist(table):
        pydict = table.to_pydict()
        names = table.schema.names
        pylist = [
            {column: pydict[column][row] for column in names}
            for row in range(table.num_rows)
        ]
        return pylist

    # Default ArrowRows.
    for row, t_row in zip(ds.iter_rows(), to_pylist(t)):
        assert isinstance(row, dict)
        assert row == t_row

    # PandasRows after conversion.
    pandas_ds = ds.map_batches(lambda x: x, batch_format="pandas")
    df = t.to_pandas()
    for row, (index, df_row) in zip(pandas_ds.iter_rows(), df.iterrows()):
        assert isinstance(row, dict)
        assert row == df_row.to_dict()


def test_iter_batches_basic(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": [5, 6, 7]})
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": [8, 9, 10]})
    df4 = pd.DataFrame({"one": [10, 11, 12], "two": [11, 12, 13]})
    dfs = [df1, df2, df3, df4]
    ds = ray.data.from_blocks(dfs)

    # Default.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="pandas"), dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    # pyarrow.Table format.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="pyarrow"), dfs):
        assert isinstance(batch, pa.Table)
        assert batch.equals(pa.Table.from_pandas(df))

    # NumPy format.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Test NumPy format on Arrow blocks.
    ds2 = ds.map_batches(lambda b: b, batch_size=None, batch_format="pyarrow")
    for batch, df in zip(ds2.iter_batches(batch_size=None, batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Default format -> numpy.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="default"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Batch size.
    batch_size = 2
    batches = list(ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Batch size larger than block.
    batch_size = 4
    batches = list(ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Batch size larger than dataset.
    batch_size = 15
    batches = list(ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == ds.count() for batch in batches)
    assert len(batches) == 1
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Batch size drop partial.
    batch_size = 5
    batches = list(
        ds.iter_batches(batch_size=batch_size, drop_last=True, batch_format="pandas")
    )
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == (len(df1) + len(df2) + len(df3) + len(df4)) // batch_size
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)[:10]
    )

    # Batch size don't drop partial.
    batch_size = 5
    batches = list(
        ds.iter_batches(batch_size=batch_size, drop_last=False, batch_format="pandas")
    )
    assert all(len(batch) == batch_size for batch in batches[:-1])
    assert len(batches[-1]) == (len(df1) + len(df2) + len(df3) + len(df4)) % batch_size
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Prefetch.
    batches = list(
        ds.iter_batches(prefetch_batches=1, batch_size=None, batch_format="pandas")
    )
    assert len(batches) == len(dfs)
    for batch, df in zip(batches, dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    batch_size = 2
    batches = list(
        ds.iter_batches(
            prefetch_batches=2, batch_size=batch_size, batch_format="pandas"
        )
    )
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Prefetch more than number of blocks.
    batches = list(
        ds.iter_batches(
            prefetch_batches=len(dfs), batch_size=None, batch_format="pandas"
        )
    )
    assert len(batches) == len(dfs)
    for batch, df in zip(batches, dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    # Prefetch with ray.wait.
    context = DataContext.get_current()
    old_config = context.actor_prefetcher_enabled
    try:
        context.actor_prefetcher_enabled = False
        batches = list(
            ds.iter_batches(prefetch_batches=1, batch_size=None, batch_format="pandas")
        )
        assert len(batches) == len(dfs)
        for batch, df in zip(batches, dfs):
            assert isinstance(batch, pd.DataFrame)
            assert batch.equals(df)
    finally:
        context.actor_prefetcher_enabled = old_config


def test_iter_batches_empty_block(ray_start_regular_shared):
    ds = ray.data.range(1).repartition(10)
    assert str(list(ds.iter_batches(batch_size=None))) == "[{'id': array([0])}]"
    assert (
        str(list(ds.iter_batches(batch_size=1, local_shuffle_buffer_size=1)))
        == "[{'id': array([0])}]"
    )


@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_iter_batches_local_shuffle(shutdown_only, ds_format):
    # Input validation.
    # Batch size must be given for local shuffle.
    with pytest.raises(ValueError):
        list(
            ray.data.range(100).iter_batches(
                batch_size=None, local_shuffle_buffer_size=10
            )
        )

    def range(n, parallelism=200):
        if ds_format == "arrow":
            ds = ray.data.range(n, override_num_blocks=parallelism)
        elif ds_format == "pandas":
            ds = ray.data.range(n, override_num_blocks=parallelism).map_batches(
                lambda df: df, batch_size=None, batch_format="pandas"
            )
        return ds

    def to_row_dicts(batch):
        if isinstance(batch, pd.DataFrame):
            return batch.to_dict(orient="records")
        return [{"id": v} for v in batch["id"]]

    def unbatch(batches):
        return [r for batch in batches for r in to_row_dicts(batch)]

    def sort(r):
        return sorted(r, key=lambda v: v["id"])

    base = range(100).take_all()

    # Local shuffle.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Set seed.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
            local_shuffle_seed=0,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
            local_shuffle_seed=0,
        )
    )
    # Check randomness of shuffle.
    assert r1 == r2, (r1, r2)
    assert r1 != base
    # Check content.
    assert sort(r1) == sort(base)

    # Single block.
    r1 = unbatch(
        range(100, parallelism=1).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=1).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Single-row blocks.
    r1 = unbatch(
        range(100, parallelism=100).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=100).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Buffer larger than dataset.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=200,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=200,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Batch size larger than block.
    r1 = unbatch(
        range(100, parallelism=20).iter_batches(
            batch_size=12,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=20).iter_batches(
            batch_size=12,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Batch size larger than dataset.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=200,
            local_shuffle_buffer_size=400,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=200,
            local_shuffle_buffer_size=400,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Drop partial batches.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=7,
            local_shuffle_buffer_size=21,
            drop_last=True,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=7,
            local_shuffle_buffer_size=21,
            drop_last=True,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    # Check that partial batches were dropped.
    assert len(r1) % 7 == 0
    assert len(r2) % 7 == 0
    tmp_base = base
    if ds_format in ("arrow", "pandas"):
        r1 = [tuple(r.items()) for r in r1]
        r2 = [tuple(r.items()) for r in r2]
        tmp_base = [tuple(r.items()) for r in base]
    assert set(r1) <= set(tmp_base)
    assert set(r2) <= set(tmp_base)

    # Test empty dataset.
    ds = ray.data.from_items([])
    r1 = unbatch(ds.iter_batches(batch_size=2, local_shuffle_buffer_size=10))
    assert len(r1) == 0
    assert r1 == ds.take()


def test_iter_batches_grid(ray_start_regular_shared):
    # Tests slicing, batch combining, and partial batch dropping logic over
    # a grid of dataset, batching, and dropping configurations.
    # Grid: num_blocks x num_rows_block_1 x ... x num_rows_block_N x
    #       batch_size x drop_last
    seed = int(time.time())
    print(f"Seeding RNG for test_iter_batches_grid with: {seed}")
    random.seed(seed)
    max_num_blocks = 20
    max_num_rows_per_block = 20
    num_blocks_samples = 3
    block_sizes_samples = 3
    batch_size_samples = 3

    for num_blocks in np.random.randint(1, max_num_blocks + 1, size=num_blocks_samples):
        block_sizes_list = [
            np.random.randint(1, max_num_rows_per_block + 1, size=num_blocks)
            for _ in range(block_sizes_samples)
        ]
        for block_sizes in block_sizes_list:
            # Create the dataset with the given block sizes.
            dfs = []
            running_size = 0
            for block_size in block_sizes:
                dfs.append(
                    pd.DataFrame(
                        {"value": list(range(running_size, running_size + block_size))}
                    )
                )
                running_size += block_size
            num_rows = running_size
            ds = ray.data.from_blocks(dfs)
            for batch_size in np.random.randint(
                1, num_rows + 1, size=batch_size_samples
            ):
                for drop_last in (False, True):
                    batches = list(
                        ds.iter_batches(
                            batch_size=batch_size,
                            drop_last=drop_last,
                            batch_format="pandas",
                        )
                    )
                    if num_rows % batch_size == 0 or not drop_last:
                        # Number of batches should be equal to
                        # num_rows / batch_size,  rounded up.
                        assert len(batches) == math.ceil(num_rows / batch_size)
                        # Concatenated batches should equal the DataFrame
                        # representation of the entire dataset.
                        assert pd.concat(batches, ignore_index=True).equals(
                            ds.to_pandas()
                        )
                    else:
                        # Number of batches should be equal to
                        # num_rows / batch_size, rounded down.
                        assert len(batches) == num_rows // batch_size
                        # Concatenated batches should equal the DataFrame
                        # representation of the dataset with the partial batch
                        # remainder sliced off.
                        assert pd.concat(batches, ignore_index=True).equals(
                            ds.to_pandas()[: batch_size * (num_rows // batch_size)]
                        )
                    if num_rows % batch_size == 0 or drop_last:
                        assert all(len(batch) == batch_size for batch in batches)
                    else:
                        assert all(len(batch) == batch_size for batch in batches[:-1])
                        assert len(batches[-1]) == num_rows % batch_size


def test_union(ray_start_regular_shared):
    ds = ray.data.range(20, override_num_blocks=10).materialize()

    # Test lazy union.
    ds = ds.union(ds, ds, ds, ds)
    assert ds._plan.initial_num_blocks() == 50
    assert ds.count() == 100
    assert ds.sum() == 950

    ds = ds.union(ds)
    assert ds.count() == 200
    assert ds.sum() == (950 * 2)

    # Test materialized union.
    ds2 = ray.data.from_items([1, 2, 3, 4, 5])
    assert ds2.count() == 5
    ds2 = ds2.union(ds2)
    assert ds2.count() == 10
    ds2 = ds2.union(ds)
    assert ds2.count() == 210


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="No tensorflow for Python 3.12+"
)
def test_iter_tf_batches_emits_deprecation_warning(ray_start_regular_shared):
    with pytest.warns(DeprecationWarning):
        ray.data.range(1).iter_tf_batches()


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="No tensorflow for Python 3.12+"
)
def test_iter_tf_batches(ray_start_regular_shared):
    df1 = pd.DataFrame(
        {"one": [1, 2, 3], "two": [1.0, 2.0, 3.0], "label": [1.0, 2.0, 3.0]}
    )
    df2 = pd.DataFrame(
        {"one": [4, 5, 6], "two": [4.0, 5.0, 6.0], "label": [4.0, 5.0, 6.0]}
    )
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.data.from_pandas([df1, df2, df3])

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_tf_batches(batch_size=3):
            iterations.append(
                np.stack((batch["one"], batch["two"], batch["label"]), axis=1)
            )
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(np.sort(df.values), np.sort(combined_iterations))


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="No tensorflow for Python 3.12+"
)
def test_iter_tf_batches_tensor_ds(ray_start_regular_shared):
    arr1 = np.arange(12).reshape((3, 2, 2))
    arr2 = np.arange(12, 24).reshape((3, 2, 2))
    arr = np.concatenate((arr1, arr2))
    ds = ray.data.from_numpy([arr1, arr2])

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_tf_batches(batch_size=2):
            iterations.append(batch["data"])
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(arr, combined_iterations)


def test_block_builder_for_block(ray_start_regular_shared):
    # pandas dataframe
    builder = BlockBuilder.for_block(pd.DataFrame())
    b1 = pd.DataFrame({"A": [1], "B": ["a"]})
    builder.add_block(b1)
    assert builder.build().equals(b1)
    b2 = pd.DataFrame({"A": [2, 3], "B": ["c", "d"]})
    builder.add_block(b2)
    expected = pd.DataFrame({"A": [1, 2, 3], "B": ["a", "c", "d"]})
    assert builder.build().equals(expected)

    # pyarrow table
    builder = BlockBuilder.for_block(pa.Table.from_arrays(list()))
    b1 = pa.Table.from_pydict({"A": [1], "B": ["a"]})
    builder.add_block(b1)
    builder.build().equals(b1)
    b2 = pa.Table.from_pydict({"A": [2, 3], "B": ["c", "d"]})
    builder.add_block(b2)
    expected = pa.Table.from_pydict({"A": [1, 2, 3], "B": ["a", "c", "d"]})
    builder.build().equals(expected)

    # wrong type
    with pytest.raises(TypeError):
        BlockBuilder.for_block(str())


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_min(ray_start_regular_shared, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_min with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global min aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.min("A") == 0

    # Test empty dataset
    # Note: we explicitly set parallelism here to ensure there are no empty
    # input blocks.
    ds = ray.data.range(10, override_num_blocks=10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["id"] > 10).min("id") is None

    # Test built-in global min aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.min("A") == 0
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.min("A", ignore_nulls=False))
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert pd.isnull(nan_ds.min("A"))
    assert pd.isnull(nan_ds.min("A", ignore_nulls=False))


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_max(ray_start_regular_shared, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_max with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global max aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.max("A") == 99

    # Test empty dataset
    # Note: we explicitly set parallelism here to ensure there are no empty
    # input blocks.
    ds = ray.data.range(10, override_num_blocks=10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["id"] > 10).max("id") is None

    # Test built-in global max aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.max("A") == 99
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.max("A", ignore_nulls=False))
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert pd.isnull(nan_ds.max("A"))
    assert pd.isnull(nan_ds.max("A", ignore_nulls=False))


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_mean(ray_start_regular_shared, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_mean with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global mean aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.mean("A") == 49.5

    # Test empty dataset
    # Note: we explicitly set parallelism here to ensure there are no empty
    # input blocks.
    ds = ray.data.range(10, override_num_blocks=10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["id"] > 10).mean("id") is None

    # Test built-in global mean aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.mean("A") == 49.5
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.mean("A", ignore_nulls=False))
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert pd.isnull(nan_ds.mean("A"))
    assert pd.isnull(nan_ds.mean("A", ignore_nulls=False))


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_std(ray_start_regular_shared, ds_format, num_parts):
    # NOTE: Do not change the seed
    seed = 1740035705

    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_arrow(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pyarrow")

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global max aggregation
    df = pd.DataFrame({"A": xs})
    ds = ray.data.from_pandas(df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    assert math.isclose(ds.std("A"), df["A"].std())
    assert math.isclose(ds.std("A", ddof=0), df["A"].std(ddof=0))

    # Test empty dataset
    ds = ray.data.from_pandas(pd.DataFrame({"A": []}))
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    assert pd.isnull(ds.std("A"))
    # Test edge cases
    ds = ray.data.from_pandas(pd.DataFrame({"A": [3]}))
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    assert np.isnan(ds.std("A"))

    # Test built-in global std aggregation with nans
    nan_df = pd.DataFrame({"A": xs + [None]})
    nan_ds = ray.data.from_pandas(nan_df).repartition(num_parts)
    if ds_format == "arrow":
        nan_ds = _to_arrow(nan_ds)
    assert math.isclose(nan_ds.std("A"), nan_df["A"].std())
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.std("A", ignore_nulls=False))
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert pd.isnull(nan_ds.std("A"))
    assert pd.isnull(nan_ds.std("A", ignore_nulls=False))


def test_column_name_type_check(ray_start_regular_shared):
    df = pd.DataFrame({"1": np.random.rand(10), "a": np.random.rand(10)})
    ds = ray.data.from_pandas(df)
    assert ds.schema() == Schema(pa.schema([("1", pa.float64()), ("a", pa.float64())]))
    assert ds.count() == 10

    df = pd.DataFrame({1: np.random.rand(10), "a": np.random.rand(10)})
    with pytest.raises(ValueError):
        ray.data.from_pandas(df)


def test_len(ray_start_regular_shared):
    ds = ray.data.range(1)
    with pytest.raises(AttributeError):
        len(ds)


def test_pandas_block_select():
    df = pd.DataFrame({"one": [10, 11, 12], "two": [11, 12, 13], "three": [14, 15, 16]})
    block_accessor = BlockAccessor.for_block(df)

    block = block_accessor.select(["two"])
    assert block.equals(df[["two"]])

    block = block_accessor.select(["two", "one"])
    assert block.equals(df[["two", "one"]])

    with pytest.raises(ValueError):
        block = block_accessor.select([lambda x: x % 3, "two"])


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="TODO(scottjlee): Not working yet for py312"
)
def test_unsupported_pyarrow_versions_check(shutdown_only):
    ray.shutdown()

    # Test that unsupported pyarrow versions cause an error to be raised upon the
    # initial pyarrow use.
    ray.init(runtime_env={"pip": ["pyarrow==8.0.0"]})

    @ray.remote
    def should_error():
        _check_pyarrow_version()

    with pytest.raises(
        Exception,
        match=r".*Dataset requires pyarrow >= 9.0.0, but 8.0.0 is installed.*",
    ):
        ray.get(should_error.remote())


def test_read_write_local_node_ray_client(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=4)
    cluster.head_node._ray_params.ray_client_server_port = "10004"
    cluster.head_node.start_ray_client_server()
    address = "ray://localhost:10004"

    import tempfile

    data_path = tempfile.mkdtemp()
    df = pd.DataFrame({"one": list(range(0, 10)), "two": list(range(10, 20))})
    path = os.path.join(data_path, "test.parquet")
    df.to_parquet(path)

    # Read/write from Ray Client will result in error.
    ray.init(address)
    with pytest.raises(ValueError):
        ds = ray.data.read_parquet("local://" + path).materialize()
    ds = ray.data.from_pandas(df)
    with pytest.raises(ValueError):
        ds.write_parquet("local://" + data_path).materialize()


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="No tensorflow for Python 3.12+"
)
def test_read_warning_large_parallelism(ray_start_regular, propagate_logs, caplog):
    with caplog.at_level(logging.WARNING, logger="ray.data.read_api"):
        ray.data.range(5000, override_num_blocks=5000).materialize()
    assert (
        "The requested number of read blocks of 5000 is "
        "more than 4x the number of available CPU slots in the cluster" in caplog.text
    ), caplog.text


def test_read_write_local_node(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"bar:1": 100},
        num_cpus=10,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(resources={"bar:2": 100}, num_cpus=10)
    cluster.add_node(resources={"bar:3": 100}, num_cpus=10)

    ray.shutdown()
    ray.init(cluster.address)

    import os
    import tempfile

    data_path = tempfile.mkdtemp()
    num_files = 5
    for idx in range(num_files):
        df = pd.DataFrame(
            {"one": list(range(idx, idx + 10)), "two": list(range(idx + 10, idx + 20))}
        )
        path = os.path.join(data_path, f"test{idx}.parquet")
        df.to_parquet(path)

    ctx = ray.data.context.DataContext.get_current()
    ctx.read_write_local_node = True

    def check_dataset_is_local(ds):
        bundles = ds.iter_internal_ref_bundles()
        block_refs = _ref_bundles_iterator_to_block_refs_list(bundles)
        ray.wait(block_refs, num_returns=len(block_refs), fetch_local=False)
        location_data = ray.experimental.get_object_locations(block_refs)
        locations = []
        for block in block_refs:
            locations.extend(location_data[block]["node_ids"])
        assert set(locations) == {ray.get_runtime_context().get_node_id()}

    local_path = "local://" + data_path
    # Plain read.
    ds = ray.data.read_parquet(local_path).materialize()
    check_dataset_is_local(ds)

    # SPREAD scheduling got overridden when read local scheme.
    ds = ray.data.read_parquet(
        local_path, ray_remote_args={"scheduling_strategy": "SPREAD"}
    ).materialize()
    check_dataset_is_local(ds)

    # With fusion.
    ds = (
        ray.data.read_parquet(local_path, override_num_blocks=1)
        .map(lambda x: x)
        .materialize()
    )
    check_dataset_is_local(ds)

    # Write back to local scheme.
    output = os.path.join(local_path, "test_read_write_local_node")
    ds.write_parquet(output)
    assert "1 nodes used" in ds.stats(), ds.stats()
    ray.data.read_parquet(output).take_all() == ds.take_all()

    # Mixing paths of local and non-local scheme is invalid.
    with pytest.raises(ValueError):
        ds = ray.data.read_parquet(
            [local_path + "/test1.parquet", data_path + "/test2.parquet"]
        ).materialize()
    with pytest.raises(ValueError):
        ds = ray.data.read_parquet(
            [local_path + "/test1.parquet", "example://iris.parquet"]
        ).materialize()
    with pytest.raises(ValueError):
        ds = ray.data.read_parquet(
            ["example://iris.parquet", local_path + "/test1.parquet"]
        ).materialize()


@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value


class FlakyCSVDatasource(CSVDatasource):
    def __init__(self, paths, **csv_datasource_kwargs):
        super().__init__(paths, **csv_datasource_kwargs)

        self.counter = Counter.remote()

    def _read_stream(self, f: "pa.NativeFile", path: str):
        count = self.counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError("oops")
        else:
            for block in CSVDatasource._read_stream(self, f, path):
                yield block


class FlakyCSVDatasink(CSVDatasink):
    def __init__(self, path, **csv_datasink_kwargs):
        super().__init__(path, **csv_datasink_kwargs)

        self.counter = Counter.remote()

    def write_block_to_file(self, block: BlockAccessor, file):
        count = self.counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError("oops")
        else:
            super().write_block_to_file(block, file)


def test_datasource(ray_start_regular):
    source = ray.data.datasource.RandomIntRowDatasource(n=10, num_columns=2)
    assert len(ray.data.read_datasource(source).take()) == 10
    source = RangeDatasource(n=10)
    assert extract_values(
        "value",
        ray.data.read_datasource(source).take(),
    ) == list(range(10))


@pytest.mark.skip(reason="")
def test_polars_lazy_import(shutdown_only):
    import sys

    ctx = ray.data.context.DataContext.get_current()

    try:
        original_use_polars = ctx.use_polars
        ctx.use_polars = True

        num_items = 100
        parallelism = 4
        ray.init(num_cpus=4)

        @ray.remote
        def f(should_import_polars):
            # Sleep to spread the tasks.
            time.sleep(1)
            polars_imported = "polars" in sys.modules.keys()
            return polars_imported == should_import_polars

        # We should not use polars for non-Arrow sort.
        _ = ray.data.range(num_items, override_num_blocks=parallelism).sort()
        assert all(ray.get([f.remote(False) for _ in range(parallelism)]))

        a = range(100)
        dfs = []
        partition_size = num_items // parallelism
        for i in range(parallelism):
            dfs.append(
                pd.DataFrame({"a": a[i * partition_size : (i + 1) * partition_size]})
            )
        # At least one worker should have imported polars.
        _ = (
            ray.data.from_pandas(dfs)
            .map_batches(lambda t: t, batch_format="pyarrow", batch_size=None)
            .sort(key="a")
            .materialize()
        )
        assert any(ray.get([f.remote(True) for _ in range(parallelism)]))

    finally:
        ctx.use_polars = original_use_polars


def test_batch_formats(shutdown_only):
    ds = ray.data.range(100)
    assert isinstance(next(iter(ds.iter_batches(batch_format=None))), pa.Table)
    assert isinstance(next(iter(ds.iter_batches(batch_format="default"))), dict)
    assert isinstance(next(iter(ds.iter_batches(batch_format="pandas"))), pd.DataFrame)
    assert isinstance(next(iter(ds.iter_batches(batch_format="pyarrow"))), pa.Table)
    assert isinstance(next(iter(ds.iter_batches(batch_format="numpy"))), dict)

    ds = ray.data.range_tensor(100)
    assert isinstance(next(iter(ds.iter_batches(batch_format=None))), pa.Table)
    assert isinstance(next(iter(ds.iter_batches(batch_format="default"))), dict)
    assert isinstance(next(iter(ds.iter_batches(batch_format="pandas"))), pd.DataFrame)
    assert isinstance(next(iter(ds.iter_batches(batch_format="pyarrow"))), pa.Table)
    assert isinstance(next(iter(ds.iter_batches(batch_format="numpy"))), dict)

    df = pd.DataFrame({"foo": ["a", "b"], "bar": [0, 1]})
    ds = ray.data.from_pandas(df)
    assert isinstance(next(iter(ds.iter_batches(batch_format=None))), pd.DataFrame)
    assert isinstance(next(iter(ds.iter_batches(batch_format="default"))), dict)
    assert isinstance(next(iter(ds.iter_batches(batch_format="pandas"))), pd.DataFrame)
    assert isinstance(next(iter(ds.iter_batches(batch_format="pyarrow"))), pa.Table)
    assert isinstance(next(iter(ds.iter_batches(batch_format="numpy"))), dict)


def test_dataset_schema_after_read_stats(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(cluster.address)
    cluster.add_node(num_cpus=1, resources={"foo": 1})
    ds = ray.data.read_csv(
        "example://iris.csv", ray_remote_args={"resources": {"foo": 1}}
    )
    schema = ds.schema()
    ds.stats()
    assert schema == ds.schema()


def test_dataset_plan_as_string(ray_start_cluster):
    ds = ray.data.read_parquet("example://iris.parquet", override_num_blocks=8)
    assert ds._plan.get_plan_as_string(type(ds)) == (
        "Dataset(\n"
        "   num_rows=?,\n"
        "   schema={\n"
        "      sepal.length: double,\n"
        "      sepal.width: double,\n"
        "      petal.length: double,\n"
        "      petal.width: double,\n"
        "      variety: string\n"
        "   }\n"
        ")"
    )
    for _ in range(5):
        ds = ds.map_batches(lambda x: x)
    assert ds._plan.get_plan_as_string(type(ds)) == (
        "MapBatches(<lambda>)\n"
        "+- MapBatches(<lambda>)\n"
        "   +- MapBatches(<lambda>)\n"
        "      +- MapBatches(<lambda>)\n"
        "         +- MapBatches(<lambda>)\n"
        "            +- Dataset(\n"
        "                  num_rows=?,\n"
        "                  schema={\n"
        "                     sepal.length: double,\n"
        "                     sepal.width: double,\n"
        "                     petal.length: double,\n"
        "                     petal.width: double,\n"
        "                     variety: string\n"
        "                  }\n"
        "               )"
    )


class LoggerWarningCalled(Exception):
    """Custom exception used in test_warning_execute_with_no_cpu() and
    test_nowarning_execute_with_cpu(). Raised when the `logger.warning` method
    is called, so that we can kick out of `plan.execute()` by catching this Exception
    and check logging was done properly."""

    pass


def test_warning_execute_with_no_cpu(ray_start_cluster):
    """Tests ExecutionPlan.execute() to ensure a warning is logged
    when no CPU resources are available."""
    # Create one node with no CPUs to trigger the Dataset warning
    ray.init(ray_start_cluster.address)
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)

    try:
        ds = ray.data.range(10)
        ds = ds.map_batches(lambda x: x)
        ds.take()
    except Exception as e:
        assert isinstance(e, ValueError)
        assert "exceeds the execution limits ExecutionResources(cpu=0.0" in str(e)


def test_nowarning_execute_with_cpu(ray_start_cluster):
    """Tests ExecutionPlan.execute() to ensure no warning is logged
    when there are available CPU resources."""
    # Create one node with CPUs to avoid triggering the Dataset warning
    ray.init(ray_start_cluster.address)

    logger = logging.getLogger("ray.data._internal.plan")
    with patch.object(
        logger,
        "warning",
        side_effect=LoggerWarningCalled,
    ) as mock_logger:
        ds = ray.data.range(10)
        ds = ds.map_batches(lambda x: x)
        ds.take()
        mock_logger.assert_not_called()


def test_per_task_row_limit_basic(ray_start_regular_shared, restore_data_context):
    """Test basic per-block limiting functionality."""
    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Simple test that should work with the existing range datasource
    ds = ray.data.range(1000, override_num_blocks=10).limit(50)
    result = ds.take_all()

    # Verify we get the correct results
    assert len(result) == 50
    assert [row["id"] for row in result] == list(range(50))


def test_per_task_row_limit_with_custom_readtask(ray_start_regular_shared):
    """Test per-block limiting directly with ReadTask implementation."""

    def read_data_with_limit():
        # This simulates a ReadTask that reads 200 rows
        return [pd.DataFrame({"id": range(200)})]

    # Create ReadTask with per-block limit
    task_with_limit = ReadTask(
        read_fn=read_data_with_limit,
        metadata=BlockMetadata(
            num_rows=200, size_bytes=1600, input_files=None, exec_stats=None
        ),
        schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
        per_task_row_limit=50,
    )

    # Execute the ReadTask
    result_blocks = list(task_with_limit())

    # Should get only 50 rows due to per-block limiting
    assert len(result_blocks) == 1
    assert len(result_blocks[0]) == 50
    assert result_blocks[0]["id"].tolist() == list(range(50))


def test_per_task_row_limit_multiple_blocks_per_task(ray_start_regular_shared):
    """Test per-block limiting when ReadTasks return multiple blocks."""

    def read_multiple_blocks_with_limit():
        # This simulates a ReadTask that returns 3 blocks of 30 rows each
        return [
            pd.DataFrame({"id": range(0, 30)}),
            pd.DataFrame({"id": range(30, 60)}),
            pd.DataFrame({"id": range(60, 90)}),
        ]

    # Create ReadTask with per-block limit of 70 (should get 2.33 blocks)
    task = ReadTask(
        read_fn=read_multiple_blocks_with_limit,
        metadata=BlockMetadata(
            num_rows=90, size_bytes=720, input_files=None, exec_stats=None
        ),
        schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
        per_task_row_limit=70,
    )

    result_blocks = list(task())

    # Should get first 2 full blocks (60 rows) plus 10 rows from third block
    total_rows = sum(len(block) for block in result_blocks)
    assert total_rows == 70

    # Verify the data is correct
    all_ids = []
    for block in result_blocks:
        all_ids.extend(block["id"].tolist())
    assert all_ids == list(range(70))


def test_per_task_row_limit_larger_than_data(
    ray_start_regular_shared, restore_data_context
):
    """Test per-block limiting when limit is larger than available data."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    total_rows = 50
    ds = ray.data.range(total_rows, override_num_blocks=5)
    limited_ds = ds.limit(100)  # Limit larger than data
    result = limited_ds.take_all()

    assert len(result) == total_rows
    assert [row["id"] for row in result] == list(range(total_rows))


def test_per_task_row_limit_exact_block_boundary(
    ray_start_regular_shared, restore_data_context
):
    """Test per-block limiting when limit exactly matches block boundaries."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    rows_per_block = 20
    num_blocks = 5
    limit = rows_per_block * 2  # Exactly 2 blocks

    ds = ray.data.range(rows_per_block * num_blocks, override_num_blocks=num_blocks)
    limited_ds = ds.limit(limit)
    result = limited_ds.take_all()

    assert len(result) == limit
    assert [row["id"] for row in result] == list(range(limit))


@pytest.mark.parametrize("limit", [1, 5, 10, 25, 50, 99])
def test_per_task_row_limit_various_sizes(
    ray_start_regular_shared, limit, restore_data_context
):
    """Test per-block limiting with various limit sizes."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    total_rows = 100
    num_blocks = 10

    ds = ray.data.range(total_rows, override_num_blocks=num_blocks)
    limited_ds = ds.limit(limit)
    result = limited_ds.take_all()

    expected_len = min(limit, total_rows)
    assert len(result) == expected_len
    assert [row["id"] for row in result] == list(range(expected_len))


def test_per_task_row_limit_with_transformations(
    ray_start_regular_shared, restore_data_context
):
    """Test that per-block limiting works correctly with transformations."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Test with map operation after limit
    ds = ray.data.range(100, override_num_blocks=10)
    limited_ds = ds.limit(20).map(lambda x: {"doubled": x["id"] * 2})
    result = limited_ds.take_all()

    assert len(result) == 20
    assert [row["doubled"] for row in result] == [i * 2 for i in range(20)]

    # Test with map operation before limit
    ds = ray.data.range(100, override_num_blocks=10)
    limited_ds = ds.map(lambda x: {"doubled": x["id"] * 2}).limit(20)
    result = limited_ds.take_all()

    assert len(result) == 20
    assert [row["doubled"] for row in result] == [i * 2 for i in range(20)]


def test_per_task_row_limit_with_filter(ray_start_regular_shared, restore_data_context):
    """Test per-block limiting with filter operations."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Filter before limit - per-block limiting should still work at read level
    ds = ray.data.range(200, override_num_blocks=10)
    filtered_limited = ds.filter(lambda x: x["id"] % 2 == 0).limit(15)
    result = filtered_limited.take_all()

    assert len(result) == 15
    # Should get first 15 even numbers
    assert [row["id"] for row in result] == [i * 2 for i in range(15)]


def test_per_task_row_limit_readtask_properties(ray_start_regular_shared):
    """Test ReadTask per_block_limit property."""

    def dummy_read():
        return [pd.DataFrame({"id": [1, 2, 3]})]

    # Test ReadTask without per_block_limit
    task_no_limit = ReadTask(
        read_fn=dummy_read,
        metadata=BlockMetadata(
            num_rows=3, size_bytes=24, input_files=None, exec_stats=None
        ),
    )
    assert task_no_limit.per_task_row_limit is None

    # Test ReadTask with per_block_limit
    task_with_limit = ReadTask(
        read_fn=dummy_read,
        metadata=BlockMetadata(
            num_rows=3, size_bytes=24, input_files=None, exec_stats=None
        ),
        per_task_row_limit=10,
    )
    assert task_with_limit.per_task_row_limit == 10


def test_per_task_row_limit_edge_cases(ray_start_regular_shared, restore_data_context):
    """Test per-block limiting edge cases."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Test with single row
    ds = ray.data.range(1, override_num_blocks=1).limit(1)
    result = ds.take_all()
    assert len(result) == 1
    assert result[0]["id"] == 0

    # Test with limit of 1 on large dataset
    ds = ray.data.range(10000, override_num_blocks=100).limit(1)
    result = ds.take_all()
    assert len(result) == 1
    assert result[0]["id"] == 0

    # Test with very large limit
    ds = ray.data.range(100, override_num_blocks=10).limit(999999)
    result = ds.take_all()
    assert len(result) == 100
    assert [row["id"] for row in result] == list(range(100))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
