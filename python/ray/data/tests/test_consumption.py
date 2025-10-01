import logging
import os
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
from ray.data.block import BlockAccessor
from ray.data.dataset import Dataset, MaterializedDataset
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
        == "MaterializedDataset(num_blocks=2, num_rows=0, schema=Unknown schema)"
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
def test_unsupported_pyarrow_versions_check(shutdown_only, unsupported_pyarrow_version):
    ray.shutdown()

    # Test that unsupported pyarrow versions cause an error to be raised upon the
    # initial pyarrow use.
    ray.init(runtime_env={"pip": [f"pyarrow=={unsupported_pyarrow_version}"]})

    @ray.remote
    def should_error():
        _check_pyarrow_version()

    with pytest.raises(ImportError):
        ray.get(should_error.remote())


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="TODO(scottjlee): Not working yet for py312"
)
def test_unsupported_pyarrow_versions_check_disabled(
    shutdown_only,
    unsupported_pyarrow_version,
    disable_pyarrow_version_check,
):
    ray.shutdown()

    # Test that unsupported pyarrow versions DO NOT cause an error to be raised upon the
    # initial pyarrow use when the version check is disabled.
    ray.init(
        runtime_env={
            "pip": [f"pyarrow=={unsupported_pyarrow_version}"],
            "env_vars": {"RAY_DISABLE_PYARROW_VERSION_CHECK": "1"},
        },
    )

    @ray.remote
    def should_pass():
        _check_pyarrow_version()

    try:
        ray.get(should_pass.remote())
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
