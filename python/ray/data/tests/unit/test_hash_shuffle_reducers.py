import sys
from typing import cast

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.execution.operators.hash_shuffle_v2 import (
    _concat_reduce,
    _sort_reduce,
)
from ray.data.extensions import ArrowTensorArray


@pytest.mark.parametrize("sort", [False, True])
@pytest.mark.parametrize("case", ["reordered", "empty", "tensor"])
def test_reduce_unifies_shard_schemas(sort, case):
    first = pa.table({"key": [2], "value": [20]})
    second = pa.table({"key": [1], "value": [10]})
    if case == "reordered":
        second = second.select(["value", "key"])
    elif case == "empty":
        second = pa.table({})
    elif case == "tensor":
        first = pa.table(
            {"key": [2], "value": ArrowTensorArray.from_numpy(np.array([[2, 3]]))}
        )
        second = pa.table(
            {"value": ArrowTensorArray.from_numpy(np.array([[4, 5]])), "key": [1]}
        )

    reducer = _sort_reduce(["key"]) if sort else _concat_reduce
    blocks = list(reducer(0, [[first, second]]))
    assert len(blocks) == 1
    result = blocks[0]
    expected = pa.table({"key": [2], "value": [20]}) if case == "empty" else None
    assert result.num_rows == first.num_rows + second.num_rows
    assert result.column_names == ["key", "value"]
    keys = result["key"].to_pylist()
    expected_keys = [2] if case == "empty" else ([1, 2] if sort else [2, 1])
    assert keys == expected_keys
    if case == "tensor":
        np.testing.assert_array_equal(
            result["value"].combine_chunks().to_numpy(),
            np.array([[4, 5], [2, 3]]) if sort else np.array([[2, 3], [4, 5]]),
        )
    elif expected is not None:
        assert result.equals(expected)
    else:
        values = [20, None if case == "null" else 10]
        assert result["value"].to_pylist() == (values[::-1] if sort else values)


@pytest.mark.parametrize("sort", [False, True])
@pytest.mark.parametrize("typed", [False, True])
def test_reduce_all_empty_shards(sort, typed):
    table = pa.table({"key": pa.array([], pa.int64())}) if typed else pa.table({})
    reducer = _sort_reduce(["key"]) if sort else _concat_reduce
    assert list(reducer(0, [[]])) == []
    result = list(reducer(0, [[table, pa.table({})]]))
    assert len(result) == 1
    assert result[0].num_rows == 0
    assert result[0].schema == table.schema


@pytest.fixture(scope="module")
def shuffle_ray_runtime():
    import ray

    ray.init(num_cpus=2, include_dashboard=False)
    yield
    ray.shutdown()


@pytest.mark.parametrize("sort", [False, True])
@pytest.mark.parametrize("disk_shuffle", [False, True])
@pytest.mark.parametrize("batch_bytes", [0, 1024 * 1024])
@pytest.mark.parametrize("mixed_keys", [False, True])
def test_keyed_repartition_key_types(
    shuffle_ray_runtime, sort, disk_shuffle, batch_bytes, mixed_keys
):
    import ray
    from ray.data.context import DataContext, ShuffleStrategy

    ctx = DataContext.get_current()
    saved = ctx.copy()
    try:
        ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
        ctx.use_disk_based_hash_shuffle = disk_shuffle
        ctx.shuffle_input_batch_bytes = batch_bytes
        first = pa.table({"key": [1], "value": [10.0]})
        second = pa.table({"key": [1.0] if mixed_keys else [1], "value": [20.0]})
        ds = ray.data.from_arrow([first, second]).repartition(
            2, keys=["key"], sort=sort
        )
        if mixed_keys:
            with pytest.raises(Exception, match="identical key types across blocks"):
                ds.materialize()
        else:
            ds = ds.materialize()
            blocks = [
                ray.get(ref)
                for bundle in ds.iter_internal_ref_bundles()
                for ref in bundle.block_refs
            ]
            nonempty = [block for block in blocks if block.num_rows]
            assert len(nonempty) == 1
            assert nonempty[0]["key"].to_pylist() == [1, 1]
            assert sorted(nonempty[0]["value"].to_pylist()) == [10.0, 20.0]
            assert nonempty[0].schema.field("value").type == pa.float64()
    finally:
        DataContext._set_current(saved)


@pytest.mark.parametrize("disk_shuffle", [False, True])
@pytest.mark.parametrize("pandas_input", [False, True])
@pytest.mark.parametrize("batch_bytes", [0, 1024 * 1024])
def test_keyed_repartition_with_empty_input(
    shuffle_ray_runtime, disk_shuffle, pandas_input, batch_bytes
):
    import ray
    from ray.data.context import DataContext, ShuffleStrategy

    ctx = DataContext.get_current()
    saved = ctx.copy()
    try:
        ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
        ctx.use_disk_based_hash_shuffle = disk_shuffle
        ctx.shuffle_input_batch_bytes = batch_bytes
        tables = [pa.table({}), pa.table({"key": [1], "value": [10]})]
        ds = (
            ray.data.from_pandas([t.to_pandas() for t in tables])
            if pandas_input
            else ray.data.from_arrow(tables)
        )
        result = ds.repartition(2, keys=["key"]).materialize()
        expected_schema = tables[1].schema
        bundles = list(result.iter_internal_ref_bundles())
        for bundle in bundles:
            assert bundle.schema == expected_schema
            for ref in bundle.block_refs:
                assert ray.get(ref).schema == expected_schema
        assert result.take_all() == [{"key": 1, "value": 10}]
    finally:
        DataContext._set_current(saved)


@pytest.mark.parametrize("sort", [False, True])
@pytest.mark.parametrize("case", ["reordered", "null", "promotion"])
@pytest.mark.parametrize("batch_bytes", [0, 1024 * 1024])
@pytest.mark.parametrize("disk_shuffle", [False, True])
def test_repartition_rejects_type_changes(
    shuffle_ray_runtime, sort, case, batch_bytes, disk_shuffle
):
    import ray
    from ray.data.context import DataContext, ShuffleStrategy

    ctx = DataContext.get_current()
    saved = ctx.copy()
    try:
        ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
        ctx.use_disk_based_hash_shuffle = disk_shuffle
        ctx.shuffle_input_batch_bytes = batch_bytes
        first = pa.table({"key": [1], "value": [10]})
        second = pa.table({"key": [1], "value": [20]})
        if case == "reordered":
            second = second.select(["value", "key"])
        elif case == "null":
            second = pa.table({"key": [1], "value": [None]})
        else:
            second = pa.table({"key": [1], "value": [20.5]})
        dataset = ray.data.from_arrow([first, second]).repartition(
            2, keys=["key"], sort=sort
        )
        if case != "reordered":
            with pytest.raises(Exception, match="matching column names and types"):
                dataset.materialize()
        else:
            dataset = dataset.materialize()
            assert sorted(dataset.take_all(), key=lambda row: row["value"]) == [
                {"key": 1, "value": 10},
                {"key": 1, "value": 20},
            ]
            for bundle in dataset.iter_internal_ref_bundles():
                for ref in bundle.block_refs:
                    assert ray.get(ref).schema == bundle.schema
    finally:
        DataContext._set_current(saved)


@pytest.mark.parametrize("nonempty_first", [False, True])
@pytest.mark.parametrize("all_empty", [False, True])
def test_disk_handles_prefer_nonempty_schema(nonempty_first, all_empty):
    from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_runtime import (
        ShuffleHandle,
        _handles_to_sources,
    )

    schema = pa.schema([("key", pa.int64()), ("value", pa.int64())])
    empty = {
        "schema": pa.schema([]),
        "num_rows": np.array([0, 0]),
        "index_ranges": np.zeros((2, 2), dtype=np.int64),
    }
    typed = {
        "schema": schema,
        "num_rows": np.array([0 if all_empty else 1, 0]),
        "index_ranges": np.zeros((2, 2), dtype=np.int64),
    }
    handles = cast(
        list[ShuffleHandle], [typed, empty] if nonempty_first else [empty, typed]
    )
    # Partition 1 has no rows even when another partition has data.
    sources, actual = _handles_to_sources(handles, 1, batch_bytes=1)
    assert sources == []
    assert actual == (handles[0]["schema"] if all_empty else schema)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
