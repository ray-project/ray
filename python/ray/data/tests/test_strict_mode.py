from collections import UserDict

import numpy as np
import pytest

import ray
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_strict_read_schemas(ray_start_regular_shared):
    ds = ray.data.range(1)
    assert ds.take()[0] == {"id": 0}

    ds = ray.data.range_tensor(1)
    assert ds.take()[0] == {"data": np.array([0])}

    ds = ray.data.from_items([1])
    assert ds.take()[0] == {"item": 1}

    ds = ray.data.from_items([object()])
    assert isinstance(ds.take()[0]["item"], object)

    ds = ray.data.read_numpy("example://mnist_subset.npy")
    assert "data" in ds.take()[0]

    ds = ray.data.from_numpy(np.ones((100, 10)))
    assert "data" in ds.take()[0]

    ds = ray.data.from_numpy_refs(ray.put(np.ones((100, 10))))
    assert "data" in ds.take()[0]

    ds = ray.data.read_binary_files("example://image-datasets/simple")
    assert "bytes" in ds.take()[0]

    ds = ray.data.read_images("example://image-datasets/simple")
    assert "image" in ds.take()[0]

    ds = ray.data.read_text("example://sms_spam_collection_subset.txt")
    assert "text" in ds.take()[0]


def test_strict_map_output(ray_start_regular_shared):
    ds = ray.data.range(1)

    with pytest.raises(ValueError):
        ds.map(lambda x: 0, max_retries=0).materialize()
    ds.map(lambda x: {"id": 0}).materialize()
    ds.map(lambda x: UserDict({"id": 0})).materialize()

    with pytest.raises(ValueError):
        ds.map_batches(lambda x: np.array([0]), max_retries=0).materialize()
    ds.map_batches(lambda x: {"id": [0]}).materialize()
    ds.map_batches(lambda x: UserDict({"id": [0]})).materialize()

    with pytest.raises(ValueError):
        ds.map(lambda x: np.ones(10), max_retries=0).materialize()
    ds.map(lambda x: {"x": np.ones(10)}).materialize()
    ds.map(lambda x: UserDict({"x": np.ones(10)})).materialize()

    with pytest.raises(ValueError):
        ds.map_batches(lambda x: np.ones(10), max_retries=0).materialize()
    ds.map_batches(lambda x: {"x": np.ones(10)}).materialize()
    ds.map_batches(lambda x: UserDict({"x": np.ones(10)})).materialize()

    # Not allowed in normal mode either.
    with pytest.raises(ValueError):
        ds.map_batches(lambda x: object(), max_retries=0).materialize()
    with pytest.raises(ValueError):
        ds.map_batches(lambda x: {"x": object()}, max_retries=0).materialize()
    ds.map_batches(lambda x: {"x": [object()]}).materialize()
    ds.map_batches(lambda x: UserDict({"x": [object()]})).materialize()

    with pytest.raises(ValueError):
        ds.map(lambda x: object(), max_retries=0).materialize()
    ds.map(lambda x: {"x": object()}).materialize()
    ds.map(lambda x: UserDict({"x": object()})).materialize()


def test_strict_convert_map_output(ray_start_regular_shared):
    ds = ray.data.range(1).map_batches(lambda x: {"id": [0, 1, 2, 3]}).materialize()
    assert ds.take_batch()["id"].tolist() == [0, 1, 2, 3]

    with pytest.raises(ValueError):
        # Strings not converted into array.
        ray.data.range(1).map_batches(
            lambda x: {"id": "string"}, max_retries=0
        ).materialize()

    class UserObj:
        def __eq__(self, other):
            return isinstance(other, UserObj)

    ds = (
        ray.data.range(1)
        .map_batches(lambda x: {"id": [0, 1, 2, UserObj()]})
        .materialize()
    )
    assert ds.take_batch()["id"].tolist() == [0, 1, 2, UserObj()]


def test_strict_convert_map_groups(ray_start_regular_shared):
    ds = ray.data.read_csv("example://iris.csv")

    def process_group(group):
        variety = group["variety"][0]
        count = len(group["variety"])

        # Test implicit list->array conversion here.
        return {
            "variety": [variety],
            "count": [count],
        }

    ds = ds.groupby("variety").map_groups(process_group)
    ds.show()


def test_strict_default_batch_format(ray_start_regular_shared):
    ds = ray.data.range(1)

    @ray.remote
    class Queue:
        def __init__(self):
            self.item = None

        def put(self, item):
            old = self.item
            self.item = item
            return old

    q = Queue.remote()

    assert isinstance(next(iter(ds.iter_batches()))["id"], np.ndarray)
    assert isinstance(ds.take_batch()["id"], np.ndarray)

    def f(x):
        ray.get(q.put.remote(x))
        return x

    ds.map_batches(f).materialize()
    batch = ray.get(q.put.remote(None))
    assert isinstance(batch["id"], np.ndarray), batch


@pytest.mark.parametrize("shape", [(10,), (10, 2)])
def test_strict_tensor_support(ray_start_regular_shared, restore_data_context, shape):
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = False

    ds = ray.data.from_items([np.ones(shape), np.ones(shape)])
    assert np.array_equal(ds.take()[0]["item"], np.ones(shape))

    ds = ds.map(lambda x: {"item": x["item"] * 2})
    assert np.array_equal(ds.take()[0]["item"], 2 * np.ones(shape))

    ds = ds.map_batches(lambda x: {"item": x["item"] * 2})
    assert np.array_equal(ds.take()[0]["item"], 4 * np.ones(shape))


def test_strict_value_repr(ray_start_regular_shared):
    ds = ray.data.from_items([{"__value__": np.ones(10)}])

    ds = ds.map_batches(lambda x: {"__value__": x["__value__"] * 2})
    ds = ds.map(lambda x: {"x": x["__value__"] * 2})
    assert np.array_equal(ds.take()[0]["x"], 4 * np.ones(10))
    assert np.array_equal(ds.take_batch()["x"][0], 4 * np.ones(10))


def test_strict_object_support(ray_start_regular_shared):
    ds = ray.data.from_items([{"x": 2}, {"x": object()}])
    ds.map_batches(lambda x: x, batch_format="numpy").materialize()


def test_strict_compute(ray_start_regular_shared):
    with pytest.raises(ValueError):
        ray.data.range(10).map(lambda x: x, compute="actors").show()
    with pytest.raises(ValueError):
        ray.data.range(10).map(lambda x: x, compute="tasks").show()


def test_strict_schema(ray_start_regular_shared):
    import pyarrow as pa

    from ray.data._internal.pandas_block import PandasBlockSchema
    from ray.data.extensions.object_extension import (
        ArrowPythonObjectType,
        _object_extension_type_allowed,
    )
    from ray.data.extensions.tensor_extension import ArrowTensorType

    ds = ray.data.from_items([{"x": 2}])
    schema = ds.schema()
    assert isinstance(schema.base_schema, pa.lib.Schema)
    assert schema.names == ["x"]
    assert schema.types == [pa.int64()]

    ds = ray.data.from_items([{"x": 2, "y": [1, 2]}])
    schema = ds.schema()
    assert isinstance(schema.base_schema, pa.lib.Schema)
    assert schema.names == ["x", "y"]
    assert schema.types == [pa.int64(), pa.list_(pa.int64())]

    ds = ray.data.from_items([{"x": 2, "y": object(), "z": [1, 2]}])
    schema = ds.schema()
    if _object_extension_type_allowed():
        assert isinstance(schema.base_schema, pa.lib.Schema)
        assert schema.names == ["x", "y", "z"]
        assert schema.types == [
            pa.int64(),
            ArrowPythonObjectType(),
            pa.list_(pa.int64()),
        ]
    else:
        assert schema.names == ["x", "y", "z"]
        assert schema.types == [
            pa.int64(),
            object,
            object,
        ]

    ds = ray.data.from_numpy(np.ones((100, 10)))
    schema = ds.schema()
    assert isinstance(schema.base_schema, pa.lib.Schema)
    assert schema.names == ["data"]

    from ray.air.util.tensor_extensions.arrow import ArrowTensorTypeV2
    from ray.data import DataContext

    if DataContext.get_current().use_arrow_tensor_v2:
        expected_arrow_ext_type = ArrowTensorTypeV2(shape=(10,), dtype=pa.float64())
    else:
        expected_arrow_ext_type = ArrowTensorType(shape=(10,), dtype=pa.float64())

    assert schema.types == [expected_arrow_ext_type]

    schema = ds.map_batches(lambda x: x, batch_format="pandas").schema()
    assert isinstance(schema.base_schema, PandasBlockSchema)
    assert schema.names == ["data"]
    assert schema.types == [expected_arrow_ext_type]


def test_use_raw_dicts(ray_start_regular_shared):
    assert type(ray.data.range(10).take(1)[0]) is dict
    assert type(ray.data.from_items([1]).take(1)[0]) is dict

    def checker(x):
        assert type(x) is dict
        return x

    ray.data.range(10).map(checker).show()


def test_strict_require_batch_size_for_gpu():
    ray.shutdown()
    ray.init(num_cpus=4, num_gpus=1)
    ds = ray.data.range(1)
    with pytest.raises(ValueError):
        ds.map_batches(lambda x: x, num_gpus=1)

    ds.map_batches(lambda x: x, num_gpus=0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
