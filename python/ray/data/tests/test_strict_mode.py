import numpy as np
from collections import UserDict
import pytest

import ray
from ray.data.block import StrictModeError
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_strict_read_schemas(ray_start_regular_shared, enable_strict_mode):
    ds = ray.data.range(1)
    assert ds.take()[0] == {"id": 0}

    with pytest.raises(DeprecationWarning):
        ds = ray.data.range_table(1)

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


def test_strict_map_output(ray_start_regular_shared, enable_strict_mode):
    ds = ray.data.range(1)

    with pytest.raises(StrictModeError):
        ds.map(lambda x: 0, max_retries=0).materialize()
    ds.map(lambda x: {"id": 0}).materialize()
    ds.map(lambda x: UserDict({"id": 0})).materialize()

    with pytest.raises(StrictModeError):
        ds.map_batches(lambda x: np.array([0]), max_retries=0).materialize()
    ds.map_batches(lambda x: {"id": [0]}).materialize()
    ds.map_batches(lambda x: UserDict({"id": [0]})).materialize()

    with pytest.raises(StrictModeError):
        ds.map(lambda x: np.ones(10), max_retries=0).materialize()
    ds.map(lambda x: {"x": np.ones(10)}).materialize()
    ds.map(lambda x: UserDict({"x": np.ones(10)})).materialize()

    with pytest.raises(StrictModeError):
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

    with pytest.raises(StrictModeError):
        ds.map(lambda x: object(), max_retries=0).materialize()
    ds.map(lambda x: {"x": object()}).materialize()
    ds.map(lambda x: UserDict({"x": object()})).materialize()


def test_strict_convert_map_output(ray_start_regular_shared, enable_strict_mode):
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


def test_strict_convert_map_groups(ray_start_regular_shared, enable_strict_mode):
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


def test_strict_default_batch_format(ray_start_regular_shared, enable_strict_mode):
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

    assert isinstance(next(ds.iter_batches())["id"], np.ndarray)
    assert isinstance(ds.take_batch()["id"], np.ndarray)

    def f(x):
        ray.get(q.put.remote(x))
        return x

    ds.map_batches(f).materialize()
    batch = ray.get(q.put.remote(None))
    assert isinstance(batch["id"], np.ndarray), batch


def test_strict_tensor_support(ray_start_regular_shared, enable_strict_mode):
    ds = ray.data.from_items([np.ones(10), np.ones(10)])
    assert np.array_equal(ds.take()[0]["item"], np.ones(10))

    ds = ds.map(lambda x: {"item": x["item"] * 2})
    assert np.array_equal(ds.take()[0]["item"], 2 * np.ones(10))

    ds = ds.map_batches(lambda x: {"item": x["item"] * 2})
    assert np.array_equal(ds.take()[0]["item"], 4 * np.ones(10))


def test_strict_value_repr(ray_start_regular_shared, enable_strict_mode):
    ds = ray.data.from_items([{"__value__": np.ones(10)}])

    ds = ds.map_batches(lambda x: {"__value__": x["__value__"] * 2})
    ds = ds.map(lambda x: {"x": x["__value__"] * 2})
    assert np.array_equal(ds.take()[0]["x"], 4 * np.ones(10))
    assert np.array_equal(ds.take_batch()["x"][0], 4 * np.ones(10))


def test_strict_object_support(ray_start_regular_shared, enable_strict_mode):
    ds = ray.data.from_items([{"x": 2}, {"x": object()}])
    ds.map_batches(lambda x: x, batch_format="numpy").materialize()


def test_strict_compute(ray_start_regular_shared, enable_strict_mode):
    with pytest.raises(StrictModeError):
        ray.data.range(10).map(lambda x: x, compute="actors").show()
    with pytest.raises(StrictModeError):
        ray.data.range(10).map(
            lambda x: x, compute=ray.data.ActorPoolStrategy(1, 1)
        ).show()
    with pytest.raises(StrictModeError):
        ray.data.range(10).map(lambda x: x, compute="tasks").show()


def test_strict_schema(ray_start_regular_shared, enable_strict_mode):
    import pyarrow
    from ray.data._internal.pandas_block import PandasBlockSchema

    ds = ray.data.from_items([{"x": 2}])
    schema = ds.schema()
    assert isinstance(schema.base_schema, pyarrow.lib.Schema)
    assert str(schema) == "Schema({'x': DataType(int64)})"

    ds = ray.data.from_items([{"x": 2, "y": [1, 2]}])
    schema = ds.schema()
    assert isinstance(schema.base_schema, pyarrow.lib.Schema)
    assert (
        str(schema)
        == "Schema({'x': DataType(int64), 'y': ListType(list<item: int64>)})"
    )

    ds = ray.data.from_items([{"x": 2, "y": object(), "z": [1, 2]}])
    schema = ds.schema()
    assert isinstance(schema.base_schema, PandasBlockSchema)
    assert str(schema) == (
        "Schema({'x': DataType(int64), 'y': "
        "<class 'object'>, 'z': <class 'object'>})"
    )

    ds = ray.data.from_numpy(np.ones((100, 10)))
    schema = ds.schema()
    assert isinstance(schema.base_schema, pyarrow.lib.Schema)
    assert str(schema) == "Schema({'data': numpy.ndarray(shape=(10,), dtype=double)})"

    schema = ds.map_batches(lambda x: x, batch_format="pandas").schema()
    assert str(schema) == "Schema({'data': numpy.ndarray(shape=(10,), dtype=double)})"
    assert isinstance(schema.base_schema, PandasBlockSchema)


def test_use_raw_dicts(ray_start_regular_shared, enable_strict_mode):
    assert type(ray.data.range(10).take(1)[0]) is dict
    assert type(ray.data.from_items([1]).take(1)[0]) is dict

    def checker(x):
        assert type(x) is dict
        return x

    ray.data.range(10).map(checker).show()


def test_strict_require_batch_size_for_gpu(enable_strict_mode):
    ray.shutdown()
    ray.init(num_cpus=4, num_gpus=1)
    ds = ray.data.range(1)
    with pytest.raises(StrictModeError):
        ds.map_batches(lambda x: x, num_gpus=1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
