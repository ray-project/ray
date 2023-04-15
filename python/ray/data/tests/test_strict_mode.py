import numpy as np
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# Force strict mode.
ctx = ray.data.DatasetContext.get_current()
ctx.strict_mode = True


def test_strict_read_schemas(ray_start_regular_shared):
    ds = ray.data.range(1)
    assert ds.take()[0] == {"id": 0}

    ds = ray.data.range_table(1)
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

    with pytest.raises(ValueError):
        ds.map_batches(lambda x: np.array([0]), max_retries=0).materialize()
    ds.map_batches(lambda x: {"id": np.array([0])}).materialize()

    with pytest.raises(ValueError):
        ds.map(lambda x: np.ones(10), max_retries=0).materialize()
    ds.map(lambda x: {"x": np.ones(10)}).materialize()

    with pytest.raises(ValueError):
        ds.map_batches(lambda x: np.ones(10), max_retries=0).materialize()
    ds.map_batches(lambda x: {"x": np.ones(10)}).materialize()

    with pytest.raises(ValueError):
        ds.map_batches(lambda x: object(), max_retries=0).materialize()
    with pytest.raises(ValueError):
        ds.map_batches(lambda x: {"x": object()}, max_retries=0).materialize()
    ds.map_batches(lambda x: {"x": np.array([object()])}).materialize()


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

    assert isinstance(next(ds.iter_batches())["id"], np.ndarray)
    assert isinstance(ds.take_batch()["id"], np.ndarray)

    def f(x):
        ray.get(q.put.remote(x))
        return x

    ds.map_batches(f).materialize()
    batch = ray.get(q.put.remote(None))
    assert isinstance(batch["id"], np.ndarray), batch


def test_strict_tensor_support(ray_start_regular_shared):
    ds = ray.data.from_items([np.ones(10), np.ones(10)])
    assert ds.take()[0] == {"item": 1.0}

    ds = ds.map(lambda x: {"item": x["item"] * 2})
    assert ds.take()[0] == {"item": 2.0}

    ds = ds.map_batches(lambda x: {"item": x["item"] * 2})
    assert ds.take()[0] == {"item": 4.0}


def test_strict_object_support(ray_start_regular_shared):
    ds = ray.data.from_items([{"x": 2}, {"x": object()}])
    ds.map_batches(lambda x: x, batch_format="numpy").materialize()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
