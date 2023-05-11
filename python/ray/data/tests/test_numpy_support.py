import numpy as np
import torch
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class UserObj:
    def __eq__(self, other):
        return isinstance(other, UserObj)


def do_map_batches(data):
    ds = ray.data.range(1)
    ds = ds.map_batches(lambda x: {"output": data})
    return ds.take_batch()["output"]


def assert_structure_equals(a, b):
    assert type(a) == type(b), (type(a), type(b))
    assert type(a[0]) == type(b[0]), (type(a[0]), type(b[0]))  # noqa: E721
    assert a.dtype == b.dtype
    assert a.shape == b.shape
    for i in range(len(a)):
        assert np.array_equiv(a[i], b[i])


def test_list_of_scalars(ray_start_regular_shared):
    data = [1, 2, 3]
    output = do_map_batches(data)
    assert_structure_equals(output, np.array([1, 2, 3], dtype=np.int64))


def test_list_of_numpy_scalars(ray_start_regular_shared):
    data = [np.int64(1), np.int64(2), np.int64(3)]
    output = do_map_batches(data)
    assert_structure_equals(output, np.array([1, 2, 3], dtype=np.int64))


def test_list_of_objects(ray_start_regular_shared):
    data = [1, 2, 3, UserObj()]
    output = do_map_batches(data)
    assert_structure_equals(output, np.array([1, 2, 3, UserObj()]))


def test_array_like(ray_start_regular_shared):
    data = torch.Tensor([1, 2, 3])
    output = do_map_batches(data)
    assert_structure_equals(output, np.array([1.0, 2.0, 3.0], dtype=np.float32))


def test_list_of_arrays(ray_start_regular_shared):
    data = [np.array([1, 2, 3]), np.array([4, 5, 6])]
    output = do_map_batches(data)
    assert_structure_equals(output, np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int64))


def test_list_of_array_like(ray_start_regular_shared):
    data = [torch.Tensor([1, 2, 3]), torch.Tensor([4, 5, 6])]
    output = do_map_batches(data)
    assert_structure_equals(output, np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32))


def test_ragged_array_like(ray_start_regular_shared):
    data = [torch.Tensor([1, 2, 3]), torch.Tensor([1, 2])]
    output = do_map_batches(data)
    assert_structure_equals(
        output, np.array([np.array([1, 2, 3]), np.array([1, 2])], dtype=object)
    )


def test_ragged_lists(ray_start_regular_shared):
    data = [[1, 2, 3], [1, 2]]
    output = do_map_batches(data)
    assert_structure_equals(
        output, np.array([np.array([1, 2, 3]), np.array([1, 2])], dtype=object)
    )


def test_scalar_numpy(ray_start_regular_shared):
    data = np.int64(1)
    ds = ray.data.range(2)
    ds = ds.map(lambda x: {"output": data})
    output = ds.take_batch()["output"]
    assert_structure_equals(output, np.array([1, 1], dtype=np.int64))


def test_scalar_arrays(ray_start_regular_shared):
    data = np.array([1, 2, 3])
    ds = ray.data.range(2)
    ds = ds.map(lambda x: {"output": data})
    output = ds.take_batch()["output"]
    assert_structure_equals(output, np.array([[1, 2, 3], [1, 2, 3]], dtype=np.int64))


def test_scalar_array_like(ray_start_regular_shared):
    data = torch.Tensor([1, 2, 3])
    ds = ray.data.range(2)
    ds = ds.map(lambda x: {"output": data})
    output = ds.take_batch()["output"]
    assert_structure_equals(output, np.array([[1, 2, 3], [1, 2, 3]], dtype=np.float32))


def test_scalar_ragged_arrays(ray_start_regular_shared):
    data = [np.array([1, 2, 3]), np.array([1, 2])]
    ds = ray.data.range(2)
    ds = ds.map(lambda x: {"output": data[x["id"]]})
    output = ds.take_batch()["output"]
    assert_structure_equals(
        output, np.array([np.array([1, 2, 3]), np.array([1, 2])], dtype=object)
    )


def test_scalar_ragged_array_like(ray_start_regular_shared):
    data = [torch.Tensor([1, 2, 3]), torch.Tensor([1, 2])]
    ds = ray.data.range(2)
    ds = ds.map(lambda x: {"output": data[x["id"]]})
    output = ds.take_batch()["output"]
    assert_structure_equals(
        output, np.array([np.array([1, 2, 3]), np.array([1, 2])], dtype=object)
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
