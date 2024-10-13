from datetime import datetime

import numpy as np
import pytest
import torch

import ray
from ray.air.util.tensor_extensions.utils import create_ragged_ndarray
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
    assert isinstance(a, b), (type(a), type(b))
    assert type(a[0]) == type(b[0]), (type(a[0]), type(b[0]))  # noqa: E721
    assert a.dtype == b.dtype
    assert a.shape == b.shape
    for i in range(len(a)):
        assert np.array_equiv(a[i], b[i]), (i, a, b)


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


DATETIME_DAY_PRECISION = datetime(year=2024, month=1, day=1)
DATETIME_HOUR_PRECISION = datetime(year=2024, month=1, day=1, hour=1)
DATETIME_MIN_PRECISION = datetime(year=2024, month=1, day=1, minute=1)
DATETIME_SEC_PRECISION = datetime(year=2024, month=1, day=1, second=1)
DATETIME_MILLISEC_PRECISION = datetime(year=2024, month=1, day=1, microsecond=1000)
DATETIME_MICROSEC_PRECISION = datetime(year=2024, month=1, day=1, microsecond=1)

DATETIME64_DAY_PRECISION = np.datetime64("2024-01-01")
DATETIME64_HOUR_PRECISION = np.datetime64("2024-01-01T01:00", "s")
DATETIME64_MIN_PRECISION = np.datetime64("2024-01-01T00:01", "s")
DATETIME64_SEC_PRECISION = np.datetime64("2024-01-01T00:00:01")
DATETIME64_MILLISEC_PRECISION = np.datetime64("2024-01-01T00:00:00.001")
DATETIME64_MICROSEC_PRECISION = np.datetime64("2024-01-01T00:00:00.000001")


@pytest.mark.parametrize(
    "data,expected_output",
    [
        ([DATETIME_DAY_PRECISION], np.array([DATETIME64_DAY_PRECISION])),
        ([DATETIME_HOUR_PRECISION], np.array([DATETIME64_HOUR_PRECISION])),
        ([DATETIME_MIN_PRECISION], np.array([DATETIME64_MIN_PRECISION])),
        ([DATETIME_SEC_PRECISION], np.array([DATETIME64_SEC_PRECISION])),
        ([DATETIME_MILLISEC_PRECISION], np.array([DATETIME64_MILLISEC_PRECISION])),
        ([DATETIME_MICROSEC_PRECISION], np.array([DATETIME64_MICROSEC_PRECISION])),
        (
            [DATETIME_MICROSEC_PRECISION, DATETIME_MILLISEC_PRECISION],
            np.array(
                [DATETIME64_MICROSEC_PRECISION, DATETIME_MILLISEC_PRECISION],
                dtype="datetime64[us]",
            ),
        ),
        (
            [DATETIME_SEC_PRECISION, DATETIME_MILLISEC_PRECISION],
            np.array(
                [DATETIME64_SEC_PRECISION, DATETIME_MILLISEC_PRECISION],
                dtype="datetime64[ms]",
            ),
        ),
    ],
)
def test_list_of_datetimes(data, expected_output, ray_start_regular_shared):
    output = do_map_batches(data)
    assert_structure_equals(output, expected_output)


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

    data = [torch.zeros((3, 5, 10)), torch.zeros((3, 8, 8))]
    output = do_map_batches(data)
    assert_structure_equals(
        output, create_ragged_ndarray([np.zeros((3, 5, 10)), np.zeros((3, 8, 8))])
    )


def test_scalar_nested_arrays(ray_start_regular_shared):
    data = [[[1]], [[2]]]
    output = do_map_batches(data)
    assert_structure_equals(output, create_ragged_ndarray(data))


def test_scalar_lists_not_converted(ray_start_regular_shared):
    data = [[1, 2], [1, 2]]
    output = do_map_batches(data)
    assert_structure_equals(output, create_ragged_ndarray([[1, 2], [1, 2]]))

    data = [[1, 2, 3], [1, 2]]
    output = do_map_batches(data)
    assert_structure_equals(output, create_ragged_ndarray([[1, 2, 3], [1, 2]]))


def test_scalar_numpy(ray_start_regular_shared):
    data = np.int64(1)
    ds = ray.data.range(2, override_num_blocks=1)
    ds = ds.map(lambda x: {"output": data})
    output = ds.take_batch()["output"]
    assert_structure_equals(output, np.array([1, 1], dtype=np.int64))


def test_scalar_arrays(ray_start_regular_shared):
    data = np.array([1, 2, 3])
    ds = ray.data.range(2, override_num_blocks=1)
    ds = ds.map(lambda x: {"output": data})
    output = ds.take_batch()["output"]
    assert_structure_equals(output, np.array([[1, 2, 3], [1, 2, 3]], dtype=np.int64))


def test_bytes(ray_start_regular_shared):
    """Tests that bytes are converted to object dtype instead of zero-terminated."""
    data = b"\x1a\n\x00\n\x1a"
    ds = ray.data.range(1, override_num_blocks=1)
    ds = ds.map(lambda x: {"output": data})
    output = ds.take_batch()["output"]
    assert_structure_equals(output, np.array([b"\x1a\n\x00\n\x1a"], dtype=object))


def test_scalar_array_like(ray_start_regular_shared):
    data = torch.Tensor([1, 2, 3])
    ds = ray.data.range(2, override_num_blocks=1)
    ds = ds.map(lambda x: {"output": data})
    output = ds.take_batch()["output"]
    assert_structure_equals(output, np.array([[1, 2, 3], [1, 2, 3]], dtype=np.float32))


def test_scalar_ragged_arrays(ray_start_regular_shared):
    data = [np.array([1, 2, 3]), np.array([1, 2])]
    ds = ray.data.range(2, override_num_blocks=1)
    ds = ds.map(lambda x: {"output": data[x["id"]]})
    output = ds.take_batch()["output"]
    assert_structure_equals(
        output, np.array([np.array([1, 2, 3]), np.array([1, 2])], dtype=object)
    )


def test_scalar_ragged_array_like(ray_start_regular_shared):
    data = [torch.Tensor([1, 2, 3]), torch.Tensor([1, 2])]
    ds = ray.data.range(2, override_num_blocks=1)
    ds = ds.map(lambda x: {"output": data[x["id"]]})
    output = ds.take_batch()["output"]
    assert_structure_equals(
        output, np.array([np.array([1, 2, 3]), np.array([1, 2])], dtype=object)
    )

    data = [torch.zeros((3, 5, 10)), torch.zeros((3, 8, 8))]
    ds = ray.data.range(2, override_num_blocks=1)
    ds = ds.map(lambda x: {"output": data[x["id"]]})
    output = ds.take_batch()["output"]
    assert_structure_equals(
        output, create_ragged_ndarray([np.zeros((3, 5, 10)), np.zeros((3, 8, 8))])
    )


def test_nested_ragged_arrays(ray_start_regular_shared):
    data = [
        {"a": [[1], [2, 3]]},
        {"a": [[4, 5], [6]]},
    ]

    def f(row):
        return data[row["id"]]

    output = ray.data.range(2).map(f).take_all()
    assert output == data


# https://github.com/ray-project/ray/issues/35340
def test_complex_ragged_arrays(ray_start_regular_shared):
    data = [[{"a": 1}, {"a": 2}, {"a": 3}], [{"b": 1}]]
    output = do_map_batches(data)
    assert_structure_equals(output, create_ragged_ndarray(data))

    data = ["hi", 1, None, [[[[]]]], {"a": [[{"b": 2, "c": UserObj()}]]}, UserObj()]
    output = do_map_batches(data)
    assert_structure_equals(output, create_ragged_ndarray(data))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
