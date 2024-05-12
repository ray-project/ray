import numpy as np
import pyarrow as pa
import pytest

from ray.air.util.object_extensions.arrow import ArrowPythonObjectArray
from ray.air.util.object_extensions.pandas import PythonObjectArray


def test_object_array_validation():
    # Test unknown input type raises TypeError.
    with pytest.raises(TypeError):
        PythonObjectArray(object())

    PythonObjectArray(np.array([object(), object()]))
    PythonObjectArray([object(), object()])


def test_arrow_scalar_tensor_array_roundtrip():
    arr = np.array(
        ["test", 20, False, {"some": "value"}, None, np.zeros((10, 10))], dtype=object
    )
    ata = ArrowPythonObjectArray.from_objects(arr)
    assert isinstance(ata.type, pa.DataType)
    assert len(ata) == len(arr)
    out = ata.to_numpy()
    np.testing.assert_array_equal(out[:-1], arr[:-1])
    assert np.all(out[-1] == arr[-1])


def test_arrow_python_object_array_slice():
    arr = np.array(["test", 20, "test2", 40, "test3", 60], dtype=object)
    ata = ArrowPythonObjectArray.from_objects(arr)
    assert list(ata[1:3].to_pandas()) == [20, "test2"]
    assert ata[2:4].to_pylist() == ["test2", 40]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
