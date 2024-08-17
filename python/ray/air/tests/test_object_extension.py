import types

import numpy as np
import pyarrow as pa
import pytest

from ray.air.util.object_extensions.arrow import (
    ArrowPythonObjectArray,
    ArrowPythonObjectType,
    object_extension_type_allowed,
)
from ray.air.util.object_extensions.pandas import PythonObjectArray


@pytest.mark.skipif(
    not object_extension_type_allowed(), reason="Object extension not supported."
)
def test_object_array_validation():
    # Test unknown input type raises TypeError.
    with pytest.raises(TypeError):
        PythonObjectArray(object())

    PythonObjectArray(np.array([object(), object()]))
    PythonObjectArray([object(), object()])


@pytest.mark.skipif(
    not object_extension_type_allowed(), reason="Object extension not supported."
)
def test_arrow_scalar_object_array_roundtrip():
    arr = np.array(
        ["test", 20, False, {"some": "value"}, None, np.zeros((10, 10))], dtype=object
    )
    ata = ArrowPythonObjectArray.from_objects(arr)
    assert isinstance(ata.type, ArrowPythonObjectType)
    assert isinstance(ata, ArrowPythonObjectArray)
    assert len(ata) == len(arr)
    out = ata.to_numpy()
    np.testing.assert_array_equal(out[:-1], arr[:-1])
    assert np.all(out[-1] == arr[-1])


@pytest.mark.skipif(
    not object_extension_type_allowed(), reason="Object extension not supported."
)
def test_arrow_python_object_array_slice():
    arr = np.array(["test", 20, "test2", 40, "test3", 60], dtype=object)
    ata = ArrowPythonObjectArray.from_objects(arr)
    assert list(ata[1:3].to_pandas()) == [20, "test2"]
    assert ata[2:4].to_pylist() == ["test2", 40]


@pytest.mark.skipif(
    not object_extension_type_allowed(), reason="Object extension not supported."
)
def test_arrow_pandas_roundtrip():
    obj = types.SimpleNamespace(a=1, b="test")
    t1 = pa.table({"a": ArrowPythonObjectArray.from_objects([obj, obj]), "b": [0, 1]})
    t2 = pa.Table.from_pandas(t1.to_pandas())
    assert t1.equals(t2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
