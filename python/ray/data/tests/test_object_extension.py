import types

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from ray.air.util.object_extensions.arrow import (
    ArrowPythonObjectArray,
    ArrowPythonObjectType,
    _object_extension_type_allowed,
)
from ray.air.util.object_extensions.pandas import PythonObjectArray


@pytest.mark.skipif(
    not _object_extension_type_allowed(), reason="Object extension not supported."
)
def test_object_array_validation():
    # Test unknown input type raises TypeError.
    with pytest.raises(TypeError):
        PythonObjectArray(object())

    PythonObjectArray(np.array([object(), object()]))
    PythonObjectArray([object(), object()])


@pytest.mark.skipif(
    not _object_extension_type_allowed(), reason="Object extension not supported."
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
    not _object_extension_type_allowed(), reason="Object extension not supported."
)
def test_arrow_python_object_array_slice():
    arr = np.array(["test", 20, "test2", 40, "test3", 60], dtype=object)
    ata = ArrowPythonObjectArray.from_objects(arr)
    assert list(ata[1:3].to_pandas()) == [20, "test2"]
    assert ata[2:4].to_pylist() == ["test2", 40]


@pytest.mark.skipif(
    not _object_extension_type_allowed(), reason="Object extension not supported."
)
def test_arrow_pandas_roundtrip():
    obj = types.SimpleNamespace(a=1, b="test")
    t1 = pa.table({"a": ArrowPythonObjectArray.from_objects([obj, obj]), "b": [0, 1]})
    t2 = pa.Table.from_pandas(t1.to_pandas())
    assert t1.equals(t2)


@pytest.mark.skipif(
    not _object_extension_type_allowed(), reason="Object extension not supported."
)
def test_pandas_python_object_isna():
    arr = np.array([1, np.nan, 3, 4, 5, np.nan, 7, 8, 9], dtype=object)
    ta = PythonObjectArray(arr)
    np.testing.assert_array_equal(ta.isna(), pd.isna(arr))


@pytest.mark.skipif(
    not _object_extension_type_allowed(), reason="Object extension not supported."
)
def test_pandas_python_object_take():
    arr = np.array([1, 2, 3, 4, 5], dtype=object)
    ta = PythonObjectArray(arr)
    indices = [1, 2, 3]
    np.testing.assert_array_equal(ta.take(indices).to_numpy(), arr[indices])
    indices = [1, 2, -1]
    np.testing.assert_array_equal(
        ta.take(indices, allow_fill=True, fill_value=100).to_numpy(),
        np.array([2, 3, 100]),
    )


@pytest.mark.skipif(
    not _object_extension_type_allowed(), reason="Object extension not supported."
)
def test_pandas_python_object_concat():
    arr1 = np.array([1, 2, 3, 4, 5], dtype=object)
    arr2 = np.array([6, 7, 8, 9, 10], dtype=object)
    ta1 = PythonObjectArray(arr1)
    ta2 = PythonObjectArray(arr2)
    concat_arr = PythonObjectArray._concat_same_type([ta1, ta2])
    assert len(concat_arr) == arr1.shape[0] + arr2.shape[0]
    np.testing.assert_array_equal(concat_arr.to_numpy(), np.concatenate([arr1, arr2]))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
