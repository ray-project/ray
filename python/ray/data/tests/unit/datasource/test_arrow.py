import pickle
import sys

import pyarrow as pa
import pytest

from ray.data._internal.object_extensions.arrow import (
    ARROW_PYTHON_OBJECT_EXTENSION_NAME,
    ArrowPythonObjectType,
    raise_on_pickle_object_columns,
)


def _create_object_array() -> pa.ExtensionArray:
    ext_type = ArrowPythonObjectType()
    storage = pa.array([pickle.dumps({"key": "value"})], type=ext_type.storage_type)
    return pa.ExtensionArray.from_storage(ext_type, storage)


def test_raise_on_pickle_object_columns_rejects_top_level():
    table = pa.table({"col": _create_object_array()})

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_raise_on_pickle_object_columns_rejects_list():
    obj = _create_object_array()
    table = pa.table({"col": pa.ListArray.from_arrays([0, len(obj)], obj)})

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_raise_on_pickle_object_columns_rejects_large_list():
    obj = _create_object_array()
    table = pa.table({"col": pa.LargeListArray.from_arrays([0, len(obj)], obj)})

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_raise_on_pickle_object_columns_rejects_fixed_size_list():
    obj = _create_object_array()
    table = pa.table({"col": pa.FixedSizeListArray.from_arrays(obj, len(obj))})

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_raise_on_pickle_object_columns_rejects_struct():
    obj = _create_object_array()
    table = pa.table({"col": pa.StructArray.from_arrays([obj], ["field"])})

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_raise_on_pickle_object_columns_rejects_list_of_struct():
    obj = _create_object_array()
    struct = pa.StructArray.from_arrays([obj], ["field"])
    table = pa.table({"col": pa.ListArray.from_arrays([0, len(struct)], struct)})

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_raise_on_pickle_object_columns_rejects_map():
    obj = _create_object_array()
    keys = pa.array([str(i) for i in range(len(obj))])
    table = pa.table({"col": pa.MapArray.from_arrays([0, len(obj)], keys, obj)})

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_raise_on_pickle_object_columns_rejects_dictionary_encoded():
    # Dictionary types report `num_fields == 0`, so the guard must recurse into the
    # value type explicitly rather than relying on `num_fields`.
    obj = _create_object_array()
    indices = pa.array(list(range(len(obj))))
    table = pa.table({"col": pa.DictionaryArray.from_arrays(indices, obj)})

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_raise_on_pickle_object_columns_rejects_unregistered_extension():
    # If PyArrow can't resolve the registered extension type, it surfaces the column
    # as an unknown extension type. Matching on the extension name (rather than an
    # `isinstance(dtype, ArrowPythonObjectType)` check) ensures the guard still fires.
    class UnknownExtensionType(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.large_binary(), ARROW_PYTHON_OBJECT_EXTENSION_NAME)

        def __arrow_ext_serialize__(self):
            return b""

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return cls()

    ext_type = UnknownExtensionType()
    storage = pa.array([pickle.dumps({"key": "value"})], type=ext_type.storage_type)
    table = pa.table({"col": pa.ExtensionArray.from_storage(ext_type, storage)})

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_raise_on_pickle_object_columns_allows_plain_columns():
    table = pa.table(
        {
            "ints": pa.array([1, 2]),
            "strings": pa.array(["a", "b"]),
            "lists": pa.array([[1], [2]], type=pa.list_(pa.int64())),
        }
    )

    raise_on_pickle_object_columns(table)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
