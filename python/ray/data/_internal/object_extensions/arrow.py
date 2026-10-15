import pickle
import typing

import numpy as np
import pyarrow as pa

import ray.data._internal.object_extensions.pandas
from ray._common.serialization import pickle_dumps
from ray._common.utils import env_bool
from ray.data._internal.utils.arrow_utils import _check_pyarrow_version
from ray.util.annotations import PublicAPI

# First, assert Arrow version is w/in expected bounds
_check_pyarrow_version()

# Some datasource implementations call `raise_on_pickle_object_columns` to protect users
# from arbitrary code execution. If you set this env var, then
# `raise_on_pickle_object_columns` no-ops, and you can read pickled data.
AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR = "RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR"

ARROW_PYTHON_OBJECT_EXTENSION_NAME = "ray.data.arrow_pickled_object"


def raise_on_pickle_object_columns(table: "pa.Table") -> None:
    """Raise if ``table`` has data stored as the pickled-object extension type.

    Deserializing ``ray.data.arrow_pickled_object`` columns requires unpickling, which
    can execute arbitrary code. To avoid exposing users to this vulnerability,
    datasource implementations can call this function after reading tables.

    If you set ``RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR=1``, this function no-ops
    """
    if env_bool(AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR, False):
        return

    pickle_cols = [
        field.name for field in table.schema if _contains_pickle_object_type(field.type)
    ]
    if pickle_cols:
        raise ValueError(
            f"This file contains columns stored as "
            f"'ray.data.arrow_pickled_object': {pickle_cols}. Reading these "
            f"columns requires unpickling, which can execute arbitrary code "
            f"and is unsafe with untrusted files.\n\n"
            f"If you trust the source of this data, set the environment "
            f"variable {AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR}=1 to allow "
            f"reading these columns. In a Ray cluster, this variable must "
            f"be set on all worker nodes (e.g. via 'runtime_env')."
        )


def _contains_pickle_object_type(dtype: "pa.DataType") -> bool:
    """Return whether ``dtype`` is, or nests, the pickled-object extension type."""
    if isinstance(dtype, pa.ExtensionType):
        if dtype.extension_name == ARROW_PYTHON_OBJECT_EXTENSION_NAME:
            return True

        # An extension type wraps a storage type that may itself nest the object type.
        return _contains_pickle_object_type(dtype.storage_type)

    # Dictionary-encoded columns report ``num_fields == 0``, so recurse explicitly.
    if pa.types.is_dictionary(dtype):
        return _contains_pickle_object_type(dtype.value_type)

    return any(
        _contains_pickle_object_type(dtype.field(i).type)
        for i in range(dtype.num_fields)
    )


# Please see https://arrow.apache.org/docs/python/extending_types.html for more info
@PublicAPI(stability="alpha")
class ArrowPythonObjectType(pa.ExtensionType):
    """Defines a new Arrow extension type for Python objects.
    We do not require a parametrized type, so the constructor does not
    take any arguments
    """

    def __init__(self) -> None:
        # Defines the underlying storage type as the PyArrow LargeBinary type
        super().__init__(pa.large_binary(), ARROW_PYTHON_OBJECT_EXTENSION_NAME)

    def __arrow_ext_serialize__(self) -> bytes:
        # Since there are no type parameters, we are free to return empty
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "ArrowPythonObjectType":
        return ArrowPythonObjectType()

    def __arrow_ext_scalar_class__(self) -> type:
        """Returns the scalar class of the extension type. Indexing out of the
        PyArrow extension array will return instances of this type.
        """
        return ArrowPythonObjectScalar

    def __arrow_ext_class__(self) -> type:
        """Returns the array type of the extension type. Selecting one array
        out of the ChunkedArray that makes up a column in a Table with
        this custom type will return an instance of this type.
        """
        return ArrowPythonObjectArray

    def to_pandas_dtype(self):
        """Pandas interoperability type. This describes the Pandas counterpart
        to the Arrow type. See https://pandas.pydata.org/docs/development/extending.html
        for more information.
        """
        return ray.data._internal.object_extensions.pandas.PythonObjectDtype()

    def __reduce__(self):
        # Earlier PyArrow versions require custom pickling behavior.
        return self.__arrow_ext_deserialize__, (
            self.storage_type,
            self.__arrow_ext_serialize__(),
        )

    def __hash__(self) -> int:
        return hash((type(self), self.storage_type.id, self.extension_name))


@PublicAPI(stability="alpha")
class ArrowPythonObjectScalar(pa.ExtensionScalar):
    """Scalar class for ArrowPythonObjectType"""

    def as_py(self, **kwargs) -> typing.Any:
        # Handle None/null values
        if self.value is None:
            return None

        if not isinstance(self.value, pa.LargeBinaryScalar):
            raise RuntimeError(
                f"{type(self.value)} is not the expected LargeBinaryScalar"
            )
        return pickle.load(pa.BufferReader(self.value.as_buffer()))


@PublicAPI(stability="alpha")
class ArrowPythonObjectArray(pa.ExtensionArray):
    """Array class for ArrowPythonObjectType"""

    def from_objects(
        objects: typing.Union[np.ndarray, typing.Iterable[typing.Any]],
    ) -> "ArrowPythonObjectArray":
        if isinstance(objects, np.ndarray):
            objects = objects.tolist()
        type_ = ArrowPythonObjectType()
        all_dumped_bytes = []
        for obj in objects:
            dumped_bytes = pickle_dumps(
                obj, "Error pickling object to convert to Arrow"
            )
            all_dumped_bytes.append(dumped_bytes)
        arr = pa.array(all_dumped_bytes, type=type_.storage_type)
        return type_.wrap_array(arr)

    def to_numpy(
        self, zero_copy_only: bool = False, writable: bool = False
    ) -> np.ndarray:
        arr = np.empty(len(self), dtype=object)
        arr[:] = self.to_pylist()
        return arr


try:
    pa.register_extension_type(ArrowPythonObjectType())
except pa.ArrowKeyError:
    # Already registered
    pass
