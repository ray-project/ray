import pickle
import typing

import numpy as np
import pyarrow as pa
from packaging.version import parse as parse_version

import ray.air.util.object_extensions.pandas
from ray._private.utils import _get_pyarrow_version
from ray.util.annotations import PublicAPI

MIN_PYARROW_VERSION_SCALAR_SUBCLASS = parse_version("9.0.0")
PYARROW_VERSION = _get_pyarrow_version()


def object_extension_type_allowed() -> bool:
    return (
        PYARROW_VERSION is None
        or PYARROW_VERSION >= MIN_PYARROW_VERSION_SCALAR_SUBCLASS
    )


@PublicAPI(stability="alpha")
class ArrowPythonObjectType(pa.ExtensionType):
    def __init__(self) -> None:
        super().__init__(pa.large_binary(), "ray.data.arrow_pickled_object")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "ArrowPythonObjectType":
        return ArrowPythonObjectType()

    def __arrow_ext_scalar_class__(self) -> type:
        return ArrowPythonObjectScalar

    def __arrow_ext_class__(self) -> type:
        return ArrowPythonObjectArray

    def to_pandas_dtype(self):
        return ray.air.util.object_extensions.pandas.PythonObjectDtype()


@PublicAPI(stability="alpha")
class ArrowPythonObjectScalar(pa.ExtensionScalar):
    def as_py(self) -> typing.Any:
        if not isinstance(self.value, pa.LargeBinaryScalar):
            raise RuntimeError(
                f"{type(self.value)} is not the expected LargeBinaryScalar"
            )
        return pickle.load(pa.BufferReader(self.value.as_buffer()))


@PublicAPI(stability="alpha")
class ArrowPythonObjectArray(pa.ExtensionArray):
    def from_objects(
        objects: typing.Union[np.ndarray, typing.Iterable[typing.Any]]
    ) -> "ArrowPythonObjectArray":
        if isinstance(objects, np.ndarray):
            objects = objects.tolist()
        type_ = ArrowPythonObjectType()
        all_dumped_bytes = []
        for obj in objects:
            dumped_bytes = pickle.dumps(obj)
            all_dumped_bytes.append(dumped_bytes)
        arr = pa.array(all_dumped_bytes, type=type_)
        return arr

    def to_numpy(self, zero_copy_only: bool = False) -> np.ndarray:
        arr = np.empty(len(self), dtype=object)
        arr[:] = self.to_pylist()
        return arr


pa.register_extension_type(ArrowPythonObjectType())
