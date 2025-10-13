from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pyarrow as pa

from ray.air.util.tensor_extensions.arrow import (
    _infer_pyarrow_type,
)
from ray.util.annotations import PublicAPI

PYARROW_TYPE_DEFINITIONS: Dict[str, Tuple[callable, str]] = {
    "int8": (pa.int8, "an 8-bit signed integer"),
    "int16": (pa.int16, "a 16-bit signed integer"),
    "int32": (pa.int32, "a 32-bit signed integer"),
    "int64": (pa.int64, "a 64-bit signed integer"),
    "uint8": (pa.uint8, "an 8-bit unsigned integer"),
    "uint16": (pa.uint16, "a 16-bit unsigned integer"),
    "uint32": (pa.uint32, "a 32-bit unsigned integer"),
    "uint64": (pa.uint64, "a 64-bit unsigned integer"),
    "float32": (pa.float32, "a 32-bit floating point number"),
    "float64": (pa.float64, "a 64-bit floating point number"),
    "string": (pa.string, "a variable-length string"),
    "bool": (pa.bool_, "a boolean value"),
    "binary": (pa.binary, "variable-length binary data"),
}


def _factory_methods(cls: type):
    """Metaprogramming: Class decorator to generate factory methods for PyArrow types using from_arrow.

    This decorator automatically creates class methods for common PyArrow data types.
    Each generated method is a convenient factory that calls cls.from_arrow(pa.type()).

    Generated methods include:
    - Signed integers: int8, int16, int32, int64
    - Unsigned integers: uint8, uint16, uint32, uint64
    - Floating point: float32, float64
    - Other types: string, bool, binary

    Examples of generated methods::

        @classmethod
        def int32(cls):
            \"\"\"Create a DataType representing a 32-bit signed integer.

            Returns:
                DataType: A DataType with PyArrow int32 type
            \"\"\"
            return cls.from_arrow(pa.int32())

        @classmethod
        def string(cls):
            \"\"\"Create a DataType representing a variable-length string.

            Returns:
                DataType: A DataType with PyArrow string type
            \"\"\"
            return cls.from_arrow(pa.string())

    Usage:
        Instead of DataType.from_arrow(pa.int32()), you can use DataType.int32()
    """

    for method_name, (pa_func, description) in PYARROW_TYPE_DEFINITIONS.items():

        def create_method(name, func, desc):
            def factory_method(cls):
                return cls.from_arrow(func())

            factory_method.__doc__ = f"""Create a DataType representing {desc}.

        Returns:
            DataType: A DataType with PyArrow {name} type
        """
            factory_method.__name__ = name
            factory_method.__qualname__ = f"{cls.__name__}.{name}"
            return classmethod(factory_method)

        setattr(cls, method_name, create_method(method_name, pa_func, description))

    return cls


@PublicAPI(stability="alpha")
@dataclass
@_factory_methods
class DataType:
    """A simplified Ray Data DataType supporting Arrow, NumPy, and Python types."""

    _internal_type: Union[pa.DataType, np.dtype, type]

    def __post_init__(self):
        """Validate the _internal_type after initialization."""
        # TODO: Support Pandas extension types
        if not isinstance(
            self._internal_type,
            (pa.DataType, np.dtype, type),
        ):
            raise TypeError(
                f"DataType supports only PyArrow DataType, NumPy dtype, or Python type, but was given type {type(self._internal_type)}."
            )

    # Type checking methods
    def is_arrow_type(self) -> bool:
        return isinstance(self._internal_type, pa.DataType)

    def is_numpy_type(self) -> bool:
        return isinstance(self._internal_type, np.dtype)

    def is_python_type(self) -> bool:
        return isinstance(self._internal_type, type)

    # Conversion methods
    def to_arrow_dtype(self, values: Optional[List[Any]] = None) -> pa.DataType:
        """
        Convert the DataType to a PyArrow DataType.

        Args:
            values: Optional list of values to infer the Arrow type from. Required if the DataType is a Python type.

        Returns:
            A PyArrow DataType
        """
        if self.is_arrow_type():
            return self._internal_type
        else:
            if isinstance(self._internal_type, np.dtype):
                return pa.from_numpy_dtype(self._internal_type)
            else:
                assert (
                    values is not None and len(values) > 0
                ), "Values are required to infer Arrow type if the provided type is a Python type"
                return _infer_pyarrow_type(values)

    def to_numpy_dtype(self) -> np.dtype:
        if self.is_numpy_type():
            return self._internal_type
        elif self.is_arrow_type():
            try:
                # For most basic arrow types, this will work
                pandas_dtype = self._internal_type.to_pandas_dtype()
                if isinstance(pandas_dtype, np.dtype):
                    return pandas_dtype
                else:
                    # If pandas returns an extension dtype, fall back to object
                    return np.dtype("object")
            except (TypeError, NotImplementedError, pa.ArrowNotImplementedError):
                return np.dtype("object")
        else:
            return np.dtype("object")

    def to_python_type(self) -> type:
        if self.is_python_type():
            return self._internal_type
        else:
            raise ValueError(f"DataType {self} is not a Python type")

    # Factory methods from external systems
    @classmethod
    def from_arrow(cls, arrow_type: pa.DataType) -> "DataType":
        """Create a DataType from a PyArrow DataType.

        Args:
            arrow_type: A PyArrow DataType to wrap

        Returns:
            DataType: A DataType wrapping the given PyArrow type

        Examples:
            >>> import pyarrow as pa
            >>> from ray.data.datatype import DataType
            >>> DataType.from_arrow(pa.timestamp('s'))
            DataType(arrow:timestamp[s])
            >>> DataType.from_arrow(pa.int64())
            DataType(arrow:int64)
        """
        return cls(_internal_type=arrow_type)

    @classmethod
    def from_numpy(cls, numpy_dtype: Union[np.dtype, str]) -> "DataType":
        """Create a DataType from a NumPy dtype.

        Args:
            numpy_dtype: A NumPy dtype object or string representation

        Returns:
            DataType: A DataType wrapping the given NumPy dtype

        Examples:
            >>> import numpy as np
            >>> from ray.data.datatype import DataType
            >>> DataType.from_numpy(np.dtype('int32'))
            DataType(numpy:int32)
            >>> DataType.from_numpy('float64')
            DataType(numpy:float64)
        """
        if isinstance(numpy_dtype, str):
            numpy_dtype = np.dtype(numpy_dtype)
        return cls(_internal_type=numpy_dtype)

    @classmethod
    def infer_dtype(cls, value: Any) -> "DataType":
        """Infer DataType from a Python value, handling numpy, Arrow, and Python types.

        Args:
            value: Any Python value to infer the type from

        Returns:
            DataType: The inferred data type

        Examples:
            >>> import numpy as np
            >>> from ray.data.datatype import DataType
            >>> DataType.infer_dtype(5)
            DataType(arrow:int64)
            >>> DataType.infer_dtype("hello")
            DataType(arrow:string)
            >>> DataType.infer_dtype(np.int32(42))
            DataType(numpy:int32)
        """
        # 1. Handle numpy arrays and scalars
        if isinstance(value, (np.ndarray, np.generic)):
            return cls.from_numpy(value.dtype)
        # 3. Try PyArrow type inference for regular Python values
        try:
            inferred_arrow_type = _infer_pyarrow_type([value])
            if inferred_arrow_type is not None:
                return cls.from_arrow(inferred_arrow_type)
        except Exception:
            return cls(type(value))

    def __repr__(self) -> str:
        if self.is_arrow_type():
            return f"DataType(arrow:{self._internal_type})"
        elif self.is_numpy_type():
            return f"DataType(numpy:{self._internal_type})"
        else:
            return f"DataType(python:{self._internal_type.__name__})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, DataType):
            return False

        # Ensure they're from the same type system by checking the actual type
        # of the internal type object, not just the value
        if type(self._internal_type) is not type(other._internal_type):
            return False

        return self._internal_type == other._internal_type

    def __hash__(self) -> int:
        # Include the type of the internal type in the hash to ensure
        # different type systems don't collide
        return hash((type(self._internal_type), self._internal_type))
