from dataclasses import dataclass
from typing import Any, Union

import numpy as np
import pyarrow as pa

from ray.util.annotations import PublicAPI


def generate_arrow_factory_methods(cls: type):
    """Metaprogrammming: Class decorator to generate factory methods for PyArrow types using from_arrow."""

    # Define the mapping of method name -> (pyarrow_func, description)
    type_definitions = {
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

    for method_name, (pa_func, description) in type_definitions.items():

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
@generate_arrow_factory_methods
class DataType:
    """A simplified Ray Data DataType supporting Arrow, NumPy, and Python types."""

    _internal_type: Union[pa.DataType, np.dtype, type]

    def __post_init__(self):
        """Validate the _internal_type after initialization."""
        if not isinstance(self._internal_type, (pa.DataType, np.dtype, type)):
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
    def to_arrow_dtype(self) -> pa.DataType:
        if self.is_arrow_type():
            return self._internal_type
        elif self.is_numpy_type():
            return pa.from_numpy_dtype(self._internal_type)
        else:
            try:
                return pa.from_numpy_dtype(np.dtype(self._internal_type))
            except (TypeError, pa.ArrowNotImplementedError):
                return pa.py_object()

    def to_numpy_dtype(self) -> np.dtype:
        if self.is_numpy_type():
            return self._internal_type
        elif self.is_arrow_type():
            return self._internal_type.to_pandas_dtype()
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
    def from_python(cls, python_type: type) -> "DataType":
        """Create a DataType from a Python type.

        Args:
            python_type: A Python type object

        Returns:
            DataType: A DataType wrapping the given Python type

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.from_python(int)
            DataType(python:int)
            >>> DataType.from_python(str)
            DataType(python:str)
        """
        return cls(_internal_type=python_type)

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
        # 2. Handle PyArrow scalars
        elif (
            hasattr(value, "type")
            and hasattr(pa, "Scalar")
            and isinstance(value, pa.Scalar)
        ):
            return cls.from_arrow(value.type)

        # 3. Try PyArrow type inference for regular Python values
        else:
            try:
                inferred_arrow_type = pa.infer_type([value])
                return cls.from_arrow(inferred_arrow_type)
            except pa.ArrowInvalid:
                # Fall back to Python type if Arrow type inference fails
                return cls.from_python(type(value))

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
        return self._internal_type == other._internal_type

    def __hash__(self) -> int:
        return hash(self._internal_type)
