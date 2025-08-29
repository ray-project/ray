from dataclasses import dataclass
from typing import Any, Union

import numpy as np
import pyarrow as pa

from ray.data._internal.pyarrow_compat import PyArrowCompat
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
@PyArrowCompat.factory_methods
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
    def to_arrow_dtype(self) -> pa.DataType:
        if self.is_arrow_type():
            return self._internal_type
        else:
            return PyArrowCompat.convert_to_arrow_type(self._internal_type)

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
        # 3. Try PyArrow type inference for regular Python values
        try:
            inferred_arrow_type = PyArrowCompat.infer_type([value])
            if inferred_arrow_type is not None:
                return cls.from_arrow(inferred_arrow_type)
        except Exception:
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

        # Ensure they're from the same type system by checking the actual type
        # of the internal type object, not just the value
        if type(self._internal_type) is not type(other._internal_type):
            return False

        return self._internal_type == other._internal_type

    def __hash__(self) -> int:
        # Include the type of the internal type in the hash to ensure
        # different type systems don't collide
        return hash((type(self._internal_type), self._internal_type))
