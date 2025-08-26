from dataclasses import dataclass
from typing import Any, Union

import numpy as np
import pyarrow as pa

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class DataType:
    """A simplified Ray Data DataType supporting Arrow, NumPy, and Python types."""

    internal_type: Union[pa.DataType, np.dtype, type]

    def __post_init__(self):
        """Validate the internal_type after initialization."""
        if not isinstance(self.internal_type, (pa.DataType, np.dtype, type)):
            raise TypeError(
                "DataType supports only PyArrow DataType, NumPy dtype, or Python type."
            )

    # Type checking methods
    def is_arrow_type(self) -> bool:
        return isinstance(self.internal_type, pa.DataType)

    def is_numpy_type(self) -> bool:
        return isinstance(self.internal_type, np.dtype)

    def is_python_type(self) -> bool:
        return isinstance(self.internal_type, type)

    # Conversion methods
    def to_arrow_dtype(self) -> pa.DataType:
        if self.is_arrow_type():
            return self.internal_type
        elif self.is_numpy_type():
            return pa.from_numpy_dtype(self.internal_type)
        else:
            try:
                return pa.from_numpy_dtype(np.dtype(self.internal_type))
            except (TypeError, pa.ArrowNotImplementedError):
                return pa.py_object()

    def to_numpy_dtype(self) -> np.dtype:
        if self.is_numpy_type():
            return self.internal_type
        elif self.is_arrow_type():
            return self.internal_type.to_pandas_dtype()
        else:
            return np.dtype("object")

    def to_python_type(self) -> type:
        if self.is_python_type():
            return self.internal_type
        else:
            raise ValueError(f"DataType {self} is not a Python type")

    # Factory methods for Arrow types
    @classmethod
    def int8(cls) -> "DataType":
        """Create a DataType representing an 8-bit signed integer.

        Returns:
            DataType: A DataType with PyArrow int8 type
        """
        return cls(internal_type=pa.int8())

    @classmethod
    def int16(cls) -> "DataType":
        """Create a DataType representing a 16-bit signed integer.

        Returns:
            DataType: A DataType with PyArrow int16 type
        """
        return cls(internal_type=pa.int16())

    @classmethod
    def int32(cls) -> "DataType":
        """Create a DataType representing a 32-bit signed integer.

        Returns:
            DataType: A DataType with PyArrow int32 type
        """
        return cls(internal_type=pa.int32())

    @classmethod
    def int64(cls) -> "DataType":
        """Create a DataType representing a 64-bit signed integer.

        Returns:
            DataType: A DataType with PyArrow int64 type
        """
        return cls(internal_type=pa.int64())

    @classmethod
    def uint8(cls) -> "DataType":
        """Create a DataType representing an 8-bit unsigned integer.

        Returns:
            DataType: A DataType with PyArrow uint8 type
        """
        return cls(internal_type=pa.uint8())

    @classmethod
    def uint16(cls) -> "DataType":
        """Create a DataType representing a 16-bit unsigned integer.

        Returns:
            DataType: A DataType with PyArrow uint16 type
        """
        return cls(internal_type=pa.uint16())

    @classmethod
    def uint32(cls) -> "DataType":
        """Create a DataType representing a 32-bit unsigned integer.

        Returns:
            DataType: A DataType with PyArrow uint32 type
        """
        return cls(internal_type=pa.uint32())

    @classmethod
    def uint64(cls) -> "DataType":
        """Create a DataType representing a 64-bit unsigned integer.

        Returns:
            DataType: A DataType with PyArrow uint64 type
        """
        return cls(internal_type=pa.uint64())

    @classmethod
    def float32(cls) -> "DataType":
        """Create a DataType representing a 32-bit floating point number.

        Returns:
            DataType: A DataType with PyArrow float32 type
        """
        return cls(internal_type=pa.float32())

    @classmethod
    def float64(cls) -> "DataType":
        """Create a DataType representing a 64-bit floating point number.

        Returns:
            DataType: A DataType with PyArrow float64 type
        """
        return cls(internal_type=pa.float64())

    @classmethod
    def string(cls) -> "DataType":
        """Create a DataType representing a variable-length string.

        Returns:
            DataType: A DataType with PyArrow string type
        """
        return cls(internal_type=pa.string())

    @classmethod
    def bool(cls) -> "DataType":
        """Create a DataType representing a boolean value.

        Returns:
            DataType: A DataType with PyArrow boolean type
        """
        return cls(internal_type=pa.bool_())

    @classmethod
    def binary(cls) -> "DataType":
        """Create a DataType representing variable-length binary data.

        Returns:
            DataType: A DataType with PyArrow binary type
        """
        return cls(internal_type=pa.binary())

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
        return cls(internal_type=arrow_type)

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
        return cls(internal_type=numpy_dtype)

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
        return cls(internal_type=python_type)

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
        if isinstance(value, np.ndarray):
            return cls.from_numpy(value.dtype)
        elif hasattr(value, "dtype") and hasattr(value, "item"):
            # This catches numpy scalars (e.g., np.int32(5), np.float64(3.14))
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
            return f"DataType(arrow:{self.internal_type})"
        elif self.is_numpy_type():
            return f"DataType(numpy:{self.internal_type})"
        else:
            return f"DataType(python:{self.internal_type.__name__})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, DataType):
            return False
        return self.internal_type == other.internal_type

    def __hash__(self) -> int:
        return hash(self.internal_type)
