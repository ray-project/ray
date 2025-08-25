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
        return cls(internal_type=pa.int8())

    @classmethod
    def int16(cls) -> "DataType":
        return cls(internal_type=pa.int16())

    @classmethod
    def int32(cls) -> "DataType":
        return cls(internal_type=pa.int32())

    @classmethod
    def int64(cls) -> "DataType":
        return cls(internal_type=pa.int64())

    @classmethod
    def uint8(cls) -> "DataType":
        return cls(internal_type=pa.uint8())

    @classmethod
    def uint16(cls) -> "DataType":
        return cls(internal_type=pa.uint16())

    @classmethod
    def uint32(cls) -> "DataType":
        return cls(internal_type=pa.uint32())

    @classmethod
    def uint64(cls) -> "DataType":
        return cls(internal_type=pa.uint64())

    @classmethod
    def float32(cls) -> "DataType":
        return cls(internal_type=pa.float32())

    @classmethod
    def float64(cls) -> "DataType":
        return cls(internal_type=pa.float64())

    @classmethod
    def string(cls) -> "DataType":
        return cls(internal_type=pa.string())

    @classmethod
    def bool(cls) -> "DataType":
        return cls(internal_type=pa.bool_())

    @classmethod
    def binary(cls) -> "DataType":
        return cls(internal_type=pa.binary())

    # Factory methods from external systems
    @classmethod
    def from_arrow(cls, arrow_type: pa.DataType) -> "DataType":
        return cls(internal_type=arrow_type)

    @classmethod
    def from_numpy(cls, numpy_dtype: Union[np.dtype, str]) -> "DataType":
        if isinstance(numpy_dtype, str):
            numpy_dtype = np.dtype(numpy_dtype)
        return cls(internal_type=numpy_dtype)

    @classmethod
    def from_python(cls, python_type: type) -> "DataType":
        return cls(internal_type=python_type)

    @classmethod
    def infer_dtype(cls, value: Any) -> "DataType":
        """Infer DataType from a Python value, handling numpy, Arrow, and Python types.

        Args:
            value: Any Python value to infer the type from

        Returns:
            DataType: The inferred data type

        Examples:
            >>> DataType.infer_dtype(5)  # DataType(arrow:int64)
            >>> DataType.infer_dtype(np.int32(5))  # DataType(numpy:int32)
            >>> DataType.infer_dtype("hello")  # DataType(arrow:string)
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
        return hash(repr(self))
