from typing import Union

import numpy as np
import pyarrow as pa

try:
    from pydantic import BaseModel, field_validator
except ImportError:
    pass


from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class DataType(BaseModel):
    """A simplified Ray Data DataType supporting Arrow, NumPy, and Python types."""

    internal_type: Union[pa.DataType, np.dtype, type]

    class Config:
        arbitrary_types_allowed = True

    @field_validator("internal_type")
    @classmethod
    def validate_type(cls, v):
        if not isinstance(v, (pa.DataType, np.dtype, type)):
            raise TypeError(
                "DataType supports only PyArrow DataType, NumPy dtype, or Python type."
            )
        return v

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
            return pa.string()

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
