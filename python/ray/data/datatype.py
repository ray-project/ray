from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pyarrow as pa

from ray.air.util.tensor_extensions.arrow import (
    _infer_pyarrow_type,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class TypeCategory(str, Enum):
    """High-level categories of data types.

    These categories correspond to groups of concrete data types.
    Use DataType.is_of(category) to check if a DataType belongs to a category.
    """

    LIST = "list"
    LARGE_LIST = "large_list"
    STRUCT = "struct"
    MAP = "map"
    TENSOR = "tensor"
    TEMPORAL = "temporal"


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

    # Physical dtype: The concrete type implementation (e.g., pa.list_(pa.int64()), np.float64, str)
    _physical_dtype: Union[pa.DataType, np.dtype, type]

    def __post_init__(self):
        """Validate the _physical_dtype after initialization."""
        # TODO: Support Pandas extension types
        if not isinstance(
            self._physical_dtype,
            (pa.DataType, np.dtype, type),
        ):
            raise TypeError(
                f"DataType supports only PyArrow DataType, NumPy dtype, or Python type, but was given type {type(self._physical_dtype)}."
            )

    def is_of(self, category: Union["TypeCategory", str]) -> bool:
        """Check if this DataType belongs to a specific type category.

        Args:
            category: The category to check against.

        Returns:
            True if the DataType belongs to the category.
        """
        if isinstance(category, str):
            try:
                category = TypeCategory(category)
            except ValueError:
                return False

        if category == TypeCategory.LIST:
            return self.is_list_type()
        elif category == TypeCategory.LARGE_LIST:
            if not self.is_arrow_type():
                return False
            pa_type = self._physical_dtype
            return pa.types.is_large_list(pa_type) or (
                hasattr(pa.types, "is_large_list_view")
                and pa.types.is_large_list_view(pa_type)
            )
        elif category == TypeCategory.STRUCT:
            return self.is_struct_type()
        elif category == TypeCategory.MAP:
            return self.is_map_type()
        elif category == TypeCategory.TENSOR:
            return self.is_tensor_type()
        elif category == TypeCategory.TEMPORAL:
            return self.is_temporal_type()
        return False

    # Type checking methods
    def is_arrow_type(self) -> bool:
        """Check if this DataType is backed by a PyArrow DataType.

        Returns:
            bool: True if the internal type is a PyArrow DataType
        """
        return isinstance(self._physical_dtype, pa.DataType)

    def is_numpy_type(self) -> bool:
        """Check if this DataType is backed by a NumPy dtype.

        Returns:
            bool: True if the internal type is a NumPy dtype
        """
        return isinstance(self._physical_dtype, np.dtype)

    def is_python_type(self) -> bool:
        """Check if this DataType is backed by a Python type.

        Returns:
            bool: True if the internal type is a Python type
        """
        return isinstance(self._physical_dtype, type)

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
            return self._physical_dtype
        else:
            if isinstance(self._physical_dtype, np.dtype):
                return pa.from_numpy_dtype(self._physical_dtype)
            else:
                assert (
                    values is not None and len(values) > 0
                ), "Values are required to infer Arrow type if the provided type is a Python type"
                return _infer_pyarrow_type(values)

    def to_numpy_dtype(self) -> np.dtype:
        """Convert the DataType to a NumPy dtype.

        For PyArrow types, attempts to convert via pandas dtype.
        For Python types, returns object dtype.

        Returns:
            np.dtype: A NumPy dtype representation

        Examples:
            >>> import numpy as np
            >>> DataType.from_numpy(np.dtype('int64')).to_numpy_dtype()
            dtype('int64')
            >>> DataType.from_numpy(np.dtype('float32')).to_numpy_dtype()
            dtype('float32')
        """
        if self.is_numpy_type():
            return self._physical_dtype
        elif self.is_arrow_type():
            try:
                # For most basic arrow types, this will work
                pandas_dtype = self._physical_dtype.to_pandas_dtype()
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
        """Get the internal type if it's a Python type.

        This method doesn't perform conversion, it only returns the internal
        type if it's already a Python type.

        Returns:
            type: The internal Python type

        Raises:
            ValueError: If the DataType is not backed by a Python type

        Examples:
            >>> dt = DataType(int)
            >>> dt.to_python_type()
            <class 'int'>
            >>> DataType.int64().to_python_type()  # doctest: +SKIP
            ValueError: DataType is not backed by a Python type
        """
        if self.is_python_type():
            return self._physical_dtype
        else:
            raise ValueError(
                f"DataType {self} is not backed by a Python type. "
                f"Use to_arrow_dtype() or to_numpy_dtype() for conversion."
            )

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
        return cls(_physical_dtype=arrow_type)

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
        return cls(_physical_dtype=numpy_dtype)

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
        # 2. Try PyArrow type inference for regular Python values
        try:
            inferred_arrow_type = _infer_pyarrow_type([value])
            if inferred_arrow_type is not None:
                return cls.from_arrow(inferred_arrow_type)
        except Exception:
            return cls(type(value))

    def __repr__(self) -> str:
        if self.is_arrow_type():
            return f"DataType(arrow:{self._physical_dtype})"
        elif self.is_numpy_type():
            return f"DataType(numpy:{self._physical_dtype})"
        else:
            return f"DataType(python:{self._physical_dtype.__name__})"

    def __eq__(self, other: "DataType") -> bool:
        if not isinstance(other, DataType):
            return False

        # Ensure they're from the same type system by checking the actual type
        # of the internal type object, not just the value
        if type(self._physical_dtype) is not type(other._physical_dtype):
            return False

        return self._physical_dtype == other._physical_dtype

    def __hash__(self) -> int:
        # Include the type of the internal type in the hash to ensure
        # different type systems don't collide
        return hash((type(self._physical_dtype), self._physical_dtype))

    @classmethod
    def list(cls, value_type: "DataType") -> "DataType":
        """Create a DataType representing a list with the given element type.

        Args:
            value_type: The DataType of the list elements.

        Returns:
            DataType: A DataType with PyArrow list type

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.list(DataType.int64())  # Exact match: list<int64>
            DataType(arrow:list<item: int64>)
        """
        value_arrow_type = value_type.to_arrow_dtype()
        return cls.from_arrow(pa.list_(value_arrow_type))

    @classmethod
    def large_list(cls, value_type: "DataType") -> "DataType":
        """Create a DataType representing a large_list with the given element type.

        Args:
            value_type: The DataType of the list elements.

        Returns:
            DataType: A DataType with PyArrow large_list type

        Examples:
            >>> DataType.large_list(DataType.int64())
            DataType(arrow:large_list<item: int64>)
        """
        value_arrow_type = value_type.to_arrow_dtype()
        return cls.from_arrow(pa.large_list(value_arrow_type))

    @classmethod
    def fixed_size_list(cls, value_type: "DataType", list_size: int) -> "DataType":
        """Create a DataType representing a fixed-size list.

        Args:
            value_type: The DataType of the list elements
            list_size: The fixed size of the list

        Returns:
            DataType: A DataType with PyArrow fixed_size_list type

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.fixed_size_list(DataType.float32(), 3)
            DataType(arrow:fixed_size_list<item: float>[3])
        """
        value_arrow_type = value_type.to_arrow_dtype()
        return cls.from_arrow(pa.list_(value_arrow_type, list_size))

    @classmethod
    def struct(cls, fields: List[Tuple[str, "DataType"]]) -> "DataType":
        """Create a DataType representing a struct with the given fields.

        Args:
            fields: List of (field_name, field_type) tuples.

        Returns:
            DataType: A DataType with PyArrow struct type

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.struct([("x", DataType.int64()), ("y", DataType.float64())])
            DataType(arrow:struct<x: int64, y: double>)
        """
        arrow_fields = [(name, dtype.to_arrow_dtype()) for name, dtype in fields]
        return cls.from_arrow(pa.struct(arrow_fields))

    @classmethod
    def map(
        cls,
        key_type: "DataType",
        value_type: "DataType",
    ) -> "DataType":
        """Create a DataType representing a map with the given key and value types.

        Args:
            key_type: The DataType of the map keys.
            value_type: The DataType of the map values.

        Returns:
            DataType: A DataType with PyArrow map type

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.map(DataType.string(), DataType.int64())
            DataType(arrow:map<string, int64>)
        """
        key_arrow_type = key_type.to_arrow_dtype()
        value_arrow_type = value_type.to_arrow_dtype()
        return cls.from_arrow(pa.map_(key_arrow_type, value_arrow_type))

    @classmethod
    def tensor(
        cls,
        shape: Tuple[int, ...],
        dtype: "DataType",
    ) -> "DataType":
        """Create a DataType representing a fixed-shape tensor.

        Args:
            shape: The fixed shape of the tensor.
            dtype: The DataType of the tensor elements.

        Returns:
            DataType: A DataType with Ray's ArrowTensorType

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.tensor(shape=(3, 4), dtype=DataType.float32())  # doctest: +ELLIPSIS
            DataType(arrow:ArrowTensorType(...))
        """
        from ray.air.util.tensor_extensions.arrow import ArrowTensorType

        element_arrow_type = dtype.to_arrow_dtype()
        return cls.from_arrow(ArrowTensorType(shape, element_arrow_type))

    @classmethod
    def variable_shaped_tensor(
        cls,
        dtype: "DataType",
        ndim: int = 2,
    ) -> "DataType":
        """Create a DataType representing a variable-shaped tensor.

        Args:
            dtype: The DataType of the tensor elements.
            ndim: The number of dimensions of the tensor. Defaults to 2.

        Returns:
            DataType: A DataType with Ray's ArrowVariableShapedTensorType

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.variable_shaped_tensor(dtype=DataType.float32(), ndim=2)  # doctest: +ELLIPSIS
            DataType(arrow:ArrowVariableShapedTensorType(...))
        """
        from ray.air.util.tensor_extensions.arrow import ArrowVariableShapedTensorType

        element_arrow_type = dtype.to_arrow_dtype()
        return cls.from_arrow(ArrowVariableShapedTensorType(element_arrow_type, ndim))

    @classmethod
    def temporal(
        cls,
        temporal_type: str,
        unit: Optional[str] = None,
        tz: Optional[str] = None,
    ) -> "DataType":
        """Create a DataType representing a temporal type.

        Args:
            temporal_type: Type of temporal value - one of:
                - "timestamp": Timestamp with optional unit and timezone
                - "date32": 32-bit date (days since UNIX epoch)
                - "date64": 64-bit date (milliseconds since UNIX epoch)
                - "time32": 32-bit time of day (s or ms precision)
                - "time64": 64-bit time of day (us or ns precision)
                - "duration": Time duration with unit
            unit: Time unit for timestamp/time/duration types:
                - timestamp: "s", "ms", "us", "ns" (default: "us")
                - time32: "s", "ms" (default: "s")
                - time64: "us", "ns" (default: "us")
                - duration: "s", "ms", "us", "ns" (default: "us")
            tz: Optional timezone string for timestamp types (e.g., "UTC", "America/New_York")

        Returns:
            DataType: A DataType with PyArrow temporal type

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.temporal("timestamp", unit="s")
            DataType(arrow:timestamp[s])
            >>> DataType.temporal("timestamp", unit="us", tz="UTC")
            DataType(arrow:timestamp[us, tz=UTC])
            >>> DataType.temporal("date32")
            DataType(arrow:date32[day])
            >>> DataType.temporal("time64", unit="ns")
            DataType(arrow:time64[ns])
            >>> DataType.temporal("duration", unit="ms")
            DataType(arrow:duration[ms])
        """
        temporal_type_lower = temporal_type.lower()

        if temporal_type_lower == "timestamp":
            unit = unit or "us"
            return cls.from_arrow(pa.timestamp(unit, tz=tz))
        elif temporal_type_lower == "date32":
            return cls.from_arrow(pa.date32())
        elif temporal_type_lower == "date64":
            return cls.from_arrow(pa.date64())
        elif temporal_type_lower == "time32":
            unit = unit or "s"
            if unit not in ("s", "ms"):
                raise ValueError(f"time32 unit must be 's' or 'ms', got {unit}")
            return cls.from_arrow(pa.time32(unit))
        elif temporal_type_lower == "time64":
            unit = unit or "us"
            if unit not in ("us", "ns"):
                raise ValueError(f"time64 unit must be 'us' or 'ns', got {unit}")
            return cls.from_arrow(pa.time64(unit))
        elif temporal_type_lower == "duration":
            unit = unit or "us"
            return cls.from_arrow(pa.duration(unit))
        else:
            raise ValueError(
                f"Invalid temporal_type '{temporal_type}'. Must be one of: "
                f"'timestamp', 'date32', 'date64', 'time32', 'time64', 'duration'"
            )

    def is_list_type(self) -> bool:
        """Check if this DataType represents a list type

        Returns:
            True if this is any list variant (list, large_list, fixed_size_list)

        Examples:
            >>> DataType.list(DataType.int64()).is_list_type()
            True
            >>> DataType.int64().is_list_type()
            False
        """
        if not self.is_arrow_type():
            return False

        pa_type = self._physical_dtype
        return (
            pa.types.is_list(pa_type)
            or pa.types.is_large_list(pa_type)
            or pa.types.is_fixed_size_list(pa_type)
            # Pyarrow 16.0.0+ supports list views
            or (hasattr(pa.types, "is_list_view") and pa.types.is_list_view(pa_type))
            or (
                hasattr(pa.types, "is_large_list_view")
                and pa.types.is_large_list_view(pa_type)
            )
        )

    def is_tensor_type(self) -> bool:
        """Check if this DataType represents a tensor type.

        Returns:
            True if this is a tensor type
        """
        if not self.is_arrow_type():
            return False

        from ray.air.util.tensor_extensions.arrow import (
            get_arrow_extension_tensor_types,
        )

        return isinstance(self._physical_dtype, get_arrow_extension_tensor_types())

    def is_struct_type(self) -> bool:
        """Check if this DataType represents a struct type.

        Returns:
            True if this is a struct type

        Examples:
            >>> DataType.struct([("x", DataType.int64())]).is_struct_type()
            True
            >>> DataType.int64().is_struct_type()
            False
        """
        if not self.is_arrow_type():
            return False
        return pa.types.is_struct(self._physical_dtype)

    def is_map_type(self) -> bool:
        """Check if this DataType represents a map type.

        Returns:
            True if this is a map type

        Examples:
            >>> DataType.map(DataType.string(), DataType.int64()).is_map_type()
            True
            >>> DataType.int64().is_map_type()
            False
        """
        if not self.is_arrow_type():
            return False
        return pa.types.is_map(self._physical_dtype)

    def is_nested_type(self) -> bool:
        """Check if this DataType represents a nested type.

        Nested types include: lists, structs, maps, unions

        Returns:
            True if this is any nested type

        Examples:
            >>> DataType.list(DataType.int64()).is_nested_type()
            True
            >>> DataType.struct([("x", DataType.int64())]).is_nested_type()
            True
            >>> DataType.int64().is_nested_type()
            False
        """
        if not self.is_arrow_type():
            return False
        return pa.types.is_nested(self._physical_dtype)

    def _get_underlying_arrow_type(self) -> pa.DataType:
        """Get the underlying Arrow type, handling dictionary and run-end encoding.

        Returns:
            The underlying PyArrow type, unwrapping dictionary/run-end encoding

        Raises:
            ValueError: If called on a non-Arrow type (pattern-matching, NumPy, or Python types)
        """
        if not self.is_arrow_type():
            raise ValueError(
                f"Cannot get Arrow type for non-Arrow DataType {self}. "
                f"Type is: {type(self._physical_dtype)}"
            )

        pa_type = self._physical_dtype
        if pa.types.is_dictionary(pa_type):
            return pa_type.value_type
        elif pa.types.is_run_end_encoded(pa_type):
            return pa_type.value_type
        return pa_type

    def is_numerical_type(self) -> bool:
        """Check if this DataType represents a numerical type.

        Numerical types support arithmetic operations and include:
        integers, floats, decimals

        Returns:
            True if this is a numerical type

        Examples:
            >>> DataType.int64().is_numerical_type()
            True
            >>> DataType.float32().is_numerical_type()
            True
            >>> DataType.string().is_numerical_type()
            False
        """
        if self.is_arrow_type():
            underlying = self._get_underlying_arrow_type()
            return (
                pa.types.is_integer(underlying)
                or pa.types.is_floating(underlying)
                or pa.types.is_decimal(underlying)
            )
        elif self.is_numpy_type():
            return (
                np.issubdtype(self._physical_dtype, np.integer)
                or np.issubdtype(self._physical_dtype, np.floating)
                or np.issubdtype(self._physical_dtype, np.complexfloating)
            )
        elif self.is_python_type():
            return self._physical_dtype in (int, float, complex)
        return False

    def is_string_type(self) -> bool:
        """Check if this DataType represents a string type.

        Includes: string, large_string, string_view

        Returns:
            True if this is a string type

        Examples:
            >>> DataType.string().is_string_type()
            True
            >>> DataType.int64().is_string_type()
            False
        """
        if self.is_arrow_type():
            underlying = self._get_underlying_arrow_type()
            return (
                pa.types.is_string(underlying)
                or pa.types.is_large_string(underlying)
                or (
                    hasattr(pa.types, "is_string_view")
                    and pa.types.is_string_view(underlying)
                )
            )
        elif self.is_numpy_type():
            # Check for Unicode (U) or byte string (S) types
            return self._physical_dtype.kind in ("U", "S")
        elif self.is_python_type():
            return self._physical_dtype is str
        return False

    def is_binary_type(self) -> bool:
        """Check if this DataType represents a binary type.

        Includes: binary, large_binary, binary_view, fixed_size_binary

        Returns:
            True if this is a binary type

        Examples:
            >>> DataType.binary().is_binary_type()
            True
            >>> DataType.string().is_binary_type()
            False
        """
        if self.is_arrow_type():
            underlying = self._get_underlying_arrow_type()
            return (
                pa.types.is_binary(underlying)
                or pa.types.is_large_binary(underlying)
                or (
                    hasattr(pa.types, "is_binary_view")
                    and pa.types.is_binary_view(underlying)
                )
                or pa.types.is_fixed_size_binary(underlying)
            )
        elif self.is_numpy_type():
            # NumPy doesn't have a specific binary type, but void or object dtypes might contain bytes
            return self._physical_dtype.kind == "V"  # void type (raw bytes)
        elif self.is_python_type():
            return self._physical_dtype in (bytes, bytearray)
        return False

    def is_temporal_type(self) -> bool:
        """Check if this DataType represents a temporal type.

        Includes: date, time, timestamp, duration, interval

        Returns:
            True if this is a temporal type

        Examples:
            >>> import pyarrow as pa
            >>> DataType.from_arrow(pa.timestamp('s')).is_temporal_type()
            True
            >>> DataType.int64().is_temporal_type()
            False
        """
        if self.is_arrow_type():
            underlying = self._get_underlying_arrow_type()
            return pa.types.is_temporal(underlying)
        elif self.is_numpy_type():
            return np.issubdtype(self._physical_dtype, np.datetime64) or np.issubdtype(
                self._physical_dtype, np.timedelta64
            )
        elif self.is_python_type():
            import datetime

            return self._physical_dtype in (
                datetime.datetime,
                datetime.date,
                datetime.time,
                datetime.timedelta,
            )
        return False
