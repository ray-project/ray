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
    _pattern_category: Optional[
        str
    ] = None  # Used for pattern matching (e.g., "temporal", "list", "struct")

    # Sentinel value to represent "any type" for pattern matching in nested types
    class _AnyType:
        """Sentinel to indicate 'any type' when creating pattern-matching DataTypes."""

        def __repr__(self):
            return "ANY"

    ANY = _AnyType()

    def __post_init__(self):
        """Validate the _internal_type after initialization."""
        # Allow the ANY sentinel as a special case
        if isinstance(self._internal_type, DataType._AnyType):
            return

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
        if isinstance(self._internal_type, DataType._AnyType):
            if self._pattern_category:
                return f"DataType(pattern:{self._pattern_category})"
            return "DataType(pattern:ANY)"
        elif self.is_arrow_type():
            return f"DataType(arrow:{self._internal_type})"
        elif self.is_numpy_type():
            return f"DataType(numpy:{self._internal_type})"
        else:
            return f"DataType(python:{self._internal_type.__name__})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, DataType):
            return False

        # Handle ANY sentinel
        if isinstance(self._internal_type, DataType._AnyType) or isinstance(
            other._internal_type, DataType._AnyType
        ):
            # Both must be ANY and have the same pattern category
            if not (
                isinstance(self._internal_type, DataType._AnyType)
                and isinstance(other._internal_type, DataType._AnyType)
            ):
                return False
            return self._pattern_category == other._pattern_category

        # Ensure they're from the same type system by checking the actual type
        # of the internal type object, not just the value
        if type(self._internal_type) is not type(other._internal_type):
            return False

        return self._internal_type == other._internal_type

    def __hash__(self) -> int:
        # Handle ANY sentinel
        if isinstance(self._internal_type, DataType._AnyType):
            return hash(("ANY", DataType._AnyType, self._pattern_category))
        # Include the type of the internal type in the hash to ensure
        # different type systems don't collide
        return hash((type(self._internal_type), self._internal_type))

    @classmethod
    def list_(
        cls, value_type: Union["DataType", "_AnyType", None] = None
    ) -> "DataType":
        """Create a DataType representing a list with the given element type.

        If value_type is DataType.ANY or None, creates a pattern that matches any list type.

        Args:
            value_type: The DataType of the list elements, or DataType.ANY to match any list

        Returns:
            DataType: A DataType with PyArrow list type or a pattern-matching DataType

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.list_(DataType.int64())  # Exact match: list<int64>
            DataType(arrow:list<item: int64>)
            >>> DataType.list_(DataType.ANY)  # Pattern: matches any list
            DataType(list_pattern)
            >>> DataType.list_()  # Same as DataType.list_(DataType.ANY)
            DataType(list_pattern)
        """
        if value_type is None or isinstance(value_type, DataType._AnyType):
            # Create a special pattern-matching DataType for "any list"
            return cls(_internal_type=DataType.ANY, _pattern_category="list")

        value_arrow_type = value_type.to_arrow_dtype()
        return cls.from_arrow(pa.list_(value_arrow_type))

    @classmethod
    def large_list(
        cls, value_type: Union["DataType", "_AnyType", None] = None
    ) -> "DataType":
        """Create a DataType representing a large_list with the given element type.

        If value_type is DataType.ANY or None, creates a pattern that matches any large_list type.

        Args:
            value_type: The DataType of the list elements, or DataType.ANY to match any large_list

        Returns:
            DataType: A DataType with PyArrow large_list type or a pattern-matching DataType

        Examples:
            >>> DataType.large_list(DataType.int64())  # Exact match
            DataType(arrow:large_list<item: int64>)
            >>> DataType.large_list(DataType.ANY)  # Pattern: matches any large_list
            DataType(large_list_pattern)
        """
        if value_type is None or isinstance(value_type, DataType._AnyType):
            return cls(_internal_type=DataType.ANY, _pattern_category="list")

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
    def struct(
        cls, fields: Union[List[Tuple[str, "DataType"]], "_AnyType", None] = None
    ) -> "DataType":
        """Create a DataType representing a struct with the given fields.

        If fields is DataType.ANY or None, creates a pattern that matches any struct type.

        Args:
            fields: List of (field_name, field_type) tuples, or DataType.ANY to match any struct

        Returns:
            DataType: A DataType with PyArrow struct type or a pattern-matching DataType

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.struct([("x", DataType.int64()), ("y", DataType.float64())])
            DataType(arrow:struct<x: int64, y: double>)
            >>> DataType.struct(DataType.ANY)  # Pattern: matches any struct
            DataType(struct_pattern)
            >>> DataType.struct()  # Same as DataType.struct(DataType.ANY)
            DataType(struct_pattern)
        """
        if fields is None or isinstance(fields, DataType._AnyType):
            return cls(_internal_type=DataType.ANY, _pattern_category="struct")

        arrow_fields = [(name, dtype.to_arrow_dtype()) for name, dtype in fields]
        return cls.from_arrow(pa.struct(arrow_fields))

    @classmethod
    def map_(
        cls,
        key_type: Union["DataType", "_AnyType", None] = None,
        value_type: Union["DataType", "_AnyType", None] = None,
    ) -> "DataType":
        """Create a DataType representing a map with the given key and value types.

        If key_type or value_type is DataType.ANY or None, creates a pattern that matches any map type.

        Args:
            key_type: The DataType of the map keys, or DataType.ANY to match any map
            value_type: The DataType of the map values (ignored if key_type is ANY)

        Returns:
            DataType: A DataType with PyArrow map type or a pattern-matching DataType

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.map_(DataType.string(), DataType.int64())
            DataType(arrow:map<string, int64>)
            >>> DataType.map_(DataType.ANY)  # Pattern: matches any map
            DataType(map_pattern)
            >>> DataType.map_()  # Same as DataType.map_(DataType.ANY)
            DataType(map_pattern)
        """
        if key_type is None or isinstance(key_type, DataType._AnyType):
            return cls(_internal_type=DataType.ANY, _pattern_category="map")

        if value_type is None:
            value_type = DataType.from_arrow(pa.null())

        key_arrow_type = key_type.to_arrow_dtype()
        value_arrow_type = value_type.to_arrow_dtype()
        return cls.from_arrow(pa.map_(key_arrow_type, value_arrow_type))

    @classmethod
    def tensor(
        cls,
        shape: Union[Tuple[int, ...], "_AnyType", None] = None,
        dtype: Union["DataType", "_AnyType", None] = None,
    ) -> "DataType":
        """Create a DataType representing a fixed-shape tensor.

        If shape or dtype is DataType.ANY or None, creates a pattern that matches any tensor type.

        Args:
            shape: The fixed shape of the tensor, or DataType.ANY to match any tensor
            dtype: The DataType of the tensor elements

        Returns:
            DataType: A DataType with Ray's ArrowTensorType or a pattern-matching DataType

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.tensor(shape=(3, 4), dtype=DataType.float32())
            DataType(arrow:extension<...>)
            >>> DataType.tensor(DataType.ANY)  # Pattern: matches any tensor
            DataType(tensor_pattern)
            >>> DataType.tensor()  # Same as DataType.tensor(DataType.ANY)
            DataType(tensor_pattern)
        """
        if shape is None or isinstance(shape, DataType._AnyType):
            return cls(_internal_type=DataType.ANY, _pattern_category="list")

        if dtype is None:
            dtype = DataType.float32()

        from ray.air.util.tensor_extensions.arrow import ArrowTensorType

        element_arrow_type = dtype.to_arrow_dtype()
        return cls.from_arrow(ArrowTensorType(shape, element_arrow_type))

    @classmethod
    def variable_shaped_tensor(
        cls,
        dtype: Union["DataType", "_AnyType", None] = None,
        ndim: Optional[int] = None,
    ) -> "DataType":
        """Create a DataType representing a variable-shaped tensor.

        If dtype is DataType.ANY or None, creates a pattern that matches any variable-shaped tensor.

        Args:
            dtype: The DataType of the tensor elements, or DataType.ANY
            ndim: The number of dimensions of the tensor

        Returns:
            DataType: A DataType with Ray's ArrowVariableShapedTensorType or pattern-matching DataType

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.variable_shaped_tensor(dtype=DataType.float32(), ndim=2)
            DataType(arrow:extension<...>)
            >>> DataType.variable_shaped_tensor(DataType.ANY)  # Pattern: matches any var tensor
            DataType(var_tensor_pattern)
            >>> DataType.variable_shaped_tensor()  # Same as above
            DataType(var_tensor_pattern)
        """
        if dtype is None or isinstance(dtype, DataType._AnyType):
            return cls(_internal_type=DataType.ANY, _pattern_category="list")

        if ndim is None:
            ndim = 2

        from ray.air.util.tensor_extensions.arrow import ArrowVariableShapedTensorType

        element_arrow_type = dtype.to_arrow_dtype()
        return cls.from_arrow(ArrowVariableShapedTensorType(element_arrow_type, ndim))

    @classmethod
    def temporal_(
        cls,
        temporal_type: Union[str, "_AnyType", None] = None,
        unit: Optional[str] = None,
        tz: Optional[str] = None,
    ) -> "DataType":
        """Create a DataType representing a temporal type.

        If temporal_type is DataType.ANY or None, creates a pattern that matches any temporal type.

        Args:
            temporal_type: Type of temporal value - one of:
                - "timestamp": Timestamp with optional unit and timezone
                - "date32": 32-bit date (days since UNIX epoch)
                - "date64": 64-bit date (milliseconds since UNIX epoch)
                - "time32": 32-bit time of day (s or ms precision)
                - "time64": 64-bit time of day (us or ns precision)
                - "duration": Time duration with unit
                - DataType.ANY or None: Pattern to match any temporal type
            unit: Time unit for timestamp/time/duration types:
                - timestamp: "s", "ms", "us", "ns" (default: "us")
                - time32: "s", "ms" (default: "s")
                - time64: "us", "ns" (default: "us")
                - duration: "s", "ms", "us", "ns" (default: "us")
            tz: Optional timezone string for timestamp types (e.g., "UTC", "America/New_York")

        Returns:
            DataType: A DataType with PyArrow temporal type or a pattern-matching DataType

        Examples:
            >>> from ray.data.datatype import DataType
            >>> DataType.temporal_("timestamp", unit="s")
            DataType(arrow:timestamp[s])
            >>> DataType.temporal_("timestamp", unit="us", tz="UTC")
            DataType(arrow:timestamp[us, tz=UTC])
            >>> DataType.temporal_("date32")
            DataType(arrow:date32)
            >>> DataType.temporal_("time64", unit="ns")
            DataType(arrow:time64[ns])
            >>> DataType.temporal_("duration", unit="ms")
            DataType(arrow:duration[ms])
            >>> DataType.temporal_(DataType.ANY)  # Pattern: matches any temporal
            DataType(temporal_pattern)
            >>> DataType.temporal_()  # Same as DataType.temporal_(DataType.ANY)
            DataType(temporal_pattern)
        """
        if temporal_type is None or isinstance(temporal_type, DataType._AnyType):
            return cls(_internal_type=DataType.ANY, _pattern_category="temporal")

        temporal_type = temporal_type.lower()

        if temporal_type == "timestamp":
            unit = unit or "us"
            return cls.from_arrow(pa.timestamp(unit, tz=tz))
        elif temporal_type == "date32":
            return cls.from_arrow(pa.date32())
        elif temporal_type == "date64":
            return cls.from_arrow(pa.date64())
        elif temporal_type == "time32":
            unit = unit or "s"
            if unit not in ("s", "ms"):
                raise ValueError(f"time32 unit must be 's' or 'ms', got {unit}")
            return cls.from_arrow(pa.time32(unit))
        elif temporal_type == "time64":
            unit = unit or "us"
            if unit not in ("us", "ns"):
                raise ValueError(f"time64 unit must be 'us' or 'ns', got {unit}")
            return cls.from_arrow(pa.time64(unit))
        elif temporal_type == "duration":
            unit = unit or "us"
            return cls.from_arrow(pa.duration(unit))
        else:
            raise ValueError(
                f"Invalid temporal_type '{temporal_type}'. Must be one of: "
                f"'timestamp', 'date32', 'date64', 'time32', 'time64', 'duration'"
            )

    def is_list_type(self) -> bool:
        """Check if this DataType represents a list type (including tensors).

        Returns:
            True if this is any list variant (list, large_list, fixed_size_list, tensor, etc.)

        Examples:
            >>> DataType.list_(DataType.int64()).is_list_type()
            True
            >>> DataType.int64().is_list_type()
            False
        """
        if not self.is_arrow_type():
            return False

        from ray.air.util.tensor_extensions.arrow import (
            ArrowTensorType,
            ArrowTensorTypeV2,
            ArrowVariableShapedTensorType,
        )

        pa_type = self._internal_type
        return (
            pa.types.is_list(pa_type)
            or pa.types.is_large_list(pa_type)
            or pa.types.is_fixed_size_list(pa_type)
            or (hasattr(pa.types, "is_list_view") and pa.types.is_list_view(pa_type))
            or (
                hasattr(pa.types, "is_large_list_view")
                and pa.types.is_large_list_view(pa_type)
            )
            or isinstance(
                pa_type,
                (ArrowTensorType, ArrowTensorTypeV2, ArrowVariableShapedTensorType),
            )
        )

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
        return pa.types.is_struct(self._internal_type)

    def is_map_type(self) -> bool:
        """Check if this DataType represents a map type.

        Returns:
            True if this is a map type

        Examples:
            >>> DataType.map_(DataType.string(), DataType.int64()).is_map_type()
            True
            >>> DataType.int64().is_map_type()
            False
        """
        if not self.is_arrow_type():
            return False
        return pa.types.is_map(self._internal_type)

    def is_nested_type(self) -> bool:
        """Check if this DataType represents a nested type.

        Nested types include: lists, structs, maps, unions

        Returns:
            True if this is any nested type

        Examples:
            >>> DataType.list_(DataType.int64()).is_nested_type()
            True
            >>> DataType.struct([("x", DataType.int64())]).is_nested_type()
            True
            >>> DataType.int64().is_nested_type()
            False
        """
        if not self.is_arrow_type():
            return False
        return pa.types.is_nested(self._internal_type)

    def _get_underlying_arrow_type(self) -> pa.DataType:
        """Get the underlying Arrow type, handling dictionary and run-end encoding.

        Returns:
            The underlying PyArrow type, unwrapping dictionary/run-end encoding
        """
        if not self.is_arrow_type():
            return self._internal_type

        pa_type = self._internal_type
        if pa.types.is_dictionary(pa_type):
            return pa_type.value_type
        elif pa.types.is_run_end_encoded(pa_type):
            return pa_type.value_type
        return pa_type

    def is_numerical_type(self) -> bool:
        """Check if this DataType represents a numerical type.

        Numerical types support arithmetic operations and include:
        integers, floats, decimals, and booleans.

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
                np.issubdtype(self._internal_type, np.integer)
                or np.issubdtype(self._internal_type, np.floating)
                or np.issubdtype(self._internal_type, np.complexfloating)
            )
        elif self.is_python_type():
            return self._internal_type in (int, float, bool, complex)
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
            return self._internal_type.kind in ("U", "S")
        elif self.is_python_type():
            return self._internal_type is str
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
            return self._internal_type.kind == "V"  # void type (raw bytes)
        elif self.is_python_type():
            return self._internal_type in (bytes, bytearray)
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
            return np.issubdtype(self._internal_type, np.datetime64) or np.issubdtype(
                self._internal_type, np.timedelta64
            )
        elif self.is_python_type():
            import datetime

            return self._internal_type in (
                datetime.datetime,
                datetime.date,
                datetime.time,
                datetime.timedelta,
            )
        return False
