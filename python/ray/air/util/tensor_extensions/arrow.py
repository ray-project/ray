import abc
import functools
import itertools
import json
import logging
import sys
import threading
from abc import abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Collection, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
import pyarrow as pa
from packaging.version import parse as parse_version

import ray.cloudpickle as cloudpickle
from ray._private.arrow_utils import _check_pyarrow_version, get_pyarrow_version
from ray._private.ray_constants import env_integer
from ray.air.util.object_extensions.arrow import (
    MIN_PYARROW_VERSION_SCALAR_SUBCLASS,
    ArrowPythonObjectArray,
    _object_extension_type_allowed,
)
from ray.air.util.tensor_extensions.utils import (
    ArrayLike,
    _is_ndarray_variable_shaped_tensor,
    _should_convert_to_tensor,
    create_ragged_ndarray,
)
from ray.data._internal.numpy_support import (
    _convert_datetime_to_np_datetime,
    convert_to_numpy,
)
from ray.util import log_once
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.common import INT32_MAX

# First, assert Arrow version is w/in expected bounds
_check_pyarrow_version()


PYARROW_VERSION = get_pyarrow_version()


# Minimum version supporting `zero_copy_only` flag in `ChunkedArray.to_numpy`
MIN_PYARROW_VERSION_CHUNKED_ARRAY_TO_NUMPY_ZERO_COPY_ONLY = parse_version("13.0.0")
# Min version supporting ``ExtensionArray``s in ``pyarrow.concat``
MIN_PYARROW_VERSION_EXT_ARRAY_CONCAT_SUPPORTED = parse_version("12.0.0")


NUM_BYTES_PER_UNICODE_CHAR = 4


class _SerializationFormat(Enum):
    # JSON format is legacy and inefficient, only kept for backward compatibility
    JSON = 0
    CLOUDPICKLE = 1


# Set the default serialization format for Arrow extension types.
ARROW_EXTENSION_SERIALIZATION_FORMAT = _SerializationFormat(
    _SerializationFormat.JSON  # legacy
    if env_integer("RAY_DATA_ARROW_EXTENSION_SERIALIZATION_LEGACY_JSON_FORMAT", 0) == 1
    else _SerializationFormat.CLOUDPICKLE  # default
)

# 100,000 entries, about 10MB in memory.
# Most users tables should have less than 100K columns.
ARROW_EXTENSION_SERIALIZATION_CACHE_MAXSIZE = env_integer(
    "RAY_EXTENSION_SERIALIZATION_CACHE_MAXSIZE", 10**5
)

logger = logging.getLogger(__name__)


def _extension_array_concat_supported() -> bool:
    return get_pyarrow_version() >= MIN_PYARROW_VERSION_EXT_ARRAY_CONCAT_SUPPORTED


def _deserialize_with_fallback(serialized: bytes, field_name: str = "data"):
    """Deserialize data with cloudpickle first, fallback to JSON."""
    try:
        # Try cloudpickle first (new format)
        return cloudpickle.loads(serialized)
    except Exception:
        # Fallback to JSON format (legacy)
        try:
            return json.loads(serialized)
        except json.JSONDecodeError:
            raise ValueError(
                f"Unable to deserialize {field_name} from {type(serialized)}"
            )


@DeveloperAPI(stability="beta")
class ArrowExtensionSerializeDeserializeCache(abc.ABC):
    """Base class for caching Arrow extension type serialization and deserialization.

    The deserialization and serialization of Arrow extension types is frequent,
    so we cache the results here to improve performance.

    The deserialization cache uses functools.lru_cache as a classmethod. There is
    a single cache instance shared across all subclasses, but the cache key includes
    the class (cls parameter) as the first argument, so different subclasses get
    different cache entries even when called with the same parameters. The cache is
    thread-safe and has a maximum size limit to control memory usage. The cache key
    is (cls, *args) where args are the parameters returned by _get_deserialize_parameter().

    Attributes:
        _serialize_cache: Instance-level cache for serialization results.
            This is a simple cached value (bytes) that is computed once per
            instance and reused.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the extension type with caching support.

        Args:
            *args: Positional arguments passed to the parent class.
            **kwargs: Keyword arguments passed to the parent class.
        """
        # Instance-level cache for serialization results, no TTL
        self._serialize_cache = None
        self._cache_lock = threading.RLock()
        super().__init__(*args, **kwargs)

    def __arrow_ext_serialize__(self) -> bytes:
        """Serialize the extension type using caching if enabled."""
        if self._serialize_cache is not None:
            return self._serialize_cache
        with self._cache_lock:
            if self._serialize_cache is None:
                self._serialize_cache = self._arrow_ext_serialize_compute()
            return self._serialize_cache

    @abstractmethod
    def _arrow_ext_serialize_compute(self) -> bytes:
        """Subclasses must implement this method to compute serialization."""
        ...

    @classmethod
    @functools.lru_cache(maxsize=ARROW_EXTENSION_SERIALIZATION_CACHE_MAXSIZE)
    def _arrow_ext_deserialize_cache(cls: type, *args: Any, **kwargs: Any) -> Any:
        """Deserialize the extension type using the class-level cache.

        This method is cached using functools.lru_cache to improve performance
        when deserializing extension types. The cache key includes the class (cls)
        as the first argument, ensuring different subclasses get separate cache entries.

        Args:
            *args: Positional arguments passed to _arrow_ext_deserialize_compute.
            **kwargs: Keyword arguments passed to _arrow_ext_deserialize_compute.

        Returns:
            The deserialized extension type instance.
        """
        return cls._arrow_ext_deserialize_compute(*args, **kwargs)

    @classmethod
    @abstractmethod
    def _arrow_ext_deserialize_compute(cls, *args: Any, **kwargs: Any) -> Any:
        """Subclasses must implement this method to compute deserialization."""
        ...

    @classmethod
    @abstractmethod
    def _get_deserialize_parameter(cls, storage_type, serialized) -> Tuple:
        """Subclasses must implement this method to return the parameters for the deserialization cache."""
        ...

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized) -> Any:
        """Deserialize the extension type using caching if enabled."""
        return cls._arrow_ext_deserialize_cache(
            *cls._get_deserialize_parameter(storage_type, serialized)
        )


@DeveloperAPI
class ArrowConversionError(Exception):
    """Error raised when there is an issue converting data to Arrow."""

    MAX_DATA_STR_LEN = 200

    def __init__(self, data_str: str):
        if len(data_str) > self.MAX_DATA_STR_LEN:
            data_str = data_str[: self.MAX_DATA_STR_LEN] + "..."
        message = f"Error converting data to Arrow: {data_str}"
        super().__init__(message)


@DeveloperAPI
def pyarrow_table_from_pydict(
    pydict: Dict[str, Union[List[Any], pa.Array]],
) -> pa.Table:
    """
    Convert a Python dictionary to a pyarrow Table.

    Raises:
        ArrowConversionError: if the conversion fails.
    """
    try:
        return pa.Table.from_pydict(pydict)
    except Exception as e:
        raise ArrowConversionError(str(pydict)) from e


@DeveloperAPI(stability="alpha")
def convert_to_pyarrow_array(
    column_values: Union[List[Any], np.ndarray, ArrayLike], column_name: str
) -> pa.Array:
    """Converts provided NumPy `ndarray` into PyArrow's `array` while utilizing
    both Arrow's natively supported types as well as custom extension types:

        - ArrowTensorArray (for tensors)
        - ArrowPythonObjectArray (for user-defined python class objects, as well as
        any python object that aren't represented by a corresponding Arrow's native
        scalar type)
    """

    try:
        # Since Arrow does NOT support tensors (aka multidimensional arrays) natively,
        # we have to make sure that we handle this case utilizing `ArrowTensorArray`
        # extension type
        if len(column_values) > 0 and _should_convert_to_tensor(
            column_values, column_name
        ):
            from ray.data.extensions.tensor_extension import ArrowTensorArray

            # Convert to Numpy before creating instance of `ArrowTensorArray` to
            # align tensor shapes falling back to ragged ndarray only if necessary
            return ArrowTensorArray.from_numpy(
                convert_to_numpy(column_values), column_name
            )
        else:
            return _convert_to_pyarrow_native_array(column_values, column_name)

    except ArrowConversionError as ace:
        from ray.data.context import DataContext

        enable_fallback_config: Optional[
            bool
        ] = DataContext.get_current().enable_fallback_to_arrow_object_ext_type

        if not _object_extension_type_allowed():
            object_ext_type_fallback_allowed = False
            object_ext_type_detail = (
                "skipping fallback to serialize as pickled python"
                f" objects (due to unsupported Arrow version {PYARROW_VERSION}, "
                f"min required version is {MIN_PYARROW_VERSION_SCALAR_SUBCLASS})"
            )
        else:
            # NOTE: By default setting is unset which (for compatibility reasons)
            #       is allowing the fallback
            object_ext_type_fallback_allowed = (
                enable_fallback_config is None or enable_fallback_config
            )

            if object_ext_type_fallback_allowed:
                object_ext_type_detail = (
                    "falling back to serialize as pickled python objects"
                )
            else:
                object_ext_type_detail = (
                    "skipping fallback to serialize as pickled python objects "
                    "(due to DataContext.enable_fallback_to_arrow_object_ext_type "
                    "= False)"
                )

        # To avoid logging following warning for every block it's
        # only going to be logged in following cases
        #   - It's being logged for the first time, and
        #   - When config enabling fallback is not set explicitly (in this case
        #       fallback will still occur by default for compatibility reasons), or
        #   - Fallback is disallowed (either explicitly or due to use of incompatible
        #       Pyarrow version)
        if (
            enable_fallback_config is None or not object_ext_type_fallback_allowed
        ) and log_once("_fallback_to_arrow_object_extension_type_warning"):
            logger.warning(
                f"Failed to convert column '{column_name}' into pyarrow "
                f"array due to: {ace}; {object_ext_type_detail}",
                exc_info=ace,
            )

        if not object_ext_type_fallback_allowed:
            # If `ArrowPythonObjectType` is not supported raise original exception
            raise

        # Otherwise, attempt to fall back to serialize as python objects
        return ArrowPythonObjectArray.from_objects(column_values)


def _convert_to_pyarrow_native_array(
    column_values: Union[List[Any], np.ndarray], column_name: str
) -> pa.Array:
    """Converts provided NumPy `ndarray` into PyArrow's `array` while only utilizing
    Arrow's natively supported types (ie no custom extension types)"""

    try:
        # NOTE: Python's `datetime` only supports precision up to us and could
        #       inadvertently lose precision when handling Pandas `Timestamp` type.
        #       To avoid that we convert provided list of `datetime` objects into
        #       ndarray of `np.datetime64`
        if len(column_values) > 0 and isinstance(column_values[0], datetime):
            column_values = _convert_datetime_to_np_datetime(column_values)

        # To avoid deserialization penalty of converting Arrow arrays (`Array` and `ChunkedArray`)
        # to Python objects and then back to Arrow, we instead combine them into ListArray manually
        if len(column_values) > 0 and isinstance(
            column_values[0], (pa.Array, pa.ChunkedArray)
        ):
            return _combine_as_list_array(column_values)

        # NOTE: We explicitly infer PyArrow `DataType` so that
        #       we can perform upcasting to be able to accommodate
        #       blocks that are larger than 2Gb in size (limited
        #       by int32 offsets used by Arrow internally)
        pa_type = _infer_pyarrow_type(column_values)

        if pa_type and pa.types.is_timestamp(pa_type):
            # NOTE: Quirky Arrow behavior will coerce unsupported Numpy `datetime64`
            #       precisions that are nested inside a list type, but won't do it,
            #       if these are top-level ndarray. To work this around we have to cast
            #       ndarray values manually
            if isinstance(column_values, np.ndarray):
                column_values = _coerce_np_datetime_to_pa_timestamp_precision(
                    column_values, pa_type, column_name
                )

        logger.log(
            logging.getLevelName("TRACE"),
            f"Inferred dtype of '{pa_type}' for column '{column_name}'",
        )

        # NOTE: Pyarrow 19.0 is not able to properly handle `ListScalar(None)` when
        #       creating native array and hence we have to manually replace any such
        #       cases w/ an explicit null value
        #
        # See for more details https://github.com/apache/arrow/issues/45682
        if len(column_values) > 0 and isinstance(column_values[0], pa.ListScalar):
            column_values = [v if v.is_valid else None for v in column_values]

        return pa.array(column_values, type=pa_type)
    except Exception as e:
        raise ArrowConversionError(str(column_values)) from e


def _combine_as_list_array(column_values: List[Union[pa.Array, pa.ChunkedArray]]):
    """Combines list of Arrow arrays into a single `ListArray`"""

    # First, compute respective offsets in the resulting array
    lens = [len(v) for v in column_values]
    offsets = pa.array(np.concatenate([[0], np.cumsum(lens)]), type=pa.int32())

    # Concat all the chunks into a single contiguous array
    combined = pa.concat_arrays(
        itertools.chain(
            *[
                v.chunks if isinstance(v, pa.ChunkedArray) else [v]
                for v in column_values
            ]
        )
    )

    # TODO support null masking
    return pa.ListArray.from_arrays(offsets, combined, pa.list_(combined.type))


def _coerce_np_datetime_to_pa_timestamp_precision(
    column_values: np.ndarray, dtype: pa.TimestampType, column_name: str
):
    assert np.issubdtype(column_values.dtype, np.datetime64)

    numpy_precision, _ = np.datetime_data(column_values.dtype)
    arrow_precision = dtype.unit

    if arrow_precision != numpy_precision:
        # Arrow supports fewer timestamp resolutions than NumPy. So, if Arrow
        # doesn't support the resolution, we need to cast the NumPy array to a
        # different type. This can be a lossy conversion.
        column_values = column_values.astype(f"datetime64[{arrow_precision}]")

        if log_once(f"column_{column_name}_timestamp_warning"):
            logger.warning(
                f"Converting a {numpy_precision!r} precision datetime NumPy "
                f"array to '{arrow_precision}' precision Arrow timestamp. This "
                "conversion occurs because Arrow supports fewer precisions "
                "than Arrow and might result in a loss of precision or "
                "unrepresentable values."
            )

    return column_values


def _infer_pyarrow_type(
    column_values: Union[List[Any], np.ndarray]
) -> Optional[pa.DataType]:
    """Infers target Pyarrow `DataType` based on the provided
    columnar values.

    NOTE: This is a wrapper on top of `pa.infer_type(...)` utility
          performing up-casting of `binary` and `string` types to
          corresponding `large_binary` and `large_string` types in case
          any of the array elements exceeds 2Gb in size therefore
          making it impossible for original types to accommodate such
          values.

          Unfortunately, for unknown reasons PA doesn't perform
          that upcasting itself henceforth we have to do perform
          it manually

    Args:
        column_values: List of columnar values

    Returns:
        Instance of PyArrow's `DataType` based on the provided
        column values
    """

    if len(column_values) == 0:
        return None

    # `pyarrow.infer_type` leaks memory if you pass an array with a datetime64 dtype.
    # To avoid this, we handle datetime64 dtypes separately.
    # See https://github.com/apache/arrow/issues/45493.
    dtype_with_timestamp_type = _try_infer_pa_timestamp_type(column_values)

    if dtype_with_timestamp_type is not None:
        return dtype_with_timestamp_type

    inferred_pa_dtype = pa.infer_type(column_values)

    def _len_gt_overflow_threshold(obj: Any) -> bool:
        # NOTE: This utility could be seeing objects other than strings or bytes in
        #       cases when column contains non-scalar non-homogeneous object types as
        #       column values, therefore making Arrow unable to infer corresponding
        #       column type appropriately, therefore falling back to assume the type
        #       of the first element in the list.
        #
        #       Check out test cases for this method for an additional context.
        if isinstance(obj, (str, bytes)):
            return len(obj) > INT32_MAX

        return False

    if pa.types.is_binary(inferred_pa_dtype) and any(
        [_len_gt_overflow_threshold(v) for v in column_values]
    ):
        return pa.large_binary()
    elif pa.types.is_string(inferred_pa_dtype) and any(
        [_len_gt_overflow_threshold(v) for v in column_values]
    ):
        return pa.large_string()

    return inferred_pa_dtype


_NUMPY_TO_ARROW_PRECISION_MAP = {
    # Coarsest timestamp precision in Arrow is seconds
    "Y": "s",
    "D": "s",
    "M": "s",
    "W": "s",
    "h": "s",
    "m": "s",
    "s": "s",
    "ms": "ms",
    "us": "us",
    "ns": "ns",
    # Finest timestamp precision in Arrow is nanoseconds
    "ps": "ns",
    "fs": "ns",
    "as": "ns",
}


def _try_infer_pa_timestamp_type(
    column_values: Union[List[Any], np.ndarray]
) -> Optional[pa.DataType]:
    if isinstance(column_values, list) and len(column_values) > 0:
        # In case provided column values is a list of elements, this
        # utility assumes homogeneity (in line with the behavior of Arrow
        # type inference utils)
        element_type = _try_infer_pa_timestamp_type(column_values[0])
        return pa.list_(element_type) if element_type else None

    if isinstance(column_values, np.ndarray) and np.issubdtype(
        column_values.dtype, np.datetime64
    ):
        np_precision, _ = np.datetime_data(column_values.dtype)
        return pa.timestamp(_NUMPY_TO_ARROW_PRECISION_MAP[np_precision])

    else:
        return None


@DeveloperAPI
def get_arrow_extension_tensor_types():
    """Returns list of extension types of Arrow Array holding
    multidimensional tensors
    """
    return (
        *get_arrow_extension_fixed_shape_tensor_types(),
        *get_arrow_extension_variable_shape_tensor_types(),
    )


@DeveloperAPI
def get_arrow_extension_fixed_shape_tensor_types():
    """Returns list of Arrow extension types holding multidimensional
    tensors of *fixed* shape
    """
    return ArrowTensorType, ArrowTensorTypeV2


@DeveloperAPI
def get_arrow_extension_variable_shape_tensor_types():
    """Returns list of Arrow extension types holding multidimensional
    tensors of *fixed* shape
    """
    return (ArrowVariableShapedTensorType,)


# ArrowExtensionSerializeDeserializeCache needs to be first in the MRO to ensure the cache is used
class _BaseFixedShapeArrowTensorType(
    ArrowExtensionSerializeDeserializeCache, pa.ExtensionType
):
    """
    Arrow ExtensionType for an array of fixed-shaped, homogeneous-typed
    tensors.

    This is the Arrow side of TensorDtype.

    See Arrow extension type docs:
    https://arrow.apache.org/docs/python/extending_types.html#defining-extension-types-user-defined-types
    """

    def __init__(
        self, shape: Tuple[int, ...], tensor_dtype: pa.DataType, ext_type_id: str
    ):
        self._shape = shape
        super().__init__(tensor_dtype, ext_type_id)

    @property
    def shape(self) -> Tuple[int, ...]:
        """
        Shape of contained tensors.
        """
        return self._shape

    @property
    def scalar_type(self) -> pa.DataType:
        """Returns the type of the underlying tensor elements."""
        return self.storage_type.value_type

    def to_pandas_dtype(self):
        """
        Convert Arrow extension type to corresponding Pandas dtype.

        Returns:
            An instance of pd.api.extensions.ExtensionDtype.
        """
        from ray.air.util.tensor_extensions.pandas import TensorDtype

        return TensorDtype(self._shape, self.scalar_type.to_pandas_dtype())

    def __reduce__(self):
        return self.__arrow_ext_deserialize__, (
            self.storage_type,
            self.__arrow_ext_serialize__(),
        )

    def _arrow_ext_serialize_compute(self):
        if ARROW_EXTENSION_SERIALIZATION_FORMAT == _SerializationFormat.CLOUDPICKLE:
            return cloudpickle.dumps(self._shape)
        elif ARROW_EXTENSION_SERIALIZATION_FORMAT == _SerializationFormat.JSON:
            return json.dumps(self._shape).encode()
        else:
            raise ValueError(
                f"Invalid serialization format: {ARROW_EXTENSION_SERIALIZATION_FORMAT}"
            )

    def __arrow_ext_class__(self):
        """
        ExtensionArray subclass with custom logic for this array of tensors
        type.

        Returns:
            A subclass of pd.api.extensions.ExtensionArray.
        """
        return ArrowTensorArray

    def __arrow_ext_scalar_class__(self):
        """
        ExtensionScalar subclass with custom logic for this array of tensors type.
        """
        return ArrowTensorScalar

    def _extension_scalar_to_ndarray(self, scalar: "pa.ExtensionScalar") -> np.ndarray:
        """
        Convert an ExtensionScalar to a tensor element.
        """
        # Handle None/null values
        if scalar.value is None:
            return None

        raw_values = scalar.value.values
        shape = scalar.type.shape
        value_type = raw_values.type
        offset = raw_values.offset
        data_buffer = raw_values.buffers()[1]
        return _to_ndarray_helper(shape, value_type, offset, data_buffer)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(shape={self.shape}, dtype={self.storage_type.value_type})"

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other):
        return (
            isinstance(other, type(self))
            and other.extension_name == self.extension_name
            and other.shape == self.shape
            and other.scalar_type == self.scalar_type
        )

    def __ne__(self, other):
        # NOTE: We override ``__ne__`` to override base class' method
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((self.extension_name, self.scalar_type, self._shape))


@PublicAPI(stability="beta")
class ArrowTensorType(_BaseFixedShapeArrowTensorType):
    """Arrow ExtensionType (v1) for tensors.

    NOTE: This type does *NOT* support tensors larger than 4Gb (due to
          overflow of int32 offsets utilized inside Pyarrow `ListType`)
    """

    OFFSET_DTYPE = pa.int32()

    def __init__(self, shape: Tuple[int, ...], dtype: pa.DataType):
        """
        Construct the Arrow extension type for array of fixed-shaped tensors.

        Args:
            shape: Shape of contained tensors.
            dtype: pyarrow dtype of tensor elements.
        """

        super().__init__(shape, pa.list_(dtype), "ray.data.arrow_tensor")

    @classmethod
    def _get_deserialize_parameter(cls, storage_type, serialized):
        return (serialized, storage_type.value_type)

    @classmethod
    def _arrow_ext_deserialize_compute(cls, serialized, value_type):
        shape = tuple(_deserialize_with_fallback(serialized, "shape"))
        return cls(shape, value_type)


@PublicAPI(stability="alpha")
class ArrowTensorTypeV2(_BaseFixedShapeArrowTensorType):
    """Arrow ExtensionType (v2) for tensors (supporting tensors > 4Gb)."""

    OFFSET_DTYPE = pa.int64()

    def __init__(self, shape: Tuple[int, ...], dtype: pa.DataType):
        """
        Construct the Arrow extension type for array of fixed-shaped tensors.

        Args:
            shape: Shape of contained tensors.
            dtype: pyarrow dtype of tensor elements.
        """

        super().__init__(shape, pa.large_list(dtype), "ray.data.arrow_tensor_v2")

    @classmethod
    def _get_deserialize_parameter(cls, storage_type, serialized):
        return (serialized, storage_type.value_type)

    @classmethod
    def _arrow_ext_deserialize_compute(cls, serialized, value_type):
        shape = tuple(_deserialize_with_fallback(serialized, "shape"))
        return cls(shape, value_type)


@PublicAPI(stability="beta")
class ArrowTensorScalar(pa.ExtensionScalar):
    def as_py(self, **kwargs) -> np.ndarray:
        return self.__array__()

    def __array__(self) -> np.ndarray:
        return self.type._extension_scalar_to_ndarray(self)


@PublicAPI(stability="beta")
class ArrowTensorArray(pa.ExtensionArray):
    """
    An array of fixed-shape, homogeneous-typed tensors.

    This is the Arrow side of TensorArray.

    See Arrow docs for customizing extension arrays:
    https://arrow.apache.org/docs/python/extending_types.html#custom-extension-array-class
    """

    @classmethod
    def from_numpy(
        cls,
        arr: Union[np.ndarray, Iterable[np.ndarray]],
        column_name: Optional[str] = None,
    ) -> Union["ArrowTensorArray", "ArrowVariableShapedTensorArray"]:
        """
        Convert an ndarray or an iterable of ndarrays to an array of homogeneous-typed
        tensors. If given fixed-shape tensor elements, this will return an
        ``ArrowTensorArray``; if given variable-shape tensor elements, this will return
        an ``ArrowVariableShapedTensorArray``.

        Args:
            arr: An ndarray or an iterable of ndarrays.
            column_name: Optional. Used only in logging outputs to provide
                additional details.

        Returns:
            - If fixed-shape tensor elements, an ``ArrowTensorArray`` containing
              ``len(arr)`` tensors of fixed shape.
            - If variable-shaped tensor elements, an ``ArrowVariableShapedTensorArray``
              containing ``len(arr)`` tensors of variable shape.
            - If scalar elements, a ``pyarrow.Array``.
        """
        if not isinstance(arr, np.ndarray) and isinstance(arr, Iterable):
            arr = list(arr)

        if isinstance(arr, (list, tuple)) and arr and isinstance(arr[0], np.ndarray):
            # Stack ndarrays and pass through to ndarray handling logic below.
            try:
                arr = np.stack(arr, axis=0)
            except ValueError as ve:
                logger.warning(
                    f"Failed to stack lists due to: {ve}; "
                    f"falling back to using np.array(..., dtype=object)",
                    exc_info=ve,
                )

                # ndarray stacking may fail if the arrays are heterogeneously-shaped.
                arr = np.array(arr, dtype=object)

        if not isinstance(arr, np.ndarray):
            raise ValueError(
                f"Must give ndarray or iterable of ndarrays, got {type(arr)} {arr}"
            )

        try:
            timestamp_dtype = _try_infer_pa_timestamp_type(arr)

            if timestamp_dtype:
                # NOTE: Quirky Arrow behavior will coerce unsupported Numpy `datetime64`
                #       precisions that are nested inside a list type, but won't do it,
                #       if these are top-level ndarray. To work this around we have to cast
                #       ndarray values manually
                arr = _coerce_np_datetime_to_pa_timestamp_precision(
                    arr, timestamp_dtype, column_name
                )

            return cls._from_numpy(arr)
        except Exception as e:
            data_str = ""
            if column_name:
                data_str += f"column: '{column_name}', "
            data_str += f"shape: {arr.shape}, dtype: {arr.dtype}, data: {arr}"
            raise ArrowConversionError(data_str) from e

    @classmethod
    def _from_numpy(
        cls,
        arr: np.ndarray,
    ) -> Union["ArrowTensorArray", "ArrowVariableShapedTensorArray"]:
        if len(arr) > 0 and np.isscalar(arr[0]):
            # Elements are scalar so a plain Arrow Array will suffice.
            return pa.array(arr)

        if _is_ndarray_variable_shaped_tensor(arr):
            # Tensor elements have variable shape, so we delegate to
            # ArrowVariableShapedTensorArray.
            return ArrowVariableShapedTensorArray.from_numpy(arr)

        if not arr.flags.c_contiguous:
            # We only natively support C-contiguous ndarrays.
            arr = np.ascontiguousarray(arr)

        scalar_dtype = pa.from_numpy_dtype(arr.dtype)

        if pa.types.is_string(scalar_dtype):
            if arr.dtype.byteorder == ">" or (
                arr.dtype.byteorder == "=" and sys.byteorder == "big"
            ):
                raise ValueError(
                    "Only little-endian string tensors are supported, "
                    f"but got: {arr.dtype}",
                )
            scalar_dtype = pa.binary(arr.dtype.itemsize)

        outer_len = arr.shape[0]
        element_shape = arr.shape[1:]
        total_num_items = arr.size
        num_items_per_element = np.prod(element_shape) if element_shape else 1

        # Shape up data buffer
        if pa.types.is_boolean(scalar_dtype):
            # NumPy doesn't represent boolean arrays as bit-packed, so we manually
            # bit-pack the booleans before handing the buffer off to Arrow.
            # NOTE: Arrow expects LSB bit-packed ordering.
            # NOTE: This creates a copy.
            arr = np.packbits(arr, bitorder="little")

        data_buffer = pa.py_buffer(arr)
        data_array = pa.Array.from_buffers(
            scalar_dtype, total_num_items, [None, data_buffer]
        )

        from ray.data import DataContext

        if DataContext.get_current().use_arrow_tensor_v2:
            pa_type_ = ArrowTensorTypeV2(element_shape, scalar_dtype)
        else:
            pa_type_ = ArrowTensorType(element_shape, scalar_dtype)

        offset_dtype = pa_type_.OFFSET_DTYPE.to_pandas_dtype()

        # Create offsets buffer
        if num_items_per_element == 0:
            offsets = np.zeros(outer_len + 1, dtype=offset_dtype)
        else:
            offsets = np.arange(
                0,
                (outer_len + 1) * num_items_per_element,
                num_items_per_element,
                dtype=offset_dtype,
            )
        offset_buffer = pa.py_buffer(offsets)

        storage = pa.Array.from_buffers(
            pa_type_.storage_type,
            outer_len,
            [None, offset_buffer],
            children=[data_array],
        )

        return pa_type_.wrap_array(storage)

    def to_numpy(self, zero_copy_only: bool = True):
        """
        Convert the entire array of tensors into a single ndarray.

        Args:
            zero_copy_only: If True, an exception will be raised if the
                conversion to a NumPy array would require copying the
                underlying data (e.g. in presence of nulls, or for
                non-primitive types). This argument is currently ignored, so
                zero-copy isn't enforced even if this argument is true.

        Returns:
            A single ndarray representing the entire array of tensors.
        """

        # Buffers layout: [None, offset_buffer, None, data_buffer]
        buffers = self.buffers()
        data_buffer = buffers[3]
        storage_list_type = self.storage.type
        value_type = storage_list_type.value_type
        shape = self.type.shape

        # Batch type checks
        is_boolean = pa.types.is_boolean(value_type)

        # Calculate buffer item width once
        if is_boolean:
            # Arrow boolean array buffers are bit-packed, with 8 entries per byte,
            # and are accessed via bit offsets.
            buffer_item_width = value_type.bit_width
        else:
            # We assume all other array types are accessed via byte array
            # offsets.
            buffer_item_width = value_type.bit_width // 8

        # Number of items per inner ndarray.
        num_items_per_element = np.prod(shape) if shape else 1
        # Base offset into data buffer, e.g. due to zero-copy slice.
        buffer_offset = self.offset * num_items_per_element
        # Offset of array data in buffer.
        offset = buffer_item_width * buffer_offset
        # Update the shape for ndarray
        shape = (len(self),) + shape

        if is_boolean:
            # Special handling for boolean arrays, since Arrow bit-packs boolean arrays
            # while NumPy does not.
            # Cast as uint8 array and let NumPy unpack into a boolean view.
            # Offset into uint8 array, where each element is a bucket for 8 booleans.
            byte_bucket_offset = offset // 8
            # Offset for a specific boolean, within a uint8 array element.
            bool_offset = offset % 8
            # The number of uint8 array elements (buckets) that our slice spans.
            # Note that, due to the offset for a specific boolean, the slice can span
            # byte boundaries even if it contains less than 8 booleans.
            num_boolean_byte_buckets = 1 + ((bool_offset + np.prod(shape) - 1) // 8)
            # Construct the uint8 array view on the buffer.
            arr = np.ndarray(
                (num_boolean_byte_buckets,),
                dtype=np.uint8,
                buffer=data_buffer,
                offset=byte_bucket_offset,
            )
            # Unpack into a byte per boolean, using LSB bit-packed ordering.
            arr = np.unpackbits(arr, bitorder="little")
            # Interpret buffer as boolean array.
            return np.ndarray(shape, dtype=np.bool_, buffer=arr, offset=bool_offset)

        # Special handling of binary/string types. Assumes unicode string tensor columns
        if pa.types.is_fixed_size_binary(value_type):
            ext_dtype = np.dtype(
                f"<U{value_type.byte_width // NUM_BYTES_PER_UNICODE_CHAR}"
            )
        else:
            ext_dtype = value_type.to_pandas_dtype()

        return np.ndarray(shape, dtype=ext_dtype, buffer=data_buffer, offset=offset)

    def to_var_shaped_tensor_array(
        self,
        ndim: int,
    ) -> "ArrowVariableShapedTensorArray":
        """
        Convert this tensor array to a variable-shaped tensor array.
        """

        shape = self.type.shape
        if ndim < len(shape):
            raise ValueError(
                f"Can't convert {self.type} to var-shaped tensor type with {ndim=}"
            )

        # NOTE: For ``ArrowTensorTypeV2`` we can construct variable-shaped
        #       tensor directly w/o modifying its internal storage.
        #
        #       For (deprecated) ``ArrowTensorType`` we fallback to converting to Numpy,
        #       and reconstructing.
        if not isinstance(self.type, ArrowTensorTypeV2):
            return ArrowVariableShapedTensorArray.from_numpy(self.to_numpy())

        # Pad target shape with singleton axis to match target number of
        # dimensions
        # TODO avoid padding
        target_shape = _pad_shape_with_singleton_axes(shape, ndim)
        # Construct shapes array
        shape_array = pa.nulls(
            len(self.storage),
            type=ArrowVariableShapedTensorArray.SHAPES_ARRAY_TYPE,
        ).fill_null(target_shape)

        storage = pa.StructArray.from_arrays(
            [self.storage, shape_array],
            ["data", "shape"],
        )

        target_type = ArrowVariableShapedTensorType(
            self.type.scalar_type,
            ndim=ndim,
        )

        return target_type.wrap_array(storage)


# ArrowExtensionSerializeDeserializeCache needs to be first in the MRO to ensure the cache is used
@PublicAPI(stability="alpha")
class ArrowVariableShapedTensorType(
    ArrowExtensionSerializeDeserializeCache, pa.ExtensionType
):
    """
    Arrow ExtensionType for an array of heterogeneous-shaped, homogeneous-typed
    tensors.

    This is the Arrow side of ``TensorDtype`` for tensor elements with different shapes.

    NOTE: This extension only supports tensor elements with non-ragged, well-defined
    shapes; i.e. every tensor element must have a well-defined shape and all of their
    shapes have to have same number of dimensions (ie ``len(shape)`` has to be the
    same for all of them).

    See Arrow extension type docs:
    https://arrow.apache.org/docs/python/extending_types.html#defining-extension-types-user-defined-types
    """

    OFFSET_DTYPE = pa.int64()

    def __init__(self, dtype: pa.DataType, ndim: int):
        """
        Construct the Arrow extension type for array of heterogeneous-shaped tensors.

        Args:
            dtype: pyarrow dtype of tensor elements.
            ndim: The number of dimensions in the tensor elements.
        """
        self._ndim = ndim
        super().__init__(
            pa.struct(
                [("data", pa.large_list(dtype)), ("shape", pa.list_(self.OFFSET_DTYPE))]
            ),
            "ray.data.arrow_variable_shaped_tensor",
        )

    def to_pandas_dtype(self):
        """
        Convert Arrow extension type to corresponding Pandas dtype.

        Returns:
            An instance of pd.api.extensions.ExtensionDtype.
        """
        from ray.air.util.tensor_extensions.pandas import TensorDtype

        return TensorDtype(
            self.shape,
            self.storage_type["data"].type.value_type.to_pandas_dtype(),
        )

    @property
    def ndim(self) -> int:
        """Return the number of dimensions in the tensor elements."""
        return self._ndim

    @property
    def shape(self) -> Tuple[None, ...]:
        return (None,) * self.ndim

    @property
    def scalar_type(self) -> pa.DataType:
        """Returns the type of the underlying tensor elements."""
        data_field_index = self.storage_type.get_field_index("data")
        return self.storage_type[data_field_index].type.value_type

    def __reduce__(self):
        return self.__arrow_ext_deserialize__, (
            self.storage_type,
            self.__arrow_ext_serialize__(),
        )

    def _arrow_ext_serialize_compute(self):
        if ARROW_EXTENSION_SERIALIZATION_FORMAT == _SerializationFormat.CLOUDPICKLE:
            return cloudpickle.dumps(self._ndim)
        elif ARROW_EXTENSION_SERIALIZATION_FORMAT == _SerializationFormat.JSON:
            return json.dumps(self._ndim).encode()
        else:
            raise ValueError(
                f"Invalid serialization format: {ARROW_EXTENSION_SERIALIZATION_FORMAT}"
            )

    @classmethod
    def _get_deserialize_parameter(cls, storage_type, serialized):
        return (serialized, storage_type["data"].type.value_type)

    @classmethod
    def _arrow_ext_deserialize_compute(cls, serialized, value_type):
        ndim = _deserialize_with_fallback(serialized, "ndim")
        return cls(value_type, ndim)

    def __arrow_ext_class__(self):
        """
        ExtensionArray subclass with custom logic for this array of tensors
        type.

        Returns:
            A subclass of pd.api.extensions.ExtensionArray.
        """
        return ArrowVariableShapedTensorArray

    def __arrow_ext_scalar_class__(self):
        """
        ExtensionScalar subclass with custom logic for this array of tensors type.
        """
        return ArrowTensorScalar

    def __str__(self) -> str:
        dtype = self.storage_type["data"].type.value_type
        return f"ArrowVariableShapedTensorType(ndim={self.ndim}, dtype={dtype})"

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other):
        # NOTE: This check is deliberately not comparing the ``ndim`` since
        #       we allow tensor types w/ varying ``ndim``s to be combined
        return (
            isinstance(other, ArrowVariableShapedTensorType)
            and other.extension_name == self.extension_name
            and other.scalar_type == self.scalar_type
        )

    def __ne__(self, other):
        # NOTE: We override ``__ne__`` to override base class' method
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((self.extension_name, self.scalar_type))

    def _extension_scalar_to_ndarray(self, scalar: "pa.ExtensionScalar") -> np.ndarray:
        """
        Convert an ExtensionScalar to a tensor element.
        """

        # Handle None/null values
        if scalar.value is None:
            return None

        data = scalar.value.get("data")
        raw_values = data.values
        value_type = raw_values.type
        offset = raw_values.offset
        data_buffer = raw_values.buffers()[1]

        shape = tuple(scalar.value.get("shape").as_py())

        return _to_ndarray_helper(shape, value_type, offset, data_buffer)


@PublicAPI(stability="alpha")
class ArrowVariableShapedTensorArray(pa.ExtensionArray):
    """
    An array of heterogeneous-shaped, homogeneous-typed tensors.

    This is the Arrow side of TensorArray for tensor elements that have differing
    shapes. Note that this extension only supports non-ragged tensor elements; i.e.,
    when considering each tensor element in isolation, they must have a well-defined
    shape. This extension also only supports tensor elements that all have the same
    number of dimensions.

    See Arrow docs for customizing extension arrays:
    https://arrow.apache.org/docs/python/extending_types.html#custom-extension-array-class
    """

    SHAPES_ARRAY_TYPE = pa.list_(pa.int64())

    @classmethod
    def from_numpy(
        cls, arr: Union[np.ndarray, List[np.ndarray], Tuple[np.ndarray]]
    ) -> "ArrowVariableShapedTensorArray":
        """
        Convert an ndarray or an iterable of heterogeneous-shaped ndarrays to an array
        of heterogeneous-shaped, homogeneous-typed tensors.

        Args:
            arr: An ndarray or an iterable of heterogeneous-shaped ndarrays.

        Returns:
            An ArrowVariableShapedTensorArray containing len(arr) tensors of
            heterogeneous shape.
        """
        # Implementation note - Arrow representation of ragged tensors:
        #
        # We represent an array of ragged tensors using a struct array containing two
        # fields:
        #  - data: a variable-sized list array, where each element in the array is a
        #    tensor element stored in a 1D (raveled) variable-sized list of the
        #    underlying scalar data type.
        #  - shape: a variable-sized list array containing the shapes of each tensor
        #    element.
        if not isinstance(arr, (list, tuple, np.ndarray)):
            raise ValueError(
                "ArrowVariableShapedTensorArray can only be constructed from an "
                f"ndarray or a list/tuple of ndarrays, but got: {type(arr)}"
            )

        if len(arr) == 0:
            # Empty ragged tensor arrays are not supported.
            raise ValueError("Creating empty ragged tensor arrays is not supported.")

        # Pre-allocate lists for better performance
        raveled = np.empty(len(arr), dtype=np.object_)
        shapes = np.empty(len(arr), dtype=np.object_)

        sizes = np.arange(len(arr), dtype=np.int64)

        ndim = None

        for i, a in enumerate(arr):
            a = np.asarray(a)

            if ndim is not None and a.ndim != ndim:
                raise ValueError(
                    "ArrowVariableShapedTensorArray only supports tensor elements that "
                    "all have the same number of dimensions, but got tensor elements "
                    f"with dimensions: {ndim}, {a.ndim}"
                )

            ndim = a.ndim
            shapes[i] = a.shape
            sizes[i] = a.size
            # Convert to 1D array view; this should be zero-copy in the common case.
            # NOTE: If array is not in C-contiguous order, this will convert it to
            # C-contiguous order, incurring a copy.
            raveled[i] = np.ravel(a, order="C")

        # Get size offsets and total size.
        size_offsets = np.cumsum(sizes)
        total_size = size_offsets[-1]

        # An optimized zero-copy path if raveled tensor elements are already
        # contiguous in memory, e.g. if this tensor array has already done a
        # roundtrip through our Arrow representation.
        data_buffer = _concat_ndarrays(raveled)

        dtype = data_buffer.dtype
        pa_scalar_type = pa.from_numpy_dtype(dtype)

        if pa.types.is_string(pa_scalar_type):
            if dtype.byteorder == ">" or (
                dtype.byteorder == "=" and sys.byteorder == "big"
            ):
                raise ValueError(
                    "Only little-endian string tensors are supported, "
                    f"but got: {dtype}"
                )
            pa_scalar_type = pa.binary(dtype.itemsize)

        if dtype.type is np.bool_ and data_buffer.size > 0:
            # NumPy doesn't represent boolean arrays as bit-packed, so we manually
            # bit-pack the booleans before handing the buffer off to Arrow.
            # NOTE: Arrow expects LSB bit-packed ordering.
            # NOTE: This creates a copy.
            data_buffer = np.packbits(data_buffer, bitorder="little")

        # Use foreign_buffer for better performance when possible
        data_buffer = pa.py_buffer(data_buffer)
        # Construct underlying data array.
        data_array = pa.Array.from_buffers(
            pa_scalar_type, total_size, [None, data_buffer]
        )

        # Construct array for offsets into the 1D data array, where each offset
        # corresponds to a tensor element.
        size_offsets = np.insert(size_offsets, 0, 0)
        offset_array = pa.array(size_offsets)
        data_storage_array = pa.LargeListArray.from_arrays(offset_array, data_array)
        # We store the tensor element shapes so we can reconstruct each tensor when
        # converting back to NumPy ndarrays.
        shape_array = pa.array(shapes)

        # Build storage array containing tensor data and the tensor element shapes.
        storage = pa.StructArray.from_arrays(
            [data_storage_array, shape_array],
            ["data", "shape"],
        )

        type_ = ArrowVariableShapedTensorType(pa_scalar_type, ndim)
        return type_.wrap_array(storage)

    def to_numpy(self, zero_copy_only: bool = True):
        """
        Convert the entire array of tensors into a single ndarray.

        Args:
            zero_copy_only: If True, an exception will be raised if the conversion to a
                NumPy array would require copying the underlying data (e.g. in presence
                of nulls, or for non-primitive types). This argument is currently
                ignored, so zero-copy isn't enforced even if this argument is true.

        Returns:
            A single ndarray representing the entire array of tensors.
        """

        data_array = self.storage.field("data")
        shapes_array = self.storage.field("shape")

        data_value_type = data_array.type.value_type
        data_array_buffer = data_array.buffers()[3]

        shapes = shapes_array.to_pylist()
        offsets = data_array.offsets.to_pylist()

        return create_ragged_ndarray(
            [
                _to_ndarray_helper(shape, data_value_type, offset, data_array_buffer)
                for shape, offset in zip(shapes, offsets)
            ]
        )

    def to_var_shaped_tensor_array(self, ndim: int) -> "ArrowVariableShapedTensorArray":
        if ndim == self.type.ndim:
            return self
        elif ndim < self.type.ndim:
            raise ValueError(
                f"Can't convert {self.type} to var-shaped tensor type with {ndim=}"
            )

        target_type = ArrowVariableShapedTensorType(self.type.scalar_type, ndim)

        # Unpack source tensor array into internal data storage and shapes
        # array
        data_array = self.storage.field("data")
        shapes_array = self.storage.field("shape")
        # Pad individual shapes with singleton axes to match target number of
        # dimensions
        #
        # TODO avoid python loop
        expanded_shapes_array = pa.array(
            [_pad_shape_with_singleton_axes(s, ndim) for s in shapes_array.to_pylist()]
        )

        storage = pa.StructArray.from_arrays([data_array, expanded_shapes_array])

        return target_type.wrap_array(storage)


def _pad_shape_with_singleton_axes(
    shape: Tuple[int, ...], ndim: int
) -> Tuple[int, ...]:
    assert ndim >= len(shape)

    return (1,) * (ndim - len(shape)) + shape


AnyArrowExtTensorType = Union[
    ArrowTensorType, ArrowTensorTypeV2, ArrowVariableShapedTensorType
]


@DeveloperAPI(stability="alpha")
def unify_tensor_types(
    types: Collection[AnyArrowExtTensorType],
) -> AnyArrowExtTensorType:
    """Unifies provided tensor types if compatible.

    Otherwise raises a ``ValueError``.
    """

    assert types, "List of tensor types may not be empty"

    if len(types) == 1:
        return types[0]

    shapes = {t.shape for t in types}
    scalar_types = {t.scalar_type for t in types}

    # Only tensors with homogenous scalar types and shape dimensions
    # are currently supported
    if len(scalar_types) > 1:
        raise pa.lib.ArrowTypeError(
            f"Can't unify tensor types with divergent scalar types: {types}"
        )

    # If all shapes are identical, it's a single tensor type
    if len(shapes) == 1:
        return next(iter(types))

    return ArrowVariableShapedTensorType(
        dtype=scalar_types.pop(),
        # NOTE: Cardinality of variable-shaped tensor type's (``ndims``) is
        #       derived as the max length of the shapes that are making it up
        ndim=max(len(s) for s in shapes),
    )


@DeveloperAPI(stability="alpha")
def unify_tensor_arrays(
    arrs: List[Union[ArrowTensorArray, ArrowVariableShapedTensorArray]]
) -> List[Union[ArrowTensorArray, ArrowVariableShapedTensorArray]]:
    supported_tensor_types = get_arrow_extension_tensor_types()

    # Derive number of distinct tensor types
    distinct_types_ = set()

    for arr in arrs:
        if isinstance(arr.type, supported_tensor_types):
            distinct_types_.add(arr.type)
        else:
            raise ValueError(
                f"Trying to unify unsupported tensor type: {arr.type} (supported types: {supported_tensor_types})"
            )

    if len(distinct_types_) == 1:
        return arrs

    # Verify provided tensor arrays could be unified
    #
    # NOTE: If there's more than 1 distinct tensor types, then unified
    #       type will be variable-shaped
    unified_tensor_type = unify_tensor_types(distinct_types_)

    assert isinstance(unified_tensor_type, ArrowVariableShapedTensorType)

    unified_arrs = []
    for arr in arrs:
        unified_arrs.append(
            arr.to_var_shaped_tensor_array(ndim=unified_tensor_type.ndim)
        )

    return unified_arrs


@DeveloperAPI(stability="alpha")
def concat_tensor_arrays(
    arrays: List[Union["ArrowTensorArray", "ArrowVariableShapedTensorArray"]],
    ensure_copy: bool = False,
) -> Union["ArrowTensorArray", "ArrowVariableShapedTensorArray"]:
    """
    Concatenates multiple tensor arrays.

    NOTE: If one or more of the tensor arrays are variable-shaped and/or any
    of the tensor arrays have a different shape than the others, a variable-shaped
    tensor array will be returned.

    Args:
        arrays: Tensor arrays to concat
        ensure_copy: Skip copying when ensure_copy is False and there is exactly 1 chunk.

    Returns:
        Either ``ArrowTensorArray`` or ``ArrowVariableShapedTensorArray`` holding
        all of the given tensor arrays concatenated.
    """

    assert arrays, "List of tensor arrays may not be empty"

    if len(arrays) == 1 and not ensure_copy:
        # Short-circuit
        return arrays[0]

    # First, unify provided tensor arrays
    unified_arrays = unify_tensor_arrays(arrays)
    # Then, simply concat underlying internal storage
    storage = pa.concat_arrays([c.storage for c in unified_arrays])

    unified_array_type = unified_arrays[0].type
    return unified_array_type.wrap_array(storage)


def _concat_ndarrays(arrs: Union[np.ndarray, List[np.ndarray]]) -> np.ndarray:
    """Concatenates provided collection of ``np.ndarray``s in either of the following
    ways:

        - If provided ndarrays are contiguous, 1D views sharing the same dtype,
        living w/in the same base view, these will be concatenated zero-copy
        by reusing underlying view

        - Otherwise, ``np.concatenate(arrays)`` will be invoked
    """

    assert len(arrs) > 0, "Provided collection of ndarrays may not be empty"

    if len(arrs) == 1:
        # Short-circuit
        return arrs[0]
    elif not _are_contiguous_1d_views(arrs):
        return np.concatenate(arrs)

    dtype = arrs[0].dtype
    base = _get_root_base(arrs[0])

    base_ptr = _get_buffer_address(base)
    start_byte = _get_buffer_address(arrs[0]) - base_ptr
    end_byte = start_byte + sum(a.nbytes for a in arrs)

    # Build the view from the base, using byte offsets for generality
    byte_view = base.view(np.uint8).reshape(-1)
    out = byte_view[start_byte:end_byte].view(dtype)

    return out


def _are_contiguous_1d_views(arrs: Union[np.ndarray, List[np.ndarray]]) -> bool:
    dtype = arrs[0].dtype
    base = _get_root_base(arrs[0])
    expected_addr = _get_base_ptr(arrs[0])

    for a in arrs:
        # Assert all provided arrays are
        #   - Raveled (1D)
        #   - Share dtype
        #   - Contiguous
        #   - Share the same `base` view (this is crucial to make sure
        #     that all provided ndarrays live w/in the same allocation and
        #     share its lifecycle)
        if (
            a.ndim != 1
            or a.dtype != dtype
            or not a.flags.c_contiguous
            or _get_root_base(a) is not base
        ):
            return False
        # Skip empty ndarrays
        if a.size == 0:
            continue

        buffer_addr = _get_base_ptr(a)
        if buffer_addr != expected_addr:
            return False

        expected_addr = buffer_addr + a.size * dtype.itemsize

    return True


def _get_base_ptr(a: np.ndarray) -> int:
    # same as a.ctypes.data, but robust for views
    return _get_buffer_address(a)


def _get_root_base(a: np.ndarray) -> np.ndarray:
    b = a
    while isinstance(b.base, np.ndarray):
        b = b.base
    return b if b.base is not None else b  # owner if base is None


def _get_buffer_address(arr: np.ndarray) -> int:
    """Get the address of the buffer underlying the provided NumPy ndarray."""
    return arr.__array_interface__["data"][0]


def _to_ndarray_helper(shape, value_type, offset, data_buffer):
    if pa.types.is_boolean(value_type):
        # Arrow boolean array buffers are bit-packed, with 8 entries per byte,
        # and are accessed via bit offsets.
        buffer_item_width = value_type.bit_width
    else:
        # We assume all other array types are accessed via byte array
        # offsets.
        buffer_item_width = value_type.bit_width // 8
    data_offset = buffer_item_width * offset

    if pa.types.is_boolean(value_type):
        # Special handling for boolean arrays, since Arrow
        # bit-packs boolean arrays while NumPy does not.
        # Cast as uint8 array and let NumPy unpack into a boolean view.
        # Offset into uint8 array, where each element is
        # a bucket for 8 booleans.
        byte_bucket_offset = data_offset // 8
        # Offset for a specific boolean, within a uint8 array element.
        bool_offset = data_offset % 8
        # The number of uint8 array elements (buckets) that our slice spans.
        # Note that, due to the offset for a specific boolean,
        # the slice can span byte boundaries even if it contains
        # less than 8 booleans.
        num_boolean_byte_buckets = 1 + ((bool_offset + np.prod(shape) - 1) // 8)
        # Construct the uint8 array view on the buffer.
        arr = np.ndarray(
            (num_boolean_byte_buckets,),
            dtype=np.uint8,
            buffer=data_buffer,
            offset=byte_bucket_offset,
        )
        # Unpack into a byte per boolean, using LSB bit-packed ordering.
        arr = np.unpackbits(arr, bitorder="little")
        # Interpret buffer as boolean array.
        return np.ndarray(shape, dtype=np.bool_, buffer=arr, offset=bool_offset)
    ext_dtype = value_type.to_pandas_dtype()
    # Special handling of ragged string tensors
    if pa.types.is_fixed_size_binary(value_type):
        ext_dtype = np.dtype(f"<U{value_type.byte_width // NUM_BYTES_PER_UNICODE_CHAR}")
    return np.ndarray(shape, dtype=ext_dtype, buffer=data_buffer, offset=data_offset)


try:
    # Registration needs an extension type instance, but then works for any instance of
    # the same subclass regardless of parametrization of the type.
    pa.register_extension_type(ArrowTensorType((0,), pa.int64()))
    pa.register_extension_type(ArrowTensorTypeV2((0,), pa.int64()))
    pa.register_extension_type(ArrowVariableShapedTensorType(pa.int64(), 0))
except pa.ArrowKeyError:
    # Extension types are already registered.
    pass
