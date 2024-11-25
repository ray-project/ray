import abc
import itertools
import json
import logging
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np
import pyarrow as pa
from packaging.version import parse as parse_version

from ray._private.utils import _get_pyarrow_version
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.tensor_extensions.utils import (
    _is_ndarray_tensor,
    _is_ndarray_variable_shaped_tensor,
    create_ragged_ndarray,
)
from ray.data._internal.util import GiB
from ray.util import log_once
from ray.util.annotations import DeveloperAPI, PublicAPI

PYARROW_VERSION = _get_pyarrow_version()
if PYARROW_VERSION is not None:
    PYARROW_VERSION = parse_version(PYARROW_VERSION)
# Minimum version of Arrow that supports ExtensionScalars.
# TODO(Clark): Remove conditional definition once we only support Arrow 8.0.0+.
MIN_PYARROW_VERSION_SCALAR = parse_version("8.0.0")
# Minimum version of Arrow that supports subclassable ExtensionScalars.
# TODO(Clark): Remove conditional definition once we only support Arrow 9.0.0+.
MIN_PYARROW_VERSION_SCALAR_SUBCLASS = parse_version("9.0.0")
# Minimum version supporting `zero_copy_only` flag in `ChunkedArray.to_numpy`
MIN_PYARROW_VERSION_CHUNKED_ARRAY_TO_NUMPY_ZERO_COPY_ONLY = parse_version("13.0.0")

NUM_BYTES_PER_UNICODE_CHAR = 4

# NOTE: Overflow threshold in bytes for most Arrow types using int32 as
#       its offsets
INT32_OVERFLOW_THRESHOLD = 2 * GiB

logger = logging.getLogger(__name__)


@DeveloperAPI
class ArrowConversionError(Exception):
    """Error raised when there is an issue converting data to Arrow."""

    MAX_DATA_STR_LEN = 200

    def __init__(self, data_str: str):
        if len(data_str) > self.MAX_DATA_STR_LEN:
            data_str = data_str[: self.MAX_DATA_STR_LEN] + "..."
        message = f"Error converting data to Arrow: {data_str}"
        super().__init__(message)


def _arrow_supports_extension_scalars():
    """
    Whether Arrow ExtensionScalars are supported in the current pyarrow version.

    This returns True if the pyarrow version is 8.0.0+, or if the pyarrow version is
    unknown.
    """
    # TODO(Clark): Remove utility once we only support Arrow 8.0.0+.
    return PYARROW_VERSION is None or PYARROW_VERSION >= MIN_PYARROW_VERSION_SCALAR


def _arrow_extension_scalars_are_subclassable():
    """
    Whether Arrow ExtensionScalars support subclassing in the current pyarrow version.

    This returns True if the pyarrow version is 9.0.0+, or if the pyarrow version is
    unknown.
    """
    # TODO(Clark): Remove utility once we only support Arrow 9.0.0+.
    return (
        PYARROW_VERSION is None
        or PYARROW_VERSION >= MIN_PYARROW_VERSION_SCALAR_SUBCLASS
    )


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
def convert_to_pyarrow_array(column_values: np.ndarray, column_name: str) -> pa.Array:
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
        if column_name == TENSOR_COLUMN_NAME or _is_ndarray_tensor(column_values):
            from ray.data.extensions.tensor_extension import ArrowTensorArray

            return ArrowTensorArray.from_numpy(column_values, column_name)
        else:
            return _convert_to_pyarrow_native_array(column_values, column_name)

    except ArrowConversionError as ace:
        from ray.data.extensions.object_extension import (
            ArrowPythonObjectArray,
            _object_extension_type_allowed,
        )

        if not _object_extension_type_allowed():
            should_serialize_as_object_ext_type = False
            object_ext_type_detail = (
                "skipping fallback to serialize as pickled python"
                f" objects (due to unsupported Arrow version {PYARROW_VERSION}, "
                f"min required version is {MIN_PYARROW_VERSION_SCALAR_SUBCLASS})"
            )
        else:
            from ray.data import DataContext

            if not DataContext.get_current().enable_fallback_to_arrow_object_ext_type:
                should_serialize_as_object_ext_type = False
                object_ext_type_detail = (
                    "skipping fallback to serialize as pickled python objects "
                    "(due to DataContext.enable_fallback_to_arrow_object_ext_type "
                    "= False)"
                )
            else:
                should_serialize_as_object_ext_type = True
                object_ext_type_detail = (
                    "falling back to serialize as pickled python objects"
                )

        # NOTE: To avoid logging following warning for every block it's
        #       only going to be logged in following cases
        #           - When fallback is disabled, or
        #           - It's being logged for the first time
        if not should_serialize_as_object_ext_type or log_once(
            "_fallback_to_arrow_object_extension_type_warning"
        ):
            logger.warning(
                f"Failed to convert column '{column_name}' into pyarrow "
                f"array due to: {ace}; {object_ext_type_detail}",
                exc_info=ace,
            )

        # If `ArrowPythonObjectType` is not supported raise original exception
        if not should_serialize_as_object_ext_type:
            raise

        # Otherwise, attempt to fall back to serialize as python objects
        return ArrowPythonObjectArray.from_objects(column_values)


def _convert_to_pyarrow_native_array(
    column_values: np.ndarray, column_name: str
) -> pa.Array:
    """Converts provided NumPy `ndarray` into PyArrow's `array` while only utilizing
    Arrow's natively supported types (ie no custom extension types)"""

    try:
        # NOTE: We explicitly infer PyArrow `DataType` so that
        #       we can perform upcasting to be able to accommodate
        #       blocks that are larger than 2Gb in size (limited
        #       by int32 offsets used by Arrow internally)
        dtype = _infer_pyarrow_type(column_values)

        logger.log(
            logging.getLevelName("TRACE"),
            f"Inferred dtype of '{dtype}' for column '{column_name}'",
        )

        return pa.array(column_values, type=dtype)
    except Exception as e:
        raise ArrowConversionError(str(column_values)) from e


def _infer_pyarrow_type(column_values: np.ndarray) -> Optional[pa.DataType]:
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
            return len(obj) > INT32_OVERFLOW_THRESHOLD

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


class _BaseFixedShapeArrowTensorType(pa.ExtensionType, abc.ABC):
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
    def shape(self):
        """
        Shape of contained tensors.
        """
        return self._shape

    @property
    def scalar_type(self):
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

    def __arrow_ext_serialize__(self):
        return json.dumps(self._shape).encode()

    def __arrow_ext_class__(self):
        """
        ExtensionArray subclass with custom logic for this array of tensors
        type.

        Returns:
            A subclass of pd.api.extensions.ExtensionArray.
        """
        return ArrowTensorArray

    if _arrow_extension_scalars_are_subclassable():
        # TODO(Clark): Remove this version guard once we only support Arrow 9.0.0+.
        def __arrow_ext_scalar_class__(self):
            """
            ExtensionScalar subclass with custom logic for this array of tensors type.
            """
            return ArrowTensorScalar

    if _arrow_supports_extension_scalars():
        # TODO(Clark): Remove this version guard once we only support Arrow 8.0.0+.
        def _extension_scalar_to_ndarray(
            self, scalar: pa.ExtensionScalar
        ) -> np.ndarray:
            """
            Convert an ExtensionScalar to a tensor element.
            """
            raw_values = scalar.value.values
            shape = scalar.type.shape
            value_type = raw_values.type
            offset = raw_values.offset
            data_buffer = raw_values.buffers()[1]
            return _to_ndarray_helper(shape, value_type, offset, data_buffer)

    def __str__(self) -> str:
        return (
            f"numpy.ndarray(shape={self.shape}, dtype={self.storage_type.value_type})"
        )

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def _need_variable_shaped_tensor_array(
        cls,
        array_types: Sequence[
            Union[
                "ArrowTensorType", "ArrowTensorTypeV2", "ArrowVariableShapedTensorType"
            ]
        ],
    ) -> bool:
        """
        Whether the provided list of tensor types needs a variable-shaped
        representation (i.e. `ArrowVariableShapedTensorType`) when concatenating
        or chunking. If one or more of the tensor types in `array_types` are
        variable-shaped and/or any of the tensor arrays have a different shape
        than the others, a variable-shaped tensor array representation will be
        required and this method will return True.

        Args:
            array_types: List of tensor types to check if a variable-shaped
            representation is required for concatenation

        Returns:
            True if concatenating arrays with types `array_types` requires
            a variable-shaped representation
        """
        shape = None
        for arr_type in array_types:
            # If at least one of the arrays is variable-shaped, we can immediately
            # short-circuit since we require a variable-shaped representation.
            if isinstance(arr_type, ArrowVariableShapedTensorType):
                return True
            if not isinstance(arr_type, get_arrow_extension_fixed_shape_tensor_types()):
                raise ValueError(
                    "All provided array types must be an instance of either "
                    "ArrowTensorType or ArrowVariableShapedTensorType, but "
                    f"got {arr_type}"
                )
            # We need variable-shaped representation if any of the tensor arrays have
            # different shapes.
            if shape is not None and arr_type.shape != shape:
                return True
            shape = arr_type.shape
        return False


@PublicAPI(stability="beta")
class ArrowTensorType(_BaseFixedShapeArrowTensorType):
    """Arrow ExtensionType (v1) for tensors.

    NOTE: This type does *NOT* support tensors larger than 4Gb (due to
          overflow of int32 offsets utilized inside Pyarrow `ListType`)
    """

    OFFSET_DTYPE = np.int32

    def __init__(self, shape: Tuple[int, ...], dtype: pa.DataType):
        """
        Construct the Arrow extension type for array of fixed-shaped tensors.

        Args:
            shape: Shape of contained tensors.
            dtype: pyarrow dtype of tensor elements.
        """

        super().__init__(shape, pa.list_(dtype), "ray.data.arrow_tensor")

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        shape = tuple(json.loads(serialized))
        return cls(shape, storage_type.value_type)


@PublicAPI(stability="alpha")
class ArrowTensorTypeV2(_BaseFixedShapeArrowTensorType):
    """Arrow ExtensionType (v2) for tensors (supporting tensors > 4Gb)."""

    OFFSET_DTYPE = np.int64

    def __init__(self, shape: Tuple[int, ...], dtype: pa.DataType):
        """
        Construct the Arrow extension type for array of fixed-shaped tensors.

        Args:
            shape: Shape of contained tensors.
            dtype: pyarrow dtype of tensor elements.
        """

        super().__init__(shape, pa.large_list(dtype), "ray.data.arrow_tensor_v2")

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        shape = tuple(json.loads(serialized))
        return cls(shape, storage_type.value_type)


if _arrow_extension_scalars_are_subclassable():
    # TODO(Clark): Remove this version guard once we only support Arrow 9.0.0+.
    @PublicAPI(stability="beta")
    class ArrowTensorScalar(pa.ExtensionScalar):
        def as_py(self) -> np.ndarray:
            return self.type._extension_scalar_to_ndarray(self)

        def __array__(self) -> np.ndarray:
            return self.as_py()


# TODO(Clark): Remove this mixin once we only support Arrow 9.0.0+.
class _ArrowTensorScalarIndexingMixin:
    """
    A mixin providing support for scalar indexing in tensor extension arrays for
    Arrow < 9.0.0, before full ExtensionScalar support was added. This mixin overrides
    __getitem__, __iter__, and to_pylist.
    """

    # This mixin will be a no-op (no methods added) for Arrow 9.0.0+.
    if not _arrow_extension_scalars_are_subclassable():
        # NOTE: These __iter__ and to_pylist definitions are shared for both
        # Arrow < 8.0.0 and Arrow 8.*.
        def __iter__(self):
            # Override pa.Array.__iter__() in order to return an iterator of
            # properly shaped tensors instead of an iterator of flattened tensors.
            # See comment in above __getitem__ method.
            for i in range(len(self)):
                # Use overridden __getitem__ method.
                yield self.__getitem__(i)

        def to_pylist(self):
            # Override pa.Array.to_pylist() due to a lack of ExtensionScalar
            # support (see comment in __getitem__).
            return list(self)

        if _arrow_supports_extension_scalars():
            # NOTE(Clark): This __getitem__ override is only needed for Arrow 8.*,
            # before ExtensionScalar subclassing support was added.
            # TODO(Clark): Remove these methods once we only support Arrow 9.0.0+.
            def __getitem__(self, key):
                # This __getitem__ hook allows us to support proper indexing when
                # accessing a single tensor (a "scalar" item of the array). Without this
                # hook for integer keys, the indexing will fail on pyarrow < 9.0.0 due
                # to a lack of ExtensionScalar subclassing support.

                # NOTE(Clark): We'd like to override the pa.Array.getitem() helper
                # instead, which would obviate the need for overriding __iter__(), but
                # unfortunately overriding Cython cdef methods with normal Python
                # methods isn't allowed.
                item = super().__getitem__(key)
                if not isinstance(key, slice):
                    item = item.type._extension_scalar_to_ndarray(item)
                return item

        else:
            # NOTE(Clark): This __getitem__ override is only needed for Arrow < 8.0.0,
            # before any ExtensionScalar support was added.
            # TODO(Clark): Remove these methods once we only support Arrow 8.0.0+.
            def __getitem__(self, key):
                # This __getitem__ hook allows us to support proper indexing when
                # accessing a single tensor (a "scalar" item of the array). Without this
                # hook for integer keys, the indexing will fail on pyarrow < 8.0.0 due
                # to a lack of ExtensionScalar support.

                # NOTE(Clark): We'd like to override the pa.Array.getitem() helper
                # instead, which would obviate the need for overriding __iter__(), but
                # unfortunately overriding Cython cdef methods with normal Python
                # methods isn't allowed.
                if isinstance(key, slice):
                    return super().__getitem__(key)
                return self._to_numpy(key)


# NOTE: We need to inherit from the mixin before pa.ExtensionArray to ensure that the
# mixin's overriding methods appear first in the MRO.
# TODO(Clark): Remove this mixin once we only support Arrow 9.0.0+.
@PublicAPI(stability="beta")
class ArrowTensorArray(_ArrowTensorScalarIndexingMixin, pa.ExtensionArray):
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

        # Data buffer.
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

        # Create Offset buffer
        offset_buffer = pa.py_buffer(
            pa_type_.OFFSET_DTYPE(
                [i * num_items_per_element for i in range(outer_len + 1)]
            )
        )

        storage = pa.Array.from_buffers(
            pa_type_.storage_type,
            outer_len,
            [None, offset_buffer],
            children=[data_array],
        )

        return pa.ExtensionArray.from_storage(pa_type_, storage)

    def _to_numpy(self, index: Optional[int] = None, zero_copy_only: bool = False):
        """
        Helper for getting either an element of the array of tensors as an
        ndarray, or the entire array of tensors as a single ndarray.

        Args:
            index: The index of the tensor element that we wish to return as
                an ndarray. If not given, the entire array of tensors is
                returned as an ndarray.
            zero_copy_only: If True, an exception will be raised if the
                conversion to a NumPy array would require copying the
                underlying data (e.g. in presence of nulls, or for
                non-primitive types). This argument is currently ignored, so
                zero-copy isn't enforced even if this argument is true.

        Returns:
            The corresponding tensor element as an ndarray if an index was
            given, or the entire array of tensors as an ndarray otherwise.
        """
        # TODO(Clark): Enforce zero_copy_only.
        # TODO(Clark): Support strides?
        # Buffers schema:
        # [None, offset_buffer, None, data_buffer]
        buffers = self.buffers()
        data_buffer = buffers[3]
        storage_list_type = self.storage.type
        value_type = storage_list_type.value_type
        ext_dtype = value_type.to_pandas_dtype()
        shape = self.type.shape
        if pa.types.is_boolean(value_type):
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
        if index is not None:
            # Getting a single tensor element of the array.
            offset_buffer = buffers[1]
            offset_array = np.ndarray(
                (len(self),), buffer=offset_buffer, dtype=self.type.OFFSET_DTYPE
            )
            # Offset into array to reach logical index.
            index_offset = offset_array[index]
            # Add the index offset to the base offset.
            offset += buffer_item_width * index_offset
        else:
            # Getting the entire array of tensors.
            shape = (len(self),) + shape
        if pa.types.is_boolean(value_type):
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
        return np.ndarray(shape, dtype=ext_dtype, buffer=data_buffer, offset=offset)

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
        return self._to_numpy(zero_copy_only=zero_copy_only)

    @classmethod
    def _concat_same_type(
        cls,
        to_concat: Sequence[
            Union["ArrowTensorArray", "ArrowVariableShapedTensorArray"]
        ],
    ) -> Union["ArrowTensorArray", "ArrowVariableShapedTensorArray"]:
        """
        Concatenate multiple tensor arrays.

        If one or more of the tensor arrays in to_concat are variable-shaped and/or any
        of the tensor arrays have a different shape than the others, a variable-shaped
        tensor array will be returned.
        """
        to_concat_types = [arr.type for arr in to_concat]
        if ArrowTensorType._need_variable_shaped_tensor_array(to_concat_types):
            # Need variable-shaped tensor array.
            # TODO(Clark): Eliminate this NumPy roundtrip by directly constructing the
            # underlying storage array buffers (NumPy roundtrip will not be zero-copy
            # for e.g. boolean arrays).
            # NOTE(Clark): Iterating over a tensor extension array converts each element
            # to an ndarray view.
            return ArrowVariableShapedTensorArray.from_numpy(
                [e for a in to_concat for e in a]
            )
        else:
            storage = pa.concat_arrays([c.storage for c in to_concat])

            return ArrowTensorArray.from_storage(to_concat[0].type, storage)

    @classmethod
    def _chunk_tensor_arrays(
        cls, arrs: Sequence[Union["ArrowTensorArray", "ArrowVariableShapedTensorArray"]]
    ) -> pa.ChunkedArray:
        """
        Create a ChunkedArray from multiple tensor arrays.
        """
        arrs_types = [arr.type for arr in arrs]
        if ArrowTensorType._need_variable_shaped_tensor_array(arrs_types):
            new_arrs = []
            for a in arrs:
                if isinstance(a.type, get_arrow_extension_fixed_shape_tensor_types()):
                    a = a.to_variable_shaped_tensor_array()
                assert isinstance(a.type, ArrowVariableShapedTensorType)
                new_arrs.append(a)
            arrs = new_arrs
        return pa.chunked_array(arrs)

    def to_variable_shaped_tensor_array(self) -> "ArrowVariableShapedTensorArray":
        """
        Convert this tensor array to a variable-shaped tensor array.

        This is primarily used when concatenating multiple chunked tensor arrays where
        at least one chunked array is already variable-shaped and/or the shapes of the
        chunked arrays differ, in which case the resulting concatenated tensor array
        will need to be in the variable-shaped representation.
        """
        # TODO(Clark): Eliminate this NumPy roundtrip by directly constructing the
        # underlying storage array buffers (NumPy roundtrip will not be zero-copy for
        # e.g. boolean arrays).
        return ArrowVariableShapedTensorArray.from_numpy(self.to_numpy())


@PublicAPI(stability="alpha")
class ArrowVariableShapedTensorType(pa.ExtensionType):
    """
    Arrow ExtensionType for an array of heterogeneous-shaped, homogeneous-typed
    tensors.

    This is the Arrow side of TensorDtype for tensor elements with different shapes.
    Note that this extension only supports non-ragged tensor elements; i.e., when
    considering each tensor element in isolation, they must have a well-defined,
    non-ragged shape.

    See Arrow extension type docs:
    https://arrow.apache.org/docs/python/extending_types.html#defining-extension-types-user-defined-types
    """

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
                [("data", pa.large_list(dtype)), ("shape", pa.list_(pa.int64()))]
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
            (None,) * self.ndim,
            self.storage_type["data"].type.value_type.to_pandas_dtype(),
        )

    @property
    def ndim(self) -> int:
        """Return the number of dimensions in the tensor elements."""
        return self._ndim

    @property
    def scalar_type(self):
        """Returns the type of the underlying tensor elements."""
        data_field_index = self.storage_type.get_field_index("data")
        return self.storage_type[data_field_index].type.value_type

    def __reduce__(self):
        return self.__arrow_ext_deserialize__, (
            self.storage_type,
            self.__arrow_ext_serialize__(),
        )

    def __arrow_ext_serialize__(self):
        return json.dumps(self._ndim).encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        ndim = json.loads(serialized)
        dtype = storage_type["data"].type.value_type
        return cls(dtype, ndim)

    def __arrow_ext_class__(self):
        """
        ExtensionArray subclass with custom logic for this array of tensors
        type.

        Returns:
            A subclass of pd.api.extensions.ExtensionArray.
        """
        return ArrowVariableShapedTensorArray

    if _arrow_extension_scalars_are_subclassable():
        # TODO(Clark): Remove this version guard once we only support Arrow 9.0.0+.
        def __arrow_ext_scalar_class__(self):
            """
            ExtensionScalar subclass with custom logic for this array of tensors type.
            """
            return ArrowTensorScalar

    def __str__(self) -> str:
        dtype = self.storage_type["data"].type.value_type
        return f"numpy.ndarray(ndim={self.ndim}, dtype={dtype})"

    def __repr__(self) -> str:
        return str(self)

    if _arrow_supports_extension_scalars():
        # TODO(Clark): Remove this version guard once we only support Arrow 8.0.0+.
        def _extension_scalar_to_ndarray(
            self, scalar: pa.ExtensionScalar
        ) -> np.ndarray:
            """
            Convert an ExtensionScalar to a tensor element.
            """
            data = scalar.value.get("data")
            raw_values = data.values

            shape = tuple(scalar.value.get("shape").as_py())
            value_type = raw_values.type
            offset = raw_values.offset
            data_buffer = raw_values.buffers()[1]
            return _to_ndarray_helper(shape, value_type, offset, data_buffer)


# NOTE: We need to inherit from the mixin before pa.ExtensionArray to ensure that the
# mixin's overriding methods appear first in the MRO.
# TODO(Clark): Remove this mixin once we only support Arrow 9.0.0+.
@PublicAPI(stability="alpha")
class ArrowVariableShapedTensorArray(
    _ArrowTensorScalarIndexingMixin, pa.ExtensionArray
):
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

        # Whether all subndarrays are contiguous views of the same ndarray.
        shapes, sizes, raveled = [], [], []
        ndim = None
        for a in arr:
            a = np.asarray(a)
            if ndim is not None and a.ndim != ndim:
                raise ValueError(
                    "ArrowVariableShapedTensorArray only supports tensor elements that "
                    "all have the same number of dimensions, but got tensor elements "
                    f"with dimensions: {ndim}, {a.ndim}"
                )
            ndim = a.ndim
            shapes.append(a.shape)
            sizes.append(a.size)
            # Convert to 1D array view; this should be zero-copy in the common case.
            # NOTE: If array is not in C-contiguous order, this will convert it to
            # C-contiguous order, incurring a copy.
            a = np.ravel(a, order="C")
            raveled.append(a)
        # Get size offsets and total size.
        sizes = np.array(sizes)
        size_offsets = np.cumsum(sizes)
        total_size = size_offsets[-1]
        # Concatenate 1D views into a contiguous 1D array.
        if all(_is_contiguous_view(curr, prev) for prev, curr in _pairwise(raveled)):
            # An optimized zero-copy path if raveled tensor elements are already
            # contiguous in memory, e.g. if this tensor array has already done a
            # roundtrip through our Arrow representation.
            np_data_buffer = raveled[-1].base
        else:
            np_data_buffer = np.concatenate(raveled)
        dtype = np_data_buffer.dtype
        pa_dtype = pa.from_numpy_dtype(dtype)
        if pa.types.is_string(pa_dtype):
            if dtype.byteorder == ">" or (
                dtype.byteorder == "=" and sys.byteorder == "big"
            ):
                raise ValueError(
                    "Only little-endian string tensors are supported, "
                    f"but got: {dtype}"
                )
            pa_dtype = pa.binary(dtype.itemsize)
        if dtype.type is np.bool_:
            # NumPy doesn't represent boolean arrays as bit-packed, so we manually
            # bit-pack the booleans before handing the buffer off to Arrow.
            # NOTE: Arrow expects LSB bit-packed ordering.
            # NOTE: This creates a copy.
            np_data_buffer = np.packbits(np_data_buffer, bitorder="little")
        data_buffer = pa.py_buffer(np_data_buffer)
        # Construct underlying data array.
        value_array = pa.Array.from_buffers(pa_dtype, total_size, [None, data_buffer])
        # Construct array for offsets into the 1D data array, where each offset
        # corresponds to a tensor element.
        size_offsets = np.insert(size_offsets, 0, 0)
        offset_array = pa.array(size_offsets)
        data_array = pa.LargeListArray.from_arrays(offset_array, value_array)
        # We store the tensor element shapes so we can reconstruct each tensor when
        # converting back to NumPy ndarrays.
        shape_array = pa.array(shapes)
        # Build storage array containing tensor data and the tensor element shapes.
        storage = pa.StructArray.from_arrays(
            [data_array, shape_array],
            ["data", "shape"],
        )
        type_ = ArrowVariableShapedTensorType(pa_dtype, ndim)
        return pa.ExtensionArray.from_storage(type_, storage)

    def _to_numpy(self, index: Optional[int] = None, zero_copy_only: bool = False):
        """
        Helper for getting either an element of the array of tensors as an ndarray, or
        the entire array of tensors as a single ndarray.

        Args:
            index: The index of the tensor element that we wish to return as an
                ndarray. If not given, the entire array of tensors is returned as an
                ndarray.
            zero_copy_only: If True, an exception will be raised if the conversion to a
                NumPy array would require copying the underlying data (e.g. in presence
                of nulls, or for non-primitive types). This argument is currently
                ignored, so zero-copy isn't enforced even if this argument is true.

        Returns:
            The corresponding tensor element as an ndarray if an index was given, or
            the entire array of tensors as an ndarray otherwise.
        """
        # TODO(Clark): Enforce zero_copy_only.
        # TODO(Clark): Support strides?
        if index is None:
            # Get individual ndarrays for each tensor element.
            arrs = [self._to_numpy(i, zero_copy_only) for i in range(len(self))]
            # Return ragged NumPy ndarray in the ndarray of ndarray pointers
            # representation.
            return create_ragged_ndarray(arrs)
        data = self.storage.field("data")
        shapes = self.storage.field("shape")

        shape = shapes[index].as_py()
        value_type = data.type.value_type
        offset = data.offsets[index].as_py()
        data_buffer = data.buffers()[3]
        return _to_ndarray_helper(shape, value_type, offset, data_buffer)

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
        return self._to_numpy(zero_copy_only=zero_copy_only)


def _is_contiguous_view(curr: np.ndarray, prev: Optional[np.ndarray]) -> bool:
    """Check if the provided tensor element is contiguous with the previous tensor
    element.

    Args:
        curr: The tensor element whose contiguity that we wish to check.
        prev: The previous tensor element in the tensor array.

    Returns:
        Whether the provided tensor element is contiguous with the previous tensor
        element.
    """
    if (
        curr.base is None
        or not curr.data.c_contiguous
        or (prev is not None and curr.base is not prev.base)
    ):
        # curr is either:
        # - not a view,
        # - not in C-contiguous order,
        # - a view that does not share its base with the other subndarrays.
        return False
    else:
        # curr is a C-contiguous view that shares the same base with the seen
        # subndarrays, but we need to confirm that it is contiguous with the
        # previous subndarray.
        if prev is not None and (
            _get_buffer_address(curr) - _get_buffer_address(prev)
            != prev.base.dtype.itemsize * prev.size
        ):
            # This view is not contiguous with the previous view.
            return False
        else:
            return True


def _get_buffer_address(arr: np.ndarray) -> int:
    """Get the address of the buffer underlying the provided NumPy ndarray."""
    return arr.__array_interface__["data"][0]


def _pairwise(iterable):
    # pairwise('ABCDEFG') --> AB BC CD DE EF FG
    # Backport of itertools.pairwise for Python < 3.10.
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


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
