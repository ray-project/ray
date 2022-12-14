import itertools
from typing import Iterable, Optional, Tuple, List, Sequence, Union

from pkg_resources._vendor.packaging.version import parse as parse_version
import numpy as np
import pyarrow as pa

from ray.air.util.tensor_extensions.utils import (
    _is_ndarray_variable_shaped_tensor,
    _create_strict_ragged_ndarray,
)
from ray._private.utils import _get_pyarrow_version
from ray.util.annotations import PublicAPI


PYARROW_VERSION = _get_pyarrow_version()
if PYARROW_VERSION is not None:
    PYARROW_VERSION = parse_version(PYARROW_VERSION)
# Minimum version of Arrow that supports ExtensionScalars.
# TODO(Clark): Remove conditional definition once we only support Arrow 8.0.0+.
MIN_PYARROW_VERSION_SCALAR = parse_version("8.0.0")
# Minimum version of Arrow that supports subclassable ExtensionScalars.
# TODO(Clark): Remove conditional definition once we only support Arrow 9.0.0+.
MIN_PYARROW_VERSION_SCALAR_SUBCLASS = parse_version("9.0.0")


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


@PublicAPI(stability="beta")
class ArrowTensorType(pa.PyExtensionType):
    """
    Arrow ExtensionType for an array of fixed-shaped, homogeneous-typed
    tensors.

    This is the Arrow side of TensorDtype.

    See Arrow extension type docs:
    https://arrow.apache.org/docs/python/extending_types.html#defining-extension-types-user-defined-types
    """

    def __init__(self, shape: Tuple[int, ...], dtype: pa.DataType):
        """
        Construct the Arrow extension type for array of fixed-shaped tensors.

        Args:
            shape: Shape of contained tensors.
            dtype: pyarrow dtype of tensor elements.
        """
        self._shape = shape
        super().__init__(pa.list_(dtype))

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

        return TensorDtype(self._shape, self.storage_type.value_type.to_pandas_dtype())

    def __reduce__(self):
        return ArrowTensorType, (self._shape, self.storage_type.value_type)

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
            # TODO(Clark): Construct ndarray view directly on tensor element buffer to
            # ensure reliable zero-copy semantics.
            flat_ndarray = scalar.value.values.to_numpy(zero_copy_only=False)
            return flat_ndarray.reshape(self.shape)

    def __str__(self) -> str:
        return (
            f"ArrowTensorType(shape={self.shape}, dtype={self.storage_type.value_type})"
        )

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def _need_variable_shaped_tensor_array(
        cls,
        array_types: Sequence[
            Union["ArrowTensorType", "ArrowVariableShapedTensorType"]
        ],
    ) -> bool:
        """
        Whether the provided list of tensor types need a variable-shaped
        representation (i.e. `ArrowVariableShapedTensorType`) when concatenating
        or chunking. If one or more of the tensor types in `array_types` are
        variable-shaped and/or any of the tensor arrays have a different shape
        than the others, a variable-shaped tensor array representation will be
        required and this method will return True.

        Args:
            array_types: List of tensor types to check if a variable-shaped
            representation is required for concatenation

        Returns: True if concatenating arrays with types `array_types` requires
            a variable-shaped representation
        """
        needs_variable_shaped = False
        shape = None
        for arr_type in array_types:
            if isinstance(arr_type, ArrowVariableShapedTensorType) or (
                shape is not None and arr_type.shape != shape
            ):
                needs_variable_shaped = True
                break
            if shape is None:
                shape = arr_type.shape
        return needs_variable_shaped


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

    OFFSET_DTYPE = np.int32

    @classmethod
    def from_numpy(
        cls, arr: Union[np.ndarray, Iterable[np.ndarray]]
    ) -> Union["ArrowTensorArray", "ArrowVariableShapedTensorArray"]:
        """
        Convert an ndarray or an iterable of ndarrays to an array of homogeneous-typed
        tensors. If given fixed-shape tensor elements, this will return an
        ``ArrowTensorArray``; if given variable-shape tensor elements, this will return
        an ``ArrowVariableShapedTensorArray``.

        Args:
            arr: An ndarray or an iterable of ndarrays.

        Returns:
            - If fixed-shape tensor elements, an ``ArrowTensorArray`` containing
              ``len(arr)`` tensors of fixed shape.
            - If variable-shaped tensor elements, an ``ArrowVariableShapedTensorArray``
              containing ``len(arr)`` tensors of variable shape.
            - If scalar elements, a ``pyarrow.Array``.
        """
        if isinstance(arr, (list, tuple)) and arr and isinstance(arr[0], np.ndarray):
            # Stack ndarrays and pass through to ndarray handling logic below.
            try:
                arr = np.stack(arr, axis=0)
            except ValueError:
                # ndarray stacking may fail if the arrays are heterogeneously-shaped.
                arr = np.array(arr, dtype=object)
        if isinstance(arr, np.ndarray):
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
            pa_dtype = pa.from_numpy_dtype(arr.dtype)
            outer_len = arr.shape[0]
            element_shape = arr.shape[1:]
            total_num_items = arr.size
            num_items_per_element = np.prod(element_shape) if element_shape else 1

            # Data buffer.
            if pa.types.is_boolean(pa_dtype):
                # NumPy doesn't represent boolean arrays as bit-packed, so we manually
                # bit-pack the booleans before handing the buffer off to Arrow.
                # NOTE: Arrow expects LSB bit-packed ordering.
                # NOTE: This creates a copy.
                arr = np.packbits(arr, bitorder="little")
            data_buffer = pa.py_buffer(arr)
            data_array = pa.Array.from_buffers(
                pa_dtype, total_num_items, [None, data_buffer]
            )

            # Offset buffer.
            offset_buffer = pa.py_buffer(
                cls.OFFSET_DTYPE(
                    [i * num_items_per_element for i in range(outer_len + 1)]
                )
            )

            storage = pa.Array.from_buffers(
                pa.list_(pa_dtype),
                outer_len,
                [None, offset_buffer],
                children=[data_array],
            )
            type_ = ArrowTensorType(element_shape, pa_dtype)
            return pa.ExtensionArray.from_storage(type_, storage)
        elif isinstance(arr, Iterable):
            return cls.from_numpy(list(arr))
        else:
            raise ValueError("Must give ndarray or iterable of ndarrays.")

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
                (len(self),), buffer=offset_buffer, dtype=self.OFFSET_DTYPE
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
                if isinstance(a.type, ArrowTensorType):
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
class ArrowVariableShapedTensorType(pa.PyExtensionType):
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
            pa.struct([("data", pa.list_(dtype)), ("shape", pa.list_(pa.int64()))])
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
        return (
            ArrowVariableShapedTensorType,
            (self.storage_type["data"].type.value_type, self._ndim),
        )

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
        return f"ArrowVariableShapedTensorType(dtype={dtype}, ndim={self.ndim})"

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
            # TODO(Clark): Construct ndarray view directly on tensor element buffer to
            # ensure reliable zero-copy semantics.
            flat_ndarray = scalar.value.get("data").values.to_numpy(
                zero_copy_only=False
            )
            shape = tuple(scalar.value.get("shape").as_py())
            return flat_ndarray.reshape(shape)


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

    OFFSET_DTYPE = np.int32

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
        if isinstance(arr, Iterable):
            arr = list(arr)
        elif not isinstance(arr, (list, tuple)):
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
        if dtype.type is np.object_:
            types_and_shapes = [(f"dtype={a.dtype}", f"shape={a.shape}") for a in arr]
            raise ValueError(
                "ArrowVariableShapedTensorArray only supports heterogeneous-shaped "
                "tensor collections, not arbitrarily nested ragged tensors. Got "
                f"arrays: {types_and_shapes}"
            )
        pa_dtype = pa.from_numpy_dtype(dtype)
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
        data_array = pa.ListArray.from_arrays(offset_array, value_array)
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
            return _create_strict_ragged_ndarray(arrs)
        data = self.storage.field("data")
        shapes = self.storage.field("shape")
        value_type = data.type.value_type
        if pa.types.is_boolean(value_type):
            # Arrow boolean array buffers are bit-packed, with 8 entries per byte,
            # and are accessed via bit offsets.
            buffer_item_width = value_type.bit_width
        else:
            # We assume all other array types are accessed via byte array
            # offsets.
            buffer_item_width = value_type.bit_width // 8
        shape = shapes[index].as_py()
        offset = data.offsets[index].as_py()
        data_offset = buffer_item_width * offset
        data_buffer = data.buffers()[3]
        if not pa.types.is_boolean(value_type):
            return np.ndarray(
                shape,
                dtype=value_type.to_pandas_dtype(),
                buffer=data_buffer,
                offset=data_offset,
            )
        # Special handling for boolean arrays, since Arrow bit-packs boolean arrays
        # while NumPy does not.
        # Cast as uint8 array and let NumPy unpack into a boolean view.
        # Offset into uint8 array, where each element is a bucket for 8 booleans.
        byte_bucket_offset = data_offset // 8
        # Offset for a specific boolean, within a uint8 array element.
        bool_offset = data_offset % 8
        # The number of uint8 array elements (buckets) that our slice spans.
        # Note that, due to the offset for a specific boolean, the slice can span byte
        # boundaries even if it contains less than 8 booleans.
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
