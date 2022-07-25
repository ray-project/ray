from typing import Iterable, Optional, Tuple

import numpy as np
import pyarrow as pa

from ray.util.annotations import PublicAPI


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

    def to_pandas_dtype(self):
        """
        Convert Arrow extension type to corresponding Pandas dtype.

        Returns:
            An instance of pd.api.extensions.ExtensionDtype.
        """
        from ray.air.util.tensor_extensions.pandas import TensorDtype

        return TensorDtype()

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

    def __str__(self):
        return "<ArrowTensorType: shape={}, dtype={}>".format(
            self.shape, self.storage_type.value_type
        )


@PublicAPI(stability="beta")
class ArrowTensorArray(pa.ExtensionArray):
    """
    An array of fixed-shape, homogeneous-typed tensors.

    This is the Arrow side of TensorArray.

    See Arrow docs for customizing extension arrays:
    https://arrow.apache.org/docs/python/extending_types.html#custom-extension-array-class
    """

    OFFSET_DTYPE = np.int32

    def __getitem__(self, key):
        # This __getitem__ hook allows us to support proper
        # indexing when accessing a single tensor (a "scalar" item of the
        # array). Without this hook for integer keys, the indexing will fail on
        # all currently released pyarrow versions due to a lack of proper
        # ExtensionScalar support. Support was added in
        # https://github.com/apache/arrow/pull/10904, but hasn't been released
        # at the time of this comment, and even with this support, the returned
        # ndarray is a flat representation of the n-dimensional tensor.

        # NOTE(Clark): We'd like to override the pa.Array.getitem() helper
        # instead, which would obviate the need for overriding __iter__()
        # below, but unfortunately overriding Cython cdef methods with normal
        # Python methods isn't allowed.
        if isinstance(key, slice):
            return super().__getitem__(key)
        return self._to_numpy(key)

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

    @classmethod
    def from_numpy(cls, arr):
        """
        Convert an ndarray or an iterable of fixed-shape ndarrays to an array
        of fixed-shape, homogeneous-typed tensors.

        Args:
            arr: An ndarray or an iterable of fixed-shape ndarrays.

        Returns:
            An ArrowTensorArray containing len(arr) tensors of fixed shape.
        """
        if isinstance(arr, (list, tuple)):
            if np.isscalar(arr[0]):
                return pa.array(arr)
            elif isinstance(arr[0], np.ndarray):
                # Stack ndarrays and pass through to ndarray handling logic
                # below.
                arr = np.stack(arr, axis=0)
        if isinstance(arr, np.ndarray):
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
