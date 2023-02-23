from typing import TYPE_CHECKING, Dict, List, Optional, Union, Tuple

import numpy as np
import pyarrow
import tensorflow as tf

from ray.air.util.data_batch_conversion import _unwrap_ndarray_object_type_if_needed

if TYPE_CHECKING:
    from ray.data._internal.pandas_block import PandasBlockSchema


def convert_ndarray_to_tf_tensor(
    ndarray: np.ndarray,
    dtype: Optional[tf.dtypes.DType] = None,
    type_spec: Optional[tf.TypeSpec] = None,
) -> tf.Tensor:
    """Convert a NumPy ndarray to a TensorFlow Tensor.

    Args:
        ndarray: A NumPy ndarray that we wish to convert to a TensorFlow Tensor.
        dtype: A TensorFlow dtype for the created tensor; if None, the dtype will be
            inferred from the NumPy ndarray data.
        type_spec: A type spec that specifies the shape and dtype of the returned
            tensor. If you specify ``dtype``, the dtype stored in the type spec is
            ignored.

    Returns: A TensorFlow Tensor.
    """
    if dtype is None and type_spec is not None:
        dtype = type_spec.dtype

    is_ragged = isinstance(type_spec, tf.RaggedTensorSpec)
    ndarray = _unwrap_ndarray_object_type_if_needed(ndarray)
    if is_ragged:
        return tf.ragged.constant(ndarray, dtype=dtype)
    else:
        return tf.convert_to_tensor(ndarray, dtype=dtype)


def convert_ndarray_batch_to_tf_tensor_batch(
    ndarrays: Union[np.ndarray, Dict[str, np.ndarray]],
    dtypes: Optional[Union[tf.dtypes.DType, Dict[str, tf.dtypes.DType]]] = None,
) -> Union[tf.Tensor, Dict[str, tf.Tensor]]:
    """Convert a NumPy ndarray batch to a TensorFlow Tensor batch.

    Args:
        ndarray: A (dict of) NumPy ndarray(s) that we wish to convert to a TensorFlow
            Tensor.
        dtype: A (dict of) TensorFlow dtype(s) for the created tensor; if None, the
            dtype will be inferred from the NumPy ndarray data.

    Returns: A (dict of) TensorFlow Tensor(s).
    """
    if isinstance(ndarrays, np.ndarray):
        # Single-tensor case.
        if isinstance(dtypes, dict):
            if len(dtypes) != 1:
                raise ValueError(
                    "When constructing a single-tensor batch, only a single dtype "
                    f"should be given, instead got: {dtypes}"
                )
            dtypes = next(iter(dtypes.values()))
        batch = convert_ndarray_to_tf_tensor(ndarrays, dtypes)
    else:
        # Multi-tensor case.
        batch = {
            col_name: convert_ndarray_to_tf_tensor(
                col_ndarray,
                dtype=dtypes[col_name] if isinstance(dtypes, dict) else dtypes,
            )
            for col_name, col_ndarray in ndarrays.items()
        }

    return batch


# This is not foolproof, but it's better than nothing
# The place it is used in will be deprecated soon
def contains_tensorflow_object(obj):
    if hasattr(obj, "__module__") and (
        "keras" in obj.__module__ or "tensorflow" in obj.__module__
    ):
        return True
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if contains_tensorflow_object(k):
                return True
            if contains_tensorflow_object(v):
                return True
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            if contains_tensorflow_object(v):
                return True
    return False


def get_type_spec(
    schema: Union["pyarrow.lib.Schema", "PandasBlockSchema"],
    columns: Union[str, List[str]],
) -> Union[tf.TypeSpec, Dict[str, tf.TypeSpec]]:
    import pyarrow as pa
    from ray.data.extensions import TensorDtype, ArrowTensorType

    assert not isinstance(schema, type)

    dtypes: Dict[str, Union[np.dtype, pa.DataType]] = dict(
        zip(schema.names, schema.types)
    )

    def get_dtype(dtype: Union[np.dtype, pa.DataType]) -> tf.dtypes.DType:
        if isinstance(dtype, pa.DataType):
            dtype = dtype.to_pandas_dtype()
        if isinstance(dtype, TensorDtype):
            dtype = dtype.element_dtype
        return tf.dtypes.as_dtype(dtype)

    def get_shape(dtype: Union[np.dtype, pa.DataType]) -> Tuple[int, ...]:
        shape = (None,)
        if isinstance(dtype, ArrowTensorType):
            dtype = dtype.to_pandas_dtype()
        if isinstance(dtype, TensorDtype):
            shape += dtype.element_shape
        return shape

    def get_tensor_spec(
        dtype: Union[np.dtype, pa.DataType], *, name: str
    ) -> tf.TypeSpec:
        shape, dtype = get_shape(dtype), get_dtype(dtype)
        # Batch dimension is always `None`. So, if there's more than one `None`-valued
        # dimension, then the tensor is ragged.
        is_ragged = sum(dim is None for dim in shape) > 1
        if is_ragged:
            type_spec = tf.RaggedTensorSpec(shape, dtype=dtype)
        else:
            type_spec = tf.TensorSpec(shape, dtype=dtype, name=name)
        return type_spec

    if isinstance(columns, str):
        name, dtype = columns, dtypes[columns]
        return get_tensor_spec(dtype, name=name)

    return {
        name: get_tensor_spec(dtype, name=name)
        for name, dtype in dtypes.items()
        if name in columns
    }
