"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: sparse_ops.cc
"""

import collections as _collections
import six as _six

from tensorflow.python import pywrap_tensorflow as _pywrap_tensorflow
from tensorflow.python.eager import context as _context
from tensorflow.python.eager import core as _core
from tensorflow.python.eager import execute as _execute
from tensorflow.python.framework import dtypes as _dtypes
from tensorflow.python.framework import errors as _errors
from tensorflow.python.framework import tensor_shape as _tensor_shape

from tensorflow.core.framework import op_def_pb2 as _op_def_pb2
# Needed to trigger the call to _set_call_cpp_shape_fn.
from tensorflow.python.framework import common_shapes as _common_shapes
from tensorflow.python.framework import op_def_registry as _op_def_registry
from tensorflow.python.framework import ops as _ops
from tensorflow.python.framework import op_def_library as _op_def_library
from tensorflow.python.util.deprecation import deprecated_endpoints
from tensorflow.python.util import dispatch as _dispatch
from tensorflow.python.util.tf_export import tf_export


def add_many_sparse_to_tensors_map(sparse_indices, sparse_values, sparse_shape, container="", shared_name="", name=None):
  r"""Add an `N`-minibatch `SparseTensor` to a `SparseTensorsMap`, return `N` handles.

  A `SparseTensor` of rank `R` is represented by three tensors: `sparse_indices`,
  `sparse_values`, and `sparse_shape`, where

  ```sparse_indices.shape[1] == sparse_shape.shape[0] == R```

  An `N`-minibatch of `SparseTensor` objects is represented as a `SparseTensor`
  having a first `sparse_indices` column taking values between `[0, N)`, where
  the minibatch size `N == sparse_shape[0]`.

  The input `SparseTensor` must have rank `R` greater than 1, and the first
  dimension is treated as the minibatch dimension.  Elements of the `SparseTensor`
  must be sorted in increasing order of this first dimension.  The stored
  `SparseTensor` objects pointed to by each row of the output `sparse_handles`
  will have rank `R-1`.

  The `SparseTensor` values can then be read out as part of a minibatch by passing
  the given keys as vector elements to `TakeManySparseFromTensorsMap`.  To ensure
  the correct `SparseTensorsMap` is accessed, ensure that the same
  `container` and `shared_name` are passed to that Op.  If no `shared_name`
  is provided here, instead use the *name* of the Operation created by calling
  `AddManySparseToTensorsMap` as the `shared_name` passed to
  `TakeManySparseFromTensorsMap`.  Ensure the Operations are colocated.

  Args:
    sparse_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the minibatch `SparseTensor`.
      `sparse_indices[:, 0]` must be ordered values in `[0, N)`.
    sparse_values: A `Tensor`.
      1-D.  The `values` of the minibatch `SparseTensor`.
    sparse_shape: A `Tensor` of type `int64`.
      1-D.  The `shape` of the minibatch `SparseTensor`.
      The minibatch size `N == sparse_shape[0]`.
    container: An optional `string`. Defaults to `""`.
      The container name for the `SparseTensorsMap` created by this op.
    shared_name: An optional `string`. Defaults to `""`.
      The shared name for the `SparseTensorsMap` created by this op.
      If blank, the new Operation's unique name is used.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AddManySparseToTensorsMap", name, _ctx._post_execution_callbacks,
        sparse_indices, sparse_values, sparse_shape, "container", container,
        "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return add_many_sparse_to_tensors_map_eager_fallback(
            sparse_indices, sparse_values, sparse_shape, container=container,
            shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AddManySparseToTensorsMap", sparse_indices=sparse_indices,
                                     sparse_values=sparse_values,
                                     sparse_shape=sparse_shape,
                                     container=container,
                                     shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "container", _op.get_attr("container"),
            "shared_name", _op.get_attr("shared_name"))
  _execute.record_gradient(
      "AddManySparseToTensorsMap", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def add_many_sparse_to_tensors_map_eager_fallback(sparse_indices, sparse_values, sparse_shape, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function add_many_sparse_to_tensors_map
  """
  _ctx = ctx if ctx else _context.context()
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _attr_T, (sparse_values,) = _execute.args_to_matching_eager([sparse_values], _ctx)
  sparse_indices = _ops.convert_to_tensor(sparse_indices, _dtypes.int64)
  sparse_shape = _ops.convert_to_tensor(sparse_shape, _dtypes.int64)
  _inputs_flat = [sparse_indices, sparse_values, sparse_shape]
  _attrs = ("T", _attr_T, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"AddManySparseToTensorsMap", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "AddManySparseToTensorsMap", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def add_sparse_to_tensors_map(sparse_indices, sparse_values, sparse_shape, container="", shared_name="", name=None):
  r"""Add a `SparseTensor` to a `SparseTensorsMap` return its handle.

  A `SparseTensor` is represented by three tensors: `sparse_indices`,
  `sparse_values`, and `sparse_shape`.

  This operator takes the given `SparseTensor` and adds it to a container
  object (a `SparseTensorsMap`).  A unique key within this container is generated
  in the form of an `int64`, and this is the value that is returned.

  The `SparseTensor` can then be read out as part of a minibatch by passing
  the key as a vector element to `TakeManySparseFromTensorsMap`.  To ensure
  the correct `SparseTensorsMap` is accessed, ensure that the same
  `container` and `shared_name` are passed to that Op.  If no `shared_name`
  is provided here, instead use the *name* of the Operation created by calling
  `AddSparseToTensorsMap` as the `shared_name` passed to
  `TakeManySparseFromTensorsMap`.  Ensure the Operations are colocated.

  Args:
    sparse_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the `SparseTensor`.
    sparse_values: A `Tensor`. 1-D.  The `values` of the `SparseTensor`.
    sparse_shape: A `Tensor` of type `int64`.
      1-D.  The `shape` of the `SparseTensor`.
    container: An optional `string`. Defaults to `""`.
      The container name for the `SparseTensorsMap` created by this op.
    shared_name: An optional `string`. Defaults to `""`.
      The shared name for the `SparseTensorsMap` created by this op.
      If blank, the new Operation's unique name is used.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AddSparseToTensorsMap", name, _ctx._post_execution_callbacks,
        sparse_indices, sparse_values, sparse_shape, "container", container,
        "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return add_sparse_to_tensors_map_eager_fallback(
            sparse_indices, sparse_values, sparse_shape, container=container,
            shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AddSparseToTensorsMap", sparse_indices=sparse_indices,
                                 sparse_values=sparse_values,
                                 sparse_shape=sparse_shape,
                                 container=container, shared_name=shared_name,
                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "container", _op.get_attr("container"),
            "shared_name", _op.get_attr("shared_name"))
  _execute.record_gradient(
      "AddSparseToTensorsMap", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def add_sparse_to_tensors_map_eager_fallback(sparse_indices, sparse_values, sparse_shape, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function add_sparse_to_tensors_map
  """
  _ctx = ctx if ctx else _context.context()
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _attr_T, (sparse_values,) = _execute.args_to_matching_eager([sparse_values], _ctx)
  sparse_indices = _ops.convert_to_tensor(sparse_indices, _dtypes.int64)
  sparse_shape = _ops.convert_to_tensor(sparse_shape, _dtypes.int64)
  _inputs_flat = [sparse_indices, sparse_values, sparse_shape]
  _attrs = ("T", _attr_T, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"AddSparseToTensorsMap", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AddSparseToTensorsMap", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_deserialize_many_sparse_outputs = ["sparse_indices", "sparse_values",
                                   "sparse_shape"]
_DeserializeManySparseOutput = _collections.namedtuple(
    "DeserializeManySparse", _deserialize_many_sparse_outputs)


def deserialize_many_sparse(serialized_sparse, dtype, name=None):
  r"""Deserialize and concatenate `SparseTensors` from a serialized minibatch.

  The input `serialized_sparse` must be a string matrix of shape `[N x 3]` where
  `N` is the minibatch size and the rows correspond to packed outputs of
  `SerializeSparse`.  The ranks of the original `SparseTensor` objects
  must all match.  When the final `SparseTensor` is created, it has rank one
  higher than the ranks of the incoming `SparseTensor` objects
  (they have been concatenated along a new row dimension).

  The output `SparseTensor` object's shape values for all dimensions but the
  first are the max across the input `SparseTensor` objects' shape values
  for the corresponding dimensions.  Its first shape value is `N`, the minibatch
  size.

  The input `SparseTensor` objects' indices are assumed ordered in
  standard lexicographic order.  If this is not the case, after this
  step run `SparseReorder` to restore index ordering.

  For example, if the serialized input is a `[2 x 3]` matrix representing two
  original `SparseTensor` objects:

      index = [ 0]
              [10]
              [20]
      values = [1, 2, 3]
      shape = [50]

  and

      index = [ 2]
              [10]
      values = [4, 5]
      shape = [30]

  then the final deserialized `SparseTensor` will be:

      index = [0  0]
              [0 10]
              [0 20]
              [1  2]
              [1 10]
      values = [1, 2, 3, 4, 5]
      shape = [2 50]

  Args:
    serialized_sparse: A `Tensor` of type `string`.
      2-D, The `N` serialized `SparseTensor` objects.
      Must have 3 columns.
    dtype: A `tf.DType`. The `dtype` of the serialized `SparseTensor` objects.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sparse_indices, sparse_values, sparse_shape).

    sparse_indices: A `Tensor` of type `int64`.
    sparse_values: A `Tensor` of type `dtype`.
    sparse_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DeserializeManySparse", name, _ctx._post_execution_callbacks,
        serialized_sparse, "dtype", dtype)
      _result = _DeserializeManySparseOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return deserialize_many_sparse_eager_fallback(
            serialized_sparse, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DeserializeManySparse", serialized_sparse=serialized_sparse,
                                 dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "DeserializeManySparse", _inputs_flat, _attrs, _result, name)
  _result = _DeserializeManySparseOutput._make(_result)
  return _result



def deserialize_many_sparse_eager_fallback(serialized_sparse, dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function deserialize_many_sparse
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  serialized_sparse = _ops.convert_to_tensor(serialized_sparse, _dtypes.string)
  _inputs_flat = [serialized_sparse]
  _attrs = ("dtype", dtype)
  _result = _execute.execute(b"DeserializeManySparse", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DeserializeManySparse", _inputs_flat, _attrs, _result, name)
  _result = _DeserializeManySparseOutput._make(_result)
  return _result


_deserialize_sparse_outputs = ["sparse_indices", "sparse_values",
                              "sparse_shape"]
_DeserializeSparseOutput = _collections.namedtuple(
    "DeserializeSparse", _deserialize_sparse_outputs)


def deserialize_sparse(serialized_sparse, dtype, name=None):
  r"""Deserialize `SparseTensor` objects.

  The input `serialized_sparse` must have the shape `[?, ?, ..., ?, 3]` where
  the last dimension stores serialized `SparseTensor` objects and the other N
  dimensions (N >= 0) correspond to a batch. The ranks of the original
  `SparseTensor` objects must all match. When the final `SparseTensor` is
  created, its rank is the rank of the incoming `SparseTensor` objects plus N;
  the sparse tensors have been concatenated along new dimensions, one for each
  batch.

  The output `SparseTensor` object's shape values for the original dimensions
  are the max across the input `SparseTensor` objects' shape values for the
  corresponding dimensions. The new dimensions match the size of the batch.

  The input `SparseTensor` objects' indices are assumed ordered in
  standard lexicographic order.  If this is not the case, after this
  step run `SparseReorder` to restore index ordering.

  For example, if the serialized input is a `[2 x 3]` matrix representing two
  original `SparseTensor` objects:

      index = [ 0]
              [10]
              [20]
      values = [1, 2, 3]
      shape = [50]

  and

      index = [ 2]
              [10]
      values = [4, 5]
      shape = [30]

  then the final deserialized `SparseTensor` will be:

      index = [0  0]
              [0 10]
              [0 20]
              [1  2]
              [1 10]
      values = [1, 2, 3, 4, 5]
      shape = [2 50]

  Args:
    serialized_sparse: A `Tensor`. Must be one of the following types: `string`, `variant`.
      The serialized `SparseTensor` objects. The last dimension
      must have 3 columns.
    dtype: A `tf.DType`. The `dtype` of the serialized `SparseTensor` objects.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sparse_indices, sparse_values, sparse_shape).

    sparse_indices: A `Tensor` of type `int64`.
    sparse_values: A `Tensor` of type `dtype`.
    sparse_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DeserializeSparse", name, _ctx._post_execution_callbacks,
        serialized_sparse, "dtype", dtype)
      _result = _DeserializeSparseOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return deserialize_sparse_eager_fallback(
            serialized_sparse, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DeserializeSparse", serialized_sparse=serialized_sparse, dtype=dtype,
                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "Tserialized",
            _op.get_attr("Tserialized"))
  _execute.record_gradient(
      "DeserializeSparse", _inputs_flat, _attrs, _result, name)
  _result = _DeserializeSparseOutput._make(_result)
  return _result



def deserialize_sparse_eager_fallback(serialized_sparse, dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function deserialize_sparse
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  _attr_Tserialized, (serialized_sparse,) = _execute.args_to_matching_eager([serialized_sparse], _ctx, _dtypes.string)
  _inputs_flat = [serialized_sparse]
  _attrs = ("dtype", dtype, "Tserialized", _attr_Tserialized)
  _result = _execute.execute(b"DeserializeSparse", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DeserializeSparse", _inputs_flat, _attrs, _result, name)
  _result = _DeserializeSparseOutput._make(_result)
  return _result


def serialize_many_sparse(sparse_indices, sparse_values, sparse_shape, out_type=_dtypes.string, name=None):
  r"""Serialize an `N`-minibatch `SparseTensor` into an `[N, 3]` `Tensor` object.

  The `SparseTensor` must have rank `R` greater than 1, and the first dimension
  is treated as the minibatch dimension.  Elements of the `SparseTensor`
  must be sorted in increasing order of this first dimension.  The serialized
  `SparseTensor` objects going into each row of `serialized_sparse` will have
  rank `R-1`.

  The minibatch size `N` is extracted from `sparse_shape[0]`.

  Args:
    sparse_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the minibatch `SparseTensor`.
    sparse_values: A `Tensor`.
      1-D.  The `values` of the minibatch `SparseTensor`.
    sparse_shape: A `Tensor` of type `int64`.
      1-D.  The `shape` of the minibatch `SparseTensor`.
    out_type: An optional `tf.DType` from: `tf.string, tf.variant`. Defaults to `tf.string`.
      The `dtype` to use for serialization; the supported types are `string`
      (default) and `variant`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `out_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SerializeManySparse", name, _ctx._post_execution_callbacks,
        sparse_indices, sparse_values, sparse_shape, "out_type", out_type)
      return _result
    except _core._FallbackException:
      try:
        return serialize_many_sparse_eager_fallback(
            sparse_indices, sparse_values, sparse_shape, out_type=out_type,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_type is None:
    out_type = _dtypes.string
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SerializeManySparse", sparse_indices=sparse_indices,
                               sparse_values=sparse_values,
                               sparse_shape=sparse_shape, out_type=out_type,
                               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "out_type", _op.get_attr("out_type"))
  _execute.record_gradient(
      "SerializeManySparse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def serialize_many_sparse_eager_fallback(sparse_indices, sparse_values, sparse_shape, out_type=_dtypes.string, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function serialize_many_sparse
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.string
  out_type = _execute.make_type(out_type, "out_type")
  _attr_T, (sparse_values,) = _execute.args_to_matching_eager([sparse_values], _ctx)
  sparse_indices = _ops.convert_to_tensor(sparse_indices, _dtypes.int64)
  sparse_shape = _ops.convert_to_tensor(sparse_shape, _dtypes.int64)
  _inputs_flat = [sparse_indices, sparse_values, sparse_shape]
  _attrs = ("T", _attr_T, "out_type", out_type)
  _result = _execute.execute(b"SerializeManySparse", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SerializeManySparse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def serialize_sparse(sparse_indices, sparse_values, sparse_shape, out_type=_dtypes.string, name=None):
  r"""Serialize a `SparseTensor` into a `[3]` `Tensor` object.

  Args:
    sparse_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the `SparseTensor`.
    sparse_values: A `Tensor`. 1-D.  The `values` of the `SparseTensor`.
    sparse_shape: A `Tensor` of type `int64`.
      1-D.  The `shape` of the `SparseTensor`.
    out_type: An optional `tf.DType` from: `tf.string, tf.variant`. Defaults to `tf.string`.
      The `dtype` to use for serialization; the supported types are `string`
      (default) and `variant`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `out_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SerializeSparse", name, _ctx._post_execution_callbacks,
        sparse_indices, sparse_values, sparse_shape, "out_type", out_type)
      return _result
    except _core._FallbackException:
      try:
        return serialize_sparse_eager_fallback(
            sparse_indices, sparse_values, sparse_shape, out_type=out_type,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_type is None:
    out_type = _dtypes.string
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SerializeSparse", sparse_indices=sparse_indices,
                           sparse_values=sparse_values,
                           sparse_shape=sparse_shape, out_type=out_type,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "out_type", _op.get_attr("out_type"))
  _execute.record_gradient(
      "SerializeSparse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def serialize_sparse_eager_fallback(sparse_indices, sparse_values, sparse_shape, out_type=_dtypes.string, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function serialize_sparse
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.string
  out_type = _execute.make_type(out_type, "out_type")
  _attr_T, (sparse_values,) = _execute.args_to_matching_eager([sparse_values], _ctx)
  sparse_indices = _ops.convert_to_tensor(sparse_indices, _dtypes.int64)
  sparse_shape = _ops.convert_to_tensor(sparse_shape, _dtypes.int64)
  _inputs_flat = [sparse_indices, sparse_values, sparse_shape]
  _attrs = ("T", _attr_T, "out_type", out_type)
  _result = _execute.execute(b"SerializeSparse", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SerializeSparse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_sparse_add_outputs = ["sum_indices", "sum_values", "sum_shape"]
_SparseAddOutput = _collections.namedtuple(
    "SparseAdd", _sparse_add_outputs)


def sparse_add(a_indices, a_values, a_shape, b_indices, b_values, b_shape, thresh, name=None):
  r"""Adds two `SparseTensor` objects to produce another `SparseTensor`.

  The input `SparseTensor` objects' indices are assumed ordered in standard
  lexicographic order.  If this is not the case, before this step run
  `SparseReorder` to restore index ordering.

  By default, if two values sum to zero at some index, the output `SparseTensor`
  would still include that particular location in its index, storing a zero in the
  corresponding value slot.  To override this, callers can specify `thresh`,
  indicating that if the sum has a magnitude strictly smaller than `thresh`, its
  corresponding value and index would then not be included.  In particular,
  `thresh == 0` (default) means everything is kept and actual thresholding happens
  only for a positive value.

  In the following shapes, `nnz` is the count after taking `thresh` into account.

  Args:
    a_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the first `SparseTensor`, size `[nnz, ndims]` Matrix.
    a_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D.  The `values` of the first `SparseTensor`, size `[nnz]` Vector.
    a_shape: A `Tensor` of type `int64`.
      1-D.  The `shape` of the first `SparseTensor`, size `[ndims]` Vector.
    b_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the second `SparseTensor`, size `[nnz, ndims]` Matrix.
    b_values: A `Tensor`. Must have the same type as `a_values`.
      1-D.  The `values` of the second `SparseTensor`, size `[nnz]` Vector.
    b_shape: A `Tensor` of type `int64`.
      1-D.  The `shape` of the second `SparseTensor`, size `[ndims]` Vector.
    thresh: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      0-D.  The magnitude threshold that determines if an output value/index
      pair takes space.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sum_indices, sum_values, sum_shape).

    sum_indices: A `Tensor` of type `int64`.
    sum_values: A `Tensor`. Has the same type as `a_values`.
    sum_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SparseAdd",
        name, _ctx._post_execution_callbacks, a_indices, a_values, a_shape,
        b_indices, b_values, b_shape, thresh)
      _result = _SparseAddOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_add_eager_fallback(
            a_indices, a_values, a_shape, b_indices, b_values, b_shape,
            thresh, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseAdd", a_indices=a_indices, a_values=a_values, a_shape=a_shape,
                     b_indices=b_indices, b_values=b_values, b_shape=b_shape,
                     thresh=thresh, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Treal", _op.get_attr("Treal"))
  _execute.record_gradient(
      "SparseAdd", _inputs_flat, _attrs, _result, name)
  _result = _SparseAddOutput._make(_result)
  return _result



def sparse_add_eager_fallback(a_indices, a_values, a_shape, b_indices, b_values, b_shape, thresh, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_add
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a_values, b_values], _ctx)
  (a_values, b_values) = _inputs_T
  _attr_Treal, (thresh,) = _execute.args_to_matching_eager([thresh], _ctx)
  a_indices = _ops.convert_to_tensor(a_indices, _dtypes.int64)
  a_shape = _ops.convert_to_tensor(a_shape, _dtypes.int64)
  b_indices = _ops.convert_to_tensor(b_indices, _dtypes.int64)
  b_shape = _ops.convert_to_tensor(b_shape, _dtypes.int64)
  _inputs_flat = [a_indices, a_values, a_shape, b_indices, b_values, b_shape, thresh]
  _attrs = ("T", _attr_T, "Treal", _attr_Treal)
  _result = _execute.execute(b"SparseAdd", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseAdd", _inputs_flat, _attrs, _result, name)
  _result = _SparseAddOutput._make(_result)
  return _result


_sparse_add_grad_outputs = ["a_val_grad", "b_val_grad"]
_SparseAddGradOutput = _collections.namedtuple(
    "SparseAddGrad", _sparse_add_grad_outputs)


def sparse_add_grad(backprop_val_grad, a_indices, b_indices, sum_indices, name=None):
  r"""The gradient operator for the SparseAdd op.

  The SparseAdd op calculates A + B, where A, B, and the sum are all represented
  as `SparseTensor` objects.  This op takes in the upstream gradient w.r.t.
  non-empty values of the sum, and outputs the gradients w.r.t. the non-empty
  values of A and B.

  Args:
    backprop_val_grad: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D with shape `[nnz(sum)]`.  The gradient with respect to
      the non-empty values of the sum.
    a_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the `SparseTensor` A, size `[nnz(A), ndims]`.
    b_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the `SparseTensor` B, size `[nnz(B), ndims]`.
    sum_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the sum `SparseTensor`, size
      `[nnz(sum), ndims]`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (a_val_grad, b_val_grad).

    a_val_grad: A `Tensor`. Has the same type as `backprop_val_grad`.
    b_val_grad: A `Tensor`. Has the same type as `backprop_val_grad`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseAddGrad", name, _ctx._post_execution_callbacks,
        backprop_val_grad, a_indices, b_indices, sum_indices)
      _result = _SparseAddGradOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_add_grad_eager_fallback(
            backprop_val_grad, a_indices, b_indices, sum_indices, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseAddGrad", backprop_val_grad=backprop_val_grad,
                         a_indices=a_indices, b_indices=b_indices,
                         sum_indices=sum_indices, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseAddGrad", _inputs_flat, _attrs, _result, name)
  _result = _SparseAddGradOutput._make(_result)
  return _result



def sparse_add_grad_eager_fallback(backprop_val_grad, a_indices, b_indices, sum_indices, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_add_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (backprop_val_grad,) = _execute.args_to_matching_eager([backprop_val_grad], _ctx)
  a_indices = _ops.convert_to_tensor(a_indices, _dtypes.int64)
  b_indices = _ops.convert_to_tensor(b_indices, _dtypes.int64)
  sum_indices = _ops.convert_to_tensor(sum_indices, _dtypes.int64)
  _inputs_flat = [backprop_val_grad, a_indices, b_indices, sum_indices]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseAddGrad", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseAddGrad", _inputs_flat, _attrs, _result, name)
  _result = _SparseAddGradOutput._make(_result)
  return _result


_sparse_concat_outputs = ["output_indices", "output_values", "output_shape"]
_SparseConcatOutput = _collections.namedtuple(
    "SparseConcat", _sparse_concat_outputs)


def sparse_concat(indices, values, shapes, concat_dim, name=None):
  r"""Concatenates a list of `SparseTensor` along the specified dimension.

  Concatenation is with respect to the dense versions of these sparse tensors.
  It is assumed that each input is a `SparseTensor` whose elements are ordered
  along increasing dimension number.

  All inputs' shapes must match, except for the concat dimension.  The
  `indices`, `values`, and `shapes` lists must have the same length.

  The output shape is identical to the inputs', except along the concat
  dimension, where it is the sum of the inputs' sizes along that dimension.

  The output elements will be resorted to preserve the sort order along
  increasing dimension number.

  This op runs in `O(M log M)` time, where `M` is the total number of non-empty
  values across all inputs. This is due to the need for an internal sort in
  order to concatenate efficiently across an arbitrary dimension.

  For example, if `concat_dim = 1` and the inputs are

      sp_inputs[0]: shape = [2, 3]
      [0, 2]: "a"
      [1, 0]: "b"
      [1, 1]: "c"

      sp_inputs[1]: shape = [2, 4]
      [0, 1]: "d"
      [0, 2]: "e"

  then the output will be

      shape = [2, 7]
      [0, 2]: "a"
      [0, 4]: "d"
      [0, 5]: "e"
      [1, 0]: "b"
      [1, 1]: "c"

  Graphically this is equivalent to doing

      [    a] concat [  d e  ] = [    a   d e  ]
      [b c  ]        [       ]   [b c          ]

  Args:
    indices: A list of at least 2 `Tensor` objects with type `int64`.
      2-D.  Indices of each input `SparseTensor`.
    values: A list with the same length as `indices` of `Tensor` objects with the same type.
      1-D.  Non-empty values of each `SparseTensor`.
    shapes: A list with the same length as `indices` of `Tensor` objects with type `int64`.
      1-D.  Shapes of each `SparseTensor`.
    concat_dim: An `int`.
      Dimension to concatenate along. Must be in range [-rank, rank),
      where rank is the number of dimensions in each input `SparseTensor`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values, output_shape).

    output_indices: A `Tensor` of type `int64`.
    output_values: A `Tensor`. Has the same type as `values`.
    output_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SparseConcat",
        name, _ctx._post_execution_callbacks, indices, values, shapes,
        "concat_dim", concat_dim)
      _result = _SparseConcatOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_concat_eager_fallback(
            indices, values, shapes, concat_dim=concat_dim, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'sparse_concat' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'sparse_concat' Op, not %r." % values)
  if len(values) != _attr_N:
    raise ValueError(
        "List argument 'values' to 'sparse_concat' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(values), _attr_N))
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'sparse_concat' Op, not %r." % shapes)
  if len(shapes) != _attr_N:
    raise ValueError(
        "List argument 'shapes' to 'sparse_concat' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(shapes), _attr_N))
  concat_dim = _execute.make_int(concat_dim, "concat_dim")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseConcat", indices=indices, values=values, shapes=shapes,
                        concat_dim=concat_dim, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("concat_dim", _op.get_attr("concat_dim"), "N", _op.get_attr("N"),
            "T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseConcat", _inputs_flat, _attrs, _result, name)
  _result = _SparseConcatOutput._make(_result)
  return _result



def sparse_concat_eager_fallback(indices, values, shapes, concat_dim, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_concat
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'sparse_concat' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'sparse_concat' Op, not %r." % values)
  if len(values) != _attr_N:
    raise ValueError(
        "List argument 'values' to 'sparse_concat' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(values), _attr_N))
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'sparse_concat' Op, not %r." % shapes)
  if len(shapes) != _attr_N:
    raise ValueError(
        "List argument 'shapes' to 'sparse_concat' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(shapes), _attr_N))
  concat_dim = _execute.make_int(concat_dim, "concat_dim")
  _attr_T, values = _execute.args_to_matching_eager(list(values), _ctx)
  indices = _ops.convert_n_to_tensor(indices, _dtypes.int64)
  shapes = _ops.convert_n_to_tensor(shapes, _dtypes.int64)
  _inputs_flat = list(indices) + list(values) + list(shapes)
  _attrs = ("concat_dim", concat_dim, "N", _attr_N, "T", _attr_T)
  _result = _execute.execute(b"SparseConcat", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseConcat", _inputs_flat, _attrs, _result, name)
  _result = _SparseConcatOutput._make(_result)
  return _result


_sparse_cross_outputs = ["output_indices", "output_values", "output_shape"]
_SparseCrossOutput = _collections.namedtuple(
    "SparseCross", _sparse_cross_outputs)


def sparse_cross(indices, values, shapes, dense_inputs, hashed_output, num_buckets, hash_key, out_type, internal_type, name=None):
  r"""Generates sparse cross from a list of sparse and dense tensors.

  The op takes two lists, one of 2D `SparseTensor` and one of 2D `Tensor`, each
  representing features of one feature column. It outputs a 2D `SparseTensor` with
  the batchwise crosses of these features.

  For example, if the inputs are

      inputs[0]: SparseTensor with shape = [2, 2]
      [0, 0]: "a"
      [1, 0]: "b"
      [1, 1]: "c"

      inputs[1]: SparseTensor with shape = [2, 1]
      [0, 0]: "d"
      [1, 0]: "e"

      inputs[2]: Tensor [["f"], ["g"]]

  then the output will be

      shape = [2, 2]
      [0, 0]: "a_X_d_X_f"
      [1, 0]: "b_X_e_X_g"
      [1, 1]: "c_X_e_X_g"

  if hashed_output=true then the output will be

      shape = [2, 2]
      [0, 0]: FingerprintCat64(
                  Fingerprint64("f"), FingerprintCat64(
                      Fingerprint64("d"), Fingerprint64("a")))
      [1, 0]: FingerprintCat64(
                  Fingerprint64("g"), FingerprintCat64(
                      Fingerprint64("e"), Fingerprint64("b")))
      [1, 1]: FingerprintCat64(
                  Fingerprint64("g"), FingerprintCat64(
                      Fingerprint64("e"), Fingerprint64("c")))

  Args:
    indices: A list of `Tensor` objects with type `int64`.
      2-D.  Indices of each input `SparseTensor`.
    values: A list of `Tensor` objects with types from: `int64`, `string`.
      1-D.   values of each `SparseTensor`.
    shapes: A list with the same length as `indices` of `Tensor` objects with type `int64`.
      1-D.   Shapes of each `SparseTensor`.
    dense_inputs: A list of `Tensor` objects with types from: `int64`, `string`.
      2-D.    Columns represented by dense `Tensor`.
    hashed_output: A `bool`.
      If true, returns the hash of the cross instead of the string.
      This will allow us avoiding string manipulations.
    num_buckets: An `int` that is `>= 0`. It is used if hashed_output is true.
      output = hashed_value%num_buckets if num_buckets > 0 else hashed_value.
    hash_key: An `int`.
      Specify the hash_key that will be used by the `FingerprintCat64`
      function to combine the crosses fingerprints.
    out_type: A `tf.DType` from: `tf.int64, tf.string`.
    internal_type: A `tf.DType` from: `tf.int64, tf.string`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values, output_shape).

    output_indices: A `Tensor` of type `int64`.
    output_values: A `Tensor` of type `out_type`.
    output_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SparseCross",
        name, _ctx._post_execution_callbacks, indices, values, shapes,
        dense_inputs, "hashed_output", hashed_output, "num_buckets",
        num_buckets, "hash_key", hash_key, "out_type", out_type,
        "internal_type", internal_type)
      _result = _SparseCrossOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_cross_eager_fallback(
            indices, values, shapes, dense_inputs,
            hashed_output=hashed_output, num_buckets=num_buckets,
            hash_key=hash_key, out_type=out_type, internal_type=internal_type,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'sparse_cross' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'sparse_cross' Op, not %r." % shapes)
  if len(shapes) != _attr_N:
    raise ValueError(
        "List argument 'shapes' to 'sparse_cross' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(shapes), _attr_N))
  hashed_output = _execute.make_bool(hashed_output, "hashed_output")
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  hash_key = _execute.make_int(hash_key, "hash_key")
  out_type = _execute.make_type(out_type, "out_type")
  internal_type = _execute.make_type(internal_type, "internal_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseCross", indices=indices, values=values, shapes=shapes,
                       dense_inputs=dense_inputs, hashed_output=hashed_output,
                       num_buckets=num_buckets, hash_key=hash_key,
                       out_type=out_type, internal_type=internal_type,
                       name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "hashed_output",
            _op.get_attr("hashed_output"), "num_buckets",
            _op.get_attr("num_buckets"), "hash_key", _op.get_attr("hash_key"),
            "sparse_types", _op.get_attr("sparse_types"), "dense_types",
            _op.get_attr("dense_types"), "out_type", _op.get_attr("out_type"),
            "internal_type", _op.get_attr("internal_type"))
  _execute.record_gradient(
      "SparseCross", _inputs_flat, _attrs, _result, name)
  _result = _SparseCrossOutput._make(_result)
  return _result



def sparse_cross_eager_fallback(indices, values, shapes, dense_inputs, hashed_output, num_buckets, hash_key, out_type, internal_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_cross
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'sparse_cross' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'sparse_cross' Op, not %r." % shapes)
  if len(shapes) != _attr_N:
    raise ValueError(
        "List argument 'shapes' to 'sparse_cross' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(shapes), _attr_N))
  hashed_output = _execute.make_bool(hashed_output, "hashed_output")
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  hash_key = _execute.make_int(hash_key, "hash_key")
  out_type = _execute.make_type(out_type, "out_type")
  internal_type = _execute.make_type(internal_type, "internal_type")
  _attr_sparse_types, values = _execute.convert_to_mixed_eager_tensors(values, _ctx)
  _attr_dense_types, dense_inputs = _execute.convert_to_mixed_eager_tensors(dense_inputs, _ctx)
  indices = _ops.convert_n_to_tensor(indices, _dtypes.int64)
  shapes = _ops.convert_n_to_tensor(shapes, _dtypes.int64)
  _inputs_flat = list(indices) + list(values) + list(shapes) + list(dense_inputs)
  _attrs = ("N", _attr_N, "hashed_output", hashed_output, "num_buckets",
  num_buckets, "hash_key", hash_key, "sparse_types", _attr_sparse_types,
  "dense_types", _attr_dense_types, "out_type", out_type, "internal_type",
  internal_type)
  _result = _execute.execute(b"SparseCross", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseCross", _inputs_flat, _attrs, _result, name)
  _result = _SparseCrossOutput._make(_result)
  return _result


def sparse_dense_cwise_add(sp_indices, sp_values, sp_shape, dense, name=None):
  r"""Adds up a SparseTensor and a dense Tensor, using these special rules:

  (1) Broadcasts the dense side to have the same shape as the sparse side, if
      eligible;
  (2) Then, only the dense values pointed to by the indices of the SparseTensor
      participate in the cwise addition.

  By these rules, the result is a logical SparseTensor with exactly the same
  indices and shape, but possibly with different non-zero values.  The output of
  this Op is the resultant non-zero values.

  Args:
    sp_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, possibly not in canonical ordering.
    sp_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D.  `N` non-empty values corresponding to `sp_indices`.
    sp_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    dense: A `Tensor`. Must have the same type as `sp_values`.
      `R`-D.  The dense Tensor operand.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `sp_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseDenseCwiseAdd", name, _ctx._post_execution_callbacks,
        sp_indices, sp_values, sp_shape, dense)
      return _result
    except _core._FallbackException:
      try:
        return sparse_dense_cwise_add_eager_fallback(
            sp_indices, sp_values, sp_shape, dense, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseDenseCwiseAdd", sp_indices=sp_indices, sp_values=sp_values,
                               sp_shape=sp_shape, dense=dense, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseDenseCwiseAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_dense_cwise_add_eager_fallback(sp_indices, sp_values, sp_shape, dense, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_dense_cwise_add
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([sp_values, dense], _ctx)
  (sp_values, dense) = _inputs_T
  sp_indices = _ops.convert_to_tensor(sp_indices, _dtypes.int64)
  sp_shape = _ops.convert_to_tensor(sp_shape, _dtypes.int64)
  _inputs_flat = [sp_indices, sp_values, sp_shape, dense]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseDenseCwiseAdd", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseDenseCwiseAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_dense_cwise_div(sp_indices, sp_values, sp_shape, dense, name=None):
  r"""Component-wise divides a SparseTensor by a dense Tensor.

  *Limitation*: this Op only broadcasts the dense side to the sparse side, but not
  the other direction.

  Args:
    sp_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, possibly not in canonical ordering.
    sp_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D.  `N` non-empty values corresponding to `sp_indices`.
    sp_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    dense: A `Tensor`. Must have the same type as `sp_values`.
      `R`-D.  The dense Tensor operand.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `sp_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseDenseCwiseDiv", name, _ctx._post_execution_callbacks,
        sp_indices, sp_values, sp_shape, dense)
      return _result
    except _core._FallbackException:
      try:
        return sparse_dense_cwise_div_eager_fallback(
            sp_indices, sp_values, sp_shape, dense, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseDenseCwiseDiv", sp_indices=sp_indices, sp_values=sp_values,
                               sp_shape=sp_shape, dense=dense, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseDenseCwiseDiv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_dense_cwise_div_eager_fallback(sp_indices, sp_values, sp_shape, dense, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_dense_cwise_div
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([sp_values, dense], _ctx)
  (sp_values, dense) = _inputs_T
  sp_indices = _ops.convert_to_tensor(sp_indices, _dtypes.int64)
  sp_shape = _ops.convert_to_tensor(sp_shape, _dtypes.int64)
  _inputs_flat = [sp_indices, sp_values, sp_shape, dense]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseDenseCwiseDiv", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseDenseCwiseDiv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_dense_cwise_mul(sp_indices, sp_values, sp_shape, dense, name=None):
  r"""Component-wise multiplies a SparseTensor by a dense Tensor.

  The output locations corresponding to the implicitly zero elements in the sparse
  tensor will be zero (i.e., will not take up storage space), regardless of the
  contents of the dense tensor (even if it's +/-INF and that INF*0 == NaN).

  *Limitation*: this Op only broadcasts the dense side to the sparse side, but not
  the other direction.

  Args:
    sp_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, possibly not in canonical ordering.
    sp_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D.  `N` non-empty values corresponding to `sp_indices`.
    sp_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    dense: A `Tensor`. Must have the same type as `sp_values`.
      `R`-D.  The dense Tensor operand.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `sp_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseDenseCwiseMul", name, _ctx._post_execution_callbacks,
        sp_indices, sp_values, sp_shape, dense)
      return _result
    except _core._FallbackException:
      try:
        return sparse_dense_cwise_mul_eager_fallback(
            sp_indices, sp_values, sp_shape, dense, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseDenseCwiseMul", sp_indices=sp_indices, sp_values=sp_values,
                               sp_shape=sp_shape, dense=dense, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseDenseCwiseMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_dense_cwise_mul_eager_fallback(sp_indices, sp_values, sp_shape, dense, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_dense_cwise_mul
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([sp_values, dense], _ctx)
  (sp_values, dense) = _inputs_T
  sp_indices = _ops.convert_to_tensor(sp_indices, _dtypes.int64)
  sp_shape = _ops.convert_to_tensor(sp_shape, _dtypes.int64)
  _inputs_flat = [sp_indices, sp_values, sp_shape, dense]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseDenseCwiseMul", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseDenseCwiseMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_sparse_fill_empty_rows_outputs = ["output_indices", "output_values",
                                  "empty_row_indicator", "reverse_index_map"]
_SparseFillEmptyRowsOutput = _collections.namedtuple(
    "SparseFillEmptyRows", _sparse_fill_empty_rows_outputs)


def sparse_fill_empty_rows(indices, values, dense_shape, default_value, name=None):
  r"""Fills empty rows in the input 2-D `SparseTensor` with a default value.

  The input `SparseTensor` is represented via the tuple of inputs
  (`indices`, `values`, `dense_shape`).  The output `SparseTensor` has the
  same `dense_shape` but with indices `output_indices` and values
  `output_values`.

  This op inserts a single entry for every row that doesn't have any values.
  The index is created as `[row, 0, ..., 0]` and the inserted value
  is `default_value`.

  For example, suppose `sp_input` has shape `[5, 6]` and non-empty values:

      [0, 1]: a
      [0, 3]: b
      [2, 0]: c
      [3, 1]: d

  Rows 1 and 4 are empty, so the output will be of shape `[5, 6]` with values:

      [0, 1]: a
      [0, 3]: b
      [1, 0]: default_value
      [2, 0]: c
      [3, 1]: d
      [4, 0]: default_value

  The output `SparseTensor` will be in row-major order and will have the
  same shape as the input.

  This op also returns an indicator vector shaped `[dense_shape[0]]` such that

      empty_row_indicator[i] = True iff row i was an empty row.

  And a reverse index map vector shaped `[indices.shape[0]]` that is used during
  backpropagation,

      reverse_index_map[j] = out_j s.t. indices[j, :] == output_indices[out_j, :]

  Args:
    indices: A `Tensor` of type `int64`.
      2-D. the indices of the sparse tensor.
    values: A `Tensor`. 1-D. the values of the sparse tensor.
    dense_shape: A `Tensor` of type `int64`.
      1-D. the shape of the sparse tensor.
    default_value: A `Tensor`. Must have the same type as `values`.
      0-D. default value to insert into location `[row, 0, ..., 0]`
        for rows missing from the input sparse tensor.
      output indices: 2-D. the indices of the filled sparse tensor.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values, empty_row_indicator, reverse_index_map).

    output_indices: A `Tensor` of type `int64`.
    output_values: A `Tensor`. Has the same type as `values`.
    empty_row_indicator: A `Tensor` of type `bool`.
    reverse_index_map: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseFillEmptyRows", name, _ctx._post_execution_callbacks, indices,
        values, dense_shape, default_value)
      _result = _SparseFillEmptyRowsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_fill_empty_rows_eager_fallback(
            indices, values, dense_shape, default_value, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseFillEmptyRows", indices=indices, values=values,
                               dense_shape=dense_shape,
                               default_value=default_value, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseFillEmptyRows", _inputs_flat, _attrs, _result, name)
  _result = _SparseFillEmptyRowsOutput._make(_result)
  return _result



def sparse_fill_empty_rows_eager_fallback(indices, values, dense_shape, default_value, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_fill_empty_rows
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([values, default_value], _ctx)
  (values, default_value) = _inputs_T
  indices = _ops.convert_to_tensor(indices, _dtypes.int64)
  dense_shape = _ops.convert_to_tensor(dense_shape, _dtypes.int64)
  _inputs_flat = [indices, values, dense_shape, default_value]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseFillEmptyRows", 4, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseFillEmptyRows", _inputs_flat, _attrs, _result, name)
  _result = _SparseFillEmptyRowsOutput._make(_result)
  return _result


_sparse_fill_empty_rows_grad_outputs = ["d_values", "d_default_value"]
_SparseFillEmptyRowsGradOutput = _collections.namedtuple(
    "SparseFillEmptyRowsGrad", _sparse_fill_empty_rows_grad_outputs)


def sparse_fill_empty_rows_grad(reverse_index_map, grad_values, name=None):
  r"""The gradient of SparseFillEmptyRows.

  Takes vectors reverse_index_map, shaped `[N]`, and grad_values,
  shaped `[N_full]`, where `N_full >= N` and copies data into either
  `d_values` or `d_default_value`.  Here `d_values` is shaped `[N]` and
  `d_default_value` is a scalar.

    d_values[j] = grad_values[reverse_index_map[j]]
    d_default_value = sum_{k : 0 .. N_full - 1} (
       grad_values[k] * 1{k not in reverse_index_map})

  Args:
    reverse_index_map: A `Tensor` of type `int64`.
      1-D.  The reverse index map from SparseFillEmptyRows.
    grad_values: A `Tensor`. 1-D.  The gradients from backprop.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (d_values, d_default_value).

    d_values: A `Tensor`. Has the same type as `grad_values`.
    d_default_value: A `Tensor`. Has the same type as `grad_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseFillEmptyRowsGrad", name, _ctx._post_execution_callbacks,
        reverse_index_map, grad_values)
      _result = _SparseFillEmptyRowsGradOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_fill_empty_rows_grad_eager_fallback(
            reverse_index_map, grad_values, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseFillEmptyRowsGrad", reverse_index_map=reverse_index_map,
                                   grad_values=grad_values, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseFillEmptyRowsGrad", _inputs_flat, _attrs, _result, name)
  _result = _SparseFillEmptyRowsGradOutput._make(_result)
  return _result



def sparse_fill_empty_rows_grad_eager_fallback(reverse_index_map, grad_values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_fill_empty_rows_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (grad_values,) = _execute.args_to_matching_eager([grad_values], _ctx)
  reverse_index_map = _ops.convert_to_tensor(reverse_index_map, _dtypes.int64)
  _inputs_flat = [reverse_index_map, grad_values]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseFillEmptyRowsGrad", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SparseFillEmptyRowsGrad", _inputs_flat, _attrs, _result, name)
  _result = _SparseFillEmptyRowsGradOutput._make(_result)
  return _result


def sparse_reduce_max(input_indices, input_values, input_shape, reduction_axes, keep_dims=False, name=None):
  r"""Computes the max of elements across dimensions of a SparseTensor.

  This Op takes a SparseTensor and is the sparse counterpart to
  `tf.reduce_max()`.  In particular, this Op also returns a dense `Tensor`
  instead of a sparse one.

  Reduces `sp_input` along the dimensions given in `reduction_axes`.  Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `reduction_axes`. If `keep_dims` is true, the reduced dimensions are retained
  with length 1.

  If `reduction_axes` has no entries, all dimensions are reduced, and a tensor
  with a single element is returned.  Additionally, the axes can be negative,
  which are interpreted according to the indexing rules in Python.

  Args:
    input_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, possibly not in canonical ordering.
    input_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      1-D.  `N` non-empty values corresponding to `input_indices`.
    input_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    reduction_axes: A `Tensor` of type `int32`.
      1-D.  Length-`K` vector containing the reduction axes.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseReduceMax", name, _ctx._post_execution_callbacks,
        input_indices, input_values, input_shape, reduction_axes, "keep_dims",
        keep_dims)
      return _result
    except _core._FallbackException:
      try:
        return sparse_reduce_max_eager_fallback(
            input_indices, input_values, input_shape, reduction_axes,
            keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseReduceMax", input_indices=input_indices,
                           input_values=input_values, input_shape=input_shape,
                           reduction_axes=reduction_axes, keep_dims=keep_dims,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseReduceMax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_reduce_max_eager_fallback(input_indices, input_values, input_shape, reduction_axes, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_reduce_max
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_T, (input_values,) = _execute.args_to_matching_eager([input_values], _ctx)
  input_indices = _ops.convert_to_tensor(input_indices, _dtypes.int64)
  input_shape = _ops.convert_to_tensor(input_shape, _dtypes.int64)
  reduction_axes = _ops.convert_to_tensor(reduction_axes, _dtypes.int32)
  _inputs_flat = [input_indices, input_values, input_shape, reduction_axes]
  _attrs = ("keep_dims", keep_dims, "T", _attr_T)
  _result = _execute.execute(b"SparseReduceMax", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseReduceMax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_sparse_reduce_max_sparse_outputs = ["output_indices", "output_values",
                                    "output_shape"]
_SparseReduceMaxSparseOutput = _collections.namedtuple(
    "SparseReduceMaxSparse", _sparse_reduce_max_sparse_outputs)


def sparse_reduce_max_sparse(input_indices, input_values, input_shape, reduction_axes, keep_dims=False, name=None):
  r"""Computes the max of elements across dimensions of a SparseTensor.

  This Op takes a SparseTensor and is the sparse counterpart to
  `tf.reduce_max()`.  In contrast to SparseReduceMax, this Op returns a
  SparseTensor.

  Reduces `sp_input` along the dimensions given in `reduction_axes`.  Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `reduction_axes`. If `keep_dims` is true, the reduced dimensions are retained
  with length 1.

  If `reduction_axes` has no entries, all dimensions are reduced, and a tensor
  with a single element is returned.  Additionally, the axes can be negative,
  which are interpreted according to the indexing rules in Python.

  Args:
    input_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, possibly not in canonical ordering.
    input_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      1-D.  `N` non-empty values corresponding to `input_indices`.
    input_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    reduction_axes: A `Tensor` of type `int32`.
      1-D.  Length-`K` vector containing the reduction axes.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values, output_shape).

    output_indices: A `Tensor` of type `int64`.
    output_values: A `Tensor`. Has the same type as `input_values`.
    output_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseReduceMaxSparse", name, _ctx._post_execution_callbacks,
        input_indices, input_values, input_shape, reduction_axes, "keep_dims",
        keep_dims)
      _result = _SparseReduceMaxSparseOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_reduce_max_sparse_eager_fallback(
            input_indices, input_values, input_shape, reduction_axes,
            keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseReduceMaxSparse", input_indices=input_indices,
                                 input_values=input_values,
                                 input_shape=input_shape,
                                 reduction_axes=reduction_axes,
                                 keep_dims=keep_dims, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseReduceMaxSparse", _inputs_flat, _attrs, _result, name)
  _result = _SparseReduceMaxSparseOutput._make(_result)
  return _result



def sparse_reduce_max_sparse_eager_fallback(input_indices, input_values, input_shape, reduction_axes, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_reduce_max_sparse
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_T, (input_values,) = _execute.args_to_matching_eager([input_values], _ctx)
  input_indices = _ops.convert_to_tensor(input_indices, _dtypes.int64)
  input_shape = _ops.convert_to_tensor(input_shape, _dtypes.int64)
  reduction_axes = _ops.convert_to_tensor(reduction_axes, _dtypes.int32)
  _inputs_flat = [input_indices, input_values, input_shape, reduction_axes]
  _attrs = ("keep_dims", keep_dims, "T", _attr_T)
  _result = _execute.execute(b"SparseReduceMaxSparse", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseReduceMaxSparse", _inputs_flat, _attrs, _result, name)
  _result = _SparseReduceMaxSparseOutput._make(_result)
  return _result


def sparse_reduce_sum(input_indices, input_values, input_shape, reduction_axes, keep_dims=False, name=None):
  r"""Computes the sum of elements across dimensions of a SparseTensor.

  This Op takes a SparseTensor and is the sparse counterpart to
  `tf.reduce_sum()`.  In particular, this Op also returns a dense `Tensor`
  instead of a sparse one.

  Reduces `sp_input` along the dimensions given in `reduction_axes`.  Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `reduction_axes`. If `keep_dims` is true, the reduced dimensions are retained
  with length 1.

  If `reduction_axes` has no entries, all dimensions are reduced, and a tensor
  with a single element is returned.  Additionally, the axes can be negative,
  which are interpreted according to the indexing rules in Python.

  Args:
    input_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, possibly not in canonical ordering.
    input_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D.  `N` non-empty values corresponding to `input_indices`.
    input_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    reduction_axes: A `Tensor` of type `int32`.
      1-D.  Length-`K` vector containing the reduction axes.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseReduceSum", name, _ctx._post_execution_callbacks,
        input_indices, input_values, input_shape, reduction_axes, "keep_dims",
        keep_dims)
      return _result
    except _core._FallbackException:
      try:
        return sparse_reduce_sum_eager_fallback(
            input_indices, input_values, input_shape, reduction_axes,
            keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseReduceSum", input_indices=input_indices,
                           input_values=input_values, input_shape=input_shape,
                           reduction_axes=reduction_axes, keep_dims=keep_dims,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseReduceSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_reduce_sum_eager_fallback(input_indices, input_values, input_shape, reduction_axes, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_reduce_sum
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_T, (input_values,) = _execute.args_to_matching_eager([input_values], _ctx)
  input_indices = _ops.convert_to_tensor(input_indices, _dtypes.int64)
  input_shape = _ops.convert_to_tensor(input_shape, _dtypes.int64)
  reduction_axes = _ops.convert_to_tensor(reduction_axes, _dtypes.int32)
  _inputs_flat = [input_indices, input_values, input_shape, reduction_axes]
  _attrs = ("keep_dims", keep_dims, "T", _attr_T)
  _result = _execute.execute(b"SparseReduceSum", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseReduceSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_sparse_reduce_sum_sparse_outputs = ["output_indices", "output_values",
                                    "output_shape"]
_SparseReduceSumSparseOutput = _collections.namedtuple(
    "SparseReduceSumSparse", _sparse_reduce_sum_sparse_outputs)


def sparse_reduce_sum_sparse(input_indices, input_values, input_shape, reduction_axes, keep_dims=False, name=None):
  r"""Computes the sum of elements across dimensions of a SparseTensor.

  This Op takes a SparseTensor and is the sparse counterpart to
  `tf.reduce_sum()`.  In contrast to SparseReduceSum, this Op returns a
  SparseTensor.

  Reduces `sp_input` along the dimensions given in `reduction_axes`.  Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `reduction_axes`. If `keep_dims` is true, the reduced dimensions are retained
  with length 1.

  If `reduction_axes` has no entries, all dimensions are reduced, and a tensor
  with a single element is returned.  Additionally, the axes can be negative,
  which are interpreted according to the indexing rules in Python.

  Args:
    input_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, possibly not in canonical ordering.
    input_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D.  `N` non-empty values corresponding to `input_indices`.
    input_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    reduction_axes: A `Tensor` of type `int32`.
      1-D.  Length-`K` vector containing the reduction axes.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values, output_shape).

    output_indices: A `Tensor` of type `int64`.
    output_values: A `Tensor`. Has the same type as `input_values`.
    output_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseReduceSumSparse", name, _ctx._post_execution_callbacks,
        input_indices, input_values, input_shape, reduction_axes, "keep_dims",
        keep_dims)
      _result = _SparseReduceSumSparseOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_reduce_sum_sparse_eager_fallback(
            input_indices, input_values, input_shape, reduction_axes,
            keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseReduceSumSparse", input_indices=input_indices,
                                 input_values=input_values,
                                 input_shape=input_shape,
                                 reduction_axes=reduction_axes,
                                 keep_dims=keep_dims, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseReduceSumSparse", _inputs_flat, _attrs, _result, name)
  _result = _SparseReduceSumSparseOutput._make(_result)
  return _result



def sparse_reduce_sum_sparse_eager_fallback(input_indices, input_values, input_shape, reduction_axes, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_reduce_sum_sparse
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_T, (input_values,) = _execute.args_to_matching_eager([input_values], _ctx)
  input_indices = _ops.convert_to_tensor(input_indices, _dtypes.int64)
  input_shape = _ops.convert_to_tensor(input_shape, _dtypes.int64)
  reduction_axes = _ops.convert_to_tensor(reduction_axes, _dtypes.int32)
  _inputs_flat = [input_indices, input_values, input_shape, reduction_axes]
  _attrs = ("keep_dims", keep_dims, "T", _attr_T)
  _result = _execute.execute(b"SparseReduceSumSparse", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseReduceSumSparse", _inputs_flat, _attrs, _result, name)
  _result = _SparseReduceSumSparseOutput._make(_result)
  return _result


_sparse_reorder_outputs = ["output_indices", "output_values"]
_SparseReorderOutput = _collections.namedtuple(
    "SparseReorder", _sparse_reorder_outputs)


def sparse_reorder(input_indices, input_values, input_shape, name=None):
  r"""Reorders a SparseTensor into the canonical, row-major ordering.

  Note that by convention, all sparse ops preserve the canonical ordering along
  increasing dimension number. The only time ordering can be violated is during
  manual manipulation of the indices and values vectors to add entries.

  Reordering does not affect the shape of the SparseTensor.

  If the tensor has rank `R` and `N` non-empty values, `input_indices` has
  shape `[N, R]`, input_values has length `N`, and input_shape has length `R`.

  Args:
    input_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, possibly not in canonical ordering.
    input_values: A `Tensor`.
      1-D.  `N` non-empty values corresponding to `input_indices`.
    input_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values).

    output_indices: A `Tensor` of type `int64`.
    output_values: A `Tensor`. Has the same type as `input_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseReorder", name, _ctx._post_execution_callbacks, input_indices,
        input_values, input_shape)
      _result = _SparseReorderOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_reorder_eager_fallback(
            input_indices, input_values, input_shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseReorder", input_indices=input_indices,
                         input_values=input_values, input_shape=input_shape,
                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseReorder", _inputs_flat, _attrs, _result, name)
  _result = _SparseReorderOutput._make(_result)
  return _result



def sparse_reorder_eager_fallback(input_indices, input_values, input_shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_reorder
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input_values,) = _execute.args_to_matching_eager([input_values], _ctx)
  input_indices = _ops.convert_to_tensor(input_indices, _dtypes.int64)
  input_shape = _ops.convert_to_tensor(input_shape, _dtypes.int64)
  _inputs_flat = [input_indices, input_values, input_shape]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseReorder", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseReorder", _inputs_flat, _attrs, _result, name)
  _result = _SparseReorderOutput._make(_result)
  return _result


_sparse_reshape_outputs = ["output_indices", "output_shape"]
_SparseReshapeOutput = _collections.namedtuple(
    "SparseReshape", _sparse_reshape_outputs)


def sparse_reshape(input_indices, input_shape, new_shape, name=None):
  r"""Reshapes a SparseTensor to represent values in a new dense shape.

  This operation has the same semantics as reshape on the represented dense
  tensor.  The `input_indices` are recomputed based on the requested `new_shape`.

  If one component of `new_shape` is the special value -1, the size of that
  dimension is computed so that the total dense size remains constant.  At
  most one component of `new_shape` can be -1.  The number of dense elements
  implied by `new_shape` must be the same as the number of dense elements
  originally implied by `input_shape`.

  Reshaping does not affect the order of values in the SparseTensor.

  If the input tensor has rank `R_in` and `N` non-empty values, and `new_shape`
  has length `R_out`, then `input_indices` has shape `[N, R_in]`,
  `input_shape` has length `R_in`, `output_indices` has shape `[N, R_out]`, and
  `output_shape` has length `R_out`.

  Args:
    input_indices: A `Tensor` of type `int64`.
      2-D.  `N x R_in` matrix with the indices of non-empty values in a
      SparseTensor.
    input_shape: A `Tensor` of type `int64`.
      1-D.  `R_in` vector with the input SparseTensor's dense shape.
    new_shape: A `Tensor` of type `int64`.
      1-D.  `R_out` vector with the requested new dense shape.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_shape).

    output_indices: A `Tensor` of type `int64`.
    output_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseReshape", name, _ctx._post_execution_callbacks, input_indices,
        input_shape, new_shape)
      _result = _SparseReshapeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_reshape_eager_fallback(
            input_indices, input_shape, new_shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseReshape", input_indices=input_indices, input_shape=input_shape,
                         new_shape=new_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "SparseReshape", _inputs_flat, _attrs, _result, name)
  _result = _SparseReshapeOutput._make(_result)
  return _result



def sparse_reshape_eager_fallback(input_indices, input_shape, new_shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_reshape
  """
  _ctx = ctx if ctx else _context.context()
  input_indices = _ops.convert_to_tensor(input_indices, _dtypes.int64)
  input_shape = _ops.convert_to_tensor(input_shape, _dtypes.int64)
  new_shape = _ops.convert_to_tensor(new_shape, _dtypes.int64)
  _inputs_flat = [input_indices, input_shape, new_shape]
  _attrs = None
  _result = _execute.execute(b"SparseReshape", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseReshape", _inputs_flat, _attrs, _result, name)
  _result = _SparseReshapeOutput._make(_result)
  return _result


_sparse_slice_outputs = ["output_indices", "output_values", "output_shape"]
_SparseSliceOutput = _collections.namedtuple(
    "SparseSlice", _sparse_slice_outputs)


def sparse_slice(indices, values, shape, start, size, name=None):
  r"""Slice a `SparseTensor` based on the `start` and `size`.

  For example, if the input is

      input_tensor = shape = [2, 7]
      [    a   d e  ]
      [b c          ]

  Graphically the output tensors are:

      sparse_slice([0, 0], [2, 4]) = shape = [2, 4]
      [    a  ]
      [b c    ]

      sparse_slice([0, 4], [2, 3]) = shape = [2, 3]
      [ d e  ]
      [      ]

  Args:
    indices: A `Tensor` of type `int64`.
      2-D tensor represents the indices of the sparse tensor.
    values: A `Tensor`. 1-D tensor represents the values of the sparse tensor.
    shape: A `Tensor` of type `int64`.
      1-D. tensor represents the shape of the sparse tensor.
    start: A `Tensor` of type `int64`.
      1-D. tensor represents the start of the slice.
    size: A `Tensor` of type `int64`.
      1-D. tensor represents the size of the slice.
      output indices: A list of 1-D tensors represents the indices of the output
      sparse tensors.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values, output_shape).

    output_indices: A `Tensor` of type `int64`.
    output_values: A `Tensor`. Has the same type as `values`.
    output_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SparseSlice",
        name, _ctx._post_execution_callbacks, indices, values, shape, start,
        size)
      _result = _SparseSliceOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_slice_eager_fallback(
            indices, values, shape, start, size, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseSlice", indices=indices, values=values, shape=shape,
                       start=start, size=size, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseSlice", _inputs_flat, _attrs, _result, name)
  _result = _SparseSliceOutput._make(_result)
  return _result



def sparse_slice_eager_fallback(indices, values, shape, start, size, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_slice
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (values,) = _execute.args_to_matching_eager([values], _ctx)
  indices = _ops.convert_to_tensor(indices, _dtypes.int64)
  shape = _ops.convert_to_tensor(shape, _dtypes.int64)
  start = _ops.convert_to_tensor(start, _dtypes.int64)
  size = _ops.convert_to_tensor(size, _dtypes.int64)
  _inputs_flat = [indices, values, shape, start, size]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseSlice", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSlice", _inputs_flat, _attrs, _result, name)
  _result = _SparseSliceOutput._make(_result)
  return _result


def sparse_slice_grad(backprop_val_grad, input_indices, input_start, output_indices, name=None):
  r"""The gradient operator for the SparseSlice op.

  This op takes in the upstream gradient w.r.t. non-empty values of
  the sliced `SparseTensor`, and outputs the gradients w.r.t.
  the non-empty values of input `SparseTensor`.

  Args:
    backprop_val_grad: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D. The gradient with respect to
      the non-empty values of the sliced `SparseTensor`.
    input_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the input `SparseTensor`.
    input_start: A `Tensor` of type `int64`.
      1-D. tensor represents the start of the slice.
    output_indices: A `Tensor` of type `int64`.
      2-D.  The `indices` of the sliced `SparseTensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `backprop_val_grad`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSliceGrad", name, _ctx._post_execution_callbacks,
        backprop_val_grad, input_indices, input_start, output_indices)
      return _result
    except _core._FallbackException:
      try:
        return sparse_slice_grad_eager_fallback(
            backprop_val_grad, input_indices, input_start, output_indices,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseSliceGrad", backprop_val_grad=backprop_val_grad,
                           input_indices=input_indices,
                           input_start=input_start,
                           output_indices=output_indices, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseSliceGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_slice_grad_eager_fallback(backprop_val_grad, input_indices, input_start, output_indices, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_slice_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (backprop_val_grad,) = _execute.args_to_matching_eager([backprop_val_grad], _ctx)
  input_indices = _ops.convert_to_tensor(input_indices, _dtypes.int64)
  input_start = _ops.convert_to_tensor(input_start, _dtypes.int64)
  output_indices = _ops.convert_to_tensor(output_indices, _dtypes.int64)
  _inputs_flat = [backprop_val_grad, input_indices, input_start, output_indices]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseSliceGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSliceGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_softmax(sp_indices, sp_values, sp_shape, name=None):
  r"""Applies softmax to a batched N-D `SparseTensor`.

  The inputs represent an N-D SparseTensor  with logical shape `[..., B, C]`
  (where `N >= 2`), and with indices sorted in the canonical lexicographic order.

  This op is equivalent to applying the normal `tf.nn.softmax()` to each innermost
  logical submatrix with shape `[B, C]`, but with the catch that *the implicitly
  zero elements do not participate*.  Specifically, the algorithm is equivalent
  to the following:

    (1) Applies `tf.nn.softmax()` to a densified view of each innermost submatrix
        with shape `[B, C]`, along the size-C dimension;
    (2) Masks out the original implicitly-zero locations;
    (3) Renormalizes the remaining elements.

  Hence, the `SparseTensor` result has exactly the same non-zero indices and
  shape.

  Args:
    sp_indices: A `Tensor` of type `int64`.
      2-D.  `NNZ x R` matrix with the indices of non-empty values in a
      SparseTensor, in canonical ordering.
    sp_values: A `Tensor`. Must be one of the following types: `float32`, `float64`.
      1-D.  `NNZ` non-empty values corresponding to `sp_indices`.
    sp_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `sp_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSoftmax", name, _ctx._post_execution_callbacks, sp_indices,
        sp_values, sp_shape)
      return _result
    except _core._FallbackException:
      try:
        return sparse_softmax_eager_fallback(
            sp_indices, sp_values, sp_shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseSoftmax", sp_indices=sp_indices, sp_values=sp_values,
                         sp_shape=sp_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseSoftmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_softmax_eager_fallback(sp_indices, sp_values, sp_shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_softmax
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (sp_values,) = _execute.args_to_matching_eager([sp_values], _ctx)
  sp_indices = _ops.convert_to_tensor(sp_indices, _dtypes.int64)
  sp_shape = _ops.convert_to_tensor(sp_shape, _dtypes.int64)
  _inputs_flat = [sp_indices, sp_values, sp_shape]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseSoftmax", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSoftmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_sparse_sparse_maximum_outputs = ["output_indices", "output_values"]
_SparseSparseMaximumOutput = _collections.namedtuple(
    "SparseSparseMaximum", _sparse_sparse_maximum_outputs)


def sparse_sparse_maximum(a_indices, a_values, a_shape, b_indices, b_values, b_shape, name=None):
  r"""Returns the element-wise max of two SparseTensors.

  Assumes the two SparseTensors have the same shape, i.e., no broadcasting.

  Args:
    a_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, in the canonical lexicographic ordering.
    a_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      1-D.  `N` non-empty values corresponding to `a_indices`.
    a_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    b_indices: A `Tensor` of type `int64`.
      counterpart to `a_indices` for the other operand.
    b_values: A `Tensor`. Must have the same type as `a_values`.
      counterpart to `a_values` for the other operand; must be of the same dtype.
    b_shape: A `Tensor` of type `int64`.
      counterpart to `a_shape` for the other operand; the two shapes must be equal.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values).

    output_indices: A `Tensor` of type `int64`.
    output_values: A `Tensor`. Has the same type as `a_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSparseMaximum", name, _ctx._post_execution_callbacks,
        a_indices, a_values, a_shape, b_indices, b_values, b_shape)
      _result = _SparseSparseMaximumOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_sparse_maximum_eager_fallback(
            a_indices, a_values, a_shape, b_indices, b_values, b_shape,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseSparseMaximum", a_indices=a_indices, a_values=a_values,
                               a_shape=a_shape, b_indices=b_indices,
                               b_values=b_values, b_shape=b_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseSparseMaximum", _inputs_flat, _attrs, _result, name)
  _result = _SparseSparseMaximumOutput._make(_result)
  return _result



def sparse_sparse_maximum_eager_fallback(a_indices, a_values, a_shape, b_indices, b_values, b_shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_sparse_maximum
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a_values, b_values], _ctx)
  (a_values, b_values) = _inputs_T
  a_indices = _ops.convert_to_tensor(a_indices, _dtypes.int64)
  a_shape = _ops.convert_to_tensor(a_shape, _dtypes.int64)
  b_indices = _ops.convert_to_tensor(b_indices, _dtypes.int64)
  b_shape = _ops.convert_to_tensor(b_shape, _dtypes.int64)
  _inputs_flat = [a_indices, a_values, a_shape, b_indices, b_values, b_shape]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseSparseMaximum", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSparseMaximum", _inputs_flat, _attrs, _result, name)
  _result = _SparseSparseMaximumOutput._make(_result)
  return _result


_sparse_sparse_minimum_outputs = ["output_indices", "output_values"]
_SparseSparseMinimumOutput = _collections.namedtuple(
    "SparseSparseMinimum", _sparse_sparse_minimum_outputs)


def sparse_sparse_minimum(a_indices, a_values, a_shape, b_indices, b_values, b_shape, name=None):
  r"""Returns the element-wise min of two SparseTensors.

  Assumes the two SparseTensors have the same shape, i.e., no broadcasting.

  Args:
    a_indices: A `Tensor` of type `int64`.
      2-D.  `N x R` matrix with the indices of non-empty values in a
      SparseTensor, in the canonical lexicographic ordering.
    a_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D.  `N` non-empty values corresponding to `a_indices`.
    a_shape: A `Tensor` of type `int64`.
      1-D.  Shape of the input SparseTensor.
    b_indices: A `Tensor` of type `int64`.
      counterpart to `a_indices` for the other operand.
    b_values: A `Tensor`. Must have the same type as `a_values`.
      counterpart to `a_values` for the other operand; must be of the same dtype.
    b_shape: A `Tensor` of type `int64`.
      counterpart to `a_shape` for the other operand; the two shapes must be equal.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values).

    output_indices: A `Tensor` of type `int64`.
    output_values: A `Tensor`. Has the same type as `a_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSparseMinimum", name, _ctx._post_execution_callbacks,
        a_indices, a_values, a_shape, b_indices, b_values, b_shape)
      _result = _SparseSparseMinimumOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_sparse_minimum_eager_fallback(
            a_indices, a_values, a_shape, b_indices, b_values, b_shape,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseSparseMinimum", a_indices=a_indices, a_values=a_values,
                               a_shape=a_shape, b_indices=b_indices,
                               b_values=b_values, b_shape=b_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseSparseMinimum", _inputs_flat, _attrs, _result, name)
  _result = _SparseSparseMinimumOutput._make(_result)
  return _result



def sparse_sparse_minimum_eager_fallback(a_indices, a_values, a_shape, b_indices, b_values, b_shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_sparse_minimum
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a_values, b_values], _ctx)
  (a_values, b_values) = _inputs_T
  a_indices = _ops.convert_to_tensor(a_indices, _dtypes.int64)
  a_shape = _ops.convert_to_tensor(a_shape, _dtypes.int64)
  b_indices = _ops.convert_to_tensor(b_indices, _dtypes.int64)
  b_shape = _ops.convert_to_tensor(b_shape, _dtypes.int64)
  _inputs_flat = [a_indices, a_values, a_shape, b_indices, b_values, b_shape]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SparseSparseMinimum", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSparseMinimum", _inputs_flat, _attrs, _result, name)
  _result = _SparseSparseMinimumOutput._make(_result)
  return _result


_sparse_split_outputs = ["output_indices", "output_values", "output_shape"]
_SparseSplitOutput = _collections.namedtuple(
    "SparseSplit", _sparse_split_outputs)


def sparse_split(split_dim, indices, values, shape, num_split, name=None):
  r"""Split a `SparseTensor` into `num_split` tensors along one dimension.

  If the `shape[split_dim]` is not an integer multiple of `num_split`. Slices
  `[0 : shape[split_dim] % num_split]` gets one extra dimension.
  For example, if `split_dim = 1` and `num_split = 2` and the input is

      input_tensor = shape = [2, 7]
      [    a   d e  ]
      [b c          ]

  Graphically the output tensors are:

      output_tensor[0] = shape = [2, 4]
      [    a  ]
      [b c    ]

      output_tensor[1] = shape = [2, 3]
      [ d e  ]
      [      ]

  Args:
    split_dim: A `Tensor` of type `int64`.
      0-D.  The dimension along which to split.  Must be in the range
      `[0, rank(shape))`.
    indices: A `Tensor` of type `int64`.
      2-D tensor represents the indices of the sparse tensor.
    values: A `Tensor`. 1-D tensor represents the values of the sparse tensor.
    shape: A `Tensor` of type `int64`.
      1-D. tensor represents the shape of the sparse tensor.
      output indices: A list of 1-D tensors represents the indices of the output
      sparse tensors.
    num_split: An `int` that is `>= 1`. The number of ways to split.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values, output_shape).

    output_indices: A list of `num_split` `Tensor` objects with type `int64`.
    output_values: A list of `num_split` `Tensor` objects with the same type as `values`.
    output_shape: A list of `num_split` `Tensor` objects with type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SparseSplit",
        name, _ctx._post_execution_callbacks, split_dim, indices, values,
        shape, "num_split", num_split)
      _result = _SparseSplitOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_split_eager_fallback(
            split_dim, indices, values, shape, num_split=num_split, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_split = _execute.make_int(num_split, "num_split")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseSplit", split_dim=split_dim, indices=indices, values=values,
                       shape=shape, num_split=num_split, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_split", _op.get_attr("num_split"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "SparseSplit", _inputs_flat, _attrs, _result, name)
  _result = [_result[:num_split]] + _result[num_split:]
  _result = _result[:1] + [_result[1:1 + num_split]] + _result[1 + num_split:]
  _result = _result[:2] + [_result[2:]]
  _result = _SparseSplitOutput._make(_result)
  return _result



def sparse_split_eager_fallback(split_dim, indices, values, shape, num_split, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_split
  """
  _ctx = ctx if ctx else _context.context()
  num_split = _execute.make_int(num_split, "num_split")
  _attr_T, (values,) = _execute.args_to_matching_eager([values], _ctx)
  split_dim = _ops.convert_to_tensor(split_dim, _dtypes.int64)
  indices = _ops.convert_to_tensor(indices, _dtypes.int64)
  shape = _ops.convert_to_tensor(shape, _dtypes.int64)
  _inputs_flat = [split_dim, indices, values, shape]
  _attrs = ("num_split", num_split, "T", _attr_T)
  _result = _execute.execute(b"SparseSplit", num_split + num_split +
                             num_split, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSplit", _inputs_flat, _attrs, _result, name)
  _result = [_result[:num_split]] + _result[num_split:]
  _result = _result[:1] + [_result[1:1 + num_split]] + _result[1 + num_split:]
  _result = _result[:2] + [_result[2:]]
  _result = _SparseSplitOutput._make(_result)
  return _result


def sparse_tensor_dense_add(a_indices, a_values, a_shape, b, name=None):
  r"""Adds up a `SparseTensor` and a dense `Tensor`, producing a dense `Tensor`.

  This Op does not require `a_indices` be sorted in standard lexicographic order.

  Args:
    a_indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2-D.  The `indices` of the `SparseTensor`, with shape `[nnz, ndims]`.
    a_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      1-D.  The `values` of the `SparseTensor`, with shape `[nnz]`.
    a_shape: A `Tensor`. Must have the same type as `a_indices`.
      1-D.  The `shape` of the `SparseTensor`, with shape `[ndims]`.
    b: A `Tensor`. Must have the same type as `a_values`.
      `ndims`-D Tensor.  With shape `a_shape`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `a_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseTensorDenseAdd", name, _ctx._post_execution_callbacks,
        a_indices, a_values, a_shape, b)
      return _result
    except _core._FallbackException:
      try:
        return sparse_tensor_dense_add_eager_fallback(
            a_indices, a_values, a_shape, b, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseTensorDenseAdd", a_indices=a_indices, a_values=a_values,
                                a_shape=a_shape, b=b, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "SparseTensorDenseAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_tensor_dense_add_eager_fallback(a_indices, a_values, a_shape, b, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_tensor_dense_add
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a_values, b], _ctx)
  (a_values, b) = _inputs_T
  _attr_Tindices, _inputs_Tindices = _execute.args_to_matching_eager([a_indices, a_shape], _ctx)
  (a_indices, a_shape) = _inputs_Tindices
  _inputs_flat = [a_indices, a_values, a_shape, b]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"SparseTensorDenseAdd", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseTensorDenseAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_tensor_dense_mat_mul(a_indices, a_values, a_shape, b, adjoint_a=False, adjoint_b=False, name=None):
  r"""Multiply SparseTensor (of rank 2) "A" by dense matrix "B".

  No validity checking is performed on the indices of A.  However, the following
  input format is recommended for optimal behavior:

  if adjoint_a == false:
    A should be sorted in lexicographically increasing order.  Use SparseReorder
    if you're not sure.
  if adjoint_a == true:
    A should be sorted in order of increasing dimension 1 (i.e., "column major"
    order instead of "row major" order).

  Args:
    a_indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2-D.  The `indices` of the `SparseTensor`, size `[nnz, 2]` Matrix.
    a_values: A `Tensor`.
      1-D.  The `values` of the `SparseTensor`, size `[nnz]` Vector.
    a_shape: A `Tensor` of type `int64`.
      1-D.  The `shape` of the `SparseTensor`, size `[2]` Vector.
    b: A `Tensor`. Must have the same type as `a_values`.
      2-D.  A dense Matrix.
    adjoint_a: An optional `bool`. Defaults to `False`.
      Use the adjoint of A in the matrix multiply.  If A is complex, this
      is transpose(conj(A)).  Otherwise it's transpose(A).
    adjoint_b: An optional `bool`. Defaults to `False`.
      Use the adjoint of B in the matrix multiply.  If B is complex, this
      is transpose(conj(B)).  Otherwise it's transpose(B).
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `a_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseTensorDenseMatMul", name, _ctx._post_execution_callbacks,
        a_indices, a_values, a_shape, b, "adjoint_a", adjoint_a, "adjoint_b",
        adjoint_b)
      return _result
    except _core._FallbackException:
      try:
        return sparse_tensor_dense_mat_mul_eager_fallback(
            a_indices, a_values, a_shape, b, adjoint_a=adjoint_a,
            adjoint_b=adjoint_b, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if adjoint_a is None:
    adjoint_a = False
  adjoint_a = _execute.make_bool(adjoint_a, "adjoint_a")
  if adjoint_b is None:
    adjoint_b = False
  adjoint_b = _execute.make_bool(adjoint_b, "adjoint_b")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseTensorDenseMatMul", a_indices=a_indices, a_values=a_values,
                                   a_shape=a_shape, b=b, adjoint_a=adjoint_a,
                                   adjoint_b=adjoint_b, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "adjoint_a", _op.get_attr("adjoint_a"), "adjoint_b",
            _op.get_attr("adjoint_b"))
  _execute.record_gradient(
      "SparseTensorDenseMatMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_tensor_dense_mat_mul_eager_fallback(a_indices, a_values, a_shape, b, adjoint_a=False, adjoint_b=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_tensor_dense_mat_mul
  """
  _ctx = ctx if ctx else _context.context()
  if adjoint_a is None:
    adjoint_a = False
  adjoint_a = _execute.make_bool(adjoint_a, "adjoint_a")
  if adjoint_b is None:
    adjoint_b = False
  adjoint_b = _execute.make_bool(adjoint_b, "adjoint_b")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a_values, b], _ctx)
  (a_values, b) = _inputs_T
  _attr_Tindices, (a_indices,) = _execute.args_to_matching_eager([a_indices], _ctx, _dtypes.int64)
  a_shape = _ops.convert_to_tensor(a_shape, _dtypes.int64)
  _inputs_flat = [a_indices, a_values, a_shape, b]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "adjoint_a", adjoint_a,
  "adjoint_b", adjoint_b)
  _result = _execute.execute(b"SparseTensorDenseMatMul", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SparseTensorDenseMatMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_to_dense(sparse_indices, output_shape, sparse_values, default_value, validate_indices=True, name=None):
  r"""Converts a sparse representation into a dense tensor.

  Builds an array `dense` with shape `output_shape` such that

  ```
  # If sparse_indices is scalar
  dense[i] = (i == sparse_indices ? sparse_values : default_value)

  # If sparse_indices is a vector, then for each i
  dense[sparse_indices[i]] = sparse_values[i]

  # If sparse_indices is an n by d matrix, then for each i in [0, n)
  dense[sparse_indices[i][0], ..., sparse_indices[i][d-1]] = sparse_values[i]
  ```

  All other values in `dense` are set to `default_value`.  If `sparse_values` is a
  scalar, all sparse indices are set to this single value.

  Indices should be sorted in lexicographic order, and indices must not
  contain any repeats. If `validate_indices` is true, these properties
  are checked during execution.

  Args:
    sparse_indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      0-D, 1-D, or 2-D.  `sparse_indices[i]` contains the complete
      index where `sparse_values[i]` will be placed.
    output_shape: A `Tensor`. Must have the same type as `sparse_indices`.
      1-D.  Shape of the dense output tensor.
    sparse_values: A `Tensor`.
      1-D.  Values corresponding to each row of `sparse_indices`,
      or a scalar value to be used for all sparse indices.
    default_value: A `Tensor`. Must have the same type as `sparse_values`.
      Scalar value to set for indices not specified in
      `sparse_indices`.
    validate_indices: An optional `bool`. Defaults to `True`.
      If true, indices are checked to make sure they are sorted in
      lexicographic order and that there are no repeats.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `sparse_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseToDense", name, _ctx._post_execution_callbacks, sparse_indices,
        output_shape, sparse_values, default_value, "validate_indices",
        validate_indices)
      return _result
    except _core._FallbackException:
      try:
        return sparse_to_dense_eager_fallback(
            sparse_indices, output_shape, sparse_values, default_value,
            validate_indices=validate_indices, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseToDense", sparse_indices=sparse_indices,
                         output_shape=output_shape,
                         sparse_values=sparse_values,
                         default_value=default_value,
                         validate_indices=validate_indices, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("validate_indices", _op.get_attr("validate_indices"), "T",
            _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "SparseToDense", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_to_dense_eager_fallback(sparse_indices, output_shape, sparse_values, default_value, validate_indices=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_to_dense
  """
  _ctx = ctx if ctx else _context.context()
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([sparse_values, default_value], _ctx)
  (sparse_values, default_value) = _inputs_T
  _attr_Tindices, _inputs_Tindices = _execute.args_to_matching_eager([sparse_indices, output_shape], _ctx)
  (sparse_indices, output_shape) = _inputs_Tindices
  _inputs_flat = [sparse_indices, output_shape, sparse_values, default_value]
  _attrs = ("validate_indices", validate_indices, "T", _attr_T, "Tindices",
  _attr_Tindices)
  _result = _execute.execute(b"SparseToDense", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseToDense", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_take_many_sparse_from_tensors_map_outputs = ["sparse_indices",
                                             "sparse_values", "sparse_shape"]
_TakeManySparseFromTensorsMapOutput = _collections.namedtuple(
    "TakeManySparseFromTensorsMap",
    _take_many_sparse_from_tensors_map_outputs)


def take_many_sparse_from_tensors_map(sparse_handles, dtype, container="", shared_name="", name=None):
  r"""Read `SparseTensors` from a `SparseTensorsMap` and concatenate them.

  The input `sparse_handles` must be an `int64` matrix of shape `[N, 1]` where
  `N` is the minibatch size and the rows correspond to the output handles of
  `AddSparseToTensorsMap` or `AddManySparseToTensorsMap`.  The ranks of the
  original `SparseTensor` objects that went into the given input ops must all
  match.  When the final `SparseTensor` is created, it has rank one
  higher than the ranks of the incoming `SparseTensor` objects
  (they have been concatenated along a new row dimension on the left).

  The output `SparseTensor` object's shape values for all dimensions but the
  first are the max across the input `SparseTensor` objects' shape values
  for the corresponding dimensions.  Its first shape value is `N`, the minibatch
  size.

  The input `SparseTensor` objects' indices are assumed ordered in
  standard lexicographic order.  If this is not the case, after this
  step run `SparseReorder` to restore index ordering.

  For example, if the handles represent an input, which is a `[2, 3]` matrix
  representing two original `SparseTensor` objects:

  ```
      index = [ 0]
              [10]
              [20]
      values = [1, 2, 3]
      shape = [50]
  ```

  and

  ```
      index = [ 2]
              [10]
      values = [4, 5]
      shape = [30]
  ```

  then the final `SparseTensor` will be:

  ```
      index = [0  0]
              [0 10]
              [0 20]
              [1  2]
              [1 10]
      values = [1, 2, 3, 4, 5]
      shape = [2 50]
  ```

  Args:
    sparse_handles: A `Tensor` of type `int64`.
      1-D, The `N` serialized `SparseTensor` objects.
      Shape: `[N]`.
    dtype: A `tf.DType`.
      The `dtype` of the `SparseTensor` objects stored in the
      `SparseTensorsMap`.
    container: An optional `string`. Defaults to `""`.
      The container name for the `SparseTensorsMap` read by this op.
    shared_name: An optional `string`. Defaults to `""`.
      The shared name for the `SparseTensorsMap` read by this op.
      It should not be blank; rather the `shared_name` or unique Operation name
      of the Op that created the original `SparseTensorsMap` should be used.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sparse_indices, sparse_values, sparse_shape).

    sparse_indices: A `Tensor` of type `int64`.
    sparse_values: A `Tensor` of type `dtype`.
    sparse_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TakeManySparseFromTensorsMap", name, _ctx._post_execution_callbacks,
        sparse_handles, "dtype", dtype, "container", container, "shared_name",
        shared_name)
      _result = _TakeManySparseFromTensorsMapOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return take_many_sparse_from_tensors_map_eager_fallback(
            sparse_handles, dtype=dtype, container=container,
            shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TakeManySparseFromTensorsMap", sparse_handles=sparse_handles,
                                        dtype=dtype, container=container,
                                        shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "container",
            _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "TakeManySparseFromTensorsMap", _inputs_flat, _attrs, _result, name)
  _result = _TakeManySparseFromTensorsMapOutput._make(_result)
  return _result



def take_many_sparse_from_tensors_map_eager_fallback(sparse_handles, dtype, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function take_many_sparse_from_tensors_map
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  sparse_handles = _ops.convert_to_tensor(sparse_handles, _dtypes.int64)
  _inputs_flat = [sparse_handles]
  _attrs = ("dtype", dtype, "container", container, "shared_name",
  shared_name)
  _result = _execute.execute(b"TakeManySparseFromTensorsMap", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TakeManySparseFromTensorsMap", _inputs_flat, _attrs, _result, name)
  _result = _TakeManySparseFromTensorsMapOutput._make(_result)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "AddManySparseToTensorsMap"
#   input_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sparse_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sparse_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sparse_handles"
#     type: DT_INT64
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "container"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "AddSparseToTensorsMap"
#   input_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sparse_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sparse_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sparse_handle"
#     type: DT_INT64
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "container"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "DeserializeManySparse"
#   input_arg {
#     name: "serialized_sparse"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sparse_values"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "sparse_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
# }
# op {
#   name: "DeserializeSparse"
#   input_arg {
#     name: "serialized_sparse"
#     type_attr: "Tserialized"
#   }
#   output_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sparse_values"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "sparse_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "Tserialized"
#     type: "type"
#     default_value {
#       type: DT_STRING
#     }
#     allowed_values {
#       list {
#         type: DT_STRING
#         type: DT_VARIANT
#       }
#     }
#   }
# }
# op {
#   name: "SerializeManySparse"
#   input_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sparse_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sparse_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "serialized_sparse"
#     type_attr: "out_type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     default_value {
#       type: DT_STRING
#     }
#     allowed_values {
#       list {
#         type: DT_STRING
#         type: DT_VARIANT
#       }
#     }
#   }
# }
# op {
#   name: "SerializeSparse"
#   input_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sparse_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sparse_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "serialized_sparse"
#     type_attr: "out_type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     default_value {
#       type: DT_STRING
#     }
#     allowed_values {
#       list {
#         type: DT_STRING
#         type: DT_VARIANT
#       }
#     }
#   }
# }
# op {
#   name: "SparseAdd"
#   input_arg {
#     name: "a_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "a_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "a_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "b_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "b_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "thresh"
#     type_attr: "Treal"
#   }
#   output_arg {
#     name: "sum_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sum_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "sum_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Treal"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseAddGrad"
#   input_arg {
#     name: "backprop_val_grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "a_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "b_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sum_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "a_val_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "b_val_grad"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseConcat"
#   input_arg {
#     name: "indices"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   input_arg {
#     name: "shapes"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "concat_dim"
#     type: "int"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 2
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "SparseCross"
#   input_arg {
#     name: "indices"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   input_arg {
#     name: "values"
#     type_list_attr: "sparse_types"
#   }
#   input_arg {
#     name: "shapes"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   input_arg {
#     name: "dense_inputs"
#     type_list_attr: "dense_types"
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "output_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "hashed_output"
#     type: "bool"
#   }
#   attr {
#     name: "num_buckets"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "hash_key"
#     type: "int"
#   }
#   attr {
#     name: "sparse_types"
#     type: "list(type)"
#     has_minimum: true
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "dense_types"
#     type: "list(type)"
#     has_minimum: true
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "internal_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
# }
# op {
#   name: "SparseDenseCwiseAdd"
#   input_arg {
#     name: "sp_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sp_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sp_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "dense"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseDenseCwiseDiv"
#   input_arg {
#     name: "sp_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sp_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sp_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "dense"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseDenseCwiseMul"
#   input_arg {
#     name: "sp_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sp_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sp_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "dense"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseFillEmptyRows"
#   input_arg {
#     name: "indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dense_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "default_value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "empty_row_indicator"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "reverse_index_map"
#     type: DT_INT64
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "SparseFillEmptyRowsGrad"
#   input_arg {
#     name: "reverse_index_map"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "grad_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "d_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "d_default_value"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "SparseReduceMax"
#   input_arg {
#     name: "input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "input_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "reduction_axes"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "keep_dims"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseReduceMaxSparse"
#   input_arg {
#     name: "input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "input_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "reduction_axes"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "keep_dims"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseReduceSum"
#   input_arg {
#     name: "input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "input_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "reduction_axes"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "keep_dims"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseReduceSumSparse"
#   input_arg {
#     name: "input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "input_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "reduction_axes"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "keep_dims"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseReorder"
#   input_arg {
#     name: "input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "input_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "SparseReshape"
#   input_arg {
#     name: "input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "input_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "new_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_shape"
#     type: DT_INT64
#   }
# }
# op {
#   name: "SparseSlice"
#   input_arg {
#     name: "indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "start"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "SparseSliceGrad"
#   input_arg {
#     name: "backprop_val_grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "input_start"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "val_grad"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseSoftmax"
#   input_arg {
#     name: "sp_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sp_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sp_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "SparseSparseMaximum"
#   input_arg {
#     name: "a_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "a_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "a_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "b_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "b_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseSparseMinimum"
#   input_arg {
#     name: "a_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "a_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "a_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "b_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "b_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseSplit"
#   input_arg {
#     name: "split_dim"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#     number_attr: "num_split"
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "T"
#     number_attr: "num_split"
#   }
#   output_arg {
#     name: "output_shape"
#     type: DT_INT64
#     number_attr: "num_split"
#   }
#   attr {
#     name: "num_split"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "SparseTensorDenseAdd"
#   input_arg {
#     name: "a_indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "a_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "a_shape"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "SparseTensorDenseMatMul"
#   input_arg {
#     name: "a_indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "a_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "a_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "product"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     default_value {
#       type: DT_INT64
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "adjoint_a"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "adjoint_b"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SparseToDense"
#   input_arg {
#     name: "sparse_indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "output_shape"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "sparse_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "default_value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "dense"
#     type_attr: "T"
#   }
#   attr {
#     name: "validate_indices"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "TakeManySparseFromTensorsMap"
#   input_arg {
#     name: "sparse_handles"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sparse_values"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "sparse_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "container"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\253\001\n\031AddManySparseToTensorsMap\022\022\n\016sparse_indices\030\t\022\022\n\rsparse_values\"\001T\022\020\n\014sparse_shape\030\t\032\022\n\016sparse_handles\030\t\"\t\n\001T\022\004type\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\246\001\n\025AddSparseToTensorsMap\022\022\n\016sparse_indices\030\t\022\022\n\rsparse_values\"\001T\022\020\n\014sparse_shape\030\t\032\021\n\rsparse_handle\030\t\"\t\n\001T\022\004type\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n{\n\025DeserializeManySparse\022\025\n\021serialized_sparse\030\007\032\022\n\016sparse_indices\030\t\032\026\n\rsparse_values\"\005dtype\032\020\n\014sparse_shape\030\t\"\r\n\005dtype\022\004type\n\243\001\n\021DeserializeSparse\022 \n\021serialized_sparse\"\013Tserialized\032\022\n\016sparse_indices\030\t\032\026\n\rsparse_values\"\005dtype\032\020\n\014sparse_shape\030\t\"\r\n\005dtype\022\004type\"\037\n\013Tserialized\022\004type\032\0020\007:\006\n\0042\002\007\025\n\227\001\n\023SerializeManySparse\022\022\n\016sparse_indices\030\t\022\022\n\rsparse_values\"\001T\022\020\n\014sparse_shape\030\t\032\035\n\021serialized_sparse\"\010out_type\"\t\n\001T\022\004type\"\034\n\010out_type\022\004type\032\0020\007:\006\n\0042\002\007\025\n\223\001\n\017SerializeSparse\022\022\n\016sparse_indices\030\t\022\022\n\rsparse_values\"\001T\022\020\n\014sparse_shape\030\t\032\035\n\021serialized_sparse\"\010out_type\"\t\n\001T\022\004type\"\034\n\010out_type\022\004type\032\0020\007:\006\n\0042\002\007\025\n\346\001\n\tSparseAdd\022\r\n\ta_indices\030\t\022\r\n\010a_values\"\001T\022\013\n\007a_shape\030\t\022\r\n\tb_indices\030\t\022\r\n\010b_values\"\001T\022\013\n\007b_shape\030\t\022\017\n\006thresh\"\005Treal\032\017\n\013sum_indices\030\t\032\017\n\nsum_values\"\001T\032\r\n\tsum_shape\030\t\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\037\n\005Treal\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\232\001\n\rSparseAddGrad\022\026\n\021backprop_val_grad\"\001T\022\r\n\ta_indices\030\t\022\r\n\tb_indices\030\t\022\017\n\013sum_indices\030\t\032\017\n\na_val_grad\"\001T\032\017\n\nb_val_grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\243\001\n\014SparseConcat\022\016\n\007indices\030\t*\001N\022\016\n\006values\"\001T*\001N\022\r\n\006shapes\030\t*\001N\032\022\n\016output_indices\030\t\032\022\n\routput_values\"\001T\032\020\n\014output_shape\030\t\"\021\n\nconcat_dim\022\003int\"\014\n\001N\022\003int(\0010\002\"\t\n\001T\022\004type\n\360\002\n\013SparseCross\022\016\n\007indices\030\t*\001N\022\026\n\006values2\014sparse_types\022\r\n\006shapes\030\t*\001N\022\033\n\014dense_inputs2\013dense_types\032\022\n\016output_indices\030\t\032\031\n\routput_values\"\010out_type\032\020\n\014output_shape\030\t\"\n\n\001N\022\003int(\001\"\025\n\rhashed_output\022\004bool\"\024\n\013num_buckets\022\003int(\001\"\017\n\010hash_key\022\003int\"$\n\014sparse_types\022\nlist(type)(\001:\006\n\0042\002\t\007\"#\n\013dense_types\022\nlist(type)(\001:\006\n\0042\002\t\007\"\030\n\010out_type\022\004type:\006\n\0042\002\t\007\"\035\n\rinternal_type\022\004type:\006\n\0042\002\t\007\n~\n\023SparseDenseCwiseAdd\022\016\n\nsp_indices\030\t\022\016\n\tsp_values\"\001T\022\014\n\010sp_shape\030\t\022\n\n\005dense\"\001T\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n~\n\023SparseDenseCwiseDiv\022\016\n\nsp_indices\030\t\022\016\n\tsp_values\"\001T\022\014\n\010sp_shape\030\t\022\n\n\005dense\"\001T\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n~\n\023SparseDenseCwiseMul\022\016\n\nsp_indices\030\t\022\016\n\tsp_values\"\001T\022\014\n\010sp_shape\030\t\022\n\n\005dense\"\001T\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\267\001\n\023SparseFillEmptyRows\022\013\n\007indices\030\t\022\013\n\006values\"\001T\022\017\n\013dense_shape\030\t\022\022\n\rdefault_value\"\001T\032\022\n\016output_indices\030\t\032\022\n\routput_values\"\001T\032\027\n\023empty_row_indicator\030\n\032\025\n\021reverse_index_map\030\t\"\t\n\001T\022\004type\nr\n\027SparseFillEmptyRowsGrad\022\025\n\021reverse_index_map\030\t\022\020\n\013grad_values\"\001T\032\r\n\010d_values\"\001T\032\024\n\017d_default_value\"\001T\"\t\n\001T\022\004type\n\235\001\n\017SparseReduceMax\022\021\n\rinput_indices\030\t\022\021\n\014input_values\"\001T\022\017\n\013input_shape\030\t\022\022\n\016reduction_axes\030\003\032\013\n\006output\"\001T\"\025\n\tkeep_dims\022\004bool\032\002(\000\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\320\001\n\025SparseReduceMaxSparse\022\021\n\rinput_indices\030\t\022\021\n\014input_values\"\001T\022\017\n\013input_shape\030\t\022\022\n\016reduction_axes\030\003\032\022\n\016output_indices\030\t\032\022\n\routput_values\"\001T\032\020\n\014output_shape\030\t\"\025\n\tkeep_dims\022\004bool\032\002(\000\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\242\001\n\017SparseReduceSum\022\021\n\rinput_indices\030\t\022\021\n\014input_values\"\001T\022\017\n\013input_shape\030\t\022\022\n\016reduction_axes\030\003\032\013\n\006output\"\001T\"\025\n\tkeep_dims\022\004bool\032\002(\000\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\325\001\n\025SparseReduceSumSparse\022\021\n\rinput_indices\030\t\022\021\n\014input_values\"\001T\022\017\n\013input_shape\030\t\022\022\n\016reduction_axes\030\003\032\022\n\016output_indices\030\t\032\022\n\routput_values\"\001T\032\020\n\014output_shape\030\t\"\025\n\tkeep_dims\022\004bool\032\002(\000\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\ny\n\rSparseReorder\022\021\n\rinput_indices\030\t\022\021\n\014input_values\"\001T\022\017\n\013input_shape\030\t\032\022\n\016output_indices\030\t\032\022\n\routput_values\"\001T\"\t\n\001T\022\004type\nh\n\rSparseReshape\022\021\n\rinput_indices\030\t\022\017\n\013input_shape\030\t\022\r\n\tnew_shape\030\t\032\022\n\016output_indices\030\t\032\020\n\014output_shape\030\t\n\214\001\n\013SparseSlice\022\013\n\007indices\030\t\022\013\n\006values\"\001T\022\t\n\005shape\030\t\022\t\n\005start\030\t\022\010\n\004size\030\t\032\022\n\016output_indices\030\t\032\022\n\routput_values\"\001T\032\020\n\014output_shape\030\t\"\t\n\001T\022\004type\n\222\001\n\017SparseSliceGrad\022\026\n\021backprop_val_grad\"\001T\022\021\n\rinput_indices\030\t\022\017\n\013input_start\030\t\022\022\n\016output_indices\030\t\032\r\n\010val_grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n]\n\rSparseSoftmax\022\016\n\nsp_indices\030\t\022\016\n\tsp_values\"\001T\022\014\n\010sp_shape\030\t\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\n\260\001\n\023SparseSparseMaximum\022\r\n\ta_indices\030\t\022\r\n\010a_values\"\001T\022\013\n\007a_shape\030\t\022\r\n\tb_indices\030\t\022\r\n\010b_values\"\001T\022\013\n\007b_shape\030\t\032\022\n\016output_indices\030\t\032\022\n\routput_values\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\265\001\n\023SparseSparseMinimum\022\r\n\ta_indices\030\t\022\r\n\010a_values\"\001T\022\013\n\007a_shape\030\t\022\r\n\tb_indices\030\t\022\r\n\010b_values\"\001T\022\013\n\007b_shape\030\t\032\022\n\016output_indices\030\t\032\022\n\routput_values\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\275\001\n\013SparseSplit\022\r\n\tsplit_dim\030\t\022\013\n\007indices\030\t\022\013\n\006values\"\001T\022\t\n\005shape\030\t\032\035\n\016output_indices\030\t*\tnum_split\032\035\n\routput_values\"\001T*\tnum_split\032\033\n\014output_shape\030\t*\tnum_split\"\024\n\tnum_split\022\003int(\0010\001\"\t\n\001T\022\004type\n\242\001\n\024SparseTensorDenseAdd\022\025\n\ta_indices\"\010Tindices\022\r\n\010a_values\"\001T\022\023\n\007a_shape\"\010Tindices\022\006\n\001b\"\001T\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\n\271\001\n\027SparseTensorDenseMatMul\022\025\n\ta_indices\"\010Tindices\022\r\n\010a_values\"\001T\022\013\n\007a_shape\030\t\022\006\n\001b\"\001T\032\014\n\007product\"\001T\"\t\n\001T\022\004type\"\034\n\010Tindices\022\004type\032\0020\t:\006\n\0042\002\003\t\"\025\n\tadjoint_a\022\004bool\032\002(\000\"\025\n\tadjoint_b\022\004bool\032\002(\000\n\274\001\n\rSparseToDense\022\032\n\016sparse_indices\"\010Tindices\022\030\n\014output_shape\"\010Tindices\022\022\n\rsparse_values\"\001T\022\022\n\rdefault_value\"\001T\032\n\n\005dense\"\001T\"\034\n\020validate_indices\022\004bool\032\002(\001\"\t\n\001T\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\n\266\001\n\034TakeManySparseFromTensorsMap\022\022\n\016sparse_handles\030\t\032\022\n\016sparse_indices\030\t\032\026\n\rsparse_values\"\005dtype\032\020\n\014sparse_shape\030\t\"\r\n\005dtype\022\004type\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001")
