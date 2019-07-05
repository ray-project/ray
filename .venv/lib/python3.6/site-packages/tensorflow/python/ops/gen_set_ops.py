"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: set_ops.cc
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


_dense_to_dense_set_operation_outputs = ["result_indices", "result_values",
                                        "result_shape"]
_DenseToDenseSetOperationOutput = _collections.namedtuple(
    "DenseToDenseSetOperation", _dense_to_dense_set_operation_outputs)


def dense_to_dense_set_operation(set1, set2, set_operation, validate_indices=True, name=None):
  r"""Applies set operation along last dimension of 2 `Tensor` inputs.

  See SetOperationOp::SetOperationFromContext for values of `set_operation`.

  Output `result` is a `SparseTensor` represented by `result_indices`,
  `result_values`, and `result_shape`. For `set1` and `set2` ranked `n`, this
  has rank `n` and the same 1st `n-1` dimensions as `set1` and `set2`. The `nth`
  dimension contains the result of `set_operation` applied to the corresponding
  `[0...n-1]` dimension of `set`.

  Args:
    set1: A `Tensor`. Must be one of the following types: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `string`.
      `Tensor` with rank `n`. 1st `n-1` dimensions must be the same as `set2`.
      Dimension `n` contains values in a set, duplicates are allowed but ignored.
    set2: A `Tensor`. Must have the same type as `set1`.
      `Tensor` with rank `n`. 1st `n-1` dimensions must be the same as `set1`.
      Dimension `n` contains values in a set, duplicates are allowed but ignored.
    set_operation: A `string`.
    validate_indices: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (result_indices, result_values, result_shape).

    result_indices: A `Tensor` of type `int64`.
    result_values: A `Tensor`. Has the same type as `set1`.
    result_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DenseToDenseSetOperation", name, _ctx._post_execution_callbacks,
        set1, set2, "set_operation", set_operation, "validate_indices",
        validate_indices)
      _result = _DenseToDenseSetOperationOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return dense_to_dense_set_operation_eager_fallback(
            set1, set2, set_operation=set_operation,
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
  set_operation = _execute.make_str(set_operation, "set_operation")
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DenseToDenseSetOperation", set1=set1, set2=set2,
                                    set_operation=set_operation,
                                    validate_indices=validate_indices,
                                    name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("set_operation", _op.get_attr("set_operation"),
            "validate_indices", _op.get_attr("validate_indices"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "DenseToDenseSetOperation", _inputs_flat, _attrs, _result, name)
  _result = _DenseToDenseSetOperationOutput._make(_result)
  return _result



def dense_to_dense_set_operation_eager_fallback(set1, set2, set_operation, validate_indices=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dense_to_dense_set_operation
  """
  _ctx = ctx if ctx else _context.context()
  set_operation = _execute.make_str(set_operation, "set_operation")
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([set1, set2], _ctx)
  (set1, set2) = _inputs_T
  _inputs_flat = [set1, set2]
  _attrs = ("set_operation", set_operation, "validate_indices",
  validate_indices, "T", _attr_T)
  _result = _execute.execute(b"DenseToDenseSetOperation", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "DenseToDenseSetOperation", _inputs_flat, _attrs, _result, name)
  _result = _DenseToDenseSetOperationOutput._make(_result)
  return _result


_dense_to_sparse_set_operation_outputs = ["result_indices", "result_values",
                                         "result_shape"]
_DenseToSparseSetOperationOutput = _collections.namedtuple(
    "DenseToSparseSetOperation", _dense_to_sparse_set_operation_outputs)


def dense_to_sparse_set_operation(set1, set2_indices, set2_values, set2_shape, set_operation, validate_indices=True, name=None):
  r"""Applies set operation along last dimension of `Tensor` and `SparseTensor`.

  See SetOperationOp::SetOperationFromContext for values of `set_operation`.

  Input `set2` is a `SparseTensor` represented by `set2_indices`, `set2_values`,
  and `set2_shape`. For `set2` ranked `n`, 1st `n-1` dimensions must be the same
  as `set1`. Dimension `n` contains values in a set, duplicates are allowed but
  ignored.

  If `validate_indices` is `True`, this op validates the order and range of `set2`
  indices.

  Output `result` is a `SparseTensor` represented by `result_indices`,
  `result_values`, and `result_shape`. For `set1` and `set2` ranked `n`, this
  has rank `n` and the same 1st `n-1` dimensions as `set1` and `set2`. The `nth`
  dimension contains the result of `set_operation` applied to the corresponding
  `[0...n-1]` dimension of `set`.

  Args:
    set1: A `Tensor`. Must be one of the following types: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `string`.
      `Tensor` with rank `n`. 1st `n-1` dimensions must be the same as `set2`.
      Dimension `n` contains values in a set, duplicates are allowed but ignored.
    set2_indices: A `Tensor` of type `int64`.
      2D `Tensor`, indices of a `SparseTensor`. Must be in row-major
      order.
    set2_values: A `Tensor`. Must have the same type as `set1`.
      1D `Tensor`, values of a `SparseTensor`. Must be in row-major
      order.
    set2_shape: A `Tensor` of type `int64`.
      1D `Tensor`, shape of a `SparseTensor`. `set2_shape[0...n-1]` must
      be the same as the 1st `n-1` dimensions of `set1`, `result_shape[n]` is the
      max set size across `n-1` dimensions.
    set_operation: A `string`.
    validate_indices: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (result_indices, result_values, result_shape).

    result_indices: A `Tensor` of type `int64`.
    result_values: A `Tensor`. Has the same type as `set1`.
    result_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DenseToSparseSetOperation", name, _ctx._post_execution_callbacks,
        set1, set2_indices, set2_values, set2_shape, "set_operation",
        set_operation, "validate_indices", validate_indices)
      _result = _DenseToSparseSetOperationOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return dense_to_sparse_set_operation_eager_fallback(
            set1, set2_indices, set2_values, set2_shape,
            set_operation=set_operation, validate_indices=validate_indices,
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
  set_operation = _execute.make_str(set_operation, "set_operation")
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DenseToSparseSetOperation", set1=set1, set2_indices=set2_indices,
                                     set2_values=set2_values,
                                     set2_shape=set2_shape,
                                     set_operation=set_operation,
                                     validate_indices=validate_indices,
                                     name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("set_operation", _op.get_attr("set_operation"),
            "validate_indices", _op.get_attr("validate_indices"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "DenseToSparseSetOperation", _inputs_flat, _attrs, _result, name)
  _result = _DenseToSparseSetOperationOutput._make(_result)
  return _result



def dense_to_sparse_set_operation_eager_fallback(set1, set2_indices, set2_values, set2_shape, set_operation, validate_indices=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dense_to_sparse_set_operation
  """
  _ctx = ctx if ctx else _context.context()
  set_operation = _execute.make_str(set_operation, "set_operation")
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([set1, set2_values], _ctx)
  (set1, set2_values) = _inputs_T
  set2_indices = _ops.convert_to_tensor(set2_indices, _dtypes.int64)
  set2_shape = _ops.convert_to_tensor(set2_shape, _dtypes.int64)
  _inputs_flat = [set1, set2_indices, set2_values, set2_shape]
  _attrs = ("set_operation", set_operation, "validate_indices",
  validate_indices, "T", _attr_T)
  _result = _execute.execute(b"DenseToSparseSetOperation", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "DenseToSparseSetOperation", _inputs_flat, _attrs, _result, name)
  _result = _DenseToSparseSetOperationOutput._make(_result)
  return _result


def set_size(set_indices, set_values, set_shape, validate_indices=True, name=None):
  r"""Number of unique elements along last dimension of input `set`.

  Input `set` is a `SparseTensor` represented by `set_indices`, `set_values`,
  and `set_shape`. The last dimension contains values in a set, duplicates are
  allowed but ignored.

  If `validate_indices` is `True`, this op validates the order and range of `set`
  indices.

  Args:
    set_indices: A `Tensor` of type `int64`.
      2D `Tensor`, indices of a `SparseTensor`.
    set_values: A `Tensor`. Must be one of the following types: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `string`.
      1D `Tensor`, values of a `SparseTensor`.
    set_shape: A `Tensor` of type `int64`.
      1D `Tensor`, shape of a `SparseTensor`.
    validate_indices: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SetSize",
        name, _ctx._post_execution_callbacks, set_indices, set_values,
        set_shape, "validate_indices", validate_indices)
      return _result
    except _core._FallbackException:
      try:
        return set_size_eager_fallback(
            set_indices, set_values, set_shape,
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
        "SetSize", set_indices=set_indices, set_values=set_values,
                   set_shape=set_shape, validate_indices=validate_indices,
                   name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("validate_indices", _op.get_attr("validate_indices"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "SetSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def set_size_eager_fallback(set_indices, set_values, set_shape, validate_indices=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function set_size
  """
  _ctx = ctx if ctx else _context.context()
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _attr_T, (set_values,) = _execute.args_to_matching_eager([set_values], _ctx)
  set_indices = _ops.convert_to_tensor(set_indices, _dtypes.int64)
  set_shape = _ops.convert_to_tensor(set_shape, _dtypes.int64)
  _inputs_flat = [set_indices, set_values, set_shape]
  _attrs = ("validate_indices", validate_indices, "T", _attr_T)
  _result = _execute.execute(b"SetSize", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "SetSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_sparse_to_sparse_set_operation_outputs = ["result_indices", "result_values",
                                          "result_shape"]
_SparseToSparseSetOperationOutput = _collections.namedtuple(
    "SparseToSparseSetOperation", _sparse_to_sparse_set_operation_outputs)


def sparse_to_sparse_set_operation(set1_indices, set1_values, set1_shape, set2_indices, set2_values, set2_shape, set_operation, validate_indices=True, name=None):
  r"""Applies set operation along last dimension of 2 `SparseTensor` inputs.

  See SetOperationOp::SetOperationFromContext for values of `set_operation`.

  If `validate_indices` is `True`, `SparseToSparseSetOperation` validates the
  order and range of `set1` and `set2` indices.

  Input `set1` is a `SparseTensor` represented by `set1_indices`, `set1_values`,
  and `set1_shape`. For `set1` ranked `n`, 1st `n-1` dimensions must be the same
  as `set2`. Dimension `n` contains values in a set, duplicates are allowed but
  ignored.

  Input `set2` is a `SparseTensor` represented by `set2_indices`, `set2_values`,
  and `set2_shape`. For `set2` ranked `n`, 1st `n-1` dimensions must be the same
  as `set1`. Dimension `n` contains values in a set, duplicates are allowed but
  ignored.

  If `validate_indices` is `True`, this op validates the order and range of `set1`
  and `set2` indices.

  Output `result` is a `SparseTensor` represented by `result_indices`,
  `result_values`, and `result_shape`. For `set1` and `set2` ranked `n`, this
  has rank `n` and the same 1st `n-1` dimensions as `set1` and `set2`. The `nth`
  dimension contains the result of `set_operation` applied to the corresponding
  `[0...n-1]` dimension of `set`.

  Args:
    set1_indices: A `Tensor` of type `int64`.
      2D `Tensor`, indices of a `SparseTensor`. Must be in row-major
      order.
    set1_values: A `Tensor`. Must be one of the following types: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `string`.
      1D `Tensor`, values of a `SparseTensor`. Must be in row-major
      order.
    set1_shape: A `Tensor` of type `int64`.
      1D `Tensor`, shape of a `SparseTensor`. `set1_shape[0...n-1]` must
      be the same as `set2_shape[0...n-1]`, `set1_shape[n]` is the
      max set size across `0...n-1` dimensions.
    set2_indices: A `Tensor` of type `int64`.
      2D `Tensor`, indices of a `SparseTensor`. Must be in row-major
      order.
    set2_values: A `Tensor`. Must have the same type as `set1_values`.
      1D `Tensor`, values of a `SparseTensor`. Must be in row-major
      order.
    set2_shape: A `Tensor` of type `int64`.
      1D `Tensor`, shape of a `SparseTensor`. `set2_shape[0...n-1]` must
      be the same as `set1_shape[0...n-1]`, `set2_shape[n]` is the
      max set size across `0...n-1` dimensions.
    set_operation: A `string`.
    validate_indices: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (result_indices, result_values, result_shape).

    result_indices: A `Tensor` of type `int64`.
    result_values: A `Tensor`. Has the same type as `set1_values`.
    result_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseToSparseSetOperation", name, _ctx._post_execution_callbacks,
        set1_indices, set1_values, set1_shape, set2_indices, set2_values,
        set2_shape, "set_operation", set_operation, "validate_indices",
        validate_indices)
      _result = _SparseToSparseSetOperationOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_to_sparse_set_operation_eager_fallback(
            set1_indices, set1_values, set1_shape, set2_indices, set2_values,
            set2_shape, set_operation=set_operation,
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
  set_operation = _execute.make_str(set_operation, "set_operation")
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseToSparseSetOperation", set1_indices=set1_indices,
                                      set1_values=set1_values,
                                      set1_shape=set1_shape,
                                      set2_indices=set2_indices,
                                      set2_values=set2_values,
                                      set2_shape=set2_shape,
                                      set_operation=set_operation,
                                      validate_indices=validate_indices,
                                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("set_operation", _op.get_attr("set_operation"),
            "validate_indices", _op.get_attr("validate_indices"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "SparseToSparseSetOperation", _inputs_flat, _attrs, _result, name)
  _result = _SparseToSparseSetOperationOutput._make(_result)
  return _result



def sparse_to_sparse_set_operation_eager_fallback(set1_indices, set1_values, set1_shape, set2_indices, set2_values, set2_shape, set_operation, validate_indices=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_to_sparse_set_operation
  """
  _ctx = ctx if ctx else _context.context()
  set_operation = _execute.make_str(set_operation, "set_operation")
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([set1_values, set2_values], _ctx)
  (set1_values, set2_values) = _inputs_T
  set1_indices = _ops.convert_to_tensor(set1_indices, _dtypes.int64)
  set1_shape = _ops.convert_to_tensor(set1_shape, _dtypes.int64)
  set2_indices = _ops.convert_to_tensor(set2_indices, _dtypes.int64)
  set2_shape = _ops.convert_to_tensor(set2_shape, _dtypes.int64)
  _inputs_flat = [set1_indices, set1_values, set1_shape, set2_indices, set2_values, set2_shape]
  _attrs = ("set_operation", set_operation, "validate_indices",
  validate_indices, "T", _attr_T)
  _result = _execute.execute(b"SparseToSparseSetOperation", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SparseToSparseSetOperation", _inputs_flat, _attrs, _result, name)
  _result = _SparseToSparseSetOperationOutput._make(_result)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "DenseToDenseSetOperation"
#   input_arg {
#     name: "set1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "set2"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "result_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "result_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "result_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "set_operation"
#     type: "string"
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
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_UINT8
#         type: DT_UINT16
#         type: DT_STRING
#       }
#     }
#   }
# }
# op {
#   name: "DenseToSparseSetOperation"
#   input_arg {
#     name: "set1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "set2_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "set2_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "set2_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "result_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "result_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "result_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "set_operation"
#     type: "string"
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
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_UINT8
#         type: DT_UINT16
#         type: DT_STRING
#       }
#     }
#   }
# }
# op {
#   name: "SetSize"
#   input_arg {
#     name: "set_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "set_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "set_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT32
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
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_UINT8
#         type: DT_UINT16
#         type: DT_STRING
#       }
#     }
#   }
# }
# op {
#   name: "SparseToSparseSetOperation"
#   input_arg {
#     name: "set1_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "set1_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "set1_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "set2_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "set2_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "set2_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "result_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "result_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "result_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "set_operation"
#     type: "string"
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
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_UINT8
#         type: DT_UINT16
#         type: DT_STRING
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\271\001\n\030DenseToDenseSetOperation\022\t\n\004set1\"\001T\022\t\n\004set2\"\001T\032\022\n\016result_indices\030\t\032\022\n\rresult_values\"\001T\032\020\n\014result_shape\030\t\"\027\n\rset_operation\022\006string\"\034\n\020validate_indices\022\004bool\032\002(\001\"\026\n\001T\022\004type:\013\n\t2\007\006\005\003\t\004\021\007\n\343\001\n\031DenseToSparseSetOperation\022\t\n\004set1\"\001T\022\020\n\014set2_indices\030\t\022\020\n\013set2_values\"\001T\022\016\n\nset2_shape\030\t\032\022\n\016result_indices\030\t\032\022\n\rresult_values\"\001T\032\020\n\014result_shape\030\t\"\027\n\rset_operation\022\006string\"\034\n\020validate_indices\022\004bool\032\002(\001\"\026\n\001T\022\004type:\013\n\t2\007\006\005\003\t\004\021\007\nz\n\007SetSize\022\017\n\013set_indices\030\t\022\017\n\nset_values\"\001T\022\r\n\tset_shape\030\t\032\010\n\004size\030\003\"\034\n\020validate_indices\022\004bool\032\002(\001\"\026\n\001T\022\004type:\013\n\t2\007\006\005\003\t\004\021\007\n\215\002\n\032SparseToSparseSetOperation\022\020\n\014set1_indices\030\t\022\020\n\013set1_values\"\001T\022\016\n\nset1_shape\030\t\022\020\n\014set2_indices\030\t\022\020\n\013set2_values\"\001T\022\016\n\nset2_shape\030\t\032\022\n\016result_indices\030\t\032\022\n\rresult_values\"\001T\032\020\n\014result_shape\030\t\"\027\n\rset_operation\022\006string\"\034\n\020validate_indices\022\004bool\032\002(\001\"\026\n\001T\022\004type:\013\n\t2\007\006\005\003\t\004\021\007")
