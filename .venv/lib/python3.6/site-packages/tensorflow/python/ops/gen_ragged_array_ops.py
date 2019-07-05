"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: ragged_array_ops.cc
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


_ragged_gather_outputs = ["output_nested_splits", "output_dense_values"]
_RaggedGatherOutput = _collections.namedtuple(
    "RaggedGather", _ragged_gather_outputs)


def ragged_gather(params_nested_splits, params_dense_values, indices, OUTPUT_RAGGED_RANK, name=None):
  r"""Gather ragged slices from `params` axis `0` according to `indices`.

  Outputs a `RaggedTensor` output composed from `output_dense_values` and
  `output_nested_splits`, such that:

  ```python
  output.shape = indices.shape + params.shape[1:]
  output.ragged_rank = indices.shape.ndims + params.ragged_rank
  output[i...j, d0...dn] = params[indices[i...j], d0...dn]
  ```

  where

  * `params =
     ragged.from_nested_row_splits(params_dense_values, params_nested_splits)`
     provides the values that should be gathered.
  * `indices` ia a dense tensor with dtype `int32` or `int64`, indicating which
     values should be gathered.
  * `output =
     ragged.from_nested_row_splits(output_dense_values, output_nested_splits)`
     is the output tensor.

  (Note: This c++ op is used to implement the higher-level python
  `tf.ragged.gather` op, which also supports ragged indices.)

  Args:
    params_nested_splits: A list of at least 1 `Tensor` objects with type `int64`.
      The `nested_row_splits` tensors that define the row-partitioning for the
      `params` RaggedTensor input.
    params_dense_values: A `Tensor`.
      The `flat_values` for the `params` RaggedTensor. There was a terminology change
      at the python level from dense_values to flat_values, so dense_values is the
      deprecated name.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Indices in the outermost dimension of `params` of the values that should be
      gathered.
    OUTPUT_RAGGED_RANK: An `int` that is `>= 0`.
      The ragged rank of the output RaggedTensor. `output_nested_splits` will contain
      this number of `row_splits` tensors. This value should equal
      `indices.shape.ndims + params.ragged_rank - 1`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_nested_splits, output_dense_values).

    output_nested_splits: A list of `OUTPUT_RAGGED_RANK` `Tensor` objects with type `int64`.
    output_dense_values: A `Tensor`. Has the same type as `params_dense_values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RaggedGather",
        name, _ctx._post_execution_callbacks, params_nested_splits,
        params_dense_values, indices, "OUTPUT_RAGGED_RANK",
        OUTPUT_RAGGED_RANK)
      _result = _RaggedGatherOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return ragged_gather_eager_fallback(
            params_nested_splits, params_dense_values, indices,
            OUTPUT_RAGGED_RANK=OUTPUT_RAGGED_RANK, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(params_nested_splits, (list, tuple)):
    raise TypeError(
        "Expected list for 'params_nested_splits' argument to "
        "'ragged_gather' Op, not %r." % params_nested_splits)
  _attr_PARAMS_RAGGED_RANK = len(params_nested_splits)
  OUTPUT_RAGGED_RANK = _execute.make_int(OUTPUT_RAGGED_RANK, "OUTPUT_RAGGED_RANK")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RaggedGather", params_nested_splits=params_nested_splits,
                        params_dense_values=params_dense_values,
                        indices=indices,
                        OUTPUT_RAGGED_RANK=OUTPUT_RAGGED_RANK, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tvalues", _op.get_attr("Tvalues"), "Tindices",
            _op.get_attr("Tindices"), "PARAMS_RAGGED_RANK",
            _op.get_attr("PARAMS_RAGGED_RANK"), "OUTPUT_RAGGED_RANK",
            _op.get_attr("OUTPUT_RAGGED_RANK"))
  _execute.record_gradient(
      "RaggedGather", _inputs_flat, _attrs, _result, name)
  _result = [_result[:OUTPUT_RAGGED_RANK]] + _result[OUTPUT_RAGGED_RANK:]
  _result = _RaggedGatherOutput._make(_result)
  return _result



def ragged_gather_eager_fallback(params_nested_splits, params_dense_values, indices, OUTPUT_RAGGED_RANK, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ragged_gather
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(params_nested_splits, (list, tuple)):
    raise TypeError(
        "Expected list for 'params_nested_splits' argument to "
        "'ragged_gather' Op, not %r." % params_nested_splits)
  _attr_PARAMS_RAGGED_RANK = len(params_nested_splits)
  OUTPUT_RAGGED_RANK = _execute.make_int(OUTPUT_RAGGED_RANK, "OUTPUT_RAGGED_RANK")
  _attr_Tvalues, (params_dense_values,) = _execute.args_to_matching_eager([params_dense_values], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  params_nested_splits = _ops.convert_n_to_tensor(params_nested_splits, _dtypes.int64)
  _inputs_flat = list(params_nested_splits) + [params_dense_values, indices]
  _attrs = ("Tvalues", _attr_Tvalues, "Tindices", _attr_Tindices,
  "PARAMS_RAGGED_RANK", _attr_PARAMS_RAGGED_RANK, "OUTPUT_RAGGED_RANK",
  OUTPUT_RAGGED_RANK)
  _result = _execute.execute(b"RaggedGather", OUTPUT_RAGGED_RANK + 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RaggedGather", _inputs_flat, _attrs, _result, name)
  _result = [_result[:OUTPUT_RAGGED_RANK]] + _result[OUTPUT_RAGGED_RANK:]
  _result = _RaggedGatherOutput._make(_result)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "RaggedGather"
#   input_arg {
#     name: "params_nested_splits"
#     type: DT_INT64
#     number_attr: "PARAMS_RAGGED_RANK"
#   }
#   input_arg {
#     name: "params_dense_values"
#     type_attr: "Tvalues"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "output_nested_splits"
#     type: DT_INT64
#     number_attr: "OUTPUT_RAGGED_RANK"
#   }
#   output_arg {
#     name: "output_dense_values"
#     type_attr: "Tvalues"
#   }
#   attr {
#     name: "Tvalues"
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
#   attr {
#     name: "PARAMS_RAGGED_RANK"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "OUTPUT_RAGGED_RANK"
#     type: "int"
#     has_minimum: true
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\246\002\n\014RaggedGather\022,\n\024params_nested_splits\030\t*\022PARAMS_RAGGED_RANK\022\036\n\023params_dense_values\"\007Tvalues\022\023\n\007indices\"\010Tindices\032,\n\024output_nested_splits\030\t*\022OUTPUT_RAGGED_RANK\032\036\n\023output_dense_values\"\007Tvalues\"\017\n\007Tvalues\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\035\n\022PARAMS_RAGGED_RANK\022\003int(\0010\001\"\033\n\022OUTPUT_RAGGED_RANK\022\003int(\001")
