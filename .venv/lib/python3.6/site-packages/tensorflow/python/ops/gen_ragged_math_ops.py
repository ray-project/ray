"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: ragged_math_ops.cc
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


_ragged_range_outputs = ["rt_nested_splits", "rt_dense_values"]
_RaggedRangeOutput = _collections.namedtuple(
    "RaggedRange", _ragged_range_outputs)


def ragged_range(starts, limits, deltas, name=None):
  r"""Returns a `RaggedTensor` containing the specified sequences of numbers.

  
  Returns a `RaggedTensor` `result` composed from `rt_dense_values` and
  `rt_nested_splits`, such that
  `result[i] = range(starts[i], limits[i], deltas[i])`.

  ```python
  >>> (rt_nested_splits, rt_dense_values) = gen_ragged_ops.ragged_range(
  ...     starts=[2, 5, 8], limits=[3, 5, 12], deltas=1)
  >>> result = ragged.from_nested_row_splits(rt_dense_values, rt_nested_splits)
  >>> print result.eval().tolist()
  [[2],               # result[0] = range(2, 3)
   [],                # result[1] = range(5, 5)
   [8, 9, 10, 11]]    # result[2] = range(8, 12)
  ```

  The input tensors `starts`, `limits`, and `deltas` may be scalars or vectors.
  The vector inputs must all have the same size.  Scalar inputs are broadcast
  to match the size of the vector inputs.

  Args:
    starts: A `Tensor`. Must be one of the following types: `bfloat16`, `float32`, `float64`, `int32`, `int64`.
      The starts of each range.
    limits: A `Tensor`. Must have the same type as `starts`.
      The limits of each range.
    deltas: A `Tensor`. Must have the same type as `starts`.
      The deltas of each range.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (rt_nested_splits, rt_dense_values).

    rt_nested_splits: A `Tensor` of type `int64`.
    rt_dense_values: A `Tensor`. Has the same type as `starts`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RaggedRange",
        name, _ctx._post_execution_callbacks, starts, limits, deltas)
      _result = _RaggedRangeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return ragged_range_eager_fallback(
            starts, limits, deltas, name=name, ctx=_ctx)
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
        "RaggedRange", starts=starts, limits=limits, deltas=deltas, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "RaggedRange", _inputs_flat, _attrs, _result, name)
  _result = _RaggedRangeOutput._make(_result)
  return _result



def ragged_range_eager_fallback(starts, limits, deltas, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ragged_range
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([starts, limits, deltas], _ctx, _dtypes.int32)
  (starts, limits, deltas) = _inputs_T
  _inputs_flat = [starts, limits, deltas]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"RaggedRange", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RaggedRange", _inputs_flat, _attrs, _result, name)
  _result = _RaggedRangeOutput._make(_result)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "RaggedRange"
#   input_arg {
#     name: "starts"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "limits"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "deltas"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "rt_nested_splits"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "rt_dense_values"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\nz\n\013RaggedRange\022\013\n\006starts\"\001T\022\013\n\006limits\"\001T\022\013\n\006deltas\"\001T\032\024\n\020rt_nested_splits\030\t\032\024\n\017rt_dense_values\"\001T\"\030\n\001T\022\004type\032\0020\003:\t\n\0072\005\016\001\002\003\t")
