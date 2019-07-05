"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: manip_ops.cc
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


def roll(input, shift, axis, name=None):
  r"""Rolls the elements of a tensor along an axis.

  The elements are shifted positively (towards larger indices) by the offset of
  `shift` along the dimension of `axis`. Negative `shift` values will shift
  elements in the opposite direction. Elements that roll passed the last position
  will wrap around to the first and vice versa. Multiple shifts along multiple
  axes may be specified.

  For example:

  ```
  # 't' is [0, 1, 2, 3, 4]
  roll(t, shift=2, axis=0) ==> [3, 4, 0, 1, 2]

  # shifting along multiple dimensions
  # 't' is [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
  roll(t, shift=[1, -2], axis=[0, 1]) ==> [[7, 8, 9, 5, 6], [2, 3, 4, 0, 1]]

  # shifting along the same axis multiple times
  # 't' is [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
  roll(t, shift=[2, -3], axis=[1, 1]) ==> [[1, 2, 3, 4, 0], [6, 7, 8, 9, 5]]
  ```

  Args:
    input: A `Tensor`.
    shift: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Dimension must be 0-D or 1-D. `shift[i]` specifies the number of places by which
      elements are shifted positively (towards larger indices) along the dimension
      specified by `axis[i]`. Negative shifts will roll the elements in the opposite
      direction.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Dimension must be 0-D or 1-D. `axis[i]` specifies the dimension that the shift
      `shift[i]` should occur. If the same axis is referenced more than once, the
      total shift for that axis will be the sum of all the shifts that belong to that
      axis.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Roll", name,
        _ctx._post_execution_callbacks, input, shift, axis)
      return _result
    except _core._FallbackException:
      try:
        return roll_eager_fallback(
            input, shift, axis, name=name, ctx=_ctx)
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
        "Roll", input=input, shift=shift, axis=axis, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tshift", _op.get_attr("Tshift"), "Taxis",
            _op.get_attr("Taxis"))
  _execute.record_gradient(
      "Roll", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def roll_eager_fallback(input, shift, axis, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function roll
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tshift, (shift,) = _execute.args_to_matching_eager([shift], _ctx)
  _attr_Taxis, (axis,) = _execute.args_to_matching_eager([axis], _ctx)
  _inputs_flat = [input, shift, axis]
  _attrs = ("T", _attr_T, "Tshift", _attr_Tshift, "Taxis", _attr_Taxis)
  _result = _execute.execute(b"Roll", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Roll", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "Roll"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "shift"
#     type_attr: "Tshift"
#   }
#   input_arg {
#     name: "axis"
#     type_attr: "Taxis"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tshift"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "Taxis"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\ny\n\004Roll\022\n\n\005input\"\001T\022\017\n\005shift\"\006Tshift\022\r\n\004axis\"\005Taxis\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\026\n\006Tshift\022\004type:\006\n\0042\002\003\t\"\025\n\005Taxis\022\004type:\006\n\0042\002\003\t")
