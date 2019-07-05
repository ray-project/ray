"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: input_pipeline_ops.cc
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


@_dispatch.add_dispatch_list
@tf_export('obtain_next')
def obtain_next(list, counter, name=None):
  r"""Takes a list and returns the next based on a counter in a round-robin fashion.

  Returns the element in the list at the new position of the counter, so if you
  want to circle the list around start by setting the counter value = -1.

  Args:
    list: A `Tensor` of type `string`. A list of strings
    counter: A `Tensor` of type mutable `int64`.
      A reference to an int64 variable
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("obtain_next op does not support eager execution. Arg 'counter' is a ref.")
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ObtainNext", list=list, counter=counter, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          obtain_next, list=list, counter=counter, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ObtainNext", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def obtain_next_eager_fallback(list, counter, name=None, ctx=None):
  raise RuntimeError("obtain_next op does not support eager execution. Arg 'counter' is a ref.")
_ops.RegisterShape("ObtainNext")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "ObtainNext"
#   input_arg {
#     name: "list"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "counter"
#     type: DT_INT64
#     is_ref: true
#   }
#   output_arg {
#     name: "out_element"
#     type: DT_STRING
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n7\n\nObtainNext\022\010\n\004list\030\007\022\016\n\007counter\030\t\200\001\001\032\017\n\013out_element\030\007")
