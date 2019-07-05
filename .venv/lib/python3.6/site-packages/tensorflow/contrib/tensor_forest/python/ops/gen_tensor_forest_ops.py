"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_tensor_forest_ops.cc
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
@tf_export('reinterpret_string_to_float')
def reinterpret_string_to_float(input_data, name=None):
  r"""   Converts byte arrays represented by strings to 32-bit

     floating point numbers. The output numbers themselves are meaningless, and
     should only be used in == comparisons.

     input_data: A batch of string features as a 2-d tensor; `input_data[i][j]`
       gives the j-th feature of the i-th input.
     output_data: A tensor of the same shape as input_data but the values are
       float32.

  Args:
    input_data: A `Tensor` of type `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReinterpretStringToFloat", name, _ctx._post_execution_callbacks,
        input_data)
      return _result
    except _core._FallbackException:
      try:
        return reinterpret_string_to_float_eager_fallback(
            input_data, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              reinterpret_string_to_float, input_data=input_data, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ReinterpretStringToFloat", input_data=input_data, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          reinterpret_string_to_float, input_data=input_data, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReinterpretStringToFloat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reinterpret_string_to_float_eager_fallback(input_data, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reinterpret_string_to_float
  """
  _ctx = ctx if ctx else _context.context()
  input_data = _ops.convert_to_tensor(input_data, _dtypes.string)
  _inputs_flat = [input_data]
  _attrs = None
  _result = _execute.execute(b"ReinterpretStringToFloat", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ReinterpretStringToFloat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("ReinterpretStringToFloat")(None)


@_dispatch.add_dispatch_list
@tf_export('scatter_add_ndim')
def scatter_add_ndim(input, indices, deltas, name=None):
  r"""  Add elements in deltas to mutable input according to indices.

    input: A N-dimensional float tensor to mutate.
    indices:= A 2-D int32 tensor. The size of dimension 0 is the number of
      deltas, the size of dimension 1 is the rank of the input.  `indices[i]`
      gives the coordinates of input that `deltas[i]` should add to.  If
      `indices[i]` does not fully specify a location (it has less indices than
      there are dimensions in `input`), it is assumed that they are start
      indices and that deltas contains enough values to fill in the remaining
      input dimensions.
    deltas: `deltas[i]` is the value to add to input at index indices[i][:]

  Args:
    input: A `Tensor` of type mutable `float32`.
    indices: A `Tensor` of type `int32`.
    deltas: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("scatter_add_ndim op does not support eager execution. Arg 'input' is a ref.")
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ScatterAddNdim", input=input, indices=indices, deltas=deltas,
                          name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          scatter_add_ndim, input=input, indices=indices, deltas=deltas,
                            name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def scatter_add_ndim_eager_fallback(input, indices, deltas, name=None, ctx=None):
  raise RuntimeError("scatter_add_ndim op does not support eager execution. Arg 'input' is a ref.")
_ops.RegisterShape("ScatterAddNdim")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "ReinterpretStringToFloat"
#   input_arg {
#     name: "input_data"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output_data"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "ScatterAddNdim"
#   input_arg {
#     name: "input"
#     type: DT_FLOAT
#     is_ref: true
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "deltas"
#     type: DT_FLOAT
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n;\n\030ReinterpretStringToFloat\022\016\n\ninput_data\030\007\032\017\n\013output_data\030\001\n7\n\016ScatterAddNdim\022\014\n\005input\030\001\200\001\001\022\013\n\007indices\030\003\022\n\n\006deltas\030\001")
