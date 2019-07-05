"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_variable_ops.cc
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
@tf_export('zero_initializer')
def zero_initializer(ref, name=None):
  r"""Initialize 'ref' with all zeros. This op requires that the tensor is not

  initialized. The tensor will first be allocated memory, then be filled with all
  zeros. This op is intended to save memory during initialization,
  if you use this op, you should not run initializer of the 'ref' tensor.

  Args:
    ref: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      Should be from a `Variable` node.
    name: A name for the operation (optional).

  Returns:
    Same as "ref".
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("zero_initializer op does not support eager execution. Arg 'output_ref' is a ref.")
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ZeroInitializer", ref=ref, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          zero_initializer, ref=ref, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "ZeroInitializer", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def zero_initializer_eager_fallback(ref, name=None, ctx=None):
  raise RuntimeError("zero_initializer op does not support eager execution. Arg 'output_ref' is a ref.")
_ops.RegisterShape("ZeroInitializer")(None)


@_dispatch.add_dispatch_list
@tf_export('zero_var_initializer')
def zero_var_initializer(var, dtype, shape, name=None):
  r"""Initialize 'var' with all zeros. This op requires that the resource var is not

  initialized. The var will first be allocated memory, then be filled with all
  zeros. This op is intended to save memory during initialization,
  if you use this op, you should not run initializer of the var.

  Args:
    var: A `Tensor` of type `resource`. Should be a ResourceVariable.
    dtype: A `tf.DType`.
    shape: A `tf.TensorShape` or list of `ints`.
    name: A name for the operation (optional).

  Returns:
    Same as "var".
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ZeroVarInitializer", name, _ctx._post_execution_callbacks, var,
        "dtype", dtype, "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return zero_var_initializer_eager_fallback(
            var, dtype=dtype, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              zero_var_initializer, var=var, dtype=dtype, shape=shape,
                                    name=name)
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
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ZeroVarInitializer", var=var, dtype=dtype, shape=shape, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          zero_var_initializer, var=var, dtype=dtype, shape=shape, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "ZeroVarInitializer", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def zero_var_initializer_eager_fallback(var, dtype, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function zero_var_initializer
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  _inputs_flat = [var]
  _attrs = ("dtype", dtype, "shape", shape)
  _result = _execute.execute(b"ZeroVarInitializer", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ZeroVarInitializer", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("ZeroVarInitializer")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "ZeroInitializer"
#   input_arg {
#     name: "ref"
#     type_attr: "T"
#     is_ref: true
#   }
#   output_arg {
#     name: "output_ref"
#     type_attr: "T"
#     is_ref: true
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
#   allows_uninitialized_input: true
# }
# op {
#   name: "ZeroVarInitializer"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "output_var"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   is_stateful: true
#   allows_uninitialized_input: true
# }
_op_def_lib = _InitOpDefLibrary(b"\nR\n\017ZeroInitializer\022\013\n\003ref\"\001T\200\001\001\032\022\n\noutput_ref\"\001T\200\001\001\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\230\001\001\nR\n\022ZeroVarInitializer\022\007\n\003var\030\024\032\016\n\noutput_var\030\024\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shape\210\001\001\230\001\001")
