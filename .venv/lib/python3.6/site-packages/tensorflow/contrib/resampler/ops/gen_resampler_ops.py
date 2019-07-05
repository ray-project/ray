"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: resampler_ops.cc
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
@tf_export('resampler')
def resampler(data, warp, name=None):
  r"""Resampler op.

  Args:
    data: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
    warp: A `Tensor`. Must have the same type as `data`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Resampler",
        name, _ctx._post_execution_callbacks, data, warp)
      return _result
    except _core._FallbackException:
      try:
        return resampler_eager_fallback(
            data, warp, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              resampler, data=data, warp=warp, name=name)
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
        "Resampler", data=data, warp=warp, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          resampler, data=data, warp=warp, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Resampler", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def resampler_eager_fallback(data, warp, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resampler
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([data, warp], _ctx)
  (data, warp) = _inputs_T
  _inputs_flat = [data, warp]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Resampler", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Resampler", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("Resampler")(None)


_resampler_grad_outputs = ["grad_data", "grad_warp"]
_ResamplerGradOutput = _collections.namedtuple(
    "ResamplerGrad", _resampler_grad_outputs)


@_dispatch.add_dispatch_list
@tf_export('resampler_grad')
def resampler_grad(data, warp, grad_output, name=None):
  r"""Resampler Grad op.

  Args:
    data: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
    warp: A `Tensor`. Must have the same type as `data`.
    grad_output: A `Tensor`. Must have the same type as `data`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (grad_data, grad_warp).

    grad_data: A `Tensor`. Has the same type as `data`.
    grad_warp: A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResamplerGrad", name, _ctx._post_execution_callbacks, data, warp,
        grad_output)
      _result = _ResamplerGradOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return resampler_grad_eager_fallback(
            data, warp, grad_output, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              resampler_grad, data=data, warp=warp, grad_output=grad_output,
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
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ResamplerGrad", data=data, warp=warp, grad_output=grad_output,
                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          resampler_grad, data=data, warp=warp, grad_output=grad_output,
                          name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "ResamplerGrad", _inputs_flat, _attrs, _result, name)
  _result = _ResamplerGradOutput._make(_result)
  return _result



def resampler_grad_eager_fallback(data, warp, grad_output, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resampler_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([data, warp, grad_output], _ctx)
  (data, warp, grad_output) = _inputs_T
  _inputs_flat = [data, warp, grad_output]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"ResamplerGrad", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ResamplerGrad", _inputs_flat, _attrs, _result, name)
  _result = _ResamplerGradOutput._make(_result)
  return _result

_ops.RegisterShape("ResamplerGrad")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "Resampler"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "warp"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "ResamplerGrad"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "warp"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad_output"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "grad_data"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "grad_warp"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\nC\n\tResampler\022\t\n\004data\"\001T\022\t\n\004warp\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\nl\n\rResamplerGrad\022\t\n\004data\"\001T\022\t\n\004warp\"\001T\022\020\n\013grad_output\"\001T\032\016\n\tgrad_data\"\001T\032\016\n\tgrad_warp\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002")
