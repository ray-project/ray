"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: nccl_ops.cc
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


def nccl_all_reduce(input, reduction, num_devices, shared_name, name=None):
  r"""Outputs a tensor containing the reduction across all input tensors.

  Outputs a tensor containing the reduction across all input tensors passed to ops
  within the same `shared_name.

  The graph should be constructed so if one op runs with shared_name value `c`,
  then `num_devices` ops will run with shared_name value `c`.  Failure to do so
  will cause the graph execution to fail to complete.

  input: the input to the reduction
  data: the value of the reduction across all `num_devices` devices.
  reduction: the reduction operation to perform.
  num_devices: The number of devices participating in this reduction.
  shared_name: Identifier that shared between ops of the same reduction.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`, `int32`, `int64`.
    reduction: A `string` from: `"min", "max", "prod", "sum"`.
    num_devices: An `int`.
    shared_name: A `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "NcclAllReduce", name, _ctx._post_execution_callbacks, input,
        "reduction", reduction, "num_devices", num_devices, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return nccl_all_reduce_eager_fallback(
            input, reduction=reduction, num_devices=num_devices,
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
  reduction = _execute.make_str(reduction, "reduction")
  num_devices = _execute.make_int(num_devices, "num_devices")
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "NcclAllReduce", input=input, reduction=reduction,
                         num_devices=num_devices, shared_name=shared_name,
                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("reduction", _op.get_attr("reduction"), "T", _op.get_attr("T"),
            "num_devices", _op.get_attr("num_devices"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "NcclAllReduce", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def nccl_all_reduce_eager_fallback(input, reduction, num_devices, shared_name, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function nccl_all_reduce
  """
  _ctx = ctx if ctx else _context.context()
  reduction = _execute.make_str(reduction, "reduction")
  num_devices = _execute.make_int(num_devices, "num_devices")
  shared_name = _execute.make_str(shared_name, "shared_name")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("reduction", reduction, "T", _attr_T, "num_devices", num_devices,
  "shared_name", shared_name)
  _result = _execute.execute(b"NcclAllReduce", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NcclAllReduce", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def nccl_broadcast(input, shape, name=None):
  r"""Sends `input` to all devices that are connected to the output.

  Sends `input` to all devices that are connected to the output.

  The graph should be constructed so that all ops connected to the output have a
  valid device assignment, and the op itself is assigned one of these devices.

  input: The input to the broadcast.
  output: The same as input.
  shape: The shape of the input tensor.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`, `int32`, `int64`.
    shape: A `tf.TensorShape` or list of `ints`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "NcclBroadcast", name, _ctx._post_execution_callbacks, input, "shape",
        shape)
      return _result
    except _core._FallbackException:
      try:
        return nccl_broadcast_eager_fallback(
            input, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  shape = _execute.make_shape(shape, "shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "NcclBroadcast", input=input, shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "NcclBroadcast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def nccl_broadcast_eager_fallback(input, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function nccl_broadcast
  """
  _ctx = ctx if ctx else _context.context()
  shape = _execute.make_shape(shape, "shape")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "shape", shape)
  _result = _execute.execute(b"NcclBroadcast", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NcclBroadcast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def nccl_reduce(input, reduction, name=None):
  r"""Reduces `input` from `num_devices` using `reduction` to a single device.

  Reduces `input` from `num_devices` using `reduction` to a single device.

  The graph should be constructed so that all inputs have a valid device
  assignment, and the op itself is assigned one of these devices.

  input: The input to the reduction.
  data: the value of the reduction across all `num_devices` devices.
  reduction: the reduction operation to perform.

  Args:
    input: A list of at least 1 `Tensor` objects with the same type in: `half`, `float32`, `float64`, `int32`, `int64`.
    reduction: A `string` from: `"min", "max", "prod", "sum"`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "NcclReduce",
        name, _ctx._post_execution_callbacks, input, "reduction", reduction)
      return _result
    except _core._FallbackException:
      try:
        return nccl_reduce_eager_fallback(
            input, reduction=reduction, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(input, (list, tuple)):
    raise TypeError(
        "Expected list for 'input' argument to "
        "'nccl_reduce' Op, not %r." % input)
  _attr_num_devices = len(input)
  reduction = _execute.make_str(reduction, "reduction")
  _, _, _op = _op_def_lib._apply_op_helper(
        "NcclReduce", input=input, reduction=reduction, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("reduction", _op.get_attr("reduction"), "T", _op.get_attr("T"),
            "num_devices", _op.get_attr("num_devices"))
  _execute.record_gradient(
      "NcclReduce", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def nccl_reduce_eager_fallback(input, reduction, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function nccl_reduce
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(input, (list, tuple)):
    raise TypeError(
        "Expected list for 'input' argument to "
        "'nccl_reduce' Op, not %r." % input)
  _attr_num_devices = len(input)
  reduction = _execute.make_str(reduction, "reduction")
  _attr_T, input = _execute.args_to_matching_eager(list(input), _ctx)
  _inputs_flat = list(input)
  _attrs = ("reduction", reduction, "T", _attr_T, "num_devices",
  _attr_num_devices)
  _result = _execute.execute(b"NcclReduce", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NcclReduce", _inputs_flat, _attrs, _result, name)
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
#   name: "NcclAllReduce"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   attr {
#     name: "reduction"
#     type: "string"
#     allowed_values {
#       list {
#         s: "min"
#         s: "max"
#         s: "prod"
#         s: "sum"
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "num_devices"
#     type: "int"
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "NcclBroadcast"
#   input_arg {
#     name: "input"
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   is_stateful: true
# }
# op {
#   name: "NcclReduce"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#     number_attr: "num_devices"
#   }
#   output_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   attr {
#     name: "reduction"
#     type: "string"
#     allowed_values {
#       list {
#         s: "min"
#         s: "max"
#         s: "prod"
#         s: "sum"
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "num_devices"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\230\001\n\rNcclAllReduce\022\n\n\005input\"\001T\032\t\n\004data\"\001T\",\n\treduction\022\006string:\027\n\025\022\003min\022\003max\022\004prod\022\003sum\"\024\n\001T\022\004type:\t\n\0072\005\023\001\002\003\t\"\022\n\013num_devices\022\003int\"\025\n\013shared_name\022\006string\210\001\001\nQ\n\rNcclBroadcast\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\024\n\001T\022\004type:\t\n\0072\005\023\001\002\003\t\"\016\n\005shape\022\005shape\210\001\001\n\217\001\n\nNcclReduce\022\027\n\005input\"\001T*\013num_devices\032\t\n\004data\"\001T\",\n\treduction\022\006string:\027\n\025\022\003min\022\003max\022\004prod\022\003sum\"\024\n\001T\022\004type:\t\n\0072\005\023\001\002\003\t\"\026\n\013num_devices\022\003int(\0010\001\210\001\001")
