"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: collective_ops.cc
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


def collective_bcast_recv(T, group_size, group_key, instance_key, shape, name=None):
  r"""Receives a tensor value broadcast from another device.

  Args:
    T: A `tf.DType` from: `tf.float32, tf.half, tf.float64, tf.int32, tf.int64`.
    group_size: An `int`.
    group_key: An `int`.
    instance_key: An `int`.
    shape: A `tf.TensorShape` or list of `ints`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `T`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CollectiveBcastRecv", name, _ctx._post_execution_callbacks, "T", T,
        "group_size", group_size, "group_key", group_key, "instance_key",
        instance_key, "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return collective_bcast_recv_eager_fallback(
            T=T, group_size=group_size, group_key=group_key,
            instance_key=instance_key, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  T = _execute.make_type(T, "T")
  group_size = _execute.make_int(group_size, "group_size")
  group_key = _execute.make_int(group_key, "group_key")
  instance_key = _execute.make_int(instance_key, "instance_key")
  shape = _execute.make_shape(shape, "shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "CollectiveBcastRecv", T=T, group_size=group_size,
                               group_key=group_key, instance_key=instance_key,
                               shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "group_size", _op.get_attr("group_size"),
            "group_key", _op.get_attr("group_key"), "instance_key",
            _op.get_attr("instance_key"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "CollectiveBcastRecv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def collective_bcast_recv_eager_fallback(T, group_size, group_key, instance_key, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function collective_bcast_recv
  """
  _ctx = ctx if ctx else _context.context()
  T = _execute.make_type(T, "T")
  group_size = _execute.make_int(group_size, "group_size")
  group_key = _execute.make_int(group_key, "group_key")
  instance_key = _execute.make_int(instance_key, "instance_key")
  shape = _execute.make_shape(shape, "shape")
  _inputs_flat = []
  _attrs = ("T", T, "group_size", group_size, "group_key", group_key,
  "instance_key", instance_key, "shape", shape)
  _result = _execute.execute(b"CollectiveBcastRecv", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CollectiveBcastRecv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def collective_bcast_send(input, group_size, group_key, instance_key, shape, name=None):
  r"""Broadcasts a tensor value to one or more other devices.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `half`, `float64`, `int32`, `int64`.
    group_size: An `int`.
    group_key: An `int`.
    instance_key: An `int`.
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
        "CollectiveBcastSend", name, _ctx._post_execution_callbacks, input,
        "group_size", group_size, "group_key", group_key, "instance_key",
        instance_key, "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return collective_bcast_send_eager_fallback(
            input, group_size=group_size, group_key=group_key,
            instance_key=instance_key, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  group_size = _execute.make_int(group_size, "group_size")
  group_key = _execute.make_int(group_key, "group_key")
  instance_key = _execute.make_int(instance_key, "instance_key")
  shape = _execute.make_shape(shape, "shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "CollectiveBcastSend", input=input, group_size=group_size,
                               group_key=group_key, instance_key=instance_key,
                               shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "group_size", _op.get_attr("group_size"),
            "group_key", _op.get_attr("group_key"), "instance_key",
            _op.get_attr("instance_key"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "CollectiveBcastSend", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def collective_bcast_send_eager_fallback(input, group_size, group_key, instance_key, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function collective_bcast_send
  """
  _ctx = ctx if ctx else _context.context()
  group_size = _execute.make_int(group_size, "group_size")
  group_key = _execute.make_int(group_key, "group_key")
  instance_key = _execute.make_int(instance_key, "instance_key")
  shape = _execute.make_shape(shape, "shape")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "group_size", group_size, "group_key", group_key,
  "instance_key", instance_key, "shape", shape)
  _result = _execute.execute(b"CollectiveBcastSend", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CollectiveBcastSend", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def collective_reduce(input, group_size, group_key, instance_key, merge_op, final_op, subdiv_offsets, name=None):
  r"""Mutually reduces multiple tensors of identical type and shape.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `half`, `float64`, `int32`, `int64`.
    group_size: An `int`.
    group_key: An `int`.
    instance_key: An `int`.
    merge_op: A `string` from: `"Min", "Max", "Mul", "Add"`.
    final_op: A `string` from: `"Id", "Div"`.
    subdiv_offsets: A list of `ints`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CollectiveReduce", name, _ctx._post_execution_callbacks, input,
        "group_size", group_size, "group_key", group_key, "instance_key",
        instance_key, "merge_op", merge_op, "final_op", final_op,
        "subdiv_offsets", subdiv_offsets)
      return _result
    except _core._FallbackException:
      try:
        return collective_reduce_eager_fallback(
            input, group_size=group_size, group_key=group_key,
            instance_key=instance_key, merge_op=merge_op, final_op=final_op,
            subdiv_offsets=subdiv_offsets, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  group_size = _execute.make_int(group_size, "group_size")
  group_key = _execute.make_int(group_key, "group_key")
  instance_key = _execute.make_int(instance_key, "instance_key")
  merge_op = _execute.make_str(merge_op, "merge_op")
  final_op = _execute.make_str(final_op, "final_op")
  if not isinstance(subdiv_offsets, (list, tuple)):
    raise TypeError(
        "Expected list for 'subdiv_offsets' argument to "
        "'collective_reduce' Op, not %r." % subdiv_offsets)
  subdiv_offsets = [_execute.make_int(_i, "subdiv_offsets") for _i in subdiv_offsets]
  _, _, _op = _op_def_lib._apply_op_helper(
        "CollectiveReduce", input=input, group_size=group_size,
                            group_key=group_key, instance_key=instance_key,
                            merge_op=merge_op, final_op=final_op,
                            subdiv_offsets=subdiv_offsets, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "group_size", _op.get_attr("group_size"),
            "group_key", _op.get_attr("group_key"), "instance_key",
            _op.get_attr("instance_key"), "merge_op",
            _op.get_attr("merge_op"), "final_op", _op.get_attr("final_op"),
            "subdiv_offsets", _op.get_attr("subdiv_offsets"))
  _execute.record_gradient(
      "CollectiveReduce", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def collective_reduce_eager_fallback(input, group_size, group_key, instance_key, merge_op, final_op, subdiv_offsets, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function collective_reduce
  """
  _ctx = ctx if ctx else _context.context()
  group_size = _execute.make_int(group_size, "group_size")
  group_key = _execute.make_int(group_key, "group_key")
  instance_key = _execute.make_int(instance_key, "instance_key")
  merge_op = _execute.make_str(merge_op, "merge_op")
  final_op = _execute.make_str(final_op, "final_op")
  if not isinstance(subdiv_offsets, (list, tuple)):
    raise TypeError(
        "Expected list for 'subdiv_offsets' argument to "
        "'collective_reduce' Op, not %r." % subdiv_offsets)
  subdiv_offsets = [_execute.make_int(_i, "subdiv_offsets") for _i in subdiv_offsets]
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "group_size", group_size, "group_key", group_key,
  "instance_key", instance_key, "merge_op", merge_op, "final_op", final_op,
  "subdiv_offsets", subdiv_offsets)
  _result = _execute.execute(b"CollectiveReduce", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CollectiveReduce", _inputs_flat, _attrs, _result, name)
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
#   name: "CollectiveBcastRecv"
#   output_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_HALF
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "group_size"
#     type: "int"
#   }
#   attr {
#     name: "group_key"
#     type: "int"
#   }
#   attr {
#     name: "instance_key"
#     type: "int"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   is_stateful: true
# }
# op {
#   name: "CollectiveBcastSend"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_HALF
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "group_size"
#     type: "int"
#   }
#   attr {
#     name: "group_key"
#     type: "int"
#   }
#   attr {
#     name: "instance_key"
#     type: "int"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   is_stateful: true
# }
# op {
#   name: "CollectiveReduce"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_HALF
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "group_size"
#     type: "int"
#   }
#   attr {
#     name: "group_key"
#     type: "int"
#   }
#   attr {
#     name: "instance_key"
#     type: "int"
#   }
#   attr {
#     name: "merge_op"
#     type: "string"
#     allowed_values {
#       list {
#         s: "Min"
#         s: "Max"
#         s: "Mul"
#         s: "Add"
#       }
#     }
#   }
#   attr {
#     name: "final_op"
#     type: "string"
#     allowed_values {
#       list {
#         s: "Id"
#         s: "Div"
#       }
#     }
#   }
#   attr {
#     name: "subdiv_offsets"
#     type: "list(int)"
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\203\001\n\023CollectiveBcastRecv\032\t\n\004data\"\001T\"\024\n\001T\022\004type:\t\n\0072\005\001\023\002\003\t\"\021\n\ngroup_size\022\003int\"\020\n\tgroup_key\022\003int\"\023\n\014instance_key\022\003int\"\016\n\005shape\022\005shape\210\001\001\n\217\001\n\023CollectiveBcastSend\022\n\n\005input\"\001T\032\t\n\004data\"\001T\"\024\n\001T\022\004type:\t\n\0072\005\001\023\002\003\t\"\021\n\ngroup_size\022\003int\"\020\n\tgroup_key\022\003int\"\023\n\014instance_key\022\003int\"\016\n\005shape\022\005shape\210\001\001\n\346\001\n\020CollectiveReduce\022\n\n\005input\"\001T\032\t\n\004data\"\001T\"\024\n\001T\022\004type:\t\n\0072\005\001\023\002\003\t\"\021\n\ngroup_size\022\003int\"\020\n\tgroup_key\022\003int\"\023\n\014instance_key\022\003int\"*\n\010merge_op\022\006string:\026\n\024\022\003Min\022\003Max\022\003Mul\022\003Add\"\037\n\010final_op\022\006string:\013\n\t\022\002Id\022\003Div\"\033\n\016subdiv_offsets\022\tlist(int)\210\001\001")
