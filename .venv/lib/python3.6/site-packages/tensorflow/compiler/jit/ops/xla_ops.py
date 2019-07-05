"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: xla_ops_wrapper_py.cc
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
@tf_export('xla_cluster_output')
def xla_cluster_output(input, name=None):
  r"""Operator that connects the output of an XLA computation to other consumer graph nodes.

  Args:
    input: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "XlaClusterOutput", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return xla_cluster_output_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              xla_cluster_output, input=input, name=name)
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
        "XlaClusterOutput", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          xla_cluster_output, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "XlaClusterOutput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def xla_cluster_output_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function xla_cluster_output
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"XlaClusterOutput", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "XlaClusterOutput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("XlaClusterOutput")(None)


@_dispatch.add_dispatch_list
@tf_export('xla_launch')
def xla_launch(constants, args, resources, Tresults, function, name=None):
  r"""XLA Launch Op. For use by the XLA JIT only.

  Args:
    constants: A list of `Tensor` objects.
    args: A list of `Tensor` objects.
    resources: A list of `Tensor` objects with type `resource`.
    Tresults: A list of `tf.DTypes`.
    function: A function decorated with @Defun.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tresults`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "XlaLaunch",
        name, _ctx._post_execution_callbacks, constants, args, resources,
        "Tresults", Tresults, "function", function)
      return _result
    except _core._FallbackException:
      try:
        return xla_launch_eager_fallback(
            constants, args, resources, Tresults=Tresults, function=function,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              xla_launch, constants=constants, args=args, resources=resources,
                          Tresults=Tresults, function=function, name=name)
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
  if not isinstance(resources, (list, tuple)):
    raise TypeError(
        "Expected list for 'resources' argument to "
        "'xla_launch' Op, not %r." % resources)
  _attr_Nresources = len(resources)
  if not isinstance(Tresults, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tresults' argument to "
        "'xla_launch' Op, not %r." % Tresults)
  Tresults = [_execute.make_type(_t, "Tresults") for _t in Tresults]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "XlaLaunch", constants=constants, args=args, resources=resources,
                     Tresults=Tresults, function=function, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          xla_launch, constants=constants, args=args, resources=resources,
                      Tresults=Tresults, function=function, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("Tconstants", _op.get_attr("Tconstants"), "Targs",
            _op.get_attr("Targs"), "Nresources", _op.get_attr("Nresources"),
            "Tresults", _op.get_attr("Tresults"), "function",
            _op.get_attr("function"))
  _execute.record_gradient(
      "XlaLaunch", _inputs_flat, _attrs, _result, name)
  return _result



def xla_launch_eager_fallback(constants, args, resources, Tresults, function, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function xla_launch
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(resources, (list, tuple)):
    raise TypeError(
        "Expected list for 'resources' argument to "
        "'xla_launch' Op, not %r." % resources)
  _attr_Nresources = len(resources)
  if not isinstance(Tresults, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tresults' argument to "
        "'xla_launch' Op, not %r." % Tresults)
  Tresults = [_execute.make_type(_t, "Tresults") for _t in Tresults]
  _attr_Tconstants, constants = _execute.convert_to_mixed_eager_tensors(constants, _ctx)
  _attr_Targs, args = _execute.convert_to_mixed_eager_tensors(args, _ctx)
  resources = _ops.convert_n_to_tensor(resources, _dtypes.resource)
  _inputs_flat = list(constants) + list(args) + list(resources)
  _attrs = ("Tconstants", _attr_Tconstants, "Targs", _attr_Targs,
  "Nresources", _attr_Nresources, "Tresults", Tresults, "function", function)
  _result = _execute.execute(b"XlaLaunch", len(Tresults), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "XlaLaunch", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("XlaLaunch")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "XlaClusterOutput"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "outputs"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "XlaLaunch"
#   input_arg {
#     name: "constants"
#     type_list_attr: "Tconstants"
#   }
#   input_arg {
#     name: "args"
#     type_list_attr: "Targs"
#   }
#   input_arg {
#     name: "resources"
#     type: DT_RESOURCE
#     number_attr: "Nresources"
#   }
#   output_arg {
#     name: "results"
#     type_list_attr: "Tresults"
#   }
#   attr {
#     name: "Tconstants"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Targs"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Nresources"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "Tresults"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "function"
#     type: "func"
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n7\n\020XlaClusterOutput\022\n\n\005input\"\001T\032\014\n\007outputs\"\001T\"\t\n\001T\022\004type\n\332\001\n\tXlaLaunch\022\027\n\tconstants2\nTconstants\022\r\n\004args2\005Targs\022\031\n\tresources\030\024*\nNresources\032\023\n\007results2\010Tresults\"\032\n\nTconstants\022\nlist(type)(\001\"\025\n\005Targs\022\nlist(type)(\001\"\023\n\nNresources\022\003int(\001\"\030\n\010Tresults\022\nlist(type)(\001\"\020\n\010function\022\004func\210\001\001")
