"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: script_ops.cc
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


def eager_py_func(input, token, Tout, name=None):
  r"""Eagerly executes a python function to compute func(input)->output. The

  semantics of the input, output, and attributes are the same as those for
  PyFunc.

  Args:
    input: A list of `Tensor` objects.
    token: A `string`.
    Tout: A list of `tf.DTypes`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EagerPyFunc",
        name, _ctx._post_execution_callbacks, input, "token", token, "Tout",
        Tout)
      return _result
    except _core._FallbackException:
      try:
        return eager_py_func_eager_fallback(
            input, token=token, Tout=Tout, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  token = _execute.make_str(token, "token")
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'eager_py_func' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _, _, _op = _op_def_lib._apply_op_helper(
        "EagerPyFunc", input=input, token=token, Tout=Tout, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("token", _op.get_attr("token"), "Tin", _op.get_attr("Tin"),
            "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "EagerPyFunc", _inputs_flat, _attrs, _result, name)
  return _result



def eager_py_func_eager_fallback(input, token, Tout, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function eager_py_func
  """
  _ctx = ctx if ctx else _context.context()
  token = _execute.make_str(token, "token")
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'eager_py_func' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _attr_Tin, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  _inputs_flat = list(input)
  _attrs = ("token", token, "Tin", _attr_Tin, "Tout", Tout)
  _result = _execute.execute(b"EagerPyFunc", len(Tout), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EagerPyFunc", _inputs_flat, _attrs, _result, name)
  return _result


def py_func(input, token, Tout, name=None):
  r"""Invokes a python function to compute func(input)->output.

  This operation is considered stateful. For a stateless version, see
  PyFuncStateless.

  Args:
    input: A list of `Tensor` objects.
      List of Tensors that will provide input to the Op.
    token: A `string`.
      A token representing a registered python function in this address space.
    Tout: A list of `tf.DTypes`. Data types of the outputs from the op.
      The length of the list specifies the number of outputs.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "PyFunc", name,
        _ctx._post_execution_callbacks, input, "token", token, "Tout", Tout)
      return _result
    except _core._FallbackException:
      try:
        return py_func_eager_fallback(
            input, token=token, Tout=Tout, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  token = _execute.make_str(token, "token")
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'py_func' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _, _, _op = _op_def_lib._apply_op_helper(
        "PyFunc", input=input, token=token, Tout=Tout, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("token", _op.get_attr("token"), "Tin", _op.get_attr("Tin"),
            "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "PyFunc", _inputs_flat, _attrs, _result, name)
  return _result



def py_func_eager_fallback(input, token, Tout, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function py_func
  """
  _ctx = ctx if ctx else _context.context()
  token = _execute.make_str(token, "token")
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'py_func' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _attr_Tin, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  _inputs_flat = list(input)
  _attrs = ("token", token, "Tin", _attr_Tin, "Tout", Tout)
  _result = _execute.execute(b"PyFunc", len(Tout), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "PyFunc", _inputs_flat, _attrs, _result, name)
  return _result


def py_func_stateless(input, token, Tout, name=None):
  r"""A stateless version of PyFunc.

  Args:
    input: A list of `Tensor` objects.
    token: A `string`.
    Tout: A list of `tf.DTypes`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "PyFuncStateless", name, _ctx._post_execution_callbacks, input,
        "token", token, "Tout", Tout)
      return _result
    except _core._FallbackException:
      try:
        return py_func_stateless_eager_fallback(
            input, token=token, Tout=Tout, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  token = _execute.make_str(token, "token")
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'py_func_stateless' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _, _, _op = _op_def_lib._apply_op_helper(
        "PyFuncStateless", input=input, token=token, Tout=Tout, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("token", _op.get_attr("token"), "Tin", _op.get_attr("Tin"),
            "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "PyFuncStateless", _inputs_flat, _attrs, _result, name)
  return _result



def py_func_stateless_eager_fallback(input, token, Tout, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function py_func_stateless
  """
  _ctx = ctx if ctx else _context.context()
  token = _execute.make_str(token, "token")
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'py_func_stateless' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _attr_Tin, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  _inputs_flat = list(input)
  _attrs = ("token", token, "Tin", _attr_Tin, "Tout", Tout)
  _result = _execute.execute(b"PyFuncStateless", len(Tout),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "PyFuncStateless", _inputs_flat, _attrs, _result, name)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "EagerPyFunc"
#   input_arg {
#     name: "input"
#     type_list_attr: "Tin"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "Tout"
#   }
#   attr {
#     name: "token"
#     type: "string"
#   }
#   attr {
#     name: "Tin"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tout"
#     type: "list(type)"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "PyFunc"
#   input_arg {
#     name: "input"
#     type_list_attr: "Tin"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "Tout"
#   }
#   attr {
#     name: "token"
#     type: "string"
#   }
#   attr {
#     name: "Tin"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tout"
#     type: "list(type)"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "PyFuncStateless"
#   input_arg {
#     name: "input"
#     type_list_attr: "Tin"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "Tout"
#   }
#   attr {
#     name: "token"
#     type: "string"
#   }
#   attr {
#     name: "Tin"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tout"
#     type: "list(type)"
#     has_minimum: true
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\nj\n\013EagerPyFunc\022\014\n\005input2\003Tin\032\016\n\006output2\004Tout\"\017\n\005token\022\006string\"\023\n\003Tin\022\nlist(type)(\001\"\024\n\004Tout\022\nlist(type)(\001\210\001\001\ne\n\006PyFunc\022\014\n\005input2\003Tin\032\016\n\006output2\004Tout\"\017\n\005token\022\006string\"\023\n\003Tin\022\nlist(type)(\001\"\024\n\004Tout\022\nlist(type)(\001\210\001\001\nk\n\017PyFuncStateless\022\014\n\005input2\003Tin\032\016\n\006output2\004Tout\"\017\n\005token\022\006string\"\023\n\003Tin\022\nlist(type)(\001\"\024\n\004Tout\022\nlist(type)(\001")
