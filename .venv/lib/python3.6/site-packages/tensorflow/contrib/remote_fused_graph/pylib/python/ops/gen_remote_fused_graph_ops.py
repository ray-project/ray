"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_remote_fused_graph_ops.cc
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
@tf_export('remote_fused_graph_execute')
def remote_fused_graph_execute(inputs, Toutputs, serialized_remote_fused_graph_execute_info, name=None):
  r"""TODO: add doc.

  Args:
    inputs: A list of `Tensor` objects.
    Toutputs: A list of `tf.DTypes`.
    serialized_remote_fused_graph_execute_info: A `string`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Toutputs`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RemoteFusedGraphExecute", name, _ctx._post_execution_callbacks,
        inputs, "Toutputs", Toutputs,
        "serialized_remote_fused_graph_execute_info",
        serialized_remote_fused_graph_execute_info)
      return _result
    except _core._FallbackException:
      try:
        return remote_fused_graph_execute_eager_fallback(
            inputs, Toutputs=Toutputs,
            serialized_remote_fused_graph_execute_info=serialized_remote_fused_graph_execute_info,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              remote_fused_graph_execute, inputs=inputs, Toutputs=Toutputs,
                                          serialized_remote_fused_graph_execute_info=serialized_remote_fused_graph_execute_info,
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
  if not isinstance(Toutputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'Toutputs' argument to "
        "'remote_fused_graph_execute' Op, not %r." % Toutputs)
  Toutputs = [_execute.make_type(_t, "Toutputs") for _t in Toutputs]
  serialized_remote_fused_graph_execute_info = _execute.make_str(serialized_remote_fused_graph_execute_info, "serialized_remote_fused_graph_execute_info")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RemoteFusedGraphExecute", inputs=inputs, Toutputs=Toutputs,
                                   serialized_remote_fused_graph_execute_info=serialized_remote_fused_graph_execute_info,
                                   name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          remote_fused_graph_execute, inputs=inputs, Toutputs=Toutputs,
                                      serialized_remote_fused_graph_execute_info=serialized_remote_fused_graph_execute_info,
                                      name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tinputs", _op.get_attr("Tinputs"), "Toutputs",
            _op.get_attr("Toutputs"),
            "serialized_remote_fused_graph_execute_info",
            _op.get_attr("serialized_remote_fused_graph_execute_info"))
  _execute.record_gradient(
      "RemoteFusedGraphExecute", _inputs_flat, _attrs, _result, name)
  return _result



def remote_fused_graph_execute_eager_fallback(inputs, Toutputs, serialized_remote_fused_graph_execute_info, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function remote_fused_graph_execute
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(Toutputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'Toutputs' argument to "
        "'remote_fused_graph_execute' Op, not %r." % Toutputs)
  Toutputs = [_execute.make_type(_t, "Toutputs") for _t in Toutputs]
  serialized_remote_fused_graph_execute_info = _execute.make_str(serialized_remote_fused_graph_execute_info, "serialized_remote_fused_graph_execute_info")
  _attr_Tinputs, inputs = _execute.convert_to_mixed_eager_tensors(inputs, _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("Tinputs", _attr_Tinputs, "Toutputs", Toutputs,
  "serialized_remote_fused_graph_execute_info",
  serialized_remote_fused_graph_execute_info)
  _result = _execute.execute(b"RemoteFusedGraphExecute", len(Toutputs),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RemoteFusedGraphExecute", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("RemoteFusedGraphExecute")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "RemoteFusedGraphExecute"
#   input_arg {
#     name: "inputs"
#     type_list_attr: "Tinputs"
#   }
#   output_arg {
#     name: "outputs"
#     type_list_attr: "Toutputs"
#   }
#   attr {
#     name: "Tinputs"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Toutputs"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "serialized_remote_fused_graph_execute_info"
#     type: "string"
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\252\001\n\027RemoteFusedGraphExecute\022\021\n\006inputs2\007Tinputs\032\023\n\007outputs2\010Toutputs\"\027\n\007Tinputs\022\nlist(type)(\001\"\030\n\010Toutputs\022\nlist(type)(\001\"4\n*serialized_remote_fused_graph_execute_info\022\006string")
