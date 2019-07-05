"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_encode_proto_op_py.cc
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
@tf_export('encode_proto')
def encode_proto(sizes, values, field_names, message_type, descriptor_source="local://", name=None):
  r"""TODO: add doc.

  Args:
    sizes: A `Tensor` of type `int32`.
    values: A list of `Tensor` objects.
    field_names: A list of `strings`.
    message_type: A `string`.
    descriptor_source: An optional `string`. Defaults to `"local://"`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EncodeProto",
        name, _ctx._post_execution_callbacks, sizes, values, "field_names",
        field_names, "message_type", message_type, "descriptor_source",
        descriptor_source)
      return _result
    except _core._FallbackException:
      try:
        return encode_proto_eager_fallback(
            sizes, values, field_names=field_names, message_type=message_type,
            descriptor_source=descriptor_source, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              encode_proto, sizes=sizes, values=values,
                            field_names=field_names,
                            message_type=message_type,
                            descriptor_source=descriptor_source, name=name)
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
  if not isinstance(field_names, (list, tuple)):
    raise TypeError(
        "Expected list for 'field_names' argument to "
        "'encode_proto' Op, not %r." % field_names)
  field_names = [_execute.make_str(_s, "field_names") for _s in field_names]
  message_type = _execute.make_str(message_type, "message_type")
  if descriptor_source is None:
    descriptor_source = "local://"
  descriptor_source = _execute.make_str(descriptor_source, "descriptor_source")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "EncodeProto", sizes=sizes, values=values, field_names=field_names,
                       message_type=message_type,
                       descriptor_source=descriptor_source, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          encode_proto, sizes=sizes, values=values, field_names=field_names,
                        message_type=message_type,
                        descriptor_source=descriptor_source, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("field_names", _op.get_attr("field_names"), "message_type",
            _op.get_attr("message_type"), "descriptor_source",
            _op.get_attr("descriptor_source"), "Tinput_types",
            _op.get_attr("Tinput_types"))
  _execute.record_gradient(
      "EncodeProto", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def encode_proto_eager_fallback(sizes, values, field_names, message_type, descriptor_source="local://", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function encode_proto
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(field_names, (list, tuple)):
    raise TypeError(
        "Expected list for 'field_names' argument to "
        "'encode_proto' Op, not %r." % field_names)
  field_names = [_execute.make_str(_s, "field_names") for _s in field_names]
  message_type = _execute.make_str(message_type, "message_type")
  if descriptor_source is None:
    descriptor_source = "local://"
  descriptor_source = _execute.make_str(descriptor_source, "descriptor_source")
  _attr_Tinput_types, values = _execute.convert_to_mixed_eager_tensors(values, _ctx)
  sizes = _ops.convert_to_tensor(sizes, _dtypes.int32)
  _inputs_flat = [sizes] + list(values)
  _attrs = ("field_names", field_names, "message_type", message_type,
  "descriptor_source", descriptor_source, "Tinput_types", _attr_Tinput_types)
  _result = _execute.execute(b"EncodeProto", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EncodeProto", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("EncodeProto")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "EncodeProto"
#   input_arg {
#     name: "sizes"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "values"
#     type_list_attr: "Tinput_types"
#   }
#   output_arg {
#     name: "bytes"
#     type: DT_STRING
#   }
#   attr {
#     name: "field_names"
#     type: "list(string)"
#   }
#   attr {
#     name: "message_type"
#     type: "string"
#   }
#   attr {
#     name: "descriptor_source"
#     type: "string"
#     default_value {
#       s: "local://"
#     }
#   }
#   attr {
#     name: "Tinput_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\271\001\n\013EncodeProto\022\t\n\005sizes\030\003\022\026\n\006values2\014Tinput_types\032\t\n\005bytes\030\007\"\033\n\013field_names\022\014list(string)\"\026\n\014message_type\022\006string\"\'\n\021descriptor_source\022\006string\032\n\022\010local://\"\036\n\014Tinput_types\022\nlist(type)(\0010\001")
