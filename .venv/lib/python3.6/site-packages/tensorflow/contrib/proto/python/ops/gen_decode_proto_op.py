"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_decode_proto_op_py.cc
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


_decode_proto_v2_outputs = ["sizes", "values"]
_DecodeProtoV2Output = _collections.namedtuple(
    "DecodeProtoV2", _decode_proto_v2_outputs)


@_dispatch.add_dispatch_list
@tf_export('decode_proto_v2')
def decode_proto_v2(bytes, message_type, field_names, output_types, descriptor_source="local://", message_format="binary", sanitize=False, name=None):
  r"""TODO: add doc.

  Args:
    bytes: A `Tensor` of type `string`.
    message_type: A `string`.
    field_names: A list of `strings`.
    output_types: A list of `tf.DTypes`.
    descriptor_source: An optional `string`. Defaults to `"local://"`.
    message_format: An optional `string`. Defaults to `"binary"`.
    sanitize: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sizes, values).

    sizes: A `Tensor` of type `int32`.
    values: A list of `Tensor` objects of type `output_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DecodeProtoV2", name, _ctx._post_execution_callbacks, bytes,
        "message_type", message_type, "field_names", field_names,
        "output_types", output_types, "descriptor_source", descriptor_source,
        "message_format", message_format, "sanitize", sanitize)
      _result = _DecodeProtoV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return decode_proto_v2_eager_fallback(
            bytes, message_type=message_type, field_names=field_names,
            output_types=output_types, descriptor_source=descriptor_source,
            message_format=message_format, sanitize=sanitize, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              decode_proto_v2, bytes=bytes, message_type=message_type,
                               field_names=field_names,
                               output_types=output_types,
                               descriptor_source=descriptor_source,
                               message_format=message_format,
                               sanitize=sanitize, name=name)
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
  message_type = _execute.make_str(message_type, "message_type")
  if not isinstance(field_names, (list, tuple)):
    raise TypeError(
        "Expected list for 'field_names' argument to "
        "'decode_proto_v2' Op, not %r." % field_names)
  field_names = [_execute.make_str(_s, "field_names") for _s in field_names]
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'decode_proto_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if descriptor_source is None:
    descriptor_source = "local://"
  descriptor_source = _execute.make_str(descriptor_source, "descriptor_source")
  if message_format is None:
    message_format = "binary"
  message_format = _execute.make_str(message_format, "message_format")
  if sanitize is None:
    sanitize = False
  sanitize = _execute.make_bool(sanitize, "sanitize")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "DecodeProtoV2", bytes=bytes, message_type=message_type,
                         field_names=field_names, output_types=output_types,
                         descriptor_source=descriptor_source,
                         message_format=message_format, sanitize=sanitize,
                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          decode_proto_v2, bytes=bytes, message_type=message_type,
                           field_names=field_names, output_types=output_types,
                           descriptor_source=descriptor_source,
                           message_format=message_format, sanitize=sanitize,
                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("message_type", _op.get_attr("message_type"), "field_names",
            _op.get_attr("field_names"), "output_types",
            _op.get_attr("output_types"), "descriptor_source",
            _op.get_attr("descriptor_source"), "message_format",
            _op.get_attr("message_format"), "sanitize",
            _op.get_attr("sanitize"))
  _execute.record_gradient(
      "DecodeProtoV2", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:]]
  _result = _DecodeProtoV2Output._make(_result)
  return _result



def decode_proto_v2_eager_fallback(bytes, message_type, field_names, output_types, descriptor_source="local://", message_format="binary", sanitize=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_proto_v2
  """
  _ctx = ctx if ctx else _context.context()
  message_type = _execute.make_str(message_type, "message_type")
  if not isinstance(field_names, (list, tuple)):
    raise TypeError(
        "Expected list for 'field_names' argument to "
        "'decode_proto_v2' Op, not %r." % field_names)
  field_names = [_execute.make_str(_s, "field_names") for _s in field_names]
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'decode_proto_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if descriptor_source is None:
    descriptor_source = "local://"
  descriptor_source = _execute.make_str(descriptor_source, "descriptor_source")
  if message_format is None:
    message_format = "binary"
  message_format = _execute.make_str(message_format, "message_format")
  if sanitize is None:
    sanitize = False
  sanitize = _execute.make_bool(sanitize, "sanitize")
  bytes = _ops.convert_to_tensor(bytes, _dtypes.string)
  _inputs_flat = [bytes]
  _attrs = ("message_type", message_type, "field_names", field_names,
  "output_types", output_types, "descriptor_source", descriptor_source,
  "message_format", message_format, "sanitize", sanitize)
  _result = _execute.execute(b"DecodeProtoV2", len(output_types) + 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "DecodeProtoV2", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:]]
  _result = _DecodeProtoV2Output._make(_result)
  return _result

_ops.RegisterShape("DecodeProtoV2")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "DecodeProtoV2"
#   input_arg {
#     name: "bytes"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "sizes"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "values"
#     type_list_attr: "output_types"
#   }
#   attr {
#     name: "message_type"
#     type: "string"
#   }
#   attr {
#     name: "field_names"
#     type: "list(string)"
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "descriptor_source"
#     type: "string"
#     default_value {
#       s: "local://"
#     }
#   }
#   attr {
#     name: "message_format"
#     type: "string"
#     default_value {
#       s: "binary"
#     }
#   }
#   attr {
#     name: "sanitize"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\363\001\n\rDecodeProtoV2\022\t\n\005bytes\030\007\032\t\n\005sizes\030\003\032\026\n\006values2\014output_types\"\026\n\014message_type\022\006string\"\033\n\013field_names\022\014list(string)\"\034\n\014output_types\022\nlist(type)(\001\"\'\n\021descriptor_source\022\006string\032\n\022\010local://\"\"\n\016message_format\022\006string\032\010\022\006binary\"\024\n\010sanitize\022\004bool\032\002(\000")
