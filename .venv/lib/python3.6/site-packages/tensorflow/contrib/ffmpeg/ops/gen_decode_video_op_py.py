"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: decode_video_op_py.cc
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
@tf_export('decode_video')
def decode_video(contents, name=None):
  r"""Processes the contents of an video file into a tensor using FFmpeg to decode

  the file.

  Args:
    contents: A `Tensor` of type `string`.
      The binary contents of the video file to decode. This is a
      scalar.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `uint8`.
    A rank-4 `Tensor` that has `[frames, height, width, 3]` RGB as output.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DecodeVideo",
        name, _ctx._post_execution_callbacks, contents)
      return _result
    except _core._FallbackException:
      try:
        return decode_video_eager_fallback(
            contents, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              decode_video, contents=contents, name=name)
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
        "DecodeVideo", contents=contents, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          decode_video, contents=contents, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "DecodeVideo", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def decode_video_eager_fallback(contents, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_video
  """
  _ctx = ctx if ctx else _context.context()
  contents = _ops.convert_to_tensor(contents, _dtypes.string)
  _inputs_flat = [contents]
  _attrs = None
  _result = _execute.execute(b"DecodeVideo", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DecodeVideo", _inputs_flat, _attrs, _result, name)
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
#   name: "DecodeVideo"
#   input_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_UINT8
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\'\n\013DecodeVideo\022\014\n\010contents\030\007\032\n\n\006output\030\004")
