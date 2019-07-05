"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: memory_stats_ops.cc
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
@tf_export('bytes_in_use')
def bytes_in_use(name=None):
  r"""TODO: add doc.

  Args:
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BytesInUse",
        name, _ctx._post_execution_callbacks)
      return _result
    except _core._FallbackException:
      try:
        return bytes_in_use_eager_fallback(
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bytes_in_use, name=name)
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
        "BytesInUse", name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bytes_in_use, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BytesInUse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bytes_in_use_eager_fallback(name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bytes_in_use
  """
  _ctx = ctx if ctx else _context.context()
  _inputs_flat = []
  _attrs = None
  _result = _execute.execute(b"BytesInUse", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BytesInUse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BytesInUse")(None)


@_dispatch.add_dispatch_list
@tf_export('bytes_limit')
def bytes_limit(name=None):
  r"""TODO: add doc.

  Args:
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BytesLimit",
        name, _ctx._post_execution_callbacks)
      return _result
    except _core._FallbackException:
      try:
        return bytes_limit_eager_fallback(
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bytes_limit, name=name)
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
        "BytesLimit", name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bytes_limit, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BytesLimit", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bytes_limit_eager_fallback(name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bytes_limit
  """
  _ctx = ctx if ctx else _context.context()
  _inputs_flat = []
  _attrs = None
  _result = _execute.execute(b"BytesLimit", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BytesLimit", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BytesLimit")(None)


@_dispatch.add_dispatch_list
@tf_export('max_bytes_in_use')
def max_bytes_in_use(name=None):
  r"""TODO: add doc.

  Args:
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MaxBytesInUse", name, _ctx._post_execution_callbacks)
      return _result
    except _core._FallbackException:
      try:
        return max_bytes_in_use_eager_fallback(
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              max_bytes_in_use, name=name)
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
        "MaxBytesInUse", name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          max_bytes_in_use, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "MaxBytesInUse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_bytes_in_use_eager_fallback(name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_bytes_in_use
  """
  _ctx = ctx if ctx else _context.context()
  _inputs_flat = []
  _attrs = None
  _result = _execute.execute(b"MaxBytesInUse", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxBytesInUse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("MaxBytesInUse")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BytesInUse"
#   output_arg {
#     name: "out"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
# op {
#   name: "BytesLimit"
#   output_arg {
#     name: "out"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
# op {
#   name: "MaxBytesInUse"
#   output_arg {
#     name: "out"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\030\n\nBytesInUse\032\007\n\003out\030\t\210\001\001\n\030\n\nBytesLimit\032\007\n\003out\030\t\210\001\001\n\033\n\rMaxBytesInUse\032\007\n\003out\030\t\210\001\001")
