"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_dataset_ops.cc
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
@tf_export('kinesis_dataset')
def kinesis_dataset(stream, shard, read_indefinitely, interval, name=None):
  r"""Creates a dataset that emits the messages of one or more Kinesis topics.

  Args:
    stream: A `Tensor` of type `string`.
      A `tf.string` tensor containing the name of the stream.
    shard: A `Tensor` of type `string`.
      A `tf.string` tensor containing the id of the shard.
    read_indefinitely: A `Tensor` of type `bool`.
      If `True`, the Kinesis dataset will keep retry
      again on `EOF` after the `interval` period. If `False`, then
      the dataset will stop on `EOF`. The default value is `True`.
    interval: A `Tensor` of type `int64`.
      The interval for the Kinesis Client to wait before
      it tries to get records again (in millisecond).
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "KinesisDataset", name, _ctx._post_execution_callbacks, stream, shard,
        read_indefinitely, interval)
      return _result
    except _core._FallbackException:
      try:
        return kinesis_dataset_eager_fallback(
            stream, shard, read_indefinitely, interval, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              kinesis_dataset, stream=stream, shard=shard,
                               read_indefinitely=read_indefinitely,
                               interval=interval, name=name)
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
        "KinesisDataset", stream=stream, shard=shard,
                          read_indefinitely=read_indefinitely,
                          interval=interval, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          kinesis_dataset, stream=stream, shard=shard,
                           read_indefinitely=read_indefinitely,
                           interval=interval, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "KinesisDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def kinesis_dataset_eager_fallback(stream, shard, read_indefinitely, interval, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function kinesis_dataset
  """
  _ctx = ctx if ctx else _context.context()
  stream = _ops.convert_to_tensor(stream, _dtypes.string)
  shard = _ops.convert_to_tensor(shard, _dtypes.string)
  read_indefinitely = _ops.convert_to_tensor(read_indefinitely, _dtypes.bool)
  interval = _ops.convert_to_tensor(interval, _dtypes.int64)
  _inputs_flat = [stream, shard, read_indefinitely, interval]
  _attrs = None
  _result = _execute.execute(b"KinesisDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "KinesisDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("KinesisDataset")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "KinesisDataset"
#   input_arg {
#     name: "stream"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "shard"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "read_indefinitely"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "interval"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n[\n\016KinesisDataset\022\n\n\006stream\030\007\022\t\n\005shard\030\007\022\025\n\021read_indefinitely\030\n\022\014\n\010interval\030\t\032\n\n\006handle\030\025\210\001\001")
