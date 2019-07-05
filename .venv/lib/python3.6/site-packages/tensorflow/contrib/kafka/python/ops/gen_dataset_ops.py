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
@tf_export('kafka_dataset')
def kafka_dataset(topics, servers, group, eof, timeout, name=None):
  r"""Creates a dataset that emits the messages of one or more Kafka topics.

  Args:
    topics: A `Tensor` of type `string`.
      A `tf.string` tensor containing one or more subscriptions,
      in the format of [topic:partition:offset:length],
      by default length is -1 for unlimited.
    servers: A `Tensor` of type `string`. A list of bootstrap servers.
    group: A `Tensor` of type `string`. The consumer group id.
    eof: A `Tensor` of type `bool`.
      If True, the kafka reader will stop on EOF.
    timeout: A `Tensor` of type `int64`.
      The timeout value for the Kafka Consumer to wait
      (in millisecond).
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "KafkaDataset",
        name, _ctx._post_execution_callbacks, topics, servers, group, eof,
        timeout)
      return _result
    except _core._FallbackException:
      try:
        return kafka_dataset_eager_fallback(
            topics, servers, group, eof, timeout, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              kafka_dataset, topics=topics, servers=servers, group=group,
                             eof=eof, timeout=timeout, name=name)
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
        "KafkaDataset", topics=topics, servers=servers, group=group, eof=eof,
                        timeout=timeout, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          kafka_dataset, topics=topics, servers=servers, group=group, eof=eof,
                         timeout=timeout, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "KafkaDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def kafka_dataset_eager_fallback(topics, servers, group, eof, timeout, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function kafka_dataset
  """
  _ctx = ctx if ctx else _context.context()
  topics = _ops.convert_to_tensor(topics, _dtypes.string)
  servers = _ops.convert_to_tensor(servers, _dtypes.string)
  group = _ops.convert_to_tensor(group, _dtypes.string)
  eof = _ops.convert_to_tensor(eof, _dtypes.bool)
  timeout = _ops.convert_to_tensor(timeout, _dtypes.int64)
  _inputs_flat = [topics, servers, group, eof, timeout]
  _attrs = None
  _result = _execute.execute(b"KafkaDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "KafkaDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("KafkaDataset")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "KafkaDataset"
#   input_arg {
#     name: "topics"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "servers"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "group"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "eof"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "timeout"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\nW\n\014KafkaDataset\022\n\n\006topics\030\007\022\013\n\007servers\030\007\022\t\n\005group\030\007\022\007\n\003eof\030\n\022\013\n\007timeout\030\t\032\n\n\006handle\030\025\210\001\001")
