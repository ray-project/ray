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
@tf_export('ignite_dataset')
def ignite_dataset(cache_name, host, port, local, part, page_size, schema, permutation, name=None):
  r"""IgniteDataset that allows to get data from Apache Ignite.

  Apache Ignite is a memory-centric distributed database, caching, and processing
  platform for transactional, analytical, and streaming workloads, delivering
  in-memory speeds at petabyte scale. This contrib package contains an
  integration between Apache Ignite and TensorFlow. The integration is based on
  tf.data from TensorFlow side and Binary Client Protocol from Apache Ignite side.
  It allows to use Apache Ignite as a datasource for neural network training,
  inference and all other computations supported by TensorFlow. Ignite Dataset
  is based on Apache Ignite Binary Client Protocol.

  Args:
    cache_name: A `Tensor` of type `string`. Ignite Cache Name.
    host: A `Tensor` of type `string`. Ignite Thin Client Host.
    port: A `Tensor` of type `int32`. Ignite Thin Client Port.
    local: A `Tensor` of type `bool`.
      Local flag that defines that data should be fetched from local host only.
    part: A `Tensor` of type `int32`. Partition data should be fetched from.
    page_size: A `Tensor` of type `int32`. Page size for Ignite Thin Client.
    schema: A `Tensor` of type `int32`.
      Internal structure that defines schema of cache objects.
    permutation: A `Tensor` of type `int32`.
      Internal structure that defines permutation of cache objects.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "IgniteDataset", name, _ctx._post_execution_callbacks, cache_name,
        host, port, local, part, page_size, schema, permutation)
      return _result
    except _core._FallbackException:
      try:
        return ignite_dataset_eager_fallback(
            cache_name, host, port, local, part, page_size, schema,
            permutation, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              ignite_dataset, cache_name=cache_name, host=host, port=port,
                              local=local, part=part, page_size=page_size,
                              schema=schema, permutation=permutation,
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
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "IgniteDataset", cache_name=cache_name, host=host, port=port,
                         local=local, part=part, page_size=page_size,
                         schema=schema, permutation=permutation, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          ignite_dataset, cache_name=cache_name, host=host, port=port,
                          local=local, part=part, page_size=page_size,
                          schema=schema, permutation=permutation, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "IgniteDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ignite_dataset_eager_fallback(cache_name, host, port, local, part, page_size, schema, permutation, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ignite_dataset
  """
  _ctx = ctx if ctx else _context.context()
  cache_name = _ops.convert_to_tensor(cache_name, _dtypes.string)
  host = _ops.convert_to_tensor(host, _dtypes.string)
  port = _ops.convert_to_tensor(port, _dtypes.int32)
  local = _ops.convert_to_tensor(local, _dtypes.bool)
  part = _ops.convert_to_tensor(part, _dtypes.int32)
  page_size = _ops.convert_to_tensor(page_size, _dtypes.int32)
  schema = _ops.convert_to_tensor(schema, _dtypes.int32)
  permutation = _ops.convert_to_tensor(permutation, _dtypes.int32)
  _inputs_flat = [cache_name, host, port, local, part, page_size, schema, permutation]
  _attrs = None
  _result = _execute.execute(b"IgniteDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "IgniteDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("IgniteDataset")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "IgniteDataset"
#   input_arg {
#     name: "cache_name"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "host"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "port"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "local"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "part"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "page_size"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "schema"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "permutation"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\203\001\n\rIgniteDataset\022\016\n\ncache_name\030\007\022\010\n\004host\030\007\022\010\n\004port\030\003\022\t\n\005local\030\n\022\010\n\004part\030\003\022\r\n\tpage_size\030\003\022\n\n\006schema\030\003\022\017\n\013permutation\030\003\032\n\n\006handle\030\025\210\001\001")
