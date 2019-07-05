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
@tf_export('sequence_file_dataset')
def sequence_file_dataset(filenames, output_types, name=None):
  r"""TODO: add doc.

  Args:
    filenames: A `Tensor` of type `string`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SequenceFileDataset", name, _ctx._post_execution_callbacks,
        filenames, "output_types", output_types)
      return _result
    except _core._FallbackException:
      try:
        return sequence_file_dataset_eager_fallback(
            filenames, output_types=output_types, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              sequence_file_dataset, filenames=filenames,
                                     output_types=output_types, name=name)
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
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'sequence_file_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "SequenceFileDataset", filenames=filenames, output_types=output_types,
                               name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sequence_file_dataset, filenames=filenames,
                                 output_types=output_types, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"))
  _execute.record_gradient(
      "SequenceFileDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sequence_file_dataset_eager_fallback(filenames, output_types, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sequence_file_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'sequence_file_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  filenames = _ops.convert_to_tensor(filenames, _dtypes.string)
  _inputs_flat = [filenames]
  _attrs = ("output_types", output_types)
  _result = _execute.execute(b"SequenceFileDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SequenceFileDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("SequenceFileDataset")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "SequenceFileDataset"
#   input_arg {
#     name: "filenames"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\nS\n\023SequenceFileDataset\022\r\n\tfilenames\030\007\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\210\001\001")
