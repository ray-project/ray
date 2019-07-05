"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: beam_search_ops.cc
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
@tf_export('gather_tree')
def gather_tree(step_ids, parent_ids, max_sequence_lengths, end_token, name=None):
  r"""Calculates the full beams from the per-step ids and parent beam ids.

  On CPU, if an out of bound parent id is found, an error is returned.
  On GPU, if an out of bound parent id is found, a -1 is stored in the
  corresponding output value and the execution for that beam returns early.

  For a given beam, past the time step containing the first decoded `end_token`
  all values are filled in with `end_token`.

  TODO(ebrevdo): fill in the remainder of this docstring.

  Args:
    step_ids: A `Tensor`. Must be one of the following types: `int32`.
      `[max_time, batch_size, beam_width]`.
    parent_ids: A `Tensor`. Must have the same type as `step_ids`.
      `[max_time, batch_size, beam_width]`.
    max_sequence_lengths: A `Tensor` of type `int32`. `[batch_size]`.
    end_token: A `Tensor`. Must have the same type as `step_ids`. `[]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `step_ids`.
    `[max_time, batch_size, beam_width]`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "GatherTree",
        name, _ctx._post_execution_callbacks, step_ids, parent_ids,
        max_sequence_lengths, end_token)
      return _result
    except _core._FallbackException:
      try:
        return gather_tree_eager_fallback(
            step_ids, parent_ids, max_sequence_lengths, end_token, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              gather_tree, step_ids=step_ids, parent_ids=parent_ids,
                           max_sequence_lengths=max_sequence_lengths,
                           end_token=end_token, name=name)
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
        "GatherTree", step_ids=step_ids, parent_ids=parent_ids,
                      max_sequence_lengths=max_sequence_lengths,
                      end_token=end_token, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          gather_tree, step_ids=step_ids, parent_ids=parent_ids,
                       max_sequence_lengths=max_sequence_lengths,
                       end_token=end_token, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "GatherTree", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def gather_tree_eager_fallback(step_ids, parent_ids, max_sequence_lengths, end_token, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function gather_tree
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([step_ids, parent_ids, end_token], _ctx)
  (step_ids, parent_ids, end_token) = _inputs_T
  max_sequence_lengths = _ops.convert_to_tensor(max_sequence_lengths, _dtypes.int32)
  _inputs_flat = [step_ids, parent_ids, max_sequence_lengths, end_token]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"GatherTree", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GatherTree", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("GatherTree")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "GatherTree"
#   input_arg {
#     name: "step_ids"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "parent_ids"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "max_sequence_lengths"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "end_token"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "beams"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\nt\n\nGatherTree\022\r\n\010step_ids\"\001T\022\017\n\nparent_ids\"\001T\022\030\n\024max_sequence_lengths\030\003\022\016\n\tend_token\"\001T\032\n\n\005beams\"\001T\"\020\n\001T\022\004type:\005\n\0032\001\003")
