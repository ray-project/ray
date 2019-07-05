"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: ragged_conversion_ops.cc
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


_ragged_tensor_to_sparse_outputs = ["sparse_indices", "sparse_values",
                                   "sparse_dense_shape"]
_RaggedTensorToSparseOutput = _collections.namedtuple(
    "RaggedTensorToSparse", _ragged_tensor_to_sparse_outputs)


def ragged_tensor_to_sparse(rt_nested_splits, rt_dense_values, name=None):
  r"""Converts a `RaggedTensor` into a `SparseTensor` with the same values.

  input=ragged.from_nested_row_splits(rt_dense_values, rt_nested_splits)
  output=SparseTensor(indices=sparse_indices, values=sparse_values,
                      dense_shape=sparse_dense_shape)

  Args:
    rt_nested_splits: A list of at least 1 `Tensor` objects with type `int64`.
      The `row_splits` for the `RaggedTensor`.
    rt_dense_values: A `Tensor`. The `flat_values` for the `RaggedTensor`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sparse_indices, sparse_values, sparse_dense_shape).

    sparse_indices: A `Tensor` of type `int64`.
    sparse_values: A `Tensor`. Has the same type as `rt_dense_values`.
    sparse_dense_shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RaggedTensorToSparse", name, _ctx._post_execution_callbacks,
        rt_nested_splits, rt_dense_values)
      _result = _RaggedTensorToSparseOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return ragged_tensor_to_sparse_eager_fallback(
            rt_nested_splits, rt_dense_values, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(rt_nested_splits, (list, tuple)):
    raise TypeError(
        "Expected list for 'rt_nested_splits' argument to "
        "'ragged_tensor_to_sparse' Op, not %r." % rt_nested_splits)
  _attr_RAGGED_RANK = len(rt_nested_splits)
  _, _, _op = _op_def_lib._apply_op_helper(
        "RaggedTensorToSparse", rt_nested_splits=rt_nested_splits,
                                rt_dense_values=rt_dense_values, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("RAGGED_RANK", _op.get_attr("RAGGED_RANK"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "RaggedTensorToSparse", _inputs_flat, _attrs, _result, name)
  _result = _RaggedTensorToSparseOutput._make(_result)
  return _result



def ragged_tensor_to_sparse_eager_fallback(rt_nested_splits, rt_dense_values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ragged_tensor_to_sparse
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(rt_nested_splits, (list, tuple)):
    raise TypeError(
        "Expected list for 'rt_nested_splits' argument to "
        "'ragged_tensor_to_sparse' Op, not %r." % rt_nested_splits)
  _attr_RAGGED_RANK = len(rt_nested_splits)
  _attr_T, (rt_dense_values,) = _execute.args_to_matching_eager([rt_dense_values], _ctx)
  rt_nested_splits = _ops.convert_n_to_tensor(rt_nested_splits, _dtypes.int64)
  _inputs_flat = list(rt_nested_splits) + [rt_dense_values]
  _attrs = ("RAGGED_RANK", _attr_RAGGED_RANK, "T", _attr_T)
  _result = _execute.execute(b"RaggedTensorToSparse", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RaggedTensorToSparse", _inputs_flat, _attrs, _result, name)
  _result = _RaggedTensorToSparseOutput._make(_result)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "RaggedTensorToSparse"
#   input_arg {
#     name: "rt_nested_splits"
#     type: DT_INT64
#     number_attr: "RAGGED_RANK"
#   }
#   input_arg {
#     name: "rt_dense_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "sparse_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "sparse_dense_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "RAGGED_RANK"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\262\001\n\024RaggedTensorToSparse\022!\n\020rt_nested_splits\030\t*\013RAGGED_RANK\022\024\n\017rt_dense_values\"\001T\032\022\n\016sparse_indices\030\t\032\022\n\rsparse_values\"\001T\032\026\n\022sparse_dense_shape\030\t\"\026\n\013RAGGED_RANK\022\003int(\0010\001\"\t\n\001T\022\004type")
