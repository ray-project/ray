"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_factorization_ops.cc
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
@tf_export('masked_matmul')
def masked_matmul(a, b, mask_indices, transpose_a, transpose_b, name=None):
  r"""Computes the product a * b, but only for indices (i, j) in mask_indices. The

  result is stored in prod_values, a rank 1 tensor, such that for all i,
  prod_values[i] = (a * b)[mask_indices[i, 0], mask_indices[i, 1]].
  Note that the shapes of the input matrices a, b should be compatible (after
  transposing as specified by the arguments transpose_a and transpose_b).

  Input arguments:

  Args:
    a: A `Tensor` of type `float32`. A rank 2 tensor of shape [m, n].
    b: A `Tensor` of type `float32`.
      A rank 2 tensor of shape [s, t]. The inner dimensions of a and b should match
      after transposition.
    mask_indices: A `Tensor` of type `int64`.
      A rank 2 tensor, of shape [nnz, 2] where nnz is the number of
      non-zero elements in the output. The indices are not assumed to be in
      lexicographic, or any particular order.
      For all i, mask_indices[i, :] should represent a valid index of the product
      matrix (a * b) (after transposition). That is:
      mask_indices[i, 0] should be in [0, m) if !transpose_a, and in [0, n)
        otherwise.
      mask_indices[i, 1] should be in [0, t) if !transpose_b, and in [0, s)
        otherwise.
    transpose_a: A `Tensor` of type `bool`.
      A boolean, specifies whether to transpose the matrix a.
    transpose_b: A `Tensor` of type `bool`.
      A boolean, specifies whether to transpose the matrix b.

      Output arguments:
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
    A rank 1 tensor of shape [nnz], representing the values of the
    non-zero elements in the product, such that for all i,
    prod_values[i] = (a * b)[mask_indices[i, 0], mask_indices[i, 1]].
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MaskedMatmul",
        name, _ctx._post_execution_callbacks, a, b, mask_indices, transpose_a,
        transpose_b)
      return _result
    except _core._FallbackException:
      try:
        return masked_matmul_eager_fallback(
            a, b, mask_indices, transpose_a, transpose_b, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              masked_matmul, a=a, b=b, mask_indices=mask_indices,
                             transpose_a=transpose_a, transpose_b=transpose_b,
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
        "MaskedMatmul", a=a, b=b, mask_indices=mask_indices,
                        transpose_a=transpose_a, transpose_b=transpose_b,
                        name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          masked_matmul, a=a, b=b, mask_indices=mask_indices,
                         transpose_a=transpose_a, transpose_b=transpose_b,
                         name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "MaskedMatmul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def masked_matmul_eager_fallback(a, b, mask_indices, transpose_a, transpose_b, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function masked_matmul
  """
  _ctx = ctx if ctx else _context.context()
  a = _ops.convert_to_tensor(a, _dtypes.float32)
  b = _ops.convert_to_tensor(b, _dtypes.float32)
  mask_indices = _ops.convert_to_tensor(mask_indices, _dtypes.int64)
  transpose_a = _ops.convert_to_tensor(transpose_a, _dtypes.bool)
  transpose_b = _ops.convert_to_tensor(transpose_b, _dtypes.bool)
  _inputs_flat = [a, b, mask_indices, transpose_a, transpose_b]
  _attrs = None
  _result = _execute.execute(b"MaskedMatmul", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaskedMatmul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("MaskedMatmul")(None)


_wals_compute_partial_lhs_and_rhs_outputs = ["partial_lhs", "partial_rhs"]
_WALSComputePartialLhsAndRhsOutput = _collections.namedtuple(
    "WALSComputePartialLhsAndRhs", _wals_compute_partial_lhs_and_rhs_outputs)


@_dispatch.add_dispatch_list
@tf_export('wals_compute_partial_lhs_and_rhs')
def wals_compute_partial_lhs_and_rhs(factors, factor_weights, unobserved_weights, input_weights, input_indices, input_values, entry_weights, input_block_size, input_is_transpose, name=None):
  r"""Computes the partial left-hand side and right-hand side of WALS update. For

  observed entry input_indices[i]=[m, n] with value input_values[i]=v, the weight
  should be specified either through (1) entry_weights[i] or (2) through
  input_weights[m] * factor_weights[n] (if input_is_transpose is false) or
  input_weights[n] * factor_weights[m] (if input_is_transpose is true). Note it is
  not allowed to have both (1) and (2) specified at the same time: when one
  approach is used, the input tensors related to the other approach must be kept
  completely empty.

  Args:
    factors: A `Tensor` of type `float32`. Matrix of size m * k.
    factor_weights: A `Tensor` of type `float32`.
      Vector of size m. Corresponds to column weights. Should be empty
      if entry_weights is used.
    unobserved_weights: A `Tensor` of type `float32`.
      Scalar. Weight for unobserved input entries.
    input_weights: A `Tensor` of type `float32`.
      Vector of size n. Corresponds to row weights. Should be empty if
      entry_weights is used.
    input_indices: A `Tensor` of type `int64`.
      Indices for the input SparseTensor.
    input_values: A `Tensor` of type `float32`.
      Values for the input SparseTensor.
    entry_weights: A `Tensor` of type `float32`.
      If not empty, this must be same length as input_vaues and is used
      as the per-entry non-zero weight. If this is used, input_weights and
      factor_weights must be empty.
    input_block_size: A `Tensor` of type `int64`.
      Scalar. Number of rows spanned by input.
    input_is_transpose: A `Tensor` of type `bool`.
      If true, logically transposes the input for processing.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (partial_lhs, partial_rhs).

    partial_lhs: A `Tensor` of type `float32`. 3-D tensor with size input_block_size x k x k.
    partial_rhs: A `Tensor` of type `float32`. Matrix with size input_block_size x k.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "WALSComputePartialLhsAndRhs", name, _ctx._post_execution_callbacks,
        factors, factor_weights, unobserved_weights, input_weights,
        input_indices, input_values, entry_weights, input_block_size,
        input_is_transpose)
      _result = _WALSComputePartialLhsAndRhsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return wals_compute_partial_lhs_and_rhs_eager_fallback(
            factors, factor_weights, unobserved_weights, input_weights,
            input_indices, input_values, entry_weights, input_block_size,
            input_is_transpose, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              wals_compute_partial_lhs_and_rhs, factors=factors,
                                                factor_weights=factor_weights,
                                                unobserved_weights=unobserved_weights,
                                                input_weights=input_weights,
                                                input_indices=input_indices,
                                                input_values=input_values,
                                                entry_weights=entry_weights,
                                                input_block_size=input_block_size,
                                                input_is_transpose=input_is_transpose,
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
        "WALSComputePartialLhsAndRhs", factors=factors,
                                       factor_weights=factor_weights,
                                       unobserved_weights=unobserved_weights,
                                       input_weights=input_weights,
                                       input_indices=input_indices,
                                       input_values=input_values,
                                       entry_weights=entry_weights,
                                       input_block_size=input_block_size,
                                       input_is_transpose=input_is_transpose,
                                       name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          wals_compute_partial_lhs_and_rhs, factors=factors,
                                            factor_weights=factor_weights,
                                            unobserved_weights=unobserved_weights,
                                            input_weights=input_weights,
                                            input_indices=input_indices,
                                            input_values=input_values,
                                            entry_weights=entry_weights,
                                            input_block_size=input_block_size,
                                            input_is_transpose=input_is_transpose,
                                            name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "WALSComputePartialLhsAndRhs", _inputs_flat, _attrs, _result, name)
  _result = _WALSComputePartialLhsAndRhsOutput._make(_result)
  return _result



def wals_compute_partial_lhs_and_rhs_eager_fallback(factors, factor_weights, unobserved_weights, input_weights, input_indices, input_values, entry_weights, input_block_size, input_is_transpose, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function wals_compute_partial_lhs_and_rhs
  """
  _ctx = ctx if ctx else _context.context()
  factors = _ops.convert_to_tensor(factors, _dtypes.float32)
  factor_weights = _ops.convert_to_tensor(factor_weights, _dtypes.float32)
  unobserved_weights = _ops.convert_to_tensor(unobserved_weights, _dtypes.float32)
  input_weights = _ops.convert_to_tensor(input_weights, _dtypes.float32)
  input_indices = _ops.convert_to_tensor(input_indices, _dtypes.int64)
  input_values = _ops.convert_to_tensor(input_values, _dtypes.float32)
  entry_weights = _ops.convert_to_tensor(entry_weights, _dtypes.float32)
  input_block_size = _ops.convert_to_tensor(input_block_size, _dtypes.int64)
  input_is_transpose = _ops.convert_to_tensor(input_is_transpose, _dtypes.bool)
  _inputs_flat = [factors, factor_weights, unobserved_weights, input_weights, input_indices, input_values, entry_weights, input_block_size, input_is_transpose]
  _attrs = None
  _result = _execute.execute(b"WALSComputePartialLhsAndRhs", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "WALSComputePartialLhsAndRhs", _inputs_flat, _attrs, _result, name)
  _result = _WALSComputePartialLhsAndRhsOutput._make(_result)
  return _result

_ops.RegisterShape("WALSComputePartialLhsAndRhs")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "MaskedMatmul"
#   input_arg {
#     name: "a"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "b"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "mask_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "transpose_a"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "transpose_b"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "prod_values"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "WALSComputePartialLhsAndRhs"
#   input_arg {
#     name: "factors"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "factor_weights"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "unobserved_weights"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "input_weights"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "input_values"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "entry_weights"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "input_block_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "input_is_transpose"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "partial_lhs"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "partial_rhs"
#     type: DT_FLOAT
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\na\n\014MaskedMatmul\022\005\n\001a\030\001\022\005\n\001b\030\001\022\020\n\014mask_indices\030\t\022\017\n\013transpose_a\030\n\022\017\n\013transpose_b\030\n\032\017\n\013prod_values\030\001\n\361\001\n\033WALSComputePartialLhsAndRhs\022\013\n\007factors\030\001\022\022\n\016factor_weights\030\001\022\026\n\022unobserved_weights\030\001\022\021\n\rinput_weights\030\001\022\021\n\rinput_indices\030\t\022\020\n\014input_values\030\001\022\021\n\rentry_weights\030\001\022\024\n\020input_block_size\030\t\022\026\n\022input_is_transpose\030\n\032\017\n\013partial_lhs\030\001\032\017\n\013partial_rhs\030\001")
