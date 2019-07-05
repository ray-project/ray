"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: linalg_ops.cc
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


def batch_cholesky(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchCholesky", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_cholesky_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchCholesky", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchCholesky", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_cholesky_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_cholesky
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BatchCholesky", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchCholesky", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_cholesky_grad(l, grad, name=None):
  r"""TODO: add doc.

  Args:
    l: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    grad: A `Tensor`. Must have the same type as `l`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `l`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchCholeskyGrad", name, _ctx._post_execution_callbacks, l, grad)
      return _result
    except _core._FallbackException:
      try:
        return batch_cholesky_grad_eager_fallback(
            l, grad, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchCholeskyGrad", l=l, grad=grad, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchCholeskyGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_cholesky_grad_eager_fallback(l, grad, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_cholesky_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([l, grad], _ctx)
  (l, grad) = _inputs_T
  _inputs_flat = [l, grad]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BatchCholeskyGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchCholeskyGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_matrix_determinant(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchMatrixDeterminant", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_matrix_determinant_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatrixDeterminant", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchMatrixDeterminant", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_matrix_determinant_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_matrix_determinant
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BatchMatrixDeterminant", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BatchMatrixDeterminant", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_matrix_inverse(input, adjoint=False, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`.
    adjoint: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchMatrixInverse", name, _ctx._post_execution_callbacks, input,
        "adjoint", adjoint)
      return _result
    except _core._FallbackException:
      try:
        return batch_matrix_inverse_eager_fallback(
            input, adjoint=adjoint, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatrixInverse", input=input, adjoint=adjoint, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("adjoint", _op.get_attr("adjoint"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchMatrixInverse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_matrix_inverse_eager_fallback(input, adjoint=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_matrix_inverse
  """
  _ctx = ctx if ctx else _context.context()
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("adjoint", adjoint, "T", _attr_T)
  _result = _execute.execute(b"BatchMatrixInverse", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchMatrixInverse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_matrix_solve(matrix, rhs, adjoint=False, name=None):
  r"""TODO: add doc.

  Args:
    matrix: A `Tensor`. Must be one of the following types: `float64`, `float32`.
    rhs: A `Tensor`. Must have the same type as `matrix`.
    adjoint: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `matrix`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchMatrixSolve", name, _ctx._post_execution_callbacks, matrix, rhs,
        "adjoint", adjoint)
      return _result
    except _core._FallbackException:
      try:
        return batch_matrix_solve_eager_fallback(
            matrix, rhs, adjoint=adjoint, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatrixSolve", matrix=matrix, rhs=rhs, adjoint=adjoint,
                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("adjoint", _op.get_attr("adjoint"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchMatrixSolve", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_matrix_solve_eager_fallback(matrix, rhs, adjoint=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_matrix_solve
  """
  _ctx = ctx if ctx else _context.context()
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([matrix, rhs], _ctx)
  (matrix, rhs) = _inputs_T
  _inputs_flat = [matrix, rhs]
  _attrs = ("adjoint", adjoint, "T", _attr_T)
  _result = _execute.execute(b"BatchMatrixSolve", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchMatrixSolve", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_matrix_solve_ls(matrix, rhs, l2_regularizer, fast=True, name=None):
  r"""TODO: add doc.

  Args:
    matrix: A `Tensor`. Must be one of the following types: `float64`, `float32`.
    rhs: A `Tensor`. Must have the same type as `matrix`.
    l2_regularizer: A `Tensor` of type `float64`.
    fast: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `matrix`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchMatrixSolveLs", name, _ctx._post_execution_callbacks, matrix,
        rhs, l2_regularizer, "fast", fast)
      return _result
    except _core._FallbackException:
      try:
        return batch_matrix_solve_ls_eager_fallback(
            matrix, rhs, l2_regularizer, fast=fast, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if fast is None:
    fast = True
  fast = _execute.make_bool(fast, "fast")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatrixSolveLs", matrix=matrix, rhs=rhs,
                              l2_regularizer=l2_regularizer, fast=fast,
                              name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "fast", _op.get_attr("fast"))
  _execute.record_gradient(
      "BatchMatrixSolveLs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_matrix_solve_ls_eager_fallback(matrix, rhs, l2_regularizer, fast=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_matrix_solve_ls
  """
  _ctx = ctx if ctx else _context.context()
  if fast is None:
    fast = True
  fast = _execute.make_bool(fast, "fast")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([matrix, rhs], _ctx)
  (matrix, rhs) = _inputs_T
  l2_regularizer = _ops.convert_to_tensor(l2_regularizer, _dtypes.float64)
  _inputs_flat = [matrix, rhs, l2_regularizer]
  _attrs = ("T", _attr_T, "fast", fast)
  _result = _execute.execute(b"BatchMatrixSolveLs", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchMatrixSolveLs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_matrix_triangular_solve(matrix, rhs, lower=True, adjoint=False, name=None):
  r"""TODO: add doc.

  Args:
    matrix: A `Tensor`. Must be one of the following types: `float64`, `float32`.
    rhs: A `Tensor`. Must have the same type as `matrix`.
    lower: An optional `bool`. Defaults to `True`.
    adjoint: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `matrix`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchMatrixTriangularSolve", name, _ctx._post_execution_callbacks,
        matrix, rhs, "lower", lower, "adjoint", adjoint)
      return _result
    except _core._FallbackException:
      try:
        return batch_matrix_triangular_solve_eager_fallback(
            matrix, rhs, lower=lower, adjoint=adjoint, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if lower is None:
    lower = True
  lower = _execute.make_bool(lower, "lower")
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatrixTriangularSolve", matrix=matrix, rhs=rhs, lower=lower,
                                      adjoint=adjoint, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("lower", _op.get_attr("lower"), "adjoint",
            _op.get_attr("adjoint"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchMatrixTriangularSolve", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_matrix_triangular_solve_eager_fallback(matrix, rhs, lower=True, adjoint=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_matrix_triangular_solve
  """
  _ctx = ctx if ctx else _context.context()
  if lower is None:
    lower = True
  lower = _execute.make_bool(lower, "lower")
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([matrix, rhs], _ctx)
  (matrix, rhs) = _inputs_T
  _inputs_flat = [matrix, rhs]
  _attrs = ("lower", lower, "adjoint", adjoint, "T", _attr_T)
  _result = _execute.execute(b"BatchMatrixTriangularSolve", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BatchMatrixTriangularSolve", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_self_adjoint_eig(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchSelfAdjointEig", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_self_adjoint_eig_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchSelfAdjointEig", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchSelfAdjointEig", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_self_adjoint_eig_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_self_adjoint_eig
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BatchSelfAdjointEig", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchSelfAdjointEig", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_batch_self_adjoint_eig_v2_outputs = ["e", "v"]
_BatchSelfAdjointEigV2Output = _collections.namedtuple(
    "BatchSelfAdjointEigV2", _batch_self_adjoint_eig_v2_outputs)


def batch_self_adjoint_eig_v2(input, compute_v=True, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`.
    compute_v: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (e, v).

    e: A `Tensor`. Has the same type as `input`.
    v: A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchSelfAdjointEigV2", name, _ctx._post_execution_callbacks, input,
        "compute_v", compute_v)
      _result = _BatchSelfAdjointEigV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return batch_self_adjoint_eig_v2_eager_fallback(
            input, compute_v=compute_v, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if compute_v is None:
    compute_v = True
  compute_v = _execute.make_bool(compute_v, "compute_v")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchSelfAdjointEigV2", input=input, compute_v=compute_v, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("compute_v", _op.get_attr("compute_v"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchSelfAdjointEigV2", _inputs_flat, _attrs, _result, name)
  _result = _BatchSelfAdjointEigV2Output._make(_result)
  return _result



def batch_self_adjoint_eig_v2_eager_fallback(input, compute_v=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_self_adjoint_eig_v2
  """
  _ctx = ctx if ctx else _context.context()
  if compute_v is None:
    compute_v = True
  compute_v = _execute.make_bool(compute_v, "compute_v")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("compute_v", compute_v, "T", _attr_T)
  _result = _execute.execute(b"BatchSelfAdjointEigV2", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchSelfAdjointEigV2", _inputs_flat, _attrs, _result, name)
  _result = _BatchSelfAdjointEigV2Output._make(_result)
  return _result


_batch_svd_outputs = ["s", "u", "v"]
_BatchSvdOutput = _collections.namedtuple(
    "BatchSvd", _batch_svd_outputs)


def batch_svd(input, compute_uv=True, full_matrices=False, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
    compute_uv: An optional `bool`. Defaults to `True`.
    full_matrices: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (s, u, v).

    s: A `Tensor`. Has the same type as `input`.
    u: A `Tensor`. Has the same type as `input`.
    v: A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchSvd",
        name, _ctx._post_execution_callbacks, input, "compute_uv", compute_uv,
        "full_matrices", full_matrices)
      _result = _BatchSvdOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return batch_svd_eager_fallback(
            input, compute_uv=compute_uv, full_matrices=full_matrices,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if compute_uv is None:
    compute_uv = True
  compute_uv = _execute.make_bool(compute_uv, "compute_uv")
  if full_matrices is None:
    full_matrices = False
  full_matrices = _execute.make_bool(full_matrices, "full_matrices")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchSvd", input=input, compute_uv=compute_uv,
                    full_matrices=full_matrices, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("compute_uv", _op.get_attr("compute_uv"), "full_matrices",
            _op.get_attr("full_matrices"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchSvd", _inputs_flat, _attrs, _result, name)
  _result = _BatchSvdOutput._make(_result)
  return _result



def batch_svd_eager_fallback(input, compute_uv=True, full_matrices=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_svd
  """
  _ctx = ctx if ctx else _context.context()
  if compute_uv is None:
    compute_uv = True
  compute_uv = _execute.make_bool(compute_uv, "compute_uv")
  if full_matrices is None:
    full_matrices = False
  full_matrices = _execute.make_bool(full_matrices, "full_matrices")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("compute_uv", compute_uv, "full_matrices", full_matrices, "T",
  _attr_T)
  _result = _execute.execute(b"BatchSvd", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchSvd", _inputs_flat, _attrs, _result, name)
  _result = _BatchSvdOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.cholesky', v1=['linalg.cholesky', 'cholesky'])
@deprecated_endpoints('cholesky')
def cholesky(input, name=None):
  r"""Computes the Cholesky decomposition of one or more square matrices.

  The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
  form square matrices.

  The input has to be symmetric and positive definite. Only the lower-triangular
  part of the input will be used for this operation. The upper-triangular part
  will not be read.

  The output is a tensor of the same shape as the input
  containing the Cholesky decompositions for all input submatrices `[..., :, :]`.

  **Note**: The gradient computation on GPU is faster for large matrices but
  not for large batch dimensions when the submatrices are small. In this
  case it might be faster to use the CPU.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      Shape is `[..., M, M]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Cholesky",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return cholesky_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              cholesky, input=input, name=name)
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
        "Cholesky", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          cholesky, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Cholesky", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cholesky_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cholesky
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Cholesky", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Cholesky", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def cholesky_grad(l, grad, name=None):
  r"""Computes the reverse mode backpropagated gradient of the Cholesky algorithm.

  For an explanation see "Differentiation of the Cholesky algorithm" by
  Iain Murray http://arxiv.org/abs/1602.07527.

  Args:
    l: A `Tensor`. Must be one of the following types: `float32`, `float64`.
      Output of batch Cholesky algorithm l = cholesky(A). Shape is `[..., M, M]`.
      Algorithm depends only on lower triangular part of the innermost matrices of
      this tensor.
    grad: A `Tensor`. Must have the same type as `l`.
      df/dl where f is some scalar function. Shape is `[..., M, M]`.
      Algorithm depends only on lower triangular part of the innermost matrices of
      this tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `l`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "CholeskyGrad",
        name, _ctx._post_execution_callbacks, l, grad)
      return _result
    except _core._FallbackException:
      try:
        return cholesky_grad_eager_fallback(
            l, grad, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "CholeskyGrad", l=l, grad=grad, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "CholeskyGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cholesky_grad_eager_fallback(l, grad, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cholesky_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([l, grad], _ctx)
  (l, grad) = _inputs_T
  _inputs_flat = [l, grad]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"CholeskyGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CholeskyGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_log_matrix_determinant_outputs = ["sign", "log_abs_determinant"]
_LogMatrixDeterminantOutput = _collections.namedtuple(
    "LogMatrixDeterminant", _log_matrix_determinant_outputs)


def log_matrix_determinant(input, name=None):
  r"""Computes the sign and the log of the absolute value of the determinant of

  one or more square matrices.

  The input is a tensor of shape `[N, M, M]` whose inner-most 2 dimensions
  form square matrices. The outputs are two tensors containing the signs and
  absolute values of the log determinants for all N input submatrices
  `[..., :, :]` such that the determinant = sign*exp(log_abs_determinant).
  The log_abs_determinant is computed as det(P)*sum(log(diag(LU))) where LU
  is the LU decomposition of the input and P is the corresponding
  permutation matrix.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `complex64`, `complex128`.
      Shape is `[N, M, M]`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (sign, log_abs_determinant).

    sign: A `Tensor`. Has the same type as `input`.
    log_abs_determinant: A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LogMatrixDeterminant", name, _ctx._post_execution_callbacks, input)
      _result = _LogMatrixDeterminantOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return log_matrix_determinant_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "LogMatrixDeterminant", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "LogMatrixDeterminant", _inputs_flat, _attrs, _result, name)
  _result = _LogMatrixDeterminantOutput._make(_result)
  return _result



def log_matrix_determinant_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function log_matrix_determinant
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"LogMatrixDeterminant", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LogMatrixDeterminant", _inputs_flat, _attrs, _result, name)
  _result = _LogMatrixDeterminantOutput._make(_result)
  return _result


_lu_outputs = ["lu", "p"]
_LuOutput = _collections.namedtuple(
    "Lu", _lu_outputs)


@_dispatch.add_dispatch_list
@tf_export('linalg.lu')
def lu(input, output_idx_type=_dtypes.int32, name=None):
  r"""Computes the LU decomposition of one or more square matrices.

  The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
  form square matrices.

  The input has to be invertible.

  The output consists of two tensors LU and P containing the LU decomposition
  of all input submatrices `[..., :, :]`. LU encodes the lower triangular and
  upper triangular factors.

  For each input submatrix of shape `[M, M]`, L is a lower triangular matrix of
  shape `[M, M]` with unit diagonal whose entries correspond to the strictly lower
  triangular part of LU. U is a upper triangular matrix of shape `[M, M]` whose
  entries correspond to the upper triangular part, including the diagonal, of LU.

  P represents a permutation matrix encoded as a list of indices each between `0`
  and `M-1`, inclusive. If P_mat denotes the permutation matrix corresponding to
  P, then the L, U and P satisfies P_mat * input = L * U.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      A tensor of shape `[..., M, M]` whose inner-most 2 dimensions form matrices of
      size `[M, M]`.
    output_idx_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (lu, p).

    lu: A `Tensor`. Has the same type as `input`.
    p: A `Tensor` of type `output_idx_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Lu", name,
        _ctx._post_execution_callbacks, input, "output_idx_type",
        output_idx_type)
      _result = _LuOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return lu_eager_fallback(
            input, output_idx_type=output_idx_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              lu, input=input, output_idx_type=output_idx_type, name=name)
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
  if output_idx_type is None:
    output_idx_type = _dtypes.int32
  output_idx_type = _execute.make_type(output_idx_type, "output_idx_type")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "Lu", input=input, output_idx_type=output_idx_type, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          lu, input=input, output_idx_type=output_idx_type, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "output_idx_type",
            _op.get_attr("output_idx_type"))
  _execute.record_gradient(
      "Lu", _inputs_flat, _attrs, _result, name)
  _result = _LuOutput._make(_result)
  return _result



def lu_eager_fallback(input, output_idx_type=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lu
  """
  _ctx = ctx if ctx else _context.context()
  if output_idx_type is None:
    output_idx_type = _dtypes.int32
  output_idx_type = _execute.make_type(output_idx_type, "output_idx_type")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "output_idx_type", output_idx_type)
  _result = _execute.execute(b"Lu", 2, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Lu", _inputs_flat, _attrs, _result, name)
  _result = _LuOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.det', v1=['linalg.det', 'matrix_determinant'])
@deprecated_endpoints('matrix_determinant')
def matrix_determinant(input, name=None):
  r"""Computes the determinant of one or more square matrices.

  The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
  form square matrices. The output is a tensor containing the determinants
  for all input submatrices `[..., :, :]`.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `complex64`, `complex128`.
      Shape is `[..., M, M]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixDeterminant", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return matrix_determinant_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matrix_determinant, input=input, name=name)
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
        "MatrixDeterminant", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matrix_determinant, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixDeterminant", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_determinant_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_determinant
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"MatrixDeterminant", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixDeterminant", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def matrix_exponential(input, name=None):
  r"""Deprecated, use python implementation tf.linalg.matrix_exponential.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixExponential", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return matrix_exponential_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "MatrixExponential", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixExponential", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_exponential_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_exponential
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"MatrixExponential", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixExponential", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.inv', v1=['linalg.inv', 'matrix_inverse'])
@deprecated_endpoints('matrix_inverse')
def matrix_inverse(input, adjoint=False, name=None):
  r"""Computes the inverse of one or more square invertible matrices or their

  adjoints (conjugate transposes).

  The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
  form square matrices. The output is a tensor of the same shape as the input
  containing the inverse for all input submatrices `[..., :, :]`.

  The op uses LU decomposition with partial pivoting to compute the inverses.

  If a matrix is not invertible there is no guarantee what the op does. It
  may detect the condition and raise an exception or it may simply return a
  garbage result.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      Shape is `[..., M, M]`.
    adjoint: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixInverse", name, _ctx._post_execution_callbacks, input,
        "adjoint", adjoint)
      return _result
    except _core._FallbackException:
      try:
        return matrix_inverse_eager_fallback(
            input, adjoint=adjoint, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matrix_inverse, input=input, adjoint=adjoint, name=name)
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
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "MatrixInverse", input=input, adjoint=adjoint, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matrix_inverse, input=input, adjoint=adjoint, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("adjoint", _op.get_attr("adjoint"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixInverse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_inverse_eager_fallback(input, adjoint=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_inverse
  """
  _ctx = ctx if ctx else _context.context()
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("adjoint", adjoint, "T", _attr_T)
  _result = _execute.execute(b"MatrixInverse", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixInverse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def matrix_logarithm(input, name=None):
  r"""Computes the matrix logarithm of one or more square matrices:

  
  \\(log(exp(A)) = A\\)

  This op is only defined for complex matrices. If A is positive-definite and
  real, then casting to a complex matrix, taking the logarithm and casting back
  to a real matrix will give the correct result.

  This function computes the matrix logarithm using the Schur-Parlett algorithm.
  Details of the algorithm can be found in Section 11.6.2 of:
  Nicholas J. Higham, Functions of Matrices: Theory and Computation, SIAM 2008.
  ISBN 978-0-898716-46-7.

  The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
  form square matrices. The output is a tensor of the same shape as the input
  containing the exponential for all input submatrices `[..., :, :]`.

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
      Shape is `[..., M, M]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixLogarithm", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return matrix_logarithm_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "MatrixLogarithm", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixLogarithm", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_logarithm_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_logarithm
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"MatrixLogarithm", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixLogarithm", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.solve', v1=['linalg.solve', 'matrix_solve'])
@deprecated_endpoints('matrix_solve')
def matrix_solve(matrix, rhs, adjoint=False, name=None):
  r"""Solves systems of linear equations.

  `Matrix` is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
  form square matrices. `Rhs` is a tensor of shape `[..., M, K]`. The `output` is
  a tensor shape `[..., M, K]`.  If `adjoint` is `False` then each output matrix
  satisfies `matrix[..., :, :] * output[..., :, :] = rhs[..., :, :]`.
  If `adjoint` is `True` then each output matrix satisfies
  `adjoint(matrix[..., :, :]) * output[..., :, :] = rhs[..., :, :]`.

  Args:
    matrix: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      Shape is `[..., M, M]`.
    rhs: A `Tensor`. Must have the same type as `matrix`.
      Shape is `[..., M, K]`.
    adjoint: An optional `bool`. Defaults to `False`.
      Boolean indicating whether to solve with `matrix` or its (block-wise)
      adjoint.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `matrix`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MatrixSolve",
        name, _ctx._post_execution_callbacks, matrix, rhs, "adjoint", adjoint)
      return _result
    except _core._FallbackException:
      try:
        return matrix_solve_eager_fallback(
            matrix, rhs, adjoint=adjoint, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matrix_solve, matrix=matrix, rhs=rhs, adjoint=adjoint,
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
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "MatrixSolve", matrix=matrix, rhs=rhs, adjoint=adjoint, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matrix_solve, matrix=matrix, rhs=rhs, adjoint=adjoint, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("adjoint", _op.get_attr("adjoint"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixSolve", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_solve_eager_fallback(matrix, rhs, adjoint=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_solve
  """
  _ctx = ctx if ctx else _context.context()
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([matrix, rhs], _ctx)
  (matrix, rhs) = _inputs_T
  _inputs_flat = [matrix, rhs]
  _attrs = ("adjoint", adjoint, "T", _attr_T)
  _result = _execute.execute(b"MatrixSolve", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixSolve", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def matrix_solve_ls(matrix, rhs, l2_regularizer, fast=True, name=None):
  r"""Solves one or more linear least-squares problems.

  `matrix` is a tensor of shape `[..., M, N]` whose inner-most 2 dimensions
  form real or complex matrices of size `[M, N]`. `Rhs` is a tensor of the same
  type as `matrix` and shape `[..., M, K]`.
  The output is a tensor shape `[..., N, K]` where each output matrix solves
  each of the equations
  `matrix[..., :, :]` * `output[..., :, :]` = `rhs[..., :, :]`
  in the least squares sense.

  We use the following notation for (complex) matrix and right-hand sides
  in the batch:

  `matrix`=\\(A \in \mathbb{C}^{m \times n}\\),
  `rhs`=\\(B  \in \mathbb{C}^{m \times k}\\),
  `output`=\\(X  \in \mathbb{C}^{n \times k}\\),
  `l2_regularizer`=\\(\lambda \in \mathbb{R}\\).

  If `fast` is `True`, then the solution is computed by solving the normal
  equations using Cholesky decomposition. Specifically, if \\(m \ge n\\) then
  \\(X = (A^H A + \lambda I)^{-1} A^H B\\), which solves the least-squares
  problem \\(X = \mathrm{argmin}_{Z \in \Re^{n \times k} } ||A Z - B||_F^2 + \lambda ||Z||_F^2\\). 
  If \\(m \lt n\\) then `output` is computed as
  \\(X = A^H (A A^H + \lambda I)^{-1} B\\), which (for \\(\lambda = 0\\)) is the
  minimum-norm solution to the under-determined linear system, i.e.
  \\(X = \mathrm{argmin}_{Z \in \mathbb{C}^{n \times k} } ||Z||_F^2 \\),
  subject to \\(A Z = B\\). Notice that the fast path is only numerically stable
  when \\(A\\) is numerically full rank and has a condition number
  \\(\mathrm{cond}(A) \lt \frac{1}{\sqrt{\epsilon_{mach} } }\\) or \\(\lambda\\) is
  sufficiently large.

  If `fast` is `False` an algorithm based on the numerically robust complete
  orthogonal decomposition is used. This computes the minimum-norm
  least-squares solution, even when \\(A\\) is rank deficient. This path is
  typically 6-7 times slower than the fast path. If `fast` is `False` then
  `l2_regularizer` is ignored.

  Args:
    matrix: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      Shape is `[..., M, N]`.
    rhs: A `Tensor`. Must have the same type as `matrix`.
      Shape is `[..., M, K]`.
    l2_regularizer: A `Tensor` of type `float64`. Scalar tensor.

      @compatibility(numpy)
      Equivalent to np.linalg.lstsq
      @end_compatibility
    fast: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `matrix`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixSolveLs", name, _ctx._post_execution_callbacks, matrix, rhs,
        l2_regularizer, "fast", fast)
      return _result
    except _core._FallbackException:
      try:
        return matrix_solve_ls_eager_fallback(
            matrix, rhs, l2_regularizer, fast=fast, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if fast is None:
    fast = True
  fast = _execute.make_bool(fast, "fast")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MatrixSolveLs", matrix=matrix, rhs=rhs,
                         l2_regularizer=l2_regularizer, fast=fast, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "fast", _op.get_attr("fast"))
  _execute.record_gradient(
      "MatrixSolveLs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_solve_ls_eager_fallback(matrix, rhs, l2_regularizer, fast=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_solve_ls
  """
  _ctx = ctx if ctx else _context.context()
  if fast is None:
    fast = True
  fast = _execute.make_bool(fast, "fast")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([matrix, rhs], _ctx)
  (matrix, rhs) = _inputs_T
  l2_regularizer = _ops.convert_to_tensor(l2_regularizer, _dtypes.float64)
  _inputs_flat = [matrix, rhs, l2_regularizer]
  _attrs = ("T", _attr_T, "fast", fast)
  _result = _execute.execute(b"MatrixSolveLs", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixSolveLs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.sqrtm', 'matrix_square_root')
def matrix_square_root(input, name=None):
  r"""Computes the matrix square root of one or more square matrices:

  matmul(sqrtm(A), sqrtm(A)) = A

  The input matrix should be invertible. If the input matrix is real, it should
  have no eigenvalues which are real and negative (pairs of complex conjugate
  eigenvalues are allowed).

  The matrix square root is computed by first reducing the matrix to 
  quasi-triangular form with the real Schur decomposition. The square root 
  of the quasi-triangular matrix is then computed directly. Details of 
  the algorithm can be found in: Nicholas J. Higham, "Computing real 
  square roots of a real matrix", Linear Algebra Appl., 1987.

  The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
  form square matrices. The output is a tensor of the same shape as the input
  containing the matrix square root for all input submatrices `[..., :, :]`.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      Shape is `[..., M, M]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixSquareRoot", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return matrix_square_root_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matrix_square_root, input=input, name=name)
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
        "MatrixSquareRoot", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matrix_square_root, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixSquareRoot", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_square_root_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_square_root
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"MatrixSquareRoot", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixSquareRoot", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.triangular_solve', v1=['linalg.triangular_solve', 'matrix_triangular_solve'])
@deprecated_endpoints('matrix_triangular_solve')
def matrix_triangular_solve(matrix, rhs, lower=True, adjoint=False, name=None):
  r"""Solves systems of linear equations with upper or lower triangular matrices by

  backsubstitution.

  `matrix` is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions form
  square matrices. If `lower` is `True` then the strictly upper triangular part
  of each inner-most matrix is assumed to be zero and not accessed.
  If `lower` is False then the strictly lower triangular part of each inner-most
  matrix is assumed to be zero and not accessed.
  `rhs` is a tensor of shape `[..., M, K]`.

  The output is a tensor of shape `[..., M, K]`. If `adjoint` is
  `True` then the innermost matrices in `output` satisfy matrix equations
  `matrix[..., :, :] * output[..., :, :] = rhs[..., :, :]`.
  If `adjoint` is `False` then the strictly then the  innermost matrices in
  `output` satisfy matrix equations
  `adjoint(matrix[..., i, k]) * output[..., k, j] = rhs[..., i, j]`.

  Args:
    matrix: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      Shape is `[..., M, M]`.
    rhs: A `Tensor`. Must have the same type as `matrix`.
      Shape is `[..., M, K]`.
    lower: An optional `bool`. Defaults to `True`.
      Boolean indicating whether the innermost matrices in `matrix` are
      lower or upper triangular.
    adjoint: An optional `bool`. Defaults to `False`.
      Boolean indicating whether to solve with `matrix` or its (block-wise)
               adjoint.

      @compatibility(numpy)
      Equivalent to scipy.linalg.solve_triangular
      @end_compatibility
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `matrix`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixTriangularSolve", name, _ctx._post_execution_callbacks, matrix,
        rhs, "lower", lower, "adjoint", adjoint)
      return _result
    except _core._FallbackException:
      try:
        return matrix_triangular_solve_eager_fallback(
            matrix, rhs, lower=lower, adjoint=adjoint, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matrix_triangular_solve, matrix=matrix, rhs=rhs, lower=lower,
                                       adjoint=adjoint, name=name)
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
  if lower is None:
    lower = True
  lower = _execute.make_bool(lower, "lower")
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "MatrixTriangularSolve", matrix=matrix, rhs=rhs, lower=lower,
                                 adjoint=adjoint, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matrix_triangular_solve, matrix=matrix, rhs=rhs, lower=lower,
                                   adjoint=adjoint, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("lower", _op.get_attr("lower"), "adjoint",
            _op.get_attr("adjoint"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixTriangularSolve", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_triangular_solve_eager_fallback(matrix, rhs, lower=True, adjoint=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_triangular_solve
  """
  _ctx = ctx if ctx else _context.context()
  if lower is None:
    lower = True
  lower = _execute.make_bool(lower, "lower")
  if adjoint is None:
    adjoint = False
  adjoint = _execute.make_bool(adjoint, "adjoint")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([matrix, rhs], _ctx)
  (matrix, rhs) = _inputs_T
  _inputs_flat = [matrix, rhs]
  _attrs = ("lower", lower, "adjoint", adjoint, "T", _attr_T)
  _result = _execute.execute(b"MatrixTriangularSolve", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixTriangularSolve", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_qr_outputs = ["q", "r"]
_QrOutput = _collections.namedtuple(
    "Qr", _qr_outputs)


@_dispatch.add_dispatch_list
@tf_export('linalg.qr', v1=['linalg.qr', 'qr'])
@deprecated_endpoints('qr')
def qr(input, full_matrices=False, name=None):
  r"""Computes the QR decompositions of one or more matrices.

  Computes the QR decomposition of each inner matrix in `tensor` such that
  `tensor[..., :, :] = q[..., :, :] * r[..., :,:])`

  ```python
  # a is a tensor.
  # q is a tensor of orthonormal matrices.
  # r is a tensor of upper triangular matrices.
  q, r = qr(a)
  q_full, r_full = qr(a, full_matrices=True)
  ```

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      A tensor of shape `[..., M, N]` whose inner-most 2 dimensions
      form matrices of size `[M, N]`. Let `P` be the minimum of `M` and `N`.
    full_matrices: An optional `bool`. Defaults to `False`.
      If true, compute full-sized `q` and `r`. If false
      (the default), compute only the leading `P` columns of `q`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (q, r).

    q: A `Tensor`. Has the same type as `input`.
    r: A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Qr", name,
        _ctx._post_execution_callbacks, input, "full_matrices", full_matrices)
      _result = _QrOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return qr_eager_fallback(
            input, full_matrices=full_matrices, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              qr, input=input, full_matrices=full_matrices, name=name)
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
  if full_matrices is None:
    full_matrices = False
  full_matrices = _execute.make_bool(full_matrices, "full_matrices")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "Qr", input=input, full_matrices=full_matrices, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          qr, input=input, full_matrices=full_matrices, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("full_matrices", _op.get_attr("full_matrices"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "Qr", _inputs_flat, _attrs, _result, name)
  _result = _QrOutput._make(_result)
  return _result



def qr_eager_fallback(input, full_matrices=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function qr
  """
  _ctx = ctx if ctx else _context.context()
  if full_matrices is None:
    full_matrices = False
  full_matrices = _execute.make_bool(full_matrices, "full_matrices")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("full_matrices", full_matrices, "T", _attr_T)
  _result = _execute.execute(b"Qr", 2, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Qr", _inputs_flat, _attrs, _result, name)
  _result = _QrOutput._make(_result)
  return _result


def self_adjoint_eig(input, name=None):
  r"""Computes the Eigen Decomposition of a batch of square self-adjoint matrices.

  The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
  form square matrices, with the same constraints as the single matrix
  SelfAdjointEig.

  The result is a [..., M+1, M] matrix with [..., 0,:] containing the
  eigenvalues, and subsequent [...,1:, :] containing the eigenvectors. The eigenvalues
  are sorted in non-decreasing order.

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`.
      Shape is `[..., M, M]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SelfAdjointEig", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return self_adjoint_eig_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "SelfAdjointEig", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SelfAdjointEig", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def self_adjoint_eig_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function self_adjoint_eig
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SelfAdjointEig", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SelfAdjointEig", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_self_adjoint_eig_v2_outputs = ["e", "v"]
_SelfAdjointEigV2Output = _collections.namedtuple(
    "SelfAdjointEigV2", _self_adjoint_eig_v2_outputs)


def self_adjoint_eig_v2(input, compute_v=True, name=None):
  r"""Computes the eigen decomposition of one or more square self-adjoint matrices.

  Computes the eigenvalues and (optionally) eigenvectors of each inner matrix in
  `input` such that `input[..., :, :] = v[..., :, :] * diag(e[..., :])`. The eigenvalues
  are sorted in non-decreasing order.

  ```python
  # a is a tensor.
  # e is a tensor of eigenvalues.
  # v is a tensor of eigenvectors.
  e, v = self_adjoint_eig(a)
  e = self_adjoint_eig(a, compute_v=False)
  ```

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      `Tensor` input of shape `[N, N]`.
    compute_v: An optional `bool`. Defaults to `True`.
      If `True` then eigenvectors will be computed and returned in `v`.
      Otherwise, only the eigenvalues will be computed.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (e, v).

    e: A `Tensor`. Has the same type as `input`.
    v: A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SelfAdjointEigV2", name, _ctx._post_execution_callbacks, input,
        "compute_v", compute_v)
      _result = _SelfAdjointEigV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return self_adjoint_eig_v2_eager_fallback(
            input, compute_v=compute_v, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if compute_v is None:
    compute_v = True
  compute_v = _execute.make_bool(compute_v, "compute_v")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SelfAdjointEigV2", input=input, compute_v=compute_v, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("compute_v", _op.get_attr("compute_v"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "SelfAdjointEigV2", _inputs_flat, _attrs, _result, name)
  _result = _SelfAdjointEigV2Output._make(_result)
  return _result



def self_adjoint_eig_v2_eager_fallback(input, compute_v=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function self_adjoint_eig_v2
  """
  _ctx = ctx if ctx else _context.context()
  if compute_v is None:
    compute_v = True
  compute_v = _execute.make_bool(compute_v, "compute_v")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("compute_v", compute_v, "T", _attr_T)
  _result = _execute.execute(b"SelfAdjointEigV2", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SelfAdjointEigV2", _inputs_flat, _attrs, _result, name)
  _result = _SelfAdjointEigV2Output._make(_result)
  return _result


_svd_outputs = ["s", "u", "v"]
_SvdOutput = _collections.namedtuple(
    "Svd", _svd_outputs)


def svd(input, compute_uv=True, full_matrices=False, name=None):
  r"""Computes the singular value decompositions of one or more matrices.

  Computes the SVD of each inner matrix in `input` such that
  `input[..., :, :] = u[..., :, :] * diag(s[..., :, :]) * transpose(v[..., :, :])`

  ```python
  # a is a tensor containing a batch of matrices.
  # s is a tensor of singular values for each matrix.
  # u is the tensor containing of left singular vectors for each matrix.
  # v is the tensor containing of right singular vectors for each matrix.
  s, u, v = svd(a)
  s, _, _ = svd(a, compute_uv=False)
  ```

  Args:
    input: A `Tensor`. Must be one of the following types: `float64`, `float32`, `complex64`, `complex128`.
      A tensor of shape `[..., M, N]` whose inner-most 2 dimensions
      form matrices of size `[M, N]`. Let `P` be the minimum of `M` and `N`.
    compute_uv: An optional `bool`. Defaults to `True`.
      If true, left and right singular vectors will be
      computed and returned in `u` and `v`, respectively.
      If false, `u` and `v` are not set and should never referenced.
    full_matrices: An optional `bool`. Defaults to `False`.
      If true, compute full-sized `u` and `v`. If false
      (the default), compute only the leading `P` singular vectors.
      Ignored if `compute_uv` is `False`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (s, u, v).

    s: A `Tensor`. Has the same type as `input`.
    u: A `Tensor`. Has the same type as `input`.
    v: A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Svd", name,
        _ctx._post_execution_callbacks, input, "compute_uv", compute_uv,
        "full_matrices", full_matrices)
      _result = _SvdOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return svd_eager_fallback(
            input, compute_uv=compute_uv, full_matrices=full_matrices,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if compute_uv is None:
    compute_uv = True
  compute_uv = _execute.make_bool(compute_uv, "compute_uv")
  if full_matrices is None:
    full_matrices = False
  full_matrices = _execute.make_bool(full_matrices, "full_matrices")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Svd", input=input, compute_uv=compute_uv,
               full_matrices=full_matrices, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("compute_uv", _op.get_attr("compute_uv"), "full_matrices",
            _op.get_attr("full_matrices"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "Svd", _inputs_flat, _attrs, _result, name)
  _result = _SvdOutput._make(_result)
  return _result



def svd_eager_fallback(input, compute_uv=True, full_matrices=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function svd
  """
  _ctx = ctx if ctx else _context.context()
  if compute_uv is None:
    compute_uv = True
  compute_uv = _execute.make_bool(compute_uv, "compute_uv")
  if full_matrices is None:
    full_matrices = False
  full_matrices = _execute.make_bool(full_matrices, "full_matrices")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("compute_uv", compute_uv, "full_matrices", full_matrices, "T",
  _attr_T)
  _result = _execute.execute(b"Svd", 3, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Svd", _inputs_flat, _attrs, _result, name)
  _result = _SvdOutput._make(_result)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BatchCholesky"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#       }
#     }
#   }
#   deprecation {
#     version: 13
#     explanation: "Use Cholesky instead."
#   }
# }
# op {
#   name: "BatchCholeskyGrad"
#   input_arg {
#     name: "l"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   deprecation {
#     version: 13
#     explanation: "Use CholeskyGrad instead."
#   }
# }
# op {
#   name: "BatchMatrixDeterminant"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   deprecation {
#     version: 13
#     explanation: "Use MatrixDeterminant instead."
#   }
# }
# op {
#   name: "BatchMatrixInverse"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "adjoint"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#       }
#     }
#   }
#   deprecation {
#     version: 13
#     explanation: "Use MatrixInverse instead."
#   }
# }
# op {
#   name: "BatchMatrixSolve"
#   input_arg {
#     name: "matrix"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rhs"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "adjoint"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#       }
#     }
#   }
#   deprecation {
#     version: 13
#     explanation: "Use MatrixSolve instead."
#   }
# }
# op {
#   name: "BatchMatrixSolveLs"
#   input_arg {
#     name: "matrix"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rhs"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2_regularizer"
#     type: DT_DOUBLE
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "fast"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   deprecation {
#     version: 13
#     explanation: "Use MatrixSolveLs instead."
#   }
# }
# op {
#   name: "BatchMatrixTriangularSolve"
#   input_arg {
#     name: "matrix"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rhs"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "lower"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "adjoint"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#       }
#     }
#   }
#   deprecation {
#     version: 13
#     explanation: "Use MatrixTriangularSolve instead."
#   }
# }
# op {
#   name: "BatchSelfAdjointEig"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#       }
#     }
#   }
#   deprecation {
#     version: 11
#     explanation: "Use SelfAdjointEigV2 instead."
#   }
# }
# op {
#   name: "BatchSelfAdjointEigV2"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "e"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "v"
#     type_attr: "T"
#   }
#   attr {
#     name: "compute_v"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#       }
#     }
#   }
#   deprecation {
#     version: 13
#     explanation: "Use SelfAdjointEigV2 instead."
#   }
# }
# op {
#   name: "BatchSvd"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "s"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "u"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "v"
#     type_attr: "T"
#   }
#   attr {
#     name: "compute_uv"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "full_matrices"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   deprecation {
#     version: 13
#     explanation: "Use Svd instead."
#   }
# }
# op {
#   name: "Cholesky"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "CholeskyGrad"
#   input_arg {
#     name: "l"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "LogMatrixDeterminant"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "sign"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "log_abs_determinant"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Lu"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "lu"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "p"
#     type_attr: "output_idx_type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   attr {
#     name: "output_idx_type"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "MatrixDeterminant"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "MatrixExponential"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   deprecation {
#     version: 27
#     explanation: "Use Python implementation tf.linalg.matrix_exponential instead."
#   }
# }
# op {
#   name: "MatrixInverse"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "adjoint"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "MatrixLogarithm"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "MatrixSolve"
#   input_arg {
#     name: "matrix"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rhs"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "adjoint"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "MatrixSolveLs"
#   input_arg {
#     name: "matrix"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rhs"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2_regularizer"
#     type: DT_DOUBLE
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   attr {
#     name: "fast"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "MatrixSquareRoot"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "MatrixTriangularSolve"
#   input_arg {
#     name: "matrix"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rhs"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "lower"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "adjoint"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Qr"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "q"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "r"
#     type_attr: "T"
#   }
#   attr {
#     name: "full_matrices"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "SelfAdjointEig"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#       }
#     }
#   }
#   deprecation {
#     version: 11
#     explanation: "Use SelfAdjointEigV2 instead."
#   }
# }
# op {
#   name: "SelfAdjointEigV2"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "e"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "v"
#     type_attr: "T"
#   }
#   attr {
#     name: "compute_v"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Svd"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "s"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "u"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "v"
#     type_attr: "T"
#   }
#   attr {
#     name: "compute_uv"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "full_matrices"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\nV\n\rBatchCholesky\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\002\001B\031\010\r\022\025Use Cholesky instead.\ne\n\021BatchCholeskyGrad\022\006\n\001l\"\001T\022\t\n\004grad\"\001T\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002B\035\010\r\022\031Use CholeskyGrad instead.\nj\n\026BatchMatrixDeterminant\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\001\002\010\022B\"\010\r\022\036Use MatrixDeterminant instead.\nu\n\022BatchMatrixInverse\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\023\n\007adjoint\022\004bool\032\002(\000\"\021\n\001T\022\004type:\006\n\0042\002\002\001B\036\010\r\022\032Use MatrixInverse instead.\n|\n\020BatchMatrixSolve\022\013\n\006matrix\"\001T\022\010\n\003rhs\"\001T\032\013\n\006output\"\001T\"\023\n\007adjoint\022\004bool\032\002(\000\"\021\n\001T\022\004type:\006\n\0042\002\002\001B\034\010\r\022\030Use MatrixSolve instead.\n\221\001\n\022BatchMatrixSolveLs\022\013\n\006matrix\"\001T\022\010\n\003rhs\"\001T\022\022\n\016l2_regularizer\030\002\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\002\001\"\020\n\004fast\022\004bool\032\002(\001B\036\010\r\022\032Use MatrixSolveLs instead.\n\243\001\n\032BatchMatrixTriangularSolve\022\013\n\006matrix\"\001T\022\010\n\003rhs\"\001T\032\013\n\006output\"\001T\"\021\n\005lower\022\004bool\032\002(\001\"\023\n\007adjoint\022\004bool\032\002(\000\"\021\n\001T\022\004type:\006\n\0042\002\002\001B&\010\r\022\"Use MatrixTriangularSolve instead.\nd\n\023BatchSelfAdjointEig\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\002\001B!\010\013\022\035Use SelfAdjointEigV2 instead.\n\200\001\n\025BatchSelfAdjointEigV2\022\n\n\005input\"\001T\032\006\n\001e\"\001T\032\006\n\001v\"\001T\"\025\n\tcompute_v\022\004bool\032\002(\001\"\021\n\001T\022\004type:\006\n\0042\002\002\001B!\010\r\022\035Use SelfAdjointEigV2 instead.\n\214\001\n\010BatchSvd\022\n\n\005input\"\001T\032\006\n\001s\"\001T\032\006\n\001u\"\001T\032\006\n\001v\"\001T\"\026\n\ncompute_uv\022\004bool\032\002(\001\"\031\n\rfull_matrices\022\004bool\032\002(\000\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022B\024\010\r\022\020Use Svd instead.\n8\n\010Cholesky\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022\nA\n\014CholeskyGrad\022\006\n\001l\"\001T\022\t\n\004grad\"\001T\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\n\\\n\024LogMatrixDeterminant\022\n\n\005input\"\001T\032\t\n\004sign\"\001T\032\030\n\023log_abs_determinant\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\001\002\010\022\ni\n\002Lu\022\n\n\005input\"\001T\032\007\n\002lu\"\001T\032\024\n\001p\"\017output_idx_type\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022\"#\n\017output_idx_type\022\004type\032\0020\003:\006\n\0042\002\003\t\nA\n\021MatrixDeterminant\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\001\002\010\022\n\206\001\n\021MatrixExponential\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022BC\010\033\022?Use Python implementation tf.linalg.matrix_exponential instead.\nR\n\rMatrixInverse\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\023\n\007adjoint\022\004bool\032\002(\000\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022\n=\n\017MatrixLogarithm\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\010\022\n[\n\013MatrixSolve\022\013\n\006matrix\"\001T\022\010\n\003rhs\"\001T\032\013\n\006output\"\001T\"\023\n\007adjoint\022\004bool\032\002(\000\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022\nn\n\rMatrixSolveLs\022\013\n\006matrix\"\001T\022\010\n\003rhs\"\001T\022\022\n\016l2_regularizer\030\002\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022\"\020\n\004fast\022\004bool\032\002(\001\n@\n\020MatrixSquareRoot\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022\nx\n\025MatrixTriangularSolve\022\013\n\006matrix\"\001T\022\010\n\003rhs\"\001T\032\013\n\006output\"\001T\"\021\n\005lower\022\004bool\032\002(\001\"\023\n\007adjoint\022\004bool\032\002(\000\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022\nP\n\002Qr\022\n\n\005input\"\001T\032\006\n\001q\"\001T\032\006\n\001r\"\001T\"\031\n\rfull_matrices\022\004bool\032\002(\000\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022\n_\n\016SelfAdjointEig\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\002\001B!\010\013\022\035Use SelfAdjointEigV2 instead.\nZ\n\020SelfAdjointEigV2\022\n\n\005input\"\001T\032\006\n\001e\"\001T\032\006\n\001v\"\001T\"\025\n\tcompute_v\022\004bool\032\002(\001\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022\nq\n\003Svd\022\n\n\005input\"\001T\032\006\n\001s\"\001T\032\006\n\001u\"\001T\032\006\n\001v\"\001T\"\026\n\ncompute_uv\022\004bool\032\002(\001\"\031\n\rfull_matrices\022\004bool\032\002(\000\"\023\n\001T\022\004type:\010\n\0062\004\002\001\010\022")
