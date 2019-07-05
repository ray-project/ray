"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: spectral_ops.cc
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


def batch_fft(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor` of type `complex64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `complex64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchFFT",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_fft_eager_fallback(
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
        "BatchFFT", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BatchFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_fft_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_fft
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"BatchFFT", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_fft2d(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor` of type `complex64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `complex64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchFFT2D",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_fft2d_eager_fallback(
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
        "BatchFFT2D", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BatchFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_fft2d_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_fft2d
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"BatchFFT2D", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_fft3d(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor` of type `complex64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `complex64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchFFT3D",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_fft3d_eager_fallback(
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
        "BatchFFT3D", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BatchFFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_fft3d_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_fft3d
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"BatchFFT3D", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchFFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_ifft(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor` of type `complex64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `complex64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchIFFT",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_ifft_eager_fallback(
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
        "BatchIFFT", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BatchIFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_ifft_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_ifft
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"BatchIFFT", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchIFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_ifft2d(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor` of type `complex64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `complex64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchIFFT2D",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_ifft2d_eager_fallback(
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
        "BatchIFFT2D", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BatchIFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_ifft2d_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_ifft2d
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"BatchIFFT2D", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchIFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_ifft3d(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor` of type `complex64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `complex64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchIFFT3D",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_ifft3d_eager_fallback(
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
        "BatchIFFT3D", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BatchIFFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_ifft3d_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_ifft3d
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"BatchIFFT3D", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchIFFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('signal.fft', v1=['signal.fft', 'spectral.fft', 'fft'])
@deprecated_endpoints('spectral.fft', 'fft')
def fft(input, name=None):
  r"""Fast Fourier transform.

  Computes the 1-dimensional discrete Fourier transform over the inner-most
  dimension of `input`.

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
      A complex tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "FFT", name,
        _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return fft_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fft, input=input, name=name)
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
        "FFT", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fft, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tcomplex", _op.get_attr("Tcomplex"))
  _execute.record_gradient(
      "FFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fft_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fft
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tcomplex, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("Tcomplex", _attr_Tcomplex)
  _result = _execute.execute(b"FFT", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "FFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('signal.fft2d', v1=['signal.fft2d', 'spectral.fft2d', 'fft2d'])
@deprecated_endpoints('spectral.fft2d', 'fft2d')
def fft2d(input, name=None):
  r"""2D fast Fourier transform.

  Computes the 2-dimensional discrete Fourier transform over the inner-most
  2 dimensions of `input`.

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
      A complex tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "FFT2D", name,
        _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return fft2d_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fft2d, input=input, name=name)
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
        "FFT2D", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fft2d, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tcomplex", _op.get_attr("Tcomplex"))
  _execute.record_gradient(
      "FFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fft2d_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fft2d
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tcomplex, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("Tcomplex", _attr_Tcomplex)
  _result = _execute.execute(b"FFT2D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "FFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('signal.fft3d', v1=['signal.fft3d', 'spectral.fft3d', 'fft3d'])
@deprecated_endpoints('spectral.fft3d', 'fft3d')
def fft3d(input, name=None):
  r"""3D fast Fourier transform.

  Computes the 3-dimensional discrete Fourier transform over the inner-most 3
  dimensions of `input`.

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
      A complex64 tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "FFT3D", name,
        _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return fft3d_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fft3d, input=input, name=name)
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
        "FFT3D", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fft3d, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tcomplex", _op.get_attr("Tcomplex"))
  _execute.record_gradient(
      "FFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fft3d_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fft3d
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tcomplex, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("Tcomplex", _attr_Tcomplex)
  _result = _execute.execute(b"FFT3D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "FFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('signal.ifft', v1=['signal.ifft', 'spectral.ifft', 'ifft'])
@deprecated_endpoints('spectral.ifft', 'ifft')
def ifft(input, name=None):
  r"""Inverse fast Fourier transform.

  Computes the inverse 1-dimensional discrete Fourier transform over the
  inner-most dimension of `input`.

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
      A complex tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IFFT", name,
        _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return ifft_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              ifft, input=input, name=name)
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
        "IFFT", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          ifft, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tcomplex", _op.get_attr("Tcomplex"))
  _execute.record_gradient(
      "IFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ifft_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ifft
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tcomplex, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("Tcomplex", _attr_Tcomplex)
  _result = _execute.execute(b"IFFT", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "IFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('signal.ifft2d', v1=['signal.ifft2d', 'spectral.ifft2d', 'ifft2d'])
@deprecated_endpoints('spectral.ifft2d', 'ifft2d')
def ifft2d(input, name=None):
  r"""Inverse 2D fast Fourier transform.

  Computes the inverse 2-dimensional discrete Fourier transform over the
  inner-most 2 dimensions of `input`.

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
      A complex tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IFFT2D", name,
        _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return ifft2d_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              ifft2d, input=input, name=name)
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
        "IFFT2D", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          ifft2d, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tcomplex", _op.get_attr("Tcomplex"))
  _execute.record_gradient(
      "IFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ifft2d_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ifft2d
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tcomplex, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("Tcomplex", _attr_Tcomplex)
  _result = _execute.execute(b"IFFT2D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "IFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('signal.ifft3d', v1=['signal.ifft3d', 'spectral.ifft3d', 'ifft3d'])
@deprecated_endpoints('spectral.ifft3d', 'ifft3d')
def ifft3d(input, name=None):
  r"""Inverse 3D fast Fourier transform.

  Computes the inverse 3-dimensional discrete Fourier transform over the
  inner-most 3 dimensions of `input`.

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
      A complex64 tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IFFT3D", name,
        _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return ifft3d_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              ifft3d, input=input, name=name)
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
        "IFFT3D", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          ifft3d, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tcomplex", _op.get_attr("Tcomplex"))
  _execute.record_gradient(
      "IFFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ifft3d_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ifft3d
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tcomplex, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("Tcomplex", _attr_Tcomplex)
  _result = _execute.execute(b"IFFT3D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "IFFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def irfft(input, fft_length, name=None):
  r"""Inverse real-valued fast Fourier transform.

  Computes the inverse 1-dimensional discrete Fourier transform of a real-valued
  signal over the inner-most dimension of `input`.

  The inner-most dimension of `input` is assumed to be the result of `RFFT`: the
  `fft_length / 2 + 1` unique components of the DFT of a real-valued signal. If
  `fft_length` is not provided, it is computed from the size of the inner-most
  dimension of `input` (`fft_length = 2 * (inner - 1)`). If the FFT length used to
  compute `input` is odd, it should be provided since it cannot be inferred
  properly.

  Along the axis `IRFFT` is computed on, if `fft_length / 2 + 1` is smaller
  than the corresponding dimension of `input`, the dimension is cropped. If it is
  larger, the dimension is padded with zeros.

  Args:
    input: A `Tensor` of type `complex64`. A complex64 tensor.
    fft_length: A `Tensor` of type `int32`.
      An int32 tensor of shape [1]. The FFT length.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IRFFT", name,
        _ctx._post_execution_callbacks, input, fft_length)
      return _result
    except _core._FallbackException:
      try:
        return irfft_eager_fallback(
            input, fft_length, name=name, ctx=_ctx)
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
        "IRFFT", input=input, fft_length=fft_length, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "IRFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def irfft_eager_fallback(input, fft_length, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function irfft
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.complex64)
  fft_length = _ops.convert_to_tensor(fft_length, _dtypes.int32)
  _inputs_flat = [input, fft_length]
  _attrs = None
  _result = _execute.execute(b"IRFFT", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "IRFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def irfft2d(input, fft_length, name=None):
  r"""Inverse 2D real-valued fast Fourier transform.

  Computes the inverse 2-dimensional discrete Fourier transform of a real-valued
  signal over the inner-most 2 dimensions of `input`.

  The inner-most 2 dimensions of `input` are assumed to be the result of `RFFT2D`:
  The inner-most dimension contains the `fft_length / 2 + 1` unique components of
  the DFT of a real-valued signal. If `fft_length` is not provided, it is computed
  from the size of the inner-most 2 dimensions of `input`. If the FFT length used
  to compute `input` is odd, it should be provided since it cannot be inferred
  properly.

  Along each axis `IRFFT2D` is computed on, if `fft_length` (or
  `fft_length / 2 + 1` for the inner-most dimension) is smaller than the
  corresponding dimension of `input`, the dimension is cropped. If it is larger,
  the dimension is padded with zeros.

  Args:
    input: A `Tensor` of type `complex64`. A complex64 tensor.
    fft_length: A `Tensor` of type `int32`.
      An int32 tensor of shape [2]. The FFT length for each dimension.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IRFFT2D",
        name, _ctx._post_execution_callbacks, input, fft_length)
      return _result
    except _core._FallbackException:
      try:
        return irfft2d_eager_fallback(
            input, fft_length, name=name, ctx=_ctx)
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
        "IRFFT2D", input=input, fft_length=fft_length, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "IRFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def irfft2d_eager_fallback(input, fft_length, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function irfft2d
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.complex64)
  fft_length = _ops.convert_to_tensor(fft_length, _dtypes.int32)
  _inputs_flat = [input, fft_length]
  _attrs = None
  _result = _execute.execute(b"IRFFT2D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "IRFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def irfft3d(input, fft_length, name=None):
  r"""Inverse 3D real-valued fast Fourier transform.

  Computes the inverse 3-dimensional discrete Fourier transform of a real-valued
  signal over the inner-most 3 dimensions of `input`.

  The inner-most 3 dimensions of `input` are assumed to be the result of `RFFT3D`:
  The inner-most dimension contains the `fft_length / 2 + 1` unique components of
  the DFT of a real-valued signal. If `fft_length` is not provided, it is computed
  from the size of the inner-most 3 dimensions of `input`. If the FFT length used
  to compute `input` is odd, it should be provided since it cannot be inferred
  properly.

  Along each axis `IRFFT3D` is computed on, if `fft_length` (or
  `fft_length / 2 + 1` for the inner-most dimension) is smaller than the
  corresponding dimension of `input`, the dimension is cropped. If it is larger,
  the dimension is padded with zeros.

  Args:
    input: A `Tensor` of type `complex64`. A complex64 tensor.
    fft_length: A `Tensor` of type `int32`.
      An int32 tensor of shape [3]. The FFT length for each dimension.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IRFFT3D",
        name, _ctx._post_execution_callbacks, input, fft_length)
      return _result
    except _core._FallbackException:
      try:
        return irfft3d_eager_fallback(
            input, fft_length, name=name, ctx=_ctx)
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
        "IRFFT3D", input=input, fft_length=fft_length, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "IRFFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def irfft3d_eager_fallback(input, fft_length, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function irfft3d
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.complex64)
  fft_length = _ops.convert_to_tensor(fft_length, _dtypes.int32)
  _inputs_flat = [input, fft_length]
  _attrs = None
  _result = _execute.execute(b"IRFFT3D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "IRFFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def rfft(input, fft_length, name=None):
  r"""Real-valued fast Fourier transform.

  Computes the 1-dimensional discrete Fourier transform of a real-valued signal
  over the inner-most dimension of `input`.

  Since the DFT of a real signal is Hermitian-symmetric, `RFFT` only returns the
  `fft_length / 2 + 1` unique components of the FFT: the zero-frequency term,
  followed by the `fft_length / 2` positive-frequency terms.

  Along the axis `RFFT` is computed on, if `fft_length` is smaller than the
  corresponding dimension of `input`, the dimension is cropped. If it is larger,
  the dimension is padded with zeros.

  Args:
    input: A `Tensor` of type `float32`. A float32 tensor.
    fft_length: A `Tensor` of type `int32`.
      An int32 tensor of shape [1]. The FFT length.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `complex64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RFFT", name,
        _ctx._post_execution_callbacks, input, fft_length)
      return _result
    except _core._FallbackException:
      try:
        return rfft_eager_fallback(
            input, fft_length, name=name, ctx=_ctx)
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
        "RFFT", input=input, fft_length=fft_length, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "RFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def rfft_eager_fallback(input, fft_length, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function rfft
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.float32)
  fft_length = _ops.convert_to_tensor(fft_length, _dtypes.int32)
  _inputs_flat = [input, fft_length]
  _attrs = None
  _result = _execute.execute(b"RFFT", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "RFFT", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def rfft2d(input, fft_length, name=None):
  r"""2D real-valued fast Fourier transform.

  Computes the 2-dimensional discrete Fourier transform of a real-valued signal
  over the inner-most 2 dimensions of `input`.

  Since the DFT of a real signal is Hermitian-symmetric, `RFFT2D` only returns the
  `fft_length / 2 + 1` unique components of the FFT for the inner-most dimension
  of `output`: the zero-frequency term, followed by the `fft_length / 2`
  positive-frequency terms.

  Along each axis `RFFT2D` is computed on, if `fft_length` is smaller than the
  corresponding dimension of `input`, the dimension is cropped. If it is larger,
  the dimension is padded with zeros.

  Args:
    input: A `Tensor` of type `float32`. A float32 tensor.
    fft_length: A `Tensor` of type `int32`.
      An int32 tensor of shape [2]. The FFT length for each dimension.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `complex64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RFFT2D", name,
        _ctx._post_execution_callbacks, input, fft_length)
      return _result
    except _core._FallbackException:
      try:
        return rfft2d_eager_fallback(
            input, fft_length, name=name, ctx=_ctx)
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
        "RFFT2D", input=input, fft_length=fft_length, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "RFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def rfft2d_eager_fallback(input, fft_length, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function rfft2d
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.float32)
  fft_length = _ops.convert_to_tensor(fft_length, _dtypes.int32)
  _inputs_flat = [input, fft_length]
  _attrs = None
  _result = _execute.execute(b"RFFT2D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "RFFT2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def rfft3d(input, fft_length, name=None):
  r"""3D real-valued fast Fourier transform.

  Computes the 3-dimensional discrete Fourier transform of a real-valued signal
  over the inner-most 3 dimensions of `input`.

  Since the DFT of a real signal is Hermitian-symmetric, `RFFT3D` only returns the
  `fft_length / 2 + 1` unique components of the FFT for the inner-most dimension
  of `output`: the zero-frequency term, followed by the `fft_length / 2`
  positive-frequency terms.

  Along each axis `RFFT3D` is computed on, if `fft_length` is smaller than the
  corresponding dimension of `input`, the dimension is cropped. If it is larger,
  the dimension is padded with zeros.

  Args:
    input: A `Tensor` of type `float32`. A float32 tensor.
    fft_length: A `Tensor` of type `int32`.
      An int32 tensor of shape [3]. The FFT length for each dimension.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `complex64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RFFT3D", name,
        _ctx._post_execution_callbacks, input, fft_length)
      return _result
    except _core._FallbackException:
      try:
        return rfft3d_eager_fallback(
            input, fft_length, name=name, ctx=_ctx)
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
        "RFFT3D", input=input, fft_length=fft_length, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "RFFT3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def rfft3d_eager_fallback(input, fft_length, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function rfft3d
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.float32)
  fft_length = _ops.convert_to_tensor(fft_length, _dtypes.int32)
  _inputs_flat = [input, fft_length]
  _attrs = None
  _result = _execute.execute(b"RFFT3D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "RFFT3D", _inputs_flat, _attrs, _result, name)
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
#   name: "BatchFFT"
#   input_arg {
#     name: "input"
#     type: DT_COMPLEX64
#   }
#   output_arg {
#     name: "output"
#     type: DT_COMPLEX64
#   }
#   deprecation {
#     version: 15
#     explanation: "Use FFT"
#   }
# }
# op {
#   name: "BatchFFT2D"
#   input_arg {
#     name: "input"
#     type: DT_COMPLEX64
#   }
#   output_arg {
#     name: "output"
#     type: DT_COMPLEX64
#   }
#   deprecation {
#     version: 15
#     explanation: "Use FFT2D"
#   }
# }
# op {
#   name: "BatchFFT3D"
#   input_arg {
#     name: "input"
#     type: DT_COMPLEX64
#   }
#   output_arg {
#     name: "output"
#     type: DT_COMPLEX64
#   }
#   deprecation {
#     version: 15
#     explanation: "Use FFT3D"
#   }
# }
# op {
#   name: "BatchIFFT"
#   input_arg {
#     name: "input"
#     type: DT_COMPLEX64
#   }
#   output_arg {
#     name: "output"
#     type: DT_COMPLEX64
#   }
#   deprecation {
#     version: 15
#     explanation: "Use IFFT"
#   }
# }
# op {
#   name: "BatchIFFT2D"
#   input_arg {
#     name: "input"
#     type: DT_COMPLEX64
#   }
#   output_arg {
#     name: "output"
#     type: DT_COMPLEX64
#   }
#   deprecation {
#     version: 15
#     explanation: "Use IFFT2D"
#   }
# }
# op {
#   name: "BatchIFFT3D"
#   input_arg {
#     name: "input"
#     type: DT_COMPLEX64
#   }
#   output_arg {
#     name: "output"
#     type: DT_COMPLEX64
#   }
#   deprecation {
#     version: 15
#     explanation: "Use IFFT3D"
#   }
# }
# op {
#   name: "FFT"
#   input_arg {
#     name: "input"
#     type_attr: "Tcomplex"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tcomplex"
#   }
#   attr {
#     name: "Tcomplex"
#     type: "type"
#     default_value {
#       type: DT_COMPLEX64
#     }
#     allowed_values {
#       list {
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "FFT2D"
#   input_arg {
#     name: "input"
#     type_attr: "Tcomplex"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tcomplex"
#   }
#   attr {
#     name: "Tcomplex"
#     type: "type"
#     default_value {
#       type: DT_COMPLEX64
#     }
#     allowed_values {
#       list {
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "FFT3D"
#   input_arg {
#     name: "input"
#     type_attr: "Tcomplex"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tcomplex"
#   }
#   attr {
#     name: "Tcomplex"
#     type: "type"
#     default_value {
#       type: DT_COMPLEX64
#     }
#     allowed_values {
#       list {
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "IFFT"
#   input_arg {
#     name: "input"
#     type_attr: "Tcomplex"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tcomplex"
#   }
#   attr {
#     name: "Tcomplex"
#     type: "type"
#     default_value {
#       type: DT_COMPLEX64
#     }
#     allowed_values {
#       list {
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "IFFT2D"
#   input_arg {
#     name: "input"
#     type_attr: "Tcomplex"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tcomplex"
#   }
#   attr {
#     name: "Tcomplex"
#     type: "type"
#     default_value {
#       type: DT_COMPLEX64
#     }
#     allowed_values {
#       list {
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "IFFT3D"
#   input_arg {
#     name: "input"
#     type_attr: "Tcomplex"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tcomplex"
#   }
#   attr {
#     name: "Tcomplex"
#     type: "type"
#     default_value {
#       type: DT_COMPLEX64
#     }
#     allowed_values {
#       list {
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "IRFFT"
#   input_arg {
#     name: "input"
#     type: DT_COMPLEX64
#   }
#   input_arg {
#     name: "fft_length"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "IRFFT2D"
#   input_arg {
#     name: "input"
#     type: DT_COMPLEX64
#   }
#   input_arg {
#     name: "fft_length"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "IRFFT3D"
#   input_arg {
#     name: "input"
#     type: DT_COMPLEX64
#   }
#   input_arg {
#     name: "fft_length"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "RFFT"
#   input_arg {
#     name: "input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "fft_length"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_COMPLEX64
#   }
# }
# op {
#   name: "RFFT2D"
#   input_arg {
#     name: "input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "fft_length"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_COMPLEX64
#   }
# }
# op {
#   name: "RFFT3D"
#   input_arg {
#     name: "input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "fft_length"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_COMPLEX64
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n.\n\010BatchFFT\022\t\n\005input\030\010\032\n\n\006output\030\010B\013\010\017\022\007Use FFT\n2\n\nBatchFFT2D\022\t\n\005input\030\010\032\n\n\006output\030\010B\r\010\017\022\tUse FFT2D\n2\n\nBatchFFT3D\022\t\n\005input\030\010\032\n\n\006output\030\010B\r\010\017\022\tUse FFT3D\n0\n\tBatchIFFT\022\t\n\005input\030\010\032\n\n\006output\030\010B\014\010\017\022\010Use IFFT\n4\n\013BatchIFFT2D\022\t\n\005input\030\010\032\n\n\006output\030\010B\016\010\017\022\nUse IFFT2D\n4\n\013BatchIFFT3D\022\t\n\005input\030\010\032\n\n\006output\030\010B\016\010\017\022\nUse IFFT3D\nJ\n\003FFT\022\021\n\005input\"\010Tcomplex\032\022\n\006output\"\010Tcomplex\"\034\n\010Tcomplex\022\004type\032\0020\010:\006\n\0042\002\010\022\nL\n\005FFT2D\022\021\n\005input\"\010Tcomplex\032\022\n\006output\"\010Tcomplex\"\034\n\010Tcomplex\022\004type\032\0020\010:\006\n\0042\002\010\022\nL\n\005FFT3D\022\021\n\005input\"\010Tcomplex\032\022\n\006output\"\010Tcomplex\"\034\n\010Tcomplex\022\004type\032\0020\010:\006\n\0042\002\010\022\nK\n\004IFFT\022\021\n\005input\"\010Tcomplex\032\022\n\006output\"\010Tcomplex\"\034\n\010Tcomplex\022\004type\032\0020\010:\006\n\0042\002\010\022\nM\n\006IFFT2D\022\021\n\005input\"\010Tcomplex\032\022\n\006output\"\010Tcomplex\"\034\n\010Tcomplex\022\004type\032\0020\010:\006\n\0042\002\010\022\nM\n\006IFFT3D\022\021\n\005input\"\010Tcomplex\032\022\n\006output\"\010Tcomplex\"\034\n\010Tcomplex\022\004type\032\0020\010:\006\n\0042\002\010\022\n.\n\005IRFFT\022\t\n\005input\030\010\022\016\n\nfft_length\030\003\032\n\n\006output\030\001\n0\n\007IRFFT2D\022\t\n\005input\030\010\022\016\n\nfft_length\030\003\032\n\n\006output\030\001\n0\n\007IRFFT3D\022\t\n\005input\030\010\022\016\n\nfft_length\030\003\032\n\n\006output\030\001\n-\n\004RFFT\022\t\n\005input\030\001\022\016\n\nfft_length\030\003\032\n\n\006output\030\010\n/\n\006RFFT2D\022\t\n\005input\030\001\022\016\n\nfft_length\030\003\032\n\n\006output\030\010\n/\n\006RFFT3D\022\t\n\005input\030\001\022\016\n\nfft_length\030\003\032\n\n\006output\030\010")
