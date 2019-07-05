"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: math_ops.cc
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


def _abs(x, name=None):
  r"""Computes the absolute value of a tensor.

  Given a tensor `x`, this operation returns a tensor containing the absolute
  value of each element in `x`. For example, if x is an input element and y is
  an output element, this operation computes \\(y = |x|\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Abs", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return _abs_eager_fallback(
            x, name=name, ctx=_ctx)
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
        "Abs", x=x, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Abs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _abs_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _abs
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Abs", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Abs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def accumulate_nv2(inputs, shape, name=None):
  r"""Returns the element-wise sum of a list of tensors.

  `tf.accumulate_n_v2` performs the same operation as `tf.add_n`, but does not
  wait for all of its inputs to be ready before beginning to sum. This can
  save memory if inputs are ready at different times, since minimum temporary
  storage is proportional to the output size rather than the inputs size.

  Unlike the original `accumulate_n`, `accumulate_n_v2` is differentiable.

  Returns a `Tensor` of same shape and type as the elements of `inputs`.

  Args:
    inputs: A list of at least 1 `Tensor` objects with the same type in: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A list of `Tensor` objects, each with same shape and type.
    shape: A `tf.TensorShape` or list of `ints`.
      Shape of elements of `inputs`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `inputs`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AccumulateNV2", name, _ctx._post_execution_callbacks, inputs,
        "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return accumulate_nv2_eager_fallback(
            inputs, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'accumulate_nv2' Op, not %r." % inputs)
  _attr_N = len(inputs)
  shape = _execute.make_shape(shape, "shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AccumulateNV2", inputs=inputs, shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"), "shape",
            _op.get_attr("shape"))
  _execute.record_gradient(
      "AccumulateNV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def accumulate_nv2_eager_fallback(inputs, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function accumulate_nv2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'accumulate_nv2' Op, not %r." % inputs)
  _attr_N = len(inputs)
  shape = _execute.make_shape(shape, "shape")
  _attr_T, inputs = _execute.args_to_matching_eager(list(inputs), _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("N", _attr_N, "T", _attr_T, "shape", shape)
  _result = _execute.execute(b"AccumulateNV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AccumulateNV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.acos', 'acos')
def acos(x, name=None):
  r"""Computes acos of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Acos", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return acos_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              acos, x=x, name=name)
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
        "Acos", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          acos, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Acos", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def acos_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function acos
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Acos", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Acos", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.acosh', 'acosh')
def acosh(x, name=None):
  r"""Computes inverse hyperbolic cosine of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Acosh", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return acosh_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              acosh, x=x, name=name)
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
        "Acosh", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          acosh, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Acosh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def acosh_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function acosh
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Acosh", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Acosh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.add', 'add')
def add(x, y, name=None):
  r"""Returns x + y element-wise.

  *NOTE*: `math.add` supports broadcasting. `AddN` does not. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `int16`, `int32`, `int64`, `complex64`, `complex128`, `string`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Add", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return add_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              add, x=x, y=y, name=name)
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
        "Add", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          add, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Add", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def add_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function add
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Add", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Add", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def add_n(inputs, name=None):
  r"""Add all input tensors element wise.

  Args:
    inputs: A list of at least 1 `Tensor` objects with the same type in: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`, `variant`.
      Must all be the same size and shape.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `inputs`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "AddN", name,
        _ctx._post_execution_callbacks, inputs)
      return _result
    except _core._FallbackException:
      try:
        return add_n_eager_fallback(
            inputs, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'add_n' Op, not %r." % inputs)
  _attr_N = len(inputs)
  _, _, _op = _op_def_lib._apply_op_helper(
        "AddN", inputs=inputs, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "AddN", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def add_n_eager_fallback(inputs, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function add_n
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'add_n' Op, not %r." % inputs)
  _attr_N = len(inputs)
  _attr_T, inputs = _execute.args_to_matching_eager(list(inputs), _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("N", _attr_N, "T", _attr_T)
  _result = _execute.execute(b"AddN", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "AddN", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def add_v2(x, y, name=None):
  r"""Returns x + y element-wise.

  *NOTE*: `Add` supports broadcasting. `AddN` does not. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `int16`, `int32`, `int64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "AddV2", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return add_v2_eager_fallback(
            x, y, name=name, ctx=_ctx)
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
        "AddV2", x=x, y=y, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "AddV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def add_v2_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function add_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"AddV2", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "AddV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _all(input, axis, keep_dims=False, name=None):
  r"""Computes the "logical and" of elements across dimensions of a tensor.

  Reduces `input` along the dimensions given in `axis`. Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `axis`. If `keep_dims` is true, the reduced dimensions are
  retained with length 1.

  Args:
    input: A `Tensor` of type `bool`. The tensor to reduce.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The dimensions to reduce. Must be in the range
      `[-rank(input), rank(input))`.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "All", name,
        _ctx._post_execution_callbacks, input, axis, "keep_dims", keep_dims)
      return _result
    except _core._FallbackException:
      try:
        return _all_eager_fallback(
            input, axis, keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "All", input=input, reduction_indices=axis, keep_dims=keep_dims,
               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "Tidx",
            _op.get_attr("Tidx"))
  _execute.record_gradient(
      "All", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _all_eager_fallback(input, axis, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _all
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  input = _ops.convert_to_tensor(input, _dtypes.bool)
  _inputs_flat = [input, axis]
  _attrs = ("keep_dims", keep_dims, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"All", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "All", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def angle(input, Tout=_dtypes.float32, name=None):
  r"""Returns the argument of a complex number.

  Given a tensor `input` of complex numbers, this operation returns a tensor of
  type `float` that is the argument of each element in `input`. All elements in
  `input` must be complex numbers of the form \\(a + bj\\), where *a*
  is the real part and *b* is the imaginary part.

  The argument returned by this operation is of the form \\(atan2(b, a)\\).

  For example:

  ```
  # tensor 'input' is [-2.25 + 4.75j, 3.25 + 5.75j]
  tf.angle(input) ==> [2.0132, 1.056]
  ```

  @compatibility(numpy)
  Equivalent to np.angle.
  @end_compatibility

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
    Tout: An optional `tf.DType` from: `tf.float32, tf.float64`. Defaults to `tf.float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Angle", name,
        _ctx._post_execution_callbacks, input, "Tout", Tout)
      return _result
    except _core._FallbackException:
      try:
        return angle_eager_fallback(
            input, Tout=Tout, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if Tout is None:
    Tout = _dtypes.float32
  Tout = _execute.make_type(Tout, "Tout")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Angle", input=input, Tout=Tout, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "Angle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def angle_eager_fallback(input, Tout=_dtypes.float32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function angle
  """
  _ctx = ctx if ctx else _context.context()
  if Tout is None:
    Tout = _dtypes.float32
  Tout = _execute.make_type(Tout, "Tout")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "Tout", Tout)
  _result = _execute.execute(b"Angle", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Angle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _any(input, axis, keep_dims=False, name=None):
  r"""Computes the "logical or" of elements across dimensions of a tensor.

  Reduces `input` along the dimensions given in `axis`. Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `axis`. If `keep_dims` is true, the reduced dimensions are
  retained with length 1.

  Args:
    input: A `Tensor` of type `bool`. The tensor to reduce.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The dimensions to reduce. Must be in the range
      `[-rank(input), rank(input))`.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Any", name,
        _ctx._post_execution_callbacks, input, axis, "keep_dims", keep_dims)
      return _result
    except _core._FallbackException:
      try:
        return _any_eager_fallback(
            input, axis, keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Any", input=input, reduction_indices=axis, keep_dims=keep_dims,
               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "Tidx",
            _op.get_attr("Tidx"))
  _execute.record_gradient(
      "Any", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _any_eager_fallback(input, axis, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _any
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  input = _ops.convert_to_tensor(input, _dtypes.bool)
  _inputs_flat = [input, axis]
  _attrs = ("keep_dims", keep_dims, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"Any", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Any", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def approximate_equal(x, y, tolerance=1e-05, name=None):
  r"""Returns the truth value of abs(x-y) < tolerance element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    y: A `Tensor`. Must have the same type as `x`.
    tolerance: An optional `float`. Defaults to `1e-05`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ApproximateEqual", name, _ctx._post_execution_callbacks, x, y,
        "tolerance", tolerance)
      return _result
    except _core._FallbackException:
      try:
        return approximate_equal_eager_fallback(
            x, y, tolerance=tolerance, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if tolerance is None:
    tolerance = 1e-05
  tolerance = _execute.make_float(tolerance, "tolerance")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApproximateEqual", x=x, y=y, tolerance=tolerance, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "tolerance", _op.get_attr("tolerance"))
  _execute.record_gradient(
      "ApproximateEqual", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def approximate_equal_eager_fallback(x, y, tolerance=1e-05, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function approximate_equal
  """
  _ctx = ctx if ctx else _context.context()
  if tolerance is None:
    tolerance = 1e-05
  tolerance = _execute.make_float(tolerance, "tolerance")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T, "tolerance", tolerance)
  _result = _execute.execute(b"ApproximateEqual", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ApproximateEqual", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def arg_max(input, dimension, output_type=_dtypes.int64, name=None):
  r"""Returns the index with the largest value across dimensions of a tensor.

  Note that in case of ties the identity of the return value is not guaranteed.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    dimension: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      int32 or int64, must be in the range `[-rank(input), rank(input))`.
      Describes which dimension of the input Tensor to reduce across. For vectors,
      use dimension = 0.
    output_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `output_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ArgMax", name,
        _ctx._post_execution_callbacks, input, dimension, "output_type",
        output_type)
      return _result
    except _core._FallbackException:
      try:
        return arg_max_eager_fallback(
            input, dimension, output_type=output_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if output_type is None:
    output_type = _dtypes.int64
  output_type = _execute.make_type(output_type, "output_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ArgMax", input=input, dimension=dimension, output_type=output_type,
                  name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"),
            "output_type", _op.get_attr("output_type"))
  _execute.record_gradient(
      "ArgMax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def arg_max_eager_fallback(input, dimension, output_type=_dtypes.int64, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function arg_max
  """
  _ctx = ctx if ctx else _context.context()
  if output_type is None:
    output_type = _dtypes.int64
  output_type = _execute.make_type(output_type, "output_type")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tidx, (dimension,) = _execute.args_to_matching_eager([dimension], _ctx, _dtypes.int32)
  _inputs_flat = [input, dimension]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx, "output_type", output_type)
  _result = _execute.execute(b"ArgMax", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "ArgMax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def arg_min(input, dimension, output_type=_dtypes.int64, name=None):
  r"""Returns the index with the smallest value across dimensions of a tensor.

  Note that in case of ties the identity of the return value is not guaranteed.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    dimension: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      int32 or int64, must be in the range `[-rank(input), rank(input))`.
      Describes which dimension of the input Tensor to reduce across. For vectors,
      use dimension = 0.
    output_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `output_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ArgMin", name,
        _ctx._post_execution_callbacks, input, dimension, "output_type",
        output_type)
      return _result
    except _core._FallbackException:
      try:
        return arg_min_eager_fallback(
            input, dimension, output_type=output_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if output_type is None:
    output_type = _dtypes.int64
  output_type = _execute.make_type(output_type, "output_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ArgMin", input=input, dimension=dimension, output_type=output_type,
                  name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"),
            "output_type", _op.get_attr("output_type"))
  _execute.record_gradient(
      "ArgMin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def arg_min_eager_fallback(input, dimension, output_type=_dtypes.int64, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function arg_min
  """
  _ctx = ctx if ctx else _context.context()
  if output_type is None:
    output_type = _dtypes.int64
  output_type = _execute.make_type(output_type, "output_type")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tidx, (dimension,) = _execute.args_to_matching_eager([dimension], _ctx, _dtypes.int32)
  _inputs_flat = [input, dimension]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx, "output_type", output_type)
  _result = _execute.execute(b"ArgMin", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "ArgMin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.asin', 'asin')
def asin(x, name=None):
  r"""Computes asin of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Asin", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return asin_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              asin, x=x, name=name)
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
        "Asin", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          asin, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Asin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def asin_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function asin
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Asin", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Asin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.asinh', 'asinh')
def asinh(x, name=None):
  r"""Computes inverse hyperbolic sine of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Asinh", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return asinh_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              asinh, x=x, name=name)
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
        "Asinh", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          asinh, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Asinh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def asinh_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function asinh
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Asinh", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Asinh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.atan', 'atan')
def atan(x, name=None):
  r"""Computes atan of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Atan", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return atan_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              atan, x=x, name=name)
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
        "Atan", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          atan, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Atan", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def atan_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function atan
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Atan", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Atan", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.atan2', 'atan2')
def atan2(y, x, name=None):
  r"""Computes arctangent of `y/x` element-wise, respecting signs of the arguments.

  This is the angle \( \theta \in [-\pi, \pi] \) such that
  \[ x = r \cos(\theta) \]
  and
  \[ y = r \sin(\theta) \]
  where \(r = \sqrt(x^2 + y^2) \).

  Args:
    y: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    x: A `Tensor`. Must have the same type as `y`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `y`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Atan2", name,
        _ctx._post_execution_callbacks, y, x)
      return _result
    except _core._FallbackException:
      try:
        return atan2_eager_fallback(
            y, x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              atan2, y=y, x=x, name=name)
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
        "Atan2", y=y, x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          atan2, y=y, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Atan2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def atan2_eager_fallback(y, x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function atan2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([y, x], _ctx)
  (y, x) = _inputs_T
  _inputs_flat = [y, x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Atan2", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Atan2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.atanh', 'atanh')
def atanh(x, name=None):
  r"""Computes inverse hyperbolic tangent of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Atanh", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return atanh_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              atanh, x=x, name=name)
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
        "Atanh", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          atanh, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Atanh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def atanh_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function atanh
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Atanh", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Atanh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_mat_mul(x, y, adj_x=False, adj_y=False, name=None):
  r"""Multiplies slices of two tensors in batches.

  Multiplies all slices of `Tensor` `x` and `y` (each slice can be
  viewed as an element of a batch), and arranges the individual results
  in a single output tensor of the same batch size. Each of the
  individual slices can optionally be adjointed (to adjoint a matrix
  means to transpose and conjugate it) before multiplication by setting
  the `adj_x` or `adj_y` flag to `True`, which are by default `False`.

  The input tensors `x` and `y` are 2-D or higher with shape `[..., r_x, c_x]`
  and `[..., r_y, c_y]`.

  The output tensor is 2-D or higher with shape `[..., r_o, c_o]`, where:

      r_o = c_x if adj_x else r_x
      c_o = r_y if adj_y else c_y

  It is computed as:

      output[..., :, :] = matrix(x[..., :, :]) * matrix(y[..., :, :])

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
      2-D or higher with shape `[..., r_x, c_x]`.
    y: A `Tensor`. Must have the same type as `x`.
      2-D or higher with shape `[..., r_y, c_y]`.
    adj_x: An optional `bool`. Defaults to `False`.
      If `True`, adjoint the slices of `x`. Defaults to `False`.
    adj_y: An optional `bool`. Defaults to `False`.
      If `True`, adjoint the slices of `y`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchMatMul",
        name, _ctx._post_execution_callbacks, x, y, "adj_x", adj_x, "adj_y",
        adj_y)
      return _result
    except _core._FallbackException:
      try:
        return batch_mat_mul_eager_fallback(
            x, y, adj_x=adj_x, adj_y=adj_y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if adj_x is None:
    adj_x = False
  adj_x = _execute.make_bool(adj_x, "adj_x")
  if adj_y is None:
    adj_y = False
  adj_y = _execute.make_bool(adj_y, "adj_y")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatMul", x=x, y=y, adj_x=adj_x, adj_y=adj_y, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "adj_x", _op.get_attr("adj_x"), "adj_y",
            _op.get_attr("adj_y"))
  _execute.record_gradient(
      "BatchMatMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_mat_mul_eager_fallback(x, y, adj_x=False, adj_y=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_mat_mul
  """
  _ctx = ctx if ctx else _context.context()
  if adj_x is None:
    adj_x = False
  adj_x = _execute.make_bool(adj_x, "adj_x")
  if adj_y is None:
    adj_y = False
  adj_y = _execute.make_bool(adj_y, "adj_y")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T, "adj_x", adj_x, "adj_y", adj_y)
  _result = _execute.execute(b"BatchMatMul", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchMatMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.bessel_i0e')
def bessel_i0e(x, name=None):
  r"""Computes the Bessel i0e function of `x` element-wise.

  Exponentially scaled modified Bessel function of order 0 defined as
  `bessel_i0e(x) = exp(-abs(x)) bessel_i0(x)`.

  This function is faster and numerically stabler than `bessel_i0(x)`.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BesselI0e",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return bessel_i0e_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bessel_i0e, x=x, name=name)
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
        "BesselI0e", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bessel_i0e, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BesselI0e", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bessel_i0e_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bessel_i0e
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BesselI0e", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BesselI0e", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.bessel_i1e')
def bessel_i1e(x, name=None):
  r"""Computes the Bessel i1e function of `x` element-wise.

  Exponentially scaled modified Bessel function of order 0 defined as
  `bessel_i1e(x) = exp(-abs(x)) bessel_i1(x)`.

  This function is faster and numerically stabler than `bessel_i1(x)`.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BesselI1e",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return bessel_i1e_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bessel_i1e, x=x, name=name)
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
        "BesselI1e", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bessel_i1e, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BesselI1e", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bessel_i1e_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bessel_i1e
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BesselI1e", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BesselI1e", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.betainc', v1=['math.betainc', 'betainc'])
@deprecated_endpoints('betainc')
def betainc(a, b, x, name=None):
  r"""Compute the regularized incomplete beta integral \\(I_x(a, b)\\).

  The regularized incomplete beta integral is defined as:


  \\(I_x(a, b) = \frac{B(x; a, b)}{B(a, b)}\\)

  where


  \\(B(x; a, b) = \int_0^x t^{a-1} (1 - t)^{b-1} dt\\)


  is the incomplete beta function and \\(B(a, b)\\) is the *complete*
  beta function.

  Args:
    a: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    b: A `Tensor`. Must have the same type as `a`.
    x: A `Tensor`. Must have the same type as `a`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `a`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Betainc",
        name, _ctx._post_execution_callbacks, a, b, x)
      return _result
    except _core._FallbackException:
      try:
        return betainc_eager_fallback(
            a, b, x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              betainc, a=a, b=b, x=x, name=name)
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
        "Betainc", a=a, b=b, x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          betainc, a=a, b=b, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Betainc", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def betainc_eager_fallback(a, b, x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function betainc
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a, b, x], _ctx)
  (a, b, x) = _inputs_T
  _inputs_flat = [a, b, x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Betainc", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Betainc", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def bincount(arr, size, weights, name=None):
  r"""Counts the number of occurrences of each value in an integer array.

  Outputs a vector with length `size` and the same dtype as `weights`. If
  `weights` are empty, then index `i` stores the number of times the value `i` is
  counted in `arr`. If `weights` are non-empty, then index `i` stores the sum of
  the value in `weights` at each index where the corresponding value in `arr` is
  `i`.

  Values in `arr` outside of the range [0, size) are ignored.

  Args:
    arr: A `Tensor` of type `int32`. int32 `Tensor`.
    size: A `Tensor` of type `int32`. non-negative int32 scalar `Tensor`.
    weights: A `Tensor`. Must be one of the following types: `int32`, `int64`, `float32`, `float64`.
      is an int32, int64, float32, or float64 `Tensor` with the same
      shape as `arr`, or a length-0 `Tensor`, in which case it acts as all weights
      equal to 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `weights`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Bincount",
        name, _ctx._post_execution_callbacks, arr, size, weights)
      return _result
    except _core._FallbackException:
      try:
        return bincount_eager_fallback(
            arr, size, weights, name=name, ctx=_ctx)
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
        "Bincount", arr=arr, size=size, weights=weights, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Bincount", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bincount_eager_fallback(arr, size, weights, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bincount
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (weights,) = _execute.args_to_matching_eager([weights], _ctx)
  arr = _ops.convert_to_tensor(arr, _dtypes.int32)
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  _inputs_flat = [arr, size, weights]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Bincount", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Bincount", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def bucketize(input, boundaries, name=None):
  r"""Bucketizes 'input' based on 'boundaries'.

  For example, if the inputs are
      boundaries = [0, 10, 100]
      input = [[-5, 10000]
               [150,   10]
               [5,    100]]

  then the output will be
      output = [[0, 3]
                [3, 2]
                [1, 3]]

  Args:
    input: A `Tensor`. Must be one of the following types: `int32`, `int64`, `float32`, `float64`.
      Any shape of Tensor contains with int or float type.
    boundaries: A list of `floats`.
      A sorted list of floats gives the boundary of the buckets.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Bucketize",
        name, _ctx._post_execution_callbacks, input, "boundaries", boundaries)
      return _result
    except _core._FallbackException:
      try:
        return bucketize_eager_fallback(
            input, boundaries=boundaries, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(boundaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'boundaries' argument to "
        "'bucketize' Op, not %r." % boundaries)
  boundaries = [_execute.make_float(_f, "boundaries") for _f in boundaries]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Bucketize", input=input, boundaries=boundaries, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "boundaries", _op.get_attr("boundaries"))
  _execute.record_gradient(
      "Bucketize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bucketize_eager_fallback(input, boundaries, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bucketize
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(boundaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'boundaries' argument to "
        "'bucketize' Op, not %r." % boundaries)
  boundaries = [_execute.make_float(_f, "boundaries") for _f in boundaries]
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "boundaries", boundaries)
  _result = _execute.execute(b"Bucketize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Bucketize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def cast(x, DstT, Truncate=False, name=None):
  r"""Cast x of type SrcT to y of DstT.

  Args:
    x: A `Tensor`.
    DstT: A `tf.DType`.
    Truncate: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `DstT`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Cast", name,
        _ctx._post_execution_callbacks, x, "DstT", DstT, "Truncate", Truncate)
      return _result
    except _core._FallbackException:
      try:
        return cast_eager_fallback(
            x, DstT=DstT, Truncate=Truncate, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  DstT = _execute.make_type(DstT, "DstT")
  if Truncate is None:
    Truncate = False
  Truncate = _execute.make_bool(Truncate, "Truncate")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Cast", x=x, DstT=DstT, Truncate=Truncate, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("SrcT", _op.get_attr("SrcT"), "DstT", _op.get_attr("DstT"),
            "Truncate", _op.get_attr("Truncate"))
  _execute.record_gradient(
      "Cast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cast_eager_fallback(x, DstT, Truncate=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cast
  """
  _ctx = ctx if ctx else _context.context()
  DstT = _execute.make_type(DstT, "DstT")
  if Truncate is None:
    Truncate = False
  Truncate = _execute.make_bool(Truncate, "Truncate")
  _attr_SrcT, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("SrcT", _attr_SrcT, "DstT", DstT, "Truncate", Truncate)
  _result = _execute.execute(b"Cast", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Cast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.ceil', v1=['math.ceil', 'ceil'])
@deprecated_endpoints('ceil')
def ceil(x, name=None):
  r"""Returns element-wise smallest integer not less than x.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Ceil", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return ceil_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              ceil, x=x, name=name)
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
        "Ceil", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          ceil, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Ceil", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ceil_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ceil
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Ceil", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Ceil", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _clip_by_value(t, clip_value_min, clip_value_max, name=None):
  r"""Clips tensor values to a specified min and max.

  Given a tensor `t`, this operation returns a tensor of the same type and
  shape as `t` with its values clipped to `clip_value_min` and `clip_value_max`.
  Any values less than `clip_value_min` are set to `clip_value_min`. Any values
  greater than `clip_value_max` are set to `clip_value_max`.

  Args:
    t: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A `Tensor`.
    clip_value_min: A `Tensor`. Must have the same type as `t`.
      A 0-D (scalar) `Tensor`, or a `Tensor` with the same shape
      as `t`. The minimum value to clip by.
    clip_value_max: A `Tensor`. Must have the same type as `t`.
      A 0-D (scalar) `Tensor`, or a `Tensor` with the same shape
      as `t`. The maximum value to clip by.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `t`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ClipByValue",
        name, _ctx._post_execution_callbacks, t, clip_value_min,
        clip_value_max)
      return _result
    except _core._FallbackException:
      try:
        return _clip_by_value_eager_fallback(
            t, clip_value_min, clip_value_max, name=name, ctx=_ctx)
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
        "ClipByValue", t=t, clip_value_min=clip_value_min,
                       clip_value_max=clip_value_max, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "ClipByValue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _clip_by_value_eager_fallback(t, clip_value_min, clip_value_max, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _clip_by_value
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([t, clip_value_min, clip_value_max], _ctx)
  (t, clip_value_min, clip_value_max) = _inputs_T
  _inputs_flat = [t, clip_value_min, clip_value_max]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"ClipByValue", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ClipByValue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def compare_and_bitpack(input, threshold, name=None):
  r"""Compare values of `input` to `threshold` and pack resulting bits into a `uint8`.

  Each comparison returns a boolean `true` (if `input_value > threshold`)
  or and `false` otherwise.

  This operation is useful for Locality-Sensitive-Hashing (LSH) and other
  algorithms that use hashing approximations of cosine and `L2` distances;
  codes can be generated from an input via:

  ```python
  codebook_size = 50
  codebook_bits = codebook_size * 32
  codebook = tf.get_variable('codebook', [x.shape[-1].value, codebook_bits],
                             dtype=x.dtype,
                             initializer=tf.orthogonal_initializer())
  codes = compare_and_threshold(tf.matmul(x, codebook), threshold=0.)
  codes = tf.bitcast(codes, tf.int32)  # go from uint8 to int32
  # now codes has shape x.shape[:-1] + [codebook_size]
  ```

  **NOTE**: Currently, the innermost dimension of the tensor must be divisible
  by 8.

  Given an `input` shaped `[s0, s1, ..., s_n]`, the output is
  a `uint8` tensor shaped `[s0, s1, ..., s_n / 8]`.

  Args:
    input: A `Tensor`. Must be one of the following types: `bool`, `half`, `float32`, `float64`, `int8`, `int16`, `int32`, `int64`.
      Values to compare against `threshold` and bitpack.
    threshold: A `Tensor`. Must have the same type as `input`.
      Threshold to compare against.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `uint8`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CompareAndBitpack", name, _ctx._post_execution_callbacks, input,
        threshold)
      return _result
    except _core._FallbackException:
      try:
        return compare_and_bitpack_eager_fallback(
            input, threshold, name=name, ctx=_ctx)
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
        "CompareAndBitpack", input=input, threshold=threshold, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "CompareAndBitpack", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def compare_and_bitpack_eager_fallback(input, threshold, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function compare_and_bitpack
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, threshold], _ctx)
  (input, threshold) = _inputs_T
  _inputs_flat = [input, threshold]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"CompareAndBitpack", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CompareAndBitpack", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _complex(real, imag, Tout=_dtypes.complex64, name=None):
  r"""Converts two real numbers to a complex number.

  Given a tensor `real` representing the real part of a complex number, and a
  tensor `imag` representing the imaginary part of a complex number, this
  operation returns complex numbers elementwise of the form \\(a + bj\\), where
  *a* represents the `real` part and *b* represents the `imag` part.

  The input tensors `real` and `imag` must have the same shape.

  For example:

  ```
  # tensor 'real' is [2.25, 3.25]
  # tensor `imag` is [4.75, 5.75]
  tf.complex(real, imag) ==> [[2.25 + 4.75j], [3.25 + 5.75j]]
  ```

  Args:
    real: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    imag: A `Tensor`. Must have the same type as `real`.
    Tout: An optional `tf.DType` from: `tf.complex64, tf.complex128`. Defaults to `tf.complex64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Complex",
        name, _ctx._post_execution_callbacks, real, imag, "Tout", Tout)
      return _result
    except _core._FallbackException:
      try:
        return _complex_eager_fallback(
            real, imag, Tout=Tout, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if Tout is None:
    Tout = _dtypes.complex64
  Tout = _execute.make_type(Tout, "Tout")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Complex", real=real, imag=imag, Tout=Tout, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "Complex", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _complex_eager_fallback(real, imag, Tout=_dtypes.complex64, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _complex
  """
  _ctx = ctx if ctx else _context.context()
  if Tout is None:
    Tout = _dtypes.complex64
  Tout = _execute.make_type(Tout, "Tout")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([real, imag], _ctx, _dtypes.float32)
  (real, imag) = _inputs_T
  _inputs_flat = [real, imag]
  _attrs = ("T", _attr_T, "Tout", Tout)
  _result = _execute.execute(b"Complex", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Complex", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def complex_abs(x, Tout=_dtypes.float32, name=None):
  r"""Computes the complex absolute value of a tensor.

  Given a tensor `x` of complex numbers, this operation returns a tensor of type
  `float` or `double` that is the absolute value of each element in `x`. All
  elements in `x` must be complex numbers of the form \\(a + bj\\). The absolute
  value is computed as \\( \sqrt{a^2 + b^2}\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
    Tout: An optional `tf.DType` from: `tf.float32, tf.float64`. Defaults to `tf.float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ComplexAbs",
        name, _ctx._post_execution_callbacks, x, "Tout", Tout)
      return _result
    except _core._FallbackException:
      try:
        return complex_abs_eager_fallback(
            x, Tout=Tout, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if Tout is None:
    Tout = _dtypes.float32
  Tout = _execute.make_type(Tout, "Tout")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ComplexAbs", x=x, Tout=Tout, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "ComplexAbs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def complex_abs_eager_fallback(x, Tout=_dtypes.float32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function complex_abs
  """
  _ctx = ctx if ctx else _context.context()
  if Tout is None:
    Tout = _dtypes.float32
  Tout = _execute.make_type(Tout, "Tout")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx, _dtypes.complex64)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T, "Tout", Tout)
  _result = _execute.execute(b"ComplexAbs", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ComplexAbs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def conj(input, name=None):
  r"""Returns the complex conjugate of a complex number.

  Given a tensor `input` of complex numbers, this operation returns a tensor of
  complex numbers that are the complex conjugate of each element in `input`. The
  complex numbers in `input` must be of the form \\(a + bj\\), where *a* is the
  real part and *b* is the imaginary part.

  The complex conjugate returned by this operation is of the form \\(a - bj\\).

  For example:

  ```
  # tensor 'input' is [-2.25 + 4.75j, 3.25 + 5.75j]
  tf.conj(input) ==> [-2.25 - 4.75j, 3.25 - 5.75j]
  ```

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`, `variant`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Conj", name,
        _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return conj_eager_fallback(
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
        "Conj", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Conj", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conj_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conj
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Conj", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Conj", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.cos', 'cos')
def cos(x, name=None):
  r"""Computes cos of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Cos", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return cos_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              cos, x=x, name=name)
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
        "Cos", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          cos, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Cos", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cos_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cos
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Cos", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Cos", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.cosh', 'cosh')
def cosh(x, name=None):
  r"""Computes hyperbolic cosine of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Cosh", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return cosh_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              cosh, x=x, name=name)
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
        "Cosh", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          cosh, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Cosh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cosh_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cosh
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Cosh", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Cosh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.cross', v1=['linalg.cross', 'cross'])
@deprecated_endpoints('cross')
def cross(a, b, name=None):
  r"""Compute the pairwise cross product.

  `a` and `b` must be the same shape; they can either be simple 3-element vectors,
  or any shape where the innermost dimension is 3. In the latter case, each pair
  of corresponding 3-element vectors is cross-multiplied independently.

  Args:
    a: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      A tensor containing 3-element vectors.
    b: A `Tensor`. Must have the same type as `a`.
      Another tensor, of same type and shape as `a`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `a`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Cross", name,
        _ctx._post_execution_callbacks, a, b)
      return _result
    except _core._FallbackException:
      try:
        return cross_eager_fallback(
            a, b, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              cross, a=a, b=b, name=name)
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
        "Cross", a=a, b=b, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          cross, a=a, b=b, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Cross", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cross_eager_fallback(a, b, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cross
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a, b], _ctx)
  (a, b) = _inputs_T
  _inputs_flat = [a, b]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Cross", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Cross", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def cumprod(x, axis, exclusive=False, reverse=False, name=None):
  r"""Compute the cumulative product of the tensor `x` along `axis`.

  By default, this op performs an inclusive cumprod, which means that the first
  element of the input is identical to the first element of the output:

  ```python
  tf.cumprod([a, b, c])  # => [a, a * b, a * b * c]
  ```

  By setting the `exclusive` kwarg to `True`, an exclusive cumprod is
  performed instead:

  ```python
  tf.cumprod([a, b, c], exclusive=True)  # => [1, a, a * b]
  ```

  By setting the `reverse` kwarg to `True`, the cumprod is performed in the
  opposite direction:

  ```python
  tf.cumprod([a, b, c], reverse=True)  # => [a * b * c, b * c, c]
  ```

  This is more efficient than using separate `tf.reverse` ops.

  The `reverse` and `exclusive` kwargs can also be combined:

  ```python
  tf.cumprod([a, b, c], exclusive=True, reverse=True)  # => [b * c, c, 1]
  ```

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A `Tensor`. Must be one of the following types: `float32`, `float64`,
      `int64`, `int32`, `uint8`, `uint16`, `int16`, `int8`, `complex64`,
      `complex128`, `qint8`, `quint8`, `qint32`, `half`.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A `Tensor` of type `int32` (default: 0). Must be in the range
      `[-rank(x), rank(x))`.
    exclusive: An optional `bool`. Defaults to `False`.
      If `True`, perform exclusive cumprod.
    reverse: An optional `bool`. Defaults to `False`.
      A `bool` (default: False).
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Cumprod",
        name, _ctx._post_execution_callbacks, x, axis, "exclusive", exclusive,
        "reverse", reverse)
      return _result
    except _core._FallbackException:
      try:
        return cumprod_eager_fallback(
            x, axis, exclusive=exclusive, reverse=reverse, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if exclusive is None:
    exclusive = False
  exclusive = _execute.make_bool(exclusive, "exclusive")
  if reverse is None:
    reverse = False
  reverse = _execute.make_bool(reverse, "reverse")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Cumprod", x=x, axis=axis, exclusive=exclusive, reverse=reverse,
                   name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("exclusive", _op.get_attr("exclusive"), "reverse",
            _op.get_attr("reverse"), "T", _op.get_attr("T"), "Tidx",
            _op.get_attr("Tidx"))
  _execute.record_gradient(
      "Cumprod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cumprod_eager_fallback(x, axis, exclusive=False, reverse=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cumprod
  """
  _ctx = ctx if ctx else _context.context()
  if exclusive is None:
    exclusive = False
  exclusive = _execute.make_bool(exclusive, "exclusive")
  if reverse is None:
    reverse = False
  reverse = _execute.make_bool(reverse, "reverse")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _inputs_flat = [x, axis]
  _attrs = ("exclusive", exclusive, "reverse", reverse, "T", _attr_T, "Tidx",
  _attr_Tidx)
  _result = _execute.execute(b"Cumprod", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Cumprod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def cumsum(x, axis, exclusive=False, reverse=False, name=None):
  r"""Compute the cumulative sum of the tensor `x` along `axis`.

  By default, this op performs an inclusive cumsum, which means that the first
  element of the input is identical to the first element of the output:

  ```python
  tf.cumsum([a, b, c])  # => [a, a + b, a + b + c]
  ```

  By setting the `exclusive` kwarg to `True`, an exclusive cumsum is
  performed instead:

  ```python
  tf.cumsum([a, b, c], exclusive=True)  # => [0, a, a + b]
  ```

  By setting the `reverse` kwarg to `True`, the cumsum is performed in the
  opposite direction:

  ```python
  tf.cumsum([a, b, c], reverse=True)  # => [a + b + c, b + c, c]
  ```

  This is more efficient than using separate `tf.reverse` ops.

  The `reverse` and `exclusive` kwargs can also be combined:

  ```python
  tf.cumsum([a, b, c], exclusive=True, reverse=True)  # => [b + c, c, 0]
  ```

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A `Tensor`. Must be one of the following types: `float32`, `float64`,
      `int64`, `int32`, `uint8`, `uint16`, `int16`, `int8`, `complex64`,
      `complex128`, `qint8`, `quint8`, `qint32`, `half`.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A `Tensor` of type `int32` (default: 0). Must be in the range
      `[-rank(x), rank(x))`.
    exclusive: An optional `bool`. Defaults to `False`.
      If `True`, perform exclusive cumsum.
    reverse: An optional `bool`. Defaults to `False`.
      A `bool` (default: False).
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Cumsum", name,
        _ctx._post_execution_callbacks, x, axis, "exclusive", exclusive,
        "reverse", reverse)
      return _result
    except _core._FallbackException:
      try:
        return cumsum_eager_fallback(
            x, axis, exclusive=exclusive, reverse=reverse, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if exclusive is None:
    exclusive = False
  exclusive = _execute.make_bool(exclusive, "exclusive")
  if reverse is None:
    reverse = False
  reverse = _execute.make_bool(reverse, "reverse")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Cumsum", x=x, axis=axis, exclusive=exclusive, reverse=reverse,
                  name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("exclusive", _op.get_attr("exclusive"), "reverse",
            _op.get_attr("reverse"), "T", _op.get_attr("T"), "Tidx",
            _op.get_attr("Tidx"))
  _execute.record_gradient(
      "Cumsum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cumsum_eager_fallback(x, axis, exclusive=False, reverse=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cumsum
  """
  _ctx = ctx if ctx else _context.context()
  if exclusive is None:
    exclusive = False
  exclusive = _execute.make_bool(exclusive, "exclusive")
  if reverse is None:
    reverse = False
  reverse = _execute.make_bool(reverse, "reverse")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _inputs_flat = [x, axis]
  _attrs = ("exclusive", exclusive, "reverse", reverse, "T", _attr_T, "Tidx",
  _attr_Tidx)
  _result = _execute.execute(b"Cumsum", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Cumsum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.digamma', v1=['math.digamma', 'digamma'])
@deprecated_endpoints('digamma')
def digamma(x, name=None):
  r"""Computes Psi, the derivative of Lgamma (the log of the absolute value of

  `Gamma(x)`), element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Digamma",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return digamma_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              digamma, x=x, name=name)
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
        "Digamma", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          digamma, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Digamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def digamma_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function digamma
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Digamma", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Digamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def div(x, y, name=None):
  r"""Returns x / y element-wise.

  *NOTE*: `Div` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `uint16`, `int16`, `int32`, `int64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Div", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return div_eager_fallback(
            x, y, name=name, ctx=_ctx)
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
        "Div", x=x, y=y, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Div", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def div_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function div
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Div", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Div", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def div_no_nan(x, y, name=None):
  r"""Returns 0 if the denominator is zero.

  
  *NOTE*: `DivNoNan` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DivNoNan",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return div_no_nan_eager_fallback(
            x, y, name=name, ctx=_ctx)
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
        "DivNoNan", x=x, y=y, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "DivNoNan", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def div_no_nan_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function div_no_nan
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"DivNoNan", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DivNoNan", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.equal', 'equal')
def equal(x, y, name=None):
  r"""Returns the truth value of (x == y) element-wise.

  *NOTE*: `math.equal` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `int16`, `int32`, `int64`, `complex64`, `quint8`, `qint8`, `qint32`, `string`, `bool`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Equal", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return equal_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              equal, x=x, y=y, name=name)
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
        "Equal", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          equal, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Equal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def equal_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function equal
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Equal", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Equal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.erf', v1=['math.erf', 'erf'])
@deprecated_endpoints('erf')
def erf(x, name=None):
  r"""Computes the Gauss error function of `x` element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Erf", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return erf_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              erf, x=x, name=name)
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
        "Erf", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          erf, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Erf", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def erf_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function erf
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Erf", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Erf", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.erfc', v1=['math.erfc', 'erfc'])
@deprecated_endpoints('erfc')
def erfc(x, name=None):
  r"""Computes the complementary error function of `x` element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Erfc", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return erfc_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              erfc, x=x, name=name)
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
        "Erfc", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          erfc, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Erfc", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def erfc_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function erfc
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Erfc", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Erfc", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.exp', 'exp')
def exp(x, name=None):
  r"""Computes exponential of x element-wise.  \\(y = e^x\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Exp", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return exp_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              exp, x=x, name=name)
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
        "Exp", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          exp, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Exp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def exp_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function exp
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Exp", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Exp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.expm1', v1=['math.expm1', 'expm1'])
@deprecated_endpoints('expm1')
def expm1(x, name=None):
  r"""Computes exponential of x - 1 element-wise.

  I.e., \\(y = (\exp x) - 1\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Expm1", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return expm1_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              expm1, x=x, name=name)
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
        "Expm1", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          expm1, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Expm1", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def expm1_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function expm1
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Expm1", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Expm1", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.floor', 'floor')
def floor(x, name=None):
  r"""Returns element-wise largest integer not greater than x.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Floor", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return floor_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              floor, x=x, name=name)
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
        "Floor", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          floor, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Floor", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def floor_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function floor
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Floor", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Floor", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('floor_div')
def floor_div(x, y, name=None):
  r"""Returns x // y element-wise.

  *NOTE*: `floor_div` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `uint16`, `int16`, `int32`, `int64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "FloorDiv",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return floor_div_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              floor_div, x=x, y=y, name=name)
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
        "FloorDiv", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          floor_div, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "FloorDiv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def floor_div_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function floor_div
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"FloorDiv", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FloorDiv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('floormod', 'mod')
def floor_mod(x, y, name=None):
  r"""Returns element-wise remainder of division. When `x < 0` xor `y < 0` is

  true, this follows Python semantics in that the result here is consistent
  with a flooring divide. E.g. `floor(x / y) * y + mod(x, y) = x`.

  *NOTE*: `floormod` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `int32`, `int64`, `bfloat16`, `half`, `float32`, `float64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "FloorMod",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return floor_mod_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              floor_mod, x=x, y=y, name=name)
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
        "FloorMod", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          floor_mod, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "FloorMod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def floor_mod_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function floor_mod
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"FloorMod", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FloorMod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.greater', 'greater')
def greater(x, y, name=None):
  r"""Returns the truth value of (x > y) element-wise.

  *NOTE*: `math.greater` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Greater",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return greater_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              greater, x=x, y=y, name=name)
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
        "Greater", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          greater, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Greater", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def greater_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function greater
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Greater", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Greater", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.greater_equal', 'greater_equal')
def greater_equal(x, y, name=None):
  r"""Returns the truth value of (x >= y) element-wise.

  *NOTE*: `math.greater_equal` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "GreaterEqual",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return greater_equal_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              greater_equal, x=x, y=y, name=name)
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
        "GreaterEqual", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          greater_equal, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "GreaterEqual", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def greater_equal_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function greater_equal
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"GreaterEqual", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GreaterEqual", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _histogram_fixed_width(values, value_range, nbins, dtype=_dtypes.int32, name=None):
  r"""Return histogram of values.

  Given the tensor `values`, this operation returns a rank 1 histogram counting
  the number of entries in `values` that fall into every bin.  The bins are
  equal width and determined by the arguments `value_range` and `nbins`.

  ```python
  # Bins will be:  (-inf, 1), [1, 2), [2, 3), [3, 4), [4, inf)
  nbins = 5
  value_range = [0.0, 5.0]
  new_values = [-1.0, 0.0, 1.5, 2.0, 5.0, 15]

  with tf.get_default_session() as sess:
    hist = tf.histogram_fixed_width(new_values, value_range, nbins=5)
    variables.global_variables_initializer().run()
    sess.run(hist) => [2, 1, 1, 0, 2]
  ```

  Args:
    values: A `Tensor`. Must be one of the following types: `int32`, `int64`, `float32`, `float64`.
      Numeric `Tensor`.
    value_range: A `Tensor`. Must have the same type as `values`.
      Shape [2] `Tensor` of same `dtype` as `values`.
      values <= value_range[0] will be mapped to hist[0],
      values >= value_range[1] will be mapped to hist[-1].
    nbins: A `Tensor` of type `int32`.
      Scalar `int32 Tensor`.  Number of histogram bins.
    dtype: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "HistogramFixedWidth", name, _ctx._post_execution_callbacks, values,
        value_range, nbins, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return _histogram_fixed_width_eager_fallback(
            values, value_range, nbins, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if dtype is None:
    dtype = _dtypes.int32
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "HistogramFixedWidth", values=values, value_range=value_range,
                               nbins=nbins, dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "HistogramFixedWidth", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _histogram_fixed_width_eager_fallback(values, value_range, nbins, dtype=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _histogram_fixed_width
  """
  _ctx = ctx if ctx else _context.context()
  if dtype is None:
    dtype = _dtypes.int32
  dtype = _execute.make_type(dtype, "dtype")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([values, value_range], _ctx)
  (values, value_range) = _inputs_T
  nbins = _ops.convert_to_tensor(nbins, _dtypes.int32)
  _inputs_flat = [values, value_range, nbins]
  _attrs = ("T", _attr_T, "dtype", dtype)
  _result = _execute.execute(b"HistogramFixedWidth", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "HistogramFixedWidth", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.igamma', v1=['math.igamma', 'igamma'])
@deprecated_endpoints('igamma')
def igamma(a, x, name=None):
  r"""Compute the lower regularized incomplete Gamma function `P(a, x)`.

  The lower regularized incomplete Gamma function is defined as:


  \\(P(a, x) = gamma(a, x) / Gamma(a) = 1 - Q(a, x)\\)

  where

  \\(gamma(a, x) = \\int_{0}^{x} t^{a-1} exp(-t) dt\\)

  is the lower incomplete Gamma function.

  Note, above `Q(a, x)` (`Igammac`) is the upper regularized complete
  Gamma function.

  Args:
    a: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    x: A `Tensor`. Must have the same type as `a`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `a`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Igamma", name,
        _ctx._post_execution_callbacks, a, x)
      return _result
    except _core._FallbackException:
      try:
        return igamma_eager_fallback(
            a, x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              igamma, a=a, x=x, name=name)
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
        "Igamma", a=a, x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          igamma, a=a, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Igamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def igamma_eager_fallback(a, x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function igamma
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a, x], _ctx)
  (a, x) = _inputs_T
  _inputs_flat = [a, x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Igamma", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Igamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def igamma_grad_a(a, x, name=None):
  r"""Computes the gradient of `igamma(a, x)` wrt `a`.

  Args:
    a: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    x: A `Tensor`. Must have the same type as `a`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `a`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IgammaGradA",
        name, _ctx._post_execution_callbacks, a, x)
      return _result
    except _core._FallbackException:
      try:
        return igamma_grad_a_eager_fallback(
            a, x, name=name, ctx=_ctx)
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
        "IgammaGradA", a=a, x=x, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "IgammaGradA", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def igamma_grad_a_eager_fallback(a, x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function igamma_grad_a
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a, x], _ctx)
  (a, x) = _inputs_T
  _inputs_flat = [a, x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"IgammaGradA", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "IgammaGradA", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.igammac', v1=['math.igammac', 'igammac'])
@deprecated_endpoints('igammac')
def igammac(a, x, name=None):
  r"""Compute the upper regularized incomplete Gamma function `Q(a, x)`.

  The upper regularized incomplete Gamma function is defined as:

  \\(Q(a, x) = Gamma(a, x) / Gamma(a) = 1 - P(a, x)\\)

  where

  \\(Gamma(a, x) = int_{x}^{\infty} t^{a-1} exp(-t) dt\\)

  is the upper incomplete Gama function.

  Note, above `P(a, x)` (`Igamma`) is the lower regularized complete
  Gamma function.

  Args:
    a: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    x: A `Tensor`. Must have the same type as `a`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `a`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Igammac",
        name, _ctx._post_execution_callbacks, a, x)
      return _result
    except _core._FallbackException:
      try:
        return igammac_eager_fallback(
            a, x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              igammac, a=a, x=x, name=name)
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
        "Igammac", a=a, x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          igammac, a=a, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Igammac", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def igammac_eager_fallback(a, x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function igammac
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a, x], _ctx)
  (a, x) = _inputs_T
  _inputs_flat = [a, x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Igammac", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Igammac", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def imag(input, Tout=_dtypes.float32, name=None):
  r"""Returns the imaginary part of a complex number.

  Given a tensor `input` of complex numbers, this operation returns a tensor of
  type `float` that is the imaginary part of each element in `input`. All
  elements in `input` must be complex numbers of the form \\(a + bj\\), where *a*
  is the real part and *b* is the imaginary part returned by this operation.

  For example:

  ```
  # tensor 'input' is [-2.25 + 4.75j, 3.25 + 5.75j]
  tf.imag(input) ==> [4.75, 5.75]
  ```

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
    Tout: An optional `tf.DType` from: `tf.float32, tf.float64`. Defaults to `tf.float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Imag", name,
        _ctx._post_execution_callbacks, input, "Tout", Tout)
      return _result
    except _core._FallbackException:
      try:
        return imag_eager_fallback(
            input, Tout=Tout, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if Tout is None:
    Tout = _dtypes.float32
  Tout = _execute.make_type(Tout, "Tout")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Imag", input=input, Tout=Tout, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "Imag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def imag_eager_fallback(input, Tout=_dtypes.float32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function imag
  """
  _ctx = ctx if ctx else _context.context()
  if Tout is None:
    Tout = _dtypes.float32
  Tout = _execute.make_type(Tout, "Tout")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "Tout", Tout)
  _result = _execute.execute(b"Imag", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Imag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def inv(x, name=None):
  r"""Computes the reciprocal of x element-wise.

  I.e., \\(y = 1 / x\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Inv", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return inv_eager_fallback(
            x, name=name, ctx=_ctx)
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
        "Inv", x=x, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Inv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def inv_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function inv
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Inv", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Inv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def inv_grad(y, dy, name=None):
  r"""Computes the gradient for the inverse of `x` wrt its input.

  Specifically, `grad = -dy * y*y`, where `y = 1/x`, and `dy`
  is the corresponding input gradient.

  Args:
    y: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    dy: A `Tensor`. Must have the same type as `y`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `y`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "InvGrad",
        name, _ctx._post_execution_callbacks, y, dy)
      return _result
    except _core._FallbackException:
      try:
        return inv_grad_eager_fallback(
            y, dy, name=name, ctx=_ctx)
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
        "InvGrad", y=y, dy=dy, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "InvGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def inv_grad_eager_fallback(y, dy, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function inv_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([y, dy], _ctx)
  (y, dy) = _inputs_T
  _inputs_flat = [y, dy]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"InvGrad", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "InvGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.is_finite', v1=['math.is_finite', 'debugging.is_finite', 'is_finite'])
@deprecated_endpoints('debugging.is_finite', 'is_finite')
def is_finite(x, name=None):
  r"""Returns which elements of x are finite.

  @compatibility(numpy)
  Equivalent to np.isfinite
  @end_compatibility

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IsFinite",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return is_finite_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              is_finite, x=x, name=name)
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
        "IsFinite", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          is_finite, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "IsFinite", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def is_finite_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function is_finite
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"IsFinite", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "IsFinite", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.is_inf', v1=['math.is_inf', 'debugging.is_inf', 'is_inf'])
@deprecated_endpoints('debugging.is_inf', 'is_inf')
def is_inf(x, name=None):
  r"""Returns which elements of x are Inf.

  @compatibility(numpy)
  Equivalent to np.isinf
  @end_compatibility

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IsInf", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return is_inf_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              is_inf, x=x, name=name)
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
        "IsInf", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          is_inf, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "IsInf", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def is_inf_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function is_inf
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"IsInf", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "IsInf", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.is_nan', v1=['math.is_nan', 'debugging.is_nan', 'is_nan'])
@deprecated_endpoints('debugging.is_nan', 'is_nan')
def is_nan(x, name=None):
  r"""Returns which elements of x are NaN.

  @compatibility(numpy)
  Equivalent to np.isnan
  @end_compatibility

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IsNan", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return is_nan_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              is_nan, x=x, name=name)
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
        "IsNan", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          is_nan, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "IsNan", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def is_nan_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function is_nan
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"IsNan", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "IsNan", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.less', 'less')
def less(x, y, name=None):
  r"""Returns the truth value of (x < y) element-wise.

  *NOTE*: `math.less` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Less", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return less_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              less, x=x, y=y, name=name)
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
        "Less", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          less, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Less", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def less_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function less
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Less", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Less", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.less_equal', 'less_equal')
def less_equal(x, y, name=None):
  r"""Returns the truth value of (x <= y) element-wise.

  *NOTE*: `math.less_equal` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LessEqual",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return less_equal_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              less_equal, x=x, y=y, name=name)
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
        "LessEqual", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          less_equal, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "LessEqual", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def less_equal_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function less_equal
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"LessEqual", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LessEqual", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.lgamma', v1=['math.lgamma', 'lgamma'])
@deprecated_endpoints('lgamma')
def lgamma(x, name=None):
  r"""Computes the log of the absolute value of `Gamma(x)` element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Lgamma", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return lgamma_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              lgamma, x=x, name=name)
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
        "Lgamma", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          lgamma, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Lgamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lgamma_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lgamma
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Lgamma", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Lgamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linspace', v1=['lin_space', 'linspace'])
@deprecated_endpoints('lin_space')
def lin_space(start, stop, num, name=None):
  r"""Generates values in an interval.

  A sequence of `num` evenly-spaced values are generated beginning at `start`.
  If `num > 1`, the values in the sequence increase by `stop - start / num - 1`,
  so that the last one is exactly `stop`.

  For example:

  ```
  tf.linspace(10.0, 12.0, 3, name="linspace") => [ 10.0  11.0  12.0]
  ```

  Args:
    start: A `Tensor`. Must be one of the following types: `bfloat16`, `float32`, `float64`.
      0-D tensor. First entry in the range.
    stop: A `Tensor`. Must have the same type as `start`.
      0-D tensor. Last entry in the range.
    num: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      0-D tensor. Number of values to generate.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `start`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LinSpace",
        name, _ctx._post_execution_callbacks, start, stop, num)
      return _result
    except _core._FallbackException:
      try:
        return lin_space_eager_fallback(
            start, stop, num, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              lin_space, start=start, stop=stop, num=num, name=name)
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
        "LinSpace", start=start, stop=stop, num=num, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          lin_space, start=start, stop=stop, num=num, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "LinSpace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lin_space_eager_fallback(start, stop, num, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lin_space
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([start, stop], _ctx)
  (start, stop) = _inputs_T
  _attr_Tidx, (num,) = _execute.args_to_matching_eager([num], _ctx, _dtypes.int32)
  _inputs_flat = [start, stop, num]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"LinSpace", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LinSpace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.log', v1=['math.log', 'log'])
@deprecated_endpoints('log')
def log(x, name=None):
  r"""Computes natural logarithm of x element-wise.

  I.e., \\(y = \log_e x\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Log", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return log_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              log, x=x, name=name)
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
        "Log", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          log, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Log", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def log_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function log
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Log", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Log", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.log1p', v1=['math.log1p', 'log1p'])
@deprecated_endpoints('log1p')
def log1p(x, name=None):
  r"""Computes natural logarithm of (1 + x) element-wise.

  I.e., \\(y = \log_e (1 + x)\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Log1p", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return log1p_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              log1p, x=x, name=name)
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
        "Log1p", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          log1p, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Log1p", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def log1p_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function log1p
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Log1p", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Log1p", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.logical_and', 'logical_and')
def logical_and(x, y, name=None):
  r"""Returns the truth value of x AND y element-wise.

  *NOTE*: `math.logical_and` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor` of type `bool`.
    y: A `Tensor` of type `bool`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LogicalAnd",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return logical_and_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              logical_and, x=x, y=y, name=name)
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
        "LogicalAnd", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          logical_and, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "LogicalAnd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def logical_and_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function logical_and
  """
  _ctx = ctx if ctx else _context.context()
  x = _ops.convert_to_tensor(x, _dtypes.bool)
  y = _ops.convert_to_tensor(y, _dtypes.bool)
  _inputs_flat = [x, y]
  _attrs = None
  _result = _execute.execute(b"LogicalAnd", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LogicalAnd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.logical_not', 'logical_not')
def logical_not(x, name=None):
  r"""Returns the truth value of NOT x element-wise.

  Args:
    x: A `Tensor` of type `bool`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LogicalNot",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return logical_not_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              logical_not, x=x, name=name)
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
        "LogicalNot", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          logical_not, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "LogicalNot", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def logical_not_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function logical_not
  """
  _ctx = ctx if ctx else _context.context()
  x = _ops.convert_to_tensor(x, _dtypes.bool)
  _inputs_flat = [x]
  _attrs = None
  _result = _execute.execute(b"LogicalNot", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LogicalNot", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.logical_or', 'logical_or')
def logical_or(x, y, name=None):
  r"""Returns the truth value of x OR y element-wise.

  *NOTE*: `math.logical_or` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor` of type `bool`.
    y: A `Tensor` of type `bool`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LogicalOr",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return logical_or_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              logical_or, x=x, y=y, name=name)
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
        "LogicalOr", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          logical_or, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "LogicalOr", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def logical_or_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function logical_or
  """
  _ctx = ctx if ctx else _context.context()
  x = _ops.convert_to_tensor(x, _dtypes.bool)
  y = _ops.convert_to_tensor(y, _dtypes.bool)
  _inputs_flat = [x, y]
  _attrs = None
  _result = _execute.execute(b"LogicalOr", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LogicalOr", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mat_mul(a, b, transpose_a=False, transpose_b=False, name=None):
  r"""Multiply the matrix "a" by the matrix "b".

  The inputs must be two-dimensional matrices and the inner dimension of
  "a" (after being transposed if transpose_a is true) must match the
  outer dimension of "b" (after being transposed if transposed_b is
  true).

  *Note*: The default kernel implementation for MatMul on GPUs uses
  cublas.

  Args:
    a: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    b: A `Tensor`. Must have the same type as `a`.
    transpose_a: An optional `bool`. Defaults to `False`.
      If true, "a" is transposed before multiplication.
    transpose_b: An optional `bool`. Defaults to `False`.
      If true, "b" is transposed before multiplication.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `a`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MatMul", name,
        _ctx._post_execution_callbacks, a, b, "transpose_a", transpose_a,
        "transpose_b", transpose_b)
      return _result
    except _core._FallbackException:
      try:
        return mat_mul_eager_fallback(
            a, b, transpose_a=transpose_a, transpose_b=transpose_b, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if transpose_a is None:
    transpose_a = False
  transpose_a = _execute.make_bool(transpose_a, "transpose_a")
  if transpose_b is None:
    transpose_b = False
  transpose_b = _execute.make_bool(transpose_b, "transpose_b")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MatMul", a=a, b=b, transpose_a=transpose_a, transpose_b=transpose_b,
                  name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("transpose_a", _op.get_attr("transpose_a"), "transpose_b",
            _op.get_attr("transpose_b"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mat_mul_eager_fallback(a, b, transpose_a=False, transpose_b=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mat_mul
  """
  _ctx = ctx if ctx else _context.context()
  if transpose_a is None:
    transpose_a = False
  transpose_a = _execute.make_bool(transpose_a, "transpose_a")
  if transpose_b is None:
    transpose_b = False
  transpose_b = _execute.make_bool(transpose_b, "transpose_b")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a, b], _ctx)
  (a, b) = _inputs_T
  _inputs_flat = [a, b]
  _attrs = ("transpose_a", transpose_a, "transpose_b", transpose_b, "T",
  _attr_T)
  _result = _execute.execute(b"MatMul", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _max(input, axis, keep_dims=False, name=None):
  r"""Computes the maximum of elements across dimensions of a tensor.

  Reduces `input` along the dimensions given in `axis`. Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `axis`. If `keep_dims` is true, the reduced dimensions are
  retained with length 1.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The tensor to reduce.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The dimensions to reduce. Must be in the range
      `[-rank(input), rank(input))`.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Max", name,
        _ctx._post_execution_callbacks, input, axis, "keep_dims", keep_dims)
      return _result
    except _core._FallbackException:
      try:
        return _max_eager_fallback(
            input, axis, keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Max", input=input, reduction_indices=axis, keep_dims=keep_dims,
               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "T", _op.get_attr("T"),
            "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "Max", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _max_eager_fallback(input, axis, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _max
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _inputs_flat = [input, axis]
  _attrs = ("keep_dims", keep_dims, "T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"Max", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Max", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.maximum', 'maximum')
def maximum(x, y, name=None):
  r"""Returns the max of x and y (i.e. x > y ? x : y) element-wise.

  *NOTE*: `math.maximum` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Maximum",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return maximum_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              maximum, x=x, y=y, name=name)
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
        "Maximum", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          maximum, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Maximum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def maximum_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function maximum
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Maximum", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Maximum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mean(input, axis, keep_dims=False, name=None):
  r"""Computes the mean of elements across dimensions of a tensor.

  Reduces `input` along the dimensions given in `axis`. Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `axis`. If `keep_dims` is true, the reduced dimensions are
  retained with length 1.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The tensor to reduce.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The dimensions to reduce. Must be in the range
      `[-rank(input), rank(input))`.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Mean", name,
        _ctx._post_execution_callbacks, input, axis, "keep_dims", keep_dims)
      return _result
    except _core._FallbackException:
      try:
        return mean_eager_fallback(
            input, axis, keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Mean", input=input, reduction_indices=axis, keep_dims=keep_dims,
                name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "T", _op.get_attr("T"),
            "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "Mean", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mean_eager_fallback(input, axis, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mean
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _inputs_flat = [input, axis]
  _attrs = ("keep_dims", keep_dims, "T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"Mean", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Mean", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _min(input, axis, keep_dims=False, name=None):
  r"""Computes the minimum of elements across dimensions of a tensor.

  Reduces `input` along the dimensions given in `axis`. Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `axis`. If `keep_dims` is true, the reduced dimensions are
  retained with length 1.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The tensor to reduce.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The dimensions to reduce. Must be in the range
      `[-rank(input), rank(input))`.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Min", name,
        _ctx._post_execution_callbacks, input, axis, "keep_dims", keep_dims)
      return _result
    except _core._FallbackException:
      try:
        return _min_eager_fallback(
            input, axis, keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Min", input=input, reduction_indices=axis, keep_dims=keep_dims,
               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "T", _op.get_attr("T"),
            "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "Min", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _min_eager_fallback(input, axis, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _min
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _inputs_flat = [input, axis]
  _attrs = ("keep_dims", keep_dims, "T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"Min", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Min", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.minimum', 'minimum')
def minimum(x, y, name=None):
  r"""Returns the min of x and y (i.e. x < y ? x : y) element-wise.

  *NOTE*: `math.minimum` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Minimum",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return minimum_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              minimum, x=x, y=y, name=name)
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
        "Minimum", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          minimum, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Minimum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def minimum_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function minimum
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Minimum", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Minimum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mod(x, y, name=None):
  r"""Returns element-wise remainder of division. This emulates C semantics in that

  the result here is consistent with a truncating divide. E.g.
  `tf.truncatediv(x, y) * y + truncate_mod(x, y) = x`.

  *NOTE*: `Mod` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `int32`, `int64`, `half`, `half`, `bfloat16`, `float32`, `float64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Mod", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return mod_eager_fallback(
            x, y, name=name, ctx=_ctx)
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
        "Mod", x=x, y=y, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Mod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mod_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mod
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Mod", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Mod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mul(x, y, name=None):
  r"""Returns x * y element-wise.

  *NOTE*: `Multiply` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `uint16`, `int16`, `int32`, `int64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Mul", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return mul_eager_fallback(
            x, y, name=name, ctx=_ctx)
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
        "Mul", x=x, y=y, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Mul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mul_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mul
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Mul", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Mul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.negative', 'negative')
def neg(x, name=None):
  r"""Computes numerical negative value element-wise.

  I.e., \\(y = -x\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Neg", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return neg_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              neg, x=x, name=name)
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
        "Neg", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          neg, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Neg", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def neg_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function neg
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Neg", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Neg", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.not_equal', 'not_equal')
def not_equal(x, y, name=None):
  r"""Returns the truth value of (x != y) element-wise.

  *NOTE*: `math.not_equal` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `int16`, `int32`, `int64`, `complex64`, `quint8`, `qint8`, `qint32`, `string`, `bool`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "NotEqual",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return not_equal_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              not_equal, x=x, y=y, name=name)
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
        "NotEqual", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          not_equal, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "NotEqual", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def not_equal_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function not_equal
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"NotEqual", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NotEqual", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.polygamma', v1=['math.polygamma', 'polygamma'])
@deprecated_endpoints('polygamma')
def polygamma(a, x, name=None):
  r"""Compute the polygamma function \\(\psi^{(n)}(x)\\).

  The polygamma function is defined as:


  \\(\psi^{(n)}(x) = \frac{d^n}{dx^n} \psi(x)\\)

  where \\(\psi(x)\\) is the digamma function.

  Args:
    a: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    x: A `Tensor`. Must have the same type as `a`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `a`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Polygamma",
        name, _ctx._post_execution_callbacks, a, x)
      return _result
    except _core._FallbackException:
      try:
        return polygamma_eager_fallback(
            a, x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              polygamma, a=a, x=x, name=name)
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
        "Polygamma", a=a, x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          polygamma, a=a, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Polygamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def polygamma_eager_fallback(a, x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function polygamma
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([a, x], _ctx)
  (a, x) = _inputs_T
  _inputs_flat = [a, x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Polygamma", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Polygamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _pow(x, y, name=None):
  r"""Computes the power of one value to another.

  Given a tensor `x` and a tensor `y`, this operation computes \\(x^y\\) for
  corresponding elements in `x` and `y`. For example:

  ```
  # tensor 'x' is [[2, 2]], [3, 3]]
  # tensor 'y' is [[8, 16], [2, 3]]
  tf.pow(x, y) ==> [[256, 65536], [9, 27]]
  ```

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `float32`, `half`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Pow", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return _pow_eager_fallback(
            x, y, name=name, ctx=_ctx)
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
        "Pow", x=x, y=y, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Pow", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _pow_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _pow
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Pow", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Pow", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def prod(input, axis, keep_dims=False, name=None):
  r"""Computes the product of elements across dimensions of a tensor.

  Reduces `input` along the dimensions given in `axis`. Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `axis`. If `keep_dims` is true, the reduced dimensions are
  retained with length 1.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The tensor to reduce.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The dimensions to reduce. Must be in the range
      `[-rank(input), rank(input))`.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Prod", name,
        _ctx._post_execution_callbacks, input, axis, "keep_dims", keep_dims)
      return _result
    except _core._FallbackException:
      try:
        return prod_eager_fallback(
            input, axis, keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Prod", input=input, reduction_indices=axis, keep_dims=keep_dims,
                name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "T", _op.get_attr("T"),
            "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "Prod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def prod_eager_fallback(input, axis, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function prod
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _inputs_flat = [input, axis]
  _attrs = ("keep_dims", keep_dims, "T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"Prod", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Prod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_quantize_down_and_shrink_range_outputs = ["output", "output_min",
                                          "output_max"]
_QuantizeDownAndShrinkRangeOutput = _collections.namedtuple(
    "QuantizeDownAndShrinkRange", _quantize_down_and_shrink_range_outputs)


def quantize_down_and_shrink_range(input, input_min, input_max, out_type, name=None):
  r"""Convert the quantized 'input' tensor into a lower-precision 'output', using the

  actual distribution of the values to maximize the usage of the lower bit depth
  and adjusting the output min and max ranges accordingly.

  [input_min, input_max] are scalar floats that specify the range for the float
  interpretation of the 'input' data. For example, if input_min is -1.0f and
  input_max is 1.0f, and we are dealing with quint16 quantized data, then a 0
  value in the 16-bit data should be interpreted as -1.0f, and a 65535 means 1.0f.

  This operator tries to squeeze as much precision as possible into an output with
  a lower bit depth by calculating the actual min and max values found in the
  data. For example, maybe that quint16 input has no values lower than 16,384 and
  none higher than 49,152. That means only half the range is actually needed, all
  the float interpretations are between -0.5f and 0.5f, so if we want to compress
  the data into a quint8 output, we can use that range rather than the theoretical
  -1.0f to 1.0f that is suggested by the input min and max.

  In practice, this is most useful for taking output from operations like
  QuantizedMatMul that can produce higher bit-depth outputs than their inputs and
  may have large potential output ranges, but in practice have a distribution of
  input values that only uses a small fraction of the possible range. By feeding
  that output into this operator, we can reduce it from 32 bits down to 8 with
  minimal loss of accuracy.

  Args:
    input: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    input_min: A `Tensor` of type `float32`.
      The float value that the minimum quantized input value represents.
    input_max: A `Tensor` of type `float32`.
      The float value that the maximum quantized input value represents.
    out_type: A `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`.
      The type of the output. Should be a lower bit depth than Tinput.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, output_min, output_max).

    output: A `Tensor` of type `out_type`.
    output_min: A `Tensor` of type `float32`.
    output_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizeDownAndShrinkRange", name, _ctx._post_execution_callbacks,
        input, input_min, input_max, "out_type", out_type)
      _result = _QuantizeDownAndShrinkRangeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantize_down_and_shrink_range_eager_fallback(
            input, input_min, input_max, out_type=out_type, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizeDownAndShrinkRange", input=input, input_min=input_min,
                                      input_max=input_max, out_type=out_type,
                                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tinput", _op.get_attr("Tinput"), "out_type",
            _op.get_attr("out_type"))
  _execute.record_gradient(
      "QuantizeDownAndShrinkRange", _inputs_flat, _attrs, _result, name)
  _result = _QuantizeDownAndShrinkRangeOutput._make(_result)
  return _result



def quantize_down_and_shrink_range_eager_fallback(input, input_min, input_max, out_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantize_down_and_shrink_range
  """
  _ctx = ctx if ctx else _context.context()
  out_type = _execute.make_type(out_type, "out_type")
  _attr_Tinput, (input,) = _execute.args_to_matching_eager([input], _ctx)
  input_min = _ops.convert_to_tensor(input_min, _dtypes.float32)
  input_max = _ops.convert_to_tensor(input_max, _dtypes.float32)
  _inputs_flat = [input, input_min, input_max]
  _attrs = ("Tinput", _attr_Tinput, "out_type", out_type)
  _result = _execute.execute(b"QuantizeDownAndShrinkRange", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QuantizeDownAndShrinkRange", _inputs_flat, _attrs, _result, name)
  _result = _QuantizeDownAndShrinkRangeOutput._make(_result)
  return _result


_quantized_add_outputs = ["z", "min_z", "max_z"]
_QuantizedAddOutput = _collections.namedtuple(
    "QuantizedAdd", _quantized_add_outputs)


def quantized_add(x, y, min_x, max_x, min_y, max_y, Toutput=_dtypes.qint32, name=None):
  r"""Returns x + y element-wise, working on quantized buffers.

  Args:
    x: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    y: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    min_x: A `Tensor` of type `float32`.
      The float value that the lowest quantized `x` value represents.
    max_x: A `Tensor` of type `float32`.
      The float value that the highest quantized `x` value represents.
    min_y: A `Tensor` of type `float32`.
      The float value that the lowest quantized `y` value represents.
    max_y: A `Tensor` of type `float32`.
      The float value that the highest quantized `y` value represents.
    Toutput: An optional `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`. Defaults to `tf.qint32`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (z, min_z, max_z).

    z: A `Tensor` of type `Toutput`.
    min_z: A `Tensor` of type `float32`.
    max_z: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "QuantizedAdd",
        name, _ctx._post_execution_callbacks, x, y, min_x, max_x, min_y,
        max_y, "Toutput", Toutput)
      _result = _QuantizedAddOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_add_eager_fallback(
            x, y, min_x, max_x, min_y, max_y, Toutput=Toutput, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if Toutput is None:
    Toutput = _dtypes.qint32
  Toutput = _execute.make_type(Toutput, "Toutput")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedAdd", x=x, y=y, min_x=min_x, max_x=max_x, min_y=min_y,
                        max_y=max_y, Toutput=Toutput, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T1", _op.get_attr("T1"), "T2", _op.get_attr("T2"), "Toutput",
            _op.get_attr("Toutput"))
  _execute.record_gradient(
      "QuantizedAdd", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedAddOutput._make(_result)
  return _result



def quantized_add_eager_fallback(x, y, min_x, max_x, min_y, max_y, Toutput=_dtypes.qint32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_add
  """
  _ctx = ctx if ctx else _context.context()
  if Toutput is None:
    Toutput = _dtypes.qint32
  Toutput = _execute.make_type(Toutput, "Toutput")
  _attr_T1, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _attr_T2, (y,) = _execute.args_to_matching_eager([y], _ctx)
  min_x = _ops.convert_to_tensor(min_x, _dtypes.float32)
  max_x = _ops.convert_to_tensor(max_x, _dtypes.float32)
  min_y = _ops.convert_to_tensor(min_y, _dtypes.float32)
  max_y = _ops.convert_to_tensor(max_y, _dtypes.float32)
  _inputs_flat = [x, y, min_x, max_x, min_y, max_y]
  _attrs = ("T1", _attr_T1, "T2", _attr_T2, "Toutput", Toutput)
  _result = _execute.execute(b"QuantizedAdd", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedAdd", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedAddOutput._make(_result)
  return _result


_quantized_mat_mul_outputs = ["out", "min_out", "max_out"]
_QuantizedMatMulOutput = _collections.namedtuple(
    "QuantizedMatMul", _quantized_mat_mul_outputs)


def quantized_mat_mul(a, b, min_a, max_a, min_b, max_b, Toutput=_dtypes.qint32, transpose_a=False, transpose_b=False, Tactivation=_dtypes.quint8, name=None):
  r"""Perform a quantized matrix multiplication of  `a` by the matrix `b`.

  The inputs must be two-dimensional matrices and the inner dimension of
  `a` (after being transposed if `transpose_a` is non-zero) must match the
  outer dimension of `b` (after being transposed if `transposed_b` is
  non-zero).

  Args:
    a: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
      Must be a two-dimensional tensor.
    b: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
      Must be a two-dimensional tensor.
    min_a: A `Tensor` of type `float32`.
      The float value that the lowest quantized `a` value represents.
    max_a: A `Tensor` of type `float32`.
      The float value that the highest quantized `a` value represents.
    min_b: A `Tensor` of type `float32`.
      The float value that the lowest quantized `b` value represents.
    max_b: A `Tensor` of type `float32`.
      The float value that the highest quantized `b` value represents.
    Toutput: An optional `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`. Defaults to `tf.qint32`.
    transpose_a: An optional `bool`. Defaults to `False`.
      If true, `a` is transposed before multiplication.
    transpose_b: An optional `bool`. Defaults to `False`.
      If true, `b` is transposed before multiplication.
    Tactivation: An optional `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`. Defaults to `tf.quint8`.
      The type of output produced by activation function
      following this operation.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (out, min_out, max_out).

    out: A `Tensor` of type `Toutput`.
    min_out: A `Tensor` of type `float32`.
    max_out: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedMatMul", name, _ctx._post_execution_callbacks, a, b, min_a,
        max_a, min_b, max_b, "Toutput", Toutput, "transpose_a", transpose_a,
        "transpose_b", transpose_b, "Tactivation", Tactivation)
      _result = _QuantizedMatMulOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_mat_mul_eager_fallback(
            a, b, min_a, max_a, min_b, max_b, Toutput=Toutput,
            transpose_a=transpose_a, transpose_b=transpose_b,
            Tactivation=Tactivation, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if Toutput is None:
    Toutput = _dtypes.qint32
  Toutput = _execute.make_type(Toutput, "Toutput")
  if transpose_a is None:
    transpose_a = False
  transpose_a = _execute.make_bool(transpose_a, "transpose_a")
  if transpose_b is None:
    transpose_b = False
  transpose_b = _execute.make_bool(transpose_b, "transpose_b")
  if Tactivation is None:
    Tactivation = _dtypes.quint8
  Tactivation = _execute.make_type(Tactivation, "Tactivation")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedMatMul", a=a, b=b, min_a=min_a, max_a=max_a, min_b=min_b,
                           max_b=max_b, Toutput=Toutput,
                           transpose_a=transpose_a, transpose_b=transpose_b,
                           Tactivation=Tactivation, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T1", _op.get_attr("T1"), "T2", _op.get_attr("T2"), "Toutput",
            _op.get_attr("Toutput"), "transpose_a",
            _op.get_attr("transpose_a"), "transpose_b",
            _op.get_attr("transpose_b"), "Tactivation",
            _op.get_attr("Tactivation"))
  _execute.record_gradient(
      "QuantizedMatMul", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedMatMulOutput._make(_result)
  return _result



def quantized_mat_mul_eager_fallback(a, b, min_a, max_a, min_b, max_b, Toutput=_dtypes.qint32, transpose_a=False, transpose_b=False, Tactivation=_dtypes.quint8, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_mat_mul
  """
  _ctx = ctx if ctx else _context.context()
  if Toutput is None:
    Toutput = _dtypes.qint32
  Toutput = _execute.make_type(Toutput, "Toutput")
  if transpose_a is None:
    transpose_a = False
  transpose_a = _execute.make_bool(transpose_a, "transpose_a")
  if transpose_b is None:
    transpose_b = False
  transpose_b = _execute.make_bool(transpose_b, "transpose_b")
  if Tactivation is None:
    Tactivation = _dtypes.quint8
  Tactivation = _execute.make_type(Tactivation, "Tactivation")
  _attr_T1, (a,) = _execute.args_to_matching_eager([a], _ctx)
  _attr_T2, (b,) = _execute.args_to_matching_eager([b], _ctx)
  min_a = _ops.convert_to_tensor(min_a, _dtypes.float32)
  max_a = _ops.convert_to_tensor(max_a, _dtypes.float32)
  min_b = _ops.convert_to_tensor(min_b, _dtypes.float32)
  max_b = _ops.convert_to_tensor(max_b, _dtypes.float32)
  _inputs_flat = [a, b, min_a, max_a, min_b, max_b]
  _attrs = ("T1", _attr_T1, "T2", _attr_T2, "Toutput", Toutput, "transpose_a",
  transpose_a, "transpose_b", transpose_b, "Tactivation", Tactivation)
  _result = _execute.execute(b"QuantizedMatMul", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedMatMul", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedMatMulOutput._make(_result)
  return _result


_quantized_mul_outputs = ["z", "min_z", "max_z"]
_QuantizedMulOutput = _collections.namedtuple(
    "QuantizedMul", _quantized_mul_outputs)


def quantized_mul(x, y, min_x, max_x, min_y, max_y, Toutput=_dtypes.qint32, name=None):
  r"""Returns x * y element-wise, working on quantized buffers.

  Args:
    x: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    y: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    min_x: A `Tensor` of type `float32`.
      The float value that the lowest quantized `x` value represents.
    max_x: A `Tensor` of type `float32`.
      The float value that the highest quantized `x` value represents.
    min_y: A `Tensor` of type `float32`.
      The float value that the lowest quantized `y` value represents.
    max_y: A `Tensor` of type `float32`.
      The float value that the highest quantized `y` value represents.
    Toutput: An optional `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`. Defaults to `tf.qint32`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (z, min_z, max_z).

    z: A `Tensor` of type `Toutput`.
    min_z: A `Tensor` of type `float32`.
    max_z: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "QuantizedMul",
        name, _ctx._post_execution_callbacks, x, y, min_x, max_x, min_y,
        max_y, "Toutput", Toutput)
      _result = _QuantizedMulOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_mul_eager_fallback(
            x, y, min_x, max_x, min_y, max_y, Toutput=Toutput, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if Toutput is None:
    Toutput = _dtypes.qint32
  Toutput = _execute.make_type(Toutput, "Toutput")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedMul", x=x, y=y, min_x=min_x, max_x=max_x, min_y=min_y,
                        max_y=max_y, Toutput=Toutput, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T1", _op.get_attr("T1"), "T2", _op.get_attr("T2"), "Toutput",
            _op.get_attr("Toutput"))
  _execute.record_gradient(
      "QuantizedMul", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedMulOutput._make(_result)
  return _result



def quantized_mul_eager_fallback(x, y, min_x, max_x, min_y, max_y, Toutput=_dtypes.qint32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_mul
  """
  _ctx = ctx if ctx else _context.context()
  if Toutput is None:
    Toutput = _dtypes.qint32
  Toutput = _execute.make_type(Toutput, "Toutput")
  _attr_T1, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _attr_T2, (y,) = _execute.args_to_matching_eager([y], _ctx)
  min_x = _ops.convert_to_tensor(min_x, _dtypes.float32)
  max_x = _ops.convert_to_tensor(max_x, _dtypes.float32)
  min_y = _ops.convert_to_tensor(min_y, _dtypes.float32)
  max_y = _ops.convert_to_tensor(max_y, _dtypes.float32)
  _inputs_flat = [x, y, min_x, max_x, min_y, max_y]
  _attrs = ("T1", _attr_T1, "T2", _attr_T2, "Toutput", Toutput)
  _result = _execute.execute(b"QuantizedMul", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedMul", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedMulOutput._make(_result)
  return _result


def _range(start, limit, delta, name=None):
  r"""Creates a sequence of numbers.

  This operation creates a sequence of numbers that begins at `start` and
  extends by increments of `delta` up to but not including `limit`.

  For example:

  ```
  # 'start' is 3
  # 'limit' is 18
  # 'delta' is 3
  tf.range(start, limit, delta) ==> [3, 6, 9, 12, 15]
  ```

  Args:
    start: A `Tensor`. Must be one of the following types: `bfloat16`, `float32`, `float64`, `int32`, `int64`.
      0-D (scalar). First entry in the sequence.
    limit: A `Tensor`. Must have the same type as `start`.
      0-D (scalar). Upper limit of sequence, exclusive.
    delta: A `Tensor`. Must have the same type as `start`.
      0-D (scalar). Optional. Default is 1. Number that increments `start`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `start`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Range", name,
        _ctx._post_execution_callbacks, start, limit, delta)
      return _result
    except _core._FallbackException:
      try:
        return _range_eager_fallback(
            start, limit, delta, name=name, ctx=_ctx)
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
        "Range", start=start, limit=limit, delta=delta, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "Range", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _range_eager_fallback(start, limit, delta, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _range
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tidx, _inputs_Tidx = _execute.args_to_matching_eager([start, limit, delta], _ctx, _dtypes.int32)
  (start, limit, delta) = _inputs_Tidx
  _inputs_flat = [start, limit, delta]
  _attrs = ("Tidx", _attr_Tidx)
  _result = _execute.execute(b"Range", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Range", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def real(input, Tout=_dtypes.float32, name=None):
  r"""Returns the real part of a complex number.

  Given a tensor `input` of complex numbers, this operation returns a tensor of
  type `float` that is the real part of each element in `input`. All elements in
  `input` must be complex numbers of the form \\(a + bj\\), where *a* is the real
   part returned by this operation and *b* is the imaginary part.

  For example:

  ```
  # tensor 'input' is [-2.25 + 4.75j, 3.25 + 5.75j]
  tf.real(input) ==> [-2.25, 3.25]
  ```

  Args:
    input: A `Tensor`. Must be one of the following types: `complex64`, `complex128`.
    Tout: An optional `tf.DType` from: `tf.float32, tf.float64`. Defaults to `tf.float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Real", name,
        _ctx._post_execution_callbacks, input, "Tout", Tout)
      return _result
    except _core._FallbackException:
      try:
        return real_eager_fallback(
            input, Tout=Tout, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if Tout is None:
    Tout = _dtypes.float32
  Tout = _execute.make_type(Tout, "Tout")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Real", input=input, Tout=Tout, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "Real", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def real_eager_fallback(input, Tout=_dtypes.float32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function real
  """
  _ctx = ctx if ctx else _context.context()
  if Tout is None:
    Tout = _dtypes.float32
  Tout = _execute.make_type(Tout, "Tout")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.complex64)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "Tout", Tout)
  _result = _execute.execute(b"Real", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Real", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('realdiv')
def real_div(x, y, name=None):
  r"""Returns x / y element-wise for real types.

  If `x` and `y` are reals, this will return the floating-point division.

  *NOTE*: `Div` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `uint16`, `int16`, `int32`, `int64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RealDiv",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return real_div_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              real_div, x=x, y=y, name=name)
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
        "RealDiv", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          real_div, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "RealDiv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def real_div_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function real_div
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"RealDiv", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "RealDiv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.reciprocal', v1=['math.reciprocal', 'reciprocal'])
@deprecated_endpoints('reciprocal')
def reciprocal(x, name=None):
  r"""Computes the reciprocal of x element-wise.

  I.e., \\(y = 1 / x\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Reciprocal",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return reciprocal_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              reciprocal, x=x, name=name)
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
        "Reciprocal", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          reciprocal, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Reciprocal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reciprocal_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reciprocal
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Reciprocal", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Reciprocal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def reciprocal_grad(y, dy, name=None):
  r"""Computes the gradient for the inverse of `x` wrt its input.

  Specifically, `grad = -dy * y*y`, where `y = 1/x`, and `dy`
  is the corresponding input gradient.

  Args:
    y: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    dy: A `Tensor`. Must have the same type as `y`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `y`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReciprocalGrad", name, _ctx._post_execution_callbacks, y, dy)
      return _result
    except _core._FallbackException:
      try:
        return reciprocal_grad_eager_fallback(
            y, dy, name=name, ctx=_ctx)
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
        "ReciprocalGrad", y=y, dy=dy, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "ReciprocalGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reciprocal_grad_eager_fallback(y, dy, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reciprocal_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([y, dy], _ctx)
  (y, dy) = _inputs_T
  _inputs_flat = [y, dy]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"ReciprocalGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ReciprocalGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_requantization_range_outputs = ["output_min", "output_max"]
_RequantizationRangeOutput = _collections.namedtuple(
    "RequantizationRange", _requantization_range_outputs)


def requantization_range(input, input_min, input_max, name=None):
  r"""Given a quantized tensor described by (input, input_min, input_max), outputs a

  range that covers the actual values present in that tensor.  This op is
  typically used to produce the requested_output_min and requested_output_max for
  Requantize.

  Args:
    input: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    input_min: A `Tensor` of type `float32`.
      The float value that the minimum quantized input value represents.
    input_max: A `Tensor` of type `float32`.
      The float value that the maximum quantized input value represents.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_min, output_max).

    output_min: A `Tensor` of type `float32`.
    output_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RequantizationRange", name, _ctx._post_execution_callbacks, input,
        input_min, input_max)
      _result = _RequantizationRangeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return requantization_range_eager_fallback(
            input, input_min, input_max, name=name, ctx=_ctx)
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
        "RequantizationRange", input=input, input_min=input_min,
                               input_max=input_max, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tinput", _op.get_attr("Tinput"))
  _execute.record_gradient(
      "RequantizationRange", _inputs_flat, _attrs, _result, name)
  _result = _RequantizationRangeOutput._make(_result)
  return _result



def requantization_range_eager_fallback(input, input_min, input_max, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function requantization_range
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tinput, (input,) = _execute.args_to_matching_eager([input], _ctx)
  input_min = _ops.convert_to_tensor(input_min, _dtypes.float32)
  input_max = _ops.convert_to_tensor(input_max, _dtypes.float32)
  _inputs_flat = [input, input_min, input_max]
  _attrs = ("Tinput", _attr_Tinput)
  _result = _execute.execute(b"RequantizationRange", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RequantizationRange", _inputs_flat, _attrs, _result, name)
  _result = _RequantizationRangeOutput._make(_result)
  return _result


_requantize_outputs = ["output", "output_min", "output_max"]
_RequantizeOutput = _collections.namedtuple(
    "Requantize", _requantize_outputs)


def requantize(input, input_min, input_max, requested_output_min, requested_output_max, out_type, name=None):
  r"""Convert the quantized 'input' tensor into a lower-precision 'output', using the

  output range specified with 'requested_output_min' and 'requested_output_max'.

  [input_min, input_max] are scalar floats that specify the range for the float
  interpretation of the 'input' data. For example, if input_min is -1.0f and
  input_max is 1.0f, and we are dealing with quint16 quantized data, then a 0
  value in the 16-bit data should be interpreted as -1.0f, and a 65535 means 1.0f.

  Args:
    input: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    input_min: A `Tensor` of type `float32`.
      The float value that the minimum quantized input value represents.
    input_max: A `Tensor` of type `float32`.
      The float value that the maximum quantized input value represents.
    requested_output_min: A `Tensor` of type `float32`.
      The float value that the minimum quantized output value represents.
    requested_output_max: A `Tensor` of type `float32`.
      The float value that the maximum quantized output value represents.
    out_type: A `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`.
      The type of the output. Should be a lower bit depth than Tinput.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, output_min, output_max).

    output: A `Tensor` of type `out_type`.
    output_min: A `Tensor` of type `float32`.
    output_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Requantize",
        name, _ctx._post_execution_callbacks, input, input_min, input_max,
        requested_output_min, requested_output_max, "out_type", out_type)
      _result = _RequantizeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return requantize_eager_fallback(
            input, input_min, input_max, requested_output_min,
            requested_output_max, out_type=out_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Requantize", input=input, input_min=input_min, input_max=input_max,
                      requested_output_min=requested_output_min,
                      requested_output_max=requested_output_max,
                      out_type=out_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tinput", _op.get_attr("Tinput"), "out_type",
            _op.get_attr("out_type"))
  _execute.record_gradient(
      "Requantize", _inputs_flat, _attrs, _result, name)
  _result = _RequantizeOutput._make(_result)
  return _result



def requantize_eager_fallback(input, input_min, input_max, requested_output_min, requested_output_max, out_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function requantize
  """
  _ctx = ctx if ctx else _context.context()
  out_type = _execute.make_type(out_type, "out_type")
  _attr_Tinput, (input,) = _execute.args_to_matching_eager([input], _ctx)
  input_min = _ops.convert_to_tensor(input_min, _dtypes.float32)
  input_max = _ops.convert_to_tensor(input_max, _dtypes.float32)
  requested_output_min = _ops.convert_to_tensor(requested_output_min, _dtypes.float32)
  requested_output_max = _ops.convert_to_tensor(requested_output_max, _dtypes.float32)
  _inputs_flat = [input, input_min, input_max, requested_output_min, requested_output_max]
  _attrs = ("Tinput", _attr_Tinput, "out_type", out_type)
  _result = _execute.execute(b"Requantize", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Requantize", _inputs_flat, _attrs, _result, name)
  _result = _RequantizeOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.rint', v1=['math.rint', 'rint'])
@deprecated_endpoints('rint')
def rint(x, name=None):
  r"""Returns element-wise integer closest to x.

  If the result is midway between two representable values,
  the even representable is chosen.
  For example:

  ```
  rint(-1.5) ==> -2.0
  rint(0.5000001) ==> 1.0
  rint([-1.7, -1.5, -0.2, 0.2, 1.5, 1.7, 2.0]) ==> [-2., -2., -0., 0., 2., 2., 2.]
  ```

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Rint", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return rint_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              rint, x=x, name=name)
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
        "Rint", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          rint, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Rint", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def rint_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function rint
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Rint", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Rint", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def round(x, name=None):
  r"""Rounds the values of a tensor to the nearest integer, element-wise.

  Rounds half to even.  Also known as bankers rounding. If you want to round
  according to the current system rounding mode use std::cint.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Round", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return round_eager_fallback(
            x, name=name, ctx=_ctx)
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
        "Round", x=x, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Round", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def round_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function round
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Round", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Round", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.rsqrt', v1=['math.rsqrt', 'rsqrt'])
@deprecated_endpoints('rsqrt')
def rsqrt(x, name=None):
  r"""Computes reciprocal of square root of x element-wise.

  I.e., \\(y = 1 / \sqrt{x}\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Rsqrt", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return rsqrt_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              rsqrt, x=x, name=name)
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
        "Rsqrt", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          rsqrt, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Rsqrt", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def rsqrt_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function rsqrt
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Rsqrt", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Rsqrt", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def rsqrt_grad(y, dy, name=None):
  r"""Computes the gradient for the rsqrt of `x` wrt its input.

  Specifically, `grad = dy * -0.5 * y^3`, where `y = rsqrt(x)`, and `dy`
  is the corresponding input gradient.

  Args:
    y: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    dy: A `Tensor`. Must have the same type as `y`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `y`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RsqrtGrad",
        name, _ctx._post_execution_callbacks, y, dy)
      return _result
    except _core._FallbackException:
      try:
        return rsqrt_grad_eager_fallback(
            y, dy, name=name, ctx=_ctx)
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
        "RsqrtGrad", y=y, dy=dy, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "RsqrtGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def rsqrt_grad_eager_fallback(y, dy, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function rsqrt_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([y, dy], _ctx)
  (y, dy) = _inputs_T
  _inputs_flat = [y, dy]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"RsqrtGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RsqrtGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.segment_max', v1=['math.segment_max', 'segment_max'])
@deprecated_endpoints('segment_max')
def segment_max(data, segment_ids, name=None):
  r"""Computes the maximum along segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Computes a tensor such that
  \\(output_i = \max_j(data_j)\\) where `max` is over `j` such
  that `segment_ids[j] == i`.

  If the max is empty for a given segment ID `i`, `output[i] = 0`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/SegmentMax.png" alt>
  </div>

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    segment_ids: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor whose size is equal to the size of `data`'s
      first dimension.  Values should be sorted and can be repeated.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SegmentMax",
        name, _ctx._post_execution_callbacks, data, segment_ids)
      return _result
    except _core._FallbackException:
      try:
        return segment_max_eager_fallback(
            data, segment_ids, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              segment_max, data=data, segment_ids=segment_ids, name=name)
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
        "SegmentMax", data=data, segment_ids=segment_ids, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          segment_max, data=data, segment_ids=segment_ids, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "SegmentMax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def segment_max_eager_fallback(data, segment_ids, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function segment_max
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tindices, (segment_ids,) = _execute.args_to_matching_eager([segment_ids], _ctx)
  _inputs_flat = [data, segment_ids]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"SegmentMax", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SegmentMax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.segment_mean', v1=['math.segment_mean', 'segment_mean'])
@deprecated_endpoints('segment_mean')
def segment_mean(data, segment_ids, name=None):
  r"""Computes the mean along segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Computes a tensor such that
  \\(output_i = \frac{\sum_j data_j}{N}\\) where `mean` is
  over `j` such that `segment_ids[j] == i` and `N` is the total number of
  values summed.

  If the mean is empty for a given segment ID `i`, `output[i] = 0`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/SegmentMean.png" alt>
  </div>

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    segment_ids: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor whose size is equal to the size of `data`'s
      first dimension.  Values should be sorted and can be repeated.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SegmentMean",
        name, _ctx._post_execution_callbacks, data, segment_ids)
      return _result
    except _core._FallbackException:
      try:
        return segment_mean_eager_fallback(
            data, segment_ids, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              segment_mean, data=data, segment_ids=segment_ids, name=name)
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
        "SegmentMean", data=data, segment_ids=segment_ids, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          segment_mean, data=data, segment_ids=segment_ids, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "SegmentMean", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def segment_mean_eager_fallback(data, segment_ids, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function segment_mean
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tindices, (segment_ids,) = _execute.args_to_matching_eager([segment_ids], _ctx)
  _inputs_flat = [data, segment_ids]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"SegmentMean", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SegmentMean", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.segment_min', v1=['math.segment_min', 'segment_min'])
@deprecated_endpoints('segment_min')
def segment_min(data, segment_ids, name=None):
  r"""Computes the minimum along segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Computes a tensor such that
  \\(output_i = \min_j(data_j)\\) where `min` is over `j` such
  that `segment_ids[j] == i`.

  If the min is empty for a given segment ID `i`, `output[i] = 0`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/SegmentMin.png" alt>
  </div>

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    segment_ids: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor whose size is equal to the size of `data`'s
      first dimension.  Values should be sorted and can be repeated.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SegmentMin",
        name, _ctx._post_execution_callbacks, data, segment_ids)
      return _result
    except _core._FallbackException:
      try:
        return segment_min_eager_fallback(
            data, segment_ids, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              segment_min, data=data, segment_ids=segment_ids, name=name)
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
        "SegmentMin", data=data, segment_ids=segment_ids, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          segment_min, data=data, segment_ids=segment_ids, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "SegmentMin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def segment_min_eager_fallback(data, segment_ids, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function segment_min
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tindices, (segment_ids,) = _execute.args_to_matching_eager([segment_ids], _ctx)
  _inputs_flat = [data, segment_ids]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"SegmentMin", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SegmentMin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.segment_prod', v1=['math.segment_prod', 'segment_prod'])
@deprecated_endpoints('segment_prod')
def segment_prod(data, segment_ids, name=None):
  r"""Computes the product along segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Computes a tensor such that
  \\(output_i = \prod_j data_j\\) where the product is over `j` such
  that `segment_ids[j] == i`.

  If the product is empty for a given segment ID `i`, `output[i] = 1`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/SegmentProd.png" alt>
  </div>

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    segment_ids: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor whose size is equal to the size of `data`'s
      first dimension.  Values should be sorted and can be repeated.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SegmentProd",
        name, _ctx._post_execution_callbacks, data, segment_ids)
      return _result
    except _core._FallbackException:
      try:
        return segment_prod_eager_fallback(
            data, segment_ids, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              segment_prod, data=data, segment_ids=segment_ids, name=name)
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
        "SegmentProd", data=data, segment_ids=segment_ids, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          segment_prod, data=data, segment_ids=segment_ids, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "SegmentProd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def segment_prod_eager_fallback(data, segment_ids, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function segment_prod
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tindices, (segment_ids,) = _execute.args_to_matching_eager([segment_ids], _ctx)
  _inputs_flat = [data, segment_ids]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"SegmentProd", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SegmentProd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.segment_sum', v1=['math.segment_sum', 'segment_sum'])
@deprecated_endpoints('segment_sum')
def segment_sum(data, segment_ids, name=None):
  r"""Computes the sum along segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Computes a tensor such that
  \\(output_i = \sum_j data_j\\) where sum is over `j` such
  that `segment_ids[j] == i`.

  If the sum is empty for a given segment ID `i`, `output[i] = 0`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/SegmentSum.png" alt>
  </div>

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    segment_ids: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor whose size is equal to the size of `data`'s
      first dimension.  Values should be sorted and can be repeated.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SegmentSum",
        name, _ctx._post_execution_callbacks, data, segment_ids)
      return _result
    except _core._FallbackException:
      try:
        return segment_sum_eager_fallback(
            data, segment_ids, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              segment_sum, data=data, segment_ids=segment_ids, name=name)
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
        "SegmentSum", data=data, segment_ids=segment_ids, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          segment_sum, data=data, segment_ids=segment_ids, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "SegmentSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def segment_sum_eager_fallback(data, segment_ids, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function segment_sum
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tindices, (segment_ids,) = _execute.args_to_matching_eager([segment_ids], _ctx)
  _inputs_flat = [data, segment_ids]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"SegmentSum", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SegmentSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def select(condition, x, y, name=None):
  r"""Selects elements from `x` or `y`, depending on `condition`.

  The `x`, and `y` tensors must all have the same shape, and the
  output will also have that shape.

  The `condition` tensor must be a scalar if `x` and `y` are scalars.
  If `x` and `y` are vectors or higher rank, then `condition` must be either a
  scalar, a vector with size matching the first dimension of `x`, or must have
  the same shape as `x`.

  The `condition` tensor acts as a mask that chooses, based on the value at each
  element, whether the corresponding element / row in the output should be
  taken from `x` (if true) or `y` (if false).

  If `condition` is a vector and `x` and `y` are higher rank matrices, then
  it chooses which row (outer dimension) to copy from `x` and `y`.
  If `condition` has the same shape as `x` and `y`, then it chooses which
  element to copy from `x` and `y`.

  For example:

  ```python
  # 'condition' tensor is [[True,  False]
  #                        [False, True]]
  # 't' is [[1, 2],
  #         [3, 4]]
  # 'e' is [[5, 6],
  #         [7, 8]]
  select(condition, t, e)  # => [[1, 6], [7, 4]]


  # 'condition' tensor is [True, False]
  # 't' is [[1, 2],
  #         [3, 4]]
  # 'e' is [[5, 6],
  #         [7, 8]]
  select(condition, t, e) ==> [[1, 2],
                               [7, 8]]

  ```

  Args:
    condition: A `Tensor` of type `bool`.
    x:  A `Tensor` which may have the same shape as `condition`.
      If `condition` is rank 1, `x` may have higher rank,
      but its first dimension must match the size of `condition`.
    y:  A `Tensor` with the same type and shape as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `t`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Select", name,
        _ctx._post_execution_callbacks, condition, x, y)
      return _result
    except _core._FallbackException:
      try:
        return select_eager_fallback(
            condition, x, y, name=name, ctx=_ctx)
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
        "Select", condition=condition, t=x, e=y, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Select", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def select_eager_fallback(condition, x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function select
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  condition = _ops.convert_to_tensor(condition, _dtypes.bool)
  _inputs_flat = [condition, x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Select", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Select", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sigmoid(x, name=None):
  r"""Computes sigmoid of `x` element-wise.

  Specifically, `y = 1 / (1 + exp(-x))`.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Sigmoid",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return sigmoid_eager_fallback(
            x, name=name, ctx=_ctx)
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
        "Sigmoid", x=x, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Sigmoid", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sigmoid_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sigmoid
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Sigmoid", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Sigmoid", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sigmoid_grad(y, dy, name=None):
  r"""Computes the gradient of the sigmoid of `x` wrt its input.

  Specifically, `grad = dy * y * (1 - y)`, where `y = sigmoid(x)`, and
  `dy` is the corresponding input gradient.

  Args:
    y: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    dy: A `Tensor`. Must have the same type as `y`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `y`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SigmoidGrad",
        name, _ctx._post_execution_callbacks, y, dy)
      return _result
    except _core._FallbackException:
      try:
        return sigmoid_grad_eager_fallback(
            y, dy, name=name, ctx=_ctx)
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
        "SigmoidGrad", y=y, dy=dy, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SigmoidGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sigmoid_grad_eager_fallback(y, dy, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sigmoid_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([y, dy], _ctx)
  (y, dy) = _inputs_T
  _inputs_flat = [y, dy]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SigmoidGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SigmoidGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.sign', 'sign')
def sign(x, name=None):
  r"""Returns an element-wise indication of the sign of a number.

  `y = sign(x) = -1` if `x < 0`; 0 if `x == 0`; 1 if `x > 0`.

  For complex numbers, `y = sign(x) = x / |x|` if `x != 0`, otherwise `y = 0`.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Sign", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return sign_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              sign, x=x, name=name)
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
        "Sign", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sign, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Sign", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sign_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sign
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Sign", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Sign", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.sin', 'sin')
def sin(x, name=None):
  r"""Computes sin of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Sin", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return sin_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              sin, x=x, name=name)
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
        "Sin", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sin, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Sin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sin_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sin
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Sin", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Sin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.sinh', 'sinh')
def sinh(x, name=None):
  r"""Computes hyperbolic sine of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Sinh", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return sinh_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              sinh, x=x, name=name)
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
        "Sinh", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sinh, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Sinh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sinh_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sinh
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Sinh", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Sinh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_mat_mul(a, b, transpose_a=False, transpose_b=False, a_is_sparse=False, b_is_sparse=False, name=None):
  r"""Multiply matrix "a" by matrix "b".

  The inputs must be two-dimensional matrices and the inner dimension of "a" must
  match the outer dimension of "b". Both "a" and "b" must be `Tensor`s not
  `SparseTensor`s.  This op is optimized for the case where at least one of "a" or
  "b" is sparse, in the sense that they have a large proportion of zero values.
  The breakeven for using this versus a dense matrix multiply on one platform was
  30% zero values in the sparse matrix.

  The gradient computation of this operation will only take advantage of sparsity
  in the input gradient when that gradient comes from a Relu.

  Args:
    a: A `Tensor`. Must be one of the following types: `float32`, `bfloat16`.
    b: A `Tensor`. Must be one of the following types: `float32`, `bfloat16`.
    transpose_a: An optional `bool`. Defaults to `False`.
    transpose_b: An optional `bool`. Defaults to `False`.
    a_is_sparse: An optional `bool`. Defaults to `False`.
    b_is_sparse: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SparseMatMul",
        name, _ctx._post_execution_callbacks, a, b, "transpose_a",
        transpose_a, "transpose_b", transpose_b, "a_is_sparse", a_is_sparse,
        "b_is_sparse", b_is_sparse)
      return _result
    except _core._FallbackException:
      try:
        return sparse_mat_mul_eager_fallback(
            a, b, transpose_a=transpose_a, transpose_b=transpose_b,
            a_is_sparse=a_is_sparse, b_is_sparse=b_is_sparse, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if transpose_a is None:
    transpose_a = False
  transpose_a = _execute.make_bool(transpose_a, "transpose_a")
  if transpose_b is None:
    transpose_b = False
  transpose_b = _execute.make_bool(transpose_b, "transpose_b")
  if a_is_sparse is None:
    a_is_sparse = False
  a_is_sparse = _execute.make_bool(a_is_sparse, "a_is_sparse")
  if b_is_sparse is None:
    b_is_sparse = False
  b_is_sparse = _execute.make_bool(b_is_sparse, "b_is_sparse")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseMatMul", a=a, b=b, transpose_a=transpose_a,
                        transpose_b=transpose_b, a_is_sparse=a_is_sparse,
                        b_is_sparse=b_is_sparse, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("transpose_a", _op.get_attr("transpose_a"), "transpose_b",
            _op.get_attr("transpose_b"), "a_is_sparse",
            _op.get_attr("a_is_sparse"), "b_is_sparse",
            _op.get_attr("b_is_sparse"), "Ta", _op.get_attr("Ta"), "Tb",
            _op.get_attr("Tb"))
  _execute.record_gradient(
      "SparseMatMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_mat_mul_eager_fallback(a, b, transpose_a=False, transpose_b=False, a_is_sparse=False, b_is_sparse=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_mat_mul
  """
  _ctx = ctx if ctx else _context.context()
  if transpose_a is None:
    transpose_a = False
  transpose_a = _execute.make_bool(transpose_a, "transpose_a")
  if transpose_b is None:
    transpose_b = False
  transpose_b = _execute.make_bool(transpose_b, "transpose_b")
  if a_is_sparse is None:
    a_is_sparse = False
  a_is_sparse = _execute.make_bool(a_is_sparse, "a_is_sparse")
  if b_is_sparse is None:
    b_is_sparse = False
  b_is_sparse = _execute.make_bool(b_is_sparse, "b_is_sparse")
  _attr_Ta, (a,) = _execute.args_to_matching_eager([a], _ctx, _dtypes.float32)
  _attr_Tb, (b,) = _execute.args_to_matching_eager([b], _ctx, _dtypes.float32)
  _inputs_flat = [a, b]
  _attrs = ("transpose_a", transpose_a, "transpose_b", transpose_b,
  "a_is_sparse", a_is_sparse, "b_is_sparse", b_is_sparse, "Ta", _attr_Ta,
  "Tb", _attr_Tb)
  _result = _execute.execute(b"SparseMatMul", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseMatMul", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_segment_mean(data, indices, segment_ids, name=None):
  r"""Computes the mean along sparse segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Like `SegmentMean`, but `segment_ids` can have rank less than `data`'s first
  dimension, selecting a subset of dimension 0, specified by `indices`.

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor. Has same rank as `segment_ids`.
    segment_ids: A `Tensor` of type `int32`.
      A 1-D tensor. Values should be sorted and can be repeated.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSegmentMean", name, _ctx._post_execution_callbacks, data,
        indices, segment_ids)
      return _result
    except _core._FallbackException:
      try:
        return sparse_segment_mean_eager_fallback(
            data, indices, segment_ids, name=name, ctx=_ctx)
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
        "SparseSegmentMean", data=data, indices=indices,
                             segment_ids=segment_ids, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "SparseSegmentMean", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_segment_mean_eager_fallback(data, indices, segment_ids, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_segment_mean
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tidx, (indices,) = _execute.args_to_matching_eager([indices], _ctx, _dtypes.int32)
  segment_ids = _ops.convert_to_tensor(segment_ids, _dtypes.int32)
  _inputs_flat = [data, indices, segment_ids]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"SparseSegmentMean", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSegmentMean", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_segment_mean_grad(grad, indices, segment_ids, output_dim0, name=None):
  r"""Computes gradients for SparseSegmentMean.

  Returns tensor "output" with same shape as grad, except for dimension 0 whose
  value is output_dim0.

  Args:
    grad: A `Tensor`. Must be one of the following types: `float32`, `float64`.
      gradient propagated to the SparseSegmentMean op.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      indices passed to the corresponding SparseSegmentMean op.
    segment_ids: A `Tensor` of type `int32`.
      segment_ids passed to the corresponding SparseSegmentMean op.
    output_dim0: A `Tensor` of type `int32`.
      dimension 0 of "data" passed to SparseSegmentMean op.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `grad`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSegmentMeanGrad", name, _ctx._post_execution_callbacks, grad,
        indices, segment_ids, output_dim0)
      return _result
    except _core._FallbackException:
      try:
        return sparse_segment_mean_grad_eager_fallback(
            grad, indices, segment_ids, output_dim0, name=name, ctx=_ctx)
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
        "SparseSegmentMeanGrad", grad=grad, indices=indices,
                                 segment_ids=segment_ids,
                                 output_dim0=output_dim0, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "SparseSegmentMeanGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_segment_mean_grad_eager_fallback(grad, indices, segment_ids, output_dim0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_segment_mean_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (grad,) = _execute.args_to_matching_eager([grad], _ctx)
  _attr_Tidx, (indices,) = _execute.args_to_matching_eager([indices], _ctx, _dtypes.int32)
  segment_ids = _ops.convert_to_tensor(segment_ids, _dtypes.int32)
  output_dim0 = _ops.convert_to_tensor(output_dim0, _dtypes.int32)
  _inputs_flat = [grad, indices, segment_ids, output_dim0]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"SparseSegmentMeanGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSegmentMeanGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_segment_mean_with_num_segments(data, indices, segment_ids, num_segments, name=None):
  r"""Computes the mean along sparse segments of a tensor.

  Like `SparseSegmentMean`, but allows missing ids in `segment_ids`. If an id is
  misisng, the `output` tensor at that position will be zeroed.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor. Has same rank as `segment_ids`.
    segment_ids: A `Tensor` of type `int32`.
      A 1-D tensor. Values should be sorted and can be repeated.
    num_segments: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Should equal the number of distinct segment IDs.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSegmentMeanWithNumSegments", name,
        _ctx._post_execution_callbacks, data, indices, segment_ids,
        num_segments)
      return _result
    except _core._FallbackException:
      try:
        return sparse_segment_mean_with_num_segments_eager_fallback(
            data, indices, segment_ids, num_segments, name=name, ctx=_ctx)
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
        "SparseSegmentMeanWithNumSegments", data=data, indices=indices,
                                            segment_ids=segment_ids,
                                            num_segments=num_segments,
                                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"),
            "Tnumsegments", _op.get_attr("Tnumsegments"))
  _execute.record_gradient(
      "SparseSegmentMeanWithNumSegments", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_segment_mean_with_num_segments_eager_fallback(data, indices, segment_ids, num_segments, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_segment_mean_with_num_segments
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tidx, (indices,) = _execute.args_to_matching_eager([indices], _ctx, _dtypes.int32)
  _attr_Tnumsegments, (num_segments,) = _execute.args_to_matching_eager([num_segments], _ctx, _dtypes.int32)
  segment_ids = _ops.convert_to_tensor(segment_ids, _dtypes.int32)
  _inputs_flat = [data, indices, segment_ids, num_segments]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx, "Tnumsegments",
  _attr_Tnumsegments)
  _result = _execute.execute(b"SparseSegmentMeanWithNumSegments", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SparseSegmentMeanWithNumSegments", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_segment_sqrt_n(data, indices, segment_ids, name=None):
  r"""Computes the sum along sparse segments of a tensor divided by the sqrt of N.

  N is the size of the segment being reduced.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor. Has same rank as `segment_ids`.
    segment_ids: A `Tensor` of type `int32`.
      A 1-D tensor. Values should be sorted and can be repeated.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSegmentSqrtN", name, _ctx._post_execution_callbacks, data,
        indices, segment_ids)
      return _result
    except _core._FallbackException:
      try:
        return sparse_segment_sqrt_n_eager_fallback(
            data, indices, segment_ids, name=name, ctx=_ctx)
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
        "SparseSegmentSqrtN", data=data, indices=indices,
                              segment_ids=segment_ids, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "SparseSegmentSqrtN", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_segment_sqrt_n_eager_fallback(data, indices, segment_ids, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_segment_sqrt_n
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tidx, (indices,) = _execute.args_to_matching_eager([indices], _ctx, _dtypes.int32)
  segment_ids = _ops.convert_to_tensor(segment_ids, _dtypes.int32)
  _inputs_flat = [data, indices, segment_ids]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"SparseSegmentSqrtN", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSegmentSqrtN", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_segment_sqrt_n_grad(grad, indices, segment_ids, output_dim0, name=None):
  r"""Computes gradients for SparseSegmentSqrtN.

  Returns tensor "output" with same shape as grad, except for dimension 0 whose
  value is output_dim0.

  Args:
    grad: A `Tensor`. Must be one of the following types: `float32`, `float64`.
      gradient propagated to the SparseSegmentSqrtN op.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      indices passed to the corresponding SparseSegmentSqrtN op.
    segment_ids: A `Tensor` of type `int32`.
      segment_ids passed to the corresponding SparseSegmentSqrtN op.
    output_dim0: A `Tensor` of type `int32`.
      dimension 0 of "data" passed to SparseSegmentSqrtN op.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `grad`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSegmentSqrtNGrad", name, _ctx._post_execution_callbacks, grad,
        indices, segment_ids, output_dim0)
      return _result
    except _core._FallbackException:
      try:
        return sparse_segment_sqrt_n_grad_eager_fallback(
            grad, indices, segment_ids, output_dim0, name=name, ctx=_ctx)
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
        "SparseSegmentSqrtNGrad", grad=grad, indices=indices,
                                  segment_ids=segment_ids,
                                  output_dim0=output_dim0, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "SparseSegmentSqrtNGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_segment_sqrt_n_grad_eager_fallback(grad, indices, segment_ids, output_dim0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_segment_sqrt_n_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (grad,) = _execute.args_to_matching_eager([grad], _ctx)
  _attr_Tidx, (indices,) = _execute.args_to_matching_eager([indices], _ctx, _dtypes.int32)
  segment_ids = _ops.convert_to_tensor(segment_ids, _dtypes.int32)
  output_dim0 = _ops.convert_to_tensor(output_dim0, _dtypes.int32)
  _inputs_flat = [grad, indices, segment_ids, output_dim0]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"SparseSegmentSqrtNGrad", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SparseSegmentSqrtNGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_segment_sqrt_n_with_num_segments(data, indices, segment_ids, num_segments, name=None):
  r"""Computes the sum along sparse segments of a tensor divided by the sqrt of N.

  N is the size of the segment being reduced.

  Like `SparseSegmentSqrtN`, but allows missing ids in `segment_ids`. If an id is
  misisng, the `output` tensor at that position will be zeroed.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor. Has same rank as `segment_ids`.
    segment_ids: A `Tensor` of type `int32`.
      A 1-D tensor. Values should be sorted and can be repeated.
    num_segments: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Should equal the number of distinct segment IDs.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSegmentSqrtNWithNumSegments", name,
        _ctx._post_execution_callbacks, data, indices, segment_ids,
        num_segments)
      return _result
    except _core._FallbackException:
      try:
        return sparse_segment_sqrt_n_with_num_segments_eager_fallback(
            data, indices, segment_ids, num_segments, name=name, ctx=_ctx)
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
        "SparseSegmentSqrtNWithNumSegments", data=data, indices=indices,
                                             segment_ids=segment_ids,
                                             num_segments=num_segments,
                                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"),
            "Tnumsegments", _op.get_attr("Tnumsegments"))
  _execute.record_gradient(
      "SparseSegmentSqrtNWithNumSegments", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_segment_sqrt_n_with_num_segments_eager_fallback(data, indices, segment_ids, num_segments, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_segment_sqrt_n_with_num_segments
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tidx, (indices,) = _execute.args_to_matching_eager([indices], _ctx, _dtypes.int32)
  _attr_Tnumsegments, (num_segments,) = _execute.args_to_matching_eager([num_segments], _ctx, _dtypes.int32)
  segment_ids = _ops.convert_to_tensor(segment_ids, _dtypes.int32)
  _inputs_flat = [data, indices, segment_ids, num_segments]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx, "Tnumsegments",
  _attr_Tnumsegments)
  _result = _execute.execute(b"SparseSegmentSqrtNWithNumSegments", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SparseSegmentSqrtNWithNumSegments", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_segment_sum(data, indices, segment_ids, name=None):
  r"""Computes the sum along sparse segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Like `SegmentSum`, but `segment_ids` can have rank less than `data`'s first
  dimension, selecting a subset of dimension 0, specified by `indices`.

  For example:

  ```python
  c = tf.constant([[1,2,3,4], [-1,-2,-3,-4], [5,6,7,8]])

  # Select two rows, one segment.
  tf.sparse_segment_sum(c, tf.constant([0, 1]), tf.constant([0, 0]))
  # => [[0 0 0 0]]

  # Select two rows, two segment.
  tf.sparse_segment_sum(c, tf.constant([0, 1]), tf.constant([0, 1]))
  # => [[ 1  2  3  4]
  #     [-1 -2 -3 -4]]

  # Select all rows, two segments.
  tf.sparse_segment_sum(c, tf.constant([0, 1, 2]), tf.constant([0, 0, 1]))
  # => [[0 0 0 0]
  #     [5 6 7 8]]

  # Which is equivalent to:
  tf.segment_sum(c, tf.constant([0, 0, 1]))
  ```

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor. Has same rank as `segment_ids`.
    segment_ids: A `Tensor` of type `int32`.
      A 1-D tensor. Values should be sorted and can be repeated.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSegmentSum", name, _ctx._post_execution_callbacks, data,
        indices, segment_ids)
      return _result
    except _core._FallbackException:
      try:
        return sparse_segment_sum_eager_fallback(
            data, indices, segment_ids, name=name, ctx=_ctx)
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
        "SparseSegmentSum", data=data, indices=indices,
                            segment_ids=segment_ids, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "SparseSegmentSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_segment_sum_eager_fallback(data, indices, segment_ids, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_segment_sum
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tidx, (indices,) = _execute.args_to_matching_eager([indices], _ctx, _dtypes.int32)
  segment_ids = _ops.convert_to_tensor(segment_ids, _dtypes.int32)
  _inputs_flat = [data, indices, segment_ids]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"SparseSegmentSum", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseSegmentSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_segment_sum_with_num_segments(data, indices, segment_ids, num_segments, name=None):
  r"""Computes the sum along sparse segments of a tensor.

  Like `SparseSegmentSum`, but allows missing ids in `segment_ids`. If an id is
  misisng, the `output` tensor at that position will be zeroed.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  For example:

  ```python
  c = tf.constant([[1,2,3,4], [-1,-2,-3,-4], [5,6,7,8]])

  tf.sparse_segment_sum_with_num_segments(
      c, tf.constant([0, 1]), tf.constant([0, 0]), num_segments=3)
  # => [[0 0 0 0]
  #     [0 0 0 0]
  #     [0 0 0 0]]

  tf.sparse_segment_sum_with_num_segments(c,
                                          tf.constant([0, 1]),
                                          tf.constant([0, 2],
                                          num_segments=4))
  # => [[ 1  2  3  4]
  #     [ 0  0  0  0]
  #     [-1 -2 -3 -4]
  #     [ 0  0  0  0]]
  ```

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A 1-D tensor. Has same rank as `segment_ids`.
    segment_ids: A `Tensor` of type `int32`.
      A 1-D tensor. Values should be sorted and can be repeated.
    num_segments: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Should equal the number of distinct segment IDs.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSegmentSumWithNumSegments", name,
        _ctx._post_execution_callbacks, data, indices, segment_ids,
        num_segments)
      return _result
    except _core._FallbackException:
      try:
        return sparse_segment_sum_with_num_segments_eager_fallback(
            data, indices, segment_ids, num_segments, name=name, ctx=_ctx)
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
        "SparseSegmentSumWithNumSegments", data=data, indices=indices,
                                           segment_ids=segment_ids,
                                           num_segments=num_segments,
                                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"),
            "Tnumsegments", _op.get_attr("Tnumsegments"))
  _execute.record_gradient(
      "SparseSegmentSumWithNumSegments", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_segment_sum_with_num_segments_eager_fallback(data, indices, segment_ids, num_segments, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_segment_sum_with_num_segments
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tidx, (indices,) = _execute.args_to_matching_eager([indices], _ctx, _dtypes.int32)
  _attr_Tnumsegments, (num_segments,) = _execute.args_to_matching_eager([num_segments], _ctx, _dtypes.int32)
  segment_ids = _ops.convert_to_tensor(segment_ids, _dtypes.int32)
  _inputs_flat = [data, indices, segment_ids, num_segments]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx, "Tnumsegments",
  _attr_Tnumsegments)
  _result = _execute.execute(b"SparseSegmentSumWithNumSegments", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SparseSegmentSumWithNumSegments", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.sqrt', 'sqrt')
def sqrt(x, name=None):
  r"""Computes square root of x element-wise.

  I.e., \\(y = \sqrt{x} = x^{1/2}\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Sqrt", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return sqrt_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              sqrt, x=x, name=name)
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
        "Sqrt", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sqrt, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Sqrt", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sqrt_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sqrt
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Sqrt", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Sqrt", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sqrt_grad(y, dy, name=None):
  r"""Computes the gradient for the sqrt of `x` wrt its input.

  Specifically, `grad = dy * 0.5 / y`, where `y = sqrt(x)`, and `dy`
  is the corresponding input gradient.

  Args:
    y: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    dy: A `Tensor`. Must have the same type as `y`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `y`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SqrtGrad",
        name, _ctx._post_execution_callbacks, y, dy)
      return _result
    except _core._FallbackException:
      try:
        return sqrt_grad_eager_fallback(
            y, dy, name=name, ctx=_ctx)
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
        "SqrtGrad", y=y, dy=dy, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SqrtGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sqrt_grad_eager_fallback(y, dy, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sqrt_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([y, dy], _ctx)
  (y, dy) = _inputs_T
  _inputs_flat = [y, dy]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SqrtGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SqrtGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.square', 'square')
def square(x, name=None):
  r"""Computes square of x element-wise.

  I.e., \\(y = x * x = x^2\\).

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Square", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return square_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              square, x=x, name=name)
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
        "Square", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          square, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Square", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def square_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function square
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Square", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Square", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.squared_difference', v1=['math.squared_difference', 'squared_difference'])
@deprecated_endpoints('squared_difference')
def squared_difference(x, y, name=None):
  r"""Returns (x - y)(x - y) element-wise.

  *NOTE*: `math.squared_difference` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SquaredDifference", name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return squared_difference_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              squared_difference, x=x, y=y, name=name)
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
        "SquaredDifference", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          squared_difference, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SquaredDifference", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def squared_difference_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function squared_difference
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SquaredDifference", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SquaredDifference", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sub(x, y, name=None):
  r"""Returns x - y element-wise.

  *NOTE*: `Subtract` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `uint16`, `int16`, `int32`, `int64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Sub", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return sub_eager_fallback(
            x, y, name=name, ctx=_ctx)
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
        "Sub", x=x, y=y, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Sub", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sub_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sub
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Sub", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Sub", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _sum(input, axis, keep_dims=False, name=None):
  r"""Computes the sum of elements across dimensions of a tensor.

  Reduces `input` along the dimensions given in `axis`. Unless
  `keep_dims` is true, the rank of the tensor is reduced by 1 for each entry in
  `axis`. If `keep_dims` is true, the reduced dimensions are
  retained with length 1.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The tensor to reduce.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The dimensions to reduce. Must be in the range
      `[-rank(input), rank(input))`.
    keep_dims: An optional `bool`. Defaults to `False`.
      If true, retain reduced dimensions with length 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Sum", name,
        _ctx._post_execution_callbacks, input, axis, "keep_dims", keep_dims)
      return _result
    except _core._FallbackException:
      try:
        return _sum_eager_fallback(
            input, axis, keep_dims=keep_dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Sum", input=input, reduction_indices=axis, keep_dims=keep_dims,
               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "T", _op.get_attr("T"),
            "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "Sum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _sum_eager_fallback(input, axis, keep_dims=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _sum
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _inputs_flat = [input, axis]
  _attrs = ("keep_dims", keep_dims, "T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"Sum", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Sum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.tan', 'tan')
def tan(x, name=None):
  r"""Computes tan of x element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Tan", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return tan_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tan, x=x, name=name)
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
        "Tan", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tan, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Tan", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tan_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tan
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Tan", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Tan", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.tanh', 'nn.tanh', 'tanh')
def tanh(x, name=None):
  r"""Computes hyperbolic tangent of `x` element-wise.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Tanh", name,
        _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return tanh_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tanh, x=x, name=name)
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
        "Tanh", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tanh, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Tanh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tanh_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tanh
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Tanh", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Tanh", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tanh_grad(y, dy, name=None):
  r"""Computes the gradient for the tanh of `x` wrt its input.

  Specifically, `grad = dy * (1 - y*y)`, where `y = tanh(x)`, and `dy`
  is the corresponding input gradient.

  Args:
    y: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`.
    dy: A `Tensor`. Must have the same type as `y`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `y`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TanhGrad",
        name, _ctx._post_execution_callbacks, y, dy)
      return _result
    except _core._FallbackException:
      try:
        return tanh_grad_eager_fallback(
            y, dy, name=name, ctx=_ctx)
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
        "TanhGrad", y=y, dy=dy, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TanhGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tanh_grad_eager_fallback(y, dy, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tanh_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([y, dy], _ctx)
  (y, dy) = _inputs_T
  _inputs_flat = [y, dy]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TanhGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TanhGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('truncatediv')
def truncate_div(x, y, name=None):
  r"""Returns x / y element-wise for integer types.

  Truncation designates that negative numbers will round fractional quantities
  toward zero. I.e. -7 / 5 = -1. This matches C semantics but it is different
  than Python semantics. See `FloorDiv` for a division function that matches
  Python Semantics.

  *NOTE*: `truncatediv` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `uint8`, `int8`, `uint16`, `int16`, `int32`, `int64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TruncateDiv",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return truncate_div_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              truncate_div, x=x, y=y, name=name)
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
        "TruncateDiv", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          truncate_div, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TruncateDiv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def truncate_div_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function truncate_div
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TruncateDiv", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TruncateDiv", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('truncatemod')
def truncate_mod(x, y, name=None):
  r"""Returns element-wise remainder of division. This emulates C semantics in that

  the result here is consistent with a truncating divide. E.g. `truncate(x / y) *
  y + truncate_mod(x, y) = x`.

  *NOTE*: `truncatemod` supports broadcasting. More about broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  Args:
    x: A `Tensor`. Must be one of the following types: `int32`, `int64`, `bfloat16`, `half`, `float32`, `float64`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TruncateMod",
        name, _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return truncate_mod_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              truncate_mod, x=x, y=y, name=name)
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
        "TruncateMod", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          truncate_mod, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TruncateMod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def truncate_mod_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function truncate_mod
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TruncateMod", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TruncateMod", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.unsorted_segment_max', v1=['math.unsorted_segment_max', 'unsorted_segment_max'])
@deprecated_endpoints('unsorted_segment_max')
def unsorted_segment_max(data, segment_ids, num_segments, name=None):
  r"""Computes the maximum along segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  This operator is similar to the unsorted segment sum operator found
  [(here)](../../../api_docs/python/math_ops.md#UnsortedSegmentSum).
  Instead of computing the sum over segments, it computes the maximum such that:

  \\(output_i = \max_{j...} data[j...]\\) where max is over tuples `j...` such
  that `segment_ids[j...] == i`.

  If the maximum is empty for a given segment ID `i`, it outputs the smallest
  possible value for the specific numeric type,
  `output[i] = numeric_limits<T>::lowest()`.

  If the given segment ID `i` is negative, then the corresponding value is
  dropped, and will not be included in the result.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/UnsortedSegmentMax.png" alt>
  </div>

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    segment_ids: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor whose shape is a prefix of `data.shape`.END
        }
        out_arg {
          name: "output"
          description: <<END
      Has same shape as data, except for the first `segment_ids.rank`
      dimensions, which are replaced with a single dimension which has size
      `num_segments`.
    num_segments: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnsortedSegmentMax", name, _ctx._post_execution_callbacks, data,
        segment_ids, num_segments)
      return _result
    except _core._FallbackException:
      try:
        return unsorted_segment_max_eager_fallback(
            data, segment_ids, num_segments, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              unsorted_segment_max, data=data, segment_ids=segment_ids,
                                    num_segments=num_segments, name=name)
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
        "UnsortedSegmentMax", data=data, segment_ids=segment_ids,
                              num_segments=num_segments, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          unsorted_segment_max, data=data, segment_ids=segment_ids,
                                num_segments=num_segments, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "Tnumsegments", _op.get_attr("Tnumsegments"))
  _execute.record_gradient(
      "UnsortedSegmentMax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unsorted_segment_max_eager_fallback(data, segment_ids, num_segments, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unsorted_segment_max
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tindices, (segment_ids,) = _execute.args_to_matching_eager([segment_ids], _ctx)
  _attr_Tnumsegments, (num_segments,) = _execute.args_to_matching_eager([num_segments], _ctx, _dtypes.int32)
  _inputs_flat = [data, segment_ids, num_segments]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "Tnumsegments",
  _attr_Tnumsegments)
  _result = _execute.execute(b"UnsortedSegmentMax", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnsortedSegmentMax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.unsorted_segment_min', v1=['math.unsorted_segment_min', 'unsorted_segment_min'])
@deprecated_endpoints('unsorted_segment_min')
def unsorted_segment_min(data, segment_ids, num_segments, name=None):
  r"""Computes the minimum along segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#segmentation)
  for an explanation of segments.

  This operator is similar to the unsorted segment sum operator found
  [(here)](../../../api_docs/python/math_ops.md#UnsortedSegmentSum).
  Instead of computing the sum over segments, it computes the minimum such that:

  \\(output_i = \min_{j...} data_[j...]\\) where min is over tuples `j...` such
  that `segment_ids[j...] == i`.

  If the minimum is empty for a given segment ID `i`, it outputs the largest
  possible value for the specific numeric type,
  `output[i] = numeric_limits<T>::max()`.

  If the given segment ID `i` is negative, then the corresponding value is
  dropped, and will not be included in the result.

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    segment_ids: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor whose shape is a prefix of `data.shape`.
    num_segments: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnsortedSegmentMin", name, _ctx._post_execution_callbacks, data,
        segment_ids, num_segments)
      return _result
    except _core._FallbackException:
      try:
        return unsorted_segment_min_eager_fallback(
            data, segment_ids, num_segments, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              unsorted_segment_min, data=data, segment_ids=segment_ids,
                                    num_segments=num_segments, name=name)
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
        "UnsortedSegmentMin", data=data, segment_ids=segment_ids,
                              num_segments=num_segments, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          unsorted_segment_min, data=data, segment_ids=segment_ids,
                                num_segments=num_segments, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "Tnumsegments", _op.get_attr("Tnumsegments"))
  _execute.record_gradient(
      "UnsortedSegmentMin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unsorted_segment_min_eager_fallback(data, segment_ids, num_segments, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unsorted_segment_min
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tindices, (segment_ids,) = _execute.args_to_matching_eager([segment_ids], _ctx)
  _attr_Tnumsegments, (num_segments,) = _execute.args_to_matching_eager([num_segments], _ctx, _dtypes.int32)
  _inputs_flat = [data, segment_ids, num_segments]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "Tnumsegments",
  _attr_Tnumsegments)
  _result = _execute.execute(b"UnsortedSegmentMin", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnsortedSegmentMin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.unsorted_segment_prod', v1=['math.unsorted_segment_prod', 'unsorted_segment_prod'])
@deprecated_endpoints('unsorted_segment_prod')
def unsorted_segment_prod(data, segment_ids, num_segments, name=None):
  r"""Computes the product along segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#segmentation)
  for an explanation of segments.

  This operator is similar to the unsorted segment sum operator found
  [(here)](../../../api_docs/python/math_ops.md#UnsortedSegmentSum).
  Instead of computing the sum over segments, it computes the product of all
  entries belonging to a segment such that:

  \\(output_i = \prod_{j...} data[j...]\\) where the product is over tuples
  `j...` such that `segment_ids[j...] == i`.

  If there is no entry for a given segment ID `i`, it outputs 1.

  If the given segment ID `i` is negative, then the corresponding value is
  dropped, and will not be included in the result.

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    segment_ids: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor whose shape is a prefix of `data.shape`.
    num_segments: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnsortedSegmentProd", name, _ctx._post_execution_callbacks, data,
        segment_ids, num_segments)
      return _result
    except _core._FallbackException:
      try:
        return unsorted_segment_prod_eager_fallback(
            data, segment_ids, num_segments, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              unsorted_segment_prod, data=data, segment_ids=segment_ids,
                                     num_segments=num_segments, name=name)
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
        "UnsortedSegmentProd", data=data, segment_ids=segment_ids,
                               num_segments=num_segments, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          unsorted_segment_prod, data=data, segment_ids=segment_ids,
                                 num_segments=num_segments, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "Tnumsegments", _op.get_attr("Tnumsegments"))
  _execute.record_gradient(
      "UnsortedSegmentProd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unsorted_segment_prod_eager_fallback(data, segment_ids, num_segments, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unsorted_segment_prod
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tindices, (segment_ids,) = _execute.args_to_matching_eager([segment_ids], _ctx)
  _attr_Tnumsegments, (num_segments,) = _execute.args_to_matching_eager([num_segments], _ctx, _dtypes.int32)
  _inputs_flat = [data, segment_ids, num_segments]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "Tnumsegments",
  _attr_Tnumsegments)
  _result = _execute.execute(b"UnsortedSegmentProd", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnsortedSegmentProd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.unsorted_segment_sum', v1=['math.unsorted_segment_sum', 'unsorted_segment_sum'])
@deprecated_endpoints('unsorted_segment_sum')
def unsorted_segment_sum(data, segment_ids, num_segments, name=None):
  r"""Computes the sum along segments of a tensor.

  Read
  [the section on segmentation](https://tensorflow.org/api_guides/python/math_ops#Segmentation)
  for an explanation of segments.

  Computes a tensor such that
  \\(output[i] = \sum_{j...} data[j...]\\) where the sum is over tuples `j...` such
  that `segment_ids[j...] == i`.  Unlike `SegmentSum`, `segment_ids`
  need not be sorted and need not cover all values in the full
  range of valid values.

  If the sum is empty for a given segment ID `i`, `output[i] = 0`.
  If the given segment ID `i` is negative, the value is dropped and will not be
  added to the sum of the segment.

  `num_segments` should equal the number of distinct segment IDs.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/UnsortedSegmentSum.png" alt>
  </div>

  Args:
    data: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    segment_ids: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor whose shape is a prefix of `data.shape`.
    num_segments: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnsortedSegmentSum", name, _ctx._post_execution_callbacks, data,
        segment_ids, num_segments)
      return _result
    except _core._FallbackException:
      try:
        return unsorted_segment_sum_eager_fallback(
            data, segment_ids, num_segments, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              unsorted_segment_sum, data=data, segment_ids=segment_ids,
                                    num_segments=num_segments, name=name)
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
        "UnsortedSegmentSum", data=data, segment_ids=segment_ids,
                              num_segments=num_segments, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          unsorted_segment_sum, data=data, segment_ids=segment_ids,
                                num_segments=num_segments, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "Tnumsegments", _op.get_attr("Tnumsegments"))
  _execute.record_gradient(
      "UnsortedSegmentSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unsorted_segment_sum_eager_fallback(data, segment_ids, num_segments, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unsorted_segment_sum
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  _attr_Tindices, (segment_ids,) = _execute.args_to_matching_eager([segment_ids], _ctx)
  _attr_Tnumsegments, (num_segments,) = _execute.args_to_matching_eager([num_segments], _ctx, _dtypes.int32)
  _inputs_flat = [data, segment_ids, num_segments]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "Tnumsegments",
  _attr_Tnumsegments)
  _result = _execute.execute(b"UnsortedSegmentSum", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnsortedSegmentSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.xdivy')
def xdivy(x, y, name=None):
  r"""Returns 0 if x == 0, and x / y otherwise, elementwise.

  Args:
    x: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Xdivy", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return xdivy_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              xdivy, x=x, y=y, name=name)
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
        "Xdivy", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          xdivy, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Xdivy", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def xdivy_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function xdivy
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Xdivy", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Xdivy", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.xlogy')
def xlogy(x, y, name=None):
  r"""Returns 0 if x == 0, and x * log(y) otherwise, elementwise.

  Args:
    x: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`, `complex64`, `complex128`.
    y: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Xlogy", name,
        _ctx._post_execution_callbacks, x, y)
      return _result
    except _core._FallbackException:
      try:
        return xlogy_eager_fallback(
            x, y, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              xlogy, x=x, y=y, name=name)
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
        "Xlogy", x=x, y=y, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          xlogy, x=x, y=y, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Xlogy", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def xlogy_eager_fallback(x, y, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function xlogy
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Xlogy", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Xlogy", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.zeta', v1=['math.zeta', 'zeta'])
@deprecated_endpoints('zeta')
def zeta(x, q, name=None):
  r"""Compute the Hurwitz zeta function \\(\zeta(x, q)\\).

  The Hurwitz zeta function is defined as:


  \\(\zeta(x, q) = \sum_{n=0}^{\infty} (q + n)^{-x}\\)

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    q: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Zeta", name,
        _ctx._post_execution_callbacks, x, q)
      return _result
    except _core._FallbackException:
      try:
        return zeta_eager_fallback(
            x, q, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              zeta, x=x, q=q, name=name)
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
        "Zeta", x=x, q=q, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          zeta, x=x, q=q, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Zeta", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def zeta_eager_fallback(x, q, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function zeta
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, q], _ctx)
  (x, q) = _inputs_T
  _inputs_flat = [x, q]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Zeta", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Zeta", _inputs_flat, _attrs, _result, name)
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
#   name: "Abs"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "AccumulateNV2"
#   input_arg {
#     name: "inputs"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "sum"
#     type_attr: "T"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   is_aggregate: true
#   is_commutative: true
# }
# op {
#   name: "Acos"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Acosh"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Add"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#         type: DT_STRING
#       }
#     }
#   }
# }
# op {
#   name: "AddN"
#   input_arg {
#     name: "inputs"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "sum"
#     type_attr: "T"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#         type: DT_VARIANT
#       }
#     }
#   }
#   is_aggregate: true
#   is_commutative: true
# }
# op {
#   name: "AddV2"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   is_aggregate: true
#   is_commutative: true
# }
# op {
#   name: "All"
#   input_arg {
#     name: "input"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "reduction_indices"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type: DT_BOOL
#   }
#   attr {
#     name: "keep_dims"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "Angle"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "T"
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
#   attr {
#     name: "Tout"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Any"
#   input_arg {
#     name: "input"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "reduction_indices"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type: DT_BOOL
#   }
#   attr {
#     name: "keep_dims"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "ApproximateEqual"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "tolerance"
#     type: "float"
#     default_value {
#       f: 1e-05
#     }
#   }
#   is_commutative: true
# }
# op {
#   name: "ArgMax"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dimension"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "output_type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   attr {
#     name: "output_type"
#     type: "type"
#     default_value {
#       type: DT_INT64
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
#   name: "ArgMin"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dimension"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "output_type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   attr {
#     name: "output_type"
#     type: "type"
#     default_value {
#       type: DT_INT64
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
#   name: "Asin"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Asinh"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Atan"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Atan2"
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Atanh"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "BatchMatMul"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
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
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   attr {
#     name: "adj_x"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "adj_y"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "BesselI0e"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "BesselI1e"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Betainc"
#   input_arg {
#     name: "a"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
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
#   name: "Bincount"
#   input_arg {
#     name: "arr"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "weights"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "bins"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Bucketize"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type: DT_INT32
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "boundaries"
#     type: "list(float)"
#   }
# }
# op {
#   name: "Cast"
#   input_arg {
#     name: "x"
#     type_attr: "SrcT"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "DstT"
#   }
#   attr {
#     name: "SrcT"
#     type: "type"
#   }
#   attr {
#     name: "DstT"
#     type: "type"
#   }
#   attr {
#     name: "Truncate"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "Ceil"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "ClipByValue"
#   input_arg {
#     name: "t"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "clip_value_min"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "clip_value_max"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "CompareAndBitpack"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "threshold"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type: DT_UINT8
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BOOL
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Complex"
#   input_arg {
#     name: "real"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "imag"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "Tout"
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
#   name: "ComplexAbs"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "T"
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
#   attr {
#     name: "Tout"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Conj"
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
#     default_value {
#       type: DT_COMPLEX64
#     }
#     allowed_values {
#       list {
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#         type: DT_VARIANT
#       }
#     }
#   }
# }
# op {
#   name: "Cos"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Cosh"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Cross"
#   input_arg {
#     name: "a"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "product"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "Cumprod"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "axis"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#   }
#   attr {
#     name: "exclusive"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "reverse"
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "Cumsum"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "axis"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#   }
#   attr {
#     name: "exclusive"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "reverse"
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "Digamma"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Div"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "DivNoNan"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
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
#   name: "Equal"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_QUINT8
#         type: DT_QINT8
#         type: DT_QINT32
#         type: DT_STRING
#         type: DT_BOOL
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   is_commutative: true
# }
# op {
#   name: "Erf"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Erfc"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Exp"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Expm1"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Floor"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "FloorDiv"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "FloorMod"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Greater"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "GreaterEqual"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "HistogramFixedWidth"
#   input_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "value_range"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "nbins"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "out"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "dtype"
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
#   name: "Igamma"
#   input_arg {
#     name: "a"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
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
#   name: "IgammaGradA"
#   input_arg {
#     name: "a"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
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
#   name: "Igammac"
#   input_arg {
#     name: "a"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
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
#   name: "Imag"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "T"
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
#   attr {
#     name: "Tout"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Inv"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "InvGrad"
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dy"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "IsFinite"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "IsInf"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "IsNan"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Less"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "LessEqual"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "Lgamma"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "LinSpace"
#   input_arg {
#     name: "start"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "stop"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "num"
#     type_attr: "Tidx"
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
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "Log"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Log1p"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "LogicalAnd"
#   input_arg {
#     name: "x"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "y"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "z"
#     type: DT_BOOL
#   }
#   is_commutative: true
# }
# op {
#   name: "LogicalNot"
#   input_arg {
#     name: "x"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "y"
#     type: DT_BOOL
#   }
# }
# op {
#   name: "LogicalOr"
#   input_arg {
#     name: "x"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "y"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "z"
#     type: DT_BOOL
#   }
#   is_commutative: true
# }
# op {
#   name: "MatMul"
#   input_arg {
#     name: "a"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "product"
#     type_attr: "T"
#   }
#   attr {
#     name: "transpose_a"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "transpose_b"
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
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Max"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "reduction_indices"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "keep_dims"
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "Maximum"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_commutative: true
# }
# op {
#   name: "Mean"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "reduction_indices"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "keep_dims"
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "Min"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "reduction_indices"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "keep_dims"
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "Minimum"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_commutative: true
# }
# op {
#   name: "Mod"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_HALF
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Mul"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   is_commutative: true
# }
# op {
#   name: "Neg"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "NotEqual"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_QUINT8
#         type: DT_QINT8
#         type: DT_QINT32
#         type: DT_STRING
#         type: DT_BOOL
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   is_commutative: true
# }
# op {
#   name: "Polygamma"
#   input_arg {
#     name: "a"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
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
#   name: "Pow"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_HALF
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Prod"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "reduction_indices"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "keep_dims"
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "QuantizeDownAndShrinkRange"
#   input_arg {
#     name: "input"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "input_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "input_max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "output_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "Tinput"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
# }
# op {
#   name: "QuantizedAdd"
#   input_arg {
#     name: "x"
#     type_attr: "T1"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T2"
#   }
#   input_arg {
#     name: "min_x"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_x"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_y"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_y"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "z"
#     type_attr: "Toutput"
#   }
#   output_arg {
#     name: "min_z"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "max_z"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T1"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "T2"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "Toutput"
#     type: "type"
#     default_value {
#       type: DT_QINT32
#     }
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   is_commutative: true
# }
# op {
#   name: "QuantizedMatMul"
#   input_arg {
#     name: "a"
#     type_attr: "T1"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T2"
#   }
#   input_arg {
#     name: "min_a"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_a"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_b"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_b"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "out"
#     type_attr: "Toutput"
#   }
#   output_arg {
#     name: "min_out"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "max_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T1"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "T2"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "Toutput"
#     type: "type"
#     default_value {
#       type: DT_QINT32
#     }
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "transpose_a"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "transpose_b"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "Tactivation"
#     type: "type"
#     default_value {
#       type: DT_QUINT8
#     }
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
# }
# op {
#   name: "QuantizedMul"
#   input_arg {
#     name: "x"
#     type_attr: "T1"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T2"
#   }
#   input_arg {
#     name: "min_x"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_x"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_y"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_y"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "z"
#     type_attr: "Toutput"
#   }
#   output_arg {
#     name: "min_z"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "max_z"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T1"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "T2"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "Toutput"
#     type: "type"
#     default_value {
#       type: DT_QINT32
#     }
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   is_commutative: true
# }
# op {
#   name: "Range"
#   input_arg {
#     name: "start"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "limit"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "delta"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tidx"
#   }
#   attr {
#     name: "Tidx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Real"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "T"
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
#   attr {
#     name: "Tout"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "RealDiv"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Reciprocal"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "ReciprocalGrad"
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dy"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "RequantizationRange"
#   input_arg {
#     name: "input"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "input_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "input_max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "Tinput"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
# }
# op {
#   name: "Requantize"
#   input_arg {
#     name: "input"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "input_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "input_max"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "requested_output_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "requested_output_max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "output_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "Tinput"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
# }
# op {
#   name: "Rint"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Round"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Rsqrt"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "RsqrtGrad"
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dy"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "SegmentMax"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "segment_ids"
#     type_attr: "Tindices"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "SegmentMean"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "segment_ids"
#     type_attr: "Tindices"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "SegmentMin"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "segment_ids"
#     type_attr: "Tindices"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "SegmentProd"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "segment_ids"
#     type_attr: "Tindices"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "SegmentSum"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "segment_ids"
#     type_attr: "Tindices"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Select"
#   input_arg {
#     name: "condition"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "t"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "e"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "Sigmoid"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "SigmoidGrad"
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dy"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Sign"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Sin"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Sinh"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "SparseMatMul"
#   input_arg {
#     name: "a"
#     type_attr: "Ta"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "Tb"
#   }
#   output_arg {
#     name: "product"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "transpose_a"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "transpose_b"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "a_is_sparse"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "b_is_sparse"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "Ta"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_BFLOAT16
#       }
#     }
#   }
#   attr {
#     name: "Tb"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_BFLOAT16
#       }
#     }
#   }
# }
# op {
#   name: "SparseSegmentMean"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "segment_ids"
#     type: DT_INT32
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
#   attr {
#     name: "Tidx"
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
#   name: "SparseSegmentMeanGrad"
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "segment_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "output_dim0"
#     type: DT_INT32
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
#   attr {
#     name: "Tidx"
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
#   name: "SparseSegmentMeanWithNumSegments"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "segment_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "num_segments"
#     type_attr: "Tnumsegments"
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
#   attr {
#     name: "Tidx"
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
#   attr {
#     name: "Tnumsegments"
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
#   name: "SparseSegmentSqrtN"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "segment_ids"
#     type: DT_INT32
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
#   attr {
#     name: "Tidx"
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
#   name: "SparseSegmentSqrtNGrad"
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "segment_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "output_dim0"
#     type: DT_INT32
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
#   attr {
#     name: "Tidx"
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
#   name: "SparseSegmentSqrtNWithNumSegments"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "segment_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "num_segments"
#     type_attr: "Tnumsegments"
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
#   attr {
#     name: "Tidx"
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
#   attr {
#     name: "Tnumsegments"
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
#   name: "SparseSegmentSum"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "segment_ids"
#     type: DT_INT32
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "SparseSegmentSumWithNumSegments"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "segment_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "num_segments"
#     type_attr: "Tnumsegments"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   attr {
#     name: "Tnumsegments"
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
#   name: "Sqrt"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "SqrtGrad"
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dy"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Square"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "SquaredDifference"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
#   is_commutative: true
# }
# op {
#   name: "Sub"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Sum"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "reduction_indices"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "keep_dims"
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tidx"
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
#   name: "Tan"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Tanh"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "TanhGrad"
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dy"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "TruncateDiv"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "TruncateMod"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "UnsortedSegmentMax"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "segment_ids"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "num_segments"
#     type_attr: "Tnumsegments"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "Tnumsegments"
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
#   name: "UnsortedSegmentMin"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "segment_ids"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "num_segments"
#     type_attr: "Tnumsegments"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "Tnumsegments"
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
#   name: "UnsortedSegmentProd"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "segment_ids"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "num_segments"
#     type_attr: "Tnumsegments"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "Tnumsegments"
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
#   name: "UnsortedSegmentSum"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "segment_ids"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "num_segments"
#     type_attr: "Tnumsegments"
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
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "Tnumsegments"
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
#   name: "Xdivy"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Xlogy"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "Zeta"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "q"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "z"
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
_op_def_lib = _InitOpDefLibrary(b"\n,\n\003Abs\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\003\t\no\n\rAccumulateNV2\022\016\n\006inputs\"\001T*\001N\032\010\n\003sum\"\001T\"\014\n\001N\022\003int(\0010\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\016\n\005shape\022\005shape\200\001\001\220\001\001\n/\n\004Acos\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n.\n\005Acosh\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n:\n\003Add\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\016\023\001\002\004\006\005\003\t\010\022\007\nW\n\004AddN\022\016\n\006inputs\"\001T*\001N\032\010\n\003sum\"\001T\"\014\n\001N\022\003int(\0010\001\"!\n\001T\022\004type:\026\n\0242\022\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\025\200\001\001\220\001\001\nA\n\005AddV2\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\032\n\001T\022\004type:\017\n\r2\013\016\023\001\002\004\006\005\003\t\010\022\200\001\001\220\001\001\nh\n\003All\022\t\n\005input\030\n\022\031\n\021reduction_indices\"\004Tidx\032\n\n\006output\030\n\"\025\n\tkeep_dims\022\004bool\032\002(\000\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\nT\n\005Angle\022\n\n\005input\"\001T\032\016\n\006output\"\004Tout\"\025\n\001T\022\004type\032\0020\010:\006\n\0042\002\010\022\"\030\n\004Tout\022\004type\032\0020\001:\006\n\0042\002\001\002\nh\n\003Any\022\t\n\005input\030\n\022\031\n\021reduction_indices\"\004Tidx\032\n\n\006output\030\n\"\025\n\tkeep_dims\022\004bool\032\002(\000\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\ni\n\020ApproximateEqual\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\005\n\001z\030\n\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\031\n\ttolerance\022\005float\032\005%\254\305\'7\220\001\001\n\233\001\n\006ArgMax\022\n\n\005input\"\001T\022\021\n\tdimension\"\004Tidx\032\025\n\006output\"\013output_type\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\"\037\n\013output_type\022\004type\032\0020\t:\006\n\0042\002\003\t\n\233\001\n\006ArgMin\022\n\n\005input\"\001T\022\021\n\tdimension\"\004Tidx\032\025\n\006output\"\013output_type\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\"\037\n\013output_type\022\004type\032\0020\t:\006\n\0042\002\003\t\n/\n\004Asin\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n.\n\005Asinh\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n/\n\004Atan\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n4\n\005Atan2\022\006\n\001y\"\001T\022\006\n\001x\"\001T\032\006\n\001z\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n.\n\005Atanh\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\ni\n\013BatchMatMul\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\013\n\006output\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\"\021\n\005adj_x\022\004bool\032\002(\000\"\021\n\005adj_y\022\004bool\032\002(\000\n0\n\tBesselI0e\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n0\n\tBesselI1e\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n<\n\007Betainc\022\006\n\001a\"\001T\022\006\n\001b\"\001T\022\006\n\001x\"\001T\032\006\n\001z\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\nK\n\010Bincount\022\007\n\003arr\030\003\022\010\n\004size\030\003\022\014\n\007weights\"\001T\032\t\n\004bins\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\003\t\001\002\nS\n\tBucketize\022\n\n\005input\"\001T\032\n\n\006output\030\003\"\023\n\001T\022\004type:\010\n\0062\004\003\t\001\002\"\031\n\nboundaries\022\013list(float)\nN\n\004Cast\022\t\n\001x\"\004SrcT\032\t\n\001y\"\004DstT\"\014\n\004SrcT\022\004type\"\014\n\004DstT\022\004type\"\024\n\010Truncate\022\004bool\032\002(\000\n+\n\004Ceil\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\nn\n\013ClipByValue\022\006\n\001t\"\001T\022\023\n\016clip_value_min\"\001T\022\023\n\016clip_value_max\"\001T\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\nT\n\021CompareAndBitpack\022\n\n\005input\"\001T\022\016\n\tthreshold\"\001T\032\n\n\006output\030\004\"\027\n\001T\022\004type:\014\n\n2\010\n\023\001\002\006\005\003\t\n]\n\007Complex\022\t\n\004real\"\001T\022\t\n\004imag\"\001T\032\013\n\003out\"\004Tout\"\025\n\001T\022\004type\032\0020\001:\006\n\0042\002\001\002\"\030\n\004Tout\022\004type\032\0020\010:\006\n\0042\002\010\022\nP\n\nComplexAbs\022\006\n\001x\"\001T\032\t\n\001y\"\004Tout\"\025\n\001T\022\004type\032\0020\010:\006\n\0042\002\010\022\"\030\n\004Tout\022\004type\032\0020\001:\006\n\0042\002\001\002\n7\n\004Conj\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\026\n\001T\022\004type\032\0020\010:\007\n\0052\003\010\022\025\n,\n\003Cos\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n-\n\004Cosh\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\nB\n\005Cross\022\006\n\001a\"\001T\022\006\n\001b\"\001T\032\014\n\007product\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\221\001\n\007Cumprod\022\006\n\001x\"\001T\022\014\n\004axis\"\004Tidx\032\010\n\003out\"\001T\"\025\n\texclusive\022\004bool\032\002(\000\"\023\n\007reverse\022\004bool\032\002(\000\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\220\001\n\006Cumsum\022\006\n\001x\"\001T\022\014\n\004axis\"\004Tidx\032\010\n\003out\"\001T\"\025\n\texclusive\022\004bool\032\002(\000\"\023\n\007reverse\022\004bool\032\002(\000\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n.\n\007Digamma\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n:\n\003Div\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\016\023\001\002\004\006\021\005\003\t\010\022\n5\n\010DivNoNan\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\nB\n\005Equal\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\005\n\001z\030\n\"\037\n\001T\022\004type:\024\n\0222\020\016\023\001\002\004\006\005\003\t\010\014\013\r\007\n\022\220\001\001\n*\n\003Erf\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n+\n\004Erfc\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n,\n\003Exp\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n.\n\005Expm1\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n,\n\005Floor\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n?\n\010FloorDiv\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\016\023\001\002\004\006\021\005\003\t\010\022\n9\n\010FloorMod\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\003\t\016\023\001\002\n=\n\007Greater\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\005\n\001z\030\n\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\nB\n\014GreaterEqual\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\005\n\001z\030\n\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n}\n\023HistogramFixedWidth\022\013\n\006values\"\001T\022\020\n\013value_range\"\001T\022\t\n\005nbins\030\003\032\014\n\003out\"\005dtype\"\023\n\001T\022\004type:\010\n\0062\004\003\t\001\002\"\031\n\005dtype\022\004type\032\0020\003:\006\n\0042\002\003\t\n3\n\006Igamma\022\006\n\001a\"\001T\022\006\n\001x\"\001T\032\006\n\001z\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\n8\n\013IgammaGradA\022\006\n\001a\"\001T\022\006\n\001x\"\001T\032\006\n\001z\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\n4\n\007Igammac\022\006\n\001a\"\001T\022\006\n\001x\"\001T\032\006\n\001z\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\nS\n\004Imag\022\n\n\005input\"\001T\032\016\n\006output\"\004Tout\"\025\n\001T\022\004type\032\0020\010:\006\n\0042\002\010\022\"\030\n\004Tout\022\004type\032\0020\001:\006\n\0042\002\001\002\n.\n\003Inv\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n9\n\007InvGrad\022\006\n\001y\"\001T\022\007\n\002dy\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n.\n\010IsFinite\022\006\n\001x\"\001T\032\005\n\001y\030\n\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n+\n\005IsInf\022\006\n\001x\"\001T\032\005\n\001y\030\n\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n+\n\005IsNan\022\006\n\001x\"\001T\032\005\n\001y\030\n\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n:\n\004Less\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\005\n\001z\030\n\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n?\n\tLessEqual\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\005\n\001z\030\n\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n-\n\006Lgamma\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\ni\n\010LinSpace\022\n\n\005start\"\001T\022\t\n\004stop\"\001T\022\013\n\003num\"\004Tidx\032\013\n\006output\"\001T\"\022\n\001T\022\004type:\007\n\0052\003\016\001\002\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n,\n\003Log\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n.\n\005Log1p\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n$\n\nLogicalAnd\022\005\n\001x\030\n\022\005\n\001y\030\n\032\005\n\001z\030\n\220\001\001\n\032\n\nLogicalNot\022\005\n\001x\030\n\032\005\n\001y\030\n\n#\n\tLogicalOr\022\005\n\001x\030\n\022\005\n\001y\030\n\032\005\n\001z\030\n\220\001\001\nq\n\006MatMul\022\006\n\001a\"\001T\022\006\n\001b\"\001T\032\014\n\007product\"\001T\"\027\n\013transpose_a\022\004bool\032\002(\000\"\027\n\013transpose_b\022\004bool\032\002(\000\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n\214\001\n\003Max\022\n\n\005input\"\001T\022\031\n\021reduction_indices\"\004Tidx\032\013\n\006output\"\001T\"\025\n\tkeep_dims\022\004bool\032\002(\000\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n;\n\007Maximum\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\003\t\220\001\001\n\215\001\n\004Mean\022\n\n\005input\"\001T\022\031\n\021reduction_indices\"\004Tidx\032\013\n\006output\"\001T\"\025\n\tkeep_dims\022\004bool\032\002(\000\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\214\001\n\003Min\022\n\n\005input\"\001T\022\031\n\021reduction_indices\"\004Tidx\032\013\n\006output\"\001T\"\025\n\tkeep_dims\022\004bool\032\002(\000\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n;\n\007Minimum\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\003\t\220\001\001\n5\n\003Mod\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\026\n\001T\022\004type:\013\n\t2\007\003\t\023\023\016\001\002\n=\n\003Mul\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\016\023\001\002\004\006\021\005\003\t\010\022\220\001\001\n.\n\003Neg\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\nE\n\010NotEqual\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\005\n\001z\030\n\"\037\n\001T\022\004type:\024\n\0222\020\016\023\001\002\004\006\005\003\t\010\014\013\r\007\n\022\220\001\001\n6\n\tPolygamma\022\006\n\001a\"\001T\022\006\n\001x\"\001T\032\006\n\001z\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\n6\n\003Pow\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\001\023\002\003\t\010\022\n\215\001\n\004Prod\022\n\n\005input\"\001T\022\031\n\021reduction_indices\"\004Tidx\032\013\n\006output\"\001T\"\025\n\tkeep_dims\022\004bool\032\002(\000\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\267\001\n\032QuantizeDownAndShrinkRange\022\017\n\005input\"\006Tinput\022\r\n\tinput_min\030\001\022\r\n\tinput_max\030\001\032\022\n\006output\"\010out_type\032\016\n\noutput_min\030\001\032\016\n\noutput_max\030\001\"\031\n\006Tinput\022\004type:\t\n\0072\005\013\014\r\017\020\"\033\n\010out_type\022\004type:\t\n\0072\005\013\014\r\017\020\n\301\001\n\014QuantizedAdd\022\007\n\001x\"\002T1\022\007\n\001y\"\002T2\022\t\n\005min_x\030\001\022\t\n\005max_x\030\001\022\t\n\005min_y\030\001\022\t\n\005max_y\030\001\032\014\n\001z\"\007Toutput\032\t\n\005min_z\030\001\032\t\n\005max_z\030\001\"\025\n\002T1\022\004type:\t\n\0072\005\013\014\r\017\020\"\025\n\002T2\022\004type:\t\n\0072\005\013\014\r\017\020\"\036\n\007Toutput\022\004type\032\0020\r:\t\n\0072\005\013\014\r\017\020\220\001\001\n\235\002\n\017QuantizedMatMul\022\007\n\001a\"\002T1\022\007\n\001b\"\002T2\022\t\n\005min_a\030\001\022\t\n\005max_a\030\001\022\t\n\005min_b\030\001\022\t\n\005max_b\030\001\032\016\n\003out\"\007Toutput\032\013\n\007min_out\030\001\032\013\n\007max_out\030\001\"\025\n\002T1\022\004type:\t\n\0072\005\013\014\r\017\020\"\025\n\002T2\022\004type:\t\n\0072\005\013\014\r\017\020\"\036\n\007Toutput\022\004type\032\0020\r:\t\n\0072\005\013\014\r\017\020\"\027\n\013transpose_a\022\004bool\032\002(\000\"\027\n\013transpose_b\022\004bool\032\002(\000\"\"\n\013Tactivation\022\004type\032\0020\014:\t\n\0072\005\013\014\r\017\020\n\301\001\n\014QuantizedMul\022\007\n\001x\"\002T1\022\007\n\001y\"\002T2\022\t\n\005min_x\030\001\022\t\n\005max_x\030\001\022\t\n\005min_y\030\001\022\t\n\005max_y\030\001\032\014\n\001z\"\007Toutput\032\t\n\005min_z\030\001\032\t\n\005max_z\030\001\"\025\n\002T1\022\004type:\t\n\0072\005\013\014\r\017\020\"\025\n\002T2\022\004type:\t\n\0072\005\013\014\r\017\020\"\036\n\007Toutput\022\004type\032\0020\r:\t\n\0072\005\013\014\r\017\020\220\001\001\na\n\005Range\022\r\n\005start\"\004Tidx\022\r\n\005limit\"\004Tidx\022\r\n\005delta\"\004Tidx\032\016\n\006output\"\004Tidx\"\033\n\004Tidx\022\004type\032\0020\003:\t\n\0072\005\016\001\002\003\t\nS\n\004Real\022\n\n\005input\"\001T\032\016\n\006output\"\004Tout\"\025\n\001T\022\004type\032\0020\010:\006\n\0042\002\010\022\"\030\n\004Tout\022\004type\032\0020\001:\006\n\0042\002\001\002\n>\n\007RealDiv\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\016\023\001\002\004\006\021\005\003\t\010\022\n5\n\nReciprocal\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n@\n\016ReciprocalGrad\022\006\n\001y\"\001T\022\007\n\002dy\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n\177\n\023RequantizationRange\022\017\n\005input\"\006Tinput\022\r\n\tinput_min\030\001\022\r\n\tinput_max\030\001\032\016\n\noutput_min\030\001\032\016\n\noutput_max\030\001\"\031\n\006Tinput\022\004type:\t\n\0072\005\013\014\r\017\020\n\333\001\n\nRequantize\022\017\n\005input\"\006Tinput\022\r\n\tinput_min\030\001\022\r\n\tinput_max\030\001\022\030\n\024requested_output_min\030\001\022\030\n\024requested_output_max\030\001\032\022\n\006output\"\010out_type\032\016\n\noutput_min\030\001\032\016\n\noutput_max\030\001\"\031\n\006Tinput\022\004type:\t\n\0072\005\013\014\r\017\020\"\033\n\010out_type\022\004type:\t\n\0072\005\013\014\r\017\020\n+\n\004Rint\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n0\n\005Round\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n.\n\005Rsqrt\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n;\n\tRsqrtGrad\022\006\n\001y\"\001T\022\007\n\002dy\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\nt\n\nSegmentMax\022\t\n\004data\"\001T\022\027\n\013segment_ids\"\010Tindices\032\013\n\006output\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\nz\n\013SegmentMean\022\t\n\004data\"\001T\022\027\n\013segment_ids\"\010Tindices\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\nt\n\nSegmentMin\022\t\n\004data\"\001T\022\027\n\013segment_ids\"\010Tindices\032\013\n\006output\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\nz\n\013SegmentProd\022\t\n\004data\"\001T\022\027\n\013segment_ids\"\010Tindices\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\ny\n\nSegmentSum\022\t\n\004data\"\001T\022\027\n\013segment_ids\"\010Tindices\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\n?\n\006Select\022\r\n\tcondition\030\n\022\006\n\001t\"\001T\022\006\n\001e\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\n0\n\007Sigmoid\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n=\n\013SigmoidGrad\022\006\n\001y\"\001T\022\007\n\002dy\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n/\n\004Sign\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n,\n\003Sin\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n-\n\004Sinh\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n\301\001\n\014SparseMatMul\022\007\n\001a\"\002Ta\022\007\n\001b\"\002Tb\032\013\n\007product\030\001\"\027\n\013transpose_a\022\004bool\032\002(\000\"\027\n\013transpose_b\022\004bool\032\002(\000\"\027\n\013a_is_sparse\022\004bool\032\002(\000\"\027\n\013b_is_sparse\022\004bool\032\002(\000\"\026\n\002Ta\022\004type\032\0020\001:\006\n\0042\002\001\016\"\026\n\002Tb\022\004type\032\0020\001:\006\n\0042\002\001\016\nz\n\021SparseSegmentMean\022\t\n\004data\"\001T\022\017\n\007indices\"\004Tidx\022\017\n\013segment_ids\030\003\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\217\001\n\025SparseSegmentMeanGrad\022\t\n\004grad\"\001T\022\017\n\007indices\"\004Tidx\022\017\n\013segment_ids\030\003\022\017\n\013output_dim0\030\003\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\311\001\n SparseSegmentMeanWithNumSegments\022\t\n\004data\"\001T\022\017\n\007indices\"\004Tidx\022\017\n\013segment_ids\030\003\022\034\n\014num_segments\"\014Tnumsegments\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\" \n\014Tnumsegments\022\004type\032\0020\003:\006\n\0042\002\003\t\n{\n\022SparseSegmentSqrtN\022\t\n\004data\"\001T\022\017\n\007indices\"\004Tidx\022\017\n\013segment_ids\030\003\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\220\001\n\026SparseSegmentSqrtNGrad\022\t\n\004grad\"\001T\022\017\n\007indices\"\004Tidx\022\017\n\013segment_ids\030\003\022\017\n\013output_dim0\030\003\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\312\001\n!SparseSegmentSqrtNWithNumSegments\022\t\n\004data\"\001T\022\017\n\007indices\"\004Tidx\022\017\n\013segment_ids\030\003\022\034\n\014num_segments\"\014Tnumsegments\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\" \n\014Tnumsegments\022\004type\032\0020\003:\006\n\0042\002\003\t\n\203\001\n\020SparseSegmentSum\022\t\n\004data\"\001T\022\017\n\007indices\"\004Tidx\022\017\n\013segment_ids\030\003\032\013\n\006output\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\322\001\n\037SparseSegmentSumWithNumSegments\022\t\n\004data\"\001T\022\017\n\007indices\"\004Tidx\022\017\n\013segment_ids\030\003\022\034\n\014num_segments\"\014Tnumsegments\032\013\n\006output\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\" \n\014Tnumsegments\022\004type\032\0020\003:\006\n\0042\002\003\t\n-\n\004Sqrt\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n:\n\010SqrtGrad\022\006\n\001y\"\001T\022\007\n\002dy\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n1\n\006Square\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\nG\n\021SquaredDifference\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\220\001\001\n:\n\003Sub\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\016\023\001\002\004\006\021\005\003\t\010\022\n\214\001\n\003Sum\022\n\n\005input\"\001T\022\031\n\021reduction_indices\"\004Tidx\032\013\n\006output\"\001T\"\025\n\tkeep_dims\022\004bool\032\002(\000\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n.\n\003Tan\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n-\n\004Tanh\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\n:\n\010TanhGrad\022\006\n\001y\"\001T\022\007\n\002dy\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\016\023\001\002\010\022\nB\n\013TruncateDiv\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\016\023\001\002\004\006\021\005\003\t\010\022\n<\n\013TruncateMod\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\003\t\016\023\001\002\n\274\001\n\022UnsortedSegmentMax\022\t\n\004data\"\001T\022\027\n\013segment_ids\"\010Tindices\022\034\n\014num_segments\"\014Tnumsegments\032\013\n\006output\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\" \n\014Tnumsegments\022\004type\032\0020\003:\006\n\0042\002\003\t\n\274\001\n\022UnsortedSegmentMin\022\t\n\004data\"\001T\022\027\n\013segment_ids\"\010Tindices\022\034\n\014num_segments\"\014Tnumsegments\032\013\n\006output\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\" \n\014Tnumsegments\022\004type\032\0020\003:\006\n\0042\002\003\t\n\302\001\n\023UnsortedSegmentProd\022\t\n\004data\"\001T\022\027\n\013segment_ids\"\010Tindices\022\034\n\014num_segments\"\014Tnumsegments\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\" \n\014Tnumsegments\022\004type\032\0020\003:\006\n\0042\002\003\t\n\301\001\n\022UnsortedSegmentSum\022\t\n\004data\"\001T\022\027\n\013segment_ids\"\010Tindices\022\034\n\014num_segments\"\014Tnumsegments\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\" \n\014Tnumsegments\022\004type\032\0020\003:\006\n\0042\002\003\t\n5\n\005Xdivy\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\024\n\001T\022\004type:\t\n\0072\005\023\001\002\010\022\n5\n\005Xlogy\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\006\n\001z\"\001T\"\024\n\001T\022\004type:\t\n\0072\005\023\001\002\010\022\n1\n\004Zeta\022\006\n\001x\"\001T\022\006\n\001q\"\001T\032\006\n\001z\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002")
