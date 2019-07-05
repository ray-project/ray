"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: nn_ops.cc
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


def avg_pool(value, ksize, strides, padding, data_format="NHWC", name=None):
  r"""Performs average pooling on the input.

  Each entry in `output` is the mean of the corresponding size `ksize`
  window in `value`.

  Args:
    value: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      4-D with shape `[batch, height, width, channels]`.
    ksize: A list of `ints` that has length `>= 4`.
      The size of the sliding window for each dimension of `value`.
    strides: A list of `ints` that has length `>= 4`.
      The stride of the sliding window for each dimension of `value`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "AvgPool",
        name, _ctx._post_execution_callbacks, value, "ksize", ksize,
        "strides", strides, "padding", padding, "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return avg_pool_eager_fallback(
            value, ksize=ksize, strides=strides, padding=padding,
            data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'avg_pool' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'avg_pool' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AvgPool", value=value, ksize=ksize, strides=strides, padding=padding,
                   data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "AvgPool", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def avg_pool_eager_fallback(value, ksize, strides, padding, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function avg_pool
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'avg_pool' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'avg_pool' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _inputs_flat = [value]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding,
  "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"AvgPool", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "AvgPool", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.avg_pool3d')
def avg_pool3d(input, ksize, strides, padding, data_format="NDHWC", name=None):
  r"""Performs 3D average pooling on the input.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      Shape `[batch, depth, rows, cols, channels]` tensor to pool over.
    ksize: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The size of the window for each dimension of
      the input tensor. Must have `ksize[0] = ksize[4] = 1`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NDHWC", "NCDHW"`. Defaults to `"NDHWC"`.
      The data format of the input and output data. With the
      default format "NDHWC", the data is stored in the order of:
          [batch, in_depth, in_height, in_width, in_channels].
      Alternatively, the format could be "NCDHW", the data storage order is:
          [batch, in_channels, in_depth, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "AvgPool3D",
        name, _ctx._post_execution_callbacks, input, "ksize", ksize,
        "strides", strides, "padding", padding, "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return avg_pool3d_eager_fallback(
            input, ksize=ksize, strides=strides, padding=padding,
            data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              avg_pool3d, input=input, ksize=ksize, strides=strides,
                          padding=padding, data_format=data_format, name=name)
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
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'avg_pool3d' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'avg_pool3d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "AvgPool3D", input=input, ksize=ksize, strides=strides,
                     padding=padding, data_format=data_format, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          avg_pool3d, input=input, ksize=ksize, strides=strides,
                      padding=padding, data_format=data_format, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "AvgPool3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def avg_pool3d_eager_fallback(input, ksize, strides, padding, data_format="NDHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function avg_pool3d
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'avg_pool3d' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'avg_pool3d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding,
  "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"AvgPool3D", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AvgPool3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def avg_pool3d_grad(orig_input_shape, grad, ksize, strides, padding, data_format="NDHWC", name=None):
  r"""Computes gradients of average pooling function.

  Args:
    orig_input_shape: A `Tensor` of type `int32`.
      The original input dimensions.
    grad: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      Output backprop of shape `[batch, depth, rows, cols, channels]`.
    ksize: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The size of the window for each dimension of
      the input tensor. Must have `ksize[0] = ksize[4] = 1`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NDHWC", "NCDHW"`. Defaults to `"NDHWC"`.
      The data format of the input and output data. With the
      default format "NDHWC", the data is stored in the order of:
          [batch, in_depth, in_height, in_width, in_channels].
      Alternatively, the format could be "NCDHW", the data storage order is:
          [batch, in_channels, in_depth, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `grad`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AvgPool3DGrad", name, _ctx._post_execution_callbacks,
        orig_input_shape, grad, "ksize", ksize, "strides", strides, "padding",
        padding, "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return avg_pool3d_grad_eager_fallback(
            orig_input_shape, grad, ksize=ksize, strides=strides,
            padding=padding, data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'avg_pool3d_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'avg_pool3d_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AvgPool3DGrad", orig_input_shape=orig_input_shape, grad=grad,
                         ksize=ksize, strides=strides, padding=padding,
                         data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "AvgPool3DGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def avg_pool3d_grad_eager_fallback(orig_input_shape, grad, ksize, strides, padding, data_format="NDHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function avg_pool3d_grad
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'avg_pool3d_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'avg_pool3d_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (grad,) = _execute.args_to_matching_eager([grad], _ctx)
  orig_input_shape = _ops.convert_to_tensor(orig_input_shape, _dtypes.int32)
  _inputs_flat = [orig_input_shape, grad]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding,
  "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"AvgPool3DGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AvgPool3DGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def avg_pool_grad(orig_input_shape, grad, ksize, strides, padding, data_format="NHWC", name=None):
  r"""Computes gradients of the average pooling function.

  Args:
    orig_input_shape: A `Tensor` of type `int32`.
      1-D.  Shape of the original input to `avg_pool`.
    grad: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      4-D with shape `[batch, height, width, channels]`.  Gradients w.r.t.
      the output of `avg_pool`.
    ksize: A list of `ints` that has length `>= 4`.
      The size of the sliding window for each dimension of the input.
    strides: A list of `ints` that has length `>= 4`.
      The stride of the sliding window for each dimension of the input.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `grad`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "AvgPoolGrad",
        name, _ctx._post_execution_callbacks, orig_input_shape, grad, "ksize",
        ksize, "strides", strides, "padding", padding, "data_format",
        data_format)
      return _result
    except _core._FallbackException:
      try:
        return avg_pool_grad_eager_fallback(
            orig_input_shape, grad, ksize=ksize, strides=strides,
            padding=padding, data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'avg_pool_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'avg_pool_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AvgPoolGrad", orig_input_shape=orig_input_shape, grad=grad,
                       ksize=ksize, strides=strides, padding=padding,
                       data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "AvgPoolGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def avg_pool_grad_eager_fallback(orig_input_shape, grad, ksize, strides, padding, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function avg_pool_grad
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'avg_pool_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'avg_pool_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (grad,) = _execute.args_to_matching_eager([grad], _ctx)
  orig_input_shape = _ops.convert_to_tensor(orig_input_shape, _dtypes.int32)
  _inputs_flat = [orig_input_shape, grad]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding,
  "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"AvgPoolGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AvgPoolGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _batch_norm_with_global_normalization(t, m, v, beta, gamma, variance_epsilon, scale_after_normalization, name=None):
  r"""Batch normalization.

  This op is deprecated. Prefer `tf.nn.batch_normalization`.

  Args:
    t: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A 4D input Tensor.
    m: A `Tensor`. Must have the same type as `t`.
      A 1D mean Tensor with size matching the last dimension of t.
      This is the first output from tf.nn.moments,
      or a saved moving average thereof.
    v: A `Tensor`. Must have the same type as `t`.
      A 1D variance Tensor with size matching the last dimension of t.
      This is the second output from tf.nn.moments,
      or a saved moving average thereof.
    beta: A `Tensor`. Must have the same type as `t`.
      A 1D beta Tensor with size matching the last dimension of t.
      An offset to be added to the normalized tensor.
    gamma: A `Tensor`. Must have the same type as `t`.
      A 1D gamma Tensor with size matching the last dimension of t.
      If "scale_after_normalization" is true, this tensor will be multiplied
      with the normalized tensor.
    variance_epsilon: A `float`. A small float number to avoid dividing by 0.
    scale_after_normalization: A `bool`.
      A bool indicating whether the resulted tensor
      needs to be multiplied with gamma.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `t`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchNormWithGlobalNormalization", name,
        _ctx._post_execution_callbacks, t, m, v, beta, gamma,
        "variance_epsilon", variance_epsilon, "scale_after_normalization",
        scale_after_normalization)
      return _result
    except _core._FallbackException:
      try:
        return _batch_norm_with_global_normalization_eager_fallback(
            t, m, v, beta, gamma, variance_epsilon=variance_epsilon,
            scale_after_normalization=scale_after_normalization, name=name,
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
  variance_epsilon = _execute.make_float(variance_epsilon, "variance_epsilon")
  scale_after_normalization = _execute.make_bool(scale_after_normalization, "scale_after_normalization")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchNormWithGlobalNormalization", t=t, m=m, v=v, beta=beta,
                                            gamma=gamma,
                                            variance_epsilon=variance_epsilon,
                                            scale_after_normalization=scale_after_normalization,
                                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "variance_epsilon",
            _op.get_attr("variance_epsilon"), "scale_after_normalization",
            _op.get_attr("scale_after_normalization"))
  _execute.record_gradient(
      "BatchNormWithGlobalNormalization", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _batch_norm_with_global_normalization_eager_fallback(t, m, v, beta, gamma, variance_epsilon, scale_after_normalization, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _batch_norm_with_global_normalization
  """
  _ctx = ctx if ctx else _context.context()
  variance_epsilon = _execute.make_float(variance_epsilon, "variance_epsilon")
  scale_after_normalization = _execute.make_bool(scale_after_normalization, "scale_after_normalization")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([t, m, v, beta, gamma], _ctx)
  (t, m, v, beta, gamma) = _inputs_T
  _inputs_flat = [t, m, v, beta, gamma]
  _attrs = ("T", _attr_T, "variance_epsilon", variance_epsilon,
  "scale_after_normalization", scale_after_normalization)
  _result = _execute.execute(b"BatchNormWithGlobalNormalization", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BatchNormWithGlobalNormalization", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_batch_norm_with_global_normalization_grad_outputs = ["dx", "dm", "dv", "db",
                                                     "dg"]
_BatchNormWithGlobalNormalizationGradOutput = _collections.namedtuple(
    "BatchNormWithGlobalNormalizationGrad",
    _batch_norm_with_global_normalization_grad_outputs)


def batch_norm_with_global_normalization_grad(t, m, v, gamma, backprop, variance_epsilon, scale_after_normalization, name=None):
  r"""Gradients for batch normalization.

  This op is deprecated. See `tf.nn.batch_normalization`.

  Args:
    t: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A 4D input Tensor.
    m: A `Tensor`. Must have the same type as `t`.
      A 1D mean Tensor with size matching the last dimension of t.
      This is the first output from tf.nn.moments,
      or a saved moving average thereof.
    v: A `Tensor`. Must have the same type as `t`.
      A 1D variance Tensor with size matching the last dimension of t.
      This is the second output from tf.nn.moments,
      or a saved moving average thereof.
    gamma: A `Tensor`. Must have the same type as `t`.
      A 1D gamma Tensor with size matching the last dimension of t.
      If "scale_after_normalization" is true, this Tensor will be multiplied
      with the normalized Tensor.
    backprop: A `Tensor`. Must have the same type as `t`. 4D backprop Tensor.
    variance_epsilon: A `float`. A small float number to avoid dividing by 0.
    scale_after_normalization: A `bool`.
      A bool indicating whether the resulted tensor
      needs to be multiplied with gamma.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (dx, dm, dv, db, dg).

    dx: A `Tensor`. Has the same type as `t`.
    dm: A `Tensor`. Has the same type as `t`.
    dv: A `Tensor`. Has the same type as `t`.
    db: A `Tensor`. Has the same type as `t`.
    dg: A `Tensor`. Has the same type as `t`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchNormWithGlobalNormalizationGrad", name,
        _ctx._post_execution_callbacks, t, m, v, gamma, backprop,
        "variance_epsilon", variance_epsilon, "scale_after_normalization",
        scale_after_normalization)
      _result = _BatchNormWithGlobalNormalizationGradOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return batch_norm_with_global_normalization_grad_eager_fallback(
            t, m, v, gamma, backprop, variance_epsilon=variance_epsilon,
            scale_after_normalization=scale_after_normalization, name=name,
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
  variance_epsilon = _execute.make_float(variance_epsilon, "variance_epsilon")
  scale_after_normalization = _execute.make_bool(scale_after_normalization, "scale_after_normalization")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchNormWithGlobalNormalizationGrad", t=t, m=m, v=v, gamma=gamma,
                                                backprop=backprop,
                                                variance_epsilon=variance_epsilon,
                                                scale_after_normalization=scale_after_normalization,
                                                name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "variance_epsilon",
            _op.get_attr("variance_epsilon"), "scale_after_normalization",
            _op.get_attr("scale_after_normalization"))
  _execute.record_gradient(
      "BatchNormWithGlobalNormalizationGrad", _inputs_flat, _attrs, _result, name)
  _result = _BatchNormWithGlobalNormalizationGradOutput._make(_result)
  return _result



def batch_norm_with_global_normalization_grad_eager_fallback(t, m, v, gamma, backprop, variance_epsilon, scale_after_normalization, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_norm_with_global_normalization_grad
  """
  _ctx = ctx if ctx else _context.context()
  variance_epsilon = _execute.make_float(variance_epsilon, "variance_epsilon")
  scale_after_normalization = _execute.make_bool(scale_after_normalization, "scale_after_normalization")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([t, m, v, gamma, backprop], _ctx)
  (t, m, v, gamma, backprop) = _inputs_T
  _inputs_flat = [t, m, v, gamma, backprop]
  _attrs = ("T", _attr_T, "variance_epsilon", variance_epsilon,
  "scale_after_normalization", scale_after_normalization)
  _result = _execute.execute(b"BatchNormWithGlobalNormalizationGrad", 5,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BatchNormWithGlobalNormalizationGrad", _inputs_flat, _attrs, _result, name)
  _result = _BatchNormWithGlobalNormalizationGradOutput._make(_result)
  return _result


def bias_add(value, bias, data_format="NHWC", name=None):
  r"""Adds `bias` to `value`.

  This is a special case of `tf.add` where `bias` is restricted to be 1-D.
  Broadcasting is supported, so `value` may have any number of dimensions.

  Args:
    value: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Any number of dimensions.
    bias: A `Tensor`. Must have the same type as `value`.
      1-D with size the last dimension of `value`.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the bias tensor will be added to the last dimension
      of the value tensor.
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
      The tensor will be added to "in_channels", the third-to-the-last
          dimension.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BiasAdd",
        name, _ctx._post_execution_callbacks, value, bias, "data_format",
        data_format)
      return _result
    except _core._FallbackException:
      try:
        return bias_add_eager_fallback(
            value, bias, data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BiasAdd", value=value, bias=bias, data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "data_format",
            _op.get_attr("data_format"))
  _execute.record_gradient(
      "BiasAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bias_add_eager_fallback(value, bias, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bias_add
  """
  _ctx = ctx if ctx else _context.context()
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([value, bias], _ctx)
  (value, bias) = _inputs_T
  _inputs_flat = [value, bias]
  _attrs = ("T", _attr_T, "data_format", data_format)
  _result = _execute.execute(b"BiasAdd", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "BiasAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def bias_add_grad(out_backprop, data_format="NHWC", name=None):
  r"""The backward operation for "BiasAdd" on the "bias" tensor.

  It accumulates all the values from out_backprop into the feature dimension.
  For NHWC data format, the feature dimension is the last. For NCHW data format,
  the feature dimension is the third-to-last.

  Args:
    out_backprop: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Any number of dimensions.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the bias tensor will be added to the last dimension
      of the value tensor.
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
      The tensor will be added to "in_channels", the third-to-the-last
          dimension.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `out_backprop`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BiasAddGrad",
        name, _ctx._post_execution_callbacks, out_backprop, "data_format",
        data_format)
      return _result
    except _core._FallbackException:
      try:
        return bias_add_grad_eager_fallback(
            out_backprop, data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BiasAddGrad", out_backprop=out_backprop, data_format=data_format,
                       name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "data_format",
            _op.get_attr("data_format"))
  _execute.record_gradient(
      "BiasAddGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bias_add_grad_eager_fallback(out_backprop, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bias_add_grad
  """
  _ctx = ctx if ctx else _context.context()
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (out_backprop,) = _execute.args_to_matching_eager([out_backprop], _ctx)
  _inputs_flat = [out_backprop]
  _attrs = ("T", _attr_T, "data_format", data_format)
  _result = _execute.execute(b"BiasAddGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BiasAddGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def bias_add_v1(value, bias, name=None):
  r"""Adds `bias` to `value`.

  This is a deprecated version of BiasAdd and will be soon removed.

  This is a special case of `tf.add` where `bias` is restricted to be 1-D.
  Broadcasting is supported, so `value` may have any number of dimensions.

  Args:
    value: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Any number of dimensions.
    bias: A `Tensor`. Must have the same type as `value`.
      1-D with size the last dimension of `value`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BiasAddV1",
        name, _ctx._post_execution_callbacks, value, bias)
      return _result
    except _core._FallbackException:
      try:
        return bias_add_v1_eager_fallback(
            value, bias, name=name, ctx=_ctx)
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
        "BiasAddV1", value=value, bias=bias, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BiasAddV1", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bias_add_v1_eager_fallback(value, bias, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bias_add_v1
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([value, bias], _ctx)
  (value, bias) = _inputs_T
  _inputs_flat = [value, bias]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BiasAddV1", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BiasAddV1", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def conv2d(input, filter, strides, padding, use_cudnn_on_gpu=True, data_format="NHWC", dilations=[1, 1, 1, 1], name=None):
  r"""Computes a 2-D convolution given 4-D `input` and `filter` tensors.

  Given an input tensor of shape `[batch, in_height, in_width, in_channels]`
  and a filter / kernel tensor of shape
  `[filter_height, filter_width, in_channels, out_channels]`, this op
  performs the following:

  1. Flattens the filter to a 2-D matrix with shape
     `[filter_height * filter_width * in_channels, output_channels]`.
  2. Extracts image patches from the input tensor to form a *virtual*
     tensor of shape `[batch, out_height, out_width,
     filter_height * filter_width * in_channels]`.
  3. For each patch, right-multiplies the filter matrix and the image patch
     vector.

  In detail, with the default NHWC format,

      output[b, i, j, k] =
          sum_{di, dj, q} input[b, strides[1] * i + di, strides[2] * j + dj, q] *
                          filter[di, dj, q, k]

  Must have `strides[0] = strides[3] = 1`.  For the most common case of the same
  horizontal and vertices strides, `strides = [1, stride, stride, 1]`.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      A 4-D tensor. The dimension order is interpreted according to the value
      of `data_format`, see below for details.
    filter: A `Tensor`. Must have the same type as `input`.
      A 4-D tensor of shape
      `[filter_height, filter_width, in_channels, out_channels]`
    strides: A list of `ints`.
      1-D tensor of length 4.  The stride of the sliding window for each
      dimension of `input`. The dimension order is determined by the value of
      `data_format`, see below for details.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    use_cudnn_on_gpu: An optional `bool`. Defaults to `True`.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, height, width, channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, channels, height, width].
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1]`.
      1-D tensor of length 4.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each
      filter element on that dimension. The dimension order is determined by the
      value of `data_format`, see above for details. Dilations in the batch and
      depth dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Conv2D", name,
        _ctx._post_execution_callbacks, input, filter, "strides", strides,
        "use_cudnn_on_gpu", use_cudnn_on_gpu, "padding", padding,
        "data_format", data_format, "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return conv2d_eager_fallback(
            input, filter, strides=strides, use_cudnn_on_gpu=use_cudnn_on_gpu,
            padding=padding, data_format=data_format, dilations=dilations,
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
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if use_cudnn_on_gpu is None:
    use_cudnn_on_gpu = True
  use_cudnn_on_gpu = _execute.make_bool(use_cudnn_on_gpu, "use_cudnn_on_gpu")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv2d' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Conv2D", input=input, filter=filter, strides=strides,
                  padding=padding, use_cudnn_on_gpu=use_cudnn_on_gpu,
                  data_format=data_format, dilations=dilations, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "use_cudnn_on_gpu", _op.get_attr("use_cudnn_on_gpu"), "padding",
            _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "Conv2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conv2d_eager_fallback(input, filter, strides, padding, use_cudnn_on_gpu=True, data_format="NHWC", dilations=[1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conv2d
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if use_cudnn_on_gpu is None:
    use_cudnn_on_gpu = True
  use_cudnn_on_gpu = _execute.make_bool(use_cudnn_on_gpu, "use_cudnn_on_gpu")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv2d' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter], _ctx)
  (input, filter) = _inputs_T
  _inputs_flat = [input, filter]
  _attrs = ("T", _attr_T, "strides", strides, "use_cudnn_on_gpu",
  use_cudnn_on_gpu, "padding", padding, "data_format", data_format,
  "dilations", dilations)
  _result = _execute.execute(b"Conv2D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Conv2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def conv2d_backprop_filter(input, filter_sizes, out_backprop, strides, padding, use_cudnn_on_gpu=True, data_format="NHWC", dilations=[1, 1, 1, 1], name=None):
  r"""Computes the gradients of convolution with respect to the filter.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      4-D with shape `[batch, in_height, in_width, in_channels]`.
    filter_sizes: A `Tensor` of type `int32`.
      An integer vector representing the tensor shape of `filter`,
      where `filter` is a 4-D
      `[filter_height, filter_width, in_channels, out_channels]` tensor.
    out_backprop: A `Tensor`. Must have the same type as `input`.
      4-D with shape `[batch, out_height, out_width, out_channels]`.
      Gradients w.r.t. the output of the convolution.
    strides: A list of `ints`.
      The stride of the sliding window for each dimension of the input
      of the convolution. Must be in the same order as the dimension specified with
      format.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    use_cudnn_on_gpu: An optional `bool`. Defaults to `True`.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1]`.
      1-D tensor of length 4.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each filter
      element on that dimension. The dimension order is determined by the value of
      `data_format`, see above for details. Dilations in the batch and depth
      dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "Conv2DBackpropFilter", name, _ctx._post_execution_callbacks, input,
        filter_sizes, out_backprop, "strides", strides, "use_cudnn_on_gpu",
        use_cudnn_on_gpu, "padding", padding, "data_format", data_format,
        "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return conv2d_backprop_filter_eager_fallback(
            input, filter_sizes, out_backprop, strides=strides,
            use_cudnn_on_gpu=use_cudnn_on_gpu, padding=padding,
            data_format=data_format, dilations=dilations, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv2d_backprop_filter' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if use_cudnn_on_gpu is None:
    use_cudnn_on_gpu = True
  use_cudnn_on_gpu = _execute.make_bool(use_cudnn_on_gpu, "use_cudnn_on_gpu")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv2d_backprop_filter' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Conv2DBackpropFilter", input=input, filter_sizes=filter_sizes,
                                out_backprop=out_backprop, strides=strides,
                                padding=padding,
                                use_cudnn_on_gpu=use_cudnn_on_gpu,
                                data_format=data_format, dilations=dilations,
                                name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "use_cudnn_on_gpu", _op.get_attr("use_cudnn_on_gpu"), "padding",
            _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "Conv2DBackpropFilter", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conv2d_backprop_filter_eager_fallback(input, filter_sizes, out_backprop, strides, padding, use_cudnn_on_gpu=True, data_format="NHWC", dilations=[1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conv2d_backprop_filter
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv2d_backprop_filter' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if use_cudnn_on_gpu is None:
    use_cudnn_on_gpu = True
  use_cudnn_on_gpu = _execute.make_bool(use_cudnn_on_gpu, "use_cudnn_on_gpu")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv2d_backprop_filter' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, out_backprop], _ctx)
  (input, out_backprop) = _inputs_T
  filter_sizes = _ops.convert_to_tensor(filter_sizes, _dtypes.int32)
  _inputs_flat = [input, filter_sizes, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "use_cudnn_on_gpu",
  use_cudnn_on_gpu, "padding", padding, "data_format", data_format,
  "dilations", dilations)
  _result = _execute.execute(b"Conv2DBackpropFilter", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Conv2DBackpropFilter", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def conv2d_backprop_input(input_sizes, filter, out_backprop, strides, padding, use_cudnn_on_gpu=True, data_format="NHWC", dilations=[1, 1, 1, 1], name=None):
  r"""Computes the gradients of convolution with respect to the input.

  Args:
    input_sizes: A `Tensor` of type `int32`.
      An integer vector representing the shape of `input`,
      where `input` is a 4-D `[batch, height, width, channels]` tensor.
    filter: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      4-D with shape
      `[filter_height, filter_width, in_channels, out_channels]`.
    out_backprop: A `Tensor`. Must have the same type as `filter`.
      4-D with shape `[batch, out_height, out_width, out_channels]`.
      Gradients w.r.t. the output of the convolution.
    strides: A list of `ints`.
      The stride of the sliding window for each dimension of the input
      of the convolution. Must be in the same order as the dimension specified with
      format.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    use_cudnn_on_gpu: An optional `bool`. Defaults to `True`.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1]`.
      1-D tensor of length 4.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each filter
      element on that dimension. The dimension order is determined by the value of
      `data_format`, see above for details. Dilations in the batch and depth
      dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `filter`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "Conv2DBackpropInput", name, _ctx._post_execution_callbacks,
        input_sizes, filter, out_backprop, "strides", strides,
        "use_cudnn_on_gpu", use_cudnn_on_gpu, "padding", padding,
        "data_format", data_format, "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return conv2d_backprop_input_eager_fallback(
            input_sizes, filter, out_backprop, strides=strides,
            use_cudnn_on_gpu=use_cudnn_on_gpu, padding=padding,
            data_format=data_format, dilations=dilations, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv2d_backprop_input' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if use_cudnn_on_gpu is None:
    use_cudnn_on_gpu = True
  use_cudnn_on_gpu = _execute.make_bool(use_cudnn_on_gpu, "use_cudnn_on_gpu")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv2d_backprop_input' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Conv2DBackpropInput", input_sizes=input_sizes, filter=filter,
                               out_backprop=out_backprop, strides=strides,
                               padding=padding,
                               use_cudnn_on_gpu=use_cudnn_on_gpu,
                               data_format=data_format, dilations=dilations,
                               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "use_cudnn_on_gpu", _op.get_attr("use_cudnn_on_gpu"), "padding",
            _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "Conv2DBackpropInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conv2d_backprop_input_eager_fallback(input_sizes, filter, out_backprop, strides, padding, use_cudnn_on_gpu=True, data_format="NHWC", dilations=[1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conv2d_backprop_input
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv2d_backprop_input' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if use_cudnn_on_gpu is None:
    use_cudnn_on_gpu = True
  use_cudnn_on_gpu = _execute.make_bool(use_cudnn_on_gpu, "use_cudnn_on_gpu")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv2d_backprop_input' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([filter, out_backprop], _ctx)
  (filter, out_backprop) = _inputs_T
  input_sizes = _ops.convert_to_tensor(input_sizes, _dtypes.int32)
  _inputs_flat = [input_sizes, filter, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "use_cudnn_on_gpu",
  use_cudnn_on_gpu, "padding", padding, "data_format", data_format,
  "dilations", dilations)
  _result = _execute.execute(b"Conv2DBackpropInput", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Conv2DBackpropInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def conv3d(input, filter, strides, padding, data_format="NDHWC", dilations=[1, 1, 1, 1, 1], name=None):
  r"""Computes a 3-D convolution given 5-D `input` and `filter` tensors.

  In signal processing, cross-correlation is a measure of similarity of
  two waveforms as a function of a time-lag applied to one of them. This
  is also known as a sliding dot product or sliding inner-product.

  Our Conv3D implements a form of cross-correlation.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      Shape `[batch, in_depth, in_height, in_width, in_channels]`.
    filter: A `Tensor`. Must have the same type as `input`.
      Shape `[filter_depth, filter_height, filter_width, in_channels,
      out_channels]`. `in_channels` must match between `input` and `filter`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NDHWC", "NCDHW"`. Defaults to `"NDHWC"`.
      The data format of the input and output data. With the
      default format "NDHWC", the data is stored in the order of:
          [batch, in_depth, in_height, in_width, in_channels].
      Alternatively, the format could be "NCDHW", the data storage order is:
          [batch, in_channels, in_depth, in_height, in_width].
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1, 1]`.
      1-D tensor of length 5.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each
      filter element on that dimension. The dimension order is determined by the
      value of `data_format`, see above for details. Dilations in the batch and
      depth dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Conv3D", name,
        _ctx._post_execution_callbacks, input, filter, "strides", strides,
        "padding", padding, "data_format", data_format, "dilations",
        dilations)
      return _result
    except _core._FallbackException:
      try:
        return conv3d_eager_fallback(
            input, filter, strides=strides, padding=padding,
            data_format=data_format, dilations=dilations, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Conv3D", input=input, filter=filter, strides=strides,
                  padding=padding, data_format=data_format,
                  dilations=dilations, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "padding", _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "Conv3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conv3d_eager_fallback(input, filter, strides, padding, data_format="NDHWC", dilations=[1, 1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conv3d
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter], _ctx)
  (input, filter) = _inputs_T
  _inputs_flat = [input, filter]
  _attrs = ("T", _attr_T, "strides", strides, "padding", padding,
  "data_format", data_format, "dilations", dilations)
  _result = _execute.execute(b"Conv3D", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Conv3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def conv3d_backprop_filter(input, filter, out_backprop, strides, padding, dilations=[1, 1, 1, 1, 1], name=None):
  r"""Computes the gradients of 3-D convolution with respect to the filter.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`.
      Shape `[batch, depth, rows, cols, in_channels]`.
    filter: A `Tensor`. Must have the same type as `input`.
      Shape `[depth, rows, cols, in_channels, out_channels]`.
      `in_channels` must match between `input` and `filter`.
    out_backprop: A `Tensor`. Must have the same type as `input`.
      Backprop signal of shape `[batch, out_depth, out_rows, out_cols,
      out_channels]`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1, 1]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "Conv3DBackpropFilter", name, _ctx._post_execution_callbacks, input,
        filter, out_backprop, "strides", strides, "padding", padding,
        "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return conv3d_backprop_filter_eager_fallback(
            input, filter, out_backprop, strides=strides, padding=padding,
            dilations=dilations, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d_backprop_filter' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d_backprop_filter' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Conv3DBackpropFilter", input=input, filter=filter,
                                out_backprop=out_backprop, strides=strides,
                                padding=padding, dilations=dilations,
                                name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "padding", _op.get_attr("padding"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "Conv3DBackpropFilter", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conv3d_backprop_filter_eager_fallback(input, filter, out_backprop, strides, padding, dilations=[1, 1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conv3d_backprop_filter
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d_backprop_filter' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d_backprop_filter' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter, out_backprop], _ctx)
  (input, filter, out_backprop) = _inputs_T
  _inputs_flat = [input, filter, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "padding", padding, "dilations",
  dilations)
  _result = _execute.execute(b"Conv3DBackpropFilter", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Conv3DBackpropFilter", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.conv3d_backprop_filter', v1=['nn.conv3d_backprop_filter', 'nn.conv3d_backprop_filter_v2'])
@deprecated_endpoints('nn.conv3d_backprop_filter_v2')
def conv3d_backprop_filter_v2(input, filter_sizes, out_backprop, strides, padding, data_format="NDHWC", dilations=[1, 1, 1, 1, 1], name=None):
  r"""Computes the gradients of 3-D convolution with respect to the filter.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      Shape `[batch, depth, rows, cols, in_channels]`.
    filter_sizes: A `Tensor` of type `int32`.
      An integer vector representing the tensor shape of `filter`,
      where `filter` is a 5-D
      `[filter_depth, filter_height, filter_width, in_channels, out_channels]`
      tensor.
    out_backprop: A `Tensor`. Must have the same type as `input`.
      Backprop signal of shape `[batch, out_depth, out_rows, out_cols,
      out_channels]`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NDHWC", "NCDHW"`. Defaults to `"NDHWC"`.
      The data format of the input and output data. With the
      default format "NDHWC", the data is stored in the order of:
          [batch, in_depth, in_height, in_width, in_channels].
      Alternatively, the format could be "NCDHW", the data storage order is:
          [batch, in_channels, in_depth, in_height, in_width].
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1, 1]`.
      1-D tensor of length 5.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each
      filter element on that dimension. The dimension order is determined by the
      value of `data_format`, see above for details. Dilations in the batch and
      depth dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "Conv3DBackpropFilterV2", name, _ctx._post_execution_callbacks, input,
        filter_sizes, out_backprop, "strides", strides, "padding", padding,
        "data_format", data_format, "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return conv3d_backprop_filter_v2_eager_fallback(
            input, filter_sizes, out_backprop, strides=strides,
            padding=padding, data_format=data_format, dilations=dilations,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              conv3d_backprop_filter_v2, input=input,
                                         filter_sizes=filter_sizes,
                                         out_backprop=out_backprop,
                                         strides=strides, padding=padding,
                                         data_format=data_format,
                                         dilations=dilations, name=name)
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
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d_backprop_filter_v2' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d_backprop_filter_v2' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "Conv3DBackpropFilterV2", input=input, filter_sizes=filter_sizes,
                                  out_backprop=out_backprop, strides=strides,
                                  padding=padding, data_format=data_format,
                                  dilations=dilations, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          conv3d_backprop_filter_v2, input=input, filter_sizes=filter_sizes,
                                     out_backprop=out_backprop,
                                     strides=strides, padding=padding,
                                     data_format=data_format,
                                     dilations=dilations, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "padding", _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "Conv3DBackpropFilterV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conv3d_backprop_filter_v2_eager_fallback(input, filter_sizes, out_backprop, strides, padding, data_format="NDHWC", dilations=[1, 1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conv3d_backprop_filter_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d_backprop_filter_v2' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d_backprop_filter_v2' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, out_backprop], _ctx)
  (input, out_backprop) = _inputs_T
  filter_sizes = _ops.convert_to_tensor(filter_sizes, _dtypes.int32)
  _inputs_flat = [input, filter_sizes, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "padding", padding,
  "data_format", data_format, "dilations", dilations)
  _result = _execute.execute(b"Conv3DBackpropFilterV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "Conv3DBackpropFilterV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def conv3d_backprop_input(input, filter, out_backprop, strides, padding, dilations=[1, 1, 1, 1, 1], name=None):
  r"""Computes the gradients of 3-D convolution with respect to the input.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`.
      Shape `[batch, depth, rows, cols, in_channels]`.
    filter: A `Tensor`. Must have the same type as `input`.
      Shape `[depth, rows, cols, in_channels, out_channels]`.
      `in_channels` must match between `input` and `filter`.
    out_backprop: A `Tensor`. Must have the same type as `input`.
      Backprop signal of shape `[batch, out_depth, out_rows, out_cols,
      out_channels]`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1, 1]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "Conv3DBackpropInput", name, _ctx._post_execution_callbacks, input,
        filter, out_backprop, "strides", strides, "padding", padding,
        "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return conv3d_backprop_input_eager_fallback(
            input, filter, out_backprop, strides=strides, padding=padding,
            dilations=dilations, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d_backprop_input' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d_backprop_input' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Conv3DBackpropInput", input=input, filter=filter,
                               out_backprop=out_backprop, strides=strides,
                               padding=padding, dilations=dilations,
                               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "padding", _op.get_attr("padding"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "Conv3DBackpropInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conv3d_backprop_input_eager_fallback(input, filter, out_backprop, strides, padding, dilations=[1, 1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conv3d_backprop_input
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d_backprop_input' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d_backprop_input' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter, out_backprop], _ctx)
  (input, filter, out_backprop) = _inputs_T
  _inputs_flat = [input, filter, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "padding", padding, "dilations",
  dilations)
  _result = _execute.execute(b"Conv3DBackpropInput", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Conv3DBackpropInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def conv3d_backprop_input_v2(input_sizes, filter, out_backprop, strides, padding, data_format="NDHWC", dilations=[1, 1, 1, 1, 1], name=None):
  r"""Computes the gradients of 3-D convolution with respect to the input.

  Args:
    input_sizes: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      An integer vector representing the tensor shape of `input`,
      where `input` is a 5-D
      `[batch, depth, rows, cols, in_channels]` tensor.
    filter: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      Shape `[depth, rows, cols, in_channels, out_channels]`.
      `in_channels` must match between `input` and `filter`.
    out_backprop: A `Tensor`. Must have the same type as `filter`.
      Backprop signal of shape `[batch, out_depth, out_rows, out_cols,
      out_channels]`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NDHWC", "NCDHW"`. Defaults to `"NDHWC"`.
      The data format of the input and output data. With the
      default format "NDHWC", the data is stored in the order of:
          [batch, in_depth, in_height, in_width, in_channels].
      Alternatively, the format could be "NCDHW", the data storage order is:
          [batch, in_channels, in_depth, in_height, in_width].
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1, 1]`.
      1-D tensor of length 5.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each
      filter element on that dimension. The dimension order is determined by the
      value of `data_format`, see above for details. Dilations in the batch and
      depth dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `filter`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "Conv3DBackpropInputV2", name, _ctx._post_execution_callbacks,
        input_sizes, filter, out_backprop, "strides", strides, "padding",
        padding, "data_format", data_format, "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return conv3d_backprop_input_v2_eager_fallback(
            input_sizes, filter, out_backprop, strides=strides,
            padding=padding, data_format=data_format, dilations=dilations,
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
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d_backprop_input_v2' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d_backprop_input_v2' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Conv3DBackpropInputV2", input_sizes=input_sizes, filter=filter,
                                 out_backprop=out_backprop, strides=strides,
                                 padding=padding, data_format=data_format,
                                 dilations=dilations, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "padding", _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "dilations",
            _op.get_attr("dilations"), "Tshape", _op.get_attr("Tshape"))
  _execute.record_gradient(
      "Conv3DBackpropInputV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conv3d_backprop_input_v2_eager_fallback(input_sizes, filter, out_backprop, strides, padding, data_format="NDHWC", dilations=[1, 1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conv3d_backprop_input_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'conv3d_backprop_input_v2' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'conv3d_backprop_input_v2' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([filter, out_backprop], _ctx)
  (filter, out_backprop) = _inputs_T
  _attr_Tshape, (input_sizes,) = _execute.args_to_matching_eager([input_sizes], _ctx, _dtypes.int32)
  _inputs_flat = [input_sizes, filter, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "padding", padding,
  "data_format", data_format, "dilations", dilations, "Tshape", _attr_Tshape)
  _result = _execute.execute(b"Conv3DBackpropInputV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Conv3DBackpropInputV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def data_format_dim_map(x, src_format="NHWC", dst_format="NCHW", name=None):
  r"""Returns the dimension index in the destination data format given the one in

  the source data format.

  Args:
    x: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A Tensor with each element as a dimension index in source data format.
      Must be in the range [-4, 4).
    src_format: An optional `string`. Defaults to `"NHWC"`.
      source data format.
    dst_format: An optional `string`. Defaults to `"NCHW"`.
      destination data format.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DataFormatDimMap", name, _ctx._post_execution_callbacks, x,
        "src_format", src_format, "dst_format", dst_format)
      return _result
    except _core._FallbackException:
      try:
        return data_format_dim_map_eager_fallback(
            x, src_format=src_format, dst_format=dst_format, name=name,
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
  if src_format is None:
    src_format = "NHWC"
  src_format = _execute.make_str(src_format, "src_format")
  if dst_format is None:
    dst_format = "NCHW"
  dst_format = _execute.make_str(dst_format, "dst_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DataFormatDimMap", x=x, src_format=src_format, dst_format=dst_format,
                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "src_format", _op.get_attr("src_format"),
            "dst_format", _op.get_attr("dst_format"))
  _execute.record_gradient(
      "DataFormatDimMap", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def data_format_dim_map_eager_fallback(x, src_format="NHWC", dst_format="NCHW", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function data_format_dim_map
  """
  _ctx = ctx if ctx else _context.context()
  if src_format is None:
    src_format = "NHWC"
  src_format = _execute.make_str(src_format, "src_format")
  if dst_format is None:
    dst_format = "NCHW"
  dst_format = _execute.make_str(dst_format, "dst_format")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx, _dtypes.int32)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T, "src_format", src_format, "dst_format", dst_format)
  _result = _execute.execute(b"DataFormatDimMap", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DataFormatDimMap", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def data_format_vec_permute(x, src_format="NHWC", dst_format="NCHW", name=None):
  r"""Returns the permuted vector/tensor in the destination data format given the

  one in the source data format.

  Args:
    x: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Vector of size 4 or Tensor of shape (4, 2) in source data format.
    src_format: An optional `string`. Defaults to `"NHWC"`.
      source data format.
    dst_format: An optional `string`. Defaults to `"NCHW"`.
      destination data format.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DataFormatVecPermute", name, _ctx._post_execution_callbacks, x,
        "src_format", src_format, "dst_format", dst_format)
      return _result
    except _core._FallbackException:
      try:
        return data_format_vec_permute_eager_fallback(
            x, src_format=src_format, dst_format=dst_format, name=name,
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
  if src_format is None:
    src_format = "NHWC"
  src_format = _execute.make_str(src_format, "src_format")
  if dst_format is None:
    dst_format = "NCHW"
  dst_format = _execute.make_str(dst_format, "dst_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DataFormatVecPermute", x=x, src_format=src_format,
                                dst_format=dst_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "src_format", _op.get_attr("src_format"),
            "dst_format", _op.get_attr("dst_format"))
  _execute.record_gradient(
      "DataFormatVecPermute", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def data_format_vec_permute_eager_fallback(x, src_format="NHWC", dst_format="NCHW", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function data_format_vec_permute
  """
  _ctx = ctx if ctx else _context.context()
  if src_format is None:
    src_format = "NHWC"
  src_format = _execute.make_str(src_format, "src_format")
  if dst_format is None:
    dst_format = "NCHW"
  dst_format = _execute.make_str(dst_format, "dst_format")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx, _dtypes.int32)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T, "src_format", src_format, "dst_format", dst_format)
  _result = _execute.execute(b"DataFormatVecPermute", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DataFormatVecPermute", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export(v1=['nn.depthwise_conv2d_native'])
@deprecated_endpoints('nn.depthwise_conv2d_native')
def depthwise_conv2d_native(input, filter, strides, padding, data_format="NHWC", dilations=[1, 1, 1, 1], name=None):
  r"""Computes a 2-D depthwise convolution given 4-D `input` and `filter` tensors.

  Given an input tensor of shape `[batch, in_height, in_width, in_channels]`
  and a filter / kernel tensor of shape
  `[filter_height, filter_width, in_channels, channel_multiplier]`, containing
  `in_channels` convolutional filters of depth 1, `depthwise_conv2d` applies
  a different filter to each input channel (expanding from 1 channel to
  `channel_multiplier` channels for each), then concatenates the results
  together. Thus, the output has `in_channels * channel_multiplier` channels.

  ```
  for k in 0..in_channels-1
    for q in 0..channel_multiplier-1
      output[b, i, j, k * channel_multiplier + q] =
        sum_{di, dj} input[b, strides[1] * i + di, strides[2] * j + dj, k] *
                          filter[di, dj, k, q]
  ```

  Must have `strides[0] = strides[3] = 1`.  For the most common case of the same
  horizontal and vertices strides, `strides = [1, stride, stride, 1]`.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
    filter: A `Tensor`. Must have the same type as `input`.
    strides: A list of `ints`.
      1-D of length 4.  The stride of the sliding window for each dimension
      of `input`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, height, width, channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, channels, height, width].
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1]`.
      1-D tensor of length 4.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each filter
      element on that dimension. The dimension order is determined by the value of
      `data_format`, see above for details. Dilations in the batch and depth
      dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DepthwiseConv2dNative", name, _ctx._post_execution_callbacks, input,
        filter, "strides", strides, "padding", padding, "data_format",
        data_format, "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return depthwise_conv2d_native_eager_fallback(
            input, filter, strides=strides, padding=padding,
            data_format=data_format, dilations=dilations, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              depthwise_conv2d_native, input=input, filter=filter,
                                       strides=strides, padding=padding,
                                       data_format=data_format,
                                       dilations=dilations, name=name)
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
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'depthwise_conv2d_native' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'depthwise_conv2d_native' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "DepthwiseConv2dNative", input=input, filter=filter, strides=strides,
                                 padding=padding, data_format=data_format,
                                 dilations=dilations, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          depthwise_conv2d_native, input=input, filter=filter,
                                   strides=strides, padding=padding,
                                   data_format=data_format,
                                   dilations=dilations, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "padding", _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "DepthwiseConv2dNative", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def depthwise_conv2d_native_eager_fallback(input, filter, strides, padding, data_format="NHWC", dilations=[1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function depthwise_conv2d_native
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'depthwise_conv2d_native' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'depthwise_conv2d_native' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter], _ctx)
  (input, filter) = _inputs_T
  _inputs_flat = [input, filter]
  _attrs = ("T", _attr_T, "strides", strides, "padding", padding,
  "data_format", data_format, "dilations", dilations)
  _result = _execute.execute(b"DepthwiseConv2dNative", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DepthwiseConv2dNative", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.depthwise_conv2d_backprop_filter', v1=['nn.depthwise_conv2d_native_backprop_filter', 'nn.depthwise_conv2d_backprop_filter'])
@deprecated_endpoints('nn.depthwise_conv2d_native_backprop_filter')
def depthwise_conv2d_native_backprop_filter(input, filter_sizes, out_backprop, strides, padding, data_format="NHWC", dilations=[1, 1, 1, 1], name=None):
  r"""Computes the gradients of depthwise convolution with respect to the filter.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      4-D with shape based on `data_format`.  For example, if
      `data_format` is 'NHWC' then `input` is a 4-D `[batch, in_height,
      in_width, in_channels]` tensor.
    filter_sizes: A `Tensor` of type `int32`.
      An integer vector representing the tensor shape of `filter`,
      where `filter` is a 4-D
      `[filter_height, filter_width, in_channels, depthwise_multiplier]` tensor.
    out_backprop: A `Tensor`. Must have the same type as `input`.
      4-D with shape  based on `data_format`.
      For example, if `data_format` is 'NHWC' then
      out_backprop shape is `[batch, out_height, out_width, out_channels]`.
      Gradients w.r.t. the output of the convolution.
    strides: A list of `ints`.
      The stride of the sliding window for each dimension of the input
      of the convolution.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, height, width, channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, channels, height, width].
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1]`.
      1-D tensor of length 4.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each filter
      element on that dimension. The dimension order is determined by the value of
      `data_format`, see above for details. Dilations in the batch and depth
      dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DepthwiseConv2dNativeBackpropFilter", name,
        _ctx._post_execution_callbacks, input, filter_sizes, out_backprop,
        "strides", strides, "padding", padding, "data_format", data_format,
        "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return depthwise_conv2d_native_backprop_filter_eager_fallback(
            input, filter_sizes, out_backprop, strides=strides,
            padding=padding, data_format=data_format, dilations=dilations,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              depthwise_conv2d_native_backprop_filter, input=input,
                                                       filter_sizes=filter_sizes,
                                                       out_backprop=out_backprop,
                                                       strides=strides,
                                                       padding=padding,
                                                       data_format=data_format,
                                                       dilations=dilations,
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
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'depthwise_conv2d_native_backprop_filter' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'depthwise_conv2d_native_backprop_filter' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "DepthwiseConv2dNativeBackpropFilter", input=input,
                                               filter_sizes=filter_sizes,
                                               out_backprop=out_backprop,
                                               strides=strides,
                                               padding=padding,
                                               data_format=data_format,
                                               dilations=dilations, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          depthwise_conv2d_native_backprop_filter, input=input,
                                                   filter_sizes=filter_sizes,
                                                   out_backprop=out_backprop,
                                                   strides=strides,
                                                   padding=padding,
                                                   data_format=data_format,
                                                   dilations=dilations,
                                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "padding", _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "DepthwiseConv2dNativeBackpropFilter", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def depthwise_conv2d_native_backprop_filter_eager_fallback(input, filter_sizes, out_backprop, strides, padding, data_format="NHWC", dilations=[1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function depthwise_conv2d_native_backprop_filter
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'depthwise_conv2d_native_backprop_filter' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'depthwise_conv2d_native_backprop_filter' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, out_backprop], _ctx)
  (input, out_backprop) = _inputs_T
  filter_sizes = _ops.convert_to_tensor(filter_sizes, _dtypes.int32)
  _inputs_flat = [input, filter_sizes, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "padding", padding,
  "data_format", data_format, "dilations", dilations)
  _result = _execute.execute(b"DepthwiseConv2dNativeBackpropFilter", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "DepthwiseConv2dNativeBackpropFilter", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.depthwise_conv2d_backprop_input', v1=['nn.depthwise_conv2d_native_backprop_input', 'nn.depthwise_conv2d_backprop_input'])
@deprecated_endpoints('nn.depthwise_conv2d_native_backprop_input')
def depthwise_conv2d_native_backprop_input(input_sizes, filter, out_backprop, strides, padding, data_format="NHWC", dilations=[1, 1, 1, 1], name=None):
  r"""Computes the gradients of depthwise convolution with respect to the input.

  Args:
    input_sizes: A `Tensor` of type `int32`.
      An integer vector representing the shape of `input`, based
      on `data_format`.  For example, if `data_format` is 'NHWC' then
       `input` is a 4-D `[batch, height, width, channels]` tensor.
    filter: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      4-D with shape
      `[filter_height, filter_width, in_channels, depthwise_multiplier]`.
    out_backprop: A `Tensor`. Must have the same type as `filter`.
      4-D with shape  based on `data_format`.
      For example, if `data_format` is 'NHWC' then
      out_backprop shape is `[batch, out_height, out_width, out_channels]`.
      Gradients w.r.t. the output of the convolution.
    strides: A list of `ints`.
      The stride of the sliding window for each dimension of the input
      of the convolution.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, height, width, channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, channels, height, width].
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1]`.
      1-D tensor of length 4.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each filter
      element on that dimension. The dimension order is determined by the value of
      `data_format`, see above for details. Dilations in the batch and depth
      dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `filter`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DepthwiseConv2dNativeBackpropInput", name,
        _ctx._post_execution_callbacks, input_sizes, filter, out_backprop,
        "strides", strides, "padding", padding, "data_format", data_format,
        "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return depthwise_conv2d_native_backprop_input_eager_fallback(
            input_sizes, filter, out_backprop, strides=strides,
            padding=padding, data_format=data_format, dilations=dilations,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              depthwise_conv2d_native_backprop_input, input_sizes=input_sizes,
                                                      filter=filter,
                                                      out_backprop=out_backprop,
                                                      strides=strides,
                                                      padding=padding,
                                                      data_format=data_format,
                                                      dilations=dilations,
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
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'depthwise_conv2d_native_backprop_input' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'depthwise_conv2d_native_backprop_input' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "DepthwiseConv2dNativeBackpropInput", input_sizes=input_sizes,
                                              filter=filter,
                                              out_backprop=out_backprop,
                                              strides=strides,
                                              padding=padding,
                                              data_format=data_format,
                                              dilations=dilations, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          depthwise_conv2d_native_backprop_input, input_sizes=input_sizes,
                                                  filter=filter,
                                                  out_backprop=out_backprop,
                                                  strides=strides,
                                                  padding=padding,
                                                  data_format=data_format,
                                                  dilations=dilations,
                                                  name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "padding", _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "DepthwiseConv2dNativeBackpropInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def depthwise_conv2d_native_backprop_input_eager_fallback(input_sizes, filter, out_backprop, strides, padding, data_format="NHWC", dilations=[1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function depthwise_conv2d_native_backprop_input
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'depthwise_conv2d_native_backprop_input' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'depthwise_conv2d_native_backprop_input' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([filter, out_backprop], _ctx)
  (filter, out_backprop) = _inputs_T
  input_sizes = _ops.convert_to_tensor(input_sizes, _dtypes.int32)
  _inputs_flat = [input_sizes, filter, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "padding", padding,
  "data_format", data_format, "dilations", dilations)
  _result = _execute.execute(b"DepthwiseConv2dNativeBackpropInput", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "DepthwiseConv2dNativeBackpropInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export(v1=['nn.dilation2d'])
@deprecated_endpoints('nn.dilation2d')
def dilation2d(input, filter, strides, rates, padding, name=None):
  r"""Computes the grayscale dilation of 4-D `input` and 3-D `filter` tensors.

  The `input` tensor has shape `[batch, in_height, in_width, depth]` and the
  `filter` tensor has shape `[filter_height, filter_width, depth]`, i.e., each
  input channel is processed independently of the others with its own structuring
  function. The `output` tensor has shape
  `[batch, out_height, out_width, depth]`. The spatial dimensions of the output
  tensor depend on the `padding` algorithm. We currently only support the default
  "NHWC" `data_format`.

  In detail, the grayscale morphological 2-D dilation is the max-sum correlation
  (for consistency with `conv2d`, we use unmirrored filters):

      output[b, y, x, c] =
         max_{dy, dx} input[b,
                            strides[1] * y + rates[1] * dy,
                            strides[2] * x + rates[2] * dx,
                            c] +
                      filter[dy, dx, c]

  Max-pooling is a special case when the filter has size equal to the pooling
  kernel size and contains all zeros.

  Note on duality: The dilation of `input` by the `filter` is equal to the
  negation of the erosion of `-input` by the reflected `filter`.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      4-D with shape `[batch, in_height, in_width, depth]`.
    filter: A `Tensor`. Must have the same type as `input`.
      3-D with shape `[filter_height, filter_width, depth]`.
    strides: A list of `ints` that has length `>= 4`.
      The stride of the sliding window for each dimension of the input
      tensor. Must be: `[1, stride_height, stride_width, 1]`.
    rates: A list of `ints` that has length `>= 4`.
      The input stride for atrous morphological dilation. Must be:
      `[1, rate_height, rate_width, 1]`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Dilation2D",
        name, _ctx._post_execution_callbacks, input, filter, "strides",
        strides, "rates", rates, "padding", padding)
      return _result
    except _core._FallbackException:
      try:
        return dilation2d_eager_fallback(
            input, filter, strides=strides, rates=rates, padding=padding,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              dilation2d, input=input, filter=filter, strides=strides,
                          rates=rates, padding=padding, name=name)
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
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'dilation2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  if not isinstance(rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'rates' argument to "
        "'dilation2d' Op, not %r." % rates)
  rates = [_execute.make_int(_i, "rates") for _i in rates]
  padding = _execute.make_str(padding, "padding")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "Dilation2D", input=input, filter=filter, strides=strides,
                      rates=rates, padding=padding, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          dilation2d, input=input, filter=filter, strides=strides,
                      rates=rates, padding=padding, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "rates", _op.get_attr("rates"), "padding",
            _op.get_attr("padding"))
  _execute.record_gradient(
      "Dilation2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def dilation2d_eager_fallback(input, filter, strides, rates, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dilation2d
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'dilation2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  if not isinstance(rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'rates' argument to "
        "'dilation2d' Op, not %r." % rates)
  rates = [_execute.make_int(_i, "rates") for _i in rates]
  padding = _execute.make_str(padding, "padding")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter], _ctx)
  (input, filter) = _inputs_T
  _inputs_flat = [input, filter]
  _attrs = ("T", _attr_T, "strides", strides, "rates", rates, "padding",
  padding)
  _result = _execute.execute(b"Dilation2D", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Dilation2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def dilation2d_backprop_filter(input, filter, out_backprop, strides, rates, padding, name=None):
  r"""Computes the gradient of morphological 2-D dilation with respect to the filter.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      4-D with shape `[batch, in_height, in_width, depth]`.
    filter: A `Tensor`. Must have the same type as `input`.
      3-D with shape `[filter_height, filter_width, depth]`.
    out_backprop: A `Tensor`. Must have the same type as `input`.
      4-D with shape `[batch, out_height, out_width, depth]`.
    strides: A list of `ints` that has length `>= 4`.
      1-D of length 4. The stride of the sliding window for each dimension of
      the input tensor. Must be: `[1, stride_height, stride_width, 1]`.
    rates: A list of `ints` that has length `>= 4`.
      1-D of length 4. The input stride for atrous morphological dilation.
      Must be: `[1, rate_height, rate_width, 1]`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "Dilation2DBackpropFilter", name, _ctx._post_execution_callbacks,
        input, filter, out_backprop, "strides", strides, "rates", rates,
        "padding", padding)
      return _result
    except _core._FallbackException:
      try:
        return dilation2d_backprop_filter_eager_fallback(
            input, filter, out_backprop, strides=strides, rates=rates,
            padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'dilation2d_backprop_filter' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  if not isinstance(rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'rates' argument to "
        "'dilation2d_backprop_filter' Op, not %r." % rates)
  rates = [_execute.make_int(_i, "rates") for _i in rates]
  padding = _execute.make_str(padding, "padding")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Dilation2DBackpropFilter", input=input, filter=filter,
                                    out_backprop=out_backprop,
                                    strides=strides, rates=rates,
                                    padding=padding, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "rates", _op.get_attr("rates"), "padding",
            _op.get_attr("padding"))
  _execute.record_gradient(
      "Dilation2DBackpropFilter", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def dilation2d_backprop_filter_eager_fallback(input, filter, out_backprop, strides, rates, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dilation2d_backprop_filter
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'dilation2d_backprop_filter' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  if not isinstance(rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'rates' argument to "
        "'dilation2d_backprop_filter' Op, not %r." % rates)
  rates = [_execute.make_int(_i, "rates") for _i in rates]
  padding = _execute.make_str(padding, "padding")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter, out_backprop], _ctx)
  (input, filter, out_backprop) = _inputs_T
  _inputs_flat = [input, filter, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "rates", rates, "padding",
  padding)
  _result = _execute.execute(b"Dilation2DBackpropFilter", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "Dilation2DBackpropFilter", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def dilation2d_backprop_input(input, filter, out_backprop, strides, rates, padding, name=None):
  r"""Computes the gradient of morphological 2-D dilation with respect to the input.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      4-D with shape `[batch, in_height, in_width, depth]`.
    filter: A `Tensor`. Must have the same type as `input`.
      3-D with shape `[filter_height, filter_width, depth]`.
    out_backprop: A `Tensor`. Must have the same type as `input`.
      4-D with shape `[batch, out_height, out_width, depth]`.
    strides: A list of `ints` that has length `>= 4`.
      1-D of length 4. The stride of the sliding window for each dimension of
      the input tensor. Must be: `[1, stride_height, stride_width, 1]`.
    rates: A list of `ints` that has length `>= 4`.
      1-D of length 4. The input stride for atrous morphological dilation.
      Must be: `[1, rate_height, rate_width, 1]`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "Dilation2DBackpropInput", name, _ctx._post_execution_callbacks,
        input, filter, out_backprop, "strides", strides, "rates", rates,
        "padding", padding)
      return _result
    except _core._FallbackException:
      try:
        return dilation2d_backprop_input_eager_fallback(
            input, filter, out_backprop, strides=strides, rates=rates,
            padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'dilation2d_backprop_input' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  if not isinstance(rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'rates' argument to "
        "'dilation2d_backprop_input' Op, not %r." % rates)
  rates = [_execute.make_int(_i, "rates") for _i in rates]
  padding = _execute.make_str(padding, "padding")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Dilation2DBackpropInput", input=input, filter=filter,
                                   out_backprop=out_backprop, strides=strides,
                                   rates=rates, padding=padding, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "strides", _op.get_attr("strides"),
            "rates", _op.get_attr("rates"), "padding",
            _op.get_attr("padding"))
  _execute.record_gradient(
      "Dilation2DBackpropInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def dilation2d_backprop_input_eager_fallback(input, filter, out_backprop, strides, rates, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dilation2d_backprop_input
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'dilation2d_backprop_input' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  if not isinstance(rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'rates' argument to "
        "'dilation2d_backprop_input' Op, not %r." % rates)
  rates = [_execute.make_int(_i, "rates") for _i in rates]
  padding = _execute.make_str(padding, "padding")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter, out_backprop], _ctx)
  (input, filter, out_backprop) = _inputs_T
  _inputs_flat = [input, filter, out_backprop]
  _attrs = ("T", _attr_T, "strides", strides, "rates", rates, "padding",
  padding)
  _result = _execute.execute(b"Dilation2DBackpropInput", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "Dilation2DBackpropInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.elu')
def elu(features, name=None):
  r"""Computes exponential linear: `exp(features) - 1` if < 0, `features` otherwise.

  See [Fast and Accurate Deep Network Learning by Exponential Linear Units (ELUs)
  ](http://arxiv.org/abs/1511.07289)

  Args:
    features: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `features`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Elu", name,
        _ctx._post_execution_callbacks, features)
      return _result
    except _core._FallbackException:
      try:
        return elu_eager_fallback(
            features, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              elu, features=features, name=name)
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
        "Elu", features=features, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          elu, features=features, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Elu", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def elu_eager_fallback(features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function elu
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (features,) = _execute.args_to_matching_eager([features], _ctx)
  _inputs_flat = [features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Elu", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Elu", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def elu_grad(gradients, outputs, name=None):
  r"""Computes gradients for the exponential linear (Elu) operation.

  Args:
    gradients: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      The backpropagated gradients to the corresponding Elu operation.
    outputs: A `Tensor`. Must have the same type as `gradients`.
      The outputs of the corresponding Elu operation.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `gradients`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EluGrad",
        name, _ctx._post_execution_callbacks, gradients, outputs)
      return _result
    except _core._FallbackException:
      try:
        return elu_grad_eager_fallback(
            gradients, outputs, name=name, ctx=_ctx)
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
        "EluGrad", gradients=gradients, outputs=outputs, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "EluGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def elu_grad_eager_fallback(gradients, outputs, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function elu_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([gradients, outputs], _ctx)
  (gradients, outputs) = _inputs_T
  _inputs_flat = [gradients, outputs]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"EluGrad", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "EluGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_fractional_avg_pool_outputs = ["output", "row_pooling_sequence",
                               "col_pooling_sequence"]
_FractionalAvgPoolOutput = _collections.namedtuple(
    "FractionalAvgPool", _fractional_avg_pool_outputs)


def fractional_avg_pool(value, pooling_ratio, pseudo_random=False, overlapping=False, deterministic=False, seed=0, seed2=0, name=None):
  r"""Performs fractional average pooling on the input.

  Fractional average pooling is similar to Fractional max pooling in the pooling
  region generation step. The only difference is that after pooling regions are
  generated, a mean operation is performed instead of a max operation in each
  pooling region.

  Args:
    value: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `int64`.
      4-D with shape `[batch, height, width, channels]`.
    pooling_ratio: A list of `floats` that has length `>= 4`.
      Pooling ratio for each dimension of `value`, currently only
      supports row and col dimension and should be >= 1.0. For example, a valid
      pooling ratio looks like [1.0, 1.44, 1.73, 1.0]. The first and last elements
      must be 1.0 because we don't allow pooling on batch and channels
      dimensions. 1.44 and 1.73 are pooling ratio on height and width dimensions
      respectively.
    pseudo_random: An optional `bool`. Defaults to `False`.
      When set to True, generates the pooling sequence in a
      pseudorandom fashion, otherwise, in a random fashion. Check paper [Benjamin
      Graham, Fractional Max-Pooling](http://arxiv.org/abs/1412.6071) for
      difference between pseudorandom and random.
    overlapping: An optional `bool`. Defaults to `False`.
      When set to True, it means when pooling, the values at the boundary
      of adjacent pooling cells are used by both cells. For example:

      `index  0  1  2  3  4`

      `value  20 5  16 3  7`

      If the pooling sequence is [0, 2, 4], then 16, at index 2 will be used twice.
      The result would be [41/3, 26/3] for fractional avg pooling.
    deterministic: An optional `bool`. Defaults to `False`.
      When set to True, a fixed pooling region will be used when
      iterating over a FractionalAvgPool node in the computation graph. Mainly used
      in unit test to make FractionalAvgPool deterministic.
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, row_pooling_sequence, col_pooling_sequence).

    output: A `Tensor`. Has the same type as `value`.
    row_pooling_sequence: A `Tensor` of type `int64`.
    col_pooling_sequence: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FractionalAvgPool", name, _ctx._post_execution_callbacks, value,
        "pooling_ratio", pooling_ratio, "pseudo_random", pseudo_random,
        "overlapping", overlapping, "deterministic", deterministic, "seed",
        seed, "seed2", seed2)
      _result = _FractionalAvgPoolOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return fractional_avg_pool_eager_fallback(
            value, pooling_ratio=pooling_ratio, pseudo_random=pseudo_random,
            overlapping=overlapping, deterministic=deterministic, seed=seed,
            seed2=seed2, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(pooling_ratio, (list, tuple)):
    raise TypeError(
        "Expected list for 'pooling_ratio' argument to "
        "'fractional_avg_pool' Op, not %r." % pooling_ratio)
  pooling_ratio = [_execute.make_float(_f, "pooling_ratio") for _f in pooling_ratio]
  if pseudo_random is None:
    pseudo_random = False
  pseudo_random = _execute.make_bool(pseudo_random, "pseudo_random")
  if overlapping is None:
    overlapping = False
  overlapping = _execute.make_bool(overlapping, "overlapping")
  if deterministic is None:
    deterministic = False
  deterministic = _execute.make_bool(deterministic, "deterministic")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FractionalAvgPool", value=value, pooling_ratio=pooling_ratio,
                             pseudo_random=pseudo_random,
                             overlapping=overlapping,
                             deterministic=deterministic, seed=seed,
                             seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("pooling_ratio", _op.get_attr("pooling_ratio"), "pseudo_random",
            _op.get_attr("pseudo_random"), "overlapping",
            _op.get_attr("overlapping"), "deterministic",
            _op.get_attr("deterministic"), "seed", _op.get_attr("seed"),
            "seed2", _op.get_attr("seed2"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "FractionalAvgPool", _inputs_flat, _attrs, _result, name)
  _result = _FractionalAvgPoolOutput._make(_result)
  return _result



def fractional_avg_pool_eager_fallback(value, pooling_ratio, pseudo_random=False, overlapping=False, deterministic=False, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fractional_avg_pool
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(pooling_ratio, (list, tuple)):
    raise TypeError(
        "Expected list for 'pooling_ratio' argument to "
        "'fractional_avg_pool' Op, not %r." % pooling_ratio)
  pooling_ratio = [_execute.make_float(_f, "pooling_ratio") for _f in pooling_ratio]
  if pseudo_random is None:
    pseudo_random = False
  pseudo_random = _execute.make_bool(pseudo_random, "pseudo_random")
  if overlapping is None:
    overlapping = False
  overlapping = _execute.make_bool(overlapping, "overlapping")
  if deterministic is None:
    deterministic = False
  deterministic = _execute.make_bool(deterministic, "deterministic")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _inputs_flat = [value]
  _attrs = ("pooling_ratio", pooling_ratio, "pseudo_random", pseudo_random,
  "overlapping", overlapping, "deterministic", deterministic, "seed", seed,
  "seed2", seed2, "T", _attr_T)
  _result = _execute.execute(b"FractionalAvgPool", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FractionalAvgPool", _inputs_flat, _attrs, _result, name)
  _result = _FractionalAvgPoolOutput._make(_result)
  return _result


def fractional_avg_pool_grad(orig_input_tensor_shape, out_backprop, row_pooling_sequence, col_pooling_sequence, overlapping=False, name=None):
  r"""Computes gradient of the FractionalAvgPool function.

  Unlike FractionalMaxPoolGrad, we don't need to find arg_max for
  FractionalAvgPoolGrad, we just need to evenly back-propagate each element of
  out_backprop to those indices that form the same pooling cell. Therefore, we
  just need to know the shape of original input tensor, instead of the whole
  tensor.

  Args:
    orig_input_tensor_shape: A `Tensor` of type `int64`.
      Original input tensor shape for `fractional_avg_pool`
    out_backprop: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `int64`.
      4-D with shape `[batch, height, width, channels]`.  Gradients
      w.r.t. the output of `fractional_avg_pool`.
    row_pooling_sequence: A `Tensor` of type `int64`.
      row pooling sequence, form pooling region with
      col_pooling_sequence.
    col_pooling_sequence: A `Tensor` of type `int64`.
      column pooling sequence, form pooling region with
      row_pooling sequence.
    overlapping: An optional `bool`. Defaults to `False`.
      When set to True, it means when pooling, the values at the boundary
      of adjacent pooling cells are used by both cells. For example:

      `index  0  1  2  3  4`

      `value  20 5  16 3  7`

      If the pooling sequence is [0, 2, 4], then 16, at index 2 will be used twice.
      The result would be [41/3, 26/3] for fractional avg pooling.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `out_backprop`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FractionalAvgPoolGrad", name, _ctx._post_execution_callbacks,
        orig_input_tensor_shape, out_backprop, row_pooling_sequence,
        col_pooling_sequence, "overlapping", overlapping)
      return _result
    except _core._FallbackException:
      try:
        return fractional_avg_pool_grad_eager_fallback(
            orig_input_tensor_shape, out_backprop, row_pooling_sequence,
            col_pooling_sequence, overlapping=overlapping, name=name,
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
  if overlapping is None:
    overlapping = False
  overlapping = _execute.make_bool(overlapping, "overlapping")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FractionalAvgPoolGrad", orig_input_tensor_shape=orig_input_tensor_shape,
                                 out_backprop=out_backprop,
                                 row_pooling_sequence=row_pooling_sequence,
                                 col_pooling_sequence=col_pooling_sequence,
                                 overlapping=overlapping, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("overlapping", _op.get_attr("overlapping"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "FractionalAvgPoolGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fractional_avg_pool_grad_eager_fallback(orig_input_tensor_shape, out_backprop, row_pooling_sequence, col_pooling_sequence, overlapping=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fractional_avg_pool_grad
  """
  _ctx = ctx if ctx else _context.context()
  if overlapping is None:
    overlapping = False
  overlapping = _execute.make_bool(overlapping, "overlapping")
  _attr_T, (out_backprop,) = _execute.args_to_matching_eager([out_backprop], _ctx)
  orig_input_tensor_shape = _ops.convert_to_tensor(orig_input_tensor_shape, _dtypes.int64)
  row_pooling_sequence = _ops.convert_to_tensor(row_pooling_sequence, _dtypes.int64)
  col_pooling_sequence = _ops.convert_to_tensor(col_pooling_sequence, _dtypes.int64)
  _inputs_flat = [orig_input_tensor_shape, out_backprop, row_pooling_sequence, col_pooling_sequence]
  _attrs = ("overlapping", overlapping, "T", _attr_T)
  _result = _execute.execute(b"FractionalAvgPoolGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FractionalAvgPoolGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_fractional_max_pool_outputs = ["output", "row_pooling_sequence",
                               "col_pooling_sequence"]
_FractionalMaxPoolOutput = _collections.namedtuple(
    "FractionalMaxPool", _fractional_max_pool_outputs)


def fractional_max_pool(value, pooling_ratio, pseudo_random=False, overlapping=False, deterministic=False, seed=0, seed2=0, name=None):
  r"""Performs fractional max pooling on the input.

  Fractional max pooling is slightly different than regular max pooling.  In
  regular max pooling, you downsize an input set by taking the maximum value of
  smaller N x N subsections of the set (often 2x2), and try to reduce the set by
  a factor of N, where N is an integer.  Fractional max pooling, as you might
  expect from the word "fractional", means that the overall reduction ratio N
  does not have to be an integer.

  The sizes of the pooling regions are generated randomly but are fairly uniform.
  For example, let's look at the height dimension, and the constraints on the
  list of rows that will be pool boundaries.

  First we define the following:

  1.  input_row_length : the number of rows from the input set
  2.  output_row_length : which will be smaller than the input
  3.  alpha = input_row_length / output_row_length : our reduction ratio
  4.  K = floor(alpha)
  5.  row_pooling_sequence : this is the result list of pool boundary rows

  Then, row_pooling_sequence should satisfy:

  1.  a[0] = 0 : the first value of the sequence is 0
  2.  a[end] = input_row_length : the last value of the sequence is the size
  3.  K <= (a[i+1] - a[i]) <= K+1 : all intervals are K or K+1 size
  4.  length(row_pooling_sequence) = output_row_length+1

  For more details on fractional max pooling, see this paper:
  [Benjamin Graham, Fractional Max-Pooling](http://arxiv.org/abs/1412.6071)

  Args:
    value: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `int64`.
      4-D with shape `[batch, height, width, channels]`.
    pooling_ratio: A list of `floats` that has length `>= 4`.
      Pooling ratio for each dimension of `value`, currently only
      supports row and col dimension and should be >= 1.0. For example, a valid
      pooling ratio looks like [1.0, 1.44, 1.73, 1.0]. The first and last elements
      must be 1.0 because we don't allow pooling on batch and channels
      dimensions. 1.44 and 1.73 are pooling ratio on height and width dimensions
      respectively.
    pseudo_random: An optional `bool`. Defaults to `False`.
      When set to True, generates the pooling sequence in a
      pseudorandom fashion, otherwise, in a random fashion. Check paper [Benjamin
      Graham, Fractional Max-Pooling](http://arxiv.org/abs/1412.6071) for
      difference between pseudorandom and random.
    overlapping: An optional `bool`. Defaults to `False`.
      When set to True, it means when pooling, the values at the boundary
      of adjacent pooling cells are used by both cells. For example:

      `index  0  1  2  3  4`

      `value  20 5  16 3  7`

      If the pooling sequence is [0, 2, 4], then 16, at index 2 will be used twice.
      The result would be [20, 16] for fractional max pooling.
    deterministic: An optional `bool`. Defaults to `False`.
      When set to True, a fixed pooling region will be used when
      iterating over a FractionalMaxPool node in the computation graph. Mainly used
      in unit test to make FractionalMaxPool deterministic.
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, row_pooling_sequence, col_pooling_sequence).

    output: A `Tensor`. Has the same type as `value`.
    row_pooling_sequence: A `Tensor` of type `int64`.
    col_pooling_sequence: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FractionalMaxPool", name, _ctx._post_execution_callbacks, value,
        "pooling_ratio", pooling_ratio, "pseudo_random", pseudo_random,
        "overlapping", overlapping, "deterministic", deterministic, "seed",
        seed, "seed2", seed2)
      _result = _FractionalMaxPoolOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return fractional_max_pool_eager_fallback(
            value, pooling_ratio=pooling_ratio, pseudo_random=pseudo_random,
            overlapping=overlapping, deterministic=deterministic, seed=seed,
            seed2=seed2, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(pooling_ratio, (list, tuple)):
    raise TypeError(
        "Expected list for 'pooling_ratio' argument to "
        "'fractional_max_pool' Op, not %r." % pooling_ratio)
  pooling_ratio = [_execute.make_float(_f, "pooling_ratio") for _f in pooling_ratio]
  if pseudo_random is None:
    pseudo_random = False
  pseudo_random = _execute.make_bool(pseudo_random, "pseudo_random")
  if overlapping is None:
    overlapping = False
  overlapping = _execute.make_bool(overlapping, "overlapping")
  if deterministic is None:
    deterministic = False
  deterministic = _execute.make_bool(deterministic, "deterministic")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FractionalMaxPool", value=value, pooling_ratio=pooling_ratio,
                             pseudo_random=pseudo_random,
                             overlapping=overlapping,
                             deterministic=deterministic, seed=seed,
                             seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("pooling_ratio", _op.get_attr("pooling_ratio"), "pseudo_random",
            _op.get_attr("pseudo_random"), "overlapping",
            _op.get_attr("overlapping"), "deterministic",
            _op.get_attr("deterministic"), "seed", _op.get_attr("seed"),
            "seed2", _op.get_attr("seed2"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "FractionalMaxPool", _inputs_flat, _attrs, _result, name)
  _result = _FractionalMaxPoolOutput._make(_result)
  return _result



def fractional_max_pool_eager_fallback(value, pooling_ratio, pseudo_random=False, overlapping=False, deterministic=False, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fractional_max_pool
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(pooling_ratio, (list, tuple)):
    raise TypeError(
        "Expected list for 'pooling_ratio' argument to "
        "'fractional_max_pool' Op, not %r." % pooling_ratio)
  pooling_ratio = [_execute.make_float(_f, "pooling_ratio") for _f in pooling_ratio]
  if pseudo_random is None:
    pseudo_random = False
  pseudo_random = _execute.make_bool(pseudo_random, "pseudo_random")
  if overlapping is None:
    overlapping = False
  overlapping = _execute.make_bool(overlapping, "overlapping")
  if deterministic is None:
    deterministic = False
  deterministic = _execute.make_bool(deterministic, "deterministic")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _inputs_flat = [value]
  _attrs = ("pooling_ratio", pooling_ratio, "pseudo_random", pseudo_random,
  "overlapping", overlapping, "deterministic", deterministic, "seed", seed,
  "seed2", seed2, "T", _attr_T)
  _result = _execute.execute(b"FractionalMaxPool", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FractionalMaxPool", _inputs_flat, _attrs, _result, name)
  _result = _FractionalMaxPoolOutput._make(_result)
  return _result


def fractional_max_pool_grad(orig_input, orig_output, out_backprop, row_pooling_sequence, col_pooling_sequence, overlapping=False, name=None):
  r"""Computes gradient of the FractionalMaxPool function.

  Args:
    orig_input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `int64`.
      Original input for `fractional_max_pool`
    orig_output: A `Tensor`. Must have the same type as `orig_input`.
      Original output for `fractional_max_pool`
    out_backprop: A `Tensor`. Must have the same type as `orig_input`.
      4-D with shape `[batch, height, width, channels]`.  Gradients
      w.r.t. the output of `fractional_max_pool`.
    row_pooling_sequence: A `Tensor` of type `int64`.
      row pooling sequence, form pooling region with
      col_pooling_sequence.
    col_pooling_sequence: A `Tensor` of type `int64`.
      column pooling sequence, form pooling region with
      row_pooling sequence.
    overlapping: An optional `bool`. Defaults to `False`.
      When set to True, it means when pooling, the values at the boundary
      of adjacent pooling cells are used by both cells. For example:

      `index  0  1  2  3  4`

      `value  20 5  16 3  7`

      If the pooling sequence is [0, 2, 4], then 16, at index 2 will be used twice.
      The result would be [20, 16] for fractional max pooling.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `orig_input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FractionalMaxPoolGrad", name, _ctx._post_execution_callbacks,
        orig_input, orig_output, out_backprop, row_pooling_sequence,
        col_pooling_sequence, "overlapping", overlapping)
      return _result
    except _core._FallbackException:
      try:
        return fractional_max_pool_grad_eager_fallback(
            orig_input, orig_output, out_backprop, row_pooling_sequence,
            col_pooling_sequence, overlapping=overlapping, name=name,
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
  if overlapping is None:
    overlapping = False
  overlapping = _execute.make_bool(overlapping, "overlapping")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FractionalMaxPoolGrad", orig_input=orig_input,
                                 orig_output=orig_output,
                                 out_backprop=out_backprop,
                                 row_pooling_sequence=row_pooling_sequence,
                                 col_pooling_sequence=col_pooling_sequence,
                                 overlapping=overlapping, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("overlapping", _op.get_attr("overlapping"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "FractionalMaxPoolGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fractional_max_pool_grad_eager_fallback(orig_input, orig_output, out_backprop, row_pooling_sequence, col_pooling_sequence, overlapping=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fractional_max_pool_grad
  """
  _ctx = ctx if ctx else _context.context()
  if overlapping is None:
    overlapping = False
  overlapping = _execute.make_bool(overlapping, "overlapping")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([orig_input, orig_output, out_backprop], _ctx)
  (orig_input, orig_output, out_backprop) = _inputs_T
  row_pooling_sequence = _ops.convert_to_tensor(row_pooling_sequence, _dtypes.int64)
  col_pooling_sequence = _ops.convert_to_tensor(col_pooling_sequence, _dtypes.int64)
  _inputs_flat = [orig_input, orig_output, out_backprop, row_pooling_sequence, col_pooling_sequence]
  _attrs = ("overlapping", overlapping, "T", _attr_T)
  _result = _execute.execute(b"FractionalMaxPoolGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FractionalMaxPoolGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


__fused_batch_norm_outputs = ["y", "batch_mean", "batch_variance",
                             "reserve_space_1", "reserve_space_2"]
_FusedBatchNormOutput = _collections.namedtuple(
    "FusedBatchNorm", __fused_batch_norm_outputs)


def _fused_batch_norm(x, scale, offset, mean, variance, epsilon=0.0001, data_format="NHWC", is_training=True, name=None):
  r"""Batch normalization.

  Note that the size of 4D Tensors are defined by either "NHWC" or "NCHW".
  The size of 1D Tensors matches the dimension C of the 4D Tensors.

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`.
      A 4D Tensor for input data.
    scale: A `Tensor`. Must have the same type as `x`.
      A 1D Tensor for scaling factor, to scale the normalized x.
    offset: A `Tensor`. Must have the same type as `x`.
      A 1D Tensor for offset, to shift to the normalized x.
    mean: A `Tensor`. Must have the same type as `x`.
      A 1D Tensor for population mean. Used for inference only;
      must be empty for training.
    variance: A `Tensor`. Must have the same type as `x`.
      A 1D Tensor for population variance. Used for inference only;
      must be empty for training.
    epsilon: An optional `float`. Defaults to `0.0001`.
      A small float number added to the variance of x.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      The data format for x and y. Either "NHWC" (default) or "NCHW".
    is_training: An optional `bool`. Defaults to `True`.
      A bool value to indicate the operation is for training (default)
      or inference.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (y, batch_mean, batch_variance, reserve_space_1, reserve_space_2).

    y: A `Tensor`. Has the same type as `x`.
    batch_mean: A `Tensor`. Has the same type as `x`.
    batch_variance: A `Tensor`. Has the same type as `x`.
    reserve_space_1: A `Tensor`. Has the same type as `x`.
    reserve_space_2: A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FusedBatchNorm", name, _ctx._post_execution_callbacks, x, scale,
        offset, mean, variance, "epsilon", epsilon, "data_format",
        data_format, "is_training", is_training)
      _result = _FusedBatchNormOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return _fused_batch_norm_eager_fallback(
            x, scale, offset, mean, variance, epsilon=epsilon,
            data_format=data_format, is_training=is_training, name=name,
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
  if epsilon is None:
    epsilon = 0.0001
  epsilon = _execute.make_float(epsilon, "epsilon")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if is_training is None:
    is_training = True
  is_training = _execute.make_bool(is_training, "is_training")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FusedBatchNorm", x=x, scale=scale, offset=offset, mean=mean,
                          variance=variance, epsilon=epsilon,
                          data_format=data_format, is_training=is_training,
                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "epsilon", _op.get_attr("epsilon"),
            "data_format", _op.get_attr("data_format"), "is_training",
            _op.get_attr("is_training"))
  _execute.record_gradient(
      "FusedBatchNorm", _inputs_flat, _attrs, _result, name)
  _result = _FusedBatchNormOutput._make(_result)
  return _result



def _fused_batch_norm_eager_fallback(x, scale, offset, mean, variance, epsilon=0.0001, data_format="NHWC", is_training=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _fused_batch_norm
  """
  _ctx = ctx if ctx else _context.context()
  if epsilon is None:
    epsilon = 0.0001
  epsilon = _execute.make_float(epsilon, "epsilon")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if is_training is None:
    is_training = True
  is_training = _execute.make_bool(is_training, "is_training")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, scale, offset, mean, variance], _ctx)
  (x, scale, offset, mean, variance) = _inputs_T
  _inputs_flat = [x, scale, offset, mean, variance]
  _attrs = ("T", _attr_T, "epsilon", epsilon, "data_format", data_format,
  "is_training", is_training)
  _result = _execute.execute(b"FusedBatchNorm", 5, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FusedBatchNorm", _inputs_flat, _attrs, _result, name)
  _result = _FusedBatchNormOutput._make(_result)
  return _result


_fused_batch_norm_grad_outputs = ["x_backprop", "scale_backprop",
                                 "offset_backprop", "reserve_space_3",
                                 "reserve_space_4"]
_FusedBatchNormGradOutput = _collections.namedtuple(
    "FusedBatchNormGrad", _fused_batch_norm_grad_outputs)


def fused_batch_norm_grad(y_backprop, x, scale, reserve_space_1, reserve_space_2, epsilon=0.0001, data_format="NHWC", is_training=True, name=None):
  r"""Gradient for batch normalization.

  Note that the size of 4D Tensors are defined by either "NHWC" or "NCHW".
  The size of 1D Tensors matches the dimension C of the 4D Tensors.

  Args:
    y_backprop: A `Tensor`. Must be one of the following types: `float32`.
      A 4D Tensor for the gradient with respect to y.
    x: A `Tensor`. Must have the same type as `y_backprop`.
      A 4D Tensor for input data.
    scale: A `Tensor`. Must have the same type as `y_backprop`.
      A 1D Tensor for scaling factor, to scale the normalized x.
    reserve_space_1: A `Tensor`. Must have the same type as `y_backprop`.
      When is_training is True, a 1D Tensor for the computed batch
      mean to be reused in gradient computation. When is_training is
      False, a 1D Tensor for the population mean to be reused in both
      1st and 2nd order gradient computation.
    reserve_space_2: A `Tensor`. Must have the same type as `y_backprop`.
      When is_training is True, a 1D Tensor for the computed batch
      variance (inverted variance in the cuDNN case) to be reused in
      gradient computation. When is_training is False, a 1D Tensor
      for the population variance to be reused in both 1st and 2nd
      order gradient computation.
    epsilon: An optional `float`. Defaults to `0.0001`.
      A small float number added to the variance of x.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      The data format for y_backprop, x, x_backprop.
      Either "NHWC" (default) or "NCHW".
    is_training: An optional `bool`. Defaults to `True`.
      A bool value to indicate the operation is for training (default)
      or inference.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (x_backprop, scale_backprop, offset_backprop, reserve_space_3, reserve_space_4).

    x_backprop: A `Tensor`. Has the same type as `y_backprop`.
    scale_backprop: A `Tensor`. Has the same type as `y_backprop`.
    offset_backprop: A `Tensor`. Has the same type as `y_backprop`.
    reserve_space_3: A `Tensor`. Has the same type as `y_backprop`.
    reserve_space_4: A `Tensor`. Has the same type as `y_backprop`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FusedBatchNormGrad", name, _ctx._post_execution_callbacks,
        y_backprop, x, scale, reserve_space_1, reserve_space_2, "epsilon",
        epsilon, "data_format", data_format, "is_training", is_training)
      _result = _FusedBatchNormGradOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return fused_batch_norm_grad_eager_fallback(
            y_backprop, x, scale, reserve_space_1, reserve_space_2,
            epsilon=epsilon, data_format=data_format, is_training=is_training,
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
  if epsilon is None:
    epsilon = 0.0001
  epsilon = _execute.make_float(epsilon, "epsilon")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if is_training is None:
    is_training = True
  is_training = _execute.make_bool(is_training, "is_training")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FusedBatchNormGrad", y_backprop=y_backprop, x=x, scale=scale,
                              reserve_space_1=reserve_space_1,
                              reserve_space_2=reserve_space_2,
                              epsilon=epsilon, data_format=data_format,
                              is_training=is_training, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "epsilon", _op.get_attr("epsilon"),
            "data_format", _op.get_attr("data_format"), "is_training",
            _op.get_attr("is_training"))
  _execute.record_gradient(
      "FusedBatchNormGrad", _inputs_flat, _attrs, _result, name)
  _result = _FusedBatchNormGradOutput._make(_result)
  return _result



def fused_batch_norm_grad_eager_fallback(y_backprop, x, scale, reserve_space_1, reserve_space_2, epsilon=0.0001, data_format="NHWC", is_training=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fused_batch_norm_grad
  """
  _ctx = ctx if ctx else _context.context()
  if epsilon is None:
    epsilon = 0.0001
  epsilon = _execute.make_float(epsilon, "epsilon")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if is_training is None:
    is_training = True
  is_training = _execute.make_bool(is_training, "is_training")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([y_backprop, x, scale, reserve_space_1, reserve_space_2], _ctx)
  (y_backprop, x, scale, reserve_space_1, reserve_space_2) = _inputs_T
  _inputs_flat = [y_backprop, x, scale, reserve_space_1, reserve_space_2]
  _attrs = ("T", _attr_T, "epsilon", epsilon, "data_format", data_format,
  "is_training", is_training)
  _result = _execute.execute(b"FusedBatchNormGrad", 5, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FusedBatchNormGrad", _inputs_flat, _attrs, _result, name)
  _result = _FusedBatchNormGradOutput._make(_result)
  return _result


_fused_batch_norm_grad_v2_outputs = ["x_backprop", "scale_backprop",
                                    "offset_backprop", "reserve_space_3",
                                    "reserve_space_4"]
_FusedBatchNormGradV2Output = _collections.namedtuple(
    "FusedBatchNormGradV2", _fused_batch_norm_grad_v2_outputs)


def fused_batch_norm_grad_v2(y_backprop, x, scale, reserve_space_1, reserve_space_2, epsilon=0.0001, data_format="NHWC", is_training=True, name=None):
  r"""Gradient for batch normalization.

  Note that the size of 4D Tensors are defined by either "NHWC" or "NCHW".
  The size of 1D Tensors matches the dimension C of the 4D Tensors.

  Args:
    y_backprop: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`.
      A 4D Tensor for the gradient with respect to y.
    x: A `Tensor`. Must have the same type as `y_backprop`.
      A 4D Tensor for input data.
    scale: A `Tensor` of type `float32`.
      A 1D Tensor for scaling factor, to scale the normalized x.
    reserve_space_1: A `Tensor`. Must be one of the following types: `float32`.
      When is_training is True, a 1D Tensor for the computed batch
      mean to be reused in gradient computation. When is_training is
      False, a 1D Tensor for the population mean to be reused in both
      1st and 2nd order gradient computation.
    reserve_space_2: A `Tensor`. Must have the same type as `reserve_space_1`.
      When is_training is True, a 1D Tensor for the computed batch
      variance (inverted variance in the cuDNN case) to be reused in
      gradient computation. When is_training is False, a 1D Tensor
      for the population variance to be reused in both 1st and 2nd
      order gradient computation.
    epsilon: An optional `float`. Defaults to `0.0001`.
      A small float number added to the variance of x.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      The data format for y_backprop, x, x_backprop.
      Either "NHWC" (default) or "NCHW".
    is_training: An optional `bool`. Defaults to `True`.
      A bool value to indicate the operation is for training (default)
      or inference.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (x_backprop, scale_backprop, offset_backprop, reserve_space_3, reserve_space_4).

    x_backprop: A `Tensor`. Has the same type as `y_backprop`.
    scale_backprop: A `Tensor`. Has the same type as `reserve_space_1`.
    offset_backprop: A `Tensor`. Has the same type as `reserve_space_1`.
    reserve_space_3: A `Tensor`. Has the same type as `reserve_space_1`.
    reserve_space_4: A `Tensor`. Has the same type as `reserve_space_1`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FusedBatchNormGradV2", name, _ctx._post_execution_callbacks,
        y_backprop, x, scale, reserve_space_1, reserve_space_2, "epsilon",
        epsilon, "data_format", data_format, "is_training", is_training)
      _result = _FusedBatchNormGradV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return fused_batch_norm_grad_v2_eager_fallback(
            y_backprop, x, scale, reserve_space_1, reserve_space_2,
            epsilon=epsilon, data_format=data_format, is_training=is_training,
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
  if epsilon is None:
    epsilon = 0.0001
  epsilon = _execute.make_float(epsilon, "epsilon")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if is_training is None:
    is_training = True
  is_training = _execute.make_bool(is_training, "is_training")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FusedBatchNormGradV2", y_backprop=y_backprop, x=x, scale=scale,
                                reserve_space_1=reserve_space_1,
                                reserve_space_2=reserve_space_2,
                                epsilon=epsilon, data_format=data_format,
                                is_training=is_training, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "U", _op.get_attr("U"), "epsilon",
            _op.get_attr("epsilon"), "data_format",
            _op.get_attr("data_format"), "is_training",
            _op.get_attr("is_training"))
  _execute.record_gradient(
      "FusedBatchNormGradV2", _inputs_flat, _attrs, _result, name)
  _result = _FusedBatchNormGradV2Output._make(_result)
  return _result



def fused_batch_norm_grad_v2_eager_fallback(y_backprop, x, scale, reserve_space_1, reserve_space_2, epsilon=0.0001, data_format="NHWC", is_training=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fused_batch_norm_grad_v2
  """
  _ctx = ctx if ctx else _context.context()
  if epsilon is None:
    epsilon = 0.0001
  epsilon = _execute.make_float(epsilon, "epsilon")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if is_training is None:
    is_training = True
  is_training = _execute.make_bool(is_training, "is_training")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([y_backprop, x], _ctx)
  (y_backprop, x) = _inputs_T
  _attr_U, _inputs_U = _execute.args_to_matching_eager([reserve_space_1, reserve_space_2], _ctx)
  (reserve_space_1, reserve_space_2) = _inputs_U
  scale = _ops.convert_to_tensor(scale, _dtypes.float32)
  _inputs_flat = [y_backprop, x, scale, reserve_space_1, reserve_space_2]
  _attrs = ("T", _attr_T, "U", _attr_U, "epsilon", epsilon, "data_format",
  data_format, "is_training", is_training)
  _result = _execute.execute(b"FusedBatchNormGradV2", 5, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FusedBatchNormGradV2", _inputs_flat, _attrs, _result, name)
  _result = _FusedBatchNormGradV2Output._make(_result)
  return _result


_fused_batch_norm_v2_outputs = ["y", "batch_mean", "batch_variance",
                               "reserve_space_1", "reserve_space_2"]
_FusedBatchNormV2Output = _collections.namedtuple(
    "FusedBatchNormV2", _fused_batch_norm_v2_outputs)


def fused_batch_norm_v2(x, scale, offset, mean, variance, epsilon=0.0001, data_format="NHWC", is_training=True, name=None):
  r"""Batch normalization.

  Note that the size of 4D Tensors are defined by either "NHWC" or "NCHW".
  The size of 1D Tensors matches the dimension C of the 4D Tensors.

  Args:
    x: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`.
      A 4D Tensor for input data.
    scale: A `Tensor`. Must be one of the following types: `float32`.
      A 1D Tensor for scaling factor, to scale the normalized x.
    offset: A `Tensor`. Must have the same type as `scale`.
      A 1D Tensor for offset, to shift to the normalized x.
    mean: A `Tensor`. Must have the same type as `scale`.
      A 1D Tensor for population mean. Used for inference only;
      must be empty for training.
    variance: A `Tensor`. Must have the same type as `scale`.
      A 1D Tensor for population variance. Used for inference only;
      must be empty for training.
    epsilon: An optional `float`. Defaults to `0.0001`.
      A small float number added to the variance of x.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      The data format for x and y. Either "NHWC" (default) or "NCHW".
    is_training: An optional `bool`. Defaults to `True`.
      A bool value to indicate the operation is for training (default)
      or inference.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (y, batch_mean, batch_variance, reserve_space_1, reserve_space_2).

    y: A `Tensor`. Has the same type as `x`.
    batch_mean: A `Tensor`. Has the same type as `scale`.
    batch_variance: A `Tensor`. Has the same type as `scale`.
    reserve_space_1: A `Tensor`. Has the same type as `scale`.
    reserve_space_2: A `Tensor`. Has the same type as `scale`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FusedBatchNormV2", name, _ctx._post_execution_callbacks, x, scale,
        offset, mean, variance, "epsilon", epsilon, "data_format",
        data_format, "is_training", is_training)
      _result = _FusedBatchNormV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return fused_batch_norm_v2_eager_fallback(
            x, scale, offset, mean, variance, epsilon=epsilon,
            data_format=data_format, is_training=is_training, name=name,
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
  if epsilon is None:
    epsilon = 0.0001
  epsilon = _execute.make_float(epsilon, "epsilon")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if is_training is None:
    is_training = True
  is_training = _execute.make_bool(is_training, "is_training")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FusedBatchNormV2", x=x, scale=scale, offset=offset, mean=mean,
                            variance=variance, epsilon=epsilon,
                            data_format=data_format, is_training=is_training,
                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "U", _op.get_attr("U"), "epsilon",
            _op.get_attr("epsilon"), "data_format",
            _op.get_attr("data_format"), "is_training",
            _op.get_attr("is_training"))
  _execute.record_gradient(
      "FusedBatchNormV2", _inputs_flat, _attrs, _result, name)
  _result = _FusedBatchNormV2Output._make(_result)
  return _result



def fused_batch_norm_v2_eager_fallback(x, scale, offset, mean, variance, epsilon=0.0001, data_format="NHWC", is_training=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fused_batch_norm_v2
  """
  _ctx = ctx if ctx else _context.context()
  if epsilon is None:
    epsilon = 0.0001
  epsilon = _execute.make_float(epsilon, "epsilon")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if is_training is None:
    is_training = True
  is_training = _execute.make_bool(is_training, "is_training")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _attr_U, _inputs_U = _execute.args_to_matching_eager([scale, offset, mean, variance], _ctx)
  (scale, offset, mean, variance) = _inputs_U
  _inputs_flat = [x, scale, offset, mean, variance]
  _attrs = ("T", _attr_T, "U", _attr_U, "epsilon", epsilon, "data_format",
  data_format, "is_training", is_training)
  _result = _execute.execute(b"FusedBatchNormV2", 5, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FusedBatchNormV2", _inputs_flat, _attrs, _result, name)
  _result = _FusedBatchNormV2Output._make(_result)
  return _result


def fused_pad_conv2d(input, paddings, filter, mode, strides, padding, name=None):
  r"""Performs a padding as a preprocess during a convolution.

  Similar to FusedResizeAndPadConv2d, this op allows for an optimized
  implementation where the spatial padding transformation stage is fused with the
  im2col lookup, but in this case without the bilinear filtering required for
  resizing. Fusing the padding prevents the need to write out the intermediate
  results as whole tensors, reducing memory pressure, and we can get some latency
  gains by merging the transformation calculations.
  The data_format attribute for Conv2D isn't supported by this op, and 'NHWC'
  order is used instead.
  Internally this op uses a single per-graph scratch buffer, which means that it
  will block if multiple versions are being run in parallel. This is because this
  operator is primarily an optimization to minimize memory usage.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`.
      4-D with shape `[batch, in_height, in_width, in_channels]`.
    paddings: A `Tensor` of type `int32`.
      A two-column matrix specifying the padding sizes. The number of
      rows must be the same as the rank of `input`.
    filter: A `Tensor`. Must have the same type as `input`. 4-D with shape
      `[filter_height, filter_width, in_channels, out_channels]`.
    mode: A `string` from: `"REFLECT", "SYMMETRIC"`.
    strides: A list of `ints`.
      1-D of length 4.  The stride of the sliding window for each dimension
      of `input`. Must be in the same order as the dimension specified with format.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FusedPadConv2D", name, _ctx._post_execution_callbacks, input,
        paddings, filter, "mode", mode, "strides", strides, "padding",
        padding)
      return _result
    except _core._FallbackException:
      try:
        return fused_pad_conv2d_eager_fallback(
            input, paddings, filter, mode=mode, strides=strides,
            padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  mode = _execute.make_str(mode, "mode")
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'fused_pad_conv2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FusedPadConv2D", input=input, paddings=paddings, filter=filter,
                          mode=mode, strides=strides, padding=padding,
                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "mode", _op.get_attr("mode"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"))
  _execute.record_gradient(
      "FusedPadConv2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fused_pad_conv2d_eager_fallback(input, paddings, filter, mode, strides, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fused_pad_conv2d
  """
  _ctx = ctx if ctx else _context.context()
  mode = _execute.make_str(mode, "mode")
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'fused_pad_conv2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter], _ctx)
  (input, filter) = _inputs_T
  paddings = _ops.convert_to_tensor(paddings, _dtypes.int32)
  _inputs_flat = [input, paddings, filter]
  _attrs = ("T", _attr_T, "mode", mode, "strides", strides, "padding",
  padding)
  _result = _execute.execute(b"FusedPadConv2D", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FusedPadConv2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def fused_resize_and_pad_conv2d(input, size, paddings, filter, mode, strides, padding, resize_align_corners=False, name=None):
  r"""Performs a resize and padding as a preprocess during a convolution.

  It's often possible to do spatial transformations more efficiently as part of
  the packing stage of a convolution, so this op allows for an optimized
  implementation where these stages are fused together. This prevents the need to
  write out the intermediate results as whole tensors, reducing memory pressure,
  and we can get some latency gains by merging the transformation calculations.
  The data_format attribute for Conv2D isn't supported by this op, and defaults to
  'NHWC' order.
  Internally this op uses a single per-graph scratch buffer, which means that it
  will block if multiple versions are being run in parallel. This is because this
  operator is primarily an optimization to minimize memory usage.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`.
      4-D with shape `[batch, in_height, in_width, in_channels]`.
    size: A `Tensor` of type `int32`.
      A 1-D int32 Tensor of 2 elements: `new_height, new_width`.  The
      new size for the images.
    paddings: A `Tensor` of type `int32`.
      A two-column matrix specifying the padding sizes. The number of
      rows must be the same as the rank of `input`.
    filter: A `Tensor`. Must have the same type as `input`. 4-D with shape
      `[filter_height, filter_width, in_channels, out_channels]`.
    mode: A `string` from: `"REFLECT", "SYMMETRIC"`.
    strides: A list of `ints`.
      1-D of length 4.  The stride of the sliding window for each dimension
      of `input`. Must be in the same order as the dimension specified with format.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    resize_align_corners: An optional `bool`. Defaults to `False`.
      If true, the centers of the 4 corner pixels of the input and output tensors are
      aligned, preserving the values at the corner pixels. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FusedResizeAndPadConv2D", name, _ctx._post_execution_callbacks,
        input, size, paddings, filter, "resize_align_corners",
        resize_align_corners, "mode", mode, "strides", strides, "padding",
        padding)
      return _result
    except _core._FallbackException:
      try:
        return fused_resize_and_pad_conv2d_eager_fallback(
            input, size, paddings, filter,
            resize_align_corners=resize_align_corners, mode=mode,
            strides=strides, padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  mode = _execute.make_str(mode, "mode")
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'fused_resize_and_pad_conv2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if resize_align_corners is None:
    resize_align_corners = False
  resize_align_corners = _execute.make_bool(resize_align_corners, "resize_align_corners")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FusedResizeAndPadConv2D", input=input, size=size, paddings=paddings,
                                   filter=filter, mode=mode, strides=strides,
                                   padding=padding,
                                   resize_align_corners=resize_align_corners,
                                   name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "resize_align_corners",
            _op.get_attr("resize_align_corners"), "mode",
            _op.get_attr("mode"), "strides", _op.get_attr("strides"),
            "padding", _op.get_attr("padding"))
  _execute.record_gradient(
      "FusedResizeAndPadConv2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fused_resize_and_pad_conv2d_eager_fallback(input, size, paddings, filter, mode, strides, padding, resize_align_corners=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fused_resize_and_pad_conv2d
  """
  _ctx = ctx if ctx else _context.context()
  mode = _execute.make_str(mode, "mode")
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'fused_resize_and_pad_conv2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if resize_align_corners is None:
    resize_align_corners = False
  resize_align_corners = _execute.make_bool(resize_align_corners, "resize_align_corners")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, filter], _ctx)
  (input, filter) = _inputs_T
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  paddings = _ops.convert_to_tensor(paddings, _dtypes.int32)
  _inputs_flat = [input, size, paddings, filter]
  _attrs = ("T", _attr_T, "resize_align_corners", resize_align_corners,
  "mode", mode, "strides", strides, "padding", padding)
  _result = _execute.execute(b"FusedResizeAndPadConv2D", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FusedResizeAndPadConv2D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def in_top_k(predictions, targets, k, name=None):
  r"""Says whether the targets are in the top `K` predictions.

  This outputs a `batch_size` bool array, an entry `out[i]` is `true` if the
  prediction for the target class is among the top `k` predictions among
  all predictions for example `i`. Note that the behavior of `InTopK` differs
  from the `TopK` op in its handling of ties; if multiple classes have the
  same prediction value and straddle the top-`k` boundary, all of those
  classes are considered to be in the top `k`.

  More formally, let

    \\(predictions_i\\) be the predictions for all classes for example `i`,
    \\(targets_i\\) be the target class for example `i`,
    \\(out_i\\) be the output for example `i`,

  $$out_i = predictions_{i, targets_i} \in TopKIncludingTies(predictions_i)$$

  Args:
    predictions: A `Tensor` of type `float32`.
      A `batch_size` x `classes` tensor.
    targets: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A `batch_size` vector of class ids.
    k: An `int`. Number of top elements to look at for computing precision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "InTopK", name,
        _ctx._post_execution_callbacks, predictions, targets, "k", k)
      return _result
    except _core._FallbackException:
      try:
        return in_top_k_eager_fallback(
            predictions, targets, k=k, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  k = _execute.make_int(k, "k")
  _, _, _op = _op_def_lib._apply_op_helper(
        "InTopK", predictions=predictions, targets=targets, k=k, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("k", _op.get_attr("k"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "InTopK", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def in_top_k_eager_fallback(predictions, targets, k, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function in_top_k
  """
  _ctx = ctx if ctx else _context.context()
  k = _execute.make_int(k, "k")
  _attr_T, (targets,) = _execute.args_to_matching_eager([targets], _ctx, _dtypes.int32)
  predictions = _ops.convert_to_tensor(predictions, _dtypes.float32)
  _inputs_flat = [predictions, targets]
  _attrs = ("k", k, "T", _attr_T)
  _result = _execute.execute(b"InTopK", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "InTopK", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def in_top_kv2(predictions, targets, k, name=None):
  r"""Says whether the targets are in the top `K` predictions.

  This outputs a `batch_size` bool array, an entry `out[i]` is `true` if the
  prediction for the target class is among the top `k` predictions among
  all predictions for example `i`. Note that the behavior of `InTopK` differs
  from the `TopK` op in its handling of ties; if multiple classes have the
  same prediction value and straddle the top-`k` boundary, all of those
  classes are considered to be in the top `k`.

  More formally, let

    \\(predictions_i\\) be the predictions for all classes for example `i`,
    \\(targets_i\\) be the target class for example `i`,
    \\(out_i\\) be the output for example `i`,

  $$out_i = predictions_{i, targets_i} \in TopKIncludingTies(predictions_i)$$

  Args:
    predictions: A `Tensor` of type `float32`.
      A `batch_size` x `classes` tensor.
    targets: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A `batch_size` vector of class ids.
    k: A `Tensor`. Must have the same type as `targets`.
      Number of top elements to look at for computing precision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "InTopKV2",
        name, _ctx._post_execution_callbacks, predictions, targets, k)
      return _result
    except _core._FallbackException:
      try:
        return in_top_kv2_eager_fallback(
            predictions, targets, k, name=name, ctx=_ctx)
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
        "InTopKV2", predictions=predictions, targets=targets, k=k, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "InTopKV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def in_top_kv2_eager_fallback(predictions, targets, k, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function in_top_kv2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([targets, k], _ctx, _dtypes.int32)
  (targets, k) = _inputs_T
  predictions = _ops.convert_to_tensor(predictions, _dtypes.float32)
  _inputs_flat = [predictions, targets, k]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"InTopKV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "InTopKV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.l2_loss')
def l2_loss(t, name=None):
  r"""L2 Loss.

  Computes half the L2 norm of a tensor without the `sqrt`:

      output = sum(t ** 2) / 2

  Args:
    t: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      Typically 2-D, but may have any dimensions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `t`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "L2Loss", name,
        _ctx._post_execution_callbacks, t)
      return _result
    except _core._FallbackException:
      try:
        return l2_loss_eager_fallback(
            t, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              l2_loss, t=t, name=name)
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
        "L2Loss", t=t, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          l2_loss, t=t, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "L2Loss", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def l2_loss_eager_fallback(t, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function l2_loss
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (t,) = _execute.args_to_matching_eager([t], _ctx)
  _inputs_flat = [t]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"L2Loss", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "L2Loss", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.local_response_normalization', 'nn.lrn')
def lrn(input, depth_radius=5, bias=1, alpha=1, beta=0.5, name=None):
  r"""Local Response Normalization.

  The 4-D `input` tensor is treated as a 3-D array of 1-D vectors (along the last
  dimension), and each vector is normalized independently.  Within a given vector,
  each component is divided by the weighted, squared sum of inputs within
  `depth_radius`.  In detail,

      sqr_sum[a, b, c, d] =
          sum(input[a, b, c, d - depth_radius : d + depth_radius + 1] ** 2)
      output = input / (bias + alpha * sqr_sum) ** beta

  For details, see [Krizhevsky et al., ImageNet classification with deep
  convolutional neural networks (NIPS 2012)](http://papers.nips.cc/paper/4824-imagenet-classification-with-deep-convolutional-neural-networks).

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`.
      4-D.
    depth_radius: An optional `int`. Defaults to `5`.
      0-D.  Half-width of the 1-D normalization window.
    bias: An optional `float`. Defaults to `1`.
      An offset (usually positive to avoid dividing by 0).
    alpha: An optional `float`. Defaults to `1`.
      A scale factor, usually positive.
    beta: An optional `float`. Defaults to `0.5`. An exponent.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LRN", name,
        _ctx._post_execution_callbacks, input, "depth_radius", depth_radius,
        "bias", bias, "alpha", alpha, "beta", beta)
      return _result
    except _core._FallbackException:
      try:
        return lrn_eager_fallback(
            input, depth_radius=depth_radius, bias=bias, alpha=alpha,
            beta=beta, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              lrn, input=input, depth_radius=depth_radius, bias=bias,
                   alpha=alpha, beta=beta, name=name)
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
  if depth_radius is None:
    depth_radius = 5
  depth_radius = _execute.make_int(depth_radius, "depth_radius")
  if bias is None:
    bias = 1
  bias = _execute.make_float(bias, "bias")
  if alpha is None:
    alpha = 1
  alpha = _execute.make_float(alpha, "alpha")
  if beta is None:
    beta = 0.5
  beta = _execute.make_float(beta, "beta")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LRN", input=input, depth_radius=depth_radius, bias=bias, alpha=alpha,
               beta=beta, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          lrn, input=input, depth_radius=depth_radius, bias=bias, alpha=alpha,
               beta=beta, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("depth_radius", _op.get_attr("depth_radius"), "bias",
            _op.get_attr("bias"), "alpha", _op.get_attr("alpha"), "beta",
            _op.get_attr("beta"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "LRN", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lrn_eager_fallback(input, depth_radius=5, bias=1, alpha=1, beta=0.5, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lrn
  """
  _ctx = ctx if ctx else _context.context()
  if depth_radius is None:
    depth_radius = 5
  depth_radius = _execute.make_int(depth_radius, "depth_radius")
  if bias is None:
    bias = 1
  bias = _execute.make_float(bias, "bias")
  if alpha is None:
    alpha = 1
  alpha = _execute.make_float(alpha, "alpha")
  if beta is None:
    beta = 0.5
  beta = _execute.make_float(beta, "beta")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.float32)
  _inputs_flat = [input]
  _attrs = ("depth_radius", depth_radius, "bias", bias, "alpha", alpha,
  "beta", beta, "T", _attr_T)
  _result = _execute.execute(b"LRN", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "LRN", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def lrn_grad(input_grads, input_image, output_image, depth_radius=5, bias=1, alpha=1, beta=0.5, name=None):
  r"""Gradients for Local Response Normalization.

  Args:
    input_grads: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`.
      4-D with shape `[batch, height, width, channels]`.
    input_image: A `Tensor`. Must have the same type as `input_grads`.
      4-D with shape `[batch, height, width, channels]`.
    output_image: A `Tensor`. Must have the same type as `input_grads`.
      4-D with shape `[batch, height, width, channels]`.
    depth_radius: An optional `int`. Defaults to `5`. A depth radius.
    bias: An optional `float`. Defaults to `1`.
      An offset (usually > 0 to avoid dividing by 0).
    alpha: An optional `float`. Defaults to `1`.
      A scale factor, usually positive.
    beta: An optional `float`. Defaults to `0.5`. An exponent.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input_grads`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LRNGrad",
        name, _ctx._post_execution_callbacks, input_grads, input_image,
        output_image, "depth_radius", depth_radius, "bias", bias, "alpha",
        alpha, "beta", beta)
      return _result
    except _core._FallbackException:
      try:
        return lrn_grad_eager_fallback(
            input_grads, input_image, output_image, depth_radius=depth_radius,
            bias=bias, alpha=alpha, beta=beta, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if depth_radius is None:
    depth_radius = 5
  depth_radius = _execute.make_int(depth_radius, "depth_radius")
  if bias is None:
    bias = 1
  bias = _execute.make_float(bias, "bias")
  if alpha is None:
    alpha = 1
  alpha = _execute.make_float(alpha, "alpha")
  if beta is None:
    beta = 0.5
  beta = _execute.make_float(beta, "beta")
  _, _, _op = _op_def_lib._apply_op_helper(
        "LRNGrad", input_grads=input_grads, input_image=input_image,
                   output_image=output_image, depth_radius=depth_radius,
                   bias=bias, alpha=alpha, beta=beta, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("depth_radius", _op.get_attr("depth_radius"), "bias",
            _op.get_attr("bias"), "alpha", _op.get_attr("alpha"), "beta",
            _op.get_attr("beta"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "LRNGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lrn_grad_eager_fallback(input_grads, input_image, output_image, depth_radius=5, bias=1, alpha=1, beta=0.5, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lrn_grad
  """
  _ctx = ctx if ctx else _context.context()
  if depth_radius is None:
    depth_radius = 5
  depth_radius = _execute.make_int(depth_radius, "depth_radius")
  if bias is None:
    bias = 1
  bias = _execute.make_float(bias, "bias")
  if alpha is None:
    alpha = 1
  alpha = _execute.make_float(alpha, "alpha")
  if beta is None:
    beta = 0.5
  beta = _execute.make_float(beta, "beta")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input_grads, input_image, output_image], _ctx, _dtypes.float32)
  (input_grads, input_image, output_image) = _inputs_T
  _inputs_flat = [input_grads, input_image, output_image]
  _attrs = ("depth_radius", depth_radius, "bias", bias, "alpha", alpha,
  "beta", beta, "T", _attr_T)
  _result = _execute.execute(b"LRNGrad", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "LRNGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def leaky_relu(features, alpha=0.2, name=None):
  r"""Computes rectified linear: `max(features, features * alpha)`.

  Args:
    features: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
    alpha: An optional `float`. Defaults to `0.2`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `features`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LeakyRelu",
        name, _ctx._post_execution_callbacks, features, "alpha", alpha)
      return _result
    except _core._FallbackException:
      try:
        return leaky_relu_eager_fallback(
            features, alpha=alpha, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if alpha is None:
    alpha = 0.2
  alpha = _execute.make_float(alpha, "alpha")
  _, _, _op = _op_def_lib._apply_op_helper(
        "LeakyRelu", features=features, alpha=alpha, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("alpha", _op.get_attr("alpha"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "LeakyRelu", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def leaky_relu_eager_fallback(features, alpha=0.2, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function leaky_relu
  """
  _ctx = ctx if ctx else _context.context()
  if alpha is None:
    alpha = 0.2
  alpha = _execute.make_float(alpha, "alpha")
  _attr_T, (features,) = _execute.args_to_matching_eager([features], _ctx, _dtypes.float32)
  _inputs_flat = [features]
  _attrs = ("alpha", alpha, "T", _attr_T)
  _result = _execute.execute(b"LeakyRelu", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LeakyRelu", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def leaky_relu_grad(gradients, features, alpha=0.2, name=None):
  r"""Computes rectified linear gradients for a LeakyRelu operation.

  Args:
    gradients: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      The backpropagated gradients to the corresponding LeakyRelu operation.
    features: A `Tensor`. Must have the same type as `gradients`.
      The features passed as input to the corresponding LeakyRelu operation,
      OR the outputs of that operation (both work equivalently).
    alpha: An optional `float`. Defaults to `0.2`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `gradients`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LeakyReluGrad", name, _ctx._post_execution_callbacks, gradients,
        features, "alpha", alpha)
      return _result
    except _core._FallbackException:
      try:
        return leaky_relu_grad_eager_fallback(
            gradients, features, alpha=alpha, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if alpha is None:
    alpha = 0.2
  alpha = _execute.make_float(alpha, "alpha")
  _, _, _op = _op_def_lib._apply_op_helper(
        "LeakyReluGrad", gradients=gradients, features=features, alpha=alpha,
                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("alpha", _op.get_attr("alpha"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "LeakyReluGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def leaky_relu_grad_eager_fallback(gradients, features, alpha=0.2, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function leaky_relu_grad
  """
  _ctx = ctx if ctx else _context.context()
  if alpha is None:
    alpha = 0.2
  alpha = _execute.make_float(alpha, "alpha")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([gradients, features], _ctx, _dtypes.float32)
  (gradients, features) = _inputs_T
  _inputs_flat = [gradients, features]
  _attrs = ("alpha", alpha, "T", _attr_T)
  _result = _execute.execute(b"LeakyReluGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LeakyReluGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def log_softmax(logits, name=None):
  r"""Computes log softmax activations.

  For each batch `i` and class `j` we have

      logsoftmax[i, j] = logits[i, j] - log(sum(exp(logits[i])))

  Args:
    logits: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      2-D with shape `[batch_size, num_classes]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `logits`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LogSoftmax",
        name, _ctx._post_execution_callbacks, logits)
      return _result
    except _core._FallbackException:
      try:
        return log_softmax_eager_fallback(
            logits, name=name, ctx=_ctx)
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
        "LogSoftmax", logits=logits, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "LogSoftmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def log_softmax_eager_fallback(logits, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function log_softmax
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (logits,) = _execute.args_to_matching_eager([logits], _ctx)
  _inputs_flat = [logits]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"LogSoftmax", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LogSoftmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool(input, ksize, strides, padding, data_format="NHWC", name=None):
  r"""Performs max pooling on the input.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`, `int32`, `int64`, `uint8`, `int16`, `int8`, `uint16`, `qint8`.
      4-D input to pool over.
    ksize: A list of `ints` that has length `>= 4`.
      The size of the window for each dimension of the input tensor.
    strides: A list of `ints` that has length `>= 4`.
      The stride of the sliding window for each dimension of the
      input tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW", "NCHW_VECT_C"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MaxPool",
        name, _ctx._post_execution_callbacks, input, "ksize", ksize,
        "strides", strides, "padding", padding, "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return max_pool_eager_fallback(
            input, ksize=ksize, strides=strides, padding=padding,
            data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPool", input=input, ksize=ksize, strides=strides, padding=padding,
                   data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"))
  _execute.record_gradient(
      "MaxPool", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool_eager_fallback(input, ksize, strides, padding, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.float32)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "ksize", ksize, "strides", strides, "padding",
  padding, "data_format", data_format)
  _result = _execute.execute(b"MaxPool", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPool", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.max_pool3d')
def max_pool3d(input, ksize, strides, padding, data_format="NDHWC", name=None):
  r"""Performs 3D max pooling on the input.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`.
      Shape `[batch, depth, rows, cols, channels]` tensor to pool over.
    ksize: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The size of the window for each dimension of
      the input tensor. Must have `ksize[0] = ksize[4] = 1`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NDHWC", "NCDHW"`. Defaults to `"NDHWC"`.
      The data format of the input and output data. With the
      default format "NDHWC", the data is stored in the order of:
          [batch, in_depth, in_height, in_width, in_channels].
      Alternatively, the format could be "NCDHW", the data storage order is:
          [batch, in_channels, in_depth, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MaxPool3D",
        name, _ctx._post_execution_callbacks, input, "ksize", ksize,
        "strides", strides, "padding", padding, "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return max_pool3d_eager_fallback(
            input, ksize=ksize, strides=strides, padding=padding,
            data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              max_pool3d, input=input, ksize=ksize, strides=strides,
                          padding=padding, data_format=data_format, name=name)
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
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool3d' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool3d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPool3D", input=input, ksize=ksize, strides=strides,
                     padding=padding, data_format=data_format, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          max_pool3d, input=input, ksize=ksize, strides=strides,
                      padding=padding, data_format=data_format, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "MaxPool3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool3d_eager_fallback(input, ksize, strides, padding, data_format="NDHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool3d
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool3d' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool3d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding,
  "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"MaxPool3D", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPool3D", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool3d_grad(orig_input, orig_output, grad, ksize, strides, padding, data_format="NDHWC", name=None):
  r"""Computes gradients of max pooling function.

  Args:
    orig_input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`.
      The original input tensor.
    orig_output: A `Tensor`. Must have the same type as `orig_input`.
      The original output tensor.
    grad: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`.
      Output backprop of shape `[batch, depth, rows, cols, channels]`.
    ksize: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The size of the window for each dimension of
      the input tensor. Must have `ksize[0] = ksize[4] = 1`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NDHWC", "NCDHW"`. Defaults to `"NDHWC"`.
      The data format of the input and output data. With the
      default format "NDHWC", the data is stored in the order of:
          [batch, in_depth, in_height, in_width, in_channels].
      Alternatively, the format could be "NCDHW", the data storage order is:
          [batch, in_channels, in_depth, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `grad`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MaxPool3DGrad", name, _ctx._post_execution_callbacks, orig_input,
        orig_output, grad, "ksize", ksize, "strides", strides, "padding",
        padding, "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return max_pool3d_grad_eager_fallback(
            orig_input, orig_output, grad, ksize=ksize, strides=strides,
            padding=padding, data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool3d_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool3d_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPool3DGrad", orig_input=orig_input, orig_output=orig_output,
                         grad=grad, ksize=ksize, strides=strides,
                         padding=padding, data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "T",
            _op.get_attr("T"), "TInput", _op.get_attr("TInput"))
  _execute.record_gradient(
      "MaxPool3DGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool3d_grad_eager_fallback(orig_input, orig_output, grad, ksize, strides, padding, data_format="NDHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool3d_grad
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool3d_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool3d_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (grad,) = _execute.args_to_matching_eager([grad], _ctx, _dtypes.float32)
  _attr_TInput, _inputs_TInput = _execute.args_to_matching_eager([orig_input, orig_output], _ctx, _dtypes.float32)
  (orig_input, orig_output) = _inputs_TInput
  _inputs_flat = [orig_input, orig_output, grad]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding,
  "data_format", data_format, "T", _attr_T, "TInput", _attr_TInput)
  _result = _execute.execute(b"MaxPool3DGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPool3DGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool3d_grad_grad(orig_input, orig_output, grad, ksize, strides, padding, data_format="NDHWC", name=None):
  r"""Computes second-order gradients of the maxpooling function.

  Args:
    orig_input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      The original input tensor.
    orig_output: A `Tensor`. Must have the same type as `orig_input`.
      The original output tensor.
    grad: A `Tensor`. Must have the same type as `orig_input`.
      Output backprop of shape `[batch, depth, rows, cols, channels]`.
    ksize: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The size of the window for each dimension of
      the input tensor. Must have `ksize[0] = ksize[4] = 1`.
    strides: A list of `ints` that has length `>= 5`.
      1-D tensor of length 5. The stride of the sliding window for each
      dimension of `input`. Must have `strides[0] = strides[4] = 1`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NDHWC", "NCDHW"`. Defaults to `"NDHWC"`.
      The data format of the input and output data. With the
      default format "NDHWC", the data is stored in the order of:
          [batch, in_depth, in_height, in_width, in_channels].
      Alternatively, the format could be "NCDHW", the data storage order is:
          [batch, in_channels, in_depth, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `orig_input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MaxPool3DGradGrad", name, _ctx._post_execution_callbacks, orig_input,
        orig_output, grad, "ksize", ksize, "strides", strides, "padding",
        padding, "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return max_pool3d_grad_grad_eager_fallback(
            orig_input, orig_output, grad, ksize=ksize, strides=strides,
            padding=padding, data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool3d_grad_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool3d_grad_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPool3DGradGrad", orig_input=orig_input, orig_output=orig_output,
                             grad=grad, ksize=ksize, strides=strides,
                             padding=padding, data_format=data_format,
                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "MaxPool3DGradGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool3d_grad_grad_eager_fallback(orig_input, orig_output, grad, ksize, strides, padding, data_format="NDHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool3d_grad_grad
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool3d_grad_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool3d_grad_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NDHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([orig_input, orig_output, grad], _ctx)
  (orig_input, orig_output, grad) = _inputs_T
  _inputs_flat = [orig_input, orig_output, grad]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding,
  "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"MaxPool3DGradGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPool3DGradGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool_grad(orig_input, orig_output, grad, ksize, strides, padding, data_format="NHWC", name=None):
  r"""Computes gradients of the maxpooling function.

  Args:
    orig_input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      The original input tensor.
    orig_output: A `Tensor`. Must have the same type as `orig_input`.
      The original output tensor.
    grad: A `Tensor`. Must have the same type as `orig_input`.
      4-D.  Gradients w.r.t. the output of `max_pool`.
    ksize: A list of `ints` that has length `>= 4`.
      The size of the window for each dimension of the input tensor.
    strides: A list of `ints` that has length `>= 4`.
      The stride of the sliding window for each dimension of the
      input tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `orig_input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MaxPoolGrad",
        name, _ctx._post_execution_callbacks, orig_input, orig_output, grad,
        "ksize", ksize, "strides", strides, "padding", padding, "data_format",
        data_format)
      return _result
    except _core._FallbackException:
      try:
        return max_pool_grad_eager_fallback(
            orig_input, orig_output, grad, ksize=ksize, strides=strides,
            padding=padding, data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPoolGrad", orig_input=orig_input, orig_output=orig_output,
                       grad=grad, ksize=ksize, strides=strides,
                       padding=padding, data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "MaxPoolGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool_grad_eager_fallback(orig_input, orig_output, grad, ksize, strides, padding, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool_grad
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([orig_input, orig_output, grad], _ctx, _dtypes.float32)
  (orig_input, orig_output, grad) = _inputs_T
  _inputs_flat = [orig_input, orig_output, grad]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding,
  "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"MaxPoolGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPoolGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool_grad_grad(orig_input, orig_output, grad, ksize, strides, padding, data_format="NHWC", name=None):
  r"""Computes second-order gradients of the maxpooling function.

  Args:
    orig_input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      The original input tensor.
    orig_output: A `Tensor`. Must have the same type as `orig_input`.
      The original output tensor.
    grad: A `Tensor`. Must have the same type as `orig_input`.
      4-D.  Gradients of gradients w.r.t. the input of `max_pool`.
    ksize: A list of `ints` that has length `>= 4`.
      The size of the window for each dimension of the input tensor.
    strides: A list of `ints` that has length `>= 4`.
      The stride of the sliding window for each dimension of the
      input tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `orig_input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MaxPoolGradGrad", name, _ctx._post_execution_callbacks, orig_input,
        orig_output, grad, "ksize", ksize, "strides", strides, "padding",
        padding, "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return max_pool_grad_grad_eager_fallback(
            orig_input, orig_output, grad, ksize=ksize, strides=strides,
            padding=padding, data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_grad_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_grad_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPoolGradGrad", orig_input=orig_input, orig_output=orig_output,
                           grad=grad, ksize=ksize, strides=strides,
                           padding=padding, data_format=data_format,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "MaxPoolGradGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool_grad_grad_eager_fallback(orig_input, orig_output, grad, ksize, strides, padding, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool_grad_grad
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_grad_grad' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_grad_grad' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([orig_input, orig_output, grad], _ctx)
  (orig_input, orig_output, grad) = _inputs_T
  _inputs_flat = [orig_input, orig_output, grad]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding,
  "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"MaxPoolGradGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPoolGradGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool_grad_grad_v2(orig_input, orig_output, grad, ksize, strides, padding, data_format="NHWC", name=None):
  r"""Computes second-order gradients of the maxpooling function.

  Args:
    orig_input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      The original input tensor.
    orig_output: A `Tensor`. Must have the same type as `orig_input`.
      The original output tensor.
    grad: A `Tensor`. Must have the same type as `orig_input`.
      4-D.  Gradients of gradients w.r.t. the input of `max_pool`.
    ksize: A `Tensor` of type `int32`.
      The size of the window for each dimension of the input tensor.
    strides: A `Tensor` of type `int32`.
      The stride of the sliding window for each dimension of the
      input tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `orig_input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MaxPoolGradGradV2", name, _ctx._post_execution_callbacks, orig_input,
        orig_output, grad, ksize, strides, "padding", padding, "data_format",
        data_format)
      return _result
    except _core._FallbackException:
      try:
        return max_pool_grad_grad_v2_eager_fallback(
            orig_input, orig_output, grad, ksize, strides, padding=padding,
            data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPoolGradGradV2", orig_input=orig_input, orig_output=orig_output,
                             grad=grad, ksize=ksize, strides=strides,
                             padding=padding, data_format=data_format,
                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("padding", _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "MaxPoolGradGradV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool_grad_grad_v2_eager_fallback(orig_input, orig_output, grad, ksize, strides, padding, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool_grad_grad_v2
  """
  _ctx = ctx if ctx else _context.context()
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([orig_input, orig_output, grad], _ctx)
  (orig_input, orig_output, grad) = _inputs_T
  ksize = _ops.convert_to_tensor(ksize, _dtypes.int32)
  strides = _ops.convert_to_tensor(strides, _dtypes.int32)
  _inputs_flat = [orig_input, orig_output, grad, ksize, strides]
  _attrs = ("padding", padding, "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"MaxPoolGradGradV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPoolGradGradV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool_grad_grad_with_argmax(input, grad, argmax, ksize, strides, padding, name=None):
  r"""Computes second-order gradients of the maxpooling function.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      The original input.
    grad: A `Tensor`. Must have the same type as `input`.
      4-D with shape `[batch, height, width, channels]`.  Gradients w.r.t. the
      input of `max_pool`.
    argmax: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The indices of the maximum values chosen for each output of `max_pool`.
    ksize: A list of `ints` that has length `>= 4`.
      The size of the window for each dimension of the input tensor.
    strides: A list of `ints` that has length `>= 4`.
      The stride of the sliding window for each dimension of the
      input tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MaxPoolGradGradWithArgmax", name, _ctx._post_execution_callbacks,
        input, grad, argmax, "ksize", ksize, "strides", strides, "padding",
        padding)
      return _result
    except _core._FallbackException:
      try:
        return max_pool_grad_grad_with_argmax_eager_fallback(
            input, grad, argmax, ksize=ksize, strides=strides,
            padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_grad_grad_with_argmax' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_grad_grad_with_argmax' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPoolGradGradWithArgmax", input=input, grad=grad, argmax=argmax,
                                     ksize=ksize, strides=strides,
                                     padding=padding, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "Targmax", _op.get_attr("Targmax"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "MaxPoolGradGradWithArgmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool_grad_grad_with_argmax_eager_fallback(input, grad, argmax, ksize, strides, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool_grad_grad_with_argmax
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_grad_grad_with_argmax' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_grad_grad_with_argmax' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _attr_Targmax, (argmax,) = _execute.args_to_matching_eager([argmax], _ctx)
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, grad], _ctx)
  (input, grad) = _inputs_T
  _inputs_flat = [input, grad, argmax]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding, "Targmax",
  _attr_Targmax, "T", _attr_T)
  _result = _execute.execute(b"MaxPoolGradGradWithArgmax", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "MaxPoolGradGradWithArgmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool_grad_v2(orig_input, orig_output, grad, ksize, strides, padding, data_format="NHWC", name=None):
  r"""Computes gradients of the maxpooling function.

  Args:
    orig_input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      The original input tensor.
    orig_output: A `Tensor`. Must have the same type as `orig_input`.
      The original output tensor.
    grad: A `Tensor`. Must have the same type as `orig_input`.
      4-D.  Gradients w.r.t. the output of `max_pool`.
    ksize: A `Tensor` of type `int32`.
      The size of the window for each dimension of the input tensor.
    strides: A `Tensor` of type `int32`.
      The stride of the sliding window for each dimension of the
      input tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `orig_input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MaxPoolGradV2", name, _ctx._post_execution_callbacks, orig_input,
        orig_output, grad, ksize, strides, "padding", padding, "data_format",
        data_format)
      return _result
    except _core._FallbackException:
      try:
        return max_pool_grad_v2_eager_fallback(
            orig_input, orig_output, grad, ksize, strides, padding=padding,
            data_format=data_format, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPoolGradV2", orig_input=orig_input, orig_output=orig_output,
                         grad=grad, ksize=ksize, strides=strides,
                         padding=padding, data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("padding", _op.get_attr("padding"), "data_format",
            _op.get_attr("data_format"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "MaxPoolGradV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool_grad_v2_eager_fallback(orig_input, orig_output, grad, ksize, strides, padding, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool_grad_v2
  """
  _ctx = ctx if ctx else _context.context()
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([orig_input, orig_output, grad], _ctx, _dtypes.float32)
  (orig_input, orig_output, grad) = _inputs_T
  ksize = _ops.convert_to_tensor(ksize, _dtypes.int32)
  strides = _ops.convert_to_tensor(strides, _dtypes.int32)
  _inputs_flat = [orig_input, orig_output, grad, ksize, strides]
  _attrs = ("padding", padding, "data_format", data_format, "T", _attr_T)
  _result = _execute.execute(b"MaxPoolGradV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPoolGradV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool_grad_with_argmax(input, grad, argmax, ksize, strides, padding, name=None):
  r"""Computes gradients of the maxpooling function.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      The original input.
    grad: A `Tensor`. Must have the same type as `input`.
      4-D with shape `[batch, height, width, channels]`.  Gradients w.r.t. the
      output of `max_pool`.
    argmax: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The indices of the maximum values chosen for each output of `max_pool`.
    ksize: A list of `ints` that has length `>= 4`.
      The size of the window for each dimension of the input tensor.
    strides: A list of `ints` that has length `>= 4`.
      The stride of the sliding window for each dimension of the
      input tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MaxPoolGradWithArgmax", name, _ctx._post_execution_callbacks, input,
        grad, argmax, "ksize", ksize, "strides", strides, "padding", padding)
      return _result
    except _core._FallbackException:
      try:
        return max_pool_grad_with_argmax_eager_fallback(
            input, grad, argmax, ksize=ksize, strides=strides,
            padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_grad_with_argmax' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_grad_with_argmax' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPoolGradWithArgmax", input=input, grad=grad, argmax=argmax,
                                 ksize=ksize, strides=strides,
                                 padding=padding, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "Targmax", _op.get_attr("Targmax"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "MaxPoolGradWithArgmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool_grad_with_argmax_eager_fallback(input, grad, argmax, ksize, strides, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool_grad_with_argmax
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_grad_with_argmax' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_grad_with_argmax' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _attr_Targmax, (argmax,) = _execute.args_to_matching_eager([argmax], _ctx)
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, grad], _ctx)
  (input, grad) = _inputs_T
  _inputs_flat = [input, grad, argmax]
  _attrs = ("ksize", ksize, "strides", strides, "padding", padding, "Targmax",
  _attr_Targmax, "T", _attr_T)
  _result = _execute.execute(b"MaxPoolGradWithArgmax", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPoolGradWithArgmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def max_pool_v2(input, ksize, strides, padding, data_format="NHWC", name=None):
  r"""Performs max pooling on the input.

  Args:
    input: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`, `int32`, `int64`, `uint8`, `int16`, `int8`, `uint16`, `qint8`.
      4-D input to pool over.
    ksize: A `Tensor` of type `int32`.
      The size of the window for each dimension of the input tensor.
    strides: A `Tensor` of type `int32`.
      The stride of the sliding window for each dimension of the
      input tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    data_format: An optional `string` from: `"NHWC", "NCHW", "NCHW_VECT_C"`. Defaults to `"NHWC"`.
      Specify the data format of the input and output data. With the
      default format "NHWC", the data is stored in the order of:
          [batch, in_height, in_width, in_channels].
      Alternatively, the format could be "NCHW", the data storage order of:
          [batch, in_channels, in_height, in_width].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MaxPoolV2",
        name, _ctx._post_execution_callbacks, input, ksize, strides,
        "padding", padding, "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return max_pool_v2_eager_fallback(
            input, ksize, strides, padding=padding, data_format=data_format,
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
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPoolV2", input=input, ksize=ksize, strides=strides,
                     padding=padding, data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"))
  _execute.record_gradient(
      "MaxPoolV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def max_pool_v2_eager_fallback(input, ksize, strides, padding, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool_v2
  """
  _ctx = ctx if ctx else _context.context()
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx, _dtypes.float32)
  ksize = _ops.convert_to_tensor(ksize, _dtypes.int32)
  strides = _ops.convert_to_tensor(strides, _dtypes.int32)
  _inputs_flat = [input, ksize, strides]
  _attrs = ("T", _attr_T, "padding", padding, "data_format", data_format)
  _result = _execute.execute(b"MaxPoolV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPoolV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_max_pool_with_argmax_outputs = ["output", "argmax"]
_MaxPoolWithArgmaxOutput = _collections.namedtuple(
    "MaxPoolWithArgmax", _max_pool_with_argmax_outputs)


@_dispatch.add_dispatch_list
@tf_export(v1=['nn.max_pool_with_argmax'])
@deprecated_endpoints('nn.max_pool_with_argmax')
def max_pool_with_argmax(input, ksize, strides, padding, Targmax=_dtypes.int64, name=None):
  r"""Performs max pooling on the input and outputs both max values and indices.

  The indices in `argmax` are flattened, so that a maximum value at position
  `[b, y, x, c]` becomes flattened index
  `((b * height + y) * width + x) * channels + c`.

  The indices returned are always in `[0, height) x [0, width)` before flattening,
  even if padding is involved and the mathematically correct answer is outside
  (either negative or too large).  This is a bug, but fixing it is difficult to do
  in a safe backwards compatible way, especially due to flattening.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      4-D with shape `[batch, height, width, channels]`.  Input to pool over.
    ksize: A list of `ints` that has length `>= 4`.
      The size of the window for each dimension of the input tensor.
    strides: A list of `ints` that has length `>= 4`.
      The stride of the sliding window for each dimension of the
      input tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    Targmax: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int64`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, argmax).

    output: A `Tensor`. Has the same type as `input`.
    argmax: A `Tensor` of type `Targmax`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MaxPoolWithArgmax", name, _ctx._post_execution_callbacks, input,
        "ksize", ksize, "strides", strides, "Targmax", Targmax, "padding",
        padding)
      _result = _MaxPoolWithArgmaxOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return max_pool_with_argmax_eager_fallback(
            input, ksize=ksize, strides=strides, Targmax=Targmax,
            padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              max_pool_with_argmax, input=input, ksize=ksize, strides=strides,
                                    padding=padding, Targmax=Targmax,
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
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_with_argmax' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_with_argmax' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if Targmax is None:
    Targmax = _dtypes.int64
  Targmax = _execute.make_type(Targmax, "Targmax")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "MaxPoolWithArgmax", input=input, ksize=ksize, strides=strides,
                             padding=padding, Targmax=Targmax, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          max_pool_with_argmax, input=input, ksize=ksize, strides=strides,
                                padding=padding, Targmax=Targmax, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "Targmax", _op.get_attr("Targmax"),
            "padding", _op.get_attr("padding"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "MaxPoolWithArgmax", _inputs_flat, _attrs, _result, name)
  _result = _MaxPoolWithArgmaxOutput._make(_result)
  return _result



def max_pool_with_argmax_eager_fallback(input, ksize, strides, padding, Targmax=_dtypes.int64, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function max_pool_with_argmax
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'max_pool_with_argmax' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'max_pool_with_argmax' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if Targmax is None:
    Targmax = _dtypes.int64
  Targmax = _execute.make_type(Targmax, "Targmax")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("ksize", ksize, "strides", strides, "Targmax", Targmax, "padding",
  padding, "T", _attr_T)
  _result = _execute.execute(b"MaxPoolWithArgmax", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MaxPoolWithArgmax", _inputs_flat, _attrs, _result, name)
  _result = _MaxPoolWithArgmaxOutput._make(_result)
  return _result


def nth_element(input, n, reverse=False, name=None):
  r"""Finds values of the `n`-th order statistic for the last dimension.

  If the input is a vector (rank-1), finds the entries which is the nth-smallest
  value in the vector and outputs their values as scalar tensor.

  For matrices (resp. higher rank input), computes the entries which is the
  nth-smallest value in each row (resp. vector along the last dimension). Thus,

      values.shape = input.shape[:-1]

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      1-D or higher with last dimension at least `n+1`.
    n: A `Tensor` of type `int32`.
      0-D. Position of sorted vector to select along the last dimension (along
      each row for matrices). Valid range of n is `[0, input.shape[:-1])`
    reverse: An optional `bool`. Defaults to `False`.
      When set to True, find the nth-largest value in the vector and vice
      versa.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "NthElement",
        name, _ctx._post_execution_callbacks, input, n, "reverse", reverse)
      return _result
    except _core._FallbackException:
      try:
        return nth_element_eager_fallback(
            input, n, reverse=reverse, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if reverse is None:
    reverse = False
  reverse = _execute.make_bool(reverse, "reverse")
  _, _, _op = _op_def_lib._apply_op_helper(
        "NthElement", input=input, n=n, reverse=reverse, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("reverse", _op.get_attr("reverse"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "NthElement", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def nth_element_eager_fallback(input, n, reverse=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function nth_element
  """
  _ctx = ctx if ctx else _context.context()
  if reverse is None:
    reverse = False
  reverse = _execute.make_bool(reverse, "reverse")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  n = _ops.convert_to_tensor(n, _dtypes.int32)
  _inputs_flat = [input, n]
  _attrs = ("reverse", reverse, "T", _attr_T)
  _result = _execute.execute(b"NthElement", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NthElement", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_quantized_avg_pool_outputs = ["output", "min_output", "max_output"]
_QuantizedAvgPoolOutput = _collections.namedtuple(
    "QuantizedAvgPool", _quantized_avg_pool_outputs)


def quantized_avg_pool(input, min_input, max_input, ksize, strides, padding, name=None):
  r"""Produces the average pool of the input tensor for quantized types.

  Args:
    input: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
      4-D with shape `[batch, height, width, channels]`.
    min_input: A `Tensor` of type `float32`.
      The float value that the lowest quantized input value represents.
    max_input: A `Tensor` of type `float32`.
      The float value that the highest quantized input value represents.
    ksize: A list of `ints`.
      The size of the window for each dimension of the input tensor.
      The length must be 4 to match the number of dimensions of the input.
    strides: A list of `ints`.
      The stride of the sliding window for each dimension of the input
      tensor.  The length must be 4 to match the number of dimensions of the input.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, min_output, max_output).

    output: A `Tensor`. Has the same type as `input`.
    min_output: A `Tensor` of type `float32`.
    max_output: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedAvgPool", name, _ctx._post_execution_callbacks, input,
        min_input, max_input, "ksize", ksize, "strides", strides, "padding",
        padding)
      _result = _QuantizedAvgPoolOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_avg_pool_eager_fallback(
            input, min_input, max_input, ksize=ksize, strides=strides,
            padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'quantized_avg_pool' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'quantized_avg_pool' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedAvgPool", input=input, min_input=min_input,
                            max_input=max_input, ksize=ksize, strides=strides,
                            padding=padding, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"))
  _execute.record_gradient(
      "QuantizedAvgPool", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedAvgPoolOutput._make(_result)
  return _result



def quantized_avg_pool_eager_fallback(input, min_input, max_input, ksize, strides, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_avg_pool
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'quantized_avg_pool' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'quantized_avg_pool' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  min_input = _ops.convert_to_tensor(min_input, _dtypes.float32)
  max_input = _ops.convert_to_tensor(max_input, _dtypes.float32)
  _inputs_flat = [input, min_input, max_input]
  _attrs = ("T", _attr_T, "ksize", ksize, "strides", strides, "padding",
  padding)
  _result = _execute.execute(b"QuantizedAvgPool", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedAvgPool", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedAvgPoolOutput._make(_result)
  return _result


_quantized_batch_norm_with_global_normalization_outputs = ["result",
                                                          "result_min",
                                                          "result_max"]
_QuantizedBatchNormWithGlobalNormalizationOutput = _collections.namedtuple(
    "QuantizedBatchNormWithGlobalNormalization",
    _quantized_batch_norm_with_global_normalization_outputs)


def quantized_batch_norm_with_global_normalization(t, t_min, t_max, m, m_min, m_max, v, v_min, v_max, beta, beta_min, beta_max, gamma, gamma_min, gamma_max, out_type, variance_epsilon, scale_after_normalization, name=None):
  r"""Quantized Batch normalization.

  This op is deprecated and will be removed in the future. Prefer
  `tf.nn.batch_normalization`.

  Args:
    t: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
      A 4D input Tensor.
    t_min: A `Tensor` of type `float32`.
      The value represented by the lowest quantized input.
    t_max: A `Tensor` of type `float32`.
      The value represented by the highest quantized input.
    m: A `Tensor`. Must have the same type as `t`.
      A 1D mean Tensor with size matching the last dimension of t.
      This is the first output from tf.nn.moments,
      or a saved moving average thereof.
    m_min: A `Tensor` of type `float32`.
      The value represented by the lowest quantized mean.
    m_max: A `Tensor` of type `float32`.
      The value represented by the highest quantized mean.
    v: A `Tensor`. Must have the same type as `t`.
      A 1D variance Tensor with size matching the last dimension of t.
      This is the second output from tf.nn.moments,
      or a saved moving average thereof.
    v_min: A `Tensor` of type `float32`.
      The value represented by the lowest quantized variance.
    v_max: A `Tensor` of type `float32`.
      The value represented by the highest quantized variance.
    beta: A `Tensor`. Must have the same type as `t`.
      A 1D beta Tensor with size matching the last dimension of t.
      An offset to be added to the normalized tensor.
    beta_min: A `Tensor` of type `float32`.
      The value represented by the lowest quantized offset.
    beta_max: A `Tensor` of type `float32`.
      The value represented by the highest quantized offset.
    gamma: A `Tensor`. Must have the same type as `t`.
      A 1D gamma Tensor with size matching the last dimension of t.
      If "scale_after_normalization" is true, this tensor will be multiplied
      with the normalized tensor.
    gamma_min: A `Tensor` of type `float32`.
      The value represented by the lowest quantized gamma.
    gamma_max: A `Tensor` of type `float32`.
      The value represented by the highest quantized gamma.
    out_type: A `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`.
    variance_epsilon: A `float`. A small float number to avoid dividing by 0.
    scale_after_normalization: A `bool`.
      A bool indicating whether the resulted tensor
      needs to be multiplied with gamma.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (result, result_min, result_max).

    result: A `Tensor` of type `out_type`.
    result_min: A `Tensor` of type `float32`.
    result_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedBatchNormWithGlobalNormalization", name,
        _ctx._post_execution_callbacks, t, t_min, t_max, m, m_min, m_max, v,
        v_min, v_max, beta, beta_min, beta_max, gamma, gamma_min, gamma_max,
        "out_type", out_type, "variance_epsilon", variance_epsilon,
        "scale_after_normalization", scale_after_normalization)
      _result = _QuantizedBatchNormWithGlobalNormalizationOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_batch_norm_with_global_normalization_eager_fallback(
            t, t_min, t_max, m, m_min, m_max, v, v_min, v_max, beta, beta_min,
            beta_max, gamma, gamma_min, gamma_max, out_type=out_type,
            variance_epsilon=variance_epsilon,
            scale_after_normalization=scale_after_normalization, name=name,
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
  variance_epsilon = _execute.make_float(variance_epsilon, "variance_epsilon")
  scale_after_normalization = _execute.make_bool(scale_after_normalization, "scale_after_normalization")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedBatchNormWithGlobalNormalization", t=t, t_min=t_min,
                                                     t_max=t_max, m=m,
                                                     m_min=m_min, m_max=m_max,
                                                     v=v, v_min=v_min,
                                                     v_max=v_max, beta=beta,
                                                     beta_min=beta_min,
                                                     beta_max=beta_max,
                                                     gamma=gamma,
                                                     gamma_min=gamma_min,
                                                     gamma_max=gamma_max,
                                                     out_type=out_type,
                                                     variance_epsilon=variance_epsilon,
                                                     scale_after_normalization=scale_after_normalization,
                                                     name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tinput", _op.get_attr("Tinput"), "out_type",
            _op.get_attr("out_type"), "variance_epsilon",
            _op.get_attr("variance_epsilon"), "scale_after_normalization",
            _op.get_attr("scale_after_normalization"))
  _execute.record_gradient(
      "QuantizedBatchNormWithGlobalNormalization", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedBatchNormWithGlobalNormalizationOutput._make(_result)
  return _result



def quantized_batch_norm_with_global_normalization_eager_fallback(t, t_min, t_max, m, m_min, m_max, v, v_min, v_max, beta, beta_min, beta_max, gamma, gamma_min, gamma_max, out_type, variance_epsilon, scale_after_normalization, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_batch_norm_with_global_normalization
  """
  _ctx = ctx if ctx else _context.context()
  out_type = _execute.make_type(out_type, "out_type")
  variance_epsilon = _execute.make_float(variance_epsilon, "variance_epsilon")
  scale_after_normalization = _execute.make_bool(scale_after_normalization, "scale_after_normalization")
  _attr_Tinput, _inputs_Tinput = _execute.args_to_matching_eager([t, m, v, beta, gamma], _ctx)
  (t, m, v, beta, gamma) = _inputs_Tinput
  t_min = _ops.convert_to_tensor(t_min, _dtypes.float32)
  t_max = _ops.convert_to_tensor(t_max, _dtypes.float32)
  m_min = _ops.convert_to_tensor(m_min, _dtypes.float32)
  m_max = _ops.convert_to_tensor(m_max, _dtypes.float32)
  v_min = _ops.convert_to_tensor(v_min, _dtypes.float32)
  v_max = _ops.convert_to_tensor(v_max, _dtypes.float32)
  beta_min = _ops.convert_to_tensor(beta_min, _dtypes.float32)
  beta_max = _ops.convert_to_tensor(beta_max, _dtypes.float32)
  gamma_min = _ops.convert_to_tensor(gamma_min, _dtypes.float32)
  gamma_max = _ops.convert_to_tensor(gamma_max, _dtypes.float32)
  _inputs_flat = [t, t_min, t_max, m, m_min, m_max, v, v_min, v_max, beta, beta_min, beta_max, gamma, gamma_min, gamma_max]
  _attrs = ("Tinput", _attr_Tinput, "out_type", out_type, "variance_epsilon",
  variance_epsilon, "scale_after_normalization", scale_after_normalization)
  _result = _execute.execute(b"QuantizedBatchNormWithGlobalNormalization", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QuantizedBatchNormWithGlobalNormalization", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedBatchNormWithGlobalNormalizationOutput._make(_result)
  return _result


_quantized_bias_add_outputs = ["output", "min_out", "max_out"]
_QuantizedBiasAddOutput = _collections.namedtuple(
    "QuantizedBiasAdd", _quantized_bias_add_outputs)


def quantized_bias_add(input, bias, min_input, max_input, min_bias, max_bias, out_type, name=None):
  r"""Adds Tensor 'bias' to Tensor 'input' for Quantized types.

  Broadcasts the values of bias on dimensions 0..N-2 of 'input'.

  Args:
    input: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    bias: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
      A 1D bias Tensor with size matching the last dimension of 'input'.
    min_input: A `Tensor` of type `float32`.
      The float value that the lowest quantized input value represents.
    max_input: A `Tensor` of type `float32`.
      The float value that the highest quantized input value represents.
    min_bias: A `Tensor` of type `float32`.
      The float value that the lowest quantized bias value represents.
    max_bias: A `Tensor` of type `float32`.
      The float value that the highest quantized bias value represents.
    out_type: A `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, min_out, max_out).

    output: A `Tensor` of type `out_type`.
    min_out: A `Tensor` of type `float32`.
    max_out: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedBiasAdd", name, _ctx._post_execution_callbacks, input, bias,
        min_input, max_input, min_bias, max_bias, "out_type", out_type)
      _result = _QuantizedBiasAddOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_bias_add_eager_fallback(
            input, bias, min_input, max_input, min_bias, max_bias,
            out_type=out_type, name=name, ctx=_ctx)
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
        "QuantizedBiasAdd", input=input, bias=bias, min_input=min_input,
                            max_input=max_input, min_bias=min_bias,
                            max_bias=max_bias, out_type=out_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T1", _op.get_attr("T1"), "T2", _op.get_attr("T2"), "out_type",
            _op.get_attr("out_type"))
  _execute.record_gradient(
      "QuantizedBiasAdd", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedBiasAddOutput._make(_result)
  return _result



def quantized_bias_add_eager_fallback(input, bias, min_input, max_input, min_bias, max_bias, out_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_bias_add
  """
  _ctx = ctx if ctx else _context.context()
  out_type = _execute.make_type(out_type, "out_type")
  _attr_T1, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_T2, (bias,) = _execute.args_to_matching_eager([bias], _ctx)
  min_input = _ops.convert_to_tensor(min_input, _dtypes.float32)
  max_input = _ops.convert_to_tensor(max_input, _dtypes.float32)
  min_bias = _ops.convert_to_tensor(min_bias, _dtypes.float32)
  max_bias = _ops.convert_to_tensor(max_bias, _dtypes.float32)
  _inputs_flat = [input, bias, min_input, max_input, min_bias, max_bias]
  _attrs = ("T1", _attr_T1, "T2", _attr_T2, "out_type", out_type)
  _result = _execute.execute(b"QuantizedBiasAdd", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedBiasAdd", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedBiasAddOutput._make(_result)
  return _result


_quantized_conv2d_outputs = ["output", "min_output", "max_output"]
_QuantizedConv2DOutput = _collections.namedtuple(
    "QuantizedConv2D", _quantized_conv2d_outputs)


def quantized_conv2d(input, filter, min_input, max_input, min_filter, max_filter, strides, padding, out_type=_dtypes.qint32, dilations=[1, 1, 1, 1], name=None):
  r"""Computes a 2D convolution given quantized 4D input and filter tensors.

  The inputs are quantized tensors where the lowest value represents the real
  number of the associated minimum, and the highest represents the maximum.
  This means that you can only interpret the quantized output in the same way, by
  taking the returned minimum and maximum values into account.

  Args:
    input: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    filter: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
      filter's input_depth dimension must match input's depth dimensions.
    min_input: A `Tensor` of type `float32`.
      The float value that the lowest quantized input value represents.
    max_input: A `Tensor` of type `float32`.
      The float value that the highest quantized input value represents.
    min_filter: A `Tensor` of type `float32`.
      The float value that the lowest quantized filter value represents.
    max_filter: A `Tensor` of type `float32`.
      The float value that the highest quantized filter value represents.
    strides: A list of `ints`.
      The stride of the sliding window for each dimension of the input
      tensor.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    out_type: An optional `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`. Defaults to `tf.qint32`.
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1]`.
      1-D tensor of length 4.  The dilation factor for each dimension of
      `input`. If set to k > 1, there will be k-1 skipped cells between each
      filter element on that dimension. The dimension order is determined by the
      value of `data_format`, see above for details. Dilations in the batch and
      depth dimensions must be 1.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, min_output, max_output).

    output: A `Tensor` of type `out_type`.
    min_output: A `Tensor` of type `float32`.
    max_output: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedConv2D", name, _ctx._post_execution_callbacks, input,
        filter, min_input, max_input, min_filter, max_filter, "out_type",
        out_type, "strides", strides, "padding", padding, "dilations",
        dilations)
      _result = _QuantizedConv2DOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_conv2d_eager_fallback(
            input, filter, min_input, max_input, min_filter, max_filter,
            out_type=out_type, strides=strides, padding=padding,
            dilations=dilations, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'quantized_conv2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if out_type is None:
    out_type = _dtypes.qint32
  out_type = _execute.make_type(out_type, "out_type")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'quantized_conv2d' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedConv2D", input=input, filter=filter, min_input=min_input,
                           max_input=max_input, min_filter=min_filter,
                           max_filter=max_filter, strides=strides,
                           padding=padding, out_type=out_type,
                           dilations=dilations, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tinput", _op.get_attr("Tinput"), "Tfilter",
            _op.get_attr("Tfilter"), "out_type", _op.get_attr("out_type"),
            "strides", _op.get_attr("strides"), "padding",
            _op.get_attr("padding"), "dilations", _op.get_attr("dilations"))
  _execute.record_gradient(
      "QuantizedConv2D", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedConv2DOutput._make(_result)
  return _result



def quantized_conv2d_eager_fallback(input, filter, min_input, max_input, min_filter, max_filter, strides, padding, out_type=_dtypes.qint32, dilations=[1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_conv2d
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'quantized_conv2d' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if out_type is None:
    out_type = _dtypes.qint32
  out_type = _execute.make_type(out_type, "out_type")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'quantized_conv2d' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_Tinput, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tfilter, (filter,) = _execute.args_to_matching_eager([filter], _ctx)
  min_input = _ops.convert_to_tensor(min_input, _dtypes.float32)
  max_input = _ops.convert_to_tensor(max_input, _dtypes.float32)
  min_filter = _ops.convert_to_tensor(min_filter, _dtypes.float32)
  max_filter = _ops.convert_to_tensor(max_filter, _dtypes.float32)
  _inputs_flat = [input, filter, min_input, max_input, min_filter, max_filter]
  _attrs = ("Tinput", _attr_Tinput, "Tfilter", _attr_Tfilter, "out_type",
  out_type, "strides", strides, "padding", padding, "dilations", dilations)
  _result = _execute.execute(b"QuantizedConv2D", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedConv2D", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedConv2DOutput._make(_result)
  return _result


_quantized_max_pool_outputs = ["output", "min_output", "max_output"]
_QuantizedMaxPoolOutput = _collections.namedtuple(
    "QuantizedMaxPool", _quantized_max_pool_outputs)


def quantized_max_pool(input, min_input, max_input, ksize, strides, padding, name=None):
  r"""Produces the max pool of the input tensor for quantized types.

  Args:
    input: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
      The 4D (batch x rows x cols x depth) Tensor to MaxReduce over.
    min_input: A `Tensor` of type `float32`.
      The float value that the lowest quantized input value represents.
    max_input: A `Tensor` of type `float32`.
      The float value that the highest quantized input value represents.
    ksize: A list of `ints`.
      The size of the window for each dimension of the input tensor.
      The length must be 4 to match the number of dimensions of the input.
    strides: A list of `ints`.
      The stride of the sliding window for each dimension of the input
      tensor. The length must be 4 to match the number of dimensions of the input.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, min_output, max_output).

    output: A `Tensor`. Has the same type as `input`.
    min_output: A `Tensor` of type `float32`.
    max_output: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedMaxPool", name, _ctx._post_execution_callbacks, input,
        min_input, max_input, "ksize", ksize, "strides", strides, "padding",
        padding)
      _result = _QuantizedMaxPoolOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_max_pool_eager_fallback(
            input, min_input, max_input, ksize=ksize, strides=strides,
            padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'quantized_max_pool' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'quantized_max_pool' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedMaxPool", input=input, min_input=min_input,
                            max_input=max_input, ksize=ksize, strides=strides,
                            padding=padding, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "ksize", _op.get_attr("ksize"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"))
  _execute.record_gradient(
      "QuantizedMaxPool", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedMaxPoolOutput._make(_result)
  return _result



def quantized_max_pool_eager_fallback(input, min_input, max_input, ksize, strides, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_max_pool
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksize, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksize' argument to "
        "'quantized_max_pool' Op, not %r." % ksize)
  ksize = [_execute.make_int(_i, "ksize") for _i in ksize]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'quantized_max_pool' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  min_input = _ops.convert_to_tensor(min_input, _dtypes.float32)
  max_input = _ops.convert_to_tensor(max_input, _dtypes.float32)
  _inputs_flat = [input, min_input, max_input]
  _attrs = ("T", _attr_T, "ksize", ksize, "strides", strides, "padding",
  padding)
  _result = _execute.execute(b"QuantizedMaxPool", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedMaxPool", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedMaxPoolOutput._make(_result)
  return _result


_quantized_relu_outputs = ["activations", "min_activations",
                          "max_activations"]
_QuantizedReluOutput = _collections.namedtuple(
    "QuantizedRelu", _quantized_relu_outputs)


def quantized_relu(features, min_features, max_features, out_type=_dtypes.quint8, name=None):
  r"""Computes Quantized Rectified Linear: `max(features, 0)`

  Args:
    features: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    min_features: A `Tensor` of type `float32`.
      The float value that the lowest quantized value represents.
    max_features: A `Tensor` of type `float32`.
      The float value that the highest quantized value represents.
    out_type: An optional `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`. Defaults to `tf.quint8`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (activations, min_activations, max_activations).

    activations: A `Tensor` of type `out_type`.
    min_activations: A `Tensor` of type `float32`.
    max_activations: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedRelu", name, _ctx._post_execution_callbacks, features,
        min_features, max_features, "out_type", out_type)
      _result = _QuantizedReluOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_relu_eager_fallback(
            features, min_features, max_features, out_type=out_type,
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
  if out_type is None:
    out_type = _dtypes.quint8
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedRelu", features=features, min_features=min_features,
                         max_features=max_features, out_type=out_type,
                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tinput", _op.get_attr("Tinput"), "out_type",
            _op.get_attr("out_type"))
  _execute.record_gradient(
      "QuantizedRelu", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedReluOutput._make(_result)
  return _result



def quantized_relu_eager_fallback(features, min_features, max_features, out_type=_dtypes.quint8, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_relu
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.quint8
  out_type = _execute.make_type(out_type, "out_type")
  _attr_Tinput, (features,) = _execute.args_to_matching_eager([features], _ctx)
  min_features = _ops.convert_to_tensor(min_features, _dtypes.float32)
  max_features = _ops.convert_to_tensor(max_features, _dtypes.float32)
  _inputs_flat = [features, min_features, max_features]
  _attrs = ("Tinput", _attr_Tinput, "out_type", out_type)
  _result = _execute.execute(b"QuantizedRelu", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedRelu", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedReluOutput._make(_result)
  return _result


_quantized_relu6_outputs = ["activations", "min_activations",
                           "max_activations"]
_QuantizedRelu6Output = _collections.namedtuple(
    "QuantizedRelu6", _quantized_relu6_outputs)


def quantized_relu6(features, min_features, max_features, out_type=_dtypes.quint8, name=None):
  r"""Computes Quantized Rectified Linear 6: `min(max(features, 0), 6)`

  Args:
    features: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    min_features: A `Tensor` of type `float32`.
      The float value that the lowest quantized value represents.
    max_features: A `Tensor` of type `float32`.
      The float value that the highest quantized value represents.
    out_type: An optional `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`. Defaults to `tf.quint8`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (activations, min_activations, max_activations).

    activations: A `Tensor` of type `out_type`.
    min_activations: A `Tensor` of type `float32`.
    max_activations: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedRelu6", name, _ctx._post_execution_callbacks, features,
        min_features, max_features, "out_type", out_type)
      _result = _QuantizedRelu6Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_relu6_eager_fallback(
            features, min_features, max_features, out_type=out_type,
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
  if out_type is None:
    out_type = _dtypes.quint8
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedRelu6", features=features, min_features=min_features,
                          max_features=max_features, out_type=out_type,
                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tinput", _op.get_attr("Tinput"), "out_type",
            _op.get_attr("out_type"))
  _execute.record_gradient(
      "QuantizedRelu6", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedRelu6Output._make(_result)
  return _result



def quantized_relu6_eager_fallback(features, min_features, max_features, out_type=_dtypes.quint8, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_relu6
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.quint8
  out_type = _execute.make_type(out_type, "out_type")
  _attr_Tinput, (features,) = _execute.args_to_matching_eager([features], _ctx)
  min_features = _ops.convert_to_tensor(min_features, _dtypes.float32)
  max_features = _ops.convert_to_tensor(max_features, _dtypes.float32)
  _inputs_flat = [features, min_features, max_features]
  _attrs = ("Tinput", _attr_Tinput, "out_type", out_type)
  _result = _execute.execute(b"QuantizedRelu6", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedRelu6", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedRelu6Output._make(_result)
  return _result


_quantized_relu_x_outputs = ["activations", "min_activations",
                            "max_activations"]
_QuantizedReluXOutput = _collections.namedtuple(
    "QuantizedReluX", _quantized_relu_x_outputs)


def quantized_relu_x(features, max_value, min_features, max_features, out_type=_dtypes.quint8, name=None):
  r"""Computes Quantized Rectified Linear X: `min(max(features, 0), max_value)`

  Args:
    features: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    max_value: A `Tensor` of type `float32`.
    min_features: A `Tensor` of type `float32`.
      The float value that the lowest quantized value represents.
    max_features: A `Tensor` of type `float32`.
      The float value that the highest quantized value represents.
    out_type: An optional `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`. Defaults to `tf.quint8`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (activations, min_activations, max_activations).

    activations: A `Tensor` of type `out_type`.
    min_activations: A `Tensor` of type `float32`.
    max_activations: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedReluX", name, _ctx._post_execution_callbacks, features,
        max_value, min_features, max_features, "out_type", out_type)
      _result = _QuantizedReluXOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_relu_x_eager_fallback(
            features, max_value, min_features, max_features,
            out_type=out_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_type is None:
    out_type = _dtypes.quint8
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedReluX", features=features, max_value=max_value,
                          min_features=min_features,
                          max_features=max_features, out_type=out_type,
                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tinput", _op.get_attr("Tinput"), "out_type",
            _op.get_attr("out_type"))
  _execute.record_gradient(
      "QuantizedReluX", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedReluXOutput._make(_result)
  return _result



def quantized_relu_x_eager_fallback(features, max_value, min_features, max_features, out_type=_dtypes.quint8, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_relu_x
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.quint8
  out_type = _execute.make_type(out_type, "out_type")
  _attr_Tinput, (features,) = _execute.args_to_matching_eager([features], _ctx)
  max_value = _ops.convert_to_tensor(max_value, _dtypes.float32)
  min_features = _ops.convert_to_tensor(min_features, _dtypes.float32)
  max_features = _ops.convert_to_tensor(max_features, _dtypes.float32)
  _inputs_flat = [features, max_value, min_features, max_features]
  _attrs = ("Tinput", _attr_Tinput, "out_type", out_type)
  _result = _execute.execute(b"QuantizedReluX", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedReluX", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedReluXOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.relu')
def relu(features, name=None):
  r"""Computes rectified linear: `max(features, 0)`.

  Args:
    features: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`, `qint8`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `features`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Relu", name,
        _ctx._post_execution_callbacks, features)
      return _result
    except _core._FallbackException:
      try:
        return relu_eager_fallback(
            features, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              relu, features=features, name=name)
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
        "Relu", features=features, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          relu, features=features, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Relu", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def relu_eager_fallback(features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function relu
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (features,) = _execute.args_to_matching_eager([features], _ctx)
  _inputs_flat = [features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Relu", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Relu", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def relu6(features, name=None):
  r"""Computes rectified linear 6: `min(max(features, 0), 6)`.

  Args:
    features: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `features`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Relu6", name,
        _ctx._post_execution_callbacks, features)
      return _result
    except _core._FallbackException:
      try:
        return relu6_eager_fallback(
            features, name=name, ctx=_ctx)
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
        "Relu6", features=features, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Relu6", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def relu6_eager_fallback(features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function relu6
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (features,) = _execute.args_to_matching_eager([features], _ctx)
  _inputs_flat = [features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Relu6", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Relu6", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def relu6_grad(gradients, features, name=None):
  r"""Computes rectified linear 6 gradients for a Relu6 operation.

  Args:
    gradients: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      The backpropagated gradients to the corresponding Relu6 operation.
    features: A `Tensor`. Must have the same type as `gradients`.
      The features passed as input to the corresponding Relu6 operation, or
      its output; using either one produces the same result.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `gradients`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Relu6Grad",
        name, _ctx._post_execution_callbacks, gradients, features)
      return _result
    except _core._FallbackException:
      try:
        return relu6_grad_eager_fallback(
            gradients, features, name=name, ctx=_ctx)
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
        "Relu6Grad", gradients=gradients, features=features, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Relu6Grad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def relu6_grad_eager_fallback(gradients, features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function relu6_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([gradients, features], _ctx)
  (gradients, features) = _inputs_T
  _inputs_flat = [gradients, features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Relu6Grad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Relu6Grad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def relu_grad(gradients, features, name=None):
  r"""Computes rectified linear gradients for a Relu operation.

  Args:
    gradients: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      The backpropagated gradients to the corresponding Relu operation.
    features: A `Tensor`. Must have the same type as `gradients`.
      The features passed as input to the corresponding Relu operation, OR
      the outputs of that operation (both work equivalently).
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `gradients`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ReluGrad",
        name, _ctx._post_execution_callbacks, gradients, features)
      return _result
    except _core._FallbackException:
      try:
        return relu_grad_eager_fallback(
            gradients, features, name=name, ctx=_ctx)
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
        "ReluGrad", gradients=gradients, features=features, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "ReluGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def relu_grad_eager_fallback(gradients, features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function relu_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([gradients, features], _ctx)
  (gradients, features) = _inputs_T
  _inputs_flat = [gradients, features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"ReluGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ReluGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.selu')
def selu(features, name=None):
  r"""Computes scaled exponential linear: `scale * alpha * (exp(features) - 1)`

  if < 0, `scale * features` otherwise.

  To be used together with
  `initializer = tf.variance_scaling_initializer(factor=1.0, mode='FAN_IN')`.
  For correct dropout, use `tf.contrib.nn.alpha_dropout`.

  See [Self-Normalizing Neural Networks](https://arxiv.org/abs/1706.02515)

  Args:
    features: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `features`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Selu", name,
        _ctx._post_execution_callbacks, features)
      return _result
    except _core._FallbackException:
      try:
        return selu_eager_fallback(
            features, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              selu, features=features, name=name)
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
        "Selu", features=features, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          selu, features=features, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Selu", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def selu_eager_fallback(features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function selu
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (features,) = _execute.args_to_matching_eager([features], _ctx)
  _inputs_flat = [features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Selu", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Selu", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def selu_grad(gradients, outputs, name=None):
  r"""Computes gradients for the scaled exponential linear (Selu) operation.

  Args:
    gradients: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      The backpropagated gradients to the corresponding Selu operation.
    outputs: A `Tensor`. Must have the same type as `gradients`.
      The outputs of the corresponding Selu operation.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `gradients`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SeluGrad",
        name, _ctx._post_execution_callbacks, gradients, outputs)
      return _result
    except _core._FallbackException:
      try:
        return selu_grad_eager_fallback(
            gradients, outputs, name=name, ctx=_ctx)
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
        "SeluGrad", gradients=gradients, outputs=outputs, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SeluGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def selu_grad_eager_fallback(gradients, outputs, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function selu_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([gradients, outputs], _ctx)
  (gradients, outputs) = _inputs_T
  _inputs_flat = [gradients, outputs]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SeluGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SeluGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def softmax(logits, name=None):
  r"""Computes softmax activations.

  For each batch `i` and class `j` we have

      $$softmax[i, j] = exp(logits[i, j]) / sum_j(exp(logits[i, j]))$$

  Args:
    logits: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      2-D with shape `[batch_size, num_classes]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `logits`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Softmax",
        name, _ctx._post_execution_callbacks, logits)
      return _result
    except _core._FallbackException:
      try:
        return softmax_eager_fallback(
            logits, name=name, ctx=_ctx)
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
        "Softmax", logits=logits, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Softmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def softmax_eager_fallback(logits, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function softmax
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (logits,) = _execute.args_to_matching_eager([logits], _ctx)
  _inputs_flat = [logits]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Softmax", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Softmax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_softmax_cross_entropy_with_logits_outputs = ["loss", "backprop"]
_SoftmaxCrossEntropyWithLogitsOutput = _collections.namedtuple(
    "SoftmaxCrossEntropyWithLogits",
    _softmax_cross_entropy_with_logits_outputs)


def softmax_cross_entropy_with_logits(features, labels, name=None):
  r"""Computes softmax cross entropy cost and gradients to backpropagate.

  Inputs are the logits, not probabilities.

  Args:
    features: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      batch_size x num_classes matrix
    labels: A `Tensor`. Must have the same type as `features`.
      batch_size x num_classes matrix
      The caller must ensure that each batch of labels represents a valid
      probability distribution.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (loss, backprop).

    loss: A `Tensor`. Has the same type as `features`.
    backprop: A `Tensor`. Has the same type as `features`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SoftmaxCrossEntropyWithLogits", name, _ctx._post_execution_callbacks,
        features, labels)
      _result = _SoftmaxCrossEntropyWithLogitsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return softmax_cross_entropy_with_logits_eager_fallback(
            features, labels, name=name, ctx=_ctx)
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
        "SoftmaxCrossEntropyWithLogits", features=features, labels=labels,
                                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SoftmaxCrossEntropyWithLogits", _inputs_flat, _attrs, _result, name)
  _result = _SoftmaxCrossEntropyWithLogitsOutput._make(_result)
  return _result



def softmax_cross_entropy_with_logits_eager_fallback(features, labels, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function softmax_cross_entropy_with_logits
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([features, labels], _ctx)
  (features, labels) = _inputs_T
  _inputs_flat = [features, labels]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SoftmaxCrossEntropyWithLogits", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SoftmaxCrossEntropyWithLogits", _inputs_flat, _attrs, _result, name)
  _result = _SoftmaxCrossEntropyWithLogitsOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.softplus', 'nn.softplus')
def softplus(features, name=None):
  r"""Computes softplus: `log(exp(features) + 1)`.

  Args:
    features: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `features`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Softplus",
        name, _ctx._post_execution_callbacks, features)
      return _result
    except _core._FallbackException:
      try:
        return softplus_eager_fallback(
            features, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              softplus, features=features, name=name)
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
        "Softplus", features=features, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          softplus, features=features, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Softplus", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def softplus_eager_fallback(features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function softplus
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (features,) = _execute.args_to_matching_eager([features], _ctx)
  _inputs_flat = [features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Softplus", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Softplus", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def softplus_grad(gradients, features, name=None):
  r"""Computes softplus gradients for a softplus operation.

  Args:
    gradients: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      The backpropagated gradients to the corresponding softplus operation.
    features: A `Tensor`. Must have the same type as `gradients`.
      The features passed as input to the corresponding softplus operation.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `gradients`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SoftplusGrad",
        name, _ctx._post_execution_callbacks, gradients, features)
      return _result
    except _core._FallbackException:
      try:
        return softplus_grad_eager_fallback(
            gradients, features, name=name, ctx=_ctx)
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
        "SoftplusGrad", gradients=gradients, features=features, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SoftplusGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def softplus_grad_eager_fallback(gradients, features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function softplus_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([gradients, features], _ctx)
  (gradients, features) = _inputs_T
  _inputs_flat = [gradients, features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SoftplusGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SoftplusGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('nn.softsign', 'math.softsign')
def softsign(features, name=None):
  r"""Computes softsign: `features / (abs(features) + 1)`.

  Args:
    features: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `features`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Softsign",
        name, _ctx._post_execution_callbacks, features)
      return _result
    except _core._FallbackException:
      try:
        return softsign_eager_fallback(
            features, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              softsign, features=features, name=name)
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
        "Softsign", features=features, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          softsign, features=features, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Softsign", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def softsign_eager_fallback(features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function softsign
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (features,) = _execute.args_to_matching_eager([features], _ctx)
  _inputs_flat = [features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Softsign", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Softsign", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def softsign_grad(gradients, features, name=None):
  r"""Computes softsign gradients for a softsign operation.

  Args:
    gradients: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      The backpropagated gradients to the corresponding softsign operation.
    features: A `Tensor`. Must have the same type as `gradients`.
      The features passed as input to the corresponding softsign operation.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `gradients`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SoftsignGrad",
        name, _ctx._post_execution_callbacks, gradients, features)
      return _result
    except _core._FallbackException:
      try:
        return softsign_grad_eager_fallback(
            gradients, features, name=name, ctx=_ctx)
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
        "SoftsignGrad", gradients=gradients, features=features, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "SoftsignGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def softsign_grad_eager_fallback(gradients, features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function softsign_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([gradients, features], _ctx)
  (gradients, features) = _inputs_T
  _inputs_flat = [gradients, features]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SoftsignGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SoftsignGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_sparse_softmax_cross_entropy_with_logits_outputs = ["loss", "backprop"]
_SparseSoftmaxCrossEntropyWithLogitsOutput = _collections.namedtuple(
    "SparseSoftmaxCrossEntropyWithLogits",
    _sparse_softmax_cross_entropy_with_logits_outputs)


def sparse_softmax_cross_entropy_with_logits(features, labels, name=None):
  r"""Computes softmax cross entropy cost and gradients to backpropagate.

  Unlike `SoftmaxCrossEntropyWithLogits`, this operation does not accept
  a matrix of label probabilities, but rather a single label per row
  of features.  This label is considered to have probability 1.0 for the
  given row.

  Inputs are the logits, not probabilities.

  Args:
    features: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      batch_size x num_classes matrix
    labels: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      batch_size vector with values in [0, num_classes).
      This is the label for the given minibatch entry.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (loss, backprop).

    loss: A `Tensor`. Has the same type as `features`.
    backprop: A `Tensor`. Has the same type as `features`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseSoftmaxCrossEntropyWithLogits", name,
        _ctx._post_execution_callbacks, features, labels)
      _result = _SparseSoftmaxCrossEntropyWithLogitsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_softmax_cross_entropy_with_logits_eager_fallback(
            features, labels, name=name, ctx=_ctx)
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
        "SparseSoftmaxCrossEntropyWithLogits", features=features,
                                               labels=labels, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tlabels", _op.get_attr("Tlabels"))
  _execute.record_gradient(
      "SparseSoftmaxCrossEntropyWithLogits", _inputs_flat, _attrs, _result, name)
  _result = _SparseSoftmaxCrossEntropyWithLogitsOutput._make(_result)
  return _result



def sparse_softmax_cross_entropy_with_logits_eager_fallback(features, labels, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_softmax_cross_entropy_with_logits
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (features,) = _execute.args_to_matching_eager([features], _ctx)
  _attr_Tlabels, (labels,) = _execute.args_to_matching_eager([labels], _ctx, _dtypes.int64)
  _inputs_flat = [features, labels]
  _attrs = ("T", _attr_T, "Tlabels", _attr_Tlabels)
  _result = _execute.execute(b"SparseSoftmaxCrossEntropyWithLogits", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SparseSoftmaxCrossEntropyWithLogits", _inputs_flat, _attrs, _result, name)
  _result = _SparseSoftmaxCrossEntropyWithLogitsOutput._make(_result)
  return _result


_top_k_outputs = ["values", "indices"]
_TopKOutput = _collections.namedtuple(
    "TopK", _top_k_outputs)


def top_k(input, k, sorted=True, name=None):
  r"""Finds values and indices of the `k` largest elements for the last dimension.

  If the input is a vector (rank-1), finds the `k` largest entries in the vector
  and outputs their values and indices as vectors.  Thus `values[j]` is the
  `j`-th largest entry in `input`, and its index is `indices[j]`.

  For matrices (resp. higher rank input), computes the top `k` entries in each
  row (resp. vector along the last dimension).  Thus,

      values.shape = indices.shape = input.shape[:-1] + [k]

  If two elements are equal, the lower-index element appears first.

  If `k` varies dynamically, use `TopKV2` below.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      1-D or higher with last dimension at least `k`.
    k: An `int` that is `>= 0`.
      Number of top elements to look for along the last dimension (along each
      row for matrices).
    sorted: An optional `bool`. Defaults to `True`.
      If true the resulting `k` elements will be sorted by the values in
      descending order.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (values, indices).

    values: A `Tensor`. Has the same type as `input`.
    indices: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TopK", name,
        _ctx._post_execution_callbacks, input, "k", k, "sorted", sorted)
      _result = _TopKOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return top_k_eager_fallback(
            input, k=k, sorted=sorted, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  k = _execute.make_int(k, "k")
  if sorted is None:
    sorted = True
  sorted = _execute.make_bool(sorted, "sorted")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TopK", input=input, k=k, sorted=sorted, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("k", _op.get_attr("k"), "sorted", _op.get_attr("sorted"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "TopK", _inputs_flat, _attrs, _result, name)
  _result = _TopKOutput._make(_result)
  return _result



def top_k_eager_fallback(input, k, sorted=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function top_k
  """
  _ctx = ctx if ctx else _context.context()
  k = _execute.make_int(k, "k")
  if sorted is None:
    sorted = True
  sorted = _execute.make_bool(sorted, "sorted")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("k", k, "sorted", sorted, "T", _attr_T)
  _result = _execute.execute(b"TopK", 2, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "TopK", _inputs_flat, _attrs, _result, name)
  _result = _TopKOutput._make(_result)
  return _result


_top_kv2_outputs = ["values", "indices"]
_TopKV2Output = _collections.namedtuple(
    "TopKV2", _top_kv2_outputs)


def top_kv2(input, k, sorted=True, name=None):
  r"""Finds values and indices of the `k` largest elements for the last dimension.

  If the input is a vector (rank-1), finds the `k` largest entries in the vector
  and outputs their values and indices as vectors.  Thus `values[j]` is the
  `j`-th largest entry in `input`, and its index is `indices[j]`.

  For matrices (resp. higher rank input), computes the top `k` entries in each
  row (resp. vector along the last dimension).  Thus,

      values.shape = indices.shape = input.shape[:-1] + [k]

  If two elements are equal, the lower-index element appears first.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      1-D or higher with last dimension at least `k`.
    k: A `Tensor` of type `int32`.
      0-D.  Number of top elements to look for along the last dimension (along each
      row for matrices).
    sorted: An optional `bool`. Defaults to `True`.
      If true the resulting `k` elements will be sorted by the values in
      descending order.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (values, indices).

    values: A `Tensor`. Has the same type as `input`.
    indices: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TopKV2", name,
        _ctx._post_execution_callbacks, input, k, "sorted", sorted)
      _result = _TopKV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return top_kv2_eager_fallback(
            input, k, sorted=sorted, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if sorted is None:
    sorted = True
  sorted = _execute.make_bool(sorted, "sorted")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TopKV2", input=input, k=k, sorted=sorted, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("sorted", _op.get_attr("sorted"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "TopKV2", _inputs_flat, _attrs, _result, name)
  _result = _TopKV2Output._make(_result)
  return _result



def top_kv2_eager_fallback(input, k, sorted=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function top_kv2
  """
  _ctx = ctx if ctx else _context.context()
  if sorted is None:
    sorted = True
  sorted = _execute.make_bool(sorted, "sorted")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  k = _ops.convert_to_tensor(k, _dtypes.int32)
  _inputs_flat = [input, k]
  _attrs = ("sorted", sorted, "T", _attr_T)
  _result = _execute.execute(b"TopKV2", 2, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "TopKV2", _inputs_flat, _attrs, _result, name)
  _result = _TopKV2Output._make(_result)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "AvgPool"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "AvgPool3D"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NDHWC"
#     }
#     allowed_values {
#       list {
#         s: "NDHWC"
#         s: "NCDHW"
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "AvgPool3DGrad"
#   input_arg {
#     name: "orig_input_shape"
#     type: DT_INT32
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
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NDHWC"
#     }
#     allowed_values {
#       list {
#         s: "NDHWC"
#         s: "NCDHW"
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "AvgPoolGrad"
#   input_arg {
#     name: "orig_input_shape"
#     type: DT_INT32
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
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "BatchNormWithGlobalNormalization"
#   input_arg {
#     name: "t"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "m"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "v"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "gamma"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "result"
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
#     name: "variance_epsilon"
#     type: "float"
#   }
#   attr {
#     name: "scale_after_normalization"
#     type: "bool"
#   }
#   deprecation {
#     version: 9
#     explanation: "Use tf.nn.batch_normalization()"
#   }
# }
# op {
#   name: "BatchNormWithGlobalNormalizationGrad"
#   input_arg {
#     name: "t"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "m"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "v"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "gamma"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "backprop"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "dx"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "dm"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "dv"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "db"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "dg"
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
#     name: "variance_epsilon"
#     type: "float"
#   }
#   attr {
#     name: "scale_after_normalization"
#     type: "bool"
#   }
#   deprecation {
#     version: 9
#     explanation: "Use tf.nn.batch_normalization()"
#   }
# }
# op {
#   name: "BiasAdd"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "bias"
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
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
# }
# op {
#   name: "BiasAddGrad"
#   input_arg {
#     name: "out_backprop"
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
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
# }
# op {
#   name: "BiasAddV1"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "bias"
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
#   name: "Conv2D"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "use_cudnn_on_gpu"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
# op {
#   name: "Conv2DBackpropFilter"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter_sizes"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "out_backprop"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "use_cudnn_on_gpu"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
# op {
#   name: "Conv2DBackpropInput"
#   input_arg {
#     name: "input_sizes"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "filter"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "out_backprop"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "use_cudnn_on_gpu"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
# op {
#   name: "Conv3D"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NDHWC"
#     }
#     allowed_values {
#       list {
#         s: "NDHWC"
#         s: "NCDHW"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
# op {
#   name: "Conv3DBackpropFilter"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "out_backprop"
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
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
#   deprecation {
#     version: 10
#     explanation: "Use Conv3DBackpropFilterV2"
#   }
# }
# op {
#   name: "Conv3DBackpropFilterV2"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter_sizes"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "out_backprop"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NDHWC"
#     }
#     allowed_values {
#       list {
#         s: "NDHWC"
#         s: "NCDHW"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
# op {
#   name: "Conv3DBackpropInput"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "out_backprop"
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
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
#   deprecation {
#     version: 10
#     explanation: "Use Conv3DBackpropInputV2"
#   }
# }
# op {
#   name: "Conv3DBackpropInputV2"
#   input_arg {
#     name: "input_sizes"
#     type_attr: "Tshape"
#   }
#   input_arg {
#     name: "filter"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "out_backprop"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NDHWC"
#     }
#     allowed_values {
#       list {
#         s: "NDHWC"
#         s: "NCDHW"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
#   attr {
#     name: "Tshape"
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
#   name: "DataFormatDimMap"
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
#     name: "src_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#   }
#   attr {
#     name: "dst_format"
#     type: "string"
#     default_value {
#       s: "NCHW"
#     }
#   }
# }
# op {
#   name: "DataFormatVecPermute"
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
#     name: "src_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#   }
#   attr {
#     name: "dst_format"
#     type: "string"
#     default_value {
#       s: "NCHW"
#     }
#   }
# }
# op {
#   name: "DepthwiseConv2dNative"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
# op {
#   name: "DepthwiseConv2dNativeBackpropFilter"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter_sizes"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "out_backprop"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
# op {
#   name: "DepthwiseConv2dNativeBackpropInput"
#   input_arg {
#     name: "input_sizes"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "filter"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "out_backprop"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
# op {
#   name: "Dilation2D"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter"
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
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "rates"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
# }
# op {
#   name: "Dilation2DBackpropFilter"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "out_backprop"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "filter_backprop"
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
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "rates"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
# }
# op {
#   name: "Dilation2DBackpropInput"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "out_backprop"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "in_backprop"
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
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "rates"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
# }
# op {
#   name: "Elu"
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "activations"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "EluGrad"
#   input_arg {
#     name: "gradients"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "outputs"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "backprops"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "FractionalAvgPool"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "row_pooling_sequence"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "col_pooling_sequence"
#     type: DT_INT64
#   }
#   attr {
#     name: "pooling_ratio"
#     type: "list(float)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "pseudo_random"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "overlapping"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "deterministic"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
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
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "FractionalAvgPoolGrad"
#   input_arg {
#     name: "orig_input_tensor_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "out_backprop"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "row_pooling_sequence"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "col_pooling_sequence"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "overlapping"
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
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "FractionalMaxPool"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "row_pooling_sequence"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "col_pooling_sequence"
#     type: DT_INT64
#   }
#   attr {
#     name: "pooling_ratio"
#     type: "list(float)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "pseudo_random"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "overlapping"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "deterministic"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
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
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "FractionalMaxPoolGrad"
#   input_arg {
#     name: "orig_input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "orig_output"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "out_backprop"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "row_pooling_sequence"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "col_pooling_sequence"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "overlapping"
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
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "FusedBatchNorm"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "scale"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "offset"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "mean"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "variance"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "batch_mean"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "batch_variance"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "reserve_space_1"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "reserve_space_2"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "epsilon"
#     type: "float"
#     default_value {
#       f: 0.0001
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "is_training"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "FusedBatchNormGrad"
#   input_arg {
#     name: "y_backprop"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "scale"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "reserve_space_1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "reserve_space_2"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "x_backprop"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "scale_backprop"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "offset_backprop"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "reserve_space_3"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "reserve_space_4"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "epsilon"
#     type: "float"
#     default_value {
#       f: 0.0001
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "is_training"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "FusedBatchNormGradV2"
#   input_arg {
#     name: "y_backprop"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "scale"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "reserve_space_1"
#     type_attr: "U"
#   }
#   input_arg {
#     name: "reserve_space_2"
#     type_attr: "U"
#   }
#   output_arg {
#     name: "x_backprop"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "scale_backprop"
#     type_attr: "U"
#   }
#   output_arg {
#     name: "offset_backprop"
#     type_attr: "U"
#   }
#   output_arg {
#     name: "reserve_space_3"
#     type_attr: "U"
#   }
#   output_arg {
#     name: "reserve_space_4"
#     type_attr: "U"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "U"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "epsilon"
#     type: "float"
#     default_value {
#       f: 0.0001
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "is_training"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "FusedBatchNormV2"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "scale"
#     type_attr: "U"
#   }
#   input_arg {
#     name: "offset"
#     type_attr: "U"
#   }
#   input_arg {
#     name: "mean"
#     type_attr: "U"
#   }
#   input_arg {
#     name: "variance"
#     type_attr: "U"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "batch_mean"
#     type_attr: "U"
#   }
#   output_arg {
#     name: "batch_variance"
#     type_attr: "U"
#   }
#   output_arg {
#     name: "reserve_space_1"
#     type_attr: "U"
#   }
#   output_arg {
#     name: "reserve_space_2"
#     type_attr: "U"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "U"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "epsilon"
#     type: "float"
#     default_value {
#       f: 0.0001
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
#   }
#   attr {
#     name: "is_training"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "FusedPadConv2D"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "paddings"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "filter"
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
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "mode"
#     type: "string"
#     allowed_values {
#       list {
#         s: "REFLECT"
#         s: "SYMMETRIC"
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
# }
# op {
#   name: "FusedResizeAndPadConv2D"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "paddings"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "filter"
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
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "resize_align_corners"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "mode"
#     type: "string"
#     allowed_values {
#       list {
#         s: "REFLECT"
#         s: "SYMMETRIC"
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
# }
# op {
#   name: "InTopK"
#   input_arg {
#     name: "predictions"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "targets"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "precision"
#     type: DT_BOOL
#   }
#   attr {
#     name: "k"
#     type: "int"
#   }
#   attr {
#     name: "T"
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
#   name: "InTopKV2"
#   input_arg {
#     name: "predictions"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "targets"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "k"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "precision"
#     type: DT_BOOL
#   }
#   attr {
#     name: "T"
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
#   name: "L2Loss"
#   input_arg {
#     name: "t"
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
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "LRN"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "depth_radius"
#     type: "int"
#     default_value {
#       i: 5
#     }
#   }
#   attr {
#     name: "bias"
#     type: "float"
#     default_value {
#       f: 1
#     }
#   }
#   attr {
#     name: "alpha"
#     type: "float"
#     default_value {
#       f: 1
#     }
#   }
#   attr {
#     name: "beta"
#     type: "float"
#     default_value {
#       f: 0.5
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "LRNGrad"
#   input_arg {
#     name: "input_grads"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_image"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "output_image"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "depth_radius"
#     type: "int"
#     default_value {
#       i: 5
#     }
#   }
#   attr {
#     name: "bias"
#     type: "float"
#     default_value {
#       f: 1
#     }
#   }
#   attr {
#     name: "alpha"
#     type: "float"
#     default_value {
#       f: 1
#     }
#   }
#   attr {
#     name: "beta"
#     type: "float"
#     default_value {
#       f: 0.5
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "LeakyRelu"
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "activations"
#     type_attr: "T"
#   }
#   attr {
#     name: "alpha"
#     type: "float"
#     default_value {
#       f: 0.2
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "LeakyReluGrad"
#   input_arg {
#     name: "gradients"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "backprops"
#     type_attr: "T"
#   }
#   attr {
#     name: "alpha"
#     type: "float"
#     default_value {
#       f: 0.2
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "LogSoftmax"
#   input_arg {
#     name: "logits"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "logsoftmax"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "MaxPool"
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
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_QINT8
#       }
#     }
#   }
#   attr {
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#         s: "NCHW_VECT_C"
#       }
#     }
#   }
# }
# op {
#   name: "MaxPool3D"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NDHWC"
#     }
#     allowed_values {
#       list {
#         s: "NDHWC"
#         s: "NCDHW"
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "MaxPool3DGrad"
#   input_arg {
#     name: "orig_input"
#     type_attr: "TInput"
#   }
#   input_arg {
#     name: "orig_output"
#     type_attr: "TInput"
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
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NDHWC"
#     }
#     allowed_values {
#       list {
#         s: "NDHWC"
#         s: "NCDHW"
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "TInput"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "MaxPool3DGradGrad"
#   input_arg {
#     name: "orig_input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "orig_output"
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
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NDHWC"
#     }
#     allowed_values {
#       list {
#         s: "NDHWC"
#         s: "NCDHW"
#       }
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
#   name: "MaxPoolGrad"
#   input_arg {
#     name: "orig_input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "orig_output"
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
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
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
#   name: "MaxPoolGradGrad"
#   input_arg {
#     name: "orig_input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "orig_output"
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
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
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
#   name: "MaxPoolGradGradV2"
#   input_arg {
#     name: "orig_input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "orig_output"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "ksize"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "strides"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
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
#   name: "MaxPoolGradGradWithArgmax"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "argmax"
#     type_attr: "Targmax"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "Targmax"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
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
#   name: "MaxPoolGradV2"
#   input_arg {
#     name: "orig_input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "orig_output"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "ksize"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "strides"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#       }
#     }
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
#   name: "MaxPoolGradWithArgmax"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "argmax"
#     type_attr: "Targmax"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "Targmax"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
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
#   name: "MaxPoolV2"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "ksize"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "strides"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_QINT8
#       }
#     }
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#         s: "NCHW_VECT_C"
#       }
#     }
#   }
# }
# op {
#   name: "MaxPoolWithArgmax"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "argmax"
#     type_attr: "Targmax"
#   }
#   attr {
#     name: "ksize"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "Targmax"
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
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
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
#   name: "NthElement"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "n"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "values"
#     type_attr: "T"
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
#   name: "QuantizedAvgPool"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "min_input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_input"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "min_output"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "max_output"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
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
#     name: "ksize"
#     type: "list(int)"
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
# }
# op {
#   name: "QuantizedBatchNormWithGlobalNormalization"
#   input_arg {
#     name: "t"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "t_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "t_max"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "m"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "m_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "m_max"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "v"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "v_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "v_max"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "beta"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "beta_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "beta_max"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "gamma"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "gamma_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "gamma_max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "result"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "result_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "result_max"
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
#   attr {
#     name: "variance_epsilon"
#     type: "float"
#   }
#   attr {
#     name: "scale_after_normalization"
#     type: "bool"
#   }
# }
# op {
#   name: "QuantizedBiasAdd"
#   input_arg {
#     name: "input"
#     type_attr: "T1"
#   }
#   input_arg {
#     name: "bias"
#     type_attr: "T2"
#   }
#   input_arg {
#     name: "min_input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_bias"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_bias"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
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
#   name: "QuantizedConv2D"
#   input_arg {
#     name: "input"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "filter"
#     type_attr: "Tfilter"
#   }
#   input_arg {
#     name: "min_input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_filter"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_filter"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "min_output"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "max_output"
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
#     name: "Tfilter"
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
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
# op {
#   name: "QuantizedMaxPool"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "min_input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_input"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "min_output"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "max_output"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
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
#     name: "ksize"
#     type: "list(int)"
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
# }
# op {
#   name: "QuantizedRelu"
#   input_arg {
#     name: "features"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "min_features"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_features"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "activations"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "min_activations"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "max_activations"
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
#   name: "QuantizedRelu6"
#   input_arg {
#     name: "features"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "min_features"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_features"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "activations"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "min_activations"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "max_activations"
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
#   name: "QuantizedReluX"
#   input_arg {
#     name: "features"
#     type_attr: "Tinput"
#   }
#   input_arg {
#     name: "max_value"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_features"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_features"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "activations"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "min_activations"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "max_activations"
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
#   name: "Relu"
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "activations"
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
#         type: DT_QINT8
#       }
#     }
#   }
# }
# op {
#   name: "Relu6"
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "activations"
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
#   name: "Relu6Grad"
#   input_arg {
#     name: "gradients"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "backprops"
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
#   name: "ReluGrad"
#   input_arg {
#     name: "gradients"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "backprops"
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
#   name: "Selu"
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "activations"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "SeluGrad"
#   input_arg {
#     name: "gradients"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "outputs"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "backprops"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Softmax"
#   input_arg {
#     name: "logits"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "softmax"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "SoftmaxCrossEntropyWithLogits"
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "labels"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "loss"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "backprop"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Softplus"
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "activations"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "SoftplusGrad"
#   input_arg {
#     name: "gradients"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "backprops"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "Softsign"
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "activations"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "SoftsignGrad"
#   input_arg {
#     name: "gradients"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "backprops"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "SparseSoftmaxCrossEntropyWithLogits"
#   input_arg {
#     name: "features"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "labels"
#     type_attr: "Tlabels"
#   }
#   output_arg {
#     name: "loss"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "backprop"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "Tlabels"
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
#   name: "TopK"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   attr {
#     name: "k"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "sorted"
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
#   deprecation {
#     version: 7
#     explanation: "Use TopKV2 instead"
#   }
# }
# op {
#   name: "TopKV2"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "k"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   attr {
#     name: "sorted"
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
_op_def_lib = _InitOpDefLibrary(b"\n\274\001\n\007AvgPool\022\n\n\005value\"\001T\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\004\"\030\n\007strides\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n\301\001\n\tAvgPool3D\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\005\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"0\n\013data_format\022\006string\032\007\022\005NDHWC:\020\n\016\022\005NDHWC\022\005NCDHW\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n\332\001\n\rAvgPool3DGrad\022\024\n\020orig_input_shape\030\003\022\t\n\004grad\"\001T\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\005\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"0\n\013data_format\022\006string\032\007\022\005NDHWC:\020\n\016\022\005NDHWC\022\005NCDHW\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n\325\001\n\013AvgPoolGrad\022\024\n\020orig_input_shape\030\003\022\t\n\004grad\"\001T\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\004\"\030\n\007strides\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n\343\001\n BatchNormWithGlobalNormalization\022\006\n\001t\"\001T\022\006\n\001m\"\001T\022\006\n\001v\"\001T\022\t\n\004beta\"\001T\022\n\n\005gamma\"\001T\032\013\n\006result\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\031\n\020variance_epsilon\022\005float\"!\n\031scale_after_normalization\022\004boolB#\010\t\022\037Use tf.nn.batch_normalization()\n\213\002\n$BatchNormWithGlobalNormalizationGrad\022\006\n\001t\"\001T\022\006\n\001m\"\001T\022\006\n\001v\"\001T\022\n\n\005gamma\"\001T\022\r\n\010backprop\"\001T\032\007\n\002dx\"\001T\032\007\n\002dm\"\001T\032\007\n\002dv\"\001T\032\007\n\002db\"\001T\032\007\n\002dg\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\031\n\020variance_epsilon\022\005float\"!\n\031scale_after_normalization\022\004boolB#\010\t\022\037Use tf.nn.batch_normalization()\n~\n\007BiasAdd\022\n\n\005value\"\001T\022\t\n\004bias\"\001T\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\n~\n\013BiasAddGrad\022\021\n\014out_backprop\"\001T\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\nQ\n\tBiasAddV1\022\n\n\005value\"\001T\022\t\n\004bias\"\001T\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\354\001\n\006Conv2D\022\n\n\005input\"\001T\022\013\n\006filter\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\024\n\007strides\022\tlist(int)\"\034\n\020use_cudnn_on_gpu\022\004bool\032\002(\001\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\" \n\tdilations\022\tlist(int)\032\010\n\006\032\004\001\001\001\001\n\222\002\n\024Conv2DBackpropFilter\022\n\n\005input\"\001T\022\020\n\014filter_sizes\030\003\022\021\n\014out_backprop\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\024\n\007strides\022\tlist(int)\"\034\n\020use_cudnn_on_gpu\022\004bool\032\002(\001\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\" \n\tdilations\022\tlist(int)\032\010\n\006\032\004\001\001\001\001\n\221\002\n\023Conv2DBackpropInput\022\017\n\013input_sizes\030\003\022\013\n\006filter\"\001T\022\021\n\014out_backprop\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\024\n\007strides\022\tlist(int)\"\034\n\020use_cudnn_on_gpu\022\004bool\032\002(\001\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\" \n\tdilations\022\tlist(int)\032\010\n\006\032\004\001\001\001\001\n\326\001\n\006Conv3D\022\n\n\005input\"\001T\022\013\n\006filter\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"0\n\013data_format\022\006string\032\007\022\005NDHWC:\020\n\016\022\005NDHWC\022\005NCDHW\"!\n\tdilations\022\tlist(int)\032\t\n\007\032\005\001\001\001\001\001\n\344\001\n\024Conv3DBackpropFilter\022\n\n\005input\"\001T\022\013\n\006filter\"\001T\022\021\n\014out_backprop\"\001T\032\013\n\006output\"\001T\"\022\n\001T\022\004type:\007\n\0052\003\023\001\002\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"!\n\tdilations\022\tlist(int)\032\t\n\007\032\005\001\001\001\001\001B\036\010\n\022\032Use Conv3DBackpropFilterV2\n\376\001\n\026Conv3DBackpropFilterV2\022\n\n\005input\"\001T\022\020\n\014filter_sizes\030\003\022\021\n\014out_backprop\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"0\n\013data_format\022\006string\032\007\022\005NDHWC:\020\n\016\022\005NDHWC\022\005NCDHW\"!\n\tdilations\022\tlist(int)\032\t\n\007\032\005\001\001\001\001\001\n\342\001\n\023Conv3DBackpropInput\022\n\n\005input\"\001T\022\013\n\006filter\"\001T\022\021\n\014out_backprop\"\001T\032\013\n\006output\"\001T\"\022\n\001T\022\004type:\007\n\0052\003\023\001\002\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"!\n\tdilations\022\tlist(int)\032\t\n\007\032\005\001\001\001\001\001B\035\010\n\022\031Use Conv3DBackpropInputV2\n\237\002\n\025Conv3DBackpropInputV2\022\025\n\013input_sizes\"\006Tshape\022\013\n\006filter\"\001T\022\021\n\014out_backprop\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"0\n\013data_format\022\006string\032\007\022\005NDHWC:\020\n\016\022\005NDHWC\022\005NCDHW\"!\n\tdilations\022\tlist(int)\032\t\n\007\032\005\001\001\001\001\001\"\032\n\006Tshape\022\004type\032\0020\003:\006\n\0042\002\003\t\nu\n\020DataFormatDimMap\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\"\034\n\nsrc_format\022\006string\032\006\022\004NHWC\"\034\n\ndst_format\022\006string\032\006\022\004NCHW\ny\n\024DataFormatVecPermute\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\"\034\n\nsrc_format\022\006string\032\006\022\004NHWC\"\034\n\ndst_format\022\006string\032\006\022\004NCHW\n\335\001\n\025DepthwiseConv2dNative\022\n\n\005input\"\001T\022\013\n\006filter\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\024\n\007strides\022\tlist(int)\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\" \n\tdilations\022\tlist(int)\032\010\n\006\032\004\001\001\001\001\n\203\002\n#DepthwiseConv2dNativeBackpropFilter\022\n\n\005input\"\001T\022\020\n\014filter_sizes\030\003\022\021\n\014out_backprop\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\024\n\007strides\022\tlist(int)\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\" \n\tdilations\022\tlist(int)\032\010\n\006\032\004\001\001\001\001\n\202\002\n\"DepthwiseConv2dNativeBackpropInput\022\017\n\013input_sizes\030\003\022\013\n\006filter\"\001T\022\021\n\014out_backprop\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\024\n\007strides\022\tlist(int)\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\" \n\tdilations\022\tlist(int)\032\010\n\006\032\004\001\001\001\001\n\245\001\n\nDilation2D\022\n\n\005input\"\001T\022\013\n\006filter\"\001T\032\013\n\006output\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\030\n\007strides\022\tlist(int)(\0010\004\"\026\n\005rates\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\n\317\001\n\030Dilation2DBackpropFilter\022\n\n\005input\"\001T\022\013\n\006filter\"\001T\022\021\n\014out_backprop\"\001T\032\024\n\017filter_backprop\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\030\n\007strides\022\tlist(int)(\0010\004\"\026\n\005rates\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\n\312\001\n\027Dilation2DBackpropInput\022\n\n\005input\"\001T\022\013\n\006filter\"\001T\022\021\n\014out_backprop\"\001T\032\020\n\013in_backprop\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\030\n\007strides\022\tlist(int)(\0010\004\"\026\n\005rates\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\n;\n\003Elu\022\r\n\010features\"\001T\032\020\n\013activations\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\nL\n\007EluGrad\022\016\n\tgradients\"\001T\022\014\n\007outputs\"\001T\032\016\n\tbackprops\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n\211\002\n\021FractionalAvgPool\022\n\n\005value\"\001T\032\013\n\006output\"\001T\032\030\n\024row_pooling_sequence\030\t\032\030\n\024col_pooling_sequence\030\t\" \n\rpooling_ratio\022\013list(float)(\0010\004\"\031\n\rpseudo_random\022\004bool\032\002(\000\"\027\n\013overlapping\022\004bool\032\002(\000\"\031\n\rdeterministic\022\004bool\032\002(\000\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\023\n\001T\022\004type:\010\n\0062\004\001\002\003\t\n\266\001\n\025FractionalAvgPoolGrad\022\033\n\027orig_input_tensor_shape\030\t\022\021\n\014out_backprop\"\001T\022\030\n\024row_pooling_sequence\030\t\022\030\n\024col_pooling_sequence\030\t\032\013\n\006output\"\001T\"\027\n\013overlapping\022\004bool\032\002(\000\"\023\n\001T\022\004type:\010\n\0062\004\001\002\003\t\n\211\002\n\021FractionalMaxPool\022\n\n\005value\"\001T\032\013\n\006output\"\001T\032\030\n\024row_pooling_sequence\030\t\032\030\n\024col_pooling_sequence\030\t\" \n\rpooling_ratio\022\013list(float)(\0010\004\"\031\n\rpseudo_random\022\004bool\032\002(\000\"\027\n\013overlapping\022\004bool\032\002(\000\"\031\n\rdeterministic\022\004bool\032\002(\000\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\023\n\001T\022\004type:\010\n\0062\004\001\002\003\t\n\274\001\n\025FractionalMaxPoolGrad\022\017\n\norig_input\"\001T\022\020\n\013orig_output\"\001T\022\021\n\014out_backprop\"\001T\022\030\n\024row_pooling_sequence\030\t\022\030\n\024col_pooling_sequence\030\t\032\013\n\006output\"\001T\"\027\n\013overlapping\022\004bool\032\002(\000\"\023\n\001T\022\004type:\010\n\0062\004\001\002\003\t\n\230\002\n\016FusedBatchNorm\022\006\n\001x\"\001T\022\n\n\005scale\"\001T\022\013\n\006offset\"\001T\022\t\n\004mean\"\001T\022\r\n\010variance\"\001T\032\006\n\001y\"\001T\032\017\n\nbatch_mean\"\001T\032\023\n\016batch_variance\"\001T\032\024\n\017reserve_space_1\"\001T\032\024\n\017reserve_space_2\"\001T\"\020\n\001T\022\004type:\005\n\0032\001\001\"\027\n\007epsilon\022\005float\032\005%\027\267\3218\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\027\n\013is_training\022\004bool\032\002(\001\n\300\002\n\022FusedBatchNormGrad\022\017\n\ny_backprop\"\001T\022\006\n\001x\"\001T\022\n\n\005scale\"\001T\022\024\n\017reserve_space_1\"\001T\022\024\n\017reserve_space_2\"\001T\032\017\n\nx_backprop\"\001T\032\023\n\016scale_backprop\"\001T\032\024\n\017offset_backprop\"\001T\032\024\n\017reserve_space_3\"\001T\032\024\n\017reserve_space_4\"\001T\"\020\n\001T\022\004type:\005\n\0032\001\001\"\027\n\007epsilon\022\005float\032\005%\027\267\3218\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\027\n\013is_training\022\004bool\032\002(\001\n\325\002\n\024FusedBatchNormGradV2\022\017\n\ny_backprop\"\001T\022\006\n\001x\"\001T\022\t\n\005scale\030\001\022\024\n\017reserve_space_1\"\001U\022\024\n\017reserve_space_2\"\001U\032\017\n\nx_backprop\"\001T\032\023\n\016scale_backprop\"\001U\032\024\n\017offset_backprop\"\001U\032\024\n\017reserve_space_3\"\001U\032\024\n\017reserve_space_4\"\001U\"\022\n\001T\022\004type:\007\n\0052\003\023\016\001\"\020\n\001U\022\004type:\005\n\0032\001\001\"\027\n\007epsilon\022\005float\032\005%\027\267\3218\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\027\n\013is_training\022\004bool\032\002(\001\n\256\002\n\020FusedBatchNormV2\022\006\n\001x\"\001T\022\n\n\005scale\"\001U\022\013\n\006offset\"\001U\022\t\n\004mean\"\001U\022\r\n\010variance\"\001U\032\006\n\001y\"\001T\032\017\n\nbatch_mean\"\001U\032\023\n\016batch_variance\"\001U\032\024\n\017reserve_space_1\"\001U\032\024\n\017reserve_space_2\"\001U\"\022\n\001T\022\004type:\007\n\0052\003\023\016\001\"\020\n\001U\022\004type:\005\n\0032\001\001\"\027\n\007epsilon\022\005float\032\005%\027\267\3218\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\027\n\013is_training\022\004bool\032\002(\001\n\272\001\n\016FusedPadConv2D\022\n\n\005input\"\001T\022\014\n\010paddings\030\003\022\013\n\006filter\"\001T\032\013\n\006output\"\001T\"\022\n\001T\022\004type:\007\n\0052\003\023\001\002\"&\n\004mode\022\006string:\026\n\024\022\007REFLECT\022\tSYMMETRIC\"\024\n\007strides\022\tlist(int)\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\n\357\001\n\027FusedResizeAndPadConv2D\022\n\n\005input\"\001T\022\010\n\004size\030\003\022\014\n\010paddings\030\003\022\013\n\006filter\"\001T\032\013\n\006output\"\001T\"\022\n\001T\022\004type:\007\n\0052\003\023\001\002\" \n\024resize_align_corners\022\004bool\032\002(\000\"&\n\004mode\022\006string:\026\n\024\022\007REFLECT\022\tSYMMETRIC\"\024\n\007strides\022\tlist(int)\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\nW\n\006InTopK\022\017\n\013predictions\030\001\022\014\n\007targets\"\001T\032\r\n\tprecision\030\n\"\010\n\001k\022\003int\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\nW\n\010InTopKV2\022\017\n\013predictions\030\001\022\014\n\007targets\"\001T\022\006\n\001k\"\001T\032\r\n\tprecision\030\n\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\n2\n\006L2Loss\022\006\n\001t\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n\222\001\n\003LRN\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\027\n\014depth_radius\022\003int\032\002\030\005\"\024\n\004bias\022\005float\032\005%\000\000\200?\"\025\n\005alpha\022\005float\032\005%\000\000\200?\"\024\n\004beta\022\005float\032\005%\000\000\000?\"\026\n\001T\022\004type\032\0020\001:\007\n\0052\003\023\016\001\n\301\001\n\007LRNGrad\022\020\n\013input_grads\"\001T\022\020\n\013input_image\"\001T\022\021\n\014output_image\"\001T\032\013\n\006output\"\001T\"\027\n\014depth_radius\022\003int\032\002\030\005\"\024\n\004bias\022\005float\032\005%\000\000\200?\"\025\n\005alpha\022\005float\032\005%\000\000\200?\"\024\n\004beta\022\005float\032\005%\000\000\000?\"\026\n\001T\022\004type\032\0020\001:\007\n\0052\003\023\016\001\n\\\n\tLeakyRelu\022\r\n\010features\"\001T\032\020\n\013activations\"\001T\"\025\n\005alpha\022\005float\032\005%\315\314L>\"\027\n\001T\022\004type\032\0020\001:\010\n\0062\004\023\016\001\002\nn\n\rLeakyReluGrad\022\016\n\tgradients\"\001T\022\r\n\010features\"\001T\032\016\n\tbackprops\"\001T\"\025\n\005alpha\022\005float\032\005%\315\314L>\"\027\n\001T\022\004type\032\0020\001:\010\n\0062\004\023\016\001\002\n?\n\nLogSoftmax\022\013\n\006logits\"\001T\032\017\n\nlogsoftmax\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n\324\001\n\007MaxPool\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\036\n\001T\022\004type\032\0020\001:\017\n\r2\013\023\016\001\002\003\t\004\005\006\021\013\"\026\n\005ksize\022\tlist(int)(\0010\004\"\030\n\007strides\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\":\n\013data_format\022\006string\032\006\022\004NHWC:\033\n\031\022\004NHWC\022\004NCHW\022\013NCHW_VECT_C\n\300\001\n\tMaxPool3D\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\005\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"0\n\013data_format\022\006string\032\007\022\005NDHWC:\020\n\016\022\005NDHWC\022\005NCDHW\"\022\n\001T\022\004type:\007\n\0052\003\023\016\001\n\221\002\n\rMaxPool3DGrad\022\024\n\norig_input\"\006TInput\022\025\n\013orig_output\"\006TInput\022\t\n\004grad\"\001T\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\005\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"0\n\013data_format\022\006string\032\007\022\005NDHWC:\020\n\016\022\005NDHWC\022\005NCDHW\"\026\n\001T\022\004type\032\0020\001:\007\n\0052\003\023\016\001\"\033\n\006TInput\022\004type\032\0020\001:\007\n\0052\003\023\016\001\n\363\001\n\021MaxPool3DGradGrad\022\017\n\norig_input\"\001T\022\020\n\013orig_output\"\001T\022\t\n\004grad\"\001T\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\005\"\030\n\007strides\022\tlist(int)(\0010\005\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"0\n\013data_format\022\006string\032\007\022\005NDHWC:\020\n\016\022\005NDHWC\022\005NCDHW\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\356\001\n\013MaxPoolGrad\022\017\n\norig_input\"\001T\022\020\n\013orig_output\"\001T\022\t\n\004grad\"\001T\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\004\"\030\n\007strides\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\037\n\001T\022\004type\032\0020\001:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\356\001\n\017MaxPoolGradGrad\022\017\n\norig_input\"\001T\022\020\n\013orig_output\"\001T\022\t\n\004grad\"\001T\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\004\"\030\n\007strides\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\326\001\n\021MaxPoolGradGradV2\022\017\n\norig_input\"\001T\022\020\n\013orig_output\"\001T\022\t\n\004grad\"\001T\022\t\n\005ksize\030\003\022\013\n\007strides\030\003\032\013\n\006output\"\001T\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\336\001\n\031MaxPoolGradGradWithArgmax\022\n\n\005input\"\001T\022\t\n\004grad\"\001T\022\021\n\006argmax\"\007Targmax\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\004\"\030\n\007strides\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"\027\n\007Targmax\022\004type:\006\n\0042\002\003\t\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\326\001\n\rMaxPoolGradV2\022\017\n\norig_input\"\001T\022\020\n\013orig_output\"\001T\022\t\n\004grad\"\001T\022\t\n\005ksize\030\003\022\013\n\007strides\030\003\032\013\n\006output\"\001T\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"-\n\013data_format\022\006string\032\006\022\004NHWC:\016\n\014\022\004NHWC\022\004NCHW\"\037\n\001T\022\004type\032\0020\001:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\332\001\n\025MaxPoolGradWithArgmax\022\n\n\005input\"\001T\022\t\n\004grad\"\001T\022\021\n\006argmax\"\007Targmax\032\013\n\006output\"\001T\"\026\n\005ksize\022\tlist(int)(\0010\004\"\030\n\007strides\022\tlist(int)(\0010\004\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"\027\n\007Targmax\022\004type:\006\n\0042\002\003\t\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\274\001\n\tMaxPoolV2\022\n\n\005input\"\001T\022\t\n\005ksize\030\003\022\013\n\007strides\030\003\032\013\n\006output\"\001T\"\036\n\001T\022\004type\032\0020\001:\017\n\r2\013\023\016\001\002\003\t\004\005\006\021\013\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\":\n\013data_format\022\006string\032\006\022\004NHWC:\033\n\031\022\004NHWC\022\004NCHW\022\013NCHW_VECT_C\n\317\001\n\021MaxPoolWithArgmax\022\n\n\005input\"\001T\032\013\n\006output\"\001T\032\021\n\006argmax\"\007Targmax\"\026\n\005ksize\022\tlist(int)(\0010\004\"\030\n\007strides\022\tlist(int)(\0010\004\"\033\n\007Targmax\022\004type\032\0020\t:\006\n\0042\002\003\t\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n^\n\nNthElement\022\n\n\005input\"\001T\022\005\n\001n\030\003\032\013\n\006values\"\001T\"\023\n\007reverse\022\004bool\032\002(\000\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\315\001\n\020QuantizedAvgPool\022\n\n\005input\"\001T\022\r\n\tmin_input\030\001\022\r\n\tmax_input\030\001\032\013\n\006output\"\001T\032\016\n\nmin_output\030\001\032\016\n\nmax_output\030\001\"\024\n\001T\022\004type:\t\n\0072\005\013\014\r\017\020\"\022\n\005ksize\022\tlist(int)\"\024\n\007strides\022\tlist(int)\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\n\231\003\n)QuantizedBatchNormWithGlobalNormalization\022\013\n\001t\"\006Tinput\022\t\n\005t_min\030\001\022\t\n\005t_max\030\001\022\013\n\001m\"\006Tinput\022\t\n\005m_min\030\001\022\t\n\005m_max\030\001\022\013\n\001v\"\006Tinput\022\t\n\005v_min\030\001\022\t\n\005v_max\030\001\022\016\n\004beta\"\006Tinput\022\014\n\010beta_min\030\001\022\014\n\010beta_max\030\001\022\017\n\005gamma\"\006Tinput\022\r\n\tgamma_min\030\001\022\r\n\tgamma_max\030\001\032\022\n\006result\"\010out_type\032\016\n\nresult_min\030\001\032\016\n\nresult_max\030\001\"\031\n\006Tinput\022\004type:\t\n\0072\005\013\014\r\017\020\"\033\n\010out_type\022\004type:\t\n\0072\005\013\014\r\017\020\"\031\n\020variance_epsilon\022\005float\"!\n\031scale_after_normalization\022\004bool\n\336\001\n\020QuantizedBiasAdd\022\013\n\005input\"\002T1\022\n\n\004bias\"\002T2\022\r\n\tmin_input\030\001\022\r\n\tmax_input\030\001\022\014\n\010min_bias\030\001\022\014\n\010max_bias\030\001\032\022\n\006output\"\010out_type\032\013\n\007min_out\030\001\032\013\n\007max_out\030\001\"\025\n\002T1\022\004type:\t\n\0072\005\013\014\r\017\020\"\025\n\002T2\022\004type:\t\n\0072\005\013\014\r\017\020\"\033\n\010out_type\022\004type:\t\n\0072\005\013\014\r\017\020\n\333\002\n\017QuantizedConv2D\022\017\n\005input\"\006Tinput\022\021\n\006filter\"\007Tfilter\022\r\n\tmin_input\030\001\022\r\n\tmax_input\030\001\022\016\n\nmin_filter\030\001\022\016\n\nmax_filter\030\001\032\022\n\006output\"\010out_type\032\016\n\nmin_output\030\001\032\016\n\nmax_output\030\001\"\031\n\006Tinput\022\004type:\t\n\0072\005\013\014\r\017\020\"\032\n\007Tfilter\022\004type:\t\n\0072\005\013\014\r\017\020\"\037\n\010out_type\022\004type\032\0020\r:\t\n\0072\005\013\014\r\017\020\"\024\n\007strides\022\tlist(int)\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\" \n\tdilations\022\tlist(int)\032\010\n\006\032\004\001\001\001\001\n\315\001\n\020QuantizedMaxPool\022\n\n\005input\"\001T\022\r\n\tmin_input\030\001\022\r\n\tmax_input\030\001\032\013\n\006output\"\001T\032\016\n\nmin_output\030\001\032\016\n\nmax_output\030\001\"\024\n\001T\022\004type:\t\n\0072\005\013\014\r\017\020\"\022\n\005ksize\022\tlist(int)\"\024\n\007strides\022\tlist(int)\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\n\306\001\n\rQuantizedRelu\022\022\n\010features\"\006Tinput\022\020\n\014min_features\030\001\022\020\n\014max_features\030\001\032\027\n\013activations\"\010out_type\032\023\n\017min_activations\030\001\032\023\n\017max_activations\030\001\"\031\n\006Tinput\022\004type:\t\n\0072\005\013\014\r\017\020\"\037\n\010out_type\022\004type\032\0020\014:\t\n\0072\005\013\014\r\017\020\n\307\001\n\016QuantizedRelu6\022\022\n\010features\"\006Tinput\022\020\n\014min_features\030\001\022\020\n\014max_features\030\001\032\027\n\013activations\"\010out_type\032\023\n\017min_activations\030\001\032\023\n\017max_activations\030\001\"\031\n\006Tinput\022\004type:\t\n\0072\005\013\014\r\017\020\"\037\n\010out_type\022\004type\032\0020\014:\t\n\0072\005\013\014\r\017\020\n\326\001\n\016QuantizedReluX\022\022\n\010features\"\006Tinput\022\r\n\tmax_value\030\001\022\020\n\014min_features\030\001\022\020\n\014max_features\030\001\032\027\n\013activations\"\010out_type\032\023\n\017min_activations\030\001\032\023\n\017max_activations\030\001\"\031\n\006Tinput\022\004type:\t\n\0072\005\013\014\r\017\020\"\037\n\010out_type\022\004type\032\0020\014:\t\n\0072\005\013\014\r\017\020\nE\n\004Relu\022\r\n\010features\"\001T\032\020\n\013activations\"\001T\"\034\n\001T\022\004type:\021\n\0172\r\001\002\003\004\005\006\t\016\021\023\026\027\013\nE\n\005Relu6\022\r\n\010features\"\001T\032\020\n\013activations\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\nW\n\tRelu6Grad\022\016\n\tgradients\"\001T\022\r\n\010features\"\001T\032\016\n\tbackprops\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\nV\n\010ReluGrad\022\016\n\tgradients\"\001T\022\r\n\010features\"\001T\032\016\n\tbackprops\"\001T\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n<\n\004Selu\022\r\n\010features\"\001T\032\020\n\013activations\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\nM\n\010SeluGrad\022\016\n\tgradients\"\001T\022\014\n\007outputs\"\001T\032\016\n\tbackprops\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n9\n\007Softmax\022\013\n\006logits\"\001T\032\014\n\007softmax\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\nj\n\035SoftmaxCrossEntropyWithLogits\022\r\n\010features\"\001T\022\013\n\006labels\"\001T\032\t\n\004loss\"\001T\032\r\n\010backprop\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n@\n\010Softplus\022\r\n\010features\"\001T\032\020\n\013activations\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\nR\n\014SoftplusGrad\022\016\n\tgradients\"\001T\022\r\n\010features\"\001T\032\016\n\tbackprops\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n@\n\010Softsign\022\r\n\010features\"\001T\032\020\n\013activations\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\nR\n\014SoftsignGrad\022\016\n\tgradients\"\001T\022\r\n\010features\"\001T\032\016\n\tbackprops\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\n\223\001\n#SparseSoftmaxCrossEntropyWithLogits\022\r\n\010features\"\001T\022\021\n\006labels\"\007Tlabels\032\t\n\004loss\"\001T\032\r\n\010backprop\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\023\016\001\002\"\033\n\007Tlabels\022\004type\032\0020\t:\006\n\0042\002\003\t\n\201\001\n\004TopK\022\n\n\005input\"\001T\032\013\n\006values\"\001T\032\013\n\007indices\030\003\"\n\n\001k\022\003int(\001\"\022\n\006sorted\022\004bool\032\002(\001\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027B\026\010\007\022\022Use TopKV2 instead\nf\n\006TopKV2\022\n\n\005input\"\001T\022\005\n\001k\030\003\032\013\n\006values\"\001T\032\013\n\007indices\030\003\"\022\n\006sorted\022\004bool\032\002(\001\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027")
